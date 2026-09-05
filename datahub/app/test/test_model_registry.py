"""Tests for the scoring model-version registry.

The registry maps an immutable model_version -> per-horizon scoring override
(weights / thresholds / component directions) pinned by a config_hash. A
scoring service that names a registered version loads that config; explicit
scoring_config still wins; unregistered versions fall back to built-in
SCORING_CONFIG (backward compatible).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.lib.scoring_engine.config import model_config_hash
from app.lib.scoring_engine.scoring_service import StockScoringService


class _Registered:
    def __init__(self, config: dict, status: str = "ACTIVE"):
        self.config = config
        self.status = status


class _RegistryQS:
    def __init__(self, matches: list):
        self._matches = matches

    def first(self):
        return self._matches[0] if self._matches else None


def test_model_config_hash_order_insensitive():
    a = model_config_hash(
        {"20": {"directions": {"momentum": -1}, "weights": {"momentum": 40.0}}}
    )
    b = model_config_hash(
        {"20": {"weights": {"momentum": 40.0}, "directions": {"momentum": -1}}}
    )
    assert a == b
    assert len(a) == 64


def test_registered_config_loaded_when_no_explicit_config():
    reg = _Registered({"20": {"directions": {"momentum": -1}}})
    qs = _RegistryQS([reg])
    with (
        patch(
            "app.model.scoring.ScoreModelVersion.objects",
            return_value=qs,
        ) as objects_mock,
        patch("app.lib.scoring_engine.scoring_service.FinanceMarket.objects") as market,
    ):
        market.return_value.first.return_value = MagicMock(trade_calendar=[])
        service = StockScoringService(model_version="flip_v1")
    assert service.scoring_config == {"20": {"directions": {"momentum": -1}}}
    # The registry lookup must filter status="ACTIVE" at the query level so a
    # retired version never feeds the scoring run.
    objects_mock.assert_called_once_with(model_version="flip_v1", status="ACTIVE")


def test_explicit_config_wins_over_registered():
    reg = _Registered({"20": {"directions": {"momentum": -1}}})
    explicit = {"20": {"directions": {"trend_alignment": -1}}}
    with (
        patch(
            "app.model.scoring.ScoreModelVersion.objects",
            return_value=_RegistryQS([reg]),
        ),
        patch("app.lib.scoring_engine.scoring_service.FinanceMarket.objects") as market,
    ):
        market.return_value.first.return_value = MagicMock(trade_calendar=[])
        service = StockScoringService(model_version="flip_v1", scoring_config=explicit)
    assert service.scoring_config == explicit


def test_unregistered_version_falls_back_to_builtin():
    with (
        patch(
            "app.model.scoring.ScoreModelVersion.objects",
            return_value=_RegistryQS([]),
        ),
        patch("app.lib.scoring_engine.scoring_service.FinanceMarket.objects") as market,
    ):
        market.return_value.first.return_value = MagicMock(trade_calendar=[])
        service = StockScoringService(model_version="never_registered")
    assert service.scoring_config == {}


def test_registry_db_error_falls_back_cleanly():
    with (
        patch(
            "app.model.scoring.ScoreModelVersion.objects",
            side_effect=RuntimeError("db down"),
        ),
        patch("app.lib.scoring_engine.scoring_service.FinanceMarket.objects") as market,
    ):
        market.return_value.first.return_value = MagicMock(trade_calendar=[])
        service = StockScoringService(model_version="flip_v1")
    assert service.scoring_config == {}


class TestRegistryValidation:
    def test_invalid_horizon_rejected(self):
        from app.jobs.model_registry_runner import _validate_config

        with pytest.raises(ValueError, match="horizon"):
            _validate_config({"99": {"weights": {}}})

    def test_invalid_direction_rejected_at_registration(self):
        from app.jobs.model_registry_runner import _validate_config

        with pytest.raises(ValueError, match="must be -1, 0, or 1"):
            _validate_config({"20": {"directions": {"momentum": 5}}})

    def test_unknown_direction_key_rejected(self):
        from app.jobs.model_registry_runner import _validate_config

        with pytest.raises(ValueError, match="direction override keys"):
            _validate_config({"20": {"directions": {"bogus": -1}}})

    def test_valid_flip_config_accepts(self):
        from app.jobs.model_registry_runner import _validate_config

        _validate_config(
            {"20": {"directions": {"signal_strength": -1, "momentum": -1}}}
        )  # no raise


class TestRegistryRunnerFunctions:
    """Coverage for jobs.model_registry_runner register/list/retire logic."""

    def _fake_model(self, monkeypatch):
        from app.jobs import model_registry_runner as runner

        records = []

        class FakeDoc:
            def __init__(self, **kwargs):
                self.activated_at = kwargs.pop("activated_at", None)
                self.retired_at = kwargs.pop("retired_at", None)
                self.updated_at = kwargs.pop("updated_at", None)
                self.created_at = kwargs.pop("created_at", None)
                self.__dict__.update(kwargs)
                records.append(self)

            def save(self, *a, **k):
                return self

            def delete(self):
                records.remove(self)

        class FakeQS:
            def __init__(self, matches):
                self._matches = matches

            def first(self):
                return self._matches[0] if self._matches else None

            def order_by(self, *a, **k):
                return self

            def __iter__(self):
                return iter(self._matches)

        class FakeModel(FakeDoc):
            @classmethod
            def objects(cls, **kwargs):
                matches = [
                    r
                    for r in records
                    if all(getattr(r, k, None) == v for k, v in kwargs.items())
                ]
                return FakeQS(matches)

        # register/list/retire import ScoreModelVersion lazily inside each
        # function, so patch the module attribute they import from.
        import app.model.scoring as model_scoring

        monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
        return runner, FakeDoc

    def test_register_and_list(self, monkeypatch):
        from app.lib.scoring_engine.config import model_config_hash

        runner, _ = self._fake_model(monkeypatch)
        cfg = {"20": {"directions": {"momentum": -1}}}
        out = runner.register("v1", cfg)
        assert out["status"] == "ACTIVE"
        assert out["config_hash"] == model_config_hash(cfg)
        versions = runner.list_versions()
        assert versions[0]["model_version"] == "v1"
        assert versions[0]["config_hash"] == model_config_hash(cfg)

    def test_register_duplicate_requires_force(self, monkeypatch):
        runner, _ = self._fake_model(monkeypatch)
        runner.register("v1", {"20": {"directions": {"momentum": -1}}})
        import pytest

        with pytest.raises(ValueError, match="already registered"):
            runner.register("v1", {"20": {"directions": {"momentum": 1}}})
        # force replaces: config_hash changes
        from app.lib.scoring_engine.config import model_config_hash

        out = runner.register("v1", {"20": {"directions": {"momentum": 1}}}, force=True)
        assert out["status"] == "ACTIVE"
        versions = runner.list_versions()
        assert versions[0]["config_hash"] != model_config_hash(
            {"20": {"directions": {"momentum": -1}}}
        )

    def test_retire_active_only(self, monkeypatch):
        runner, _ = self._fake_model(monkeypatch)
        runner.register("v1", {"20": {"directions": {"momentum": -1}}})
        out = runner.retire("v1")
        assert out["status"] == "RETIRED"
        versions = runner.list_versions()
        assert versions[0]["status"] == "RETIRED"
        # retired version no longer loads as active config
        from app.lib.scoring_engine.scoring_service import StockScoringService

        with patch(
            "app.lib.scoring_engine.scoring_service.FinanceMarket.objects"
        ) as market:
            market.return_value.first.return_value = MagicMock(trade_calendar=[])
            service = StockScoringService(model_version="v1")
        assert service.scoring_config == {}

    def test_retire_missing_raises(self, monkeypatch):
        runner, _ = self._fake_model(monkeypatch)
        import pytest

        with pytest.raises(ValueError, match="no ACTIVE registration"):
            runner.retire("ghost")


class TestWeightsValidation:
    def test_typo_weight_key_rejected(self):
        from app.jobs.model_registry_runner import _validate_config

        with pytest.raises(ValueError, match="real scored components"):
            _validate_config({"20": {"weights": {"momemtum": 60.0}}})

    def test_valid_weight_key_accepts(self):
        from app.jobs.model_registry_runner import _validate_config

        _validate_config({"20": {"weights": {"momentum": 40.0}}})  # no raise

    def test_non_dict_weights_rejected(self):
        from app.jobs.model_registry_runner import _validate_config

        with pytest.raises(ValueError, match="weights override must be a dict"):
            _validate_config({"20": {"weights": [("momentum", 40.0)]}})

    def test_typo_direction_key_rejected(self):
        from app.jobs.model_registry_runner import _validate_config

        with pytest.raises(ValueError, match="real scored components"):
            _validate_config({"20": {"directions": {"momemtum": -1}}})


class TestFlipWideShadowArtifact:
    """flip_wide shadow registration config (research -> registry bridge).

    The artifact registers the construction-layer reversal at horizon 20 only
    (the horizon whose production weights equal the research composite, sum
    110). It must validate, resolve to flipped directions, pin a stable hash,
    and register as a non-default shadow version (DEFAULT_MODEL_VERSION
    unchanged). No DB: _validate_config is pure and register() is exercised
    through the same fake-model harness as the other registry tests.
    """

    _ARTIFACT = (
        "datahub/research/autoresearch/h20_excess_alpha/flip_wide_registry_config.json"
    )

    @staticmethod
    def _load():
        import json
        from pathlib import Path

        path = Path(TestFlipWideShadowArtifact._ARTIFACT)
        assert path.exists(), f"artifact missing: {path}"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_artifact_validates(self):
        from app.jobs.model_registry_runner import _validate_config

        _validate_config(self._load())  # no raise

    def test_artifact_resolves_flipped_directions(self):
        from app.lib.scoring_engine.config import get_effective_horizon_config

        resolved = get_effective_horizon_config(20, self._load())
        directions = resolved["directions"]
        for component in (
            "signal_strength",
            "momentum",
            "trend_alignment",
            "breakout_or_position",
            "industry_momentum",
            "relative_strength",
            "real_relative_strength",
        ):
            assert directions[component] == -1, component
        # risk_penalty keeps its default penalty direction (never flipped).
        assert directions["risk_penalty"] == -1

    def test_artifact_weights_match_production_h20(self):
        resolved = self._load()["20"]["weights"]
        assert abs(sum(resolved.values()) - 110.0) < 1e-9

    def test_artifact_config_hash_stable(self):
        # Golden hash pins the registered semantics: any accidental change to
        # the artifact (directions/weights/horizon) breaks this test.
        from app.lib.scoring_engine.config import model_config_hash

        assert (
            model_config_hash(self._load())
            == "8c8f3ee4b2f32f54281a7ecbb04b33b87de15fae1826a640d056df4575e547dc"
        )

    def test_artifact_registers_as_shadow(self, monkeypatch):
        from app.lib.scoring_engine.config import model_config_hash

        registry_runner = _fake_runner_with_model(monkeypatch)
        out = registry_runner.register(
            "flip_wide_shadow_v1", self._load(), description="shadow"
        )
        assert out["status"] == "ACTIVE"
        assert out["config_hash"] == model_config_hash(self._load())


def _fake_runner_with_model(monkeypatch):
    """Minimal fake ScoreModelVersion harness for register/list/retire."""
    import app.jobs.model_registry_runner as runner_module
    import app.model.scoring as model_scoring

    records = []

    class FakeDoc:
        def __init__(self, **kwargs):
            self.model_version = kwargs["model_version"]
            self.config = kwargs.get("config") or {}
            self.config_hash = kwargs.get("config_hash")
            self.status = kwargs.get("status", "ACTIVE")
            self.description = kwargs.get("description", "")

        def delete(self):
            records[:] = [r for r in records if r is not self]

        def save(self):
            records.append(self)

    class FakeQS:
        def __init__(self, items):
            self.items = items

        def first(self):
            return self.items[0] if self.items else None

        def order_by(self, *fields):
            return self

        def __iter__(self):
            return iter(self.items)

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            matches = [
                r
                for r in records
                if all(getattr(r, k, None) == v for k, v in query.items())
            ]
            return FakeQS(matches)

        def __init__(self, **kwargs):
            self.model_version = kwargs["model_version"]
            self.description = kwargs.get("description", "")
            self.scoring_mode = kwargs.get("scoring_mode")
            self.config = kwargs.get("config") or {}
            self.config_hash = kwargs.get("config_hash")
            self.status = kwargs.get("status", "ACTIVE")
            records.append(self)

        def delete(self):
            records[:] = [r for r in records if r is not self]

        def save(self):
            return self

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    monkeypatch.setattr(runner_module, "_init_db", lambda: None)
    return runner_module
