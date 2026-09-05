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
