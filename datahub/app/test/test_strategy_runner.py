# -*- coding: utf-8 -*-
"""Tests for the strategy_runner job wiring (parser + fail-closed registry).

DB-dependent run_strategy internals are exercised with monkeypatched models;
the engine decision logic itself is covered in test_strategy_engine.py.
"""

import datetime


class _FakeRegistered:
    def __init__(self, config):
        self.config = config


class _FakeQS:
    def __init__(self, items):
        self.items = items

    def first(self):
        return self.items[0] if self.items else None

    def limit(self, count):
        return _FakeQS(self.items[:count])

    def order_by(self, *fields):
        return self

    def __iter__(self):
        return iter(self.items)

    def delete(self):
        self.items = []


def test_resolve_model_version_ok(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring

    cfg = {"score_model_version": "flip_wide_shadow_v1", "horizon": 20}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            assert query["model_version"] == "flip_wide_shadow_v1"
            assert query["status"] == "ACTIVE"
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    assert strategy_runner._resolve_model_version(cfg) == "flip_wide_shadow_v1"


def test_resolve_model_version_fails_closed_when_unregistered(monkeypatch):
    import pytest

    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring

    cfg = {"score_model_version": "ghost_v1", "horizon": 20}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    with pytest.raises(ValueError, match="not ACTIVE-registered"):
        strategy_runner._resolve_model_version(cfg)


def test_resolve_model_version_fails_closed_when_horizon_missing(monkeypatch):
    import pytest

    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring

    cfg = {"score_model_version": "flip_wide_shadow_v1", "horizon": 60}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    with pytest.raises(ValueError, match="does not cover"):
        strategy_runner._resolve_model_version(cfg)


def test_main_run_parser_requires_date(monkeypatch):
    import pytest

    import app.jobs.strategy_runner as strategy_runner

    def fake_main_guard(_):
        raise AssertionError("must not run without date")

    monkeypatch.setattr(strategy_runner, "_init_db", lambda: None)
    monkeypatch.setattr(strategy_runner, "_run_with_tracking", fake_main_guard)
    with pytest.raises(SystemExit) as excinfo:
        strategy_runner.main(["run"])
    assert excinfo.value.code == 2


def test_main_report_parser_requires_date():
    import pytest

    from app.jobs.strategy_runner import build_parser

    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["report"])
    assert excinfo.value.code == 2


def test_run_strategy_dry_run_skips_when_no_predictions(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    cfg = {"score_model_version": "flip_wide_shadow_v1", "horizon": 20}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    class FakeRunQS(_FakeQS):
        pass

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            return FakeRunQS([])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(
        strategy_runner,
        "_query_verified_predictions",
        lambda mv, date, h: [],
    )
    monkeypatch.setattr(strategy_runner, "_query_flags", lambda date, h: {})
    result = strategy_runner.run_strategy(
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        config=cfg,
        dry_run=True,
    )
    assert result["dry_run"] is True
    assert result["plan"]["skipped"] is True
    assert "no VERIFIED predictions" in result["plan"]["reason"]
