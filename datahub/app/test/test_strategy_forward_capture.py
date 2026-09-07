# -*- coding: utf-8 -*-
"""NEXT.1 forward evidence capture — failing tests first.

Covers: pure FORWARD/REPLAY classification, model defaults (evidence_kind
default REPLAY; StrategyForwardWindow ACTIVE uniqueness per
(model_version, horizon)), runner immutability (replace on a FORWARD COMPLETED
plan fails closed), and continuity (previous-holdings and run_nav queries span
REPLAY + FORWARD).
"""

import datetime

import pytest


def _dt(iso):
    return datetime.datetime.fromisoformat(iso)


def _utc(iso):
    return datetime.datetime.fromisoformat(iso).replace(tzinfo=datetime.UTC)


class _Window:
    def __init__(self, start_date, config_hash="hashA"):
        self.start_date = _dt(start_date)
        self.config_hash = config_hash
        self.status = "ACTIVE"


# ---------------------------------------------------------------------------
# 1) Pure classifier (runner.classify_evidence_kind)
# ---------------------------------------------------------------------------


def _classify(**kw):
    from app.lib.strategy_engine.runner import classify_evidence_kind

    return classify_evidence_kind(**kw)


def test_on_time_same_session_run_is_forward():
    # signal 2026-04-10 (Fri); execution 2026-04-13; decision Fri evening UTC
    # (07:00Z = 15:00 CST close) is before Mon 09:30 CST (Mon 01:30Z).
    kind = _classify(
        date=_dt("2026-04-10"),
        decision_at=_utc("2026-04-10T07:00:00"),
        execution_date=_dt("2026-04-13"),
        window=_Window("2026-04-10"),
        config_hash="hashA",
        existing_status=None,
        replace=False,
    )
    assert kind == "FORWARD"


def test_late_run_after_execution_open_is_replay():
    # Decision after the execution session (Mon) has opened.
    kind = _classify(
        date=_dt("2026-04-10"),
        decision_at=_utc("2026-04-13T02:00:00"),  # Mon 10:00 CST
        execution_date=_dt("2026-04-13"),
        window=_Window("2026-04-10"),
        config_hash="hashA",
        existing_status=None,
        replace=False,
    )
    assert kind == "REPLAY"


def test_backdated_date_before_window_start_is_replay():
    kind = _classify(
        date=_dt("2026-04-03"),
        decision_at=_utc("2026-04-10T07:00:00"),
        execution_date=_dt("2026-04-07"),
        window=_Window("2026-04-10"),
        config_hash="hashA",
        existing_status=None,
        replace=False,
    )
    assert kind == "REPLAY"


def test_no_window_is_replay():
    kind = _classify(
        date=_dt("2026-04-10"),
        decision_at=_utc("2026-04-10T07:00:00"),
        execution_date=_dt("2026-04-13"),
        window=None,
        config_hash="hashA",
        existing_status=None,
        replace=False,
    )
    assert kind == "REPLAY"


def test_window_config_mismatch_is_replay():
    kind = _classify(
        date=_dt("2026-04-10"),
        decision_at=_utc("2026-04-10T07:00:00"),
        execution_date=_dt("2026-04-13"),
        window=_Window("2026-04-10", config_hash="hashA"),
        config_hash="hashB",  # different strategy/score config
        existing_status=None,
        replace=False,
    )
    assert kind == "REPLAY"


def test_replace_of_completed_plan_is_replay():
    kind = _classify(
        date=_dt("2026-04-10"),
        decision_at=_utc("2026-04-10T07:00:00"),
        execution_date=_dt("2026-04-13"),
        window=_Window("2026-04-10"),
        config_hash="hashA",
        existing_status="COMPLETED",
        replace=True,
    )
    assert kind == "REPLAY"


def test_skipped_rewrite_upgrades_to_forward():
    kind = _classify(
        date=_dt("2026-04-10"),
        decision_at=_utc("2026-04-10T07:00:00"),
        execution_date=_dt("2026-04-13"),
        window=_Window("2026-04-10"),
        config_hash="hashA",
        existing_status="SKIPPED",
        replace=False,
    )
    assert kind == "FORWARD"


# ---------------------------------------------------------------------------
# 2) Model defaults
# ---------------------------------------------------------------------------


def test_evidence_kind_defaults_to_replay():
    from app.model.strategy import StrategyPaperRun

    run = StrategyPaperRun(
        strategy_name="flip_wide_paper",
        date=_dt("2026-04-10"),
        model_version="flip_wide_shadow_v1",
        horizon=20,
        config_hash="hashA",
    )
    assert run.evidence_kind == "REPLAY"


def test_forward_window_document_exists_with_expected_fields():
    from app.model.strategy import StrategyForwardWindow

    w = StrategyForwardWindow(
        model_version="flip_wide_shadow_v1",
        horizon=20,
        config_hash="hashA",
        start_date=_dt("2026-04-10"),
        status="ACTIVE",
    )
    assert w.status == "ACTIVE"
    assert w.config_hash == "hashA"


# ---------------------------------------------------------------------------
# 3) Runner-level: immutability + continuity (monkeypatched model)
# ---------------------------------------------------------------------------


class _Run:
    def __init__(self, **kw):
        self.status = kw.get("status", "COMPLETED")
        self.evidence_kind = kw.get("evidence_kind", "REPLAY")
        self.target_holdings = kw.get("target_holdings", [])
        self.date = kw.get("date")
        for k, v in kw.items():
            setattr(self, k, v)


def test_run_rejects_replace_of_forward_completed(monkeypatch):
    """replace on a FORWARD COMPLETED plan must fail closed."""
    import app.jobs.strategy_runner as job
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    existing = _Run(
        status="COMPLETED",
        evidence_kind="FORWARD",
        date=_dt("2026-04-10"),
        model_version="flip_wide_shadow_v1",
        horizon=20,
        config_hash="hashA",
    )

    class FakeQS:
        def __init__(self, items):
            self.items = items

        def first(self):
            return self.items[0] if self.items else None

        def order_by(self, *fields):
            return self

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            return FakeQS([existing])

    class FakeRegistered:
        def __init__(self, config):
            self.config = config

    class FakeRegistry:
        @classmethod
        def objects(cls, **query):
            return FakeQS([FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    class FakeWindowModel:
        @classmethod
        def objects(cls, **query):
            return FakeQS([])

    monkeypatch.setattr(job, "_execution_date", lambda d: _dt("2026-04-13"))
    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegistry)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", FakeWindowModel)
    with pytest.raises(ValueError, match="FORWARD"):
        job.run_strategy(
            date=_dt("2026-04-10"),
            config={
                "score_model_version": "flip_wide_shadow_v1",
                "horizon": 20,
            },
            replace=True,
        )


# ---------------------------------------------------------------------------
# 4) Window lifecycle + counter (service level, in-memory model fakes)
# ---------------------------------------------------------------------------


def _window_store():
    """Small in-memory model store for windows + runs with mongoengine-like
    query operators (eq / __in / __gte / __lte / __lt)."""
    records = {"windows": [], "runs": []}

    class QS(list):
        def first(self):
            return self[0] if self else None

        def order_by(self, field, *rest):
            key = field.lstrip("-")
            return QS(
                sorted(
                    self,
                    key=lambda r: getattr(r, key).date().isoformat(),
                    reverse=field.startswith("-"),
                )
            )

    def _matches(row, query):
        for key, value in query.items():
            name, _, op = key.partition("__")
            current = getattr(row, name, None)
            if op == "in" and current not in value:
                return False
            if op == "gte" and not current >= value:
                return False
            if op == "lte" and not current <= value:
                return False
            if op == "lt" and not current < value:
                return False
            if not op and current != value:
                return False
        return True

    class Win:
        def __init__(self, **kw):
            self.__dict__.update(kw)

        def save(self):
            if self not in records["windows"]:
                records["windows"].append(self)

        @staticmethod
        def objects(**query):
            return QS(w for w in records["windows"] if _matches(w, query))

    class Run:
        def __init__(self, **kw):
            self.__dict__.update(kw)

        def save(self):
            if self not in records["runs"]:
                records["runs"].append(self)

        def delete(self):
            if self in records["runs"]:
                records["runs"].remove(self)

        @staticmethod
        def objects(**query):
            return QS(r for r in records["runs"] if _matches(r, query))

    return records, Win, Run


def test_certify_closes_active_predecessor_and_restart_resets(monkeypatch):
    import app.jobs.strategy_runner as job
    import app.model.strategy as model_strategy

    records, Win, Run = _window_store()
    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", Win)
    monkeypatch.setattr(
        job, "_resolve_model_version", lambda cfg: cfg["score_model_version"]
    )
    monkeypatch.setattr(job, "_resolve_start_date", lambda: _dt("2026-09-04"))

    # first certification (config hash A)
    monkeypatch.setattr(
        job,
        "strategy_config_hash",
        lambda cfg: "hashA",
    )
    first = job.certify_forward_window(
        model_version="flip_wide_shadow_v1",
        horizon=20,
        config={"score_model_version": "flip_wide_shadow_v1"},
    )
    assert first["status"] == "ACTIVE"
    assert first["start_date"] == _dt("2026-09-04")

    # config change -> certify again with hash B: predecessor closes
    monkeypatch.setattr(
        job,
        "strategy_config_hash",
        lambda cfg: "hashB",
    )
    second = job.certify_forward_window(
        model_version="flip_wide_shadow_v1",
        horizon=20,
        config={"score_model_version": "flip_wide_shadow_v1"},
    )
    assert second["closed_predecessors"] == ["hashA"]
    actives = [w for w in records["windows"] if w.status == "ACTIVE"]
    assert len(actives) == 1
    assert actives[0].config_hash == "hashB"


def test_forward_progress_counts_only_forward_completed_in_window(monkeypatch):
    import app.jobs.strategy_runner as job
    import app.model.strategy as model_strategy

    records, Win, Run = _window_store()
    records["windows"].append(
        Win(
            model_version="flip_wide_shadow_v1",
            horizon=20,
            config_hash="hashA",
            start_date=_dt("2026-09-04"),
            status="ACTIVE",
        )
    )
    records["runs"] = [
        # FORWARD COMPLETED inside window: count
        Run(
            strategy_name="flip_wide_paper",
            model_version="flip_wide_shadow_v1",
            horizon=20,
            evidence_kind="FORWARD",
            status="COMPLETED",
            date=_dt("2026-09-04"),
        ),
        Run(
            strategy_name="flip_wide_paper",
            model_version="flip_wide_shadow_v1",
            horizon=20,
            evidence_kind="FORWARD",
            status="COMPLETED",
            date=_dt("2026-09-07"),
        ),
        # REPLAY and SKIPPED never count
        Run(
            strategy_name="flip_wide_paper",
            model_version="flip_wide_shadow_v1",
            horizon=20,
            evidence_kind="REPLAY",
            status="COMPLETED",
            date=_dt("2026-09-08"),
        ),
        Run(
            strategy_name="flip_wide_paper",
            model_version="flip_wide_shadow_v1",
            horizon=20,
            evidence_kind="FORWARD",
            status="SKIPPED",
            date=_dt("2026-09-09"),
        ),
        # before window start never counts
        Run(
            strategy_name="flip_wide_paper",
            model_version="flip_wide_shadow_v1",
            horizon=20,
            evidence_kind="FORWARD",
            status="COMPLETED",
            date=_dt("2026-09-01"),
        ),
    ]
    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", Win)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", Run)
    from app.lib.utilities import trading_day_helper

    monkeypatch.setattr(
        trading_day_helper,
        "get_a_stock_market_trade_calendar",
        lambda: [
            _dt("2026-09-04"),
            _dt("2026-09-07"),
            _dt("2026-09-08"),
            _dt("2026-09-09"),
        ],
    )

    progress = job.forward_progress(model_version="flip_wide_shadow_v1", horizon=20)
    assert progress["certified"] is True
    assert progress["count"] == 2
    assert progress["first_date"] == "2026-09-04"
    assert progress["last_date"] == "2026-09-07"


# ---------------------------------------------------------------------------
# 5) Missing spec-scenario regressions (spec-guardian P2) + index intent
# ---------------------------------------------------------------------------


def test_forward_window_active_unique_index_is_on_model_horizon_pair():
    """R3 + P3: the ACTIVE partial-unique index must be on the PAIR
    (model_version, horizon), never the config triple."""
    from app.model.strategy import StrategyForwardWindow

    indexes = StrategyForwardWindow._meta["indexes"]
    partial = [
        i for i in indexes if isinstance(i, dict) and i.get("partialFilterExpression")
    ]
    assert len(partial) == 1
    idx = partial[0]
    assert idx["unique"] is True
    assert idx["fields"] == ["model_version", "horizon"]
    assert idx["partialFilterExpression"] == {"status": "ACTIVE"}


def _forward_runner_store(monkeypatch):
    """run_strategy/run_nav harness: window ACTIVE + FORWARD-capable fakes."""
    import app.jobs.strategy_runner as job
    import app.model.strategy as model_strategy
    from types import SimpleNamespace

    from app.lib.strategy_engine.config import (
        strategy_config_hash,
        validate_strategy_config,
    )

    records, Win, Run = _window_store()
    cfg = {
        "score_model_version": "flip_wide_shadow_v1",
        "horizon": 20,
        "initial_nav": 100000,
    }
    cfg_hash = strategy_config_hash(validate_strategy_config(cfg))

    def add_forward_run(date, holdings, execution=None):
        run = Run(
            strategy_name="flip_wide_paper",
            date=date,
            model_version="flip_wide_shadow_v1",
            horizon=20,
            config_hash=cfg_hash,
            config=validate_strategy_config(cfg),
            decision_at=_utc("2026-09-04T07:00:00"),
            execution_date=execution or _dt("2026-09-07"),
            evidence_kind="FORWARD",
            status="COMPLETED",
            target_holdings=holdings,
            rebalance={},
        )
        run.save()
        return run

    records["windows"].append(
        Win(
            model_version="flip_wide_shadow_v1",
            horizon=20,
            config_hash=cfg_hash,
            start_date=_dt("2026-09-04"),
            status="ACTIVE",
        )
    )

    monkeypatch.setattr(model_strategy, "StrategyPaperRun", Run)
    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", Win)
    monkeypatch.setattr(
        job, "_resolve_model_version", lambda c: c["score_model_version"]
    )
    monkeypatch.setattr(
        job,
        "_execution_date",
        lambda date: _dt("2099-01-05"),  # far future -> decision_at < open
    )
    monkeypatch.setattr(
        job,
        "_query_usable_predictions",
        lambda *_, **__: [
            SimpleNamespace(stock_code=c, score=50.0 - i, percentile=1.0 - i / 10)
            for i, c in enumerate(["a", "b", "c"])
        ],
    )
    monkeypatch.setattr(
        job,
        "_query_flags",
        lambda *_, **__: {
            "a": {"trade_status": 1},
            "b": {"trade_status": 1},
            "c": {"trade_status": 1},
        },
    )
    return records, Win, Run, cfg, cfg_hash, add_forward_run


def test_second_forward_day_rebalances_against_first(monkeypatch):
    """R2 S1: previous holdings MUST come from the prior FORWARD run."""
    import app.jobs.strategy_runner as job

    records, Win, Run, cfg, cfg_hash, add_forward_run = _forward_runner_store(
        monkeypatch
    )
    add_forward_run(
        _dt("2026-09-04"),
        [{"stock_code": "a", "weight": 0.5}, {"stock_code": "b", "weight": 0.5}],
    )

    result = job.run_strategy(date=_dt("2026-09-07"), config=cfg)

    assert result["status"] == "COMPLETED"
    assert result["evidence_kind"] == "FORWARD"
    # Continuity found the FORWARD day-1 holdings -> only c is added. If the
    # query silently dropped FORWARD rows, a/b/c would all be "added".
    assert result["rebalance"]["added"] == ["c"]
    assert sorted(result["rebalance"]["unchanged"]) == ["a", "b"]


def test_nav_recompute_includes_forward_and_keeps_plan_immutable(monkeypatch):
    """R1 S5 + R2 S2: run_nav over a FORWARD run writes only nav_snapshot."""
    from app.lib.strategy_engine.nav import QuoteView

    import app.jobs.strategy_runner as job

    records, Win, Run, cfg, cfg_hash, add_forward_run = _forward_runner_store(
        monkeypatch
    )
    run = add_forward_run(
        _dt("2026-09-04"),
        [{"stock_code": "a", "weight": 1.0}],
        execution=_dt("2026-09-07"),
    )
    plan_before = {
        "target_holdings": list(run.target_holdings),
        "evidence_kind": run.evidence_kind,
        "status": run.status,
        "config_hash": run.config_hash,
        "execution_date": run.execution_date,
    }
    monkeypatch.setattr(
        job,
        "_load_quotes_for_codes",
        lambda codes, start, end: {"a": {"2026-09-07": QuoteView(10, 11)}},
    )
    monkeypatch.setattr(job, "_benchmark_returns_for_dates", lambda *_, **__: {})

    result = job.run_nav(
        from_date=_dt("2026-09-04"), to_date=_dt("2026-09-04"), config=cfg
    )

    assert result["found_runs"] == 1  # FORWARD run included in the curve
    assert run.nav_snapshot, "nav_snapshot must be written for the FORWARD run"
    assert run.target_holdings == plan_before["target_holdings"]
    assert run.evidence_kind == plan_before["evidence_kind"] == "FORWARD"
    assert run.status == plan_before["status"] == "COMPLETED"
    assert run.config_hash == plan_before["config_hash"] == cfg_hash
    assert run.execution_date == plan_before["execution_date"]


def test_same_config_restart_resets_counter(monkeypatch):
    """R3 S2: close + re-certify under the same key restarts the counter."""
    import app.jobs.strategy_runner as job

    records, Win, Run, cfg, cfg_hash, add_forward_run = _forward_runner_store(
        monkeypatch
    )
    add_forward_run(_dt("2026-09-04"), [{"stock_code": "a", "weight": 1.0}])

    from app.lib.utilities import trading_day_helper

    monkeypatch.setattr(
        trading_day_helper,
        "get_a_stock_market_trade_calendar",
        lambda: [_dt("2026-09-04"), _dt("2026-09-07")],
    )
    before = job.forward_progress(model_version="flip_wide_shadow_v1", horizon=20)
    assert before["count"] == 1

    job.close_forward_window(model_version="flip_wide_shadow_v1", horizon=20)
    # same config/key restart with a fresh start date
    monkeypatch.setattr(job, "_resolve_start_date", lambda: _dt("2026-09-07"))
    job.certify_forward_window(
        model_version="flip_wide_shadow_v1", horizon=20, config=cfg
    )

    after = job.forward_progress(model_version="flip_wide_shadow_v1", horizon=20)
    assert after["start_date"] == "2026-09-07"
    assert after["count"] == 0  # old-window FORWARD dates no longer count
