# -*- coding: utf-8 -*-
"""The v2 declaration reaches the paper runner, not only the planner library.

Before this wiring, `plan_from_state` had no production caller: a v2 spec was
projected onto the legacy config and then executed by the legacy path, so its
declared stop-loss and maximum holding period were never read. These tests pin
the two halves that make it real — the state is built from what the previous run
recorded, and what this run plans is recorded for the next one — plus the fact
that a v1/legacy run is untouched.
"""

import datetime
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

EXIT = {
    "rank_below": 40,
    "stop_loss_pct": 0.15,
    "max_hold_sessions": 40,
    "on_ineligible": "sell_next_open",
}

KILL = {
    "min_excess_per_period": 0.0,
    "max_drawdown": 0.2,
    "review_after_sessions": 60,
}


def _document(version="2"):
    spec = {
        "spec_version": version,
        "universe": {"market": "CN_A", "as_of": "PIT"},
        "signal": {"score_model_version": "flip_wide_shadow_v1", "horizon": 20},
        "selection": {"mode": "top_n", "portfolio_size": 10},
        "sizing": {"weighting": "equal", "cash_reserve_pct": 0},
        "constraints": {},
        "rebalance": {"cadence_days": 5},
        "risk": {},
        "execution": {},
        "evidence": {"timing_version": "paper_causal_v1", "policy": "research"},
        "initial_nav": 100000,
    }
    if version == "2":
        spec["family"] = "rank_selection"
        spec["selection"] = dict(spec["selection"], exit=dict(EXIT))
        spec["accounting"] = {
            "corporate_actions": "price_only",
            "mark_price": "close_raw",
        }
        spec["benchmark"] = {"kind": "equal_weight_universe"}
        spec["evidence"] = dict(spec["evidence"], kill_criteria=dict(KILL))
    return spec


class _QuerySet:
    def __init__(self, items):
        self.items = list(items)

    def first(self):
        return self.items[0] if self.items else None

    def order_by(self, *fields):
        return self

    def __iter__(self):
        return iter(self.items)


class RunStore:
    """A tiny stand-in for StrategyPaperRun that really keeps what was saved."""

    def __init__(self):
        self.runs = []

    def model(self):
        store = self

        class FakeRun:
            def __init__(self, **fields):
                self.__dict__.update(fields)
                self.completed_at = None

            def save(self):
                store.runs = [r for r in store.runs if r.date != self.date]
                store.runs.append(self)

            def delete(self):
                store.runs = [r for r in store.runs if r is not self]

            @classmethod
            def objects(cls, **query):
                return _QuerySet(store._select(query))

        return FakeRun

    def _select(self, query):
        rows = list(self.runs)
        for key, value in query.items():
            if key == "date__lt":
                rows = [row for row in rows if row.date < value]
            elif key == "evidence_kind__in":
                rows = [row for row in rows if row.evidence_kind in value]
            else:
                rows = [row for row in rows if getattr(row, key, None) == value]
        return sorted(rows, key=lambda row: row.date, reverse=True)


@pytest.fixture(autouse=True)
def runner_env(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    store = RunStore()

    class Registered:
        def __init__(self, config):
            self.config = config

    class FakeRegistry:
        @classmethod
        def objects(cls, **query):
            return _QuerySet([Registered({"20": {"directions": {"momentum": -1}}})])

    class NoWindows:
        @classmethod
        def objects(cls, **query):
            return _QuerySet([])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegistry)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", store.model())
    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", NoWindows)
    # Execution dates come from the exchange calendar in production; the wiring
    # under test only needs a strictly later session to exist.
    monkeypatch.setattr(
        strategy_runner,
        "_execution_date",
        lambda date: date + datetime.timedelta(days=1),
    )
    return store


def _pred(code, score):
    from types import SimpleNamespace

    return SimpleNamespace(stock_code=code, score=score, percentile=0.9)


def _patch_market(monkeypatch, predictions, flags):
    import app.jobs.strategy_runner as strategy_runner

    monkeypatch.setattr(
        strategy_runner,
        "_query_usable_predictions",
        lambda model_version, date, horizon: list(predictions),
    )
    monkeypatch.setattr(
        strategy_runner,
        "_query_flags",
        lambda date, horizon, model_version=None: dict(flags),
    )


def _prior_run(document, date, holdings, held_meta, reference_prices):
    """A COMPLETED run of the same declaration, as continuity would find it."""
    from app.lib.strategy_engine.spec import spec_hash, validate_strategy_spec

    spec_hash_value = spec_hash(validate_strategy_spec(document))
    return type(
        "PriorRun",
        (),
        {
            "date": date,
            "status": "COMPLETED",
            "evidence_kind": "REPLAY",
            "strategy_name": "flip_wide_paper",
            "model_version": "flip_wide_shadow_v1",
            "horizon": 20,
            "config_hash": spec_hash_value,
            "target_holdings": holdings,
            "hold_meta": held_meta,
            "reference_prices": reference_prices,
        },
    )()


def _run(store, monkeypatch, *, document=None, date="2026-04-07", predictions, flags):
    import app.jobs.strategy_runner as strategy_runner
    from app.lib.strategy_engine.spec import (
        spec_hash,
        spec_to_legacy_config,
        validate_strategy_spec,
    )

    _patch_market(monkeypatch, predictions, flags)
    kwargs = {"date": datetime.datetime.fromisoformat(date)}
    if document is None:
        kwargs["config"] = {
            "score_model_version": "flip_wide_shadow_v1",
            "horizon": 20,
            "selection": {"mode": "top_n", "portfolio_size": 10},
        }
    else:
        validated = validate_strategy_spec(document)
        kwargs["config"] = spec_to_legacy_config(validated)
        kwargs["strategy_spec"] = validated
        kwargs["strategy_spec_hash"] = spec_hash(validated)
    return strategy_runner.run_strategy(**kwargs)


def test_a_v2_run_plans_from_the_recorded_state(runner_env, monkeypatch):
    """The declared stop-loss actually fires through the runner."""
    runner_env.runs.append(
        _prior_run(
            _document(),
            datetime.datetime(2026, 4, 3),
            [{"stock_code": "sh600001", "weight": 0.5}],
            {"sh600001": {"hold_count": 3, "entry_reason": "added_by_rank"}},
            {"sh600001": 10.0},
        )
    )
    result = _run(
        runner_env,
        monkeypatch,
        document=_document(),
        predictions=[_pred("sh600001", 1.0), _pred("sh600002", 99.0)],
        flags={
            # 40% below the recorded cost basis: past the declared 15% stop.
            "sh600001": {"trade_status": 1, "reference_close": 6.0},
            "sh600002": {"trade_status": 1, "reference_close": 20.0},
        },
    )

    assert result["status"] == "COMPLETED"
    run = runner_env.runs[-1]
    assert "sh600001" not in [item["stock_code"] for item in run.target_holdings]
    assert run.exit_reasons["sh600001"] == "sold_stop_loss"
    # The state is recorded, and its basis is the price recorded last session.
    assert run.decision_state["positions"][0]["average_cost"] == pytest.approx(10.0)
    # And this run records its own basis for the next one.
    assert run.reference_prices == {"sh600002": 20.0}


def test_the_next_run_measures_against_the_recorded_basis(runner_env, monkeypatch):
    """Two consecutive v2 sessions: the basis comes from the record, not the close."""
    first = _run(
        runner_env,
        monkeypatch,
        document=_document(),
        date="2026-04-07",
        predictions=[_pred("sh600002", 99.0)],
        flags={"sh600002": {"trade_status": 1, "reference_close": 10.0}},
    )
    assert first["status"] == "COMPLETED"
    assert runner_env.runs[-1].reference_prices == {"sh600002": 10.0}

    result = _run(
        runner_env,
        monkeypatch,
        document=_document(),
        date="2026-04-08",
        predictions=[_pred("sh600002", 99.0), _pred("sh600003", 1.0)],
        # 8.0 is only 20% down from the recorded 10.0 basis; measured against
        # today's own close it would be no loss at all.
        flags={
            "sh600002": {"trade_status": 1, "reference_close": 8.0},
            "sh600003": {"trade_status": 1, "reference_close": 5.0},
        },
    )
    assert result["status"] == "COMPLETED"
    run = runner_env.runs[-1]
    assert run.decision_state["positions"][0]["average_cost"] == pytest.approx(10.0)
    assert run.exit_reasons["sh600002"] == "sold_stop_loss"


def test_a_v1_run_keeps_the_legacy_path(runner_env, monkeypatch):
    """A v1 declaration records no state, so its planning is unchanged."""
    result = _run(
        runner_env,
        monkeypatch,
        document=_document(version="1"),
        predictions=[_pred("sh600002", 99.0)],
        flags={"sh600002": {"trade_status": 1, "reference_close": 10.0}},
    )
    assert result["status"] == "COMPLETED"
    run = runner_env.runs[-1]
    assert run.decision_state == {}
    assert [item["stock_code"] for item in run.target_holdings] == ["sh600002"]


def test_a_target_without_a_close_is_refused_under_an_exit_contract(
    runner_env, monkeypatch
):
    """A basis the next stop could not use is not recorded as if it were one."""
    with pytest.raises(ValueError, match="no decision-date close for target"):
        _run(
            runner_env,
            monkeypatch,
            document=_document(),
            predictions=[_pred("sh600002", 99.0)],
            flags={"sh600002": {"trade_status": 1}},
        )


def test_a_mismatched_declaration_is_refused(runner_env, monkeypatch):
    """The recorded identity must be the one the config actually implements."""
    import app.jobs.strategy_runner as strategy_runner
    from app.lib.strategy_engine.spec import spec_hash, validate_strategy_spec

    _patch_market(
        monkeypatch,
        [_pred("sh600002", 99.0)],
        {"sh600002": {"trade_status": 1, "reference_close": 10.0}},
    )
    validated = validate_strategy_spec(_document())
    with pytest.raises(ValueError, match="does not match the supplied declaration"):
        strategy_runner.run_strategy(
            date=datetime.datetime(2026, 4, 7),
            config={
                "score_model_version": "flip_wide_shadow_v1",
                "horizon": 20,
                "selection": {"mode": "top_n", "portfolio_size": 20},
            },
            strategy_spec=validated,
            strategy_spec_hash=spec_hash(validated),
        )


def test_the_cli_loader_returns_the_declaration_alongside_the_projection():
    """`run --spec` must hand the runner the declaration, not only its projection.

    The projection deliberately drops `selection.exit` (the legacy config has no
    field for it), so a loader that returned only the projection would leave the
    runner unable to see the exit contract it is supposed to consume.
    """
    import json
    from types import SimpleNamespace

    import app.jobs.strategy_runner as strategy_runner

    config, identity, policy, document = strategy_runner._load_cli_spec(
        SimpleNamespace(spec_json=json.dumps(_document()), model_version=None)
    )
    assert "exit" not in config["selection"]
    assert document["selection"]["exit"]["stop_loss_pct"] == 0.15
    assert identity is not None
    assert policy is None
