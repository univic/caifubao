# -*- coding: utf-8 -*-
"""Tests for the strategy_engine core (config / selection / rebalance / NAV).

All pure-logic tests: no Mongo, no runner layer.
"""

import pytest

from app.lib.strategy_engine.config import (
    DEFAULT_STRATEGY_CONFIG,
    strategy_config_hash,
    validate_strategy_config,
)
from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav
from app.lib.strategy_engine.selection import (
    compute_rebalance,
    select_target_holdings,
)


class _Pred:
    def __init__(self, stock_code, score, percentile=None):
        self.stock_code = stock_code
        self.score = score
        self.percentile = percentile


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


def test_default_config_validates_and_normalizes():
    cfg = validate_strategy_config(DEFAULT_STRATEGY_CONFIG)
    assert cfg["score_model_version"] == "flip_wide_shadow_v1"
    assert cfg["selection"]["mode"] == "top_percentile"
    assert cfg["selection"]["portfolio_size"] == 800


def test_config_hash_order_insensitive():
    a = {
        "score_model_version": "v1",
        "selection": {"mode": "top_n", "portfolio_size": 30},
    }
    b = {
        "selection": {"portfolio_size": 30, "mode": "top_n"},
        "score_model_version": "v1",
    }
    assert strategy_config_hash(a) == strategy_config_hash(b)


def test_config_rejects_unknown_key():
    with pytest.raises(ValueError, match="unknown strategy config keys"):
        validate_strategy_config({"score_model_version": "v1", "bogus": 1})


def test_config_rejects_missing_version():
    with pytest.raises(ValueError, match="score_model_version is required"):
        validate_strategy_config({"selection": {"mode": "top_n", "portfolio_size": 10}})


def test_config_rejects_bad_selection_bounds():
    import copy

    cfg = copy.deepcopy(DEFAULT_STRATEGY_CONFIG)
    cfg["selection"] = {
        "mode": "top_percentile",
        "lower": 0.5,
        "upper": 0.2,
        "portfolio_size": 10,
    }
    with pytest.raises(ValueError, match="lower.*upper"):
        validate_strategy_config(cfg)


def test_config_rejects_zero_size():
    import copy

    cfg = copy.deepcopy(DEFAULT_STRATEGY_CONFIG)
    cfg["selection"]["portfolio_size"] = 0
    with pytest.raises(ValueError, match="portfolio_size"):
        validate_strategy_config(cfg)


def test_config_mutation_does_not_corrupt_module_default():
    # Shallow dict() copies share nested dicts; deep copy must be used by
    # callers, and validation must never mutate the exported default.
    import copy

    snapshot = copy.deepcopy(DEFAULT_STRATEGY_CONFIG)
    cfg = copy.deepcopy(DEFAULT_STRATEGY_CONFIG)
    cfg["selection"]["portfolio_size"] = 0
    with pytest.raises(ValueError, match="portfolio_size"):
        validate_strategy_config(cfg)
    assert DEFAULT_STRATEGY_CONFIG == snapshot
    # And a valid config still validates afterwards (order independence).
    validate_strategy_config(copy.deepcopy(DEFAULT_STRATEGY_CONFIG))


def test_config_rejects_unknown_nested_keys():
    # A typo inside a nested block must fail loudly (mirrors the scoring
    # registry) instead of being absorbed into the default.
    cases = [
        {
            "score_model_version": "v1",
            "selection": {"mode": "top_n", "portfolio_size": 30, "portflio_size": 30},
        },
        {
            "score_model_version": "v1",
            "constraints": {"exclude_bogus": True},
        },
        {
            "score_model_version": "v1",
            "rebalance": {"cadence": 5},
        },
    ]
    for cfg in cases:
        block = next(k for k in ("selection", "constraints", "rebalance") if k in cfg)
        with pytest.raises(ValueError, match=f"unknown {block} keys"):
            validate_strategy_config(cfg)


def test_config_top_n_with_explicit_bounds_rejected():
    cfg = {
        "score_model_version": "v1",
        "selection": {
            "mode": "top_n",
            "lower": 0.2,
            "upper": 1.0,
            "portfolio_size": 30,
        },
    }
    with pytest.raises(ValueError, match="top_n must not carry"):
        validate_strategy_config(cfg)


def test_config_top_n_strips_inherited_default_bounds():
    cfg = validate_strategy_config(
        {
            "score_model_version": "v1",
            "selection": {"mode": "top_n", "portfolio_size": 30},
        }
    )
    assert cfg["selection"]["mode"] == "top_n"
    assert "lower" not in cfg["selection"]
    assert "upper" not in cfg["selection"]


# ---------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------


def test_top_percentile_selects_high_scores_within_band():
    preds = [
        _Pred("sh600001", 90.0, percentile=0.99),
        _Pred("sh600002", 80.0, percentile=0.90),
        _Pred("sh600003", 70.0, percentile=0.50),
        _Pred("sh600004", 60.0, percentile=0.10),  # below lower bound
    ]
    cfg = {
        "score_model_version": "flip_wide_shadow_v1",
        "selection": {
            "mode": "top_percentile",
            "lower": 0.20,
            "upper": 1.0,
            "portfolio_size": 10,
        },
    }
    holdings = select_target_holdings(preds, cfg)
    codes = [h["stock_code"] for h in holdings]
    assert codes == ["sh600001", "sh600002", "sh600003"]
    # equal weight
    assert sum(h["weight"] for h in holdings) == pytest.approx(1.0)


def test_selection_respects_eligibility():
    preds = [
        _Pred("sh600001", 90.0, percentile=0.99),
        _Pred("sh600002", 80.0, percentile=0.98),
    ]
    cfg = {
        "score_model_version": "flip_wide_shadow_v1",
        "selection": {
            "mode": "top_percentile",
            "lower": 0.0,
            "upper": 1.0,
            "portfolio_size": 10,
        },
    }
    holdings = select_target_holdings(preds, cfg, eligible_codes={"sh600002"})
    assert [h["stock_code"] for h in holdings] == ["sh600002"]


def test_selection_top_n_and_cap():
    preds = [
        _Pred(f"sh6000{i:02d}", float(100 - i), percentile=float(1 - i / 100))
        for i in range(1, 11)
    ]
    cfg = {
        "score_model_version": "v1",
        "selection": {"mode": "top_n", "portfolio_size": 5},
    }
    holdings = select_target_holdings(preds, cfg, max_size=3)
    assert len(holdings) == 3
    assert holdings[0]["stock_code"] == "sh600001"


def test_selection_empty_when_nothing_eligible():
    preds = [_Pred("sh600001", 90.0, percentile=0.99)]
    cfg = {
        "score_model_version": "v1",
        "selection": {
            "mode": "top_percentile",
            "lower": 0.2,
            "upper": 1.0,
            "portfolio_size": 10,
        },
    }
    assert select_target_holdings(preds, cfg, eligible_codes=set()) == []


# ---------------------------------------------------------------------------
# rebalance
# ---------------------------------------------------------------------------


def test_rebalance_diff():
    prev = [{"stock_code": "a", "weight": 0.5}, {"stock_code": "b", "weight": 0.5}]
    target = [{"stock_code": "b", "weight": 0.5}, {"stock_code": "c", "weight": 0.5}]
    result = compute_rebalance(prev, target)
    assert result == {"added": ["c"], "removed": ["a"], "unchanged": ["b"]}


def test_rebalance_first_run_all_added():
    target = [{"stock_code": "a", "weight": 1.0}]
    assert compute_rebalance(None, target)["added"] == ["a"]


# ---------------------------------------------------------------------------
# nav
# ---------------------------------------------------------------------------


def _nav_case_prices():
    dates = ["2026-01-02", "2026-01-05"]
    # two stocks both tradable across both dates
    return {
        "a": {dates[0]: QuoteView(10.0, 10.5), dates[1]: QuoteView(10.5, 11.0)},
        "b": {dates[0]: QuoteView(20.0, 21.0), dates[1]: QuoteView(21.0, 20.0)},
    }


def test_paper_nav_basic_mark_and_turnover():
    dates = ["2026-01-02", "2026-01-05"]
    prices = _nav_case_prices()
    schedule = [
        {"date": dates[0], "holdings": {"a": 0.5, "b": 0.5}},
        {"date": dates[1], "holdings": {"a": 1.0}},  # drop b, keep a
    ]
    result = simulate_paper_nav(prices=prices, schedule=schedule, initial_nav=100_000.0)
    curve = result["curve"]
    assert len(curve) == 2
    assert curve[0]["date"] == dates[0]
    assert curve[1]["positions_count"] == 1
    assert result["terminal_nav"] > 0
    # cycle 1: buys only -> turnover > 0; cycle 2: sell b + top-up a.
    assert curve[0]["turnover"] is not None and curve[0]["turnover"] > 0
    assert curve[1]["turnover"] is not None and curve[1]["turnover"] > 0


def test_paper_nav_entry_costs_reduce_nav_on_flat_prices():
    # open == close everywhere: any NAV < initial is pure cost drag.
    date = "2026-01-02"
    prices = {"a": {date: QuoteView(10.0, 10.0)}, "b": {date: QuoteView(20.0, 20.0)}}
    schedule = [{"date": date, "holdings": {"a": 0.5, "b": 0.5}}]
    result = simulate_paper_nav(prices=prices, schedule=schedule, initial_nav=100_000.0)
    nav = result["curve"][0]["nav"]
    assert nav < 100_000.0  # commission + slippage paid at entry


def test_paper_nav_skips_suspended_buy():
    dates = ["2026-01-02"]
    prices = {
        "a": {dates[0]: QuoteView(10.0, 10.0, trade_status=1)},  # flat price
        "b": {dates[0]: QuoteView(20.0, 21.0, trade_status=0)},  # suspended
    }
    schedule = [{"date": dates[0], "holdings": {"a": 0.5, "b": 0.5}}]
    result = simulate_paper_nav(prices=prices, schedule=schedule, initial_nav=100_000.0)
    curve = result["curve"]
    # b suspended -> only a bought; positions_count == 1. Cash that could not
    # be deployed to b stays idle, so NAV < initial (a's entry cost) and the
    # idle half is not marked up.
    assert curve[0]["positions_count"] == 1
    assert 90_000.0 < curve[0]["nav"] < 100_000.0


def test_paper_nav_suspended_held_name_valued_at_last_close():
    # a is held (bought on day 1), then suspended on day 2 with NO quote:
    # it must be valued at day-1 close (last observed mark), not at entry open.
    dates = ["2026-01-02", "2026-01-05"]
    prices = {
        "a": {
            dates[0]: QuoteView(10.0, 12.0, trade_status=1),  # entry open 10
            # day 2 suspended: no quote entry at all -> roll forward
        }
    }
    schedule = [
        {"date": dates[0], "holdings": {"a": 0.9}},
        {"date": dates[1], "holdings": {"a": 0.9}},
    ]
    result = simulate_paper_nav(prices=prices, schedule=schedule, initial_nav=200_000.0)
    curve = result["curve"]
    # Day-1 close 12 vs entry 10: ~18-20% gain on the deployed 90%.
    assert curve[0]["nav"] > 220_000.0
    # Day 2 has no quote: NAV must hold the last observed close (~day-1 mark),
    # not collapse back toward the entry price.
    assert curve[1]["nav"] >= curve[0]["nav"] * 0.999


def test_paper_nav_rejects_empty_schedule():
    import pytest as _pytest

    with _pytest.raises(ValueError, match="at least one"):
        simulate_paper_nav(prices={}, schedule=[])


def test_paper_nav_records_benchmark_when_supplied():
    dates = ["2026-01-02"]
    prices = {"a": {dates[0]: QuoteView(10.0, 10.5)}}
    schedule = [{"date": dates[0], "holdings": {"a": 1.0}}]
    result = simulate_paper_nav(
        prices=prices,
        schedule=schedule,
        benchmark_returns={dates[0]: 0.005},
        initial_nav=100_000.0,
    )
    assert result["curve"][0]["benchmark_return"] == 0.005


def test_paper_nav_drawdown_never_positive():
    prices = {"a": {}}
    schedule = [
        {"date": "2026-01-02", "holdings": {}},
        {"date": "2026-01-05", "holdings": {}},
    ]
    result = simulate_paper_nav(prices=prices, schedule=schedule, initial_nav=100_000.0)
    for point in result["curve"]:
        assert point["drawdown"] <= 0.0


# ---------------------------------------------------------------------------
# runner orchestration
# ---------------------------------------------------------------------------


def test_eligible_codes_excludes_st_bse_suspended():
    from app.lib.strategy_engine.runner import eligible_codes_from_flags

    flags = {
        "sh600000": {"is_st": 0, "is_bse": 0, "trade_status": 1},  # eligible
        "sh600001": {"is_st": 1, "is_bse": 0, "trade_status": 1},  # ST
        "sh600002": {"is_st": 0, "is_bse": 1, "trade_status": 1},  # BSE
        "sh600003": {"is_st": 0, "is_bse": 0, "trade_status": 0},  # suspended
    }
    cfg = {
        "score_model_version": "flip_wide_shadow_v1",
        "selection": {"mode": "top_n", "portfolio_size": 10},
    }
    assert eligible_codes_from_flags(flags, cfg) == {"sh600000"}


def test_eligible_codes_missing_row_is_fail_closed():
    from app.lib.strategy_engine.runner import eligible_codes_from_flags

    cfg = {
        "score_model_version": "flip_wide_shadow_v1",
        "selection": {"mode": "top_n", "portfolio_size": 10},
    }
    # sh600999 has no flag row -> unknown liquidity -> excluded
    assert eligible_codes_from_flags({"sh600000": {"trade_status": 1}}, cfg) == {
        "sh600000"
    }


def test_assemble_daily_plan_skips_when_no_predictions():
    import datetime

    from app.lib.strategy_engine.runner import assemble_daily_plan

    plan = assemble_daily_plan(
        config={"score_model_version": "v1"},
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        predictions=[],
        previous_holdings=None,
    )
    assert plan["skipped"] is True
    assert "no VERIFIED predictions" in plan["reason"]


def test_assemble_daily_plan_builds_target_and_rebalance():
    import datetime

    from app.lib.strategy_engine.runner import assemble_daily_plan

    preds = [
        _Pred("sh600001", 90.0, percentile=0.99),
        _Pred("sh600002", 80.0, percentile=0.98),
    ]
    flags = {
        "sh600001": {"trade_status": 1},
        "sh600002": {"trade_status": 1},
    }
    plan = assemble_daily_plan(
        config={
            "score_model_version": "v1",
            "selection": {"mode": "top_n", "portfolio_size": 10},
        },
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        predictions=preds,
        previous_holdings=[{"stock_code": "sh600001", "weight": 1.0}],
        flags=flags,
    )
    assert plan["skipped"] is False
    codes = {h["stock_code"] for h in plan["target_holdings"]}
    assert codes == {"sh600001", "sh600002"}
    assert plan["rebalance"]["added"] == ["sh600002"]


def test_assemble_daily_plan_skips_when_selection_empty_after_flags():
    import datetime

    from app.lib.strategy_engine.runner import assemble_daily_plan

    preds = [_Pred("sh600001", 90.0, percentile=0.99)]
    # code is ST -> excluded -> no eligible holdings
    plan = assemble_daily_plan(
        config={
            "score_model_version": "v1",
            "selection": {"mode": "top_n", "portfolio_size": 10},
        },
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        predictions=preds,
        previous_holdings=None,
        flags={"sh600001": {"is_st": 1, "trade_status": 1}},
    )
    assert plan["skipped"] is True
    assert plan["reason"] == "selection produced no eligible holdings"
