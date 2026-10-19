# -*- coding: utf-8 -*-
"""Core-satellite and replay artifact mechanics (synthetic data only)."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from app.lib.strategy_engine.core_satellite import (
    DEFAULT_CORE_WEIGHTS,
    combine_sleeves,
    core_weights_for_aum,
    simulate_core,
)
from app.lib.strategy_engine.replay import build_replay_artifact


def price_frame(days: int = 250, codes=None) -> pd.DataFrame:
    codes = codes or list(DEFAULT_CORE_WEIGHTS)
    index = pd.date_range("2020-01-01", periods=days, freq="B")
    data = {}
    base = {"510300.SH": 4.0, "518880.SH": 7.0, "513100.SH": 1.5, "511010.SH": 100.0}
    for code in codes:
        start = base.get(code, 2.0)
        data[code] = [start * (1 + 0.0005 * position) for position in range(days)]
    return pd.DataFrame(data, index=index)


def test_balanced_50k_uses_feasible_three_etf_core():
    weights = core_weights_for_aum(50_000)
    assert weights == {"510300.SH": 0.40, "518880.SH": 0.30, "513100.SH": 0.30}
    with pytest.raises(ValueError, match="at least 100000"):
        core_weights_for_aum(50_000, four_leg=True)


def test_core_buys_lots_and_never_goes_negative():
    result = simulate_core(price_frame(), aum=25_000, cadence=60)
    assert len(result["curve"]) == 250
    assert all(point["nav"] > 0 for point in result["curve"])
    assert result["terminal_nav"] > 0
    assert all(fill["quantity"] % 100 == 0 for fill in result["fills"])
    assert all(fill["side"] in {"BUY", "SELL"} for fill in result["fills"])


def test_core_skips_a_leg_whose_lot_does_not_fit():
    frame = price_frame()
    frame["511010.SH"] = 100.0
    result = simulate_core(
        frame,
        aum=25_000,
        weights={"510300.SH": 0.4, "518880.SH": 0.3, "511010.SH": 0.3},
        cadence=60,
    )
    codes = {fill["code"] for fill in result["fills"]}
    assert "511010.SH" not in codes
    assert any(row["code"] == "511010.SH" for row in result["skipped"])


def test_combine_starts_at_fifty_forty_ten_and_costs_only_moves():
    days = 250
    frame = price_frame(days)
    core = simulate_core(frame, aum=25_000, cadence=60)
    satellite = [
        {
            "date": point["date"],
            "nav": round(20_000 * (1 + 0.0002 * index), 2),
            "cash": 1_000.0,
            "positions_count": 5,
        }
        for index, point in enumerate(core["curve"])
    ]
    combined = combine_sleeves(core["curve"], satellite, cash=5_000, cadence=60)
    first = combined["curve"][0]
    assert first["core_nav"] == pytest.approx(24_960.25, abs=100)
    assert first["satellite_nav"] == pytest.approx(20_000, abs=1)
    assert first["cash_nav"] == pytest.approx(5_000, abs=1)
    assert combined["curve"][-1]["nav"] > 0
    # A flat/inside-band account must not pay a transfer cost.
    flat_core = [
        {"date": f"2020-01-{day:02d}", "nav": 25_000.0} for day in range(1, 11)
    ]
    flat_sat = [{"date": f"2020-01-{day:02d}", "nav": 20_000.0} for day in range(1, 11)]
    flat = combine_sleeves(flat_core, flat_sat, cash=5_000, cadence=3)
    assert all(row["moved_notional"] == 0 for row in flat["transfers"])


def test_combine_fails_when_curves_do_not_overlap():
    with pytest.raises(ValueError, match="same sessions"):
        combine_sleeves(
            [{"date": "2020-01-01", "nav": 25_000}],
            [{"date": "2020-01-02", "nav": 20_000}],
            cash=5_000,
        )


def _curve(days: int = 3) -> list[dict]:
    return [
        {
            "date": f"2020-01-0{position + 1}",
            "nav": 100_000 + position * 100,
            "cash": 10_000,
            "positions_count": 2,
            "benchmark_nav": 100_000 + position * 50,
        }
        for position in range(days)
    ]


def _payload() -> dict:
    return {
        "replay_id": "riq_v1_2020-2026_50000_v1",
        "name": "Balanced 50k replay",
        "strategy": "core_satellite",
        "start_date": "2020-01-01",
        "end_date": "2020-01-03",
        "aum": 50_000,
        "config": {"core": "40/30/30", "satellite": "riq_v1"},
        "config_hash": "abc",
        "source_artifact": "research/core_satellite_backtest.py",
        "curve": _curve(),
        "decisions": [
            {
                "decision_id": "d2020-01-01",
                "date": "2020-01-01",
                "execution_date": "2020-01-02",
                "target_holdings": [{"code": "510300.SH", "weight": 0.4}],
            }
        ],
        "fills": [
            {
                "decision_id": "d2020-01-01",
                "date": "2020-01-02",
                "code": "510300.SH",
                "side": "BUY",
                "quantity": 100,
                "price": 4.0,
                "commission": 5.0,
                "stamp_duty": 0.0,
                "transfer_fee": 0.0,
            }
        ],
    }


def test_replay_artifact_is_replay_only_and_traceable():
    artifact = build_replay_artifact(**_payload())
    assert artifact["evidence_kind"] == "REPLAY"
    assert artifact["forward_sessions"] == 0
    assert artifact["forward_required"] == 120
    assert artifact["forward_eligible"] is False
    assert artifact["data_as_of"] == "2020-01-03"
    assert artifact["input_hash"]
    assert artifact["content_hash"]
    assert artifact["metrics"]["total_fees"] == 5.0
    assert any("上界" in item for item in artifact["limitations"])


def test_replay_rejects_a_fill_without_a_decision():
    payload = _payload()
    payload["fills"][0]["decision_id"] = "missing"
    with pytest.raises(ValueError, match="unknown decision_id"):
        build_replay_artifact(**payload)


def test_replay_rejects_more_than_the_session_bound():
    payload = _payload()
    payload["curve"] = _curve(5_001)
    with pytest.raises(ValueError, match="bounded at 5000"):
        build_replay_artifact(**payload)


def test_replay_requires_a_versioned_slug():
    payload = _payload()
    payload["replay_id"] = "Bad Slug"
    with pytest.raises(ValueError, match="lowercase slug"):
        build_replay_artifact(**payload)


def test_replay_metrics_are_finite():
    artifact = build_replay_artifact(**_payload())
    for value in artifact["metrics"].values():
        if isinstance(value, float):
            assert math.isfinite(value)


def test_cross_sleeve_breach_moves_only_the_drift_and_costs_it():
    days = ["2020-01-01", "2020-01-02", "2020-01-03"]
    core = [
        {"date": days[0], "nav": 50_000, "positions_count": 3},
        {"date": days[1], "nav": 57_000, "positions_count": 3},
        {"date": days[2], "nav": 57_000, "positions_count": 3},
    ]
    sat = [
        {"date": days[0], "nav": 40_000, "positions_count": 10},
        {"date": days[1], "nav": 33_000, "positions_count": 10},
        {"date": days[2], "nav": 33_000, "positions_count": 10},
    ]
    result = combine_sleeves(core, sat, cash=10_000, cadence=1)
    moved = [row for row in result["transfers"] if row["moved_notional"] > 0]
    assert len(moved) == 1
    assert moved[0]["moved_notional"] == pytest.approx(7_000, abs=1)
    assert moved[0]["cost"] == pytest.approx(5.0, abs=0.01)
    assert result["curve"][1]["cash"] == 10_000


def test_core_fails_closed_on_a_missing_price():
    frame = price_frame()
    frame.iloc[10, frame.columns.get_loc("510300.SH")] = float("nan")
    with pytest.raises(ValueError, match="no usable price"):
        simulate_core(frame, aum=25_000, cadence=60)


def test_simulate_core_refuses_the_four_leg_variant_at_50k():
    from app.lib.strategy_engine.core_satellite import FOUR_LEG_WEIGHTS

    with pytest.raises(ValueError, match="at least 100000"):
        simulate_core(price_frame(), aum=50_000, weights=FOUR_LEG_WEIGHTS)


def test_core_decision_cash_is_pre_trade_and_non_negative():
    result = simulate_core(price_frame(), aum=25_000, cadence=60)
    first = result["decisions"][0]
    assert first["cash_before"] == 25_000
    assert first["cash_after"] >= 0
    assert first["cash_after"] == result["curve"][0]["cash"]


def test_annual_turnover_is_not_zero_when_fills_carry_total_cost():
    dates = pd.date_range("2020-01-01", periods=300, freq="B")
    curve = [
        {
            "date": day.date().isoformat(),
            "nav": 100_000.0 + index * 10,
            "cash": 10_000.0,
            "positions_count": 2,
            "benchmark_nav": 100_000.0 + index * 5,
        }
        for index, day in enumerate(dates)
    ]
    fill = {
        "decision_id": "d2020-01-01",
        "date": "2020-01-02",
        "code": "510300.SH",
        "side": "BUY",
        "quantity": 100,
        "price": 4.0,
        "total_cost": 5.0,
        "commission": 5.0,
        "stamp_duty": 0.0,
        "transfer_fee": 0.0,
    }
    artifact = build_replay_artifact(
        replay_id="turnover_test_v1",
        name="turnover",
        strategy="test",
        start_date="2020-01-01",
        end_date="2021-02-26",
        aum=50_000,
        config={"x": 1},
        config_hash="cfg",
        source_artifact="test",
        curve=curve,
        decisions=[
            {
                "decision_id": "d2020-01-01",
                "date": "2020-01-01",
                "execution_date": "2020-01-02",
                "target_holdings": [{"code": "510300.SH", "weight": 0.4}],
            }
        ],
        fills=[fill],
    )
    assert artifact["metrics"]["annual_turnover"] > 0
