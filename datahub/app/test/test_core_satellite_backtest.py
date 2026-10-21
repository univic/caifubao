# -*- coding: utf-8 -*-
"""Core-satellite payload assembly with a synthetic satellite curve."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import core_satellite_backtest as backtest  # noqa: E402


def _build_inputs(tmp_path: Path):
    dates = pd.date_range("2020-01-01", periods=130, freq="B")
    wide = pd.DataFrame(
        {
            "510300.SH": [4.0 + 0.001 * i for i in range(len(dates))],
            "518880.SH": [7.0 + 0.002 * i for i in range(len(dates))],
            "513100.SH": [1.5 + 0.0005 * i for i in range(len(dates))],
        },
        index=dates,
    )
    prices = tmp_path / "core.parquet"
    wide.to_parquet(prices)

    nav = [
        {
            "date": day.date().isoformat(),
            "nav": round(20_000.0 * (1 + 0.0003 * index), 2),
            "cash": 1_000.0,
            "positions_count": 5,
        }
        for index, day in enumerate(dates)
    ]
    decisions = []
    fills = []
    for index in range(0, len(dates) - 1, 60):
        decision_id = "d" + dates[index].date().isoformat()
        decisions.append(
            {
                "decision_id": decision_id,
                "date": dates[index].date().isoformat(),
                "execution_date": dates[index + 1].date().isoformat(),
                "target_holdings": [{"code": "sh600000", "weight": 0.5}],
                "added": ["sh600000"],
                "removed": [],
                "cash_before": 1_000.0,
                "cash_after": 500.0,
                "estimated_cost": 5.0,
            }
        )
        fills.append(
            {
                "decision_id": decision_id,
                "date": dates[index + 1].date().isoformat(),
                "code": "sh600000",
                "side": "BUY",
                "quantity": 100,
                "price": 10.0,
                "total_cost": 5.0,
                "commission": 5.0,
                "stamp_duty": 0.0,
                "transfer_fee": 0.0,
            }
        )
    satellite = {
        "aum": 20_000,
        "names": 10,
        "weight_label": "fwd_h20",
        "weight_label_lag_sessions": 21,
        "step_sessions": 60,
        "components": [
            "reversal_10",
            "amihud_20",
            "trend_60",
            "volatility_20",
            "rsi_14",
        ],
        "decision_time_daily": {
            "nav": nav,
            "decisions": decisions,
            "fills": fills,
            "blocked_orders": [],
        },
    }
    satellite_path = tmp_path / "satellite.json"
    satellite_path.write_text(json.dumps(satellite), encoding="utf-8")
    return prices, satellite_path


def test_payload_combines_core_and_satellite(tmp_path, monkeypatch):
    prices, satellite = _build_inputs(tmp_path)
    output = tmp_path / "paidload.json"
    monkeypatch.setattr(
        backtest,
        "fetch_core_prices",
        lambda start, end: pd.read_parquet(prices),
    )
    code = backtest.main(
        [
            "--satellite-json",
            str(satellite),
            "--output",
            str(output),
            "--replay-id",
            "balanced_50k_test_v1",
        ]
    )
    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["aum"] == 50_000
    assert payload["replay_id"] == "balanced_50k_test_v1"
    assert payload["decisions"]
    assert payload["fills"]
    assert payload["curve"][0]["core_nav"] > 0
    assert payload["curve"][0]["satellite_nav"] == 20_000
    assert payload["curve"][0]["cash_nav"] == 5_000
    assert payload["curve"][0]["cash"] == 5_000
    assert payload["curve"][0]["benchmark_nav"] > 0
    assert payload["decisions"]
    assert payload["fills"]
    assert any(row["sleeve"] == "core" for row in payload["decisions"])
    assert any(row["sleeve"] == "satellite" for row in payload["decisions"])
    assert payload["survivorship_gap"] is True
    assert any("QDII" in item for item in payload["limitations"])
    from app.lib.strategy_engine.replay import build_replay_artifact

    artifact = build_replay_artifact(
        replay_id=payload["replay_id"],
        name=payload["name"],
        strategy=payload["strategy"],
        description=payload["description"],
        start_date=payload["start_date"],
        end_date=payload["end_date"],
        aum=payload["aum"],
        config=payload["config"],
        config_hash=payload["config_hash"],
        source_artifact=payload["source_artifact"],
        curve=payload["curve"],
        decisions=payload["decisions"],
        fills=payload["fills"],
        blocked_orders=payload["blocked_orders"],
        limitations=payload["limitations"],
        survivorship_gap=payload["survivorship_gap"],
    )
    assert artifact["metrics"]["average_cash_share"] is not None
    assert artifact["metrics"]["benchmark_return"] is not None
    assert artifact["forward_sessions"] == 0


def _frozen_payload(**overrides):
    payload = {
        "aum": 20_000,
        "names": 10,
        "weight_label": "fwd_h20",
        "weight_label_lag_sessions": 21,
        "step_sessions": 60,
        "components": [
            "reversal_10",
            "amihud_20",
            "trend_60",
            "volatility_20",
            "rsi_14",
        ],
    }
    payload.update(overrides)
    return {"payload": payload, "curve": [], "decisions": [], "fills": []}


def test_frozen_satellite_rejects_a_scaled_aum():
    import pytest

    with pytest.raises(ValueError, match="satellite AUM"):
        backtest.assert_frozen_satellite(_frozen_payload(aum=50_000), 60)


def test_frozen_satellite_rejects_a_different_label():
    import pytest

    with pytest.raises(ValueError, match="weight_label"):
        backtest.assert_frozen_satellite(_frozen_payload(weight_label="fwd_h60"), 60)


def test_frozen_satellite_rejects_a_different_step():
    import pytest

    with pytest.raises(ValueError, match="step"):
        backtest.assert_frozen_satellite(_frozen_payload(step_sessions=20), 60)


def test_transfer_decision_carries_cash_and_nav():
    day = "2020-01-01"
    combined = {
        "transfers": [
            {
                "date": day,
                "moved_notional": 1_000,
                "cost": 5.0,
                "weights_before": {"core": 0.6, "satellite": 0.3, "cash": 0.1},
                "weights_after": {"core": 0.5, "satellite": 0.4, "cash": 0.1},
            }
        ],
        "curve": [
            {"date": day, "nav": 100_000, "cash": 10_000},
            {"date": "2020-01-02", "nav": 100_000, "cash": 10_000},
        ],
    }
    decisions, fills = backtest._transfer_activity(
        combined,
        [day, "2020-01-02"],
        {day: {"date": day, "nav": 100_000, "cash": 10_000}},
    )
    assert decisions[0]["nav_before"] == 100_000
    assert decisions[0]["cash_before"] == 10_000
    assert decisions[0]["cash_after"] == 10_000
    assert fills[0]["side"] == "TRANSFER"
