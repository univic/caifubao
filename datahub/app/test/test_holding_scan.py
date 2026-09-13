# -*- coding: utf-8 -*-
"""Holding-period x buffer scan tests (pure logic, synthetic panel)."""

import datetime

import numpy as np
import pandas as pd
import pytest

from app.lib.strategy_engine.holding_scan import (
    ROUND_TRIP_COST,
    build_scan_input,
    scan_cell,
    scan_grid,
    summary_table,
)


def _sessions(count: int) -> list[datetime.date]:
    day = datetime.date(2026, 1, 5)
    sessions = []
    while len(sessions) < count:
        if day.weekday() < 5:
            sessions.append(day)
        day += datetime.timedelta(days=1)
    return sessions


def _panel(
    horizon: int,
    counts: list[int],
    sequences: dict[str, list[float | None]],
) -> pd.DataFrame:
    """Panel where each code's label sequence is given per session index.

    A ``None`` entry encodes a row whose label the panel blocked (NaN), which
    the scan must drop from both the basket and the benchmark.
    """
    label_column = f"fwd_h{horizon}"
    sessions = _sessions(len(counts))
    records = []
    for index, count in enumerate(counts):
        codes = [f"sz{position + 1:06d}" for position in range(count)]
        for code in codes:
            value = sequences.get(code, [None] * len(counts))[index]
            records.append(
                {
                    "date": sessions[index],
                    "stock_code": code,
                    label_column: np.nan if value is None else float(value),
                }
            )
    return pd.DataFrame.from_records(records)


def _signal(panel: pd.DataFrame, by_index: dict[int, dict[str, float]]) -> pd.Series:
    """Signal per (session index, code); lower value = lower (flip) rank."""
    sessions = sorted(pd.to_datetime(panel["date"]).unique())
    lookup = {
        (pd.Timestamp(sessions[index]), code): value
        for index, mapping in by_index.items()
        for code, value in mapping.items()
    }
    dates = pd.to_datetime(panel["date"])
    return panel.assign(_date=dates).apply(
        lambda row: lookup.get((row["_date"], row["stock_code"]), np.nan), axis=1
    )


def _ascending(count: int, code_position: int) -> dict[int, dict[str, float]]:
    """Signal mapping where lower code position gets a lower rank."""
    return {
        index: {
            f"sz{position + 1:06d}": float(count - position)
            for position in range(count)
        }
        for index in range(100)
    }


def test_rank_is_zero_based_and_flat_for_single_name_session():
    panel = _panel(2, [3, 1], {"sz000001": [0.01, 0.01]})
    signal = _signal(
        panel,
        {
            0: {"sz000001": 3.0, "sz000002": 2.0, "sz000003": 1.0},
            1: {"sz000001": 1.0},
        },
    )
    frame = build_scan_input(panel, signal, 2)
    sessions = sorted(frame["date"].unique())
    first = frame.loc[frame["date"] == sessions[0]].set_index("stock_code")
    assert first.loc["sz000003", "rank"] == pytest.approx(0.0)  # lowest signal
    assert first.loc["sz000002", "rank"] == pytest.approx(0.5)
    assert first.loc["sz000001", "rank"] == pytest.approx(1.0)
    second = frame.loc[frame["date"] == sessions[1]]
    assert second["rank"].tolist() == [pytest.approx(0.0)]


def test_annual_turnover_counts_replacements_over_rebalance_sessions():
    # h=1, one name, rank alternates every session -> every rebalance replaces it.
    counts = [2] * 6
    panel = _panel(2, counts, {"sz000001": [0.10] * 6, "sz000002": [0.10] * 6})
    signal = _signal(
        panel,
        {
            index: (
                {"sz000001": 1.0, "sz000002": 2.0}
                if index % 2 == 0
                else {"sz000001": 2.0, "sz000002": 1.0}
            )
            for index in range(6)
        },
    )
    frame = build_scan_input(panel, signal, 2)
    cell = scan_cell(frame, horizon=1, buffer=1.0, entry_pct=0.5, portfolio_size=1)
    assert cell["rebalances"] == 6
    assert cell["avg_one_way_turnover"] == pytest.approx(1.0)
    assert cell["annual_turnover"] == pytest.approx(252.0, rel=1e-9)


def test_blocked_label_is_excluded_from_basket_and_benchmark():
    counts = [3] * 2
    panel = _panel(
        2,
        counts,
        {
            "sz000001": [None, None],  # entry blocked -> cannot be bought
            "sz000002": [0.04, 0.04],
            "sz000003": [0.02, 0.02],
        },
    )
    signal = _signal(
        panel,
        {
            index: {"sz000001": 1.0, "sz000002": 3.0, "sz000003": 2.0}
            for index in range(2)
        },
    )
    frame = build_scan_input(panel, signal, 2)
    cell = scan_cell(frame, horizon=2, buffer=1.0, entry_pct=1.0, portfolio_size=2)
    # sz000001 ranks best but has no label, so the basket is 000003 + 000002.
    assert cell["entries"] == 2
    assert cell["rebalances"] == 1
    assert cell["basket_returns"][0] == pytest.approx(
        (0.02 + 0.04) / 2 - ROUND_TRIP_COST
    )
    # The benchmark sees only the two resolvable names as well.
    assert cell["excess_returns"][0] == pytest.approx(0.0)


def test_empty_input_returns_an_empty_cell_rather_than_a_number():
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-05"]),
            "stock_code": ["sz000001"],
            "label": [np.nan],
            "rank": [0.0],
        }
    )
    cell = scan_cell(frame, horizon=5, buffer=1.5, entry_pct=0.2, portfolio_size=800)
    assert cell["information_ratio"] is None
    assert cell["rebalances"] == 0
    assert cell["annual_turnover"] is None


def test_single_rebalance_is_measured_but_unscorable():
    panel = _panel(2, [2] * 2, {"sz000001": [0.05, 0.05], "sz000002": [0.01, 0.01]})
    signal = _signal(panel, {0: {"sz000001": 2.0, "sz000002": 1.0}})
    frame = build_scan_input(panel, signal, 2)
    cell = scan_cell(frame, horizon=2, buffer=1.0, entry_pct=0.5, portfolio_size=1)
    assert cell["rebalances"] == 1
    assert cell["entries"] == 1
    assert cell["information_ratio"] is None


def test_invalid_parameters_are_rejected():
    panel = _panel(2, [2], {"sz000001": [0.05], "sz000002": [0.01]})
    signal = _signal(panel, {0: {"sz000001": 2.0, "sz000002": 1.0}})
    frame = build_scan_input(panel, signal, 2)
    with pytest.raises(ValueError):
        scan_cell(frame, horizon=0, buffer=1.0)
    with pytest.raises(ValueError):
        scan_cell(frame, horizon=2, buffer=0.5)
    with pytest.raises(ValueError):
        scan_cell(frame, horizon=2, buffer=1.0, entry_pct=0.0)


def test_grid_skips_missing_horizons_and_sorts_the_summary():
    panel = _panel(2, [2], {"sz000001": [0.05], "sz000002": [0.01]})
    signal = _signal(panel, {0: {"sz000001": 2.0, "sz000002": 1.0}})
    frames = {2: build_scan_input(panel, signal, 2)}
    rows = scan_grid(frames, horizons=[2, 40], buffers=[2.0, 1.0])
    table = summary_table(rows)
    assert table["horizon"].tolist() == [2, 2]
    assert table["buffer"].tolist() == [1.0, 2.0]


def test_round_trip_cost_matches_the_factor_lab_convention():
    assert ROUND_TRIP_COST == pytest.approx(0.0035)
