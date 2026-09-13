# -*- coding: utf-8 -*-
"""Score-reconstruction tests (formulas mirrored from the scoring engine)."""

import pandas as pd
import pytest

from app.lib.small_book import (
    UNAVAILABLE,
    WEIGHTS,
    build_features,
    clamp,
    components_at,
    cross_sectional_rank,
    flip_score,
)


def _quotes(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "code",
            "date",
            "close_hfq",
            "high_hfq",
            "low_hfq",
            "trade_amount",
            "trade_status",
            "is_st",
        ],
    )


def _factors(rows):
    return pd.DataFrame(
        rows, columns=["stock_code", "date", "ma_20", "ma_60", "ma_120"]
    )


def test_clamp_bounds_and_nan():
    assert clamp(0.5) == 0.5
    assert clamp(-1.0) == 0.0
    assert clamp(2.0) == 1.0
    assert clamp(float("nan")) == 0.0


def test_components_match_engine_formulas():
    # 25 sessions of a rising series: close 11..35, MA fixed below the close so
    # the trend checks are all true and the range position is 1.0 (new high).
    days = pd.date_range("2024-01-01", periods=25)
    rows = []
    factor_rows = []
    for index, day in enumerate(days):
        close = 10.0 + index
        rows.append(["sh600000", day, close, close, close - 2.0, 1e7, 1, 0])
        factor_rows.append(["sh600000", day, 5.0, 4.0, 3.0])
    features = build_features(_quotes(rows), _factors(factor_rows))
    out = components_at(features)
    last = out.iloc[-1]
    # trend: close > ma20, close > ma60, ma20 > ma60 -> 3/3
    assert last["trend_alignment"] == pytest.approx(1.0)
    # momentum over the configured 10-session lookback: (close[t]/close[t-10]-1)/0.1
    expected_momentum = (last["close"] / out.iloc[-11]["close"] - 1.0) / 0.1
    assert last["momentum"] == pytest.approx(min(1.0, expected_momentum))
    # relative strength: (change + 0.05) / 0.15 clamped
    assert last["relative_strength"] == pytest.approx(1.0)
    # new high -> range position pinned to 1.0
    assert last["breakout_or_position"] == pytest.approx(1.0)
    # risk penalty is raw (not clamped) and positive for a volatile series
    assert last["risk_penalty"] > 0


def test_flat_series_has_no_momentum_and_mid_range_position():
    days = pd.date_range("2024-01-01", periods=25)
    # Same close every session but a slightly wider intraday range on the last
    # day, so the last close sits strictly inside the rolling high/low.
    rows = [["sh600000", day, 10.0, 10.2, 9.8, 1e7, 1, 0] for day in days[:-1]]
    rows.append(["sh600000", days[-1], 10.0, 10.5, 9.0, 1e7, 1, 0])
    factor_rows = [["sh600000", day, 20.0, 30.0, 40.0] for day in days]
    out = components_at(build_features(_quotes(rows), _factors(factor_rows)))
    last = out.iloc[-1]
    assert last["momentum"] == pytest.approx(0.0)
    # range high 10.5, low 9.0, close 10.0 -> (10.0-9.0)/(10.5-9.0) = 2/3
    assert last["breakout_or_position"] == pytest.approx(2 / 3)
    # close below every MA and ma20 < ma60 -> 0 of 3 checks
    assert last["trend_alignment"] == pytest.approx(0.0)


def test_st_name_gets_the_penalty_bump():
    days = pd.date_range("2024-01-01", periods=25)
    rows = [["sh600000", day, 10.0, 10.0, 10.0, 1e7, 1, 0] for day in days]
    factor_rows = [["sh600000", day, 10.0, 10.0, 10.0] for day in days]
    normal = components_at(build_features(_quotes(rows), _factors(factor_rows))).iloc[
        -1
    ]
    st_rows = [row[:] for row in rows]
    for row in st_rows:
        row[7] = 1  # is_st
    flagged = components_at(
        build_features(_quotes(st_rows), _factors(factor_rows))
    ).iloc[-1]
    assert flagged["risk_penalty"] == pytest.approx(normal["risk_penalty"] + 1.0)


def test_flip_score_prefers_the_weak_side():
    # Two names on the same date: A is below its MAs and falling, B is above and
    # rising. The flip side must score A higher (less negative).
    days = pd.date_range("2024-01-01", periods=25)
    rows, factor_rows = [], []
    for index, day in enumerate(days):
        rows.append(
            ["sh600000", day, 30.0 - index, 30.0 - index, 28.0 - index, 1e7, 1, 0]
        )
        factor_rows.append(["sh600000", day, 40.0, 45.0, 50.0])
        rows.append(
            ["sh600001", day, 10.0 + index, 10.0 + index, 9.0 + index, 1e7, 1, 0]
        )
        factor_rows.append(["sh600001", day, 5.0, 4.0, 3.0])
    frame = components_at(build_features(_quotes(rows), _factors(factor_rows)))
    frame["score"] = flip_score(frame)
    last = frame.loc[frame["date"] == days[-1]].set_index("stock_code")
    assert last.loc["sh600000", "score"] > last.loc["sh600001", "score"]
    session = frame.loc[frame["date"] == days[-1]]
    ranked = cross_sectional_rank(session, session["score"])
    assert ranked.loc[
        session.index[session["stock_code"] == "sh600000"][0]
    ] == pytest.approx(1.0)


def test_missing_components_are_declared_and_renormalised():
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "momentum": [1.0],
            "risk_penalty": [0.0],
        }
    )
    score = flip_score(frame)
    # renormalised over the present components (momentum 15 + penalty 15) -> -0.5
    assert score.iloc[0] == pytest.approx(-0.5)
    assert "signal_strength" in UNAVAILABLE
    assert sum(UNAVAILABLE.values()) == pytest.approx(20.0)
    assert sum(WEIGHTS.values()) == pytest.approx(90.0)
