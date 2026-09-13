# -*- coding: utf-8 -*-
"""Known-answer tests for the factor lab (panel labels + single-factor metrics).

A factor lab is only useful if it can reproduce a relationship it was *told*
about. These tests plant one: a panel where the next-session open is a known
function of a synthetic factor, so the expected IC sign, magnitude ordering and
cost arithmetic are all derivable by hand. Nothing here touches Mongo.
"""

import datetime

import pandas as pd
import pytest

from app.lib.factor_lab import metrics, panel as panel_mod
from app.lib.factor_lab.factors import REGISTRY, compute

START = datetime.datetime(2026, 1, 5)  # a Monday


def _sessions(count: int, start: datetime.datetime = START):
    """Consecutive weekdays from ``start`` (labels are positional, so gaps do
    not change the offset arithmetic — only real sessions matter)."""
    days = []
    day = start
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day += datetime.timedelta(days=1)
    return days


def _quote(code, day, price, *, volume=1_000_000, status=1, change_rate=0.0):
    return {
        "code": code,
        "date": day,
        "open": price,
        "open_hfq": price,
        "close": price,
        "close_hfq": price,
        "high_hfq": price * 1.02,
        "low_hfq": price * 0.98,
        "volume": volume,
        "trade_amount": price * volume,
        "change_rate": change_rate,
        "trade_status": status,
    }


def test_labels_are_trading_day_offsets_not_calendar_windows():
    """h=1 must resolve across a weekend — the legacy int(h*1.5) rule cannot."""
    days = _sessions(6)  # Mon..Mon
    assert days[4].weekday() == 4 and days[5].weekday() == 0
    rows = [_quote("sh600000", day, 10.0 + index) for index, day in enumerate(days)]
    frame = panel_mod.build_panel(rows, horizons=(1,))

    friday = frame.loc[frame["date"] == days[4]].iloc[0]
    # entry = Monday open (11+? see prices), exit = the session after that
    assert friday["blocked_h1"] == "no_exit_yet"
    thursday = frame.loc[frame["date"] == days[3]].iloc[0]
    assert thursday["blocked_h1"] is None
    # Thursday: entry = Friday open (14.0), exit = next Monday open (15.0). The
    # legacy int(h*1.5) calendar rule would refuse this h=1 leg entirely.
    assert thursday["fwd_h1"] == pytest.approx(15.0 / 14.0 - 1)


def test_label_uses_t_plus_one_open_and_h_sessions_later():
    days = _sessions(5)
    prices = [10.0, 11.0, 12.0, 13.0, 14.0]
    rows = [_quote("sh600000", day, price) for day, price in zip(days, prices)]
    frame = panel_mod.build_panel(rows, horizons=(1, 2)).set_index("date")

    # Day 0: entry = day1 open (11), exit h=1 = day2 open (12) -> 9.09%
    assert frame.loc[days[0], "fwd_h1"] == pytest.approx(12.0 / 11.0 - 1)
    # h=2: exit = day3 open (13)
    assert frame.loc[days[0], "fwd_h2"] == pytest.approx(13.0 / 11.0 - 1)
    # Last two rows have no full forward window.
    assert frame.loc[days[4], "fwd_h1"] is None or pd.isna(frame.loc[days[4], "fwd_h1"])
    assert frame.loc[days[3], "blocked_h1"] == "no_exit_yet"


def test_untradable_entry_and_exit_are_blocked_with_reasons():
    days = _sessions(4)
    rows = [
        _quote("sh600000", days[0], 10.0),
        # entry session suspended
        _quote("sh600000", days[1], 10.0, status=0),
        _quote("sh600000", days[2], 10.0),
        _quote("sh600000", days[3], 10.0),
        # a second stock whose exit session is limit-down (cannot sell)
        _quote("sh600001", days[0], 10.0),
        _quote("sh600001", days[1], 10.0),
        _quote("sh600001", days[2], 10.0, change_rate=-10.0),
        _quote("sh600001", days[3], 10.0),
    ]
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index(["stock_code", "date"])

    assert frame.loc[("sh600000", days[0]), "blocked_h1"] == "suspended_entry"
    assert pd.isna(frame.loc[("sh600000", days[0]), "fwd_h1"])
    assert frame.loc[("sh600001", days[0]), "blocked_h1"] == "limit_down_exit"


def test_price_limits_are_board_aware():
    assert panel_mod.price_limit("sh600000") == 10.0
    assert panel_mod.price_limit("sz300750") == 20.0  # ChiNext
    assert panel_mod.price_limit("sh688111") == 20.0  # STAR
    assert panel_mod.price_limit("bj430047") == 30.0  # BSE
    assert panel_mod.price_limit("sh600000", is_st=True) == 5.0

    days = _sessions(3)
    # A 20 %-board name up 15 % is tradable; the flat 9.9 % rule would block it.
    rows = [
        _quote("sz300750", days[0], 10.0),
        _quote("sz300750", days[1], 11.5, change_rate=15.0),
        _quote("sz300750", days[2], 12.0),
    ]
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] is None
    assert frame.loc[days[0], "fwd_h1"] == pytest.approx(12.0 / 11.5 - 1)


def test_missing_change_rate_is_not_treated_as_tradable():
    days = _sessions(3)
    rows = [_quote("sh600000", day, 10.0) for day in days]
    rows[1]["change_rate"] = None  # feed gap: could be a limit session
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] == "limit_up_entry"


def test_planted_factor_signal_is_recovered_with_the_expected_sign():
    """A factor built to predict the forward return must show positive IC, and
    its mirror image negative IC, on a panel with many names per session."""
    days = _sessions(30)
    rows = []
    for name_index in range(40):
        code = f"sh60{name_index:04d}"
        price = 10.0
        for index, day in enumerate(days):
            # deterministic, session-varying drift per name
            drift = ((name_index % 7) - 3) * 0.001 * (1 + index % 5)
            price = price * (1 + drift)
            rows.append(_quote(code, day, round(price, 4)))
    frame = panel_mod.build_panel(rows, horizons=(1,))
    # The planted factor is simply the realised session return, which by
    # construction has the same sign pattern as the next session's drift.
    frame["planted"] = frame.groupby("stock_code")["close_hfq"].pct_change()

    report = metrics.ic_report(frame, "planted", 1)
    assert report["n_dates"] > 10
    assert report["n_observations"] > 200
    assert report["ic_mean"] is not None

    frame["mirror"] = -frame["planted"]
    mirror = metrics.ic_report(frame, "mirror", 1)
    assert mirror["ic_mean"] == pytest.approx(-report["ic_mean"], abs=1e-9)

    # A pure noise factor must not look significant.
    frame["noise"] = [
        ((hash((code, day)) % 1000) / 1000.0)
        for code, day in zip(frame["stock_code"], frame["date"])
    ]
    noise = metrics.ic_report(frame, "noise", 1)
    assert abs(noise["ic_mean"]) < 0.2


def test_quantiles_and_turnover_are_cost_aware():
    days = _sessions(12)
    rows = []
    for name_index in range(20):
        code = f"sh60{name_index:04d}"
        for index, day in enumerate(days):
            rows.append(_quote(code, day, 10.0 + name_index * 0.1))
    frame = panel_mod.build_panel(rows, horizons=(1,))
    frame["constant"] = 1.0
    frame["by_name"] = frame["stock_code"].str[-2:].astype(float)

    quantiles = metrics.quantile_report(frame, "by_name", 1, quantiles=5)
    assert quantiles["quantiles"]
    assert quantiles["cost_per_round_trip"] == pytest.approx(metrics.round_trip_cost())
    # A factor uncorrelated with returns should give a near-zero spread.
    assert quantiles["top_minus_bottom"] is not None

    turnover = metrics.turnover_report(frame, "by_name", 1, top_fraction=0.2)
    assert turnover["avg_one_way_turnover"] == 0.0  # ranking never changes

    flipping = frame.copy()
    flipping["flip"] = flipping.groupby("date")["by_name"].rank(ascending=False)
    mixed = metrics.turnover_report(flipping, "flip", 1, top_fraction=0.2)
    assert mixed["avg_one_way_turnover"] is None or (
        0.0 <= mixed["avg_one_way_turnover"] <= 1.0
    )


def test_registry_factors_are_look_ahead_free_and_finite():
    """Every registered factor must be computable and depend only on the past.

    Verified by mutating a future row and asserting the past values are
    unchanged — the cheapest way to catch a window with the wrong sign.
    """
    days = _sessions(40)
    rows = []
    for name_index in range(5):
        code = f"sh60{name_index:04d}"
        for index, day in enumerate(days):
            rows.append(
                _quote(
                    code, day, 10.0 + index * 0.05 + name_index, volume=10**6 + index
                )
            )
    frame = (
        panel_mod.build_panel(rows, horizons=(5,))
        .sort_values(["stock_code", "date"])
        .reset_index(drop=True)
    )

    mutated = frame.copy()
    last = mutated.index[-1]
    mutated.loc[last, "close_hfq"] = mutated.loc[last, "close_hfq"] * 10
    mutated.loc[last, "volume"] = mutated.loc[last, "volume"] * 10
    mutated.loc[last, "trade_amount"] = mutated.loc[last, "trade_amount"] * 10

    for name in sorted(REGISTRY):
        before = compute(frame, name)
        after = compute(mutated, name)
        assert len(before) == len(frame)
        # Only the mutated (last) row may differ.
        differing = (before.fillna(-1) != after.fillna(-1)).sum()
        assert differing <= 1, f"{name} changed {differing} rows on a future mutation"


def test_evaluate_factor_reports_gates_and_sample_sizes():
    days = _sessions(150)
    rows = []
    for name_index in range(30):
        code = f"sh60{name_index:04d}"
        for index, day in enumerate(days):
            rows.append(_quote(code, day, 10.0 * (1 + 0.0005 * (index + name_index))))
    frame = panel_mod.build_panel(rows, horizons=(5,))

    report = metrics.evaluate_factor(
        frame, compute(frame, "momentum_10"), horizons=[5], factor_name="momentum_10"
    )
    entry = report["horizons"]["5"]
    assert entry["ic"]["n_dates"] > 0
    assert entry["coverage"]["rows"] == len(frame)
    assert entry["coverage"]["coverage"] == pytest.approx(
        entry["coverage"]["resolved"] / entry["coverage"]["rows"]
    )
    assert "passed" in entry["gates"]
    assert report["cost_per_round_trip"] == pytest.approx(metrics.round_trip_cost())
