# -*- coding: utf-8 -*-
"""Known-answer tests for the factor lab (panel labels + single-factor metrics).

A factor lab is only useful if it can reproduce a relationship it was *told*
about. These tests plant one: a panel where the next-session open is a known
function of a synthetic factor, so the expected IC sign, magnitude ordering and
cost arithmetic are all derivable by hand. Nothing here touches Mongo.
"""

import datetime

import numpy as np
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


def _quote(
    code,
    day,
    price,
    *,
    volume=1_000_000,
    status=1,
    change_rate=0.0,
    previous_close=None,
    is_st=0,
):
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
        "previous_close": previous_close,
        "trade_status": status,
        "isST": is_st,
    }


def test_labels_are_trading_day_offsets_not_calendar_windows():
    """h=1 must resolve across a weekend — the legacy int(h*1.5) rule cannot."""
    days = _sessions(6)  # Mon..Mon
    assert days[4].weekday() == 4 and days[5].weekday() == 0
    rows = [_quote("sh600000", day, 10.0 + index) for index, day in enumerate(days)]
    frame = panel_mod.build_panel(rows, horizons=(1,))

    friday = frame.loc[frame["date"] == days[4]].iloc[0]
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
    # The last row has no T+1 session at all; the second-to-last has a T+1 but no
    # h=1 exit.
    assert frame.loc[days[4], "blocked_h1"] == "no_next_session"
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
        _quote("sh600001", days[0], 10.0, previous_close=10.0),
        _quote("sh600001", days[1], 10.0, previous_close=10.0),
        _quote("sh600001", days[2], 9.0, previous_close=10.0),
        _quote("sh600001", days[3], 10.0, previous_close=10.0),
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
    # ChiNext ST names keep the 20 % band; SH B-shares are not BSE.
    assert panel_mod.price_limit("sz300750", is_st=True) == 20.0
    assert panel_mod.price_limit("sh900901") == 10.0
    # Bare (unprefixed) codes resolve the same way.
    assert panel_mod.price_limit("300750") == 20.0
    assert panel_mod.price_limit("430047") == 30.0
    assert panel_mod.price_limit("900901") == 10.0

    days = _sessions(3)
    # A 20 %-board name up 15 % is tradable; the flat 9.9 % rule would block it.
    rows = [
        _quote("sz300750", days[0], 10.0, previous_close=10.0),
        _quote("sz300750", days[1], 11.5, previous_close=10.0),
        _quote("sz300750", days[2], 12.0, previous_close=11.5),
    ]
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] is None
    assert frame.loc[days[0], "fwd_h1"] == pytest.approx(12.0 / 11.5 - 1)


def test_limit_detection_uses_the_open_not_the_close():
    """A session that opens at the limit is a fictive buy even if it closes below
    it; a session that closes limit-up but opened tradable is a real entry."""
    days = _sessions(3)
    # Case A: open is limit-up (+10 %), close is only +5 % (change_rate=5).
    opening_at_limit = [
        _quote("sh600000", days[0], 10.0, previous_close=10.0),
        _quote("sh600000", days[1], 11.0, previous_close=10.0, change_rate=5.0),
        _quote("sh600000", days[2], 11.0, previous_close=11.0),
    ]
    frame = panel_mod.build_panel(opening_at_limit, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] == "limit_up_entry"
    # And the mirrored short leg: an open at the *down* limit cannot be shorted.
    assert frame.loc[days[0], "blocked_short_h1"] is None

    # Case B: open is unchanged, close is limit-up (change_rate=10).
    closing_at_limit = [
        _quote("sh600000", days[0], 10.0, previous_close=10.0),
        _quote("sh600000", days[1], 10.0, previous_close=10.0, change_rate=10.0),
        _quote("sh600000", days[2], 10.5, previous_close=11.0),
    ]
    frame = panel_mod.build_panel(closing_at_limit, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] is None


def test_st_band_is_applied_end_to_end():
    """`isST` must actually reach `build_panel` and tighten a main-board entry to
    the 5 % band (a 6 % open is limit-up for an ST name but not for a normal one)."""
    days = _sessions(3)
    rows = [
        _quote("sh600000", days[0], 10.0, previous_close=10.0, is_st=1),
        _quote("sh600000", days[1], 10.6, previous_close=10.0, is_st=1),
        _quote("sh600000", days[2], 10.6, previous_close=10.6, is_st=1),
        # The same price path on a non-ST main-board name is tradable.
        _quote("sh600001", days[0], 10.0, previous_close=10.0),
        _quote("sh600001", days[1], 10.6, previous_close=10.0),
        _quote("sh600001", days[2], 10.6, previous_close=10.6),
        # A ChiNext ST name keeps the 20 % band: +6 % is not limit-up.
        _quote("sz300001", days[0], 10.0, previous_close=10.0, is_st=1),
        _quote("sz300001", days[1], 10.6, previous_close=10.0, is_st=1),
        _quote("sz300001", days[2], 10.6, previous_close=10.6, is_st=1),
    ]
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index(["stock_code", "date"])
    assert frame.loc[("sh600000", days[0]), "blocked_h1"] == "limit_up_entry"
    assert frame.loc[("sh600001", days[0]), "blocked_h1"] is None
    assert frame.loc[("sz300001", days[0]), "blocked_h1"] is None


def test_short_leg_blocks_the_mirrored_side():
    days = _sessions(3)
    rows = [
        _quote("sh600000", days[0], 10.0, previous_close=10.0),
        # entry session opens limit-down: cannot short into a limit-down open
        _quote("sh600000", days[1], 9.0, previous_close=10.0),
        _quote("sh600000", days[2], 9.2, previous_close=9.0),
    ]
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] is None
    assert frame.loc[days[0], "blocked_short_h1"] == "limit_down_entry"
    assert frame.loc[days[0], "fwd_short_h1"] is None or pd.isna(
        frame.loc[days[0], "fwd_short_h1"]
    )

    rows = [
        _quote("sh600000", days[0], 10.0, previous_close=10.0),
        _quote("sh600000", days[1], 10.0, previous_close=10.0),
        # exit session opens limit-up: cannot buy back to cover
        _quote("sh600000", days[2], 11.0, previous_close=10.0),
    ]
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] is None  # a long can sell into it
    assert frame.loc[days[0], "blocked_short_h1"] == "limit_up_exit"


def test_missing_session_between_is_not_silently_stitched():
    """A stock that is missing a quote row must not have its holding period
    silently extended: with an explicit calendar the leg is dropped."""
    days = _sessions(5)
    rows = [_quote("sh600000", day, 10.0, previous_close=10.0) for day in days]
    del rows[1]  # Monday's row is missing; Tuesday..Friday remain
    frame = panel_mod.build_panel(rows, horizons=(1,), sessions=days).set_index("date")
    # Observation on day0: its T+1 should be day1, but the next row is day2.
    assert frame.loc[days[0], "blocked_h1"] == "missing_session_between"
    # Observation on day2 is unaffected.
    assert frame.loc[days[2], "blocked_h1"] is None
    # Without a calendar the panel cannot see the market's missing session.
    positional = panel_mod.build_panel(rows, horizons=(1,)).set_index("date")
    assert positional.loc[days[0], "blocked_h1"] is None


def test_missing_previous_close_is_not_treated_as_tradable():
    days = _sessions(3)
    rows = [_quote("sh600000", day, 10.0) for day in days]
    rows[1]["change_rate"] = None  # feed gap: the limit verdict is unknown
    frame = panel_mod.build_panel(rows, horizons=(1,)).set_index("date")
    assert frame.loc[days[0], "blocked_h1"] == "missing_previous_close"


def test_planted_factor_signal_is_recovered_with_the_expected_sign():
    """A factor built to predict the forward return must show positive IC, and
    its mirror image negative IC, on a panel with many names per session.

    The planted series is exact: every name compounds at a constant daily rate,
    so its own return *is* its next-session return and the rank IC is +1.
    """
    days = _sessions(30)
    rows = []
    for name_index in range(40):
        growth = 1.001 ** (name_index + 1)
        price = 10.0
        for day in days:
            rows.append(_quote(f"sh60{name_index:04d}", day, price))
            price *= growth
    frame = panel_mod.build_panel(rows, horizons=(1,))
    frame["planted"] = frame.groupby("stock_code")["close_hfq"].pct_change()

    report = metrics.ic_report(frame, "planted", 1)
    assert report["n_dates"] > 10
    assert report["n_observations"] > 200
    # Rank IC is exactly 1: the factor ranks are the label ranks every session.
    assert report["ic_mean"] == pytest.approx(1.0)

    frame["mirror"] = -frame["planted"]
    mirror = metrics.ic_report(frame, "mirror", 1)
    assert mirror["ic_mean"] == pytest.approx(-report["ic_mean"], abs=1e-9)

    # A seeded pseudo-random factor must not look significant.
    rng = np.random.default_rng(20260913)
    frame["noise"] = rng.random(len(frame))
    noise = metrics.ic_report(frame, "noise", 1)
    assert abs(noise["ic_mean"]) < 0.2


def test_newey_west_t_matches_the_naive_t_and_penalises_overlap():
    """The Newey-West statistic is what makes the overlapping-label t honest, so it
    needs its own known-answer test, not just a non-null assertion."""
    rng = np.random.default_rng(20260913)
    white = pd.Series(rng.normal(size=400))
    naive = float(white.mean() / white.std(ddof=0) * np.sqrt(len(white)))
    # `newey_west_t` rounds to 3 dp, so compare at that precision.
    assert metrics.newey_west_t(white, 0) == pytest.approx(round(naive, 3), abs=1e-9)

    # A strongly autocorrelated IC series keeps its sign but loses significance.
    series = [0.0]
    for _ in range(799):
        series.append(0.5 + 0.7 * series[-1] + rng.normal(scale=0.5))
    overlapping = pd.Series(series)
    naive_overlap = float(
        overlapping.mean() / overlapping.std(ddof=1) * np.sqrt(len(overlapping))
    )
    corrected = metrics.newey_west_t(overlapping, 19)
    assert corrected is not None
    assert np.sign(corrected) == np.sign(naive_overlap)
    assert abs(corrected) < abs(naive_overlap)


def test_long_short_spread_pays_one_round_trip_per_leg():
    """The spread must not cancel its own cost: a long-short book pays a round
    trip on each leg, so top_minus_bottom == gross_spread - 2 * cost."""
    days = _sessions(4)
    rows = []
    for name_index in range(20):
        code = f"sh60{name_index:04d}"
        for day in days:
            rows.append(_quote(code, day, 10.0 + name_index * 0.1))
    frame = panel_mod.build_panel(rows, horizons=(1,))
    frame["by_name"] = frame["stock_code"].str[-2:].astype(float)
    names = sorted(frame["stock_code"].unique())
    # Plant a known label: every name in the top quantile returns +2 %, every name
    # in the bottom one -2 %, so the gross spread is exactly 4 %.
    frame["fwd_h1"] = 0.0
    frame.loc[frame["stock_code"].isin(names[-4:]), "fwd_h1"] = 0.02
    frame.loc[frame["stock_code"].isin(names[:4]), "fwd_h1"] = -0.02

    report = metrics.quantile_report(frame, "by_name", 1, quantiles=5, net=True)
    cost = metrics.round_trip_cost()
    assert report["gross_top_minus_bottom"] == pytest.approx(0.04, abs=1e-9)
    assert report["cost_per_leg"] == pytest.approx(cost)
    assert report["top_minus_bottom"] == pytest.approx(0.04 - 2 * cost, abs=1e-9)
    # The old bug subtracted the same cost from both legs, which cancels exactly.
    assert report["top_minus_bottom"] != pytest.approx(
        report["gross_top_minus_bottom"], abs=1e-9
    )

    # The mirrored short leg uses its own (differently filtered) sample; the net
    # spread must still be exactly `gross - 2 * cost`.
    frame["fwd_short_h1"] = -frame["fwd_h1"]
    frame.loc[frame["stock_code"] == names[1], "fwd_short_h1"] = np.nan
    mirrored = metrics.quantile_report(
        frame, "by_name", 1, quantiles=5, net=True, short_column="fwd_short_h1"
    )
    assert mirrored["bottom_leg"]["source"] == "fwd_short_h1"
    assert mirrored["bottom_leg"]["observations"] < len(frame) // 5
    assert mirrored["top_minus_bottom"] == pytest.approx(
        mirrored["gross_top_minus_bottom"] - 2 * cost, abs=1e-9
    )


def test_long_blocked_but_short_tradable_observation_still_trades_short():
    """Buckets are formed on the union of the legs, so an entry a long cannot buy
    (limit-up) must still be able to trade the short leg."""
    days = _sessions(4)
    rows = []
    for name_index in range(20):
        code = f"sh60{name_index:04d}"
        for day in days:
            rows.append(_quote(code, day, 10.0 + name_index * 0.1))
    frame = panel_mod.build_panel(rows, horizons=(1,))
    frame["by_name"] = frame["stock_code"].str[-2:].astype(float)
    names = sorted(frame["stock_code"].unique())
    frame["fwd_h1"] = 0.0
    frame["fwd_short_h1"] = 0.0
    # The lowest-factor name cannot be bought (long label null) but can be shorted.
    bottom = frame["stock_code"] == names[0]
    frame.loc[bottom, "fwd_h1"] = np.nan
    frame.loc[bottom, "fwd_short_h1"] = 0.05

    report = metrics.quantile_report(
        frame, "by_name", 1, quantiles=5, short_column="fwd_short_h1"
    )
    assert report["bottom_leg"]["source"] == "fwd_short_h1"
    # All 4 bottom names x 4 sessions; only names[0] earns 0.05 on the short leg.
    assert report["bottom_leg"]["observations"] == 16
    assert report["bottom_leg"]["gross_return"] == pytest.approx(0.05 / 4)
    # The long book still has no label for the blocked name.
    assert report["n_observations"] == int(frame["fwd_h1"].notna().sum())
    assert report["n_short_observations"] == len(frame)


def test_profit_concentration_measures_pnl_not_bucket_balance():
    """The gate must read the best trade's share of positive P&L, not the bucket
    sizes (which are equal by construction and can never trip 0.4)."""
    days = _sessions(6)
    rows = []
    for name_index in range(20):
        code = f"sh60{name_index:04d}"
        for day in days:
            rows.append(_quote(code, day, 10.0 + name_index * 0.1))
    frame = panel_mod.build_panel(rows, horizons=(1,))
    frame["by_name"] = frame["stock_code"].str[-2:].astype(float)
    frame["fwd_h1"] = 0.001  # uniform small positive P&L
    names = sorted(frame["stock_code"].unique())
    # One single trade carries almost the whole P&L.
    top_rows = frame.index[frame["stock_code"] == names[-1]]
    frame.loc[top_rows[0], "fwd_h1"] = 5.0

    report = metrics.quantile_report(frame, "by_name", 1, quantiles=5)
    assert report["profit_concentration"] > 0.4
    gates = metrics.gate_report(
        {"n_dates": 500, "ic_mean": 0.1},
        report,
        {"walk_forward_decay": 0.0},
    )
    assert "profit_concentration" in gates["failures"]


def test_walk_forward_decay_is_signed():
    """A factor that flips sign out of sample must score a large positive decay,
    not the zero an |IC|-based decay reports."""
    days = _sessions(6)
    rows = []
    for name_index in range(20):
        code = f"sh60{name_index:04d}"
        for day in days:
            rows.append(_quote(code, day, 10.0 + name_index * 0.1))
    frame = panel_mod.build_panel(rows, horizons=(1,))
    frame["by_name"] = frame["stock_code"].str[-2:].astype(float)
    names = sorted(frame["stock_code"].unique())

    # Train: factor ranks match the label. Validation/test: the label is mirrored.
    frame["fwd_h1"] = 0.0
    early = frame["date"] < days[3]
    frame.loc[early & (frame["stock_code"] == names[-1]), "fwd_h1"] = 0.05
    frame.loc[early & (frame["stock_code"] == names[0]), "fwd_h1"] = -0.05
    frame.loc[~early & (frame["stock_code"] == names[-1]), "fwd_h1"] = -0.05
    frame.loc[~early & (frame["stock_code"] == names[0]), "fwd_h1"] = 0.05

    report = metrics.walk_forward(
        frame,
        "by_name",
        1,
        [
            ("train", days[0].date().isoformat(), days[2].date().isoformat()),
            ("validation", days[3].date().isoformat(), days[4].date().isoformat()),
            ("test", days[5].date().isoformat(), days[5].date().isoformat()),
        ],
    )
    assert report["per_split"]["train"]["ic_mean"] > 0
    assert report["per_split"]["validation"]["ic_mean"] < 0
    assert report["walk_forward_decay"] > 1.0


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
    # A factor uncorrelated with returns has no gross edge, so its net long-short
    # spread is exactly the two legs' cost.
    assert quantiles["top_minus_bottom"] == pytest.approx(
        -2 * metrics.round_trip_cost(), abs=1e-9
    )

    turnover = metrics.turnover_report(frame, "by_name", 1, top_fraction=0.2)
    assert turnover["avg_one_way_turnover"] == 0.0  # ranking never changes

    # A factor whose top set alternates completely every session must turn the
    # whole book over (not merely land somewhere in [0, 1]).
    flipping = frame.copy()
    parity = flipping["date"].map({day: index % 2 for index, day in enumerate(days)})
    flipping["rot"] = np.where(parity == 0, flipping["by_name"], -flipping["by_name"])
    mixed = metrics.turnover_report(flipping, "rot", 1, top_fraction=0.2)
    assert mixed["avg_one_way_turnover"] == pytest.approx(1.0)


def test_turnover_is_measured_at_the_horizon_cadence():
    """An h-day book is re-formed every h sessions; comparing adjacent sessions
    overstates its turnover. The rotation below is period-10, so h=1 sees one
    change in twenty comparisons while h=5 sees one in four."""
    days = _sessions(21)
    rows = []
    for name_index in range(10):
        for day in days:
            rows.append(_quote(f"sh60{name_index:04d}", day, 10.0))
    frame = panel_mod.build_panel(rows, horizons=(1, 5))
    frame["fwd_h1"] = 0.0
    frame["fwd_h5"] = 0.0
    rank = frame["stock_code"].str[-2:].astype(float)
    half = pd.Series(range(len(days)), index=days).map(lambda i: (i // 10) % 2)
    frame["rot"] = np.where(frame["date"].map(half) == 0, rank, -rank)

    h1 = metrics.turnover_report(frame, "rot", 1, top_fraction=0.2)
    h5 = metrics.turnover_report(frame, "rot", 5, top_fraction=0.2)
    assert h1["rebalance_sessions"] == 1
    assert h5["rebalance_sessions"] == 5
    assert h1["avg_one_way_turnover"] < h5["avg_one_way_turnover"]


def _noisy_panel(sessions=80, names=4, seed=20260913):
    """A reproducible price panel with enough history for the 60-session factors."""
    rng = np.random.default_rng(seed)
    days = _sessions(sessions)
    rows = []
    for name_index in range(names):
        code = f"sh60{name_index:04d}"
        price = 10.0
        for day in days:
            price = max(0.5, price * (1 + rng.normal(0, 0.02)))
            rows.append(
                _quote(
                    code,
                    day,
                    round(price, 4),
                    volume=int(1_000_000 * (1 + rng.random())),
                    change_rate=0.0,
                )
            )
    return panel_mod.build_panel(rows, horizons=(5,))


def test_registry_factors_are_look_ahead_free_and_finite():
    """Every registered factor must depend only on the past.

    A middle row of every stock is mutated across *all* feature columns, and
    every value at or before that row must be unchanged. Mutating the last row
    (the previous version) cannot catch a leak of any depth, because a
    ``shift(-k)`` leak would move at most one earlier row.
    """
    frame = _noisy_panel().sort_values(["stock_code", "date"]).reset_index(drop=True)
    before = {name: compute(frame, name) for name in REGISTRY}
    for name in REGISTRY:
        # The 80-session panel must actually exercise every factor, including the
        # 60-session windows (the previous 40-session fixture left 5 of 17 all-NaN).
        assert before[name].notna().any(), f"{name} is all-NaN on the fixture"

    mutated = frame.copy()
    targets = [group.index[40] for _, group in frame.groupby("stock_code")]
    for column in (
        "open",
        "open_hfq",
        "close",
        "close_hfq",
        "high_hfq",
        "low_hfq",
        "trade_amount",
        "previous_close",
    ):
        mutated.loc[targets, column] = mutated.loc[targets, column] * 10.0
    mutated.loc[targets, "volume"] = mutated.loc[targets, "volume"] * 10

    for name in sorted(REGISTRY):
        after = compute(mutated, name)
        assert len(after) == len(frame)
        for _, group in frame.groupby("stock_code"):
            index = group.index
            pd.testing.assert_series_equal(
                before[name].loc[index[:40]].reset_index(drop=True),
                after.loc[index[:40]].reset_index(drop=True),
                check_names=False,
                obj=f"{name} past values",
            )


def test_rsi_is_defined_on_an_all_gain_window():
    """A window with no losses must give RSI 100, not NaN (the division by an
    all-zero average loss previously produced a vacuous NaN)."""
    days = _sessions(25)
    rows = [
        _quote("sh600000", day, 10.0 * (1.01**index)) for index, day in enumerate(days)
    ]
    frame = panel_mod.build_panel(rows, horizons=(5,))
    values = compute(frame, "rsi_14").dropna()
    assert not values.empty
    assert float(values.iloc[-1]) == pytest.approx(100.0)


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
    assert entry["coverage_short"]["leg"] == "short"
    assert "passed" in entry["gates"]
    assert report["cost_per_round_trip"] == pytest.approx(metrics.round_trip_cost())


def test_loader_round_trip_preserves_blocked_reasons(tmp_path):
    """Reasons must survive the parquet round trip used by `evaluate`.

    A regression here silently empties the coverage diagnostics rather than
    failing loudly: `pd.to_numeric` on the reason strings turns every entry into
    NaN before the categorical cast.
    """
    from app.jobs.factor_lab_runner import _load_panel

    days = _sessions(3)
    rows = [
        # sh600000: the entry session opens limit-up -> long blocked, short open.
        _quote("sh600000", days[0], 10.0, previous_close=10.0),
        _quote("sh600000", days[1], 11.0, previous_close=10.0),
        _quote("sh600000", days[2], 10.0, previous_close=11.0),
        # sh600001: the exit session opens limit-up -> short blocked, long open.
        _quote("sh600001", days[0], 10.0, previous_close=10.0),
        _quote("sh600001", days[1], 10.0, previous_close=10.0),
        _quote("sh600001", days[2], 11.0, previous_close=10.0),
    ]
    frame = panel_mod.build_panel(rows, horizons=(1,))
    path = tmp_path / "panel.parquet"
    frame.to_parquet(path)

    loaded = _load_panel(str(path), horizons=[1])
    long_rows = loaded.loc[loaded["stock_code"] == "sh600000"]
    short_rows = loaded.loc[loaded["stock_code"] == "sh600001"]

    assert long_rows["blocked_h1"].tolist()[0] == "limit_up_entry"
    assert long_rows["fwd_h1"].isna().tolist()[0]
    assert short_rows["blocked_short_h1"].tolist()[0] == "limit_up_exit"
    assert short_rows["fwd_h1"].notna().tolist()[0]
    # Numeric columns are still downcast for memory, and labels survive.
    assert loaded["close"].dtype == "float32"
    assert loaded["fwd_h1"].isna().sum() >= 1


def test_loader_restores_row_order_for_window_factors(tmp_path):
    """`_by_stock` rolling windows are order-dependent, so a parquet whose rows
    are not grouped/ascending must be re-sorted rather than silently inverted."""
    from app.jobs.factor_lab_runner import _load_panel

    frame = _noisy_panel(sessions=30, names=3)
    shuffled = frame.sample(frac=1.0, random_state=7).reset_index(drop=True)
    path = tmp_path / "panel.parquet"
    shuffled.to_parquet(path)

    loaded = _load_panel(str(path), horizons=[5])
    for _, group in loaded.groupby("stock_code", observed=True):
        assert group["date"].is_monotonic_increasing

    expected = compute(frame, "momentum_10").reset_index(drop=True)
    got = compute(loaded, "momentum_10").reset_index(drop=True)
    left = pd.DataFrame(
        {"code": loaded["stock_code"].astype(str), "date": loaded["date"], "got": got}
    )
    right = pd.DataFrame(
        {
            "code": frame["stock_code"].astype(str),
            "date": frame["date"],
            "exp": expected,
        }
    )
    merged = left.merge(right, on=["code", "date"], how="inner")
    assert len(merged) == len(frame)
    # float32 storage vs the in-memory float64 series: compare at float32 tolerance.
    assert np.allclose(
        merged["got"].fillna(-1).astype("float64"),
        merged["exp"].fillna(-1).astype("float64"),
        rtol=1e-4,
        atol=1e-6,
    )


def test_loader_drops_unrequested_horizons_for_both_legs(tmp_path):
    from app.jobs.factor_lab_runner import _load_panel, split_label_column

    days = _sessions(3)
    rows = [_quote("sh600000", day, 10.0, previous_close=10.0) for day in days]
    frame = panel_mod.build_panel(rows, horizons=(1, 5))
    path = tmp_path / "panel.parquet"
    frame.to_parquet(path)

    loaded = _load_panel(str(path), horizons=[1])
    families = {split_label_column(column) for column in loaded.columns}
    assert ("fwd_h", 1) in families
    assert ("fwd_short_h", 1) in families
    assert ("fwd_h", 5) not in families
    assert ("blocked_short_h", 5) not in families
