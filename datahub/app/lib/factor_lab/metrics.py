# -*- coding: utf-8 -*-
"""Single-factor metrics: IC/ICIR, decile spread, turnover, walk-forward decay.

Everything here is cross-sectional per session, net of the profile's round-trip
cost, and reports the sample size next to every statistic — the failure mode this
project already hit twice is believing a number whose denominator was tiny
("30-name book", "single-year window", "all-None IC"). Callers should read
``n_dates``/``n_observations`` before reading the sign.

Cost conventions, stated once:

* ``avg_net_return`` per quantile is a **long-only** net return (gross − 1 round
  trip); it is what the paper path would realise on that bucket.
* ``top_minus_bottom`` is a **long-short** net spread. Each leg pays its own
  round trip, so it equals ``gross_spread − 2 * round_trip_cost()``. Subtracting
  the same cost from both legs (the previous implementation) cancels exactly and
  reports the gross spread as if it were net.
* ``t_stat`` is i.i.d.; ``t_stat_nw`` is Newey-West with ``lag = horizon - 1``,
  because an h-day label makes h consecutive ICs overlap.
"""

import math

import pandas as pd

#: The paper/backtest execution model, mirrored so a factor's net edge is
#: comparable with what the strategy layer would realise.
SLIPPAGE_PER_SIDE = 0.001
COMMISSION_RATE = 0.00025
SELL_STAMP_DUTY_RATE = 0.001

#: Default gate thresholds, aligned with `autoresearch/profile.yaml`'s hard gates
#: where an analogue exists.
MIN_DATES = 120
MAX_WALK_FORWARD_DECAY = 0.2
MAX_PROFIT_CONCENTRATION = 0.4


def round_trip_cost() -> float:
    """Fraction of notional lost to one buy + one sell."""
    return 2 * SLIPPAGE_PER_SIDE + 2 * COMMISSION_RATE + SELL_STAMP_DUTY_RATE


def _rank_ic(frame: pd.DataFrame, factor: str, label: str) -> float | None:
    """Spearman IC of one session: rank correlation of factor and label."""
    subset = frame.loc[frame[factor].notna() & frame[label].notna(), [factor, label]]
    if len(subset) < 5:
        return None
    if subset[factor].nunique() < 2 or subset[label].nunique() < 2:
        return None
    ranked = subset.rank()
    return float(ranked[factor].corr(ranked[label]))


def newey_west_t(series: pd.Series, lag: int) -> float | None:
    """t-statistic of a mean that is robust to ``lag``-order autocorrelation.

    An h-day forward label is observed on h consecutive sessions, so consecutive
    ICs share up to ``h-1`` holding days. The plain ``mean/std*sqrt(n)`` t-stat
    treats them as independent and overstates significance; the usual fix is a
    Bartlett-weighted Newey-West long-run variance with ``lag = h-1``.
    """
    values = pd.Series(series, dtype="float64").dropna()
    count = len(values)
    if count < 3:
        return None
    demeaned = values - values.mean()
    gamma_0 = float((demeaned**2).sum() / count)
    variance = gamma_0
    for order in range(1, max(0, int(lag)) + 1):
        if order >= count:
            break
        weight = 1.0 - order / (lag + 1.0)
        covariance = float(
            (demeaned.iloc[order:] * demeaned.iloc[:-order]).sum() / count
        )
        variance += 2.0 * weight * covariance
    if variance <= 0:
        return None
    return round(float(values.mean() / math.sqrt(variance / count)), 3)


def ic_report(panel: pd.DataFrame, factor: str, horizon: int) -> dict:
    """Per-session cross-sectional IC plus its ICIR and t-statistic.

    The IC is a rank statistic on the gross label: a uniform round-trip cost
    shifts every label by the same amount and preserves ranks, so there is no
    ``net`` variant to report — the cost shows up in the spread, not the IC.

    ``t_stat`` is the naive i.i.d. t-statistic; ``t_stat_nw`` applies a Newey-West
    correction with ``lag = horizon - 1`` for the overlap the label creates.
    """
    label = f"fwd_h{horizon}"
    if factor not in panel.columns or label not in panel.columns:
        raise KeyError(f"panel is missing {factor!r} or {label!r}")
    work = panel.loc[panel[label].notna(), ["date", factor, label]]
    ics = []
    for _, session in work.groupby("date", sort=True):
        value = _rank_ic(session, factor, label)
        if value is not None:
            ics.append(value)
    if not ics:
        return {
            "horizon": horizon,
            "ic_mean": None,
            "ic_std": None,
            "icir": None,
            "t_stat": None,
            "t_stat_nw": None,
            "positive_share": None,
            "n_dates": 0,
            "n_observations": int(work[factor].notna().sum()),
        }
    series = pd.Series(ics, dtype="float64")
    std = float(series.std(ddof=1)) if len(series) > 1 else 0.0
    mean = float(series.mean())
    return {
        "horizon": horizon,
        "ic_mean": round(mean, 6),
        "ic_std": round(std, 6),
        "icir": round(mean / std, 4) if std > 0 else None,
        "t_stat": round(mean / std * math.sqrt(len(series)), 3) if std > 0 else None,
        "t_stat_nw": newey_west_t(series, horizon - 1),
        "positive_share": round(float((series > 0).mean()), 4),
        "n_dates": int(len(series)),
        "n_observations": int(work[factor].notna().sum()),
    }


def quantile_report(
    panel: pd.DataFrame,
    factor: str,
    horizon: int,
    *,
    quantiles: int = 10,
    net: bool = True,
    short_column: str | None = None,
) -> dict:
    """Average net return per factor quantile, per session then pooled.

    Quantiles are formed inside each session (cross-sectional), so a factor whose
    scale drifts over time is still evaluated on relative position.

    Buckets are formed on the **union of the two legs' resolvable observations**,
    so an entry that a long cannot buy but a short can still trades in the short
    leg. Each leg's mean then skips the other leg's missing labels: the long leg
    uses ``fwd_h{h}`` and the mirrored short leg uses ``fwd_short_h{h}`` (or the
    negated long label when the panel has no short column, a statistical short
    whose fills are not checked).

    ``avg_net_return`` is the long-only net return of a bucket (gross minus one
    round trip). ``top_minus_bottom`` is the **long-short** net spread: the long
    leg pays one round trip and the short leg pays one of its own, so the spread
    is ``gross_spread - 2 * round_trip_cost()``, not ``gross_spread``.
    """
    label = f"fwd_h{horizon}"
    has_short = short_column is not None and short_column in panel.columns
    columns = ["date", factor, label]
    if has_short:
        columns.append(short_column)
    resolvable = panel[label].notna()
    if has_short:
        resolvable = resolvable | panel[short_column].notna()
    work = panel.loc[panel[factor].notna() & resolvable, columns].copy()
    if work.empty:
        return {"horizon": horizon, "quantiles": [], "monotonic": None}
    cost = round_trip_cost() if net else 0.0
    work["gross"] = work[label]

    # Equal-count buckets by cross-sectional rank, computed without a per-date
    # Python callback (a groupby-apply here is both slower and deprecated).
    ranks = work.groupby("date")[factor].rank(method="first")
    counts = work.groupby("date")[factor].transform("count")
    scaled = (ranks - 1) / counts * quantiles
    work["bucket"] = scaled.clip(upper=quantiles - 1e-9).astype("int64")
    work = work.loc[counts >= quantiles]
    if work.empty:
        return {"horizon": horizon, "quantiles": [], "monotonic": None}

    # `gross` is NaN where the long label is missing, and `mean` skips those, so
    # the bucket statistics stay long-only even on the union universe.
    long_means = work.groupby("bucket")["gross"].mean().dropna()
    buckets = sorted(int(value) for value in work["bucket"].unique())
    bottom_bucket, top_bucket = buckets[0], buckets[-1]
    if (
        top_bucket not in long_means.index
        or bottom_bucket not in long_means.index
        or len(long_means) < 2
    ):
        # A whole edge bucket with no long label (all short-only) makes the
        # long-short spread unmeasurable; returning early beats silently shifting
        # the leg onto the neighbouring bucket.
        return {"horizon": horizon, "quantiles": [], "monotonic": None}
    gross_means = long_means
    long_counts = work.loc[work[label].notna()].groupby("bucket").size()
    net_means = gross_means - cost
    entries = [
        {
            "quantile": int(bucket),
            "avg_net_return": round(float(net_means.loc[bucket]), 6),
            "avg_gross_return": round(float(gross_means.loc[bucket]), 6),
            "observations": int(long_counts.get(bucket, 0)),
        }
        for bucket in gross_means.index
    ]
    # Long leg: buy the top bucket, sell at the h-th open -> one round trip.
    long_leg_gross = float(gross_means.loc[top_bucket])
    # Short leg: sell the bottom bucket at T+1's open and buy it back at the h-th
    # open -> gain -gross, and one round trip of its own. The gross spread uses the
    # same legs as the net spread, so `top_minus_bottom == gross - 2 * cost` holds
    # exactly.
    short_sample = None
    if has_short:
        short_sample = work.loc[work["bucket"] == bottom_bucket, short_column].dropna()
    if short_sample is not None and len(short_sample):
        short_leg_gross = float(short_sample.mean())
        short_leg_source = short_column
        short_leg_observations = int(len(short_sample))
    else:
        short_leg_gross = -float(gross_means.loc[bottom_bucket])
        short_leg_source = f"negated {label}"
        short_leg_observations = int(long_counts.get(bottom_bucket, 0))
    gross_spread = long_leg_gross + short_leg_gross
    spread = gross_spread - 2 * cost
    # Profit concentration mirrors `autoresearch/profile.yaml`: the best single
    # trade's share of the long book's positive net P&L. (Bucket sizes are equal
    # by construction, so "max bucket count / total" would be a constant 1/quantiles
    # and could never trip the gate.)
    top_returns = work.loc[work["bucket"] == top_bucket, "gross"] - cost
    positive = top_returns[top_returns > 0]
    profit_concentration = (
        round(float(positive.max() / positive.sum()), 6) if positive.sum() > 0 else 0.0
    )
    return {
        "horizon": horizon,
        "quantiles": entries,
        "top_minus_bottom": round(spread, 6),
        "gross_top_minus_bottom": round(gross_spread, 6),
        "monotonic": _monotonic([entry["avg_net_return"] for entry in entries]),
        "profit_concentration": profit_concentration,
        "bottom_leg": {
            "source": short_leg_source,
            "gross_return": round(short_leg_gross, 6),
            "net_return": round(short_leg_gross - cost, 6),
            "observations": short_leg_observations,
        },
        "cost_per_leg": round(cost, 6),
        "cost_per_round_trip": round(round_trip_cost(), 6),
        "n_observations": int(work[label].notna().sum()),
        "n_short_observations": (
            int(work[short_column].notna().sum()) if has_short else None
        ),
    }


def _monotonic(values: list[float]) -> float | None:
    """Spearman correlation of quantile index vs mean return (-1..1).

    A |value| near 1 means the factor sorts returns monotonically; near 0 means
    the top-bottom spread is a tail artefact rather than a gradient.
    """
    if len(values) < 3:
        return None
    index = pd.Series(range(len(values)), dtype="float64")
    return round(float(index.corr(pd.Series(values, dtype="float64"))), 4)


def turnover_report(
    panel: pd.DataFrame, factor: str, horizon: int, *, top_fraction: float = 0.1
) -> dict:
    """Average one-way turnover of the factor's top bucket, per rebalance.

    An h-day book is only re-formed every ``h`` sessions, so consecutive sessions
    of an h>1 factor must not be compared: the names picked on t and t+1 are
    largely the same trade, not a new rebalance. ``avg_one_way_turnover`` is
    measured between the books ``horizon`` sessions apart, and multiplied by the
    round-trip cost it is the friction a factor must clear at that cadence.
    """
    label = f"fwd_h{horizon}"
    work = panel.loc[panel[factor].notna() & panel[label].notna()]
    if work.empty:
        return {"horizon": horizon, "avg_one_way_turnover": None, "n_dates": 0}
    step = max(1, int(horizon))
    previous: set | None = None
    turnovers = []
    for index, (_, session) in enumerate(work.groupby("date", sort=True)):
        if index % step:
            continue
        size = max(1, int(len(session) * top_fraction))
        top = set(session.nlargest(size, factor)["stock_code"])
        if previous is not None and previous:
            turnovers.append(len(top - previous) / len(top))
        previous = top
    if not turnovers:
        return {"horizon": horizon, "avg_one_way_turnover": None, "n_dates": 0}
    avg = sum(turnovers) / len(turnovers)
    return {
        "horizon": horizon,
        "avg_one_way_turnover": round(avg, 4),
        "rebalance_sessions": step,
        "per_rebalance_cost": round(avg * round_trip_cost(), 6),
        "n_dates": len(turnovers),
    }


def walk_forward(panel: pd.DataFrame, factor: str, horizon: int, splits) -> dict:
    """IC per split plus the decay from train to the later splits."""
    from app.lib.factor_lab.panel import split_by_date

    per_split = {}
    for name, start, end in splits:
        subset = split_by_date(panel, start, end)
        report = ic_report(subset, factor, horizon)
        per_split[name] = {
            "ic_mean": report["ic_mean"],
            "icir": report["icir"],
            "n_dates": report["n_dates"],
            "from": start,
            "to": end,
        }
    train = per_split.get("train", {}).get("ic_mean")
    later = [
        value
        for name, value in (
            (name, per_split.get(name, {}).get("ic_mean"))
            for name in ("validation", "test")
        )
        if value is not None
    ]
    decay = None
    if train not in (None, 0) and later:
        # Signed decay: a factor that flips sign out of sample must score a
        # *large* positive decay, not the zero an |IC|-based decay would give
        # (train +1.0 -> validation/test -1.0 must fail). The profile's
        # `_walk_forward_decay` is signed too, but it uses validation only and
        # clamps at 0; this variant averages validation+test and leaves an
        # improving factor's decay negative.
        decay = round(1 - (sum(later) / len(later)) / train, 4)
    return {"per_split": per_split, "walk_forward_decay": decay}


def evaluate_factor(
    panel: pd.DataFrame,
    values: pd.Series,
    *,
    horizons,
    factor_name: str = "factor",
    splits=None,
    quantiles: int = 10,
    top_fraction: float = 0.1,
) -> dict:
    """Assemble the full single-factor report and apply the hard gates.

    ``values`` is the factor series aligned to ``panel.index``. It is passed in
    rather than read from a panel column so sweeping N factors does not need N
    copies of the whole panel (one 3.3M-row copy is ~400MB — enough to OOMKill
    the 2Gi pod once per factor). Each horizon is evaluated on a four-column view
    built from the panel and released before the next horizon.
    """
    from app.lib.factor_lab.panel import coverage, default_splits

    splits = default_splits(panel) if splits is None else splits
    report = {
        "factor": factor_name,
        "sessions": int(pd.to_datetime(panel["date"]).nunique()),
        "rows": int(len(panel)),
        "cost_per_round_trip": round(round_trip_cost(), 6),
        "horizons": {},
    }
    for horizon in horizons:
        label = f"fwd_h{horizon}"
        short_label = f"fwd_short_h{horizon}"
        columns = {
            "date": panel["date"],
            "stock_code": panel["stock_code"],
            "factor": values,
            label: panel[label],
        }
        if short_label in panel.columns:
            columns[short_label] = panel[short_label]
        work = pd.DataFrame(columns)
        coverage_info = coverage(panel, horizon)
        ic = ic_report(work, "factor", horizon)
        quantiles_info = quantile_report(
            work,
            "factor",
            horizon,
            quantiles=quantiles,
            short_column=short_label if short_label in work.columns else None,
        )
        turnover = turnover_report(work, "factor", horizon, top_fraction=top_fraction)
        walk = walk_forward(work, "factor", horizon, splits) if splits else {}
        report["horizons"][str(horizon)] = {
            "coverage": coverage_info,
            "coverage_short": coverage(panel, horizon, leg="short"),
            "ic": ic,
            "quantiles": quantiles_info,
            "turnover": turnover,
            "walk_forward": walk,
            "gates": gate_report(ic, quantiles_info, walk),
        }
        del work
    return report


def gate_report(ic: dict, quantiles: dict, walk: dict) -> dict:
    """Hard gates from the roadmap, applied to one factor/horizon.

    Mirrors `autoresearch/profile.yaml`: enough sessions, no walk-forward decay,
    a top-bottom spread that is not driven by one trade, and enough observations
    for the IC to mean anything. ``turnover`` is deliberately *not* a gate: the
    profile scores it with a penalty, it does not fail the candidate.
    """
    failures = []
    if (ic.get("n_dates") or 0) < MIN_DATES:
        failures.append("insufficient_dates")
    decay = walk.get("walk_forward_decay") if walk else None
    if decay is not None and decay > MAX_WALK_FORWARD_DECAY:
        failures.append("performance_decay")
    concentration = quantiles.get("profit_concentration")
    if concentration is not None and concentration > MAX_PROFIT_CONCENTRATION:
        failures.append("profit_concentration")
    if not ic.get("ic_mean"):
        failures.append("no_ic")
    return {
        "passed": not failures,
        "failures": failures,
        "passed_gates": [
            name
            for name, ok in (
                ("minimum_dates", (ic.get("n_dates") or 0) >= MIN_DATES),
                (
                    "walk_forward_decay",
                    decay is None or decay <= MAX_WALK_FORWARD_DECAY,
                ),
                (
                    "profit_concentration",
                    concentration is None or concentration <= MAX_PROFIT_CONCENTRATION,
                ),
                ("non_zero_ic", bool(ic.get("ic_mean"))),
            )
            if ok
        ],
    }
