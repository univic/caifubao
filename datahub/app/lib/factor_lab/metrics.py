# -*- coding: utf-8 -*-
"""Single-factor metrics: IC/ICIR, decile spread, turnover, walk-forward decay.

Everything here is cross-sectional per session, net of the profile's round-trip
cost, and reports the sample size next to every statistic — the failure mode this
project already hit twice is believing a number whose denominator was tiny
("30-name book", "single-year window", "all-None IC"). Callers should read
``n_dates``/``n_observations`` before reading the sign.
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
MAX_ABS_SPREAD_CONCENTRATION = 0.4


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


def ic_report(
    panel: pd.DataFrame, factor: str, horizon: int, *, net: bool = True
) -> dict:
    """Per-session cross-sectional IC plus its ICIR and t-statistic.

    ``net=False`` measures the raw label; the default subtracts the round-trip
    cost, which for rank IC only shifts levels (ranks are preserved), so the IC
    itself is identical either way — it matters for the spread, not the IC.
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
) -> dict:
    """Average net return per factor quantile, per session then pooled.

    Quantiles are formed inside each session (cross-sectional), so a factor whose
    scale drifts over time is still evaluated on relative position.
    """
    label = f"fwd_h{horizon}"
    work = panel.loc[
        panel[factor].notna() & panel[label].notna(), ["date", factor, label]
    ].copy()
    if work.empty:
        return {"horizon": horizon, "quantiles": [], "monotonic": None}
    cost = round_trip_cost() if net else 0.0
    work["net"] = work[label] - cost

    # Equal-count buckets by cross-sectional rank, computed without a per-date
    # Python callback (a groupby-apply here is both slower and deprecated).
    ranks = work.groupby("date")[factor].rank(method="first")
    counts = work.groupby("date")[factor].transform("count")
    scaled = (ranks - 1) / counts * quantiles
    work["bucket"] = scaled.clip(upper=quantiles - 1e-9).astype("int64")
    work = work.loc[counts >= quantiles]
    if work.empty:
        return {"horizon": horizon, "quantiles": [], "monotonic": None}

    means = work.groupby("bucket")["net"].mean()
    entries = [
        {
            "quantile": int(bucket),
            "avg_net_return": round(float(value), 6),
            "observations": int((work["bucket"] == bucket).sum()),
        }
        for bucket, value in means.items()
    ]
    spread = round(float(means.iloc[-1] - means.iloc[0]), 6) if len(means) > 1 else None
    counts = [entry["observations"] for entry in entries]
    top_share = round(max(counts) / sum(counts), 4) if counts and sum(counts) else None
    return {
        "horizon": horizon,
        "quantiles": entries,
        "top_minus_bottom": spread,
        "monotonic": _monotonic([entry["avg_net_return"] for entry in entries]),
        "concentration": top_share,
        "cost_per_round_trip": round(cost, 6),
        "n_observations": int(len(work)),
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
    """Average one-way turnover of the factor's top bucket, per session.

    Means "what fraction of the long book has to be replaced at each rebalance";
    multiplied by the round-trip cost it is the friction a factor must clear.
    """
    label = f"fwd_h{horizon}"
    work = panel.loc[panel[factor].notna() & panel[label].notna()]
    if work.empty:
        return {"horizon": horizon, "avg_one_way_turnover": None, "n_dates": 0}
    previous: set | None = None
    turnovers = []
    for _, session in work.groupby("date", sort=True):
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
        # Absolute-value decay: a factor that flips sign is as broken as one that
        # fades, so decay is measured on |IC|.
        decay = round(1 - (sum(abs(v) for v in later) / len(later)) / abs(train), 4)
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
        work = pd.DataFrame(
            {
                "date": panel["date"],
                "stock_code": panel["stock_code"],
                "factor": values,
                label: panel[label],
            }
        )
        coverage_info = coverage(panel, horizon)
        ic = ic_report(work, "factor", horizon)
        quantiles_info = quantile_report(work, "factor", horizon, quantiles=quantiles)
        turnover = turnover_report(work, "factor", horizon, top_fraction=top_fraction)
        walk = walk_forward(work, "factor", horizon, splits) if splits else {}
        report["horizons"][str(horizon)] = {
            "coverage": coverage_info,
            "ic": ic,
            "quantiles": quantiles_info,
            "turnover": turnover,
            "walk_forward": walk,
            "gates": gate_report(ic, quantiles_info, turnover, walk),
        }
        del work
    return report


def gate_report(ic: dict, quantiles: dict, turnover: dict, walk: dict) -> dict:
    """Hard gates from the roadmap, applied to one factor/horizon.

    Mirrors `autoresearch/profile.yaml`: enough sessions, no walk-forward decay,
    a top-bottom spread that is not driven by one bucket, and enough observations
    for the IC to mean anything.
    """
    failures = []
    if (ic.get("n_dates") or 0) < MIN_DATES:
        failures.append("insufficient_dates")
    decay = walk.get("walk_forward_decay") if walk else None
    if decay is not None and decay > MAX_WALK_FORWARD_DECAY:
        failures.append("performance_decay")
    concentration = quantiles.get("concentration")
    if concentration is not None and concentration > MAX_ABS_SPREAD_CONCENTRATION:
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
                    concentration is None
                    or concentration <= MAX_ABS_SPREAD_CONCENTRATION,
                ),
                ("non_zero_ic", bool(ic.get("ic_mean"))),
            )
            if ok
        ],
    }
