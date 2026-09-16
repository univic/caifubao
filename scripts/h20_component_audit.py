#!/usr/bin/env python3
"""H20 component isolation audit (multi-regime).

Reads a frozen H20 snapshot parquet and, per regime window and per scoring
component (plus the current production composite), reports against the forward
20-session return:

- daily_ic     : mean daily cross-sectional Spearman IC of the component value
                 against the forward return (robust monotone-signal measure).
- ls_spread_ir: excess IR of the long-short book (top quintile minus bottom
                 quintile by value, equal-weight) — tie-robust via value
                 quantiles, uses the evaluator's IR convention.
- buyTop20IR  : excess IR of buying the top-quintile book vs the same-date
                 eligible equal-weight benchmark (component direction +1).
- buyBot20IR  : same for the bottom-quintile book (direction flipped).

Forward return uses the snapshot's actual_entry_open_hfq / actual_exit_open_hfq
(T+1 entry, 20-session hold with roll-forward already resolved). Research-only.

Run: PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_component_audit.py SNAPSHOT.parquet
"""

from __future__ import annotations

import sys
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=RuntimeWarning)

COMPONENTS = [
    "signal_strength",
    "momentum",
    "trend_alignment",
    "breakout_or_position",
    "industry_momentum",
    "relative_strength",
    "real_relative_strength",
    "risk_penalty",
]

H20_WEIGHTS = {
    "signal_strength": 15,
    "momentum": 15,
    "trend_alignment": 30,
    "breakout_or_position": 5,
    "industry_momentum": 5,
    "relative_strength": 15,
    "real_relative_strength": 10,
    "risk_penalty": 15,
}
H20_DIRECTION = {name: -1 if name == "risk_penalty" else 1 for name in COMPONENTS}


def _spearman(a: pd.Series, b: pd.Series) -> float:
    ra, rb = a.rank(), b.rank()
    val = ra.corr(rb)
    return float(val) if np.isfinite(val) else float("nan")


def _ir(series: pd.Series) -> float:
    clean = pd.Series(series, dtype=float).dropna()
    if len(clean) < 2:
        return float("nan")
    std = clean.std(ddof=1)
    if not np.isfinite(std) or std < 1e-12:
        return float("nan")
    return float(clean.mean() / std * np.sqrt(252 / 20))


def _book_returns(d: pd.DataFrame, book: pd.Series) -> pd.Series:
    use = d.loc[book & d["eligibility"].astype(bool)]
    return use.groupby("date")["fwd"].mean()


def _benchmark(d: pd.DataFrame) -> pd.Series:
    use = d.loc[d["eligibility"].astype(bool)]
    return use.groupby("date")["fwd"].mean()


def audit_window(frame: pd.DataFrame, lo: str, hi: str, label: str) -> None:
    d = frame[
        (frame["date"] >= pd.Timestamp(lo, tz="UTC"))
        & (frame["date"] < pd.Timestamp(hi, tz="UTC"))
    ].copy()
    if d.empty:
        print(f"{label}: no rows")
        return
    d["fwd"] = d["actual_exit_open_hfq"] / d["actual_entry_open_hfq"] - 1.0
    elig = d["eligibility"].astype(bool)
    ndays = d.loc[elig, "date"].nunique()
    bench = _benchmark(d)
    print(f"\n=== {label} ({ndays} eligible days, {len(d)} rows) ===")
    print("  " + f"{'component':22s} {'dailyIC':>9s} {'LSspreadIR':>10s} "
                 f"{'buyTop20IR':>10s} {'buyBot20IR':>10s}")

    for comp in COMPONENTS:
        vals = d[comp]
        ics, ls_excess = [], []
        top_excess, bot_excess = [], []
        for date, g in d.groupby("date"):
            gg = g[[comp, "fwd"]].dropna()
            if len(gg) < 60:
                continue
            ic = _spearman(gg[comp], gg["fwd"])
            if np.isfinite(ic):
                ics.append(ic)
            q_hi, q_lo = gg[comp].quantile(0.8), gg[comp].quantile(0.2)
            if q_hi > q_lo:  # avoid degenerate all-tied days
                top = gg[comp] >= q_hi
                bot = gg[comp] <= q_lo
                if top.any() and bot.any():
                    spread = gg.loc[top, "fwd"].mean() - gg.loc[bot, "fwd"].mean()
                    if np.isfinite(spread):
                        ls_excess.append(spread)
                    top_excess.append(
                        gg.loc[top, "fwd"].mean() - bench.get(date, np.nan)
                    )
                    bot_excess.append(
                        gg.loc[bot, "fwd"].mean() - bench.get(date, np.nan)
                    )
        ic_mean = float(np.mean(ics)) if ics else float("nan")
        print(
            f"  {comp:22s} {ic_mean:+9.4f} "
            f"{_ir(pd.Series(ls_excess)):+10.3f} "
            f"{_ir(pd.Series(top_excess)):+10.3f} "
            f"{_ir(pd.Series(bot_excess)):+10.3f}"
        )

    # Current production composite (positive weight except risk_penalty).
    total_w = sum(H20_WEIGHTS.values())
    comp_score = sum(
        d[c].rank(pct=True) * H20_DIRECTION[c] * H20_WEIGHTS[c] / total_w
        for c in COMPONENTS
    )
    excess = []
    for date in d["date"].unique():
        g = d.loc[d["date"] == date].copy()
        g["cs"] = comp_score.loc[g.index]
        g = g.loc[elig.loc[g.index] & g["cs"].notna()]
        if len(g) >= 60:
            q = g["cs"].quantile(0.8)
            top = g.loc[g["cs"] >= q, "fwd"].mean()
            excess.append(top - bench.get(date, np.nan))
    print(
        "  " + f"{'COMPOSITE(current_h20)':22s} {'':>9s} {'':>10s} "
               f"{_ir(pd.Series(excess)):+10.3f}"
    )


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: h20_component_audit.py SNAPSHOT.parquet")
        return 2
    frame = pd.read_parquet(
        sys.argv[1],
        columns=[
            "date",
            "eligibility",
            "actual_entry_open_hfq",
            "actual_exit_open_hfq",
            *COMPONENTS,
        ],
    )
    frame["date"] = pd.to_datetime(frame["date"], utc=True)

    audit_window(frame, "2019-01-01", "2020-01-01", "REGIME 2019 (recovery)")
    audit_window(frame, "2020-01-01", "2021-01-01", "REGIME 2020 (covid crash+recovery)")
    audit_window(frame, "2021-01-01", "2022-01-01", "REGIME 2021 (structural bull)")
    audit_window(frame, "2022-01-01", "2023-01-01", "REGIME 2022 (bear)")
    audit_window(frame, "2023-01-01", "2024-01-01", "REGIME 2023 (choppy)")
    audit_window(frame, "2024-01-01", "2025-01-01", "REGIME 2024 (mixed+stimulus)")
    audit_window(frame, "2025-01-01", "2026-01-01", "REGIME 2025 (choppy/bear)")
    audit_window(frame, "2026-01-01", "2026-08-01", "REGIME 2026H1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
