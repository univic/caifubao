#!/usr/bin/env python3
"""H20 fundamental-factor (daily_basic) audit across regimes.

Joins the frozen H20 snapshot (T+1 forward 20-session returns, eligibility,
existing components) with stock_daily_basic valuation data, then reports per
regime window for each candidate fundamental factor:

- daily_ic        : mean daily cross-sectional Spearman IC of the factor
                    against the forward return.
- ic_neutral_size : same IC after cross-sectional residualizing the factor's
                    daily rank on log(total_mv) rank (size-neutral check —
                    A-share "value" factors are frequently size proxies).
- ls_spread_ir    : excess IR of long-short (top quintile - bottom quintile).
- buyTop20IR      : excess IR of buying the top-quintile book vs same-date
                    eligible equal-weight benchmark.
- buyBot20IR      : same for the bottom quintile.
- corr_logmv      : mean daily Spearman corr of the factor with log(total_mv)
                    (how strongly the factor loads on size).

Factors (point-in-time by trade_date, NaN/negative handled per design doc):
    ep_ttm  = 1/pe_ttm      (pe_ttm > 0; loss-makers excluded)
    bp      = 1/pb          (pb > 0)
    sp_ttm  = 1/ps_ttm      (ps_ttm > 0)
    dv_ttm  = dividend yield ttm (raw %)
    log_mv  = log(total_mv) (万元; monotonic in size)
    turnover= turnover_rate (raw %)
Value composite (design doc §三): equal-weight rank blend of ep_ttm/bp/dv_ttm,
then size-neutral IC reported alongside.

Forward return uses the snapshot's actual_entry_open_hfq / actual_exit_open_hfq
(T+1 entry, 20-session hold, roll-forward resolved). Research-only.

Run:
    PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_fundamental_audit.py \
        --snapshot19 /tmp/h20-2019-2023.parquet \
        --snapshot24 datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet \
        --daily-basic-dir /tmp/db_local
"""

from __future__ import annotations

import argparse
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=RuntimeWarning)

REGIMES = [
    ("REGIME 2019 (recovery)", "2019-01-01", "2020-01-01"),
    ("REGIME 2020 (covid crash+recovery)", "2020-01-01", "2021-01-01"),
    ("REGIME 2021 (structural bull)", "2021-01-01", "2022-01-01"),
    ("REGIME 2022 (bear)", "2022-01-01", "2023-01-01"),
    ("REGIME 2023 (choppy)", "2023-01-01", "2024-01-01"),
    ("REGIME 2024 (mixed+stimulus)", "2024-01-01", "2025-01-01"),
    ("REGIME 2025 (bear/chop)", "2025-01-01", "2026-01-01"),
    ("REGIME 2026H1", "2026-01-01", "2026-08-01"),
]

# Daily cross-sectional winsorize bounds for valuation ratios (design doc:
# exclude extreme values; log_mv/turnover kept raw).
WINSOR_LO, WINSOR_HI = 0.01, 0.99


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


def _daily_winsor(s: pd.Series) -> pd.Series:
    clean = pd.to_numeric(s, errors="coerce").dropna()
    if clean.empty:
        return s
    lo = float(clean.quantile(WINSOR_LO))
    hi = float(clean.quantile(WINSOR_HI))
    if not (np.isfinite(lo) and np.isfinite(hi)) or hi <= lo:
        return s
    return pd.Series(np.clip(clean.to_numpy(), lo, hi), index=clean.index)


def build_factors(frame: pd.DataFrame) -> pd.DataFrame:
    """Return canonical factor columns from a daily_basic frame."""
    out = pd.DataFrame(index=frame.index)
    out["code"] = frame["code"]
    out["date"] = frame["date"]
    pe = pd.to_numeric(frame["pe_ttm"], errors="coerce")
    pb = pd.to_numeric(frame["pb"], errors="coerce")
    ps = pd.to_numeric(frame["ps_ttm"], errors="coerce")
    dv = pd.to_numeric(frame["dv_ttm"], errors="coerce")
    mv = pd.to_numeric(frame["total_mv"], errors="coerce")
    turn = pd.to_numeric(frame["turnover_rate"], errors="coerce")

    out["ep_ttm"] = np.where(pe > 0, 1.0 / pe, np.nan)
    out["bp"] = np.where(pb > 0, 1.0 / pb, np.nan)
    out["sp_ttm"] = np.where(ps > 0, 1.0 / ps, np.nan)
    out["dv_ttm"] = np.where(dv > 0, dv, np.nan)
    out["log_mv"] = np.log(np.where(mv > 0, mv, np.nan))
    out["turnover"] = np.where(turn >= 0, turn, np.nan)

    # Value composite: equal-weight daily rank blend (only rows with all three).
    ep_r = out["ep_ttm"].rank(pct=True)
    bp_r = out["bp"].rank(pct=True)
    dv_r = out["dv_ttm"].rank(pct=True)
    composite = (ep_r + bp_r + dv_r) / 3.0
    out["value_composite"] = composite.where(
        out[["ep_ttm", "bp", "dv_ttm"]].notna().all(axis=1)
    )
    return out


FACTOR_COLUMNS = [
    "ep_ttm",
    "bp",
    "sp_ttm",
    "dv_ttm",
    "log_mv",
    "turnover",
    "value_composite",
]


def audit_window(joined: pd.DataFrame, lo: str, hi: str, label: str) -> None:
    d = joined[
        (joined["date"] >= pd.Timestamp(lo, tz="UTC"))
        & (joined["date"] < pd.Timestamp(hi, tz="UTC"))
    ].copy()
    elig = d["eligibility"].astype(bool)
    if elig.sum() == 0:
        print(f"{label}: no eligible rows")
        return
    ndays = d.loc[elig, "date"].nunique()
    print(f"\n=== {label} ({ndays} eligible days, {len(d)} joined rows) ===")
    print(
        "  " + f"{'factor':16s} {'dailyIC':>8s} {'IC_sizeN':>8s} {'LSspread':>9s} "
        f"{'top20IR':>8s} {'bot20IR':>8s} {'corr_mv':>8s}"
    )

    bench = d.loc[elig].groupby("date")["fwd"].mean()

    for factor in FACTOR_COLUMNS:
        ics, ics_neutral, ls_excess, top_excess, bot_excess, mv_corrs = (
            [],
            [],
            [],
            [],
            [],
            [],
        )
        for date, g in d.groupby("date"):
            select_cols = list(dict.fromkeys([factor, "fwd", "eligibility", "log_mv"]))
            gg = g[select_cols].dropna(subset=[factor, "fwd"])
            gg = gg[gg["eligibility"].astype(bool)]
            if len(gg) < 60:
                continue
            vals = _daily_winsor(gg[factor])
            ic = _spearman(vals, gg["fwd"])
            if np.isfinite(ic):
                ics.append(ic)
            # size-neutral IC: residualize daily rank of factor on log_mv rank
            if "log_mv" in gg and gg["log_mv"].notna().sum() >= 60:
                mv_rank = gg["log_mv"].rank()
                f_rank = vals.rank()
                if mv_rank.std() > 0:
                    slope = np.polyfit(mv_rank, f_rank, 1)
                    resid = f_rank - np.polyval(slope, mv_rank)
                    ic_n = _spearman(pd.Series(resid, index=gg.index), gg["fwd"])
                    if np.isfinite(ic_n):
                        ics_neutral.append(ic_n)
                    mv_corr = _spearman(vals, gg["log_mv"])
                    if np.isfinite(mv_corr):
                        mv_corrs.append(mv_corr)
            q_hi = vals.quantile(0.8)
            q_lo = vals.quantile(0.2)
            if q_hi > q_lo:
                top = vals >= q_hi
                bot = vals <= q_lo
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

        def _mean(x):
            return float(np.mean(x)) if x else float("nan")

        print(
            f"  {factor:16s} {_mean(ics):+8.4f} {_mean(ics_neutral):+8.4f} "
            f"{_ir(pd.Series(ls_excess)):+9.2f} {_ir(pd.Series(top_excess)):+8.2f} "
            f"{_ir(pd.Series(bot_excess)):+8.2f} {_mean(mv_corrs):+8.3f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot19", required=True, help="2019-2023 snapshot parquet"
    )
    parser.add_argument(
        "--snapshot24", required=True, help="2024-2026 snapshot parquet"
    )
    parser.add_argument(
        "--daily-basic-dir",
        required=True,
        help="dir of daily_basic_YYYYMM.parquet OR a single merged parquet file",
    )
    args = parser.parse_args()

    frames = []
    for path in [args.snapshot19, args.snapshot24]:
        frames.append(pd.read_parquet(path))
    snap = pd.concat(frames, ignore_index=True)
    snap["date"] = pd.to_datetime(snap["date"], utc=True)
    snap["fwd"] = snap["actual_exit_open_hfq"] / snap["actual_entry_open_hfq"] - 1.0
    print(
        f"snapshot rows: {len(snap)} | range {snap['date'].min()} .. {snap['date'].max()}"
    )

    import glob
    import os

    basic_paths: list[str]
    if os.path.isdir(args.daily_basic_dir):
        basic_paths = sorted(
            glob.glob(os.path.join(args.daily_basic_dir, "daily_basic_*.parquet"))
        )
    else:
        basic_paths = [args.daily_basic_dir]
    basic_frames = []
    for path in basic_paths:
        basic_frames.append(
            pd.read_parquet(
                path,
                columns=[
                    "code",
                    "date",
                    "pe_ttm",
                    "pb",
                    "ps_ttm",
                    "dv_ttm",
                    "total_mv",
                    "turnover_rate",
                ],
            )
        )
    basic = pd.concat(basic_frames, ignore_index=True)
    basic["date"] = pd.to_datetime(basic["date"], utc=True)
    print(
        f"daily_basic rows: {len(basic)} | range {basic['date'].min()} .. {basic['date'].max()}"
    )

    factors = build_factors(basic)
    factors["date"] = pd.to_datetime(factors["date"], utc=True)

    joined = snap.merge(
        factors,
        left_on=["stock_code", "date"],
        right_on=["code", "date"],
        how="left",
        suffixes=("", "_factor"),
    )
    print(
        f"joined rows: {len(joined)} | factor coverage: {joined[['ep_ttm']].notna().mean().iloc[0]:.3f}"
    )

    for label, lo, hi in REGIMES:
        audit_window(joined, lo, hi, label)


if __name__ == "__main__":
    main()
