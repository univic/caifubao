#!/usr/bin/env python3
"""Blend experiment: does value (EP/BP/low-turnover) add to flip_wide?

flip_wide = all 8 technical components direction-flipped, wide book (top-800
by flipped score). Value factors (EP/BP, size-neutralized) + low turnover
have positive IC across 2019-2026 (see fundamental-factor audit). This script
tests whether adding a value block to the flipped technical score improves
the runner-caliber walk-forward IR.

Blend score per date: (1-w)*flip_tech_rank + w*value_rank, where
  flip_tech_rank = percentile of flipped 8-component weighted score,
  value_rank     = percentile of size-neutral (EP+BP)/2 blended with
                   (1-turnover), daily ranks.
Selection: daily top-800 by blend score, equal-weight, net-of-friction
returns vs same-date eligible equal-weight benchmark (official evaluator
conventions). Windows: train 2019-23 / val 2024 / val 2025 / test 2026H1.

Run:
    PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_value_blend_validate.py \
        --snapshot19 /tmp/h20-2019-2023.parquet \
        --snapshot24 datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet \
        --daily-basic /tmp/daily_basic_all.parquet \
        --profile autoresearch/profile.yaml
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from app.lib.autoresearch import h20_excess_alpha as engine
from app.lib.autoresearch.h20_excess_alpha import load_json_yaml

W = {
    "signal_strength": 15,
    "momentum": 15,
    "trend_alignment": 30,
    "breakout_or_position": 5,
    "industry_momentum": 5,
    "relative_strength": 15,
    "real_relative_strength": 10,
    "risk_penalty": 15,
}
TOTAL_W = sum(W.values())
COMPONENTS = list(W)

RANGES = {
    "train 2019-23": ("2019-01-01", "2024-01-01"),
    "val 2024": ("2024-01-01", "2025-01-01"),
    "val 2025": ("2025-01-01", "2026-01-01"),
    "test 2026H1": ("2026-01-01", "2026-08-01"),
}
BOOK_SIZE = 800


def build_merged(snapshot_paths: list[str], daily_basic_path: str) -> pd.DataFrame:
    frames = [pd.read_parquet(p) for p in snapshot_paths]
    frame = pd.concat(frames, ignore_index=True)
    frame["date"] = pd.to_datetime(frame["date"], utc=True)

    basic = pd.read_parquet(daily_basic_path)
    basic["date"] = pd.to_datetime(basic["date"], utc=True)
    pe = pd.to_numeric(basic["pe_ttm"], errors="coerce")
    pb = pd.to_numeric(basic["pb"], errors="coerce")
    mv = pd.to_numeric(basic["total_mv"], errors="coerce")
    basic["ep"] = np.where(pe > 0, 1.0 / pe, np.nan)
    basic["bp"] = np.where(pb > 0, 1.0 / pb, np.nan)
    basic["log_mv"] = np.log(np.where(mv > 0, mv, np.nan))
    basic["turn"] = pd.to_numeric(basic["turnover_rate"], errors="coerce")
    return frame.merge(
        basic[["code", "date", "ep", "bp", "log_mv", "turn"]],
        left_on=["stock_code", "date"],
        right_on=["code", "date"],
        how="left",
    )


def add_scores(merged: pd.DataFrame) -> pd.DataFrame:
    """Add flipped-tech percentile, size-neutral value rank, per date."""
    d = merged.copy()
    score = pd.Series(0.0, index=d.index)
    for comp in COMPONENTS:
        ranked = d.groupby("date", sort=False)[comp].rank(
            method="average", pct=True, na_option="bottom"
        )
        score += ranked * (-1.0) * W[comp] / TOTAL_W
    d["flip_tech"] = score.groupby(d["date"], sort=False).rank(
        method="average", pct=True
    )

    ep = pd.to_numeric(d["ep"], errors="coerce")
    bp = pd.to_numeric(d["bp"], errors="coerce")
    turn = pd.to_numeric(d["turn"], errors="coerce")
    has_value = d[["ep", "bp"]].notna().all(axis=1)
    d["value_rank"] = np.nan
    d["size_neutral_value"] = np.nan
    for date, gg in d.groupby("date", sort=False):
        sel = gg.loc[has_value & gg["log_mv"].notna()]
        if len(sel) < 60:
            continue
        v = (ep.loc[sel.index].rank(pct=True) + bp.loc[sel.index].rank(pct=True)) / 2
        mv_r = sel["log_mv"].rank()
        if mv_r.std() > 0:
            slope = np.polyfit(mv_r, v, 1)
            resid = pd.Series(v - np.polyval(slope, mv_r), index=sel.index)
        else:
            resid = v
        lt = 1.0 - turn.loc[sel.index].rank(pct=True)
        blended = (resid.rank(pct=True) + lt.rank(pct=True)) / 2
        d.loc[sel.index, "value_rank"] = blended.rank(pct=True)
    return d


def eval_window(
    merged: pd.DataFrame, profile: dict, w_value: float, lo: str, hi: str
) -> float | None:
    dates = pd.to_datetime(merged["date"], utc=True)
    d = merged.loc[
        (dates >= pd.Timestamp(lo, tz="UTC")) & (dates < pd.Timestamp(hi, tz="UTC"))
    ]
    if d.empty:
        return None
    elig = engine.eligible_mask(d)
    tech = d["flip_tech"].fillna(d["flip_tech"].mean())
    value = d["value_rank"].fillna(d["value_rank"].mean())
    d = d.assign(blend=(1.0 - w_value) * tech + w_value * value)

    benchmark = engine.equal_weight_benchmark(d, profile)
    day_frames = []
    for date, g in d.groupby("date", sort=True):
        gg = g.loc[elig & g["blend"].notna()]
        if len(gg) < BOOK_SIZE:
            continue
        day_frames.append(gg.nlargest(BOOK_SIZE, "blend"))
    if not day_frames:
        return None
    positions = pd.concat(day_frames, ignore_index=True)
    pos_ret = engine._net_return(positions, profile)
    strategy = pos_ret.groupby(positions["date"]).mean().sort_index()
    excess = strategy.subtract(benchmark, fill_value=np.nan).dropna()
    return engine.information_ratio(excess)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot19", required=True)
    parser.add_argument("--snapshot24", required=True)
    parser.add_argument("--daily-basic", required=True)
    parser.add_argument("--profile", required=True)
    args = parser.parse_args()

    profile = load_json_yaml(args.profile)
    print("building merged frame with factors...", flush=True)
    merged = build_merged([args.snapshot19, args.snapshot24], args.daily_basic)
    merged = add_scores(merged)
    print(
        f"merged {len(merged)} rows | factor coverage ep {merged['ep'].notna().mean():.3f} "
        f"| value_rank {merged['value_rank'].notna().mean():.3f}",
        flush=True,
    )
    header = f"{'w_value':>9s}" + "".join(f"{name:>12s}" for name in RANGES)
    print(header, flush=True)
    for w_value in (0.0, 0.2, 0.3, 0.5):
        cells = []
        for name in RANGES:
            lo, hi = RANGES[name]
            ir = eval_window(merged, profile, w_value, lo, hi)
            cells.append(f"{ir:>+12.3f}" if ir is not None else f"{'n/a':>12s}")
        print(f"{w_value:>9.1f}" + "".join(cells), flush=True)


if __name__ == "__main__":
    main()
