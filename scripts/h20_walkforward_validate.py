#!/usr/bin/env python3
"""H20 construction-flip candidates: cross-regime walk-forward validation.

Evaluates direction-flipped candidates at multiple selection widths on the
merged 2019-2026 snapshot using the evaluator's official conventions (rank ->
selection -> net-of-friction returns vs same-date eligible equal-weight
benchmark, IR x sqrt(252/20), walk-forward decay between train and
validation).

Findings context: the component audit shows all 8 components are
anti-predictive across 2019-2026; construction-layer direction flip is the
fix candidate. This script shows selection width is decisive: the evaluator's
default top-5%/30-stock book has too much noise; a wide book that excludes
the flipped-score bottom quintile (~original low-momentum stocks) is the only
candidate positive in every regime.

Run:
    PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_walkforward_validate.py \
        --snapshot19 /tmp/h20-2019-2023.parquet \
        --snapshot24 datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet \
        --candidates datahub/research/autoresearch/h20_excess_alpha \
        --profile autoresearch/profile.yaml
"""

from __future__ import annotations

import argparse
import glob
import os

import numpy as np
import pandas as pd

from app.lib.autoresearch import h20_excess_alpha as engine
from app.lib.autoresearch.h20_excess_alpha import load_json_yaml

RANGES = {
    "train 2019-23": ("2019-01-01", "2024-01-01"),
    "val 2024": ("2024-01-01", "2025-01-01"),
    "val 2025": ("2025-01-01", "2026-01-01"),
    "test 2026H1": ("2026-01-01", "2026-08-01"),
}
RANGES_ORDER = list(RANGES)


def eval_range(
    frame: pd.DataFrame,
    candidate: dict,
    profile: dict,
    lo: str,
    hi: str,
) -> float | None:
    dates = pd.to_datetime(frame["date"], utc=True)
    subset = frame.loc[
        (dates >= pd.Timestamp(lo, tz="UTC")) & (dates < pd.Timestamp(hi, tz="UTC"))
    ].copy()
    if subset.empty:
        return None
    scored = engine.rank_components(subset, candidate)
    positions = engine.build_positions(scored, candidate, profile)
    benchmark = engine.equal_weight_benchmark(subset, profile)
    position_returns = engine._net_return(positions, profile)
    strategy = position_returns.groupby(positions["date"]).mean().sort_index()
    excess = strategy.subtract(benchmark, fill_value=np.nan).dropna()
    return engine.information_ratio(excess)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot19", required=True)
    parser.add_argument("--snapshot24", required=True)
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--profile", required=True)
    args = parser.parse_args()

    frames = [pd.read_parquet(args.snapshot19), pd.read_parquet(args.snapshot24)]
    frame = pd.concat(frames, ignore_index=True)
    frame["date"] = pd.to_datetime(frame["date"], utc=True)
    profile = load_json_yaml(args.profile)
    print(
        f"snapshot: {len(frame)} rows | {frame['date'].min().date()}..{frame['date'].max().date()}"
    )

    header = f"{'candidate':>20s}" + "".join(f"{name:>12s}" for name in RANGES_ORDER)
    print(header)
    for path in sorted(glob.glob(os.path.join(args.candidates, "*.yaml"))):
        candidate = load_json_yaml(path)
        cells = []
        for name in RANGES_ORDER:
            lo, hi = RANGES[name]
            ir = eval_range(frame, candidate, profile, lo, hi)
            cells.append(f"{ir:>+12.3f}" if ir is not None else f"{'n/a':>12s}")
        print(f"{candidate['name']:>20s}" + "".join(cells))


if __name__ == "__main__":
    main()
