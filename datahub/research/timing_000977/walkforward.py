# -*- coding: utf-8 -*-
"""Anchored walk-forward: re-select one config per block using history only.

Two selection rules are run side by side so the report can show that the
conclusion does not depend on the objective:

* ``dd_budget`` — highest historical CAGR among configs whose historical
  max drawdown stayed within the budget (default 35%);
* ``calmar`` — highest historical CAGR / |MaxDD|.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from engine import simulate
from scan import load
from strategy import ExposureBuilder, candidates

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def stitch(pieces: list[pd.Series]) -> pd.Series:
    level = 1.0
    out = []
    for piece in pieces:
        seg = piece / piece.iloc[0] * level
        out.append(seg)
        level = float(seg.iloc[-1])
    curve = pd.concat(out)
    return curve[~curve.index.duplicated(keep="last")]


def run(df, index_frame, blocks, rule, dd_budget=0.35, min_history_days=900):
    builder = ExposureBuilder(df, index_frame)
    cands = candidates()
    exposures = {cfg.label: builder.build(cfg) for cfg in cands}
    rows = []
    pieces = []
    for start, end in blocks:
        history = df.loc[: start - pd.Timedelta(days=1)]
        if len(history) < min_history_days:
            continue
        scored = []
        for cfg in cands:
            expo = exposures[cfg.label].reindex(history.index)
            m = simulate(history, expo, band=cfg.band).metrics
            scored.append((cfg, m))
        if rule == "dd_budget":
            feasible = [x for x in scored if x[1]["max_drawdown"] >= -dd_budget]
            pool = feasible or scored
            best_cfg, best_m = max(pool, key=lambda x: x[1]["cagr"])
        else:
            best_cfg, best_m = max(scored, key=lambda x: x[1]["calmar"])
        seg_df = df.loc[start:end]
        res = simulate(
            seg_df, exposures[best_cfg.label].reindex(seg_df.index), band=best_cfg.band
        )
        pieces.append(res.equity)
        rows.append(
            {
                "block": f"{start.date()}..{end.date()}",
                "config": best_cfg.label,
                "is_cagr": best_m["cagr"],
                "is_mdd": best_m["max_drawdown"],
                "is_calmar": best_m["calmar"],
                "block_return": float(res.equity.iloc[-1] / res.equity.iloc[0] - 1.0),
                "block_mdd": res.metrics["max_drawdown"],
                "block_avg_exposure": res.metrics["avg_exposure"],
            }
        )
    if not pieces:
        return None, pd.DataFrame(rows)
    return stitch(pieces), pd.DataFrame(rows)


def block_list(df: pd.DataFrame, first_year: int = 2011, years: int = 1):
    blocks = []
    start = pd.Timestamp(f"{first_year}-01-01")
    end_limit = df.index[-1]
    while start <= end_limit:
        end = min(start + pd.DateOffset(years=years) - pd.Timedelta(days=1), end_limit)
        blocks.append((start, end))
        start = end + pd.Timedelta(days=1)
    return blocks


def curve_metrics(curve: pd.Series) -> dict:
    from engine import BacktestResult, performance

    empty = pd.DataFrame(
        columns=["date", "side", "price", "shares", "notional", "cost"]
    )
    res = BacktestResult(
        equity=curve, exposure=pd.Series(1.0, index=curve.index), trades=empty
    )
    return performance(results=res)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, default=1)
    args = parser.parse_args()
    df, index_frame = load()
    blocks = block_list(df, 2011, args.years)
    summary = {}
    for rule in ("dd_budget", "calmar"):
        curve, choices = run(df, index_frame, blocks, rule)
        if curve is None:
            continue
        curve.to_frame("equity").to_csv(OUT / f"wf_{rule}_equity.csv")
        choices.to_csv(OUT / f"wf_{rule}_choices.csv", index=False)
        m = curve_metrics(curve)
        bh = simulate(
            df.loc[curve.index[0] : curve.index[-1]],
            pd.Series(1.0, index=curve.loc[curve.index[0] :].index),
        )
        summary[rule] = {
            "start": str(curve.index[0].date()),
            "end": str(curve.index[-1].date()),
            "metrics": m,
            "buy_hold": bh.metrics,
        }
        print(f"\n=== rule={rule} ===")
        print(
            f"  WF   CAGR={m['cagr']:.2%} MaxDD={m['max_drawdown']:.2%} Calmar={m['calmar']:.2f} Sharpe={m['sharpe']:.2f}"
        )
        print(
            f"  B&H  CAGR={bh.metrics['cagr']:.2%} MaxDD={bh.metrics['max_drawdown']:.2%} Calmar={bh.metrics['calmar']:.2f} Sharpe={bh.metrics['sharpe']:.2f}"
        )
        print(
            choices[["block", "config", "block_return", "block_mdd"]].to_string(
                index=False
            )
        )
    (OUT / "walkforward_summary.json").write_text(
        json.dumps(summary, indent=2, default=float)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
