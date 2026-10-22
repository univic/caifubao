# -*- coding: utf-8 -*-
"""Run the single-asset timing scans and dump machine-readable results."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from engine import DEFAULT_COSTS, CostModel, simulate
from signals import add_features, registry

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

IS_END = "2016-12-31"
OOS_START = "2017-01-01"


def load() -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = (
        pd.read_parquet(DATA / "000977_panel.parquet").set_index("date").sort_index()
    )
    frame = frame[~frame.index.duplicated()]
    df = add_features(frame)
    index_frame = (
        pd.read_parquet(DATA / "csi300.parquet").set_index("date").sort_index()
    )
    return df, index_frame


def run_one(df, spec, start=None, end=None, costs: CostModel = DEFAULT_COSTS):
    expo = spec.build(df, **spec.params)
    if start is not None or end is not None:
        mask = (df.index >= (start or "1900-01-01")) & (
            df.index <= (end or "2999-12-31")
        )
        df_slice = df.loc[mask]
        expo_slice = expo.reindex(df_slice.index)
    else:
        df_slice, expo_slice = df, expo
    if len(df_slice) < 30:
        return None
    return simulate(df_slice, expo_slice, costs=costs)


def scan(
    df, specs, start=None, end=None, costs: CostModel = DEFAULT_COSTS
) -> pd.DataFrame:
    rows = []
    for spec in specs:
        res = run_one(df, spec, start, end, costs)
        if res is None:
            continue
        row = {
            "strategy": spec.name,
            **{
                k: res.metrics[k]
                for k in (
                    "cagr",
                    "max_drawdown",
                    "calmar",
                    "volatility",
                    "sharpe",
                    "sortino",
                    "avg_exposure",
                    "time_in_market",
                    "trades",
                    "total_return",
                )
            },
        }
        rows.append(row)
    return (
        pd.DataFrame(rows).sort_values("calmar", ascending=False).reset_index(drop=True)
    )


def walk_forward(
    df, specs, select_from="2010", block_years=2, metric="calmar", min_history=4
):
    """Expanding-window selection: choose on history, trade the next block."""
    starts = pd.date_range(
        f"{select_from}-01-01", df.index[-1], freq=f"{block_years}YS"
    )
    pieces = []
    choices = []
    for i, start in enumerate(starts):
        end = (
            starts[i + 1] - pd.Timedelta(days=1)
            if i + 1 < len(starts)
            else df.index[-1]
        )
        history_end = start - pd.Timedelta(days=1)
        if (history_end - df.index[0]).days < min_history * 365:
            continue
        table = scan(df, specs, end=history_end)
        if table.empty:
            continue
        best = table.iloc[0]
        spec = next(s for s in specs if s.name == best["strategy"])
        res = run_one(df, spec, start, end)
        if res is None or len(res.equity) < 10:
            continue
        choices.append(
            {
                "block": f"{start.date()}..{end.date()}",
                "chosen": spec.name,
                "params": spec.params,
                "is_calmar": float(best["calmar"]),
                "is_cagr": float(best["cagr"]),
                "block_return": float(res.equity.iloc[-1] / res.equity.iloc[0] - 1),
                "block_mdd": float(res.metrics["max_drawdown"]),
            }
        )
        eq = res.equity / res.equity.iloc[0]
        pieces.append(eq)
    if not pieces:
        return None, pd.DataFrame(choices)
    # Stitch blocks multiplicatively.
    stitched = []
    level = 1.0
    for piece in pieces:
        seg = piece * level
        stitched.append(seg)
        level = float(seg.iloc[-1])
    curve = pd.concat(stitched)
    curve = curve[~curve.index.duplicated(keep="last")]
    return curve, pd.DataFrame(choices)


def metrics_from_curve(curve: pd.Series) -> dict:
    from engine import BacktestResult, performance

    empty = pd.DataFrame(
        columns=["date", "side", "price", "shares", "notional", "cost"]
    )
    res = BacktestResult(
        equity=curve, exposure=pd.Series(1.0, index=curve.index), trades=empty
    )
    return performance(results=res)


def summarize(df, specs, label, start=None, end=None) -> pd.DataFrame:
    table = scan(df, specs, start, end)
    table.insert(0, "window", label)
    return table


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--walk-forward", action="store_true")
    args = parser.parse_args()

    df, index_frame = load()
    specs = registry()
    print(
        f"bars={len(df)} strategies={len(specs)} range={df.index[0].date()}..{df.index[-1].date()}"
    )

    bh_full = simulate(df, pd.Series(1.0, index=df.index))
    bh_is = simulate(df.loc[:IS_END], pd.Series(1.0, index=df.loc[:IS_END].index))
    bh_oos = simulate(
        df.loc[OOS_START:], pd.Series(1.0, index=df.loc[OOS_START:].index)
    )
    pd.DataFrame(
        [
            {"window": "full", **bh_full.metrics},
            {"window": "is", **bh_is.metrics},
            {"window": "oos", **bh_oos.metrics},
        ]
    ).to_csv(OUT / "buy_hold.csv", index=False)
    print("\nBuy & hold:")
    for w, r in (("full", bh_full), ("is", bh_is), ("oos", bh_oos)):
        m = r.metrics
        print(
            f"  {w:4s} CAGR={m['cagr']:.2%} MaxDD={m['max_drawdown']:.2%} Calmar={m['calmar']:.2f} Sharpe={m['sharpe']:.2f}"
        )

    full = summarize(df, specs, "full")
    ins = summarize(df, specs, "is", end=IS_END)
    oos = summarize(df, specs, "oos", start=OOS_START)
    full.to_csv(OUT / "scan_full.csv", index=False)
    ins.to_csv(OUT / "scan_is.csv", index=False)
    oos.to_csv(OUT / "scan_oos.csv", index=False)

    merged = ins[
        [
            "strategy",
            "cagr",
            "max_drawdown",
            "calmar",
            "sharpe",
            "avg_exposure",
            "trades",
        ]
    ].merge(
        oos[
            [
                "strategy",
                "cagr",
                "max_drawdown",
                "calmar",
                "sharpe",
                "avg_exposure",
                "trades",
            ]
        ],
        on="strategy",
        suffixes=("_is", "_oos"),
    )
    merged["rank_is"] = merged["calmar_is"].rank(ascending=False)
    merged["rank_oos"] = merged["calmar_oos"].rank(ascending=False)
    merged["rank_corr"] = merged["rank_is"].rank().corr(merged["rank_oos"].rank())
    merged.to_csv(OUT / "is_oos_merged.csv", index=False)
    print(f"\nIS/OOS Calmar rank correlation: {merged['rank_corr'].iloc[0]:.2f}")
    print("\nTop 15 by in-sample Calmar:")
    cols = [
        "strategy",
        "cagr_is",
        "max_drawdown_is",
        "calmar_is",
        "rank_is",
        "cagr_oos",
        "max_drawdown_oos",
        "calmar_oos",
        "rank_oos",
    ]
    print(
        merged.head(15)[cols].to_string(index=False, float_format=lambda v: f"{v:.3f}")
    )

    if args.walk_forward:
        curve, choices = walk_forward(df, specs)
        if curve is not None:
            curve.to_frame("equity").to_csv(OUT / "walk_forward_equity.csv")
            choices.to_csv(OUT / "walk_forward_choices.csv", index=False)
            m = metrics_from_curve(curve)
            print("\nWalk-forward stitched OOS:")
            print(
                f"  CAGR={m['cagr']:.2%} MaxDD={m['max_drawdown']:.2%} Calmar={m['calmar']:.2f}"
            )
            print(choices.to_string(index=False))
            (OUT / "walk_forward_metrics.json").write_text(
                json.dumps(m, indent=2, default=float)
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
