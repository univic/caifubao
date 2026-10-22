# -*- coding: utf-8 -*-
"""Composite grid: base exposure x volatility sizing x market gate x drawdown brake."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from engine import simulate
from scan import IS_END, OOS_START, load
import signals as S

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def base_exposures(df):
    out = {
        "bh": pd.Series(1.0, index=df.index),
        "volreg60_500_60": S.vol_regime(df, 60, 500, 0.60),
        "volreg60_500_70": S.vol_regime(df, 60, 500, 0.70),
        "volreg60_500_80": S.vol_regime(df, 60, 500, 0.80),
        "retrev120_20": S.ret_reversal(df, 120, 0.20, 0.05),
        "retrev250_30": S.ret_reversal(df, 250, 0.30, 0.05),
        "volreg70_rev": S.combine(
            S.vol_regime(df, 60, 500, 0.70),
            S.ret_reversal(df, 120, 0.20, 0.05),
            mode="max",
        ),
        "volreg70_revmin": S.combine(
            S.vol_regime(df, 60, 500, 0.70),
            S.ret_reversal(df, 120, 0.20, 0.05),
            mode="min",
        ),
    }
    return out


def build(df, index_frame, base, vol_target, vol_n, gate_on, gate_n):
    expo = base.copy()
    if vol_target is not None:
        expo = S.combine(expo, S.vol_target(df, vol_target, vol_n), mode="min")
    if gate_on:
        expo = S.combine(expo, S.market_gate(df, index_frame, gate_n, "ma"), mode="min")
    return expo


def evaluate(df, index_frame, expo, free, maxd, reset):
    rows = {}
    for label, sl in (
        ("full", df),
        ("is", df.loc[:IS_END]),
        ("oos", df.loc[OOS_START:]),
    ):
        kw = {}
        if free is not None:
            kw = dict(dd_free=free, dd_max=maxd, dd_reset_days=reset)
        r = simulate(sl, expo.reindex(sl.index), **kw)
        rows[label] = r.metrics
    return rows


def main():
    df, index_frame = load()
    bases = base_exposures(df)
    brakes = [(None, None, None), (0.20, 0.40, 30), (0.15, 0.35, 30), (0.25, 0.45, 30)]
    results = []
    for name, base in bases.items():
        for vt, vn in ((None, None), (0.30, 20), (0.40, 20), (0.50, 20), (0.35, 60)):
            for gate_on, gate_n in ((False, 200), (True, 200)):
                expo = (
                    base
                    if (vt is None and not gate_on)
                    else build(df, index_frame, base, vt, vn, gate_on, gate_n)
                )
                for free, maxd, reset in brakes:
                    rows = evaluate(df, index_frame, expo, free, maxd, reset)
                    tag = f"{name}|vt={vt}|gate={gate_on}|brake={free},{maxd}"
                    results.append(
                        {
                            "tag": tag,
                            "base": name,
                            "vol_target": vt,
                            "gate": gate_on,
                            **{
                                f"{k}_{m}": rows[k][m]
                                for k in rows
                                for m in (
                                    "cagr",
                                    "max_drawdown",
                                    "calmar",
                                    "sharpe",
                                    "avg_exposure",
                                    "trades",
                                )
                            },
                        }
                    )
    table = pd.DataFrame(results)
    table.to_csv(OUT / "composite_grid.csv", index=False)
    pd.set_option("display.width", 260)
    cols = [
        "tag",
        "cagr_full",
        "max_drawdown_full",
        "calmar_full",
        "cagr_is",
        "max_drawdown_is",
        "calmar_is",
        "cagr_oos",
        "max_drawdown_oos",
        "calmar_oos",
        "avg_exposure_full",
        "trades_full",
    ]
    print("=== Best Calmar full ===")
    print(
        table.sort_values("calmar_full", ascending=False)
        .head(20)[cols]
        .to_string(index=False, float_format=lambda v: f"{v:.3f}")
    )
    print("\n=== CAGR >= 18% and |MDD| <= 35% ===")
    sel = table[(table.cagr_full >= 0.18) & (table.max_drawdown_full >= -0.35)]
    print(
        sel.sort_values("calmar_full", ascending=False)
        .head(20)[cols]
        .to_string(index=False, float_format=lambda v: f"{v:.3f}")
    )
    print("\n=== Best Calmar OOS (diagnostic only) ===")
    print(
        table.sort_values("calmar_oos", ascending=False)
        .head(10)[cols]
        .to_string(index=False, float_format=lambda v: f"{v:.3f}")
    )


if __name__ == "__main__":
    main()
