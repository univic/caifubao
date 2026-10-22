# -*- coding: utf-8 -*-
"""Sector layer study.

Part 1 — sector rotation: monthly rank of the 10 CSI sector indices by momentum,
low volatility or drawdown reversal, top-3 equal weight vs the equal-weight
benchmark, with 10bp per side and an IS/OOS split.

Part 2 — sector overlay on 000977: does conditioning the existing market+stock
strategy on the information-sector state add anything?  Each gate is compared
with the same base scaled to the gate's average exposure, so the test isolates
the *timing* of the sector gate rather than the de-risking.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine import simulate
from scan import load
from strategy import FINAL, ExposureBuilder

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
DATA = HERE / "data"

IS = ("2010-01-01", "2018-12-31")
OOS = ("2019-01-01", "2026-12-31")


def perf(monthly: pd.Series) -> dict:
    eq = (1 + monthly).cumprod()
    years = len(monthly) / 12
    cagr = eq.iloc[-1] ** (1 / years) - 1
    dd = (eq / eq.cummax() - 1).min()
    return {
        "cagr": cagr,
        "mdd": dd,
        "calmar": cagr / abs(dd) if dd < 0 else 0.0,
        "sharpe": monthly.mean() / monthly.std() * np.sqrt(12)
        if monthly.std() > 0
        else 0.0,
    }


def rotation(
    px: pd.DataFrame, signal_fn, top: int = 3, cost: float = 0.001
) -> pd.Series:
    monthly = px.resample("ME").last()
    rets = monthly.pct_change()
    sig = signal_fn(px)
    weights = pd.DataFrame(0.0, index=monthly.index, columns=monthly.columns)
    for i in range(len(monthly) - 1):
        s = sig.iloc[i].dropna()
        if s.empty:
            continue
        pick = s.nlargest(top).index
        weights.loc[monthly.index[i], pick] = 1.0 / len(pick)
    port = (weights.shift(1) * rets).sum(axis=1)
    turnover = weights.diff().abs().sum(axis=1).shift(1).fillna(0.0)
    return (port - turnover * cost).dropna()


def equal_weight(px: pd.DataFrame) -> pd.Series:
    monthly = px.resample("ME").last()
    rets = monthly.pct_change().mean(axis=1)
    return rets.dropna()


def main() -> int:
    px = pd.read_parquet(DATA / "sectors" / "panel.parquet")
    signals = {
        "mom120": lambda p: p.pct_change(120).resample("ME").last(),
        "mom250": lambda p: p.pct_change(250).resample("ME").last(),
        "lowvol60": lambda p: -p.pct_change().rolling(60).std().resample("ME").last(),
        "reversal250": lambda p: -(p / p.rolling(250).max() - 1).resample("ME").last(),
    }
    rows = []
    curves = {"Equal weight": equal_weight(px)}
    for name, fn in signals.items():
        curves[f"Top3 {name}"] = rotation(px, fn)
    for name, series in curves.items():
        for label, (start, end) in (
            ("is", IS),
            ("oos", OOS),
            ("full", ("2010-01-01", "2026-12-31")),
        ):
            sub = series.loc[start:end]
            if len(sub) < 24:
                continue
            rows.append(
                {
                    "strategy": name,
                    "period": label,
                    **{k: round(v, 4) for k, v in perf(sub).items()},
                }
            )
    table = pd.DataFrame(rows)
    table.to_csv(OUT / "sector_rotation.csv", index=False)
    pd.set_option("display.width", 220)
    print("=== sector rotation (monthly, 10bp/side) ===")
    print(table[table.period == "full"].to_string(index=False))

    # ---------------------------------------------------------------- part 2
    df, index_frame = load()
    builder = ExposureBuilder(df, index_frame)
    base = builder.build(FINAL)
    sec = (
        pd.read_parquet(DATA / "sectors" / "000935.parquet")
        .set_index("date")["close"]
        .reindex(df.index)
        .ffill()
    )
    dd250 = sec / sec.rolling(250).max() - 1.0
    gates = {
        "sector_dd15": pd.Series(np.where(dd250 < -0.15, 0.5, 1.0), index=df.index),
        "sector_dd20": pd.Series(np.where(dd250 < -0.20, 0.5, 1.0), index=df.index),
        "sector_mom120": pd.Series(
            np.where(sec.pct_change(120) < 0, 0.5, 1.0), index=df.index
        ),
        "sector_vol_high": pd.Series(
            np.where(
                sec.pct_change().rolling(60).std()
                > sec.pct_change()
                .rolling(60)
                .std()
                .rolling(500, min_periods=250)
                .quantile(0.70),
                0.5,
                1.0,
            ),
            index=df.index,
        ),
    }
    rows2 = []
    for label, (start, end) in (
        ("is", ("2005-01-01", "2016-12-31")),
        ("oos", ("2017-01-01", "2026-12-31")),
        ("full", ("2005-01-01", "2026-12-31")),
    ):
        sub = df.loc[start:end]
        b = simulate(sub, base.reindex(sub.index), band=0.05)
        for name, gate in gates.items():
            g = gate.reindex(sub.index)
            gated = base.reindex(sub.index) * g
            rg = simulate(sub, gated, band=0.05)
            avg_g = float(rg.metrics["avg_exposure"])
            scale = (
                avg_g / float(b.metrics["avg_exposure"])
                if b.metrics["avg_exposure"] > 0
                else 1.0
            )
            rb = simulate(sub, (base.reindex(sub.index) * scale).clip(0, 1), band=0.05)
            rows2.append(
                {
                    "period": label,
                    "variant": name,
                    "cagr": rg.metrics["cagr"],
                    "mdd": rg.metrics["max_drawdown"],
                    "calmar": rg.metrics["calmar"],
                    "matched_cagr": rb.metrics["cagr"],
                    "matched_mdd": rb.metrics["max_drawdown"],
                    "matched_calmar": rb.metrics["calmar"],
                    "d_cagr": rg.metrics["cagr"] - rb.metrics["cagr"],
                    "d_calmar": rg.metrics["calmar"] - rb.metrics["calmar"],
                    "avg_exposure": avg_g,
                }
            )
        rows2.append(
            {
                "period": label,
                "variant": "base",
                "cagr": b.metrics["cagr"],
                "mdd": b.metrics["max_drawdown"],
                "calmar": b.metrics["calmar"],
                "matched_cagr": b.metrics["cagr"],
                "matched_mdd": b.metrics["max_drawdown"],
                "matched_calmar": b.metrics["calmar"],
                "d_cagr": 0.0,
                "d_calmar": 0.0,
                "avg_exposure": b.metrics["avg_exposure"],
            }
        )
    table2 = pd.DataFrame(rows2)
    table2.to_csv(OUT / "sector_overlay_000977.csv", index=False)
    print(
        "\n=== sector overlay on 000977 (vs same base scaled to the gate's average exposure) ==="
    )
    print(table2.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    fig, ax = plt.subplots(figsize=(10.5, 4.6))
    for name, series in curves.items():
        ax.plot(
            series.index,
            (1 + series).cumprod(),
            label=name,
            lw=1.4 if "Top3" not in name else 0.9,
        )
    ax.set_title("Sector rotation: top-3 monthly vs equal weight (10bp/side)")
    ax.set_ylabel("NAV (start=1)")
    ax.legend(frameon=False, fontsize=7)
    fig.savefig(HERE / "figures" / "fig10_sector_rotation.png")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
