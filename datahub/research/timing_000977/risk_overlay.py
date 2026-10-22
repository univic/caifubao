# -*- coding: utf-8 -*-
"""Risk-management study: bear de-risking, volatility control, equity-curve control.

Two bases are used so the overlays can be judged both as pure risk management
(buy & hold) and as an incremental layer on the timing strategy (core):

* ``bh``   — 100% stock;
* ``core`` — the timing base without the market-bear override
  (``calm x (0.5 + 0.5 x oversold)``), i.e. the strategy when its only
  risk-control module is switched off.

Each overlay is applied causally and executed on the next open by the same
engine.  Extreme-loss metrics go beyond max drawdown: 95% CVaR, worst year,
longest underwater spell and the Ulcer index.
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
from strategy import Config, ExposureBuilder

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def drawdown(curve: pd.Series) -> pd.Series:
    return curve / curve.cummax() - 1.0


def extreme_metrics(equity: pd.Series) -> dict:
    rets = equity.pct_change().dropna()
    dd = drawdown(equity)
    underwater = (dd < 0).astype(int)
    longest = 0
    run = 0
    for flag in underwater:
        run = run + 1 if flag else 0
        longest = max(longest, run)
    yearly = equity.resample("YE").last().pct_change().dropna()
    cvar = float(rets[rets <= rets.quantile(0.05)].mean()) if len(rets) > 50 else 0.0
    return {
        "cvar95_daily": cvar,
        "worst_year": float(yearly.min()) if len(yearly) else 0.0,
        "longest_underwater_days": int(longest),
        "ulcer": float(np.sqrt(np.mean(dd.to_numpy() ** 2))),
    }


def bear_overlays(df, index_frame):
    csi = index_frame["close"].reindex(df.index).ffill()
    dd250 = csi / csi.rolling(250).max() - 1.0
    out = {}
    for thr, scale in ((0.15, 0.5), (0.20, 0.5), (0.25, 0.5), (0.20, 0.0)):
        name = f"csi_dd{int(thr * 100)}_x{int(scale * 10)}"
        out[name] = pd.Series(np.where(dd250 < -thr, scale, 1.0), index=df.index)
    for n in (60, 200):
        ma = csi.rolling(n, min_periods=n).mean()
        out[f"csi_ma{n}"] = pd.Series((csi > ma).astype(float), index=df.index)
    double = ((df["close"] < df["ma200"]) & (csi < csi.rolling(200).mean())).astype(
        float
    )
    out["double_bear"] = pd.Series(np.where(double > 0, 0.0, 1.0), index=df.index)
    fast = (csi.pct_change(20) < -0.10).astype(float)
    out["csi_fast20"] = pd.Series(np.where(fast > 0, 0.0, 1.0), index=df.index)
    return out


def vol_overlays(df):
    out = {}
    for target in (0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45):
        vol = df["vol20"].to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            expo = np.where(
                np.isfinite(vol) & (vol > 0), np.minimum(1.0, target / vol), 0.0
            )
        out[f"vol_target{int(target * 100)}"] = pd.Series(expo, index=df.index)
    for q in (0.60, 0.70, 0.80):
        thresh = df["vol60"].rolling(500, min_periods=250).quantile(q)
        out[f"vol_regime{int(q * 100)}"] = pd.Series(
            (df["vol60"] <= thresh).astype(float), index=df.index
        )
    return out


def main() -> int:
    df, index_frame = load()
    builder = ExposureBuilder(df, index_frame)
    bases = {
        "bh": pd.Series(1.0, index=df.index),
        "core": builder.build(
            Config(calm_q=0.70, rev_entry=0.25, blend=0.50, override_thr=None)
        ),
    }
    bear = bear_overlays(df, index_frame)
    vol = vol_overlays(df)

    engine_overlays = {
        "dd_soft_10_60": dict(dd_free=0.10, dd_max=0.60, dd_reset_days=40),
        "dd_soft_15_60": dict(dd_free=0.15, dd_max=0.60, dd_reset_days=40),
        "dd_hard_15_35": dict(dd_free=0.15, dd_max=0.35, dd_reset_days=20),
        "dd_hard_20_40": dict(dd_free=0.20, dd_max=0.40, dd_reset_days=20),
        "cppi_75_2": dict(cppi_floor=0.75, cppi_mult=2.0),
        "eqma_150": dict(equity_ma=150),
    }

    combos = {
        "bh": {
            "vol_regime70+csi_dd20_x0": lambda b: pd.concat(
                [b, vol["vol_regime70"], bear["csi_dd20_x0"]], axis=1
            ).min(axis=1),
            **{
                f"combo+vt{int(t * 100)}": (
                    lambda b, t=t: pd.concat(
                        [
                            b,
                            vol["vol_regime70"],
                            bear["csi_dd20_x0"],
                            vol[f"vol_target{int(t * 100)}"],
                        ],
                        axis=1,
                    ).min(axis=1)
                )
                for t in (0.25, 0.30, 0.35, 0.40, 0.45)
            },
        },
        "core": {
            "csi_dd20_x0+vol_regime60": lambda b: pd.concat(
                [b, bear["csi_dd20_x0"], vol["vol_regime60"]], axis=1
            ).min(axis=1),
            "csi_dd20_x0+vol_target30": lambda b: pd.concat(
                [b, bear["csi_dd20_x0"], vol["vol_target30"]], axis=1
            ).min(axis=1),
            "csi_dd20_x0+dd_soft_10_60": None,
        },
    }

    rows = []
    for base_name, base_expo in bases.items():
        base = simulate(df, base_expo, band=0.05)
        base_m = base.metrics
        rows.append(
            {
                "base": base_name,
                "overlay": "none",
                "family": "base",
                "cagr": base_m["cagr"],
                "max_drawdown": base_m["max_drawdown"],
                "calmar": base_m["calmar"],
                "sharpe": base_m["sharpe"],
                "avg_exposure": base_m["avg_exposure"],
                **extreme_metrics(base.equity),
            }
        )
        for family, overlays in (("bear", bear), ("vol", vol)):
            for name, ov in overlays.items():
                expo = pd.concat([base_expo, ov], axis=1).min(axis=1)
                res = simulate(df, expo, band=0.05)
                m = res.metrics
                rows.append(
                    {
                        "base": base_name,
                        "overlay": name,
                        "family": family,
                        "cagr": m["cagr"],
                        "max_drawdown": m["max_drawdown"],
                        "calmar": m["calmar"],
                        "sharpe": m["sharpe"],
                        "avg_exposure": m["avg_exposure"],
                        **extreme_metrics(res.equity),
                    }
                )
        for name, kwargs in engine_overlays.items():
            res = simulate(df, base_expo, band=0.05, **kwargs)
            m = res.metrics
            rows.append(
                {
                    "base": base_name,
                    "overlay": name,
                    "family": "dd_control",
                    "cagr": m["cagr"],
                    "max_drawdown": m["max_drawdown"],
                    "calmar": m["calmar"],
                    "sharpe": m["sharpe"],
                    "avg_exposure": m["avg_exposure"],
                    **extreme_metrics(res.equity),
                }
            )

        for name, fn in combos.get(base_name, {}).items():
            if fn is None:
                continue
            expo = fn(base_expo)
            res = simulate(df, expo, band=0.05)
            m = res.metrics
            rows.append(
                {
                    "base": base_name,
                    "overlay": name,
                    "family": "combo",
                    "cagr": m["cagr"],
                    "max_drawdown": m["max_drawdown"],
                    "calmar": m["calmar"],
                    "sharpe": m["sharpe"],
                    "avg_exposure": m["avg_exposure"],
                    **extreme_metrics(res.equity),
                }
            )

    table = pd.DataFrame(rows)
    base_mdd = table[table["overlay"] == "none"].set_index("base")["max_drawdown"]
    table["mdd_reduction"] = table.apply(
        lambda r: r["max_drawdown"] - base_mdd[r["base"]], axis=1
    )
    table.to_csv(OUT / "risk_overlays.csv", index=False)

    pd.set_option("display.width", 240)
    cols = [
        "base",
        "family",
        "overlay",
        "cagr",
        "max_drawdown",
        "calmar",
        "cvar95_daily",
        "worst_year",
        "longest_underwater_days",
        "ulcer",
        "avg_exposure",
    ]
    for base_name in ("bh", "core"):
        sub = table[table.base == base_name].sort_values("calmar", ascending=False)
        print(f"\n=== base={base_name} ===")
        print(sub[cols].to_string(index=False, float_format=lambda v: f"{v:.3f}"))
    table.to_json(OUT / "risk_overlays.json", orient="records", indent=2)

    colors = {
        "base": "#2c3e50",
        "bear": "#c0392b",
        "vol": "#2980b9",
        "dd_control": "#8e44ad",
        "combo": "#27ae60",
    }
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8))
    for ax, base_name in zip(axes, ("bh", "core")):
        sub = table[table.base == base_name]
        for family, color in colors.items():
            part = sub[sub.family == family]
            if part.empty:
                continue
            ax.scatter(
                part.max_drawdown * 100,
                part.cagr * 100,
                s=30,
                color=color,
                label=family,
                alpha=0.85,
            )
        for _, row in sub.iterrows():
            if row.overlay in (
                "none",
                "vol_regime70+csi_dd20_x0",
                "csi_dd20_x0+vol_regime60",
                "csi_dd20_x0",
                "vol_target30",
                "cppi_75_2",
            ):
                ax.annotate(
                    row.overlay,
                    (row.max_drawdown * 100, row.cagr * 100),
                    fontsize=6.5,
                    xytext=(3, 3),
                    textcoords="offset points",
                )
        ax.set_xlabel("Max drawdown (%)")
        ax.set_ylabel("CAGR (%)")
        ax.set_title(f"base = {base_name}")
        ax.legend(frameon=False, fontsize=7)
    fig.suptitle("Risk overlay frontier: each point is one overlay on the same base")
    fig.savefig(HERE / "figures" / "fig7_risk_frontier.png")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
