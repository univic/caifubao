# -*- coding: utf-8 -*-
"""Build the charts and the headline metrics for the 000977 timing report."""

from __future__ import annotations

import json
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
FIG = HERE / "figures"
FIG.mkdir(exist_ok=True)

plt.rcParams.update(
    {
        "figure.dpi": 130,
        "font.size": 9,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "savefig.bbox": "tight",
    }
)

from strategy import FINAL as BALANCED  # noqa: E402


def drawdown(curve: pd.Series) -> pd.Series:
    return curve / curve.cummax() - 1.0


def main() -> int:
    df, index_frame = load()
    builder = ExposureBuilder(df, index_frame)
    expo = builder.build(BALANCED)

    res = simulate(df, expo, band=BALANCED.band)
    bh = simulate(df, pd.Series(1.0, index=df.index), band=0.05)
    half = simulate(df, expo * 0.75, band=BALANCED.band)
    wf = pd.read_csv(OUT / "wf_final_top1.csv", index_col=0, parse_dates=True)["equity"]

    # ---------------------------------------------------------------- fig 1
    fig, (ax, ax2) = plt.subplots(
        2, 1, figsize=(10.5, 7.2), sharex=True, gridspec_kw={"height_ratios": [2.1, 1]}
    )
    ax.semilogy(
        res.equity.index,
        res.equity / res.equity.iloc[0],
        label="Timing (1.0x)",
        color="#c0392b",
        lw=1.4,
    )
    ax.semilogy(
        half.equity.index,
        half.equity / half.equity.iloc[0],
        label="Timing (0.75x)",
        color="#e67e22",
        lw=1.1,
    )
    ax.semilogy(
        bh.equity.index,
        bh.equity / bh.equity.iloc[0],
        label="Buy & hold",
        color="#2c3e50",
        lw=1.2,
        alpha=0.85,
    )
    ax.semilogy(
        wf.index,
        wf / wf.iloc[0],
        label="Walk-forward (OOS)",
        color="#2980b9",
        lw=1.1,
        ls="--",
    )
    ax.set_ylabel("NAV (log, start = 1)")
    ax.set_title(
        "000977 Inspur Information — timing vs buy & hold, 2005-2026 (after costs)"
    )
    ax.legend(loc="upper left", frameon=False)
    ax2.fill_between(
        res.equity.index,
        drawdown(res.equity) * 100,
        0,
        color="#c0392b",
        alpha=0.45,
        label="Timing",
    )
    ax2.plot(
        bh.equity.index,
        drawdown(bh.equity) * 100,
        color="#2c3e50",
        lw=0.9,
        alpha=0.8,
        label="Buy & hold",
    )
    ax2.set_ylabel("Drawdown (%)")
    ax2.legend(loc="lower left", frameon=False, ncol=2)
    fig.savefig(FIG / "fig1_equity_drawdown.png")
    plt.close(fig)

    # ---------------------------------------------------------------- fig 2
    fig, ax = plt.subplots(figsize=(10.5, 4.4))
    ax.semilogy(df.index, df["close"], color="#2c3e50", lw=0.9, label="Close (HFQ)")
    ax.set_ylabel("Price (CNY, log)")
    axb = ax.twinx()
    axb.fill_between(expo.index, expo * 100, 0, color="#c0392b", alpha=0.25, step="mid")
    axb.set_ylabel("Target exposure (%)")
    axb.set_ylim(0, 105)
    axb.grid(False)
    ax.set_title(
        "Exposure rule: calm-vol hold, oversold boost, market bear override, vol target"
    )
    ax.legend(loc="upper left", frameon=False)
    fig.savefig(FIG / "fig2_exposure.png")
    plt.close(fig)

    # ---------------------------------------------------------------- fig 3
    yr = pd.DataFrame(
        {
            "Timing": res.yearly["return"],
            "Buy & hold": bh.yearly["return"],
        }
    )
    ax = yr.plot(
        kind="bar", figsize=(10.5, 3.8), color=["#c0392b", "#2c3e50"], width=0.8
    )
    ax.set_ylabel("Annual return")
    ax.set_title("Annual returns: timing vs buy & hold")
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax.legend(frameon=False)
    ax.set_xlabel("")
    fig = ax.get_figure()
    fig.savefig(FIG / "fig3_yearly.png")
    plt.close(fig)

    # ---------------------------------------------------------------- fig 4
    from engine import simulate as sim

    qs = [0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
    blends = [0.0, 0.2, 0.3, 0.4, 0.5, 0.6]
    grid_full = np.zeros((len(qs), len(blends)))
    grid_oos = np.zeros((len(qs), len(blends)))
    for i, q in enumerate(qs):
        for j, w in enumerate(blends):
            cfg = Config(calm_q=q, rev_entry=0.25 if w > 0 else None, blend=w)
            e = builder.build(cfg)
            grid_full[i, j] = sim(df, e, band=0.05).metrics["calmar"]
            sl = df.loc["2017-01-01":]
            grid_oos[i, j] = sim(sl, e.reindex(sl.index), band=0.05).metrics["calmar"]

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.9))
    for ax, grid, title in (
        (axes[0], grid_full, "Full sample 2005-2026"),
        (axes[1], grid_oos, "Out-of-sample 2017-2026"),
    ):
        im = ax.imshow(grid, cmap="RdYlGn", aspect="auto", origin="lower")
        ax.set_xticks(range(len(blends)), [f"{b:.1f}" for b in blends])
        ax.set_yticks(range(len(qs)), [f"{q:.2f}" for q in qs])
        ax.set_xlabel("reversal blend weight")
        ax.set_ylabel("calm-vol quantile")
        ax.set_title(f"Calmar — {title}")
        ax.grid(False)
        for i in range(len(qs)):
            for j in range(len(blends)):
                ax.text(
                    j, i, f"{grid[i, j]:.2f}", ha="center", va="center", fontsize=6.5
                )
    fig.colorbar(im, ax=axes, shrink=0.85)
    fig.savefig(FIG / "fig4_heatmap.png")
    plt.close(fig)

    # ---------------------------------------------------------------- summary
    def metrics(r):
        return {
            k: r.metrics[k]
            for k in (
                "cagr",
                "max_drawdown",
                "calmar",
                "sharpe",
                "volatility",
                "avg_exposure",
                "trades",
            )
        }

    summary = {
        "balance": BALANCED.label,
        "full": metrics(res),
        "sd_is": metrics(
            simulate(
                df.loc[:"2016-12-31"],
                expo.reindex(df.loc[:"2016-12-31"].index),
                band=0.05,
            )
        ),
        "sd_oos": metrics(
            simulate(
                df.loc["2017-01-01":],
                expo.reindex(df.loc["2017-01-01":].index),
                band=0.05,
            )
        ),
        "from_2011": metrics(
            simulate(
                df.loc["2011-01-01":],
                expo.reindex(df.loc["2011-01-01":].index),
                band=0.05,
            )
        ),
        "buy_hold": metrics(bh),
        "scale_075": metrics(half),
        "wf": {
            "start": str(wf.index[0].date()),
            "end": str(wf.index[-1].date()),
            "cagr": float((wf.iloc[-1] / wf.iloc[0]) ** (244 / len(wf)) - 1),
            "max_drawdown": float(drawdown(wf).min()),
        },
        "current": {
            "date": str(df.index[-1].date()),
            "close": float(df["close"].iloc[-1]),
            "ma200": float(df["ma200"].iloc[-1]),
            "ret120": float(df["close"].pct_change(120).iloc[-1]),
            "calm_gate": int(builder.calm(60, 500, 0.70).iloc[-1]),
            "reversal_gate": int(builder.rev(120, 0.25, 0.05).iloc[-1]),
            "csi_dd250": float(builder.csi_dd250.iloc[-1]),
            "override": float(
                builder.override(BALANCED.override_thr, BALANCED.override_scale).iloc[
                    -1
                ]
            ),
            "target_exposure": float(expo.iloc[-1]),
        },
    }
    (OUT / "headline.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
