# -*- coding: utf-8 -*-
"""Unified out-of-sample comparison for the 000977 timing rule.

Every curve starts from the same date and passes through the same cost engine,
so the only difference is the exposure path.  Benchmarks answer the question the
timing claim has to survive:

* constant exposure at several levels, including one matched to the strategy's
  own average exposure (the natural "no timing" control);
* a volatility-target buy-and-hold matched to the strategy's realised risk;
* a block-bootstrap of the strategy's own exposure path (same level and
  autocorrelation, random alignment) as the skill placebo.
"""

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

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
FIG = HERE / "figures"

START = "2011-01-04"


def metrics(r) -> dict:
    m = r.metrics
    return {
        "cagr": m["cagr"],
        "max_drawdown": m["max_drawdown"],
        "calmar": m["calmar"],
        "sharpe": m["sharpe"],
        "volatility": m["volatility"],
        "avg_exposure": m["avg_exposure"],
        "trades": m["trades"],
    }


def curve_from_returns(rets: pd.Series) -> pd.Series:
    return (1 + rets).cumprod()


def main() -> int:
    df, index_frame = load()
    sub = df.loc[START:]
    wf_expo = pd.read_csv(OUT / "wf_final_exposure.csv", index_col=0, parse_dates=True)[
        "exposure"
    ]
    wf_expo = wf_expo.reindex(sub.index).ffill().fillna(0.0)

    curves: dict[str, pd.Series] = {}
    rows: list[dict] = []

    def add(name: str, exposure: pd.Series, kind: str):
        res = simulate(sub, exposure, band=0.05)
        curves[name] = res.equity / res.equity.iloc[0]
        rows.append({"name": name, "kind": kind, **metrics(res)})

    add("Buy & hold", pd.Series(1.0, index=sub.index), "benchmark")
    for level in (0.60, 0.40, 0.30):
        add(f"Constant {level:.0%}", pd.Series(level, index=sub.index), "constant")
    strat_avg = float(wf_expo.mean())
    add(
        f"Constant {strat_avg:.0%} (matched)",
        pd.Series(strat_avg, index=sub.index),
        "constant",
    )
    for target in (0.20, 0.25):
        vt = np.minimum(1.0, target / sub["vol20"].to_numpy())
        vt = pd.Series(np.where(np.isfinite(vt), vt, 0.0), index=sub.index)
        add(f"Vol target {target:.0%}", vt, "vol_target")
    add("Timing (walk-forward)", wf_expo, "strategy")

    table = pd.DataFrame(rows).sort_values("kind")
    table.to_csv(OUT / "benchmark_oos.csv", index=False)

    # ---------------------------------------------------------------- placebo
    rng = np.random.default_rng(19)
    base_ret = sub["close"].pct_change().fillna(0.0).to_numpy()
    expo = wf_expo.to_numpy()
    n = len(expo)
    block = 20
    n_paths = 1500
    cagrs, mdds = [], []
    for _ in range(n_paths):
        starts = rng.integers(0, max(n - block - 1, 1), size=n // block + 2)
        path = np.concatenate([expo[s : s + block] for s in starts])[:n]
        daily = path * base_ret
        curve = np.cumprod(1 + daily)
        cagrs.append(curve[-1] ** (244 / n) - 1)
        mdds.append(float(np.min(curve / np.maximum.accumulate(curve) - 1)))
    cagrs = np.array(cagrs)
    strat = next(r for r in rows if r["name"] == "Timing (walk-forward)")
    placebo = {
        "paths": n_paths,
        "cagr_p05": float(np.percentile(cagrs, 5)),
        "cagr_p50": float(np.percentile(cagrs, 50)),
        "cagr_p95": float(np.percentile(cagrs, 95)),
        "strategy_cagr": float(strat["cagr"]),
        "strategy_percentile": float((cagrs < strat["cagr"]).mean()),
        "mdd_p50": float(np.percentile(mdds, 50)),
    }

    # ---------------------------------------------------------------- alpha
    bh_ret = sub["close"].pct_change().fillna(0.0)
    st = simulate(sub, wf_expo, band=0.05)
    st_ret = st.equity.pct_change().fillna(0.0)
    beta = float(np.cov(st_ret, bh_ret)[0, 1] / np.var(bh_ret, ddof=1))
    alpha = float(st_ret.mean() - beta * bh_ret.mean()) * 244
    timing = {
        "beta_to_buy_hold": beta,
        "annualised_alpha_vs_buy_hold": alpha,
        "strategy_avg_exposure": strat_avg,
        "cagr_gap_vs_matched_constant": float(
            strat["cagr"]
            - table.loc[table["name"].str.startswith("Constant"), "cagr"].iloc[-1]
        ),
        "calmar_gap_vs_buy_hold": float(
            strat["calmar"] - table.loc[table["name"] == "Buy & hold", "calmar"].iloc[0]
        ),
        "maxdd_gap_vs_buy_hold": float(
            strat["max_drawdown"]
            - table.loc[table["name"] == "Buy & hold", "max_drawdown"].iloc[0]
        ),
    }

    summary = {
        "start": START,
        "end": str(sub.index[-1].date()),
        "placebo": placebo,
        "timing": timing,
    }
    (OUT / "benchmark_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # ---------------------------------------------------------------- chart
    fig, ax = plt.subplots(figsize=(10.5, 5.2))
    colors = {
        "Buy & hold": "#2c3e50",
        "Constant 60%": "#95a5a6",
        "Constant 40%": "#bdc3c7",
        "Constant 30%": "#d5dbdb",
        f"Constant {strat_avg:.0%} (matched)": "#7f8c8d",
        "Vol target 20%": "#8e44ad",
        "Vol target 25%": "#9b59b6",
        "Timing (walk-forward)": "#c0392b",
    }
    for name, curve in curves.items():
        ax.semilogy(
            curve.index,
            curve,
            label=name,
            color=colors.get(name, "#333"),
            lw=1.8 if "Timing" in name else 1.0,
        )
    ax.set_title(f"Unified comparison from {START} — same engine, same costs")
    ax.set_ylabel("NAV (log, start = 1)")
    ax.legend(frameon=False, ncol=2)
    fig.savefig(FIG / "fig6_benchmark.png")
    plt.close(fig)

    pd.set_option("display.width", 200)
    print(
        table[
            [
                "name",
                "kind",
                "cagr",
                "max_drawdown",
                "calmar",
                "sharpe",
                "volatility",
                "avg_exposure",
            ]
        ].to_string(index=False, float_format=lambda v: f"{v:.3f}")
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
