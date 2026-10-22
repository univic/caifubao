# -*- coding: utf-8 -*-
"""Cross-sectional test: do the timing rules improve *paired* performance?

For every stock in ``data/universe`` the same six rules are applied with the
same engine, costs and dates.  Each rule is then compared with a constant
exposure equal to that rule's own realised average exposure on the same stock
and window, so the comparison isolates timing from position size.

Windows
-------
* in-sample  : 2016-01-01 .. 2021-12-31
* out-of-sample: 2022-01-01 .. 2026-10-21
* full       : 2016-01-01 .. 2026-10-21

The rules' parameters were fixed ex ante from the 000977 study; nothing is
re-fitted per stock.  The universe is a random sample of *currently listed*
A-shares, so survivorship bias remains and the report states it.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine import simulate
from signals import add_features

HERE = Path(__file__).resolve().parent
UNI = HERE / "data" / "universe"
OUT = HERE / "out"

PERIODS = {
    "is": ("2016-01-01", "2021-12-31"),
    "oos": ("2022-01-01", "2026-12-31"),
    "full": ("2016-01-01", "2026-12-31"),
}


def sign_test(k: int, n: int) -> float:
    if n == 0:
        return 1.0
    tail = sum(math.comb(n, i) for i in range(0, min(k, n - k) + 1)) / 2**n
    return min(1.0, 2 * tail)


def build_rules(df: pd.DataFrame, index_frame: pd.DataFrame) -> dict[str, pd.Series]:
    csi = index_frame["close"].reindex(df.index).ffill()
    dd250 = csi / csi.rolling(250).max() - 1.0
    bear = pd.Series(np.where(dd250 < -0.20, 0.0, 1.0), index=df.index)
    half = pd.Series(0.5, index=df.index)
    calm = (
        df["vol60"] <= df["vol60"].rolling(500, min_periods=250).quantile(0.70)
    ).astype(float)
    # oversold state machine: 0.5 base, 1.0 after a 120d fall > 25%, off at +5%
    r120 = df["close"].pct_change(120).to_numpy()
    rev = np.zeros(len(df))
    state = 0
    for i, v in enumerate(r120):
        if not np.isfinite(v):
            continue
        if state == 0 and v < -0.25:
            state = 1
        elif state == 1 and v > 0.05:
            state = 0
        rev[i] = state
    rev = pd.Series(rev, index=df.index)
    return {
        "bear_half": half * (0.5 + 0.5 * bear),
        "bear_zero": half * bear,
        "calm": calm,
        "trend_ma200": (df["close"] > df["ma200"]).astype(float),
        "oversold": 0.5 + 0.5 * rev,
        "composite": calm * (0.5 + 0.5 * rev) * (0.5 + 0.5 * bear),
    }


def metrics(equity: pd.Series) -> dict:
    rets = equity.pct_change().dropna()
    years = len(equity) / 244.0
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1 if years > 0 else 0.0
    dd = float((equity / equity.cummax() - 1).min())
    vol = float(rets.std() * np.sqrt(244))
    sharpe = float(rets.mean() / rets.std() * np.sqrt(244)) if rets.std() > 0 else 0.0
    return {
        "cagr": cagr,
        "mdd": dd,
        "calmar": cagr / abs(dd) if dd < 0 else 0.0,
        "sharpe": sharpe,
        "vol": vol,
    }


def main() -> int:
    index_frame = (
        pd.read_parquet(HERE / "data" / "csi300.parquet").set_index("date").sort_index()
    )
    files = sorted(UNI.glob("*.parquet"))
    rows = []
    for path in files:
        raw = pd.read_parquet(path).set_index("date").sort_index()
        if len(raw) < 800:
            continue
        limit = 0.21 if path.stem.startswith(("30", "68")) else 0.11
        if (raw["close"].pct_change().dropna().abs() > limit).sum() > 5:
            continue  # same quality gate as universe_fetch
        df = add_features(raw)
        warm = df.index[min(500, len(df) - 1)]
        rules = build_rules(df, index_frame)
        for rule, expo in rules.items():
            for period, (start, end) in PERIODS.items():
                begin = max(pd.Timestamp(start), warm)
                sl = df.loc[begin:end]
                if len(sl) < 500:
                    continue
                e = expo.reindex(sl.index).fillna(0.0)
                res = simulate(sl, e, band=0.05)
                avg = float(res.metrics["avg_exposure"])
                bench = simulate(sl, pd.Series(avg, index=sl.index), band=0.05)
                bh = simulate(sl, pd.Series(1.0, index=sl.index), band=0.05)
                rm, bm, hm = res.metrics, bench.metrics, bh.metrics
                rows.append(
                    {
                        "code": path.stem,
                        "rule": rule,
                        "period": period,
                        "bars": len(sl),
                        "avg_exposure": avg,
                        "cagr": rm["cagr"],
                        "mdd": rm["max_drawdown"],
                        "calmar": rm["calmar"],
                        "sharpe": rm["sharpe"],
                        "bench_cagr": bm["cagr"],
                        "bench_mdd": bm["max_drawdown"],
                        "bench_calmar": bm["calmar"],
                        "bench_sharpe": bm["sharpe"],
                        "bh_cagr": hm["cagr"],
                        "bh_mdd": hm["max_drawdown"],
                        "d_cagr": rm["cagr"] - bm["cagr"],
                        "d_mdd": rm["max_drawdown"] - bm["max_drawdown"],
                        "d_calmar": rm["calmar"] - bm["calmar"],
                        "d_sharpe": rm["sharpe"] - bm["sharpe"],
                    }
                )
    table = pd.DataFrame(rows)
    table.to_csv(OUT / "cross_section.csv", index=False)

    agg = []
    for (rule, period), g in table.groupby(["rule", "period"]):
        n = len(g)
        wins = int((g.d_calmar > 0).sum())
        agg.append(
            {
                "rule": rule,
                "period": period,
                "n": n,
                "median_cagr": g.cagr.median(),
                "median_mdd": g.mdd.median(),
                "median_calmar": g.calmar.median(),
                "median_bench_cagr": g.bench_cagr.median(),
                "median_bench_mdd": g.bench_mdd.median(),
                "median_bench_calmar": g.bench_calmar.median(),
                "median_d_cagr": g.d_cagr.median(),
                "median_d_mdd": g.d_mdd.median(),
                "median_d_calmar": g.d_calmar.median(),
                "median_d_sharpe": g.d_sharpe.median(),
                "win_calmar": wins / n,
                "win_cagr": float((g.d_cagr > 0).mean()),
                "win_mdd": float((g.d_mdd > 0).mean()),
                "sign_p_calmar": sign_test(wins, n),
            }
        )
    summary = pd.DataFrame(agg).sort_values(
        ["period", "median_d_calmar"], ascending=[True, False]
    )
    summary.to_csv(OUT / "cross_section_summary.csv", index=False)
    (OUT / "cross_section_meta.json").write_text(
        json.dumps(
            {"stocks": int(table.code.nunique()), "rows": int(len(table))}, indent=2
        ),
        encoding="utf-8",
    )

    pd.set_option("display.width", 250)
    print(f"stocks={table.code.nunique()} rows={len(table)}")
    cols = [
        "rule",
        "period",
        "n",
        "median_bench_cagr",
        "median_bench_mdd",
        "median_cagr",
        "median_mdd",
        "median_d_cagr",
        "median_d_mdd",
        "median_d_calmar",
        "win_cagr",
        "win_calmar",
        "sign_p_calmar",
    ]
    print(summary[cols].to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    # figure: distribution of paired d_calmar by rule, OOS
    order = ["bear_half", "bear_zero", "calm", "trend_ma200", "oversold", "composite"]
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.6), sharey=True)
    for ax, period in zip(axes, ("is", "oos")):
        sub = table[table.period == period]
        data = [sub[sub.rule == r].d_calmar.dropna().to_numpy() for r in order]
        ax.boxplot(data, tick_labels=order, showfliers=False)
        ax.axhline(0, color="black", lw=0.7)
        ax.set_title(f"{period.upper()}: paired Calmar change vs matched constant")
        ax.tick_params(axis="x", rotation=25, labelsize=8)
    axes[0].set_ylabel("rule minus matched constant (Calmar)")
    fig.suptitle("Do the rules improve paired performance across A-shares?")
    fig.savefig(HERE / "figures" / "fig9_cross_section.png")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
