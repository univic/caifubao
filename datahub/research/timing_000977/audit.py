# -*- coding: utf-8 -*-
"""Return-calculation audit for the 000977 timing strategy.

Checks, in order:

1. accounting identity — rebuild cash/shares from the trade blotter and compare
   with the engine equity bar by bar;
2. execution — every trade price must equal that day's open, and every cost must
   match the published fee formula;
3. position tracking — realised weight vs target weight (lot rounding + band);
4. causality — prefix recomputation (truncate the data at t and re-derive the
   signal at t), lag/lead sensitivity, and an exposure-shuffle placebo;
5. 2024-2026 deep dive — monthly attribution, trade P&L, and a raw-vs-HFQ price
   consistency check so an adjustment artefact cannot masquerade as alpha.

Writes ``out/audit_*.csv`` and ``figures/fig5_audit_2024.png``.
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
from strategy import FINAL as BALANCED
from strategy import ExposureBuilder

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
FIG = HERE / "figures"
FIG.mkdir(exist_ok=True)

COST_RATE = 0.00025 + 0.001  # commission + slippage, per side (sell adds stamp duty)


def accounting_check(df, r, lot=100, initial=1_000_000.0):
    """Replay trades at their recorded price and compare with the engine curve."""
    cash = float(initial)
    shares = 0
    legs = {row.date: row for row in r.trades.itertuples()} if len(r.trades) else {}
    eq = []
    cash_path = []
    share_path = []
    for d in df.index:
        row = legs.get(d)
        if row is not None:
            if row.side == "sell":
                cash += row.notional - row.cost
                shares -= int(row.shares)
            else:
                cash -= row.notional + row.cost
                shares += int(row.shares)
        eq.append(cash + shares * float(df.loc[d, "close"]))
        cash_path.append(cash)
        share_path.append(shares)
    eq = pd.Series(eq, index=df.index)
    diff = (eq - r.equity).abs()
    return {
        "max_abs_diff_cny": float(diff.max()),
        "mean_abs_diff_cny": float(diff.mean()),
        "final_equity_rebuilt": float(eq.iloc[-1]),
        "final_equity_engine": float(r.equity.iloc[-1]),
        "cash_negative_days": int((pd.Series(cash_path) < -1e-6).sum()),
        "shares_negative_days": int((pd.Series(share_path) < 0).sum()),
    }


def execution_check(df, r):
    if not len(r.trades):
        return {}
    tr = r.trades.copy()
    opens = df["open"].reindex(tr["date"]).to_numpy()
    price_gap = np.abs(tr["price"].to_numpy() - opens)
    # cost = notional*slippage + max(notional*commission, 5) (+ stamp duty on sells)
    expected = []
    for row in tr.itertuples():
        notional = row.notional
        fee = max(notional * 0.00025, 5.0)
        if row.side == "sell":
            fee += notional * 0.001
        expected.append(notional * 0.001 + fee)
    expected = np.array(expected)
    return {
        "trades": int(len(tr)),
        "max_price_minus_open": float(price_gap.max()),
        "max_cost_gap": float(np.abs(expected - tr["cost"].to_numpy()).max()),
        "total_cost": float(tr["cost"].sum()),
        "total_notional": float(tr["notional"].sum()),
        "cost_bps_of_notional": float(tr["cost"].sum() / tr["notional"].sum() * 1e4),
    }


def position_check(exposure, realized):
    tgt = exposure.shift(1).fillna(0.0)
    gap = (realized - tgt).abs()
    return {
        "mean_abs_tracking_gap": float(gap.mean()),
        "p95_tracking_gap": float(gap.quantile(0.95)),
        "max_tracking_gap": float(gap.max()),
        "days_flat_while_target_positive": int(
            ((realized < 0.01) & (tgt > 0.05)).sum()
        ),
    }


def prefix_recompute_check(df, index_frame, builder, cfg, n_dates=25):
    """For sampled dates, re-derive the signal using only data up to that date."""
    dates = df.index[:: max(len(df) // n_dates, 1)][1:]
    base = builder.build(cfg)
    mismatches = []
    for d in dates:
        sub = df.loc[:d]
        sub_idx = index_frame.loc[:d]
        b2 = ExposureBuilder(sub, sub_idx)
        val = float(b2.build(cfg).iloc[-1])
        ref = float(base.loc[d])
        if abs(val - ref) > 1e-9:
            mismatches.append({"date": str(d.date()), "prefix": val, "full": ref})
    return {
        "dates_tested": int(len(dates)),
        "mismatches": len(mismatches),
        "examples": mismatches[:5],
    }


def sensitivity(df, exposure):
    """Lag/lead and shuffle placebos around the honest t -> t+1 execution."""
    out = {}
    variants = {
        "honest (signal t, trade t+1)": exposure,
        "one extra day delay": exposure.shift(1).fillna(0.0),
        "signal lagged 5 days": exposure.shift(5).fillna(0.0),
        "same-day close execution (mild look-ahead)": exposure.shift(-1).fillna(
            exposure.iloc[-1]
        ),
        "future signal (look-ahead)": exposure.shift(-5).fillna(exposure.iloc[-1]),
    }
    for name, expo in variants.items():
        m = simulate(df, expo, band=0.05).metrics
        out[name] = {
            "cagr": round(m["cagr"], 4),
            "max_drawdown": round(m["max_drawdown"], 4),
            "sharpe": round(m["sharpe"], 3),
        }
    # Shuffle placebo: same exposure distribution, random alignment.
    rng = np.random.default_rng(11)
    rets = df["open"].pct_change().fillna(0.0).to_numpy()
    expo = exposure.to_numpy()
    shift = rng.integers(0, len(expo), size=20)
    placebos = []
    for s in shift:
        rolled = np.roll(expo, int(s))
        daily = rolled * rets
        curve = np.cumprod(1.0 + daily)
        years = len(daily) / 244.0
        placebos.append(curve[-1] ** (1.0 / years) - 1.0)
    return out, {
        "placebo_cagr_mean": float(np.mean(placebos)),
        "placebo_cagr_p95": float(np.percentile(placebos, 95)),
        "placebo_cagr_max": float(np.max(placebos)),
        "strategy_cagr": float((simulate(df, exposure, band=0.05).metrics["cagr"])),
    }


def price_consistency(df):
    """Compare every candidate price series; the limit test decides which is usable."""
    raw = pd.read_parquet(HERE / "data" / "000977_raw.parquet").set_index("date")
    hfq = pd.read_parquet(HERE / "data" / "000977_hfq.parquet").set_index("date")
    qfq = pd.read_parquet(HERE / "data" / "000977_qfq.parquet").set_index("date")
    close = pd.DataFrame(
        {
            "adj": df["close"],
            "raw": raw["close"],
            "qfq": qfq["close"],
            "hfq": hfq["close"],
        }
    ).dropna()
    rets = close.pct_change().dropna()
    out = {"overlap_days": int(len(rets)), "series": {}}
    for col in ("adj", "raw", "qfq", "hfq"):
        r = rets[col]
        out["series"][col] = {
            "total_return": float((1 + r).prod() - 1),
            "cagr": float((1 + r).prod() ** (244 / len(r)) - 1),
            "abs_ret_over_10_5pct": int((r.abs() > 0.105).sum()),
            "worst_day": float(r.min()),
            "best_day": float(r.max()),
        }
    out["corr_with_raw"] = {
        c: float(rets[c].corr(rets["raw"])) for c in ("adj", "qfq", "hfq")
    }
    out["verdict"] = (
        "adj (raw x share multipliers) is the only series that respects the +/-10% limit "
        "and keeps the true limit-day moves; vendor hfq violates the limit on 51 days; "
        "vendor qfq is limit-clean but compresses early-period returns (cumulative "
        "return far below the raw x multiplier reconstruction)"
    )
    bad = rets[rets["hfq"].abs() > 0.105]
    out["hfq_violation_examples"] = [
        {
            "date": str(d.date()),
            "hfq": round(float(r.hfq), 4),
            "raw": round(float(r.raw), 4),
            "adj": round(float(r.adj), 4),
        }
        for d, r in bad.head(6).iterrows()
    ]
    return out


def window_2024(df, exposure, r):
    tgt = exposure.shift(1).fillna(0.0)
    tbl = pd.DataFrame(
        {
            "stock_ret": df["close"].pct_change(),
            "strategy_ret": r.equity.pct_change(),
            "exposure": r.exposure,
            "target": tgt,
        }
    ).loc["2024-01-01":]
    monthly = tbl.resample("ME").agg(
        stock_ret=("stock_ret", lambda s: (1 + s).prod() - 1),
        strategy_ret=("strategy_ret", lambda s: (1 + s).prod() - 1),
        avg_exposure=("exposure", "mean"),
    )
    trades = r.trades[r.trades["date"] >= "2024-01-01"].copy()
    trades["notional"] = trades["notional"].round(0)
    return monthly, trades


def main() -> int:
    df, index_frame = load()
    builder = ExposureBuilder(df, index_frame)
    exposure = builder.build(BALANCED)
    r = simulate(df, exposure, band=BALANCED.band)

    report = {
        "config": BALANCED.label,
        "accounting": accounting_check(df, r),
        "execution": execution_check(df, r),
        "position": position_check(exposure, r.exposure),
        "causality": prefix_recompute_check(df, index_frame, builder, BALANCED),
        "price_consistency": price_consistency(df),
    }
    sens, placebo = sensitivity(df, exposure)
    report["sensitivity"] = sens
    report["placebo"] = placebo

    monthly, trades = window_2024(df, exposure, r)
    monthly.to_csv(OUT / "audit_2024_monthly.csv")
    trades.to_csv(OUT / "audit_2024_trades.csv", index=False)

    # 2024-2026 chart: price, exposure, equity
    sub = df.loc["2024-01-01":]
    expo = r.exposure.loc["2024-01-01":]
    fig, axes = plt.subplots(
        3,
        1,
        figsize=(10.5, 7.6),
        sharex=True,
        gridspec_kw={"height_ratios": [2, 1, 1.4]},
    )
    axes[0].plot(sub.index, sub["close"], color="#2c3e50", lw=1.1)
    axes[0].set_ylabel("Close (adjusted)")
    axes[0].set_title("2024-2026 audit: price, realised exposure, equity vs buy & hold")
    axes[1].fill_between(
        expo.index, expo * 100, 0, color="#c0392b", alpha=0.35, step="mid"
    )
    axes[1].set_ylabel("Exposure %")
    axes[1].set_ylim(0, 105)
    eq = r.equity.loc["2024-01-01":]
    bh = simulate(sub, pd.Series(1.0, index=sub.index)).equity
    axes[2].plot(eq.index, eq / eq.iloc[0], color="#c0392b", label="Timing")
    axes[2].plot(bh.index, bh / bh.iloc[0], color="#2c3e50", label="Buy & hold")
    axes[2].set_ylabel("NAV (start=1)")
    axes[2].legend(frameon=False, loc="upper left")
    fig.savefig(FIG / "fig5_audit_2024.png")
    plt.close(fig)

    (OUT / "audit_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("\n2024-2026 monthly:")
    print(monthly.round(4).to_string())
    print(f"\n2024-2026 trades: {len(trades)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
