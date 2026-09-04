#!/usr/bin/env python3
"""flip_wide as an executable single-stock timing strategy + equity curve.

flip_wide (research #177) flips all 8 technical components at construction:
score high = LOW trend/momentum/risk (mean-reversion semantics). This script
makes it executable per-stock: each day we cross-sectionally rank the flipped
composite across the eligible universe; when the stock's flipped percentile
rises above the entry band (it is cheap/mean-reverting) we buy at next day's
open, and exit when it falls into the exit band (it has reverted / high
score). Execution follows the production backtest kernel conventions:

- signal at close of day T, execute at open of day T+1 (T+1, no look-ahead)
- friction: commission 0.025% (min 5 CNY), stamp duty 0.1% on sell,
  slippage 0.1% per side
- skip orders on trade_status != 1 (suspended) — hold position
- benchmark: buy-and-hold same stock over the same window
Output: daily equity series (strategy vs benchmark) written to JSON/CSV, plus
a matplotlib PNG curve with BUY/SELL markers.

Run:
    PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_flipwide_single_stock.py \
        --stock sz000002 \
        --snapshot /tmp/h20-2019-2026-merged.parquet \
        --from 2024-01-01 --to 2026-07-31 \
        --entry-pct 0.90 --exit-pct 0.30 \
        --out-dir datahub/research/autoresearch/h20_excess_alpha/flip_wide_curves
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

W = {
    "signal_strength": 15,
    "momentum": 15,
    "trend_alignment": 30,
    "breakout_or_position": 5,
    "industry_momentum": 5,
    "relative_strength": 15,
    "real_relative_strength": 10,
    "risk_penalty": 15,
}
TOTAL_W = sum(W.values())
COMPONENTS = list(W)
COMMISSION_RATE = 0.00025
MIN_COMMISSION = 5.0
STAMP_DUTY_RATE = 0.001
SLIPPAGE = 0.001


def _is_stock(code: str) -> bool:
    return bool(pd.Series([code]).str.match(r"^(sh[456789]|sz[0123])").iloc[0])


def _apply_friction(price: float, side: str) -> tuple[float, float, float]:
    exec_price = price * (1 + SLIPPAGE if side == "BUY" else 1 - SLIPPAGE)
    value = exec_price
    commission = max(value * COMMISSION_RATE, MIN_COMMISSION)
    duty = value * STAMP_DUTY_RATE if side == "SELL" else 0.0
    return exec_price, commission, duty


def simulate(
    frame: pd.DataFrame,
    stock: str,
    entry_pct: float,
    exit_pct: float,
    initial_cash: float = 1_000_000.0,
    max_position_pct: float = 1.0,
) -> dict:
    """Single-stock flip_wide timing backtest (all dates in frame)."""
    comp_cols = [c for c in COMPONENTS if c in frame.columns]
    if len(comp_cols) != len(COMPONENTS):
        raise ValueError(f"snapshot missing components; got {comp_cols}")

    d = frame[frame["eligibility"].astype(bool)].copy()
    # flipped composite percentile per day across eligible universe
    score = pd.Series(0.0, index=d.index)
    for c in comp_cols:
        ranked = d.groupby("date", sort=False)[c].rank(
            method="average", pct=True, na_option="bottom"
        )
        score += ranked * (-1.0) * W[c] / TOTAL_W
    d["flip_pct"] = score.groupby(d["date"], sort=False).rank(
        method="average", pct=True
    )

    stock_rows = d[d["stock_code"] == stock].sort_values("date")
    if stock_rows.empty:
        raise ValueError(f"no rows for {stock}")
    dates = stock_rows["date"].tolist()

    cash = initial_cash
    shares = 0.0
    position_value = 0.0
    trades = []
    daily = []
    price_map = stock_rows.set_index("date")

    for i, day in enumerate(dates):
        row = price_map.loc[day]
        close = float(row["close_hfq"])
        status = int(row.get("trade_status", 1))
        flip_pct = float(row["flip_pct"])

        # Signal used at close of PREVIOUS day was executed at this day's open.
        prev_flip = None
        prev_status = 1
        if i > 0:
            prev_row = price_map.loc[dates[i - 1]]
            prev_flip = float(prev_row["flip_pct"])
            prev_status = int(prev_row.get("trade_status", 1))

        # Execute pending order at today's open (T+1)
        open_price = float(row["open_hfq"])
        if (
            prev_flip is not None
            and prev_status == 1
            and status == 1
            and open_price > 0
        ):
            if shares == 0 and prev_flip >= entry_pct:
                # buy at open with available cash
                budget = cash * max_position_pct
                exec_p, comm, duty = _apply_friction(open_price, "BUY")
                qty = int(budget / exec_p)
                if qty > 0:
                    cost = qty * exec_p
                    cash -= cost + comm
                    shares = qty
                    trades.append(
                        {
                            "date": day.date().isoformat(),
                            "side": "BUY",
                            "price": round(open_price, 4),
                            "exec_price": round(exec_p, 4),
                            "quantity": qty,
                            "flip_pct": round(prev_flip, 4),
                            "reason": f"flip_pct {prev_flip:.2f} >= entry {entry_pct}",
                        }
                    )
            elif shares > 0 and prev_flip <= exit_pct:
                exec_p, comm, duty = _apply_friction(open_price, "SELL")
                proceeds = shares * exec_p
                cash += proceeds - comm - duty
                trades.append(
                    {
                        "date": day.date().isoformat(),
                        "side": "SELL",
                        "price": round(open_price, 4),
                        "exec_price": round(exec_p, 4),
                        "quantity": shares,
                        "flip_pct": round(prev_flip, 4),
                        "reason": f"flip_pct {prev_flip:.2f} <= exit {exit_pct}",
                    }
                )
                shares = 0.0

        position_value = shares * close
        equity = cash + position_value
        daily.append(
            {
                "date": day.date().isoformat(),
                "equity": round(equity, 2),
                "cash": round(cash, 2),
                "shares": shares,
                "close": round(close, 4),
                "flip_pct": round(flip_pct, 4),
            }
        )

    # buy & hold benchmark (buy first day close, hold till last close)
    bench = []
    b_cash = initial_cash
    b_shares = int(b_cash / float(price_map.loc[dates[0], "close_hfq"]))
    b_cash -= b_shares * float(price_map.loc[dates[0], "close_hfq"])
    for day in dates:
        close = float(price_map.loc[day, "close_hfq"])
        bench.append(
            {"date": day.date().isoformat(), "equity": b_cash + b_shares * close}
        )

    daily_df = pd.DataFrame(daily)
    bench_df = pd.DataFrame(bench)
    strategy_ret = daily_df["equity"].iloc[-1] / initial_cash - 1.0
    bench_ret = bench_df["equity"].iloc[-1] / initial_cash - 1.0
    return {
        "stock": stock,
        "entry_pct": entry_pct,
        "exit_pct": exit_pct,
        "initial_cash": initial_cash,
        "final_equity": daily_df["equity"].iloc[-1],
        "strategy_return_pct": round(strategy_ret * 100, 3),
        "benchmark_return_pct": round(bench_ret * 100, 3),
        "excess_return_pct": round((strategy_ret - bench_ret) * 100, 3),
        "num_trades": len(trades),
        "trades": trades,
        "daily": daily,
        "benchmark_daily": bench,
    }


def plot(result: dict, out_path: Path) -> None:
    daily = pd.DataFrame(result["daily"])
    bench = pd.DataFrame(result["benchmark_daily"])
    daily["date"] = pd.to_datetime(daily["date"])
    bench["date"] = pd.to_datetime(bench["date"])
    trades = result["trades"]

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(
        daily["date"],
        daily["equity"] / result["initial_cash"],
        label=f"flip_wide timing (stock {result['stock']})",
        color="#2a6f97",
        linewidth=1.8,
    )
    ax.plot(
        bench["date"],
        bench["equity"] / result["initial_cash"],
        label="buy & hold",
        color="#999999",
        linewidth=1.2,
        linestyle="--",
    )
    for t in trades:
        marker = "^" if t["side"] == "BUY" else "v"
        color = "#d1495b" if t["side"] == "BUY" else "#00798c"
        ax.scatter(
            pd.Timestamp(t["date"]), 1.0, marker=marker, color=color, s=40, zorder=5
        )
    ax.axhline(1.0, color="black", linewidth=0.6, alpha=0.4)
    ax.set_title(
        f"flip_wide single-stock timing — {result['stock']}  "
        f"(entry pct≥{result['entry_pct']}, exit pct≤{result['exit_pct']})\n"
        f"strategy {result['strategy_return_pct']:+.2f}%  vs  buy&hold "
        f"{result['benchmark_return_pct']:+.2f}%  (excess "
        f"{result['excess_return_pct']:+.2f}%)  ·  {result['num_trades']} trades"
    )
    ax.set_ylabel("wealth (initial = 1.0)")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--from", dest="from_date", default="2019-01-01")
    parser.add_argument("--to", dest="to_date", default="2026-07-31")
    parser.add_argument("--entry-pct", type=float, default=0.90)
    parser.add_argument("--exit-pct", type=float, default=0.30)
    parser.add_argument("--out-dir", default="/tmp/flip_wide_curves")
    args = parser.parse_args()

    if not _is_stock(args.stock):
        raise ValueError(f"{args.stock} does not look like an A-share stock code")

    frame = pd.read_parquet(args.snapshot)
    frame["date"] = pd.to_datetime(frame["date"], utc=True)
    lo = pd.Timestamp(args.from_date, tz="UTC")
    hi = pd.Timestamp(args.to_date, tz="UTC")
    frame = frame[(frame["date"] >= lo) & (frame["date"] <= hi)]

    result = simulate(frame, args.stock, args.entry_pct, args.exit_pct)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{args.stock}_flip_wide"
    with (out_dir / f"{stem}.json").open("w") as f:
        json.dump(result, f, ensure_ascii=False)
    plot(result, out_dir / f"{stem}.png")
    print(
        f"{args.stock}: strategy {result['strategy_return_pct']:+.2f}% | "
        f"buy&hold {result['benchmark_return_pct']:+.2f}% | excess "
        f"{result['excess_return_pct']:+.2f}% | {result['num_trades']} trades"
    )
    print(f"outputs: {(out_dir / stem).with_suffix('.png')} and .json")


if __name__ == "__main__":
    main()
