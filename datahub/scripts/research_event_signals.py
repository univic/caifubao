# -*- coding: utf-8 -*-
"""Event-driven research signals on a frozen panel (research-only).

Runs fixed rules, no fitted weights:

* ``oversold_rebound``: RSI14 < 30 and close > open
* ``deep_rebound``: 60-session drawdown <= -20%, RSI14 < 35 and close > open
* ``high_breakout``: close at a 250-session high with volume ratio > 1.5

Names are ranked by the event score, the bottom 30% by market cap is excluded,
and execution/costs/tradeability come from ``factor_lab_account_replay``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from factor_lab_account_replay import replay_book  # noqa: E402

PANEL_FIELDS = (
    "date",
    "stock_code",
    "close",
    "close_hfq",
    "open",
    "open_hfq",
    "high_hfq",
    "low_hfq",
    "volume",
    "trade_amount",
    "trade_status",
    "is_bse",
    "is_st",
    "limit_up",
    "limit_down",
    "previous_close",
    "total_mv",
)


def _price(frame):
    return frame["close_hfq"].where(frame["close_hfq"] > 0, frame["close"])


def _rsi(frame, window=14):
    price = _price(frame)
    returns = price.groupby(frame["stock_code"], sort=False).pct_change(
        fill_method=None
    )
    gains = returns.clip(lower=0)
    losses = (-returns).clip(lower=0)
    avg_gain = gains.groupby(frame["stock_code"], sort=False).transform(
        lambda s: s.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    )
    avg_loss = losses.groupby(frame["stock_code"], sort=False).transform(
        lambda s: s.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    )
    relative = avg_gain / avg_loss.where(avg_loss > 0)
    return (100 - 100 / (1 + relative)).mask((avg_loss == 0) & (avg_gain > 0), 100.0)


def _size_ok(frame, min_size_pct=0.30):
    size = pd.to_numeric(frame["total_mv"], errors="coerce")
    rank = size.groupby(frame["date"]).rank(pct=True)
    return rank >= min_size_pct


def build_score(frame):
    close = _price(frame)
    open_price = frame["open_hfq"].where(frame["open_hfq"] > 0, frame["open"])
    rsi = _rsi(frame)
    rolling_high = close.groupby(frame["stock_code"], sort=False).transform(
        lambda s: s.rolling(60, min_periods=30).max()
    )
    dd60 = close / rolling_high.where(rolling_high > 0) - 1.0
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    vol_ma = volume.groupby(frame["stock_code"], sort=False).transform(
        lambda s: s.rolling(20, min_periods=10).mean()
    )
    up_day = close > open_price
    size_ok = _size_ok(frame)
    scores = {
        "oversold_rebound": (-rsi).where((rsi < 30) & up_day & size_ok),
        "deep_rebound": (-dd60).where((dd60 <= -0.20) & (rsi < 35) & up_day & size_ok),
        "high_breakout": pd.Series(
            np.where(
                (close >= rolling_high * 0.999) & (volume > vol_ma * 1.5) & size_ok,
                1.0,
                np.nan,
            ),
            index=frame.index,
        ),
    }
    return scores


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", required=True)
    parser.add_argument("--start-date", default="2022-01-24")
    parser.add_argument("--end-date", default="2026-03-16")
    parser.add_argument("--step", type=int, default=20)
    parser.add_argument("--names", type=int, default=10)
    parser.add_argument("--aum", type=float, default=50000)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    import pyarrow.parquet as pq

    available = set(pq.ParquetFile(args.panel).schema.names)
    columns = [field for field in PANEL_FIELDS if field in available]
    frame = pd.read_parquet(args.panel, columns=columns)
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values(["date", "stock_code"]).reset_index(drop=True)
    scores = build_score(frame)
    sessions = [
        pd.Timestamp(value)
        for value in sorted(frame["date"].unique())
        if pd.Timestamp(args.start_date)
        <= pd.Timestamp(value)
        <= pd.Timestamp(args.end_date)
    ]
    report = {}
    for name, values in scores.items():
        frame["__score"] = values
        frame["__raw_close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["__liquid_rank"] = 1.0
        result = replay_book(
            frame,
            sessions=sessions,
            step=args.step,
            names=args.names,
            buffer_multiple=2,
            aum=args.aum,
            label="fwd_h20",
            require_label=False,
            daily_marks=True,
            include_nav=True,
        )
        report[name] = {
            "total_return": result["total_return"],
            "annualised": result["annualised"],
            "max_drawdown": result["max_drawdown"],
            "turnover_annual": result["turnover_annual"],
            "cash_share_avg": result["cash_share_avg"],
            "failed_entry_fills": result["failed_entry_fills"],
            "final_nav": result["final_nav"],
        }
    payload = {
        "panel": args.panel,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "step": args.step,
        "names": args.names,
        "aum": args.aum,
        "reports": report,
        "note": "Fixed event rules, size-filtered; research-only, not forward evidence.",
    }
    Path(args.output).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
