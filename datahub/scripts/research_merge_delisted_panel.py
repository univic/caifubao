# -*- coding: utf-8 -*-
"""Merge a research delisted-quote supplement into a frozen factor-lab panel.

The factor-lab panel is built from the live universe, so it has no delisted
cohort. This script takes:

* the frozen panel parquet,
* the delisted quote parquet from ``research_universe_backfill.py --with-quotes``,
* the ``adj_factor`` parquet fetched for the same delisted codes, and
* the delisted universe parquet (name-change intervals for ST flags),

builds a panel for the delisted codes with the *same* trading calendar and
label rules, and concatenates it with the frozen panel. The existing rows win
on any duplicate ``(stock_code, date)``.

Research-only: reads and writes parquet files; no Mongo, no production write.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.factor_lab.panel import build_panel  # noqa: E402

HORIZONS = (1, 5, 20, 60)


def _is_st_interval(name: str, start: str, end: str | None, date: pd.Timestamp) -> bool:
    if "ST" not in str(name or "").upper():
        return False
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end) if end else pd.Timestamp.max
    return lower <= date <= upper


def _st_mask(intervals_json, dates: pd.Series) -> pd.Series:
    try:
        intervals = json.loads(intervals_json) if intervals_json else []
    except (TypeError, ValueError):
        intervals = []
    mask = pd.Series(False, index=dates.index)
    for interval in intervals:
        start = interval.get("start_date")
        if not start:
            continue
        name = interval.get("name") or ""
        if "ST" not in str(name).upper():
            continue
        mask |= dates.map(
            lambda day: _is_st_interval(name, start, interval.get("end_date"), day)
        )
    return mask


def build_delisted_panel(
    quotes: pd.DataFrame,
    adj: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    sessions,
) -> pd.DataFrame:
    """Build one factor-lab-shaped panel for the delisted quotes."""
    if quotes.empty:
        raise ValueError("delisted quote frame is empty")
    quotes = quotes.copy()
    quotes["date"] = pd.to_datetime(quotes["date"]).dt.normalize()
    quotes["trade_date"] = quotes["trade_date"].astype(str)
    adj = adj.copy()
    adj["trade_date"] = adj["trade_date"].astype(str)
    frame = quotes.merge(adj, on=["ts_code", "trade_date"], how="left")
    frame["adj_factor"] = (
        frame.groupby("code")["adj_factor"].ffill().bfill().fillna(1.0)
    )
    frame = frame.loc[~frame["code"].astype(str).str.startswith("bj")].copy()
    if frame.empty:
        raise ValueError("no non-BSE delisted quotes")
    universe = universe.copy()
    st_by_code = dict(zip(universe["code"], universe["namechange_intervals"]))
    frame["is_st"] = False
    for code, group in frame.groupby("code", sort=False):
        dates = group["date"]
        mask = _st_mask(st_by_code.get(code), dates)
        frame.loc[group.index, "is_st"] = mask.to_numpy()
    rows = []
    for _, row in frame.iterrows():
        adj_value = float(row["adj_factor"]) if pd.notna(row["adj_factor"]) else 1.0
        open_price = float(row["open"]) if pd.notna(row["open"]) else None
        close_price = float(row["close"]) if pd.notna(row["close"]) else None
        high = float(row["high"]) if pd.notna(row["high"]) else None
        low = float(row["low"]) if pd.notna(row["low"]) else None
        rows.append(
            {
                "code": row["code"],
                "date": row["date"],
                "open": open_price,
                "close": close_price,
                "open_hfq": open_price * adj_value if open_price else None,
                "close_hfq": close_price * adj_value if close_price else None,
                "high_hfq": high * adj_value if high else None,
                "low_hfq": low * adj_value if low else None,
                "volume": row.get("vol"),
                "trade_amount": row.get("amount"),
                "change_rate": row.get("pct_chg"),
                "previous_close": row.get("pre_close"),
                "trade_status": 1 if close_price and close_price > 0 else 0,
                "is_bse": False,
                "is_st": bool(row["is_st"]),
            }
        )
    return build_panel(rows, HORIZONS, sessions=sessions)


def merge_panels(panel: pd.DataFrame, delisted: pd.DataFrame) -> pd.DataFrame:
    """Concatenate, keeping the existing panel on duplicate (code, date)."""
    merged = pd.concat([panel, delisted], ignore_index=True, sort=False)
    merged = merged.drop_duplicates(["stock_code", "date"], keep="first")
    return merged.sort_values(["date", "stock_code"], kind="stable").reset_index(
        drop=True
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", required=True)
    parser.add_argument("--quotes", required=True)
    parser.add_argument("--adj", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args(argv)

    panel = pd.read_parquet(args.panel)
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    sessions = sorted(panel["date"].drop_duplicates())
    if args.start_date:
        sessions = [day for day in sessions if day >= pd.Timestamp(args.start_date)]
    if args.end_date:
        sessions = [day for day in sessions if day <= pd.Timestamp(args.end_date)]

    quotes = pd.read_parquet(args.quotes)
    adj = pd.read_parquet(args.adj)
    universe = pd.read_parquet(args.universe)
    delisted = build_delisted_panel(quotes, adj, universe, sessions=sessions)
    delisted = delisted[
        delisted["date"].between(sessions[0], sessions[-1])
    ].reset_index(drop=True)
    merged = merge_panels(panel, delisted)
    merged.to_parquet(args.output, index=False)
    summary = {
        "panel_codes": int(panel["stock_code"].nunique()),
        "delisted_codes": int(delisted["stock_code"].nunique()),
        "delisted_rows": int(len(delisted)),
        "merged_codes": int(merged["stock_code"].nunique()),
        "merged_rows": int(len(merged)),
        "duplicate_rows_kept_existing": int(len(panel) + len(delisted) - len(merged)),
        "output": args.output,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
