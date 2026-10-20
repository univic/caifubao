# -*- coding: utf-8 -*-
"""Merge point-in-time valuation fields into a frozen factor-lab panel.

Reads ``stock_daily_basic`` from the research Mongo and attaches
``pe_ttm``/``pb``/``ps_ttm``/``dv_ttm``/``total_mv``/``circ_mv``/``turnover_rate``
to the panel by ``(stock_code, date)``. Research-only: reads Mongo, writes one
parquet, never mutates a collection.
"""

from __future__ import annotations

import argparse
import json
import os

import pandas as pd

FIELDS = (
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dv_ttm",
    "total_mv",
    "circ_mv",
    "turnover_rate",
)


def merge_basic(panel: pd.DataFrame, basic: pd.DataFrame) -> pd.DataFrame:
    """Left-join valuation fields; panel rows are never dropped."""
    if panel.empty:
        raise ValueError("panel is empty")
    basic = basic.copy()
    basic["date"] = pd.to_datetime(basic["date"]).dt.normalize()
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    merged = panel.merge(
        basic[["code", "date", *FIELDS]],
        left_on=["stock_code", "date"],
        right_on=["code", "date"],
        how="left",
        suffixes=("", "_basic"),
    )
    merged = merged.drop(columns=["code"])
    missing = [field for field in FIELDS if field not in merged.columns]
    if missing:
        raise ValueError(f"valuation fields missing after merge: {missing}")
    return merged


def load_basic(from_date, to_date, *, batch_size=500_000) -> pd.DataFrame:
    from pymongo import MongoClient

    client = MongoClient(
        host=os.environ["MONGODB_HOST"],
        port=int(os.environ["MONGODB_PORT"]),
        username=os.environ.get("MONGODB_USER"),
        password=os.environ.get("MONGODB_PASS"),
        authSource="admin",
    )
    collection = client[os.environ["MONGODB_NAME"]]["stock_daily_basic"]
    cursor = collection.find(
        {"date": {"$gte": from_date, "$lte": to_date}},
        {"_id": 0, "code": 1, "date": 1, **{field: 1 for field in FIELDS}},
    ).batch_size(50_000)
    frames = []
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            frames.append(pd.DataFrame(batch))
            batch = []
    if batch:
        frames.append(pd.DataFrame(batch))
    if not frames:
        raise ValueError("stock_daily_basic returned no rows in the requested window")
    return pd.concat(frames, ignore_index=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    args = parser.parse_args(argv)
    panel = pd.read_parquet(args.panel)
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    from_date = pd.Timestamp(args.from_date)
    to_date = pd.Timestamp(args.to_date)
    basic = load_basic(from_date.to_pydatetime(), to_date.to_pydatetime())
    merged = merge_basic(panel, basic)
    merged.to_parquet(args.output, index=False)
    covered = merged[FIELDS[0]].notna().sum()
    summary = {
        "panel_rows": int(len(panel)),
        "basic_rows": int(len(basic)),
        "merged_rows": int(len(merged)),
        "pe_ttm_non_null": int(covered),
        "pe_ttm_coverage": round(float(covered / max(len(merged), 1)), 4),
        "output": args.output,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
