# -*- coding: utf-8 -*-
"""Research-only Tushare dividend/corporate-action backfill.

Writes one parquet of implemented ex-date events for the panel's codes. This is
the accounting input for ``factor_lab.ledger``; it is not a production feed and
does not write Mongo.

Tushare's non-VIP ``dividend`` endpoint is per instrument. The requested window
is applied to ``ex_date`` (the date the ledger needs), not to the announcement
date; a report announced earlier but paid inside the window is still captured.
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.lib.datahub.data_source.interface.tushare_interface import (  # noqa: E402
    from_tushare_ts_code,
    to_tushare_ts_code,
)

FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "div_proc",
    "stk_div",
    "stk_bo_rate",
    "stk_co_rate",
    "cash_div",
    "cash_div_tax",
    "record_date",
    "ex_date",
    "pay_date",
)
NUMERIC = ("stk_div", "stk_bo_rate", "stk_co_rate", "cash_div", "cash_div_tax")


def _date(value):
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None


def normalize_event(row: dict) -> dict | None:
    ex_date = _date(row.get("ex_date"))
    if ex_date is None:
        return None
    try:
        code = from_tushare_ts_code(row.get("ts_code"))
    except ValueError:
        return None
    if str(row.get("div_proc") or "").strip() != "实施":
        return None
    payload = {
        "code": code,
        "ann_date": _date(row.get("ann_date")),
        "end_date": _date(row.get("end_date")),
        "record_date": _date(row.get("record_date")),
        "ex_date": ex_date,
        "pay_date": _date(row.get("pay_date")),
        "div_proc": "实施",
    }
    for field in NUMERIC:
        value = row.get(field)
        try:
            number = float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            number = 0.0
        payload[field] = number if math.isfinite(number) else 0.0
    # Tushare: stk_div is total shares per share (送转), while stk_bo_rate and
    # stk_co_rate are the送股/转增 components that add up to it. Adding all
    # three double-counts the split.
    payload["share_multiplier"] = 1.0 + payload["stk_div"]
    # Official Tushare semantics: cash_div is after tax, cash_div_tax is the
    # pre-tax figure. Use the after-tax one (they are equal in the current
    # snapshot, but the distinction matters when they diverge).
    payload["cash_per_share"] = (
        payload["cash_div"] if payload["cash_div"] > 0 else payload["cash_div_tax"]
    )
    return payload


def dedupe_events(frame: pd.DataFrame) -> pd.DataFrame:
    """One accounting event per (code, ex_date); keep the latest announcement."""
    if frame.empty:
        return frame
    frame = frame.sort_values(
        ["code", "ex_date", "ann_date"], kind="stable", na_position="first"
    )
    return frame.drop_duplicates(["code", "ex_date"], keep="last").reset_index(
        drop=True
    )


def backfill(
    codes: list[str],
    *,
    pro=None,
    from_date: str = "20200101",
    to_date: str = "20260918",
    pause: float = 0.32,
) -> tuple[pd.DataFrame, dict]:
    if pro is None:
        import tushare as ts

        pro = ts.pro_api(os.environ["TUSHARE_TOKEN"])
    start = datetime.datetime.strptime(from_date, "%Y%m%d").date()
    end = datetime.datetime.strptime(to_date, "%Y%m%d").date()
    rows = []
    summary = {
        "codes": len(codes),
        "from_date": from_date,
        "to_date": to_date,
        "empty_codes": 0,
        "failed_codes": [],
        "transfer_events": 0,
    }
    for index, code in enumerate(codes, 1):
        try:
            frame = pro.dividend(
                ts_code=to_tushare_ts_code(code),
                fields=",".join(FIELDS),
            )
        except Exception as exc:  # noqa: BLE001 - one code must not kill the run
            summary["failed_codes"].append({"code": code, "error": str(exc)[:200]})
            frame = None
        if frame is None or frame.empty:
            summary["empty_codes"] += 1
        else:
            for record in frame.to_dict("records"):
                event = normalize_event(record)
                if event is None:
                    continue
                ex_date = event["ex_date"].date()
                if not (start <= ex_date <= end):
                    continue
                rows.append(event)
        if index % 100 == 0:
            print(
                json.dumps(
                    {
                        "progress": index,
                        "events": len(rows),
                        "empty": summary["empty_codes"],
                        "failed": len(summary["failed_codes"]),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        time.sleep(pause)
    result = dedupe_events(pd.DataFrame(rows))
    summary["events"] = int(len(result))
    summary["transfer_events"] = int((result["stk_co_rate"] > 0).sum())
    if not result.empty:
        summary["min_ex_date"] = str(result["ex_date"].min().date())
        summary["max_ex_date"] = str(result["ex_date"].max().date())
        summary["cash_events"] = int((result["cash_per_share"] > 0).sum())
        summary["split_events"] = int((result["share_multiplier"] != 1.0).sum())
    return result, summary


def load_panel_codes(panel: str) -> list[str]:
    codes = pd.read_parquet(panel, columns=["stock_code"])["stock_code"].unique()
    return sorted(str(code) for code in codes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--from-date", default="20200101")
    parser.add_argument("--to-date", default="20260918")
    parser.add_argument("--pause", type=float, default=0.32)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args(argv)
    codes = load_panel_codes(args.panel)
    if args.limit:
        codes = codes[: args.limit]
    result, summary = backfill(
        codes,
        from_date=args.from_date,
        to_date=args.to_date,
        pause=args.pause,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output, index=False)
    summary["output"] = str(output)
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
