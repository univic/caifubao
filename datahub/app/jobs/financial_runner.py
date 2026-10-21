# -*- coding: utf-8 -*-
"""Point-in-time financial indicator ingestion (research-only).

Usage:
    python -m app.jobs.financial_runner fina-indicator --codes sh600519,sz000001
    python -m app.jobs.financial_runner fina-indicator --from-date 20200101 --to-date 20260930

Fetches ``tushare.pro.fina_indicator`` per instrument over a date range and upserts one document
per ``(code, end_date, report_type, ann_date)`` into
``stock_financial_indicator``. No public API, no scheduling, no scoring write.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys

from app.lib.datahub.data_source.interface.tushare_interface import (
    from_tushare_ts_code,
    to_tushare_ts_code,
)

FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "report_type",
    "roe",
    "roe_waa",
    "roa",
    "grossprofit_margin",
    "netprofit_margin",
    "assets_turn",
    "debt_to_assets",
    "netprofit_yoy",
    "or_yoy",
    "tr_yoy",
    "ocf_yoy",
    "q_ocf_to_sales",
)
NUMERIC = FIELDS[4:]


def _date(value):
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None


def normalize_row(row: dict) -> dict | None:
    ann_date = _date(row.get("ann_date"))
    end_date = _date(row.get("end_date"))
    if ann_date is None or end_date is None:
        return None
    try:
        code = from_tushare_ts_code(row.get("ts_code"))
    except ValueError:
        return None
    if not re.fullmatch(r"(sh|sz|bj)\d{6}", code):
        return None
    try:
        report_type = int(float(row.get("report_type") or 1))
    except (TypeError, ValueError):
        report_type = 1
    payload = {
        "code": code,
        "ann_date": ann_date,
        "end_date": end_date,
        "report_type": report_type,
    }
    for field in NUMERIC:
        value = row.get(field)
        try:
            payload[field] = float(value) if value is not None else None
        except (TypeError, ValueError):
            payload[field] = None
    return payload


def fetch_code(
    pro, code: str, start_date: str, end_date: str
) -> tuple[list[dict], int]:
    """Fetch all report vintages for one instrument over a date range.

    The non-VIP endpoint requires ``ts_code``; a period-only call is rejected by
    Tushare, so the backfill is per instrument rather than per period.
    """
    try:
        ts_code = to_tushare_ts_code(code)
    except ValueError:
        return [], 0
    frame = pro.fina_indicator(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=",".join(FIELDS),
    )
    if frame is None or frame.empty:
        return [], 0
    rows = []
    rejected = 0
    for record in frame.to_dict("records"):
        normalized = normalize_row(record)
        if normalized is not None:
            rows.append(normalized)
        else:
            rejected += 1
    return rows, rejected


def persist_rows(rows: list[dict], *, collection=None) -> dict:
    """Idempotent upsert by (code, end_date, report_type, ann_date)."""
    if collection is None:
        from app.model.financial import StockFinancialIndicator

        collection = StockFinancialIndicator.objects
    created = updated = 0
    for row in rows:
        identity = {
            "code": row["code"],
            "end_date": row["end_date"],
            "report_type": row["report_type"],
            "ann_date": row["ann_date"],
        }
        existing = collection(**identity).first()
        if existing is None:
            from app.model.financial import StockFinancialIndicator

            StockFinancialIndicator(**identity, **{k: row[k] for k in NUMERIC}).save()
            created += 1
        else:
            for field in NUMERIC:
                value = row.get(field)
                if value is not None:
                    setattr(existing, field, value)
            existing.save()
            updated += 1
    return {"created": created, "updated": updated}


def run(
    codes: list[str],
    *,
    start_date: str = "20200101",
    end_date: str = "20260930",
    pro=None,
    pause: float = 0.32,
) -> dict:
    import os
    import time

    if pro is None:
        import tushare as ts

        pro = ts.pro_api(os.environ["TUSHARE_TOKEN"])
    summary = {
        "codes": len(codes),
        "start_date": start_date,
        "end_date": end_date,
        "created": 0,
        "updated": 0,
        "rows": 0,
        "empty_codes": 0,
        "failed_codes": [],
        "rejected_rows": 0,
    }
    for index, code in enumerate(codes, 1):
        try:
            rows, rejected = fetch_code(pro, code, start_date, end_date)
            summary["rejected_rows"] += rejected
            if not rows:
                summary["empty_codes"] += 1
            else:
                result = persist_rows(rows)
                summary["created"] += result["created"]
                summary["updated"] += result["updated"]
                summary["rows"] += len(rows)
        except Exception as exc:  # noqa: BLE001 - one code must not kill the backfill
            summary["failed_codes"].append({"code": code, "error": str(exc)[:200]})
        if index % 100 == 0:
            print(
                json.dumps(
                    {
                        "progress": index,
                        "rows": summary["rows"],
                        "empty": summary["empty_codes"],
                        "failed": len(summary["failed_codes"]),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        time.sleep(pause)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("fina-indicator",))
    parser.add_argument("--codes", default=None, help="Comma-separated internal codes.")
    parser.add_argument("--from-date", default="20200101")
    parser.add_argument("--to-date", default="20260930")
    parser.add_argument("--pause", type=float, default=0.32)
    args = parser.parse_args(argv)
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()
    if args.codes:
        codes = [part.strip() for part in args.codes.split(",") if part.strip()]
    else:
        from app.model.stock import IndividualStock

        codes = [
            row.code
            for row in IndividualStock.objects(active_status=0)
            .only("code")
            .order_by("code")
        ]
    if not codes:
        raise RuntimeError("no active stock codes to backfill")
    summary = run(
        codes,
        start_date=args.from_date,
        end_date=args.to_date,
        pause=args.pause,
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
