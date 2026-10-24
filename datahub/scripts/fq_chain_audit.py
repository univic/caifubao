#!/usr/bin/env python3
"""Read-only ex-rights / after-adjustment (FQ/HFQ) chain audit.

Run inside an environment that already holds the data (for the research
environment, exec into the datahub pod):

    python -m app.jobs.etf_sync_runner status   # unrelated example
    python scripts/fq_chain_audit.py --jump-scan
    python scripts/fq_chain_audit.py --sample-code sh600519 --json

It reports coverage, arithmetic and (unless ``--skip-source``) a tushare
``adj_factor`` comparison for a bounded sample, plus an optional bounded
market-wide factor-jump scan. Exit code: 0 PASS, 1 WARN, 2 FAIL.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_JUMP_WINDOW_DAYS = 90


def build_client():
    from app.conf import app_config
    from pymongo import MongoClient

    return MongoClient(
        host=app_config.MONGODB_HOST,
        port=app_config.MONGODB_PORT,
        username=app_config.MONGODB_USERNAME,
        password=app_config.MONGODB_PASSWORD,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )


def database(client):
    from app.conf import app_config

    return client[app_config.MONGODB_DB]


def sample_codes(db, size: int) -> list[str]:
    """Deterministic spread over active stocks that actually have FQ rows."""
    codes = db["stock_daily_quote"].distinct(
        "code", {"fq_factor": {"$exists": True, "$ne": None}}
    )
    active = set(
        db["basic_stock"].distinct(
            "code", {"object_type": "individual_stock", "active_status": 0}
        )
    )
    pool = sorted(set(codes) & active)
    if not pool:
        return []
    size = max(1, min(size, len(pool)))
    step = max(1, len(pool) // size)
    return pool[::step][:size] or pool[:size]


def make_source_fetcher(db):
    """Return ``fetch(code) -> {date: adj_factor}`` from tushare."""
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit
    from app.lib.datahub.data_source.interface import tushare_interface

    def fetch(code: str):
        stored = fq_chain_audit.load_stored_rows(db, code)
        if not stored:
            return {}
        start = stored[0]["date"].strftime("%Y%m%d")
        end = stored[-1]["date"].strftime("%Y%m%d")
        raw = tushare_interface.adj_factor(
            tushare_interface.to_tushare_ts_code(code), start, end
        )
        return {
            datetime.datetime.strptime(str(row["trade_date"]), "%Y%m%d"): float(
                row["adj_factor"]
            )
            for _, row in raw.iterrows()
        }

    return fetch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sample-code",
        action="append",
        dest="sample_codes",
        default=[],
        help="Stock code to audit against tushare (repeatable)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Deterministic sample size when no --sample-code is given",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=5e-4,
        help="Absolute fq_factor tolerance vs the source (default 5e-4)",
    )
    parser.add_argument(
        "--skip-source",
        action="store_true",
        help="Skip the tushare source comparison (offline audit)",
    )
    parser.add_argument(
        "--jump-scan",
        action="store_true",
        help="Add the bounded market-wide fq_factor jump scan",
    )
    parser.add_argument(
        "--jump-days",
        type=int,
        default=DEFAULT_JUMP_WINDOW_DAYS,
        help="Jump-scan calendar window ending at the latest quote date",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    db = database(build_client())
    codes = args.sample_codes or sample_codes(db, args.sample_size)
    fetch = None if args.skip_source else make_source_fetcher(db)

    jump = None
    if args.jump_scan:
        from app.lib.datahub.data_integrity_keeper.handler.fq_factor_integrity import (
            scan_fq_factor_jumps,
        )

        latest = fq_chain_audit.latest_session(db) or datetime.date.today()
        date_from = latest - datetime.timedelta(days=max(1, args.jump_days))
        jump = scan_fq_factor_jumps(
            db,
            date_from=date_from,
            date_to=latest,
            target_date=latest,
            require_universe=True,
        )

    report = fq_chain_audit.run_audit(
        db,
        sample_codes=codes,
        fetch_source=fetch,
        tolerance=args.tolerance,
        jump_scan=jump,
    )
    report["sample_codes"] = codes
    report["source_checked"] = fetch is not None
    print(json.dumps(report, indent=2, default=str, ensure_ascii=False))
    return {"PASS": 0, "WARN": 1, "FAIL": 2}.get(report["verdict"], 2)


if __name__ == "__main__":
    sys.exit(main())
