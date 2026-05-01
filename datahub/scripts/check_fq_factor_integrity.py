#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_SAMPLE_CODES = ["sh600000", "sz002797"]


def get_app_config():
    from app.conf import app_config

    return app_config


def build_client():
    from pymongo import MongoClient

    app_config = get_app_config()
    uri = (
        f"mongodb://{app_config.MONGODB_USERNAME}:{app_config.MONGODB_PASSWORD}"
        f"@{app_config.MONGODB_HOST}:{app_config.MONGODB_PORT}/{app_config.MONGODB_DB}"
        "?authSource=admin"
    )
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def get_sample_latest_quote(db, code: str) -> dict[str, Any] | None:
    from pymongo import DESCENDING

    return db["stock_daily_quote"].find_one(
        {"code": code},
        sort=[("date", DESCENDING)],
        projection={
            "_id": 0,
            "code": 1,
            "date": 1,
            "close": 1,
            "previous_close": 1,
            "fq_factor": 1,
            "close_hfq": 1,
            "open_hfq": 1,
        },
    )


def build_summary(
    db, sample_codes: list[str], include_any_fq_probe: bool
) -> dict[str, Any]:
    app_config = get_app_config()
    basic_stock = db["basic_stock"]
    freshness_meta = db["data_freshness_meta"]
    stock_daily_quote = db["stock_daily_quote"]

    summary: dict[str, Any] = {
        "database": app_config.MONGODB_DB,
        "individual_stock_count": basic_stock.count_documents(
            {"object_type": "individual_stock"}
        ),
        "fq_factor_meta_count": freshness_meta.count_documents(
            {
                "object_type": "individual_stock",
                "meta_type": "factor",
                "meta_name": "FQ_FACTOR",
            }
        ),
        "sample_latest_quotes": [
            get_sample_latest_quote(db, code) for code in sample_codes if code
        ],
    }

    if include_any_fq_probe:
        summary["any_fq_factor_quote"] = stock_daily_quote.find_one(
            {"fq_factor": {"$exists": True}},
            projection={"_id": 0, "code": 1, "date": 1, "fq_factor": 1, "close_hfq": 1},
        )

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only fq_factor integrity probe for the current MongoDB environment."
    )
    parser.add_argument(
        "--sample-code",
        action="append",
        dest="sample_codes",
        default=[],
        help="Stock code to spot-check on the latest quote. Can be passed multiple times.",
    )
    parser.add_argument(
        "--skip-any-fq-probe",
        action="store_true",
        help="Skip the probe that searches for any quote document containing fq_factor.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sample_codes = args.sample_codes or DEFAULT_SAMPLE_CODES
    app_config = get_app_config()

    client = build_client()
    try:
        db = client[app_config.MONGODB_DB]
        client.admin.command("ping")
        summary = build_summary(
            db,
            sample_codes=sample_codes,
            include_any_fq_probe=not args.skip_any_fq_probe,
        )
        print(json.dumps(summary, default=str, ensure_ascii=False, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
