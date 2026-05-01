#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill MA_* factor data into stock_factor_daily."
    )
    parser.add_argument(
        "--code",
        action="append",
        dest="codes",
        default=[],
        help="Specific stock code to backfill. Can be passed multiple times.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Backfill all stocks in the ChinaAStock market.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.all and not args.codes:
        raise SystemExit("Use --all or provide at least one --code.")

    from app.lib.db_watcher.mongoengine_tool import mongo_watcher
    from app.lib.factor_factory import MovingAverageFactorService
    from app.model.stock import FinanceMarket

    mongo_watcher.get_db_connection()
    service = MovingAverageFactorService()

    if args.all:
        market = FinanceMarket.objects(name="ChinaAStock").first()
        result = service.backfill_all(market=market)
    else:
        written_total = 0
        for code in args.codes:
            result = service.backfill_code(code)
            written_total += int(result.get("written_count", 0))
        result = {"pulled_count": len(args.codes), "written_count": written_total}

    print(json.dumps(result, default=str, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
