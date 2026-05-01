from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger(__name__)

FACTOR_FQ = "fq"
FACTOR_MA = "ma"
FACTOR_ALL = "all"
MODE_STALE = "stale"
MODE_FORCE = "force"


@dataclass(frozen=True)
class FactorConfig:
    service_factory: Callable[[], Any]
    capability: str


def _load_default_runtime() -> tuple[dict[str, FactorConfig], Callable[[str], Any]]:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher
    from app.lib.factor_factory import FQFactorService, MovingAverageFactorService
    from app.model.stock import FinanceMarket

    mongo_watcher.get_db_connection()

    return (
        {
            FACTOR_FQ: FactorConfig(FQFactorService, "fq_factor"),
            FACTOR_MA: FactorConfig(MovingAverageFactorService, "ma_factor"),
        },
        lambda market_name: FinanceMarket.objects(name=market_name).first(),
    )


def _load_supported_stock_codes(service, market, capability: str) -> list[str]:
    from app.lib.utilities import data_capability_helper

    stock_query = service.stock_model.objects(active_status=0)
    if market is not None:
        stock_query = stock_query.filter(market=market)
    return [
        stock.code
        for stock in stock_query.only("code", "data_capabilities")
        if data_capability_helper.stock_supports(stock, capability)
    ]


def _apply_limit(values: Iterable[str], limit: int | None) -> list[str]:
    value_list = list(values)
    if limit is None:
        return value_list
    return value_list[:limit]


def run_factor(
    factor: str,
    *,
    mode: str = MODE_STALE,
    codes: list[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    market_name: str = "ChinaAStock",
    configs: dict[str, FactorConfig] | None = None,
    market_loader: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    if configs is None or market_loader is None:
        configs, market_loader = _load_default_runtime()

    factors = [FACTOR_FQ, FACTOR_MA] if factor == FACTOR_ALL else [factor]
    results = [
        _run_single_factor(
            factor_name,
            mode=mode,
            codes=codes,
            limit=limit,
            dry_run=dry_run,
            market_name=market_name,
            configs=configs,
            market_loader=market_loader,
        )
        for factor_name in factors
    ]
    if factor != FACTOR_ALL:
        return results[0]

    return {
        "factor": FACTOR_ALL,
        "mode": mode,
        "dry_run": dry_run,
        "results": results,
        "pulled_count": sum(item["pulled_count"] for item in results),
        "written_count": sum(item["written_count"] for item in results),
        "skipped_count": sum(item["skipped_count"] for item in results),
        "failed_count": sum(item["failed_count"] for item in results),
    }


def _run_single_factor(
    factor: str,
    *,
    mode: str,
    codes: list[str] | None,
    limit: int | None,
    dry_run: bool,
    market_name: str,
    configs: dict[str, FactorConfig],
    market_loader: Callable[[str], Any],
) -> dict[str, Any]:
    config = configs[factor]
    service = config.service_factory()
    market = market_loader(market_name)

    if codes:
        selected_codes = _apply_limit(codes, limit)
    elif mode == MODE_STALE:
        selected_codes = _apply_limit(
            service.get_codes_requiring_update(market=market), limit
        )
    else:
        selected_codes = _apply_limit(
            _load_supported_stock_codes(service, market, config.capability), limit
        )

    result: dict[str, Any] = {
        "factor": factor,
        "mode": mode,
        "dry_run": dry_run,
        "market": market_name,
        "pulled_count": len(selected_codes),
        "written_count": 0,
        "skipped_count": 0,
        "failed_count": 0,
        "failed_codes": [],
        "codes": selected_codes if dry_run else selected_codes[:20],
    }
    if dry_run:
        return result

    for code in selected_codes:
        try:
            update_result = service.update_code(code)
        except Exception as exc:
            result["failed_count"] += 1
            result["failed_codes"].append(code)
            logger.exception("%s factor update failed: code=%s", factor.upper(), code)
            result["message"] = str(exc)
            continue

        if update_result.get("code") == "SKIP":
            result["skipped_count"] += 1
        result["written_count"] += int(update_result.get("written_count", 0))

    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run datahub factor updates with safe stale-only defaults."
    )
    parser.add_argument(
        "--factor",
        choices=(FACTOR_FQ, FACTOR_MA, FACTOR_ALL),
        default=FACTOR_MA,
        help="Factor family to update. Defaults to MA.",
    )
    parser.add_argument(
        "--mode",
        choices=(MODE_STALE, MODE_FORCE),
        default=MODE_STALE,
        help="stale updates only missing/outdated factors; force scans all supported active stocks.",
    )
    parser.add_argument(
        "--code",
        action="append",
        dest="codes",
        default=[],
        help="Specific stock code to update. Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of selected codes to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print selected codes without writing factor data.",
    )
    parser.add_argument(
        "--market",
        default="ChinaAStock",
        help="FinanceMarket name used when selecting market-wide stocks.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be greater than 0.")

    result = run_factor(
        args.factor,
        mode=args.mode,
        codes=args.codes,
        limit=args.limit,
        dry_run=args.dry_run,
        market_name=args.market,
    )
    print(json.dumps(result, default=str, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
