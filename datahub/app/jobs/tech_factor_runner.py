# -*- coding: utf-8 -*-
"""Technical factor computation CLI — computes and stores new factors.

Usage:
    # Compute all 8 factors for one stock over a date range
    python -m app.jobs.tech_factor_runner compute sh600519 2024-01-01 2024-12-31 --all

    # Compute specific factors
    python -m app.jobs.tech_factor_runner compute sh600519 2024-01-01 2024-12-31 --factors rsi_14,bb_position

    # Evaluate a factor's predictive power
    python -m app.jobs.tech_factor_runner evaluate rsi_14 2024-01-01 2024-12-31 --horizon 20

    # List available factors
    python -m app.jobs.tech_factor_runner list
"""

import argparse
import datetime
import json
import logging
import os
import sys

logger = logging.getLogger(__name__)


def _init_db() -> None:
    sys.path.insert(
        0, os.path.join(os.path.dirname(__file__), "..", "..", "datahub", "app")
    )
    from mongoengine import connect

    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/caifubao")
    connect(host=uri, db="caifubao")


def parse_date(value: str) -> datetime.datetime:
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def cmd_list(args) -> None:
    """List available technical factors."""
    from app.lib.scoring_engine.technical_factors import ALL_TECHNICAL_FACTORS

    print("Available technical factors:")
    for name in sorted(ALL_TECHNICAL_FACTORS):
        doc = (ALL_TECHNICAL_FACTORS[name].__doc__ or "").strip().split("\n")[0]
        print(f"  {name:22s} — {doc}")


def cmd_compute(args) -> None:
    """Compute factor values for one stock over a date range."""
    _init_db()
    from app.lib.scoring_engine.technical_factors import ALL_TECHNICAL_FACTORS
    from app.model.stock import StockDailyQuote

    stock_code = args.stock_code
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)

    # Load quotes
    quotes = list(
        StockDailyQuote.objects(code=stock_code)
        .filter(date__gte=start, date__lte=end)
        .order_by("date")
    )
    if not quotes:
        print(
            json.dumps(
                {
                    "error": f"No quotes for {stock_code} in [{start.date()}, {end.date()}]"
                }
            )
        )
        return

    factor_names = (
        args.factors.split(",")
        if args.factors != "all"
        else list(ALL_TECHNICAL_FACTORS)
    )
    factor_names = [
        f.strip() for f in factor_names if f.strip() in ALL_TECHNICAL_FACTORS
    ]

    print(
        f"Computing {len(factor_names)} factors for {stock_code}: {', '.join(factor_names)}"
    )
    print(
        f"Quotes loaded: {len(quotes)}  ({quotes[0].date.date()} — {quotes[-1].date.date()})"
    )
    print("-" * 60)

    results = {}
    for name in factor_names:
        fn = ALL_TECHNICAL_FACTORS[name]
        values = fn(quotes)
        count = len(values)
        results[name] = {"dates": count, "sample": list(values.items())[:5]}
        print(f"  {name}: {count} values computed")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, default=str, ensure_ascii=False, indent=2)
        print(f"\nFull results written to {args.output}")


def cmd_evaluate(args) -> None:
    """Evaluate a factor's predictive power using IC/IR/quintile analysis."""
    _init_db()
    from app.lib.scoring_engine.technical_factors import ALL_TECHNICAL_FACTORS
    from app.lib.scoring_engine.factor_eval import FactorEvaluationService
    from app.model.stock import StockDailyQuote

    factor_name = args.factor_name
    if factor_name not in ALL_TECHNICAL_FACTORS:
        print(
            f"Unknown factor: {factor_name}. Available: {', '.join(ALL_TECHNICAL_FACTORS)}"
        )
        return

    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    stock_code = args.stock_code or None

    # Load quotes for all stocks, or a single stock
    if stock_code:
        quote_qs = StockDailyQuote.objects(code=stock_code).filter(
            date__gte=start, date__lte=end
        )
    else:
        quote_qs = StockDailyQuote.objects(date__gte=start, date__lte=end)

    quotes = list(quote_qs.order_by("date"))
    if not quotes:
        print("No quotes found")
        return

    # Group quotes by stock_code, then compute factor values per stock
    from collections import defaultdict

    quotes_by_stock = defaultdict(list)
    for q in quotes:
        quotes_by_stock[q.code].append(q)

    print(
        f"Evaluating {factor_name} on {len(quotes_by_stock)} stocks, {len(quotes)} quotes"
    )
    print(f"Date range: {start.date()} — {end.date()}")
    print("-" * 60)

    fn = ALL_TECHNICAL_FACTORS[factor_name]
    factor_values = {}
    for code, stock_quotes in quotes_by_stock.items():
        sorted_quotes = sorted(stock_quotes, key=lambda q: q.date)
        values = fn(sorted_quotes)
        if values:
            factor_values[code] = values

    print(f"Computed factor values for {len(factor_values)} stocks")

    # Run evaluation
    service = FactorEvaluationService()
    horizons = [5, 20, 60] if not args.horizon else [int(args.horizon)]
    report = service.evaluate(
        factor_values=factor_values,
        start_date=start,
        end_date=end,
        forward_horizons=horizons,
    )

    print(json.dumps(report, default=str, ensure_ascii=False, indent=2))

    # Save to DB if requested
    if args.save:
        from app.model.factor_eval import FactorEvalReport

        doc = FactorEvalReport(
            factor_name=factor_name,
            start_date=start,
            end_date=end,
            observation_count=report.get("observation_count", 0),
            ic_summary=report.get("ic", {}),
            icir_summary=report.get("icir", {}),
            quintile_analysis=report.get("quintiles", {}),
            correlation_matrix=report.get("correlation", {}),
            decay_curve=report.get("decay", {}),
        )
        doc.save()
        print(f"\nReport saved: {doc.id}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Technical factor computation and evaluation CLI"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # --- list ---
    subparsers.add_parser("list", help="List available factors")

    # --- compute ---
    p_compute = subparsers.add_parser(
        "compute", help="Compute factor values for a stock"
    )
    p_compute.add_argument("stock_code", help="e.g. sh600519")
    p_compute.add_argument("start_date", help="YYYY-MM-DD")
    p_compute.add_argument("end_date", help="YYYY-MM-DD")
    p_compute.add_argument("--factors", default="all", help="Comma-separated or 'all'")
    p_compute.add_argument("--output", help="JSON output file path")

    # --- evaluate ---
    p_eval = subparsers.add_parser("evaluate", help="Evaluate factor predictive power")
    p_eval.add_argument("factor_name", help="Factor name (e.g. rsi_14)")
    p_eval.add_argument("start_date", help="YYYY-MM-DD")
    p_eval.add_argument("end_date", help="YYYY-MM-DD")
    p_eval.add_argument("--stock-code", help="Limit to single stock")
    p_eval.add_argument("--horizon", type=int, help="Forward horizon (5/20/60)")
    p_eval.add_argument("--save", action="store_true", help="Save report to DB")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list":
        cmd_list(args)
    elif args.command == "compute":
        cmd_compute(args)
    elif args.command == "evaluate":
        cmd_evaluate(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
