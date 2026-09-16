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

logger = logging.getLogger(__name__)

#: Quote fields the technical factors read. Projecting exactly these keeps a
#: full-market year (~1.3M rows) out of Document hydration (perf C7/C8). Fields
#: listed here are materialised as ``None`` when the stored document lacks them
#: (index rows carry only OHLCV), i.e. they keep mongoengine's Document semantics;
#: a field NOT listed here still fails loudly with ``AttributeError``.
#:
#: Maintenance note: ``high``/``low``/``volume``/``turnover_rate`` are read through
#: ``getattr(..., 0)``, so dropping one from this tuple would silently evaluate it
#: as 0 rather than raising. Add any new field a factor reads to this tuple.
_FACTOR_QUOTE_FIELDS = (
    "code",
    "date",
    "open",
    "open_hfq",
    "high",
    "low",
    "close",
    "close_hfq",
    "volume",
    "turnover_rate",
)


def _init_db() -> None:
    """Connect through the shared ``MONGODB_*`` configuration.

    This used to read a private ``MONGO_URI`` env var defaulting to
    ``mongodb://localhost:27017/caifubao``, so the CLI could not connect in any
    deployed environment (dev/research/prod set ``MONGODB_HOST/PORT/NAME/...``)
    and would have targeted the wrong database even if it had. The scoring,
    strategy, sync, factor and quote runners all connect this way;
    ``backtest_runner`` still has its own ``MONGO_URI`` path.
    """
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


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


def load_evaluation_quotes(
    quote_model, start, end, horizons, *, stock_code=None, decay_horizons=None
):
    """Load one projected raw-row stream for factor *inputs* and the price frame.

    Returns ``(quotes_by_stock, quote_frame, factor_quote_count)``:

    * ``quotes_by_stock`` are the factor inputs and stop at ``end`` (the legacy
      CLI behaviour, unchanged);
    * ``quote_frame`` extends ``forward_window_days(...)`` past ``end`` because
      forward returns are read as ``date <= d + int(h*1.5)`` with no clamp to the
      evaluation end date — a frame that stopped at ``end`` would silently drop
      the last horizon-window of observations and change every persisted metric.

    Both come from a single query, using ``.only()`` + ``as_pymongo()`` so a
    full-market year (~1.3M rows) never hydrates Documents (perf C7/C8).
    """
    from collections import defaultdict

    from app.lib.scoring_engine.factor_eval import (
        DECAY_HORIZONS,
        _row_price,
        forward_window_days,
    )
    from app.lib.scoring_engine.scoring_service import _Row as QuoteRow

    decay_horizons = DECAY_HORIZONS if decay_horizons is None else decay_horizons
    frame_end = end + datetime.timedelta(
        days=forward_window_days(sorted(set(horizons) | set(decay_horizons)))
    )

    if stock_code:
        quote_qs = quote_model.objects(code=stock_code).filter(
            date__gte=start, date__lte=frame_end
        )
    else:
        quote_qs = quote_model.objects(date__gte=start, date__lte=frame_end)

    quotes_by_stock = defaultdict(list)
    quote_frame = defaultdict(list)
    factor_quote_count = 0
    for raw in quote_qs.only(*_FACTOR_QUOTE_FIELDS).order_by("date").as_pymongo():
        # Complete the projection with None for fields the document does not
        # store (index rows have no *_hfq), matching what a hydrated Document
        # returns; fields outside the projection still raise on access.
        row = QuoteRow({name: raw.get(name) for name in _FACTOR_QUOTE_FIELDS})
        quote_frame[row.code].append((row.date, _row_price(raw)))
        if row.date <= end:
            quotes_by_stock[row.code].append(row)
            factor_quote_count += 1

    return dict(quotes_by_stock), dict(quote_frame), factor_quote_count


def cmd_evaluate(args) -> None:
    """Evaluate a factor's predictive power using IC/IR/quintile analysis."""
    _init_db()

    from app.lib.scoring_engine.factor_eval import FactorEvaluationService
    from app.lib.scoring_engine.technical_factors import ALL_TECHNICAL_FACTORS
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

    # Horizons drive both the evaluation and the price-frame overhang, so resolve
    # them before loading anything.
    horizons = [5, 20, 60] if not args.horizon else [int(args.horizon)]

    quotes_by_stock, quote_frame, quote_count = load_evaluation_quotes(
        StockDailyQuote, start, end, horizons, stock_code=stock_code
    )

    if not quotes_by_stock:
        print("No quotes found")
        return

    print(
        f"Evaluating {factor_name} on {len(quotes_by_stock)} stocks, {quote_count} quotes"
    )
    print(
        f"Date range: {start.date()} — {end.date()} "
        f"(price frame extended for forward returns)"
    )
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
    report = service.evaluate(
        factor_values=factor_values,
        start_date=start,
        end_date=end,
        forward_horizons=horizons,
        quote_frame=dict(quote_frame),
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
