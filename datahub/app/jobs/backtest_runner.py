# -*- coding: utf-8 -*-
"""Standalone backtest CLI runner — runs backtests without Flask.

Usage:
    python -m app.jobs.backtest_runner single sh600519 MA_CROSS 2024-01-01 2024-06-30
    python -m app.jobs.backtest_runner single sh600519 SCORE_THRESHOLD 2024-01-01 2024-06-30 --horizon 20 --entry 80
    python -m app.jobs.backtest_runner multi sh600519,sz000858 TOP_N_ROTATION 2024-01-01 2024-06-30 --horizon 20 --top-n 5
    python -m app.jobs.backtest_runner compare sh600519 SCORE_THRESHOLD 2024-01-01 2024-06-30 --horizon 20 --vs MA_CROSS
"""

import argparse
import datetime
import json
import logging
import sys
import os

logger = logging.getLogger(__name__)

SUPPORTED_STRATEGIES = [
    "MA_CROSS",
    "BUY_HOLD",
    "SCORE_THRESHOLD",
    "SCORE_MOMENTUM",
    "TOP_N_ROTATION",
]


def _init_db() -> None:
    """Connect to MongoDB."""
    sys.path.insert(
        0, os.path.join(os.path.dirname(__file__), "..", "..", "backend", "app")
    )
    from mongoengine import connect

    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/caifubao")
    connect(host=uri, db="caifubao")


def parse_date(value: str) -> datetime.datetime:
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def run_single(args) -> dict:
    """Run a single-stock backtest."""
    _init_db()
    from backend.app.services.backtest_service import run_backtest

    start = parse_date(args.start_date)
    end = parse_date(args.end_date)

    params = {
        "stock_code": args.stock_code,
        "strategy": args.strategy,
        "start_date": start,
        "end_date": end,
        "initial_cash": float(args.initial_cash),
        "save_result": not args.no_save,
        "benchmark_code": args.benchmark_code,
    }
    if args.horizon:
        params["horizon"] = int(args.horizon)
    if args.entry is not None:
        params["entry_threshold"] = float(args.entry)
    if args.exit is not None:
        params["exit_threshold"] = float(args.exit)
    if args.stop_loss is not None:
        params["stop_loss_pct"] = float(args.stop_loss)
    if args.score_delta is not None:
        params["score_delta"] = float(args.score_delta)
    if args.model_version:
        params["model_version"] = args.model_version

    result = run_backtest(**params)

    if result.get("error"):
        print(json.dumps({"error": result}, ensure_ascii=False, indent=2))
        return result

    # Print summary (exclude large lists)
    summary = {k: v for k, v in result.items() if k not in ("trades", "daily_values")}
    print(json.dumps(summary, default=str, ensure_ascii=False, indent=2))
    return result


def run_multi(args) -> dict:
    """Run a multi-stock backtest."""
    _init_db()
    from backend.app.services.backtest_service import run_multi_stock_backtest

    stock_codes = [s.strip() for s in args.stock_codes.split(",") if s.strip()]
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)

    result = run_multi_stock_backtest(
        stock_codes=stock_codes,
        strategy=args.strategy,
        start_date=start,
        end_date=end,
        initial_cash=float(args.initial_cash),
        save_result=not args.no_save,
        benchmark_code=args.benchmark_code,
        horizon=int(args.horizon) if args.horizon else None,
        top_n=int(args.top_n) if args.top_n else 10,
        rebalance_interval=int(args.rebalance_interval)
        if args.rebalance_interval
        else 5,
        allocation=args.allocation or "equal_weight",
        max_position_pct=float(args.max_position_pct)
        if args.max_position_pct
        else 0.20,
        stop_loss_pct=float(args.stop_loss) if args.stop_loss else -5.0,
        model_version=args.model_version or None,
    )

    if result.get("error"):
        print(json.dumps({"error": result}, ensure_ascii=False, indent=2))
        return result

    summary = {
        k: v
        for k, v in result.items()
        if k not in ("trades", "daily_values", "per_stock_contributions")
    }
    print(json.dumps(summary, default=str, ensure_ascii=False, indent=2))
    return result


def run_compare(args) -> dict:
    """Compare two strategies on the same stock."""
    _init_db()
    from backend.app.services.backtest_service import run_backtest

    start = parse_date(args.start_date)
    end = parse_date(args.end_date)

    print(f"Running {args.strategy} vs {args.vs_strategy} on {args.stock_code}...")
    print(f"Date range: {start.date()} — {end.date()}")
    print("-" * 60)

    results = {}
    for strat_name in [args.strategy, args.vs_strategy]:
        params = {
            "stock_code": args.stock_code,
            "strategy": strat_name,
            "start_date": start,
            "end_date": end,
            "initial_cash": float(args.initial_cash),
            "save_result": False,
            "benchmark_code": args.benchmark_code,
        }
        if args.horizon and strat_name in (
            "SCORE_THRESHOLD",
            "SCORE_MOMENTUM",
            "TOP_N_ROTATION",
        ):
            params["horizon"] = int(args.horizon)
        if args.entry is not None:
            params["entry_threshold"] = float(args.entry)
        if args.exit is not None:
            params["exit_threshold"] = float(args.exit)
        if args.stop_loss is not None:
            params["stop_loss_pct"] = float(args.stop_loss)

        r = run_backtest(**params)
        if r.get("error"):
            print(f"  {strat_name}: ERROR — {r['error']}")
            continue
        results[strat_name] = r

        print(f"  {strat_name}:")
        print(
            f"    Return: {r.get('total_return_pct', '--')}%  Sharpe: {r.get('sharpe_ratio', '--')}"
        )
        print(
            f"    Max DD: {r.get('max_drawdown', '--')}%  Win Rate: {r.get('win_rate', '--')}"
        )
        if r.get("excess_return_pct") is not None:
            print(f"    Excess vs CSI300: {r['excess_return_pct']}%")
        print()

    if len(results) == 2:
        a, b = results[args.strategy], results[args.vs_strategy]
        delta_return = (a.get("total_return_pct", 0) or 0) - (
            b.get("total_return_pct", 0) or 0
        )
        delta_sharpe = (a.get("sharpe_ratio", 0) or 0) - (b.get("sharpe_ratio", 0) or 0)
        winner = args.strategy if delta_return > 0 else args.vs_strategy
        print("-" * 60)
        print(
            f"Winner: {winner}  (Δ return: {delta_return:+.2f}%,  Δ Sharpe: {delta_sharpe:+.4f})"
        )

    return results


def run_optimize(args) -> dict:
    """Run parameter optimization sweep."""
    _init_db()
    # Build a mock Flask request context for the optimize endpoint
    from flask import Flask

    app = Flask(__name__)
    app.config["TESTING"] = True

    param_grid = {}
    if args.strategy == "SCORE_THRESHOLD":
        if args.entry_range:
            entry_vals = [float(v) for v in args.entry_range.split(",")]
            param_grid["entry_threshold"] = entry_vals
        if args.exit_range:
            exit_vals = [float(v) for v in args.exit_range.split(",")]
            param_grid["exit_threshold"] = exit_vals
    elif args.strategy == "SCORE_MOMENTUM":
        if args.score_delta_range:
            delta_vals = [float(v) for v in args.score_delta_range.split(",")]
            param_grid["score_delta"] = delta_vals

    if args.stop_loss_range:
        sl_vals = [float(v) for v in args.stop_loss_range.split(",")]
        param_grid["stop_loss_pct"] = sl_vals

    if not param_grid:
        print("Error: no parameter ranges specified")
        return {"error": "no parameter ranges"}

    from backend.app.api.v1.backtest import optimize as opt_handler

    with app.test_request_context(
        method="POST",
        json={
            "stock_code": args.stock_code,
            "strategy": args.strategy,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "horizon": int(args.horizon),
            "param_grid": param_grid,
            "initial_cash": float(args.initial_cash),
            "use_split": not args.no_split,
        },
    ):
        response = opt_handler()
        data = (
            response[0].get_json()
            if isinstance(response, tuple)
            else response.get_json()
        )
        print(json.dumps(data, default=str, ensure_ascii=False, indent=2))
        return data or {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standalone backtest CLI runner")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # --- single ---
    p_single = subparsers.add_parser("single", help="Single-stock backtest")
    p_single.add_argument("stock_code", help="e.g. sh600519")
    p_single.add_argument("strategy", choices=SUPPORTED_STRATEGIES)
    p_single.add_argument("start_date", help="YYYY-MM-DD")
    p_single.add_argument("end_date", help="YYYY-MM-DD")
    p_single.add_argument("--initial-cash", default=100000)
    p_single.add_argument("--benchmark-code", default="sh000300")
    p_single.add_argument("--horizon", type=int, help="Score horizon (5/20/60)")
    p_single.add_argument("--entry", type=float, help="Entry threshold")
    p_single.add_argument("--exit", type=float, help="Exit threshold")
    p_single.add_argument("--stop-loss", type=float, help="Stop loss % (negative)")
    p_single.add_argument("--score-delta", type=float, help="Score momentum delta")
    p_single.add_argument("--model-version")
    p_single.add_argument("--no-save", action="store_true", help="Don't persist to DB")

    # --- multi ---
    p_multi = subparsers.add_parser("multi", help="Multi-stock backtest")
    p_multi.add_argument("stock_codes", help="Comma-separated: sh600519,sz000858")
    p_multi.add_argument("strategy", choices=["TOP_N_ROTATION"])
    p_multi.add_argument("start_date", help="YYYY-MM-DD")
    p_multi.add_argument("end_date", help="YYYY-MM-DD")
    p_multi.add_argument("--initial-cash", default=100000)
    p_multi.add_argument("--benchmark-code", default="sh000300")
    p_multi.add_argument("--horizon", type=int, required=True)
    p_multi.add_argument("--top-n", type=int, default=10)
    p_multi.add_argument("--rebalance-interval", type=int, default=5)
    p_multi.add_argument(
        "--allocation",
        choices=["equal_weight", "score_weighted"],
        default="equal_weight",
    )
    p_multi.add_argument("--max-position-pct", type=float, default=0.20)
    p_multi.add_argument("--stop-loss", type=float, default=-5.0)
    p_multi.add_argument("--model-version")
    p_multi.add_argument("--no-save", action="store_true")

    # --- compare ---
    p_compare = subparsers.add_parser("compare", help="Compare two strategies")
    p_compare.add_argument("stock_code")
    p_compare.add_argument("strategy", choices=SUPPORTED_STRATEGIES)
    p_compare.add_argument("start_date", help="YYYY-MM-DD")
    p_compare.add_argument("end_date", help="YYYY-MM-DD")
    p_compare.add_argument(
        "--vs", dest="vs_strategy", required=True, choices=SUPPORTED_STRATEGIES
    )
    p_compare.add_argument("--initial-cash", default=100000)
    p_compare.add_argument("--benchmark-code", default="sh000300")
    p_compare.add_argument("--horizon", type=int)
    p_compare.add_argument("--entry", type=float)
    p_compare.add_argument("--exit", type=float)
    p_compare.add_argument("--stop-loss", type=float)

    # --- optimize ---
    p_optimize = subparsers.add_parser("optimize", help="Parameter sweep optimization")
    p_optimize.add_argument("stock_code", help="e.g. sz000977")
    p_optimize.add_argument("strategy", choices=["SCORE_THRESHOLD", "SCORE_MOMENTUM"])
    p_optimize.add_argument("start_date", help="YYYY-MM-DD")
    p_optimize.add_argument("end_date", help="YYYY-MM-DD")
    p_optimize.add_argument("--horizon", type=int, required=True)
    p_optimize.add_argument("--entry-range", help="Comma-separated: 50,60,70")
    p_optimize.add_argument("--exit-range", help="Comma-separated: 30,40,50")
    p_optimize.add_argument(
        "--score-delta-range", help="Comma-separated: 5,10,15 (SCORE_MOMENTUM)"
    )
    p_optimize.add_argument("--stop-loss-range", help="Comma-separated: -3,-5,-8")
    p_optimize.add_argument("--initial-cash", default=100000)
    p_optimize.add_argument(
        "--no-split", action="store_true", help="Don't use train/val/test split"
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "single":
        run_single(args)
    elif args.command == "multi":
        run_multi(args)
    elif args.command == "compare":
        run_compare(args)
    elif args.command == "optimize":
        run_optimize(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
