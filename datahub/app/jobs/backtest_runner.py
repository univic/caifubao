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
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys

logger = logging.getLogger(__name__)

SUPPORTED_STRATEGIES = [
    "MA_CROSS",
    "BUY_HOLD",
    "SCORE_THRESHOLD",
    "SCORE_MOMENTUM",
    "TOP_N_ROTATION",
    "MULTI_HORIZON_CONSENSUS",
]

SCORE_DRIVEN_STRATEGIES = {
    "SCORE_THRESHOLD",
    "SCORE_MOMENTUM",
    "TOP_N_ROTATION",
    "MULTI_HORIZON_CONSENSUS",
}


def _load_timing_evaluator():
    try:
        # The image workflow copies this dependency-free module into the
        # datahub package so the deployed CLI has the same implementation.
        from app.services.timing_evaluator import evaluate_timing_pool

        return evaluate_timing_pool
    except ModuleNotFoundError as exc:
        if exc.name not in {"app.services", "app.services.timing_evaluator"}:
            raise
        # Source-tree fallback: backend and datahub both expose a top-level
        # ``app`` package, so load the backend module without importing Flask.
        module_path = (
            Path(__file__).resolve().parents[3]
            / "backend"
            / "app"
            / "services"
            / "timing_evaluator.py"
        )
        spec = importlib.util.spec_from_file_location(
            "_caifubao_timing_evaluator", module_path
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load timing evaluator")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.evaluate_timing_pool


def _require_score_model_version(args, *strategies: str) -> str | None:
    model_version = (getattr(args, "model_version", None) or "").strip()
    if any(strategy in SCORE_DRIVEN_STRATEGIES for strategy in strategies):
        if not model_version:
            raise ValueError("model_version is required for score-driven strategies")
        return model_version
    return None


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
    model_version = _require_score_model_version(args, args.strategy)
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
    if model_version:
        params["model_version"] = model_version
    if args.consensus_entry:
        params["consensus_entry_thresholds"] = json.loads(args.consensus_entry)
    if args.consensus_exit:
        params["consensus_exit_thresholds"] = json.loads(args.consensus_exit)

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
    model_version = _require_score_model_version(args, args.strategy)
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
        model_version=model_version,
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
    model_version = _require_score_model_version(args, args.strategy, args.vs_strategy)
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
        if strat_name in SCORE_DRIVEN_STRATEGIES:
            params["model_version"] = model_version

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


def run_timing_pool(args, evaluator=None) -> dict:
    """Aggregate an explicit, precomputed timing/buy-hold cohort artifact.

    This research-only command deliberately does not discover a cohort, run a
    live scan, or persist backtests. The input owns both normalized sides for
    every requested stock so the same-stock baseline cannot be replaced by an
    index benchmark accidentally.
    """
    payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, dict):
        raise ValueError("timing-pool input requires a results object keyed by code")
    cohort = payload.get("cohort")
    if not isinstance(cohort, list) or not cohort:
        raise ValueError("timing-pool input requires an explicit non-empty cohort")
    if evaluator is None:
        evaluator = _load_timing_evaluator()

    def _paired_runner(stock_code, **_kwargs):
        pair = results.get(stock_code)
        if pair is None:
            return {
                "status": "FAILED",
                "reason_code": "MISSING_RESULT_PAIR",
            }
        return pair

    report = evaluator(
        cohort,
        _paired_runner,
        cohort_as_of=payload.get("cohort_as_of"),
        cohort_source=payload.get("cohort_source"),
        model_version=payload.get("model_version"),
        config=payload.get("config"),
        window=payload.get("window"),
        delisted_completeness=payload.get("delisted_completeness", "UNKNOWN"),
        initial_cash=payload.get("initial_cash", 100000.0),
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return report


def _registered_model_record(model_version: str) -> dict | None:
    from app.model.scoring import ScoreModelVersion

    record = ScoreModelVersion.objects(model_version=model_version).first()
    if record is None:
        return None
    return {
        "model_version": record.model_version,
        "config_hash": record.config_hash,
        "scoring_mode": record.scoring_mode,
        "status": record.status,
    }


def _timing_replay_evidence(stock_code: str, manifest: dict) -> tuple[list, list]:
    from app.model.scoring import StockScorePrediction
    from app.model.stock import StockDailyQuote

    start = parse_date(manifest["window"]["from"])
    end = parse_date(manifest["window"]["to"])
    quotes = list(
        StockDailyQuote.objects(
            code=stock_code,
            date__gte=start,
            date__lte=end,
        ).order_by("date")
    )
    predictions = list(
        StockScorePrediction.objects(
            stock_code=stock_code,
            date__gte=start,
            date__lte=end,
            horizon=manifest["horizon"],
            model_version=manifest["model_version"],
        ).order_by("date")
    )
    return quotes, predictions


def run_timing_replay(
    args,
    *,
    evaluator=None,
    db_initializer=None,
    model_loader=None,
    evidence_loader=None,
    pair_builder=None,
) -> dict:
    """Run real, read-only evidence through the P1 adapter and P0 evaluator."""
    from app.lib.strategy_engine.timing_replay import (
        replay_pair,
        validate_model_pin,
        validate_replay_manifest,
    )

    manifest_path = Path(args.manifest_json)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(
            f"cannot read timing replay manifest: {manifest_path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"invalid timing replay manifest JSON: {manifest_path}"
        ) from exc
    manifest = validate_replay_manifest(payload)
    if db_initializer is None:
        db_initializer = _init_db
    if model_loader is None:
        model_loader = _registered_model_record
    if evidence_loader is None:
        evidence_loader = _timing_replay_evidence
    if pair_builder is None:
        pair_builder = replay_pair
    if evaluator is None:
        evaluator = _load_timing_evaluator()

    db_initializer()
    registry_record = model_loader(manifest["model_version"])
    validate_model_pin(manifest, registry_record)

    def _paired_runner(stock_code, **kwargs):
        if kwargs.get("save_result") is not False:
            raise ValueError("timing replay requires save_result=False")
        quotes, predictions = evidence_loader(stock_code, manifest)
        return pair_builder(stock_code, quotes, predictions, manifest)

    report = evaluator(
        manifest["cohort_codes"],
        _paired_runner,
        cohort_as_of=manifest["cohort_as_of"],
        cohort_source=manifest["cohort_source"],
        model_version=manifest["model_version"],
        config=manifest["config"],
        window=manifest["window"],
        delisted_completeness=manifest["delisted_completeness"],
        initial_cash=manifest["initial_cash"],
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        output_path = Path(args.output)
        try:
            output_path.write_text(rendered + "\n", encoding="utf-8")
        except OSError as exc:
            raise ValueError(
                f"cannot write timing replay report: {output_path}"
            ) from exc
    print(rendered)
    return report


def run_optimize(args) -> dict:
    """Run parameter optimization sweep."""
    model_version = _require_score_model_version(args, args.strategy)
    _init_db()
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
            "model_version": model_version,
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


def run_compare_all(args) -> dict:
    """Compare all eligible strategies on one stock via API."""
    model_version = _require_score_model_version(args, "SCORE_THRESHOLD")
    _init_db()
    from flask import Flask

    app = Flask(__name__)
    app.config["TESTING"] = True

    from backend.app.api.v1.backtest import compare as cmp_handler

    with app.test_request_context(
        method="POST",
        json={
            "stock_code": args.stock_code,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "initial_cash": float(args.initial_cash),
            "benchmark_code": args.benchmark_code,
            "model_version": model_version,
        },
    ):
        response = cmp_handler()
        data = (
            response[0].get_json()
            if isinstance(response, tuple)
            else response.get_json()
        )
        print(json.dumps(data, default=str, ensure_ascii=False, indent=2))
        return data or {}


def run_scan(args) -> dict:
    """Scan one strategy across all active stocks via API."""
    model_version = _require_score_model_version(args, args.strategy)
    _init_db()
    from flask import Flask

    app = Flask(__name__)
    app.config["TESTING"] = True

    from backend.app.api.v1.backtest import scan as scan_handler

    json_payload = {
        "strategy": args.strategy,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "initial_cash": float(args.initial_cash),
        "page": args.page,
        "per_page": args.per_page,
    }
    if args.horizon:
        json_payload["horizon"] = int(args.horizon)
    if args.min_trades:
        json_payload["min_trades"] = args.min_trades
    if model_version:
        json_payload["model_version"] = model_version

    with app.test_request_context(method="POST", json=json_payload):
        response = scan_handler()
        data = (
            response[0].get_json()
            if isinstance(response, tuple)
            else response.get_json()
        )
        # Print summary only (items list is large)
        summary = {
            k: v for k, v in (data.get("data", data) or {}).items() if k != "items"
        }
        print(json.dumps(summary, default=str, ensure_ascii=False, indent=2))
        # Print top-5 items
        items = (data.get("data", data) or {}).get("items", [])
        if items:
            print("\nTop-5 results:")
            for item in items[:5]:
                flags = item.get("flags", [])
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                print(
                    f"  {item.get('stock_code', '?')} "
                    f"{item.get('stock_name', '?')}: "
                    f"Sharpe={item.get('sharpe_ratio', 0):.2f} "
                    f"Return={item.get('total_return_pct', 0):+.1f}%"
                    f"{flag_str}"
                )
        return data or {}


def run_walk_forward(args) -> dict:
    """Run walk-forward validation via API."""
    model_version = _require_score_model_version(args, args.strategy)
    _init_db()
    from flask import Flask

    app = Flask(__name__)
    app.config["TESTING"] = True

    from backend.app.api.v1.backtest import walk_forward as wf_handler

    json_payload = {
        "stock_code": args.stock_code,
        "strategy": args.strategy,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "window_days": args.window_days,
        "step_days": args.step_days,
        "initial_cash": float(args.initial_cash),
    }
    if args.horizon:
        json_payload["horizon"] = int(args.horizon)
    if model_version:
        json_payload["model_version"] = model_version

    with app.test_request_context(method="POST", json=json_payload):
        response = wf_handler()
        data = (
            response[0].get_json()
            if isinstance(response, tuple)
            else response.get_json()
        )
        # Print summary
        if data and data.get("data"):
            d = data["data"]
            print(f"Stock: {d.get('stock_code')}")
            print(f"Strategy: {d.get('strategy')}")
            print(f"Horizon: {d.get('horizon')}")
            print(f"Total windows: {d.get('total_windows')}")
            print(f"Stability score: {d.get('stability_score')}")
            print(f"Performance decay: {d.get('performance_decay')}")
            print()
            windows = d.get("windows", [])
            if windows:
                print("Window results:")
                for i, w in enumerate(windows):
                    print(
                        f"  {i + 1}: {w.get('start_date')} -> {w.get('end_date')}"
                        f"  Return={w.get('total_return_pct', 0):+.1f}%"
                        f"  Sharpe={w.get('sharpe_ratio', 0):.2f}"
                        f"  MaxDD={w.get('max_drawdown', 0):+.1f}%"
                        f"  Trades={w.get('total_trades', 0)}"
                        f"  WinRate={w.get('win_rate', 0):.0f}%"
                    )
            else:
                print("No valid windows produced.")
        else:
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
    p_single.add_argument(
        "--consensus-entry",
        help='Consensus entry thresholds as JSON: \'{"5":60,"20":55}\'',
    )
    p_single.add_argument(
        "--consensus-exit",
        help='Consensus exit thresholds as JSON: \'{"5":30,"20":35}\'',
    )
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
    p_compare.add_argument("--model-version")

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
    p_optimize.add_argument("--model-version")
    p_optimize.add_argument(
        "--no-split", action="store_true", help="Don't use train/val/test split"
    )

    # --- compare-all ---
    p_cmp_all = subparsers.add_parser(
        "compare-all", help="Compare all strategies on one stock"
    )
    p_cmp_all.add_argument("stock_code")
    p_cmp_all.add_argument("start_date", help="YYYY-MM-DD")
    p_cmp_all.add_argument("end_date", help="YYYY-MM-DD")
    p_cmp_all.add_argument("--initial-cash", default=100000)
    p_cmp_all.add_argument("--benchmark-code", default="sh000300")
    p_cmp_all.add_argument("--model-version")

    # --- scan ---
    p_scan = subparsers.add_parser("scan", help="Scan one strategy across market")
    p_scan.add_argument("strategy", choices=SUPPORTED_STRATEGIES)
    p_scan.add_argument("start_date", help="YYYY-MM-DD")
    p_scan.add_argument("end_date", help="YYYY-MM-DD")
    p_scan.add_argument("--horizon", type=int, help="Score horizon (5/20/60)")
    p_scan.add_argument("--initial-cash", default=100000)
    p_scan.add_argument("--page", type=int, default=1)
    p_scan.add_argument("--per-page", type=int, default=20)
    p_scan.add_argument("--min-trades", type=int, help="Minimum trade count filter")
    p_scan.add_argument("--model-version")

    # --- walk-forward ---
    p_walk_forward = subparsers.add_parser(
        "walk-forward", help="Rolling-window walk-forward validation"
    )
    p_walk_forward.add_argument("stock_code", help="e.g. sh600519")
    p_walk_forward.add_argument("strategy", choices=SUPPORTED_STRATEGIES)
    p_walk_forward.add_argument("start_date", help="YYYY-MM-DD")
    p_walk_forward.add_argument("end_date", help="YYYY-MM-DD")
    p_walk_forward.add_argument("--window-days", type=int, default=120)
    p_walk_forward.add_argument("--step-days", type=int, default=60)
    p_walk_forward.add_argument("--horizon", type=int, help="Score horizon (5/20/60)")
    p_walk_forward.add_argument("--initial-cash", default=100000)
    p_walk_forward.add_argument("--model-version")

    # --- timing-pool (research-only, explicit cohort) ---
    p_timing_pool = subparsers.add_parser(
        "timing-pool",
        help="Aggregate explicit same-stock timing/buy-hold result pairs",
    )
    p_timing_pool.add_argument("input_json", help="Versioned explicit-cohort JSON")
    p_timing_pool.add_argument("--output", help="Optional report JSON path")

    # --- timing-replay (research-only, real read-only evidence) ---
    p_timing_replay = subparsers.add_parser(
        "timing-replay",
        help="Replay a frozen point-in-time cohort from stored evidence",
    )
    p_timing_replay.add_argument(
        "manifest_json", help="Versioned frozen cohort and evidence manifest"
    )
    p_timing_replay.add_argument("--output", help="Optional report JSON path")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "single":
            run_single(args)
        elif args.command == "multi":
            run_multi(args)
        elif args.command == "compare":
            run_compare(args)
        elif args.command == "optimize":
            run_optimize(args)
        elif args.command == "compare-all":
            run_compare_all(args)
        elif args.command == "scan":
            run_scan(args)
        elif args.command == "walk-forward":
            run_walk_forward(args)
        elif args.command == "timing-pool":
            run_timing_pool(args)
        elif args.command == "timing-replay":
            run_timing_replay(args)
        else:
            parser.print_help()
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
