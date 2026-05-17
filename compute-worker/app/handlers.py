# -*- coding: utf-8 -*-
"""Compute-worker task handlers.

Each handler receives a ComputeTask document and a db connection.
It updates the task status and result as it progresses.
"""

import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger(__name__)


def handle_task(task: Any) -> None:
    """Dispatch a ComputeTask to its handler by task_type."""
    task_type = task.task_type

    if task_type == "BACKTEST_SINGLE":
        _handle_backtest_single(task)
    elif task_type == "BACKTEST_MULTI":
        _handle_backtest_multi(task)
    elif task_type == "GRID_SEARCH":
        _handle_grid_search(task)
    elif task_type == "SCORE_REPLAY":
        _handle_score_replay(task)
    elif task_type == "SCORE_VERIFY":
        _handle_score_verify(task)
    elif task_type == "CALIBRATION_REPORT":
        _handle_calibration_report(task)
    elif task_type == "FACTOR_EVAL":
        _handle_factor_eval(task)
    elif task_type == "ROLLING_VALIDATION":
        _handle_rolling_validation(task)
    else:
        raise ValueError(f"Unknown task_type: {task_type}")


def _mark_started(task: Any) -> None:
    task.status = "RUNNING"
    task.started_at = datetime.now(timezone.utc)
    task.save()


def _mark_completed(task: Any, result: Dict[str, Any]) -> None:
    task.status = "COMPLETED"
    task.result = result
    task.progress = 1.0
    task.completed_at = datetime.now(timezone.utc)
    task.save()


def _mark_failed(task: Any, error: str) -> None:
    task.status = "FAILED"
    task.error = error
    task.retry_count = getattr(task, "retry_count", 0) + 1
    task.completed_at = datetime.now(timezone.utc)
    task.save()


def _update_progress(task: Any, progress: float, message: str = "") -> None:
    task.progress = min(max(progress, 0.0), 1.0)
    if message:
        task.progress_message = message
    task.save()


# ---------------------------------------------------------------------------
# Single-stock backtest
# ---------------------------------------------------------------------------


def _handle_backtest_single(task: Any) -> None:
    _mark_started(task)
    try:
        from backend.app.services.backtest_service import run_backtest

        params = task.params or {}
        _update_progress(task, 0.1, "Loading data...")

        result = run_backtest(
            stock_code=params["stock_code"],
            strategy=params.get("strategy", "MA_CROSS"),
            start_date=datetime.fromisoformat(params["start_date"]),
            end_date=datetime.fromisoformat(params["end_date"]),
            initial_cash=float(params.get("initial_cash", 100_000)),
            save_result=True,
            benchmark_code=params.get("benchmark_code", "sh000300"),
            horizon=params.get("horizon"),
            entry_threshold=float(params.get("entry_threshold", 70)),
            exit_threshold=float(params.get("exit_threshold", 50)),
            stop_loss_pct=float(params.get("stop_loss_pct", -5)),
            score_delta=float(params.get("score_delta", 10)),
            model_version=params.get("model_version"),
        )

        if result.get("error"):
            _mark_failed(task, result["error"])
        else:
            _update_progress(task, 1.0, "Complete")
            _mark_completed(task, {"backtest_id": result.get("id"), "summary": {k: v for k, v in result.items() if k not in ("trades", "daily_values")}})
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Multi-stock backtest
# ---------------------------------------------------------------------------


def _handle_backtest_multi(task: Any) -> None:
    _mark_started(task)
    try:
        from backend.app.services.backtest_service import run_multi_stock_backtest

        params = task.params or {}
        _update_progress(task, 0.1, "Loading portfolio data...")

        result = run_multi_stock_backtest(
            stock_codes=params["stock_codes"],
            strategy=params.get("strategy", "TOP_N_ROTATION"),
            start_date=datetime.fromisoformat(params["start_date"]),
            end_date=datetime.fromisoformat(params["end_date"]),
            initial_cash=float(params.get("initial_cash", 100_000)),
            save_result=True,
            benchmark_code=params.get("benchmark_code", "sh000300"),
            horizon=params.get("horizon"),
            top_n=int(params.get("top_n", 10)),
            rebalance_interval=int(params.get("rebalance_interval", 5)),
            allocation=params.get("allocation", "equal_weight"),
            max_position_pct=float(params.get("max_position_pct", 0.20)),
            stop_loss_pct=float(params.get("stop_loss_pct", -5)),
            model_version=params.get("model_version"),
        )

        if result.get("error"):
            _mark_failed(task, result["error"])
        else:
            _mark_completed(task, {"backtest_id": result.get("id"), "summary": {k: v for k, v in result.items() if k not in ("trades", "daily_values")}})
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Score replay
# ---------------------------------------------------------------------------


def _handle_score_replay(task: Any) -> None:
    _mark_started(task)
    try:
        from datahub.app.lib.scoring_engine.replay_service import ScoreReplayService

        params = task.params or {}
        _update_progress(task, 0.1, "Starting scoring replay...")

        service = ScoreReplayService(
            model_version=params.get("model_version"),
            scoring_config=params.get("scoring_config"),
        )
        result = service.backfill_predictions(
            start_date=datetime.fromisoformat(params["start_date"]),
            end_date=datetime.fromisoformat(params["end_date"]),
            horizon=params.get("horizon"),
            stock_code=params.get("stock_code"),
            dry_run=params.get("dry_run", False),
            replace=params.get("replace", False),
        )

        _mark_completed(task, result)
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def _handle_score_verify(task: Any) -> None:
    _mark_started(task)
    try:
        from datahub.app.lib.scoring_engine.verification_service import ScoreVerificationService

        params = task.params or {}
        _update_progress(task, 0.1, "Running verification...")

        service = ScoreVerificationService(model_version=params.get("model_version"))
        result = service.verify_predictions(
            start_date=datetime.fromisoformat(params["start_date"]) if params.get("start_date") else None,
            end_date=datetime.fromisoformat(params["end_date"]) if params.get("end_date") else None,
            horizon=params.get("horizon"),
        )

        _mark_completed(task, result)
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Calibration report
# ---------------------------------------------------------------------------


def _handle_calibration_report(task: Any) -> None:
    _mark_started(task)
    try:
        from datahub.app.lib.scoring_engine.calibration_report import ScoreCalibrationReport

        params = task.params or {}
        _update_progress(task, 0.1, "Generating calibration report...")

        report = ScoreCalibrationReport(model_version=params.get("model_version"))
        result = report.generate(
            start_date=datetime.fromisoformat(params["start_date"]),
            end_date=datetime.fromisoformat(params["end_date"]),
            horizon=int(params["horizon"]),
        )

        _mark_completed(task, result)
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Grid search
# ---------------------------------------------------------------------------


def _handle_grid_search(task: Any) -> None:
    _mark_started(task)
    try:
        from itertools import product

        params = task.params or {}
        horizons = params.get("horizons", [20])
        weight_grid = params.get("weight_grid", {})   # e.g. {"signal_strength": [10,20,30], ...}
        threshold_grid = params.get("threshold_grid", {"entry": [60,70,80], "exit": [40,50]})
        date_range = params.get("date_range", {"start": "2024-01-01", "end": "2024-06-30"})

        # Build weight combinations
        component_ids = list(weight_grid.keys())
        weight_values = [weight_grid[c] for c in component_ids]
        weight_combos = []

        for combo in product(*weight_values):
            total = sum(combo)
            if abs(total - 100) < 0.01 or total == 0:
                weight_combos.append(dict(zip(component_ids, combo)))

        # If weights don't sum to 100 naturally, only test combos that do
        if not weight_combos:
            for combo in product(*weight_values):
                total = sum(combo)
                # Normalize to 100
                normalized = {k: round(v / total * 100, 1) for k, v in zip(component_ids, combo)}
                weight_combos.append(normalized)

        # Limit to most distinct combinations
        if len(weight_combos) > 100:
            weight_combos = weight_combos[:100]

        # Build threshold combinations
        threshold_keys = list(threshold_grid.keys())
        threshold_values = [threshold_grid[k] for k in threshold_keys]
        threshold_combos = [dict(zip(threshold_keys, combo)) for combo in product(*threshold_values)]

        # Build all experiments
        experiments = []
        for horizon in horizons:
            for wc in weight_combos:
                for tc in threshold_combos:
                    config = {"weights": wc}
                    experiments.append({
                        "horizon": horizon,
                        "config": config,
                        **tc,
                    })

        total_experiments = len(experiments)
        _update_progress(task, 0.05, f"Running {total_experiments} experiments...")

        results = []
        for idx, exp in enumerate(experiments):
            progress = 0.05 + 0.90 * (idx / total_experiments)
            _update_progress(task, progress, f"Experiment {idx+1}/{total_experiments}: horizon={exp['horizon']}")

            try:
                from datahub.app.lib.scoring_engine.replay_service import ScoreReplayService
                from datahub.app.lib.scoring_engine.verification_service import ScoreVerificationService
                from datahub.app.lib.scoring_engine.calibration_report import ScoreCalibrationReport

                model_version = f"grid_{idx}_{exp['horizon']}h"

                replay = ScoreReplayService(
                    model_version=model_version,
                    scoring_config={str(exp["horizon"]): exp["config"]},
                )
                replay.backfill_predictions(
                    start_date=datetime.fromisoformat(date_range["start"]),
                    end_date=datetime.fromisoformat(date_range["end"]),
                    horizon=exp["horizon"],
                    replace=True,
                )

                verifier = ScoreVerificationService(model_version=model_version)
                verifier.verify_predictions(
                    start_date=datetime.fromisoformat(date_range["start"]),
                    end_date=datetime.fromisoformat(date_range["end"]),
                    horizon=exp["horizon"],
                )

                report = ScoreCalibrationReport(model_version=model_version)
                cal = report.generate(
                    start_date=datetime.fromisoformat(date_range["start"]),
                    end_date=datetime.fromisoformat(date_range["end"]),
                    horizon=exp["horizon"],
                )

                results.append({
                    "experiment_idx": idx,
                    "horizon": exp["horizon"],
                    **exp,
                    "hit_rate": cal.get("score_buckets", [{}])[-1].get("hit_rate"),
                    "top10_hit_rate": cal.get("top_n", {}).get("top_10", {}).get("hit_rate"),
                    "avg_return": cal.get("score_buckets", [{}])[-1].get("avg_return_at_target"),
                })
            except Exception as exc:
                results.append({
                    "experiment_idx": idx,
                    "horizon": exp["horizon"],
                    **exp,
                    "error": str(exc),
                })

        # Rank results
        ranked = sorted(
            [r for r in results if "error" not in r and r.get("top10_hit_rate") is not None],
            key=lambda r: r.get("top10_hit_rate", 0) or 0,
            reverse=True,
        )

        _mark_completed(task, {
            "total_experiments": total_experiments,
            "completed": len([r for r in results if "error" not in r]),
            "failed": len([r for r in results if "error" in r]),
            "top_20": ranked[:20],
            "all_results": results,
        })
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Factor evaluation
# ---------------------------------------------------------------------------


def _handle_factor_eval(task: Any) -> None:
    _mark_started(task)
    try:
        params = task.params or {}
        _update_progress(task, 0.1, "Evaluating factor...")

        result = {
            "factor_name": params.get("factor_name", "unknown"),
            "ic_mean": None,
            "ic_std": None,
            "icir": None,
            "quintile_returns": [],
            "correlations": {},
            "note": "Factor evaluation skeleton — IC/IR computation needs factor data pipeline",
        }

        _mark_completed(task, result)
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Rolling validation
# ---------------------------------------------------------------------------


def _handle_rolling_validation(task: Any) -> None:
    _mark_started(task)
    try:
        params = task.params or {}
        _update_progress(task, 0.1, "Running rolling validation...")

        result = {
            "model_version": params.get("model_version"),
            "windows": [],
            "note": "Rolling validation skeleton — needs grid search sub-tasks",
        }

        _mark_completed(task, result)
    except Exception:
        _mark_failed(task, traceback.format_exc())
