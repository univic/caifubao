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
    elif task_type == "BACKTEST_SCAN":
        _handle_backtest_scan(task)
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
            _mark_completed(
                task,
                {
                    "backtest_id": result.get("id"),
                    "summary": {
                        k: v
                        for k, v in result.items()
                        if k not in ("trades", "daily_values")
                    },
                },
            )
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
            _mark_completed(
                task,
                {
                    "backtest_id": result.get("id"),
                    "summary": {
                        k: v
                        for k, v in result.items()
                        if k not in ("trades", "daily_values")
                    },
                },
            )
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Backtest scan (one strategy across all active stocks)
# ---------------------------------------------------------------------------


def _handle_backtest_scan(task: Any) -> None:
    """Scan one strategy across all active stocks and rank results."""
    _mark_started(task)
    try:
        from datetime import datetime as _dt
        from backend.app.model.stock import IndividualStock
        from backend.app.services.backtest_service import (
            run_backtest,
            composite_score,
            anti_overfitting_flags,
            bonferroni_correction,
        )

        params = task.params or {}

        strategy = params["strategy"]
        start_date = _dt.fromisoformat(params["start_date"])
        end_date = _dt.fromisoformat(params["end_date"])
        horizon = params.get("horizon")
        initial_cash = float(params.get("initial_cash", 100_000))
        min_trades = int(params.get("min_trades", 0))
        page = int(params.get("page", 1))
        per_page = int(params.get("per_page", 20))

        _update_progress(task, 0.0, "Loading active stocks...")

        stocks = list(
            IndividualStock.objects(active_status=0).only("code", "name").limit(3000)
        )

        if not stocks:
            _mark_failed(task, "No active stocks found")
            return

        total = len(stocks)
        _update_progress(task, 0.02, f"Scanning {total} stocks...")

        results = []
        errors = 0

        for idx, stock in enumerate(stocks):
            try:
                r = run_backtest(
                    stock_code=stock.code,
                    strategy=strategy,
                    start_date=start_date,
                    end_date=end_date,
                    initial_cash=initial_cash,
                    save_result=False,
                    horizon=horizon,
                    model_version=params.get("model_version"),
                )
            except Exception:
                errors += 1
                continue

            if "error" in r:
                errors += 1
                continue

            if min_trades > 0 and r.get("total_trades", 0) < min_trades:
                continue

            entry = {
                "stock_code": stock.code,
                "stock_name": stock.name or stock.code,
                "total_return_pct": r.get("total_return_pct", 0),
                "sharpe_ratio": r.get("sharpe_ratio", 0),
                "max_drawdown": r.get("max_drawdown", 0),
                "total_trades": r.get("total_trades", 0),
                "win_rate": r.get("win_rate", 0),
                "excess_return_pct": r.get("excess_return_pct", 0),
                "information_ratio": r.get("information_ratio", 0),
            }

            comp = composite_score(
                excess_return_pct=entry["excess_return_pct"],
                max_drawdown=entry["max_drawdown"],
                information_ratio=entry["information_ratio"],
                total_trades=entry["total_trades"],
                daily_values=r.get("daily_values"),
                trades=r.get("trades"),
            )
            aof = anti_overfitting_flags(
                total_trades=entry["total_trades"],
                daily_values=r.get("daily_values"),
                trades=r.get("trades"),
                num_comparisons=total,
            )
            entry["flags"] = list(set(comp.get("flags", []) + aof.get("flags", [])))
            entry["composite_score"] = comp["score"]
            entry["composite_breakdown"] = comp.get("breakdown", {})
            entry["rankable"] = comp["rankable"]

            results.append(entry)

            # Publish progress periodically
            progress = 0.02 + 0.93 * ((idx + 1) / total)
            if (idx + 1) % 10 == 0 or idx == total - 1:
                _update_progress(
                    task, progress, f"Scanned {idx + 1}/{total} ({errors} errors)"
                )

        # Sort by composite score: rankable first, then descending composite_score
        results.sort(
            key=lambda x: (
                -(x.get("rankable", False)),
                -(x.get("composite_score", -999) or -999),
            ),
        )

        # Paginate
        total_results = len(results)
        start_idx = (page - 1) * per_page
        paged = results[start_idx : start_idx + per_page]

        bonf = bonferroni_correction(total_results)

        _mark_completed(
            task,
            {
                "strategy": strategy,
                "horizon": horizon,
                "start_date": params["start_date"],
                "end_date": params["end_date"],
                "total_stocks": total,
                "scanned": total - errors,
                "errors": errors,
                "total": total_results,
                "bonferroni": bonf,
                "page": page,
                "per_page": per_page,
                "items": paged,
            },
        )
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
        from datahub.app.lib.scoring_engine.verification_service import (
            ScoreVerificationService,
        )

        params = task.params or {}
        _update_progress(task, 0.1, "Running verification...")

        service = ScoreVerificationService(model_version=params.get("model_version"))
        result = service.verify_predictions(
            start_date=datetime.fromisoformat(params["start_date"])
            if params.get("start_date")
            else None,
            end_date=datetime.fromisoformat(params["end_date"])
            if params.get("end_date")
            else None,
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
        from datahub.app.lib.scoring_engine.calibration_report import (
            ScoreCalibrationReport,
        )

        params = task.params or {}
        _update_progress(task, 0.1, "Generating calibration report...")

        report = ScoreCalibrationReport(
            model_version=params.get("model_version"),
            scoring_config=params.get("scoring_config"),
        )
        result = report.generate(
            start_date=datetime.fromisoformat(params["start_date"]),
            end_date=datetime.fromisoformat(params["end_date"]),
            horizon=int(params["horizon"]),
        )

        _mark_completed(task, result)
    except ValueError as exc:
        _mark_failed(task, str(exc))
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
        weight_grid = params.get(
            "weight_grid", {}
        )  # e.g. {"signal_strength": [10,20,30], ...}
        threshold_grid = params.get(
            "threshold_grid", {"entry": [60, 70, 80], "exit": [40, 50]}
        )
        date_range = params.get(
            "date_range", {"start": "2024-01-01", "end": "2024-06-30"}
        )

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
                normalized = {
                    k: round(v / total * 100, 1) for k, v in zip(component_ids, combo)
                }
                weight_combos.append(normalized)

        # Limit to most distinct combinations
        if len(weight_combos) > 100:
            weight_combos = weight_combos[:100]

        # Build threshold combinations
        threshold_keys = list(threshold_grid.keys())
        threshold_values = [threshold_grid[k] for k in threshold_keys]
        threshold_combos = [
            dict(zip(threshold_keys, combo)) for combo in product(*threshold_values)
        ]

        # Build all experiments
        experiments = []
        for horizon in horizons:
            for wc in weight_combos:
                for tc in threshold_combos:
                    config = {"weights": wc}
                    experiments.append(
                        {
                            "horizon": horizon,
                            "config": config,
                            **tc,
                        }
                    )

        total_experiments = len(experiments)
        _update_progress(task, 0.05, f"Running {total_experiments} experiments...")

        results = []
        for idx, exp in enumerate(experiments):
            progress = 0.05 + 0.90 * (idx / total_experiments)
            _update_progress(
                task,
                progress,
                f"Experiment {idx + 1}/{total_experiments}: horizon={exp['horizon']}",
            )

            try:
                from datahub.app.lib.scoring_engine.replay_service import (
                    ScoreReplayService,
                )
                from datahub.app.lib.scoring_engine.verification_service import (
                    ScoreVerificationService,
                )
                from datahub.app.lib.scoring_engine.calibration_report import (
                    ScoreCalibrationReport,
                )

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

                report = ScoreCalibrationReport(
                    model_version=model_version,
                    scoring_config={str(exp["horizon"]): exp["config"]},
                )
                cal = report.generate(
                    start_date=datetime.fromisoformat(date_range["start"]),
                    end_date=datetime.fromisoformat(date_range["end"]),
                    horizon=exp["horizon"],
                )

                results.append(
                    {
                        "experiment_idx": idx,
                        "horizon": exp["horizon"],
                        **exp,
                        "hit_rate": cal.get("score_buckets", [{}])[-1].get("hit_rate"),
                        "top10_hit_rate": cal.get("top_n", {})
                        .get("top_10", {})
                        .get("hit_rate"),
                        "avg_return": cal.get("score_buckets", [{}])[-1].get(
                            "avg_return_at_target"
                        ),
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "experiment_idx": idx,
                        "horizon": exp["horizon"],
                        **exp,
                        "error": str(exc),
                    }
                )

        # Rank results
        ranked = sorted(
            [
                r
                for r in results
                if "error" not in r and r.get("top10_hit_rate") is not None
            ],
            key=lambda r: r.get("top10_hit_rate", 0) or 0,
            reverse=True,
        )

        _mark_completed(
            task,
            {
                "total_experiments": total_experiments,
                "completed": len([r for r in results if "error" not in r]),
                "failed": len([r for r in results if "error" in r]),
                "top_20": ranked[:20],
                "all_results": results,
            },
        )
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Factor evaluation
# ---------------------------------------------------------------------------


def _handle_factor_eval(task: Any) -> None:
    _mark_started(task)
    try:
        import datetime as _dt
        import sys as _sys
        import os as _os

        # Ensure datahub is on path for FactorEvaluationService imports
        _datahub_path = _os.path.join(
            _os.path.dirname(__file__), "..", "..", "datahub", "app"
        )
        if _datahub_path not in _sys.path:
            _sys.path.insert(0, _datahub_path)

        from app.lib.scoring_engine.factor_eval import FactorEvaluationService

        params = task.params or {}
        factor_name = params.get("factor_name", "")
        start_date = _dt.datetime.fromisoformat(params["start_date"])
        end_date = _dt.datetime.fromisoformat(params["end_date"])
        horizon = int(params.get("horizon", 20))
        stock_code = params.get("stock_code")

        svc = FactorEvaluationService()

        # Load factor values from StockFactorDaily
        from app.model.factor import StockFactorDaily

        factor_docs = list(
            StockFactorDaily.objects(date__gte=start_date, date__lte=end_date).only(
                "stock_code", "date", factor_name
            )
        )
        if not factor_docs:
            _mark_completed(task, {"error": f"No factor data for {factor_name}"})
            return

        factor_values: dict[str, dict[str, float]] = {}
        for doc in factor_docs:
            val = getattr(doc, factor_name, None)
            if val is not None:
                code = doc.stock_code
                dstr = (
                    doc.date.isoformat()
                    if hasattr(doc.date, "isoformat")
                    else str(doc.date)
                )
                factor_values.setdefault(code, {})[dstr] = float(val)

        _update_progress(
            task, 0.3, f"Running factor evaluation on {len(factor_values)} stocks..."
        )

        result = svc.evaluate(
            factor_values,
            start_date,
            end_date,
            forward_horizons=[5, 20, 60],
            regime_split=True,
        )

        # P&L attribution for a specific stock (if requested)
        attribution = None
        win_rates = None
        if stock_code:
            _update_progress(task, 0.7, "Computing component attribution...")
            attribution = svc.evaluate_component_contribution(
                stock_code, start_date, end_date, horizon=horizon
            )
            win_rates = svc.win_rate_by_component(
                stock_code, start_date, end_date, horizon=horizon
            )

        _update_progress(task, 0.9, "Saving FactorEvalReport...")

        result_data = {
            "factor_name": factor_name,
            "observation_count": result.get("observation_count", 0),
            "ic": result.get("ic", {}),
            "icir": result.get("icir", {}),
            "quintiles": result.get("quintiles", {}),
            "decay": result.get("decay", {}),
            "regime_ic": result.get("regime_ic", {}),
            "component_contribution": attribution,
            "win_rate_by_component": win_rates,
        }

        # Persist as FactorEvalReport for the API
        try:
            from app.model.factor_eval import FactorEvalReport

            report = FactorEvalReport(
                factor_name=factor_name,
                start_date=start_date,
                end_date=end_date,
                observation_count=result.get("observation_count", 0),
                ic_summary=result.get("ic", {}),
                icir_summary=result.get("icir", {}),
                quintile_analysis=result.get("quintiles", {}),
                decay_curve=result.get("decay", {}),
                regime_ic=result.get("regime_ic"),
                component_contribution=attribution,
                win_rate_by_component=win_rates,
            )
            report.save()
            result_data["report_id"] = str(report.id)
        except Exception as save_err:
            logger.warning("Failed to save FactorEvalReport: %s", save_err)

        _mark_completed(task, result_data)
    except Exception:
        _mark_failed(task, traceback.format_exc())


# ---------------------------------------------------------------------------
# Rolling validation
# ---------------------------------------------------------------------------


def _handle_rolling_validation(task: Any) -> None:
    """Rolling cross-validation: train on year Y, test on year Y+1, slide forward.

    Compares calibration metrics (hit rates) between training and test periods
    to detect overfitting.  Each window replays scores, verifies outcomes, and
    generates calibration reports for both periods.  Windows where the test
    hit_rate drops >20% versus train are flagged as overfit.
    """
    _mark_started(task)
    try:
        from datahub.app.lib.scoring_engine.replay_service import ScoreReplayService
        from datahub.app.lib.scoring_engine.verification_service import (
            ScoreVerificationService,
        )
        from datahub.app.lib.scoring_engine.calibration_report import (
            ScoreCalibrationReport,
        )

        params = task.params or {}
        model_version = params.get("model_version", "score_v2_202605")
        start_year = int(params.get("start_year", 2020))
        end_year = int(params.get("end_year", 2024))
        horizons = params.get("horizons", [20])
        step_years = int(params.get("step_years", 1))

        # Optional single-stock filter – backfill_predictions only accepts
        # one stock_code, so pass it when present.
        stock_code = params.get("stock_code")

        windows = []
        overfit_flags = []

        # Total slide steps for progress calculation
        total_steps = max((end_year - start_year) // step_years, 1)

        for step_idx, train_year in enumerate(range(start_year, end_year, step_years)):
            test_year = train_year + 1
            if test_year > end_year:
                break

            progress = (step_idx / total_steps) if total_steps else 0
            _update_progress(
                task,
                progress,
                f"Validating: train={train_year}, test={test_year}",
            )

            train_start = datetime(train_year, 1, 1)
            train_end = datetime(train_year, 12, 31)
            test_start = datetime(test_year, 1, 1)
            test_end = datetime(test_year, 12, 31)

            # Store test predictions under a window-specific version so they
            # don't pollute the canonical model_version in the database.
            test_model_version = f"{model_version}_rolling_test_{test_year}"

            window_result = {
                "train_year": train_year,
                "test_year": test_year,
                "horizons": {},
            }

            for horizon in horizons:
                # --- Train period ---
                replay = ScoreReplayService(model_version=model_version)
                replay.backfill_predictions(
                    train_start,
                    train_end,
                    horizon=horizon,
                    stock_code=stock_code,
                    replace=True,
                )

                verifier = ScoreVerificationService(model_version=model_version)
                verifier.verify_predictions(
                    train_start,
                    train_end,
                    horizon=horizon,
                )

                report = ScoreCalibrationReport(model_version=model_version)
                train_cal = report.generate(train_start, train_end, horizon)

                # --- Test period ---
                test_replay = ScoreReplayService(model_version=test_model_version)
                test_replay.backfill_predictions(
                    test_start,
                    test_end,
                    horizon=horizon,
                    stock_code=stock_code,
                    replace=True,
                )

                test_verifier = ScoreVerificationService(
                    model_version=test_model_version,
                )
                test_verifier.verify_predictions(
                    test_start,
                    test_end,
                    horizon=horizon,
                )

                test_report_gen = ScoreCalibrationReport(
                    model_version=test_model_version,
                )
                test_cal = test_report_gen.generate(test_start, test_end, horizon)

                # Hit rate from the highest score bucket (80-100)
                train_buckets = train_cal.get("score_buckets", [])
                test_buckets = test_cal.get("score_buckets", [])

                train_hit = (
                    (train_buckets[-1].get("hit_rate") or 0) if train_buckets else 0
                )
                test_hit = (
                    (test_buckets[-1].get("hit_rate") or 0) if test_buckets else 0
                )

                decay = (
                    (train_hit - test_hit) / max(train_hit, 0.001)
                    if train_hit > 0
                    else 0
                )
                overfit = decay > 0.20
                if overfit:
                    overfit_flags.append(
                        f"Horizon {horizon}: train {train_year} -> "
                        f"test {test_year} hit_rate dropped "
                        f"{decay * 100:.1f}%"
                    )

                window_result["horizons"][str(horizon)] = {
                    "train_hit_rate": round(train_hit, 6),
                    "test_hit_rate": round(test_hit, 6),
                    "decay": round(decay, 4),
                    "overfit": overfit,
                    "test_model_version": test_model_version,
                }

            windows.append(window_result)

        _mark_completed(
            task,
            {
                "model_version": model_version,
                "windows": windows,
                "overfit_flags": overfit_flags,
                "verdict": (
                    "Overfit detected" if overfit_flags else "Stable across windows"
                ),
            },
        )
    except Exception:
        _mark_failed(task, traceback.format_exc())
