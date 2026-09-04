# -*- coding: utf-8 -*-
"""Backtest API blueprint for the MVP single-stock daily backtesting feature."""

import csv
import io
import logging
import statistics
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from flask import Blueprint, Response, jsonify, request
from mongoengine import ValidationError

from app.model.backtest import BacktestResult
from app.services.backtest_service import (
    run_backtest,
    run_multi_stock_backtest,
    composite_score,
    bonferroni_correction,
    SCORE_DRIVEN_STRATEGIES,
    UNUSABLE_SCORE_STATUSES,
)
from app.lib.auth_decorators import block_service_tokens

logger = logging.getLogger(__name__)

backtest_bp = Blueprint("backtest", __name__, url_prefix="/api/backtest")
backtest_bp.before_request(block_service_tokens)

SCAN_ASYNC_THRESHOLD = 100

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id() -> str:
    return str(uuid.uuid4())


def _ok(data: Any = None, message: str = "ok") -> Dict[str, Any]:
    return {
        "success": True,
        "message": message,
        "request_id": _request_id(),
        "generated_at": _now_utc(),
        "data": data,
    }


def _fail(message: str, status_code: int = 400) -> tuple:
    return (
        jsonify(
            {
                "success": False,
                "message": message,
                "request_id": _request_id(),
                "generated_at": _now_utc(),
                "data": None,
            }
        ),
        status_code,
    )


def _build_csv(rows: list, fields: list[str], headers: list[str]) -> Response:
    """Build a CSV response from rows of dicts."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row.get(f, "") for f in fields])
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=backtest_export.csv"},
    )


def _parse_date(value: Any) -> datetime | None:
    """Parse a YYYY-MM-DD string to a day-resolution datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    text = str(value).strip().replace("Z", "+00:00")
    if not text:
        return None
    if len(text) == 10:
        try:
            return datetime.fromisoformat(f"{text}T00:00:00")
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(text).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    except ValueError:
        return None


def _parse_float(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(
    value: Any, default: int, minimum: int = 0, maximum: int | None = None
) -> int:
    if value is None:
        return default
    try:
        v = int(value)
        v = max(v, minimum)
        if maximum is not None:
            v = min(v, maximum)
        return v
    except (TypeError, ValueError):
        return default


def _model_version(payload: dict) -> str | None:
    return (payload.get("model_version") or "").strip() or None


def _model_version_error(strategy: str, model_version: str | None):
    if strategy.strip().upper() in SCORE_DRIVEN_STRATEGIES and not model_version:
        return _fail("model_version is required for score-driven strategies")
    return None


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _serialize_result(
    row: BacktestResult, include_details: bool = False
) -> Dict[str, Any]:
    payload = {
        "id": str(row.id),
        "name": row.name,
        "stock_code": row.stock_code,
        "stock_name": row.stock_name,
        "strategy": row.strategy,
        "start_date": _format_dt(row.start_date),
        "end_date": _format_dt(row.end_date),
        "initial_cash": row.initial_cash,
        "final_value": row.final_value,
        "total_return": row.total_return,
        "total_return_pct": row.total_return_pct,
        "annualized_return": row.annualized_return,
        "max_drawdown": row.max_drawdown,
        "max_drawdown_duration": row.max_drawdown_duration,
        "sharpe_ratio": row.sharpe_ratio,
        "win_rate": row.win_rate,
        "total_trades": row.total_trades,
        "profit_trades": row.profit_trades,
        "loss_trades": row.loss_trades,
        "best_trade": row.best_trade,
        "worst_trade": row.worst_trade,
        "status": row.status,
        "error_message": row.error_message,
        # Friction costs
        "total_commission": row.total_commission,
        "total_stamp_duty": row.total_stamp_duty,
        "total_slippage": row.total_slippage,
        "gross_return": row.gross_return,
        "gross_return_pct": row.gross_return_pct,
        # Benchmark comparison
        "benchmark_code": row.benchmark_code,
        "benchmark_return": row.benchmark_return,
        "benchmark_return_pct": row.benchmark_return_pct,
        "benchmark_annualized_return": row.benchmark_annualized_return,
        "benchmark_daily_values": getattr(row, "benchmark_daily_values", None) or [],
        "excess_return": row.excess_return,
        "excess_return_pct": row.excess_return_pct,
        "information_ratio": row.information_ratio,
        "horizon": row.horizon,
        "model_version": getattr(row, "model_version", None),
        "score_config": row.score_config,
        "data_coverage": getattr(row, "data_coverage", None) or {},
        "created_at": _format_dt(row.created_at),
        "completed_at": _format_dt(row.completed_at),
    }
    if include_details:
        payload["trades"] = [
            {
                "date": t.get("date"),
                "side": t.get("side"),
                "price": t.get("price"),
                "exec_price": t.get("exec_price"),
                "quantity": t.get("quantity"),
                "amount": t.get("amount"),
                "commission": t.get("commission"),
                "stamp_duty": t.get("stamp_duty"),
                "slippage": t.get("slippage"),
                "pnl": t.get("pnl"),
                "reason": t.get("reason"),
                "stock_code": t.get("stock_code"),
            }
            for t in (row.trades or [])
        ]
        payload["daily_values"] = [
            {
                "date": dv.get("date"),
                "close": dv.get("close"),
                "cash": dv.get("cash"),
                "shares": dv.get("shares"),
                "equity": dv.get("equity"),
                "value": dv.get("equity"),  # alias for total assets
                "positions_value": dv.get(
                    "positions_value",
                    round((dv.get("shares", 0) or 0) * (dv.get("close", 0) or 0), 4),
                ),
                "position_count": dv.get("position_count"),
                "positions": dv.get("positions"),
            }
            for dv in (row.daily_values or [])
        ]
        # Include per-stock contributions for multi-stock backtests
        if row.per_stock_contributions:
            payload["per_stock_contributions"] = [
                {
                    "stock_code": psc.get("stock_code"),
                    "stock_name": psc.get("stock_name"),
                    "realized_pnl": psc.get("realized_pnl"),
                    "trades": psc.get("trades"),
                }
                for psc in row.per_stock_contributions
            ]
    # Include multi-stock strategy params even without details
    if row.top_n is not None:
        payload["top_n"] = row.top_n
    if row.rebalance_interval is not None:
        payload["rebalance_interval"] = row.rebalance_interval
    if row.allocation:
        payload["allocation"] = row.allocation
    return payload


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@backtest_bp.route("/run", methods=["POST"])
def run():
    """Run a new backtest and return the result.

    Request body (JSON):
        stock_code    : str (required)   e.g. "sh600519"
        strategy      : str (required)   "MA_CROSS", "BUY_HOLD",
                                         "SCORE_THRESHOLD", or "SCORE_MOMENTUM"
        start_date    : str (required)   YYYY-MM-DD
        end_date      : str (required)   YYYY-MM-DD
        initial_cash  : float (optional) default 100000
        benchmark_code: str (optional)   default "sh000300"
        horizon       : int (optional)   scoring horizon (5/20/60), required for
                                         score-driven strategies
        entry_threshold : float (optional) default 70.0 (SCORE_THRESHOLD)
        exit_threshold  : float (optional) default 50.0 (SCORE_THRESHOLD)
        stop_loss_pct   : float (optional) default -5.0
        score_delta     : float (optional) default 10.0 (SCORE_MOMENTUM)
        model_version   : str (optional)  scoring model version filter
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    strategy = (payload.get("strategy") or "").strip()
    start_date_raw = payload.get("start_date")
    end_date_raw = payload.get("end_date")
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    benchmark_code = (payload.get("benchmark_code") or "sh000300").strip()

    horizon = _parse_int(payload.get("horizon"), None)  # optional, for score-driven
    entry_threshold = _parse_float(payload.get("entry_threshold"), 70.0)
    exit_threshold = _parse_float(payload.get("exit_threshold"), 50.0)
    stop_loss_pct = _parse_float(payload.get("stop_loss_pct"), -5.0)
    score_delta = _parse_float(payload.get("score_delta"), 10.0)
    model_version = _model_version(payload)

    # Validate required fields
    if not stock_code:
        return _fail("stock_code is required")
    if not strategy:
        return _fail("strategy is required")

    strategy_norm = strategy.strip().upper()
    version_error = _model_version_error(strategy_norm, model_version)
    if version_error:
        return version_error
    if strategy_norm in ("SCORE_THRESHOLD", "SCORE_MOMENTUM"):
        if horizon is None or horizon not in (5, 20, 60):
            return _fail(
                "horizon (5, 20, or 60) is required for score-driven strategies"
            )
    elif strategy_norm == "MULTI_HORIZON_CONSENSUS":
        pass  # no single horizon required — uses all three

    # Normalize and validate consensus threshold dict keys (JSON keys are strings)
    def _normalize_threshold_dict(raw: dict | None, label: str) -> dict | None:
        if not isinstance(raw, dict):
            return None
        out = {}
        for k, v in raw.items():
            try:
                key = int(k)
            except (ValueError, TypeError):
                return None
            if key not in {5, 20, 60}:
                return None
            try:
                val = float(v)
            except (ValueError, TypeError):
                return None
            import math

            if math.isnan(val) or math.isinf(val):
                return None
            out[key] = val
        return out

    consensus_entry = _normalize_threshold_dict(
        payload.get("consensus_entry_thresholds"), "consensus_entry_thresholds"
    )
    consensus_exit = _normalize_threshold_dict(
        payload.get("consensus_exit_thresholds"), "consensus_exit_thresholds"
    )
    if consensus_entry is None and isinstance(
        payload.get("consensus_entry_thresholds"), dict
    ):
        return _fail("consensus_entry_thresholds keys must be 5, 20, or 60")
    if consensus_exit is None and isinstance(
        payload.get("consensus_exit_thresholds"), dict
    ):
        return _fail("consensus_exit_thresholds keys must be 5, 20, or 60")

    start_date = _parse_date(start_date_raw)
    if start_date is None:
        return _fail("start_date is required and must be YYYY-MM-DD")
    end_date = _parse_date(end_date_raw)
    if end_date is None:
        return _fail("end_date is required and must be YYYY-MM-DD")
    if start_date > end_date:
        return _fail("start_date must be <= end_date")
    if initial_cash <= 0:
        return _fail("initial_cash must be > 0")

    logger.info(
        "Running backtest: stock=%s strategy=%s range=%s..%s cash=%s horizon=%s",
        stock_code,
        strategy,
        start_date.date(),
        end_date.date(),
        initial_cash,
        horizon,
    )

    result = run_backtest(
        stock_code=stock_code,
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        initial_cash=initial_cash,
        save_result=True,
        benchmark_code=benchmark_code,
        horizon=horizon,
        entry_threshold=entry_threshold,
        exit_threshold=exit_threshold,
        stop_loss_pct=stop_loss_pct,
        score_delta=score_delta,
        model_version=model_version,
        consensus_entry_thresholds=consensus_entry,
        consensus_exit_thresholds=consensus_exit,
    )

    if "error" in result:
        return _fail(f"{result['error']}: {result.get('detail', '')}")

    return jsonify(_ok(data=result)), 200


@backtest_bp.route("/run-multi", methods=["POST"])
def run_multi():
    """Run a multi-stock portfolio backtest and return the result.

    Request body (JSON):
        stock_codes        : list[str]  (required) e.g. ["sh600519", "sz000858"]
        strategy           : str        (required) "TOP_N_ROTATION"
        start_date         : str        (required) YYYY-MM-DD
        end_date           : str        (required) YYYY-MM-DD
        initial_cash       : float      (optional) default 100000
        benchmark_code     : str        (optional) default "sh000300"
        horizon            : int        (required for TOP_N_ROTATION) 5/20/60
        top_n              : int        (optional) default 10
        rebalance_interval : int        (optional) default 5
        allocation         : str        (optional) "equal_weight" or "score_weighted"
        max_position_pct   : float      (optional) default 0.20
        stop_loss_pct      : float      (optional) default -5.0
        model_version      : str        (optional)
    """
    payload = request.get_json(silent=True) or {}

    strategy = (payload.get("strategy") or "").strip()
    start_date_raw = payload.get("start_date")
    end_date_raw = payload.get("end_date")
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    benchmark_code = (payload.get("benchmark_code") or "sh000300").strip()

    stock_codes = payload.get("stock_codes")

    horizon = _parse_int(payload.get("horizon"), None)
    top_n = _parse_int(payload.get("top_n"), 10, minimum=1)
    rebalance_interval = _parse_int(payload.get("rebalance_interval"), 5, minimum=1)
    allocation = (payload.get("allocation") or "equal_weight").strip().lower()
    max_position_pct = _parse_float(payload.get("max_position_pct"), 0.20)
    stop_loss_pct = _parse_float(payload.get("stop_loss_pct"), -5.0)
    entry_threshold = _parse_float(payload.get("entry_threshold"), 70.0)
    exit_threshold = _parse_float(payload.get("exit_threshold"), 50.0)
    score_delta = _parse_float(payload.get("score_delta"), 10.0)
    model_version = _model_version(payload)

    # Validate required fields
    if not isinstance(stock_codes, list) or len(stock_codes) < 2:
        return _fail("stock_codes must be a list with at least 2 codes")
    if len(stock_codes) > 100:
        return _fail("stock_codes: maximum 100 codes allowed")
    if not strategy:
        return _fail("strategy is required")

    strategy_norm = strategy.strip().upper()
    version_error = _model_version_error(strategy_norm, model_version)
    if version_error:
        return version_error
    if strategy_norm != "TOP_N_ROTATION":
        return _fail("strategy must be TOP_N_ROTATION for multi-stock backtests")
    if horizon is None or horizon not in (5, 20, 60):
        return _fail("horizon (5, 20, or 60) is required for TOP_N_ROTATION")
    if allocation not in ("equal_weight", "score_weighted"):
        return _fail("allocation must be 'equal_weight' or 'score_weighted'")

    start_date = _parse_date(start_date_raw)
    if start_date is None:
        return _fail("start_date is required and must be YYYY-MM-DD")
    end_date = _parse_date(end_date_raw)
    if end_date is None:
        return _fail("end_date is required and must be YYYY-MM-DD")
    if start_date > end_date:
        return _fail("start_date must be <= end_date")
    if initial_cash <= 0:
        return _fail("initial_cash must be > 0")

    logger.info(
        "Running multi-stock backtest: stocks=%s strategy=%s range=%s..%s horizon=%s top_n=%s",
        stock_codes,
        strategy,
        start_date.date(),
        end_date.date(),
        horizon,
        top_n,
    )

    result = run_multi_stock_backtest(
        stock_codes=stock_codes,
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        initial_cash=initial_cash,
        save_result=True,
        benchmark_code=benchmark_code,
        horizon=horizon,
        entry_threshold=entry_threshold,
        exit_threshold=exit_threshold,
        stop_loss_pct=stop_loss_pct,
        score_delta=score_delta,
        model_version=model_version,
        top_n=top_n,
        rebalance_interval=rebalance_interval,
        allocation=allocation,
        max_position_pct=max_position_pct,
    )

    if "error" in result:
        return _fail(f"{result['error']}: {result.get('detail', '')}")

    return jsonify(_ok(data=result)), 200


@backtest_bp.route("/optimize", methods=["POST"])
def optimize():
    """Run parameter sweep and return the best configuration.

    Request body (JSON):
        stock_code    : str (required)   e.g. "sz000977"
        strategy      : str (required)   "SCORE_THRESHOLD", "SCORE_MOMENTUM"
        start_date    : str (required)   YYYY-MM-DD
        end_date      : str (required)   YYYY-MM-DD
        param_grid    : dict (required)  e.g. {"entry_threshold":[50,60,70]}
        initial_cash  : float (optional) default 100000
        horizon       : int (required)   5, 20, or 60
        use_split     : bool (optional)  default True — use train/val/test split
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    strategy = (payload.get("strategy") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizon = _parse_int(payload.get("horizon"), None)
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    param_grid = payload.get("param_grid") or {}
    use_split = payload.get("use_split", True)
    model_version = _model_version(payload)

    if not stock_code or not strategy:
        return _fail("stock_code and strategy are required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    if not isinstance(param_grid, dict) or not param_grid:
        return _fail("param_grid must be a non-empty dict")
    # Validate each value is a non-empty iterable
    for key, vals in param_grid.items():
        if not isinstance(vals, (list, tuple)) or len(vals) == 0:
            return _fail(f"param_grid['{key}'] must be a non-empty list")
    if horizon is None or horizon not in (5, 20, 60):
        return _fail("horizon (5, 20, or 60) is required for optimization")

    strategy_norm = strategy.strip().upper()
    if strategy_norm not in ("SCORE_THRESHOLD", "SCORE_MOMENTUM"):
        return _fail("optimize supports SCORE_THRESHOLD and SCORE_MOMENTUM only")
    version_error = _model_version_error(strategy_norm, model_version)
    if version_error:
        return version_error

    # --- Train/val/test split ---
    if use_split:
        total_days = (end_date - start_date).days
        if total_days < 300:
            logger.warning(
                "Date range has only ~%s calendar days — "
                "train/val/test split may be unreliable",
                total_days,
            )
        val_end_ratio = 0.8
        val_end = start_date + (end_date - start_date) * val_end_ratio
    else:
        val_end = end_date

    # --- Strategy-specific param_grid validation ---
    VALID_PARAM_KEYS = {
        "SCORE_THRESHOLD": {"entry_threshold", "exit_threshold", "stop_loss_pct"},
        "SCORE_MOMENTUM": {"score_delta", "stop_loss_pct"},
    }
    allowed = VALID_PARAM_KEYS.get(strategy_norm, set())
    for key in param_grid:
        if key not in allowed:
            return _fail(
                f"Unknown param '{key}' for strategy '{strategy_norm}'."
                f" Allowed: {sorted(allowed)}"
            )

    # --- Generate parameter combinations ---
    grid_keys = list(param_grid.keys())

    def _combos(keys, base=None):
        if base is None:
            base = {}
        if not keys:
            return [base]
        results = []
        key = keys[0]
        for val in param_grid[key]:
            nb = {**base, key: val}
            results.extend(_combos(keys[1:], nb))
        return results

    combos = _combos(grid_keys)
    logger.info(
        "Optimizing %s on %s with %d combinations",
        strategy,
        stock_code,
        len(combos),
    )

    # --- Run backtests (train+val for selection, test for final) ---
    results = []
    best_result = None
    best_val_sharpe = float("-inf")

    for combo in combos:
        entry = combo.get("entry_threshold", 70.0)
        exit_t = combo.get("exit_threshold", 50.0)
        stop = combo.get("stop_loss_pct", -5.0)
        delta = combo.get("score_delta", 10.0)

        # Train+validation run (60%+20% for parameter selection)
        selection_end = val_end if use_split else end_date
        r = run_backtest(
            stock_code=stock_code,
            strategy=strategy,
            start_date=start_date,
            end_date=selection_end,
            initial_cash=initial_cash,
            save_result=False,
            horizon=horizon,
            entry_threshold=entry,
            exit_threshold=exit_t,
            stop_loss_pct=stop,
            score_delta=delta,
            model_version=model_version,
        )
        if "error" in r:
            results.append({"params": combo, "error": r["error"]})
            continue

        result_entry = {
            "params": combo,
            "val_sharpe_ratio": r.get("sharpe_ratio", 0),
            "val_return_pct": r.get("total_return_pct", 0),
            "val_max_drawdown": r.get("max_drawdown", 0),
            "val_trades": r.get("total_trades", 0),
            "val_excess_return_pct": r.get("excess_return_pct", 0),
        }

        # Run on test period if using split
        if use_split:
            test_start = val_end + timedelta(days=1)
            test_r = run_backtest(
                stock_code=stock_code,
                strategy=strategy,
                start_date=test_start,
                end_date=end_date,
                initial_cash=initial_cash,
                save_result=False,
                horizon=horizon,
                entry_threshold=entry,
                exit_threshold=exit_t,
                stop_loss_pct=stop,
                score_delta=delta,
                model_version=model_version,
            )
            if "error" not in test_r:
                result_entry["test_sharpe_ratio"] = test_r.get("sharpe_ratio", 0)
                result_entry["test_return_pct"] = test_r.get("total_return_pct", 0)

        results.append(result_entry)

        # Select best by train+val Sharpe
        val_sharpe = result_entry.get("val_sharpe_ratio", 0) or 0
        if val_sharpe > best_val_sharpe:
            best_val_sharpe = val_sharpe
            best_result = result_entry

    # Sort results by val Sharpe
    results.sort(
        key=lambda x: x.get("val_sharpe_ratio", 0) or 0,
        reverse=True,
    )

    return jsonify(
        _ok(
            data={
                "stock_code": stock_code,
                "strategy": strategy,
                "horizon": horizon,
                "date_range": {
                    "selection_end": val_end.isoformat(),
                    "test_start": (val_end + timedelta(days=1)).isoformat(),
                    "test_end": end_date.isoformat(),
                }
                if use_split
                else {"full_range_end": end_date.isoformat()},
                "split_used": use_split,
                "total_combinations": len(combos),
                "completed": len([r for r in results if "error" not in r]),
                "best": best_result,
                "results": results,
            }
        )
    ), 200


@backtest_bp.route("/compare", methods=["POST"])
def compare():
    """Compare all eligible strategies on a single stock.

    Request body (JSON):
        stock_code    : str (required)
        start_date    : str (required)   YYYY-MM-DD
        end_date      : str (required)   YYYY-MM-DD
        initial_cash  : float (optional) default 100000
        benchmark_code: str (optional)   default "sh000300"
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    benchmark_code = (payload.get("benchmark_code") or "sh000300").strip()
    model_version = _model_version(payload)

    if not stock_code:
        return _fail("stock_code is required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    if start_date > end_date:
        return _fail("start_date must be <= end_date")

    # Determine which strategies are eligible based on data availability
    # Always eligible: BUY_HOLD, MA_CROSS
    strategies = [
        ("BUY_HOLD", {}),
        ("MA_CROSS", {}),
    ]

    # Score-driven strategies require score data and an explicit model version.
    has_scores = bool(model_version) and _check_score_data_available(
        stock_code, start_date, end_date, model_version
    )
    if has_scores:
        for horizon in (5, 20, 60):
            strategies.append(
                (
                    "SCORE_THRESHOLD",
                    {"horizon": horizon, "label": f"SCORE_THRESHOLD Score{horizon}"},
                )
            )
            strategies.append(
                (
                    "SCORE_MOMENTUM",
                    {"horizon": horizon, "label": f"SCORE_MOMENTUM Score{horizon}"},
                )
            )
        strategies.append(
            ("MULTI_HORIZON_CONSENSUS", {"label": "MULTI_HORIZON_CONSENSUS"})
        )

    results = []
    best_sharpe = float("-inf")
    best_strategy = None

    for strategy, opts in strategies:
        try:
            r = run_backtest(
                stock_code=stock_code,
                strategy=strategy,
                start_date=start_date,
                end_date=end_date,
                initial_cash=initial_cash,
                save_result=False,
                benchmark_code=benchmark_code,
                horizon=opts.get("horizon"),
                model_version=model_version
                if strategy in SCORE_DRIVEN_STRATEGIES
                else None,
            )
        except Exception as exc:
            results.append(
                {
                    "strategy": opts.get("label", strategy),
                    "error": str(exc),
                }
            )
            continue

        if "error" in r:
            results.append(
                {
                    "strategy": opts.get("label", strategy),
                    "error": r["error"],
                }
            )
            continue

        entry = {
            "strategy": opts.get("label", strategy),
            "total_return_pct": r.get("total_return_pct", 0),
            "sharpe_ratio": r.get("sharpe_ratio", 0),
            "max_drawdown": r.get("max_drawdown", 0),
            "win_rate": r.get("win_rate", 0),
            "total_trades": r.get("total_trades", 0),
            "excess_return_pct": r.get("excess_return_pct", 0),
            "information_ratio": r.get("information_ratio", 0),
        }

        # Composite score and anti-overfitting flags
        from app.services.backtest_service import (
            anti_overfitting_flags,
            composite_score,
        )

        comp = composite_score(
            excess_return_pct=entry["excess_return_pct"],
            max_drawdown=entry["max_drawdown"],
            information_ratio=entry["information_ratio"],
            total_trades=entry["total_trades"],
            daily_values=r.get("daily_values"),
            trades=r.get("trades"),
        )
        flags = anti_overfitting_flags(
            total_trades=entry["total_trades"],
            daily_values=r.get("daily_values"),
            trades=r.get("trades"),
        )

        entry["composite_score"] = comp["score"]
        entry["composite_breakdown"] = comp.get("breakdown", {})
        entry["flags"] = list(set(comp.get("flags", []) + flags.get("flags", [])))
        entry["rankable"] = comp["rankable"]

        results.append(entry)

        if comp["rankable"] and comp["score"] > best_sharpe:
            best_sharpe = comp["score"]
            best_strategy = entry["strategy"]

    # Sort by composite score (rankable first)
    results.sort(
        key=lambda x: (
            -(x.get("rankable", False)),
            -(x.get("composite_score", -999) or -999),
        ),
    )

    # Bonferroni correction metadata for multi-strategy comparison
    bonf = bonferroni_correction(len(results))

    return jsonify(
        _ok(
            data={
                "stock_code": stock_code,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "comparison_count": len(results),
                "best_strategy": best_strategy,
                "bonferroni": bonf,
                "results": results,
            }
        )
    ), 200


def _check_score_data_available(
    stock_code, start_date, end_date, model_version: str
) -> bool:
    """Check if any StockScorePrediction exists for the stock in range."""
    try:
        from app.model.scoring import StockScorePrediction

        return (
            StockScorePrediction.objects(
                stock_code=stock_code,
                date__gte=start_date,
                date__lte=end_date,
                model_version=model_version,
                status__nin=UNUSABLE_SCORE_STATUSES,
            ).count()
            > 0
        )
    except Exception:
        return False


@backtest_bp.route("/scan", methods=["POST"])
def scan():
    """Scan one strategy across all active stocks.

    Request body (JSON):
        strategy      : str (required)
        start_date    : str (required)
        end_date      : str (required)
        horizon       : int (required for score-driven strategies)
        initial_cash  : float (optional) default 100000
        page          : int (optional)   default 1
        per_page      : int (optional)   default 20, max 200
        min_trades    : int (optional)   filter: minimum trade count
    """
    payload = request.get_json(silent=True) or {}

    strategy = (payload.get("strategy") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizon = _parse_int(payload.get("horizon"), None)
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    page = _parse_int(payload.get("page"), 1, minimum=1)
    per_page = _parse_int(payload.get("per_page"), 20, minimum=1, maximum=200)
    min_trades = _parse_int(payload.get("min_trades"), 0, minimum=0)
    model_version = _model_version(payload)

    if not strategy:
        return _fail("strategy is required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")

    strategy_norm = strategy.strip().upper()
    if strategy_norm not in (
        "MA_CROSS",
        "BUY_HOLD",
        "SCORE_THRESHOLD",
        "SCORE_MOMENTUM",
        "MULTI_HORIZON_CONSENSUS",
    ):
        return _fail(f"Unsupported strategy: {strategy_norm}")
    if strategy_norm in ("SCORE_THRESHOLD", "SCORE_MOMENTUM"):
        if horizon is None or horizon not in (5, 20, 60):
            return _fail("horizon is required for score-driven strategies")
    version_error = _model_version_error(strategy_norm, model_version)
    if version_error:
        return version_error

    # Get active stocks
    try:
        from app.model.stock import IndividualStock

        stocks = list(
            IndividualStock.objects(active_status=0).only("code", "name").limit(3000)
        )
    except Exception:
        stocks = []

    if not stocks:
        return _fail("No active stocks found")

    total = len(stocks)

    # ---- async dispatch for large scans ----
    async_mode = request.args.get("async", "").lower().strip()
    if not async_mode:
        async_mode = (payload.get("async") or "").lower().strip()

    should_dispatch = async_mode == "force" or (
        async_mode != "false" and total > SCAN_ASYNC_THRESHOLD
    )

    if should_dispatch:
        from app.model.compute_task import ComputeTask

        task = ComputeTask(
            task_type="BACKTEST_SCAN",
            params={
                "strategy": strategy_norm,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "horizon": horizon,
                "initial_cash": initial_cash,
                "min_trades": min_trades,
                "page": page,
                "per_page": per_page,
                "model_version": model_version,
            },
        )
        task.save()
        return (
            jsonify(
                _ok(
                    data={
                        "task_id": str(task.id),
                        "status": "PENDING",
                        "total_stocks": total,
                        "note": (
                            f"Scan dispatched asynchronously "
                            f"({total} stocks > {SCAN_ASYNC_THRESHOLD} threshold). "
                            f"Poll GET /api/tasks/{task.id} for results."
                        ),
                    }
                )
            ),
            202,
        )
    # ---- end async dispatch ----

    results = []
    errors = 0

    for stock in stocks:
        try:
            r = run_backtest(
                stock_code=stock.code,
                strategy=strategy_norm,
                start_date=start_date,
                end_date=end_date,
                initial_cash=initial_cash,
                save_result=False,
                horizon=horizon,
                model_version=model_version,
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

        # Anti-overfitting flags
        from app.services.backtest_service import anti_overfitting_flags

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

    return jsonify(
        _ok(
            data={
                "strategy": strategy_norm,
                "horizon": horizon,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_stocks": total,
                "scanned": total - errors,
                "errors": errors,
                "total": total_results,
                "bonferroni": bonf,
                "page": page,
                "per_page": per_page,
                "items": paged,
            }
        )
    ), 200


@backtest_bp.route("/walk-forward", methods=["POST"])
def walk_forward():
    """Rolling-window validation of a strategy.

    Request body (JSON):
        stock_code    : str (required)
        strategy      : str (required)
        start_date    : str (required)
        end_date      : str (required)
        window_days   : int (optional)  default 120 trading days
        step_days     : int (optional)  default 60 trading days
        horizon       : int (optional)  for score-driven strategies
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    strategy = (payload.get("strategy") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizon = _parse_int(payload.get("horizon"), None)
    window_days = _parse_int(payload.get("window_days"), 120, minimum=40)
    step_days = _parse_int(payload.get("step_days"), 60, minimum=10)
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    model_version = _model_version(payload)

    if not stock_code or not strategy:
        return _fail("stock_code and strategy are required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    version_error = _model_version_error(strategy, model_version)
    if version_error:
        return version_error

    # Load trading days
    quotes = _load_quotes_helper(stock_code, start_date, end_date)
    if not quotes:
        return _fail("No quote data for this stock in range")

    trading_days = sorted(
        {q.date.replace(hour=0, minute=0, second=0, microsecond=0) for q in quotes}
    )
    if len(trading_days) < window_days:
        return _fail(
            f"Need at least {window_days} trading days, got {len(trading_days)}"
        )

    windows = []
    start_idx = 0
    while start_idx + window_days <= len(trading_days):
        end_idx = start_idx + window_days - 1
        w_start = trading_days[start_idx]
        w_end = trading_days[end_idx]

        try:
            r = run_backtest(
                stock_code=stock_code,
                strategy=strategy.strip().upper(),
                start_date=w_start,
                end_date=w_end,
                initial_cash=initial_cash,
                save_result=False,
                horizon=horizon,
                model_version=model_version,
            )
        except Exception:
            start_idx += max(1, step_days)
            continue

        if "error" not in r:
            windows.append(
                {
                    "start_date": w_start.isoformat(),
                    "end_date": w_end.isoformat(),
                    "total_return_pct": r.get("total_return_pct", 0),
                    "sharpe_ratio": r.get("sharpe_ratio", 0),
                    "max_drawdown": r.get("max_drawdown", 0),
                    "total_trades": r.get("total_trades", 0),
                    "win_rate": r.get("win_rate", 0),
                }
            )

        start_idx += max(1, step_days)

    # Stability analysis
    sharpes = [w["sharpe_ratio"] for w in windows if w["sharpe_ratio"] is not None]
    stability_score = None
    performance_decay = False
    if len(sharpes) >= 6:
        mid = len(sharpes) // 2
        first_half_sharpe = sum(sharpes[:mid]) / mid if sharpes[:mid] else 0
        second_half_sharpe = (
            sum(sharpes[mid:]) / (len(sharpes) - mid) if sharpes[mid:] else 0
        )
        if first_half_sharpe != 0:
            decay = (first_half_sharpe - second_half_sharpe) / abs(first_half_sharpe)
            performance_decay = decay > 0.2

        import statistics

        stability_score = (
            round(statistics.stdev(sharpes), 4) if len(sharpes) >= 2 else None
        )

    return jsonify(
        _ok(
            data={
                "stock_code": stock_code,
                "strategy": strategy.strip().upper(),
                "horizon": horizon,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_windows": len(windows),
                "stability_score": stability_score,
                "performance_decay": performance_decay,
                "windows": windows,
            }
        )
    ), 200


@backtest_bp.route("/export/compare", methods=["POST"])
def export_compare_csv():
    """CSV export for strategy comparison results."""
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    benchmark_code = (payload.get("benchmark_code") or "sh000300").strip()
    model_version = _model_version(payload)

    if not stock_code:
        return _fail("stock_code is required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    if start_date > end_date:
        return _fail("start_date must be <= end_date")

    from app.services.backtest_service import anti_overfitting_flags

    strategies = [("BUY_HOLD", {}), ("MA_CROSS", {})]
    has_scores = bool(model_version) and _check_score_data_available(
        stock_code, start_date, end_date, model_version
    )
    if has_scores:
        for horizon in (5, 20, 60):
            strategies.append(
                (
                    "SCORE_THRESHOLD",
                    {"horizon": horizon, "label": f"SCORE_THRESHOLD Score{horizon}"},
                )
            )
            strategies.append(
                (
                    "SCORE_MOMENTUM",
                    {"horizon": horizon, "label": f"SCORE_MOMENTUM Score{horizon}"},
                )
            )
        strategies.append(
            ("MULTI_HORIZON_CONSENSUS", {"label": "MULTI_HORIZON_CONSENSUS"})
        )

    rows = []
    for strategy, opts in strategies:
        try:
            r = run_backtest(
                stock_code=stock_code,
                strategy=strategy,
                start_date=start_date,
                end_date=end_date,
                initial_cash=initial_cash,
                save_result=False,
                benchmark_code=benchmark_code,
                horizon=opts.get("horizon"),
                model_version=model_version
                if strategy in SCORE_DRIVEN_STRATEGIES
                else None,
            )
        except Exception:
            continue
        if "error" in r:
            continue

        comp = composite_score(
            excess_return_pct=r.get("excess_return_pct", 0),
            max_drawdown=r.get("max_drawdown", 0),
            information_ratio=r.get("information_ratio", 0),
            total_trades=r.get("total_trades", 0),
            daily_values=r.get("daily_values"),
            trades=r.get("trades"),
        )
        aof = anti_overfitting_flags(
            total_trades=r.get("total_trades", 0),
            daily_values=r.get("daily_values"),
            trades=r.get("trades"),
        )

        rows.append(
            {
                "strategy": opts.get("label", strategy),
                "total_return_pct": r.get("total_return_pct", 0),
                "sharpe_ratio": r.get("sharpe_ratio", 0),
                "max_drawdown": r.get("max_drawdown", 0),
                "win_rate": r.get("win_rate", 0),
                "total_trades": r.get("total_trades", 0),
                "excess_return_pct": r.get("excess_return_pct", 0),
                "information_ratio": r.get("information_ratio", 0),
                "composite_score": comp.get("score", ""),
                "rankable": "Yes" if comp.get("rankable") else "No",
                "flags": "; ".join(aof.get("flags", [])),
            }
        )

    fields = [
        "strategy",
        "total_return_pct",
        "sharpe_ratio",
        "max_drawdown",
        "win_rate",
        "total_trades",
        "excess_return_pct",
        "information_ratio",
        "composite_score",
        "rankable",
        "flags",
    ]
    headers = [
        "策略",
        "总收益(%)",
        "Sharpe",
        "最大回撤(%)",
        "胜率(%)",
        "交易次数",
        "超额收益(%)",
        "信息比率",
        "综合评分",
        "可排名",
        "反过拟合标记",
    ]
    return _build_csv(rows, fields, headers)


@backtest_bp.route("/export/scan", methods=["POST"])
def export_scan_csv():
    """CSV export for market scan results."""
    payload = request.get_json(silent=True) or {}
    strategy = (payload.get("strategy") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizon = _parse_int(payload.get("horizon"), None)
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    min_trades = _parse_int(payload.get("min_trades"), 0)
    model_version = _model_version(payload)

    strategy_norm = strategy.upper()
    if strategy_norm not in (
        "MA_CROSS",
        "BUY_HOLD",
        "SCORE_THRESHOLD",
        "SCORE_MOMENTUM",
        "MULTI_HORIZON_CONSENSUS",
    ):
        return _fail(f"Unsupported strategy: {strategy_norm}")
    if strategy_norm in ("SCORE_THRESHOLD", "SCORE_MOMENTUM"):
        if horizon is None or horizon not in (5, 20, 60):
            return _fail("horizon is required for score-driven strategies")
    version_error = _model_version_error(strategy_norm, model_version)
    if version_error:
        return version_error

    from app.model.stock import IndividualStock
    from app.services.backtest_service import anti_overfitting_flags

    try:
        stocks = list(
            IndividualStock.objects(active_status=0).only("code", "name").limit(3000)
        )
    except Exception:
        stocks = []

    total = len(stocks)
    rows = []
    for stock in stocks:
        try:
            r = run_backtest(
                stock_code=stock.code,
                strategy=strategy_norm,
                start_date=start_date,
                end_date=end_date,
                initial_cash=initial_cash,
                save_result=False,
                horizon=horizon,
                model_version=model_version,
            )
        except Exception:
            continue
        if "error" in r:
            continue
        if min_trades > 0 and r.get("total_trades", 0) < min_trades:
            continue

        comp = composite_score(
            excess_return_pct=r.get("excess_return_pct", 0),
            max_drawdown=r.get("max_drawdown", 0),
            information_ratio=r.get("information_ratio", 0),
            total_trades=r.get("total_trades", 0),
            daily_values=r.get("daily_values"),
            trades=r.get("trades"),
        )
        aof = anti_overfitting_flags(
            total_trades=r.get("total_trades", 0),
            daily_values=r.get("daily_values"),
            trades=r.get("trades"),
            num_comparisons=total,
        )

        rows.append(
            {
                "stock_code": stock.code,
                "stock_name": stock.name or stock.code,
                "total_return_pct": r.get("total_return_pct", 0),
                "sharpe_ratio": r.get("sharpe_ratio", 0),
                "max_drawdown": r.get("max_drawdown", 0),
                "win_rate": r.get("win_rate", 0),
                "total_trades": r.get("total_trades", 0),
                "excess_return_pct": r.get("excess_return_pct", 0),
                "information_ratio": r.get("information_ratio", 0),
                "composite_score": comp.get("score", ""),
                "rankable": "Yes" if comp.get("rankable") else "No",
                "flags": "; ".join(aof.get("flags", [])),
            }
        )

    rows.sort(
        key=lambda x: (
            -(x["rankable"] == "Yes"),
            -(float(x["composite_score"]) if x["composite_score"] != "" else -999),
        )
    )

    fields = [
        "stock_code",
        "stock_name",
        "total_return_pct",
        "sharpe_ratio",
        "max_drawdown",
        "win_rate",
        "total_trades",
        "excess_return_pct",
        "information_ratio",
        "composite_score",
        "rankable",
        "flags",
    ]
    headers = [
        "股票代码",
        "股票名称",
        "总收益(%)",
        "Sharpe",
        "最大回撤(%)",
        "胜率(%)",
        "交易次数",
        "超额收益(%)",
        "信息比率",
        "综合评分",
        "可排名",
        "反过拟合标记",
    ]
    return _build_csv(rows, fields, headers)


@backtest_bp.route("/export/walk-forward", methods=["POST"])
def export_walk_forward_csv():
    """CSV export for walk-forward validation results."""
    payload = request.get_json(silent=True) or {}
    stock_code = (payload.get("stock_code") or "").strip()
    strategy = (payload.get("strategy") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizon = _parse_int(payload.get("horizon"), None)
    window_days = _parse_int(payload.get("window_days"), 120, minimum=40)
    step_days = _parse_int(payload.get("step_days"), 60, minimum=10)
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    model_version = _model_version(payload)

    if not stock_code or not strategy:
        return _fail("stock_code and strategy are required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    version_error = _model_version_error(strategy, model_version)
    if version_error:
        return version_error

    quotes = _load_quotes_helper(stock_code, start_date, end_date)
    if not quotes:
        return _fail("No quote data for this stock in range")

    trading_days = sorted(
        {q.date.replace(hour=0, minute=0, second=0, microsecond=0) for q in quotes}
    )
    if len(trading_days) < window_days:
        return _fail(
            f"Need at least {window_days} trading days, got {len(trading_days)}"
        )

    rows = []
    start_idx = 0
    while start_idx + window_days <= len(trading_days):
        end_idx = start_idx + window_days - 1
        w_start = trading_days[start_idx]
        w_end = trading_days[end_idx]

        try:
            r = run_backtest(
                stock_code=stock_code,
                strategy=strategy.strip().upper(),
                start_date=w_start,
                end_date=w_end,
                initial_cash=initial_cash,
                save_result=False,
                horizon=horizon,
                model_version=model_version,
            )
        except Exception:
            start_idx += max(1, step_days)
            continue

        if "error" not in r:
            rows.append(
                {
                    "start_date": w_start.isoformat(),
                    "end_date": w_end.isoformat(),
                    "total_return_pct": r.get("total_return_pct", 0),
                    "sharpe_ratio": r.get("sharpe_ratio", 0),
                    "max_drawdown": r.get("max_drawdown", 0),
                    "total_trades": r.get("total_trades", 0),
                    "win_rate": r.get("win_rate", 0),
                    "excess_return_pct": r.get("excess_return_pct", 0),
                    "information_ratio": r.get("information_ratio", 0),
                }
            )

        start_idx += max(1, step_days)

    fields = [
        "start_date",
        "end_date",
        "total_return_pct",
        "sharpe_ratio",
        "max_drawdown",
        "total_trades",
        "win_rate",
        "excess_return_pct",
        "information_ratio",
    ]
    headers = [
        "窗口起始",
        "窗口结束",
        "总收益(%)",
        "Sharpe",
        "最大回撤(%)",
        "交易次数",
        "胜率(%)",
        "超额收益(%)",
        "信息比率",
    ]
    return _build_csv(rows, fields, headers)


def _load_quotes_helper(stock_code, start_date, end_date):
    """Load quotes for a stock — used by walk-forward and regime."""
    try:
        from app.model.stock import StockDailyQuote

        return list(
            StockDailyQuote.objects(code=stock_code)
            .filter(date__gte=start_date, date__lte=end_date)
            .order_by("date")
        )
    except Exception:
        return []


@backtest_bp.route("", methods=["GET"])
def list_backtests():
    """List completed backtests ordered by created_at desc.

    Query params:
        limit  : int (default 20)
        offset : int (default 0)
    """
    limit = _parse_int(request.args.get("limit"), 20, minimum=1)
    offset = _parse_int(request.args.get("offset"), 0, minimum=0)

    query = BacktestResult.objects(status="COMPLETED").order_by("-created_at")
    total = query.count()
    rows = list(query.skip(offset).limit(limit))

    return jsonify(
        _ok(
            data={
                "total": total,
                "limit": limit,
                "offset": offset,
                "items": [_serialize_result(row) for row in rows],
            }
        )
    ), 200


@backtest_bp.route("/<result_id>/regime", methods=["GET"])
def regime_breakdown(result_id: str):
    """Market regime breakdown for a completed backtest.

    Classifies each trading day into bull/bear/sideways using CSI 300 trend.
    """
    try:
        row = BacktestResult.objects(id=result_id).first()
    except Exception:
        row = None
    if row is None:
        return _fail("Backtest result not found", status_code=404)
    if not row.daily_values:
        return _fail("No daily values in backtest result", status_code=404)

    # Load CSI 300 data for regime classification
    from datetime import timedelta

    lookback = 60
    csi_start = parse_date_raw(row.start_date) - timedelta(days=lookback + 30)
    csi_end = parse_date_raw(row.end_date)

    csi_quotes = _load_quotes_helper("sh000300", csi_start, csi_end)

    csi_prices: dict = {}
    for q in csi_quotes:
        d = q.date.replace(hour=0, minute=0, second=0, microsecond=0)
        csi_prices[d] = q.close_hfq or q.close or 0

    # Classify each trading day
    regimes = {"bull": [], "bear": [], "sideways": []}

    for dv in row.daily_values:
        day_str = dv.get("date", "")
        try:
            day = datetime.fromisoformat(day_str).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        except (ValueError, TypeError):
            continue

        # Find CSI 300 price on or before this day
        csi_price = None
        for offset in range(7):  # look back up to 7 days
            check = day - timedelta(days=offset)
            if check in csi_prices:
                csi_price = csi_prices[check]
                break

        if csi_price is None:
            continue

        # Look back 60 days for trend
        lookback_day = day - timedelta(days=lookback)
        lookback_price = None
        for offset in range(7):
            check = lookback_day + timedelta(days=offset)
            if check in csi_prices:
                lookback_price = csi_prices[check]
                break

        if lookback_price is None or lookback_price == 0:
            continue

        csi_change = (csi_price - lookback_price) / lookback_price
        if csi_change > 0.10:
            regime = "bull"
        elif csi_change < -0.10:
            regime = "bear"
        else:
            regime = "sideways"

        regimes[regime].append(
            {
                "date": day_str,
                "equity": dv.get("equity", 0),
                "close": dv.get("close", 0),
            }
        )

    total_days = sum(len(v) for v in regimes.values())
    if total_days == 0:
        return _fail("Unable to classify any trading days", status_code=404)

    # Compute per-regime return (equity change over days in each regime)
    def _regime_return(days_data: list) -> float:
        if len(days_data) < 2:
            return 0.0
        first_eq = days_data[0].get("equity", 0) or 0
        last_eq = days_data[-1].get("equity", 0) or 0
        if first_eq == 0:
            return 0.0
        return round((last_eq - first_eq) / first_eq * 100, 2)

    return jsonify(
        _ok(
            data={
                "result_id": result_id,
                "stock_code": row.stock_code,
                "strategy": row.strategy,
                "bull": {
                    "days": len(regimes["bull"]),
                    "pct": round(len(regimes["bull"]) / total_days * 100, 1),
                    "return_pct": _regime_return(regimes["bull"]),
                },
                "bear": {
                    "days": len(regimes["bear"]),
                    "pct": round(len(regimes["bear"]) / total_days * 100, 1),
                    "return_pct": _regime_return(regimes["bear"]),
                },
                "sideways": {
                    "days": len(regimes["sideways"]),
                    "pct": round(len(regimes["sideways"]) / total_days * 100, 1),
                    "return_pct": _regime_return(regimes["sideways"]),
                },
                "total_days": total_days,
            }
        )
    ), 200


def parse_date_raw(val):
    """Parse a stored date field (may be datetime or string)."""
    from datetime import datetime as dt

    if isinstance(val, dt):
        return val
    if isinstance(val, str):
        return dt.fromisoformat(val.replace("Z", "+00:00"))
    return val


@backtest_bp.route("/<result_id>", methods=["GET"])
def get_backtest(result_id: str):
    """Get a single backtest result with full trade list and daily values."""
    try:
        row = BacktestResult.objects(id=result_id).first()
    except ValidationError:
        row = None

    if row is None:
        return _fail("Backtest result not found", status_code=404)

    return jsonify(_ok(data=_serialize_result(row, include_details=True))), 200


@backtest_bp.route("/<result_id>/component-contribution", methods=["GET"])
def component_contribution(result_id: str):
    """Analyze which scoring components drove entry/exit for each trade.

    For score-driven backtests (SCORE_THRESHOLD, SCORE_MOMENTUM,
    MULTI_HORIZON_CONSENSUS), retrieves the StockScorePrediction
    explanation at entry and exit to attribute P&L to specific
    components.
    """
    try:
        row = BacktestResult.objects(id=result_id).first()
    except Exception:
        row = None
    if row is None:
        return _fail("Backtest result not found", status_code=404)
    if not row.trades:
        return _fail("No trades in this backtest result", status_code=404)

    # Only applicable for score-driven strategies
    if row.strategy not in (
        "SCORE_THRESHOLD",
        "SCORE_MOMENTUM",
        "MULTI_HORIZON_CONSENSUS",
    ):
        return _fail(
            f"Component contribution not available for strategy '{row.strategy}'",
            status_code=400,
        )

    from app.model.scoring import StockScorePrediction

    contributions = []
    win_count = 0
    component_pnl: dict = {}
    component_trades: dict = {}

    for trade in row.trades:
        side = trade.get("side", "")
        trade_date_str = trade.get("date", "")
        try:
            trade_date = datetime.fromisoformat(trade_date_str).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        except (ValueError, TypeError):
            continue

        # Find the score prediction for this trade date, filtered by horizon
        pred_query = StockScorePrediction.objects(
            stock_code=row.stock_code,
            date=trade_date,
        )
        if getattr(row, "horizon", None):
            pred_query = pred_query.filter(horizon=row.horizon)
        pred = pred_query.order_by("-date").first()

        if not pred or not pred.explanation:
            continue

        explanation = pred.explanation or {}
        components = explanation.get("components", [])
        if not components:
            continue

        # Find dominant component (highest absolute contribution)
        dominant = max(components, key=lambda c: abs(c.get("contribution", 0)))
        dom_id = dominant.get("id", "unknown")
        dom_contrib = dominant.get("contribution", 0)
        dom_label = dominant.get("label", dom_id)

        entry = {
            "trade_date": trade_date_str,
            "side": side,
            "pnl": trade.get("pnl"),
            "dominant_component": dom_id,
            "dominant_label": dom_label,
            "dominant_contribution": dom_contrib,
            "component_scores": {
                c.get("id", "?"): c.get("contribution", 0) for c in components
            },
        }
        contributions.append(entry)

        # Aggregate P&L by dominant component (SELL trades only)
        if side == "SELL":
            pnl = trade.get("pnl") or 0
            component_pnl.setdefault(dom_id, 0.0)
            component_pnl[dom_id] += pnl
            component_trades.setdefault(dom_id, 0)
            component_trades[dom_id] += 1
            if pnl > 0:
                win_count += 1

    # Build per-component summary
    component_summary = []
    for comp_id in sorted(component_trades.keys()):
        cnt = component_trades[comp_id]
        total_pnl = component_pnl.get(comp_id, 0)
        component_summary.append(
            {
                "component_id": comp_id,
                "trades": cnt,
                "total_pnl": round(total_pnl, 2),
                "avg_pnl": round(total_pnl / cnt, 2) if cnt else 0,
                "win_rate": round(
                    sum(
                        1
                        for c in contributions
                        if c["dominant_component"] == comp_id and (c["pnl"] or 0) > 0
                    )
                    / cnt
                    * 100,
                    1,
                )
                if cnt
                else 0,
            }
        )

    return jsonify(
        _ok(
            data={
                "result_id": result_id,
                "stock_code": row.stock_code,
                "strategy": row.strategy,
                "total_trades": len(contributions),
                "component_summary": component_summary,
                "trades": contributions,
            }
        )
    ), 200


@backtest_bp.route("/evaluate-factor", methods=["POST"])
def evaluate_factor():
    """Evaluate predictive power of a scoring component or external factor.

    Request body:
        component_id : str (required)  e.g. "momentum", "signal_strength"
        start_date   : str (required)
        end_date     : str (required)
        horizons     : list[int] (optional) default [5,20,60]
    """
    payload = request.get_json(silent=True) or {}
    component_id = (payload.get("component_id") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizons = payload.get("horizons") or [5, 20, 60]

    if not component_id:
        return _fail("component_id is required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    if not isinstance(horizons, list) or not horizons:
        return _fail("horizons must be a non-empty list")

    horizon_filter = payload.get("score_horizon")  # optional: 5, 20, or 60

    from app.model.scoring import StockScorePrediction

    # Build factor_values dict from StockScorePrediction explanations
    pred_query = StockScorePrediction.objects(
        date__gte=start_date,
        date__lte=end_date,
    )
    if horizon_filter is not None:
        if horizon_filter not in (5, 20, 60):
            return _fail("score_horizon must be 5, 20, or 60")
        pred_query = pred_query.filter(horizon=horizon_filter)
    predictions = list(pred_query.only("stock_code", "date", "explanation", "horizon"))

    if not predictions:
        return _fail("No score predictions found in date range")

    factor_values: dict = {}
    for pred in predictions:
        exp = pred.explanation or {}
        for comp in exp.get("components", []):
            if comp.get("id") == component_id:
                sc = pred.stock_code
                # Key by (stock, date, horizon) to avoid cross-horizon overwrite
                h = getattr(pred, "horizon", "?")
                ds = pred.date.isoformat()
                key = f"{ds}_h{h}"
                nv = comp.get("normalized_value", 0) or 0
                if sc not in factor_values:
                    factor_values[sc] = {}
                factor_values[sc][key] = nv
                break

    if not factor_values:
        return _fail(f"Component '{component_id}' not found in any predictions")

    import importlib.util
    import os

    _factor_eval_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "datahub",
        "app",
        "lib",
        "scoring_engine",
        "factor_eval.py",
    )
    _factor_eval_path = os.path.abspath(_factor_eval_path)
    spec = importlib.util.spec_from_file_location(
        "caifubao_factor_eval", _factor_eval_path
    )
    _fe_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_fe_mod)
    FactorEvaluationService = _fe_mod.FactorEvaluationService

    service = FactorEvaluationService()
    result = service.evaluate(
        factor_values=factor_values,
        start_date=start_date,
        end_date=end_date,
        forward_horizons=horizons,
    )

    return jsonify(
        _ok(
            data={
                "component_id": component_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "observation_count": result.get("observation_count", 0),
                "ic": result.get("ic", {}),
                "icir": result.get("icir", {}),
                "quintiles": result.get("quintiles", {}),
                "decay": result.get("decay", {}),
                "correlation": result.get("correlation", {}),
            }
        )
    ), 200


@backtest_bp.route("/<result_id>/significance", methods=["GET"])
def significance_test(result_id: str):
    """Statistical significance: permutation test + bootstrap CI."""
    try:
        row = BacktestResult.objects(id=result_id).first()
    except Exception:
        row = None
    if row is None or not row.daily_values:
        return _fail("Backtest result not found or no daily values", status_code=404)

    from app.services.backtest_service import bootstrap_ci, permutation_test

    perms = permutation_test(
        daily_values=row.daily_values or [],
        initial_cash=row.initial_cash or 100000.0,
    )
    bootstrap = bootstrap_ci(daily_values=row.daily_values or [])

    return jsonify(
        _ok(
            data={
                "result_id": result_id,
                "stock_code": row.stock_code,
                "strategy": row.strategy,
                "permutation": perms,
                "bootstrap": bootstrap,
            }
        )
    ), 200


# ===========================================================================
# Decay analysis endpoint (Task 17.2)
# ===========================================================================


@backtest_bp.route("/decay-analysis", methods=["POST"])
def decay_analysis():
    """Train vs test Sharpe decay analysis per rolling window.

    Request body (JSON):
        stock_code    : str (required)
        strategy      : str (required)
        start_date    : str (required)   YYYY-MM-DD
        end_date      : str (required)   YYYY-MM-DD
        window_days   : int (optional)   default 120 trading days
        step_days     : int (optional)   default 60 trading days
        horizon       : int (optional)   for score-driven strategies
        initial_cash  : float (optional) default 100000
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    strategy = (payload.get("strategy") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizon = _parse_int(payload.get("horizon"), None)
    window_days = _parse_int(payload.get("window_days"), 120, minimum=40)
    step_days = _parse_int(payload.get("step_days"), 60, minimum=10)
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    model_version = _model_version(payload)

    if not stock_code or not strategy:
        return _fail("stock_code and strategy are required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    version_error = _model_version_error(strategy, model_version)
    if version_error:
        return version_error

    # Load trading days
    quotes = _load_quotes_helper(stock_code, start_date, end_date)
    if not quotes:
        return _fail("No quote data for this stock in range")

    trading_days = sorted(
        {q.date.replace(hour=0, minute=0, second=0, microsecond=0) for q in quotes}
    )
    total_trading_days = len(trading_days)
    if total_trading_days < window_days + step_days:
        return _fail(
            f"Need at least {window_days + step_days} trading days, "
            f"got {total_trading_days}"
        )

    warnings = []
    if total_trading_days < 300:
        warnings.append(
            f"Total trading days ({total_trading_days}) is below 300 — "
            f"decay analysis may be unreliable"
        )

    strategy_norm = strategy.strip().upper()

    windows = []
    start_idx = 0
    while start_idx + window_days + step_days <= total_trading_days:
        train_end_idx = start_idx + window_days - 1
        test_end_idx = train_end_idx + step_days

        train_start_dt = trading_days[start_idx]
        train_end_dt = trading_days[train_end_idx]
        test_start_dt = trading_days[train_end_idx + 1]
        test_end_dt = trading_days[min(test_end_idx, total_trading_days - 1)]

        # Run train backtest
        try:
            train_r = run_backtest(
                stock_code=stock_code,
                strategy=strategy_norm,
                start_date=train_start_dt,
                end_date=train_end_dt,
                initial_cash=initial_cash,
                save_result=False,
                horizon=horizon,
                model_version=model_version,
            )
        except Exception as exc:
            logger.warning(
                "decay-analysis train backtest failed for window %s: %s",
                train_start_dt.isoformat(),
                exc,
            )
            start_idx += max(1, step_days)
            continue

        if "error" in train_r:
            start_idx += max(1, step_days)
            continue

        # Run test backtest
        try:
            test_r = run_backtest(
                stock_code=stock_code,
                strategy=strategy_norm,
                start_date=test_start_dt,
                end_date=test_end_dt,
                initial_cash=initial_cash,
                save_result=False,
                horizon=horizon,
                model_version=model_version,
            )
        except Exception as exc:
            logger.warning(
                "decay-analysis test backtest failed for window %s: %s",
                test_start_dt.isoformat(),
                exc,
            )
            start_idx += max(1, step_days)
            continue

        if "error" in test_r:
            start_idx += max(1, step_days)
            continue

        train_sharpe = train_r.get("sharpe_ratio", 0) or 0
        test_sharpe = test_r.get("sharpe_ratio", 0) or 0

        # Compute per-window decay
        denom = max(abs(train_sharpe), 0.01)
        decay_pct = round((train_sharpe - test_sharpe) / denom * 100, 1)

        windows.append(
            {
                "train_start": train_start_dt.isoformat(),
                "train_end": train_end_dt.isoformat(),
                "test_start": test_start_dt.isoformat(),
                "test_end": test_end_dt.isoformat(),
                "train_sharpe": round(train_sharpe, 4),
                "test_sharpe": round(test_sharpe, 4),
                "decay_pct": decay_pct,
            }
        )

        start_idx += max(1, step_days)

    if len(windows) < 1:
        return _fail("No valid windows — all train/test combinations failed")

    if len(windows) < 3:
        warnings.append(
            f"Only {len(windows)} window(s) available — "
            f"at least 3 recommended for reliable decay analysis"
        )

    # Overall decay
    decays = [w["decay_pct"] for w in windows]
    mean_decay = round(sum(decays) / len(decays), 1) if decays else 0.0
    overfit = mean_decay > 20.0

    return jsonify(
        _ok(
            data={
                "stock_code": stock_code,
                "strategy": strategy_norm,
                "horizon": horizon,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_trading_days": total_trading_days,
                "windows": windows,
                "mean_decay_pct": mean_decay,
                "overfit": overfit,
                "total_windows": len(windows),
                "warnings": warnings,
            }
        )
    ), 200


# ===========================================================================
# Parameter landscape endpoint (Task 17.6)
# ===========================================================================


@backtest_bp.route("/landscape", methods=["POST"])
def landscape():
    """2D parameter grid for identifying flat vs sharp optima.

    Request body (JSON):
        stock_code    : str (required)
        strategy      : str (required)   SCORE_THRESHOLD / SCORE_MOMENTUM
        start_date    : str (required)   YYYY-MM-DD
        end_date      : str (required)   YYYY-MM-DD
        param_x       : str (required)   e.g. "entry_threshold"
        x_values      : list (required)  e.g. [50, 60, 70, 80]
        param_y       : str (required)   e.g. "stop_loss_pct"
        y_values      : list (required)  e.g. [-0.10, -0.05, 0.0]
        horizon       : int (required)   5, 20, or 60
        initial_cash  : float (optional) default 100000
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    strategy = (payload.get("strategy") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    horizon = _parse_int(payload.get("horizon"), None)
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    param_x = (payload.get("param_x") or "").strip()
    param_y = (payload.get("param_y") or "").strip()
    x_values = payload.get("x_values")
    y_values = payload.get("y_values")
    model_version = _model_version(payload)

    if not stock_code or not strategy:
        return _fail("stock_code and strategy are required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    if start_date > end_date:
        return _fail("start_date must be <= end_date")
    if not param_x or not isinstance(x_values, list) or len(x_values) == 0:
        return _fail("param_x and x_values (non-empty list) are required")
    if not param_y or not isinstance(y_values, list) or len(y_values) == 0:
        return _fail("param_y and y_values (non-empty list) are required")
    if horizon is None or horizon not in (5, 20, 60):
        return _fail("horizon (5, 20, or 60) is required for landscape")

    strategy_norm = strategy.strip().upper()
    if strategy_norm not in ("SCORE_THRESHOLD", "SCORE_MOMENTUM"):
        return _fail("landscape supports SCORE_THRESHOLD and SCORE_MOMENTUM only")
    version_error = _model_version_error(strategy_norm, model_version)
    if version_error:
        return version_error

    # Validate param names against strategy whitelist
    VALID_PARAM_KEYS = {
        "SCORE_THRESHOLD": {"entry_threshold", "exit_threshold", "stop_loss_pct"},
        "SCORE_MOMENTUM": {"score_delta", "stop_loss_pct"},
    }
    allowed = VALID_PARAM_KEYS.get(strategy_norm, set())
    if param_x not in allowed:
        return _fail(
            f"Unknown param_x '{param_x}' for strategy '{strategy_norm}'."
            f" Allowed: {sorted(allowed)}"
        )
    if param_y not in allowed:
        return _fail(
            f"Unknown param_y '{param_y}' for strategy '{strategy_norm}'."
            f" Allowed: {sorted(allowed)}"
        )
    if param_x == param_y:
        return _fail("param_x and param_y must be different")

    # Grid-size cap: prevent combinatorial explosion
    MAX_GRID_CELLS = 225  # 15 × 15
    total_cells = len(x_values) * len(y_values)
    if total_cells > MAX_GRID_CELLS:
        return _fail(
            f"Grid size ({len(x_values)}×{len(y_values)}={total_cells}) "
            f"exceeds maximum of {MAX_GRID_CELLS} cells"
        )

    # Build grid (cartesian product)
    grid = []
    best_sharpe = float("-inf")
    best_cell = None

    for x_val in x_values:
        for y_val in y_values:
            kw = {
                "stock_code": stock_code,
                "strategy": strategy_norm,
                "start_date": start_date,
                "end_date": end_date,
                "initial_cash": initial_cash,
                "save_result": False,
                "horizon": horizon,
                "model_version": model_version,
            }
            # Map param_x/param_y to run_backtest kwargs
            kw[param_x] = x_val
            kw[param_y] = y_val

            try:
                r = run_backtest(**kw)
            except Exception as exc:
                logger.warning(
                    "landscape cell (%s=%s, %s=%s) failed for %s: %s",
                    param_x,
                    x_val,
                    param_y,
                    y_val,
                    stock_code,
                    exc,
                )
                grid.append(
                    {
                        "x": x_val,
                        "y": y_val,
                        "error": str(exc),
                    }
                )
                continue

            if "error" in r:
                grid.append(
                    {
                        "x": x_val,
                        "y": y_val,
                        "error": r["error"],
                    }
                )
                continue

            cell = {
                "x": x_val,
                "y": y_val,
                "sharpe": round(r.get("sharpe_ratio", 0) or 0, 4),
                "return_pct": round(r.get("total_return_pct", 0) or 0, 2),
                "trades": r.get("total_trades", 0) or 0,
                "drawdown": round(r.get("max_drawdown", 0) or 0, 2),
            }
            grid.append(cell)

            if cell["sharpe"] > best_sharpe:
                best_sharpe = cell["sharpe"]
                best_cell = {
                    "x": x_val,
                    "y": y_val,
                    "sharpe": cell["sharpe"],
                    "return_pct": cell["return_pct"],
                }

    return jsonify(
        _ok(
            data={
                "stock_code": stock_code,
                "strategy": strategy_norm,
                "horizon": horizon,
                "param_x": param_x,
                "param_y": param_y,
                "x_values": x_values,
                "y_values": y_values,
                "grid": grid,
                "best": best_cell,
                "metrics": ["sharpe", "return_pct", "trades", "drawdown"],
            }
        )
    ), 200


# ===========================================================================
# Recommendation endpoint (Task 17.7)
# ===========================================================================


@backtest_bp.route("/recommendation", methods=["POST"])
def recommendation():
    """Aggregate best strategy config per horizon with stability and decay.

    Runs backtest + walk-forward decay check for each (horizon, strategy) pair,
    ranks by composite_score, and returns per-horizon rankings with warnings.

    Request body (JSON):
        stock_code    : str (required)
        start_date    : str (required)   YYYY-MM-DD
        end_date      : str (required)   YYYY-MM-DD
        horizons      : list[int] (optional)  default [5, 20, 60]
        strategies    : list[str] (optional)  default ["SCORE_MOMENTUM", "MA_CROSS",
                                                     "BUY_HOLD"]
        initial_cash  : float (optional) default 100000
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    start_date = _parse_date(payload.get("start_date"))
    end_date = _parse_date(payload.get("end_date"))
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)
    horizons = payload.get("horizons") or [5, 20, 60]
    strategies_raw = payload.get("strategies") or [
        "SCORE_MOMENTUM",
        "MA_CROSS",
        "BUY_HOLD",
    ]
    model_version = _model_version(payload)

    if not stock_code:
        return _fail("stock_code is required")
    if not start_date or not end_date:
        return _fail("start_date and end_date are required")
    if start_date > end_date:
        return _fail("start_date must be <= end_date")
    if not isinstance(horizons, list) or not horizons:
        return _fail("horizons must be a non-empty list of integers")
    if not isinstance(strategies_raw, list) or not strategies_raw:
        return _fail("strategies must be a non-empty list")

    # Normalize horizons
    valid_horizons = []
    for h in horizons:
        if isinstance(h, int) and h in (5, 20, 60):
            valid_horizons.append(h)
        elif isinstance(h, str) and h.isdigit():
            h_int = int(h)
            if h_int in (5, 20, 60):
                valid_horizons.append(h_int)
    if not valid_horizons:
        return _fail("horizons must contain at least one of 5, 20, or 60")

    # Normalize strategies
    ALLOWED_STRATEGIES = {
        "MA_CROSS",
        "BUY_HOLD",
        "SCORE_THRESHOLD",
        "SCORE_MOMENTUM",
        "MULTI_HORIZON_CONSENSUS",
    }
    strategies = []
    for s in strategies_raw:
        sn = str(s).strip().upper()
        if sn in ALLOWED_STRATEGIES:
            strategies.append(sn)
    if not strategies:
        return _fail(
            "strategies must contain at least one valid strategy from: "
            + ", ".join(sorted(ALLOWED_STRATEGIES))
        )
    if any(strategy in SCORE_DRIVEN_STRATEGIES for strategy in strategies):
        if not model_version:
            return _fail("model_version is required for score-driven strategies")

    # Combination cap: prevent excessive backtest workload
    MAX_COMBINATIONS = 15  # 3 horizons × 5 strategies
    total_combos = len(valid_horizons) * len(strategies)
    if total_combos > MAX_COMBINATIONS:
        return _fail(
            f"horizons ({len(valid_horizons)}) × strategies ({len(strategies)}) "
            f"= {total_combos} exceeds maximum of {MAX_COMBINATIONS}"
        )

    # Load trading days once for walk-forward
    quotes = _load_quotes_helper(stock_code, start_date, end_date)
    trading_days = (
        sorted(
            {q.date.replace(hour=0, minute=0, second=0, microsecond=0) for q in quotes}
        )
        if quotes
        else []
    )

    WINDOW_DAYS = 120
    STEP_DAYS = 60

    # Per-horizon results
    per_horizon: dict = {}

    for h in valid_horizons:
        rankings = []
        warnings = []
        benchmark = None

        for s in strategies:
            score_kw: dict = {
                "stock_code": stock_code,
                "strategy": s,
                "start_date": start_date,
                "end_date": end_date,
                "initial_cash": initial_cash,
                "save_result": False,
            }
            # Only pass horizon for score-driven and multi-horizon strategies
            if s in ("SCORE_THRESHOLD", "SCORE_MOMENTUM", "MULTI_HORIZON_CONSENSUS"):
                score_kw["horizon"] = h
                score_kw["model_version"] = model_version

            try:
                r = run_backtest(**score_kw)
            except Exception as exc:
                logger.error(
                    "recommendation backtest failed for %s strategy=%s horizon=%s: %s",
                    stock_code,
                    s,
                    h,
                    exc,
                )
                warnings.append(f"{s} horizon={h}: exception — {exc}")
                continue

            if "error" in r:
                warnings.append(f"{s} horizon={h}: {r['error']}")
                continue

            # Basic metrics
            entry_metrics = {
                "strategy": s,
                "sharpe": round(r.get("sharpe_ratio", 0) or 0, 4),
                "return_pct": round(r.get("total_return_pct", 0) or 0, 2),
                "drawdown": round(r.get("max_drawdown", 0) or 0, 2),
                "trades": r.get("total_trades", 0) or 0,
                "win_rate": round(r.get("win_rate", 0) or 0, 1),
                "excess_return_pct": round(r.get("excess_return_pct", 0) or 0, 2),
                "information_ratio": round(r.get("information_ratio", 0) or 0, 4),
            }

            # Walk-forward stability check
            stability_score = None
            performance_decay = False
            if trading_days and len(trading_days) >= WINDOW_DAYS:
                wf_sharpes = []
                wf_start_idx = 0
                while wf_start_idx + WINDOW_DAYS <= len(trading_days):
                    wf_end_idx = wf_start_idx + WINDOW_DAYS - 1
                    wf_start_dt = trading_days[wf_start_idx]
                    wf_end_dt = trading_days[wf_end_idx]
                    wf_kw = dict(score_kw)
                    wf_kw["start_date"] = wf_start_dt
                    wf_kw["end_date"] = wf_end_dt
                    try:
                        wf_r = run_backtest(**wf_kw)
                    except Exception as exc:
                        logger.warning(
                            "recommendation walk-forward failed for %s "
                            "strategy=%s horizon=%s window=%s: %s",
                            stock_code,
                            s,
                            h,
                            wf_start_dt.isoformat(),
                            exc,
                        )
                    else:
                        if "error" not in wf_r:
                            wf_sharpes.append(wf_r.get("sharpe_ratio", 0) or 0)
                    wf_start_idx += max(1, STEP_DAYS)

                # Compute stability (std of window Sharpes)
                # Include all valid windows (incl. zero-Sharpe) — don't
                # filter zeros as that biases results toward higher stability.
                stdev_inputs = wf_sharpes  # all are valid
                if len(stdev_inputs) >= 2:
                    stability_score = round(statistics.stdev(stdev_inputs), 4)

                # Performance decay (first-half vs second-half)
                if len(stdev_inputs) >= 6:
                    mid = len(stdev_inputs) // 2
                    first_half = (
                        sum(stdev_inputs[:mid]) / mid if stdev_inputs[:mid] else 0
                    )
                    second_half = (
                        sum(stdev_inputs[mid:]) / (len(stdev_inputs) - mid)
                        if stdev_inputs[mid:]
                        else 0
                    )
                    decay = (first_half - second_half) / max(abs(first_half), 0.01)
                    performance_decay = decay > 0.2

            entry_metrics["stability_score"] = stability_score
            entry_metrics["performance_decay"] = performance_decay

            # Composite score
            comp = composite_score(
                excess_return_pct=entry_metrics["excess_return_pct"],
                max_drawdown=entry_metrics["drawdown"],
                information_ratio=entry_metrics["information_ratio"],
                total_trades=entry_metrics["trades"],
                daily_values=r.get("daily_values"),
                trades=r.get("trades"),
            )
            entry_metrics["composite_score"] = comp["score"]

            if performance_decay:
                warnings.append(
                    f"{s} horizon={h}: performance decay detected "
                    f"(second-half Sharpe >20% below first-half)"
                )

            # Separate BUY_HOLD as benchmark
            if s == "BUY_HOLD":
                benchmark = entry_metrics
            else:
                rankings.append(entry_metrics)

        # Sort rankings by composite_score (higher = better), rankable first
        rankings.sort(
            key=lambda x: (-(x.get("composite_score", -999) or -999),),
        )

        per_horizon[str(h)] = {
            "rankings": rankings,
            "benchmark": benchmark,
            "warnings": warnings,
        }

    return jsonify(
        _ok(
            data={
                "stock_code": stock_code,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "horizons": valid_horizons,
                "strategies": strategies,
                "per_horizon": per_horizon,
            }
        )
    ), 200


@backtest_bp.route("/<result_id>", methods=["DELETE"])
def delete_backtest(result_id: str):
    """Delete a backtest result."""
    try:
        row = BacktestResult.objects(id=result_id).first()
    except ValidationError:
        row = None

    if row is None:
        return _fail("Backtest result not found", status_code=404)

    row.delete()
    return jsonify(_ok(message="Backtest result deleted")), 200
