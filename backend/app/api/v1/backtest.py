# -*- coding: utf-8 -*-
"""Backtest API blueprint for the MVP single-stock daily backtesting feature."""

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from flask import Blueprint, jsonify, request
from mongoengine import ValidationError

from app.model.backtest import BacktestResult
from app.services.backtest_service import run_backtest, run_multi_stock_backtest

logger = logging.getLogger(__name__)

backtest_bp = Blueprint("backtest", __name__, url_prefix="/api/backtest")

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


def _parse_int(value: Any, default: int, minimum: int = 0) -> int:
    if value is None:
        return default
    try:
        return max(int(value), minimum)
    except (TypeError, ValueError):
        return default


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
        "excess_return": row.excess_return,
        "excess_return_pct": row.excess_return_pct,
        "information_ratio": row.information_ratio,
        "horizon": row.horizon,
        "score_config": row.score_config,
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
    model_version = (payload.get("model_version") or "").strip() or None

    # Validate required fields
    if not stock_code:
        return _fail("stock_code is required")
    if not strategy:
        return _fail("strategy is required")

    strategy_norm = strategy.strip().upper()
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
    model_version = (payload.get("model_version") or "").strip() or None

    # Validate required fields
    if not isinstance(stock_codes, list) or len(stock_codes) < 2:
        return _fail("stock_codes must be a list with at least 2 codes")
    if len(stock_codes) > 100:
        return _fail("stock_codes: maximum 100 codes allowed")
    if not strategy:
        return _fail("strategy is required")

    strategy_norm = strategy.strip().upper()
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

        # Aggregate P&L by dominant component
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

    from app.model.scoring import StockScorePrediction

    # Build factor_values dict from StockScorePrediction explanations
    predictions = list(
        StockScorePrediction.objects(
            date__gte=start_date,
            date__lte=end_date,
        ).only("stock_code", "date", "explanation")
    )

    if not predictions:
        return _fail("No score predictions found in date range")

    factor_values: dict = {}
    for pred in predictions:
        exp = pred.explanation or {}
        for comp in exp.get("components", []):
            if comp.get("id") == component_id:
                sc = pred.stock_code
                ds = pred.date.isoformat()
                nv = comp.get("normalized_value", 0) or 0
                if sc not in factor_values:
                    factor_values[sc] = {}
                factor_values[sc][ds] = nv
                break

    if not factor_values:
        return _fail(f"Component '{component_id}' not found in any predictions")

    import os
    import sys

    _datahub_app = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "datahub", "app"
    )
    _datahub_app = os.path.abspath(_datahub_app)
    if _datahub_app not in sys.path:
        sys.path.insert(0, _datahub_app)

    from app.lib.scoring_engine.factor_eval import FactorEvaluationService

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
