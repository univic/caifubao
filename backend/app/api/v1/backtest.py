# -*- coding: utf-8 -*-
"""Backtest API blueprint for the MVP single-stock daily backtesting feature."""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from flask import Blueprint, jsonify, request
from mongoengine import ValidationError

from app.model.backtest import BacktestResult
from app.services.backtest_service import run_backtest

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
        "created_at": _format_dt(row.created_at),
        "completed_at": _format_dt(row.completed_at),
    }
    if include_details:
        payload["trades"] = [
            {
                "date": t.get("date"),
                "side": t.get("side"),
                "price": t.get("price"),
                "quantity": t.get("quantity"),
                "amount": t.get("amount"),
                "pnl": t.get("pnl"),
                "reason": t.get("reason"),
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
                "positions_value": round(
                    (dv.get("shares", 0) or 0) * (dv.get("close", 0) or 0), 4
                ),  # holdings market value
            }
            for dv in (row.daily_values or [])
        ]
    return payload


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@backtest_bp.route("/run", methods=["POST"])
def run():
    """Run a new backtest and return the result.

    Request body (JSON):
        stock_code  : str (required)   e.g. "sh600519"
        strategy    : str (required)   "MA_CROSS" or "BUY_HOLD"
        start_date  : str (required)   YYYY-MM-DD
        end_date    : str (required)   YYYY-MM-DD
        initial_cash: float (optional) default 100000
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    strategy = (payload.get("strategy") or "").strip()
    start_date_raw = payload.get("start_date")
    end_date_raw = payload.get("end_date")
    initial_cash = _parse_float(payload.get("initial_cash"), 100_000.0)

    # Validate required fields
    if not stock_code:
        return _fail("stock_code is required")
    if not strategy:
        return _fail("strategy is required")

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
        "Running backtest: stock=%s strategy=%s range=%s..%s cash=%s",
        stock_code,
        strategy,
        start_date.date(),
        end_date.date(),
        initial_cash,
    )

    result = run_backtest(
        stock_code=stock_code,
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        initial_cash=initial_cash,
        save_result=True,
    )

    if "error" in result:
        return _fail(f"{result['error']}: {result.get('detail', '')}")

    return jsonify(_ok(data=result)), 200


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
