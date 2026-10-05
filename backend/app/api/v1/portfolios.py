# -*- coding: utf-8 -*-
# Portfolio management APIs for MVP research portfolios.

import csv
import datetime
import io
import math

from flask import Blueprint, jsonify, request
from mongoengine import NotUniqueError, ValidationError

from app.model.portfolio import (
    Portfolio,
    PortfolioPosition,
    PortfolioSnapshot,
    PortfolioTransaction,
)
from app.model.execution_ledger import (
    AccountReconciliation,
    ExecutionFill,
    OrderIntent,
)
from app.model.stock import IndividualStock, StockDailyQuote
from app.lib.auth_decorators import block_service_tokens

portfolios_bp = Blueprint("portfolios", __name__, url_prefix="/api/portfolios")
portfolios_bp.before_request(block_service_tokens)


def _parse_datetime(value):
    if not value:
        return datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    if isinstance(value, datetime.datetime):
        return value
    text = str(value).strip().replace("Z", "+00:00")
    if len(text) == 10:
        text = f"{text}T00:00:00"
    return datetime.datetime.fromisoformat(text)


def _format_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return str(value)


def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _strict_float(value, field_name, default=None):
    if value in (None, ""):
        if default is None:
            raise ValueError(f"{field_name} is required")
        value = default
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _portfolio_or_404(portfolio_id):
    try:
        portfolio = Portfolio.objects(id=portfolio_id).first()
    except ValidationError:
        portfolio = None
    if portfolio is None:
        return None, (
            jsonify({"success": False, "message": "Portfolio not found"}),
            404,
        )
    return portfolio, None


def _latest_quote(stock_code):
    return StockDailyQuote.objects(code=stock_code).order_by("-date").first()


def _latest_price(stock_code, fallback=0.0):
    quote = _latest_quote(stock_code)
    if quote is None:
        return fallback, None
    price = quote.close_hfq or quote.close or fallback
    return _to_float(price), quote.date


def _stock_name(stock_code, fallback=None):
    if fallback:
        return fallback
    stock = IndividualStock.objects(code=stock_code).only("name").first()
    return stock.name if stock else stock_code


def _serialize_portfolio(portfolio, include_summary=True):
    payload = {
        "id": str(portfolio.id),
        "name": portfolio.name,
        "description": portfolio.description,
        "base_currency": portfolio.base_currency,
        "benchmark": portfolio.benchmark,
        "account_mode": getattr(portfolio, "account_mode", "RESEARCH"),
        "book_type": getattr(portfolio, "book_type", "RESEARCH"),
        "initial_cash": portfolio.initial_cash,
        "cash": portfolio.cash,
        "status": portfolio.status,
        "created_at": _format_datetime(portfolio.created_at),
        "updated_at": _format_datetime(portfolio.updated_at),
    }
    if include_summary:
        payload["summary"] = _build_summary(portfolio)
    return payload


def _serialize_position(position, portfolio_total_value=None):
    market_price, quote_date = _latest_price(position.stock_code, position.avg_cost)
    market_value = market_price * (position.quantity or 0)
    cost_value = (position.avg_cost or 0) * (position.quantity or 0)
    unrealized_pnl = market_value - cost_value
    return {
        "id": str(position.id),
        "stock_code": position.stock_code,
        "stock_name": position.stock_name,
        "quantity": position.quantity,
        "avg_cost": position.avg_cost,
        "market_price": round(market_price, 4),
        "market_value": round(market_value, 4),
        "cost_value": round(cost_value, 4),
        "unrealized_pnl": round(unrealized_pnl, 4),
        "unrealized_pnl_pct": round(unrealized_pnl / cost_value, 6)
        if cost_value
        else None,
        "realized_pnl": position.realized_pnl,
        "weight": round(market_value / portfolio_total_value, 6)
        if portfolio_total_value
        else None,
        "quote_date": _format_datetime(quote_date),
        "updated_at": _format_datetime(position.updated_at),
    }


def _serialize_transaction(transaction):
    return {
        "id": str(transaction.id),
        "portfolio_id": str(transaction.portfolio.id),
        "stock_code": transaction.stock_code,
        "stock_name": transaction.stock_name,
        "side": transaction.side,
        "quantity": transaction.quantity,
        "price": transaction.price,
        "fee": transaction.fee,
        "amount": transaction.amount,
        "trade_date": _format_datetime(transaction.trade_date),
        "reason": transaction.reason,
        "source_score_id": transaction.source_score_id,
        "created_at": _format_datetime(transaction.created_at),
    }


def _build_summary(portfolio):
    positions = list(
        PortfolioPosition.objects(portfolio=portfolio, quantity__gt=0).order_by(
            "stock_code"
        )
    )
    position_rows = [_serialize_position(position) for position in positions]
    positions_value = sum(row["market_value"] for row in position_rows)
    total_value = (portfolio.cash or 0) + positions_value
    total_return = total_value - (portfolio.initial_cash or 0)
    for row in position_rows:
        row["weight"] = (
            round(row["market_value"] / total_value, 6) if total_value else 0
        )

    return {
        "cash": round(portfolio.cash or 0, 4),
        "positions_value": round(positions_value, 4),
        "total_value": round(total_value, 4),
        "total_return": round(total_return, 4),
        "total_return_pct": round(total_return / portfolio.initial_cash, 6)
        if portfolio.initial_cash
        else None,
        "position_count": len(position_rows),
    }


def _apply_transaction(portfolio, payload):
    side = (payload.get("side") or "").strip().upper()
    stock_code = (payload.get("stock_code") or "").strip() or None
    stock_name = (
        _stock_name(stock_code, payload.get("stock_name")) if stock_code else None
    )
    quantity = _to_float(payload.get("quantity"))
    price = _to_float(payload.get("price"))
    fee = _to_float(payload.get("fee"))
    trade_date = _parse_datetime(payload.get("trade_date"))

    if side not in {"BUY", "SELL", "CASH_IN", "CASH_OUT", "DIVIDEND"}:
        raise ValueError("Unsupported transaction side")
    if side in {"BUY", "SELL"} and (not stock_code or quantity <= 0 or price <= 0):
        raise ValueError("stock_code, quantity, and price are required for trades")
    if side in {"CASH_IN", "CASH_OUT", "DIVIDEND"} and price <= 0:
        raise ValueError("price is required as cash amount for cash transactions")

    amount = (
        round(quantity * price + fee, 4)
        if side == "BUY"
        else round(quantity * price - fee, 4)
    )
    if side == "CASH_IN":
        amount = price
        portfolio.cash = (portfolio.cash or 0) + amount
    elif side == "CASH_OUT":
        amount = price
        if (portfolio.cash or 0) < amount:
            raise ValueError("Insufficient cash")
        portfolio.cash = (portfolio.cash or 0) - amount
    elif side == "DIVIDEND":
        amount = price
        portfolio.cash = (portfolio.cash or 0) + amount
    elif side == "BUY":
        if (portfolio.cash or 0) < amount:
            raise ValueError("Insufficient cash")
        portfolio.cash = (portfolio.cash or 0) - amount
        _apply_buy_position(portfolio, stock_code, stock_name, quantity, price)
    elif side == "SELL":
        position = PortfolioPosition.objects(
            portfolio=portfolio, stock_code=stock_code
        ).first()
        if position is None or (position.quantity or 0) < quantity:
            raise ValueError("Insufficient position quantity")
        amount = round(quantity * price - fee, 4)
        portfolio.cash = (portfolio.cash or 0) + amount
        _apply_sell_position(position, quantity, price)

    portfolio.save()
    transaction = PortfolioTransaction(
        portfolio=portfolio,
        stock_code=stock_code,
        stock_name=stock_name,
        side=side,
        quantity=quantity,
        price=price,
        fee=fee,
        amount=amount,
        trade_date=trade_date,
        reason=payload.get("reason"),
        source_score_id=payload.get("source_score_id"),
    )
    transaction.save()
    return transaction


def _apply_buy_position(portfolio, stock_code, stock_name, quantity, price):
    position = PortfolioPosition.objects(
        portfolio=portfolio, stock_code=stock_code
    ).first()
    if position is None:
        position = PortfolioPosition(
            portfolio=portfolio,
            stock_code=stock_code,
            stock_name=stock_name,
            quantity=0,
            avg_cost=0,
        )
    total_quantity = (position.quantity or 0) + quantity
    total_cost = (position.quantity or 0) * (position.avg_cost or 0) + quantity * price
    position.quantity = total_quantity
    position.avg_cost = round(total_cost / total_quantity, 6) if total_quantity else 0
    position.stock_name = stock_name
    position.save()
    return position


def _apply_sell_position(position, quantity, price):
    position.realized_pnl = (position.realized_pnl or 0) + (
        price - (position.avg_cost or 0)
    ) * quantity
    position.quantity = round((position.quantity or 0) - quantity, 6)
    if position.quantity <= 0:
        position.quantity = 0
    position.save()
    return position


def _save_snapshot(portfolio):
    today = datetime.datetime.now(datetime.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    summary = _build_summary(portfolio)
    positions = [
        _serialize_position(position, summary["total_value"])
        for position in PortfolioPosition.objects(
            portfolio=portfolio, quantity__gt=0
        ).order_by("stock_code")
    ]
    snapshot = PortfolioSnapshot.objects(portfolio=portfolio, date=today).first()
    if snapshot is None:
        snapshot = PortfolioSnapshot(portfolio=portfolio, date=today)
    snapshot.total_value = summary["total_value"]
    snapshot.cash = summary["cash"]
    snapshot.positions_value = summary["positions_value"]
    snapshot.holdings = positions
    snapshot.save()
    return snapshot


@portfolios_bp.route("", methods=["GET"])
def list_portfolios():
    rows = Portfolio.objects(status__ne="ARCHIVED").order_by("-updated_at")
    return jsonify({"items": [_serialize_portfolio(row) for row in rows]}), 200


@portfolios_bp.route("", methods=["POST"])
def create_portfolio():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify({"success": False, "message": "name is required"}), 400
    initial_cash = _to_float(payload.get("initial_cash"), 1_000_000.0)
    account_mode = (payload.get("account_mode") or "RESEARCH").strip().upper()
    if account_mode not in {"RESEARCH", "MANUAL_LIVE"}:
        return (
            jsonify({"success": False, "message": "unsupported account_mode"}),
            400,
        )
    default_book_type = "QUANT" if account_mode == "MANUAL_LIVE" else "RESEARCH"
    book_type = (payload.get("book_type") or default_book_type).strip().upper()
    if book_type not in {"RESEARCH", "CORE", "QUANT", "DISCRETIONARY"}:
        return jsonify({"success": False, "message": "unsupported book_type"}), 400
    portfolio = Portfolio(
        name=name,
        description=(payload.get("description") or "").strip(),
        base_currency=(payload.get("base_currency") or "CNY").strip(),
        benchmark=(payload.get("benchmark") or "sh000001").strip(),
        account_mode=account_mode,
        book_type=book_type,
        initial_cash=initial_cash,
        cash=initial_cash,
    )
    portfolio.save()
    return jsonify(_serialize_portfolio(portfolio)), 201


@portfolios_bp.route("/<portfolio_id>", methods=["GET"])
def get_portfolio(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    return jsonify(_serialize_portfolio(portfolio)), 200


@portfolios_bp.route("/<portfolio_id>/positions", methods=["GET"])
def get_positions(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    summary = _build_summary(portfolio)
    positions = [
        _serialize_position(position, summary["total_value"])
        for position in PortfolioPosition.objects(
            portfolio=portfolio, quantity__gt=0
        ).order_by("stock_code")
    ]
    return jsonify({"summary": summary, "items": positions}), 200


@portfolios_bp.route("/<portfolio_id>/transactions", methods=["GET"])
def get_transactions(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    rows = PortfolioTransaction.objects(portfolio=portfolio).order_by("-trade_date")
    return jsonify({"items": [_serialize_transaction(row) for row in rows]}), 200


@portfolios_bp.route("/<portfolio_id>/transactions", methods=["POST"])
def create_transaction(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    try:
        transaction = _apply_transaction(portfolio, payload)
    except (ValueError, ValidationError, NotUniqueError) as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    return jsonify(_serialize_transaction(transaction)), 201


@portfolios_bp.route("/<portfolio_id>/snapshots", methods=["GET"])
def get_snapshots(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    rows = PortfolioSnapshot.objects(portfolio=portfolio).order_by("-date").limit(120)
    return jsonify(
        {
            "items": [
                {
                    "id": str(row.id),
                    "date": _format_datetime(row.date),
                    "total_value": row.total_value,
                    "cash": row.cash,
                    "positions_value": row.positions_value,
                    "daily_return": row.daily_return,
                    "drawdown": row.drawdown,
                    "holdings": row.holdings,
                    "created_at": _format_datetime(row.created_at),
                }
                for row in rows
            ]
        }
    ), 200


@portfolios_bp.route("/<portfolio_id>/snapshots", methods=["POST"])
def create_snapshot(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    snapshot = _save_snapshot(portfolio)
    return jsonify(
        {
            "id": str(snapshot.id),
            "date": _format_datetime(snapshot.date),
            "total_value": snapshot.total_value,
            "cash": snapshot.cash,
            "positions_value": snapshot.positions_value,
            "holdings": snapshot.holdings,
            "created_at": _format_datetime(snapshot.created_at),
        }
    ), 201


# --- Manual execution ledger -------------------------------------------------


def _serialize_order_intent(intent):
    return {
        "id": str(intent.id),
        "portfolio_id": str(intent.portfolio.id),
        "stock_code": intent.stock_code,
        "stock_name": intent.stock_name,
        "side": intent.side,
        "target_quantity": intent.target_quantity,
        "target_price": intent.target_price,
        "filled_quantity": intent.filled_quantity,
        "status": intent.status,
        "source_type": intent.source_type,
        "source_ref": intent.source_ref,
        "notes": intent.notes,
        "created_at": _format_datetime(intent.created_at),
        "updated_at": _format_datetime(intent.updated_at),
    }


def _serialize_execution_fill(fill):
    return {
        "id": str(fill.id),
        "portfolio_id": str(fill.portfolio.id),
        "intent_id": str(fill.intent.id) if fill.intent else None,
        "external_fill_id": fill.external_fill_id,
        "stock_code": fill.stock_code,
        "stock_name": fill.stock_name,
        "side": fill.side,
        "quantity": fill.quantity,
        "price": fill.price,
        "fee": fill.fee,
        "trade_time": _format_datetime(fill.trade_time),
        "import_source": fill.import_source,
        "apply_status": fill.apply_status,
        "apply_error": fill.apply_error,
        "portfolio_transaction_id": fill.portfolio_transaction_id,
        "created_at": _format_datetime(fill.created_at),
    }


def _serialize_reconciliation(row):
    return {
        "id": str(row.id),
        "portfolio_id": str(row.portfolio.id),
        "as_of": _format_datetime(row.as_of),
        "status": row.status,
        "expected_cash": row.expected_cash,
        "actual_cash": row.actual_cash,
        "cash_drift": row.cash_drift,
        "cash_tolerance": row.cash_tolerance,
        "quantity_tolerance": row.quantity_tolerance,
        "expected_positions": row.expected_positions,
        "actual_positions": row.actual_positions,
        "breaks": row.breaks,
        "created_at": _format_datetime(row.created_at),
    }


def _intent_or_404(portfolio, intent_id):
    try:
        intent = OrderIntent.objects(id=intent_id, portfolio=portfolio).first()
    except ValidationError:
        intent = None
    if intent is None:
        return None, (
            jsonify({"success": False, "message": "Order intent not found"}),
            404,
        )
    return intent, None


def _require_manual_live_portfolio(portfolio):
    if getattr(portfolio, "account_mode", "RESEARCH") != "MANUAL_LIVE":
        return (
            jsonify(
                {
                    "success": False,
                    "message": (
                        "execution ledger requires account_mode=MANUAL_LIVE"
                    ),
                }
            ),
            409,
        )
    return None


def _normalize_order_intent_payload(payload):
    side = (payload.get("side") or "").strip().upper()
    stock_code = (payload.get("stock_code") or "").strip()
    target_quantity = _strict_float(
        payload.get("target_quantity"), "target_quantity"
    )
    target_price = payload.get("target_price")
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if not stock_code:
        raise ValueError("stock_code is required")
    if target_quantity <= 0 or not math.isfinite(target_quantity):
        raise ValueError("target_quantity must be a positive finite number")
    normalized_price = None
    if target_price not in (None, ""):
        normalized_price = _strict_float(target_price, "target_price")
        if normalized_price <= 0:
            raise ValueError("target_price must be a positive finite number")
    return {
        "side": side,
        "stock_code": stock_code,
        "target_quantity": target_quantity,
        "target_price": normalized_price,
    }


def _normalize_fill_payload(payload):
    external_fill_id = (payload.get("external_fill_id") or "").strip()
    stock_code = (payload.get("stock_code") or "").strip()
    side = (payload.get("side") or "").strip().upper()
    quantity = _strict_float(payload.get("quantity"), "quantity")
    price = _strict_float(payload.get("price"), "price")
    fee = _strict_float(payload.get("fee"), "fee", default=0.0)
    if not external_fill_id:
        raise ValueError("external_fill_id is required")
    if not stock_code:
        raise ValueError("stock_code is required")
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if quantity <= 0 or not math.isfinite(quantity):
        raise ValueError("quantity must be a positive finite number")
    if price <= 0 or not math.isfinite(price):
        raise ValueError("price must be a positive finite number")
    if fee < 0:
        raise ValueError("fee must be a non-negative finite number")
    if not payload.get("trade_time"):
        raise ValueError("trade_time is required")
    try:
        trade_time = _parse_datetime(payload.get("trade_time"))
    except (TypeError, ValueError) as exc:
        raise ValueError("trade_time must be an ISO-8601 datetime") from exc
    return {
        "external_fill_id": external_fill_id,
        "stock_code": stock_code,
        "side": side,
        "quantity": quantity,
        "price": price,
        "fee": fee,
        "trade_time": trade_time,
    }


def _refresh_intent_status(intent):
    filled_quantity = sum(
        (row.quantity or 0.0)
        for row in ExecutionFill.objects(intent=intent, apply_status="APPLIED")
    )
    intent.filled_quantity = round(filled_quantity, 6)
    if intent.status != "CANCELLED":
        if filled_quantity <= 0:
            intent.status = "OPEN"
        elif filled_quantity + 1e-9 < (intent.target_quantity or 0):
            intent.status = "PARTIAL"
        else:
            intent.status = "FILLED"
    intent.save()
    return intent


def _ingest_execution_fill(portfolio, payload, import_source="JSON"):
    if getattr(portfolio, "account_mode", "RESEARCH") != "MANUAL_LIVE":
        raise ValueError("execution ledger requires account_mode=MANUAL_LIVE")
    normalized = _normalize_fill_payload(payload)
    existing = ExecutionFill.objects(
        portfolio=portfolio,
        external_fill_id=normalized["external_fill_id"],
    ).first()
    if existing is not None:
        if existing.apply_status == "APPLIED":
            return existing, True
        raise ValueError(
            "execution fill is PENDING; reconcile the portfolio before retrying"
        )

    intent = None
    intent_id = (payload.get("intent_id") or "").strip()
    if intent_id:
        try:
            intent = OrderIntent.objects(id=intent_id, portfolio=portfolio).first()
        except ValidationError as exc:
            raise ValueError("intent_id is invalid") from exc
        if intent is None:
            raise ValueError("intent_id does not belong to this portfolio")
        if (
            intent.stock_code != normalized["stock_code"]
            or intent.side != normalized["side"]
        ):
            raise ValueError("fill stock_code/side must match linked intent")

    stock_name = _stock_name(normalized["stock_code"], payload.get("stock_name"))
    fill = ExecutionFill(
        portfolio=portfolio,
        intent=intent,
        stock_code=normalized["stock_code"],
        stock_name=stock_name,
        side=normalized["side"],
        quantity=normalized["quantity"],
        price=normalized["price"],
        fee=normalized["fee"],
        trade_time=normalized["trade_time"],
        external_fill_id=normalized["external_fill_id"],
        import_source=import_source,
    )
    try:
        fill.save(force_insert=True)
    except NotUniqueError:
        existing = ExecutionFill.objects(
            portfolio=portfolio,
            external_fill_id=normalized["external_fill_id"],
        ).first()
        if existing is None:
            raise
        if existing.apply_status == "APPLIED":
            return existing, True
        raise ValueError(
            "execution fill is PENDING; reconcile the portfolio before retrying"
        )

    try:
        transaction = _apply_transaction(
            portfolio,
            {
                "side": normalized["side"],
                "stock_code": normalized["stock_code"],
                "stock_name": stock_name,
                "quantity": normalized["quantity"],
                "price": normalized["price"],
                "fee": normalized["fee"],
                "trade_date": normalized["trade_time"],
                "reason": "Imported execution fill",
                "source_score_id": f"execution_fill:{normalized['external_fill_id']}",
            },
        )
    except Exception as error:
        fill.apply_error = str(error)
        fill.save()
        raise

    fill.portfolio_transaction_id = str(transaction.id)
    fill.apply_status = "APPLIED"
    fill.apply_error = None
    fill.save()
    if intent is not None:
        _refresh_intent_status(intent)
    return fill, False


def _normalize_actual_positions(raw_positions):
    if not isinstance(raw_positions, list):
        raise ValueError("positions must be a list")
    result = {}
    for row in raw_positions:
        if not isinstance(row, dict):
            raise ValueError("each position must be an object")
        stock_code = (row.get("stock_code") or "").strip()
        if not stock_code:
            raise ValueError("position stock_code is required")
        if stock_code in result:
            raise ValueError(f"duplicate position stock_code: {stock_code}")
        quantity = _strict_float(row.get("quantity"), "position quantity")
        if quantity < 0:
            raise ValueError("position quantity must be a non-negative finite number")
        result[stock_code] = quantity
    return result


def _build_reconciliation(portfolio, payload):
    if getattr(portfolio, "account_mode", "RESEARCH") != "MANUAL_LIVE":
        raise ValueError("execution ledger requires account_mode=MANUAL_LIVE")
    actual_cash = _strict_float(payload.get("cash"), "cash")
    cash_tolerance = _strict_float(
        payload.get("cash_tolerance"), "cash_tolerance", default=0.01
    )
    quantity_tolerance = _strict_float(
        payload.get("quantity_tolerance"),
        "quantity_tolerance",
        default=0.000001,
    )
    if cash_tolerance < 0 or quantity_tolerance < 0:
        raise ValueError("tolerances must be non-negative finite numbers")

    actual = _normalize_actual_positions(payload.get("positions", []))
    expected_rows = PortfolioPosition.objects(
        portfolio=portfolio, quantity__gt=0
    ).order_by("stock_code")
    expected = {row.stock_code: float(row.quantity or 0.0) for row in expected_rows}

    breaks = []
    expected_cash = float(portfolio.cash or 0.0)
    cash_drift = actual_cash - expected_cash
    if abs(cash_drift) > cash_tolerance:
        breaks.append(
            {
                "type": "CASH_DRIFT",
                "expected": expected_cash,
                "actual": actual_cash,
                "drift": cash_drift,
            }
        )

    for stock_code in sorted(set(expected) | set(actual)):
        expected_qty = expected.get(stock_code, 0.0)
        actual_qty = actual.get(stock_code, 0.0)
        drift = actual_qty - expected_qty
        if abs(drift) <= quantity_tolerance:
            continue
        if expected_qty > quantity_tolerance and actual_qty <= quantity_tolerance:
            break_type = "MISSING_POSITION"
        elif actual_qty > quantity_tolerance and expected_qty <= quantity_tolerance:
            break_type = "UNEXPECTED_POSITION"
        else:
            break_type = "QUANTITY_DRIFT"
        breaks.append(
            {
                "type": break_type,
                "stock_code": stock_code,
                "expected": expected_qty,
                "actual": actual_qty,
                "drift": drift,
            }
        )

    try:
        as_of = _parse_datetime(payload.get("as_of"))
    except (TypeError, ValueError) as exc:
        raise ValueError("as_of must be an ISO-8601 datetime") from exc

    row = AccountReconciliation(
        portfolio=portfolio,
        as_of=as_of,
        status="BREAK" if breaks else "PASS",
        expected_cash=expected_cash,
        actual_cash=actual_cash,
        cash_drift=cash_drift,
        cash_tolerance=cash_tolerance,
        quantity_tolerance=quantity_tolerance,
        expected_positions=[
            {"stock_code": code, "quantity": expected[code]} for code in sorted(expected)
        ],
        actual_positions=[
            {"stock_code": code, "quantity": actual[code]} for code in sorted(actual)
        ],
        breaks=breaks,
    )
    row.save()
    return row


@portfolios_bp.route("/<portfolio_id>/execution/intents", methods=["GET"])
def list_order_intents(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    live_error = _require_manual_live_portfolio(portfolio)
    if live_error:
        return live_error
    rows = OrderIntent.objects(portfolio=portfolio).order_by("-created_at").limit(200)
    return jsonify({"items": [_serialize_order_intent(row) for row in rows]}), 200


@portfolios_bp.route("/<portfolio_id>/execution/intents", methods=["POST"])
def create_order_intent(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    live_error = _require_manual_live_portfolio(portfolio)
    if live_error:
        return live_error
    payload = request.get_json(silent=True) or {}
    try:
        normalized = _normalize_order_intent_payload(payload)
        intent = OrderIntent(
            portfolio=portfolio,
            stock_code=normalized["stock_code"],
            stock_name=_stock_name(
                normalized["stock_code"], payload.get("stock_name")
            ),
            side=normalized["side"],
            target_quantity=normalized["target_quantity"],
            target_price=normalized["target_price"],
            source_type=(payload.get("source_type") or "MANUAL").strip(),
            source_ref=(payload.get("source_ref") or "").strip() or None,
            notes=(payload.get("notes") or "").strip() or None,
        )
        intent.save()
    except (ValueError, ValidationError) as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    return jsonify(_serialize_order_intent(intent)), 201


@portfolios_bp.route("/<portfolio_id>/execution/fills", methods=["GET"])
def list_execution_fills(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    live_error = _require_manual_live_portfolio(portfolio)
    if live_error:
        return live_error
    rows = ExecutionFill.objects(portfolio=portfolio).order_by("-trade_time").limit(500)
    return jsonify({"items": [_serialize_execution_fill(row) for row in rows]}), 200


@portfolios_bp.route("/<portfolio_id>/execution/fills", methods=["POST"])
def create_execution_fill(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    live_error = _require_manual_live_portfolio(portfolio)
    if live_error:
        return live_error
    payload = request.get_json(silent=True) or {}
    try:
        fill, duplicate = _ingest_execution_fill(portfolio, payload, import_source="JSON")
    except (ValueError, ValidationError, NotUniqueError) as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    body = _serialize_execution_fill(fill)
    body["duplicate"] = duplicate
    return jsonify(body), 200 if duplicate else 201


@portfolios_bp.route("/<portfolio_id>/execution/fills/import-csv", methods=["POST"])
def import_execution_fills_csv(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    live_error = _require_manual_live_portfolio(portfolio)
    if live_error:
        return live_error
    upload = request.files.get("file")
    if upload is None:
        return jsonify({"success": False, "message": "file is required"}), 400
    try:
        text = upload.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return jsonify({"success": False, "message": "CSV must be UTF-8"}), 400

    reader = csv.DictReader(io.StringIO(text))
    required = {
        "external_fill_id",
        "stock_code",
        "side",
        "quantity",
        "price",
        "fee",
        "trade_time",
    }
    fieldnames = set(reader.fieldnames or [])
    missing = sorted(required - fieldnames)
    if missing:
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"missing CSV columns: {', '.join(missing)}",
                }
            ),
            400,
        )

    results = []
    applied = 0
    duplicates = 0
    errors = 0
    for row_number, row in enumerate(reader, start=2):
        if not any((value or "").strip() for value in row.values()):
            continue
        try:
            fill, duplicate = _ingest_execution_fill(
                portfolio, row, import_source="CSV"
            )
            if duplicate:
                duplicates += 1
                result_status = "DUPLICATE"
            else:
                applied += 1
                result_status = "APPLIED"
            results.append(
                {
                    "row": row_number,
                    "status": result_status,
                    "fill": _serialize_execution_fill(fill),
                }
            )
        except (ValueError, ValidationError, NotUniqueError) as exc:
            errors += 1
            results.append(
                {"row": row_number, "status": "ERROR", "message": str(exc)}
            )

    return (
        jsonify(
            {
                "applied": applied,
                "duplicates": duplicates,
                "errors": errors,
                "items": results,
            }
        ),
        200,
    )


@portfolios_bp.route("/<portfolio_id>/execution/reconciliations", methods=["GET"])
def list_account_reconciliations(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    live_error = _require_manual_live_portfolio(portfolio)
    if live_error:
        return live_error
    rows = AccountReconciliation.objects(portfolio=portfolio).order_by("-as_of").limit(
        120
    )
    return jsonify({"items": [_serialize_reconciliation(row) for row in rows]}), 200


@portfolios_bp.route("/<portfolio_id>/execution/reconciliations", methods=["POST"])
def create_account_reconciliation(portfolio_id):
    portfolio, error_response = _portfolio_or_404(portfolio_id)
    if error_response:
        return error_response
    live_error = _require_manual_live_portfolio(portfolio)
    if live_error:
        return live_error
    payload = request.get_json(silent=True) or {}
    try:
        row = _build_reconciliation(portfolio, payload)
    except (ValueError, ValidationError) as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    return jsonify(_serialize_reconciliation(row)), 201
