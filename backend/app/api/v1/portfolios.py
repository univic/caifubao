# -*- coding: utf-8 -*-
# Portfolio management APIs for MVP research portfolios.

import datetime

from flask import Blueprint, jsonify, request
from mongoengine import NotUniqueError, ValidationError

from app.model.portfolio import (
    Portfolio,
    PortfolioPosition,
    PortfolioSnapshot,
    PortfolioTransaction,
)
from app.model.stock import IndividualStock, StockDailyQuote

portfolios_bp = Blueprint("portfolios", __name__, url_prefix="/api/portfolios")


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
    portfolio = Portfolio(
        name=name,
        description=(payload.get("description") or "").strip(),
        base_currency=(payload.get("base_currency") or "CNY").strip(),
        benchmark=(payload.get("benchmark") or "sh000001").strip(),
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
