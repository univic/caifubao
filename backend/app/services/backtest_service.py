# -*- coding: utf-8 -*-
"""MVP single-stock daily backtesting engine.

Uses lightweight internal code only.  No external backtest framework is
introduced (backtrader, vectorbt, zipline, rqalpha, etc.).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.model.backtest import BacktestResult
from app.model.factor import StockFactorDaily
from app.model.stock import IndividualStock, StockDailyQuote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TRADING_DAYS_PER_YEAR = 252
RISK_FREE_RATE = 0.03  # annual


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def run_backtest(
    stock_code: str,
    strategy: str,
    start_date: datetime,
    end_date: datetime,
    initial_cash: float = 100_000.0,
    save_result: bool = True,
) -> Dict[str, Any]:
    """Run a single-stock daily backtest and return the result dict.

    Parameters
    ----------
    stock_code : str
        The stock symbol (e.g. ``"sh600519"``).
    strategy : str
        ``"MA_CROSS"`` or ``"BUY_HOLD"`` (case-insensitive).
    start_date : datetime
        Start of the backtest window (inclusive).  Must be a date (time part
        is ignored).
    end_date : datetime
        End of the backtest window (inclusive).
    initial_cash : float
        Starting cash (default 100000 CNY).
    save_result : bool
        When *True* (the default) persist the result as a ``BacktestResult``
        document.

    Returns
    -------
    dict
        A dictionary that includes all metrics, the trade list, the daily
        equity curve, and the result document id when persisted.
    """
    strategy_norm = (strategy or "").strip().upper()
    if strategy_norm not in ("MA_CROSS", "BUY_HOLD"):
        return _error("Unsupported strategy", f"strategy={strategy_norm}")

    # Normalize dates to day-resolution
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if start_date > end_date:
        return _error("Invalid date range", "start_date must be <= end_date")
    if initial_cash <= 0:
        return _error("Invalid initial_cash", "Must be > 0")

    # Load stock name
    stock_name = _resolve_stock_name(stock_code)

    # Load quote data
    quotes = list(
        StockDailyQuote.objects(code=stock_code)
        .filter(date__gte=start_date, date__lte=end_date)
        .order_by("date")
    )
    if not quotes:
        return _error(
            "No quote data",
            f"No StockDailyQuote for {stock_code} in [{start_date.date()}, {end_date.date()}]",
        )

    # Build quote lookup keyed by date
    quote_map: Dict[datetime, StockDailyQuote] = {
        q.date.replace(hour=0, minute=0, second=0, microsecond=0): q for q in quotes
    }
    trading_days = sorted(quote_map.keys())
    if len(trading_days) < 2:
        return _error(
            "Insufficient data",
            f"Need >=2 trading days, got {len(trading_days)}",
        )

    # Load factor data when strategy needs MA values
    factor_map: Dict[datetime, StockFactorDaily] = {}
    if strategy_norm == "MA_CROSS":
        factors = list(
            StockFactorDaily.objects(stock_code=stock_code)
            .filter(date__gte=start_date, date__lte=end_date)
            .order_by("date")
        )
        for f in factors:
            d = f.date.replace(hour=0, minute=0, second=0, microsecond=0)
            factor_map[d] = f

    # Run the strategy simulation
    result = _simulate(
        strategy=strategy_norm,
        trading_days=trading_days,
        quote_map=quote_map,
        factor_map=factor_map,
        initial_cash=initial_cash,
    )

    # Compute final metrics
    metrics = _compute_metrics(
        initial_cash=initial_cash,
        final_value=result["final_value"],
        trades=result["trades"],
        daily_values=result["daily_values"],
    )
    result.update(metrics)

    # Persist
    if save_result:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        name = f"{stock_code}-{strategy_norm}-{start_date.date()}-{end_date.date()}-{ts}"
        doc = BacktestResult(
            name=name,
            stock_code=stock_code,
            stock_name=stock_name,
            strategy=strategy_norm,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            final_value=result["final_value"],
            total_return=result["total_return"],
            total_return_pct=result["total_return_pct"],
            annualized_return=result["annualized_return"],
            max_drawdown=result["max_drawdown"],
            max_drawdown_duration=result["max_drawdown_duration"],
            sharpe_ratio=result["sharpe_ratio"],
            win_rate=result["win_rate"],
            total_trades=result["total_trades"],
            profit_trades=result["profit_trades"],
            loss_trades=result["loss_trades"],
            best_trade=result["best_trade"],
            worst_trade=result["worst_trade"],
            status="COMPLETED",
            trades=result["trades"],
            daily_values=result["daily_values"],
            completed_at=datetime.now(timezone.utc),
        )
        doc.save()
        result["id"] = str(doc.id)
        result["name"] = name
    else:
        result["id"] = None
        result["name"] = None

    return result


# ---------------------------------------------------------------------------
# Internal helpers – simulation
# ---------------------------------------------------------------------------
def _simulate(
    strategy: str,
    trading_days: List[datetime],
    quote_map: Dict[datetime, StockDailyQuote],
    factor_map: Dict[datetime, StockFactorDaily],
    initial_cash: float,
) -> Dict[str, Any]:
    """Walk through trading days and apply the selected strategy."""
    cash = initial_cash
    shares = 0.0
    trades: List[Dict[str, Any]] = []
    daily_values: List[Dict[str, Any]] = []

    num_days = len(trading_days)

    if strategy == "BUY_HOLD":
        # Buy on first day, hold, sell on last day
        for i, day in enumerate(trading_days):
            quote = quote_map[day]
            price = _closing_price(quote)

            if i == 0:
                # Initial buy
                shares = math.floor(cash / price) if price > 0 else 0
                cost = shares * price
                cash -= cost
                if shares > 0:
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "BUY",
                            "price": round(price, 4),
                            "quantity": shares,
                            "amount": round(cost, 4),
                            "reason": "Initial buy (BUY_HOLD)",
                        }
                    )
            elif i == num_days - 1:
                # Final sell
                if shares > 0:
                    proceeds = shares * price
                    cash += proceeds
                    pnl = proceeds - (
                        sum(t["amount"] for t in trades if t["side"] == "BUY")
                        - sum(t["amount"] for t in trades if t["side"] == "SELL")
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "pnl": round(pnl, 4),
                            "reason": "Final sell (BUY_HOLD)",
                        }
                    )
                    shares = 0.0

            equity = cash + shares * price
            daily_values.append(
                {
                    "date": day.isoformat(),
                    "close": round(price, 4),
                    "cash": round(cash, 4),
                    "shares": shares,
                    "equity": round(equity, 4),
                }
            )

    elif strategy == "MA_CROSS":
        prev_ma10: Optional[float] = None
        prev_ma20: Optional[float] = None

        for i, day in enumerate(trading_days):
            quote = quote_map[day]
            price = _closing_price(quote)
            factor = factor_map.get(day)

            curr_ma10 = factor.ma_10 if factor is not None else None
            curr_ma20 = factor.ma_20 if factor is not None else None

            signal: Optional[str] = None
            if i > 0 and prev_ma10 is not None and prev_ma20 is not None and curr_ma10 is not None and curr_ma20 is not None:
                # Golden cross: MA10 crosses above MA20
                if prev_ma10 <= prev_ma20 and curr_ma10 > curr_ma20:
                    signal = "BUY"
                # Dead cross: MA10 crosses below MA20
                elif prev_ma10 >= prev_ma20 and curr_ma10 < curr_ma20:
                    signal = "SELL"

            # Execute signals
            if signal == "BUY" and shares == 0 and price > 0:
                shares = math.floor(cash / price)
                cost = shares * price
                cash -= cost
                trades.append(
                    {
                        "date": day.isoformat(),
                        "side": "BUY",
                        "price": round(price, 4),
                        "quantity": shares,
                        "amount": round(cost, 4),
                        "reason": f"Golden cross (MA10 {round(curr_ma10,4)} > MA20 {round(curr_ma20,4)})",
                    }
                )
            elif signal == "SELL" and shares > 0:
                proceeds = shares * price
                cash += proceeds
                # Calculate PnL for this sell
                buy_amounts = sum(
                    t["amount"] for t in trades if t["side"] == "BUY"
                )
                sell_amounts = sum(
                    t["amount"] for t in trades if t["side"] == "SELL"
                )
                pnl = proceeds - (buy_amounts - sell_amounts)
                trades.append(
                    {
                        "date": day.isoformat(),
                        "side": "SELL",
                        "price": round(price, 4),
                        "quantity": shares,
                        "amount": round(proceeds, 4),
                        "pnl": round(pnl, 4),
                        "reason": f"Dead cross (MA10 {round(curr_ma10,4)} < MA20 {round(curr_ma20,4)})",
                    }
                )
                shares = 0.0

            # If we never got a buy signal for MA_CROSS, we stay in cash
            # If we have a position at the end, liquidate at last day's close
            if i == num_days - 1 and shares > 0:
                proceeds = shares * price
                cash += proceeds
                buy_amounts = sum(
                    t["amount"] for t in trades if t["side"] == "BUY"
                )
                sell_amounts = sum(
                    t["amount"] for t in trades if t["side"] == "SELL"
                )
                pnl = proceeds - (buy_amounts - sell_amounts)
                trades.append(
                    {
                        "date": day.isoformat(),
                        "side": "SELL",
                        "price": round(price, 4),
                        "quantity": shares,
                        "amount": round(proceeds, 4),
                        "pnl": round(pnl, 4),
                        "reason": "Liquidation at end of backtest",
                    }
                )
                shares = 0.0

            equity = cash + shares * price
            daily_values.append(
                {
                    "date": day.isoformat(),
                    "close": round(price, 4),
                    "cash": round(cash, 4),
                    "shares": shares,
                    "equity": round(equity, 4),
                }
            )

            prev_ma10 = curr_ma10
            prev_ma20 = curr_ma20

    return {
        "final_value": round(cash + shares * _closing_price(quote_map[trading_days[-1]]), 4),
        "trades": trades,
        "daily_values": daily_values,
    }


# ---------------------------------------------------------------------------
# Internal helpers – metrics
# ---------------------------------------------------------------------------
def _compute_metrics(
    initial_cash: float,
    final_value: float,
    trades: List[Dict[str, Any]],
    daily_values: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Derive performance metrics from the simulation output."""

    total_return = round(final_value - initial_cash, 4)
    total_return_pct = round(total_return / initial_cash * 100, 4)

    # Annualized return
    num_days = len(daily_values)
    if num_days >= 2 and initial_cash > 0 and final_value > 0:
        years = num_days / TRADING_DAYS_PER_YEAR
        cagr = (final_value / initial_cash) ** (1.0 / years) - 1 if years > 0 else 0.0
    else:
        cagr = 0.0

    # Max drawdown & duration from daily equity curve
    max_dd, max_dd_dur = _compute_drawdown(daily_values)

    # Sharpe ratio from daily equity values
    sharpe = _compute_sharpe(daily_values, initial_cash)

    # Trade stats
    sell_trades = [t for t in trades if t.get("side") == "SELL"]
    pnl_values = [t.get("pnl", 0) for t in sell_trades if t.get("pnl") is not None]

    total_trades = len(sell_trades)
    profit_trades = sum(1 for p in pnl_values if p > 0)
    loss_trades = sum(1 for p in pnl_values if p <= 0)
    win_rate = round(profit_trades / total_trades * 100, 4) if total_trades > 0 else 0.0
    best_trade = round(max(pnl_values), 4) if pnl_values else 0.0
    worst_trade = round(min(pnl_values), 4) if pnl_values else 0.0

    return {
        "total_return": total_return,
        "total_return_pct": total_return_pct,
        "annualized_return": round(cagr * 100, 4),  # as percentage
        "max_drawdown": round(max_dd * 100, 4) if max_dd is not None else 0.0,  # as percentage
        "max_drawdown_duration": max_dd_dur,
        "sharpe_ratio": round(sharpe, 4),
        "win_rate": win_rate,
        "total_trades": total_trades,
        "profit_trades": profit_trades,
        "loss_trades": loss_trades,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
    }


def _compute_drawdown(
    daily_values: List[Dict[str, Any]],
) -> Tuple[Optional[float], int]:
    """Compute max drawdown (as a decimal fraction of peak) and max duration."""
    if not daily_values:
        return None, 0

    peak = daily_values[0]["equity"]
    max_dd = 0.0
    max_dur = 0
    current_dur = 0

    for row in daily_values:
        eq = row["equity"]
        if eq >= peak:
            peak = eq
            current_dur = 0
        else:
            dd = (peak - eq) / peak if peak > 0 else 0.0
            current_dur += 1
            if dd > max_dd:
                max_dd = dd
            if current_dur > max_dur:
                max_dur = current_dur

    return max_dd, max_dur


def _compute_sharpe(
    daily_values: List[Dict[str, Any]],
    initial_cash: float,
) -> float:
    """Compute annualized Sharpe ratio from daily equity values."""
    if len(daily_values) < 2 or initial_cash <= 0:
        return 0.0

    daily_returns: List[float] = []
    prev_eq = initial_cash
    for row in daily_values:
        eq = row["equity"]
        ret = (eq - prev_eq) / prev_eq if prev_eq > 0 else 0.0
        daily_returns.append(ret)
        prev_eq = eq

    mean_ret = sum(daily_returns) / len(daily_returns)
    if len(daily_returns) < 2:
        return 0.0

    variance = sum((r - mean_ret) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
    std_ret = math.sqrt(variance)
    if std_ret == 0:
        return 0.0

    rf_daily = RISK_FREE_RATE / TRADING_DAYS_PER_YEAR
    return (mean_ret - rf_daily) / std_ret * math.sqrt(TRADING_DAYS_PER_YEAR)


# ---------------------------------------------------------------------------
# Internal helpers – data access
# ---------------------------------------------------------------------------
def _closing_price(quote: StockDailyQuote) -> float:
    """Return the HFQ-adjusted close, falling back to raw close."""
    price = quote.close_hfq or quote.close or 0.0
    return float(price)


def _resolve_stock_name(stock_code: str) -> str:
    """Look up the stock name; fall back to the code itself."""
    try:
        stock = IndividualStock.objects(code=stock_code).only("name").first()
        return stock.name if stock else stock_code
    except Exception:
        return stock_code


# ---------------------------------------------------------------------------
# Internal helpers – error
# ---------------------------------------------------------------------------
def _error(message: str, detail: str = "") -> Dict[str, Any]:
    logger.warning("Backtest error: %s – %s", message, detail)
    return {"error": message, "detail": detail}
