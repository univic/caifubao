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
from app.model.scoring import StockScorePrediction
from app.model.stock import IndividualStock, StockDailyQuote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TRADING_DAYS_PER_YEAR = 252
RISK_FREE_RATE = 0.03  # annual

VALID_STRATEGIES = (
    "MA_CROSS",
    "BUY_HOLD",
    "SCORE_THRESHOLD",
    "SCORE_MOMENTUM",
    "TOP_N_ROTATION",
    "MULTI_HORIZON_CONSENSUS",
)

# Friction model
COMMISSION_RATE = 0.00025  # 0.025%
MIN_COMMISSION = 5.0  # minimum 5 CNY
STAMP_DUTY_RATE = 0.001  # 0.1% sell side only
SLIPPAGE = 0.001  # 0.1% default

# Multi-stock / portfolio constants
LOT_SIZE = 100  # A-share minimum trading unit (整手)


# ---------------------------------------------------------------------------
# Friction, limit, and benchmark helpers
# ---------------------------------------------------------------------------


def _apply_friction(price: float, quantity: int, side: str) -> tuple:
    """Return (execution_price, commission, stamp_duty, slippage_cost).

    - BUY: execution price is slightly higher (slippage works against buyer).
    - SELL: execution price is slightly lower (slippage works against seller).
    - Commission applies to both sides; stamp duty only on SELL.
    """
    if side == "BUY":
        exec_price = price * (1 + SLIPPAGE)
    else:
        exec_price = price * (1 - SLIPPAGE)

    trade_value = exec_price * quantity
    commission = max(trade_value * COMMISSION_RATE, MIN_COMMISSION)
    stamp_duty = trade_value * STAMP_DUTY_RATE if side == "SELL" else 0.0
    slippage_cost = abs(exec_price - price) * quantity

    return (
        round(exec_price, 4),
        round(commission, 4),
        round(stamp_duty, 4),
        round(slippage_cost, 4),
    )


def _can_trade(quote, side: str) -> bool:
    """Check if a trade can execute based on trade_status and price limits.

    A-shares have ±10 % daily price limits (main board).  We block:
    - BUY  when the stock is limit-up   (``change_rate >= 9.9``) or suspended.
    - SELL when the stock is limit-down (``change_rate <= -9.9``) or suspended.
    """
    trade_status = getattr(quote, "trade_status", 1)  # 1 = normal, 0 = suspended
    change_rate = getattr(quote, "change_rate", 0) or 0

    if trade_status == 0:
        return False
    if side == "BUY" and change_rate >= 9.9:
        return False
    if side == "SELL" and change_rate <= -9.9:
        return False
    return True


def _round_to_lot(quantity: float) -> int:
    """Round down to 100-share lots (整手)."""
    return int(quantity // LOT_SIZE) * LOT_SIZE


def _max_buy_shares(price: float, available_cash: float) -> int:
    """Maximum shares buyable at price after slippage + commission."""
    if price <= 0 or available_cash <= MIN_COMMISSION:
        return 0
    exec_price = price * (1 + SLIPPAGE)
    # Try rate-based commission first (applies for larger trades)
    raw = int(available_cash / (exec_price * (1 + COMMISSION_RATE)))
    if raw > 0 and raw * exec_price * COMMISSION_RATE < MIN_COMMISSION:
        # Adjust for min-commission floor on small trades
        if exec_price + MIN_COMMISSION > available_cash:
            return 0
        return int((available_cash - MIN_COMMISSION) / exec_price)
    return max(raw, 0)


def _allocate_positions(
    available_stocks: List[str],
    scores: Dict[str, float],
    total_cash: float,
    current_prices: Dict[str, float],
    allocation: str = "equal_weight",
    max_position_pct: float = 0.20,
) -> Dict[str, int]:
    """Allocate cash to stocks based on strategy.

    Returns dict of stock_code -> shares (already rounded to lots).
    """
    if not available_stocks:
        return {}

    if allocation == "equal_weight":
        weight = 1.0 / len(available_stocks)
        weights = {s: weight for s in available_stocks}
    elif allocation == "score_weighted":
        total_score = sum(max(scores.get(s, 0), 0) for s in available_stocks)
        if total_score <= 0:
            weights = {s: 1.0 / len(available_stocks) for s in available_stocks}
        else:
            weights = {
                s: max(scores.get(s, 0), 0) / total_score for s in available_stocks
            }
    else:
        return {}

    allocations: Dict[str, int] = {}
    for stock in available_stocks:
        price = current_prices.get(stock, 0)
        if price <= 0:
            continue
        weight = min(weights.get(stock, 0), max_position_pct)
        cash_for_stock = total_cash * weight
        raw_shares = cash_for_stock / price
        shares = _round_to_lot(raw_shares)
        if shares >= LOT_SIZE:
            allocations[stock] = shares

    return allocations


def _compute_benchmark(
    benchmark_code: str,
    start_date,
    end_date,
    initial_cash: float,
) -> dict:
    """Simulate buy-and-hold on *benchmark_code* and return metrics.

    Returns an empty dict when the benchmark data is unavailable.
    """
    try:
        quotes = list(
            StockDailyQuote.objects(code=benchmark_code)
            .filter(date__gte=start_date, date__lte=end_date)
            .order_by("date")
        )
        if len(quotes) < 2:
            return {}

        start_price = _closing_price(quotes[0])
        end_price = _closing_price(quotes[-1])
        if start_price <= 0:
            return {}

        final_value = initial_cash * (end_price / start_price)
        total_return = final_value - initial_cash
        total_return_pct = (total_return / initial_cash) * 100
        num_days = len(quotes)
        years = num_days / TRADING_DAYS_PER_YEAR if num_days >= 2 else 0
        cagr = ((final_value / initial_cash) ** (1.0 / years) - 1) if years > 0 else 0.0

        # Daily benchmark returns for information-ratio (aligned to trading day index)
        benchmark_daily_returns: List[float] = []
        prev = start_price
        for q in quotes[1:]:
            cp = _closing_price(q)
            if prev > 0:
                benchmark_daily_returns.append((cp - prev) / prev)
            else:
                benchmark_daily_returns.append(0.0)
            prev = cp

        return {
            "benchmark_code": benchmark_code,
            "benchmark_return": round(total_return, 4),
            "benchmark_return_pct": round(total_return_pct, 4),
            "benchmark_annualized_return": round(cagr * 100, 4),
            "benchmark_daily_returns": benchmark_daily_returns,
        }
    except Exception:
        logger.warning(
            "Benchmark computation failed for %s", benchmark_code, exc_info=True
        )
        return {}


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
    benchmark_code: str = "sh000300",
    horizon: int | None = None,
    entry_threshold: float = 70.0,
    exit_threshold: float = 50.0,
    stop_loss_pct: float = -5.0,
    score_delta: float = 10.0,
    model_version: str | None = None,
    consensus_entry_thresholds: Dict[int, float] | None = None,
    consensus_exit_thresholds: Dict[int, float] | None = None,
) -> Dict[str, Any]:
    """Run a single-stock daily backtest and return the result dict.

    Parameters
    ----------
    stock_code : str
        The stock symbol (e.g. ``"sh600519"``).
    strategy : str
        ``"MA_CROSS"``, ``"BUY_HOLD"``, ``"SCORE_THRESHOLD"``,
        ``"SCORE_MOMENTUM"``, or ``"MULTI_HORIZON_CONSENSUS"``
        (case-insensitive).
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
    benchmark_code : str
        Benchmark index code (default ``"sh000300"`` for CSI 300).
    horizon : int | None
        Scoring horizon (5, 20, or 60). Required for score-driven strategies.
    entry_threshold : float
        Minimum score to enter a position (SCORE_THRESHOLD only, default 70).
    exit_threshold : float
        Maximum score before exiting a position (SCORE_THRESHOLD only, default 50).
    stop_loss_pct : float
        Stop-loss percentage as a negative number (default -5.0 = -5%).
    score_delta : float
        Score change required to trigger a trade (SCORE_MOMENTUM only, default 10).
    model_version : str | None
        Scoring model version filter. When None, all versions are used.
    consensus_entry_thresholds : dict | None
        Per-horizon entry thresholds for MULTI_HORIZON_CONSENSUS.
        Default: {5: 60, 20: 55, 60: 50}.
    consensus_exit_thresholds : dict | None
        Per-horizon exit thresholds for MULTI_HORIZON_CONSENSUS.
        Default: {5: 30, 20: 35, 60: 40}.

    Returns
    -------
    dict
        A dictionary that includes all metrics, the trade list, the daily
        equity curve, and the result document id when persisted.
    """
    strategy_norm = (strategy or "").strip().upper()
    if strategy_norm not in VALID_STRATEGIES:
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

    # Load score predictions for score-driven strategies
    score_map: Dict[datetime, Any] = {}
    score_maps: Dict[int, Dict[datetime, Any]] = {}  # for consensus: horizon->map
    if strategy_norm in ("SCORE_THRESHOLD", "SCORE_MOMENTUM"):
        if not horizon:
            return _error("horizon is required for score-driven strategies")
        score_q = StockScorePrediction.objects(
            stock_code=stock_code,
            horizon=horizon,
            date__gte=start_date,
            date__lte=end_date,
        )
        if model_version:
            score_q = score_q.filter(model_version=model_version)
        scores = list(score_q.order_by("date"))
        for s in scores:
            d = s.date.replace(hour=0, minute=0, second=0, microsecond=0)
            score_map[d] = s
        if not score_map:
            return _error(
                "No score data",
                f"No StockScorePrediction for {stock_code} horizon={horizon} in range",
            )
    elif strategy_norm == "MULTI_HORIZON_CONSENSUS":
        for h in (5, 20, 60):
            score_q = StockScorePrediction.objects(
                stock_code=stock_code,
                horizon=h,
                date__gte=start_date,
                date__lte=end_date,
            )
            if model_version:
                score_q = score_q.filter(model_version=model_version)
            h_map: Dict[datetime, Any] = {}
            for s in score_q.order_by("date"):
                d = s.date.replace(hour=0, minute=0, second=0, microsecond=0)
                h_map[d] = s
            if h_map:
                score_maps[h] = h_map

    # Set default consensus thresholds
    if consensus_entry_thresholds is None:
        consensus_entry_thresholds = {5: 60.0, 20: 55.0, 60: 50.0}
    if consensus_exit_thresholds is None:
        consensus_exit_thresholds = {5: 30.0, 20: 35.0, 60: 40.0}

    # Run the strategy simulation
    sim_result = _simulate(
        strategy=strategy_norm,
        trading_days=trading_days,
        quote_map=quote_map,
        factor_map=factor_map,
        initial_cash=initial_cash,
        score_map=score_map,
        score_maps=score_maps,
        horizon=horizon,
        entry_threshold=entry_threshold,
        exit_threshold=exit_threshold,
        stop_loss_pct=stop_loss_pct,
        score_delta=score_delta,
        consensus_entry_thresholds=consensus_entry_thresholds,
        consensus_exit_thresholds=consensus_exit_thresholds,
    )

    # Compute final metrics (with friction totals)
    total_commission = sim_result.pop("total_commission", 0.0)
    total_stamp_duty = sim_result.pop("total_stamp_duty", 0.0)
    total_slippage = sim_result.pop("total_slippage", 0.0)

    metrics = _compute_metrics(
        initial_cash=initial_cash,
        final_value=sim_result["final_value"],
        trades=sim_result["trades"],
        daily_values=sim_result["daily_values"],
        total_commission=total_commission,
        total_stamp_duty=total_stamp_duty,
        total_slippage=total_slippage,
    )
    sim_result.update(metrics)

    # Benchmark comparison
    bench = _compute_benchmark(benchmark_code, start_date, end_date, initial_cash)
    if bench:
        excess_return = round(sim_result["total_return"] - bench["benchmark_return"], 4)
        excess_return_pct = round(
            sim_result["total_return_pct"] - bench["benchmark_return_pct"], 4
        )
        sim_result.update(bench)
        sim_result["excess_return"] = excess_return
        sim_result["excess_return_pct"] = excess_return_pct

        # Information ratio from daily excess returns
        bench_returns = bench.get("benchmark_daily_returns", [])
        info_ratio = _compute_information_ratio(
            sim_result["daily_values"], bench_returns, initial_cash
        )
        sim_result["information_ratio"] = info_ratio
    else:
        sim_result["benchmark_code"] = benchmark_code
        sim_result["benchmark_return"] = 0.0
        sim_result["benchmark_return_pct"] = 0.0
        sim_result["benchmark_annualized_return"] = 0.0
        sim_result["excess_return"] = 0.0
        sim_result["excess_return_pct"] = 0.0
        sim_result["information_ratio"] = 0.0

    # Persist
    if save_result:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        name = (
            f"{stock_code}-{strategy_norm}-{start_date.date()}-{end_date.date()}-{ts}"
        )
        doc = BacktestResult(
            name=name,
            stock_code=stock_code,
            stock_name=stock_name,
            strategy=strategy_norm,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            final_value=sim_result["final_value"],
            total_return=sim_result["total_return"],
            total_return_pct=sim_result["total_return_pct"],
            annualized_return=sim_result["annualized_return"],
            max_drawdown=sim_result["max_drawdown"],
            max_drawdown_duration=sim_result["max_drawdown_duration"],
            sharpe_ratio=sim_result["sharpe_ratio"],
            win_rate=sim_result["win_rate"],
            total_trades=sim_result["total_trades"],
            profit_trades=sim_result["profit_trades"],
            loss_trades=sim_result["loss_trades"],
            best_trade=sim_result["best_trade"],
            worst_trade=sim_result["worst_trade"],
            status="COMPLETED",
            trades=sim_result["trades"],
            daily_values=sim_result["daily_values"],
            # Friction costs
            total_commission=total_commission,
            total_stamp_duty=total_stamp_duty,
            total_slippage=total_slippage,
            gross_return=sim_result.get("gross_return", 0.0),
            gross_return_pct=sim_result.get("gross_return_pct", 0.0),
            # Benchmark
            benchmark_code=sim_result.get("benchmark_code", benchmark_code),
            benchmark_return=sim_result.get("benchmark_return", 0.0),
            benchmark_return_pct=sim_result.get("benchmark_return_pct", 0.0),
            benchmark_annualized_return=sim_result.get(
                "benchmark_annualized_return", 0.0
            ),
            excess_return=sim_result.get("excess_return", 0.0),
            excess_return_pct=sim_result.get("excess_return_pct", 0.0),
            information_ratio=sim_result.get("information_ratio", 0.0),
            score_config={
                "horizon": horizon,
                "entry_threshold": entry_threshold,
                "exit_threshold": exit_threshold,
                "stop_loss_pct": stop_loss_pct,
                "score_delta": score_delta,
                "model_version": model_version,
            }
            if strategy_norm in ("SCORE_THRESHOLD", "SCORE_MOMENTUM")
            else None,
            horizon=horizon
            if strategy_norm in ("SCORE_THRESHOLD", "SCORE_MOMENTUM")
            else None,
            completed_at=datetime.now(timezone.utc),
        )
        doc.save()
        sim_result["id"] = str(doc.id)
        sim_result["name"] = name
    else:
        sim_result["id"] = None
        sim_result["name"] = None

    return sim_result


# ---------------------------------------------------------------------------
# Public entry point – multi-stock
# ---------------------------------------------------------------------------
def run_multi_stock_backtest(
    stock_codes: List[str],
    strategy: str,
    start_date: datetime,
    end_date: datetime,
    initial_cash: float = 100_000.0,
    save_result: bool = True,
    benchmark_code: str = "sh000300",
    horizon: int | None = None,
    entry_threshold: float = 70.0,
    exit_threshold: float = 50.0,
    stop_loss_pct: float = -5.0,
    score_delta: float = 10.0,
    model_version: str | None = None,
    top_n: int = 10,
    rebalance_interval: int = 5,
    allocation: str = "equal_weight",
    max_position_pct: float = 0.20,
) -> Dict[str, Any]:
    """Run a multi-stock portfolio backtest and return the result dict.

    Parameters
    ----------
    stock_codes : list[str]
        List of stock symbols (e.g. ``["sh600519", "sz000858"]``).
    strategy : str
        Currently ``"TOP_N_ROTATION"`` (case-insensitive).
    start_date : datetime
        Start of the backtest window (inclusive).
    end_date : datetime
        End of the backtest window (inclusive).
    initial_cash : float
        Starting cash (default 100000 CNY).
    save_result : bool
        When *True* (default) persist as a ``BacktestResult`` document.
    benchmark_code : str
        Benchmark index code (default ``"sh000300"`` for CSI 300).
    horizon : int
        Scoring horizon (5, 20, or 60). Required for score-driven strategies.
    entry_threshold : float
        Minimum score for entry (SCORE_THRESHOLD only, default 70).
    exit_threshold : float
        Maximum score before exit (SCORE_THRESHOLD only, default 50).
    stop_loss_pct : float
        Stop-loss percentage as a negative number (default -5.0).
    score_delta : float
        Score change required (SCORE_MOMENTUM only, default 10).
    model_version : str | None
        Scoring model version filter.
    top_n : int
        Number of stocks to hold in the portfolio (TOP_N_ROTATION only).
    rebalance_interval : int
        Rebalance every K trading days (default 5).
    allocation : str
        Position sizing method: ``"equal_weight"`` or ``"score_weighted"``.
    max_position_pct : float
        Maximum allocation per stock as a decimal (default 0.20 = 20%).

    Returns
    -------
    dict
        Includes metrics, trade list, daily equity curve, per-stock contributions,
        and the saved document id.
    """
    strategy_norm = (strategy or "").strip().upper()
    if strategy_norm not in VALID_STRATEGIES:
        return _error("Unsupported strategy", f"strategy={strategy_norm}")
    if not isinstance(stock_codes, list) or len(stock_codes) < 2:
        return _error("Invalid stock_codes", "Must be a list with at least 2 codes")
    if len(stock_codes) > 100:
        return _error("Too many stocks", f"Maximum 100, got {len(stock_codes)}")

    # Normalize dates
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    if start_date > end_date:
        return _error("Invalid date range", "start_date must be <= end_date")
    if initial_cash <= 0:
        return _error("Invalid initial_cash", "Must be > 0")

    # Validate strategy-specific params
    if strategy_norm == "TOP_N_ROTATION":
        if horizon is None or horizon not in (5, 20, 60):
            return _error("horizon (5, 20, or 60) is required for TOP_N_ROTATION")
        if allocation not in ("equal_weight", "score_weighted"):
            return _error("allocation must be 'equal_weight' or 'score_weighted'")

    # Load stock names
    stock_names: Dict[str, str] = {}
    for sc in stock_codes:
        stock_names[sc] = _resolve_stock_name(sc)

    # Load quotes for ALL stocks in a single query
    quotes = list(
        StockDailyQuote.objects(code__in=stock_codes)
        .filter(date__gte=start_date, date__lte=end_date)
        .order_by("date")
    )
    if not quotes:
        return _error(
            "No quote data",
            "No quotes for any stock in range",
        )

    # Build per-stock quote maps and a common trading-day index
    quote_maps: Dict[str, Dict[datetime, Any]] = {sc: {} for sc in stock_codes}
    all_trading_days: set = set()
    for q in quotes:
        d = q.date.replace(hour=0, minute=0, second=0, microsecond=0)
        quote_maps[q.code][d] = q
        all_trading_days.add(d)
    trading_days = sorted(all_trading_days)
    if len(trading_days) < 2:
        return _error(
            "Insufficient data",
            f"Need >=2 trading days, got {len(trading_days)}",
        )

    # Load factor data if needed (for MA_CROSS which is single-stock only)
    factor_maps: Dict[str, Dict[datetime, Any]] = {}
    if strategy_norm == "MA_CROSS":
        factors = list(
            StockFactorDaily.objects(stock_code__in=stock_codes)
            .filter(date__gte=start_date, date__lte=end_date)
            .order_by("date")
        )
        for f in factors:
            d = f.date.replace(hour=0, minute=0, second=0, microsecond=0)
            factor_maps.setdefault(f.stock_code, {})[d] = f

    # Load score predictions for score-driven strategies
    score_maps: Dict[str, Dict[datetime, Any]] = {}
    if strategy_norm in ("SCORE_THRESHOLD", "SCORE_MOMENTUM", "TOP_N_ROTATION"):
        if not horizon:
            return _error("horizon is required for score-driven strategies")
        score_q = StockScorePrediction.objects(
            stock_code__in=stock_codes,
            horizon=horizon,
            date__gte=start_date,
            date__lte=end_date,
        )
        if model_version:
            score_q = score_q.filter(model_version=model_version)
        scores = list(score_q.order_by("date"))
        for s in scores:
            d = s.date.replace(hour=0, minute=0, second=0, microsecond=0)
            score_maps.setdefault(s.stock_code, {})[d] = s
        if not score_maps:
            return _error(
                "No score data",
                f"No StockScorePrediction for horizon={horizon} in range",
            )

    # Run the multi-stock simulation (or fall back to single-stock)
    if strategy_norm == "TOP_N_ROTATION":
        sim_result = _simulate_multi(
            strategy=strategy_norm,
            trading_days=trading_days,
            quote_maps=quote_maps,
            factor_maps=factor_maps,
            score_maps=score_maps,
            stock_names=stock_names,
            initial_cash=initial_cash,
            top_n=top_n,
            rebalance_interval=rebalance_interval,
            allocation=allocation,
            max_position_pct=max_position_pct,
            horizon=horizon,
            entry_threshold=entry_threshold,
            exit_threshold=exit_threshold,
            stop_loss_pct=stop_loss_pct,
            score_delta=score_delta,
        )
    else:
        return _error(
            "Multi-stock strategy not implemented",
            f"strategy={strategy_norm}",
        )

    if "error" in sim_result:
        return sim_result

    # Compute final metrics
    total_commission = sim_result.pop("total_commission", 0.0)
    total_stamp_duty = sim_result.pop("total_stamp_duty", 0.0)
    total_slippage = sim_result.pop("total_slippage", 0.0)
    per_stock_contributions = sim_result.pop("per_stock_contributions", [])

    metrics = _compute_metrics(
        initial_cash=initial_cash,
        final_value=sim_result["final_value"],
        trades=sim_result["trades"],
        daily_values=sim_result["daily_values"],
        total_commission=total_commission,
        total_stamp_duty=total_stamp_duty,
        total_slippage=total_slippage,
    )
    sim_result.update(metrics)
    sim_result["per_stock_contributions"] = per_stock_contributions

    # Benchmark comparison
    bench = _compute_benchmark(benchmark_code, start_date, end_date, initial_cash)
    if bench:
        excess_return = round(sim_result["total_return"] - bench["benchmark_return"], 4)
        excess_return_pct = round(
            sim_result["total_return_pct"] - bench["benchmark_return_pct"], 4
        )
        sim_result.update(bench)
        sim_result["excess_return"] = excess_return
        sim_result["excess_return_pct"] = excess_return_pct

        bench_returns = bench.get("benchmark_daily_returns", [])
        info_ratio = _compute_information_ratio(
            sim_result["daily_values"], bench_returns, initial_cash
        )
        sim_result["information_ratio"] = info_ratio
    else:
        sim_result["benchmark_code"] = benchmark_code
        sim_result["benchmark_return"] = 0.0
        sim_result["benchmark_return_pct"] = 0.0
        sim_result["benchmark_annualized_return"] = 0.0
        sim_result["excess_return"] = 0.0
        sim_result["excess_return_pct"] = 0.0
        sim_result["information_ratio"] = 0.0

    # Persist
    stocks_str = ",".join(stock_codes)
    stocks_name = ", ".join(stock_names.get(sc, sc) for sc in stock_codes)
    if save_result:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        name = (
            f"{stocks_str}-{strategy_norm}-{start_date.date()}-{end_date.date()}-{ts}"
        )
        # Truncate name if too long (Mongo unique index may have length limits)
        if len(name) > 200:
            first_stock = stock_codes[0]
            name = (
                f"{first_stock}-and-{len(stock_codes) - 1}-stocks"
                f"-{strategy_norm}-{start_date.date()}-{end_date.date()}-{ts}"
            )
        doc = BacktestResult(
            name=name,
            stock_code=stocks_str,
            stock_name=stocks_name,
            strategy=strategy_norm,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            final_value=sim_result["final_value"],
            total_return=sim_result["total_return"],
            total_return_pct=sim_result["total_return_pct"],
            annualized_return=sim_result["annualized_return"],
            max_drawdown=sim_result["max_drawdown"],
            max_drawdown_duration=sim_result["max_drawdown_duration"],
            sharpe_ratio=sim_result["sharpe_ratio"],
            win_rate=sim_result["win_rate"],
            total_trades=sim_result["total_trades"],
            profit_trades=sim_result["profit_trades"],
            loss_trades=sim_result["loss_trades"],
            best_trade=sim_result["best_trade"],
            worst_trade=sim_result["worst_trade"],
            status="COMPLETED",
            trades=sim_result["trades"],
            daily_values=sim_result["daily_values"],
            # Friction costs
            total_commission=total_commission,
            total_stamp_duty=total_stamp_duty,
            total_slippage=total_slippage,
            gross_return=sim_result.get("gross_return", 0.0),
            gross_return_pct=sim_result.get("gross_return_pct", 0.0),
            # Benchmark
            benchmark_code=sim_result.get("benchmark_code", benchmark_code),
            benchmark_return=sim_result.get("benchmark_return", 0.0),
            benchmark_return_pct=sim_result.get("benchmark_return_pct", 0.0),
            benchmark_annualized_return=sim_result.get(
                "benchmark_annualized_return", 0.0
            ),
            excess_return=sim_result.get("excess_return", 0.0),
            excess_return_pct=sim_result.get("excess_return_pct", 0.0),
            information_ratio=sim_result.get("information_ratio", 0.0),
            score_config={
                "horizon": horizon,
                "entry_threshold": entry_threshold,
                "exit_threshold": exit_threshold,
                "stop_loss_pct": stop_loss_pct,
                "score_delta": score_delta,
                "model_version": model_version,
                "top_n": top_n,
                "rebalance_interval": rebalance_interval,
                "allocation": allocation,
                "max_position_pct": max_position_pct,
            },
            horizon=horizon,
            # Multi-stock fields
            per_stock_contributions=per_stock_contributions,
            top_n=top_n,
            rebalance_interval=rebalance_interval,
            allocation=allocation,
            completed_at=datetime.now(timezone.utc),
        )
        doc.save()
        sim_result["id"] = str(doc.id)
        sim_result["name"] = name
    else:
        sim_result["id"] = None
        sim_result["name"] = None

    return sim_result


# ---------------------------------------------------------------------------
# Internal helpers – simulation
# ---------------------------------------------------------------------------
def _simulate(
    strategy: str,
    trading_days: List[datetime],
    quote_map: Dict[datetime, StockDailyQuote],
    factor_map: Dict[datetime, StockFactorDaily],
    initial_cash: float,
    score_map: Dict[datetime, Any] | None = None,
    score_maps: Dict[int, Dict[datetime, Any]] | None = None,
    horizon: int | None = None,
    entry_threshold: float = 70.0,
    exit_threshold: float = 50.0,
    stop_loss_pct: float = -5.0,
    score_delta: float = 10.0,
    consensus_entry_thresholds: Dict[int, float] | None = None,
    consensus_exit_thresholds: Dict[int, float] | None = None,
) -> Dict[str, Any]:
    """Walk through trading days and apply the selected strategy.

    Now includes:
    - Friction model (commission, stamp duty, slippage)
    - Limit-up / limit-down constraints with retry on consecutive limit days
    - Skipped-trade tracking
    - Score-driven strategies (SCORE_THRESHOLD, SCORE_MOMENTUM)
    - Multi-horizon consensus (MULTI_HORIZON_CONSENSUS)
    """
    cash = initial_cash
    shares = 0.0
    trades: List[Dict[str, Any]] = []
    skipped_trades: List[Dict[str, Any]] = []
    daily_values: List[Dict[str, Any]] = []

    total_commission = 0.0
    total_stamp_duty = 0.0
    total_slippage = 0.0

    num_days = len(trading_days)

    if strategy == "BUY_HOLD":
        # Buy on first day, hold, sell on last day
        for i, day in enumerate(trading_days):
            quote = quote_map[day]
            price = _closing_price(quote)

            if i == 0:
                # Initial buy (with friction and limit check)
                if _can_trade(quote, "BUY"):
                    shares = _max_buy_shares(price, cash)
                    if shares > 0:
                        exec_price, comm, duty, slip = _apply_friction(
                            price, shares, "BUY"
                        )
                        cost = shares * exec_price
                        cash -= cost + comm
                        total_commission += comm
                        total_stamp_duty += duty  # always 0 for BUY
                        total_slippage += slip
                    else:
                        exec_price = price
                        cost = 0.0
                        comm = 0.0
                        slip = 0.0
                    if shares > 0:
                        trades.append(
                            {
                                "date": day.isoformat(),
                                "side": "BUY",
                                "price": round(price, 4),
                                "exec_price": round(exec_price, 4),
                                "quantity": shares,
                                "amount": round(cost, 4),
                                "commission": round(comm, 4),
                                "stamp_duty": 0.0,
                                "slippage": round(slip, 4),
                                "reason": "Initial buy (BUY_HOLD)",
                            }
                        )
                else:
                    reason = _blocked_reason(quote, "BUY")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_BUY",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

            elif i == num_days - 1:
                # Final sell (with friction and limit check)
                if shares > 0:
                    if _can_trade(quote, "SELL"):
                        exec_price, comm, duty, slip = _apply_friction(
                            price, shares, "SELL"
                        )
                        proceeds = shares * exec_price
                        cash += proceeds - comm - duty
                        total_commission += comm
                        total_stamp_duty += duty
                        total_slippage += slip

                        buy_amounts = sum(
                            t["amount"] for t in trades if t["side"] == "BUY"
                        )
                        sell_amounts = sum(
                            t["amount"] for t in trades if t["side"] == "SELL"
                        )
                        buy_comm = sum(
                            t.get("commission", 0) for t in trades if t["side"] == "BUY"
                        )
                        sell_comm = sum(
                            t.get("commission", 0)
                            for t in trades
                            if t["side"] == "SELL"
                        )
                        pnl = (proceeds - comm - duty) - (
                            buy_amounts - sell_amounts + buy_comm - sell_comm
                        )
                        trades.append(
                            {
                                "date": day.isoformat(),
                                "side": "SELL",
                                "price": round(price, 4),
                                "exec_price": round(exec_price, 4),
                                "quantity": shares,
                                "amount": round(proceeds, 4),
                                "commission": round(comm, 4),
                                "stamp_duty": round(duty, 4),
                                "slippage": round(slip, 4),
                                "pnl": round(pnl, 4),
                                "reason": "Final sell (BUY_HOLD)",
                            }
                        )
                        shares = 0.0
                    else:
                        reason = _blocked_reason(quote, "SELL")
                        skipped_trades.append(
                            {
                                "date": day.isoformat(),
                                "side": "SKIPPED_SELL",
                                "reason": reason,
                                "price": round(price, 4),
                            }
                        )

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
        pending_signal: Optional[str] = None  # "BUY" or "SELL" blocked by limits

        for i, day in enumerate(trading_days):
            quote = quote_map[day]
            price = _closing_price(quote)
            factor = factor_map.get(day)

            curr_ma10 = factor.ma_10 if factor is not None else None
            curr_ma20 = factor.ma_20 if factor is not None else None

            # Detect cross signals
            signal: Optional[str] = None
            if (
                i > 0
                and prev_ma10 is not None
                and prev_ma20 is not None
                and curr_ma10 is not None
                and curr_ma20 is not None
            ):
                # Golden cross: MA10 crosses above MA20
                if prev_ma10 <= prev_ma20 and curr_ma10 > curr_ma20:
                    signal = "BUY"
                # Dead cross: MA10 crosses below MA20
                elif prev_ma10 >= prev_ma20 and curr_ma10 < curr_ma20:
                    signal = "SELL"

            # If a new signal fires, it overrides any pending signal
            if signal is not None:
                pending_signal = signal

            # Try to execute pending signal
            action_taken = False
            if pending_signal == "BUY" and shares == 0 and price > 0:
                if _can_trade(quote, "BUY"):
                    shares = _max_buy_shares(price, cash)
                    if shares > 0:
                        exec_price, comm, duty, slip = _apply_friction(
                            price, shares, "BUY"
                        )
                        cost = shares * exec_price
                        cash -= cost + comm
                        total_commission += comm
                        total_stamp_duty += duty  # 0 for BUY
                        total_slippage += slip
                    else:
                        exec_price = price
                        cost = 0.0
                        comm = 0.0
                        slip = 0.0
                    reason = (
                        f"Golden cross (MA10 {round(curr_ma10, 4)}"
                        f" > MA20 {round(curr_ma20, 4)})"
                        if curr_ma10 is not None
                        and curr_ma20 is not None
                        and curr_ma10 > curr_ma20
                        else "Pending buy executed"
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "BUY",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(cost, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": 0.0,
                            "slippage": round(slip, 4),
                            "reason": reason,
                        }
                    )
                    pending_signal = None
                    action_taken = True
                else:
                    reason = _blocked_reason(quote, "BUY")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_BUY",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )
                    action_taken = True

            if not action_taken and pending_signal == "SELL" and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = shares * exec_price
                    cash += proceeds - comm - duty
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    reason = (
                        f"Dead cross (MA10 {round(curr_ma10, 4)}"
                        f" < MA20 {round(curr_ma20, 4)})"
                        if curr_ma10 is not None
                        and curr_ma20 is not None
                        and curr_ma10 < curr_ma20
                        else "Pending sell executed"
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": reason,
                        }
                    )
                    shares = 0.0
                    pending_signal = None
                else:
                    reason = _blocked_reason(quote, "SELL")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_SELL",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

            # Liquidation at end if still holding (with limit check)
            if i == num_days - 1 and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = shares * exec_price
                    cash += proceeds - comm - duty
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": "Liquidation at end of backtest",
                        }
                    )
                    shares = 0.0
                else:
                    reason = _blocked_reason(quote, "SELL")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_SELL",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

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

    elif strategy == "SCORE_THRESHOLD":
        stop_loss_price: Optional[float] = None
        pending_signal: Optional[str] = None  # "BUY" or "SELL" blocked by limits

        for i, day in enumerate(trading_days):
            quote = quote_map[day]
            price = _closing_price(quote)
            score_doc = score_map.get(day) if score_map else None

            # Get today's score (only use data from this day or earlier — look-ahead guard)
            curr_score = score_doc.score if score_doc else None

            # Check stop-loss for existing position
            if shares > 0 and stop_loss_price is not None and price <= stop_loss_price:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = exec_price * shares - comm - duty
                    cash += proceeds
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": f"Stop loss triggered at {round(price, 4)}",
                        }
                    )
                    shares = 0.0
                    stop_loss_price = None
                    pending_signal = None

            # If a score-based signal fires, it overrides any pending signal
            if curr_score is not None:
                if shares == 0 and curr_score >= entry_threshold:
                    pending_signal = "BUY"
                elif shares > 0 and curr_score < exit_threshold:
                    pending_signal = "SELL"

            # Try to execute pending signal
            action_taken = False
            if pending_signal == "BUY" and shares == 0 and price > 0:
                if _can_trade(quote, "BUY"):
                    shares = _max_buy_shares(price, cash)
                    if shares > 0:
                        exec_price, comm, duty, slip = _apply_friction(
                            price, shares, "BUY"
                        )
                        cost = shares * exec_price
                        cash -= cost + comm
                        total_commission += comm
                        total_stamp_duty += duty  # 0 for BUY
                        total_slippage += slip
                    else:
                        exec_price = price
                        cost = 0.0
                        comm = 0.0
                        slip = 0.0
                    reason = (
                        f"SCORE_THRESHOLD entry: Score{horizon}={round(curr_score, 1)}"
                        f" >= {round(entry_threshold, 1)}"
                        if curr_score is not None
                        else "SCORE_THRESHOLD entry"
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "BUY",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(cost, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": 0.0,
                            "slippage": round(slip, 4),
                            "reason": reason,
                        }
                    )
                    stop_loss_price = price * (1 + stop_loss_pct / 100)
                    pending_signal = None
                    action_taken = True
                else:
                    reason = _blocked_reason(quote, "BUY")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_BUY",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )
                    action_taken = True

            if not action_taken and pending_signal == "SELL" and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = exec_price * shares - comm - duty
                    cash += proceeds
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    reason = (
                        f"SCORE_THRESHOLD exit: Score{horizon}={round(curr_score, 1)}"
                        f" < {round(exit_threshold, 1)}"
                        if curr_score is not None
                        else "SCORE_THRESHOLD exit"
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": reason,
                        }
                    )
                    shares = 0.0
                    stop_loss_price = None
                    pending_signal = None
                else:
                    reason = _blocked_reason(quote, "SELL")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_SELL",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

            # Liquidation at end if still holding (with limit check)
            if i == num_days - 1 and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = exec_price * shares - comm - duty
                    cash += proceeds
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": "Liquidation at end of backtest",
                        }
                    )
                    shares = 0.0
                else:
                    reason = _blocked_reason(quote, "SELL")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_SELL",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

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

    elif strategy == "SCORE_MOMENTUM":
        prev_score: Optional[float] = None
        stop_loss_price: Optional[float] = None
        pending_signal: Optional[str] = None  # "BUY" or "SELL" blocked by limits

        for i, day in enumerate(trading_days):
            quote = quote_map[day]
            price = _closing_price(quote)
            score_doc = score_map.get(day) if score_map else None

            # Get today's score (only use data from this day or earlier — look-ahead guard)
            curr_score = score_doc.score if score_doc else None

            # Check stop-loss for existing position
            if shares > 0 and stop_loss_price is not None and price <= stop_loss_price:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = exec_price * shares - comm - duty
                    cash += proceeds
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": f"Stop loss triggered at {round(price, 4)}",
                        }
                    )
                    shares = 0.0
                    stop_loss_price = None
                    pending_signal = None

            # Detect score momentum signals
            if i > 0 and prev_score is not None and curr_score is not None:
                score_change = curr_score - prev_score
                if shares == 0 and score_change >= score_delta:
                    pending_signal = "BUY"
                elif shares > 0 and score_change <= -score_delta:
                    pending_signal = "SELL"

            # Try to execute pending signal
            action_taken = False
            if pending_signal == "BUY" and shares == 0 and price > 0:
                if _can_trade(quote, "BUY"):
                    shares = _max_buy_shares(price, cash)
                    if shares > 0:
                        exec_price, comm, duty, slip = _apply_friction(
                            price, shares, "BUY"
                        )
                        cost = shares * exec_price
                        cash -= cost + comm
                        total_commission += comm
                        total_stamp_duty += duty  # 0 for BUY
                        total_slippage += slip
                    else:
                        exec_price = price
                        cost = 0.0
                        comm = 0.0
                        slip = 0.0
                    reason = (
                        f"SCORE_MOMENTUM entry: Score{horizon} Δ={round(score_change, 1)}"
                        f" >= {round(score_delta, 1)}"
                        if curr_score is not None and prev_score is not None
                        else "SCORE_MOMENTUM entry"
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "BUY",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(cost, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": 0.0,
                            "slippage": round(slip, 4),
                            "reason": reason,
                        }
                    )
                    stop_loss_price = price * (1 + stop_loss_pct / 100)
                    pending_signal = None
                    action_taken = True
                else:
                    reason = _blocked_reason(quote, "BUY")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_BUY",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )
                    action_taken = True

            if not action_taken and pending_signal == "SELL" and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = exec_price * shares - comm - duty
                    cash += proceeds
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    reason = (
                        f"SCORE_MOMENTUM exit: Score{horizon} Δ={round(score_change, 1)}"
                        f" <= {-round(score_delta, 1)}"
                        if curr_score is not None and prev_score is not None
                        else "SCORE_MOMENTUM exit"
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": reason,
                        }
                    )
                    shares = 0.0
                    stop_loss_price = None
                    pending_signal = None
                else:
                    reason = _blocked_reason(quote, "SELL")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_SELL",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

            # Liquidation at end if still holding (with limit check)
            if i == num_days - 1 and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = exec_price * shares - comm - duty
                    cash += proceeds
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": "Liquidation at end of backtest",
                        }
                    )
                    shares = 0.0
                else:
                    reason = _blocked_reason(quote, "SELL")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_SELL",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

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

            prev_score = curr_score

    elif strategy == "MULTI_HORIZON_CONSENSUS":
        sc_maps = score_maps or {}
        stop_loss_price: Optional[float] = None
        pending_signal: Optional[str] = None
        skipped_consensus: List[Dict[str, Any]] = []

        for i, day in enumerate(trading_days):
            quote = quote_map[day]
            price = _closing_price(quote)

            # Gather scores across available horizons
            horizon_scores: Dict[int, Optional[float]] = {}
            for h in (5, 20, 60):
                h_map = sc_maps.get(h, {})
                sd = h_map.get(day)
                horizon_scores[h] = sd.score if sd else None

            available_horizons = [h for h, s in horizon_scores.items() if s is not None]

            # Must have at least 2 horizons with data
            if len(available_horizons) >= 2:
                # Check if all available horizons meet entry thresholds
                entry_ok = all(
                    (horizon_scores[h] or 0)
                    >= (consensus_entry_thresholds or {}).get(h, 60)
                    for h in available_horizons
                )
                # Check if ANY horizon drops below exit threshold
                exit_triggered = any(
                    (horizon_scores[h] or 0)
                    < (consensus_exit_thresholds or {}).get(h, 30)
                    for h in available_horizons
                )

                # Check stop-loss for existing position
                if (
                    shares > 0
                    and stop_loss_price is not None
                    and price <= stop_loss_price
                ):
                    pending_signal = "SELL"

                if shares == 0 and entry_ok:
                    pending_signal = "BUY"
                elif shares > 0 and exit_triggered:
                    pending_signal = "SELL"
            else:
                # Fewer than 2 horizons — skip
                skipped_consensus.append(
                    {
                        "date": day.isoformat(),
                        "reason": f"insufficient horizons ({len(available_horizons)})",
                        "available_horizons": available_horizons,
                    }
                )

            # Execute pending signal (reuse pattern from SCORE_THRESHOLD)
            action_taken = False
            if pending_signal == "BUY" and shares == 0 and price > 0:
                if _can_trade(quote, "BUY"):
                    shares = _max_buy_shares(price, cash)
                    if shares > 0:
                        exec_price, comm, duty, slip = _apply_friction(
                            price, shares, "BUY"
                        )
                        cost = shares * exec_price
                        cash -= cost + comm
                        total_commission += comm
                        total_stamp_duty += duty
                        total_slippage += slip
                    else:
                        exec_price = price
                        cost = 0.0
                        comm = 0.0
                        slip = 0.0
                    # Set stop-loss
                    stop_loss_price = price * (1 + stop_loss_pct / 100)
                    score_strs = ", ".join(
                        f"Score{h}={horizon_scores.get(h, '?')}"
                        for h in available_horizons
                    )
                    reason = f"CONSENSUS entry: {score_strs}"
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "BUY",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(cost, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": 0.0,
                            "slippage": round(slip, 4),
                            "reason": reason,
                        }
                    )
                    pending_signal = None
                    action_taken = True
                else:
                    reason = _blocked_reason(quote, "BUY")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_BUY",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )
                    action_taken = True

            if not action_taken and pending_signal == "SELL" and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = exec_price * shares - comm - duty
                    cash += proceeds
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    score_strs = ", ".join(
                        f"Score{h}={horizon_scores.get(h, '?')}"
                        for h in available_horizons
                    )
                    reason = (
                        f"CONSENSUS exit: {score_strs}"
                        if price > (stop_loss_price or price * 2)
                        else f"Stop loss triggered at {round(price, 4)}"
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
                            "pnl": round(pnl, 4),
                            "reason": reason,
                        }
                    )
                    shares = 0.0
                    stop_loss_price = None
                    pending_signal = None
                else:
                    reason = _blocked_reason(quote, "SELL")
                    skipped_trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SKIPPED_SELL",
                            "reason": reason,
                            "price": round(price, 4),
                        }
                    )

            # Liquidation at end if still holding
            if i == num_days - 1 and shares > 0:
                if _can_trade(quote, "SELL"):
                    exec_price, comm, duty, slip = _apply_friction(
                        price, shares, "SELL"
                    )
                    proceeds = shares * exec_price
                    cash += proceeds - comm - duty
                    total_commission += comm
                    total_stamp_duty += duty
                    total_slippage += slip

                    buy_amounts = sum(t["amount"] for t in trades if t["side"] == "BUY")
                    sell_amounts = sum(
                        t["amount"] for t in trades if t["side"] == "SELL"
                    )
                    buy_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "BUY"
                    )
                    sell_comm = sum(
                        t.get("commission", 0) for t in trades if t["side"] == "SELL"
                    )
                    pnl = (proceeds - comm - duty) - (
                        buy_amounts - sell_amounts + buy_comm - sell_comm
                    )
                    trades.append(
                        {
                            "date": day.isoformat(),
                            "side": "SELL",
                            "price": round(price, 4),
                            "exec_price": round(exec_price, 4),
                            "quantity": shares,
                            "amount": round(proceeds, 4),
                            "commission": round(comm, 4),
                            "stamp_duty": round(duty, 4),
                            "slippage": round(slip, 4),
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

    return {
        "final_value": round(
            cash + shares * _closing_price(quote_map[trading_days[-1]]), 4
        ),
        "trades": trades,
        "daily_values": daily_values,
        "total_commission": round(total_commission, 4),
        "total_stamp_duty": round(total_stamp_duty, 4),
        "total_slippage": round(total_slippage, 4),
        "skipped_trades": skipped_trades,
    }


# ---------------------------------------------------------------------------
# Multi-stock simulator
# ---------------------------------------------------------------------------
def _simulate_multi(
    strategy: str,
    trading_days: List[datetime],
    quote_maps: Dict[str, Dict[datetime, Any]],
    factor_maps: Dict[str, Dict[datetime, Any]],
    score_maps: Dict[str, Dict[datetime, Any]],
    stock_names: Dict[str, str],
    initial_cash: float,
    top_n: int = 10,
    rebalance_interval: int = 5,
    allocation: str = "equal_weight",
    max_position_pct: float = 0.20,
    horizon: int | None = None,
    entry_threshold: float = 70.0,
    exit_threshold: float = 50.0,
    stop_loss_pct: float = -5.0,
    score_delta: float = 10.0,
) -> Dict[str, Any]:
    """Walk through trading days applying a multi-stock strategy.

    Currently supports ``TOP_N_ROTATION``.
    """

    # -- helpers ---------------------------------------------------------------
    def _liquidate_position(
        stock_code: str,
        pos: Dict[str, Any],
        day: datetime,
    ) -> Dict[str, Any] | None:
        """Sell a held position on *day* and return a trade record (or None
        when blocked).  Updates *cash*, the friction accumulators, and
        *per_stock_contributions* in-place."""
        nonlocal cash, total_commission, total_stamp_duty, total_slippage
        quote = quote_maps.get(stock_code, {}).get(day)
        if not quote or not _can_trade(quote, "SELL"):
            return None
        price = _closing_price(quote)
        exec_price, comm, stamp, slip = _apply_friction(price, pos["shares"], "SELL")
        proceeds = exec_price * pos["shares"] - comm - stamp
        cash += proceeds
        total_commission += comm
        total_stamp_duty += stamp
        total_slippage += slip
        realized_pnl = proceeds - (pos["avg_cost"] * pos["shares"])
        if stock_code not in per_stock_contributions:
            per_stock_contributions[stock_code] = {"realized_pnl": 0.0, "trades": 0}
        per_stock_contributions[stock_code]["realized_pnl"] += realized_pnl
        per_stock_contributions[stock_code]["trades"] += 1
        return {
            "date": day.isoformat(),
            "side": "SELL",
            "stock_code": stock_code,
            "price": round(price, 4),
            "exec_price": round(exec_price, 4),
            "quantity": pos["shares"],
            "amount": round(exec_price * pos["shares"], 4),
            "commission": round(comm, 4),
            "stamp_duty": round(stamp, 4),
            "slippage": round(slip, 4),
            "pnl": round(realized_pnl, 4),
            "reason": "TOP_N_ROTATION rebalance – removed from top N",
        }

    def _buy_stock(
        stock_code: str,
        shares: int,
        day: datetime,
        price: float,
        reason_hint: str,
    ) -> None:
        """Buy *shares* of *stock_code* on *day* and append a trade record.
        Updates *cash*, friction accumulators, and *positions* in-place."""
        nonlocal cash, total_commission, total_stamp_duty, total_slippage
        quote = quote_maps.get(stock_code, {}).get(day)
        if not quote or not _can_trade(quote, "BUY") or shares <= 0:
            return
        exec_price, comm, stamp, slip = _apply_friction(price, shares, "BUY")
        cost = exec_price * shares + comm + stamp
        if cost > cash:
            # Try to buy fewer shares
            affordable_raw = (cash - comm - stamp) / exec_price if exec_price > 0 else 0
            affordable = _round_to_lot(affordable_raw)
            if affordable < LOT_SIZE:
                return
            shares = affordable
            exec_price, comm, stamp, slip = _apply_friction(price, shares, "BUY")
            cost = exec_price * shares + comm + stamp
        cash -= cost
        total_commission += comm
        total_stamp_duty += stamp
        total_slippage += slip
        positions[stock_code] = {
            "shares": shares,
            "avg_cost": exec_price,
            "stop_loss_price": exec_price * (1 + stop_loss_pct / 100),
        }
        trades.append(
            {
                "date": day.isoformat(),
                "side": "BUY",
                "stock_code": stock_code,
                "price": round(price, 4),
                "exec_price": round(exec_price, 4),
                "quantity": shares,
                "amount": round(exec_price * shares, 4),
                "commission": round(comm, 4),
                "stamp_duty": round(stamp, 4),
                "slippage": round(slip, 4),
                "reason": reason_hint,
            }
        )

    # -- state -----------------------------------------------------------------
    cash = initial_cash
    positions: Dict[str, Dict[str, Any]] = {}
    trades: List[Dict[str, Any]] = []
    daily_values: List[Dict[str, Any]] = []
    per_stock_contributions: Dict[str, Dict[str, Any]] = {}
    total_commission = 0.0
    total_stamp_duty = 0.0
    total_slippage = 0.0

    # --------------------------------------------------------------------
    # TOP_N_ROTATION
    # --------------------------------------------------------------------
    if strategy == "TOP_N_ROTATION":
        for i, day in enumerate(trading_days):
            # -- Check stop-loss for all held positions ------------------------
            for stock_code in list(positions.keys()):
                pos = positions[stock_code]
                quote = quote_maps.get(stock_code, {}).get(day)
                if quote:
                    price = _closing_price(quote)
                    if price <= pos["stop_loss_price"]:
                        # Stop-loss triggered – override trade reason
                        exec_price, comm, stamp, slip = _apply_friction(
                            price, pos["shares"], "SELL"
                        )
                        if _can_trade(quote, "SELL"):
                            proceeds = exec_price * pos["shares"] - comm - stamp
                            cash += proceeds
                            total_commission += comm
                            total_stamp_duty += stamp
                            total_slippage += slip
                            realized_pnl = proceeds - (pos["avg_cost"] * pos["shares"])
                            if stock_code not in per_stock_contributions:
                                per_stock_contributions[stock_code] = {
                                    "realized_pnl": 0.0,
                                    "trades": 0,
                                }
                            per_stock_contributions[stock_code]["realized_pnl"] += (
                                realized_pnl
                            )
                            per_stock_contributions[stock_code]["trades"] += 1
                            trades.append(
                                {
                                    "date": day.isoformat(),
                                    "side": "SELL",
                                    "stock_code": stock_code,
                                    "price": round(price, 4),
                                    "exec_price": round(exec_price, 4),
                                    "quantity": pos["shares"],
                                    "amount": round(exec_price * pos["shares"], 4),
                                    "commission": round(comm, 4),
                                    "stamp_duty": round(stamp, 4),
                                    "slippage": round(slip, 4),
                                    "pnl": round(realized_pnl, 4),
                                    "reason": f"Stop loss triggered at {round(price, 4)}",
                                }
                            )
                            del positions[stock_code]

            # -- Rebalance ------------------------------------------------------
            if i % rebalance_interval == 0 or i == 0:
                # Collect today's scores for all stocks with data
                today_scores: Dict[str, float] = {}
                for stock_code in quote_maps:
                    score_doc = score_maps.get(stock_code, {}).get(day)
                    if score_doc and score_doc.score is not None:
                        today_scores[stock_code] = score_doc.score

                # Rank and select top N
                ranked = sorted(today_scores.items(), key=lambda x: x[1], reverse=True)
                top_n_stocks = [s for s, _ in ranked[:top_n]]

                # Sell positions NOT in top N
                for stock_code in list(positions.keys()):
                    if stock_code not in top_n_stocks:
                        pos = positions.pop(stock_code)
                        trade = _liquidate_position(stock_code, pos, day)
                        if trade is not None:
                            trades.append(trade)

                # Buy new stocks in top N (that we don't already hold)
                new_stocks = [s for s in top_n_stocks if s not in positions]
                if new_stocks and cash > 0:
                    prices: Dict[str, float] = {}
                    for s in new_stocks:
                        q = quote_maps.get(s, {}).get(day)
                        if q:
                            prices[s] = _closing_price(q)

                    alloc_shares = _allocate_positions(
                        new_stocks,
                        today_scores,
                        cash,
                        prices,
                        allocation,
                        max_position_pct,
                    )
                    for stock_code, shares in alloc_shares.items():
                        if shares < LOT_SIZE:
                            continue
                        price = prices.get(stock_code, 0)
                        if price <= 0:
                            continue
                        _buy_stock(
                            stock_code,
                            shares,
                            day,
                            price,
                            f"TOP_N_ROTATION entry – Score={round(today_scores.get(stock_code, 0), 1)}",
                        )

            # -- Compute daily equity -------------------------------------------
            positions_value = 0.0
            positions_detail: List[Dict[str, Any]] = []
            for stock_code, pos in positions.items():
                quote = quote_maps.get(stock_code, {}).get(day)
                if quote:
                    price = _closing_price(quote)
                    value = pos["shares"] * price
                    positions_value += value
                    positions_detail.append(
                        {
                            "stock_code": stock_code,
                            "stock_name": stock_names.get(stock_code, stock_code),
                            "shares": pos["shares"],
                            "price": round(price, 4),
                            "value": round(value, 4),
                        }
                    )

            equity = cash + positions_value
            daily_values.append(
                {
                    "date": day.isoformat(),
                    "cash": round(cash, 4),
                    "positions_value": round(positions_value, 4),
                    "equity": round(equity, 4),
                    "position_count": len(positions),
                    "positions": positions_detail,
                }
            )

        # -- Liquidate all remaining positions on the last day ------------------
        final_day = trading_days[-1]
        for stock_code in list(positions.keys()):
            pos = positions.pop(stock_code)
            trade = _liquidate_position(stock_code, pos, final_day)
            if trade is not None:
                trade["reason"] = "Liquidation at end of backtest"
                trades.append(trade)

        # Refresh the last daily value entry with final cash
        if daily_values:
            daily_values[-1]["cash"] = round(cash, 4)
            daily_values[-1]["equity"] = round(cash, 4)
            daily_values[-1]["positions_value"] = 0.0
            daily_values[-1]["positions"] = []

        final_value = cash

    else:
        return {
            "error": f"Multi-stock strategy '{strategy}' is not implemented",
            "trades": [],
            "daily_values": [],
            "per_stock_contributions": [],
            "total_commission": 0.0,
            "total_stamp_duty": 0.0,
            "total_slippage": 0.0,
        }

    return {
        "final_value": round(final_value, 4),
        "trades": trades,
        "daily_values": daily_values,
        "per_stock_contributions": [
            {
                "stock_code": sc,
                "stock_name": stock_names.get(sc, sc),
                **contrib,
            }
            for sc, contrib in per_stock_contributions.items()
        ],
        "total_commission": round(total_commission, 4),
        "total_stamp_duty": round(total_stamp_duty, 4),
        "total_slippage": round(total_slippage, 4),
    }


# ---------------------------------------------------------------------------
# Internal helpers – metrics
# ---------------------------------------------------------------------------
def _compute_metrics(
    initial_cash: float,
    final_value: float,
    trades: List[Dict[str, Any]],
    daily_values: List[Dict[str, Any]],
    total_commission: float = 0.0,
    total_stamp_duty: float = 0.0,
    total_slippage: float = 0.0,
) -> Dict[str, Any]:
    """Derive performance metrics from the simulation output.

    ``total_return`` / ``total_return_pct`` are **net** returns (after
    friction).  ``gross_return`` / ``gross_return_pct`` add back the
    cumulative friction costs.
    """

    total_return = round(final_value - initial_cash, 4)
    total_return_pct = round(total_return / initial_cash * 100, 4)

    # Gross return = net return + all friction costs
    gross_return = round(
        total_return + total_commission + total_stamp_duty + total_slippage, 4
    )
    gross_return_pct = round(gross_return / initial_cash * 100, 4)

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
        "max_drawdown": round(max_dd * 100, 4)
        if max_dd is not None
        else 0.0,  # as percentage
        "max_drawdown_duration": max_dd_dur,
        "sharpe_ratio": round(sharpe, 4),
        "win_rate": win_rate,
        "total_trades": total_trades,
        "profit_trades": profit_trades,
        "loss_trades": loss_trades,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "gross_return": gross_return,
        "gross_return_pct": gross_return_pct,
        "total_commission": round(total_commission, 4),
        "total_stamp_duty": round(total_stamp_duty, 4),
        "total_slippage": round(total_slippage, 4),
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

    variance = sum((r - mean_ret) ** 2 for r in daily_returns) / (
        len(daily_returns) - 1
    )
    std_ret = math.sqrt(variance)
    if std_ret == 0:
        return 0.0

    rf_daily = RISK_FREE_RATE / TRADING_DAYS_PER_YEAR
    return (mean_ret - rf_daily) / std_ret * math.sqrt(TRADING_DAYS_PER_YEAR)


# ---------------------------------------------------------------------------
# Internal helpers – information ratio
# ---------------------------------------------------------------------------
def _compute_information_ratio(
    daily_values: List[Dict[str, Any]],
    benchmark_daily_returns: List[float],
    initial_cash: float,
) -> float:
    """Compute annualized information ratio from daily excess returns."""
    if not daily_values or not benchmark_daily_returns:
        return 0.0
    if len(daily_values) < 2 or initial_cash <= 0:
        return 0.0

    # Strategy daily returns (aligned to benchmark by index, skipping day 0)
    strategy_returns: List[float] = []
    prev_eq = initial_cash
    for row in daily_values:
        eq = row["equity"]
        ret = (eq - prev_eq) / prev_eq if prev_eq > 0 else 0.0
        strategy_returns.append(ret)
        prev_eq = eq

    # Benchmark daily returns are already aligned (len = len(daily_values) - 1
    # typically, since first day has no return).  Pad or truncate to match.
    n = min(len(strategy_returns), len(benchmark_daily_returns))
    if n < 2:
        return 0.0

    excess_returns = [
        strategy_returns[i] - benchmark_daily_returns[i] for i in range(n)
    ]
    mean_excess = sum(excess_returns) / n
    if n < 2:
        return 0.0
    variance = sum((r - mean_excess) ** 2 for r in excess_returns) / (n - 1)
    std_excess = math.sqrt(variance)
    if std_excess == 0:
        return 0.0

    return round(mean_excess / std_excess * math.sqrt(TRADING_DAYS_PER_YEAR), 4)


def _blocked_reason(quote, side: str) -> str:
    """Return a human-readable reason why a trade is blocked."""
    trade_status = getattr(quote, "trade_status", 1)
    change_rate = getattr(quote, "change_rate", 0) or 0
    if trade_status == 0:
        return "suspended"
    if side == "BUY" and change_rate >= 9.9:
        return "limit_up_blocked"
    if side == "SELL" and change_rate <= -9.9:
        return "limit_down_blocked"
    return "unknown"


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
