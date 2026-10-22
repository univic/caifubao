# -*- coding: utf-8 -*-
"""No-look-ahead single-asset backtest engine.

Execution convention (mirrors ``autoresearch/profile.yaml``):

* a signal is decided on the close of day ``t``;
* the resulting target exposure is executed at the **open of day ``t+1``**;
* costs: commission both sides, sell stamp duty, slippage per side;
* integer board lots (100 shares), cash constrained, starting NAV 1,000,000.

Equity is marked at every close; returns for the first day of a new position
therefore include the open-to-close move only after the trade has happened.
The engine never reads data later than the bar that produced the decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

TRADING_DAYS = 244.0  # A-share average trading days per year


@dataclass(frozen=True)
class CostModel:
    commission: float = 0.00025
    min_commission: float = 5.0
    stamp_duty: float = 0.001
    slippage: float = 0.001

    def scale(self, factor: float) -> "CostModel":
        return CostModel(
            commission=self.commission * factor,
            min_commission=self.min_commission,
            stamp_duty=self.stamp_duty * factor,
            slippage=self.slippage * factor,
        )


DEFAULT_COSTS = CostModel()


@dataclass
class BacktestResult:
    equity: pd.Series
    exposure: pd.Series
    trades: pd.DataFrame
    metrics: dict = field(default_factory=dict)
    brake: pd.Series | None = None
    yearly: pd.DataFrame | None = None
    monthly: pd.DataFrame | None = None


def _trade_cost(notional: float, is_sell: bool, costs: CostModel) -> float:
    fee = max(notional * costs.commission, costs.min_commission)
    if is_sell:
        fee += notional * costs.stamp_duty
    return fee


def simulate(
    frame: pd.DataFrame,
    exposure: pd.Series,
    costs: CostModel = DEFAULT_COSTS,
    initial_cash: float = 1_000_000.0,
    lot: int = 100,
    *,
    reference_open: pd.Series | None = None,
    dd_free: float | None = None,
    dd_max: float | None = None,
    dd_reset_days: int = 60,
    band: float = 0.0,
    cppi_floor: float | None = None,
    cppi_mult: float = 2.0,
    equity_ma: int | None = None,
) -> BacktestResult:
    """Run the engine.

    ``exposure`` is indexed by the decision date and must be in ``[0, 1]``.
    ``reference_open`` lets a satellite asset (e.g. an index used only for
    timing) supply its own open while execution happens on ``frame``.
    """
    dates = frame.index
    opens = frame["open"].to_numpy(dtype=float)
    closes = frame["close"].to_numpy(dtype=float)
    target = exposure.reindex(dates).ffill().fillna(0.0).clip(0.0, 1.0)
    # Decision on close t is executed on open t+1; held through that session.
    held = target.shift(1).fillna(0.0).to_numpy()

    n = len(dates)
    cash = float(initial_cash)
    shares = 0
    equity_curve = np.empty(n)
    exposure_curve = np.empty(n)
    trades: list[dict] = []
    gross_buy = 0.0
    gross_sell = 0.0
    fees = 0.0
    peak = float(initial_cash)
    prev_close_equity = float(initial_cash)
    flat_days = 0
    brake_scale = np.ones(n)
    equity_hist: list[float] = []

    for i in range(n):
        px = opens[i]
        tgt = held[i]
        # Mark the account at the open before trading so sizing uses live equity.
        equity_open = cash + shares * px
        if dd_free is not None and dd_max is not None:
            peak = max(peak, prev_close_equity, equity_open)
            dd = equity_open / peak - 1.0
            if dd >= -dd_free:
                scale = 1.0
            elif dd <= -dd_max:
                scale = 0.0
            else:
                scale = 1.0 - (-dd - dd_free) / (dd_max - dd_free)
            if shares == 0:
                flat_days += 1
                if flat_days >= dd_reset_days:
                    peak = equity_open  # cool-off: re-base the high-water mark
                    scale = 1.0 if tgt > 0 else 0.0
            else:
                flat_days = 0
            brake_scale[i] = scale
            tgt = tgt * scale
        if cppi_floor is not None and equity_open > 0:
            floor = cppi_floor * initial_cash
            scale = float(
                np.clip(cppi_mult * (equity_open - floor) / equity_open, 0.0, 1.0)
            )
            tgt = tgt * scale
        if equity_ma is not None and len(equity_hist) >= equity_ma and equity_open > 0:
            if equity_open < float(np.mean(equity_hist[-equity_ma:])):
                tgt = 0.0
        cur_weight = shares * px / equity_open if equity_open > 0 else 0.0
        if band > 0 and abs(tgt - cur_weight) < band:
            tgt = cur_weight
        target_shares = int(np.floor(equity_open * tgt / (px * lot))) * lot
        delta = target_shares - shares
        if delta > 0:
            # Cash constraint: never spend more than the available cash.
            while delta > 0:
                notional = delta * px
                cost = notional * costs.slippage + _trade_cost(notional, False, costs)
                if cash - notional - cost >= 0:
                    break
                delta -= lot
        if delta != 0:
            notional = abs(delta) * px
            is_sell = delta < 0
            slip = notional * costs.slippage
            fee = _trade_cost(notional, is_sell, costs)
            cost = slip + fee
            fees += fee
            if is_sell:
                cash += notional - cost
                gross_sell += notional
            else:
                cash -= notional + cost
                gross_buy += notional
            shares += delta
            trades.append(
                {
                    "date": dates[i],
                    "side": "sell" if is_sell else "buy",
                    "price": px,
                    "shares": abs(delta),
                    "target_exposure": tgt,
                    "notional": notional,
                    "cost": cost,
                }
            )
        equity_curve[i] = cash + shares * closes[i]
        exposure_curve[i] = shares * px / equity_open if equity_open > 0 else 0.0
        prev_close_equity = equity_curve[i]
        equity_hist.append(equity_curve[i])

    equity = pd.Series(equity_curve, index=dates, name="equity")
    realized_exp = pd.Series(exposure_curve, index=dates, name="exposure")
    trade_frame = pd.DataFrame(trades)
    result = BacktestResult(equity=equity, exposure=realized_exp, trades=trade_frame)
    result.metrics = performance(
        results=result, costs_fees=fees, gross=(gross_buy, gross_sell)
    )
    result.brake = pd.Series(brake_scale, index=dates, name="brake")
    result.yearly = yearly_returns(equity)
    result.monthly = monthly_returns(equity)
    return result


def _max_drawdown(equity: np.ndarray) -> float:
    peak = np.maximum.accumulate(equity)
    return float(np.min(equity / peak - 1.0))


def performance(
    *,
    results: BacktestResult,
    costs_fees: float = 0.0,
    gross: tuple[float, float] = (0.0, 0.0),
) -> dict:
    equity = results.equity
    days = np.asarray(equity.to_numpy(dtype=float), dtype=float)
    if len(days) < 2 or days[0] <= 0:
        return {}
    rets = np.diff(days) / days[:-1]
    years = len(days) / TRADING_DAYS
    cagr = (days[-1] / days[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    mdd = _max_drawdown(days)
    vol = float(np.std(rets, ddof=1) * np.sqrt(TRADING_DAYS)) if len(rets) > 1 else 0.0
    sharpe = (
        float(np.mean(rets) / np.std(rets, ddof=1) * np.sqrt(TRADING_DAYS))
        if len(rets) > 1 and np.std(rets) > 0
        else 0.0
    )
    downside = rets[rets < 0]
    sortino = (
        float(np.mean(rets) / np.std(downside, ddof=1) * np.sqrt(TRADING_DAYS))
        if len(downside) > 1 and np.std(downside) > 0
        else 0.0
    )
    exposure = results.exposure
    return {
        "start": str(equity.index[0].date()),
        "end": str(equity.index[-1].date()),
        "final_nav": float(days[-1]),
        "total_return": float(days[-1] / days[0] - 1.0),
        "cagr": float(cagr),
        "max_drawdown": mdd,
        "calmar": float(cagr / abs(mdd))
        if mdd < 0
        else float("inf")
        if cagr > 0
        else 0.0,
        "volatility": vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "avg_exposure": float(exposure.mean()),
        "time_in_market": float((exposure > 0.01).mean()),
        "trades": int(len(results.trades) // 2),
        "fees": float(costs_fees),
        "turnover": float((gross[0] + gross[1]) / max(days.mean(), 1e-9)),
    }


def yearly_returns(equity: pd.Series) -> pd.DataFrame:
    first = equity.resample("YE").first()
    last = equity.resample("YE").last()
    out = pd.DataFrame({"return": last / first - 1.0})
    out.index = out.index.year
    return out


def monthly_returns(equity: pd.Series) -> pd.DataFrame:
    month_end = equity.resample("ME").last()
    table = month_end.pct_change()
    out = pd.DataFrame(
        {
            "year": table.index.year,
            "month": table.index.month,
            "return": table.to_numpy(),
        }
    )
    return out.dropna()


def close_to_close_reference(
    frame: pd.DataFrame, costs: CostModel = DEFAULT_COSTS
) -> BacktestResult:
    """Buy & hold on the actual open/close, used as the benchmark."""
    exposure = pd.Series(1.0, index=frame.index)
    return simulate(frame, exposure, costs=costs)


def bootstrap_metrics(
    daily_returns: pd.Series,
    n_boot: int = 2000,
    block: int = 20,
    seed: int = 7,
) -> dict:
    """Stationary block bootstrap of CAGR / MaxDD / Calmar."""
    rng = np.random.default_rng(seed)
    values = daily_returns.to_numpy(dtype=float)
    n = len(values)
    if n < block * 3:
        return {}
    starts = rng.integers(0, n - block, size=(n_boot, n // block + 1))
    cagr, mdd, calmar = [], [], []
    for row in starts:
        sample = np.concatenate([values[s : s + block] for s in row])[:n]
        curve = np.cumprod(1.0 + sample)
        years = len(sample) / TRADING_DAYS
        c = curve[-1] ** (1.0 / years) - 1.0
        m = _max_drawdown(curve)
        cagr.append(c)
        mdd.append(m)
        calmar.append(c / abs(m) if m < 0 else np.nan)
    cagr_arr = np.array(cagr)
    mdd_arr = np.array(mdd)
    return {
        "cagr_p05": float(np.percentile(cagr_arr, 5)),
        "cagr_p50": float(np.percentile(cagr_arr, 50)),
        "cagr_p95": float(np.percentile(cagr_arr, 95)),
        "mdd_p05": float(np.percentile(mdd_arr, 5)),
        "mdd_p50": float(np.percentile(mdd_arr, 50)),
        "mdd_p95": float(np.percentile(mdd_arr, 95)),
        "p_calmar_gt_1": float(np.nanmean(np.array(calmar) > 1.0)),
    }
