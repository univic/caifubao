# -*- coding: utf-8 -*-
"""Multi-asset ETF trend rotation: pure selection + simulation + signal.

The structure a 5-10万 account can actually run: a handful of liquid broad
ETFs, one rebalance a month, one to three positions, so each position is
15k-50k CNY and the 5 CNY minimum commission never binds.

Rule
----
On each rebalance session pick up to ``top_n`` eligible assets whose trailing
``lookback``-session return is positive **and** whose close is above its own
``ma``-session moving average, ranked by that return; if nothing qualifies,
hold the defensive (bond) ETF.

No look-ahead
-------------
The signal uses the rebalance session's own close, so that session's return must
be earned by the **previous** book; the newly selected book only starts on the
next session, and its round-trip cost is charged then. ``simulate`` enforces
this ordering and ``test_rotation.py`` pins it with a price path that would
otherwise hand the new book a large same-session gain.

Costs
-----
ETF round trip = commission (0.025 %/side) + slippage (0.1 %/side), no stamp
duty. Turnover is charged only on the replaced fraction of the book.
"""

from __future__ import annotations

import math

import pandas as pd

#: ETF round trip, mirrored from the paper execution model minus stamp duty.
ETF_ROUND_TRIP_COST = 2 * 0.00025 + 2 * 0.001
#: Daily accrual of the uninvested/defensive cash leg (~1 %/yr).
CASH_DAILY = 0.00004
#: Default defensive asset: a treasury ETF.
DEFAULT_DEFENSIVE = "511010.SH"


def select_targets(
    momentum: pd.Series,
    trend: pd.Series,
    *,
    top_n: int,
    defensive: str = DEFAULT_DEFENSIVE,
    eligible=None,
) -> list[str]:
    """Targets for one session: positive momentum **and** above the trend MA.

    ``momentum``/``trend`` are that session's cross-section (drop NaNs before
    calling). Assets missing from either are not selectable — a name without
    enough history never enters, which is what keeps a newly listed ETF out of
    the early backtest instead of silently back-filling it.
    """
    if top_n < 1:
        raise ValueError("top_n must be >= 1")
    allowed = set(eligible) if eligible is not None else None
    candidates = []
    for code, value in momentum.items():
        if code == defensive:
            continue
        if allowed is not None and code not in allowed:
            continue
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            continue
        if value <= 0:
            continue
        flag = trend.get(code)
        if flag is None or flag is pd.NA or bool(flag) is not True:
            continue
        candidates.append((float(value), code))
    candidates.sort(key=lambda item: (-item[0], item[1]))
    picks = [code for _, code in candidates[:top_n]]
    if picks:
        return picks
    return [defensive] if defensive and defensive in momentum.index else []


def _turnover(previous, current) -> float:
    """Replaced fraction of the new book (0 when it stays the same)."""
    if not current:
        return 0.0
    if not previous:
        return 1.0
    return 1.0 - len(set(current) & set(previous)) / len(current)


def realised_vol(prices: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    """Annualised trailing volatility per asset (for exposure scaling)."""
    returns = prices.sort_index().pct_change()
    return returns.rolling(window).std() * math.sqrt(252)


def simulate(
    prices: pd.DataFrame,
    *,
    lookback: int = 250,
    top_n: int = 3,
    ma: int = 200,
    cadence: int = 20,
    cost: float = ETF_ROUND_TRIP_COST,
    cash_daily: float = CASH_DAILY,
    defensive: str = DEFAULT_DEFENSIVE,
    eligible=None,
    target_vol: float | None = None,
    vol_window: int = 60,
    start=None,
    end=None,
) -> dict:
    """Walk-forward simulation; returns curve, yearly returns, drawdown, trades.

    ``prices`` is a wide frame (index = sessions, columns = ETF codes) of HFQ
    closes. ``eligible`` optionally maps a session to the codes selectable then,
    so a point-in-time universe can be imposed without touching the logic.
    """
    if not isinstance(prices.index, pd.DatetimeIndex):
        raise TypeError("prices must be indexed by session timestamps")
    close = prices.sort_index()
    rets = close.pct_change().fillna(0.0)
    momentum = close / close.shift(lookback) - 1.0
    trend = close > close.rolling(ma).mean()
    vol = realised_vol(close, vol_window) if target_vol else None

    lower = pd.Timestamp(start) if start is not None else close.index[0]
    upper = pd.Timestamp(end) if end is not None else close.index[-1]

    equity = 1.0
    current: list[str] = []
    previous: list[str] = []
    exposure = 1.0
    pending_cost = 0.0
    sessions: list = []
    curve: list[float] = []
    trades = 0
    holdings_log: dict = {}

    for index, day in enumerate(close.index):
        if not (lower <= day <= upper):
            continue
        picks = None
        if index % cadence == 0:
            row_momentum = momentum.loc[day].dropna()
            row_trend = trend.loc[day].dropna()
            allowed = eligible(day) if callable(eligible) else eligible
            picks = select_targets(
                row_momentum,
                row_trend,
                top_n=top_n,
                defensive=defensive,
                eligible=allowed,
            )
        # The session's return belongs to the book held coming into it. With a
        # volatility target the uninvested share earns the cash leg instead.
        if not current:
            daily = cash_daily
        else:
            book_return = float(rets.loc[day, current].mean())
            daily = exposure * book_return + (1.0 - exposure) * cash_daily
        daily -= pending_cost
        pending_cost = 0.0
        equity *= 1 + daily
        sessions.append(day)
        curve.append(equity)
        holdings_log[day] = list(current)
        if picks is not None:
            if picks:
                pending_cost = _turnover(previous, picks) * cost
                trades += len(set(picks) - set(previous))
                previous = picks
                if target_vol and vol is not None:
                    book_vol = float(vol.loc[day, picks].mean())
                    exposure = (
                        min(1.0, max(0.0, target_vol / book_vol))
                        if book_vol and math.isfinite(book_vol) and book_vol > 0
                        else 1.0
                    )
                else:
                    exposure = 1.0
            current = picks

    series = pd.Series(curve, index=pd.DatetimeIndex(sessions))
    if series.empty:
        raise ValueError("no sessions inside the requested window")
    peak = series.cummax()
    drawdown = series / peak - 1.0
    trough = drawdown.idxmin()
    return {
        "curve": series,
        "yearly_returns": yearly_returns(series),
        "max_drawdown": float(drawdown.min()),
        "drawdown_peak": series.loc[:trough].idxmax(),
        "drawdown_trough": trough,
        "final_equity": float(series.iloc[-1]),
        "total_trades": int(trades),
        "final_holdings": list(current),
        "final_exposure": exposure,
    }


def yearly_returns(curve: pd.Series) -> dict[int, float]:
    """Calendar-year return of an equity curve (first year is partial)."""
    if curve.empty:
        return {}
    per_year = curve.resample("YE").last()
    first = curve.index[0]
    values: dict[int, float] = {}
    previous = 1.0
    for stamp, value in per_year.items():
        if stamp.year == first.year:
            # partial first year: measure from the curve's own start
            values[stamp.year] = float(value / curve.iloc[0] - 1.0)
        else:
            values[stamp.year] = float(value / previous - 1.0)
        previous = value
    return values


def summary(result: dict, *, start=None, end=None) -> dict:
    """Compact metrics for one simulation run."""
    curve = result["curve"]
    rets = curve.pct_change().dropna()
    years = (curve.index[-1] - curve.index[0]).days / 365.25
    annualised = curve.iloc[-1] ** (1 / years) - 1 if years > 0 else float("nan")
    sharpe = (
        float(rets.mean() / rets.std(ddof=1) * math.sqrt(252))
        if len(rets) > 1 and rets.std(ddof=1) > 0
        else None
    )
    yearly = result["yearly_returns"]
    return {
        "annualised": annualised,
        "sharpe": sharpe,
        "max_drawdown": result["max_drawdown"],
        "negative_years": sum(1 for value in yearly.values() if value < 0),
        "years": len(yearly),
        "trades": result["total_trades"],
        "final_equity": result["final_equity"],
    }


def monthly_signal(
    prices: pd.DataFrame,
    *,
    lookback: int = 250,
    top_n: int = 3,
    ma: int = 200,
    defensive: str = DEFAULT_DEFENSIVE,
    as_of=None,
    eligible=None,
) -> dict:
    """The operator artifact: today's target list plus the diagnostics behind it.

    Returns the selected codes, each ranked name's trailing return and trend
    flag, and the names that failed each filter — enough for a human to check
    the rule before acting, which is what "semi-automatic" means here.
    """
    close = prices.sort_index()
    day = pd.Timestamp(as_of) if as_of is not None else close.index[-1]
    if day not in close.index:
        earlier = close.index[close.index <= day]
        if earlier.empty:
            raise ValueError(f"no session at or before {day}")
        day = earlier[-1]
    momentum = (close / close.shift(lookback) - 1.0).loc[day].dropna()
    trend = (close > close.rolling(ma).mean()).loc[day].dropna()
    allowed = eligible(day) if callable(eligible) else eligible
    picks = select_targets(
        momentum, trend, top_n=top_n, defensive=defensive, eligible=allowed
    )
    diagnostics = []
    for code, value in momentum.sort_values(ascending=False).items():
        if allowed is not None and code not in allowed:
            continue
        flag = trend.get(code)
        reason = None
        if code == defensive:
            reason = "defensive"
        elif value <= 0:
            reason = "negative_momentum"
        elif flag is None or flag is pd.NA or not bool(flag):
            reason = "below_trend_ma"
        diagnostics.append(
            {
                "code": code,
                "momentum": round(float(value), 6),
                "above_ma": bool(flag)
                if flag is not None and flag is not pd.NA
                else None,
                "selected": code in picks,
                "reason": reason,
            }
        )
    return {"as_of": day, "selected": picks, "diagnostics": diagnostics}
