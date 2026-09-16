# -*- coding: utf-8 -*-
"""Holding-period x buffer scan for the paper strategy (research-only).

Answers one question for the 5万-style account: **at which holding period and
with how much rotation hysteresis does a signal still pay after A-share
friction, and what annual turnover does that cost?**

Relationship to the existing layers
-----------------------------------
* The **factor lab** (`app.lib.factor_lab`) already owns the label semantics:
  ``fwd_h{h}`` is the gross return from the T+1 open to the open ``h`` sessions
  later, and a name that could not be entered/exited is dropped with a reason.
  This module *consumes* those labels — it does not rebuild them, and it must
  not be used with the paper path's roll-forward labels (the two conventions
  answer different questions; see ``panel.py``'s module docstring).
* The **paper NAV engine** (`strategy_engine.nav`) simulates fills, cash and
  board lots for the *live* 5万 account. This scan deliberately does **not**
  re-implement that: it is a frequency experiment whose unit of account is one
  equal-weight basket on its rebalance session, so it can compare 5/10/20/40/60
  sessions on the same frozen panel without a second execution model.

Statistics
----------
Per rebalance date ``t`` the basket return is the equal-weight mean of the
names' net labels (gross − one round trip) and the benchmark is the same-date
equal-weight mean over **every name with a resolvable label**, also net. Excess
is their difference. Because rebalance dates are exactly ``h`` sessions apart,
consecutive baskets do not overlap, so the plain ``mean/std * sqrt(periods per
year)`` information ratio is the right one here (unlike h-day ICs, which need
the Newey-West correction the factor lab applies).

Buffer semantics (hysteresis)
-----------------------------
* entry: rank percentile ``<= entry_pct`` (rank 0 = lowest signal value, which
  is the flip/reversal side);
* exit: a held name is only sold once its percentile rises above
  ``entry_pct * buffer`` — so ``buffer == 1.0`` is a full re-sort every
  rebalance, ``1.5``/``2.0`` let winners sit until they leave the wider band;
* ``horizon`` defines the rebalance cadence.  It does not force liquidation:
  an existing name that remains inside the exit band carries into the next
  tranche without paying another round trip.

Turnover
--------
``turnover`` for one rebalance is ``names replaced / basket size``; the reported
``annual_turnover`` is ``mean(turnover) * 252 / h`` — the same definition the
factor lab's ``turnover_report`` uses, so the two are comparable.
"""

from __future__ import annotations

import math

import pandas as pd

#: Mirrors `metrics.round_trip_cost()`; duplicated as a plain constant so this
#: module has no import edge into the factor lab.
ROUND_TRIP_COST = 2 * 0.001 + 2 * 0.00025 + 0.001

#: Turnover-penalty weights, aligned with `autoresearch/profile.yaml`.
TURNOVER_FREE_ALLOWANCE = 6.0
TURNOVER_PENALTY_WEIGHT = 0.02

SCAN_COLUMNS = ("date", "stock_code", "label", "rank")


def build_scan_input(
    panel: pd.DataFrame,
    signal: pd.Series,
    horizon: int,
    *,
    stock_column: str = "stock_code",
) -> pd.DataFrame:
    """Align one horizon's label with a cross-sectional signal rank.

    Returns a frame with ``date``, ``stock_code``, ``label`` (gross, NaN when
    the panel blocked the label) and ``rank`` (per-session percentile of
    ``signal``, 0 = lowest). Rows the panel dropped keep a NaN label and are
    therefore excluded from both the basket and the benchmark — a blocked entry
    is not silently rolled forward.
    """
    label_column = f"fwd_h{horizon}"
    if label_column not in panel.columns:
        raise KeyError(f"panel is missing {label_column!r}")
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(panel["date"]).dt.normalize(),
            "stock_code": panel[stock_column].astype(str),
            "label": pd.to_numeric(panel[label_column], errors="coerce"),
            "signal": pd.to_numeric(
                pd.Series(signal).reindex(panel.index).to_numpy(), errors="coerce"
            ),
        }
    )
    eligible = frame["label"].notna() & frame["signal"].notna()
    frame["rank"] = math.nan
    frame.loc[eligible, "rank"] = (
        frame.loc[eligible]
        .groupby("date", sort=False)["signal"]
        .rank(method="first", pct=True)
    )
    # pct=True gives (i+1)/n for the i-th of n names, so the floor band is
    # (0, entry_pct]; shift to the 0..1 convention the caller expects (0 = the
    # lowest signal value) without inventing a percentile for a 1-name session.
    counts = frame.loc[eligible].groupby("date")["signal"].transform("count")
    frame.loc[eligible, "rank"] = (frame.loc[eligible, "rank"] * counts - 1.0) / (
        counts - 1.0
    ).where(counts > 1, 1.0)
    return frame.loc[:, ["date", "stock_code", "label", "rank"]]


def _basket_turnover(previous: set[str], current: set[str]) -> float:
    if not current:
        return 0.0
    return 1.0 - len(previous & current) / len(current)


def scan_cell(
    frame: pd.DataFrame,
    *,
    horizon: int,
    buffer: float,
    entry_pct: float = 0.20,
    portfolio_size: int = 800,
    friction: float = ROUND_TRIP_COST,
    start=None,
    end=None,
) -> dict:
    """Scan one (holding period, buffer) cell of the rotation rule."""
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    if buffer < 1.0:
        raise ValueError(
            "buffer must be >= 1.0 so the entry band is inside the exit band"
        )
    if not 0.0 < entry_pct <= 1.0:
        raise ValueError("entry_pct must be in (0, 1]")
    if portfolio_size < 1:
        raise ValueError("portfolio_size must be >= 1")

    working = frame.copy()
    working["date"] = pd.to_datetime(working["date"])
    lower = pd.Timestamp(start) if start is not None else working["date"].min()
    upper = pd.Timestamp(end) if end is not None else working["date"].max()
    window = working.loc[working["date"].between(lower, upper)]
    sessions = sorted(window["date"].dropna().unique())
    if not sessions:
        return _empty_cell(horizon, buffer, entry_pct, portfolio_size)

    exit_pct = min(1.0, entry_pct * buffer)
    usable = window.loc[window["label"].notna() & window["rank"].notna()]
    if usable.empty:
        return _empty_cell(horizon, buffer, entry_pct, portfolio_size)

    by_date = {
        day: (
            dict(zip(group["stock_code"], group["rank"])),
            dict(zip(group["stock_code"], group["label"])),
        )
        for day, group in usable.groupby("date", sort=True)
    }
    book: set[str] = set()
    previous_target: set[str] = set()
    previous_benchmark: set[str] = set()
    tranche_excess: list[float] = []
    turnovers: list[float] = []
    basket_returns: list[float] = []
    entries = 0

    for index, day in enumerate(sessions):
        if index % horizon:
            continue
        if day not in by_date:
            # A fully blocked date still consumes its scheduled cadence slot;
            # execution is never shifted to the next measurable date.
            book = set()
            previous_target = set()
            previous_benchmark = set()
            continue
        ranks, labels = by_date[day]
        # Keep only measurable holdings that remain inside the wider exit band.
        # A blocked/unresolved label is excluded on its original date, never
        # rolled to a later session.
        book = {
            code
            for code in book
            if code in labels and ranks.get(code, math.inf) <= exit_pct
        }
        # Fill vacancies from the entry band, best (lowest) rank first.
        vacancies = portfolio_size - len(book)
        if vacancies > 0:
            candidates = [
                (rank, code)
                for code, rank in ranks.items()
                if rank <= entry_pct
                and code not in book
                and math.isfinite(labels[code])
            ]
            candidates.sort()
            for _, code in candidates[:vacancies]:
                book.add(code)
                if code not in previous_target:
                    entries += 1
        target = set(book)
        if not target:
            previous_target = target
            continue
        # Turnover is replacement of the target book, not the mechanical churn of
        # re-buying a name whose holding period elapsed: a strategy that keeps
        # selecting the same name does not pay a round trip for it again.
        turnover = _basket_turnover(previous_target, target)
        turnovers.append(turnover)
        previous_target = target
        basket = (
            sum(labels[code] for code in target) / len(target) - friction * turnover
        )
        basket_returns.append(basket)
        benchmark_target = set(labels)
        benchmark_turnover = _basket_turnover(previous_benchmark, benchmark_target)
        previous_benchmark = benchmark_target
        benchmark_return = (
            sum(labels.values()) / len(labels) - friction * benchmark_turnover
        )
        tranche_excess.append(basket - benchmark_return)

    clean = [value for value in tranche_excess if math.isfinite(value)]
    periods_per_year = 252.0 / horizon
    if len(clean) < 2:
        # One rebalance cannot produce a standard deviation: report the cell as
        # measured-but-unscorable instead of zeroing the counts.
        cell = _empty_cell(horizon, buffer, entry_pct, portfolio_size)
        cell.update(
            {
                "rebalances": len(clean),
                "entries": entries,
                "basket_returns": basket_returns,
                "excess_returns": clean,
            }
        )
        return cell

    series = pd.Series(clean, dtype="float64")
    mean = float(series.mean())
    std = float(series.std(ddof=1))
    ir = float(mean / std * math.sqrt(periods_per_year)) if std > 0 else float("nan")
    wealth = (1.0 + series).cumprod()
    drawdown = float((wealth / wealth.cummax() - 1.0).min())
    avg_turnover = sum(turnovers) / len(turnovers) if turnovers else 0.0
    annual_turnover = avg_turnover * periods_per_year
    positive = series[series > 0]
    concentration = (
        float(positive.max() / positive.sum())
        if len(positive) and positive.sum() > 0
        else 0.0
    )
    return {
        "horizon": horizon,
        "buffer": buffer,
        "entry_pct": entry_pct,
        "portfolio_size": portfolio_size,
        "information_ratio": round(ir, 4) if math.isfinite(ir) else None,
        "annualized_net_excess_return": round(
            float((1.0 + mean) ** periods_per_year - 1.0), 6
        ),
        "excess_max_drawdown": round(drawdown, 6),
        "avg_one_way_turnover": round(avg_turnover, 4),
        "annual_turnover": round(annual_turnover, 2),
        "rebalances": len(clean),
        "entries": entries,
        "profit_concentration": round(concentration, 6),
        "objective": _objective(
            ir, mean, periods_per_year, drawdown, annual_turnover, concentration
        ),
        "basket_returns": basket_returns,
        "excess_returns": clean,
    }


def _objective(
    ir: float,
    mean: float,
    periods_per_year: float,
    drawdown: float,
    annual_turnover: float,
    concentration: float,
) -> float | None:
    """Turnover-penalised objective (profile weights, informational only)."""
    if not math.isfinite(ir):
        return None
    annual_excess = (1.0 + mean) ** periods_per_year - 1.0
    score = ir + 0.1 * annual_excess
    score -= 2.0 * max(0.0, abs(drawdown) - 0.1)
    score -= TURNOVER_PENALTY_WEIGHT * max(
        0.0, annual_turnover - TURNOVER_FREE_ALLOWANCE
    )
    score -= 1.0 * max(0.0, concentration - 0.25)
    return round(float(score), 4)


def _empty_cell(
    horizon: int, buffer: float, entry_pct: float, portfolio_size: int
) -> dict:
    return {
        "horizon": horizon,
        "buffer": buffer,
        "entry_pct": entry_pct,
        "portfolio_size": portfolio_size,
        "information_ratio": None,
        "annualized_net_excess_return": None,
        "excess_max_drawdown": None,
        "avg_one_way_turnover": None,
        "annual_turnover": None,
        "rebalances": 0,
        "entries": 0,
        "profit_concentration": None,
        "objective": None,
        "basket_returns": [],
        "excess_returns": [],
    }


def scan_grid(
    frame_by_horizon: dict[int, pd.DataFrame],
    *,
    horizons,
    buffers,
    entry_pct: float = 0.20,
    portfolio_size: int = 800,
) -> list[dict]:
    """Run every (horizon, buffer) cell and return the report rows.

    ``frame_by_horizon`` maps each horizon to the output of
    :func:`build_scan_input`; a horizon without a frame is skipped, which is how
    a panel exported for a subset of horizons degrades gracefully.
    """
    rows = []
    for horizon in sorted({int(h) for h in horizons}):
        frame = frame_by_horizon.get(horizon)
        if frame is None:
            continue
        for buffer in sorted({float(b) for b in buffers}):
            rows.append(
                scan_cell(
                    frame,
                    horizon=horizon,
                    buffer=buffer,
                    entry_pct=entry_pct,
                    portfolio_size=portfolio_size,
                )
            )
    return rows


#: Keys kept when the scan is serialised to a table (the per-rebalance series
#: are returned separately so a CSV stays readable).
SUMMARY_KEYS = (
    "horizon",
    "buffer",
    "information_ratio",
    "annualized_net_excess_return",
    "excess_max_drawdown",
    "avg_one_way_turnover",
    "annual_turnover",
    "rebalances",
    "entries",
    "profit_concentration",
    "objective",
)


def summary_table(rows: list[dict]) -> pd.DataFrame:
    """One row per cell, ordered for reading (horizon then buffer)."""
    table = pd.DataFrame([{key: row.get(key) for key in SUMMARY_KEYS} for row in rows])
    if table.empty:
        return table
    return table.sort_values(["horizon", "buffer"], ignore_index=True)
