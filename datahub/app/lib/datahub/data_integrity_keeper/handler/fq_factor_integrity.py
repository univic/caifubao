"""Read-only market-wide FQ factor jump scan.

Purpose: give the full-market FQ recompute (``fq-adj-factor-fix`` task 5.2,
TASK-404 slice 2 §8 step 3) an objective acceptance instrument. The known
discontinuity was a market-wide single-day jump on 2026-08-31 (30.95 → 6.66,
4,676/5,204 stocks moving >10%), which the two-stock spot probe in
``scripts/check_fq_factor_integrity.py`` cannot detect.

The scan is a pure aggregation over stored ``fq_factor``: no writes, no
freshness/status updates, and no change to how the factor is computed. It
streams the ``stock_daily_quote`` documents for a BOUNDED date window through
a three-field projection (using the unique ``(code, date)`` index order) and
compares each stock against its own previous row, attributing a relative
change to the newer date. Adjacency is taken from the stored A-share trade
calendar (``finance_market.trade_calendar``) when available, so the first
session after a long holiday is still compared with the last pre-holiday
session instead of being dropped as a "gap"; without a calendar the scan
falls back to a calendar-day bound and says so. Genuine ex-dividend dates legitimately move a small
number of stocks, so the report always carries the per-date count AND the
fraction of the scanned stock universe, plus the recorded baseline the
operator compares against — no single hard-coded bound is applied here.

Callers must pass an explicit window (``date_from``/``date_to``); the helper
refuses windows longer than ``max_window_days`` unless ``allow_long_window``
is set, because the pre-recompute legacy rows can be millions of documents.
"""

from __future__ import annotations

import datetime
import logging
import math
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 0.10
DEFAULT_MAX_GAP_DAYS = 5
DEFAULT_TOP_N = 10
DEFAULT_MAX_WINDOW_DAYS = 500
PROJECTION = {"_id": 0, "code": 1, "date": 1, "fq_factor": 1}
# Matches the unique (code, date) index; dates arrive newest-first per code.
SORT_SPEC = [("code", 1), ("date", -1)]
BATCH_SIZE = 5000


def _as_date(value: Any) -> datetime.date | None:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return None


def _load_stock_universe(db) -> set[str]:
    """Return the individual-stock code universe (excludes index rows)."""
    codes = set()
    cursor = db["basic_stock"].find(
        {"object_type": "individual_stock"}, {"_id": 0, "code": 1}
    )
    for doc in cursor:
        code = doc.get("code")
        if code:
            codes.add(code)
    return codes


def _relative_change(new_value: float, old_value: float) -> float:
    """Relative change; callers must exclude zero on either side first."""
    return abs(new_value / old_value - 1.0)


def _load_trading_days(
    db, date_from: datetime.date, date_to: datetime.date
) -> list[datetime.date]:
    """Return the sorted A-share trading days inside the window (read-only)."""
    doc = db["finance_market"].find_one(
        {"name": "ChinaAStock"}, {"_id": 0, "trade_calendar": 1}
    )
    days = set()
    for value in (doc or {}).get("trade_calendar") or []:
        day = _as_date(value)
        if day is not None and date_from <= day <= date_to:
            days.add(day)
    return sorted(days)


def scan_fq_factor_jumps(
    db,
    *,
    date_from: datetime.date,
    date_to: datetime.date,
    threshold: float = DEFAULT_THRESHOLD,
    max_gap_days: int = DEFAULT_MAX_GAP_DAYS,
    top_n: int = DEFAULT_TOP_N,
    target_date: datetime.date | None = None,
    max_window_days: int = DEFAULT_MAX_WINDOW_DAYS,
    allow_long_window: bool = False,
    require_universe: bool = False,
) -> dict:
    """Stream the window and report per-date fq_factor jumps.

    A "jump" is a relative change above ``threshold`` between two rows of the
    same stock that are ADJACENT trading days, or - when no trade calendar is
    available - at most ``max_gap_days`` calendar days apart. The change is
    attributed to the newer date. Non-adjacent pairs (a suspended stock
    resuming, a stock missing a session) would report cumulative moves rather
    than daily jumps and are counted separately as ``skipped_gap``.

    ``require_universe=True`` refuses to run when the individual-stock
    universe cannot be read, because an acceptance scan that counts index rows
    as stocks would overstate the anomaly.
    """
    if not isinstance(date_from, datetime.date) or not isinstance(
        date_to, datetime.date
    ):
        raise ValueError("date_from and date_to are required")
    if date_from > date_to:
        raise ValueError("date_from must not be after date_to")
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("threshold must be a finite number >= 0")
    if max_gap_days < 1:
        raise ValueError("max_gap_days must be >= 1")
    if top_n < 1:
        raise ValueError("top_n must be >= 1")
    window_days = (date_to - date_from).days
    if window_days > max_window_days and not allow_long_window:
        raise ValueError(
            f"window of {window_days} days exceeds the {max_window_days}-day "
            "guard; narrow --from-date/--to-date or pass --allow-long-window"
        )

    universe = _load_stock_universe(db)
    if require_universe and not universe:
        raise ValueError(
            "individual-stock universe is empty (basic_stock with "
            "object_type=individual_stock); refusing an acceptance scan that "
            "cannot exclude index rows"
        )
    trading_days = _load_trading_days(db, date_from, date_to)
    previous_trading_day = {
        day: trading_days[index - 1] for index, day in enumerate(trading_days[1:], 1)
    }
    start = datetime.datetime.combine(date_from, datetime.time.min)
    end = datetime.datetime.combine(date_to, datetime.time.max)

    per_date: dict[str, dict[str, float]] = {}
    counters = {
        "docs_scanned": 0,
        "docs_skipped_non_stock": 0,
        "docs_skipped_missing_factor": 0,
        "docs_skipped_bad_date": 0,
        "duplicate_dates": 0,
        "pairs_compared": 0,
        "skipped_gap": 0,
        "skipped_zero_factor": 0,
        "pairs_via_calendar_fallback": 0,
    }
    skipped_gap_by_date: dict[str, int] = {}
    skipped_zero_by_date: dict[str, int] = {}
    seen_codes: set[str] = set()
    latest_date = None
    earliest_date = None
    max_change = 0.0
    max_change_ref: dict | None = None

    previous: dict[str, tuple[datetime.date, float]] = {}
    cursor = (
        db["stock_daily_quote"]
        .find({"date": {"$gte": start, "$lte": end}}, PROJECTION)
        .sort(SORT_SPEC)
        .batch_size(BATCH_SIZE)
    )
    for doc in cursor:
        counters["docs_scanned"] += 1
        code = doc.get("code")
        day = _as_date(doc.get("date"))
        value = doc.get("fq_factor")
        if day is None:
            counters["docs_skipped_bad_date"] += 1
            continue
        if earliest_date is None or day < earliest_date:
            earliest_date = day
        if latest_date is None or day > latest_date:
            latest_date = day
        if universe and code not in universe:
            counters["docs_skipped_non_stock"] += 1
            continue
        if value is None:
            counters["docs_skipped_missing_factor"] += 1
            continue
        seen_codes.add(code)

        prior = previous.get(code)
        if prior is None:
            previous[code] = (day, float(value))
            continue
        prior_day, prior_value = prior
        if prior_day == day:
            # Defensive: the (code, date) index is unique, but if a duplicate
            # ever appears the outcome must not depend on Mongo's tie order,
            # so the pair is dropped and the next older row re-baselines.
            counters["duplicate_dates"] += 1
            previous.pop(code, None)
            continue
        # Dates descend per code, so `day` is older than `prior_day`; the
        # change is attributed to the newer date (`prior_day`).
        if prior_day < day:
            previous[code] = (day, float(value))
            continue
        previous[code] = (day, float(value))
        expected_previous = previous_trading_day.get(prior_day)
        if expected_previous is not None:
            adjacent = day == expected_previous
        else:
            # The newer date is outside the loaded calendar (first in-window
            # trading day, or a quote on a non-trading date): fall back to the
            # calendar-day bound and say so in the counters.
            counters["pairs_via_calendar_fallback"] += 1
            adjacent = (prior_day - day).days <= max_gap_days
        if not adjacent:
            counters["skipped_gap"] += 1
            skipped_gap_by_date[str(prior_day)] = (
                skipped_gap_by_date.get(str(prior_day), 0) + 1
            )
            continue
        current_value = float(value)
        if not value or not prior_value:
            # A zero factor on either side makes the relative change
            # meaningless; report it instead of inventing a 100% jump.
            counters["skipped_zero_factor"] += 1
            skipped_zero_by_date[str(prior_day)] = (
                skipped_zero_by_date.get(str(prior_day), 0) + 1
            )
            continue
        change = _relative_change(prior_value, current_value)
        counters["pairs_compared"] += 1
        if change > max_change:
            max_change = change
            max_change_ref = {
                "code": code,
                "date": str(prior_day),
                "previous_fq_factor": float(value),
                "fq_factor": prior_value,
                "relative_change": round(change, 6),
            }
        if change <= threshold:
            continue
        bucket = per_date.setdefault(str(prior_day), {"count": 0, "max_change": 0.0})
        bucket["count"] += 1
        if change > bucket["max_change"]:
            bucket["max_change"] = change

    universe_size = len(universe) or None
    ranked = sorted(
        ({"date": day, **stats} for day, stats in per_date.items()),
        key=lambda item: (-item["count"], item["date"]),
    )
    top_dates = [
        {
            "date": item["date"],
            "jump_count": item["count"],
            "max_change": round(item["max_change"], 6),
            "fraction_of_universe": (
                round(item["count"] / universe_size, 6) if universe_size else None
            ),
        }
        for item in ranked[:top_n]
    ]

    target = None
    if target_date is not None:
        stats = per_date.get(str(target_date))
        target = {
            "date": str(target_date),
            "jump_count": stats["count"] if stats else 0,
            "max_change": round(stats["max_change"], 6) if stats else 0.0,
            "fraction_of_universe": (
                round(stats["count"] / universe_size, 6)
                if stats and universe_size
                else None
            ),
        }

    return {
        "window": {"from": str(date_from), "to": str(date_to)},
        "threshold": threshold,
        "max_gap_days": max_gap_days,
        "scanned": {
            "docs": counters["docs_scanned"],
            "codes": len(seen_codes),
            "universe_size": universe_size,
            "universe_known": bool(universe),
            "earliest_date": str(earliest_date) if earliest_date else None,
            "latest_date": str(latest_date) if latest_date else None,
        },
        "counters": counters,
        "basis": {
            "adjacency": "trade_calendar" if previous_trading_day else "calendar_days",
            "trading_days_in_window": len(trading_days),
            "max_gap_days": max_gap_days,
        },
        "skipped_gap_by_date": dict(sorted(skipped_gap_by_date.items())),
        "skipped_zero_factor_by_date": dict(sorted(skipped_zero_by_date.items())),
        "warnings": (
            []
            if universe
            else [
                "individual-stock universe unavailable: index rows are not "
                "filtered, so counts may overstate the anomaly"
            ]
        ),
        "jump_dates": len(per_date),
        "top_dates": top_dates,
        "max_change": (round(max_change, 6) if max_change_ref else 0.0),
        "max_change_ref": max_change_ref,
        "target_date": target,
        "baseline_note": (
            "Compare target_date.jump_count against top_dates and the "
            "fraction_of_universe: the recorded 2026-08-31 discontinuity "
            "moved about 89% of the market in one day, real ex-dividend days "
            "move only tens of stocks."
        ),
    }
