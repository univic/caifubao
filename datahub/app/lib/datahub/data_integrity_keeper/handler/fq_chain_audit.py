"""Read-only audit of the ex-rights / after-adjustment (FQ/HFQ) chain.

Purpose: answer "is the stored adjusted-price chain reliable, and does it still
match its source of truth?" without writing anything. The audit has three
layers:

1. **Coverage** - every active stock quoted on the latest session must carry an
   ``fq_factor``/``close_hfq``; index rows and inactive/delisted funds are
   reported separately instead of being counted as stocks.
2. **Arithmetic** - on stored rows, ``close_hfq == close * fq_factor`` and the
   OHLC fields scale by the same ratio; every factor is finite and positive.
3. **Source of truth** - for a bounded sample, fetch tushare ``adj_factor`` and
   compare it with the stored factor. The acceptance tolerance matches the
   writer-switch verifier (``5e-4`` absolute), because tushare re-publishes the
   same factor with slightly different rounding between snapshots (observed up
   to ``4e-4``); anything larger is a real divergence.

Pure helpers are separated from the Mongo/tushare I/O so they can be tested
offline.
"""

from __future__ import annotations

import datetime
import math

QUOTE_COLLECTION = "stock_daily_quote"
STOCK_COLLECTION = "basic_stock"
STATUS_COLLECTION = "data_asset_status"

DEFAULT_TOLERANCE = 5e-4
#: A market-wide factor jump is an acceptance failure when it moves at least
#: this share of the scanned universe on one date (real ex-dividend days move
#: well under 1%; the 2026-08-31 discontinuity moved ~89%).
MARKET_WIDE_JUMP_FRACTION = 0.05
#: Below this share of the active supported universe having a latest-session
#: quote, the audit cannot claim the chain is covered (suspensions explain a
#: few missing names, not a tenth of the market).
MIN_ACTIVE_COVERAGE = 0.90
#: Floating-point slack so a difference exactly at the tolerance (0.0005 from
#: tushare re-publishing 4-decimal and 3-decimal factors) is not counted as a
#: divergence by a 1e-16 representation error.
TOLERANCE_EPSILON = 1e-9
ARITHMETIC_TOLERANCE = 0.02
DEFAULT_SAMPLE_SIZE = 5


def _as_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value)[:10])


def _finite(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def compare_factor_series(
    stored_rows,
    source_factors,
    *,
    tolerance: float = DEFAULT_TOLERANCE,
) -> dict:
    """Compare stored ``fq_factor`` values against the provider series.

    ``stored_rows`` are dicts with ``date`` and ``fq_factor``; ``source_factors``
    maps date -> factor. Dates absent from the source are reported as
    ``missing_in_source`` (a gap the bfill/ffill alignment would have hidden),
    not silently skipped.
    """
    compared = 0
    mismatches = []
    missing = []
    max_diff = 0.0
    first_stored = None
    normalized_source = {
        _as_date(key): float(value)
        for key, value in (source_factors or {}).items()
        if _as_date(key) is not None and _finite(value)
    }
    first_source = min(normalized_source) if normalized_source else None
    for row in stored_rows:
        day = _as_date(row.get("date"))
        factor = row.get("fq_factor")
        if day is None or not _finite(factor):
            continue
        if first_stored is None or day < first_stored:
            first_stored = day
        if day not in normalized_source:
            missing.append(str(day))
            continue
        source = normalized_source[day]
        diff = abs(float(factor) - source)
        compared += 1
        max_diff = max(max_diff, diff)
        if diff > tolerance + TOLERANCE_EPSILON:
            mismatches.append(
                {
                    "date": str(day),
                    "stored": float(factor),
                    "source": source,
                    "abs_diff": round(diff, 8),
                }
            )
    return {
        "compared": compared,
        "missing_in_source": len(missing),
        "missing_examples": missing[:5],
        "mismatch_count": len(mismatches),
        "mismatch_examples": mismatches[:5],
        "max_abs_diff": round(max_diff, 8),
        "leading_before_source": bool(
            first_stored and first_source and first_stored < first_source
        ),
        "first_stored": str(first_stored) if first_stored else None,
        "first_source": str(first_source) if first_source else None,
    }


def check_hfq_arithmetic(
    stored_rows, *, tolerance: float = ARITHMETIC_TOLERANCE
) -> dict:
    """Verify ``*_hfq == raw * fq_factor`` and positive finite factors."""
    checked = 0
    bad = []
    non_positive = 0
    for row in stored_rows:
        factor = row.get("fq_factor")
        if not _finite(factor) or float(factor) <= 0:
            non_positive += 1
            continue
        factor = float(factor)
        for raw_field in ("open", "close", "high", "low"):
            raw = row.get(raw_field)
            adjusted = row.get(f"{raw_field}_hfq")
            if raw is None:
                continue
            checked += 1
            expected = round(float(raw) * factor, 4)
            if not _finite(adjusted) or abs(float(adjusted) - expected) > tolerance:
                bad.append(
                    {
                        "date": str(_as_date(row.get("date"))),
                        "field": raw_field,
                        "raw": float(raw),
                        "factor": factor,
                        "stored_hfq": adjusted,
                        "expected_hfq": expected,
                    }
                )
    return {
        "checked": checked,
        "bad_count": len(bad),
        "bad_examples": bad[:5],
        "non_positive_factor_count": non_positive,
    }


def coverage_summary(
    *,
    active_stock_count: int,
    quoted_active_count: int,
    missing_fq_count: int,
    latest_quote_date,
    inactive_quoted_count: int = 0,
    inactive_missing_fq_count: int = 0,
) -> dict:
    return {
        "active_stock_count": active_stock_count,
        "quoted_active_count": quoted_active_count,
        "quoted_active_coverage": (
            round(quoted_active_count / active_stock_count, 4)
            if active_stock_count
            else None
        ),
        "missing_fq_count": missing_fq_count,
        "latest_quote_date": str(latest_quote_date) if latest_quote_date else None,
        "inactive_quoted_count": inactive_quoted_count,
        "inactive_missing_fq_count": inactive_missing_fq_count,
    }


def load_stored_rows(db, code: str, *, from_date=None, to_date=None):
    query = {"code": code, "fq_factor": {"$exists": True, "$ne": None}}
    if from_date or to_date:
        window = {}
        if from_date:
            window["$gte"] = datetime.datetime.combine(from_date, datetime.time.min)
        if to_date:
            window["$lte"] = datetime.datetime.combine(to_date, datetime.time.max)
        query["date"] = window
    return list(
        db[QUOTE_COLLECTION]
        .find(
            query,
            {
                "_id": 0,
                "date": 1,
                "open": 1,
                "close": 1,
                "high": 1,
                "low": 1,
                "open_hfq": 1,
                "close_hfq": 1,
                "high_hfq": 1,
                "low_hfq": 1,
                "fq_factor": 1,
            },
        )
        .sort("date", 1)
    )


def audit_code(db, code: str, source_factors, *, tolerance=DEFAULT_TOLERANCE) -> dict:
    rows = load_stored_rows(db, code)
    if not rows:
        return {"code": code, "status": "NO_DATA", "rows": 0}
    comparison = compare_factor_series(rows, source_factors, tolerance=tolerance)
    arithmetic = check_hfq_arithmetic(rows)
    status = "PASS"
    if comparison["mismatch_count"] or arithmetic["bad_count"]:
        status = "FAIL"
    elif (
        comparison["missing_in_source"]
        or comparison["leading_before_source"]
        or arithmetic["non_positive_factor_count"]
    ):
        status = "WARN"
    return {
        "code": code,
        "status": status,
        "rows": len(rows),
        "source": comparison,
        "arithmetic": arithmetic,
    }


def latest_session(db, codes=None):
    """Most recent quote date, optionally restricted to a code universe."""
    query = {}
    if codes:
        query["code"] = {"$in": sorted(codes)}
    latest = db[QUOTE_COLLECTION].find_one(query, {"date": 1}, sort=[("date", -1)])
    return _as_date((latest or {}).get("date"))


def load_coverage(db) -> dict:
    # BSE stocks are excluded from the supported universe by design
    # (data_capabilities.daily_quote=false); counting them would always show a
    # 6% coverage hole that is not a chain defect.
    active_codes = set(
        db[STOCK_COLLECTION].distinct(
            "code",
            {
                "object_type": "individual_stock",
                "active_status": 0,
                "data_capabilities.daily_quote": {"$ne": False},
            },
        )
    )
    inactive_codes = set(
        db[STOCK_COLLECTION].distinct(
            "code",
            {"object_type": "individual_stock", "active_status": {"$ne": 0}},
        )
    )
    day = latest_session(db, active_codes)
    if day is None:
        return coverage_summary(
            active_stock_count=len(active_codes),
            quoted_active_count=0,
            missing_fq_count=0,
            latest_quote_date=None,
        )
    day_dt = datetime.datetime.combine(day, datetime.time.min)
    quoted_codes = set(db[QUOTE_COLLECTION].distinct("code", {"date": day_dt}))
    quoted_active = quoted_codes & active_codes
    quoted_inactive = quoted_codes & inactive_codes
    missing_active = (
        db[QUOTE_COLLECTION].count_documents(
            {
                "code": {"$in": sorted(quoted_active)},
                "date": day_dt,
                "fq_factor": None,
            }
        )
        if quoted_active
        else 0
    )
    missing_inactive = (
        db[QUOTE_COLLECTION].count_documents(
            {
                "code": {"$in": sorted(quoted_inactive)},
                "date": day_dt,
                "fq_factor": None,
            }
        )
        if quoted_inactive
        else 0
    )
    return coverage_summary(
        active_stock_count=len(active_codes),
        quoted_active_count=len(quoted_active),
        missing_fq_count=missing_active,
        latest_quote_date=day,
        inactive_quoted_count=len(quoted_inactive),
        inactive_missing_fq_count=missing_inactive,
    )


def load_status_summary(db) -> dict:
    rows = db[STATUS_COLLECTION].aggregate(
        [
            {"$match": {"asset_name": "FQ_FACTOR"}},
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        ]
    )
    return {row["_id"]: row["count"] for row in rows}


def run_audit(
    db,
    *,
    sample_codes,
    fetch_source,
    tolerance: float = DEFAULT_TOLERANCE,
    jump_scan=None,
) -> dict:
    """Assemble the full report; ``fetch_source(code)`` returns date->factor."""
    coverage = load_coverage(db)
    codes = []
    source_results = []
    if fetch_source is not None:
        for code in sample_codes:
            try:
                source = fetch_source(code)
            except Exception as exc:  # noqa: BLE001 - reported, never hidden
                source_results.append(
                    {"code": code, "status": "ERROR", "message": str(exc)}
                )
                continue
            result = audit_code(db, code, source or {}, tolerance=tolerance)
            source_results.append(result)
            codes.append(code)

    checks_status = [result.get("status") for result in source_results]
    compared = [
        result
        for result in source_results
        if (result.get("source") or {}).get("compared")
    ]
    target = (jump_scan or {}).get("target_date") or {}
    market_wide_jump = float(target.get("fraction_of_universe") or 0.0) >= (
        MARKET_WIDE_JUMP_FRACTION
    )
    coverage_ratio = coverage.get("quoted_active_coverage") or 0.0
    if coverage["quoted_active_count"] <= 0 or coverage_ratio < MIN_ACTIVE_COVERAGE:
        verdict = "FAIL"
    elif coverage["missing_fq_count"] > 0:
        verdict = "FAIL"
    elif "FAIL" in checks_status:
        verdict = "FAIL"
    elif market_wide_jump:
        verdict = "FAIL"
    elif fetch_source is not None and not compared:
        # Every sample errored or had no data: the audit cannot claim the
        # stored chain matches its source, so it must not pass.
        verdict = "FAIL"
    elif (
        "WARN" in checks_status
        or "ERROR" in checks_status
        or "NO_DATA" in checks_status
        or coverage["inactive_missing_fq_count"]
    ):
        verdict = "WARN"
    else:
        verdict = "PASS"

    report = {
        "verdict": verdict,
        "market_wide_jump": market_wide_jump,
        "min_active_coverage": MIN_ACTIVE_COVERAGE,
        "tolerance": tolerance,
        "coverage": coverage,
        "fq_asset_status": load_status_summary(db),
        "sample": source_results,
    }
    if jump_scan is not None:
        report["jump_scan"] = jump_scan
    return report
