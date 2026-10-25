"""Read-only Tushare corporate-action REPLAY inventory with query receipts.

A fresh query of today's Tushare rows is *not* proof of point-in-time historical
availability. This producer unconditionally labels evidence_kind=REPLAY; its
output MUST NOT be submitted to a FORWARD NAV publisher without separately
verified, time-stamped vendor-change history and approval.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import math

from app.lib.datahub.data_source.interface.tushare_interface import (
    to_tushare_ts_code,
)
from app.lib.strategy_engine.corporate_action_source import (
    SUPPORTED_VERSION,
    fingerprint_events,
)

DIVIDEND_FIELDS = ",".join(
    (
        "ts_code",
        "ann_date",
        "div_proc",
        "stk_div",
        "stk_bo_rate",
        "stk_co_rate",
        "cash_div",
        "cash_div_tax",
        "record_date",
        "ex_date",
        "pay_date",
    )
)


def _iso(value, name):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, str) and len(value) == 8 and value.isdigit():
        value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    if not isinstance(value, str):
        raise ValueError(f"{name} missing or not a date")
    try:
        parsed = datetime.date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{name} is invalid") from None
    if parsed.isoformat() != value:
        raise ValueError(f"{name} must be canonical")
    return value


def _nonnegative(row, key):
    raw = row.get(key)
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"{key} missing or boolean: cannot substitute a numeric zero")
    try:
        value = float(raw)
    except (ValueError, TypeError):
        raise ValueError(f"{key} invalid") from None
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{key} must be finite and nonnegative")
    return value


def _event(row, code, ts_code, start, end, as_of):
    if row.get("div_proc") != "实施":
        return None
    if row.get("ts_code") != ts_code:
        raise ValueError("source returned an unrequested security")
    ex = _iso(row.get("ex_date"), "ex_date")
    if not (start <= ex <= end):
        return None
    ann = _iso(row.get("ann_date"), "ann_date")
    record = _iso(row.get("record_date"), "record_date")
    pay = _iso(row.get("pay_date"), "pay_date")
    if ann > as_of:
        raise ValueError("retrospective announcement after requested as_of")
    if not (record < ex <= pay):
        raise ValueError("invalid record/ex/pay causal order")
    cash = _nonnegative(row, "cash_div")
    pretax = _nonnegative(row, "cash_div_tax")
    share = _nonnegative(row, "stk_div")
    bonus = _nonnegative(row, "stk_bo_rate")
    capital = _nonnegative(row, "stk_co_rate")
    if abs(share - bonus - capital) > 1e-8:
        raise ValueError("bonus and capitalization ratios conflict with stk_div")
    if cash > pretax + 1e-8:
        raise ValueError("after-tax distribution exceeds pre-tax distribution")
    if share == 0 and cash == 0 and pretax == 0:
        raise ValueError("implemented distribution has no valid economic effect")
    return {
        "code": code,
        "ann_date": ann,
        "record_date": record,
        "ex_date": ex,
        "pay_date": pay,
        "div_proc": "实施",
        "cash_per_share": cash,
        "share_multiplier": 1.0 + share,
    }


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def capture_replay_inventory(
    *, pro, codes, from_date, to_date, as_of, source="tushare.dividend"
):
    """Query each symbol exactly once and package auditable, non-forward input.

    Missing response (None), exceptions, malformed implemented events, duplicate
    ex-dates and incomplete receipt coverage abort the complete publication.
    An explicitly successful empty response means no *currently reported*
    implemented actions, not proof no point-in-time action existed.
    """
    start = _iso(from_date, "from_date")
    end = _iso(to_date, "to_date")
    snapshot = _iso(as_of, "as_of")
    if not start <= end <= snapshot:
        raise ValueError("require from_date <= to_date <= as_of")
    if not isinstance(codes, list) or not codes or len(set(codes)) != len(codes):
        raise ValueError("nonempty unique code list required")
    if any(not isinstance(x, str) or not x for x in codes):
        raise ValueError("security codes must be nonempty strings")
    if not isinstance(source, str) or not source.strip():
        raise ValueError("provider source required")

    rows = []
    receipts = []
    failures = []
    for code in sorted(codes):
        ts_code = to_tushare_ts_code(code)
        try:
            frame = pro.dividend(ts_code=ts_code, fields=DIVIDEND_FIELDS)
            if frame is None:
                raise ValueError("unavailable response; not confirmed empty")
            batch = frame.to_dict("records")
            if not isinstance(batch, list):
                raise ValueError("malformed provider response")
            events = []
            for original in batch:
                item = _event(original, code, ts_code, start, end, snapshot)
                if item is not None:
                    events.append(item)
            if len({e["ex_date"] for e in events}) != len(events):
                raise ValueError(
                    "duplicate ex-date action requires manual reconciliation"
                )
            rows.extend(events)
            receipts.append(
                {
                    "code": code,
                    "provider_code": ts_code,
                    "queried_at": datetime.datetime.now(datetime.UTC).isoformat(),
                    "status": "SUCCESS",
                    "raw_rows": len(batch),
                    "included_events": len(events),
                }
            )
        except Exception as exc:  # noqa: BLE001 - retain failure and abort below
            failures.append({"code": code, "error": type(exc).__name__})
    if failures:
        raise ValueError(
            f"corporate action publication refused: {len(failures)} failed queries; "
            "no completeness assertion may be published"
        )

    rows.sort(key=lambda e: (e["code"], e["ex_date"]))
    ordered = sorted(codes)
    manifest = {
        "schema_version": SUPPORTED_VERSION,
        "evidence_kind": "REPLAY",
        "source": source,
        "from_date": start,
        "to_date": end,
        "as_of": snapshot,
        "covered_codes": ordered,
        "failed_codes": [],
        "events_sha256": fingerprint_events(rows),
        "receipts_sha256": hashlib.sha256(_json(receipts).encode("utf-8")).hexdigest(),
        "query_receipts": receipts,
        "events_count": len(rows),
        "coverage_claim_status": "UNVERIFIED_UPSTREAM",
        "warning": (
            "Retrospective vendor inventory: no historical announcement revision "
            "proof; NEVER use as FORWARD research NAV evidence."
        ),
    }
    return {"events": rows, "manifest": manifest}
