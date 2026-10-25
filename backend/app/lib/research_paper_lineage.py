"""Pure, fail-closed Research simulation artifact-to-decision attribution.

Content-addressed Research NAV simulations are *derivations*, not broker
orders, nor proof of FORWARD certification. Never pick a 'latest' derivation
when overlapping candidates disagree; do not attribute non-dated corporate
events to a decision without a trustworthy event session.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import re

HEX64 = re.compile(r"^[0-9a-f]{64}$")
MAX_EVENTS = 100
ORDER_KINDS = {"FILL", "REJECTED"}
SESSION_KINDS = {"MARK", "DIVIDEND_ENTITLEMENT", "DIVIDEND_SETTLEMENT"}


def _date(value):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, str) and len(value) >= 10:
        try:
            parsed = datetime.date.fromisoformat(value[:10])
        except ValueError:
            return None
        return parsed.isoformat() if value[:10] == parsed.isoformat() else None
    return None


def _digest(value: dict) -> str:
    blob = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _event_date(item: dict):
    kind = item.get("type")
    if kind in ORDER_KINDS:
        return _date(item.get("execution_date"))
    if kind == "MARK":
        return _date(item.get("as_of"))
    if kind == "DIVIDEND_ENTITLEMENT":
        return _date(item.get("ex_date"))
    if kind == "DIVIDEND_SETTLEMENT":
        return _date(item.get("pay_date"))
    # Legacy SPLIT events lack an effective timestamp, so attribution
    # is impossible. It may be in the artifact but not this day's trace.
    return None


def _snapshot_event(item: dict) -> dict:
    allowed = (
        "type",
        "event_id",
        "order_id",
        "fill_id",
        "intent_id",
        "stock_code",
        "side",
        "quantity",
        "price",
        "reference_price",
        "fees",
        "reason_code",
        "execution_date",
        "decision_date",
        "as_of",
        "ex_date",
        "pay_date",
        "record_date",
        "cash_per_share",
        "share_multiplier",
        "entitlement_id",
    )
    return {
        key: (
            _date(item[key]) if key.endswith("_date") or key == "as_of" else item[key]
        )
        for key in allowed
        if key in item
    }


def inspect_research_trace(run: dict, candidates: list[dict]) -> dict:
    """Attribute a single unambiguous immutable artifact to a pinned plan.

    `candidates` must already be DB-scoped by strategy_name, config/hash
    and execution interval. Here we verify every candidate rather than
    assuming a Mongo index/date filter proves content membership.
    """
    result = {
        "status": "NOT_LINKED",
        "research_only": True,
        "artifact_hash": None,
        "fills": [],
        "rejections": [],
        "session_events": [],
        "undated_corporate_events": 0,
        "truncated": False,
        "explanation": "No unambiguous, verified Research derivation is linked.",
    }
    if run.get("status") != "COMPLETED":
        return result
    signal = _date(run.get("date"))
    execution = _date(run.get("execution_date"))
    if not signal or not execution:
        return result
    if len(candidates) > 1:
        result["status"] = "AMBIGUOUS_ARTIFACT"
        result["explanation"] = (
            "Multiple overlapping derivations; no latest-wins selection."
        )
        return result
    if not candidates:
        return result

    artifact = candidates[0]
    payload = artifact.get("payload")
    digest = artifact.get("artifact_hash")
    if (
        not isinstance(payload, dict)
        or not isinstance(digest, str)
        or not HEX64.fullmatch(digest)
        or payload.get("schema_version") != "research-simulation-v1"
        or payload.get("config_hash") != run.get("config_hash")
        or payload.get("hash_kind") != (run.get("hash_kind") or "legacy")
        or artifact.get("strategy_name") != run.get("strategy_name")
        or artifact.get("config_hash") != run.get("config_hash")
        or artifact.get("hash_kind") != (run.get("hash_kind") or "legacy")
    ):
        result["status"] = "INVALID_PROVENANCE"
        return result

    try:
        if _digest(payload) != digest:
            raise ValueError("artifact content hash mismatch")
        dates = payload.get("run_dates")
        schedules = payload.get("schedule")
        events = payload.get("events")
        if not isinstance(dates, list) or signal not in dates:
            raise ValueError("plan not a member of artifact")
        if not isinstance(schedules, list) or not any(
            isinstance(x, dict)
            and _date(x.get("date")) == execution
            and _date(x.get("decision_date")) == signal
            for x in schedules
        ):
            raise ValueError("decision/execution pairing not evidenced")
        if not isinstance(events, list):
            raise ValueError("artifact missing events")
        ids = set()
        for event in events:
            if not isinstance(event, dict):
                raise ValueError("invalid ledger event")
            identity = event.get("event_id")
            if not isinstance(identity, str) or not identity or identity in ids:
                raise ValueError("duplicate or missing event identity")
            ids.add(identity)
            if event.get("type") in ORDER_KINDS:
                if (
                    event.get("source_domain") != "research"
                    or event.get("account_id") != "research-simulation"
                ):
                    raise ValueError("foreign execution event")
                if run.get("hash_kind") == "spec" and event.get("spec_hash") != run.get(
                    "config_hash"
                ):
                    raise ValueError("wrong strategy spec fill provenance")
    except (TypeError, ValueError, OverflowError):
        result["status"] = "INVALID_PROVENANCE"
        return result

    result["status"] = "LINKED_SIMULATION"
    result["artifact_hash"] = digest
    result["explanation"] = (
        "Read-only Research simulation derivation; fills are simulated, "
        "not real or Trading Paper executions."
    )
    # Order decisions may only be linked when they bear *both* the execution
    # session and signal decision date. Older untagged fills remain unlinked.
    fills = []
    rejections = []
    observations = []
    for event in events:
        kind = event.get("type")
        if kind == "SPLIT" and not _event_date(event):
            result["undated_corporate_events"] += 1
            continue
        if _event_date(event) != execution:
            continue
        if kind in ORDER_KINDS:
            if _date(event.get("decision_date")) != signal:
                continue
            if kind == "FILL":
                fills.append(_snapshot_event(event))
            else:
                rejections.append(_snapshot_event(event))
        elif kind in SESSION_KINDS:
            observations.append(_snapshot_event(event))
    for key, values in (
        ("fills", fills),
        ("rejections", rejections),
        ("session_events", observations),
    ):
        result[key] = values[:MAX_EVENTS]
        if len(values) > MAX_EVENTS:
            result["truncated"] = True
    return result
