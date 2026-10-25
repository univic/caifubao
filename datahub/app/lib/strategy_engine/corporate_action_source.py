"""Fail-closed, pure corporate-action feed normalization for Research NAV.

A signed or verified data-pipeline publication is NOT created by this module.
It checks internal consistency of an explicit source inventory and events,
and makes omissions visible before the NAV planner gets dates/amounts.
A manifest's own coverage claim needs independent upstream verification.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import math
import re

SUPPORTED_VERSION = "research-corporate-actions-v1"
HEX64 = re.compile(r"^[0-9a-f]{64}$")


def _day(value, key):
    if isinstance(value, datetime.datetime):
        value = value.date().isoformat()
    elif isinstance(value, datetime.date):
        value = value.isoformat()
    if not isinstance(value, str):
        raise ValueError(f"{key} must be ISO date")
    try:
        date = datetime.date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{key} must be ISO date") from None
    if date.isoformat() != value:
        raise ValueError(f"{key} must be canonical YYYY-MM-DD")
    return date


def fingerprint_events(rows: list[dict]) -> str:
    """Canonical content digest, independent of input list order."""
    if not isinstance(rows, list) or any(not isinstance(x, dict) for x in rows):
        raise ValueError("corporate action rows must be a list of objects")
    sorted_rows = sorted(
        rows,
        key=lambda row: json.dumps(
            row, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False
        ),
    )
    content = json.dumps(
        sorted_rows,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def validate_corporate_action_source(
    *,
    rows: list[dict],
    manifest: dict,
    required_codes: set[str],
    from_date: str,
    to_date: str,
    as_of: str,
) -> dict:
    """Validate a PIT-available explicit feed and return simulator-ready events.

    Historical after-the-fact extraction is REPLAY, not prospective FORWARD
    evidence. Caller must also prove that the upstream data source queried
    the complete required universe and did not omit changed announcements.
    """
    if not isinstance(manifest, dict):
        raise ValueError("corporate action manifest is required")
    if manifest.get("schema_version") != SUPPORTED_VERSION:
        raise ValueError("unsupported corporate action manifest schema")
    if manifest.get("evidence_kind") != "FORWARD_INPUT":
        raise ValueError("a REPLAY data pull cannot validate FORWARD NAV inputs")
    digest = manifest.get("events_sha256")
    if not isinstance(digest, str) or not HEX64.fullmatch(digest):
        raise ValueError("manifest needs canonical events_sha256")
    if fingerprint_events(rows) != digest:
        raise ValueError("corporate action source content hash mismatch")
    source = manifest.get("source")
    if not isinstance(source, str) or not source.strip():
        raise ValueError("source provenance required")
    if manifest.get("failed_codes"):
        raise ValueError("source contains failed corporate-action queries")
    cover = manifest.get("covered_codes")
    if not isinstance(cover, list) or not all(
        isinstance(code, str) and code.strip() for code in cover
    ):
        raise ValueError("manifest needs explicit covered_codes")
    if len(set(cover)) != len(cover):
        raise ValueError("duplicate covered code")
    if not required_codes.issubset(set(cover)):
        raise ValueError("corporate action universe incomplete")
    start = _day(from_date, "from_date")
    end = _day(to_date, "to_date")
    snapshot_at = _day(as_of, "as_of")
    if start > end or end > snapshot_at:
        raise ValueError("range must finish by as_of")
    if _day(manifest.get("from_date"), "manifest.from_date") > start:
        raise ValueError("source starts after the required NAV period")
    if _day(manifest.get("to_date"), "manifest.to_date") < end:
        raise ValueError("source ends before the required NAV period")
    if _day(manifest.get("as_of"), "manifest.as_of") > snapshot_at:
        raise ValueError("source snapshot cannot postdate the requested as_of")
    if _day(manifest.get("as_of"), "manifest.as_of") < end:
        raise ValueError("source is stale for the required period")

    normalized = []
    seen = set()
    for row in rows:
        code = row.get("code")
        if code not in cover:
            raise ValueError("corporate event code is outside source universe")
        record = _day(row.get("record_date"), "record_date")
        ex = _day(row.get("ex_date"), "ex_date")
        pay = _day(row.get("pay_date"), "pay_date")
        announced = _day(row.get("ann_date"), "ann_date")
        if not (announced <= snapshot_at and record < ex <= pay):
            raise ValueError("corporate action has invalid or future-only dates")
        if ex < start or ex > end:
            raise ValueError("corporate event outside the declared input window")
        key = (code, ex)
        if key in seen:
            raise ValueError("duplicate instrument/ex-date corporate action")
        seen.add(key)
        if row.get("div_proc") != "实施":
            raise ValueError("only implemented distributions are admissible")
        cash = row.get("cash_per_share", 0)
        multiplier = row.get("share_multiplier", 1)
        if (
            isinstance(cash, bool)
            or not isinstance(cash, (int, float))
            or not math.isfinite(cash)
            or cash < 0
            or isinstance(multiplier, bool)
            or not isinstance(multiplier, (int, float))
            or not math.isfinite(multiplier)
            or multiplier <= 0
        ):
            raise ValueError("invalid cash/share multiplier")
        normalized.append(
            {
                "stock_code": code,
                "record_date": record.isoformat(),
                "ex_date": ex.isoformat(),
                "pay_date": pay.isoformat(),
                "cash_per_share": float(cash),
                "share_multiplier": float(multiplier),
            }
        )
    normalized.sort(key=lambda row: (row["ex_date"], row["stock_code"]))
    return {
        "source": source,
        "events_sha256": digest,
        "covered_codes_count": len(cover),
        "actions": normalized,
        "coverage_claim_status": "UNVERIFIED_UPSTREAM",
        "source_as_of": manifest["as_of"],
    }
