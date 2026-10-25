"""Exact-source provenance and completeness checks before research NAV processing."""

import copy

import pytest

from app.lib.strategy_engine.corporate_action_source import (
    SUPPORTED_VERSION,
    fingerprint_events,
    validate_corporate_action_source,
)

ROW = {
    "code": "sh600000",
    "ann_date": "2026-10-20",
    "record_date": "2026-10-22",
    "ex_date": "2026-10-23",
    "pay_date": "2026-10-26",
    "cash_per_share": 0.25,
    "share_multiplier": 1.0,
    "div_proc": "实施",
}


def _manifest(rows=None):
    return {
        "schema_version": SUPPORTED_VERSION,
        "evidence_kind": "FORWARD_INPUT",
        "source": "tushare-dividend-inventory",
        "from_date": "2026-10-22",
        "to_date": "2026-10-23",
        "as_of": "2026-10-23",
        "covered_codes": ["sh600000", "sz000001"],
        "failed_codes": [],
        "events_sha256": fingerprint_events([ROW] if rows is None else rows),
    }


def _check(rows=None, manifest=None, codes=None):
    items = [ROW] if rows is None else rows
    return validate_corporate_action_source(
        rows=items,
        manifest=_manifest(items) if manifest is None else manifest,
        required_codes={"sh600000"} if codes is None else codes,
        from_date="2026-10-22",
        to_date="2026-10-23",
        as_of="2026-10-23",
    )


def test_complete_declared_input_creates_normalized_ledger_actions():
    result = _check()
    assert result["covered_codes_count"] == 2
    assert result["actions"] == [
        {
            "stock_code": ROW["code"],
            "record_date": ROW["record_date"],
            "ex_date": ROW["ex_date"],
            "pay_date": ROW["pay_date"],
            "cash_per_share": 0.25,
            "share_multiplier": 1.0,
        }
    ]
    assert result["coverage_claim_status"] == "UNVERIFIED_UPSTREAM"


def test_zero_events_requires_explicit_fresh_complete_universe_manifest():
    assert _check(rows=[])["actions"] == []
    with pytest.raises(ValueError, match="universe incomplete"):
        _check(rows=[], codes={"sh600000", "sh600002"})


def test_tampered_rows_cannot_reuse_manifest():
    manifest = _manifest()
    with pytest.raises(ValueError, match="hash mismatch"):
        _check(rows=[{**ROW, "cash_per_share": 1.0}], manifest=manifest)


def test_failed_source_query_is_blocking():
    manifest = _manifest()
    manifest["failed_codes"] = [{"code": "sz000001", "error": "quota"}]
    with pytest.raises(ValueError, match="failed corporate-action"):
        _check(manifest=manifest)


def test_replay_inventory_is_not_forward_corporate_input():
    manifest = _manifest()
    manifest["evidence_kind"] = "REPLAY"
    with pytest.raises(ValueError, match="REPLAY"):
        _check(manifest=manifest)


@pytest.mark.parametrize(
    "change,problem",
    [
        ({"from_date": "2026-10-23"}, "source starts"),
        ({"to_date": "2026-10-22"}, "source ends"),
        ({"as_of": "2026-10-22"}, "source is stale"),
        ({"covered_codes": ["sh600000", "sh600000"]}, "duplicate covered"),
        ({"covered_codes": ["sz000001"]}, "universe incomplete"),
        ({"source": ""}, "source provenance"),
        ({"events_sha256": "0" * 64}, "hash mismatch"),
    ],
)
def test_manifest_incompleteness_rejected(change, problem):
    manifest = _manifest()
    manifest.update(change)
    with pytest.raises(ValueError, match=problem):
        _check(manifest=manifest)


@pytest.mark.parametrize(
    "override,reason",
    [
        ({"ann_date": "2026-10-24"}, "future-only"),
        ({"record_date": "2026-10-23"}, "future-only"),
        ({"cash_per_share": -1.0}, "invalid"),
        ({"share_multiplier": 0}, "invalid"),
        ({"div_proc": "预案"}, "implemented"),
    ],
)
def test_bad_action_is_rejected_after_manifest_hash_check(override, reason):
    row = {**ROW, **override}
    with pytest.raises(ValueError, match=reason):
        _check(rows=[row])


def test_duplicate_code_ex_date_not_silently_deduplicated():
    extra = {**ROW, "cash_per_share": 0.2}
    with pytest.raises(ValueError, match="duplicate"):
        _check(rows=[ROW, extra])


def test_canonical_hash_independent_of_input_order_and_does_not_mutate():
    rows = [ROW, {**ROW, "code": "sz000001"}]
    before = copy.deepcopy(rows)
    assert fingerprint_events(rows) == fingerprint_events(rows[::-1])
    _check(rows=rows)
    assert rows == before
