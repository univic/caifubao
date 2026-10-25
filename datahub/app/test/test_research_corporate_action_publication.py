"""Offline corporate-action inventory must show real per-symbol query receipts."""

import hashlib
import json
from pathlib import Path

import pytest

from app.lib.strategy_engine.corporate_action_publication import (
    capture_replay_inventory,
)
from app.lib.strategy_engine.corporate_action_source import fingerprint_events
from scripts.research_capture_corporate_actions import write_archive


class Frame:
    def __init__(self, items):
        self.items = items

    def to_dict(self, orient):
        assert orient == "records"
        return self.items


def event(**changes):
    row = {
        "ts_code": "600000.SH",
        "div_proc": "实施",
        "ann_date": "20261020",
        "record_date": "20261022",
        "ex_date": "20261023",
        "pay_date": "20261026",
        "cash_div": 0.5,
        "cash_div_tax": 0.5,
        "stk_div": 0.0,
        "stk_bo_rate": 0.0,
        "stk_co_rate": 0.0,
    }
    row.update(changes)
    return row


class Provider:
    def __init__(self, responses):
        self.responses = responses
        self.called = []

    def dividend(self, *, ts_code, fields):
        self.called.append((ts_code, fields))
        value = self.responses[ts_code]
        if isinstance(value, Exception):
            raise value
        return value


def capture(provider, *, codes=None):
    return capture_replay_inventory(
        pro=provider,
        codes=codes or ["sh600000", "sz000001"],
        from_date="2026-10-22",
        to_date="2026-10-23",
        as_of="2026-10-23",
    )


def test_every_code_has_fresh_receipt_including_valid_empty():
    pro = Provider({"600000.SH": Frame([event()]), "000001.SZ": Frame([])})
    out = capture(pro)
    manifest = out["manifest"]
    assert len(pro.called) == 2
    assert manifest["evidence_kind"] == "REPLAY"
    assert manifest["coverage_claim_status"] == "UNVERIFIED_UPSTREAM"
    assert manifest["covered_codes"] == ["sh600000", "sz000001"]
    assert manifest["events_sha256"] == fingerprint_events(out["events"])
    assert manifest["events_count"] == 1
    assert [x["included_events"] for x in manifest["query_receipts"]] == [1, 0]
    assert all(x["status"] == "SUCCESS" for x in manifest["query_receipts"])
    sha = hashlib.sha256(
        json.dumps(
            manifest["query_receipts"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert sha == manifest["receipts_sha256"]
    assert out["events"][0]["cash_per_share"] == 0.5


def test_partial_error_does_not_publish_any_false_coverage():
    pro = Provider(
        {"600000.SH": Frame([event()]), "000001.SZ": TimeoutError("provider throttle")}
    )
    with pytest.raises(ValueError, match="failed queries"):
        capture(pro)


def test_null_source_is_not_a_successful_empty_response():
    with pytest.raises(ValueError, match="failed queries"):
        capture(Provider({"600000.SH": None}), codes=["sh600000"])


def test_future_vendor_revision_after_asof_is_rejected():
    with pytest.raises(ValueError, match="failed queries"):
        capture(
            Provider({"600000.SH": Frame([event(ann_date="20261024")])}),
            codes=["sh600000"],
        )


@pytest.mark.parametrize(
    "value",
    [
        {"record_date": "20261023"},
        {"pay_date": "20261021"},
        {"cash_div": float("nan")},
        {"stk_div": 0.5, "stk_bo_rate": 0.0, "stk_co_rate": 0.0},
        {"cash_div": 0.6},
        {"ts_code": "000001.SZ"},
    ],
)
def test_bad_applied_event_fails_without_silent_substitution(value):
    with pytest.raises(ValueError, match="failed queries"):
        capture(
            Provider({"600000.SH": Frame([event(**value)])}),
            codes=["sh600000"],
        )


def test_duplicate_ex_date_requires_manual_reconciliation():
    with pytest.raises(ValueError, match="failed queries"):
        capture(
            Provider({"600000.SH": Frame([event(), event(ann_date="20261021")])}),
            codes=["sh600000"],
        )


def test_atomic_digest_named_archive_is_idempotent(tmp_path: Path):
    bundle = capture(
        Provider({"600000.SH": Frame([event()])}),
        codes=["sh600000"],
    )
    folder = write_archive(tmp_path, bundle)
    assert (folder / "events.json").is_file()
    assert (folder / "manifest.json").is_file()
    assert write_archive(tmp_path, bundle) == folder
    assert not list(tmp_path.glob(".corporate-stage-*"))


def test_archive_detects_existing_tamper(tmp_path: Path):
    bundle = capture(
        Provider({"600000.SH": Frame([event()])}),
        codes=["sh600000"],
    )
    folder = write_archive(tmp_path, bundle)
    (folder / "events.json").write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="content differs"):
        write_archive(tmp_path, bundle)
