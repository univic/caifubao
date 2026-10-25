"""Research simulation provenance must be content- and date-verifiable."""

import datetime
from copy import deepcopy

from app.lib.research_paper_lineage import _digest, inspect_research_trace

DAY = "2026-10-23"
EXEC = "2026-10-26"


def _run():
    return {
        "date": datetime.datetime.fromisoformat(DAY),
        "execution_date": datetime.datetime.fromisoformat(EXEC),
        "status": "COMPLETED",
        "strategy_name": "flip_wide_paper",
        "config_hash": "spec123",
        "hash_kind": "spec",
    }


def _artifact():
    payload = {
        "schema_version": "research-simulation-v1",
        "hash_kind": "spec",
        "config_hash": "spec123",
        "run_dates": [DAY],
        "schedule": [{"date": EXEC, "decision_date": DAY, "holdings": {}}],
        "events": [
            {
                "type": "FILL",
                "event_id": "fill1",
                "fill_id": "fill1",
                "order_id": "order1",
                "source_domain": "research",
                "account_id": "research-simulation",
                "spec_hash": "spec123",
                "stock_code": "sh600000",
                "decision_date": DAY,
                "execution_date": EXEC,
                "quantity": 100,
                "price": 10,
                "fees": {"commission": 1.5},
            },
            {
                "type": "REJECTED",
                "event_id": "reject1",
                "order_id": "order2",
                "source_domain": "research",
                "account_id": "research-simulation",
                "spec_hash": "spec123",
                "stock_code": "sz000001",
                "decision_date": DAY,
                "execution_date": EXEC,
                "reason_code": "LIMIT_UP",
            },
            {
                "type": "MARK",
                "event_id": "mark1",
                "as_of": EXEC,
                "marks": {"sh600000": 10.2},
            },
            {
                "type": "DIVIDEND_ENTITLEMENT",
                "event_id": "div1",
                "stock_code": "sh600000",
                "record_date": DAY,
                "ex_date": EXEC,
                "pay_date": "2026-10-30",
                "cash_per_share": 0.5,
                "quantity": 100,
            },
            {
                "type": "SPLIT",
                "event_id": "split1",
                "stock_code": "sh600000",
                "share_multiplier": 2,
            },
        ],
        "ledger_snapshot": {},
        "curve": [],
        "nav_parity": {},
        "benchmark_returns": {},
    }
    return {
        "strategy_name": "flip_wide_paper",
        "config_hash": "spec123",
        "hash_kind": "spec",
        "artifact_hash": _digest(payload),
        "payload": payload,
    }


def test_full_immutable_trace_with_unattributable_split():
    result = inspect_research_trace(_run(), [_artifact()])
    assert result["status"] == "LINKED_SIMULATION"
    assert result["research_only"] is True
    assert len(result["fills"]) == 1
    assert result["fills"][0]["fill_id"] == "fill1"
    assert result["fills"][0]["fees"] == {"commission": 1.5}
    assert len(result["rejections"]) == 1
    assert result["rejections"][0]["reason_code"] == "LIMIT_UP"
    assert {x["type"] for x in result["session_events"]} == {
        "MARK",
        "DIVIDEND_ENTITLEMENT",
    }
    assert result["undated_corporate_events"] == 1
    assert "marks" not in result["session_events"][0]


def test_tampering_with_payload_fails_closed_even_if_hash_is_stored():
    art = _artifact()
    art["payload"]["events"][0]["quantity"] = 90000
    assert inspect_research_trace(_run(), [art])["status"] == "INVALID_PROVENANCE"


def test_wrong_run_date_and_wrong_execution_pairing_fail():
    for field, value in (("run_dates", ["2026-10-22"]), ("schedule", [])):
        art = _artifact()
        art["payload"][field] = value
        art["artifact_hash"] = _digest(art["payload"])
        assert inspect_research_trace(_run(), [art])["status"] == "INVALID_PROVENANCE"


def test_duplicate_event_ids_are_rejected_after_rehash():
    art = _artifact()
    art["payload"]["events"][1]["event_id"] = "fill1"
    art["artifact_hash"] = _digest(art["payload"])
    assert inspect_research_trace(_run(), [art])["status"] == "INVALID_PROVENANCE"


def test_wrong_spec_or_account_denies_all_fill_attribution():
    for field, value in (
        ("spec_hash", "otherhash"),
        ("account_id", "trading"),
        ("source_domain", "trading"),
    ):
        art = _artifact()
        art["payload"]["events"][0][field] = value
        art["artifact_hash"] = _digest(art["payload"])
        assert inspect_research_trace(_run(), [art])["status"] == "INVALID_PROVENANCE"


def test_no_artifact_and_two_candidates_never_pick_latest():
    run = _run()
    art = _artifact()
    assert inspect_research_trace(run, [])["status"] == "NOT_LINKED"
    assert (
        inspect_research_trace(run, [art, deepcopy(art)])["status"]
        == "AMBIGUOUS_ARTIFACT"
    )


def test_unattributed_old_fill_must_not_be_presented_as_decision_fill():
    art = _artifact()
    del art["payload"]["events"][0]["decision_date"]
    art["artifact_hash"] = _digest(art["payload"])
    result = inspect_research_trace(_run(), [art])
    assert result["status"] == "LINKED_SIMULATION"
    assert result["fills"] == []
    assert len(result["rejections"]) == 1


def test_replay_classification_is_read_only_not_promoted():
    result = inspect_research_trace(
        {**_run(), "evidence_kind": "REPLAY"}, [_artifact()]
    )
    assert result["status"] == "LINKED_SIMULATION"
    assert result["research_only"] is True
    assert "forward_eligible" not in result


def test_executing_wrong_date_cannot_share_simulated_fill():
    art = _artifact()
    run = {**_run(), "execution_date": datetime.datetime(2026, 10, 27)}
    assert inspect_research_trace(run, [art])["status"] == "INVALID_PROVENANCE"


def test_events_are_capped():
    art = _artifact()
    art["payload"]["events"] = [
        {**art["payload"]["events"][0], "event_id": f"unique{i}"} for i in range(115)
    ]
    art["artifact_hash"] = _digest(art["payload"])
    result = inspect_research_trace(_run(), [art])
    assert result["status"] == "LINKED_SIMULATION"
    assert len(result["fills"]) == 100
    assert result["truncated"] is True
