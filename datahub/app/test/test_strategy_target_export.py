# -*- coding: utf-8 -*-
"""Tests for the strategy target-portfolio export (roadmap 2.1).

Pure-logic tests only: no Mongo, no runner layer. The exporter must stay
read-only and research-grade labeled (production-capability-roadmap invariants).
"""

import datetime
import pytest

from app.lib.strategy_engine.export import (
    EXPORT_DISCLAIMER,
    EXPORT_GRADE,
    build_target_export,
    render_csv,
    select_export_run,
)

FIXED_NOW = datetime.datetime(2026, 9, 12, 10, 0, tzinfo=datetime.UTC)


def _run(
    *,
    status="COMPLETED",
    target_holdings=None,
    rebalance=None,
    nav_snapshot=None,
    config=None,
    evidence_kind="REPLAY",
):
    return {
        "strategy_name": "flip_wide_paper",
        "model_version": "flip_wide_shadow_v1",
        "horizon": 20,
        "config_hash": "abc123",
        "date": datetime.datetime(2026, 9, 11),
        "execution_date": datetime.datetime(2026, 9, 12),
        "decision_at": datetime.datetime(2026, 9, 11, 10, 40, tzinfo=datetime.UTC),
        "evidence_kind": evidence_kind,
        "status": status,
        "target_holdings": target_holdings
        if target_holdings is not None
        else [
            {"stock_code": "sz000001", "weight": 0.5},
            {"stock_code": "sh600000", "weight": 0.5},
        ],
        "rebalance": rebalance
        if rebalance is not None
        else {
            "added": ["sz000001"],
            "removed": ["sz300750"],
            "unchanged": ["sh600000"],
        },
        "nav_snapshot": nav_snapshot,
        "config": config if config is not None else {"initial_nav": 1_000_000.0},
    }


def test_added_removed_unchanged_map_to_buy_sell_hold():
    result = build_target_export(run=_run(), generated_at=FIXED_NOW)
    sides = {row["stock_code"]: row["side"] for row in result["rows"]}
    assert sides == {
        "sz000001": "BUY",
        "sz300750": "SELL",
        "sh600000": "HOLD",
    }


def test_rows_carry_weight_and_amount_from_single_base_nav():
    result = build_target_export(run=_run(), generated_at=FIXED_NOW)
    by_code = {row["stock_code"]: row for row in result["rows"]}
    assert by_code["sz000001"]["target_weight"] == 0.5
    assert by_code["sz000001"]["target_amount_cny"] == 500_000.0
    assert by_code["sh600000"]["target_amount_cny"] == 500_000.0
    # A removed name has no target weight and therefore no target amount.
    assert by_code["sz300750"]["target_weight"] is None
    assert by_code["sz300750"]["target_amount_cny"] is None
    # One base NAV for every row.
    assert result["base_nav"] == 1_000_000.0


def test_base_nav_prefers_nav_snapshot_and_reports_source():
    run = _run(nav_snapshot={"nav": 1_234_567.0})
    result = build_target_export(run=run, generated_at=FIXED_NOW)
    assert result["base_nav"] == 1_234_567.0
    assert result["base_nav_source"] == "nav_snapshot"
    by_code = {row["stock_code"]: row for row in result["rows"]}
    assert by_code["sz000001"]["target_amount_cny"] == 617_283.5


def test_base_nav_falls_back_to_config_initial_nav():
    run = _run(config={"initial_nav": 20_000_000.0})
    result = build_target_export(run=run, generated_at=FIXED_NOW)
    assert result["base_nav"] == 20_000_000.0
    assert result["base_nav_source"] == "config.initial_nav"


def test_explicit_base_nav_overrides_run_sources():
    run = _run(nav_snapshot={"nav": 1_234_567.0}, config={"initial_nav": 20_000_000.0})
    result = build_target_export(run=run, base_nav=500_000.0, generated_at=FIXED_NOW)
    assert result["base_nav"] == 500_000.0
    assert result["base_nav_source"] == "explicit"


def test_missing_base_nav_fails_closed():
    run = _run(config={}, nav_snapshot={})
    with pytest.raises(ValueError, match="base NAV"):
        build_target_export(run=run, generated_at=FIXED_NOW)


def test_research_grade_label_and_disclaimer_are_always_present():
    result = build_target_export(run=_run(), generated_at=FIXED_NOW)
    assert result["grade"] == EXPORT_GRADE
    assert result["disclaimer"] == EXPORT_DISCLAIMER
    assert "not investment advice" in result["disclaimer"].lower()


def test_replay_run_is_reported_as_replay_not_forward_validated():
    result = build_target_export(
        run=_run(evidence_kind="REPLAY"), generated_at=FIXED_NOW
    )
    assert result["evidence_kind"] == "REPLAY"


def test_forward_run_reports_forward_evidence_kind():
    result = build_target_export(
        run=_run(evidence_kind="FORWARD"), generated_at=FIXED_NOW
    )
    assert result["evidence_kind"] == "FORWARD"


@pytest.mark.parametrize("status", ["SKIPPED", "FAILED", "RUNNING"])
def test_non_completed_status_fails_closed(status):
    with pytest.raises(ValueError, match=status):
        build_target_export(run=_run(status=status), generated_at=FIXED_NOW)


def test_buy_or_hold_without_target_weight_fails_closed():
    run = _run(
        target_holdings=[{"stock_code": "sh600000", "weight": 0.5}],
        rebalance={"added": ["sz000001"], "removed": [], "unchanged": ["sh600000"]},
    )
    with pytest.raises(ValueError, match="target weight"):
        build_target_export(run=run, generated_at=FIXED_NOW)


def test_row_order_is_deterministic_grouped_by_side_then_code():
    run = _run(
        target_holdings=[
            {"stock_code": "sz000002", "weight": 0.25},
            {"stock_code": "sh600000", "weight": 0.25},
            {"stock_code": "sz000001", "weight": 0.25},
        ],
        rebalance={
            "added": ["sz000002", "sz000001"],
            "removed": ["sh601318", "sz300750"],
            "unchanged": ["sh600000"],
        },
    )
    result = build_target_export(run=run, generated_at=FIXED_NOW)
    assert [(r["side"], r["stock_code"]) for r in result["rows"]] == [
        ("BUY", "sz000001"),
        ("BUY", "sz000002"),
        ("SELL", "sh601318"),
        ("SELL", "sz300750"),
        ("HOLD", "sh600000"),
    ]
    again = build_target_export(run=run, generated_at=FIXED_NOW)
    assert result["rows"] == again["rows"]


def test_traceability_fields_are_present():
    result = build_target_export(run=_run(), generated_at=FIXED_NOW)
    assert result["strategy_name"] == "flip_wide_paper"
    assert result["model_version"] == "flip_wide_shadow_v1"
    assert result["horizon"] == 20
    assert result["config_hash"] == "abc123"
    assert result["date"] == "2026-09-11"
    assert result["execution_date"].startswith("2026-09-12")
    assert result["status"] == "COMPLETED"
    assert result["generated_at"] == FIXED_NOW.isoformat()


def test_scores_names_and_counts_are_joined_when_provided():
    result = build_target_export(
        run=_run(),
        scores={
            "sz000001": {"score": 91.5, "percentile": 0.03},
            "sh600000": {"score": 12.0, "percentile": 0.97},
        },
        names={"sz000001": "平安银行", "sh600000": "浦发银行"},
        generated_at=FIXED_NOW,
    )
    by_code = {row["stock_code"]: row for row in result["rows"]}
    assert by_code["sz000001"]["score"] == 91.5
    assert by_code["sz000001"]["percentile"] == 0.03
    assert by_code["sz000001"]["stock_name"] == "平安银行"
    # A code with no prediction row keeps nulls rather than inventing values.
    assert by_code["sz300750"]["score"] is None
    assert by_code["sz300750"]["stock_name"] is None
    assert result["counts"] == {"buy": 1, "sell": 1, "hold": 1, "total": 3}


def test_empty_rebalance_yields_no_rows_but_keeps_metadata():
    run = _run(
        target_holdings=[], rebalance={"added": [], "removed": [], "unchanged": []}
    )
    result = build_target_export(run=run, generated_at=FIXED_NOW)
    assert result["rows"] == []
    assert result["counts"]["total"] == 0
    assert result["grade"] == EXPORT_GRADE


def test_render_csv_leads_with_compliance_comment_then_header_and_rows():
    result = build_target_export(run=_run(), generated_at=FIXED_NOW)
    text = render_csv(result)
    lines = text.strip().splitlines()
    assert lines[0].startswith("# grade=RESEARCH")
    assert EXPORT_DISCLAIMER in lines[0]
    assert lines[1] == (
        "side,stock_code,stock_name,score,percentile,"
        "target_weight,target_amount_cny,reason"
    )
    assert len(lines) == 2 + len(result["rows"])
    assert lines[2].startswith("BUY,sz000001")


def test_render_csv_is_stable_across_calls():
    result = build_target_export(run=_run(), generated_at=FIXED_NOW)
    assert render_csv(result) == render_csv(result)


# ---------------------------------------------------------------------------
# run selection (fail-closed / ambiguity)
# ---------------------------------------------------------------------------


def _view(**overrides):
    view = {
        "status": "COMPLETED",
        "config_hash": "hash-a",
        "date": "2026-09-11",
    }
    view.update(overrides)
    return view


def test_select_export_run_prefers_completed_over_later_other_status():
    chosen = select_export_run(
        [_view(status="SKIPPED", config_hash="hash-b"), _view(config_hash="hash-a")]
    )
    assert chosen["config_hash"] == "hash-a"


def test_select_export_run_fails_closed_when_ambiguous():
    with pytest.raises(ValueError, match="ambiguous"):
        select_export_run([_view(config_hash="hash-a"), _view(config_hash="hash-b")])


def test_select_export_run_disambiguates_by_config_hash():
    chosen = select_export_run(
        [_view(config_hash="hash-a"), _view(config_hash="hash-b")],
        config_hash="hash-b",
    )
    assert chosen["config_hash"] == "hash-b"


def test_select_export_run_returns_non_completed_so_status_is_reported():
    chosen = select_export_run([_view(status="FAILED")])
    assert chosen["status"] == "FAILED"
    with pytest.raises(ValueError, match="FAILED"):
        build_target_export(run=_run(status="FAILED"), generated_at=FIXED_NOW)


def test_select_export_run_rejects_an_empty_candidate_set():
    with pytest.raises(ValueError, match="no completed paper run"):
        select_export_run([])
