# -*- coding: utf-8 -*-
"""Planned-vs-actual reconciliation (roadmap 2.3).

Read-only, pure, JSON-serialisable: per-name quantity drift, cash drift,
missing/unexpected names and a fail-loud ``breaks`` list. Synthetic fixtures
only; the CLI is exercised through ``main`` with tmp_path files.
"""

import datetime
import json

import pytest

from app.lib.strategy_engine.reconcile import (
    reconcile_plan_vs_account,
)

PLAN = [
    {"stock_code": "sh600000", "quantity": 1000},
    {"stock_code": "sz300750", "quantity": 500},
]

#: A fully reconcilable snapshot: matching quantities and a verifiable cash
#: balance (both the actual and the planned figure are supplied).
MATCHING_ACCOUNT = {
    "positions": {"sh600000": 1000, "sz300750": 500},
    "cash": 1234.5,
    "planned_cash": 1234.5,
}


def test_matching_account_has_no_breaks():
    result = reconcile_plan_vs_account(PLAN, MATCHING_ACCOUNT)
    assert result["ok"] is True
    assert result["breaks"] == []
    assert [row["drift"] for row in result["quantity_drift"]] == [0.0, 0.0]
    assert result["missing_in_account"] == []
    assert result["unexpected_in_account"] == []
    assert result["cash_drift"]["drift"] == 0.0
    assert result["cash_drift"]["breach"] is False


def test_quantity_drift_is_a_loud_break():
    result = reconcile_plan_vs_account(
        PLAN,
        {
            "positions": {"sh600000": 900, "sz300750": 500},
            "cash": 1234.5,
            "planned_cash": 1234.5,
        },
    )
    assert result["ok"] is False
    row = {r["stock_code"]: r for r in result["quantity_drift"]}["sh600000"]
    assert row == {
        "stock_code": "sh600000",
        "planned": 1000.0,
        "actual": 900.0,
        "drift": -100.0,
        "tolerance": 0.0,
        "breach": True,
    }
    assert result["breaks"][0]["kind"] == "quantity_drift"
    assert result["breaks"][0]["stock_code"] == "sh600000"


def test_missing_and_unexpected_names_are_reported():
    result = reconcile_plan_vs_account(
        PLAN,
        {
            "positions": {"sh600000": 1000, "sh601111": 200},
            "cash": 1234.5,
            "planned_cash": 1234.5,
        },
    )
    assert result["missing_in_account"] == ["sz300750"]
    assert result["unexpected_in_account"] == ["sh601111"]
    kinds = {(b["kind"], b.get("stock_code")) for b in result["breaks"]}
    assert ("missing_in_account", "sz300750") in kinds
    assert ("unexpected_in_account", "sh601111") in kinds


def test_cash_drift_is_a_break_and_planned_cash_is_required_to_verify():
    over = reconcile_plan_vs_account(
        PLAN,
        {
            "positions": {"sh600000": 1000, "sz300750": 500},
            "cash": 100.0,
            "planned_cash": 50.0,
        },
    )
    assert over["cash_drift"]["drift"] == 50.0
    assert over["cash_drift"]["breach"] is True
    assert any(b["kind"] == "cash_drift" for b in over["breaks"])
    # An actual cash balance with no planned figure cannot be verified: loud.
    unverifiable = reconcile_plan_vs_account(
        PLAN, {"positions": {"sh600000": 1000, "sz300750": 500}, "cash": 100.0}
    )
    assert unverifiable["ok"] is False
    assert any(b["kind"] == "cash_drift_unverifiable" for b in unverifiable["breaks"])


def test_absent_cash_is_an_unverifiable_break_not_a_pass():
    """Spec MUST: cash that cannot be verified must fail loudly. A snapshot with
    no ``cash`` key is not evidence that cash matches."""
    result = reconcile_plan_vs_account(
        PLAN, {"positions": {"sh600000": 1000, "sz300750": 500}}
    )
    assert result["ok"] is False
    assert result["cash_drift"]["actual"] is None
    assert result["cash_drift"]["breach"] is False  # no drift can be measured
    (break_row,) = [
        b for b in result["breaks"] if b["kind"] == "cash_drift_unverifiable"
    ]
    assert break_row["reason"] == "account_snapshot.cash is missing"
    # A wrong tolerance cannot excuse missing evidence either.
    loose = reconcile_plan_vs_account(
        PLAN,
        {"positions": {"sh600000": 1000, "sz300750": 500}},
        tolerances={"quantity_abs": 10_000, "cash_abs": 10_000},
    )
    assert loose["ok"] is False
    assert any(b["kind"] == "cash_drift_unverifiable" for b in loose["breaks"])


def test_tolerances_absorb_small_drift_but_never_a_missing_name():
    loose = reconcile_plan_vs_account(
        PLAN,
        {
            "positions": {"sh600000": 1005, "sz300750": 500},
            "cash": 1234.5,
            "planned_cash": 1234.5,
        },
        tolerances={"quantity_abs": 10, "cash_abs": 100},
    )
    assert loose["ok"] is True
    assert loose["tolerances"]["quantity_abs"] == 10.0
    # Missing names stay a break at any tolerance.
    missing = reconcile_plan_vs_account(
        PLAN,
        {
            "positions": {"sh600000": 1000},
            "cash": 1234.5,
            "planned_cash": 1234.5,
        },
        tolerances={"quantity_abs": 10_000},
    )
    assert missing["ok"] is False


def test_result_is_json_serialisable():
    result = reconcile_plan_vs_account(
        PLAN,
        {
            "positions": {"sh600000": 900},
            "cash": 10,
            "planned_cash": 20,
        },
    )
    encoded = json.dumps(result, default=str)
    assert json.loads(encoded)["ok"] is False


def test_as_of_datetime_is_normalised_to_an_iso_string():
    result = reconcile_plan_vs_account(
        PLAN, {**MATCHING_ACCOUNT, "as_of": datetime.datetime(2026, 9, 11, 15, 30)}
    )
    assert result["as_of"] == "2026-09-11T15:30:00"
    # No `default=` shim needed: the result itself is JSON-serialisable.
    assert json.loads(json.dumps(result))["as_of"] == "2026-09-11T15:30:00"


def test_tolerances_must_be_non_negative():
    with pytest.raises(ValueError, match="non-negative"):
        reconcile_plan_vs_account(
            PLAN, MATCHING_ACCOUNT, tolerances={"quantity_abs": -1}
        )


@pytest.mark.parametrize(
    "tolerances",
    [
        {"quantity_abs": float("nan")},
        {"quantity_abs": float("inf")},
        {"cash_abs": float("nan")},
        {"cash_abs": float("inf")},
        {"cash_abs": 1e999},  # parses to +inf from JSON
    ],
)
def test_non_finite_tolerances_are_rejected_not_silently_disabling(tolerances):
    """NaN/inf comparisons are always False, so a non-finite tolerance would
    disable the break it configures. `--quantity-tolerance nan` must not turn a
    1000-vs-50 mismatch into a pass."""
    with pytest.raises(ValueError, match="finite number"):
        reconcile_plan_vs_account(PLAN, MATCHING_ACCOUNT, tolerances=tolerances)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_account_values_are_rejected(bad):
    """`NaN`/`Infinity` are valid JSON for Python's `json.load`; a non-finite
    quantity or cash balance must fail loudly, not report ok/drift NaN."""
    with pytest.raises(ValueError, match="finite number"):
        reconcile_plan_vs_account(
            PLAN, {"positions": {"sh600000": bad}, "cash": 0, "planned_cash": 0}
        )
    with pytest.raises(ValueError, match="finite number"):
        reconcile_plan_vs_account(
            PLAN,
            {
                "positions": {"sh600000": 1000, "sz300750": 500},
                "cash": bad,
                "planned_cash": 0,
            },
        )
    with pytest.raises(ValueError, match="finite number"):
        reconcile_plan_vs_account(
            PLAN,
            {
                "positions": {"sh600000": 1000, "sz300750": 500},
                "cash": 0,
                "planned_cash": bad,
            },
        )


def test_cli_rejects_json_nan_with_a_non_zero_exit(tmp_path, capsys):
    """`NaN` is valid JSON for Python's json.load, so it reaches the reconciler
    from a file. The CLI must exit non-zero with an error, never ok=true."""
    from app.lib.strategy_engine.reconcile import main

    plan = tmp_path / "plan.json"
    account = tmp_path / "account.json"
    plan.write_text(json.dumps(PLAN), encoding="utf-8")
    account.write_text(
        '{"positions": {"sh600000": NaN, "sz300750": 500}, "cash": 0,'
        ' "planned_cash": 0}',
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as excinfo:
        main(["--plan", str(plan), "--account", str(account)])
    assert excinfo.value.code == 1
    assert "finite number" in capsys.readouterr().err


def test_cli_rejects_non_finite_tolerance(tmp_path, capsys):
    from app.lib.strategy_engine.reconcile import main

    plan = tmp_path / "plan.json"
    account = tmp_path / "account.json"
    plan.write_text(json.dumps(PLAN), encoding="utf-8")
    account.write_text(json.dumps(MATCHING_ACCOUNT), encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "--plan",
                str(plan),
                "--account",
                str(account),
                "--quantity-tolerance",
                "nan",
            ]
        )
    assert excinfo.value.code == 1
    assert "finite number" in capsys.readouterr().err


def test_positions_may_be_a_list_and_quantities_are_fail_closed():
    listed = reconcile_plan_vs_account(
        [{"stock_code": "sh600000", "quantity": 100}],
        {
            "positions": [{"stock_code": "sh600000", "quantity": 100}],
            "cash": 0,
            "planned_cash": 0,
        },
    )
    assert listed["ok"] is True
    with pytest.raises(ValueError, match="no quantity"):
        reconcile_plan_vs_account(
            [{"stock_code": "sh600000", "quantity": 100}],
            {"positions": [{"stock_code": "sh600000"}], "cash": 0},
        )


def test_cli_exits_non_zero_on_a_break(tmp_path, capsys):
    import pytest as _pytest

    from app.lib.strategy_engine.reconcile import main

    plan = tmp_path / "plan.json"
    account = tmp_path / "account.json"
    plan.write_text(json.dumps(PLAN), encoding="utf-8")
    account.write_text(
        json.dumps(
            {
                "positions": {"sh600000": 900, "sz300750": 500},
                "cash": 1234.5,
                "planned_cash": 1234.5,
            }
        ),
        encoding="utf-8",
    )
    with _pytest.raises(SystemExit) as excinfo:
        main(["--plan", str(plan), "--account", str(account)])
    assert excinfo.value.code == 1
    assert "tolerance breach" in capsys.readouterr().err


def test_cli_exits_non_zero_when_cash_cannot_be_verified(tmp_path, capsys):
    import pytest as _pytest

    from app.lib.strategy_engine.reconcile import main

    plan = tmp_path / "plan.json"
    account = tmp_path / "account.json"
    plan.write_text(json.dumps(PLAN), encoding="utf-8")
    # Matching quantities but NO cash balance: unverifiable -> loud.
    account.write_text(
        json.dumps({"positions": {"sh600000": 1000, "sz300750": 500}}),
        encoding="utf-8",
    )
    with _pytest.raises(SystemExit) as excinfo:
        main(["--plan", str(plan), "--account", str(account)])
    assert excinfo.value.code == 1
    assert "tolerance breach" in capsys.readouterr().err


def test_cli_exits_zero_when_fully_reconciled(tmp_path, capsys):
    from app.lib.strategy_engine.reconcile import main

    plan = tmp_path / "plan.json"
    account = tmp_path / "account.json"
    plan.write_text(json.dumps(PLAN), encoding="utf-8")
    account.write_text(
        json.dumps(
            {
                "positions": {"sh600000": 1000, "sz300750": 500},
                "cash": 1234.5,
                "planned_cash": 1234.5,
            }
        ),
        encoding="utf-8",
    )
    main(["--plan", str(plan), "--account", str(account)])
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["breaks"] == []
