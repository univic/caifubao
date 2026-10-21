"""Contract tests for the deterministic offline ETF benchmark replay."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, localcontext

import pytest

from app.lib.strategy_engine.etf_benchmark import replay_etf_benchmark
from app.lib.strategy_engine.halt import HaltError, engage_halt
from app.lib.strategy_engine.reconcile import reconcile_plan_vs_account


SESSIONS = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]
CODE = "sh510300"
MISSING = object()


def _quote(
    date,
    *,
    opening="4.000",
    close="4.100",
    trade_status=1,
    upper_limit="4.400",
):
    row = {
        "date": date,
        "open": opening,
        "close": close,
        "trade_status": trade_status,
    }
    if upper_limit is not MISSING:
        row["upper_limit"] = upper_limit
    return row


def _quotes(sessions=SESSIONS):
    rows = []
    for index, date in enumerate(sessions):
        rows.append(
            _quote(
                date,
                close="4.000" if index == 0 else "4.100",
            )
        )
    return rows


def _payload(
    *,
    sessions=None,
    quotes=None,
    initial_cash=MISSING,
    allocation=1,
    fees=None,
):
    sessions = list(sessions or SESSIONS)
    payload = {
        "schema_version": "etf-benchmark-v1",
        "price_basis": "raw",
        "instrument": {
            "code": CODE,
            "type": "domestic_equity_etf",
            "tick_size": "0.001",
        },
        "allocation": allocation,
        "fees": {
            "commission_rate": "0.00025",
            "minimum_commission": "5.00",
            "slippage_rate": "0",
        },
        "decision_date": sessions[0],
        "sessions": sessions,
        "quotes": deepcopy(_quotes(sessions) if quotes is None else quotes),
        "corporate_actions": [],
    }
    if initial_cash is not MISSING:
        payload["initial_cash"] = initial_cash
    if fees is not None:
        payload["fees"].update(fees)
    return payload


def _run(payload, tmp_path):
    return replay_etf_benchmark(
        payload,
        halt_path=str(tmp_path / "strategy-halt.json"),
    )


def _rows_by_date(result):
    return {row["date"]: row for row in result["curve"]}


def _decimal(value):
    return Decimal(str(value))


def test_full_allocation_reserves_fee_and_marks_daily_nav(tmp_path):
    result = _run(_payload(), tmp_path)

    assert result["evidence_kind"] == "REPLAY"
    assert result["price_basis"] == "raw"
    assert len(result["trades"]) == 1
    trade = result["trades"][0]
    assert set(trade) == {
        "trade_id",
        "date",
        "decision_date",
        "stock_code",
        "side",
        "quantity",
        "price",
        "notional",
        "commission",
        "cash_after",
    }
    assert trade["date"] == SESSIONS[1]
    assert trade["decision_date"] == SESSIONS[0]
    assert trade["stock_code"] == CODE
    assert trade["side"] == "BUY"
    assert trade["quantity"] == 24_900
    assert trade["price"] == "4"
    assert trade["notional"] == "99600.00"
    assert trade["commission"] == "24.90"
    assert trade["cash_after"] == "375.10"
    for field in ("price", "notional", "commission", "cash_after"):
        assert isinstance(trade[field], str)

    curve = _rows_by_date(result)
    assert set(curve) == set(SESSIONS[1:])
    execution = curve[SESSIONS[1]]
    assert execution["action"] == "BUY"
    assert execution["quantity"] == 24_900
    assert execution["sellable_quantity"] == 0
    assert execution["cash"] == "375.10"
    assert execution["commission"] == "24.90"
    assert execution["nav"] == "102465.10"
    assert execution["mark_price"] == "4.1"
    assert execution["mark_as_of"] == SESSIONS[1]
    assert execution["mark_source"] == "CLOSE"
    assert execution["valuation_status"] == "OK"

    assert curve[SESSIONS[2]]["action"] == "HOLD"
    assert curve[SESSIONS[2]]["sellable_quantity"] == 24_900
    assert result["final_account"] == {
        "positions": {CODE: 24_900},
        "cash": "375.10",
        "planned_cash": "375.10",
        "as_of": SESSIONS[-1],
    }
    assert result["target_holdings"] == [{"stock_code": CODE, "quantity": 24_900}]
    assert len(result["input_hash"]) == 64
    assert len(result["config_hash"]) == 64


def test_identical_frozen_input_is_byte_for_byte_reproducible(tmp_path):
    payload = _payload()
    first = _run(deepcopy(payload), tmp_path)
    second = _run(deepcopy(payload), tmp_path)
    assert first == second


def test_replay_is_independent_of_callers_decimal_context(tmp_path):
    payload = _payload()
    baseline = _run(deepcopy(payload), tmp_path)
    with localcontext() as context:
        context.prec = 6
        constrained = _run(deepcopy(payload), tmp_path)
    assert constrained == baseline


def test_execution_and_future_closes_cannot_change_opening_fill(tmp_path):
    baseline = _run(_payload(), tmp_path)
    changed = _payload()
    changed["quotes"][1]["close"] = "99.999"
    changed["quotes"][2]["close"] = "0.001"
    changed_result = _run(changed, tmp_path)

    fields = (
        "date",
        "decision_date",
        "stock_code",
        "side",
        "quantity",
        "price",
        "notional",
        "commission",
        "cash_after",
    )
    assert {field: baseline["trades"][0][field] for field in fields} == {
        field: changed_result["trades"][0][field] for field in fields
    }
    assert changed_result["curve"][0]["nav"] != baseline["curve"][0]["nav"]


def test_minimum_commission_is_included_in_lot_sizing(tmp_path):
    result = _run(
        _payload(
            allocation=0.02,
            fees={
                "commission_rate": "0.00025",
                "minimum_commission": "5",
                "slippage_rate": "0",
            },
        ),
        tmp_path,
    )
    trade = result["trades"][0]
    assert trade["quantity"] == 400
    assert trade["notional"] == "1600.00"
    assert trade["commission"] == "5.00"
    assert trade["cash_after"] == "98395.00"


def test_adverse_slippage_rounds_up_to_tick_before_sizing(tmp_path):
    result = _run(
        _payload(
            fees={
                "commission_rate": "0.00025",
                "minimum_commission": "5",
                "slippage_rate": "0.0001",
            }
        ),
        tmp_path,
    )
    trade = result["trades"][0]
    assert trade["price"] == "4.001"
    assert trade["quantity"] == 24_900
    assert trade["notional"] == "99624.90"
    assert trade["commission"] == "24.91"
    assert _decimal(trade["cash_after"]) == Decimal("350.19")


def test_budget_below_one_lot_keeps_cash_and_emits_no_fee(tmp_path):
    result = _run(_payload(initial_cash="400"), tmp_path)
    assert result["trades"] == []
    assert result["final_account"] == {
        "positions": {},
        "cash": "400.00",
        "planned_cash": "400.00",
        "as_of": SESSIONS[-1],
    }
    assert all(row["action"] == "BLOCKED" for row in result["curve"])
    assert all(row["commission"] == "0.00" for row in result["curve"])
    assert all(row["reason"] == "insufficient_lot_budget" for row in result["curve"])


def test_zero_allocation_is_cash_only(tmp_path):
    result = _run(_payload(allocation=0), tmp_path)
    assert result["trades"] == []
    assert result["target_holdings"] == []
    assert result["final_account"]["positions"] == {}
    assert result["final_account"]["cash"] == "100000.00"
    assert all(row["action"] == "HOLD_CASH" for row in result["curve"])
    assert all(row["valuation_status"] == "CASH" for row in result["curve"])


def test_missing_execution_close_does_not_block_buy_and_is_stale_mark(tmp_path):
    payload = _payload()
    payload["quotes"][1]["close"] = None
    payload["quotes"][2]["close"] = None
    result = _run(payload, tmp_path)
    trade = result["trades"][0]
    assert trade["quantity"] == 24_900
    assert trade["cash_after"] == "375.10"
    rows = _rows_by_date(result)
    assert rows[SESSIONS[1]]["action"] == "BUY"
    assert rows[SESSIONS[1]]["mark_price"] == "4"
    assert rows[SESSIONS[1]]["mark_as_of"] == SESSIONS[1]
    assert rows[SESSIONS[1]]["mark_source"] == "OPEN"
    assert rows[SESSIONS[1]]["valuation_status"] == "STALE"
    assert rows[SESSIONS[2]]["mark_price"] == "4"
    assert rows[SESSIONS[2]]["mark_as_of"] == SESSIONS[1]
    assert rows[SESSIONS[2]]["mark_source"] == "OPEN"
    assert rows[SESSIONS[2]]["valuation_status"] == "STALE"


def test_missing_quote_is_explicitly_blocked_then_retries_after_valid_predecessor(
    tmp_path,
):
    payload = _payload(
        quotes=[
            _quote(SESSIONS[0], close="4.000"),
            _quote(SESSIONS[2], opening="4.010", close="4.020"),
            _quote(SESSIONS[3], opening="4.020", close="4.030"),
        ]
    )
    result = _run(payload, tmp_path)
    rows = _rows_by_date(result)
    assert rows[SESSIONS[1]]["action"] == "BLOCKED"
    assert rows[SESSIONS[1]]["reason"]
    assert rows[SESSIONS[2]]["action"] == "BLOCKED"
    assert rows[SESSIONS[3]]["action"] == "BUY"
    assert result["trades"][0]["date"] == SESSIONS[3]
    assert result["trades"][0]["decision_date"] == SESSIONS[2]


@pytest.mark.parametrize("limit_mode", ["at_open", "unknown"])
def test_upper_limit_block_is_explicit_and_retries_next_session(tmp_path, limit_mode):
    payload = _payload()
    if limit_mode == "at_open":
        payload["quotes"][1]["upper_limit"] = "4.000"
    else:
        payload["quotes"][1].pop("upper_limit")
    result = _run(payload, tmp_path)
    rows = _rows_by_date(result)
    assert rows[SESSIONS[1]]["action"] == "BLOCKED"
    assert result["trades"][0]["date"] == SESSIONS[2]
    assert rows[SESSIONS[2]]["action"] == "BUY"


def test_slippage_above_upper_limit_blocks_then_retries(tmp_path):
    payload = _payload(
        fees={
            "commission_rate": "0.00025",
            "minimum_commission": "5",
            "slippage_rate": "0.01",
        }
    )
    payload["quotes"][1]["upper_limit"] = "4.030"
    result = _run(payload, tmp_path)
    rows = _rows_by_date(result)
    assert rows[SESSIONS[1]]["action"] == "BLOCKED"
    assert rows[SESSIONS[1]]["reason"] == "slippage_exceeds_upper_limit"
    assert result["trades"][0]["date"] == SESSIONS[2]


def test_non_tradable_session_blocks_and_later_session_can_retry(tmp_path):
    payload = _payload()
    payload["quotes"][1]["trade_status"] = 0
    result = _run(payload, tmp_path)
    rows = _rows_by_date(result)
    assert rows[SESSIONS[1]]["action"] == "BLOCKED"
    assert rows[SESSIONS[2]]["action"] == "BLOCKED"
    assert rows[SESSIONS[3]]["action"] == "BUY"
    assert result["trades"][0]["decision_date"] == SESSIONS[2]


def test_same_day_buy_is_not_sellable_until_next_supplied_session(tmp_path):
    result = _run(_payload(), tmp_path)
    rows = _rows_by_date(result)
    assert rows[SESSIONS[1]]["sellable_quantity"] == 0
    assert rows[SESSIONS[2]]["sellable_quantity"] == 24_900
    assert rows[SESSIONS[3]]["sellable_quantity"] == 24_900


def test_non_tradable_close_reuses_last_mark_as_stale(tmp_path):
    payload = _payload()
    payload["quotes"][2]["trade_status"] = 0
    payload["quotes"][2]["close"] = "4.300"
    result = _run(payload, tmp_path)
    row = _rows_by_date(result)[SESSIONS[2]]
    assert row["mark_price"] == "4.1"
    assert row["mark_as_of"] == SESSIONS[1]
    assert row["mark_source"] == "CLOSE"
    assert row["valuation_status"] == "STALE"


def test_independent_cash_units_reconstruction_and_reconcile(tmp_path):
    result = _run(_payload(), tmp_path)
    cash = Decimal("100000.00")
    units = 0
    for trade in result["trades"]:
        assert trade["side"] == "BUY"
        units += trade["quantity"]
        cash -= Decimal(trade["notional"]) + Decimal(trade["commission"])
    final = result["final_account"]
    assert units == final["positions"][CODE]
    assert cash == Decimal(final["cash"])
    assert cash == Decimal(final["planned_cash"])

    # The legacy reconciler takes numeric cash values; preserve its exact
    # account/quantity contract while converting only the cent-string fields.
    account_for_reconcile = {
        **final,
        "cash": float(final["cash"]),
        "planned_cash": float(final["planned_cash"]),
    }
    reconciled = reconcile_plan_vs_account(
        result["target_holdings"], account_for_reconcile
    )
    assert reconciled["ok"] is True
    assert reconciled["breaks"] == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update(price_basis="hfq"),
        lambda p: p.update(corporate_actions=[{"date": SESSIONS[1]}]),
        lambda p: p.update(schema_version="other-v1"),
        lambda p: p.update(unexpected=True),
        lambda p: p["fees"].update(stamp_duty_rate="0.001"),
        lambda p: p["quotes"][0].update(unexpected=True),
        lambda p: p.pop("corporate_actions"),
        lambda p: p.pop("allocation"),
    ],
)
def test_contract_rejects_adjusted_actions_unknown_or_missing_fields(tmp_path, mutate):
    payload = _payload()
    mutate(payload)
    with pytest.raises(ValueError):
        _run(payload, tmp_path)


@pytest.mark.parametrize(
    ("where", "value"),
    [
        ("allocation", True),
        ("allocation", float("nan")),
        ("initial_cash", float("inf")),
        ("commission_rate", True),
        ("commission_rate", float("nan")),
        ("slippage_rate", float("inf")),
        ("open", False),
        ("close", float("nan")),
        ("trade_status", 1.5),
    ],
)
def test_contract_rejects_non_finite_or_boolean_numeric_values(tmp_path, where, value):
    payload = _payload()
    if where in {"allocation", "initial_cash"}:
        payload[where] = value
    elif where in {"commission_rate", "slippage_rate"}:
        payload["fees"][where] = value
    else:
        payload["quotes"][0][where] = value
    with pytest.raises(ValueError):
        _run(payload, tmp_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("allocation", "0." + "9" * 45),
        ("slippage_rate", "1e-45"),
    ],
)
def test_contract_rejects_more_than_twelve_decimal_places(tmp_path, field, value):
    payload = _payload()
    if field == "allocation":
        payload["allocation"] = value
    else:
        payload["fees"][field] = value
    with pytest.raises(ValueError, match="decimal places"):
        _run(payload, tmp_path)


def test_twelve_decimal_place_ratios_are_accepted(tmp_path):
    result = _run(
        _payload(
            allocation="0.123456789012",
            fees={"slippage_rate": "0.000000000001"},
        ),
        tmp_path,
    )
    assert result["evidence_kind"] == "REPLAY"


def test_duplicate_or_unsorted_calendar_and_quotes_are_rejected(tmp_path):
    cases = []
    duplicate_sessions = _payload()
    duplicate_sessions["sessions"] = [SESSIONS[0], SESSIONS[1], SESSIONS[1]]
    cases.append(duplicate_sessions)

    unsorted_sessions = _payload()
    unsorted_sessions["sessions"] = [SESSIONS[1], SESSIONS[0], *SESSIONS[2:]]
    unsorted_sessions["decision_date"] = SESSIONS[1]
    cases.append(unsorted_sessions)

    duplicate_quotes = _payload()
    duplicate_quotes["quotes"].append(deepcopy(duplicate_quotes["quotes"][0]))
    cases.append(duplicate_quotes)

    unsorted_quotes = _payload()
    unsorted_quotes["quotes"] = [
        unsorted_quotes["quotes"][1],
        unsorted_quotes["quotes"][0],
        *unsorted_quotes["quotes"][2:],
    ]
    cases.append(unsorted_quotes)

    for payload in cases:
        with pytest.raises(ValueError):
            _run(payload, tmp_path)


def test_quote_outside_authoritative_calendar_is_rejected(tmp_path):
    payload = _payload()
    payload["quotes"].append(_quote("2026-01-08"))
    with pytest.raises(ValueError):
        _run(payload, tmp_path)


def test_halt_file_is_checked_explicitly_and_raises_halt_error(tmp_path):
    halt_path = tmp_path / "strategy-halt.json"
    engage_halt(
        changed_by="test-operator",
        reason="stop replay",
        halt_path=str(halt_path),
    )
    with pytest.raises(HaltError, match="HALTED"):
        replay_etf_benchmark(_payload(), halt_path=str(halt_path))
