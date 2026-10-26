"""Independent, read-only accounting verification of a Research simulation.

This only verifies that a provided event-sourced Research simulation is
internally consistent. It does NOT prove price-source PIT completeness, actual
broker fills, dividends coverage, or that the derivation is FORWARD evidence.
No Mongo access, ledger mutations, or Trading account adapters are involved.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys

from app.lib.strategy_engine.simulation_ledger import ResearchSimulationLedger

TOLERANCE_CNY = 0.01


def _number(value, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    number = float(value)
    if not math.isfinite(number) or (number <= 0 if positive else number < 0):
        raise ValueError(f"{label} must be a finite non-negative number")
    return number


def _equal_money(value, expected, label: str) -> None:
    a = _number_signed(value)
    b = _number_signed(expected)
    if not math.isclose(a, b, rel_tol=0, abs_tol=TOLERANCE_CNY):
        raise ValueError(f"{label} does not reconcile with replayed ledger")


def _mark_date(event: dict) -> str | None:
    if event.get("type") != "MARK":
        return None
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.startswith("mark:"):
        raise ValueError("research daily valuation needs dated MARK event IDs")
    date = event_id[5:]
    try:
        if dt.date.fromisoformat(date).isoformat() != date:
            raise ValueError("noncanonical")
    except ValueError:
        raise ValueError("MARK event_id lacks ISO valuation date") from None
    return date


def audit_simulation_account(result: dict, *, expected_initial_nav: float) -> dict:
    """Replay FILL/MARK/events, reconcile cash, holdings, receivables and NAV.

    Each reported daily curve point requires a matching MARK event, including
    valuation-only days. No missing mark or stale/unknown valuation is silently
    set to zero; if a held name is marked stale, the audit stays explicitly
    unverified for fresh-price certification even if accounting balances.
    """
    if not isinstance(result, dict):
        raise ValueError("simulation result must be a mapping")
    initial = _number(expected_initial_nav, "expected_initial_nav", positive=True)
    _equal_money(result.get("initial_nav"), initial, "initial_nav")
    events = result.get("ledger_events")
    curve = result.get("curve")
    snapshot = result.get("ledger_snapshot")
    parity = result.get("nav_parity")
    if not isinstance(events, list) or not events:
        raise ValueError("simulation ledger_events are required")
    if not isinstance(curve, list) or not curve:
        raise ValueError("simulation daily curve is required")
    if not isinstance(snapshot, dict):
        raise ValueError("simulation ledger_snapshot is required")
    if not isinstance(parity, dict):
        raise ValueError("simulation nav_parity is required")

    expected_points = {}
    previous = None
    for point in curve:
        if not isinstance(point, dict):
            raise ValueError("curve points must be mappings")
        date = point.get("date")
        if not isinstance(date, str):
            raise ValueError("valuation date is missing")
        try:
            if dt.date.fromisoformat(date).isoformat() != date:
                raise ValueError("noncanonical")
        except ValueError:
            raise ValueError("valuation dates must be canonical ISO dates") from None
        if date in expected_points or (previous is not None and date <= previous):
            raise ValueError("valuation dates must be unique and increasing")
        expected_points[date] = _number(point.get("nav"), f"curve NAV {date}")
        previous = date

    ledger = ResearchSimulationLedger(initial_cash=initial)
    seen_marks = {}
    fills = 0
    rejections = 0
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("ledger event must be a mapping")
        if event.get("type") == "FILL":
            fills += 1
        if event.get("type") == "REJECTED":
            rejections += 1
        mark_date = _mark_date(event)
        # The ledger owns idempotence and valid fill/receivable semantics.
        if not ledger.apply(event):
            raise ValueError("duplicate ledger event is not acceptable for audit")
        if mark_date is not None:
            if mark_date in seen_marks:
                raise ValueError("duplicate daily MARK")
            seen_marks[mark_date] = ledger.nav
            if mark_date not in expected_points:
                raise ValueError("unreported daily MARK")
            _equal_money(
                expected_points[mark_date], ledger.nav, f"daily NAV {mark_date}"
            )
    if set(seen_marks) != set(expected_points):
        raise ValueError("missing daily MARK event for a valuation curve date")

    projection = ledger.snapshot()
    if snapshot.get("event_count") != len(events):
        raise ValueError("ledger event_count disagrees with the journal")
    for key in ("initial_cash", "cash", "receivable_balance", "realized_pnl", "nav"):
        _equal_money(snapshot.get(key), projection[key], f"ledger {key}")
    _equal_money(result.get("terminal_nav"), projection["nav"], "terminal_nav")
    _equal_money(expected_points[previous], projection["nav"], "final curve NAV")
    for group in ("positions", "receivables"):
        stored = snapshot.get(group)
        expected = projection[group]
        if not isinstance(stored, dict) or set(stored) != set(expected):
            raise ValueError(f"ledger {group} identities differ from replay")
        for code, row in stored.items():
            if not isinstance(row, dict) or set(row) != set(expected[code]):
                raise ValueError(f"ledger {group}[{code}] fields differ")
            for field, value in expected[code].items():
                item = row[field]
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    _equal_money(item, value, f"ledger {group}[{code}].{field}")
                elif item != value or type(item) is not type(value):
                    raise ValueError(f"ledger {group}[{code}].{field} differs")

    check = (
        projection["cash"]
        + projection["receivable_balance"]
        + sum(p["quantity"] * p["last_price"] for p in projection["positions"].values())
    )
    _equal_money(check, projection["nav"], "cash plus marked holdings and receivables")
    points = parity.get("points")
    if not isinstance(points, list) or len(points) != len(curve):
        raise ValueError("NAV parity points must cover every valuation day")
    if {p.get("date") for p in points if isinstance(p, dict)} != set(expected_points):
        raise ValueError("NAV parity dates disagree with curve")
    max_delta = 0.0
    for point in points:
        if not isinstance(point, dict):
            raise ValueError("invalid NAV parity point")
        date = point["date"]
        _equal_money(
            point.get("ledger_nav"), expected_points[date], "parity ledger NAV"
        )
        _equal_money(
            point.get("legacy_nav"), expected_points[date], "parity legacy NAV"
        )
        delta = abs(_number_signed(point.get("delta")))
        if delta > TOLERANCE_CNY:
            raise ValueError(f"NAV parity drift exceeds tolerance for {date}")
        max_delta = max(max_delta, delta)
    _equal_money(parity.get("max_abs_delta_cny"), max_delta, "parity max delta")
    stale_count = sum(bool(p["stale"]) for p in projection["positions"].values())
    return {
        "status": "ACCOUNTING_RECONCILED_RESEARCH_ONLY",
        "source_certification": "UNVERIFIED",
        "forward_certification": "NOT_EVALUATED",
        "broker_execution": False,
        "initial_nav_cny": round(initial, 2),
        "terminal_nav_cny": round(projection["nav"], 2),
        "cash_cny": round(projection["cash"], 2),
        "receivables_cny": round(projection["receivable_balance"], 2),
        "positions_count": len(projection["positions"]),
        "valuation_sessions": len(curve),
        "fills_count": fills,
        "rejections_count": rejections,
        "stale_positions_count": stale_count,
        "fresh_price_certification": "UNVERIFIED" if stale_count else "NOT_EVALUATED",
        "nav_parity_max_delta_cny": round(max_delta, 6),
    }


def _number_signed(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("parity delta must be a signed finite number")
    v = float(value)
    if not math.isfinite(v):
        raise ValueError("parity delta must be finite")
    return v


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--initial-nav", type=float, required=True)
    parser.add_argument("--input", default="-", help="JSON file or - for stdin")
    args = parser.parse_args(argv)
    with (
        sys.stdin if args.input == "-" else open(args.input, encoding="utf-8")
    ) as stream:
        result = json.load(stream)
    report = audit_simulation_account(result, expected_initial_nav=args.initial_nav)
    print(json.dumps(report, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
