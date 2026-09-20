# -*- coding: utf-8 -*-
"""Planned-vs-actual reconciliation for the paper/live loop (read-only).

``reconcile_plan_vs_account`` diffs the last planned target portfolio against an
operator-supplied account snapshot (positions + cash) and returns a structured,
JSON-serialisable result:

* ``quantity_drift`` per name (planned, actual, drift, tolerance, breach);
* ``cash_drift`` planned vs actual drift;
* ``missing_in_account`` (planned but not held) and
  ``unexpected_in_account`` (held but not planned);
* ``breaks`` — one entry per tolerance breach, so the caller can fail loudly.

The module never writes, never calls a broker and never mutates the plan; the
CLI entry point (``python -m app.lib.strategy_engine.reconcile``) is the thin
operator surface. A tolerance breach exits non-zero.
"""

from __future__ import annotations

import argparse
import json
import math
import sys

#: Default tolerances: exact match. Drift is measured in shares and CNY, so any
#: non-zero difference is a break unless the operator explicitly widens it.
DEFAULT_TOLERANCES = {
    "quantity_abs": 0.0,
    "cash_abs": 0.0,
}


def _position_rows(snapshot) -> list[dict]:
    """Normalize the snapshot's positions to a list of {stock_code, quantity}."""
    if snapshot is None:
        return []
    positions = snapshot.get("positions") if isinstance(snapshot, dict) else snapshot
    if positions is None:
        return []
    if isinstance(positions, dict):
        items = positions.items()
        rows = []
        for code, value in items:
            if isinstance(value, dict):
                quantity = value.get("quantity", value.get("qty", value.get("shares")))
            else:
                quantity = value
            rows.append({"stock_code": code, "quantity": quantity})
        return rows
    rows = []
    for entry in positions:
        if not isinstance(entry, dict):
            raise ValueError("positions entries must be objects")
        code = entry.get("stock_code") or entry.get("code")
        if not code:
            raise ValueError("positions entries need a stock_code")
        rows.append(
            {
                "stock_code": code,
                "quantity": entry.get(
                    "quantity", entry.get("qty", entry.get("shares"))
                ),
            }
        )
    return rows


def _as_float(value, label: str) -> float:
    """A finite number, or a loud error.

    NaN/inf are rejected explicitly: `NaN` is valid JSON for Python's
    ``json.load``, and every comparison against NaN is False, so a non-finite
    tolerance or quantity would silently disable the very break it configures
    (the same defect class `constraints.min_trade_amount_cny` already guards).
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a number, got {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label} must be a finite number, got {value!r}")
    return number


def _planned_cash(target_holdings, snapshot) -> float | None:
    """Planned cash: explicit snapshot field when present, else 0 when the plan
    is fully invested. Returns None when it cannot be derived."""
    planned = snapshot.get("planned_cash") if isinstance(snapshot, dict) else None
    if planned is not None:
        return _as_float(planned, "planned_cash")
    if not target_holdings:
        return 0.0
    return None


def _as_of_text(value) -> str | None:
    """Normalise ``as_of`` to an ISO string so the result stays JSON-serialisable
    even when a caller passes a datetime/date rather than a string."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def reconcile_plan_vs_account(
    target_holdings,
    account_snapshot,
    *,
    tolerances: dict | None = None,
) -> dict:
    """Diff a planned target portfolio against an account snapshot.

    target_holdings: list of {"stock_code", "quantity"?} (the plan's target
    quantities; names without a quantity are reported as missing_in_account and
    cannot be drift-checked).
    account_snapshot: {"positions": {code: qty} | [{stock_code, quantity}],
    "cash"?: number, "planned_cash"?: number, "as_of"?: str}.

    Returns a JSON-serialisable dict. ``breaks`` is non-empty when any
    quantity/cash drift exceeds its tolerance, or when the account holds an
    unplanned name; the caller treats a non-empty list as a hard failure.
    """
    effective = dict(DEFAULT_TOLERANCES)
    effective.update(tolerances or {})
    quantity_tolerance = _as_float(effective["quantity_abs"], "tolerances.quantity_abs")
    cash_tolerance = _as_float(effective["cash_abs"], "tolerances.cash_abs")
    if quantity_tolerance < 0 or cash_tolerance < 0:
        raise ValueError("tolerances must be non-negative")
    if not isinstance(account_snapshot, dict):
        raise ValueError("account_snapshot must be an object")

    planned: dict[str, float] = {}
    planned_without_quantity: list[str] = []
    for holding in target_holdings or []:
        code = holding.get("stock_code")
        if not code:
            raise ValueError("target_holdings entries need a stock_code")
        if holding.get("quantity") is None:
            planned_without_quantity.append(code)
            continue
        planned[code] = _as_float(holding["quantity"], f"planned quantity for {code}")

    actual: dict[str, float] = {}
    for row in _position_rows(account_snapshot):
        code = row["stock_code"]
        if row["quantity"] is None:
            raise ValueError(f"account position {code!r} has no quantity")
        actual[code] = _as_float(row["quantity"], f"account quantity for {code}")

    breaks: list[dict] = []
    quantity_drift = []
    for code in sorted(set(planned) | set(actual)):
        if code not in actual:
            breaks.append(
                {
                    "kind": "missing_in_account",
                    "stock_code": code,
                    "planned": planned[code],
                    "actual": None,
                }
            )
            continue
        if code not in planned:
            # An unplanned holding is a break regardless of tolerance: the
            # account is not what the plan says it should be.
            breaks.append(
                {
                    "kind": "unexpected_in_account",
                    "stock_code": code,
                    "planned": None,
                    "actual": actual[code],
                }
            )
            continue
        drift = actual[code] - planned[code]
        breach = abs(drift) > quantity_tolerance
        quantity_drift.append(
            {
                "stock_code": code,
                "planned": planned[code],
                "actual": actual[code],
                "drift": round(drift, 6),
                "tolerance": quantity_tolerance,
                "breach": breach,
            }
        )
        if breach:
            breaks.append(
                {
                    "kind": "quantity_drift",
                    "stock_code": code,
                    "planned": planned[code],
                    "actual": actual[code],
                    "drift": round(drift, 6),
                    "tolerance": quantity_tolerance,
                }
            )

    cash_actual = account_snapshot.get("cash")
    cash_planned = _planned_cash(target_holdings, account_snapshot)
    cash_drift = None
    if cash_actual is None:
        # A snapshot without a cash balance cannot be reconciled: an absent
        # figure is not "cash matches". Fail loudly (the spec's MUST), and say
        # which side is missing.
        breaks.append(
            {
                "kind": "cash_drift_unverifiable",
                "planned": cash_planned,
                "actual": None,
                "reason": "account_snapshot.cash is missing",
            }
        )
    else:
        cash_actual = _as_float(cash_actual, "account_snapshot.cash")
        if cash_planned is None:
            breaks.append(
                {
                    "kind": "cash_drift_unverifiable",
                    "planned": None,
                    "actual": cash_actual,
                    "reason": "planned cash is not derivable from the plan",
                }
            )
        else:
            cash_drift = cash_actual - cash_planned
            if abs(cash_drift) > cash_tolerance:
                breaks.append(
                    {
                        "kind": "cash_drift",
                        "planned": cash_planned,
                        "actual": cash_actual,
                        "drift": round(cash_drift, 6),
                        "tolerance": cash_tolerance,
                    }
                )

    return {
        "as_of": _as_of_text(account_snapshot.get("as_of")),
        "planned_names": sorted(planned),
        "actual_names": sorted(actual),
        "planned_without_quantity": sorted(planned_without_quantity),
        "quantity_drift": quantity_drift,
        "cash_drift": {
            "planned": cash_planned,
            "actual": cash_actual,
            "drift": round(cash_drift, 6) if cash_drift is not None else None,
            "tolerance": cash_tolerance,
            "breach": bool(cash_drift is not None and abs(cash_drift) > cash_tolerance),
        },
        "missing_in_account": sorted(set(planned) - set(actual)),
        "unexpected_in_account": sorted(set(actual) - set(planned)),
        "tolerances": {
            "quantity_abs": quantity_tolerance,
            "cash_abs": cash_tolerance,
        },
        "breaks": breaks,
        "ok": not breaks,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Diff a planned target portfolio against an account snapshot "
            "(read-only; exits non-zero on a tolerance breach)"
        )
    )
    parser.add_argument(
        "--plan",
        required=True,
        help="Planned target holdings JSON: a list of {stock_code, quantity?}",
    )
    parser.add_argument(
        "--account",
        required=True,
        help='Account snapshot JSON: {"positions": {...}, "cash": ..., ...}',
    )
    parser.add_argument("--quantity-tolerance", type=float, default=0.0)
    parser.add_argument("--cash-tolerance", type=float, default=0.0)
    parser.add_argument("--output", default=None)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    with open(args.plan, encoding="utf-8") as handle:
        target_holdings = json.load(handle)
    with open(args.account, encoding="utf-8") as handle:
        account_snapshot = json.load(handle)
    try:
        result = reconcile_plan_vs_account(
            target_holdings,
            account_snapshot,
            tolerances={
                "quantity_abs": args.quantity_tolerance,
                "cash_abs": args.cash_tolerance,
            },
        )
    except ValueError as exc:
        # Invalid input (non-finite quantity/tolerance, missing quantity, ...)
        # is a loud error with a non-zero exit, not a traceback and never a
        # silent success.
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1) from None
    text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
    else:
        sys.stdout.write(text + "\n")
    if result["breaks"]:
        print(
            json.dumps(
                {
                    "error": "reconciliation tolerance breach",
                    "breaks": result["breaks"],
                },
                ensure_ascii=False,
                default=str,
            ),
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
