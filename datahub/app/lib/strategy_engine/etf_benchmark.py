"""Offline, cent-exact single domestic equity ETF buy-and-hold replay.

Inputs are explicit operator declarations, not certified market data. This
module neither mutates an account nor produces forward evidence/broker orders.
"""

from __future__ import annotations

import argparse
import datetime
from decimal import (
    Context,
    Decimal,
    InvalidOperation,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_HALF_UP,
    localcontext,
)
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

from app.lib.strategy_engine.halt import (
    HaltError,
    assert_not_halted,
    resolve_halt_path,
)

VERSION = "etf-benchmark-v1"
CENT = Decimal("0.01")
LOT = 100


def _object(value, name, allowed, required=()):
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    if set(value) - set(allowed):
        raise ValueError(f"unknown {name} fields: {sorted(set(value) - set(allowed))}")
    if set(required) - set(value):
        raise ValueError(f"missing {name} fields: {sorted(set(required) - set(value))}")
    return value


def _number(value, name, *, positive=False, money=False, places=None):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{name} must be a finite number")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"{name} must be a finite number") from None
    if not result.is_finite() or result < 0 or result > Decimal("1e12"):
        raise ValueError(f"{name} must be finite, non-negative and at most 1e12")
    if positive and result <= 0:
        raise ValueError(f"{name} must be positive")
    if places is not None and result.as_tuple().exponent < -places:
        raise ValueError(f"{name} must have at most {places} decimal places")
    if money and result != result.quantize(CENT):
        raise ValueError(f"{name} must be an exact cent amount")
    return result


def _day(value, name):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be YYYY-MM-DD")
    try:
        parsed = datetime.date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{name} must be YYYY-MM-DD") from None
    if parsed.isoformat() != value:
        raise ValueError(f"{name} must be YYYY-MM-DD")
    return value


def _text(number):
    return format(number.normalize(), "f")


def _money(number):
    return format(number.quantize(CENT, rounding=ROUND_HALF_UP), ".2f")


def _hash(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def _validate(payload):
    fields = {
        "schema_version",
        "price_basis",
        "instrument",
        "initial_cash",
        "allocation",
        "fees",
        "decision_date",
        "sessions",
        "quotes",
        "corporate_actions",
    }
    _object(payload, "input", fields, fields - {"initial_cash"})
    if payload["schema_version"] != VERSION or payload["price_basis"] != "raw":
        raise ValueError(f"schema_version must be {VERSION}; price_basis must be raw")
    actions = payload["corporate_actions"]
    if not isinstance(actions, list) or actions:
        raise ValueError(
            "corporate_actions must explicitly be []; actions are not supported"
        )
    inst = _object(
        payload["instrument"],
        "instrument",
        {"code", "type", "tick_size"},
        {"code", "type", "tick_size"},
    )
    if inst["type"] != "domestic_equity_etf":
        raise ValueError("instrument.type must be domestic_equity_etf")
    if not isinstance(inst["code"], str) or not re.fullmatch(
        r"(?:sh5|sz1)\d{5}", inst["code"]
    ):
        raise ValueError(
            "instrument.code must be a normalised sh5xxxxx or sz1xxxxx code"
        )
    tick = _number(inst["tick_size"], "tick_size", positive=True)
    if tick > 1 or tick.normalize().as_tuple().exponent < -4:
        raise ValueError("tick_size must be <=1 and use at most four decimal places")
    cash = _number(
        payload.get("initial_cash", "100000.00"),
        "initial_cash",
        positive=True,
        money=True,
    )
    weight = _number(payload["allocation"], "allocation", places=12)
    if weight > 1:
        raise ValueError("allocation must be in [0,1]")
    fee = _object(
        payload["fees"],
        "fees",
        {"commission_rate", "minimum_commission", "slippage_rate"},
        {"commission_rate", "minimum_commission", "slippage_rate"},
    )
    rate = _number(fee["commission_rate"], "commission_rate", places=12)
    minimum = _number(fee["minimum_commission"], "minimum_commission", money=True)
    slip = _number(fee["slippage_rate"], "slippage_rate", places=12)
    if rate > 1 or slip >= 1:
        raise ValueError("commission_rate must be <=1; slippage_rate must be <1")
    dates = payload["sessions"]
    if not isinstance(dates, list) or len(dates) < 2:
        raise ValueError(
            "sessions must contain a decision session and execution session"
        )
    dates = [_day(d, "session") for d in dates]
    if dates != sorted(set(dates)):
        raise ValueError("sessions must be strictly increasing and unique")
    if _day(payload["decision_date"], "decision_date") != dates[0]:
        raise ValueError("decision_date must be the first session")
    quotes = payload["quotes"]
    if not isinstance(quotes, list):
        raise ValueError("quotes must be a list")
    calendar = set(dates)
    normal_quotes = []
    for row in quotes:
        _object(
            row,
            "quote",
            {"date", "open", "close", "trade_status", "upper_limit"},
            {"date"},
        )
        date = _day(row["date"], "quote.date")
        if date not in calendar:
            raise ValueError("quote date is outside sessions")
        clean = {"date": date}
        status = row.get("trade_status")
        if status is not None and (type(status) is not int or status not in (0, 1)):
            raise ValueError("trade_status must be integer 0 or 1, or null")
        clean["trade_status"] = status
        for key in ("open", "close", "upper_limit"):
            value = row.get(key)
            if value is None:
                clean[key] = None
                continue
            number = _number(value, f"quote.{key}", positive=True)
            if number % tick != 0:
                raise ValueError(f"quote.{key} must be a multiple of tick_size")
            clean[key] = _text(number)
        normal_quotes.append(clean)
    quote_dates = [row["date"] for row in normal_quotes]
    if quote_dates != sorted(set(quote_dates)):
        raise ValueError("quote dates must be strictly increasing and unique")
    config = {
        "schema_version": VERSION,
        "price_basis": "raw",
        "instrument": {
            "code": inst["code"],
            "type": inst["type"],
            "tick_size": _text(tick),
        },
        "initial_cash": _money(cash),
        "allocation": _text(weight),
        "fees": {
            "commission_rate": _text(rate),
            "minimum_commission": _money(minimum),
            "slippage_rate": _text(slip),
        },
        "corporate_actions": [],
    }
    normal = dict(config, decision_date=dates[0], sessions=dates, quotes=normal_quotes)
    return normal, config


def _commission(notional, fees):
    return max(
        notional * Decimal(fees["commission_rate"]), Decimal(fees["minimum_commission"])
    ).quantize(CENT, rounding=ROUND_HALF_UP)


def _quantity(budget, price, fees):
    lo, hi = 0, int(budget / (price * LOT))
    while lo < hi:
        mid = (lo + hi + 1) // 2
        value = price * mid * LOT
        if value + _commission(value, fees) <= budget:
            lo = mid
        else:
            hi = mid - 1
    return lo * LOT


def replay_etf_benchmark(payload, *, halt_path):
    """Return a deterministic REPLAY; no persistent account is updated."""
    assert_not_halted(halt_path)
    # Account results must not depend on another caller's precision/rounding.
    with localcontext(Context(prec=40, rounding=ROUND_HALF_UP)):
        return _replay(payload, halt_path=halt_path)


def _replay(payload, *, halt_path):
    normal, config = _validate(payload)
    config_hash = _hash(config)
    code = config["instrument"]["code"]
    tick = Decimal(config["instrument"]["tick_size"])
    fees = config["fees"]
    cash = initial = Decimal(config["initial_cash"])
    budget = (initial * Decimal(config["allocation"])).quantize(
        CENT, rounding=ROUND_DOWN
    )
    dates = normal["sessions"]
    quotes = {q["date"]: q for q in normal["quotes"]}
    quantity = 0
    bought_on = None
    mark = mark_date = mark_source = None
    trades, curve = [], []
    for previous, date in zip(dates, dates[1:]):
        assert_not_halted(halt_path)
        prior, current = quotes.get(previous, {}), quotes.get(date, {})
        charge = Decimal(0)
        action, reason = "HOLD", "initial_purchase_already_completed"
        if not quantity:
            if not budget:
                action, reason = "HOLD_CASH", "zero_allocation"
            else:
                action = "BLOCKED"
                if prior.get("trade_status") != 1 or prior.get("close") is None:
                    reason = "previous_close_unavailable"
                elif current.get("trade_status") != 1 or current.get("open") is None:
                    reason = "execution_open_unavailable"
                elif current.get("upper_limit") is None:
                    reason = "upper_limit_unavailable"
                else:
                    opening = Decimal(current["open"])
                    limit = Decimal(current["upper_limit"])
                    price = (
                        (opening * (1 + Decimal(fees["slippage_rate"]))) / tick
                    ).to_integral_value(rounding=ROUND_CEILING) * tick
                    if opening >= limit:
                        reason = "limit_up_open"
                    elif price > limit:
                        reason = "slippage_exceeds_upper_limit"
                    else:
                        quantity = _quantity(budget, price, fees)
                        if not quantity:
                            reason = "insufficient_lot_budget"
                        else:
                            value = price * quantity
                            charge = _commission(value, fees)
                            cash -= value + charge
                            assert cash >= 0
                            bought_on = date
                            mark, mark_date, mark_source = opening, date, "OPEN"
                            action, reason = "BUY", "initial_buy_and_hold"
                            trades.append(
                                {
                                    "trade_id": _hash(
                                        {
                                            "config_hash": config_hash,
                                            "date": date,
                                            "side": "BUY",
                                        }
                                    ),
                                    "date": date,
                                    "decision_date": previous,
                                    "stock_code": code,
                                    "side": "BUY",
                                    "quantity": quantity,
                                    "price": _text(price),
                                    "notional": _money(value),
                                    "commission": _money(charge),
                                    "cash_after": _money(cash),
                                }
                            )
        freshness = "CASH"
        if quantity:
            freshness = "STALE"
            if current.get("trade_status") == 1 and current.get("close") is not None:
                mark, mark_date, mark_source = Decimal(current["close"]), date, "CLOSE"
                freshness = "OK"
        nav = cash + (quantity * mark if quantity else Decimal(0))
        curve.append(
            {
                "date": date,
                "decision_date": previous,
                "action": action,
                "reason": reason,
                "cash": _money(cash),
                "quantity": quantity,
                "sellable_quantity": quantity if bought_on and bought_on < date else 0,
                "commission": _money(charge),
                "nav": _money(nav),
                "mark_price": _text(mark) if mark is not None else None,
                "mark_as_of": mark_date,
                "mark_source": mark_source,
                "valuation_status": freshness,
            }
        )
    holdings = [{"stock_code": code, "quantity": quantity}] if quantity else []
    final = {
        "positions": {code: quantity} if quantity else {},
        "cash": _money(cash),
        "planned_cash": _money(cash),
        "as_of": dates[-1],
    }
    return {
        "schema_version": VERSION,
        "evidence_kind": "REPLAY",
        "price_basis": "raw",
        "instrument": config["instrument"],
        "config": config,
        "config_hash": config_hash,
        "input_hash": _hash(normal),
        "initial_cash": _money(initial),
        "budget": _money(budget),
        "trades": trades,
        "curve": curve,
        "final_account": final,
        "target_holdings": holdings,
    }


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Frozen raw-price JSON input")
    parser.add_argument("--output", help="Atomic JSON result; defaults to stdout")
    parser.add_argument("--halt-file", help="Existing strategy halt store path")
    return parser


def _aliases(first, second):
    a, b = Path(first), Path(second)
    return a.resolve() == b.resolve() or (
        a.exists() and b.exists() and os.path.samefile(a, b)
    )


def _write_result(path, text):
    target = Path(path)
    handle = tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=target.parent, delete=False
    )
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(handle.name, target)
    finally:
        if os.path.exists(handle.name):
            os.unlink(handle.name)


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        halt_path = resolve_halt_path(args.halt_file)
        assert_not_halted(halt_path)
        if args.output and (
            _aliases(args.input, args.output) or _aliases(halt_path, args.output)
        ):
            raise ValueError("output must not alias input or halt file")

        def reject_constant(value):
            raise ValueError(f"non-finite JSON constant: {value}")

        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f"duplicate JSON key: {key}")
                result[key] = value
            return result

        with open(args.input, encoding="utf-8") as handle:
            payload = json.load(
                handle, parse_constant=reject_constant, object_pairs_hook=unique_object
            )
        result = replay_etf_benchmark(payload, halt_path=halt_path)
        text = (
            json.dumps(
                result, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False
            )
            + "\n"
        )
        assert_not_halted(halt_path)
        if args.output:
            _write_result(args.output, text)
        else:
            print(text, end="")
        return 0
    except HaltError as exc:
        print(json.dumps({"status": "HALTED", "error": str(exc)}), file=sys.stderr)
        return 2
    except (ValueError, OSError, InvalidOperation) as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
