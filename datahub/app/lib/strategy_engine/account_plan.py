# -*- coding: utf-8 -*-
"""Read-only execution plan for a small (¥50k-style) account.

``build_plan`` turns one account snapshot and one target-weight book into the
deterministic, board-legal, fee-aware order list an operator would work: sells
of names leaving the target first, then fee-aware board-lot buys funded by the
post-sell cash, with a cash floor that is never violated. It is pure and
read-only: no Mongo, no broker, no order placement. The plan is a PRE-TRADE
PREVIEW; the execution-time gate (``tradability.tradeable_open`` inside
``simulate_paper_nav`` and the live loop) remains the authority.

Accounting contract
-------------------
* ``AUM = cash + sum(held quantity * valuation price)``. The valuation price is
  the session's ``open`` when present, otherwise its ``close``, otherwise the
  last observed close (``previous_close``) — so a suspended holding is still
  valued. A holding with none of those fails closed (name + date); it is never
  marked at zero. An optional ``aum`` argument is only a cross-check of that
  identity and a mismatch fails loudly.
* A per-name buy budget is ``target weight * AUM - current holding value``; the
  order also never spends more than ``cash - cash floor``.
* ``cash floor = cash_reserve_pct * AUM`` from the validated strategy config
  (``cash_reserve_pct``, default 0). A plan whose cash cannot respect the floor
  fails loudly instead of silently violating it.
* Fees and slippage come from ``resolve_execution(config)``: commission with a
  ¥5 minimum per side, sell stamp duty, the two-way transfer fee and per-side
  slippage — the same numbers ``simulate_paper_nav`` charges, reused through
  ``nav.exec_price`` / ``nav.order_cost`` / ``nav.fit_buy_quantity``.
* BSE/北交所 codes are outside this slice's production universe: a BSE target is
  reported as skipped (``bse_outside_universe``) and is never sized. A held BSE
  name is therefore not a target either, so it exits (sales use no board-lot
  rule, only the full position).

Reporting contract
------------------
* ``orders`` — orders the plan would place, sells before buys, each with the
  slippage-adjusted price, notional value, fee breakdown and intent reason.
* ``blocked`` — a ``tradeable_open`` refusal for the side that would be blocked
  (limit-up BUY, limit-down SELL, suspension, unknown limit, no quote). The
  refusal is reported per name, never silently dropped; a blocked sell rolls the
  position forward (the name is kept in the projected account).
* ``skipped`` — a target that produced no buy for a sizing reason (BSE, zero
  weight/AUM, one board minimum does not fit, cash floor reached, already
  at/above target).
* ``target_quantities`` / ``projected_positions`` / ``target_holdings`` — the
  expected post-plan account, so ``reconcile.reconcile_plan_vs_account`` can
  diff it against a real account without recomputation. Projected rows are
  marked at the same valuation price as AUM (the session open for a new buy).

Determinism
-----------
Iteration is sorted by code, every monetary field is rounded to six decimals
and ``plan_json`` serialises with sorted keys, so the same snapshot, targets,
prices and config produce byte-identical output.
"""

from __future__ import annotations

import hashlib
import json
import math

from app.lib.strategy_engine.config import (
    resolve_execution,
    strategy_config_hash,
    validate_strategy_config,
)
from app.lib.strategy_engine.nav import (
    QuoteView,
    board_lot_rule,
    exec_price,
    fit_buy_quantity,
    order_cost,
)
from app.lib.strategy_engine.tradability import tradeable_open
from app.lib.utilities.data_capability_helper import is_bse_stock_code

#: Bumped when the emitted document shape changes incompatibly.
PLAN_VERSION = 1

#: Every plan is a research preview; the field travels with the artifact so a
#: reader cannot mistake an order list for an executed/advised trade.
DISCLAIMER = (
    "Pre-trade research preview only: no orders are placed, no broker is "
    "contacted and this is not investment advice. The execution-time gate "
    "remains the authority; blocked/skipped rows mean the plan is incomplete."
)

_QUOTE_FIELDS = {
    "open",
    "close",
    "trade_status",
    "limit_up",
    "limit_down",
    "previous_close",
    "stock_code",
    "is_st",
}


def is_bse_code(stock_code) -> bool:
    """True for a BSE/北交所 code (``bj…``, bare ``8…``/``4…``/``92…``).

    Mirrors ``factor_lab.panel.price_limit``: the shared BSE pattern also matches
    bare 9-prefixed codes, which is why 900xxx (Shanghai B-shares) and any
    exchange-prefixed sh/sz code are explicitly not BSE here.
    """
    code = str(stock_code or "").strip().lower()
    if not code or code.startswith(("sh", "sz", "900")):
        return False
    return bool(is_bse_stock_code(code))


def _valid_price(value) -> bool:
    try:
        return (
            not isinstance(value, bool)
            and math.isfinite(float(value))
            and float(value) > 0
        )
    except (TypeError, ValueError, OverflowError):
        return False


def _finite_number(value, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a number, got {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label} must be a finite number, got {value!r}")
    return number


def _share_quantity(value, label: str):
    """A non-negative share count; fractional only when the input is (an odd-lot
    position is closed out in full, never rounded down)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a number, got {value!r}")
    quantity = float(value)
    if not math.isfinite(quantity):
        raise ValueError(f"{label} must be a finite number, got {value!r}")
    if quantity < 0:
        raise ValueError(f"{label} must be >= 0, got {value!r}")
    return int(quantity) if quantity.is_integer() else quantity


def _normalize_code(code) -> str:
    text = str(code or "").strip()
    if not text:
        raise ValueError("stock codes must be non-empty strings")
    return text


def _as_of_text(value) -> str | None:
    """Normalise ``as_of`` to an ISO string so the plan stays JSON-serialisable
    and deterministic even when a caller passes a datetime/date."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _normalized_positions(positions) -> dict:
    """Code -> share quantity (accepts either a number or a
    ``{quantity|qty|shares}`` object per name, like the reconcile service)."""
    if positions is None:
        return {}
    if not isinstance(positions, dict):
        raise ValueError("positions must be a mapping of stock code -> quantity")
    result = {}
    for code, value in positions.items():
        key = _normalize_code(code)
        if key in result:
            raise ValueError(f"duplicate position code {key!r}")
        if isinstance(value, dict):
            value = value.get("quantity", value.get("qty", value.get("shares")))
        result[key] = _share_quantity(value, f"positions[{key}]")
    return result


def _normalized_weights(target_weights) -> dict:
    if target_weights is None:
        raise ValueError("target_weights is required (pass {} for a full exit)")
    if not isinstance(target_weights, dict):
        raise ValueError("target_weights must be a mapping of stock code -> weight")
    result = {}
    for code, weight in target_weights.items():
        key = _normalize_code(code)
        if key in result:
            raise ValueError(f"duplicate target code {key!r}")
        value = _finite_number(weight, f"target_weights[{key}]")
        if value < 0:
            raise ValueError(f"target_weights[{key}] must be >= 0, got {weight!r}")
        if value > 1:
            raise ValueError(f"target_weights[{key}] must be <= 1, got {weight!r}")
        result[key] = value
    total = sum(result.values())
    if total > 1 + 1e-9:
        raise ValueError(
            f"target weights sum to {total!r} > 1: a levered plan is refused"
        )
    return result


def _coerce_quote(code: str, value):
    """QuoteView-like value from a QuoteView, a JSON-style dict row, or None.

    A dict row must state ``trade_status=1`` to be tradable: a missing status is
    passed through as unknown (fail-closed in ``tradeable_open``), never defaulted
    to tradable.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        unknown = set(value) - _QUOTE_FIELDS
        if unknown:
            raise ValueError(
                f"prices[{code!r}] has unknown keys {sorted(unknown)}; "
                f"known: {sorted(_QUOTE_FIELDS)}"
            )
        return QuoteView(
            value.get("open"),
            value.get("close"),
            value.get("trade_status"),
            limit_up=value.get("limit_up"),
            limit_down=value.get("limit_down"),
            previous_close=value.get("previous_close"),
            stock_code=code,
            is_st=bool(value.get("is_st", False)),
        )
    if hasattr(value, "open") and hasattr(value, "trade_status"):
        return value
    raise ValueError(
        f"prices[{code!r}] must be a QuoteView-like object with open/trade_status"
    )


def _normalized_quotes(prices) -> dict:
    if prices is None:
        return {}
    if not isinstance(prices, dict):
        raise ValueError("prices must be a mapping of stock code -> quote")
    result = {}
    for code, value in prices.items():
        key = _normalize_code(code)
        if key in result:
            raise ValueError(f"duplicate price code {key!r}")
        result[key] = _coerce_quote(key, value)
    return result


def _valuation_price(quote) -> tuple[float | None, str]:
    """(price, source) used to value one holding, or (None, reason).

    The live session open is the most recent observable price; the session close
    covers a snapshot built after the close; ``previous_close`` is the last
    observed close carried for a suspended name. Nothing usable is a hard
    failure upstream, never a zero mark.
    """
    if quote is None:
        return None, "no_quote"
    for field in ("open", "close", "previous_close"):
        value = getattr(quote, field, None)
        if _valid_price(value):
            return float(value), field
    return None, "no_price"


def _finite_or_none(value):
    return float(value) if _valid_price(value) else None


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _bool_or_none(value):
    return None if value is None else bool(value)


def _round(value) -> float:
    return round(float(value), 6)


def _price_fingerprint(quote) -> dict:
    return {
        "open": _finite_or_none(getattr(quote, "open", None)),
        "close": _finite_or_none(getattr(quote, "close", None)),
        "trade_status": _int_or_none(getattr(quote, "trade_status", None)),
        "limit_up": _bool_or_none(getattr(quote, "limit_up", None)),
        "limit_down": _bool_or_none(getattr(quote, "limit_down", None)),
        "previous_close": _finite_or_none(getattr(quote, "previous_close", None)),
        "is_st": bool(getattr(quote, "is_st", False)),
    }


def _input_hash(*, cash, positions, target_weights, quotes, as_of, aum) -> str:
    """Stable sha256 of the normalised plan inputs (order-insensitive)."""
    payload = {
        "as_of": _as_of_text(as_of),
        "aum": _round(aum),
        "cash": _round(cash),
        "positions": {code: positions[code] for code in sorted(positions)},
        "prices": {code: _price_fingerprint(quotes[code]) for code in sorted(quotes)},
        "target_weights": {
            code: float(target_weights[code]) for code in sorted(target_weights)
        },
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _order_row(side, code, quantity, fill, cost, reason) -> dict:
    return {
        "side": side,
        "code": code,
        "quantity": quantity,
        "price": _round(fill),
        "value": _round(cost["value"]),
        "fees": {
            "commission": _round(cost["commission"]),
            "stamp_duty": _round(cost["stamp_duty"]),
            "transfer_fee": _round(cost["transfer_fee"]),
        },
        "reason": reason,
    }


def build_plan(
    *,
    cash,
    positions,
    target_weights,
    prices,
    config,
    as_of=None,
    aum=None,
) -> dict:
    """Build the deterministic pre-trade plan for one account snapshot.

    cash: settled cash in CNY (>= 0).
    positions: code -> held quantity.
    target_weights: code -> target weight in [0, 1] (sum <= 1).
    prices: code -> QuoteView-like (or a JSON-style quote row) for the execution
    session; only referenced names are required.
    config: validated strategy config (``score_model_version`` required); its
    ``execution`` block overrides PAPER_EXECUTION and is recorded via
    ``strategy_config_hash``.
    as_of: signal/execution date recorded in the output and in fail-closed
    errors.
    aum: optional cross-check; must equal ``cash + marked holdings``.

    Raises ValueError for an unvaluable holding, negative cash, a cash balance
    below the configured floor, a plan that would overdraw cash, or malformed
    input. Returns the JSON-ready plan dict described in the module docstring.
    """
    normalized_config = validate_strategy_config(config)
    cfg = resolve_execution(normalized_config)
    config_hash = strategy_config_hash(normalized_config)

    cash_before = _finite_number(cash, "cash")
    if cash_before < 0:
        raise ValueError(f"cash must be >= 0, got {cash_before!r}")
    held = _normalized_positions(positions)
    targets = _normalized_weights(target_weights)
    quotes = _normalized_quotes(prices)

    # BSE is outside the universe: it is never a target, so it is never sized
    # (and a held BSE name is no longer a target, hence it exits).
    skipped: list[dict] = []
    effective_targets: dict[str, float] = {}
    for code in sorted(targets):
        if is_bse_code(code):
            skipped.append({"code": code, "reason": "bse_outside_universe"})
            continue
        effective_targets[code] = targets[code]

    # AUM must be derivable: an unvaluable holding fails closed with the name
    # and the date, never a zero mark.
    valuation: dict[str, float] = {}
    for code in sorted(held):
        price, source = _valuation_price(quotes.get(code))
        if price is None:
            raise ValueError(
                f"cannot value held position {code!r} as of {_as_of_text(as_of)}: "
                f"no usable price ({source})"
            )
        valuation[code] = price
    computed_aum = cash_before + sum(held[code] * valuation[code] for code in held)
    if aum is not None:
        supplied = _finite_number(aum, "aum")
        if not math.isclose(supplied, computed_aum, rel_tol=1e-9, abs_tol=0.01):
            raise ValueError(
                f"aum mismatch: supplied {supplied!r} != cash + marked holdings "
                f"{computed_aum!r}; AUM must be cash + sum(held quantity * price)"
            )

    input_hash = _input_hash(
        cash=cash_before,
        positions=held,
        target_weights=targets,
        quotes=quotes,
        as_of=as_of,
        aum=computed_aum,
    )

    reserve_pct = float(normalized_config.get("cash_reserve_pct") or 0.0)
    cash_floor = reserve_pct * computed_aum
    if cash_before < cash_floor - 1e-9:
        raise ValueError(
            f"cash {cash_before!r} is below the configured cash floor "
            f"{cash_floor!r} (cash_reserve_pct={reserve_pct!r} of AUM)"
        )

    orders: list[dict] = []
    blocked: list[dict] = []
    projected = dict(held)
    marks = dict(valuation)  # code -> mark price used for projected rows
    cash = cash_before

    # 1) Sells first: held names that left the (effective) target, at the open.
    for code in sorted(set(held) - set(effective_targets)):
        quantity = held[code]
        if quantity <= 0:
            continue
        price, reason = tradeable_open(quotes.get(code), "SELL", stock_code=code)
        if price is None:
            # A blocked exit rolls forward: the position stays in the account
            # and the refusal is reported.
            blocked.append({"code": code, "side": "SELL", "reason": reason})
            continue
        fill = exec_price(price, "SELL", cfg)
        cost = order_cost(fill, quantity, "SELL", cfg)
        cash += (
            cost["value"]
            - cost["commission"]
            - cost["stamp_duty"]
            - cost["transfer_fee"]
        )
        orders.append(
            _order_row("SELL", code, quantity, fill, cost, "exit_not_in_target")
        )
        projected.pop(code, None)
        marks.pop(code, None)

    # 2) Buys second: target weight * AUM is the per-name budget, capped by the
    # post-sell cash above the floor and by the gap to the existing holding.
    for code in sorted(effective_targets):
        weight = effective_targets[code]
        if weight <= 0:
            skipped.append({"code": code, "reason": "zero_target_weight"})
            continue
        if computed_aum <= 0:
            # A zero-AUM account has no budget to allocate; report it rather
            # than size a 0-share order or emit a misleading "at target" skip.
            skipped.append({"code": code, "reason": "zero_aum"})
            continue
        held_quantity = held.get(code, 0)
        held_value = valuation[code] * held_quantity if held_quantity else 0.0
        gap = weight * computed_aum - held_value
        if gap <= 0:
            # No trim: a holding already at/above its target is left alone.
            skipped.append({"code": code, "reason": "target_already_met"})
            continue
        available = cash - cash_floor
        if available <= 0:
            skipped.append({"code": code, "reason": "cash_floor_reached"})
            continue
        price, reason = tradeable_open(quotes.get(code), "BUY", stock_code=code)
        if price is None:
            blocked.append({"code": code, "side": "BUY", "reason": reason})
            continue
        fill = exec_price(price, "BUY", cfg)
        minimum, increment = board_lot_rule(code, int(cfg.get("board_lot") or 100))
        budget = min(gap, available)
        quantity = fit_buy_quantity(budget, fill, minimum, increment, cfg)
        if quantity <= 0:
            # One board minimum does not fit the budget: skip, never partially
            # fill and never overshoot the fee-inclusive cash.
            skipped.append(
                {
                    "code": code,
                    "reason": "budget_below_board_minimum",
                    "budget": _round(budget),
                    "minimum_quantity": int(minimum),
                    "price": _round(price),
                }
            )
            continue
        cost = order_cost(fill, quantity, "BUY", cfg)
        spend = cost["value"] + cost["commission"] + cost["transfer_fee"]
        if spend > available + 1e-6:
            # Unreachable while sizing is fee-aware; fail loudly rather than
            # emit a buy the account cannot pay for.
            raise ValueError(
                f"internal sizing error: BUY {code!r} spend {spend!r} exceeds "
                f"available cash {available!r}"
            )
        cash -= spend
        intent = "enter_target" if held_quantity == 0 else "add_to_target"
        orders.append(_order_row("BUY", code, quantity, fill, cost, intent))
        projected[code] = held_quantity + quantity
        marks[code] = float(price)

    cash_after = cash
    if cash_after < -1e-9:
        raise ValueError(
            f"plan would leave negative cash ({cash_after!r}); refusing to emit"
        )
    if cash_after < cash_floor - 1e-6:
        raise ValueError(
            f"plan would violate the cash floor ({cash_after!r} < {cash_floor!r})"
        )

    projected_positions = [
        {
            "code": code,
            "quantity": projected[code],
            "price": _round(marks[code]),
            "value": _round(projected[code] * marks[code]),
        }
        for code in sorted(projected)
        if projected[code] > 0
    ]
    skipped.sort(key=lambda row: (row["code"], row["reason"]))
    blocked.sort(key=lambda row: (row["code"], row["side"], row["reason"]))

    return {
        "as_of": _as_of_text(as_of),
        "plan_version": PLAN_VERSION,
        "aum": _round(computed_aum),
        "cash_before": _round(cash_before),
        "cash_after": _round(cash_after),
        "cash_floor": _round(cash_floor),
        "cash_reserve_pct": reserve_pct,
        "config_hash": config_hash,
        "input_hash": input_hash,
        "execution": {key: cfg[key] for key in sorted(cfg)},
        "orders": orders,
        "skipped": skipped,
        "blocked": blocked,
        "target_quantities": {
            row["code"]: row["quantity"] for row in projected_positions
        },
        "target_holdings": [
            {"stock_code": row["code"], "quantity": row["quantity"]}
            for row in projected_positions
        ],
        "projected_positions": projected_positions,
        "research_only": True,
        "orders_placed": 0,
        "disclaimer": DISCLAIMER,
    }


def plan_json(plan: dict, *, indent: int | None = None) -> str:
    """Canonical JSON for a plan: sorted keys and fixed separators, so repeated
    builds on the same inputs are byte-identical."""
    options = {"ensure_ascii": False, "sort_keys": True, "default": str}
    if indent is None:
        options["separators"] = (",", ":")
    else:
        options["indent"] = int(indent)
    return json.dumps(plan, **options)
