# -*- coding: utf-8 -*-
"""Event-sourced research-only simulation ledger.

A target portfolio is intent, never holdings. This ledger has no persistence
or trading account access; its event stream is stored in research artifacts.
"""

from __future__ import annotations

import copy
import datetime
import math


def _number(value, field: str, *, positive: bool = False) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{field} must be finite")
    result = float(value)
    if result <= 0 if positive else result < 0:
        raise ValueError(
            f"{field} must be {'positive' if positive else 'non-negative'}"
        )
    return result


def _date(value) -> datetime.date:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value)[:10])


class ResearchSimulationLedger:
    """Rebuildable account state; not a trading/paper operational ledger."""

    def __init__(self, *, initial_cash: float, max_positions: int | None = None):
        self.initial_cash = _number(initial_cash, "initial_cash", positive=True)
        if max_positions is not None and (
            type(max_positions) is not int or max_positions <= 0
        ):
            raise ValueError("max_positions must be a positive integer or None")
        self.max_positions = max_positions
        self.cash = self.initial_cash
        self.positions: dict[str, dict] = {}
        self.events: list[dict] = []
        self._by_id: dict[str, dict] = {}
        self.realized_pnl = 0.0

    def apply(self, event: dict) -> bool:
        """Apply once; duplicate identity+payload is no-op, conflicting is fatal."""
        copied = copy.deepcopy(event)
        if not isinstance(copied, dict):
            raise ValueError("event must be an object")
        identity = copied.get("event_id")
        if not isinstance(identity, str) or not identity:
            raise ValueError("event_id is required")
        if identity in self._by_id:
            if self._by_id[identity] != copied:
                raise ValueError(f"conflicting event_id {identity}")
            return False
        kind = copied.get("type")
        if kind == "REJECTED":
            if not copied.get("reason_code") or not copied.get("order_id"):
                raise ValueError("rejection requires reason_code and order_id")
        elif kind == "FILL":
            self._fill(copied)
        elif kind == "MARK":
            marks = copied.get("marks")
            if not isinstance(marks, dict):
                raise ValueError("mark event needs a quote map")
            # Validate the full mark batch before touching any position: a bad
            # quote must not leave half of the portfolio revalued.
            validated = {}
            for code in self.positions:
                value = marks.get(code)
                if value is None:
                    validated[code] = None
                else:
                    validated[code] = _number(value, f"mark {code}", positive=True)
            for code, position in self.positions.items():
                value = validated[code]
                if value is None:
                    position["stale"] = True
                else:
                    position["last_price"] = value
                    position["stale"] = False
        elif kind == "DIVIDEND":
            code = copied.get("stock_code")
            if code not in self.positions:
                raise ValueError("dividend requires existing position")
            per_share = _number(copied.get("cash_per_share"), "cash_per_share")
            self.cash += per_share * self.positions[code]["quantity"]
        elif kind == "SPLIT":
            code = copied.get("stock_code")
            if code not in self.positions:
                raise ValueError("split requires existing position")
            multiplier = _number(
                copied.get("share_multiplier"), "share_multiplier", positive=True
            )
            p = self.positions[code]
            quantity = p["quantity"] * multiplier
            if int(quantity) != quantity:
                raise ValueError(
                    "split produces fractional shares without cash-in-lieu"
                )
            p["quantity"] = int(quantity)
        elif kind == "WRITE_OFF":
            code = copied.get("stock_code")
            if code not in self.positions or not copied.get("reason_code"):
                raise ValueError("write-off requires held name and reason_code")
            price = _number(copied.get("exit_price"), "exit_price")
            p = self.positions.pop(code)
            proceeds = p["quantity"] * price
            self.cash += proceeds
            self.realized_pnl += proceeds - p["cost_basis"]
        else:
            raise ValueError(f"unsupported ledger event type: {kind!r}")
        self.events.append(copied)
        self._by_id[identity] = copied
        return True

    def _fill(self, e: dict) -> None:
        side = e.get("side")
        if side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        code = e.get("stock_code")
        if not isinstance(code, str) or not code:
            raise ValueError("stock_code is required")
        if (
            not e.get("order_id")
            or not e.get("fill_id")
            or e["event_id"] != e["fill_id"]
        ):
            raise ValueError("fill must have stable order_id and fill_id=event_id")
        if "decision_date" in e and _date(e["execution_date"]) <= _date(
            e["decision_date"]
        ):
            raise ValueError("T+1: fill date must follow decision date")
        qty = e.get("quantity")
        if type(qty) is not int or qty <= 0:
            raise ValueError("fill quantity must be a positive integer")
        price = _number(e.get("price"), "price", positive=True)
        fees = e.get("fees") or {}
        if not isinstance(fees, dict):
            raise ValueError("fees must be an object")
        expense = sum(_number(value, f"fees.{key}") for key, value in fees.items())
        notional = qty * price
        if side == "BUY":
            reference_price = _number(
                e.get("reference_price", price), "reference_price", positive=True
            )
            if notional + expense > self.cash + 1e-8:
                raise ValueError("insufficient cash; cash cannot go negative")
            if (
                code not in self.positions
                and self.max_positions is not None
                and len(self.positions) >= self.max_positions
            ):
                raise ValueError("max_positions exceeded")
            self.cash -= notional + expense
            position = self.positions.setdefault(
                code,
                {"quantity": 0, "cost_basis": 0.0, "last_price": price, "stale": False},
            )
            position["quantity"] += qty
            position["cost_basis"] += notional + expense
            position["last_price"] = reference_price
        else:
            p = self.positions.get(code)
            if p is None or qty > p["quantity"]:
                raise ValueError("cannot sell more shares than held")
            avg_cost = p["cost_basis"] / p["quantity"]
            self.realized_pnl += notional - expense - avg_cost * qty
            self.cash += notional - expense
            p["cost_basis"] -= avg_cost * qty
            p["quantity"] -= qty
            if p["quantity"] == 0:
                del self.positions[code]

    def mark(
        self, quotes: dict[str, float | None], *, as_of: str | None = None
    ) -> float:
        """Record observed close marks as events, carrying stale marks forward."""
        event_id = f"mark:{as_of}" if as_of else f"mark:sequence:{len(self.events)}"
        self.apply(
            {
                "type": "MARK",
                "event_id": event_id,
                "marks": copy.deepcopy(quotes),
            }
        )
        return self.nav

    @property
    def nav(self) -> float:
        return self.cash + sum(
            p["quantity"] * p["last_price"] for p in self.positions.values()
        )

    def snapshot(self) -> dict:
        return {
            "initial_cash": self.initial_cash,
            "cash": self.cash,
            "positions": copy.deepcopy(self.positions),
            "realized_pnl": self.realized_pnl,
            "nav": self.nav,
            "event_count": len(self.events),
        }

    @classmethod
    def rebuild(
        cls,
        events: list[dict],
        *,
        initial_cash: float,
        max_positions: int | None = None,
    ):
        ledger = cls(initial_cash=initial_cash, max_positions=max_positions)
        for e in events:
            ledger.apply(e)
        return ledger
