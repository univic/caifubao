"""Paper account ledger independent from research and from real brokers.

Fills are supplied explicitly by a future trading-owned paper exchange adapter.
Order approval is NOT a fill. This implementation has no live/broker route.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math

from caifubao_contracts.friction import DEFAULT_COST_PROFILE
from caifubao_contracts.strategy_execution import (
    board_lot_rule,
    execution_fees,
    require_t_plus_one,
)
from caifubao_contracts.strategy_ledger_events import validate_ledger_event
from caifubao_contracts.strategy_risk import evaluate_risk
from caifubao_contracts.trading_projection import require_trading_projection


class TradingPaperAccount:
    def __init__(self, *, account_id: str, initial_cash: float):
        if not isinstance(account_id, str) or not account_id.strip():
            raise ValueError("trading account_id is required")
        if (
            isinstance(initial_cash, bool)
            or not isinstance(initial_cash, (int, float))
            or not math.isfinite(initial_cash)
            or initial_cash <= 0
        ):
            raise ValueError("initial_cash must be finite and positive")
        self.account_id = account_id
        self.initial_cash = float(initial_cash)
        self.cash = float(initial_cash)
        self.positions: dict[str, int] = {}
        self.events: list[dict] = []
        self._by_id: dict[str, str] = {}
        self._lots: dict[str, list[tuple[str, int]]] = {}
        self._last_execution_date: str | None = None

    def risk_preview(
        self,
        *,
        intents: list[dict],
        nav: float,
        policy: dict,
        market: dict | None,
        halt_store,
    ) -> dict:
        # Authority-first: no orders or risk approvals while operational halt.
        halt_store.assert_orders_allowed()
        return evaluate_risk(
            intents=intents,
            account={
                "cash": self.cash,
                "nav": nav,
                "positions": {
                    code: {"quantity": qty} for code, qty in self.positions.items()
                },
            },
            policy=policy,
            market=market,
        )

    def apply_fill(self, event: dict, *, halt_store) -> dict:
        """Apply validated explicit PAPER fill atomically and idempotently."""
        halt_store.assert_orders_allowed()
        filled = validate_ledger_event(event)
        if filled["type"] != "FILL":
            raise ValueError("paper account accepts FILL only")
        if (
            filled.get("source_domain") != "trading"
            or filled.get("account_id") != self.account_id
            or filled.get("currency") != "CNY"
        ):
            raise ValueError("fill belongs to a different trading account")
        if not filled.get("decision_date"):
            raise ValueError("decision_date required for T+1")
        require_t_plus_one(filled["decision_date"], filled["execution_date"])
        fingerprint = hashlib.sha256(
            json.dumps(filled, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        existing = self._by_id.get(filled["event_id"])
        if existing is not None:
            if existing != fingerprint:
                raise ValueError("conflicting fill event_id")
            return self.projection()
        session = dt.date.fromisoformat(str(filled["execution_date"])[:10]).isoformat()
        if (
            self._last_execution_date is not None
            and session < self._last_execution_date
        ):
            raise ValueError("paper fills must be chronological")
        next_lots = copy.deepcopy(self._lots)
        code = filled["stock_code"]
        side = filled["side"]
        qty = filled["quantity"]
        price = float(filled["price"])
        lot_min, step = board_lot_rule(code)
        if side == "BUY" and (qty < lot_min or (qty - lot_min) % step):
            raise ValueError("buy violates A-share board lot")
        fees = execution_fees(price, qty, side, DEFAULT_COST_PROFILE)
        gross = price * qty
        if side == "BUY":
            required = gross + fees["commission"] + fees["transfer_fee"]
            if self.cash + 1e-8 < required:
                raise ValueError("insufficient paper cash")
            next_cash = self.cash - required
            next_positions = dict(self.positions)
            next_positions[code] = next_positions.get(code, 0) + qty
            next_lots.setdefault(code, []).append((session, qty))
        else:
            if qty > self.positions.get(code, 0):
                raise ValueError("insufficient paper shares")
            lots = next_lots.get(code, [])
            available = sum(
                quantity for acquired, quantity in lots if acquired < session
            )
            if qty > available:
                raise ValueError("T+1: cannot sell shares acquired in this session")
            to_sell = qty
            remaining_lots = []
            for acquired, quantity in lots:
                sold = min(quantity, to_sell) if acquired < session else 0
                to_sell -= sold
                if quantity > sold:
                    remaining_lots.append((acquired, quantity - sold))
            if remaining_lots:
                next_lots[code] = remaining_lots
            else:
                next_lots.pop(code, None)
            next_cash = (
                self.cash
                + gross
                - fees["commission"]
                - fees["stamp_duty"]
                - fees["transfer_fee"]
            )
            next_positions = dict(self.positions)
            remain = next_positions[code] - qty
            if remain:
                next_positions[code] = remain
            else:
                next_positions.pop(code)
        # Validate the complete next projection before changing any account state.
        projection = self._projection(next_cash, next_positions)
        next_events = [*self.events, copy.deepcopy(filled)]
        next_by_id = {**self._by_id, filled["event_id"]: fingerprint}
        self.cash = next_cash
        self.positions = next_positions
        self._lots = next_lots
        self._last_execution_date = session
        self.events = next_events
        self._by_id = next_by_id
        return projection

    def _projection(self, cash: float, positions: dict[str, int]) -> dict:
        return require_trading_projection(
            {
                "source_domain": "trading",
                "account_id": self.account_id,
                "currency": "CNY",
                "cash": cash,
                "positions": {
                    code: {"quantity": qty} for code, qty in sorted(positions.items())
                },
            }
        )

    def projection(self) -> dict:
        return self._projection(self.cash, self.positions)

    @classmethod
    def rebuild(
        cls, *, account_id: str, initial_cash: float, events: list[dict], halt_store
    ):
        obj = cls(account_id=account_id, initial_cash=initial_cash)
        for event in events:
            obj.apply_fill(event, halt_store=halt_store)
        return obj
