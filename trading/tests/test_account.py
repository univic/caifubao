"""Offline Trading-domain tests; no datahub, Mongo or broker dependencies."""

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from caifubao_trading import TradingHaltStore, TradingPaperAccount


NOW = dt.datetime(2026, 10, 24, tzinfo=dt.timezone.utc)


def fill(event_id, side, qty=100, price=10, code="sh600000"):
    return {
        "type": "FILL",
        "event_id": event_id,
        "fill_id": event_id,
        "order_id": "order-" + event_id,
        "source_domain": "trading",
        "account_id": "paper-test",
        "currency": "CNY",
        "stock_code": code,
        "side": side,
        "quantity": qty,
        "price": price,
        "decision_date": "2026-10-23",
        "execution_date": "2026-10-26",
    }


class TradingDomainTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.halt = TradingHaltStore(Path(self.tmp.name) / "halt.json")
        self.account = TradingPaperAccount(
            account_id="paper-test", initial_cash=10000
        )

    def arm(self):
        self.halt.change(
            halted=True, actor="operator", reason="initialize", at=NOW
        )
        self.halt.change(
            halted=False, actor="operator", reason="offline paper", at=NOW
        )

    def test_missing_state_fails_closed_and_requires_operator(self):
        with self.assertRaisesRegex(ValueError, "TRADING_HALTED"):
            self.account.apply_fill(fill("one", "BUY"), halt_store=self.halt)
        with self.assertRaisesRegex(ValueError, "initialize"):
            self.halt.change(
                halted=False, actor="operator", reason="skip", at=NOW
            )
        self.arm()
        self.assertFalse(self.halt.snapshot()["halted"])
        self.assertEqual(len(self.halt.snapshot()["history"]), 2)
        self.halt.change(halted=True, actor="operator", reason="incident", at=NOW)
        with self.assertRaisesRegex(ValueError, "TRADING_HALTED"):
            self.account.apply_fill(fill("one", "BUY"), halt_store=self.halt)

    def test_buy_sell_idempotence_and_rebuild(self):
        self.arm()
        initial = self.account.projection()
        with self.assertRaisesRegex(ValueError, "T\\+1"):
            self.account.apply_fill(
                {**fill("a", "BUY"), "execution_date": "2026-10-23"},
                halt_store=self.halt,
            )
        self.assertEqual(initial, self.account.projection())
        self.account.apply_fill(fill("a", "BUY"), halt_store=self.halt)
        self.assertEqual(self.account.positions["sh600000"], 100)
        self.account.apply_fill(fill("a", "BUY"), halt_store=self.halt)
        self.assertEqual(len(self.account.events), 1)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.account.apply_fill(
                {**fill("a", "BUY"), "price": 11}, halt_store=self.halt
            )
        self.account.apply_fill(fill("b", "SELL"), halt_store=self.halt)
        self.assertEqual(self.account.positions, {})
        rebuilt = TradingPaperAccount.rebuild(
            account_id="paper-test", initial_cash=10000,
            events=self.account.events, halt_store=self.halt,
        )
        self.assertEqual(rebuilt.projection(), self.account.projection())
        self.assertEqual(len(rebuilt.events), 2)
        self.assertEqual(self.account.projection()["source_domain"], "trading")

    def test_failures_are_atomic_and_star_lot_precision(self):
        self.arm()
        old = self.account.projection()
        with self.assertRaisesRegex(ValueError, "board lot"):
            self.account.apply_fill(
                fill("small", "BUY", qty=100, code="sh688001"),
                halt_store=self.halt,
            )
        with self.assertRaisesRegex(ValueError, "insufficient paper shares"):
            self.account.apply_fill(
                fill("sell", "SELL"), halt_store=self.halt
            )
        with self.assertRaisesRegex(ValueError, "insufficient paper cash"):
            self.account.apply_fill(
                fill("expensive", "BUY", price=500),
                halt_store=self.halt,
            )
        self.assertEqual(old, self.account.projection())
        self.account.apply_fill(
            fill("star", "BUY", qty=201, price=10, code="sh688001"),
            halt_store=self.halt,
        )
        self.assertEqual(self.account.positions["sh688001"], 201)

    def test_operational_halt_priority_before_risk(self):
        intents = [{
            "intent_id": "buy", "stock_code": "sh600000",
            "side": "BUY", "quantity": 100, "price": 10,
        }]
        with self.assertRaisesRegex(ValueError, "TRADING_HALTED"):
            self.account.risk_preview(
                intents=intents, nav=10000, policy={}, market=None,
                halt_store=self.halt,
            )
        self.arm()
        result = self.account.risk_preview(
            intents=intents, nav=10000, policy={}, market=None,
            halt_store=self.halt,
        )
        self.assertEqual(len(result["approved"]), 1)
        self.assertEqual(self.account.cash, 10000)


if __name__ == "__main__":
    unittest.main()
