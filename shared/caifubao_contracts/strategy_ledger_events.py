# -*- coding: utf-8 -*-
"""Versioned, pure event schema shared by research and future trading ledgers.

The schema does not persist anything, perform fills or own an account. Research
and trading MUST store events under separate domain-local authorities.
"""

from __future__ import annotations

import copy
import math

EVENT_SCHEMA_VERSION = 1
EVENT_TYPES = frozenset(
    {"FILL", "REJECTED", "MARK", "DIVIDEND", "SPLIT", "WRITE_OFF"}
)


def validate_ledger_event(event: dict) -> dict:
    """Copy and validate a domain-local ledger event before any state mutation."""
    if not isinstance(event, dict):
        raise ValueError("ledger event must be an object")
    result = copy.deepcopy(event)
    if result.get("type") not in EVENT_TYPES:
        raise ValueError("unsupported ledger event type")
    if not isinstance(result.get("event_id"), str) or not result["event_id"]:
        raise ValueError("ledger event_id must be nonempty")
    if result.get("schema_version", EVENT_SCHEMA_VERSION) != EVENT_SCHEMA_VERSION:
        raise ValueError("unsupported ledger event schema_version")
    source = result.get("source_domain")
    if source is not None:
        if source not in ("research", "trading"):
            raise ValueError("source_domain must be research or trading")
        if not result.get("account_id"):
            raise ValueError("domain-scoped ledger event needs account_id")
        if result.get("currency") != "CNY":
            raise ValueError("domain-scoped A-share event requires currency=CNY")
    if result["type"] in ("FILL", "REJECTED"):
        if not result.get("order_id") or not result.get("stock_code"):
            raise ValueError("order event requires order_id and stock_code")
        if result.get("side") not in ("BUY", "SELL"):
            raise ValueError("order event side must be BUY or SELL")
        if not result.get("execution_date"):
            raise ValueError("order event execution_date is required")
    if result["type"] == "FILL":
        if not result.get("fill_id") or result["fill_id"] != result["event_id"]:
            raise ValueError("fill identity must match event_id")
        qty = result.get("quantity")
        price = result.get("price")
        if type(qty) is not int or qty <= 0:
            raise ValueError("fill quantity must be positive integer")
        if (
            isinstance(price, bool)
            or not isinstance(price, (int, float))
            or not math.isfinite(price)
            or price <= 0
        ):
            raise ValueError("fill price must be finite and positive")
    if result["type"] == "REJECTED" and not result.get("reason_code"):
        raise ValueError("rejection requires reason_code")
    return result
