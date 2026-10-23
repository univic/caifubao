# -*- coding: utf-8 -*-
"""Pure trading ledger projection contract; no datahub/research dependencies.

The trading runtime owns event storage and account reconciliation. These
validators only prevent research projections/targets from masquerading as
filled operational positions; they never read accounts, brokers, or databases.
"""

from __future__ import annotations

import copy
import math


def require_trading_projection(projection: dict) -> dict:
    if not isinstance(projection, dict):
        raise ValueError("trading ledger projection is required")
    if projection.get("source_domain") != "trading":
        raise ValueError("trading ledger projection source_domain must be trading")
    if not isinstance(projection.get("account_id"), str) or not projection[
        "account_id"
    ]:
        raise ValueError("trading ledger projection account_id is required")
    cash = projection.get("cash")
    if (
        isinstance(cash, bool)
        or not isinstance(cash, (int, float))
        or not math.isfinite(cash)
        or cash < 0
    ):
        raise ValueError("trading ledger projection cash is required and finite")
    positions = projection.get("positions")
    if not isinstance(positions, (dict, list)):
        raise ValueError("trading ledger projection positions is required")
    # This is a frozen, detached view. Mutable research/live callers never
    # receive permission to change the underlying trading ledger.
    return copy.deepcopy(projection)
