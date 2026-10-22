# -*- coding: utf-8 -*-
"""Corporate-action ledger and total-return index construction.

The ledger is the single source of truth for share and cash events.  Adjusted
prices are derived from it; research code must never infer an adjustment factor
from a price gap and must never treat vendor qfq/hfq as the primary series.
"""

from app.lib.prices.corporate_actions import (
    ACTIONS,
    CorporateAction,
    event_factor,
    load_actions,
    normalize_action,
)
from app.lib.prices.return_index import (
    PriceContractViolation,
    build_return_index,
    validate_return_index,
)

__all__ = [
    "ACTIONS",
    "CorporateAction",
    "PriceContractViolation",
    "build_return_index",
    "event_factor",
    "load_actions",
    "normalize_action",
    "validate_return_index",
]
