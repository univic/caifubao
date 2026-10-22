# -*- coding: utf-8 -*-
"""Execution cost model — compatibility re-export.

The definition lives in ``shared/caifubao_contracts/friction.py`` and is
installed into both images, so the backend backtest service and every datahub
engine share exactly one set of A-share cost constants.
"""

from caifubao_contracts.friction import (  # noqa: F401
    BOARD_LOT,
    COMMISSION_RATE,
    DEFAULT_COST_PROFILE,
    ETF_SELL_STAMP_DUTY_RATE,
    MIN_COMMISSION_CNY,
    SELL_STAMP_DUTY_RATE,
    SLIPPAGE_PER_SIDE,
    TRANSFER_FEE_RATE,
    CostProfile,
    SideCost,
    side_cost,
)
