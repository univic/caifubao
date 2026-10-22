# -*- coding: utf-8 -*-
"""Execution cost model (single source of truth).

Every engine that turns a backtest into a P&L number must use these constants:
the research factor lab, the paper NAV engine, the ETF lab, the HFT-style
timing replay, and the backend backtest API. They previously drifted (0.1 % vs
0.05 % sell stamp duty), which made cross-engine results incomparable.

The backend mirrors these values in ``backend/app/services/backtest_service.py``
(separate image, no shared package yet); ``backend/app/test/
test_friction_contract_parity.py`` fails if the two copies diverge.
"""

from app.lib.execution.costs import (
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

__all__ = [
    "BOARD_LOT",
    "COMMISSION_RATE",
    "DEFAULT_COST_PROFILE",
    "ETF_SELL_STAMP_DUTY_RATE",
    "MIN_COMMISSION_CNY",
    "SELL_STAMP_DUTY_RATE",
    "SLIPPAGE_PER_SIDE",
    "TRANSFER_FEE_RATE",
    "CostProfile",
    "SideCost",
    "side_cost",
]
