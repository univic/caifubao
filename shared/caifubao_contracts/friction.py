# -*- coding: utf-8 -*-
"""A-share execution cost constants and per-side cost arithmetic.

Rates are fractions of notional and reflect the rules in force since
2023-08-28:

* commission: 0.025 % per side, minimum 5 CNY;
* stamp duty: 0.05 % sell side only (halved from 0.1 % on 2023-08-28);
* transfer fee (过户费): 0.001 % per side, charged on both sides;
* slippage: 0.1 % per side (a modelling assumption, not an exchange rule).

ETFs are exempt from stamp duty; use ``ETF_SELL_STAMP_DUTY_RATE`` (0.0) when
building an ETF cost profile.
"""

from dataclasses import dataclass

COMMISSION_RATE = 0.00025
MIN_COMMISSION_CNY = 5.0
SELL_STAMP_DUTY_RATE = 0.0005
TRANSFER_FEE_RATE = 0.00001
SLIPPAGE_PER_SIDE = 0.001
BOARD_LOT = 100
ETF_SELL_STAMP_DUTY_RATE = 0.0


@dataclass(frozen=True)
class CostProfile:
    """Versioned execution-cost assumptions for one backtest/paper run."""

    commission_rate: float = COMMISSION_RATE
    minimum_commission_cny: float = MIN_COMMISSION_CNY
    sell_stamp_duty_rate: float = SELL_STAMP_DUTY_RATE
    transfer_fee_rate: float = TRANSFER_FEE_RATE
    slippage_per_side: float = SLIPPAGE_PER_SIDE
    board_lot: int = BOARD_LOT

    def as_dict(self) -> dict:
        return {
            "commission_rate": self.commission_rate,
            "minimum_commission_cny": self.minimum_commission_cny,
            "sell_stamp_duty_rate": self.sell_stamp_duty_rate,
            "transfer_fee_rate": self.transfer_fee_rate,
            "slippage_per_side": self.slippage_per_side,
            "board_lot": self.board_lot,
        }


DEFAULT_COST_PROFILE = CostProfile()


@dataclass(frozen=True)
class SideCost:
    """Cost breakdown for one side of a trade, in CNY."""

    commission: float
    transfer_fee: float
    stamp_duty: float
    slippage: float

    @property
    def total(self) -> float:
        return self.commission + self.transfer_fee + self.stamp_duty + self.slippage


def side_cost(
    notional: float,
    side: str,
    profile: CostProfile = DEFAULT_COST_PROFILE,
) -> SideCost:
    """Exact cost for one side of a trade (minimum commission aware).

    ``side`` is ``"BUY"``/``"SELL"`` (case-insensitive). ``notional`` is the
    traded value at the *execution* price.
    """
    if notional <= 0:
        return SideCost(0.0, 0.0, 0.0, 0.0)
    normalized = side.strip().upper()
    commission = max(profile.minimum_commission_cny, profile.commission_rate * notional)
    transfer = profile.transfer_fee_rate * notional
    slippage = profile.slippage_per_side * notional
    stamp = profile.sell_stamp_duty_rate * notional if normalized == "SELL" else 0.0
    return SideCost(
        commission=commission,
        transfer_fee=transfer,
        stamp_duty=stamp,
        slippage=slippage,
    )
