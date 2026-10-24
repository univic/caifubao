# -*- coding: utf-8 -*-
"""Frozen pure execution vectors for research and future independent trading.

No trading runtime/DB is imported. Both domains should evaluate these exact
inputs, not call each other's simulators.
"""

GOLDEN_EXECUTION_V1 = (
    {
        "code": "sh600000",
        "side": "BUY",
        "open": 10.0,
        "quantity": 100,
        "price": 10.01,
        "commission": 5.0,
        "transfer_fee": 0.01001,
        "stamp_duty": 0.0,
        "board_minimum": 100,
        "board_step": 100,
    },
    {
        "code": "sh600000",
        "side": "SELL",
        "open": 10.0,
        "quantity": 100,
        "price": 9.99,
        "commission": 5.0,
        "transfer_fee": 0.00999,
        "stamp_duty": 0.4995,
        "board_minimum": 100,
        "board_step": 100,
    },
    {
        "code": "sh688001",
        "side": "BUY",
        "open": 10.0,
        "quantity": 201,
        "price": 10.01,
        "commission": 5.0,
        "transfer_fee": 0.0201201,
        "stamp_duty": 0.0,
        "board_minimum": 200,
        "board_step": 1,
    },
)
