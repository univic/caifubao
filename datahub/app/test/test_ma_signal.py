# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from app.lib.signal_factory import MovingAverageSignalService
from app.lib.signal_factory.moving_average import (
    SIGNAL_MA10_CROSS_MA20,
    SIGNAL_PRICE_ABOVE_MA60,
    SIGNAL_MA20_ABOVE_MA60,
)


def test_build_signal_frame_ma10_cross_ma20():
    service = MovingAverageSignalService()
    config = service.configs[SIGNAL_MA10_CROSS_MA20]

    factor_df = pd.DataFrame(
        [
            {"date": datetime.datetime(2026, 4, 8), "ma_10": 9.9, "ma_20": 10.0},
            {"date": datetime.datetime(2026, 4, 9), "ma_10": 10.1, "ma_20": 10.0},
        ]
    ).set_index("date")

    result = service.build_signal_frame(config, factor_df)

    assert len(result) == 1
    assert result.index[0] == datetime.datetime(2026, 4, 9)
    assert result.iloc[0]["strength"] == 1.0


def test_build_signal_frame_price_above_ma60():
    service = MovingAverageSignalService()
    config = service.configs[SIGNAL_PRICE_ABOVE_MA60]

    factor_df = pd.DataFrame(
        [
            {"date": datetime.datetime(2026, 4, 10), "close": 11.0, "ma_60": 10.0},
            {"date": datetime.datetime(2026, 4, 11), "close": 9.0, "ma_60": 10.0},
        ]
    ).set_index("date")

    result = service.build_signal_frame(config, factor_df)

    assert len(result) == 1
    assert result.index[0] == datetime.datetime(2026, 4, 10)
    assert result.iloc[0]["strength"] == 10.0


def test_build_signal_frame_ma20_above_ma60():
    service = MovingAverageSignalService()
    config = service.configs[SIGNAL_MA20_ABOVE_MA60]

    factor_df = pd.DataFrame(
        [
            {"date": datetime.datetime(2026, 4, 10), "ma_20": 10.5, "ma_60": 10.0},
            {"date": datetime.datetime(2026, 4, 11), "ma_20": 9.5, "ma_60": 10.0},
        ]
    ).set_index("date")

    result = service.build_signal_frame(config, factor_df)

    assert len(result) == 1
    assert result.index[0] == datetime.datetime(2026, 4, 10)
    assert result.iloc[0]["strength"] == 5.0
