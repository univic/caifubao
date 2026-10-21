# -*- coding: utf-8 -*-
"""Synthetic tests for the valuation / microstructure signal factors."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.lib.factor_lab import factors


def _panel() -> pd.DataFrame:
    rows = []
    for day in pd.to_datetime(["2026-03-02", "2026-03-03"]):
        for index in range(10):
            code = f"sh{600000 + index}"
            rows.append(
                {
                    "date": day,
                    "stock_code": code,
                    "close": 10.0 + index,
                    "close_hfq": 10.0 + index,
                    "open": 9.8 + index,
                    "open_hfq": 9.8 + index,
                    "previous_close": 9.9 + index,
                    "total_mv": float(100 * (index + 1)),
                    "pe_ttm": 5.0 + index,
                    "pb": 0.5 + index / 10,
                    "ps_ttm": 1.0 + index / 10,
                    "dv_ttm": 5.0 - index / 2,
                    "turnover_rate": 1.0 + index,
                }
            )
    return pd.DataFrame(rows)


def test_valuation_rank_is_within_date_and_size_filtered():
    panel = _panel()
    ep = factors.compute(panel, "ep_value")
    assert ep.notna().sum() == 14  # 7 of 10 names per date survive the size filter
    assert ep.dropna().between(0, 1).all()
    for _, group in panel.groupby("date"):
        smallest = group.nsmallest(3, "total_mv").index
        assert ep.loc[smallest].isna().all()


def test_value_composite_uses_available_legs_when_pe_is_missing():
    panel = _panel()
    # index 5 is a size-passing name; drop its PE but keep BP/SP/DV.
    panel.loc[5, "pe_ttm"] = np.nan
    composite = factors.compute(panel, "value_composite")
    assert not np.isnan(composite.loc[5])
    assert np.isnan(factors.compute(panel, "ep_value").loc[5])


def test_value_composite_and_microstructure_ranks():
    panel = _panel()
    composite = factors.compute(panel, "value_composite")
    assert composite.notna().sum() == 14
    for name, expected in (
        ("intraday_rank", 20),
        ("overnight_rank", 10),
        ("low_turnover", 20),
    ):
        values = factors.compute(panel, name)
        assert values.notna().sum() == expected
        assert values.dropna().between(0, 1).all()
