# -*- coding: utf-8 -*-
"""Synthetic test for the valuation merge."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import research_merge_daily_basic as merge  # noqa: E402


def test_merge_keeps_panel_rows_and_attaches_valuation():
    panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-02", "2026-03-02", "2026-03-03"]),
            "stock_code": ["sh600000", "sz000001", "sh600000"],
            "close": [10.0, 11.0, 10.5],
        }
    )
    basic = pd.DataFrame(
        {
            "code": ["sh600000", "sz000001"],
            "date": pd.to_datetime(["2026-03-02", "2026-03-02"]),
            "pe_ttm": [8.0, 12.0],
            "pb": [1.0, 1.5],
            "ps_ttm": [2.0, 3.0],
            "dv_ttm": [3.0, 2.0],
            "total_mv": [1000.0, 2000.0],
            "circ_mv": [900.0, 1800.0],
            "turnover_rate": [1.0, 2.0],
        }
    )
    merged = merge.merge_basic(panel, basic)
    assert len(merged) == len(panel)
    sh600000 = merged.loc[merged["stock_code"] == "sh600000"].sort_values("date")
    assert sh600000["pe_ttm"].iloc[0] == 8.0
    assert pd.isna(sh600000["pe_ttm"].iloc[1])
    assert "total_mv" in merged.columns


def test_duplicate_basic_rows_do_not_duplicate_panel_rows():
    panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-02"]),
            "stock_code": ["sh600000"],
            "close": [10.0],
        }
    )
    row = {
        "code": "sh600000",
        "date": pd.to_datetime("2026-03-02"),
        "pe_ttm": 8.0,
        "pb": 1.0,
        "ps_ttm": 2.0,
        "dv_ttm": 3.0,
        "total_mv": 1000.0,
        "circ_mv": 900.0,
        "turnover_rate": 1.0,
    }
    merged = merge.merge_basic(panel, pd.DataFrame([row, row]))
    assert len(merged) == 1


def test_stale_valuation_columns_are_rejected():
    import pytest

    panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-02"]),
            "stock_code": ["sh600000"],
            "close": [10.0],
            "pe_ttm": [8.0],
        }
    )
    with pytest.raises(ValueError, match="already carries valuation"):
        merge.merge_basic(panel, pd.DataFrame(columns=["code", "date"]))
