# -*- coding: utf-8 -*-
"""Synthetic tests for the delisted-panel merge."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import research_merge_delisted_panel as merge  # noqa: E402
from app.lib.factor_lab.panel import build_panel  # noqa: E402


def _rows(code, days):
    return [
        {
            "code": code,
            "date": day,
            "open": 10 + index,
            "close": 10.5 + index,
            "open_hfq": 10 + index,
            "close_hfq": 10.5 + index,
            "high_hfq": 11 + index,
            "low_hfq": 9.5 + index,
            "volume": 1000,
            "trade_amount": 10000,
            "change_rate": 1.0,
            "previous_close": 10 + index,
            "trade_status": 1,
            "is_bse": False,
            "is_st": False,
        }
        for index, day in enumerate(days)
    ]


def test_merge_adds_delisted_rows_and_st_interval():
    days = pd.date_range("2020-01-01", periods=25, freq="B")
    panel = build_panel(_rows("sh600000", days), (1, 5, 20), sessions=days)
    quotes = pd.DataFrame(
        [
            {
                "ts_code": "600001.SH",
                "trade_date": day.strftime("%Y%m%d"),
                "code": "sh600001",
                "date": day,
                "open": 5.0 + index,
                "high": 5.2 + index,
                "low": 4.8 + index,
                "close": 5.1 + index,
                "pre_close": 5.0 + index,
                "pct_chg": 1.0,
                "vol": 1000,
                "amount": 5000,
            }
            for index, day in enumerate(days)
        ]
    )
    adj = pd.DataFrame(
        {
            "ts_code": "600001.SH",
            "trade_date": [day.strftime("%Y%m%d") for day in days],
            "adj_factor": [1.0] * len(days),
        }
    )
    universe = pd.DataFrame(
        [
            {
                "code": "sh600001",
                "namechange_intervals": json.dumps(
                    [
                        {
                            "name": "*ST示例",
                            "start_date": "2020-01-01",
                            "end_date": "2020-01-10",
                        }
                    ]
                ),
            }
        ]
    )
    delisted = merge.build_delisted_panel(quotes, adj, universe, sessions=days)
    assert delisted["stock_code"].nunique() == 1
    assert delisted["is_st"].any()
    merged = merge.merge_panels(panel, delisted)
    assert merged["stock_code"].nunique() == 2
    assert len(merged) == len(panel) + len(delisted)
    assert "fwd_h20" in merged.columns
