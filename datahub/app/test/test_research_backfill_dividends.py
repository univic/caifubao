# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "research_backfill_dividends.py"
)
spec = importlib.util.spec_from_file_location("research_backfill_dividends", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


def _frame(rows):
    return pd.DataFrame(rows)


def test_normalize_requires_implemented_event_and_ex_date():
    row = {
        "ts_code": "600519.SH",
        "ann_date": "20260612",
        "end_date": "20251231",
        "div_proc": "实施",
        "stk_div": 0.0,
        "stk_bo_rate": 0.0,
        "stk_co_rate": 0.0,
        "cash_div": 28.0,
        "cash_div_tax": 28.0,
        "record_date": "20260625",
        "ex_date": "20260626",
        "pay_date": "20260626",
    }
    event = module.normalize_event(row)
    assert event["code"] == "sh600519"
    assert event["ex_date"] == datetime.datetime(2026, 6, 26)
    assert event["cash_per_share"] == 28.0
    assert event["share_multiplier"] == 1.0
    assert module.normalize_event({**row, "div_proc": "预案"}) is None
    assert module.normalize_event({**row, "ex_date": None}) is None


def test_share_multiplier_includes_split_and_transfer():
    event = module.normalize_event(
        {
            "ts_code": "600519.SH",
            "ann_date": "20260612",
            "end_date": "20251231",
            "div_proc": "实施",
            "stk_div": 0.5,
            "stk_bo_rate": 0.2,
            "stk_co_rate": 0.3,
            "cash_div": 1.0,
            "cash_div_tax": 0.8,
            "record_date": "20260625",
            "ex_date": "20260626",
            "pay_date": "20260626",
        }
    )
    assert event["share_multiplier"] == 1.5
    assert event["cash_per_share"] == 0.8


def test_multiplier_uses_total_stk_div_not_component_sum():
    event = module.normalize_event(
        {
            "ts_code": "600519.SH",
            "ann_date": "20260612",
            "end_date": "20251231",
            "div_proc": "实施",
            "stk_div": 0.4,
            "stk_bo_rate": 0.4,
            "stk_co_rate": 0.0,
            "cash_div": 0.0,
            "cash_div_tax": 0.0,
            "record_date": "20260625",
            "ex_date": "20260626",
            "pay_date": "20260626",
        }
    )
    assert event["share_multiplier"] == 1.4


def test_missing_transfer_rate_is_zero_not_nan():
    event = module.normalize_event(
        {
            "ts_code": "600519.SH",
            "ann_date": "20260612",
            "end_date": "20251231",
            "div_proc": "实施",
            "stk_div": 0.0,
            "stk_bo_rate": float("nan"),
            "stk_co_rate": float("nan"),
            "cash_div": 1.0,
            "cash_div_tax": 1.0,
            "record_date": "20260625",
            "ex_date": "20260626",
            "pay_date": "20260626",
        }
    )
    assert event["share_multiplier"] == 1.0
    assert event["stk_bo_rate"] == 0.0


def test_backfill_filters_ex_date_window_and_dedupes():
    frame = _frame(
        [
            {
                "ts_code": "600519.SH",
                "ann_date": "20260612",
                "end_date": "20251231",
                "div_proc": "实施",
                "stk_div": 0.0,
                "stk_bo_rate": 0.0,
                "stk_co_rate": 0.0,
                "cash_div": 28.0,
                "cash_div_tax": 28.0,
                "record_date": "20260625",
                "ex_date": "20260626",
                "pay_date": "20260626",
            },
            {
                "ts_code": "600519.SH",
                "ann_date": "20260620",
                "end_date": "20251231",
                "div_proc": "实施",
                "stk_div": 0.0,
                "stk_bo_rate": 0.0,
                "stk_co_rate": 0.0,
                "cash_div": 28.0,
                "cash_div_tax": 28.0,
                "record_date": "20260625",
                "ex_date": "20260626",
                "pay_date": "20260626",
            },
            {
                "ts_code": "600519.SH",
                "ann_date": "20200101",
                "end_date": "20191231",
                "div_proc": "实施",
                "stk_div": 0.0,
                "stk_bo_rate": 0.0,
                "stk_co_rate": 0.0,
                "cash_div": 1.0,
                "cash_div_tax": 1.0,
                "record_date": "20200102",
                "ex_date": "20200103",
                "pay_date": "20200103",
            },
        ]
    )
    pro = SimpleNamespace(dividend=lambda **kwargs: frame)
    result, summary = module.backfill(
        ["sh600519"], pro=pro, from_date="20200101", to_date="20260918"
    )
    assert len(result) == 2  # deduped 2026 event + 2020 event
    assert summary["events"] == 2
    assert summary["failed_codes"] == []
    assert result["cash_per_share"].sum() == 29.0
