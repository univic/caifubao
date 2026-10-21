# -*- coding: utf-8 -*-
"""Pin the financial field contract across model, runner, merge and factors."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from app.lib.factor_lab import factors
from app.model.financial import StockFinancialIndicator

SCRIPT_DIR = Path(__file__).resolve().parents[2] / "scripts"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


merge = _load("research_merge_financials")
backtest = _load("factor_lab_composite_backtest")


def test_model_runner_and_merge_share_the_quality_fields():
    from app.jobs import financial_runner

    model_fields = set(StockFinancialIndicator._fields)
    assert set(financial_runner.NUMERIC) == set(merge.FIELDS)
    assert set(financial_runner.NUMERIC) <= model_fields


def test_merged_fields_are_loaded_by_the_backtest():
    assert set(merge.FIELDS) <= set(backtest._PANEL_FIELDS)
    assert {"industry", "financial_ann_date", "financial_end_date"} <= set(
        backtest._PANEL_FIELDS
    )


def test_every_registered_quality_factor_is_exported():
    quality = {name for name in factors.REGISTRY if name.startswith("quality_")}
    assert quality == {
        "quality_roe",
        "quality_gross_margin",
        "quality_net_margin",
        "quality_asset_turnover",
        "quality_low_leverage",
        "quality_growth",
        "quality_operating_cash",
        "quality_composite",
    }
