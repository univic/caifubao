# -*- coding: utf-8 -*-
"""Synthetic tests for point-in-time industry/size-neutral quality factors."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.lib.factor_lab import factors


def _panel() -> pd.DataFrame:
    rows = []
    for day in pd.to_datetime(["2026-03-02", "2026-03-03"]):
        # The three SMALL names are the bottom 30% by market cap on each date.
        groups = {
            "SMALL": (10.0, [1.0, 2.0, 3.0]),
            "A": (1000.0, [1.0, 2.0, 3.0]),
            "B": (2000.0, [10.0, 20.0, 30.0]),
            "C": (3000.0, [100.0, 200.0, 300.0]),
        }
        for industry, (base_size, roes) in groups.items():
            for offset, roe in enumerate(roes):
                rows.append(
                    {
                        "date": day,
                        "stock_code": f"{industry}{offset}",
                        "industry": industry,
                        "total_mv": base_size + offset * 10,
                        "roe": roe,
                        "grossprofit_margin": 10.0 + offset,
                        "netprofit_margin": 5.0 + offset,
                        "assets_turn": 0.5 + offset / 10,
                        "debt_to_assets": 10.0 + offset * 10,
                        "netprofit_yoy": 5.0 + offset,
                        "or_yoy": 6.0 + offset,
                        "tr_yoy": 7.0 + offset,
                        "q_ocf_to_sales": 8.0 + offset,
                    }
                )
    return pd.DataFrame(rows)


def test_quality_rank_is_within_date_and_industry_and_size_filtered():
    panel = _panel()
    values = factors.compute(panel, "quality_roe")
    for _, group in panel.groupby("date"):
        smallest = group.nsmallest(3, "total_mv").index
        assert values.loc[smallest].isna().all()
    # Same industry, same date: lowest ROE gets the lowest within-group rank.
    first_day = panel[panel["date"] == panel["date"].min()]
    assert values.loc[first_day.index[3]] == 1 / 3  # A0
    assert values.loc[first_day.index[4]] == 2 / 3  # A1
    assert values.loc[first_day.index[5]] == 1.0  # A2


def test_low_leverage_rank_is_reversed():
    panel = _panel()
    values = factors.compute(panel, "quality_low_leverage")
    first_day = panel[panel["date"] == panel["date"].min()]
    # A0 has the lowest debt, so it gets the highest low-leverage rank.
    assert values.loc[first_day.index[3]] == 1.0
    assert values.loc[first_day.index[5]] == 1 / 3


def test_quality_composite_uses_available_legs():
    panel = _panel()
    missing = panel.index[3]
    panel.loc[missing, "grossprofit_margin"] = np.nan
    composite = factors.compute(panel, "quality_composite")
    assert not np.isnan(composite.loc[missing])
    assert np.isnan(factors.compute(panel, "quality_gross_margin").loc[missing])


def test_quality_family_is_registered():
    names = {
        "quality_roe",
        "quality_gross_margin",
        "quality_net_margin",
        "quality_asset_turnover",
        "quality_low_leverage",
        "quality_growth",
        "quality_operating_cash",
        "quality_composite",
    }
    assert names <= set(factors.REGISTRY)


def test_missing_industry_column_fails_loudly():
    panel = _panel().drop(columns=["industry"])
    try:
        factors.compute(panel, "quality_roe")
    except KeyError as exc:
        assert "industry" in str(exc)
    else:
        raise AssertionError("expected KeyError without an industry column")


def test_all_quality_legs_missing_gives_nan_composite():
    panel = _panel()
    for column in (
        "roe",
        "grossprofit_margin",
        "netprofit_margin",
        "assets_turn",
        "debt_to_assets",
        "netprofit_yoy",
        "or_yoy",
        "tr_yoy",
        "q_ocf_to_sales",
    ):
        panel[column] = np.nan
    composite = factors.compute(panel, "quality_composite")
    assert composite.isna().all()
