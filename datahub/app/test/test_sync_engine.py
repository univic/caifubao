# -*- coding: utf-8 -*-

from app.lib.datahub.sync_engine import _resolve_sync_collections


def test_resolve_sync_collections_keeps_signal_in_dev():
    assert _resolve_sync_collections(None, is_dev_environment=True) == [
        "stock_daily_quote",
        "stock_factor_daily",
        "stock_signal_daily",
        "finance_market",
        "stock_industry",
    ]


def test_resolve_sync_collections_skips_signal_outside_dev():
    assert _resolve_sync_collections(None, is_dev_environment=False) == [
        "stock_daily_quote",
        "stock_factor_daily",
        "finance_market",
        "stock_industry",
    ]


def test_resolve_sync_collections_skips_signal_alias_outside_dev():
    assert _resolve_sync_collections(
        ["quote", "signal"],
        is_dev_environment=False,
    ) == ["stock_daily_quote"]
