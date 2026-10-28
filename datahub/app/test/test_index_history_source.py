# -*- coding: utf-8 -*-
"""Index history source selection.

The daily index job sweeps ~560 indices. The akshare/Sina endpoint starts
refusing calls after a few hundred requests, which trips the source-outage
circuit breaker and fails the whole job. When the environment pins tushare as
the history source, index history must come from tushare too.
"""

import pandas
import pytest

from app.lib.datahub.data_source.handler import zh_a_daily
from app.lib.datahub.data_source.interface import tushare_interface


def _tushare_frame():
    return pandas.DataFrame(
        {
            "ts_code": ["399017.SZ", "399017.SZ"],
            "trade_date": ["20261027", "20261028"],
            "close": [4600.0, 4644.6276],
            "open": [4580.0, 4570.3892],
            "high": [4610.0, 4650.0],
            "low": [4570.0, 4560.0],
            "vol": [100.0, 200.0],
            "amount": [1000.0, 2000.0],
        }
    )


def test_internal_index_codes_map_to_tushare_ts_codes():
    assert tushare_interface.to_tushare_ts_code("sh000300") == "000300.SH"
    assert tushare_interface.to_tushare_ts_code("sz399001") == "399001.SZ"
    assert tushare_interface.to_tushare_ts_code("sz399017") == "399017.SZ"


def test_pinned_tushare_source_serves_index_history(monkeypatch):
    monkeypatch.setenv("DATAHUB_STOCK_HISTORY_SOURCE", "tushare")
    calls = []

    def fake_index_daily(ts_code, start_date=None, end_date=None):
        calls.append((ts_code, start_date, end_date))
        return _tushare_frame()

    monkeypatch.setattr(tushare_interface, "tushare_index_daily", fake_index_daily)

    def boom(_code):
        raise AssertionError("akshare index endpoint must not be called")

    monkeypatch.setattr(
        zh_a_daily.interface.akshare_interface, "stock_zh_index_daily", boom
    )

    frame = zh_a_daily.get_zh_a_index_hist_daily_quote(
        "sz399017", start_date="2026-10-01", end_date="2026-10-28"
    )

    assert calls == [("399017.SZ", "20261001", "20261028")]
    # the legacy incremental path re-sorts columns, so only membership matters
    assert set(frame.columns) == set(zh_a_daily._TUSHARE_INDEX_COLUMNS)
    assert frame["date"].is_monotonic_increasing
    assert frame["close"].iloc[-1] == pytest.approx(4644.6276)


def test_default_source_still_uses_akshare(monkeypatch):
    monkeypatch.delenv("DATAHUB_STOCK_HISTORY_SOURCE", raising=False)
    called = []

    def fake_akshare(code):
        called.append(code)
        return pandas.DataFrame(
            {
                "date": [pandas.Timestamp("2026-10-28")],
                "open": [1.0],
                "high": [2.0],
                "low": [0.5],
                "close": [1.5],
                "volume": [10.0],
            }
        )

    monkeypatch.setattr(
        zh_a_daily.interface.akshare_interface, "stock_zh_index_daily", fake_akshare
    )

    frame = zh_a_daily.get_zh_a_index_hist_daily_quote("sz399017")

    assert called == ["sz399017"]
    assert list(frame.columns) == ["date", "open", "high", "low", "close", "volume"]


def test_empty_tushare_history_keeps_the_expected_columns(monkeypatch):
    monkeypatch.setenv("DATAHUB_STOCK_HISTORY_SOURCE", "tushare")
    monkeypatch.setattr(
        tushare_interface,
        "tushare_index_daily",
        lambda *args, **kwargs: pandas.DataFrame(),
    )

    frame = zh_a_daily.get_zh_a_index_hist_daily_quote("sz399101")

    assert frame.empty
    assert list(frame.columns) == ["date", "open", "high", "low", "close", "volume"]


def test_tushare_frame_missing_columns_fails_loudly():
    with pytest.raises(ValueError, match="missing columns"):
        zh_a_daily._normalise_tushare_index_frame(
            pandas.DataFrame({"trade_date": ["20261028"]})
        )
