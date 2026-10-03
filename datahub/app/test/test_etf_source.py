"""Contract tests for the frozen Tushare-shaped ETF source adapter."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from app.lib.strategy_engine.etf_benchmark import replay_etf_benchmark
from app.lib.strategy_engine.etf_source import adapt_etf_source


EXAMPLES = Path(__file__).parents[2] / "examples"
SOURCE_FIXTURE = EXAMPLES / "etf-source-100k.json"
NATIVE_FIXTURE = EXAMPLES / "etf-benchmark-100k.json"


def _load(path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _source():
    return _load(SOURCE_FIXTURE)


def _native():
    return _load(NATIVE_FIXTURE)


def _adapted(payload):
    native, provenance = adapt_etf_source(deepcopy(payload))
    return native, provenance


def _replay(native, tmp_path):
    return replay_etf_benchmark(
        native,
        halt_path=str(tmp_path / "source-test-halt.json"),
    )


def _curve_by_date(result):
    return {row["date"]: row for row in result["curve"]}


def _weekend_source():
    payload = _source()
    payload["start_date"] = "20260904"
    payload["end_date"] = "20260907"
    payload["calendar"] = [
        {"exchange": "SSE", "cal_date": "20260904", "is_open": "1"},
        {"exchange": "SSE", "cal_date": "20260905", "is_open": "0"},
        {"exchange": "SSE", "cal_date": "20260906", "is_open": "0"},
        {"exchange": "SSE", "cal_date": "20260907", "is_open": "1"},
    ]
    payload["daily"] = [
        {
            "ts_code": "510300.SH",
            "trade_date": "20260904",
            "open": "4.000",
            "close": "4.000",
            "vol": "100",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "20260907",
            "open": "4.000",
            "close": "4.100",
            "vol": "100",
        },
    ]
    payload["execution"] = [
        {
            "ts_code": "510300.SH",
            "trade_date": "20260904",
            "trade_status": 1,
            "up_limit": "4.400",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "20260907",
            "trade_status": 1,
            "up_limit": "4.400",
        },
    ]
    return payload


def test_source_fixture_adapts_and_replays_identically_to_native_fixture(tmp_path):
    source = _source()
    adapted, provenance = _adapted(source)
    source_result = _replay(adapted, tmp_path)
    native_result = _replay(_native(), tmp_path)

    assert adapted["schema_version"] == "etf-benchmark-v1"
    assert adapted["price_basis"] == "raw"
    assert adapted["sessions"] == [
        "2026-09-01",
        "2026-09-02",
        "2026-09-03",
        "2026-09-04",
    ]
    assert source_result == native_result
    assert provenance["adapter_version"] == "etf-source-v1"
    assert provenance["status_basis"] == "operator_opening_declaration"
    assert len(provenance["source_hash"]) == 64


def test_source_row_order_does_not_change_native_replay(tmp_path):
    shuffled = _source()
    shuffled["calendar"] = list(reversed(shuffled["calendar"]))
    shuffled["daily"] = list(reversed(shuffled["daily"]))
    shuffled["execution"] = list(reversed(shuffled["execution"]))

    native, _ = _adapted(_source())
    shuffled_native, _ = _adapted(shuffled)
    assert native == shuffled_native
    assert _replay(native, tmp_path) == _replay(shuffled_native, tmp_path)


def test_open_session_without_daily_quote_remains_in_replay(tmp_path):
    source = _source()
    source["daily"] = [
        row for row in source["daily"] if row["trade_date"] != "20260902"
    ]
    native, _ = _adapted(source)
    missing = next(row for row in native["quotes"] if row["date"] == "2026-09-02")
    assert missing["open"] is None
    assert missing["close"] is None
    assert native["sessions"] == [
        "2026-09-01",
        "2026-09-02",
        "2026-09-03",
        "2026-09-04",
    ]

    result = _replay(native, tmp_path)
    rows = _curve_by_date(result)
    assert list(rows) == native["sessions"][1:]
    assert rows["2026-09-02"]["action"] == "BLOCKED"
    assert result["trades"][0]["date"] == "2026-09-04"


def test_daily_volume_never_controls_fill_but_changes_source_provenance(tmp_path):
    base = _source()
    changed = deepcopy(base)
    changed["daily"][1]["vol"] = "0"

    native_base, provenance_base = _adapted(base)
    native_changed, provenance_changed = _adapted(changed)
    assert native_base == native_changed
    assert _replay(native_base, tmp_path) == _replay(native_changed, tmp_path)
    assert provenance_base["source_hash"] != provenance_changed["source_hash"]


def test_zero_raw_prices_become_unavailable_without_price_inference(tmp_path):
    source = _source()
    source["daily"][1]["open"] = "0"
    source["daily"][1]["close"] = "0"
    native, _ = _adapted(source)
    quote = next(row for row in native["quotes"] if row["date"] == "2026-09-02")
    assert quote["open"] is None
    assert quote["close"] is None
    result = _replay(native, tmp_path)
    assert _curve_by_date(result)["2026-09-02"]["action"] == "BLOCKED"
    assert result["trades"][0]["date"] == "2026-09-04"


def test_missing_execution_is_unknown_and_blocks_until_a_valid_predecessor(tmp_path):
    source = _source()
    source["execution"] = [
        row for row in source["execution"] if row["trade_date"] != "20260902"
    ]
    native, _ = _adapted(source)
    quote = next(row for row in native["quotes"] if row["date"] == "2026-09-02")
    assert quote["trade_status"] is None
    assert quote["upper_limit"] is None

    result = _replay(native, tmp_path)
    rows = _curve_by_date(result)
    assert rows["2026-09-02"]["action"] == "BLOCKED"
    assert rows["2026-09-03"]["action"] == "BLOCKED"
    assert result["trades"][0]["date"] == "2026-09-04"


def test_missing_execution_limit_blocks_without_inference(tmp_path):
    source = _source()
    source["execution"][1].pop("up_limit")
    native, _ = _adapted(source)
    quote = next(row for row in native["quotes"] if row["date"] == "2026-09-02")
    assert quote["trade_status"] == 1
    assert quote["upper_limit"] is None

    result = _replay(native, tmp_path)
    rows = _curve_by_date(result)
    assert rows["2026-09-02"]["action"] == "BLOCKED"
    assert rows["2026-09-03"]["action"] == "BUY"


@pytest.mark.parametrize("field", ["daily", "execution"])
def test_calendar_closed_session_rows_are_rejected(field):
    source = _weekend_source()
    row = deepcopy(source[field][0])
    row["trade_date"] = "20260905"
    source[field].append(row)
    with pytest.raises(ValueError):
        adapt_etf_source(source)


@pytest.mark.parametrize("field", ["daily", "execution"])
def test_out_of_range_rows_are_rejected(field):
    source = _source()
    row = deepcopy(source[field][0])
    row["trade_date"] = "20260831"
    source[field].append(row)
    with pytest.raises(ValueError):
        adapt_etf_source(source)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["calendar"].pop(1),
        lambda p: p["calendar"].append(deepcopy(p["calendar"][0])),
        lambda p: p["calendar"][0].update(exchange="SZSE"),
        lambda p: p["calendar"][0].update(is_open=0),
        lambda p: p["calendar"][-1].update(is_open=0),
        lambda p: p["daily"][0].update(ts_code="000001.SZ"),
        lambda p: p["execution"][0].update(ts_code="000001.SZ"),
        lambda p: p["daily"][0].update(close_qfq="4.000"),
        lambda p: p["configuration"].update(corporate_actions=[{"date": "20260902"}]),
    ],
)
def test_source_rejects_calendar_identity_range_and_adjusted_data(mutate):
    source = _source()
    mutate(source)
    with pytest.raises(ValueError):
        adapt_etf_source(source)


def test_standard_raw_daily_columns_are_allowed():
    source = _source()
    source["daily"][0].update(
        high="4.100",
        low="3.900",
        pre_close="3.950",
        change="0.050",
        pct_chg="1.2658",
        amount="123456.78",
    )
    native, _ = _adapted(source)
    assert native["quotes"][0]["open"] == "4"


@pytest.mark.parametrize("field", ["daily", "execution"])
def test_future_out_of_range_rows_are_rejected(field):
    source = _source()
    row = deepcopy(source[field][0])
    row["trade_date"] = "20990102"
    source[field].append(row)
    with pytest.raises(ValueError):
        adapt_etf_source(source)
