import pandas as pd
import pyarrow.parquet as pq
import pytest

from app.jobs.autoresearch_h20_snapshot_runner import (
    COMPONENT_IDS,
    _compare_component_maps,
    _database_source,
    _pure_component_values,
    _raw_component_values,
    build_date_batch,
    build_trade_calendar,
    export_snapshot,
    reconstruct_component_rows,
    validate_export,
)


def source():
    dates = pd.date_range("2025-01-02", periods=25, freq="B")
    quotes = []
    for index, date in enumerate(dates):
        quotes.append(
            {
                "code": "sh600001",
                "date": date,
                "trade_status": 0 if index == 1 else 1,
                "change_rate": 0,
                "open_hfq": 10 + index,
                "close_hfq": 10 + index,
                "high_hfq": 11 + index,
                "low_hfq": 9 + index,
                "ma_60": 9,
                "components": {
                    name: 1
                    for name in (
                        "signal_strength",
                        "momentum",
                        "trend_alignment",
                        "breakout_or_position",
                        "industry_momentum",
                        "relative_strength",
                        "real_relative_strength",
                        "risk_penalty",
                    )
                },
            }
        )
    return {
        "quotes": quotes,
        "stocks": [{"code": "sh600001", "listing_days": 1000}],
        "calendar": quotes,
    }


def test_actual_entry_anchors_twentieth_session_exit():
    data = source()
    seen_history = []
    data["component_builder"] = lambda code, date, quote, history: (
        seen_history.append(history) or quote["components"]
    )
    frame = reconstruct_component_rows(data, [pd.Timestamp("2025-01-02")])
    row = frame.iloc[0]
    assert row.actual_entry_date == pd.Timestamp("2025-01-06")
    assert row.requested_exit_date == pd.Timestamp("2025-02-03")
    assert row.entry_blocked_sessions == 1
    assert "next_open_hfq" not in frame
    assert row.eligibility_reason == "eligible"
    assert seen_history == [[]]


def test_raw_component_values_do_not_reverse_engineer_contribution():
    values = _raw_component_values(
        [
            {
                "id": "momentum",
                "raw_value": -0.25,
                "normalized_value": 0.1,
                "weight": 15,
                "contribution": 1.5,
            },
            {
                "id": "signal_strength",
                "raw_value": ["cross"],
                "normalized_value": 0.8,
                "weight": 15,
            },
            {
                "id": "breakout_or_position",
                "raw_value": {"close": 10.0, "range_high": 12.0, "range_low": 8.0},
                "normalized_value": 0.6,
                "weight": 5,
            },
            {
                "id": "industry_momentum",
                "raw_value": None,
                "normalized_value": 0.5,
                "weight": 5,
            },
        ],
        [
            {
                "id": "risk_penalty",
                "raw_value": 99,
                "normalized_value": 0.4,
                "weight": 15,
                "contribution": -6,
            }
        ],
    )
    assert values == {
        "momentum": -0.25,
        "signal_strength": 0.8,
        "breakout_or_position": 0.6,
        "industry_momentum": 0.5,
        "risk_penalty": 0.4,
    }


def test_pure_components_use_preloaded_signal_index_and_industry_data():
    dates = pd.date_range("2024-12-02", periods=22, freq="B")
    history = [
        {
            "date": date,
            "close_hfq": 10 + index,
            "high_hfq": 11 + index,
            "low_hfq": 9 + index,
            "trade_status": 1,
            "isST": 0,
        }
        for index, date in enumerate(dates[:-1])
    ]
    quote = {
        "date": dates[-1],
        "close_hfq": 31,
        "high_hfq": 32,
        "low_hfq": 30,
        "trade_status": 1,
        "isST": 0,
    }
    index_quotes = [
        {"date": date, "close_hfq": 100 + index} for index, date in enumerate(dates)
    ]
    config = {
        "momentum_lookback": 10,
        "breakout_lookback": 20,
        "risk_lookback": 20,
        "signal_decay_max_days": 10,
        "signal_decay_factor": 0.7,
        "weights": {
            name: 12.5
            for name in (
                "signal_strength",
                "momentum",
                "trend_alignment",
                "breakout_or_position",
                "industry_momentum",
                "relative_strength",
                "real_relative_strength",
                "risk_penalty",
            )
        },
    }
    values = _pure_component_values(
        "sh600001",
        dates[-1],
        quote,
        history,
        {"ma_20": 20, "ma_60": 15, "ma_120": 10},
        [
            {
                "date": dates[-2],
                "signal_name": "cross",
                "direction": "BULLISH",
                "strength": 0.8,
            }
        ],
        index_quotes,
        {"industry_name": "Bank", "avg_score": 60, "stock_count": 5},
        config,
    )
    assert isinstance(values["signal_strength"], float)
    assert 0.0 <= values["signal_strength"] <= 1.0
    assert values["real_relative_strength"] is not None
    assert values["industry_momentum"] == pytest.approx(0.6)


@pytest.mark.parametrize(
    "index_row",
    [
        {"close": 100.0},
        {"close_hfq": 100.0},
    ],
)
def test_pure_components_accept_sparse_index_price_fields(index_row):
    dates = pd.date_range("2024-12-02", periods=12, freq="B")
    history = [
        {
            "date": date,
            "close_hfq": 10 + index,
            "high_hfq": 11 + index,
            "low_hfq": 9 + index,
            "trade_status": 1,
            "isST": 0,
        }
        for index, date in enumerate(dates[:-1])
    ]
    quote = {
        "date": dates[-1],
        "close_hfq": 21,
        "high_hfq": 22,
        "low_hfq": 20,
        "trade_status": 1,
        "isST": 0,
    }
    config = {
        "momentum_lookback": 10,
        "breakout_lookback": 10,
        "risk_lookback": 10,
        "signal_decay_max_days": 10,
        "signal_decay_factor": 0.7,
        "weights": {name: 12.5 for name in COMPONENT_IDS},
    }
    index_quotes = [{"date": date, **index_row} for date in dates]
    values = _pure_component_values(
        "sh600001",
        dates[-1],
        quote,
        history,
        None,
        [],
        index_quotes,
        None,
        config,
    )
    assert values["real_relative_strength"] is not None


def test_pure_components_accept_old_raw_only_stock_history():
    dates = pd.date_range("2024-12-02", periods=12, freq="B")
    history = [
        {
            "date": date,
            "close": 10 + index,
            "high": 11 + index,
            "low": 9 + index,
            "trade_status": 1,
            "isST": 0,
        }
        for index, date in enumerate(dates[:-1])
    ]
    quote = {
        "date": dates[-1],
        "close": 21,
        "high": 22,
        "low": 20,
        "trade_status": 1,
        "isST": 0,
    }
    config = {
        "momentum_lookback": 10,
        "breakout_lookback": 10,
        "risk_lookback": 10,
        "signal_decay_max_days": 10,
        "signal_decay_factor": 0.7,
        "weights": {name: 12.5 for name in COMPONENT_IDS},
    }
    values = _pure_component_values(
        "sh600001",
        dates[-1],
        quote,
        history,
        None,
        [],
        [{"date": date, "close": 100 + index} for index, date in enumerate(dates)],
        None,
        config,
    )
    assert values["momentum"] is not None
    assert values["breakout_or_position"] is not None
    assert values["real_relative_strength"] is not None


def test_parity_comparison_handles_numeric_and_shape_mismatches():
    left = {
        name: 1.0
        for name in (
            "signal_strength",
            "momentum",
            "trend_alignment",
            "breakout_or_position",
            "industry_momentum",
            "relative_strength",
            "real_relative_strength",
            "risk_penalty",
        )
    }
    right = dict(left)
    right["momentum"] = 1.0000001
    assert _compare_component_maps(left, right, 0.000001)[0] == 0
    right["industry_momentum"] = None
    assert _compare_component_maps(left, right, 0.000001)[0] == 1


def test_batches_are_bounded_and_duplicates_fail():
    calendar = build_trade_calendar(source()["quotes"])
    batches = build_date_batch(calendar, (calendar[0], calendar[-1]), 20)
    assert all(len(batch) <= 20 for batch in batches)
    frame = reconstruct_component_rows(source(), [calendar[0]])
    with pytest.raises(ValueError, match="duplicate"):
        validate_export(pd.concat([frame, frame]), calendar[0], calendar[0])


def test_requested_holiday_boundaries_resolve_to_completed_sessions():
    calendar = build_trade_calendar(source()["quotes"])
    batches = build_date_batch(calendar, ("2025-01-01", "2025-01-12"), 20)
    selected = [date for batch in batches for date in batch]
    assert selected[0] == pd.Timestamp("2025-01-02")
    assert selected[-1] == pd.Timestamp("2025-01-10")


def test_supplied_full_market_breadth_is_identical_across_subbatches():
    date = pd.Timestamp("2025-01-02")
    first = source()
    second = source()
    second["quotes"] = [
        {**row, "code": "sh600002", "ma_60": 1000} for row in second["quotes"]
    ]
    second["stocks"] = [{"code": "sh600002", "listing_days": 1000}]
    for subbatch in (first, second):
        subbatch["market_breadth_by_date"] = {date: 0.5}
    first_row = reconstruct_component_rows(first, [date]).iloc[0]
    second_row = reconstruct_component_rows(second, [date]).iloc[0]
    assert first_row.market_fraction_above_ma60 == 0.5
    assert second_row.market_fraction_above_ma60 == 0.5


def test_listing_age_comes_from_first_quote_date():
    data = source()
    data["stocks"] = [{"code": "sh600001", "listing_date": pd.Timestamp("2024-12-15")}]
    row = reconstruct_component_rows(data, [pd.Timestamp("2025-01-02")]).iloc[0]
    assert row.listing_days == 18
    assert row.eligibility_reason == "listing_age_below_60"


def test_historical_quote_code_survives_missing_or_inactive_stock_metadata():
    data = source()
    data["quotes"] = [{**row, "code": "sh600099"} for row in data["quotes"]]
    # The historical quote code deliberately has no current stock document;
    # an unrelated inactive metadata record must not determine membership.
    data["stocks"] = [{"code": "sh600001", "active_status": 2, "listing_days": 1000}]
    frame = reconstruct_component_rows(data, [pd.Timestamp("2025-01-02")])
    assert frame.stock_code.tolist() == ["sh600099"]
    assert frame.iloc[0].eligibility_reason == "eligible"


def test_database_source_uses_canonical_admin_authentication(monkeypatch):
    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("mongoengine.connect", fake_connect)
    monkeypatch.setenv("MONGODB_NAME", "test-db")
    monkeypatch.setenv("MONGODB_HOST", "test-host")
    monkeypatch.setenv("MONGODB_PORT", "27018")
    monkeypatch.setenv("MONGODB_USER", "test-user")
    monkeypatch.setenv("MONGODB_PASS", "test-pass")
    _database_source("2025-01-01", "2025-01-31")
    assert captured == {
        "db": "test-db",
        "host": "test-host",
        "port": 27018,
        "username": "test-user",
        "password": "test-pass",
        "authentication_source": "admin",
    }


def test_export_consumes_and_writes_batches_incrementally(tmp_path):
    data = source()

    class Provider:
        def __init__(self):
            self.calls = []

        def calendar(self):
            return build_trade_calendar(data["calendar"])

        def iter_sources(self, batch, horizon):
            self.calls.append(tuple(batch))
            yield data

    provider = Provider()
    output = tmp_path / "snapshot.parquet"
    result = export_snapshot(
        provider,
        "2025-01-02",
        "2025-02-05",
        20,
        5,
        output,
    )
    assert len(provider.calls) == 5
    assert all(len(batch) <= 5 for batch in provider.calls)
    assert pq.ParquetFile(output).num_row_groups == 5
    assert result["date_min"] == "2025-01-02"
    assert result["date_max"] == "2025-02-05"


def test_live_traversal_queries_each_code_subbatch_once_across_date_slices(tmp_path):
    data = source()

    class CodeProvider:
        def __init__(self):
            self.source_queries = 0
            self.compute_slices = []

        def calendar(self):
            return build_trade_calendar(data["calendar"])

        def iter_code_sources(self, scoring_dates, horizon):
            for suffix in ("1", "2"):
                self.source_queries += 1
                wrapped = dict(data)
                code = f"sh60000{suffix}"
                wrapped["quotes"] = [{**row, "code": code} for row in data["quotes"]]
                wrapped["stocks"] = [{"code": code, "listing_days": 1000}]

                def builder(code, date, quote, history):
                    self.compute_slices.append((code, date))
                    return quote["components"]

                wrapped["component_builder"] = builder
                yield wrapped

    provider = CodeProvider()
    output = tmp_path / "code-first.parquet"
    export_snapshot(
        provider,
        "2025-01-02",
        "2025-02-05",
        20,
        5,
        output,
    )
    assert provider.source_queries == 2
    expected_dates = len(provider.calendar())
    assert len(provider.compute_slices) == expected_dates * 2
    assert all(
        len({date for code, date in provider.compute_slices if code == target})
        == expected_dates
        for target in ("sh600001", "sh600002")
    )
    assert pq.ParquetFile(output).num_row_groups == 10
