import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


def test_build_fq_factor_frame_for_full_history():
    from app.lib.factor_factory import FQFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "previous_close": 10.0,
            },
            {
                "date": datetime.datetime(2024, 1, 9),
                "code": "sh600000",
                "open": 10.6,
                "high": 11.2,
                "low": 10.2,
                "close": 11.0,
                "previous_close": 10.5,
            },
        ]
    ).set_index("date")

    # Real adj_factor frame: constant factor (no dividend in window)
    adj_df = pd.DataFrame(
        [
            {"trade_date": "20240108", "adj_factor": 2.0},
            {"trade_date": "20240109", "adj_factor": 2.0},
        ]
    )

    result = FQFactorService.build_fq_factor_frame(quote_df, adj_factor_df=adj_df)

    # factor is the REAL tushare factor, not a cumulative close ratio
    assert result.loc[datetime.datetime(2024, 1, 8), "fq_factor"] == 2.0
    assert result.loc[datetime.datetime(2024, 1, 8), "close_hfq"] == 21.0  # 10.5*2
    assert result.loc[datetime.datetime(2024, 1, 9), "fq_factor"] == 2.0
    assert result.loc[datetime.datetime(2024, 1, 9), "close_hfq"] == 22.0  # 11.0*2
    # open/high/low scale by the same ratio as close
    assert result.loc[datetime.datetime(2024, 1, 8), "open_hfq"] == 20.0
    assert result.loc[datetime.datetime(2024, 1, 8), "high_hfq"] == 22.0
    assert result.loc[datetime.datetime(2024, 1, 8), "low_hfq"] == 19.0


def test_build_fq_factor_frame_dividend_change():
    from app.lib.factor_factory import FQFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "previous_close": 10.0,
            },
            {
                "date": datetime.datetime(2024, 1, 9),
                "code": "sh600000",
                "open": 10.6,
                "high": 11.2,
                "low": 10.2,
                "close": 11.0,
                "previous_close": 10.5,
            },
        ]
    ).set_index("date")

    # ex-dividend on 01-09: factor changes once, exactly
    adj_df = pd.DataFrame(
        [
            {"trade_date": "20240108", "adj_factor": 2.0},
            {"trade_date": "20240109", "adj_factor": 1.8},
        ]
    )

    result = FQFactorService.build_fq_factor_frame(quote_df, adj_factor_df=adj_df)

    assert result.loc[datetime.datetime(2024, 1, 8), "fq_factor"] == 2.0
    assert result.loc[datetime.datetime(2024, 1, 8), "close_hfq"] == 21.0
    assert result.loc[datetime.datetime(2024, 1, 9), "fq_factor"] == 1.8
    assert result.loc[datetime.datetime(2024, 1, 9), "close_hfq"] == 19.8  # 11.0*1.8


def test_build_fq_factor_frame_missing_factor_rows_carry_forward():
    from app.lib.factor_factory import FQFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "previous_close": 10.0,
            },
            {
                "date": datetime.datetime(2024, 1, 9),
                "code": "sh600000",
                "open": 10.6,
                "high": 11.2,
                "low": 10.2,
                "close": 11.0,
                "previous_close": 10.5,
            },
        ]
    ).set_index("date")

    # only the first day has a factor; second day must carry it forward
    adj_df = pd.DataFrame([{"trade_date": "20240108", "adj_factor": 3.0}])

    result = FQFactorService.build_fq_factor_frame(quote_df, adj_factor_df=adj_df)

    assert result.loc[datetime.datetime(2024, 1, 8), "fq_factor"] == 3.0
    assert result.loc[datetime.datetime(2024, 1, 9), "fq_factor"] == 3.0
    assert result.loc[datetime.datetime(2024, 1, 9), "close_hfq"] == 33.0


def test_build_fq_factor_frame_rejects_missing_factor_data():
    from app.lib.factor_factory import FQFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "previous_close": 10.0,
            }
        ]
    ).set_index("date")

    with pytest.raises(RuntimeError, match="adj_factor unavailable"):
        FQFactorService.build_fq_factor_frame(quote_df, adj_factor_df=None)


def test_factor_before_first_factor_row_uses_earliest_factor():
    from app.lib.factor_factory import FQFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 6),
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "previous_close": 10.0,
            }
        ]
    ).set_index("date")

    # factor series starts 01-08 (2.0 -> 1.8); a quote BEFORE the first
    # factor row must use the earliest factor (2.0), not the minimum value
    # (1.8)
    adj_df = pd.DataFrame(
        [
            {"trade_date": "20240108", "adj_factor": 2.0},
            {"trade_date": "20240109", "adj_factor": 1.8},
        ]
    )

    result = FQFactorService.build_fq_factor_frame(quote_df, adj_factor_df=adj_df)

    assert result.loc[datetime.datetime(2024, 1, 6), "fq_factor"] == 2.0
    assert result.loc[datetime.datetime(2024, 1, 6), "close_hfq"] == 21.0


@pytest.mark.parametrize("invalid_factor", [float("nan"), 0.0, -1.0, float("inf")])
def test_build_fq_factor_frame_rejects_all_invalid_factors(invalid_factor):
    from app.lib.factor_factory import FQFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "previous_close": 10.0,
            }
        ]
    ).set_index("date")

    # NaN adj_factor must not be converted into a plausible factor=1 result.
    adj_df = pd.DataFrame([{"trade_date": "20240108", "adj_factor": invalid_factor}])

    with pytest.raises(RuntimeError, match="no valid factor rows"):
        FQFactorService.build_fq_factor_frame(quote_df, adj_factor_df=adj_df)


def test_update_code_fails_without_writing_when_adj_factor_unavailable():
    """A missing adj_factor response must fail instead of silently succeeding."""

    from app.lib.factor_factory import FQFactorService

    class FakeQuery:
        def __init__(self, results):
            self.results = results

        def only(self, *fields):
            return self

        def first(self):
            return self.results[0] if self.results else None

        def order_by(self, *fields):
            return self

        def as_pymongo(self):
            import pandas as pd

            return pd.DataFrame(
                [
                    {
                        "code": "sh600000",
                        "date": datetime.datetime(2024, 1, 8),
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.5,
                        "close": 10.5,
                        "previous_close": 10.0,
                    }
                ]
            )

    class FakeQuoteModel:
        _collection_ops = []

        @staticmethod
        def objects(**kwargs):
            # fq_factor__ne=1.0 filter -> existing non-1 history present
            if "fq_factor__ne" in kwargs:
                return FakeQuery([SimpleNamespace()])
            # plain filter -> quote rows
            return FakeQuery([])

        @staticmethod
        def _get_collection():
            return SimpleNamespace(bulk_write=lambda ops, ordered=False: None)

    class FakeStockModel:
        @staticmethod
        def objects(**kwargs):
            return FakeQuery(
                [
                    SimpleNamespace(
                        code=kwargs["code"],
                        name="测试样本",
                        object_type="individual_stock",
                        data_capabilities=SimpleNamespace(
                            daily_quote=True,
                            fq_factor=True,
                            ma_factor=True,
                        ),
                    )
                ]
            )

    class FakeService(FQFactorService):
        def _load_quote_df(self, code, date_gt=None):
            return pd.DataFrame(
                [
                    {
                        "code": "sh600000",
                        "date": datetime.datetime(2024, 1, 8),
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.5,
                        "close": 10.5,
                        "previous_close": 10.0,
                    }
                ]
            ).set_index("date")

        def _load_adj_factor_df(self, code, quote_df):
            # simulate tushare outage / empty response
            return None

    service = FakeService(quote_model=FakeQuoteModel, stock_model=FakeStockModel)
    with pytest.raises(RuntimeError, match="adj_factor unavailable"):
        service.update_code("sh600000")

    assert FakeQuoteModel._collection_ops == []


def test_build_market_snapshot_frame_joins_all_quotes_and_ignores_extra_factors():
    from app.lib.factor_factory import FQFactorService

    target = datetime.datetime(2026, 8, 27)
    quote_df = pd.DataFrame(
        [
            {
                "date": target,
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "previous_close": 10.0,
            },
            {
                "date": target,
                "code": "sz000001",
                "open": 12.0,
                "high": 13.0,
                "low": 11.0,
                "close": 12.5,
                "previous_close": 12.0,
            },
        ]
    )
    factor_df = pd.DataFrame(
        [
            {"ts_code": "600000.SH", "trade_date": "20260827", "adj_factor": 2.0},
            {"ts_code": "000001.SZ", "trade_date": "20260827", "adj_factor": 3.0},
            {"ts_code": "600519.SH", "trade_date": "20260827", "adj_factor": 4.0},
        ]
    )

    result = FQFactorService.build_market_snapshot_frame(quote_df, factor_df, target)

    assert result["code"].tolist() == ["sh600000", "sz000001"]
    assert result["fq_factor"].tolist() == [2.0, 3.0]
    assert result["close_hfq"].tolist() == [21.0, 37.5]


@pytest.mark.parametrize(
    ("factor_rows", "message"),
    [
        (
            [{"ts_code": "600000.SH", "trade_date": "20260826", "adj_factor": 2.0}],
            "mismatched dates",
        ),
        (
            [
                {"ts_code": "600000.SH", "trade_date": "20260827", "adj_factor": 2.0},
                {"ts_code": "600000.SH", "trade_date": "20260827", "adj_factor": 2.0},
            ],
            "duplicate codes",
        ),
        (
            [{"ts_code": "600000.SH", "trade_date": "20260827", "adj_factor": 0.0}],
            "missing valid factors",
        ),
    ],
)
def test_build_market_snapshot_frame_rejects_invalid_batch(factor_rows, message):
    from app.lib.factor_factory import FQFactorService

    target = datetime.datetime(2026, 8, 27)
    quote_df = pd.DataFrame(
        [
            {
                "date": target,
                "code": "sh600000",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "previous_close": 10.0,
            }
        ]
    )

    with pytest.raises(RuntimeError, match=message):
        FQFactorService.build_market_snapshot_frame(
            quote_df, pd.DataFrame(factor_rows), target
        )


def test_update_market_uses_one_snapshot_and_one_bulk_write_for_same_date():
    from app.lib.factor_factory import FQFactorService

    target = datetime.datetime(2026, 8, 27)

    class FakeQuoteModel:
        writes = []

        @staticmethod
        def _get_collection():
            return SimpleNamespace(
                bulk_write=lambda operations, ordered=False: (
                    FakeQuoteModel.writes.append(operations)
                )
            )

    class FakeFQFactorService(FQFactorService):
        snapshot_calls = 0
        refreshed_codes = []

        def _get_market_update_plan(self, market=None):
            return {"sh600000": target, "sz000001": target}, []

        def _load_market_quote_snapshot(self, codes, target_date):
            return pd.DataFrame(
                [
                    {
                        "date": target,
                        "code": "sh600000",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "previous_close": 10.0,
                    },
                    {
                        "date": target,
                        "code": "sz000001",
                        "open": 12.0,
                        "high": 13.0,
                        "low": 11.0,
                        "close": 12.5,
                        "previous_close": 12.0,
                    },
                ]
            )

        def _load_market_adj_factor_snapshot(self, target_date):
            self.snapshot_calls += 1
            return pd.DataFrame(
                [
                    {
                        "ts_code": "600000.SH",
                        "trade_date": "20260827",
                        "adj_factor": 2.0,
                    },
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260827",
                        "adj_factor": 3.0,
                    },
                ]
            )

        def _refresh_market_snapshot_statuses(self, codes):
            self.refreshed_codes.extend(codes)

    service = FakeFQFactorService(quote_model=FakeQuoteModel)
    result = service.update_market()

    assert service.snapshot_calls == 1
    assert len(FakeQuoteModel.writes) == 1
    assert len(FakeQuoteModel.writes[0]) == 2
    assert {operation._filter["date"] for operation in FakeQuoteModel.writes[0]} == {
        target
    }
    assert service.refreshed_codes == ["sh600000", "sz000001"]
    assert result == {
        "pulled_count": 2,
        "written_count": 2,
        "failed_count": 0,
        "snapshot_count": 1,
        "factor_count": 2,
        "matched_count": 2,
        "ignored_extra_count": 0,
        "failed_codes": [],
    }


def test_update_market_validation_failure_writes_nothing():
    from app.lib.factor_factory import FQFactorService

    target = datetime.datetime(2026, 8, 27)

    class FakeQuoteModel:
        writes = []

        @staticmethod
        def _get_collection():
            return SimpleNamespace(
                bulk_write=lambda operations, ordered=False: (
                    FakeQuoteModel.writes.append(operations)
                )
            )

    class FakeFQFactorService(FQFactorService):
        def _get_market_update_plan(self, market=None):
            return {"sh600000": target, "sz000001": target}, []

        def _load_market_quote_snapshot(self, codes, target_date):
            return pd.DataFrame(
                [
                    {
                        "date": target,
                        "code": "sh600000",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "previous_close": 10.0,
                    },
                    {
                        "date": target,
                        "code": "sz000001",
                        "open": 12.0,
                        "high": 13.0,
                        "low": 11.0,
                        "close": 12.5,
                        "previous_close": 12.0,
                    },
                ]
            )

        def _load_market_adj_factor_snapshot(self, target_date):
            return pd.DataFrame(
                [{"ts_code": "600000.SH", "trade_date": "20260827", "adj_factor": 2.0}]
            )

    service = FakeFQFactorService(quote_model=FakeQuoteModel)
    with pytest.raises(RuntimeError, match="sz000001"):
        service.update_market()

    assert FakeQuoteModel.writes == []


def test_market_update_plan_uses_snapshot_only_for_one_trading_day_gap():
    from app.lib.factor_factory import FQFactorService

    friday = datetime.datetime(2026, 8, 21)
    monday = datetime.datetime(2026, 8, 24)
    tuesday = datetime.datetime(2026, 8, 25)
    market = SimpleNamespace(trade_calendar=[friday, monday, tuesday])

    assert FQFactorService._is_next_trading_day(friday, monday, market) is True
    assert FQFactorService._is_next_trading_day(friday, tuesday, market) is False
    assert FQFactorService._is_next_trading_day(None, monday, market) is False


def test_update_market_keeps_full_history_path_for_repairs():
    from app.lib.factor_factory import FQFactorService

    class FakeService(FQFactorService):
        updated_codes = []

        def _get_market_update_plan(self, market=None):
            return {}, ["new-code", "gap-code", "bad-code"]

        def update_code(self, code):
            self.updated_codes.append(code)
            if code == "bad-code":
                raise RuntimeError("source unavailable")
            return {"code": "GOOD", "written_count": 4, "message": None}

    service = FakeService()
    with pytest.raises(RuntimeError, match="bad-code"):
        service.update_market()

    assert service.updated_codes == ["new-code", "gap-code", "bad-code"]


def test_update_market_missing_planned_quote_fails_before_factor_request():
    from app.lib.factor_factory import FQFactorService

    target = datetime.datetime(2026, 8, 27)

    class FakeService(FQFactorService):
        factor_calls = 0

        def _get_market_update_plan(self, market=None):
            return {"sh600000": target, "sz000001": target}, []

        def _load_market_quote_snapshot(self, codes, target_date):
            return pd.DataFrame(
                [
                    {
                        "date": target,
                        "code": "sh600000",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "previous_close": 10.0,
                    }
                ]
            )

        def _load_market_adj_factor_snapshot(self, target_date):
            self.factor_calls += 1
            return pd.DataFrame()

    service = FakeService()
    with pytest.raises(RuntimeError, match="sz000001"):
        service.update_market()

    assert service.factor_calls == 0


def test_update_market_bulk_failure_does_not_refresh_freshness():
    from app.lib.factor_factory import FQFactorService

    target = datetime.datetime(2026, 8, 27)

    class FakeQuoteModel:
        @staticmethod
        def _get_collection():
            return SimpleNamespace(
                bulk_write=lambda operations, ordered=False: (_ for _ in ()).throw(
                    RuntimeError("write failed")
                )
            )

    class FakeService(FQFactorService):
        refreshed_codes = []

        def _get_market_update_plan(self, market=None):
            return {"sh600000": target}, []

        def _load_market_quote_snapshot(self, codes, target_date):
            return pd.DataFrame(
                [
                    {
                        "date": target,
                        "code": "sh600000",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "previous_close": 10.0,
                    }
                ]
            )

        def _load_market_adj_factor_snapshot(self, target_date):
            return pd.DataFrame(
                [
                    {
                        "ts_code": "600000.SH",
                        "trade_date": "20260827",
                        "adj_factor": 2.0,
                    }
                ]
            )

        def _refresh_market_snapshot_statuses(self, codes):
            self.refreshed_codes.extend(codes)

    service = FakeService(quote_model=FakeQuoteModel)
    with pytest.raises(RuntimeError, match="write failed"):
        service.update_market()

    assert service.refreshed_codes == []


def test_market_update_plan_partitions_daily_snapshot_from_history_repair(
    monkeypatch,
):
    from app.lib.factor_factory import fq_factor

    friday = datetime.datetime(2026, 8, 21)
    monday = datetime.datetime(2026, 8, 24)
    tuesday = datetime.datetime(2026, 8, 25)
    capability = SimpleNamespace(fq_factor=True)

    class FakeQuery(list):
        def only(self, *fields):
            return self

        def filter(self, **kwargs):
            return self

    class FakeStockModel:
        @staticmethod
        def objects(**kwargs):
            return FakeQuery(
                [
                    SimpleNamespace(code="one-day", data_capabilities=capability),
                    SimpleNamespace(code="multi-day", data_capabilities=capability),
                    SimpleNamespace(code="no-history", data_capabilities=capability),
                ]
            )

    quote_statuses = [
        SimpleNamespace(code="one-day", latest_data_date=tuesday),
        SimpleNamespace(code="multi-day", latest_data_date=tuesday),
        SimpleNamespace(code="no-history", latest_data_date=tuesday),
    ]
    factor_statuses = [
        SimpleNamespace(code="one-day", latest_data_date=monday, status="STALE"),
        SimpleNamespace(code="multi-day", latest_data_date=friday, status="STALE"),
        SimpleNamespace(code="no-history", latest_data_date=None, status="NO_DATA"),
    ]

    class FakeStatusModel:
        @staticmethod
        def objects(**kwargs):
            if kwargs["asset_type"] == "quote":
                return FakeQuery(quote_statuses)
            return FakeQuery(factor_statuses)

    monkeypatch.setattr(fq_factor, "DataAssetStatus", FakeStatusModel)
    service = fq_factor.FQFactorService(stock_model=FakeStockModel)
    market = SimpleNamespace(trade_calendar=[friday, monday, tuesday])

    snapshots, historical = service._get_market_update_plan(market=market)

    assert snapshots == {"one-day": tuesday}
    assert historical == ["multi-day", "no-history"]


def test_backfill_all_isolates_single_code_failure():
    from app.lib.factor_factory import FQFactorService

    class FakeQuery(list):
        def only(self, *fields):
            return self

        def filter(self, **kwargs):
            return self

    class FakeStockModel:
        @staticmethod
        def objects(**kwargs):
            capability = SimpleNamespace(fq_factor=True)
            return FakeQuery(
                [
                    SimpleNamespace(code="ok-code", data_capabilities=capability),
                    SimpleNamespace(code="bad-code", data_capabilities=capability),
                    SimpleNamespace(code="next-code", data_capabilities=capability),
                ]
            )

    class FakeService(FQFactorService):
        def update_code(self, code):
            if code == "bad-code":
                raise RuntimeError("source unavailable")
            return {"code": "GOOD", "written_count": 2, "message": None}

    result = FakeService(stock_model=FakeStockModel).backfill_all()

    assert result == {"pulled_count": 3, "written_count": 4, "failed_count": 1}


def test_update_code_skips_unsupported_fq_factor_stock():
    from app.lib.factor_factory import FQFactorService

    class FakeQuery:
        def __init__(self, stock):
            self.stock = stock

        def only(self, *fields):
            return self

        def first(self):
            return self.stock

    class FakeStockModel:
        @staticmethod
        def objects(**kwargs):
            return FakeQuery(
                SimpleNamespace(
                    code=kwargs["code"],
                    name="北交所样本",
                    object_type="individual_stock",
                    data_capabilities=SimpleNamespace(
                        daily_quote=False,
                        fq_factor=False,
                        ma_factor=False,
                    ),
                )
            )

    result = FQFactorService(stock_model=FakeStockModel).update_code("bj920118")

    assert result == {
        "code": "SKIP",
        "written_count": 0,
        "message": "fq_factor is not supported for this stock",
    }


def test_run_stock_job_includes_fq_factor_phase():
    from app.lib.datahub.processors import china_a_stock

    source = Path(china_a_stock.__file__).read_text()

    assert '"check_prerequisite", self.check_prerequisite' in source
    assert '"check_stock_data_integrity", self.check_stock_data_integrity' in source
    assert '"mark_inactive_stocks", self.mark_inactive_stocks' in source
    assert '"update_fq_factor", self.update_fq_factor' in source

    assert source.index('"check_prerequisite", self.check_prerequisite') < source.index(
        '"check_stock_data_integrity", self.check_stock_data_integrity'
    )
    assert source.index(
        '"check_stock_data_integrity", self.check_stock_data_integrity'
    ) < source.index('"mark_inactive_stocks", self.mark_inactive_stocks')
    assert source.index(
        '"mark_inactive_stocks", self.mark_inactive_stocks'
    ) < source.index('"update_fq_factor", self.update_fq_factor')


def test_run_stock_job_stops_downstream_when_fq_phase_fails():
    from app.lib.datahub.processors.china_a_stock import ChinaAStock

    class FakeChinaAStock(ChinaAStock):
        def __init__(self):
            self.market_name = "ChinaAStock"
            self.market = SimpleNamespace(name="ChinaAStock")
            self.most_recent_trading_day = datetime.datetime(2026, 8, 27)
            self._partial_phase_result = None
            self.last_job_summary = None
            self._progress_callback = None
            self.calls = []

        def check_prerequisite(self, allow_update=False):
            self.calls.append("prerequisite")

        def check_stock_data_integrity(self, allow_update=False):
            self.calls.append("quote")

        def mark_inactive_stocks(self, allow_update=False):
            self.calls.append("inactive")

        def update_fq_factor(self, allow_update=False):
            self.calls.append("fq")
            raise RuntimeError("FQ historical repair failed")

        def update_ma_factor(self, allow_update=False):
            self.calls.append("ma")

    processor = FakeChinaAStock()
    with pytest.raises(RuntimeError, match="historical repair"):
        processor.run_stock_job()

    assert processor.calls == ["prerequisite", "quote", "inactive", "fq"]
    assert processor.last_job_summary["status"] == "FAILED"
    assert processor.last_job_summary["failed_phase"] == "update_fq_factor"
