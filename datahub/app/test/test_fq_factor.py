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


def test_update_market_isolates_single_code_failure():
    from app.lib.factor_factory import FQFactorService

    class FakeFQFactorService(FQFactorService):
        def get_codes_requiring_update(self, market=None):
            return ["ok-code", "bad-code", "next-code"]

        def update_code(self, code):
            if code == "bad-code":
                raise ValueError("bad quote data")
            return {"code": "GOOD", "written_count": 2, "message": None}

    result = FakeFQFactorService().update_market()

    assert result == {"pulled_count": 3, "written_count": 4, "failed_count": 1}


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
