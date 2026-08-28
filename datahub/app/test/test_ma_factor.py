import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


def test_build_ma_frame_uses_hfq_close():
    from app.lib.factor_factory import MovingAverageFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "close": 10.0,
                "close_hfq": 20.0,
            },
            {
                "date": datetime.datetime(2024, 1, 9),
                "code": "sh600000",
                "close": 11.0,
                "close_hfq": 22.0,
            },
            {
                "date": datetime.datetime(2024, 1, 10),
                "code": "sh600000",
                "close": 12.0,
                "close_hfq": 24.0,
            },
        ]
    ).set_index("date")

    result = MovingAverageFactorService.build_ma_frame(quote_df, windows=(2,))

    assert pd.isna(result.loc[datetime.datetime(2024, 1, 8), "MA_2"])
    assert result.loc[datetime.datetime(2024, 1, 9), "MA_2"] == 21.0
    assert result.loc[datetime.datetime(2024, 1, 10), "MA_2"] == 23.0


def test_build_ma_frame_requires_price_field():
    from app.lib.factor_factory import MovingAverageFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "close": 10.0,
            }
        ]
    ).set_index("date")

    try:
        MovingAverageFactorService.build_ma_frame(quote_df, windows=(2,))
    except ValueError as exc:
        assert "close_hfq" in str(exc)
    else:
        raise AssertionError("Expected missing close_hfq to fail MA calculation")


def test_build_ma_frame_rejects_missing_hfq_values():
    from app.lib.factor_factory import MovingAverageFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 8),
                "code": "sh600000",
                "close_hfq": None,
            }
        ]
    ).set_index("date")

    try:
        MovingAverageFactorService.build_ma_frame(quote_df, windows=(2,))
    except ValueError as exc:
        assert "No valid close_hfq" in str(exc)
    else:
        raise AssertionError("Expected empty close_hfq data to skip MA calculation")


def test_ma_factor_model_uses_wide_daily_collection():
    from app.model.factor import StockFactorDaily

    assert StockFactorDaily._get_collection_name() == "stock_factor_daily"
    assert "ma_10" in StockFactorDaily._fields
    assert "ma_120" in StockFactorDaily._fields


def test_build_bulk_operations_writes_one_wide_doc_per_date():
    from app.lib.factor_factory import MovingAverageFactorService

    class StockStub:
        code = "sh600000"
        name = "浦发银行"

        @staticmethod
        def to_dbref():
            return "stock-ref"

    output_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 9),
                "MA_2": 21.0,
                "MA_3": None,
            },
            {
                "date": datetime.datetime(2024, 1, 10),
                "MA_2": 23.0,
                "MA_3": 22.0,
            },
        ]
    ).set_index("date")

    service = MovingAverageFactorService(windows=(2, 3))
    operations = service._build_bulk_operations(StockStub(), output_df)

    assert len(operations) == 2
    assert operations[0]._filter == {
        "stock_code": "sh600000",
        "date": datetime.datetime(2024, 1, 9),
    }
    assert operations[0]._doc["$set"]["ma_2"] == 21.0
    assert "ma_3" not in operations[0]._doc["$set"]
    assert operations[1]._doc["$set"]["ma_2"] == 23.0
    assert operations[1]._doc["$set"]["ma_3"] == 22.0


def test_update_market_counts_skipped_and_failed_codes():
    from app.lib.factor_factory import MovingAverageFactorService

    class FakeMovingAverageFactorService(MovingAverageFactorService):
        refreshed_codes = []

        def get_codes_requiring_update(self, market=None):
            return ["ok-code", "skip-code", "bad-code"]

        def update_code(self, code, *, refresh_statuses=True):
            if code == "skip-code":
                return {"code": "SKIP", "written_count": 0, "message": "missing hfq"}
            if code == "bad-code":
                raise ValueError("bad data")
            return {"code": "GOOD", "written_count": 2, "message": None}

        def refresh_market_statuses(self, codes):
            self.refreshed_codes.extend(codes)

    service = FakeMovingAverageFactorService()
    result = service.update_market()

    assert result == {
        "pulled_count": 3,
        "written_count": 2,
        "skipped_count": 1,
        "failed_count": 1,
        "failed_codes": ["bad-code"],
    }
    # GOOD 股票统一走批量 freshness 刷新
    assert service.refreshed_codes == ["ok-code"]


def test_update_code_skips_unsupported_ma_factor_stock():
    from app.lib.factor_factory import MovingAverageFactorService

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

    result = MovingAverageFactorService(stock_model=FakeStockModel).update_code(
        "bj920118"
    )

    assert result == {
        "code": "SKIP",
        "written_count": 0,
        "message": "ma_factor is not supported for this stock",
    }


def test_run_stock_job_updates_ma_after_fq_factor_phase():
    from app.lib.datahub.processors import china_a_stock

    source = Path(china_a_stock.__file__).read_text()

    assert '"update_fq_factor", self.update_fq_factor' in source
    assert '"update_ma_factor", self.update_ma_factor' in source
    assert source.index('"update_fq_factor", self.update_fq_factor') < source.index(
        '"update_ma_factor", self.update_ma_factor'
    )
