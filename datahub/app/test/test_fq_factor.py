import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


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

    result = FQFactorService.build_fq_factor_frame(quote_df)

    assert result.loc[datetime.datetime(2024, 1, 8), "fq_factor"] == 1.05
    assert result.loc[datetime.datetime(2024, 1, 8), "close_hfq"] == 10.5
    assert result.loc[datetime.datetime(2024, 1, 9), "fq_factor"] == 1.1
    assert result.loc[datetime.datetime(2024, 1, 9), "close_hfq"] == 11.0


def test_build_fq_factor_frame_from_incremental_anchor():
    from app.lib.factor_factory import FQFactorService

    quote_df = pd.DataFrame(
        [
            {
                "date": datetime.datetime(2024, 1, 10),
                "code": "sh600000",
                "open": 11.1,
                "high": 11.6,
                "low": 10.9,
                "close": 11.55,
                "previous_close": 11.0,
            }
        ]
    ).set_index("date")

    result = FQFactorService.build_fq_factor_frame(
        quote_df,
        base_fq_factor=1.1,
        base_close_hfq=11.0,
    )

    assert result.loc[datetime.datetime(2024, 1, 10), "fq_factor"] == 1.155
    assert result.loc[datetime.datetime(2024, 1, 10), "close_hfq"] == 11.55


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
    source = Path(
        "/Users/wenfengzhong/develop/caifubao/datahub/app/lib/datahub/processors/china_a_stock.py"
    ).read_text()

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
