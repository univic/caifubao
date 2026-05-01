import datetime
from types import SimpleNamespace

from app.jobs.data_asset_status_initializer import (
    ASSET_DAILY_QUOTE,
    ASSET_FQ_FACTOR,
    DEFAULT_BATCH_SIZE,
    DataAssetStatusInitializer,
    DataStats,
    build_asset_status_records,
    parse_args,
)
from app.model.data_asset_status import (
    DataAssetStatus,
    STATUS_NO_DATA,
    STATUS_NOT_APPLICABLE,
    STATUS_OK,
    STATUS_STALE,
)


def _stock(code, capabilities=None):
    return SimpleNamespace(
        code=code,
        object_type="individual_stock",
        data_capabilities=SimpleNamespace(**capabilities) if capabilities else None,
    )


def _dt(day):
    return datetime.datetime(2026, 4, day)


def _by_asset(records):
    return {(item["code"], item["asset_name"]): item for item in records}


def test_data_asset_status_model_uses_target_collection():
    assert DataAssetStatus._get_collection_name() == "data_asset_status"
    assert "latest_data_date" in DataAssetStatus._fields
    assert "coverage_rate" in DataAssetStatus._fields


def test_parse_args_defaults_to_write_all_active_stocks():
    args = parse_args([])

    assert args.codes == []
    assert args.limit is None
    assert args.dry_run is False
    assert args.batch_size == DEFAULT_BATCH_SIZE


def test_build_records_handles_stale_fq_and_ma_windows():
    records = build_asset_status_records(
        [_stock("sh600000"), _stock("sz001234")],
        quote_stats_by_code={
            "sh600000": DataStats(_dt(1), _dt(10), 130),
            "sz001234": DataStats(_dt(1), _dt(10), 45),
        },
        fq_stats_by_code={"sh600000": DataStats(_dt(1), _dt(9), 129)},
        ma_stats_by_asset={"MA_120": {"sh600000": DataStats(_dt(2), _dt(10), 11)}},
        calculated_at=_dt(11),
    )

    by_asset = _by_asset(records)

    assert by_asset[("sh600000", ASSET_DAILY_QUOTE)]["status"] == STATUS_OK
    assert by_asset[("sh600000", ASSET_FQ_FACTOR)]["status"] == STATUS_STALE
    assert (
        by_asset[("sh600000", ASSET_FQ_FACTOR)]["status_reason"] == "behind_daily_quote"
    )
    assert by_asset[("sh600000", "MA_120")]["status"] == STATUS_OK
    assert by_asset[("sh600000", "MA_120")]["expected_count"] == 11
    assert by_asset[("sz001234", "MA_60")]["status"] == STATUS_NOT_APPLICABLE
    assert (
        by_asset[("sz001234", "MA_60")]["status_reason"] == "insufficient_quote_history"
    )
    assert by_asset[("sh600000", "MA_60")]["status"] == STATUS_NO_DATA


def test_build_records_respects_disabled_capabilities():
    records = build_asset_status_records(
        [
            _stock(
                "bj920118",
                {
                    "daily_quote": False,
                    "fq_factor": False,
                    "ma_factor": False,
                },
            )
        ],
        quote_stats_by_code={},
        fq_stats_by_code={},
        ma_stats_by_asset={},
        calculated_at=_dt(11),
    )

    assert {item["status"] for item in records} == {STATUS_NOT_APPLICABLE}
    assert {item["status_reason"] for item in records} == {"capability_disabled"}


class FakeQuery(list):
    def filter(self, **kwargs):
        codes = set(kwargs.get("code__in", []))
        return FakeQuery([item for item in self if item.code in codes])

    def only(self, *args):
        return self

    def order_by(self, *args):
        return FakeQuery(sorted(self, key=lambda item: item.code))


class FakeStockModel:
    @classmethod
    def objects(cls, **kwargs):
        return FakeQuery([_stock("sh600000"), _stock("sz001234")])


class FakeCollection:
    def __init__(self, rows_by_field):
        self.rows_by_field = rows_by_field

    def aggregate(self, pipeline):
        match = pipeline[0]["$match"]
        value_fields = [key for key in match if key not in ("code", "stock_code")]
        field_name = value_fields[0] if value_fields else "quote"
        return self.rows_by_field.get(field_name, [])


class FakeQuoteModel:
    @classmethod
    def _get_collection(cls):
        return FakeCollection(
            {
                "quote": [
                    {
                        "_id": "sh600000",
                        "first_data_date": _dt(1),
                        "latest_data_date": _dt(10),
                        "data_count": 130,
                    }
                ],
                "fq_factor": [
                    {
                        "_id": "sh600000",
                        "first_data_date": _dt(1),
                        "latest_data_date": _dt(10),
                        "data_count": 130,
                    }
                ],
            }
        )


class FakeFactorModel:
    @classmethod
    def _get_collection(cls):
        return FakeCollection(
            {
                "ma_120": [
                    {
                        "_id": "sh600000",
                        "first_data_date": _dt(2),
                        "latest_data_date": _dt(10),
                        "data_count": 11,
                    }
                ]
            }
        )


class FakeStatusModel:
    @classmethod
    def objects(cls, **kwargs):
        raise AssertionError("dry-run should not write data_asset_status")


def test_initializer_dry_run_returns_summary_without_writes():
    initializer = DataAssetStatusInitializer(
        stock_model=FakeStockModel,
        quote_model=FakeQuoteModel,
        factor_model=FakeFactorModel,
        status_model=FakeStatusModel,
    )

    result = initializer.run(dry_run=True)

    assert result["dry_run"] is True
    assert result["stock_count"] == 2
    assert result["asset_count"] == 14
    assert result["written_count"] == 0
    assert result["batch_count"] == 1
    assert result["batch_size"] == DEFAULT_BATCH_SIZE
    assert result["status_counts"][STATUS_OK] >= 3


def test_initializer_respects_batch_size():
    initializer = DataAssetStatusInitializer(
        stock_model=FakeStockModel,
        quote_model=FakeQuoteModel,
        factor_model=FakeFactorModel,
        status_model=FakeStatusModel,
    )

    result = initializer.run(dry_run=True, batch_size=1)

    assert result["stock_count"] == 2
    assert result["asset_count"] == 14
    assert result["written_count"] == 0
    assert result["batch_count"] == 2
    assert result["batch_size"] == 1
