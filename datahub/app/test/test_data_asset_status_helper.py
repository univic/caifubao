import datetime
from types import SimpleNamespace
from unittest.mock import patch

from app.lib.utilities import data_asset_status_helper
from app.model.data_asset_status import (
    STATUS_AHEAD,
    STATUS_NO_DATA,
    STATUS_OK,
    STATUS_STALE,
)


def test_quote_status_is_classified_against_expected_date():
    expected = datetime.datetime(2026, 8, 24)

    assert data_asset_status_helper.classify_quote_status(0, None, expected) == (
        STATUS_NO_DATA,
        "no_source_data",
    )
    assert data_asset_status_helper.classify_quote_status(
        10, datetime.datetime(2026, 8, 21), expected
    ) == (
        STATUS_STALE,
        "behind_expected_quote_date",
    )
    assert data_asset_status_helper.classify_quote_status(11, expected, expected) == (
        STATUS_OK,
        None,
    )
    assert data_asset_status_helper.classify_quote_status(
        12, datetime.datetime(2026, 8, 25), expected
    ) == (
        STATUS_AHEAD,
        "ahead_of_expected_quote_date",
    )


def test_expected_quote_count_includes_missing_trading_days():
    calendar = [
        datetime.datetime(2026, 8, 21),
        datetime.datetime(2026, 8, 24),
        datetime.datetime(2026, 8, 25),
    ]

    assert (
        data_asset_status_helper.expected_quote_count(
            1,
            datetime.datetime(2026, 8, 21),
            datetime.datetime(2026, 8, 25),
            calendar,
        )
        == 3
    )


def _dt(day):
    return datetime.datetime(2026, 4, day)


def _quote_status(code, latest, data_count):
    return SimpleNamespace(
        code=code,
        object_type="individual_stock",
        latest_data_date=latest,
        data_count=data_count,
    )


class FakeQuery(list):
    def only(self, *args):
        return self

    def first(self):
        return self[0] if self else None


class TestRefreshParity:
    """Batch builders must produce the same records as the per-stock path."""

    def _patch_status_objects(self, docs):
        class FakeManager:
            @staticmethod
            def objects(**kwargs):
                code = kwargs.get("code")
                matches = [doc for doc in docs if doc.code == code]
                return FakeQuery(matches)

        return patch.object(data_asset_status_helper, "DataAssetStatus", FakeManager)

    def test_fq_builder_matches_per_stock_refresh(self):
        docs = [_quote_status("sh600000", _dt(10), 130)]
        quote_collection = SimpleNamespace(
            aggregate=lambda pipeline: iter(
                [
                    {
                        "_id": "sh600000",
                        "first_data_date": _dt(1),
                        "latest_data_date": _dt(9),
                        "data_count": 129,
                    }
                ]
            )
        )
        stock_obj = SimpleNamespace(code="sh600000", object_type="individual_stock")

        with (
            self._patch_status_objects(docs),
            patch.object(data_asset_status_helper, "_upsert_record") as upsert,
        ):
            data_asset_status_helper.refresh_fq_factor_status(
                stock_obj=stock_obj,
                quote_model=SimpleNamespace(_get_collection=lambda: quote_collection),
                last_job_name="fq_factor_sync",
            )
        per_stock_record = upsert.call_args[0][0]

        stats_row = {
            "_id": "sh600000",
            "first_data_date": _dt(1),
            "latest_data_date": _dt(9),
            "data_count": 129,
        }
        batch_record = data_asset_status_helper.build_fq_status_record(
            code="sh600000",
            object_type="individual_stock",
            stats_row=stats_row,
            quote_status=docs[0],
            last_job_name="fq_factor_sync",
            calculated_at=per_stock_record["last_calculated_at"],
        )

        assert batch_record == per_stock_record

    def test_quote_builder_matches_per_stock_refresh(self):
        quote_collection = SimpleNamespace(
            aggregate=lambda pipeline: iter(
                [
                    {
                        "_id": "sh600000",
                        "first_data_date": _dt(1),
                        "latest_data_date": _dt(10),
                        "data_count": 130,
                    }
                ]
            )
        )
        stock_obj = SimpleNamespace(code="sh600000", object_type="individual_stock")
        trade_calendar = [_dt(1), _dt(9), _dt(10), _dt(11)]

        with patch.object(data_asset_status_helper, "_upsert_record") as upsert:
            data_asset_status_helper.refresh_quote_status(
                stock_obj=stock_obj,
                quote_model=SimpleNamespace(_get_collection=lambda: quote_collection),
                last_job_name="stock_quote_sync",
                expected_latest_date=_dt(11),
                trade_calendar=trade_calendar,
            )
        per_stock_record = upsert.call_args[0][0]

        stats_row = {
            "_id": "sh600000",
            "first_data_date": _dt(1),
            "latest_data_date": _dt(10),
            "data_count": 130,
        }
        batch_record = data_asset_status_helper.build_quote_status_record(
            code="sh600000",
            object_type="individual_stock",
            stats_row=stats_row,
            expected_latest_date=_dt(11),
            trade_calendar=trade_calendar,
            last_job_name="stock_quote_sync",
            calculated_at=per_stock_record["last_calculated_at"],
        )

        assert batch_record == per_stock_record
        # 落后预期交易日 -> STALE，expected_count 计入缺失交易日
        assert batch_record["status"] == STATUS_STALE
        assert batch_record["expected_count"] == 131

    def test_ma_builder_matches_per_stock_refresh(self):
        docs = [_quote_status("sh600000", _dt(10), 130)]
        factor_collection = SimpleNamespace(
            aggregate=lambda pipeline: iter(
                [
                    {
                        "_id": "sh600000",
                        "first_data_date": _dt(2),
                        "latest_data_date": _dt(10),
                        "data_count": 11,
                    }
                ]
            )
        )
        stock_obj = SimpleNamespace(code="sh600000", object_type="individual_stock")

        with (
            self._patch_status_objects(docs),
            patch.object(data_asset_status_helper, "_upsert_record") as upsert,
        ):
            data_asset_status_helper.refresh_ma_factor_status(
                stock_obj=stock_obj,
                factor_model=SimpleNamespace(_get_collection=lambda: factor_collection),
                window=120,
                last_job_name="ma_factor_sync",
            )
        per_stock_record = upsert.call_args[0][0]

        stats_row = {
            "_id": "sh600000",
            "first_data_date": _dt(2),
            "latest_data_date": _dt(10),
            "data_count": 11,
        }
        batch_record = data_asset_status_helper.build_ma_status_record(
            code="sh600000",
            object_type="individual_stock",
            stats_row=stats_row,
            quote_status=docs[0],
            window=120,
            last_job_name="ma_factor_sync",
            calculated_at=per_stock_record["last_calculated_at"],
        )

        assert batch_record == per_stock_record
        assert batch_record["status"] == STATUS_OK
        assert batch_record["expected_count"] == 11


class TestBulkUpsertAssetStatus:
    def test_chunks_operations_and_sets_key_fields(self):
        class SpyCollection:
            def __init__(self):
                self.chunks = []

            def bulk_write(self, operations, ordered=False):
                self.chunks.append(operations)
                return SimpleNamespace(upserted_count=len(operations), modified_count=0)

        records = [
            {
                "code": f"sh60000{index}",
                "object_type": "individual_stock",
                "asset_type": "quote",
                "asset_name": "daily_quote",
                "status": STATUS_OK,
            }
            for index in range(5)
        ]
        collection = SpyCollection()

        written = data_asset_status_helper.bulk_upsert_asset_status(
            records, collection=collection, chunk_size=2
        )

        assert data_asset_status_helper.STATUS_UPSERT_CHUNK_SIZE > 0
        assert written == 5
        assert [len(chunk) for chunk in collection.chunks] == [2, 2, 1]
        first_op = collection.chunks[0][0]
        assert first_op._filter == {
            "code": "sh600000",
            "object_type": "individual_stock",
            "asset_type": "quote",
            "asset_name": "daily_quote",
        }
        assert first_op._doc["$set"]["code"] == "sh600000"
        assert first_op._upsert is True

    def test_deduplicates_records_by_unique_key(self):
        record = {
            "code": "sh600000",
            "object_type": "individual_stock",
            "asset_type": "quote",
            "asset_name": "daily_quote",
            "status": STATUS_OK,
        }

        operations = []

        class CapturingCollection:
            def bulk_write(self, batch, ordered=False):
                operations.extend(batch)
                return SimpleNamespace(upserted_count=1, modified_count=0)

        written = data_asset_status_helper.bulk_upsert_asset_status(
            [dict(record, data_count=1), dict(record, data_count=2)],
            collection=CapturingCollection(),
        )

        assert written == 1
        assert len(operations) == 1
        assert operations[0]._doc["$set"]["data_count"] == 2

    def test_duplicate_only_bulk_error_is_tolerated(self):
        from app.test.test_china_a_stock import DummyBulkWriteError

        class FailingCollection:
            def bulk_write(self, operations, ordered=False):
                raise DummyBulkWriteError(
                    {"writeErrors": [{"code": 11000, "errmsg": "duplicate key"}]}
                )

        record = {
            "code": "sh600000",
            "object_type": "individual_stock",
            "asset_type": "quote",
            "asset_name": "daily_quote",
            "status": STATUS_OK,
        }

        written = data_asset_status_helper.bulk_upsert_asset_status(
            [record], collection=FailingCollection()
        )

        assert written == 0

    def test_mixed_bulk_error_raises(self):
        from app.test.test_china_a_stock import DummyBulkWriteError

        class FailingCollection:
            def bulk_write(self, operations, ordered=False):
                raise DummyBulkWriteError(
                    {
                        "writeErrors": [
                            {"code": 11000, "errmsg": "duplicate key"},
                            {"code": 121, "errmsg": "document validation"},
                        ]
                    }
                )

        record = {
            "code": "sh600000",
            "object_type": "individual_stock",
            "asset_type": "quote",
            "asset_name": "daily_quote",
            "status": STATUS_OK,
        }

        try:
            data_asset_status_helper.bulk_upsert_asset_status(
                [record], collection=FailingCollection()
            )
        except DummyBulkWriteError:
            pass
        else:
            raise AssertionError("Expected non-duplicate bulk error to raise")

    def test_empty_records_skip_the_write(self):
        class SpyCollection:
            def __init__(self):
                self.calls = 0

            def bulk_write(self, operations, ordered=False):
                self.calls += 1
                return SimpleNamespace(upserted_count=0, modified_count=0)

        collection = SpyCollection()

        written = data_asset_status_helper.bulk_upsert_asset_status(
            [], collection=collection
        )

        assert written == 0
        assert collection.calls == 0
