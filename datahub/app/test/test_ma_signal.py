# -*- coding: utf-8 -*-
import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest
from app.lib.utilities import data_asset_status_helper
from app.lib.signal_factory import MovingAverageSignalService
from app.lib.signal_factory.moving_average import (
    SIGNAL_MA10_CROSS_MA20,
    SIGNAL_PRICE_ABOVE_MA60,
    SIGNAL_MA20_ABOVE_MA60,
)


def test_build_signal_frame_ma10_cross_ma20():
    service = MovingAverageSignalService()
    config = service.configs[SIGNAL_MA10_CROSS_MA20]

    factor_df = pd.DataFrame(
        [
            {"date": datetime.datetime(2026, 4, 8), "ma_10": 9.9, "ma_20": 10.0},
            {"date": datetime.datetime(2026, 4, 9), "ma_10": 10.1, "ma_20": 10.0},
        ]
    ).set_index("date")

    result = service.build_signal_frame(config, factor_df)

    assert len(result) == 1
    assert result.index[0] == datetime.datetime(2026, 4, 9)
    assert result.iloc[0]["strength"] == 1.0


def test_build_signal_frame_price_above_ma60():
    service = MovingAverageSignalService()
    config = service.configs[SIGNAL_PRICE_ABOVE_MA60]

    factor_df = pd.DataFrame(
        [
            {"date": datetime.datetime(2026, 4, 10), "close": 11.0, "ma_60": 10.0},
            {"date": datetime.datetime(2026, 4, 11), "close": 9.0, "ma_60": 10.0},
        ]
    ).set_index("date")

    result = service.build_signal_frame(config, factor_df)

    assert len(result) == 1
    assert result.index[0] == datetime.datetime(2026, 4, 10)
    assert result.iloc[0]["strength"] == 10.0


def test_build_signal_frame_ma20_above_ma60():
    service = MovingAverageSignalService()
    config = service.configs[SIGNAL_MA20_ABOVE_MA60]

    factor_df = pd.DataFrame(
        [
            {"date": datetime.datetime(2026, 4, 10), "ma_20": 10.5, "ma_60": 10.0},
            {"date": datetime.datetime(2026, 4, 11), "ma_20": 9.5, "ma_60": 10.0},
        ]
    ).set_index("date")

    result = service.build_signal_frame(config, factor_df)

    assert len(result) == 1
    assert result.index[0] == datetime.datetime(2026, 4, 10)
    assert result.iloc[0]["strength"] == 5.0


def _incremental_frame():
    return pd.DataFrame(
        [
            {
                "date": datetime.datetime(2026, 4, 10),
                "ma_10": 9.0,
                "ma_20": 10.0,
                "ma_60": 9.5,
                "close": 9.0,
            },
            {
                "date": datetime.datetime(2026, 4, 13),
                "ma_10": 10.5,
                "ma_20": 10.0,
                "ma_60": 9.5,
                "close": 11.0,
            },
        ]
    ).set_index("date")


class RecordingSignalCollection:
    def __init__(self, fail=False, delete_fail=False):
        self.fail = fail
        self.delete_fail = delete_fail
        self.batches = []
        self.delete_filters = []

    def bulk_write(self, operations, ordered=False):
        if self.fail:
            raise RuntimeError("signal write failed")
        self.batches.append((operations, ordered))

    def delete_many(self, query):
        if self.delete_fail:
            raise RuntimeError("signal cleanup failed")
        self.delete_filters.append(query)


class RecordingSignalModel:
    collection = RecordingSignalCollection()

    @classmethod
    def _get_collection(cls):
        return cls.collection


class IncrementalSignalService(MovingAverageSignalService):
    def __init__(self, anchors, collection=None):
        self.anchors = anchors
        self.refreshed = []
        RecordingSignalModel.collection = collection or RecordingSignalCollection()
        super().__init__(signal_model=RecordingSignalModel)

    def _load_stock(self, code):
        return SimpleNamespace(
            code=code,
            name="sample",
            object_type="individual_stock",
            data_capabilities=None,
        )

    def _load_signal_anchors(self, code):
        return self.anchors

    def _load_factor_df(self, code, anchor_date=None):
        self.loaded_anchor = anchor_date
        return _incremental_frame()

    def _load_source_freshness(self, code):
        return {"MA_60": {"status": "OK"}}

    def _refresh_signal_statuses(self, stock_obj, target_date, calculated_at):
        self.refreshed.append((stock_obj.code, target_date))


def test_incremental_update_writes_only_dates_after_evaluated_anchor():
    anchor = datetime.datetime(2026, 4, 10)
    service = IncrementalSignalService(
        {signal_name: anchor for signal_name in service_signal_names()}
    )

    result = service.update_code("sh600000")

    assert service.loaded_anchor == anchor
    operations, ordered = RecordingSignalModel.collection.batches[0]
    assert ordered is False
    assert len(operations) == 3
    assert {operation._filter["signal_name"] for operation in operations} == set(
        service_signal_names()
    )
    assert {operation._filter["date"] for operation in operations} == {
        datetime.datetime(2026, 4, 13)
    }
    assert all("generated_at" not in operation._doc["$set"] for operation in operations)
    assert all(
        "generated_at" in operation._doc["$setOnInsert"] for operation in operations
    )
    assert result["written_count"] == 3
    assert service.refreshed == [("sh600000", datetime.datetime(2026, 4, 13))]


def test_current_anchor_makes_incremental_rerun_write_nothing():
    target = datetime.datetime(2026, 4, 13)
    service = IncrementalSignalService(
        {signal_name: target for signal_name in service_signal_names()}
    )

    result = service.update_code("sh600000")

    assert RecordingSignalModel.collection.batches == []
    assert result["written_count"] == 0
    assert service.refreshed == [("sh600000", target)]


def test_force_rebuild_upserts_before_deleting_non_authoritative_rows():
    service = IncrementalSignalService({})

    result = service.update_code("sh600000", force=True)

    operations, ordered = RecordingSignalModel.collection.batches[0]
    assert ordered is True
    assert all(hasattr(operation, "_doc") for operation in operations)
    delete_filter = RecordingSignalModel.collection.delete_filters[0]
    assert delete_filter["stock_code"] == "sh600000"
    assert delete_filter["signal_name"] == {"$in": service_signal_names()}
    assert len(delete_filter["$nor"]) == len(operations)
    assert result["written_count"] == len(operations)


def test_force_write_failure_preserves_existing_rows():
    collection = RecordingSignalCollection(fail=True)
    service = IncrementalSignalService({}, collection=collection)

    with pytest.raises(RuntimeError, match="signal write failed"):
        service.update_code("sh600000", force=True)

    assert collection.delete_filters == []
    assert service.refreshed == []


def test_force_cleanup_failure_does_not_refresh_status():
    collection = RecordingSignalCollection(delete_fail=True)
    service = IncrementalSignalService({}, collection=collection)

    with pytest.raises(RuntimeError, match="signal cleanup failed"):
        service.update_code("sh600000", force=True)

    assert collection.batches
    assert service.refreshed == []


def test_force_without_factor_data_fails_without_deleting_rows():
    class EmptyForceService(IncrementalSignalService):
        def _load_factor_df(self, code, anchor_date=None):
            return pd.DataFrame()

    service = EmptyForceService({})

    with pytest.raises(RuntimeError, match="without factor data"):
        service.update_code("sh600000", force=True)

    assert RecordingSignalModel.collection.delete_filters == []
    assert service.refreshed == []


def test_multi_day_gap_persists_intermediate_state_matches():
    anchor = datetime.datetime(2026, 4, 9)
    service = IncrementalSignalService(
        {signal_name: anchor for signal_name in service_signal_names()}
    )

    service.update_code("sh600000")

    operations = RecordingSignalModel.collection.batches[0][0]
    keys = {
        (operation._filter["signal_name"], operation._filter["date"])
        for operation in operations
    }
    assert (SIGNAL_PRICE_ABOVE_MA60, datetime.datetime(2026, 4, 13)) in keys
    assert (SIGNAL_MA20_ABOVE_MA60, datetime.datetime(2026, 4, 13)) in keys
    assert (SIGNAL_MA20_ABOVE_MA60, datetime.datetime(2026, 4, 10)) in keys
    assert (SIGNAL_MA10_CROSS_MA20, datetime.datetime(2026, 4, 13)) in keys


def test_cold_start_persists_full_state_history():
    service = IncrementalSignalService({})

    service.update_code("sh600000")

    operations = RecordingSignalModel.collection.batches[0][0]
    keys = {
        (operation._filter["signal_name"], operation._filter["date"])
        for operation in operations
    }
    assert (SIGNAL_PRICE_ABOVE_MA60, datetime.datetime(2026, 4, 13)) in keys
    assert (SIGNAL_MA20_ABOVE_MA60, datetime.datetime(2026, 4, 13)) in keys
    assert (SIGNAL_MA20_ABOVE_MA60, datetime.datetime(2026, 4, 10)) in keys
    assert (SIGNAL_MA10_CROSS_MA20, datetime.datetime(2026, 4, 13)) in keys


def test_signal_write_failure_does_not_refresh_status():
    service = IncrementalSignalService(
        {}, collection=RecordingSignalCollection(fail=True)
    )

    with pytest.raises(RuntimeError, match="signal write failed"):
        service.update_code("sh600000")

    assert service.refreshed == []


def test_incremental_business_payload_matches_force_rebuild_for_new_date():
    anchor = datetime.datetime(2026, 4, 10)
    incremental = IncrementalSignalService(
        {signal_name: anchor for signal_name in service_signal_names()}
    )
    incremental.update_code("sh600000")
    incremental_ops = RecordingSignalModel.collection.batches[0][0]

    forced = IncrementalSignalService({})
    forced.update_code("sh600000", force=True)
    forced_ops = RecordingSignalModel.collection.batches[0][0]
    forced_latest = {
        operation._filter["signal_name"]: operation
        for operation in forced_ops
        if operation._filter["date"] == datetime.datetime(2026, 4, 13)
    }

    assert len(incremental_ops) == len(forced_latest) == 3
    for operation in incremental_ops:
        full_operation = forced_latest[operation._filter["signal_name"]]
        assert operation._filter == full_operation._filter
        assert operation._doc["$set"] == full_operation._doc["$set"]


def test_signal_status_refresh_uses_persisted_counts_and_target_date():
    class AggregateCollection:
        @staticmethod
        def aggregate(pipeline):
            return [
                {
                    "_id": {
                        "stock_code": "sh600000",
                        "signal_name": SIGNAL_MA10_CROSS_MA20,
                    },
                    "data_count": 7,
                },
                {
                    "_id": {
                        "stock_code": "sh600000",
                        "signal_name": SIGNAL_PRICE_ABOVE_MA60,
                    },
                    "data_count": 11,
                },
            ]

    class AggregateSignalModel:
        @staticmethod
        def _get_collection():
            return AggregateCollection()

    service = MovingAverageSignalService(signal_model=AggregateSignalModel)
    stock = SimpleNamespace(code="sh600000", object_type="individual_stock")
    target = datetime.datetime(2026, 4, 13)
    calculated_at = datetime.datetime(2026, 4, 13, 18, 30)

    with (
        patch.object(service.status_model, "_get_collection", return_value=object()),
        patch.object(
            data_asset_status_helper, "bulk_upsert_asset_status"
        ) as bulk_upsert,
    ):
        service._refresh_signal_statuses(stock, target, calculated_at)

    records = bulk_upsert.call_args.args[0]
    by_name = {record["asset_name"]: record for record in records}
    assert by_name[SIGNAL_MA10_CROSS_MA20]["data_count"] == 7
    assert by_name[SIGNAL_PRICE_ABOVE_MA60]["data_count"] == 11
    assert by_name[SIGNAL_MA20_ABOVE_MA60]["data_count"] == 0
    assert all(record["latest_data_date"] == target for record in records)


def test_market_update_failure_does_not_advance_any_status():
    class BatchService(MovingAverageSignalService):
        refreshed_targets = None

        def get_codes_requiring_update(self, market=None):
            return ["sh600000", "sz000001", "bad-code"]

        def update_code(self, code, *, force=False, refresh_statuses=True):
            assert refresh_statuses is False
            if code == "bad-code":
                raise RuntimeError("bad signal")
            return {
                "code": "GOOD",
                "written_count": 1,
                "target_date": datetime.datetime(2026, 4, 13),
            }

        def _refresh_market_signal_statuses(self, code_targets, **kwargs):
            self.refreshed_targets = code_targets

    service = BatchService()

    with pytest.raises(RuntimeError, match="bad-code") as error:
        service.update_market()

    assert service.refreshed_targets is None
    # The two good codes were persisted before the failing one aborted the
    # batch; the exception carries that count for the job-run record.
    assert getattr(error.value, "written_count", None) == 2


def test_market_status_failure_reports_all_retry_codes():
    from app.lib.signal_factory.moving_average import SignalUpdateError

    class StatusFailService(MovingAverageSignalService):
        def get_codes_requiring_update(self, market=None):
            return ["sh600000", "sz000001"]

        def update_code(self, code, *, force=False, refresh_statuses=True):
            return {
                "code": "GOOD",
                "written_count": 1,
                "target_date": datetime.datetime(2026, 4, 13),
            }

        def _refresh_market_signal_statuses(self, code_targets, **kwargs):
            raise RuntimeError("status write failed")

    with pytest.raises(SignalUpdateError) as error:
        StatusFailService().update_market()

    assert error.value.failed_codes == ["sh600000", "sz000001"]
    # Signals for both codes were persisted before the status refresh failed;
    # the exception carries that count for the job-run record.
    assert error.value.written_count == 2


def service_signal_names():
    return [
        SIGNAL_MA10_CROSS_MA20,
        SIGNAL_PRICE_ABOVE_MA60,
        SIGNAL_MA20_ABOVE_MA60,
    ]
