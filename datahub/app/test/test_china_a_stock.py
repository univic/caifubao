"""Tests for ChinaAStock processor helpers."""

from __future__ import annotations

from unittest import TestCase
from types import SimpleNamespace

import pandas


class DummyBulkWriteError(Exception):
    """Minimal exception shim exposing pymongo-like details."""

    def __init__(self, details):
        super().__init__("bulk write error")
        self.details = details


class DummyMongoEngineBulkWriteError(Exception):
    """Minimal exception shim exposing mongoengine-style args payload."""

    def __init__(self, details):
        super().__init__(details)


class EmptyQuerySet(list):
    def count(self):
        return 0


class TestChinaAStockHelpers(TestCase):
    def test_empty_stock_bootstrap_accumulates_written_quotes(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.perform_date_check = lambda: None
        processor.handle_new_stock = lambda **kwargs: {"written_count": 3}
        remote = pandas.DataFrame(
            [{"code": "sh600519", "name": "Kweichow Moutai", "close": 1}]
        )

        result = processor.check_data_integrity(
            obj_type="stock",
            local_data_list=EmptyQuerySet(),
            remote_data_df=remote,
            hist_handler="get_hist_stock_quote_data",
            allow_update=True,
        )

        self.assertEqual(result["written_count"], 3)

    def test_stock_bootstrap_fails_when_every_quote_write_is_zero(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.perform_date_check = lambda: None
        processor.handle_new_stock = lambda **kwargs: {"written_count": 0}
        remote = pandas.DataFrame(
            [{"code": "sh600519", "name": "Kweichow Moutai", "close": 1}]
        )

        with self.assertRaisesRegex(RuntimeError, "wrote zero quote rows"):
            processor.check_data_integrity(
                obj_type="stock",
                local_data_list=EmptyQuerySet(),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

    def test_duplicate_only_bulk_write_error_is_detected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyBulkWriteError(
            {
                "writeErrors": [
                    {"code": 11000, "errmsg": "dup"},
                    {"code": 11000, "errmsg": "dup"},
                ]
            }
        )

        self.assertTrue(is_duplicate_only_bulk_write_error(error))

    def test_mixed_bulk_write_error_is_not_treated_as_duplicate_only(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyBulkWriteError(
            {
                "writeErrors": [
                    {"code": 11000, "errmsg": "dup"},
                    {"code": 50, "errmsg": "other"},
                ]
            }
        )

        self.assertFalse(is_duplicate_only_bulk_write_error(error))

    def test_mongoengine_style_bulk_write_error_is_detected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyMongoEngineBulkWriteError(
            {
                "writeErrors": [
                    {"code": 11000, "errmsg": "dup"},
                    {"code": 11000, "errmsg": "dup"},
                ]
            }
        )

        self.assertTrue(is_duplicate_only_bulk_write_error(error))

    def test_stringified_mongoengine_bulk_write_error_is_detected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyMongoEngineBulkWriteError(
            "Bulk write error: ({'writeErrors': [{'index': 0, 'code': 11000, "
            "'errmsg': 'dup'}], 'writeConcernErrors': []})"
        )

        self.assertTrue(is_duplicate_only_bulk_write_error(error))

    def test_stringified_bulk_write_error_with_non_duplicate_code_is_rejected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyMongoEngineBulkWriteError(
            "Bulk write error: ({'writeErrors': [{'index': 0, 'code': 11000}, "
            "{'index': 1, 'code': 50}], 'writeConcernErrors': []})"
        )

        self.assertFalse(is_duplicate_only_bulk_write_error(error))
