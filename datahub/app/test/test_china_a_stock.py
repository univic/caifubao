"""Tests for ChinaAStock processor helpers."""

from __future__ import annotations

from unittest import TestCase


class DummyBulkWriteError(Exception):
    """Minimal exception shim exposing pymongo-like details."""

    def __init__(self, details):
        super().__init__("bulk write error")
        self.details = details


class DummyMongoEngineBulkWriteError(Exception):
    """Minimal exception shim exposing mongoengine-style args payload."""

    def __init__(self, details):
        super().__init__(details)


class TestChinaAStockHelpers(TestCase):
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
