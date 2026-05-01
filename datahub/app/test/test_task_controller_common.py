"""Tests for task controller common module."""

from __future__ import annotations

import unittest
from unittest import TestCase
from unittest.mock import MagicMock, patch


class TestConvertKwargFunctions(TestCase):
    """Test cases for convert_kwarg_to_dict and convert_dict_to_kwarg functions."""

    def test_convert_kwarg_to_dict_with_booleans(self):
        """Test convert_kwarg_to_dict correctly converts boolean values."""
        from app.lib.task_controller.common import convert_kwarg_to_dict

        mock_kwarg_1 = MagicMock()
        mock_kwarg_1.keyword = "allow_update"
        mock_kwarg_1.arg = "True"

        mock_kwarg_2 = MagicMock()
        mock_kwarg_2.keyword = "verbose"
        mock_kwarg_2.arg = "False"

        kwarg_doc_list = [mock_kwarg_1, mock_kwarg_2]
        result = convert_kwarg_to_dict(kwarg_doc_list)

        self.assertEqual(result["allow_update"], True)
        self.assertEqual(result["verbose"], False)

    def test_convert_kwarg_to_dict_with_strings(self):
        """Test convert_kwarg_to_dict correctly converts string values."""
        from app.lib.task_controller.common import convert_kwarg_to_dict

        mock_kwarg = MagicMock()
        mock_kwarg.keyword = "start_date"
        mock_kwarg.arg = "2024-01-15"

        result = convert_kwarg_to_dict([mock_kwarg])

        self.assertEqual(result["start_date"], "2024-01-15")

    def test_convert_dict_to_kwarg_with_boolean_true(self):
        """Test convert_dict_to_kwarg correctly converts True to 'True'."""
        from app.lib.task_controller.common import convert_dict_to_kwarg

        kwarg_dict = {"allow_update": True}
        result = convert_dict_to_kwarg(kwarg_dict)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].keyword, "allow_update")
        self.assertEqual(result[0].arg, "True")

    def test_convert_dict_to_kwarg_with_boolean_false(self):
        """Test convert_dict_to_kwarg correctly converts False to 'False'."""
        from app.lib.task_controller.common import convert_dict_to_kwarg

        kwarg_dict = {"verbose": False}
        result = convert_dict_to_kwarg(kwarg_dict)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].keyword, "verbose")
        self.assertEqual(result[0].arg, "False")

    def test_convert_dict_to_kwarg_with_string_value(self):
        """Test convert_dict_to_kwarg correctly handles string values."""
        from app.lib.task_controller.common import convert_dict_to_kwarg

        kwarg_dict = {"start_date": "2024-01-15"}
        result = convert_dict_to_kwarg(kwarg_dict)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].keyword, "start_date")
        self.assertEqual(result[0].arg, "2024-01-15")

    def test_convert_dict_to_kwarg_with_integer_value(self):
        """Test convert_dict_to_kwarg correctly handles integer values."""
        from app.lib.task_controller.common import convert_dict_to_kwarg

        kwarg_dict = {"count": 42}
        result = convert_dict_to_kwarg(kwarg_dict)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].keyword, "count")
        self.assertEqual(result[0].arg, 42)

    def test_convert_dict_to_kwarg_empty_dict(self):
        """Test convert_dict_to_kwarg with empty dict."""
        from app.lib.task_controller.common import convert_dict_to_kwarg

        result = convert_dict_to_kwarg({})
        self.assertEqual(result, [])

    def test_convert_dict_to_kwarg_multiple_items(self):
        """Test convert_dict_to_kwarg with multiple items."""
        from app.lib.task_controller.common import convert_dict_to_kwarg

        kwarg_dict = {
            "allow_update": True,
            "start_date": "2024-01-15",
            "count": 10,
        }
        result = convert_dict_to_kwarg(kwarg_dict)

        self.assertEqual(len(result), 3)
        keywords = [item.keyword for item in result]
        self.assertIn("allow_update", keywords)
        self.assertIn("start_date", keywords)
        self.assertIn("count", keywords)


class TestCheckTaskUniqueness(TestCase):
    """Test cases for check_task_uniqueness function."""

    def test_check_task_uniqueness_returns_false_when_task_exists(self):
        """Test check_task_uniqueness returns False when CRTD task with same uid exists."""
        with patch("app.lib.task_controller.common.Task") as mock_task:
            mock_task.objects.return_value.first.return_value = (
                MagicMock()
            )  # task exists

            from app.lib.task_controller.common import check_task_uniqueness

            mock_task_obj = MagicMock()
            mock_task_obj.name = "TestTask"
            mock_task_obj.callback_package = "test_pkg"
            mock_task_obj.callback_module = "test_mod"
            mock_task_obj.callback_handler = "test_handler"
            mock_task_obj.args = []
            mock_task_obj.kwargs = []
            mock_task_obj.scheduled_process_time = None

            result = check_task_uniqueness(mock_task_obj)
            self.assertFalse(result)

    def test_check_task_uniqueness_returns_true_when_no_task(self):
        """Test check_task_uniqueness returns True when no CRTD task with same uid exists."""
        with patch("app.lib.task_controller.common.Task") as mock_task:
            mock_task.objects.return_value.first.return_value = None  # no task

            from app.lib.task_controller.common import check_task_uniqueness

            mock_task_obj = MagicMock()
            mock_task_obj.name = "TestTask"
            mock_task_obj.callback_package = "test_pkg"
            mock_task_obj.callback_module = "test_mod"
            mock_task_obj.callback_handler = "test_handler"
            mock_task_obj.args = []
            mock_task_obj.kwargs = []
            mock_task_obj.scheduled_process_time = None

            result = check_task_uniqueness(mock_task_obj)
            self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
