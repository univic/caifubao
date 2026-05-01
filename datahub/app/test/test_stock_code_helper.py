"""Tests for stock code helper utility."""

from __future__ import annotations

import unittest


class TestAddMarketPrefix(unittest.TestCase):
    """Test cases for add_market_prefix function."""

    def test_sh_stock_code_already_has_prefix(self):
        """Test that sh prefix is preserved when already present."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("sh600000")
        self.assertEqual(result, "sh600000")

    def test_sz_stock_code_already_has_prefix(self):
        """Test that sz prefix is preserved when already present."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("sz000001")
        self.assertEqual(result, "sz000001")

    def test_bj_stock_code_already_has_prefix(self):
        """Test that bj prefix is preserved when already present."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("bj830000")
        self.assertEqual(result, "bj830000")

    def test_shanghai_stock_code_6_prefix(self):
        """Test that 6xxxxx codes get sh prefix."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("600000")
        self.assertEqual(result, "sh600000")

    def test_shanghai_stock_code_6_different_length(self):
        """Test that 6xxxxx codes with different lengths get sh prefix."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("688000")
        self.assertEqual(result, "sh688000")

    def test_shenzhen_stock_code_0_prefix(self):
        """Test that 0xxxxx codes get sz prefix."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("000001")
        self.assertEqual(result, "sz000001")

    def test_shenzhen_stock_code_3_prefix(self):
        """Test that 3xxxxx codes get sz prefix (ChiNext)."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("300001")
        self.assertEqual(result, "sz300001")

    def test_beijing_stock_code_4_prefix(self):
        """Test that 4xxxxx codes get bj prefix."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("430000")
        self.assertEqual(result, "bj430000")

    def test_beijing_stock_code_8_prefix(self):
        """Test that 8xxxxx codes get bj prefix."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("830000")
        self.assertEqual(result, "bj830000")

    def test_beijing_stock_code_9_prefix(self):
        """Test that 9xxxxx codes get bj prefix (STAR market)."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("920000")
        self.assertEqual(result, "bj920000")

    def test_invalid_stock_code_returns_none(self):
        """Test that invalid stock code patterns return None."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("12345")
        self.assertIsNone(result)

    def test_empty_string_returns_none(self):
        """Test that empty string returns None."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("")
        self.assertIsNone(result)

    def test_stock_code_as_integer(self):
        """Test that integer stock codes are handled correctly."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix(600000)
        self.assertEqual(result, "sh600000")

    def test_code_with_leading_zeros(self):
        """Test that codes with leading zeros are handled correctly."""
        from app.utilities.stock_code_helper import add_market_prefix

        result = add_market_prefix("000001")
        self.assertEqual(result, "sz000001")


if __name__ == "__main__":
    unittest.main()
