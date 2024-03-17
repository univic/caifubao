from unittest import TestCase
from app.utilities.progress_bar import get_formatted_duration_str


class Test(TestCase):
    def test_less_than_60_seconds(self):
        self.assertEqual(get_formatted_duration_str(5.123), '5.12s')

    def test_exactly_60_seconds(self):
        self.assertEqual(get_formatted_duration_str(60.0), '1m0s')

    def test_greater_than_60_seconds_less_than_3600(self):
        self.assertEqual(get_formatted_duration_str(90.765), '1m30s')

    def test_exactly_3600_seconds(self):
        self.assertEqual(get_formatted_duration_str(3600.0), '1h0m0s')

    def test_greater_than_3600_seconds(self):
        self.assertEqual(get_formatted_duration_str(3661.234), '1h1m1s')

    # def test_negative_input(self):
    #     self.assertEqual(get_formatted_duration_str(-180.0), '3m0s')  # 假设负数被处理为正数

    def test_zero_input(self):
        self.assertEqual(get_formatted_duration_str(0.0), '0.00s')

    def test_boundary_3600_seconds(self):
        self.assertEqual(get_formatted_duration_str(3599.999), '59m59s')

    def test_boundary_60_seconds(self):
        self.assertEqual(get_formatted_duration_str(59.999), '59.99s')

    def test_boundary_exactly_1_hour(self):
        self.assertEqual(get_formatted_duration_str(3600.001), '1h0m0s')
