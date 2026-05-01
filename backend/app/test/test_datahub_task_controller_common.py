import unittest
from app.model.data_retrive import KwArg
from app.lib.task_controller import common


class TestDatahubTaskControllerCommon(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def test_convert_kwarg_to_dict(self):
        given_input_1 = [
            KwArg(keyword="allow_update", arg="True"),
            KwArg(keyword="start_date", arg="2020-01-24"),
        ]
        exp_result_1 = {"allow_update": True, "start_date": "2020-01-24"}
        act_result_1 = common.convert_kwarg_to_dict(given_input_1)
        self.assertEqual(act_result_1, exp_result_1)

    def test_convert_dict_to_kwarg(self):
        exp_result_1 = [
            KwArg(keyword="allow_update", arg="True"),
            KwArg(keyword="start_date", arg="2020-01-24"),
        ]
        given_input_1 = {"allow_update": True, "start_date": "2020-01-24"}
        act_result_1 = common.convert_dict_to_kwarg(given_input_1)
        # Compare each field individually due to mongoengine document comparison issues
        self.assertEqual(len(act_result_1), len(exp_result_1))
        for i, (actual, expected) in enumerate(zip(act_result_1, exp_result_1)):
            self.assertEqual(
                actual.keyword, expected.keyword, f"Item {i} keyword mismatch"
            )
            self.assertEqual(actual.arg, expected.arg, f"Item {i} arg mismatch")


if __name__ == "__main__":
    unittest.main()
