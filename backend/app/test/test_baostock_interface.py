"""Tests for the legacy backend BaoStock interface."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


MODULE_PATH = (
    Path(__file__).parents[1]
    / "lib/datahub/data_source/interface/baostock_interface.py"
)
SPEC = spec_from_file_location("backend_baostock_interface", MODULE_PATH)
MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
BaostockInterfaceManager = MODULE.BaostockInterfaceManager


def test_resultset_to_dataframe_preserves_rows_across_pages():
    class PaginatedResult:
        error_code = "0"
        fields = ["date", "code"]

        def __init__(self):
            self.pages = [
                [["1999-11-10", "sh.600000"]],
                [["2026-08-21", "sh.600000"]],
            ]
            self.page_index = 0
            self.row_index = 0

        def next(self):
            if self.row_index < len(self.pages[self.page_index]):
                return True
            if self.page_index + 1 >= len(self.pages):
                return False
            self.page_index += 1
            self.row_index = 0
            return True

        def get_row_data(self):
            row = self.pages[self.page_index][self.row_index]
            self.row_index += 1
            return row

    result = BaostockInterfaceManager._resultset_to_dataframe(PaginatedResult())

    assert result["date"].tolist() == ["1999-11-10", "2026-08-21"]
