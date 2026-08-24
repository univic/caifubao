"""Tests for the legacy backend BaoStock interface."""

from app.lib.datahub.data_source.interface.baostock_interface import (
    BaostockInterfaceManager,
)


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
