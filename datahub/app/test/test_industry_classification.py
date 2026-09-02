"""Tests for the CSRC industry classification sync handler.

Baostock ``query_stock_industry`` returns rows in the order
``[updateDate, code, code_name, industry, industryClassification]`` where
``industry`` is a CSRC (证监会) string such as ``J66货币金融服务``. These tests
lock the corrected column mapping and CSRC parsing so the earlier
column-shift bug (which wrote ``updateDate`` into ``stock_code``) cannot
regress.
"""

from types import SimpleNamespace

from app.lib.datahub.data_integrity_keeper.handler import (
    industry_classification as handler,
)


class _FakeResultSet:
    def __init__(self, rows):
        self.rows = rows
        self.error_code = "0"
        self.error_msg = ""
        self._index = 0

    def next(self):
        return self._index < len(self.rows)

    def get_row_data(self):
        row = self.rows[self._index]
        self._index += 1
        return list(row)


def _sample_rows():
    # Real baostock format: [updateDate, code, code_name, industry, class]
    return [
        ["2026-08-31", "sh.600000", "浦发银行", "J66货币金融服务", "证监会行业分类"],
        ["2026-08-31", "sh.600001", "邯郸钢铁", "", "证监会行业分类"],
        ["2026-08-31", "sh.600004", "白云机场", "G56航空运输业", "证监会行业分类"],
    ]


def test_parse_csrc_industry():
    assert handler._parse_csrc_industry("J66货币金融服务") == ("J66", "货币金融服务")
    assert handler._parse_csrc_industry("C36汽车制造业") == ("C36", "汽车制造业")
    assert handler._parse_csrc_industry("") == (None, None)
    assert handler._parse_csrc_industry("foo") == (None, None)  # no digit code


def test_sync_writes_code_from_baostock_code_column(monkeypatch):
    captured = []

    class FakeIndustry:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def save(self):
            pass

        @staticmethod
        def objects(**kwargs):
            return SimpleNamespace(first=lambda: None)

    monkeypatch.setattr(handler, "StockIndustryClassification", FakeIndustry)
    monkeypatch.setattr("baostock.login", lambda: SimpleNamespace(error_code="0"))
    monkeypatch.setattr(
        "baostock.query_stock_industry", lambda: _FakeResultSet(_sample_rows())
    )
    monkeypatch.setattr("baostock.logout", lambda: None)

    result = handler.sync_industry_classification()

    assert result["status"] == "GOOD"
    assert result["new_classifications"] == 2  # sh.600001 has empty industry
    assert result["skipped"] == 1

    by_code = {row["stock_code"]: row for row in captured}
    # stock_code must come from baostock column 1, never the updateDate column 0
    assert "2026-08-31" not in by_code
    assert by_code["sh.600000"]["stock_code"] == "sh.600000"
    assert by_code["sh.600000"]["industry_code_sw_l1"] == "J66"
    assert by_code["sh.600000"]["industry_name_sw_l1"] == "货币金融服务"
    assert by_code["sh.600004"]["industry_code_sw_l1"] == "G56"
    assert by_code["sh.600004"]["industry_name_sw_l1"] == "航空运输业"
    # CSRC has no L2 subdivision
    assert by_code["sh.600000"]["industry_code_sw_l2"] is None
    assert by_code["sh.600000"]["industry_name_sw_l2"] is None
