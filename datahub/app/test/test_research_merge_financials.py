# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "research_merge_financials.py"
)
spec = importlib.util.spec_from_file_location("research_merge_financials", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


def _panel():
    return pd.DataFrame(
        {
            "stock_code": ["000001", "000001", "000001"],
            "date": pd.to_datetime(["2024-04-01", "2024-04-30", "2024-05-02"]),
            "close": [10.0, 10.5, 11.0],
        }
    )


def _financial():
    return pd.DataFrame(
        {
            "code": ["000001", "000001", "000001"],
            "ann_date": pd.to_datetime(["2024-04-28", "2024-04-28", "2024-05-05"]),
            "end_date": pd.to_datetime(["2024-03-31", "2023-12-31", "2024-03-31"]),
            "report_type": ["1", "1", "1"],
            "roe": [8.0, 7.0, 9.0],
        }
    )


def test_report_announced_after_date_is_invisible():
    merged = module.merge_financials(_panel(), _financial())
    assert merged.loc[0, "roe"] is None or pd.isna(merged.loc[0, "roe"])
    # 2024-04-01 is before any announcement: no report yet.
    assert pd.isna(merged.loc[0, "financial_ann_date"])


def test_exact_announcement_date_is_visible_and_latest_end_date_wins():
    merged = module.merge_financials(_panel(), _financial())
    row = merged[merged["date"] == pd.Timestamp("2024-04-30")].iloc[0]
    assert row["financial_ann_date"] == pd.Timestamp("2024-04-28")
    # Both 2023-12-31 and 2024-03-31 were announced 2024-04-28; latest period wins.
    assert row["financial_end_date"] == pd.Timestamp("2024-03-31")
    assert row["roe"] == 8.0


def test_later_report_never_leaks_backwards():
    merged = module.merge_financials(_panel(), _financial())
    before = merged[merged["date"] == pd.Timestamp("2024-04-30")].iloc[0]
    after = merged[merged["date"] == pd.Timestamp("2024-05-02")].iloc[0]
    assert before["roe"] == 8.0
    assert after["roe"] == 8.0  # 2024-05-05 report is still future


def test_panel_row_count_and_industry_attach():
    industry = pd.DataFrame({"code": ["000001"], "industry": ["J"]})
    merged = module.merge_financials(_panel(), _financial(), industry)
    assert len(merged) == 3
    assert set(merged["industry"]) == {"J"}


def test_unknown_industry_is_filled():
    merged = module.merge_financials(_panel(), _financial())
    assert set(merged["industry"]) == {"UNKNOWN"}


def test_empty_financial_frame_is_rejected():
    with pytest.raises(ValueError):
        module.merge_financials(_panel(), pd.DataFrame())


def test_interleaved_stocks_use_only_their_own_visible_report():
    panel = pd.DataFrame(
        {
            "stock_code": ["000001", "000002", "000001", "000002"],
            "date": pd.to_datetime(
                ["2024-04-30", "2024-04-30", "2024-05-02", "2024-05-02"]
            ),
        }
    )
    financial = pd.DataFrame(
        {
            "code": ["000001", "000002", "000001", "000002"],
            "ann_date": pd.to_datetime(
                ["2024-04-28", "2024-04-29", "2024-04-28", "2024-04-29"]
            ),
            "end_date": pd.to_datetime(
                ["2024-03-31", "2024-03-31", "2023-12-31", "2023-12-31"]
            ),
            "report_type": ["1", "1", "1", "1"],
            "roe": [8.0, 9.0, 7.0, 6.0],
        }
    )
    merged = module.merge_financials(panel, financial)
    first = merged[merged["date"] == pd.Timestamp("2024-04-30")]
    assert first.set_index("stock_code")["roe"].to_dict() == {
        "000001": 8.0,
        "000002": 9.0,
    }


def test_chunked_writer_preserves_rows_and_keeps_later_report_invisible(tmp_path):
    panel = _panel()
    panel["close_hfq"] = panel["close"]
    panel["total_mv"] = 1_000.0
    source = tmp_path / "panel.parquet"
    output = tmp_path / "merged.parquet"
    panel.to_parquet(source, index=False)
    result = module.merge_to_parquet(
        str(source), str(output), _financial(), pd.DataFrame()
    )
    assert result["merged_rows"] == len(panel)
    written = pd.read_parquet(output)
    assert len(written) == len(panel)
    before = written[written["date"] == pd.Timestamp("2024-04-01")].iloc[0]
    assert pd.isna(before["financial_ann_date"])
    after = written[written["date"] == pd.Timestamp("2024-04-30")].iloc[0]
    assert after["roe"] == 8.0


def test_consolidated_report_type_wins_same_announcement():
    panel = _panel().iloc[[1]]
    financial = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "ann_date": pd.to_datetime(["2024-04-28", "2024-04-28"]),
            "end_date": pd.to_datetime(["2024-03-31", "2024-03-31"]),
            "report_type": [5, 1],
            "roe": [5.0, 8.0],
        }
    )
    merged = module.merge_financials(panel, financial)
    assert merged.loc[0, "roe"] == 8.0
    reversed_rows = module.merge_financials(panel, financial.iloc[::-1])
    assert reversed_rows.loc[0, "roe"] == 8.0


def test_already_merged_panel_is_rejected():
    panel = _panel()
    panel["roe"] = 1.0
    with pytest.raises(ValueError, match="already carries financial"):
        module.merge_financials(panel, _financial())


def test_multi_chunk_writer_matches_single_chunk(tmp_path):
    panel = _panel()
    panel["close_hfq"] = panel["close"]
    panel["total_mv"] = 1_000.0
    source = tmp_path / "panel.parquet"
    panel.to_parquet(source, index=False)
    single = tmp_path / "single.parquet"
    chunked = tmp_path / "chunked.parquet"
    module.merge_to_parquet(str(source), str(single), _financial(), pd.DataFrame())
    module.merge_to_parquet(
        str(source),
        str(chunked),
        _financial(),
        pd.DataFrame(),
        sessions_per_chunk=1,
    )
    pd.testing.assert_frame_equal(pd.read_parquet(single), pd.read_parquet(chunked))


def test_later_restatement_of_older_period_beats_newer_period():
    panel = pd.DataFrame(
        {"stock_code": ["000001"], "date": pd.to_datetime(["2024-05-02"])}
    )
    financial = pd.DataFrame(
        {
            "code": ["000001"] * 3,
            "ann_date": pd.to_datetime(["2024-04-28", "2024-04-28", "2024-05-01"]),
            "end_date": pd.to_datetime(["2023-12-31", "2024-03-31", "2023-12-31"]),
            "report_type": [1, 1, 1],
            "roe": [7.0, 8.0, 9.0],
        }
    )
    merged = module.merge_financials(panel, financial)
    assert merged.loc[0, "financial_end_date"] == pd.Timestamp("2023-12-31")
    assert merged.loc[0, "roe"] == 9.0
