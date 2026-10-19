# -*- coding: utf-8 -*-
"""Known-answer tests for the research-universe survivorship audit/backfill.

The audit is the artifact that tells a reader whether a replay's returns are an
upper bound, so its two states — a panel with no delisted cohort and a panel with
one — are pinned with synthetic frames, together with the ST/BSE shares, the
listed-code diff and the ST flag basis. The backfill is research-only by design:
these tests drive it through an injected fake tushare client (no network) and
assert that it writes one research parquet and imports nothing that could reach
MongoDB or production collections.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from app.lib.factor_lab import panel as panel_mod  # noqa: E402
from app.lib.factor_lab import universe  # noqa: E402

import research_universe_audit as audit_cli  # noqa: E402
import research_universe_backfill as backfill  # noqa: E402


# --------------------------------------------------------------------------
# audit_panel: synthetic frames
# --------------------------------------------------------------------------


def _row(code, day, **overrides):
    record = {
        "date": pd.Timestamp(day),
        "stock_code": code,
        "close": 10.0,
        "trade_status": 1,
        "is_bse": False,
        "is_st": False,
    }
    record.update(overrides)
    return record


def _frame(*rows):
    return pd.DataFrame.from_records(list(rows))


def _years(report):
    return {entry["year"]: entry for entry in report["years"]}


def test_panel_without_disappearance_flags_survivorship_gap():
    """The 2026-09-19 finding: no code ever vanishes, so returns are an upper
    bound and the report must say so."""
    frame = _frame(
        _row("sh600000", "2024-06-28"),
        _row("sz000001", "2024-06-28"),
        _row("sh600000", "2025-06-30"),
        _row("sz000001", "2025-06-30"),
    )
    report = universe.audit_panel(frame)

    assert report["survivorship_gap"] is True
    assert report["delisted_cohort"] == 0
    assert report["delisted_years"] == []
    assert report["st_flag_basis"] == "current_snapshot"
    assert report["st_flag_point_in_time"] is False
    assert report["missing_listed_codes"] is None
    assert report["missing_listed_count"] is None
    assert report["panel"]["codes"] == 2
    assert report["panel"]["sessions"] == 2
    assert report["panel"]["from"] == "2024-06-28"
    assert report["panel"]["to"] == "2025-06-30"
    assert any("upper bound" in line for line in report["limitations"])
    assert any("snapshot" in line for line in report["limitations"])

    years = _years(report)
    assert years[2024]["codes"] == 2
    assert years[2025]["codes"] == 2
    assert years[2024]["disappeared"] == 0
    # Both codes exist on the panel's first session, so neither is an entrant.
    assert report["first_seen_cohort"] == 0
    assert years[2025]["first_seen"] == 0


def test_one_disappearing_code_clears_the_gap_flag():
    frame = _frame(
        _row("sh600000", "2024-01-02"),
        _row("sh600000", "2024-12-31"),
        _row("sh600000", "2025-06-30"),
        _row("sh600000", "2025-12-31"),
        _row("sz000001", "2024-01-02"),
        _row("sz000001", "2024-12-31"),
    )
    report = universe.audit_panel(frame, listed_codes=["sh600000", "sz000001"])

    assert report["survivorship_gap"] is False
    assert report["delisted_cohort"] == 1
    assert report["delisted_years"] == [2024]
    assert report["panel"]["to"] == "2025-12-31"
    years = _years(report)
    assert years[2024]["disappeared"] == 1
    assert years[2025]["disappeared"] == 0
    assert years[2024]["codes"] == 2
    assert years[2025]["codes"] == 1
    assert not any("upper bound" in line for line in report["limitations"])


def test_first_seen_counts_entrants_after_the_panel_starts():
    frame = _frame(
        _row("sh600000", "2024-01-02"),
        _row("sh600000", "2025-12-31"),
        _row("sz300750", "2025-03-04"),
        _row("sz300750", "2025-12-31"),
    )
    report = universe.audit_panel(frame)

    years = _years(report)
    assert report["first_seen_cohort"] == 1
    assert report["first_seen_years"] == [2025]
    assert years[2024]["first_seen"] == 0
    assert years[2025]["first_seen"] == 1
    # A late entrant that is still present at the end is not a disappearance.
    assert report["survivorship_gap"] is True


def test_st_and_bse_shares_are_row_shares_with_code_counts():
    frame = _frame(
        _row("sh600001", "2025-01-02", is_st=True),
        _row("sh600001", "2025-01-03", is_st=True),
        _row("sz000002", "2025-01-02"),
        _row("bj430047", "2025-01-02", is_bse=True),
        _row("bj430047", "2025-06-30", is_bse=True, trade_status=0),
    )
    report = universe.audit_panel(frame)
    year = _years(report)[2025]

    assert year["rows"] == 5
    assert year["st_count"] == 2
    assert year["st_share"] == pytest.approx(0.4)
    assert year["st_codes"] == 1
    assert year["bse_count"] == 2
    assert year["bse_share"] == pytest.approx(0.4)
    assert year["bse_codes"] == 1
    # The suspended BSE row is counted, but it cannot enter a label.
    assert year["tradeable_pool"] == 4
    assert year["tradeable_codes"] == 3


def test_tradeable_pool_fails_closed_on_missing_close_and_status():
    frame = _frame(
        _row("sh600000", "2025-01-02"),
        _row("sh600000", "2025-01-03", close=None),
        _row("sh600000", "2025-01-06", close=0.0),
        _row("sh600000", "2025-01-07", trade_status=None),
    )
    year = _years(universe.audit_panel(frame))[2025]

    assert year["rows"] == 4
    assert year["tradeable_pool"] == 1
    assert year["tradeable_codes"] == 1


def test_missing_listed_codes_diff_accepts_ts_code_and_internal_forms():
    frame = _frame(
        _row("sh600000", "2025-01-02"),
        _row("sz000001", "2025-01-02"),
    )
    listed = [
        "sh600000",  # internal form, present
        "600000.SH",  # same code in ts_code form: collapsed, not double counted
        "000001.SZ",  # ts_code form, present
        "300750.SZ",  # missing
        "600519.SH",  # missing
    ]
    report = universe.audit_panel(frame, listed_codes=listed)

    assert report["listed_code_count"] == 4
    assert report["missing_listed_codes"] == ["sh600519", "sz300750"]
    assert report["missing_listed_count"] == 2
    assert report["listed_coverage"] == pytest.approx(0.5)
    assert any("absent from the panel" in line for line in report["limitations"])


def test_st_flag_basis_must_be_named_and_namechange_is_point_in_time():
    frame = _frame(
        _row("sh600000", "2025-01-02", is_st=True),
        _row("sh600000", "2025-06-30"),
    )

    historical = universe.audit_panel(frame, st_flag_basis="namechange_history")
    assert historical["st_flag_basis"] == "namechange_history"
    assert historical["st_flag_point_in_time"] is True
    assert not any("current snapshot" in line for line in historical["limitations"])

    with pytest.raises(ValueError, match="st_flag_basis"):
        universe.audit_panel(frame, st_flag_basis="guessed")


def test_empty_or_column_incomplete_frames_raise_a_clear_error():
    with pytest.raises(ValueError, match="non-empty"):
        universe.audit_panel(pd.DataFrame(columns=["date", "stock_code"]))
    with pytest.raises(ValueError, match="non-empty"):
        universe.audit_panel(None)

    with pytest.raises(ValueError, match="stock_code"):
        universe.audit_panel(_frame({"date": pd.Timestamp("2025-01-02")}))

    # Rows whose date cannot be parsed are dropped; if that empties the panel the
    # audit refuses instead of reporting an empty year table.
    with pytest.raises(ValueError, match="no usable"):
        universe.audit_panel(
            _frame(
                {"date": "not-a-date", "stock_code": "sh600000"},
            )
        )


def test_audit_does_not_mutate_the_input_frame():
    frame = _frame(
        _row("sh600000", "2025-01-02"),
        _row("sz000001", "2025-06-30"),
    )
    frame["stock_code"] = frame["stock_code"].astype("category")
    before = frame.copy(deep=True)

    report = universe.audit_panel(frame, listed_codes=["sh600519"])

    pd.testing.assert_frame_equal(frame, before)
    assert set(report["panel"]) == {"rows", "codes", "sessions", "from", "to", "years"}


def test_audit_accepts_a_real_build_panel_frame():
    """The audit reads the exported panel's own schema, not a bespoke one."""
    quotes = []
    for day in ("2025-01-02", "2025-01-03", "2025-01-06"):
        quotes.append(
            {
                "code": "sh600000",
                "date": pd.Timestamp(day),
                "open": 10.0,
                "open_hfq": 10.0,
                "close": 10.0,
                "close_hfq": 10.0,
                "high_hfq": 10.2,
                "low_hfq": 9.8,
                "volume": 1_000_000,
                "trade_amount": 10_000_000,
                "change_rate": 0.0,
                "previous_close": 10.0,
                "trade_status": 1,
                "is_bse": False,
                "is_st": False,
            }
        )
    quotes.append(quotes[0] | {"code": "bj430047", "is_bse": True})
    frame = panel_mod.build_panel(quotes, horizons=(1,), sessions=None)

    report = universe.audit_panel(frame, listed_codes=["sh600000"])

    year = _years(report)[2025]
    assert year["codes"] == 2
    assert year["bse_count"] == 1
    assert year["st_share"] == 0.0
    assert report["missing_listed_count"] == 0


def test_normalize_stock_code_handles_panel_and_tushare_forms():
    assert universe.normalize_stock_code("sh600000") == "sh600000"
    assert universe.normalize_stock_code("600000.SH") == "sh600000"
    assert universe.normalize_stock_code("SH600000") == "sh600000"
    assert universe.normalize_stock_code("000001") == "sz000001"
    assert universe.normalize_stock_code("430001") == "bj430001"
    assert universe.normalize_stock_code("") == ""
    assert universe.normalize_stock_code("not-a-code") == ""


# --------------------------------------------------------------------------
# research_universe_backfill: injected fake tushare client, no network
# --------------------------------------------------------------------------


def _delisted_frame():
    return pd.DataFrame.from_records(
        [
            {
                "ts_code": "600001.SH",
                "name": "A退",
                "exchange": "SSE",
                "list_date": "19990104",
                "delist_date": "20240315",
            },
            {
                "ts_code": "000002.SZ",
                "name": "B退",
                "exchange": "SZSE",
                "list_date": "19950101",
                "delist_date": "20250620",
            },
            {
                "ts_code": "430001.BJ",
                "name": "C退",
                "exchange": "BSE",
                "list_date": "20180101",
                "delist_date": "20250110",
            },
        ]
    )


def _namechange_frame():
    return pd.DataFrame.from_records(
        [
            {
                "ts_code": "600001.SH",
                "name": "ST A",
                "start_date": "20200101",
                "end_date": "20210101",
                "ann_date": "20191231",
                "change_reason": "ST",
            },
            {
                "ts_code": "600001.SH",
                "name": "A退",
                "start_date": "20210102",
                "end_date": None,
                "ann_date": "20201231",
                "change_reason": "撤销ST",
            },
            {
                "ts_code": "000002.SZ",
                "name": "B",
                "start_date": "20220101",
                "end_date": None,
                "ann_date": "20211231",
                "change_reason": "改名",
            },
        ]
    )


class FakePro:
    """Records every call; slices namechange by the requested window."""

    def __init__(self, delisted=None, namechange=None, quotes=None):
        # Named with a ``_frame`` suffix so the instance attribute cannot shadow
        # the tushare methods the backfill calls.
        self.delisted_frame = delisted
        self.namechange_frame = namechange
        self.quotes = quotes or {}
        self.calls = []

    def stock_basic(self, **kwargs):
        self.calls.append(("stock_basic", kwargs))
        return self.delisted_frame

    def namechange(self, **kwargs):
        self.calls.append(("namechange", kwargs))
        if self.namechange_frame is None or self.namechange_frame.empty:
            return self.namechange_frame
        changes = pd.to_datetime(self.namechange_frame["start_date"], format="%Y%m%d")
        mask = changes.between(
            pd.Timestamp(kwargs["start_date"]), pd.Timestamp(kwargs["end_date"])
        )
        return self.namechange_frame.loc[mask].reset_index(drop=True)

    def daily(self, **kwargs):
        self.calls.append(("daily", kwargs))
        return self.quotes.get(kwargs.get("ts_code"), pd.DataFrame())


def _fake_pro(**kwargs):
    return FakePro(delisted=_delisted_frame(), namechange=_namechange_frame(), **kwargs)


def test_backfill_writes_one_research_parquet_and_nothing_else(tmp_path):
    output = tmp_path / "research" / "universe_delisted.parquet"
    pro = _fake_pro()

    summary = backfill.run(
        pro=pro,
        output=output,
        from_date=date(2015, 1, 1),
        to_date=date(2026, 9, 18),
        pace=0,
    )

    # Exactly one file, in the requested directory, with the documented columns.
    assert [path.name for path in output.parent.iterdir()] == [output.name]
    frame = pd.read_parquet(output)
    assert list(frame.columns) == list(backfill.RESEARCH_COLUMNS)
    assert frame["code"].tolist() == ["bj430001", "sh600001", "sz000002"]
    assert frame.set_index("code").loc["sh600001", "delist_date"] == "2024-03-15"
    # ``exchange`` is derived from the tushare market suffix, not a raw column.
    assert frame.set_index("code").loc["sh600001", "exchange"] == "SSE"
    assert frame.set_index("code").loc["bj430001", "exchange"] == "BSE"
    assert frame.set_index("code").loc["sh600001", "list_date"] == "1999-01-04"
    assert int(frame.set_index("code").loc["sh600001", "namechange_count"]) == 2
    intervals = json.loads(
        frame.set_index("code").loc["sh600001", "namechange_intervals"]
    )
    assert [entry["name"] for entry in intervals] == ["ST A", "A退"]
    assert intervals[0] == {
        "name": "ST A",
        "start_date": "2020-01-01",
        "end_date": "2021-01-01",
        "change_reason": "ST",
        "ann_date": "2019-12-31",
    }
    # The BSE delisted name has no name-change interval and stays empty, not null.
    assert frame.set_index("code").loc["bj430001", "namechange_intervals"] == "[]"

    assert summary["research_only"] is True
    assert "research-only" in summary["warning"]
    assert summary["dry_run"] is False
    assert summary["output"] == str(output)
    assert summary["window"] == {"from": "2015-01-01", "to": "2026-09-18"}
    assert summary["counts"] == {
        "delisted_fetched": 3,
        "delisted_in_window": 3,
        "delisted_with_namechange": 2,
        "namechange_rows": 3,
        "namechange_codes": 2,
        "non_delisted_namechange_rows": 0,
        "bse": 1,
    }
    assert summary["date_range"]["list_date"] == {
        "from": "1995-01-01",
        "to": "2018-01-01",
    }
    assert summary["date_range"]["delist_date"] == {
        "from": "2024-03-15",
        "to": "2025-06-20",
    }
    assert summary["date_range"]["namechange"] == {
        "from": "2020-01-01",
        "to": "2022-01-01",
    }
    assert summary["residual_gaps"] == {
        "delisted_outside_window": 0,
        "delisted_without_delist_date": 0,
        "delisted_without_namechange": 1,
        "b_share_excluded": 0,
    }
    assert summary["quotes"] is None
    # Only the two read calls were made (one stock_basic, one namechange per
    # calendar year); no quote call because --with-quotes is off.
    called = [name for name, _ in pro.calls]
    assert called.count("stock_basic") == 1
    assert set(called) == {"stock_basic", "namechange"}


def test_backfill_never_imports_or_calls_a_production_write_path(tmp_path):
    """A subprocess proves the module graph: no app/model, Mongo or tushare."""
    output = tmp_path / "universe.parquet"
    driver = tmp_path / "driver.py"
    driver.write_text(
        "\n".join(
            [
                "import datetime, json, sys",
                f"sys.path.insert(0, {str(SCRIPTS)!r})",
                "import pandas as pd",
                "import research_universe_backfill as backfill",
                "",
                "class Pro:",
                "    def stock_basic(self, **kwargs):",
                "        return pd.DataFrame([{",
                '            "ts_code": "600001.SH", "name": "A退", "exchange": "SSE",',
                '            "list_date": "19990104", "delist_date": "20240315",',
                "        }])",
                "    def namechange(self, **kwargs):",
                "        return pd.DataFrame([{",
                '            "ts_code": "600001.SH", "name": "ST A",',
                '            "start_date": "20200101", "end_date": "20210101",',
                '            "ann_date": "20191231", "change_reason": "ST",',
                "        }])",
                "",
                "summary = backfill.run(",
                "    pro=Pro(),",
                f"    output={str(output)!r},",
                "    from_date=datetime.date(2015, 1, 1),",
                "    to_date=datetime.date(2026, 9, 18),",
                "    pace=0,",
                ")",
                "forbidden = sorted(",
                "    name for name in sys.modules",
                '    if name.startswith("app.")',
                '    or name.split(".")[0] in {"pymongo", "mongoengine", "tushare"}',
                ")",
                'print(json.dumps({"research_only": summary["research_only"],',
                '                 "written": summary["output"],',
                '                 "forbidden": forbidden}))',
            ]
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, str(driver)],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout.splitlines()[-1])

    assert payload["research_only"] is True
    assert payload["forbidden"] == []
    assert payload["written"] == str(output)
    assert output.exists()
    assert pd.read_parquet(output)["code"].tolist() == ["sh600001"]


def test_backfill_window_filters_the_delisted_cohort(tmp_path):
    output = tmp_path / "universe.parquet"
    # One name change inside the window, so the fetch itself is exercised while
    # only one instrument's delist_date falls in it.
    namechange = pd.DataFrame.from_records(
        [
            {
                "ts_code": "600001.SH",
                "name": "A退",
                "start_date": "20240301",
                "end_date": None,
                "ann_date": "20240229",
                "change_reason": "退市整理",
            }
        ]
    )
    pro = FakePro(delisted=_delisted_frame(), namechange=namechange)
    summary = backfill.run(
        pro=pro,
        output=output,
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
        pace=0,
    )

    assert summary["counts"]["delisted_fetched"] == 3
    assert summary["counts"]["delisted_in_window"] == 1
    assert summary["residual_gaps"]["delisted_outside_window"] == 2
    assert pd.read_parquet(output)["code"].tolist() == ["sh600001"]


def test_backfill_excludes_b_shares_from_the_research_cohort(tmp_path):
    """B-shares were never in the A-share panel; keeping them would make the
    listed-code diff report a false gap."""
    delisted = pd.concat(
        [
            _delisted_frame(),
            pd.DataFrame.from_records(
                [
                    {
                        "ts_code": "900901.SH",
                        "name": "B退",
                        "exchange": "SSE",
                        "list_date": "19920101",
                        "delist_date": "20240101",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    output = tmp_path / "universe.parquet"
    summary = backfill.run(
        pro=FakePro(delisted=delisted, namechange=_namechange_frame()),
        output=output,
        from_date=date(2015, 1, 1),
        to_date=date(2026, 9, 18),
        pace=0,
    )

    assert "sh900901" not in pd.read_parquet(output)["code"].tolist()
    assert summary["residual_gaps"]["b_share_excluded"] == 1
    assert summary["counts"]["delisted_in_window"] == 3


def test_backfill_dry_run_fetches_counts_and_writes_nothing(tmp_path):
    output = tmp_path / "dry" / "universe.parquet"
    summary = backfill.run(
        pro=_fake_pro(),
        output=output,
        from_date=date(2015, 1, 1),
        to_date=date(2026, 9, 18),
        dry_run=True,
        pace=0,
    )

    assert summary["dry_run"] is True
    assert summary["counts"]["delisted_in_window"] == 3
    assert not output.exists()
    assert not output.parent.exists()


def test_backfill_with_quotes_writes_a_sibling_quotes_parquet(tmp_path):
    quotes = {
        "600001.SH": pd.DataFrame.from_records(
            [
                {
                    "ts_code": "600001.SH",
                    "trade_date": "20240314",
                    "open": 1.0,
                    "close": 1.1,
                },
                {
                    "ts_code": "600001.SH",
                    "trade_date": "20240315",
                    "open": 1.1,
                    "close": 1.2,
                },
            ]
        )
    }
    output = tmp_path / "universe.parquet"
    pro = _fake_pro(quotes=quotes)

    summary = backfill.run(
        pro=pro,
        output=output,
        from_date=date(2015, 1, 1),
        to_date=date(2026, 9, 18),
        with_quotes=True,
        pace=0,
    )

    quotes_output = tmp_path / "universe.quotes.parquet"
    assert summary["quotes_output"] == str(quotes_output)
    assert summary["quotes"] == {"codes": 1, "rows": 2, "missing_codes": 2}
    stored = pd.read_parquet(quotes_output)
    assert stored["code"].tolist() == ["sh600001", "sh600001"]
    assert stored["date"].tolist() == [
        pd.Timestamp("2024-03-14"),
        pd.Timestamp("2024-03-15"),
    ]
    # The quote window is clipped to the instrument's own list/delist dates.
    daily_calls = {
        kwargs["ts_code"]: kwargs for name, kwargs in pro.calls if name == "daily"
    }
    assert daily_calls["600001.SH"]["start_date"] == "20150101"
    assert daily_calls["600001.SH"]["end_date"] == "20240315"
    assert {name for name, _ in pro.calls} == {"stock_basic", "namechange", "daily"}


def test_backfill_fails_loudly_on_empty_or_failed_results(tmp_path):
    output = tmp_path / "universe.parquet"

    with pytest.raises(RuntimeError, match="no rows"):
        backfill.run(
            pro=FakePro(delisted=pd.DataFrame(), namechange=_namechange_frame()),
            output=output,
            from_date=date(2015, 1, 1),
            to_date=date(2026, 9, 18),
            pace=0,
        )

    outside = FakePro(
        delisted=_delisted_frame().assign(delist_date="20300101"),
        namechange=_namechange_frame(),
    )
    with pytest.raises(RuntimeError, match="none with delist_date"):
        backfill.run(
            pro=outside,
            output=output,
            from_date=date(2015, 1, 1),
            to_date=date(2026, 9, 18),
            pace=0,
        )

    with pytest.raises(RuntimeError, match="namechange returned no rows"):
        backfill.run(
            pro=FakePro(delisted=_delisted_frame(), namechange=pd.DataFrame()),
            output=output,
            from_date=date(2015, 1, 1),
            to_date=date(2026, 9, 18),
            pace=0,
        )

    with pytest.raises(RuntimeError, match="no daily quotes"):
        backfill.run(
            pro=_fake_pro(),
            output=output,
            from_date=date(2015, 1, 1),
            to_date=date(2026, 9, 18),
            with_quotes=True,
            pace=0,
        )

    assert not output.exists()


def test_backfill_names_the_failing_tushare_call(tmp_path):
    class BrokenPro:
        def stock_basic(self, **kwargs):
            raise ConnectionError("tushare down")

    with pytest.raises(RuntimeError, match="stock_basic.*tushare down"):
        backfill.run(
            pro=BrokenPro(),
            output=tmp_path / "universe.parquet",
            from_date=date(2015, 1, 1),
            to_date=date(2026, 9, 18),
            pace=0,
        )


def test_backfill_rejects_a_backwards_window(tmp_path):
    with pytest.raises(ValueError, match="before from_date"):
        backfill.run(
            pro=_fake_pro(),
            output=tmp_path / "universe.parquet",
            from_date=date(2026, 1, 1),
            to_date=date(2025, 1, 1),
            pace=0,
        )


def test_backfill_main_prints_json_and_honours_dry_run(tmp_path, capsys):
    output = tmp_path / "universe.parquet"
    status = backfill.main(
        [
            "--from-date",
            "2015-01-01",
            "--to-date",
            "2026-09-18",
            "--output",
            str(output),
            "--dry-run",
            "--pace",
            "0",
        ],
        pro=_fake_pro(),
    )

    assert status == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is True
    assert payload["counts"]["delisted_in_window"] == 3
    assert not output.exists()


# --------------------------------------------------------------------------
# research_universe_audit: read-only CLI over a panel parquet
# --------------------------------------------------------------------------


def _write_panel(path, with_label=True):
    frame = _frame(
        _row("sh600000", "2024-01-02"),
        _row("sh600000", "2025-06-30"),
        _row("sz000001", "2024-01-02", is_st=True),
        _row("sz000001", "2024-06-28"),
    )
    if with_label:
        frame["fwd_h20"] = 0.01
    frame.to_parquet(path, index=False)
    return path


def test_audit_cli_reads_only_the_audit_columns(tmp_path):
    path = _write_panel(tmp_path / "panel.parquet")

    loaded = audit_cli.load_panel(path)

    assert set(loaded.columns) == set(audit_cli.AUDIT_COLUMNS)
    assert "fwd_h20" not in loaded.columns


def test_audit_cli_prints_the_report_and_never_writes(tmp_path, capsys):
    path = _write_panel(tmp_path / "panel.parquet")
    before = {entry.name for entry in tmp_path.iterdir()}

    status = audit_cli.main(["--panel", str(path)])

    assert status == 0
    report = json.loads(capsys.readouterr().out)
    assert report["panel_path"] == str(path)
    assert report["survivorship_gap"] is True
    assert report["delisted_cohort"] == 1
    assert any("upper bound" in line for line in report["limitations"])
    assert report["st_flag_basis"] == "current_snapshot"
    assert report["st_flag_point_in_time"] is False
    assert _years(report)[2024]["disappeared"] == 1
    assert _years(report)[2024]["st_count"] == 1
    assert {entry.name for entry in tmp_path.iterdir()} == before


def test_audit_cli_diffs_a_backfill_listing_parquet(tmp_path, capsys):
    panel = _write_panel(tmp_path / "panel.parquet")
    listing = tmp_path / "universe_delisted.parquet"
    pd.DataFrame.from_records(
        [
            {"code": "sh600000", "name": "A退"},
            {"code": "sz300750", "name": "B退"},
        ]
    ).to_parquet(listing, index=False)

    status = audit_cli.main(
        [
            "--panel",
            str(panel),
            "--listing",
            str(listing),
            "--listed-codes",
            "600519.SH",
            "--st-flag-basis",
            "namechange_history",
        ]
    )

    assert status == 0
    report = json.loads(capsys.readouterr().out)
    assert report["listed_code_count"] == 3
    assert report["missing_listed_codes"] == ["sh600519", "sz300750"]
    assert report["st_flag_basis"] == "namechange_history"
    assert report["st_flag_point_in_time"] is True
