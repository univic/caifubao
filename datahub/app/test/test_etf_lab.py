# -*- coding: utf-8 -*-
"""ETF lab pipeline tests (pure functions, synthetic inputs)."""

import datetime
import json

import pandas as pd
import pytest

import app.lib.etf_lab.pipeline as pipeline
from app.lib.etf_lab.pipeline import (
    build_etf_panel,
    build_pool,
    benchmark_key,
    append_ledger,
    etf_round_trip_cost,
    factor_values,
    ic_report,
    ledger_entry,
    quantile_spread,
)


def _basic(rows):
    return pd.DataFrame(
        rows,
        columns=["ts_code", "name", "fund_type", "benchmark", "m_fee", "list_date"],
    )


def _daily(rows):
    return pd.DataFrame(rows, columns=["ts_code", "amount"])


def test_benchmark_key_collapses_wrappers_of_the_same_index():
    a = benchmark_key(
        "创业板算力基础设施指数收益率×100%", "易方达创业板算力基础设施ETF"
    )
    b = benchmark_key("创业板算力基础设施指数收益率×100%", "华夏创业板算力基础设施ETF")
    assert a == b
    assert benchmark_key("中证AAA科技创新公司债指数收益率", "某债ETF") != a
    # A missing benchmark falls back to the issuer-stripped name.
    assert benchmark_key(None, "南方中证A500ETF") == benchmark_key(
        None, "华夏中证A500ETF"
    )


def test_pool_keeps_only_the_most_liquid_equity_wrapper_per_theme():
    basic = _basic(
        [
            [
                "512470.SH",
                "广发中证工业有色金属主题ETF",
                "股票型",
                "中证工业有色金属主题指数收益率",
                0.5,
                None,
            ],
            [
                "512471.SH",
                "华夏中证工业有色金属主题ETF",
                "股票型",
                "中证工业有色金属主题指数收益率",
                0.15,
                None,
            ],
            ["511990.SH", "某货币ETF", "货币型", "活期存款利率", 0.2, None],
            [
                "511260.SH",
                "十年国债ETF",
                "债券型",
                "中债十年期国债指数收益率",
                0.3,
                None,
            ],
            [
                "515940.SH",
                "南方恒生A股电网设备ETF",
                "股票型",
                "恒生A股电网设备指数收益率",
                0.15,
                None,
            ],
        ]
    )
    daily = _daily(
        [
            ["512470.SH", 50000.0],
            ["512471.SH", 120000.0],
            ["511990.SH", 900000.0],
            ["511260.SH", 800000.0],
            ["515940.SH", 30000.0],
        ]
    )
    pool = build_pool(basic, daily, min_amount_kyuan=20000.0)
    codes = set(pool["ts_code"])
    # Bond and money-market wrappers are excluded by structure...
    assert "511990.SH" not in codes and "511260.SH" not in codes
    # ...and the duplicated theme keeps the more liquid wrapper.
    assert codes == {"512471.SH", "515940.SH"}
    assert len(pool) == pool["theme"].nunique()


def test_pool_applies_the_liquidity_floor():
    basic = _basic(
        [["510300.SH", "华泰柏瑞沪深300ETF", "股票型", "沪深300指数收益率", 0.5, None]]
    )
    daily = _daily([["510300.SH", 1000.0]])
    assert build_pool(basic, daily, min_amount_kyuan=20000.0).empty


def _prices(code_rows):
    return pd.DataFrame(
        code_rows,
        columns=[
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "vol",
            "amount",
            "pct_chg",
            "adj_factor",
        ],
    )


def test_panel_labels_are_t_plus_one_open_to_h_th_open():
    rows = []
    for index in range(6):
        day = f"2026010{index + 1}"
        rows.append(
            [
                "510300.SH",
                day,
                10.0 + index,
                11.0,
                9.0,
                10.5 + index,
                10.0 + index,
                1e6,
                1e5,
                0.5,
                1.0,
            ]
        )
    panel = build_etf_panel(_prices(rows), [1, 2])
    first = panel.iloc[0]
    # entry = row 2's open (11.0), exit h=1 = row 3's open (12.0)
    assert first["fwd_h1"] == pytest.approx(12.0 / 11.0 - 1.0)
    # exit h=2 = row 4's open (13.0)
    assert first["fwd_h2"] == pytest.approx(13.0 / 11.0 - 1.0)
    # The last two rows cannot resolve their exits.
    assert panel.iloc[-1]["blocked_h1"] == "no_next_session"
    assert pd.isna(panel.iloc[-1]["fwd_h1"])
    assert panel.iloc[-2]["blocked_h2"] == "no_exit_yet"


def test_panel_applies_adjustment_factor_and_drops_gaps():
    rows = [
        ["510300.SH", "20260101", 10.0, 10.0, 10.0, 10.0, 10.0, 1e6, 1e5, 0.0, 2.0],
        ["510300.SH", "20260102", 11.0, 11.0, 11.0, 11.0, 11.0, 1e6, 1e5, 0.0, 2.0],
        ["510300.SH", "20260103", 12.0, 12.0, 12.0, 12.0, 12.0, 1e6, 1e5, 0.0, 2.0],
    ]
    panel = build_etf_panel(_prices(rows), [1])
    assert panel.iloc[0]["open_hfq"] == pytest.approx(20.0)
    assert panel.iloc[0]["fwd_h1"] == pytest.approx(24.0 / 22.0 - 1.0)
    # An implausible next-day change (> ETF limit) is a data gap, not a move.
    rows[2][9] = 25.0
    panel = build_etf_panel(_prices(rows), [1])
    assert panel.iloc[0]["blocked_h1"] == "missing_price"


def test_panel_blocks_a_missing_market_session_in_one_etf():
    rows = [
        ["A", "20260101", 10.0, 10.0, 10.0, 10.0, 10.0, 1e6, 1e5, 0.0, 1.0],
        ["A", "20260103", 10.1, 10.1, 10.1, 10.1, 10.0, 1e6, 1e5, 1.0, 1.0],
        ["A", "20260104", 10.2, 10.2, 10.2, 10.2, 10.1, 1e6, 1e5, 1.0, 1.0],
        ["B", "20260101", 10.0, 10.0, 10.0, 10.0, 10.0, 1e6, 1e5, 0.0, 1.0],
        ["B", "20260102", 10.0, 10.0, 10.0, 10.0, 10.0, 1e6, 1e5, 0.0, 1.0],
        ["B", "20260103", 10.0, 10.0, 10.0, 10.0, 10.0, 1e6, 1e5, 0.0, 1.0],
        ["B", "20260104", 10.0, 10.0, 10.0, 10.0, 10.0, 1e6, 1e5, 0.0, 1.0],
    ]
    panel = build_etf_panel(_prices(rows), [1])
    first_a = panel.loc[panel["stock_code"] == "A"].iloc[0]
    assert first_a["blocked_h1"] == "missing_session_between"


def test_factors_are_backward_looking():
    rows = []
    for index in range(25):
        day = f"202601{index + 1:02d}"
        rows.append(
            [
                "510300.SH",
                day,
                10.0 + index,
                11.0,
                9.0,
                10.0 + index,
                10.0 + index,
                1e6,
                1e5,
                0.0,
                1.0,
            ]
        )
    panel = build_etf_panel(_prices(rows), [1, 5])
    momentum = factor_values(panel, "momentum_5")
    reversal = factor_values(panel, "reversal_5")
    assert momentum.iloc[:5].isna().all()
    assert momentum.iloc[10] == pytest.approx(
        panel["close_hfq"].iloc[10] / panel["close_hfq"].iloc[5] - 1.0
    )
    assert reversal.iloc[10] == pytest.approx(-momentum.iloc[10])
    with pytest.raises(KeyError):
        factor_values(panel, "no_such_factor")


def test_ic_and_spread_recover_a_planted_relationship():
    """A factor whose cross-section is planted to persist must show positive IC.

    Each name drifts at a name-specific rate, so ``momentum_1`` sorts the
    cross-section identically to the next session's return; the IC therefore has
    to be strongly positive and the top quantile must beat the bottom.
    """
    sessions = pd.bdate_range("2026-01-05", periods=40)
    rows = []
    for position in range(10):
        code = f"51{position:04d}.SH"
        for session, timestamp in enumerate(sessions):
            day = timestamp.strftime("%Y%m%d")
            price = 10.0 * (1 + 0.002 * position) ** session
            rows.append(
                [code, day, price, price, price, price, price, 1e6, 1e5, 0.0, 1.0]
            )
    panel = build_etf_panel(_prices(rows), [1, 5])
    report = ic_report(panel, "momentum_1", 1)
    assert report["ic_mean"] is not None
    assert report["ic_mean"] > 0.5
    spread = quantile_spread(panel, "momentum_1", 1, quantiles=5)
    assert spread["gross_top_minus_bottom"] > 0
    assert spread["top_avg_net"] > spread["quantiles"][0]


def test_etf_cost_has_no_stamp_duty():
    assert etf_round_trip_cost() == pytest.approx(0.0025)


def test_ledger_entry_keeps_rule_signal_and_evidence_kind():
    rule = {"codes": ["510300.SH"], "lookback": 250, "top_n": 1}
    signal = {"as_of": "2026-09-11", "selected": ["513100.SH"]}
    entry = ledger_entry(
        rule,
        signal,
        [{"code": "513100.SH", "selected": True, "reason": None}],
        now=datetime.datetime(2026, 9, 13, 21, 14, tzinfo=datetime.UTC),
    )
    assert entry["logged_at"] == "2026-09-13T21:14:00+00:00"
    assert entry["rule"] == rule
    assert entry["signal"] == signal
    assert entry["evidence_kind"] == "REPLAY"
    # The caller's dictionaries must not be retained, so a later mutation of the
    # report cannot rewrite an already-emitted record.
    rule["top_n"] = 99
    assert entry["rule"]["top_n"] == 1
    signal["selected"].append("510300.SH")
    assert entry["signal"]["selected"] == ["513100.SH"]


def test_append_ledger_adds_one_line_per_signal_and_creates_parents(tmp_path):
    path = tmp_path / "nested" / "forward-ledger.jsonl"
    for as_of in ("2026-09-11", "2026-09-14"):
        append_ledger(
            path,
            ledger_entry(
                {"codes": ["510300.SH"]},
                {"as_of": as_of, "selected": ["513100.SH"]},
                [],
                now=datetime.datetime(2026, 9, 14, tzinfo=datetime.UTC),
            ),
        )
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(line)["signal"]["as_of"] for line in lines] == [
        "2026-09-11",
        "2026-09-14",
    ]
    assert all(json.loads(line)["evidence_kind"] == "REPLAY" for line in lines)


def test_measure_cli_is_local_and_does_not_require_tushare_token(monkeypatch, capsys):
    panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-05"]),
            "stock_code": ["510300.SH"],
        }
    )
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(pipeline.pd, "read_parquet", lambda _: panel)
    monkeypatch.setattr(pipeline, "ic_report", lambda *args: {"ic_mean": None})
    monkeypatch.setattr(
        pipeline,
        "quantile_spread",
        lambda *args: {"gross_top_minus_bottom": None},
    )

    assert (
        pipeline.main(
            [
                "measure",
                "--panel",
                "frozen.parquet",
                "--factors",
                "momentum_20",
                "--horizons",
                "20",
            ]
        )
        == 0
    )
    assert "momentum_20    h20" in capsys.readouterr().out
