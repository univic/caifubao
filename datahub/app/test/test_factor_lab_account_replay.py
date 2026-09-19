"""Synthetic-panel tests for the no-ex-post account replay harness.

These tests use a hand-built panel (no production data) to pin the mechanics the
research conclusion depends on:

* limit-up blocks an entry (and the book stays empty while it persists),
* limit-down blocks an exit and the exit is retried on the next session,
* a long quote gap is written off as a delisting, never dropped,
* marking falls back from ``close_hfq`` to ``close``.

They are unit-scale assertions on account behaviour, not performance claims.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import factor_lab_account_replay as replay  # noqa: E402
import factor_lab_composite_backtest as core  # noqa: E402

SESSIONS = 200
CODES = ["N1", "N2", "N3", "N4", "ONLY", "GONE"]
ELIGIBLE = {"ONLY", "GONE"}
STEP = 60
NAMES = 2
AUM = 20000


def _prices(rng: np.random.Generator) -> dict[str, np.ndarray]:
    out = {}
    for index, code in enumerate(CODES):
        drift = 0.0004 * (index - 2.5)
        shocks = rng.normal(drift, 0.02, SESSIONS)
        out[code] = 10.0 * np.exp(np.cumsum(shocks))
    return out


def _field_default(field: str, price: np.ndarray, index: int, label: np.ndarray):
    if field == "close":
        return float(price[index])
    if field == "open":
        return float(price[index] * 0.995)
    if field == "high":
        return float(price[index] * 1.01)
    if field == "low":
        return float(price[index] * 0.99)
    if field == "pre_close":
        return float(price[index - 1]) if index else float(price[0])
    if "hfq" in field:
        return float(price[index])
    if field == "fq_factor":
        return 1.0
    if field.startswith("is_") or field in {"limit_up", "limit_down"}:
        return False
    if field == "trade_status":
        return 1
    if field.startswith("fwd_"):
        return float(label[index])
    if "amount" in field or "volume" in field or field.endswith("vol"):
        return 1.0e8
    return 1.0


def build_panel(
    tmp_path: Path, *, all_limit_up: bool, include_gone: bool = True
) -> Path:
    """Panel where only ONLY and GONE are tradable; GONE quotes stop after 61.

    Anomalies injected (session index):
      1..   all codes limit-up on session 1 is not used; the limit-up entry block
            is injected at session 121 (the first rebalance after GONE vanishes),
      121   limit-up on every code -> the intended ONLY entry must fail,
      181   limit-down on ONLY -> its pending exit must fail and be retried on
            session 182,
      >=170 ONLY is ST -> the session-180 target is empty, forcing that exit,
      >=62  GONE has no rows -> long quote gap -> written off after 20 misses,
      100   ONLY's close_hfq is NaN -> marking must fall back to close.
    """
    rng = np.random.default_rng(7)
    prices = _prices(rng)
    codes_used = [code for code in CODES if include_gone or code != "GONE"]
    dates = pd.bdate_range("2020-01-02", periods=SESSIONS)
    labels = {}
    for code in codes_used:
        price = prices[code]
        values = np.full(SESSIONS, np.nan)
        values[:-20] = price[20:] / price[:-20] - 1.0
        labels[code] = values
    fields = list(core._PANEL_FIELDS)
    rows: dict[str, list] = {field: [] for field in fields}
    date_col: list = []
    codes: list[str] = []
    for code in codes_used:
        for index in range(SESSIONS):
            if code == "GONE" and index > 61:
                continue
            for field in fields:
                rows[field].append(
                    _field_default(field, prices[code], index, labels[code])
                )
            date_col.append(dates[index])
            codes.append(code)
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(pd.Series(date_col))
    frame["stock_code"] = codes
    frame["is_bse"] = ~frame["stock_code"].isin(ELIGIBLE)
    frame["is_st"] = False
    frame["limit_up"] = False
    frame["limit_down"] = False
    frame["trade_status"] = 1
    previous = frame.groupby("stock_code")["close"].shift(1)
    frame["previous_close"] = previous.fillna(frame["close"])
    # ONLY is ST from session 170 so the final rebalance target is empty.
    only_st = (frame["stock_code"] == "ONLY") & (frame["date"] >= dates[170])
    frame.loc[only_st, "is_st"] = True
    # Marking fallback fixture: close_hfq missing, raw close present.
    fallback = (frame["stock_code"] == "ONLY") & (frame["date"] == dates[100])
    frame.loc[fallback, "close_hfq"] = np.nan
    if all_limit_up:
        frame["limit_up"] = True
    else:
        frame.loc[frame["date"] == dates[121], "limit_up"] = True
        frame.loc[
            (frame["stock_code"] == "ONLY") & (frame["date"] == dates[181]),
            "limit_down",
        ] = True
    label = np.full(len(frame), np.nan)
    frame = frame.reset_index(drop=True)
    frame["fwd_h20"] = np.nan
    for code, group in frame.groupby("stock_code"):
        ordered = group.sort_values("date")
        raw = ordered["close"].to_numpy(dtype="float64")
        values = np.full(len(raw), np.nan)
        values[:-20] = raw[20:] / raw[:-20] - 1.0
        frame.loc[ordered.index, "fwd_h20"] = values
    del label
    tag = "limit_up" if all_limit_up else ("retry" if not include_gone else "normal")
    path = tmp_path / f"panel_{tag}.parquet"
    frame.to_parquet(path, index=False)
    return path


def run_replay(panel: Path, tmp_path: Path) -> dict:
    output = tmp_path / "out.json"
    argv = [
        "--panel",
        str(panel),
        "--label",
        "fwd_h20",
        "--step",
        str(STEP),
        "--names",
        str(NAMES),
        "--aum",
        str(AUM),
        "--lookback",
        "20",
        "--min-ic-dates",
        "10",
        "--output",
        str(output),
    ]
    completed = subprocess.run(
        [sys.executable, str(SCRIPTS / "factor_lab_account_replay.py"), *argv],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr[-2000:]
    return json.loads(output.read_text(encoding="utf-8"))


def test_tradeability_gates() -> None:
    base = {"open": 10.0, "trade_status": 1, "limit_up": False, "limit_down": False}
    assert replay.tradeable_open(base, "buy") == 10.0
    assert replay.tradeable_open(base, "sell") == 10.0
    assert replay.tradeable_open({**base, "limit_up": True}, "buy") is None
    assert replay.tradeable_open({**base, "limit_up": True}, "sell") == 10.0
    assert replay.tradeable_open({**base, "limit_down": True}, "sell") is None
    assert replay.tradeable_open({**base, "limit_down": True}, "buy") == 10.0
    assert replay.tradeable_open({**base, "trade_status": 0}, "buy") is None
    assert replay.tradeable_open({**base, "trade_status": None}, "sell") is None
    assert replay.tradeable_open({**base, "open": float("nan")}, "buy") is None
    assert replay.tradeable_open({**base, "open": 0.0}, "buy") is None
    assert replay.tradeable_open(None, "buy") is None


def test_mark_price_fallback() -> None:
    assert replay.mark_price({"close_hfq": float("nan"), "close": 7.5}) == 7.5
    assert replay.mark_price({"close_hfq": None, "close": 7.5}) == 7.5
    assert replay.mark_price({"close_hfq": 0.0, "close": 7.5}) == 7.5
    assert replay.mark_price({"close_hfq": 12.0, "close": 7.5}) == 12.0
    assert replay.mark_price({"close_hfq": float("nan"), "close": float("nan")}) is None
    assert replay.mark_price({}) is None
    assert replay.mark_price(None) is None


@pytest.fixture(scope="module")
def normal_report(tmp_path_factory: pytest.TempPathFactory) -> dict:
    tmp_path = tmp_path_factory.mktemp("replay-normal")
    panel = build_panel(tmp_path, all_limit_up=False)
    return run_replay(panel, tmp_path)


@pytest.fixture(scope="module")
def retry_report(tmp_path_factory: pytest.TempPathFactory) -> dict:
    tmp_path = tmp_path_factory.mktemp("replay-retry")
    panel = build_panel(tmp_path, all_limit_up=False, include_gone=False)
    return run_replay(panel, tmp_path)


@pytest.fixture(scope="module")
def limit_up_report(tmp_path_factory: pytest.TempPathFactory) -> dict:
    tmp_path = tmp_path_factory.mktemp("replay-limit-up")
    panel = build_panel(tmp_path, all_limit_up=True)
    return run_replay(panel, tmp_path)


def _variant(report: dict) -> dict:
    return report["decision_time_daily"]


def test_sessions_cover_panel(normal_report: dict) -> None:
    assert normal_report["step_sessions"] == STEP
    assert _variant(normal_report)["sessions"] == SESSIONS


def test_limit_up_blocks_entry(normal_report: dict) -> None:
    book = _variant(normal_report)
    assert book["failed_entry_fills"] >= 1
    # ONLY is never acquired: the one entry attempt hits limit-up (121) and the
    # closing rebalance has an empty target (ONLY is ST from 170).
    assert book["final_positions"] == 1
    assert book["delisted_positions"] == ["GONE"]


def test_persistent_limit_up_never_buys(limit_up_report: dict) -> None:
    book = _variant(limit_up_report)
    assert book["failed_entry_fills"] >= 1
    assert book["final_positions"] == 0
    assert book["total_return"] == 0.0
    assert book["cash_share_avg"] == 1.0
    assert book["delisted_positions"] == []


def test_limit_down_exit_is_retried(normal_report: dict) -> None:
    book = _variant(normal_report)
    assert book["failed_exit_fills"] >= 1
    # GONE's quotes never return, so its pending exit (and the position itself)
    # stay in the book as an unwritten claim; ONLY's exit was carried out on the
    # 182 retry after the 181 limit-down block.
    assert book["pending_exits"] == 1
    assert book["final_positions"] == 1
    assert book["delisted_positions"] == ["GONE"]


def test_blocked_exit_completes_on_retry(retry_report: dict) -> None:
    book = _variant(retry_report)
    # No GONE in this panel: the only pending exit is ONLY's, blocked by
    # limit-down on the rebalance execution session and filled on the next one.
    assert book["failed_entry_fills"] >= 1
    assert book["failed_exit_fills"] >= 1
    assert book["pending_exits"] == 0
    assert book["final_positions"] == 0


def test_long_quote_gap_is_written_off(normal_report: dict) -> None:
    book = _variant(normal_report)
    assert book["delisted_positions"] == ["GONE"]
    assert book["delisted_write_off_cny"] > 0


def test_variants_are_distinct(normal_report: dict) -> None:
    assert set(normal_report) >= {
        "ex_post_screened_rebalance_sampled",
        "decision_time_rebalance_sampled",
        "decision_time_daily",
    }
    assert "turnover_annual" in _variant(normal_report)
