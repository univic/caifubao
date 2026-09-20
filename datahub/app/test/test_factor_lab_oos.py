"""Synthetic-panel tests for the rolling out-of-sample (purge + embargo) mode.

No production data and no network: a hand-built panel is written to ``tmp_path``
and the CLI is run in a subprocess (subprocess isolation avoids pytest-capture
and logging breakage). The panel's forward label is built with the production
open-to-open convention (``open(t+1) -> open(t+1+h)``) so the tests derive the
execution lag from the data instead of copying the implementation's ``+1``.

The assertions pin the protocol mechanics -- the purge cutoff boundary, fold
tiling, the short-panel and bad-flag refusals and the logged config -- not
performance numbers, which are research results rather than contracts.
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

import factor_lab_composite_backtest as core  # noqa: E402

SESSIONS = 400
CODES = ["N1", "N2", "N3", "N4", "N5", "N6"]
HORIZON = 20
EXECUTION_LAG = 1
LABEL = f"fwd_h{HORIZON}"
TRAIN = 120
TEST = 60
STEP = 20
NAMES = 2
AUM = 20000
LOOKBACK = 40
MIN_IC_DATES = 10


def _field_default(field: str, price: np.ndarray, session: int):
    if field == "close":
        return float(price[session])
    if field == "open":
        return float(price[session] * 0.995)
    if "hfq" in field:
        return float(price[session])
    if field.startswith("is_"):
        return False
    if "amount" in field or "volume" in field:
        return 1.0e8
    return 1.0


def _forward_label(opens: np.ndarray, horizon: int, lag: int) -> np.ndarray:
    """Production label: ``open(t + lag) -> open(t + lag + horizon)``.

    For ``fwd_h20`` with ``lag=1`` that is ``open(t+1) -> open(t+21)``, so the IC
    computed at ``t`` is only observable after session ``t + 1 + 20``.
    """
    values = np.full(len(opens), np.nan)
    values[: -(horizon + lag)] = opens[horizon + lag :] / opens[lag:-horizon] - 1.0
    return values


def build_panel(
    tmp_path: Path, sessions: int = SESSIONS
) -> tuple[Path, pd.DatetimeIndex]:
    """Hand-built panel with the columns the factor lab and the replay need."""
    rng = np.random.default_rng(11)
    dates = pd.bdate_range("2020-01-02", periods=sessions)
    fields = list(core._PANEL_FIELDS)
    rows: dict[str, list] = {field: [] for field in fields}
    date_col: list = []
    codes: list[str] = []
    for index, code in enumerate(CODES):
        drift = 0.0003 * (index - 2.5)
        price = 10.0 * np.exp(np.cumsum(rng.normal(drift, 0.018, sessions)))
        for session in range(sessions):
            for field in fields:
                rows[field].append(_field_default(field, price, session))
            date_col.append(dates[session])
            codes.append(code)
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(pd.Series(date_col))
    frame["stock_code"] = codes
    frame = frame.sort_values(["date", "stock_code"]).reset_index(drop=True)
    frame["previous_close"] = (
        frame.groupby("stock_code")["close"].shift(1).fillna(frame["close"])
    )
    frame["trade_status"] = 1
    frame["limit_up"] = False
    frame["limit_down"] = False
    frame[LABEL] = np.nan
    for _, group in frame.groupby("stock_code", sort=False):
        opens = group["open"].to_numpy(dtype="float64")
        frame.loc[group.index, LABEL] = _forward_label(opens, HORIZON, EXECUTION_LAG)
    path = tmp_path / f"panel_{sessions}.parquet"
    frame.to_parquet(path, index=False)
    return path, dates


def cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPTS / "factor_lab_composite_backtest.py"), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def run_oos(
    panel: Path,
    tmp_path: Path,
    *,
    name: str = "oos.json",
    step: int | None = STEP,
    embargo: int = 0,
    modes: tuple[str, ...] = ("oos",),
) -> dict:
    output = tmp_path / name
    argv = [
        "--panel",
        str(panel),
        "--label",
        LABEL,
        "--lookback",
        str(LOOKBACK),
        "--min-ic-dates",
        str(MIN_IC_DATES),
        "--names",
        str(NAMES),
        "--aum",
        str(AUM),
        "--train-sessions",
        str(TRAIN),
        "--test-sessions",
        str(TEST),
        "--embargo",
        str(embargo),
        "--output",
        str(output),
    ]
    for mode in modes:
        argv += ["--mode", mode]
    if step is not None:
        argv += ["--step", str(step)]
    completed = cli(*argv)
    assert completed.returncode == 0, completed.stderr[-2000:]
    return json.loads(output.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def panel_info(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, pd.DatetimeIndex, Path]:
    tmp_path = tmp_path_factory.mktemp("oos")
    panel, dates = build_panel(tmp_path)
    return panel, dates, tmp_path


@pytest.fixture(scope="module")
def oos_report(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
) -> dict:
    panel, _, tmp_path = panel_info
    return run_oos(panel, tmp_path)


@pytest.fixture(scope="module")
def embargo_report(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
) -> dict:
    panel, _, tmp_path = panel_info
    return run_oos(panel, tmp_path, name="oos_embargo.json", embargo=10)


def _oos(report: dict) -> dict:
    return report["reports"]["oos"]


def test_icir_weights_ignore_ics_after_the_cutoff() -> None:
    """A factor whose IC sign flips must keep the sign seen before the cutoff."""
    index = pd.RangeIndex(30)
    flipping = pd.Series(
        [0.50, 0.55, 0.45, 0.52, 0.48, 0.50, 0.54, 0.46, 0.50, 0.53] + [-0.5] * 20,
        index=index,
    )
    before = core._icir_weights(flipping.loc[:9], ("flip",), {"flip": flipping}, "icir")
    whole = core._icir_weights(flipping, ("flip",), {"flip": flipping}, "icir")
    assert before["flip"] > 0
    assert whole["flip"] < 0


def test_fold_weights_use_no_ic_at_or_after_the_purge_cutoff(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
    oos_report: dict,
) -> None:
    panel, dates, _ = panel_info
    config = _oos(oos_report)["config"]
    frame = core.load_panel(str(panel), (LABEL,))
    from app.lib.factor_lab import factors

    ic_by_date = {
        name: core.daily_ic(frame, factors.compute(frame, name), LABEL)
        for name in config["components"]
    }
    # The label is built open(t+1) -> open(t+1+HORIZON), so HORIZON and
    # EXECUTION_LAG are facts about the data here, not copied from the code.
    assert core.label_horizon(LABEL) == HORIZON
    for fold in _oos(oos_report)["folds"]:
        test_start = int(dates.get_loc(pd.Timestamp(fold["test_start"])))
        embargo = config["embargo"]
        expected_cutoff = test_start - HORIZON - EXECUTION_LAG - embargo
        assert pd.Timestamp(fold["ic_cutoff"]) == dates[expected_cutoff]
        # The label behind the last used IC date is fully observable before the
        # test window opens: entry open(d+lag) .. exit open(d+lag+horizon).
        assert expected_cutoff + EXECUTION_LAG + HORIZON <= test_start
        if embargo == 0:
            # The boundary is exact, and one session later the label would reach
            # into the test window: a changed execution-lag convention fails here.
            assert expected_cutoff + EXECUTION_LAG + HORIZON == test_start
            assert (expected_cutoff + 1) + EXECUTION_LAG + HORIZON > test_start
        # Recomputing the weights from ICs at or before the emitted cutoff must
        # reproduce the emitted weights exactly: nothing after the cutoff (and
        # nothing from the test window) can have entered them.
        reference = ic_by_date[config["components"][0]]
        history = reference.loc[: pd.Timestamp(fold["ic_cutoff"])].tail(
            config["lookback"]
        )
        assert fold["ic_dates_used"] == len(history)
        assert fold["ic_first_date"] == str(pd.Timestamp(history.index[0]).date())
        expected = core._icir_weights(
            history, tuple(config["components"]), ic_by_date, "icir"
        )
        emitted = {name: round(value, 6) for name, value in expected.items()}
        assert fold["factor_weights"] == emitted


def test_embargo_moves_the_cutoff_and_widens_the_purge(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
    oos_report: dict,
    embargo_report: dict,
) -> None:
    _, dates, _ = panel_info
    base = _oos(oos_report)["folds"]
    embargoed = _oos(embargo_report)["folds"]
    assert len(base) == len(embargoed)
    for plain, gapped in zip(base, embargoed):
        plain_cutoff = int(dates.get_loc(pd.Timestamp(plain["ic_cutoff"])))
        gapped_cutoff = int(dates.get_loc(pd.Timestamp(gapped["ic_cutoff"])))
        assert plain_cutoff - gapped_cutoff == 10
        assert gapped["purged_ic_dates"] == plain["purged_ic_dates"] + 10


def test_negative_embargo_is_refused(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
) -> None:
    panel, _, _ = panel_info
    completed = cli(
        "--panel",
        str(panel),
        "--mode",
        "oos",
        "--label",
        LABEL,
        "--embargo",
        "-1",
        "--lookback",
        str(LOOKBACK),
        "--min-ic-dates",
        str(MIN_IC_DATES),
    )
    assert completed.returncode != 0
    assert "--embargo" in completed.stderr
    assert "positive" in completed.stderr.lower()


def test_mixed_modes_require_an_explicit_step(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
) -> None:
    panel, _, _ = panel_info
    completed = cli(
        "--panel",
        str(panel),
        "--mode",
        "oos",
        "--mode",
        "composite",
        "--label",
        LABEL,
        "--lookback",
        str(LOOKBACK),
        "--min-ic-dates",
        str(MIN_IC_DATES),
    )
    assert completed.returncode != 0
    assert "explicit --step" in completed.stderr


def test_fold_windows_tile_without_overlap(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
    oos_report: dict,
) -> None:
    _, dates, _ = panel_info
    folds = _oos(oos_report)["folds"]
    expected = core.plan_oos_folds(len(dates), TRAIN, TEST)
    assert len(folds) == len(expected)
    for row, (train_start, train_end, test_start, test_end) in zip(folds, expected):
        assert row["train_start"] == str(dates[train_start].date())
        assert row["train_end"] == str(dates[train_end].date())
        assert row["test_start"] == str(dates[test_start].date())
        assert row["test_end"] == str(dates[test_end].date())
    for previous, current in zip(folds, folds[1:]):
        previous_end = int(dates.get_loc(pd.Timestamp(previous["test_end"])))
        current_start = int(dates.get_loc(pd.Timestamp(current["test_start"])))
        assert current_start == previous_end + 1
    assert folds[0]["test_start"] == str(dates[TRAIN].date())
    last_end = int(dates.get_loc(pd.Timestamp(folds[-1]["test_end"])))
    assert last_end == TRAIN + len(folds) * TEST - 1
    assert last_end < len(dates)


def test_short_panel_is_refused(
    tmp_path: Path,
) -> None:
    panel, _ = build_panel(tmp_path, sessions=TRAIN + TEST - 1)
    completed = cli(
        "--panel",
        str(panel),
        "--mode",
        "oos",
        "--label",
        LABEL,
        "--lookback",
        str(LOOKBACK),
        "--min-ic-dates",
        str(MIN_IC_DATES),
    )
    assert completed.returncode != 0
    assert "fewer than one full rolling out-of-sample fold" in completed.stderr


def test_config_records_every_knob(oos_report: dict) -> None:
    oos = _oos(oos_report)
    config = oos["config"]
    assert config == oos_report["config"]
    assert config["mode"] == "oos"
    assert config["step"] == STEP
    assert config["names"] == NAMES
    assert config["aum"] == float(AUM)
    assert config["components"] == list(core.DEFAULT_COMPONENTS)
    assert config["lookback"] == LOOKBACK
    assert config["min_ic_dates"] == MIN_IC_DATES
    assert config["label"] == LABEL
    assert config["label_lag"] == HORIZON + EXECUTION_LAG
    assert config["purge_sessions"] == HORIZON + EXECUTION_LAG
    assert config["train_sessions"] == TRAIN
    assert config["test_sessions"] == TEST
    assert config["embargo"] == 0
    assert config["costs"]["round_trip_cost"] > 0
    folds = oos["folds"]
    required = {
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "ic_first_date",
        "ic_cutoff",
        "ic_dates_used",
        "purged_ic_dates",
        "test_total_return",
    }
    assert required <= set(folds[0])
    # The fold table is honest about the effective IC history, which is capped
    # by --lookback rather than by the 500-session folder.
    for fold in folds:
        assert pd.Timestamp(fold["ic_first_date"]) <= pd.Timestamp(fold["ic_cutoff"])
        assert fold["ic_dates_used"] <= LOOKBACK
    assert "lookback caps" in oos["history_note"]
    summary = oos["summary"]
    assert summary["folds"] == len(folds)
    for key in ("total_return", "annualised", "max_drawdown", "turnover_annual"):
        assert key in summary


def test_oos_defaults_to_the_quarterly_step(
    panel_info: tuple[Path, pd.DatetimeIndex, Path],
) -> None:
    panel, _, tmp_path = panel_info
    report = run_oos(panel, tmp_path, name="oos_default_step.json", step=None)
    assert _oos(report)["config"]["step"] == 60
