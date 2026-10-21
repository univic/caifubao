# -*- coding: utf-8 -*-
"""P0 continuous 50k ledger backtest on the frozen factor panel.

This is the accounting verification step for the 2026-10-21 audit. It keeps the
original five-factor scores and fold weights, applies the decision-time-known
delisting-arrangement filter, and runs one continuous account through the new
causal ``replay_ledger``. Corporate actions and delisting exits are explicit
inputs; when they are unavailable the run says so instead of silently assuming a
zero write-off.

Research-only: no Mongo, no broker, no scheduler, no production write.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.lib.factor_lab import factors  # noqa: E402
from app.lib.factor_lab.ledger import LedgerConfig, replay_ledger  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from factor_lab_composite_backtest import (  # noqa: E402
    DEFAULT_COMPONENTS,
    _icir_weights,
    attach_execution_columns,
    daily_ic,
    load_panel,
    plan_oos_folds,
    zscore_by_date,
)

STEP = 20
TRAIN_SESSIONS = 500
TEST_SESSIONS = 250
LABEL_LAG = 21  # fwd_h20 + one session, matching the audit's fold cutoff


def decision_time_exclusions(
    universe: pd.DataFrame | None,
) -> dict[str, pd.Timestamp]:
    """Earliest date a code carries a known delisting-arrangement name."""
    if universe is None or universe.empty:
        return {}
    out: dict[str, pd.Timestamp] = {}
    for item in universe.itertuples():
        try:
            intervals = json.loads(item.namechange_intervals or "[]")
        except (TypeError, ValueError):
            continue
        for interval in intervals:
            if "退" not in str(interval.get("name") or ""):
                continue
            start = interval.get("start_date")
            ann = interval.get("ann_date")
            if not start or not ann:
                continue
            effective = max(
                pd.Timestamp(start),
                pd.Timestamp(ann) + pd.Timedelta(days=1),
            )
            out[item.code] = min(effective, out.get(item.code, effective))
    return out


def _session_index(sessions: list[pd.Timestamp], day: pd.Timestamp) -> int | None:
    return int(np.searchsorted(np.array(sessions), day, side="left"))


def map_actions_to_sessions(
    actions: pd.DataFrame | None, sessions: list[pd.Timestamp]
) -> pd.DataFrame:
    if actions is None or actions.empty:
        return pd.DataFrame(
            columns=["code", "ex_date", "share_multiplier", "cash_per_share"]
        )
    frame = actions.copy()
    if "cash_div" in frame.columns:
        frame["cash_per_share"] = frame["cash_per_share"].where(
            frame["cash_per_share"] > 0, frame["cash_div"].fillna(0.0)
        )
    frame["ex_date"] = pd.to_datetime(frame["ex_date"]).dt.normalize()
    frame = frame.loc[frame["ex_date"].between(sessions[0], sessions[-1])].copy()
    if frame.empty:
        return frame
    frame["ex_date"] = frame["ex_date"].map(
        lambda value: (
            sessions[_session_index(sessions, value)]
            if _session_index(sessions, value) is not None
            and _session_index(sessions, value) < len(sessions)
            else pd.NaT
        )
    )
    return frame.dropna(subset=["ex_date"]).reset_index(drop=True)


def build_delist_exits(
    panel: pd.DataFrame,
    universe: pd.DataFrame | None,
    sessions: list[pd.Timestamp],
) -> pd.DataFrame:
    if universe is None or universe.empty:
        return pd.DataFrame(columns=["code", "date", "price"])
    rows = []
    for item in universe.itertuples():
        delist_date = (
            pd.Timestamp(item.delist_date).normalize() if item.delist_date else None
        )
        if (
            delist_date is None
            or delist_date < sessions[0]
            or delist_date > sessions[-1]
        ):
            continue
        history = panel.loc[
            (panel["stock_code"] == item.code)
            & (panel["date"] <= delist_date)
            & (pd.to_numeric(panel["close"], errors="coerce") > 0)
        ]
        if history.empty:
            price = 0.0
        else:
            price = float(history.sort_values("date").iloc[-1]["close"])
        index = _session_index(sessions, delist_date)
        if index is None or index >= len(sessions):
            continue
        rows.append({"code": item.code, "date": sessions[index], "price": price})
    return pd.DataFrame(rows, columns=["code", "date", "price"])


def build_targets(
    frame: pd.DataFrame,
    decision_dates: list[pd.Timestamp],
) -> dict[pd.Timestamp, list[str]]:
    targets = {}
    previous: list[str] = []
    for day in decision_dates:
        rows = frame.loc[frame["date"] == day]
        price = pd.to_numeric(rows["close"], errors="coerce")
        eligible = (
            (~rows["is_bse"].fillna(True))
            & (~rows["is_st"].fillna(True))
            & np.isfinite(pd.to_numeric(rows["__score"], errors="coerce"))
            & np.isfinite(price)
            & (price > 0)
        )
        ranked = list(
            rows.loc[eligible]
            .sort_values(["__score", "stock_code"], ascending=[False, True])
            .loc[:, "stock_code"]
        )
        keep = [code for code in ranked[:20] if code in set(previous)][:10]
        target = (
            keep + [code for code in ranked if code not in set(keep)][: 10 - len(keep)]
        )
        targets[day] = target
        previous = list(target)
    return targets


def prepare_scores(
    frame: pd.DataFrame, holdout_start: pd.Timestamp | None = None
) -> pd.DataFrame:
    frame = frame.copy()
    dates = list(pd.to_datetime(sorted(frame["date"].unique())))
    zscores = {}
    ics = {}
    for name in DEFAULT_COMPONENTS:
        values = factors.compute(frame, name)
        zscores[name] = zscore_by_date(values, frame["date"])
        ics[name] = daily_ic(frame, values, "fwd_h20")
    folds = plan_oos_folds(len(dates), TRAIN_SESSIONS, TEST_SESSIONS)
    reference = ics[DEFAULT_COMPONENTS[0]]
    for _, _, test_start, test_end in folds:
        cutoff_index = test_start - LABEL_LAG
        cutoff = dates[cutoff_index]
        history = reference.loc[:cutoff].tail(244)
        weights = _icir_weights(history, DEFAULT_COMPONENTS, ics, "icir")
        mask = frame["date"].between(dates[test_start], dates[test_end])
        frame.loc[mask, "__score"] = sum(
            weight * zscores[name].loc[mask] for name, weight in weights.items()
        )
    if holdout_start is not None:
        last_weights = weights
        mask = frame["date"] >= holdout_start
        frame.loc[mask, "__score"] = sum(
            weight * zscores[name].loc[mask] for name, weight in last_weights.items()
        )
    return frame


def run(
    panel_path: str,
    *,
    dividend_path: str | None,
    universe_path: str | None,
    output_path: str,
    exposure: float = 1.0,
    holdout_start: str | None = None,
) -> dict:
    frame = load_panel(panel_path, ("fwd_h20",))
    frame = attach_execution_columns(frame, panel_path)
    holdout_day = pd.Timestamp(holdout_start).normalize() if holdout_start else None
    frame = prepare_scores(frame, holdout_day)
    dates = list(pd.to_datetime(sorted(frame["date"].unique())))
    folds = plan_oos_folds(len(dates), TRAIN_SESSIONS, TEST_SESSIONS)
    if holdout_day is not None:
        sessions = [day for day in dates if day >= holdout_day]
    else:
        sessions = [
            day
            for start, end in ((start, end) for _, _, start, end in folds)
            for day in dates[start : end + 1]
        ]
        # The four folds are contiguous; the dedupe is defensive only.
        sessions = list(dict.fromkeys(sessions))

    universe = (
        pd.read_parquet(universe_path)
        if universe_path and Path(universe_path).exists()
        else None
    )
    exclusions = decision_time_exclusions(universe)
    if exclusions:
        cutoff = frame["stock_code"].map(exclusions)
        frame.loc[cutoff.notna() & (frame["date"] >= cutoff), "__score"] = np.nan

    decision_dates = []
    if holdout_day is not None:
        decision_dates.extend(sessions[i] for i in range(0, len(sessions) - 1, STEP))
    else:
        for _, _, test_start, test_end in folds:
            window = dates[test_start : test_end + 1]
            decision_dates.extend(window[i] for i in range(0, len(window) - 1, STEP))
    targets = build_targets(frame, decision_dates)

    actions = (
        pd.read_parquet(dividend_path)
        if dividend_path and Path(dividend_path).exists()
        else None
    )
    rights_events = (
        int((pd.to_numeric(actions.get("stk_co_rate"), errors="coerce") > 0).sum())
        if actions is not None and not actions.empty
        else 0
    )
    actions = map_actions_to_sessions(actions, sessions)
    exits = build_delist_exits(frame, universe, sessions)

    if not 0 < exposure <= 1.0:
        raise ValueError("exposure must be in (0, 1]")
    exposure_map = {day: exposure for day in targets}
    result = replay_ledger(
        frame,
        sessions,
        targets,
        LedgerConfig(initial_cash=50_000, max_positions=10),
        corporate_actions=actions,
        delist_exits=exits,
        exposure=exposure_map,
        rebalance_to_target=exposure < 1.0,
    )
    result["inputs"] = {
        "panel": panel_path,
        "dividend": dividend_path,
        "universe": universe_path,
        "excluded_codes": len(exclusions),
        "corporate_action_rows": int(len(actions)),
        "rights_events_ignored": rights_events,
        "delist_exit_rows": int(len(exits)),
        "decision_dates": len(decision_dates),
        "exposure": exposure,
        "holdout_start": str(holdout_day.date()) if holdout_day is not None else None,
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(
        json.dumps(
            {
                "summary": result["summary"],
                "inputs": result["inputs"],
                "config": result["config"],
                "nav": result["nav"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", required=True)
    parser.add_argument("--dividend", default=None)
    parser.add_argument("--universe", default=None)
    parser.add_argument("--exposure", type=float, default=1.0)
    parser.add_argument("--holdout-start", default=None)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    result = run(
        args.panel,
        dividend_path=args.dividend,
        universe_path=args.universe,
        output_path=args.output,
        exposure=args.exposure,
        holdout_start=args.holdout_start,
    )
    print(json.dumps(result["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
