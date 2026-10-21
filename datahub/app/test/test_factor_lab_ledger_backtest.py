# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd

SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "factor_lab_ledger_backtest.py"
)
spec = importlib.util.spec_from_file_location("factor_lab_ledger_backtest", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


def test_decision_time_exclusions_uses_announcement_plus_one():
    universe = pd.DataFrame(
        [
            {
                "code": "sh600000",
                "namechange_intervals": json.dumps(
                    [
                        {
                            "name": "退市测试",
                            "start_date": "2023-01-01",
                            "ann_date": "2022-12-30",
                            "end_date": None,
                        }
                    ]
                ),
            }
        ]
    )
    cutoff = module.decision_time_exclusions(universe)
    assert cutoff["sh600000"] == pd.Timestamp("2023-01-01")


def test_targets_are_ten_and_respect_the_buffer():
    day = pd.Timestamp("2024-01-02")
    rows = []
    for index in range(25):
        rows.append(
            {
                "date": day,
                "stock_code": f"sh6{index:05d}",
                "close": 10.0,
                "is_bse": False,
                "is_st": False,
                "__score": 100.0 - index,
            }
        )
    frame = pd.DataFrame(rows)
    targets = module.build_targets(frame, [day])
    assert len(targets[day]) == 10
    assert targets[day][0] == "sh600000"


def test_actions_on_a_weekend_map_to_the_next_session():
    sessions = list(pd.to_datetime(["2024-01-05", "2024-01-08"]))
    actions = pd.DataFrame(
        [
            {
                "code": "sh600000",
                "ex_date": pd.Timestamp("2024-01-06"),
                "share_multiplier": 1.0,
                "cash_per_share": 1.0,
            }
        ]
    )
    mapped = module.map_actions_to_sessions(actions, sessions)
    assert len(mapped) == 1
    assert mapped.iloc[0]["ex_date"] == pd.Timestamp("2024-01-08")
