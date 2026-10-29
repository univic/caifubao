# -*- coding: utf-8 -*-
"""v1 behaviour is pinned to production data, not to its hash.

Hashing a declaration preserves its identity, not its behaviour: wiring a new
consumer for a field that v1 already declares changes what the certified window
``01c196b8…`` does while ``spec_hash`` stays identical. This test replays five
consecutive production sessions through the production selection path and
asserts the target book and the rebalance diff are unchanged.

The fixture holds real ``flip_wide_shadow_v1`` scores (horizon 20) from the
research database for 2026-08-03..2026-08-07, trimmed to the top 64 candidates
per session. Trimming is safe because the exit band is
``ceil(20 × 2.0) = 40`` ranks; the generator asserted that the trimmed and full
universe produce identical books before writing the fixture.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.lib.strategy_engine.selection import (
    compute_rebalance,
    plan_target_holdings,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "strategy_v1_golden.json"


@pytest.fixture(scope="module")
def golden():
    with FIXTURE.open(encoding="utf-8") as handle:
        return json.load(handle)


def _predictions(candidates):
    return [
        SimpleNamespace(
            stock_code=row["stock_code"],
            score=row["score"],
            percentile=row["percentile"],
        )
        for row in candidates
    ]


def _config(golden):
    return {
        "score_model_version": golden["model_version"],
        "horizon": golden["horizon"],
        "selection": {
            "mode": "top_percentile",
            "lower": 0.20,
            "upper": 1.00,
            "portfolio_size": golden["portfolio_size"],
        },
    }


def test_v1_replays_the_pinned_books(golden):
    """Five consecutive sessions must reproduce the recorded target books."""
    config = _config(golden)
    previous = None
    replayed = []
    for session in golden["sessions"]:
        plan = plan_target_holdings(
            _predictions(session["candidates"]),
            config,
            previous_holdings=previous,
            exit_buffer=golden["exit_buffer"],
            min_hold_sessions=golden["min_hold_sessions"],
        )
        target = plan["holdings"]
        step = {"date": session["date"], "target": [h["stock_code"] for h in target]}
        if previous is not None:
            rebalance = compute_rebalance(previous, target)
            step["added"] = rebalance["added"]
            step["removed"] = rebalance["removed"]
        replayed.append(step)
        previous = target

    assert replayed == golden["expected"]


def test_v1_book_size_is_the_declared_cap(golden):
    for step in golden["expected"]:
        assert len(step["target"]) == golden["portfolio_size"]


def test_v1_rebalance_diff_is_pinned(golden):
    """Turnover is part of the pinned behaviour, not an emergent detail."""
    added = [len(step.get("added", [])) for step in golden["expected"][1:]]
    assert added == [1, 1, 2, 1]


def test_cadence_days_does_not_skip_sessions(golden):
    """A declared five-session cadence must not collapse the replay to one step.

    v1 never consumes ``rebalance.cadence_days``; if a consumer starts reading it
    the five sessions would stop producing five plans and this fails.
    """
    assert len(golden["expected"]) == len(golden["sessions"]) == 5
    assert all(len(step["target"]) == 20 for step in golden["expected"])


def test_fixture_records_its_provenance(golden):
    assert golden["schema"] == "strategy-v1-golden-v1"
    assert golden["model_version"] == "flip_wide_shadow_v1"
    assert golden["horizon"] == 20
    assert golden["exit_buffer"] == 2.0
    assert golden["min_hold_sessions"] == 0
