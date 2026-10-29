# -*- coding: utf-8 -*-
"""P0 invariant: every declared strategy field has a consumer, or an owner.

The manifest is the answer to "what does this field actually do?". A field that
is validated and hashed but read by nobody is a defect — the declaration claims a
behaviour it never produces. ``rebalance.cadence_days`` was the first such field;
``sizing.weighting`` was found by this gate on its first run.
"""

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_spec import _ALLOWED  # noqa: E402
from caifubao_contracts.strategy_spec_consumers import (  # noqa: E402
    CONSUMED,
    UNCONSUMED,
    check,
    leaf_paths,
    load_manifest,
)

MANIFEST_PATH = (
    REPO_ROOT / "shared" / "caifubao_contracts" / "strategy_spec_consumers.json"
)

#: Fields that are validated and hashed but read by no consumer. Pinned so that a
#: silent status flip (either direction) fails CI and has to be argued for.
KNOWN_UNCONSUMED: set[str] = set()


@pytest.fixture(scope="module")
def manifest():
    return load_manifest(MANIFEST_PATH)


def test_manifest_is_honest(manifest):
    """Every allowed field is declared and every consumer really mentions it."""
    assert check(manifest, _ALLOWED, REPO_ROOT) == []


def test_manifest_covers_exactly_the_allowed_fields(manifest):
    assert set(manifest["fields"]) == set(leaf_paths(_ALLOWED))


def test_known_unconsumed_set_is_pinned(manifest):
    declared = {
        path
        for path, entry in manifest["fields"].items()
        if entry["status"] == UNCONSUMED
    }
    assert declared == KNOWN_UNCONSUMED


def test_unconsumed_fields_name_an_owner(manifest):
    for path in KNOWN_UNCONSUMED:
        entry = manifest["fields"][path]
        assert entry.get("tracked_by"), path
        assert entry.get("note"), path


def test_cadence_days_is_consumed_by_v2_only(manifest):
    """v2 reads the cadence; v1 must never start doing so."""
    entry = manifest["fields"]["rebalance.cadence_days"]
    assert entry["status"] == CONSUMED
    assert entry["consumed_by_versions"] == ["2"]


def test_missing_field_is_reported(manifest):
    broken = json.loads(json.dumps(manifest))
    del broken["fields"]["rebalance.cadence_days"]
    problems = check(broken, _ALLOWED, REPO_ROOT)
    assert any(
        "rebalance.cadence_days" in p and "no manifest entry" in p for p in problems
    )


def test_unlisted_field_is_reported(manifest):
    broken = json.loads(json.dumps(manifest))
    broken["fields"]["signal.not_a_field"] = {"status": CONSUMED, "consumers": []}
    problems = check(broken, _ALLOWED, REPO_ROOT)
    assert any(
        "signal.not_a_field" in p and "not an allowed field" in p for p in problems
    )


def test_missing_consumer_file_is_reported(manifest):
    broken = json.loads(json.dumps(manifest))
    broken["fields"]["signal.horizon"]["consumers"] = ["datahub/app/does_not_exist.py"]
    problems = check(broken, _ALLOWED, REPO_ROOT)
    assert any("does not exist" in p for p in problems)


def test_consumer_that_never_mentions_the_field_is_reported(manifest):
    broken = json.loads(json.dumps(manifest))
    # This module exists but never contains the string "horizon" in a way that
    # refers to the field; use a file that certainly does not mention it.
    broken["fields"]["signal.horizon"]["consumers"] = [
        "shared/caifubao_contracts/strategy_modes.py"
    ]
    problems = check(broken, _ALLOWED, REPO_ROOT)
    assert any("never mentions" in p for p in problems)


def test_unconsumed_without_owner_is_reported(manifest):
    broken = json.loads(json.dumps(manifest))
    broken["fields"]["rebalance.cadence_days"] = {"status": UNCONSUMED}
    problems = check(broken, _ALLOWED, REPO_ROOT)
    assert any("tracked_by" in p for p in problems)


def test_unknown_status_is_reported(manifest):
    broken = json.loads(json.dumps(manifest))
    broken["fields"]["signal.horizon"]["status"] = "maybe"
    problems = check(broken, _ALLOWED, REPO_ROOT)
    assert any("status must be one of" in p for p in problems)


def test_manifest_schema_is_enforced(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text('{"schema": "wrong", "fields": {}}', encoding="utf-8")
    with pytest.raises(ValueError, match="schema"):
        load_manifest(bad)


def test_strategy_spec_module_matches_manifest_location(manifest):
    assert manifest["spec_module"] == "shared/caifubao_contracts/strategy_spec.py"
    assert (REPO_ROOT / manifest["spec_module"]).is_file()
