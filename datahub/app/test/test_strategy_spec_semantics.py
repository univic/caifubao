# -*- coding: utf-8 -*-
"""Dispatch is by spec_version, and v1's consumed set is frozen.

If a new consumer is wired for a field v1 already declares, the certified window
changes behaviour while its hash stays identical. These tests fail in that case,
and they also fail if v2 is allowed to ship with an unconsumed field.
"""

import copy
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_spec_consumers import (  # noqa: E402
    CONSUMED,
    load_manifest,
)
from caifubao_contracts.strategy_spec_semantics import (  # noqa: E402
    V1_CONSUMED,
    V1_DECLARED_BUT_UNUSED,
    consumed_by_version,
    consumed_fields,
    declared_but_unused,
    field_is_consumed,
)

MANIFEST_PATH = (
    REPO_ROOT / "shared" / "caifubao_contracts" / "strategy_spec_consumers.json"
)


@pytest.fixture(scope="module")
def manifest():
    return load_manifest(MANIFEST_PATH)


def test_v1_consumed_set_matches_the_manifest(manifest):
    """A newly wired v1 consumer changes this answer and must be argued for."""
    assert V1_CONSUMED == consumed_by_version(manifest, "1")


def test_v2_only_consumer_does_not_leak_into_v1(manifest):
    """A field consumed only by v2 must stay out of the v1 set."""
    extended = copy.deepcopy(manifest)
    extended["fields"]["rebalance.cadence_days"] = {
        "status": CONSUMED,
        "consumers": ["datahub/app/lib/strategy_engine/selection.py"],
        "consumed_by_versions": ["2"],
    }
    assert "rebalance.cadence_days" in consumed_by_version(extended, "2")
    assert "rebalance.cadence_days" not in consumed_by_version(extended, "1")
    assert not field_is_consumed("1", "rebalance.cadence_days", extended)


def test_v1_unused_set_matches_the_manifest(manifest):
    assert V1_DECLARED_BUT_UNUSED == declared_but_unused(manifest)


def test_sets_partition_the_manifest(manifest):
    """v1-consumed, v2-only, and unconsumed must be disjoint and cover everything."""
    v2_only = consumed_by_version(manifest, "2") - V1_CONSUMED
    assert not (V1_CONSUMED & V1_DECLARED_BUT_UNUSED)
    assert not (V1_CONSUMED & v2_only)
    assert not (v2_only & V1_DECLARED_BUT_UNUSED)
    assert V1_CONSUMED | v2_only | V1_DECLARED_BUT_UNUSED == frozenset(
        manifest["fields"]
    )


def test_v1_does_not_consume_the_cadence(manifest):
    assert not field_is_consumed("1", "rebalance.cadence_days", manifest)


def test_v1_does_not_consume_the_risk_block(manifest):
    for field in sorted(f for f in manifest["fields"] if f.startswith("risk.")):
        assert not field_is_consumed("1", field, manifest), field


def test_v1_does_not_consume_the_weighting(manifest):
    assert not field_is_consumed("1", "sizing.weighting", manifest)


def test_v1_consumed_set_is_independent_of_the_manifest(manifest):
    """v1 must not silently follow a manifest change."""
    shrunk = copy.deepcopy(manifest)
    shrunk["fields"]["rebalance.cadence_days"] = {
        "status": CONSUMED,
        "consumers": ["datahub/app/lib/strategy_engine/selection.py"],
    }
    # The manifest now claims cadence_days is consumed, but v1 still refuses it:
    # the version, not the field, decides.
    assert not field_is_consumed("1", "rebalance.cadence_days", shrunk)


def test_v2_consumes_the_whole_shipped_manifest(manifest):
    """Every declared field now has a v2 consumer; that is the goal state."""
    assert consumed_fields("2", manifest) == frozenset(manifest["fields"])


def test_v2_refuses_a_field_no_v2_consumer_reads(manifest):
    """The rule still bites: drop the v2 attribution and v2 must refuse."""
    broken = copy.deepcopy(manifest)
    broken["fields"]["risk.max_drawdown_pct"]["consumed_by_versions"] = ["1"]
    with pytest.raises(ValueError, match="no v2 consumer reads"):
        consumed_fields("2", broken)


def test_the_manifest_has_no_unconsumed_fields(manifest):
    """An unconsumed field is a defect; the shipped manifest has none."""
    assert declared_but_unused(manifest) == frozenset()
    assert consumed_by_version(manifest, "1") | consumed_by_version(
        manifest, "2"
    ) == frozenset(manifest["fields"])


def test_unknown_version_is_rejected(manifest):
    with pytest.raises(ValueError, match="unsupported spec_version"):
        consumed_fields("3", manifest)


def test_pin_file_is_not_a_copy_of_the_manifest(manifest):
    """The pinned v1 set must live in code so a manifest edit cannot loosen it."""
    source = (
        REPO_ROOT / "shared" / "caifubao_contracts" / "strategy_spec_semantics.py"
    ).read_text(encoding="utf-8")
    assert "rebalance.cadence_days" in source
    assert "V1_CONSUMED = frozenset(" in source
    # Sanity: the manifest contract still parses as JSON with a schema.
    assert json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))["schema"]
