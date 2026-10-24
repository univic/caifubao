"""Pure artifact publisher tests; no research database or broker needed."""

import copy
import datetime as dt
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from caifubao_contracts.strategy_artifact import verify_strategy_artifact
from caifubao_contracts.strategy_artifact_publish import (
    publish_versioned_research_artifact,
)
from caifubao_contracts.strategy_artifact_store import ImmutableArtifactStore


SHA = "a" * 40
AS_OF = "2026-10-23T15:00:00+08:00"
INPUTS = {
    "spec": {"hash": "b" * 64},
    "quotes": {"hash": "c" * 64, "as_of": AS_OF},
}
PAYLOAD = {
    "nav": 10000.25,
    "weights": [0.25, 0.75],
    "fills": [{"shares": 201, "commission": 5.02}],
    "evidence_kind": "REPLAY",
}
UNITS = {
    "nav": "cny_cent",
    "weights[0]": "fraction_ppm",
    "weights[1]": "fraction_ppm",
    "fills[0].shares": "shares",
    "fills[0].commission": "cny_cent",
}


def publish(kind="replay", **overrides):
    values = {
        "artifact_type": kind,
        "legacy_payload": PAYLOAD,
        "numeric_units": UNITS,
        "inputs": INPUTS,
        "as_of": AS_OF,
        "generated_at": "2026-10-24T00:00:00Z",
    }
    values.update(overrides)
    with patch.dict("os.environ", {"CAIFUBAO_BUILD_SHA": SHA}):
        return publish_versioned_research_artifact(**values)


class StrategyArtifactPublisherTests(unittest.TestCase):
    def test_three_producer_types_are_distinct_and_legacy_is_not_mutated(self):
        legacy = copy.deepcopy(PAYLOAD)
        artifacts = [publish(kind) for kind in ("paper", "replay", "backtest")]
        self.assertEqual(PAYLOAD, legacy)
        self.assertEqual(len({x["artifact_hash"] for x in artifacts}), 3)
        self.assertTrue(all(verify_strategy_artifact(x) for x in artifacts))
        replay = artifacts[1]
        self.assertEqual(replay["payload"]["nav"], 1000025)
        self.assertEqual(replay["payload"]["weights"], [250000, 750000])
        self.assertEqual(replay["payload"]["fills"][0]["shares"], 201)
        self.assertEqual(replay["payload"]["fills"][0]["commission"], 502)
        self.assertEqual(replay["grade"], "RESEARCH")
        self.assertEqual(replay["as_of"], "2026-10-23T07:00:00Z")

    def test_repeat_publication_keeps_existing_generation_time(self):
        with tempfile.TemporaryDirectory() as root:
            store = ImmutableArtifactStore(Path(root))
            original = publish()
            first = store.publish(original)
            later = publish(generated_at="2026-10-30T00:00:00Z")
            again = store.publish(later)
            self.assertEqual(first["artifact_id"], again["artifact_id"])
            self.assertEqual(first["generated_at"], again["generated_at"])
            self.assertEqual(len(list(Path(root).glob("*.json"))), 1)
            stored = store.read(first["artifact_id"])
            self.assertEqual(stored, first)

    def test_fail_closed_on_unknown_units_precision_and_unpinned_input(self):
        with self.assertRaisesRegex(ValueError, "missing numeric unit"):
            publish(numeric_units={"nav": "cny_cent"})
        with self.assertRaisesRegex(ValueError, "unsupported declared numeric unit"):
            publish(numeric_units={**UNITS, "nav": "banana"})
        with self.assertRaisesRegex(ValueError, "cannot be represented"):
            publish(legacy_payload={**PAYLOAD, "nav": 10000.251})
        with self.assertRaisesRegex(ValueError, "unused or incorrect"):
            publish(numeric_units={**UNITS, "extra": "shares"})
        with self.assertRaisesRegex(ValueError, "unresolvable input"):
            publish(inputs={"spec": {}})
        with self.assertRaisesRegex(ValueError, "modified"):
            tampered = publish()
            tampered["payload"]["nav"] += 1
            verify_strategy_artifact(tampered)
        with self.assertRaisesRegex(ValueError, "research-grade"):
            publish(grade="TRADING_APPROVED")


if __name__ == "__main__":
    unittest.main()
