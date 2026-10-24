"""Pure artifact publisher tests; no research database or broker needed."""

import copy
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


class PublicationRegressionTests(unittest.TestCase):
    def test_large_integer_is_lossless_and_fraction_is_rejected(self):
        from decimal import Decimal

        large = 12345678901234567890123456789
        result = publish(legacy_payload={"n": large}, numeric_units={"n": "count"})
        self.assertEqual(result["payload"]["n"], large)
        with self.assertRaisesRegex(ValueError, "cannot be represented"):
            publish(
                legacy_payload={"n": Decimal("1.00000000000000000000000000001")},
                numeric_units={"n": "count"},
            )

    def test_failed_write_can_be_retried(self):
        with tempfile.TemporaryDirectory() as root:
            store = ImmutableArtifactStore(root)
            artifact = publish()
            with patch(
                "caifubao_contracts.strategy_artifact_store.json.dump",
                side_effect=OSError("disk full"),
            ):
                with self.assertRaises(OSError):
                    store.publish(artifact)
            self.assertEqual(list(Path(root).iterdir()), [])
            self.assertEqual(store.publish(artifact), artifact)

    def test_concurrent_publishers_observe_complete_first_record(self):
        from concurrent.futures import ThreadPoolExecutor

        with tempfile.TemporaryDirectory() as root:
            artifacts = [
                publish(generated_at=f"2026-10-{day:02d}T00:00:00Z")
                for day in range(24, 32)
            ]
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(
                    pool.map(ImmutableArtifactStore(root).publish, artifacts)
                )
            self.assertTrue(all(result == results[0] for result in results))
            self.assertEqual(len(list(Path(root).iterdir())), 1)

    def test_cli_preserves_json_decimal_precision(self):
        import contextlib
        import io
        import json
        import runpy

        job = (
            Path(__file__).resolve().parents[2]
            / "datahub/app/jobs/strategy_artifact_publish.py"
        )
        main = runpy.run_path(str(job))["main"]
        with tempfile.TemporaryDirectory() as root:
            snapshot = Path(root) / "snapshot.json"
            values = {
                "artifact_type": "replay",
                "legacy_payload": {"nav": "EXACT_DECIMAL"},
                "numeric_units": {"nav": "cny_cent"},
                "inputs": INPUTS,
                "as_of": AS_OF,
                "generated_at": "2026-10-24T00:00:00Z",
            }
            snapshot.write_text(
                json.dumps(values).replace('"EXACT_DECIMAL"', "9007199254740992.01"),
                encoding="utf-8",
            )
            output = io.StringIO()
            with patch.dict("os.environ", {"CAIFUBAO_BUILD_SHA": SHA}):
                with contextlib.redirect_stdout(output):
                    main(
                        [
                            "--snapshot",
                            str(snapshot),
                            "--output-dir",
                            root + "/artifacts",
                        ]
                    )
            identity = json.loads(output.getvalue())["artifact_id"]
            self.assertEqual(
                ImmutableArtifactStore(root + "/artifacts").read(identity)["payload"][
                    "nav"
                ],
                900719925474099201,
            )

    def test_incomplete_write_is_not_visible_at_final_path(self):
        from concurrent.futures import ThreadPoolExecutor
        import json
        from threading import Event

        started, release = Event(), Event()
        real_dump = json.dump

        def delayed_dump(*args, **kwargs):
            started.set()
            if not release.wait(5):
                raise TimeoutError("test did not release writer")
            return real_dump(*args, **kwargs)

        with tempfile.TemporaryDirectory() as root:
            store = ImmutableArtifactStore(root)
            artifact = publish()
            with patch(
                "caifubao_contracts.strategy_artifact_store.json.dump", delayed_dump
            ):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(store.publish, artifact)
                    try:
                        self.assertTrue(started.wait(5))
                        self.assertEqual(list(Path(root).glob("*.json")), [])
                    finally:
                        release.set()
                    self.assertEqual(future.result(), artifact)

    def test_retry_after_directory_fsync_failure_syncs_existing_record(self):
        import os
        import stat

        real_fsync = os.fsync
        directory_syncs = []

        def sync_once_fails(descriptor):
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                directory_syncs.append(descriptor)
                if len(directory_syncs) == 1:
                    raise OSError("directory fsync failed")
            return real_fsync(descriptor)

        with tempfile.TemporaryDirectory() as root:
            store = ImmutableArtifactStore(root)
            artifact = publish()
            with patch(
                "caifubao_contracts.strategy_artifact_store.os.fsync", sync_once_fails
            ):
                with self.assertRaisesRegex(OSError, "directory fsync failed"):
                    store.publish(artifact)
                self.assertEqual(store.publish(artifact), artifact)
            self.assertEqual(len(directory_syncs), 2)


if __name__ == "__main__":
    unittest.main()
