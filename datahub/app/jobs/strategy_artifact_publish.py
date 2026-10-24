# -*- coding: utf-8 -*-
"""Offline, explicit-input strategy evidence publication (RESEARCH only).

Example:
    CAIFUBAO_BUILD_SHA=<build-git-sha> python -m app.jobs.strategy_artifact_publish \
        --snapshot frozen-producer-result.json --output-dir artifacts/strategy/

The supplied snapshot contains artifact_type, legacy_payload, numeric_units,
inputs, as_of and generated_at. Source code identity MUST be provided by build.
This job never advances FORWARD certification and never calls a broker.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from caifubao_contracts.strategy_artifact_publish import (
    publish_versioned_research_artifact,
)
from caifubao_contracts.strategy_artifact_store import ImmutableArtifactStore


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    with args.snapshot.open(encoding="utf-8") as stream:
        snapshot = json.load(stream)
    if not isinstance(snapshot, dict):
        raise ValueError("producer snapshot must be a dictionary")
    allowed = {
        "artifact_type", "legacy_payload", "numeric_units", "inputs",
        "as_of", "generated_at", "grade",
    }
    unknown = set(snapshot) - allowed
    if unknown:
        raise ValueError(f"unsupported producer snapshot fields: {sorted(unknown)}")
    artifact = publish_versioned_research_artifact(**snapshot)
    saved = ImmutableArtifactStore(args.output_dir).publish(artifact)
    print(json.dumps({
        "artifact_id": saved["artifact_id"],
        "artifact_hash": saved["artifact_hash"],
        "grade": saved["grade"],
        "producer": saved["producer"],
        "as_of": saved["as_of"],
        "generated_at": saved["generated_at"],
    }, ensure_ascii=False, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
