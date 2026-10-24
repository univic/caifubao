# -*- coding: utf-8 -*-
"""Append-only, exact-ID strategy artifact store for research evidence.

No Mongo, trading account, broker or promotion logic is imported. The store
never updates a published artifact's hash, manifest, payload or first timestamp.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile

from caifubao_contracts.strategy_artifact import verify_strategy_artifact


class ImmutableArtifactStore:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        if not str(directory).strip():
            raise ValueError("artifact store directory is required")

    def _path(self, artifact_id: str) -> Path:
        if (
            not isinstance(artifact_id, str)
            or len(artifact_id) != 71
            or not artifact_id.startswith("sha256:")
            or any(char not in "0123456789abcdef" for char in artifact_id[7:])
        ):
            raise ValueError("invalid artifact_id")
        return self.directory / (artifact_id[7:] + ".json")

    def read(self, artifact_id: str) -> dict:
        with self._path(artifact_id).open(encoding="utf-8") as stream:
            artifact = json.load(stream)
        verify_strategy_artifact(artifact)
        if artifact["artifact_id"] != artifact_id:
            raise ValueError("artifact store identity mismatch")
        return artifact

    def publish(self, artifact: dict) -> dict:
        verify_strategy_artifact(artifact)
        target = self._path(artifact["artifact_id"])
        self.directory.mkdir(parents=True, exist_ok=True)
        # Publish a complete, durable inode atomically without replacing an ID.
        descriptor, temporary = tempfile.mkstemp(
            prefix=".artifact-", dir=self.directory
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(
                    artifact,
                    stream,
                    sort_keys=True,
                    ensure_ascii=False,
                    allow_nan=False,
                )
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                # Concurrent or repeated publication retains the first timestamp.
                # Also sync the winner: its directory fsync may have failed.
                pass
            directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            os.unlink(temporary)
        return self.read(artifact["artifact_id"])
