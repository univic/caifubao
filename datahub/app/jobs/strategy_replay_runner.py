# -*- coding: utf-8 -*-
"""Operator-run strategy replay persistence.

Reads a JSON payload produced by the research backtests, validates and builds a
``StrategyReplay`` artifact, and writes it to ``strategy_replays``. This runner is
never scheduled by a CronJob; it is a research publication step. It does not
read or write ``strategy_paper_runs`` or the certified forward windows, and it
never places an order.

Usage:
    python -m app.jobs.strategy_replay_runner --payload replay.json
    python -m app.jobs.strategy_replay_runner --payload replay.json --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

from app.lib.strategy_engine.replay import build_replay_artifact


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--payload", required=True, help="Replay input JSON file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate only; do not write Mongo",
    )
    parser.add_argument("--output", default=None, help="Optional artifact output JSON")
    return parser.parse_args(argv)


def _to_datetime(value):
    if isinstance(value, datetime.datetime):
        return value
    if not value:
        return datetime.datetime.now(datetime.UTC)
    return datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _model_from_artifact(artifact: dict):
    from app.model.strategy_replay import StrategyReplay

    document = StrategyReplay(
        replay_id=artifact["replay_id"],
        schema_version=artifact["schema_version"],
        source_artifact=artifact["source_artifact"],
        name=artifact["name"],
        strategy=artifact["strategy"],
        description=artifact.get("description", ""),
        start_date=artifact["start_date"],
        end_date=artifact["end_date"],
        data_as_of=artifact["data_as_of"],
        aum=artifact["aum"],
        config=artifact["config"],
        config_hash=artifact["config_hash"],
        input_hash=artifact["input_hash"],
        content_hash=artifact["content_hash"],
        evidence_kind="REPLAY",
        research_grade=artifact["research_grade"],
        forward_sessions=0,
        forward_required=artifact["forward_required"],
        forward_eligible=False,
        survivorship_gap=artifact["survivorship_gap"],
        limitations=artifact["limitations"],
        disclaimer=artifact["disclaimer"],
        metrics=artifact["metrics"],
        curve=artifact["curve"],
        decisions=artifact["decisions"],
        fills=artifact["fills"],
        blocked_orders=artifact["blocked_orders"],
        generated_at=_to_datetime(artifact["generated_at"]),
    )
    return document


def persist_artifact(artifact: dict, *, collection=None) -> dict:
    """Persist one artifact immutably by ``replay_id`` and ``input_hash``.

    A same-id/same-input rebuild with the same content is a no-op; a same-id
    rebuild with a different input hash or different content fails closed.
    """
    if collection is None:
        from app.model.strategy_replay import StrategyReplay

        collection = StrategyReplay.objects
    existing = collection(replay_id=artifact["replay_id"]).first()
    if existing is not None:
        if existing.input_hash != artifact["input_hash"]:
            raise ValueError(
                f"replay_id {artifact['replay_id']} already exists with a different "
                "input hash; use a new versioned replay_id"
            )
        if existing.content_hash != artifact["content_hash"]:
            raise ValueError(
                f"replay_id {artifact['replay_id']} already exists with different "
                "content for the same input; refusing to overwrite"
            )
        return {"status": "UNCHANGED", "replay_id": artifact["replay_id"]}
    document = _model_from_artifact(artifact)
    document.save()
    return {"status": "CREATED", "replay_id": artifact["replay_id"]}


def build_from_payload(payload: dict) -> dict:
    return build_replay_artifact(
        replay_id=payload["replay_id"],
        name=payload["name"],
        strategy=payload["strategy"],
        description=payload.get("description", ""),
        start_date=payload["start_date"],
        end_date=payload["end_date"],
        aum=float(payload["aum"]),
        config=payload["config"],
        config_hash=payload["config_hash"],
        source_artifact=payload["source_artifact"],
        curve=payload["curve"],
        decisions=payload["decisions"],
        fills=payload["fills"],
        blocked_orders=payload.get("blocked_orders"),
        limitations=payload.get("limitations"),
        survivorship_gap=bool(payload.get("survivorship_gap", True)),
        metrics=payload.get("metrics"),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = json.loads(Path(args.payload).read_text(encoding="utf-8"))
    artifact = build_from_payload(payload)
    if args.output:
        Path(args.output).write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "DRY_RUN",
                    "replay_id": artifact["replay_id"],
                    "sessions": len(artifact["curve"]),
                    "decisions": len(artifact["decisions"]),
                    "fills": len(artifact["fills"]),
                    "content_hash": artifact["content_hash"],
                },
                ensure_ascii=False,
            )
        )
        return 0
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()
    result = persist_artifact(artifact)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
