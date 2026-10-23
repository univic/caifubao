# -*- coding: utf-8 -*-
"""Replay a frozen StrategySpec risk decision without accessing trading state.

Example:
    python -m app.jobs.strategy_risk_replay --spec spec.json \
        --account account.json --intents intents.json --market market.json

This is a research-only evaluator. It neither approves a real order nor
reads an operational account, broker, or halt switch.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from caifubao_contracts.strategy_risk import evaluate_spec_risk


def read_frozen_halt_snapshot(snapshot: dict) -> bool:
    """Research reads an explicit snapshot, never the operational halt store."""
    if not isinstance(snapshot, dict) or type(snapshot.get("halted")) is not bool:
        raise ValueError("frozen halt snapshot requires boolean halted")
    if snapshot["halted"] and (
        not isinstance(snapshot.get("changed_by"), str)
        or not snapshot["changed_by"].strip()
        or not isinstance(snapshot.get("reason"), str)
        or not snapshot["reason"].strip()
    ):
        raise ValueError("engaged halt snapshot requires changed_by and reason")
    return snapshot["halted"]


def replay_from_snapshots(
    *,
    spec: dict,
    account: dict,
    intents: list[dict],
    market: dict | None = None,
    halted: bool = False,
) -> dict:
    """Evaluate a deterministic research-side decision over frozen inputs."""
    result = evaluate_spec_risk(
        spec=spec,
        account=account,
        intents=intents,
        market=market,
        halted=halted,
    )
    return {
        **result,
        "grade": "RESEARCH",
        "evidence_kind": "REPLAY",
        "operational_approval": False,
    }


def replay_with_market_regime(
    *,
    spec: dict,
    account: dict,
    intents: list[dict],
    decision_date,
    regime_service=None,
    halt_snapshot: dict | None = None,
) -> dict:
    """Research entry: bind CSI300 PIT regime to a pinned Spec before replay."""
    from app.lib.strategy_engine.market_snapshot import frozen_regime_snapshot
    from caifubao_contracts.strategy_spec import validate_strategy_spec

    resolved = validate_strategy_spec(spec)
    policy = resolved["risk"]["regime_policy"]
    market = (
        frozen_regime_snapshot(decision_date, service=regime_service)
        if policy != "none"
        else None
    )
    halted = (
        read_frozen_halt_snapshot(halt_snapshot)
        if halt_snapshot is not None
        else False
    )
    return replay_from_snapshots(
        spec=resolved,
        account=account,
        intents=intents,
        market=market,
        halted=halted,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spec", required=True, type=Path)
    parser.add_argument("--account", required=True, type=Path)
    parser.add_argument("--intents", required=True, type=Path)
    parser.add_argument("--market", type=Path)
    parser.add_argument("--halted-snapshot", action="store_true")
    parser.add_argument(
        "--halt-snapshot", type=Path,
        help="Frozen research-only JSON halt snapshot; never the live halt store",
    )
    args = parser.parse_args(argv)

    def load(path: Path):
        with path.open(encoding="utf-8") as source:
            return json.load(source)

    if args.halt_snapshot and args.halted_snapshot:
        parser.error("--halt-snapshot and --halted-snapshot are exclusive")
    halted = (
        read_frozen_halt_snapshot(load(args.halt_snapshot))
        if args.halt_snapshot
        else args.halted_snapshot
    )
    result = replay_from_snapshots(
        spec=load(args.spec),
        account=load(args.account),
        intents=load(args.intents),
        market=load(args.market) if args.market else None,
        halted=halted,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
