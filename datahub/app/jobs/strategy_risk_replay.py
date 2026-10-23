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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spec", required=True, type=Path)
    parser.add_argument("--account", required=True, type=Path)
    parser.add_argument("--intents", required=True, type=Path)
    parser.add_argument("--market", type=Path)
    parser.add_argument("--halted-snapshot", action="store_true")
    args = parser.parse_args(argv)

    def load(path: Path):
        with path.open(encoding="utf-8") as source:
            return json.load(source)

    result = replay_from_snapshots(
        spec=load(args.spec),
        account=load(args.account),
        intents=load(args.intents),
        market=load(args.market) if args.market else None,
        halted=args.halted_snapshot,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
