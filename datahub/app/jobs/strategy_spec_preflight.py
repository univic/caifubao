# -*- coding: utf-8 -*-
"""Fail-closed account/board-lot feasibility preflight for a StrategySpec.

Only frozen, decision-time reference prices may be supplied. This job does
not read the market database, create plans, or certify forward evidence.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from caifubao_contracts.strategy_spec import preflight_execution_feasibility


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spec", required=True, type=Path)
    parser.add_argument("--reference-prices", required=True, type=Path)
    parser.add_argument("--max-minimum-commission-pct", type=float, default=0.01)
    args = parser.parse_args(argv)
    with args.spec.open(encoding="utf-8") as stream:
        spec = json.load(stream)
    with args.reference_prices.open(encoding="utf-8") as stream:
        prices = json.load(stream)
    result = preflight_execution_feasibility(
        spec,
        reference_prices=prices,
        max_minimum_commission_pct=args.max_minimum_commission_pct,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
