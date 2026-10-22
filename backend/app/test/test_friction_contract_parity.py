# -*- coding: utf-8 -*-
"""Friction-model parity between the backend and the datahub execution model.

The backend image and the datahub image cannot share a Python package yet, so
the A-share cost constants are mirrored (canonical source:
``datahub/app/lib/execution/costs.py``). This test parses both files and fails
when a rate diverges, which is how the 0.1 % / 0.05 % stamp-duty split went
unnoticed and made backtest and research P&L incomparable.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_SERVICE = REPO_ROOT / "backend" / "app" / "services" / "backtest_service.py"
CANONICAL_COSTS = REPO_ROOT / "datahub" / "app" / "lib" / "execution" / "costs.py"

# backend constant -> canonical constant
SHARED_CONSTANTS = {
    "COMMISSION_RATE": "COMMISSION_RATE",
    "MIN_COMMISSION": "MIN_COMMISSION_CNY",
    "STAMP_DUTY_RATE": "SELL_STAMP_DUTY_RATE",
    "SLIPPAGE": "SLIPPAGE_PER_SIDE",
}


def _module_constants(path: Path) -> dict[str, float]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    constants: dict[str, float] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, (int, float)):
            constants[target.id] = float(value.value)
    return constants


def test_backend_friction_constants_match_the_canonical_model():
    if not BACKEND_SERVICE.is_file() or not CANONICAL_COSTS.is_file():
        pytest.skip("repository layout does not contain both cost models")

    backend = _module_constants(BACKEND_SERVICE)
    canonical = _module_constants(CANONICAL_COSTS)

    problems = []
    for backend_name, canonical_name in SHARED_CONSTANTS.items():
        if backend_name not in backend:
            problems.append(f"missing backend constant: {backend_name}")
            continue
        if canonical_name not in canonical:
            problems.append(f"missing canonical constant: {canonical_name}")
            continue
        if backend[backend_name] != canonical[canonical_name]:
            problems.append(
                f"{backend_name}={backend[backend_name]} != "
                f"{canonical_name}={canonical[canonical_name]}"
            )

    assert not problems, "friction model drift:\n" + "\n".join(problems)
