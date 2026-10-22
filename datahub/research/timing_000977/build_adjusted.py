# -*- coding: utf-8 -*-
"""Rebuild the canonical 000977 panel through the datahub price contract.

The event ledger lives in ``data/corporate_actions_000977.csv``; the builder in
``datahub/app/lib/prices`` applies the account-based event arithmetic and fails
on any contract violation.  This script is a thin wrapper so research runs and
production checks use exactly the same code path.
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATAHUB = HERE.parents[1]  # datahub/
sys.path.insert(0, str(DATAHUB))

import importlib.util  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "build_price_panel", DATAHUB / "scripts" / "build_price_panel.py"
)
_module = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(_module)
build_main = _module.main


def main() -> int:
    return build_main(
        [
            "--raw",
            str(HERE / "data" / "000977_raw.parquet"),
            "--actions",
            str(HERE / "data" / "corporate_actions_000977.csv"),
            "--code",
            "000977",
            "--output",
            str(HERE / "data" / "000977_panel.parquet"),
            "--manifest",
            str(HERE / "data" / "000977_panel.manifest.json"),
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
