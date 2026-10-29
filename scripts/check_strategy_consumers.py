#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fail CI when the strategy declaration and its consumer manifest drift apart.

Usage:
    python scripts/check_strategy_consumers.py [--root PATH]

See ``shared/caifubao_contracts/strategy_spec_consumers.py`` for the invariant.
Exits non-zero and lists every problem when the manifest is not honest.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED = REPO_ROOT / "shared"
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

from caifubao_contracts.strategy_spec import _ALLOWED  # noqa: E402
from caifubao_contracts.strategy_spec_consumers import (  # noqa: E402
    check,
    load_manifest,
)

DEFAULT_MANIFEST = SHARED / "caifubao_contracts" / "strategy_spec_consumers.json"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--root", default=str(REPO_ROOT))
    args = parser.parse_args(argv)

    manifest = load_manifest(Path(args.manifest))
    problems = check(manifest, _ALLOWED, Path(args.root))
    if problems:
        print("strategy declaration consumer manifest is inconsistent:")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    unconsumed = [
        path
        for path, entry in sorted(manifest["fields"].items())
        if entry["status"] == "unconsumed"
    ]
    total = len(manifest["fields"])
    print(
        f"ok: {total} declared fields, {total - len(unconsumed)} consumed, "
        f"{len(unconsumed)} unconsumed (tracked)"
    )
    for path in unconsumed:
        print(f"  unconsumed: {path} -> {manifest['fields'][path]['tracked_by']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
