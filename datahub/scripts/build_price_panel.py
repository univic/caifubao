# -*- coding: utf-8 -*-
"""Build a canonical total-return panel from raw prices and the event ledger.

Usage::

    python scripts/build_price_panel.py \
      --raw data/000977_raw.parquet \
      --actions data/corporate_actions_000977.csv \
      --code 000977 \
      --output data/000977_panel.parquet \
      --manifest data/000977_panel.manifest.json

The command fails (exit 1) if any contract invariant is violated; it never
writes a panel that fails the limit / positivity / event reconciliation checks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.lib.prices import build_return_index, load_actions  # noqa: E402


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", required=True)
    parser.add_argument("--actions", required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--tolerance", type=float, default=0.005)
    args = parser.parse_args(argv)

    raw_path = Path(args.raw)
    raw = pd.read_parquet(raw_path)
    if "date" in raw.columns:
        raw = raw.set_index("date")
    raw = raw.sort_index()
    actions = [a for a in load_actions(args.actions) if a.code == args.code]
    if not actions:
        raise SystemExit(f"no corporate actions for {args.code} in {args.actions}")

    adjusted, violations = build_return_index(
        raw, actions, code=args.code, tolerance=args.tolerance
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    adjusted.reset_index().to_parquet(output, index=False)

    manifest = {
        "code": args.code,
        "rows": int(len(adjusted)),
        "start": str(adjusted.index.min().date()),
        "end": str(adjusted.index.max().date()),
        "events": [
            {
                "ex_date": str(a.ex_date.date()),
                "action_type": a.action_type,
                "share_ratio": a.share_ratio,
                "cash_per_share": a.cash_per_share,
                "rights_ratio": a.rights_ratio,
                "rights_price": a.rights_price,
                "source": a.source,
            }
            for a in actions
        ],
        "violations": [v.as_dict() for v in violations],
        "inputs": {
            "raw_sha256": sha256(raw_path),
            "actions_sha256": sha256(Path(args.actions)),
        },
        "output_sha256": sha256(output),
        "tolerance": args.tolerance,
    }
    Path(args.manifest).write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(
        f"{args.code}: rows={len(adjusted)} events={len(actions)} "
        f"violations={len(violations)} index_end={adjusted['return_index'].iloc[-1]:.4f}"
    )
    for violation in violations[:10]:
        print(f"  VIOLATION {violation.rule} {violation.date} {violation.detail}")
    return 0 if not violations else 1


if __name__ == "__main__":
    raise SystemExit(main())
