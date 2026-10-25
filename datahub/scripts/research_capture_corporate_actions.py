"""Export an immutable, REPLAY-only corporate-action inventory and query receipts.

Requires a configured TUSHARE_TOKEN and per-symbol dividend API access.
Never touches Mongo, research plans, certified windows, Trading or real money.
Output is an offline retrospective source archive, not forward proof.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.lib.strategy_engine.corporate_action_publication import (  # noqa: E402
    capture_replay_inventory,
)


def _canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def write_archive(root: Path, bundle: dict) -> Path:
    """Atomically publish a digest-named *directory* or fail if it exists."""
    root.mkdir(parents=True, exist_ok=True)
    payload = _canonical(bundle)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    target = root / digest
    if target.exists():
        files = [target / "events.json", target / "manifest.json"]
        if not all(f.is_file() for f in files):
            raise ValueError("archive path occupied by incomplete bundle")
        existing = {
            "events": json.loads(files[0].read_text(encoding="utf-8")),
            "manifest": json.loads(files[1].read_text(encoding="utf-8")),
        }
        if _canonical(existing) != payload:
            raise ValueError("existing archive content differs from digest")
        return target
    with tempfile.TemporaryDirectory(dir=root, prefix=".corporate-stage-") as tmp:
        stage = Path(tmp)
        (stage / "events.json").write_text(
            _canonical(bundle["events"]) + "\n", encoding="utf-8"
        )
        (stage / "manifest.json").write_text(
            _canonical(bundle["manifest"]) + "\n", encoding="utf-8"
        )
        stage.rename(target)
    return target


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--codes-file", required=True, help="UTF-8, one stock code per line")
    parser.add_argument("--output-dir", required=True, help="Offline immutable archive parent")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    args = parser.parse_args(argv)

    codes = [
        c.strip() for c in Path(args.codes_file).read_text(encoding="utf-8").splitlines()
        if c.strip()
    ]
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        raise ValueError("TUSHARE_TOKEN must be provided through secure environment")
    import tushare as ts

    result = capture_replay_inventory(
        pro=ts.pro_api(token),
        codes=codes,
        from_date=args.from_date,
        to_date=args.to_date,
        as_of=args.as_of,
    )
    output = write_archive(Path(args.output_dir), result)
    print(
        _canonical(
            {
                "archive": str(output),
                "evidence_kind": "REPLAY",
                "coverage_claim_status": "UNVERIFIED_UPSTREAM",
                "events_count": len(result["events"]),
                "queried_codes": len(result["manifest"]["covered_codes"]),
                "written_at": datetime.datetime.now(datetime.UTC).isoformat(),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
