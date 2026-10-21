"""Read-only survivorship/ST/BSE audit over a frozen factor-lab panel.

Thin wrapper: it loads only the panel columns the audit needs, calls the pure
``app.lib.factor_lab.universe.audit_panel`` and prints the report as JSON. It
writes nothing, opens no database and fetches nothing, so it is safe to run
against a research artifact while the production cluster is untouched.

Usage::

    datahub/.venv/bin/python datahub/scripts/research_universe_audit.py \\
        --panel /data/lab_2026q3.parquet

    # diff the panel against a supplied universe: a backfill parquet (delisted
    # cohort) or an explicit comma-separated list
    ... --panel /data/lab_2026q3.parquet \\
        --listing /data/research/universe_delisted.parquet
    ... --panel /data/lab_2026q3.parquet \\
        --listed-codes sh600000,sz000001,600519.SH

Exit status is non-zero (traceback from the audit/parquet layer) when the panel
is empty, unreadable, or missing ``date``/``stock_code`` — a report that cannot
be produced is never printed as a clean one.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Importing the ``app`` package configures datahub logging and prints a one-line
# environment notice to stdout, which corrupts the JSON this CLI is documented to
# print (and pipe into jq). The import therefore runs with stdout redirected to
# stderr: diagnostics stay on stderr, the report is the only thing on stdout.
with contextlib.redirect_stdout(sys.stderr):
    from app.lib.factor_lab.universe import (  # noqa: E402
        ST_FLAG_BASES,
        ST_FLAG_BASIS_CURRENT,
        audit_panel,
    )

#: The only panel columns the audit reads. Projected at read time so auditing a
#: 3.3M-row, 34-column panel never loads the label columns.
AUDIT_COLUMNS = ("date", "stock_code", "close", "trade_status", "is_bse", "is_st")


def load_panel(path) -> pd.DataFrame:
    """Read the audit columns of a frozen panel parquet (read-only)."""
    available = set(pq.ParquetFile(path).schema.names)
    wanted = [column for column in AUDIT_COLUMNS if column in available]
    return pd.read_parquet(path, columns=wanted)


def load_listed_codes(*, codes=None, listing=None):
    """Collect the universe to diff against the panel.

    ``codes`` is a comma-separated list (``sh600000``, ``600000.SH`` or a bare
    ``600000``); ``listing`` is a research parquet from
    ``research_universe_backfill.py`` (its ``code`` column is read). Both may be
    given at once. Returns ``None`` when no universe was supplied, so the audit
    reports the diff keys as ``None`` instead of an empty list.
    """
    collected = []
    if listing:
        frame = pd.read_parquet(listing, columns=["code"])
        collected.extend(frame["code"].tolist())
    if codes:
        collected.extend(part for part in codes.split(",") if part.strip())
    if not collected:
        return None
    return collected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only survivorship/ST/BSE audit over a factor-lab panel"
    )
    parser.add_argument("--panel", required=True, help="frozen panel parquet")
    parser.add_argument(
        "--listed-codes",
        default=None,
        help="comma-separated listed codes to diff against the panel",
    )
    parser.add_argument(
        "--listing",
        default=None,
        help="research universe parquet whose code column is the listed universe",
    )
    parser.add_argument(
        "--st-flag-basis",
        default=ST_FLAG_BASIS_CURRENT,
        choices=list(ST_FLAG_BASES),
        help="provenance of the panel's is_st column (default: %(default)s)",
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    frame = load_panel(args.panel)
    report = audit_panel(
        frame,
        listed_codes=load_listed_codes(codes=args.listed_codes, listing=args.listing),
        st_flag_basis=args.st_flag_basis,
    )
    report["panel_path"] = str(args.panel)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
