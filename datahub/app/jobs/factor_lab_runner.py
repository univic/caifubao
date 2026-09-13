# -*- coding: utf-8 -*-
"""Factor-lab CLI — frozen research panel + single-factor evaluation.

Usage::

    # 1) freeze a panel (read-only against Mongo, writes one parquet)
    python -m app.jobs.factor_lab_runner export \
        --from-date 2024-01-01 --to-date 2026-09-11 \
        --horizons 1,5,20,60 --output /tmp/lab.parquet

    # 2) evaluate one factor, or every registered factor
    python -m app.jobs.factor_lab_runner evaluate --panel /tmp/lab.parquet \
        --factor momentum_10 --horizons 5,20,60
    python -m app.jobs.factor_lab_runner evaluate --panel /tmp/lab.parquet \
        --all --horizons 20

    # 3) list the factor menu
    python -m app.jobs.factor_lab_runner list

Design notes
------------
* **No look-ahead**: labels are positional (T+1 open to the open ``h`` sessions
  later), factors use backward windows only, and the split helper derives windows
  from the panel itself.
* **Code-chunked export**: labels need future sessions *of the same stock*, so the
  export chunks by stock code (not by date) — a date chunk would cut every label
  at the boundary. Each chunk is appended to the parquet independently, which
  keeps peak memory near one chunk instead of the whole history.
* **Drop, don't roll**: an untradable entry/exit yields a null label (with the
  reason recorded), because this measures a factor's edge. The paper path rolls a
  blocked order forward instead — a different question with a different holding
  period, so the two must not share labels.
"""

import argparse
import datetime
import json
import logging
import os
import sys

logger = logging.getLogger(__name__)

#: Quote fields the lab needs. Projected explicitly so a full-history export
#: never hydrates Documents and a missing field is obvious.
QUOTE_FIELDS = (
    "code",
    "date",
    "open",
    "open_hfq",
    "close",
    "close_hfq",
    "high_hfq",
    "low_hfq",
    "volume",
    "trade_amount",
    "change_rate",
    "trade_status",
)

#: Codes per export chunk. Chunking by code keeps every stock's history whole
#: while bounding memory to one chunk of the panel frame.
DEFAULT_CHUNK_CODES = 400


def _init_db() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def parse_date(value: str) -> datetime.datetime:
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def parse_horizons(value: str):
    return sorted({int(part) for part in value.split(",") if part.strip()})


def iter_code_chunks(all_codes, size):
    codes = sorted(all_codes)
    for index in range(0, len(codes), size):
        yield codes[index : index + size]


def export_panel(
    *,
    from_date,
    to_date,
    horizons,
    output,
    chunk_codes=DEFAULT_CHUNK_CODES,
    limit_codes=None,
    stock_code=None,
    quote_model=None,
    dry_run=False,
) -> dict:
    """Freeze a factor-research panel to ``output`` (parquet)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from app.lib.factor_lab.panel import build_panel

    if quote_model is None:
        from app.model.stock import StockDailyQuote

        quote_model = StockDailyQuote

    if stock_code:
        codes = [stock_code]
    else:
        from app.model.stock import IndividualStock

        codes = list(IndividualStock.objects(active_status=0).distinct("code"))
    if limit_codes:
        codes = sorted(codes)[: int(limit_codes)]

    chunks = list(iter_code_chunks(codes, chunk_codes))
    writer = None
    rows_written = 0
    for index, chunk in enumerate(chunks, start=1):
        queryset = (
            quote_model.objects(
                code__in=chunk,
                date__gte=from_date,
                date__lte=to_date,
            )
            .only(*QUOTE_FIELDS)
            .order_by("date")
            .as_pymongo()
        )
        panel = build_panel(list(queryset), horizons)
        rows_written += len(panel)
        if not dry_run and not panel.empty:
            if writer is None:
                writer = pq.ParquetWriter(output, pa.Table.from_pandas(panel).schema)
            writer.write_table(pa.Table.from_pandas(panel, preserve_index=False))
        print(
            "  chunk %d/%d codes=%d rows=%d"
            % (index, len(chunks), len(chunk), len(panel)),
            file=sys.stderr,
            flush=True,
        )
    if writer is not None:
        writer.close()

    return {
        "output": str(output),
        "from": from_date.date().isoformat(),
        "to": to_date.date().isoformat(),
        "horizons": list(horizons),
        "codes": len(codes),
        "chunks": len(chunks),
        "rows": rows_written,
        "dry_run": bool(dry_run),
    }


def _load_panel(path, horizons=None):
    """Load a frozen panel, trimmed to the horizons being evaluated.

    A 2024-2026 panel is ~3.3M rows; carrying every horizon's label AND its
    blocking-reason string would be hundreds of MB of object columns for nothing.
    Non-requested horizons are dropped, and the reason columns are read as
    ``category`` (short repeated strings) instead of Python objects.
    """
    import pandas as pd

    frame = pd.read_parquet(path)
    for column in frame.columns:
        if column not in ("date", "stock_code"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(
                "float32"
            )
    frame["stock_code"] = frame["stock_code"].astype("category")
    if horizons:
        keep = {int(h) for h in horizons}
        dropped = [
            column
            for column in frame.columns
            if column.startswith(("fwd_h", "blocked_h"))
            and int(column.rsplit("h", 1)[1]) not in keep
        ]
        frame = frame.drop(columns=dropped)
    for column in list(frame.columns):
        if column.startswith("blocked_h"):
            frame[column] = frame[column].astype("category")
    return frame


def evaluate_panel(
    *, path, factor, horizons, all_factors=False, quantiles=10, top_fraction=0.1
) -> dict:
    """Evaluate one factor (or every registered factor) on a frozen panel."""
    import gc

    from app.lib.factor_lab.factors import REGISTRY, compute
    from app.lib.factor_lab.metrics import evaluate_factor
    from app.lib.factor_lab.panel import session_span

    panel = _load_panel(path, horizons=horizons)
    names = sorted(REGISTRY) if all_factors else [factor]
    unknown = [name for name in names if name not in REGISTRY]
    if unknown:
        raise KeyError(f"unknown factor(s): {', '.join(unknown)}")

    results = {}
    for name in names:
        values = compute(panel, name)
        results[name] = evaluate_factor(
            panel,
            values,
            horizons=horizons,
            factor_name=name,
            quantiles=quantiles,
            top_fraction=top_fraction,
        )
        del values
        gc.collect()
    return {
        "panel": str(path),
        "span": session_span(panel),
        "factors": results,
    }


def _summary(report: dict) -> str:
    """One line per factor/horizon so a sweep is readable at a glance."""
    lines = []
    for name, entry in report["factors"].items():
        for horizon, stats in entry["horizons"].items():
            ic = stats["ic"]
            quantiles = stats["quantiles"]
            walk = stats["walk_forward"].get("walk_forward_decay")
            lines.append(
                "%-18s h%-3s ic=%+0.4f icir=%s t=%s pos=%s n_dates=%-5d "
                "top-bottom=%s decay=%s gates=%s"
                % (
                    name,
                    horizon,
                    ic["ic_mean"] if ic["ic_mean"] is not None else float("nan"),
                    ic["icir"],
                    ic["t_stat"],
                    ic["positive_share"],
                    ic["n_dates"],
                    quantiles.get("top_minus_bottom"),
                    walk,
                    "PASS"
                    if stats["gates"]["passed"]
                    else ",".join(stats["gates"]["failures"]),
                )
            )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Factor research lab")
    commands = parser.add_subparsers(dest="command", required=True)

    export = commands.add_parser("export", help="Freeze a research panel")
    export.add_argument("--from-date", required=True)
    export.add_argument("--to-date", required=True)
    export.add_argument("--horizons", default="1,5,20,60")
    export.add_argument("--output", required=True)
    export.add_argument("--chunk-codes", type=int, default=DEFAULT_CHUNK_CODES)
    export.add_argument("--limit-codes", type=int, default=None)
    export.add_argument("--stock-code", default=None)
    export.add_argument("--dry-run", action="store_true")

    evaluate = commands.add_parser("evaluate", help="Evaluate factors on a panel")
    evaluate.add_argument("--panel", required=True)
    evaluate.add_argument("--factor")
    evaluate.add_argument("--all", action="store_true")
    evaluate.add_argument("--horizons", default="5,20,60")
    evaluate.add_argument("--quantiles", type=int, default=10)
    evaluate.add_argument("--top-fraction", type=float, default=0.1)
    evaluate.add_argument("--output", default=None)

    commands.add_parser("list", help="List registered factors")
    return parser


def main(argv=None) -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "WARNING"))
    args = build_parser().parse_args(argv)

    if args.command == "list":
        from app.lib.factor_lab.factors import REGISTRY

        for name in sorted(REGISTRY):
            print(f"{name:20s} {REGISTRY[name][1]}")
        return 0

    if args.command == "export":
        _init_db()
        result = export_panel(
            from_date=parse_date(args.from_date),
            to_date=parse_date(args.to_date),
            horizons=parse_horizons(args.horizons),
            output=args.output,
            chunk_codes=args.chunk_codes,
            limit_codes=args.limit_codes,
            stock_code=args.stock_code,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, sort_keys=True))
        return 0

    if not args.factor and not args.all:
        raise SystemExit("evaluate requires --factor NAME or --all")
    report = evaluate_panel(
        path=args.panel,
        factor=args.factor,
        horizons=parse_horizons(args.horizons),
        all_factors=args.all,
        quantiles=args.quantiles,
        top_fraction=args.top_fraction,
    )
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2, default=str)
    print(_summary(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
