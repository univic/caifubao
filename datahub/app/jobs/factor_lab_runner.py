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
* **Four conventions coexist** (this lab, the h20 snapshot, the backtest service,
  `scoring_engine/factor_eval`'s calendar window). `panel.py`'s module docstring
  tabulates them and `openspec/changes/factor-lab-label-semantics/` records which
  one is authoritative for which consumer.
"""

import argparse
import datetime
import json
import logging
import os
import sys

logger = logging.getLogger(__name__)

#: Quote fields the lab needs. Projected explicitly so a full-history export
#: never hydrates Documents and a missing field is obvious. ``previous_close``
#: is what the *open*-vs-limit verdict needs; ``isST`` is projected so the 5 %
#: main-board ST band is actually reachable (it previously read a field that was
#: never selected, so every name looked non-ST).
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
    "previous_close",
    "trade_status",
    "isST",
)

#: Label column families, longest prefix first so `fwd_short_h5` is not read as
#: `fwd_h` + `_short_h5`.
LABEL_FAMILIES = ("blocked_short_h", "blocked_h", "fwd_short_h", "fwd_h")

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


def split_label_column(column):
    """Return ``(family, horizon)`` for a label column, else ``None``."""
    for family in LABEL_FAMILIES:
        if column.startswith(family):
            try:
                return family, int(column[len(family) :])
            except ValueError:
                return None
    return None


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
    sessions=None,
) -> dict:
    """Freeze a factor-research panel to ``output`` (parquet).

    ``sessions`` is the market trading calendar; the CLI passes
    ``trading_day_helper.get_a_stock_market_trade_calendar()`` so a missing quote
    row cannot silently lengthen a holding period.
    """
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
        panel = build_panel(list(queryset), horizons, sessions=sessions)
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
        "calendar_sessions": len(sessions) if sessions else 0,
        "dry_run": bool(dry_run),
    }


#: Raw columns every registered factor consumes. The export writes more (limit
#: flags, ST/BSE markers) for audit; evaluation does not read them.
_EVAL_COLUMNS = (
    "date",
    "stock_code",
    "open",
    "open_hfq",
    "close",
    "close_hfq",
    "high_hfq",
    "low_hfq",
    "volume",
    "trade_amount",
    "change_rate",
    "previous_close",
    "trade_status",
)


def _panel_is_grouped(frame) -> bool:
    """True when each stock's rows are adjacent and ascending by date.

    The export writes chunks of sorted codes, so this holds; the check exists so
    a differently-ordered parquet is re-sorted explicitly instead of being fed to
    the rolling windows in the wrong order.
    """
    import pandas as pd

    codes = frame["stock_code"].cat.codes
    if len(codes) == 0:
        return True
    if not codes.is_monotonic_increasing:
        return False
    boundary = codes.diff().ne(0)
    boundary.iloc[0] = True
    deltas = frame["date"].diff().where(~boundary, pd.Timedelta(0))
    return bool((deltas >= pd.Timedelta(0)).all())


def _load_panel(path, horizons=None):
    """Load a frozen panel, trimmed to the horizons being evaluated.

    A 2024-2026 panel is ~3.3M rows and ~34 columns; the blocking-reason strings
    alone are hundreds of MB of object columns. Only the columns an evaluation
    reads are projected at *read* time (``pd.read_parquet(columns=...)``), so the
    peak never includes the other horizons' labels or the export-only limit flags
    — which is what let the 3.3M-row sweep fit inside the 2Gi pod limit.
    """
    import pandas as pd
    import pyarrow.parquet as pq

    available = set(pq.ParquetFile(path).schema.names)
    wanted = [column for column in _EVAL_COLUMNS if column in available]
    selected = {int(h) for h in horizons} if horizons else None
    for column in sorted(available):
        parsed = split_label_column(column)
        if parsed and (selected is None or parsed[1] in selected):
            wanted.append(column)

    frame = pd.read_parquet(path, columns=wanted)
    frame["stock_code"] = frame["stock_code"].astype("category")
    if not _panel_is_grouped(frame):
        frame.sort_values(
            ["stock_code", "date"], kind="stable", ignore_index=True, inplace=True
        )
    for column in list(frame.columns):
        # `blocked_*` columns hold short reason strings, not numbers: coercing
        # them would erase every reason before the category cast below
        # (measured: "limit_up_entry" -> NaN), silently emptying the coverage
        # diagnostics.
        parsed = split_label_column(column)
        if column == "stock_code" or column == "date":
            continue
        if parsed and parsed[0].startswith("blocked"):
            continue
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float32")
    for column in list(frame.columns):
        parsed = split_label_column(column)
        if parsed and parsed[0].startswith("blocked"):
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
                "%-18s h%-3s ic=%+0.4f icir=%s t=%s tnw=%s pos=%s n_dates=%-5d "
                "top-bottom=%s decay=%s gates=%s"
                % (
                    name,
                    horizon,
                    ic["ic_mean"] if ic["ic_mean"] is not None else float("nan"),
                    ic["icir"],
                    ic["t_stat"],
                    ic.get("t_stat_nw"),
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


def holding_scan_panel(
    *,
    path,
    factor,
    horizons,
    buffers,
    portfolio_size,
    entry_pct,
) -> dict:
    """Holding-period x buffer scan of one registered factor on a frozen panel.

    This is the bridge between the factor lab's labels and the portfolio-level
    question ("how often can I afford to rebalance before friction eats the
    edge?"). The label semantics stay the lab's: a blocked entry/exit is dropped,
    never rolled forward, so every cell is measured on the same frozen panel.
    """
    from app.lib.factor_lab.factors import REGISTRY, compute
    from app.lib.factor_lab.panel import session_span

    # Imported here (not at module import time) to keep the lab's module graph
    # free of a load-time edge into the strategy engine.
    from app.lib.strategy_engine.holding_scan import (
        build_scan_input,
        scan_cell,
        summary_table,
    )

    if factor not in REGISTRY:
        raise KeyError(
            f"unknown factor {factor!r}; known: {', '.join(sorted(REGISTRY))}"
        )

    panel = _load_panel(path, horizons=horizons)
    signal = compute(panel, factor)
    cells = []
    for horizon in sorted({int(h) for h in horizons}):
        if f"fwd_h{horizon}" not in panel.columns:
            continue
        # One horizon at a time: each scan frame is a few hundred MB on a
        # full-history panel, and holding every horizon's copy at once
        # OOMKilled the 6 GiB pod.
        frame = build_scan_input(panel, signal, horizon)
        for buffer in sorted({float(b) for b in buffers}):
            cells.append(
                scan_cell(
                    frame,
                    horizon=horizon,
                    buffer=buffer,
                    entry_pct=entry_pct,
                    portfolio_size=portfolio_size,
                )
            )
        del frame
    table = summary_table(cells)
    return {
        "panel": str(path),
        "span": session_span(panel),
        "factor": factor,
        "entry_pct": entry_pct,
        "portfolio_size": portfolio_size,
        "cells": cells,
        "summary": table.to_dict(orient="records") if not table.empty else [],
    }


def _scan_summary(report: dict) -> str:
    lines = [
        "%-18s h%-3s buffer=%s ir=%s ann_excess=%s turnover=%s rebalances=%s"
        % (
            report["factor"],
            row.get("horizon"),
            row.get("buffer"),
            row.get("information_ratio"),
            row.get("annualized_net_excess_return"),
            row.get("annual_turnover"),
            row.get("rebalances"),
        )
        for row in report["summary"]
    ]
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

    scan = commands.add_parser(
        "holding-scan",
        help="Holding-period x buffer scan of one factor (portfolio rotation)",
    )
    scan.add_argument("--panel", required=True)
    scan.add_argument("--factor", required=True)
    scan.add_argument("--horizons", default="5,10,20,40,60")
    scan.add_argument("--buffers", default="1.0,1.5,2.0")
    scan.add_argument("--portfolio-size", type=int, default=800)
    scan.add_argument("--entry-pct", type=float, default=0.2)
    scan.add_argument("--output", default=None)

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
        from app.lib.utilities.trading_day_helper import (
            get_a_stock_market_trade_calendar,
        )

        result = export_panel(
            from_date=parse_date(args.from_date),
            to_date=parse_date(args.to_date),
            horizons=parse_horizons(args.horizons),
            output=args.output,
            chunk_codes=args.chunk_codes,
            limit_codes=args.limit_codes,
            stock_code=args.stock_code,
            dry_run=args.dry_run,
            sessions=get_a_stock_market_trade_calendar(),
        )
        print(json.dumps(result, sort_keys=True))
        return 0

    if args.command == "holding-scan":
        report = holding_scan_panel(
            path=args.panel,
            factor=args.factor,
            horizons=parse_horizons(args.horizons),
            buffers=[float(part) for part in args.buffers.split(",") if part.strip()],
            portfolio_size=args.portfolio_size,
            entry_pct=args.entry_pct,
        )
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                json.dump(report, handle, ensure_ascii=False, indent=2, default=str)
        print(_scan_summary(report))
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
