"""Research-only universe backfill: delisted instruments and name-change history.

``basic_stock`` and ``stock_daily_quote`` are production collections, and a
survivorship supplement must never touch them. This tool therefore does exactly
one thing: it reads tushare and writes **one research parquet** (plus an optional
quotes parquet next to it). It is never scheduled, exposes no API, and imports no
production module — no model, no DB watcher, no writer. That is structural, not a
promise: the module's only dependencies are the standard library and pandas, the
tushare client is injected (``run(pro=...)``), and the tests drive it with a fake
client.

Why delisted instruments
------------------------
The frozen factor-lab panel's code set never shrinks (see
``app/lib/factor_lab/universe.py`` and the 2026-09-19 progress entry): the
delisted cohort is missing altogether, so every replay return built on that panel
is an upper bound. ``stock_basic(list_status='D')`` is the tushare call that lists
those instruments, and ``namechange`` is the windowed call that gives their ST
history as intervals rather than a snapshot.

Artifact
--------
One parquet, one row per delisted instrument, sorted by code:

``code`` (internal ``sh600000``), ``ts_code``, ``name``, ``exchange``,
``list_date``/``delist_date`` (ISO ``YYYY-MM-DD`` or null), ``namechange_count``
and ``namechange_intervals`` (a compact JSON array of
``{name, start_date, end_date, change_reason, ann_date}``, ascending by start
date). ISO dates match the panel's ``date`` column; use ``pd.to_datetime`` when
joining.

Usage::

    datahub/.venv/bin/python datahub/scripts/research_universe_backfill.py \\
        --from-date 2015-01-01 --to-date 2026-09-18 \\
        --output /data/research/universe_delisted.parquet

    # sample the daily quotes of the delisted codes too (off by default)
    ... --with-quotes
    # fetch and count, write nothing
    ... --dry-run

``--dry-run`` still fetches (so the counts are real) and writes nothing, not even
a parent directory. Empty results and tushare errors are loud: the CLI exits
non-zero with the failing call named, never a silently empty parquet. The JSON
summary reports the counts, the date ranges and the residual gaps.

Residual gaps this tool cannot close
------------------------------------
* ``stock_basic`` carries no delisting *reason* and no last-trade date; the
  ``delist_date`` it reports is the exchange's formal delisting date.
* Instruments delisted before tushare's coverage, or never reclassified into
  ``list_status='D'``, are simply absent. ``counts.delisted_fetched`` and the
  echoed ``window`` make that visible instead of implying completeness.
* B-shares (``sh900*``/``sz200*``) are excluded: they are delisted alongside the
  A-shares but were never in the A-share panel, so keeping them would make the
  listed-code diff report a false "missing from the panel". The count is reported
  as ``residual_gaps.b_share_excluded``.
* ``namechange`` is fetched for change dates inside the window, so an ST interval
  that was already in force when the window starts is only visible through its
  earlier change. Start the window before the panel you intend to annotate.
* Fetching quotes for delisted codes does not add them to the frozen panel. The
  re-export, and the out-of-sample rerun on the supplemented universe, are
  explicit operator steps.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import time
from pathlib import Path

import pandas as pd

#: Every artifact this tool writes is research evidence, never production data.
RESEARCH_ONLY = True
WARNING = (
    "research-only artifact: production basic_stock and stock_daily_quote are "
    "not read or written; this parquet is not a production collection"
)

TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"
#: Same pacing as the production tushare interface (300 calls/min tier).
DEFAULT_PACE_SECONDS = 0.25
#: ``pro.daily`` caps one call at 6000 rows (~23 years of one code), so quote
#: history is requested in 18-year windows; a window that hits the cap is
#: refused instead of silently truncated.
QUOTE_WINDOW_YEARS = 18
#: ``pro.namechange`` caps one call at 2000 rows. Name changes are fetched in
#: one-year windows; a window that reaches the cap is refused, not truncated.
NAMECHANGE_ROW_CAP = 2000
DELISTED_FIELDS = "ts_code,name,list_date,delist_date"
NAMECHANGE_FIELDS = "ts_code,name,start_date,end_date,ann_date,change_reason"
#: ``stock_basic`` returns a market suffix, not an exchange column; map it back.
_EXCHANGE_BY_MARKET = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
#: B-shares are delisted and listed alongside the A-shares, but they were never in
#: the A-share panel. Keeping them would make the listed-code diff report them as
#: "missing from the panel" for the wrong reason.
_B_SHARE_PREFIXES = ("sh900", "sz200")
QUOTE_FIELDS = (
    "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
)

DELISTED_COLUMNS = ("code", "ts_code", "name", "exchange", "list_date", "delist_date")
NAMECHANGE_COLUMNS = (
    "code",
    "ts_code",
    "name",
    "start_date",
    "end_date",
    "ann_date",
    "change_reason",
)
RESEARCH_COLUMNS = DELISTED_COLUMNS + ("namechange_count", "namechange_intervals")


def parse_date(value: str) -> datetime.date:
    """Parse a CLI date (``YYYY-MM-DD``)."""
    try:
        return datetime.datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError as error:
        raise ValueError(f"invalid date {value!r}; expected YYYY-MM-DD") from error


def _tushare_date(value: datetime.date) -> str:
    return value.strftime("%Y%m%d")


def _iso_date(value) -> str | None:
    """Normalize a tushare ``YYYYMMDD`` (or date-like) value to ISO or ``None``.

    ``None`` is honest here: tushare returns blanks for a missing
    ``delist_date``, and coercing that to an epoch date would invent a cohort.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None
    digits = text.replace("-", "").replace("/", "").replace(" ", "")
    if len(digits) != 8 or not digits.isdigit():
        return None
    try:
        return datetime.date(
            int(digits[:4]), int(digits[4:6]), int(digits[6:])
        ).isoformat()
    except ValueError:
        return None


def _internal_code(ts_code) -> str:
    """Map a tushare ``600000.SH`` code to the panel's ``sh600000`` form."""
    digits, _, market = str(ts_code or "").strip().partition(".")
    if len(digits) == 6 and digits.isdigit() and market.lower() in {"sh", "sz", "bj"}:
        return market.lower() + digits
    return ""


def _year_windows(start: datetime.date, end: datetime.date, span_years: int):
    """Yield inclusive ``(YYYYMMDD, YYYYMMDD)`` windows covering ``[start, end]``."""
    cursor = start
    while cursor <= end:
        window_end = min(
            datetime.date(cursor.year + span_years - 1, 12, 31),
            end,
        )
        yield _tushare_date(cursor), _tushare_date(window_end)
        cursor = window_end + datetime.timedelta(days=1)


def _call(pro, method_name: str, *, label: str, pace: float, **kwargs):
    """Call one injected tushare client method, failing loudly with context."""
    method = getattr(pro, method_name, None)
    if not callable(method):
        raise RuntimeError(
            f"tushare client has no callable {method_name}(); "
            "the backfill needs pro.stock_basic/pro.namechange/pro.daily"
        )
    try:
        frame = method(**kwargs)
    except Exception as error:  # noqa: BLE001 - re-raised with the call named
        raise RuntimeError(f"tushare call failed: {label}: {error}") from error
    if pace:
        time.sleep(pace)
    return frame


def fetch_delisted(pro, *, pace: float = DEFAULT_PACE_SECONDS) -> pd.DataFrame:
    """Fetch every delisted A-share instrument (``list_status='D'``)."""
    raw = _call(
        pro,
        "stock_basic",
        label="stock_basic(list_status='D')",
        pace=pace,
        exchange="",
        list_status="D",
        fields=DELISTED_FIELDS,
    )
    if raw is None or len(raw) == 0:
        raise RuntimeError(
            "tushare stock_basic(list_status='D') returned no rows; the delisted "
            "cohort is the point of this backfill, so an empty result is a hard "
            "failure (check the token, the tier and tushare's status)"
        )
    records = []
    for row in raw.to_dict("records"):
        code = _internal_code(row.get("ts_code"))
        if not code:
            continue
        records.append(
            {
                "code": code,
                "ts_code": str(row.get("ts_code") or "").strip().upper(),
                "name": str(row.get("name") or "").strip(),
                "exchange": _EXCHANGE_BY_MARKET.get(
                    str(row.get("ts_code") or "").strip().upper().rpartition(".")[2]
                ),
                "list_date": _iso_date(row.get("list_date")),
                "delist_date": _iso_date(row.get("delist_date")),
            }
        )
    frame = pd.DataFrame.from_records(records, columns=DELISTED_COLUMNS)
    if frame.empty:
        raise RuntimeError(
            "tushare stock_basic(list_status='D') returned no usable ts_code; "
            "refusing to write an empty research universe"
        )
    frame = frame.drop_duplicates("code").sort_values("code", kind="stable")
    return frame.reset_index(drop=True)


def fetch_namechange(
    pro,
    *,
    from_date: datetime.date,
    to_date: datetime.date,
    pace: float = DEFAULT_PACE_SECONDS,
) -> pd.DataFrame:
    """Fetch name-change intervals for the requested change-date window.

    One tushare call per calendar year keeps every page under the endpoint's
    2000-row cap; a page that reaches the cap raises instead of truncating a
    year's ST history.
    """
    frames = []
    for window_start, window_end in _year_windows(from_date, to_date, 1):
        frame = _call(
            pro,
            "namechange",
            label=f"namechange(start_date={window_start}, end_date={window_end})",
            pace=pace,
            start_date=window_start,
            end_date=window_end,
            fields=NAMECHANGE_FIELDS,
        )
        if frame is None or len(frame) == 0:
            continue
        if len(frame) >= NAMECHANGE_ROW_CAP:
            raise RuntimeError(
                "tushare namechange window %s-%s returned %d rows, at the "
                "endpoint's per-call cap; split the window so no year's ST "
                "history is silently truncated" % (window_start, window_end, len(frame))
            )
        frames.append(frame)
    if not frames:
        raise RuntimeError(
            "tushare namechange returned no rows for %s..%s; an empty ST history "
            "means the window or the token is wrong, so this is a hard failure"
            % (from_date.isoformat(), to_date.isoformat())
        )
    records = []
    for row in pd.concat(frames, ignore_index=True).to_dict("records"):
        code = _internal_code(row.get("ts_code"))
        if not code:
            continue
        records.append(
            {
                "code": code,
                "ts_code": str(row.get("ts_code") or "").strip().upper(),
                "name": str(row.get("name") or "").strip(),
                "start_date": _iso_date(row.get("start_date")),
                "end_date": _iso_date(row.get("end_date")),
                "ann_date": _iso_date(row.get("ann_date")),
                "change_reason": str(row.get("change_reason") or "").strip() or None,
            }
        )
    frame = pd.DataFrame.from_records(records, columns=NAMECHANGE_COLUMNS)
    if frame.empty:
        raise RuntimeError(
            "tushare namechange rows carried no usable ts_code; refusing to "
            "write a research universe without ST history"
        )
    return frame.sort_values(
        ["code", "start_date", "name"], kind="stable", na_position="first"
    ).reset_index(drop=True)


def fetch_quotes(
    pro,
    instruments: pd.DataFrame,
    *,
    from_date: datetime.date,
    to_date: datetime.date,
    pace: float = DEFAULT_PACE_SECONDS,
) -> pd.DataFrame:
    """Fetch daily bars for the delisted codes (operator opt-in, default off).

    The window is clipped to each instrument's ``list_date``/``delist_date`` so
    the calls stay small; a code with no rows in its own window is simply absent
    from the result and is reported as a residual gap by ``run``.
    """
    frames = []
    for row in instruments.to_dict("records"):
        ts_code = row.get("ts_code")
        start = from_date
        if row.get("list_date"):
            start = max(start, datetime.date.fromisoformat(row["list_date"]))
        end = to_date
        if row.get("delist_date"):
            end = min(end, datetime.date.fromisoformat(row["delist_date"]))
        if start > end:
            continue
        for window_start, window_end in _year_windows(start, end, QUOTE_WINDOW_YEARS):
            frame = _call(
                pro,
                "daily",
                label="daily(ts_code=%s, start_date=%s, end_date=%s)"
                % (ts_code, window_start, window_end),
                pace=pace,
                ts_code=ts_code,
                start_date=window_start,
                end_date=window_end,
                fields=QUOTE_FIELDS,
            )
            if frame is None or len(frame) == 0:
                continue
            if len(frame) >= 6000:
                raise RuntimeError(
                    "tushare daily window %s-%s for %s returned %d rows, at the "
                    "endpoint's per-call cap; split the window"
                    % (window_start, window_end, ts_code, len(frame))
                )
            frames.append(frame.assign(code=row["code"]))
    if not frames:
        return pd.DataFrame(columns=("code", "ts_code", "date"))
    quotes = pd.concat(frames, ignore_index=True)
    # ``date`` is panel-compatible (a normalized timestamp) so a join against the
    # frozen panel needs no reformatting; ``trade_date`` keeps tushare's raw form.
    quotes["date"] = pd.to_datetime(
        quotes["trade_date"], errors="coerce"
    ).dt.normalize()
    quotes = quotes.loc[quotes["date"].notna()]
    quotes = quotes.drop_duplicates(["code", "ts_code", "date"])
    return quotes.sort_values(["code", "date"], kind="stable").reset_index(drop=True)


def _intervals_by_code(namechange: pd.DataFrame | None) -> dict:
    """Group normalized name-change rows into per-code interval lists."""
    grouped: dict = {}
    if namechange is None or len(namechange) == 0:
        return grouped
    for row in namechange.to_dict("records"):
        code = row.get("code")
        if not code:
            continue
        grouped.setdefault(code, []).append(
            {
                "name": row.get("name"),
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "change_reason": row.get("change_reason"),
                "ann_date": row.get("ann_date"),
            }
        )
    for entries in grouped.values():
        entries.sort(key=lambda entry: (entry["start_date"] or "", entry["name"] or ""))
    return grouped


def build_research_frame(
    delisted: pd.DataFrame, namechange: pd.DataFrame
) -> pd.DataFrame:
    """Turn fetched instruments + intervals into the research parquet frame.

    Pure transform: no client, no I/O. Empty instruments raise because a
    research supplement with no delisted cohort would repeat the very gap the
    audit flags.
    """
    if delisted is None or len(delisted) == 0:
        raise RuntimeError(
            "no delisted instruments to write; refusing to produce an empty "
            "research universe"
        )
    intervals = _intervals_by_code(namechange)
    records = []
    for row in delisted.to_dict("records"):
        entries = intervals.get(row["code"], [])
        records.append(
            {
                "code": row["code"],
                "ts_code": row.get("ts_code"),
                "name": row.get("name"),
                "exchange": row.get("exchange"),
                "list_date": row.get("list_date"),
                "delist_date": row.get("delist_date"),
                "namechange_count": len(entries),
                "namechange_intervals": json.dumps(
                    entries, ensure_ascii=False, sort_keys=True
                ),
            }
        )
    frame = pd.DataFrame.from_records(records, columns=RESEARCH_COLUMNS)
    return frame.sort_values("code", kind="stable").reset_index(drop=True)


def _split_window(
    delisted: pd.DataFrame, from_date: datetime.date, to_date: datetime.date
) -> tuple:
    """Split fetched instruments into the window's cohort and the rest."""
    delist_dates = pd.to_datetime(delisted["delist_date"], errors="coerce")
    lower = pd.Timestamp(from_date)
    upper = pd.Timestamp(to_date)
    mask = delist_dates.between(lower, upper)
    return (
        delisted.loc[mask].reset_index(drop=True),
        delisted.loc[~mask].reset_index(drop=True),
    )


def _iso_range(values: pd.Series) -> dict:
    dates = pd.to_datetime(values, errors="coerce").dropna()
    if dates.empty:
        return {"from": None, "to": None}
    return {
        "from": dates.min().date().isoformat(),
        "to": dates.max().date().isoformat(),
    }


def run(
    *,
    pro,
    output,
    from_date: datetime.date,
    to_date: datetime.date,
    with_quotes: bool = False,
    quotes_output=None,
    dry_run: bool = False,
    pace: float = DEFAULT_PACE_SECONDS,
) -> dict:
    """Fetch, transform and write the research universe; return its summary.

    ``pro`` is the injected tushare client (``stock_basic``, ``namechange`` and,
    only when ``with_quotes``, ``daily``). No other dependency exists: there is
    no database handle in this function, so no production write is reachable
    from it. ``dry_run`` returns the same summary without touching the disk.
    """
    if to_date < from_date:
        raise ValueError(
            "to_date %s is before from_date %s"
            % (to_date.isoformat(), from_date.isoformat())
        )
    output = Path(output)

    delisted = fetch_delisted(pro, pace=pace)
    b_shares = delisted["code"].str.startswith(_B_SHARE_PREFIXES)
    b_share_count = int(b_shares.sum())
    delisted = delisted.loc[~b_shares].reset_index(drop=True)
    in_window, outside_window = _split_window(delisted, from_date, to_date)
    if in_window.empty:
        raise RuntimeError(
            "tushare stock_basic(list_status='D') returned %d delisted "
            "instruments but none with delist_date in %s..%s; widen the window "
            "instead of writing an empty cohort"
            % (len(delisted), from_date.isoformat(), to_date.isoformat())
        )
    namechange = fetch_namechange(pro, from_date=from_date, to_date=to_date, pace=pace)
    research = build_research_frame(in_window, namechange)

    quotes = None
    quotes_summary = None
    if with_quotes:
        quotes = fetch_quotes(
            pro, research, from_date=from_date, to_date=to_date, pace=pace
        )
        covered = set(quotes["code"]) if len(quotes) else set()
        if not covered:
            raise RuntimeError(
                "no daily quotes returned for any of the %d delisted codes in "
                "%s..%s; a quote backfill that fetched nothing is a hard failure"
                % (len(research), from_date.isoformat(), to_date.isoformat())
            )
        quotes_summary = {
            "codes": len(covered),
            "rows": int(len(quotes)),
            "missing_codes": int(len(set(research["code"]) - covered)),
        }

    delisted_codes = set(research["code"])
    namechange_codes = set(namechange["code"]) if len(namechange) else set()
    summary = {
        "research_only": RESEARCH_ONLY,
        "warning": WARNING,
        "output": str(output),
        "dry_run": bool(dry_run),
        "window": {"from": from_date.isoformat(), "to": to_date.isoformat()},
        "counts": {
            "delisted_fetched": int(len(delisted)),
            "delisted_in_window": int(len(research)),
            "delisted_with_namechange": int((research["namechange_count"] > 0).sum()),
            "namechange_rows": int(len(namechange)),
            "namechange_codes": len(namechange_codes),
            # Name changes are fetched for the whole market in the window, so
            # most rows belong to still-listed codes; that is expected, not a gap.
            "non_delisted_namechange_rows": int(
                (~namechange["code"].isin(delisted_codes)).sum()
            ),
            "bse": int(research["code"].str.startswith("bj").sum()),
        },
        "date_range": {
            "list_date": _iso_range(research["list_date"]),
            "delist_date": _iso_range(research["delist_date"]),
            "namechange": _iso_range(namechange["start_date"]),
        },
        "residual_gaps": {
            "delisted_outside_window": int(len(outside_window)),
            "b_share_excluded": b_share_count,
            "delisted_without_delist_date": int(delisted["delist_date"].isna().sum()),
            "delisted_without_namechange": int(
                (research["namechange_count"] == 0).sum()
            ),
        },
        "quotes": quotes_summary,
        "quotes_output": None,
    }

    if not dry_run:
        output.parent.mkdir(parents=True, exist_ok=True)
        research.to_parquet(output, index=False)
        if quotes is not None:
            quotes_path = (
                Path(quotes_output)
                if quotes_output
                else output.with_name(output.stem + ".quotes.parquet")
            )
            quotes_path.parent.mkdir(parents=True, exist_ok=True)
            quotes.to_parquet(quotes_path, index=False)
            summary["quotes_output"] = str(quotes_path)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only delisted/namechange backfill (writes research "
        "parquet only; never basic_stock, stock_daily_quote or MongoDB)"
    )
    parser.add_argument("--from-date", required=True, help="window start, YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="window end, YYYY-MM-DD")
    parser.add_argument(
        "--output",
        required=True,
        help="research parquet to write (required, also for --dry-run)",
    )
    parser.add_argument(
        "--with-quotes",
        action="store_true",
        help="also fetch daily bars for the delisted codes (off by default)",
    )
    parser.add_argument(
        "--quotes-output",
        default=None,
        help="quotes parquet path (default: <output stem>.quotes.parquet)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="fetch and count, write nothing"
    )
    parser.add_argument(
        "--pace",
        type=float,
        default=DEFAULT_PACE_SECONDS,
        help="seconds to sleep after each tushare call (default: %(default)s)",
    )
    return parser


def _get_pro():
    """Build the tushare pro client from the environment (network, operator only)."""
    token = os.getenv(TUSHARE_TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(
            f"{TUSHARE_TOKEN_ENV} is not set; cannot backfill the research universe"
        )
    import tushare as ts

    ts.set_token(token)
    return ts.pro_api()


def main(argv=None, *, pro=None) -> int:
    args = build_parser().parse_args(argv)
    client = pro if pro is not None else _get_pro()
    summary = run(
        pro=client,
        output=args.output,
        from_date=parse_date(args.from_date),
        to_date=parse_date(args.to_date),
        with_quotes=args.with_quotes,
        quotes_output=args.quotes_output,
        dry_run=args.dry_run,
        pace=args.pace,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
