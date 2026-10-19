# -*- coding: utf-8 -*-
"""Survivorship / ST / BSE audit over a frozen factor-lab panel.

Why an audit instead of a fix
-----------------------------
The 2020-2026 panel the research replays ran on is survivorship-biased: it holds
5,221 codes, 1,668 of them (32 %) first appear after 2020-01, and **no code ever
disappears** — the delisted cohort is missing altogether, so a return computed on
it is an upper bound. The same panel's ``is_st`` share is 0.28 %-0.82 % against a
real A-share 2 %-5 % (so the ST exclusion is barely active), and ``is_bse`` is
constantly 0 because the data-source layer already excludes BSE. Patching a
frozen panel cannot invent prices for delisted names, so this module does not try
to repair the bias. It makes the bias *explicit and machine-readable* so that a
replay artifact cannot present a biased return as an unbiased one.

The supplement is fetched by ``datahub/scripts/research_universe_backfill.py``
(research-only: tushare delisted list + name-change intervals). Re-exporting the
panel with that supplement, and re-running the out-of-sample arm on it, are
explicit operator steps — not a silent part of the audit.

Everything here is pure over the panel frame
--------------------------------------------
`audit_panel` reads only the columns of the frozen panel (``date``,
``stock_code``, ``is_st``, ``is_bse``, ``trade_status``, ``close``); it opens
no database, fetches nothing, and never mutates its input. `panel.py`'s export
already carries every column the audit needs, so no second data source can
disagree with the panel being audited.

Definitions the report pins
---------------------------
* ``first_seen`` (per year): a code whose first panel session is **later than the
  panel's first session** — an entrant inside the sample. Codes present on the
  first session are the pre-sample base, not entrants.
* ``disappeared`` (per year): a code whose **last** panel session is earlier than
  the panel's last session. From inside a panel a terminal trading suspension is
  indistinguishable from a delisting, so the audit counts any vanishing code as a
  delisted-cohort member. The choice is deliberate: it can only *clear* the gap
  flag (never hide a gap), and the residual limit is reported.
* ``st_share`` / ``bse_share``: share of panel **rows** (code-days) in the year
  carrying the flag, because the label rules act per row. Distinct code counts
  are reported alongside as ``st_codes`` / ``bse_codes``.
* ``tradeable_pool``: rows in the year that could enter a label at all —
  ``trade_status == 1`` and a positive ``close``. It deliberately does **not**
  pre-apply the BSE/ST exclusion: that is caller policy, and both sizes are
  reported separately so a reader can subtract them.
* ``survivorship_gap``: ``true`` when **no** code disappears during the panel, so
  there is no delisted cohort to estimate a delisting return from. This is the
  flag a replay artifact must carry (``REPLAY`` + upper-bound limitation).
* ``st_flag_basis``: ``current_snapshot`` when ``is_st`` is the panel's snapshot
  flag (its only native source is the quote feed's ``isST``), or
  ``namechange_history`` when the caller rebuilt ``is_st`` from tushare
  ``namechange`` intervals (``research_universe_backfill.py``). A snapshot flag is
  not point-in-time and is never labelled as such. This function cannot verify
  the provenance of a boolean column, so a caller claiming
  ``namechange_history`` is asserting it rebuilt the column from intervals; the
  basis is reported, never inferred from the data.
"""

import pandas as pd

#: The two ST flag provenances a report may name.
ST_FLAG_BASIS_CURRENT = "current_snapshot"
ST_FLAG_BASIS_NAMECHANGE = "namechange_history"
ST_FLAG_BASES = (ST_FLAG_BASIS_CURRENT, ST_FLAG_BASIS_NAMECHANGE)

#: Columns without which a per-year report is meaningless. Every other column is
#: optional: its metric is reported as ``None`` rather than fabricated when the
#: panel does not carry it.
REQUIRED_COLUMNS = ("date", "stock_code")


def normalize_stock_code(value) -> str:
    """Normalize an A-share code to the panel's internal ``sh600000`` form.

    Accepts ``sh600000``, ``600000.SH``, ``SH600000`` and a bare ``600000``
    (bare digits are mapped by exchange prefix: 6/9 -> sh, 0/3 -> sz, 4/8 -> bj).
    Returns ``""`` for an unusable value instead of guessing.
    """
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "." in text:
        left, _, right = text.partition(".")
        if len(left) == 6 and left.isdigit() and right in {"sh", "sz", "bj"}:
            return right + left
        return ""
    if len(text) == 8 and text[:2] in {"sh", "sz", "bj"} and text[2:].isdigit():
        return text
    if len(text) == 6 and text.isdigit():
        if text[0] in {"6", "9"}:
            return "sh" + text
        if text[0] in {"0", "3"}:
            return "sz" + text
        if text[0] in {"4", "8"}:
            return "bj" + text
    return ""


def _code_key(value) -> str:
    """Comparison key for a code on either side of a set difference.

    A normalizable code becomes its internal form; anything else (a synthetic
    test code, a placeholder) is compared as a stripped lowercase string. Both
    the panel and the supplied listing go through this key, so ``600000.SH`` in
    a listing matches ``sh600000`` in the panel.
    """
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    normalized = normalize_stock_code(value)
    return normalized or str(value or "").strip().lower()


def _flag_series(frame: pd.DataFrame, column: str):
    """Boolean mask for an optional 0/1 or bool column, else ``None``.

    Non-numeric values (``None``/``NaN``/odd strings) read as ``False`` so an
    unknown flag never counts as coverage, and a missing column is reported as
    ``None`` rather than silently as an all-false cohort.
    """
    if column not in frame.columns:
        return None
    values = frame[column]
    if values.dtype == bool:
        return values
    if str(values.dtype) == "boolean":
        return values.fillna(False).astype(bool)
    return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)


def _share(count: int, total: int):
    return round(count / total, 4) if total else 0.0


def _empty_frame_error() -> ValueError:
    return ValueError(
        "audit_panel needs a non-empty panel frame; an empty frame cannot "
        "report survivorship, ST or BSE coverage (check the panel export "
        "window and columns before retrying)"
    )


def audit_panel(frame, *, listed_codes=None, st_flag_basis=None) -> dict:
    """Audit one frozen panel for survivorship, ST and BSE coverage.

    ``frame``: a factor-lab panel frame (or any frame with ``date`` and
    ``stock_code`` plus, optionally, ``is_st``/``is_bse``/``trade_status``/
    ``close``). It is never mutated and nothing outside it is read.

    ``listed_codes``: optional exchange universe (active or delisted) to diff
    against the panel; each entry may be ``sh600000``, ``600000.SH`` or
    ``600000``. When supplied, ``missing_listed_codes`` (sorted, internal form)
    and its counts are reported; when omitted they are ``None``.

    ``st_flag_basis``: ``current_snapshot`` (default) or ``namechange_history``;
    see the module docstring for what each asserts. Anything else raises
    ``ValueError`` — the report must name its basis, not guess.

    Returns the per-year coverage table, the ``survivorship_gap`` flag, the
    basis name, the optional missing-listed-code diff, and the limitations a
    replay artifact should carry. Empty or column-incomplete frames raise
    ``ValueError`` instead of returning a report that looks clean.
    """
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise _empty_frame_error()
    missing_columns = [name for name in REQUIRED_COLUMNS if name not in frame.columns]
    if missing_columns:
        raise ValueError(
            "audit_panel requires the panel columns %s; missing %s"
            % (", ".join(REQUIRED_COLUMNS), ", ".join(missing_columns))
        )
    basis = (st_flag_basis or ST_FLAG_BASIS_CURRENT).strip().lower()
    if basis not in ST_FLAG_BASES:
        raise ValueError(
            "unknown st_flag_basis %r; expected one of %s"
            % (st_flag_basis, ", ".join(ST_FLAG_BASES))
        )

    dates = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    # ``astype(object)`` keeps a categorical ``stock_code`` (the export's dtype)
    # from turning the derived key into a categorical too, which would make the
    # per-code groupby carry every unobserved category.
    codes = frame["stock_code"].astype(object).map(_code_key)
    usable = dates.notna() & codes.ne("")
    dropped = {
        "invalid_date": int(dates.isna().sum()),
        "invalid_code": int((~dates.isna() & codes.eq("")).sum()),
    }

    work = pd.DataFrame(
        {"code": codes[usable], "date": dates[usable]},
        columns=("code", "date"),
    )
    if work.empty:
        raise ValueError(
            "audit_panel found no usable (code, date) rows; "
            "dropped %d rows with an invalid date and %d with an empty code"
            % (dropped["invalid_date"], dropped["invalid_code"])
        )
    work["year"] = work["date"].dt.year.astype("int64")

    # The flags are aligned to the original frame index, then narrowed to the
    # rows the date/code filter kept.
    st_flag = _flag_series(frame, "is_st")
    bse_flag = _flag_series(frame, "is_bse")
    st_flag = st_flag[usable] if st_flag is not None else None
    bse_flag = bse_flag[usable] if bse_flag is not None else None

    status = (
        pd.to_numeric(frame["trade_status"], errors="coerce")
        if "trade_status" in frame.columns
        else None
    )
    close = (
        pd.to_numeric(frame["close"], errors="coerce")
        if "close" in frame.columns
        else None
    )
    tradeable = None
    if status is not None:
        tradeable = status.eq(1)
    if close is not None:
        tradeable = close.gt(0) if tradeable is None else tradeable & close.gt(0)
    tradeable = tradeable[usable] if tradeable is not None else None

    panel_first = work["date"].min()
    panel_last = work["date"].max()
    first_by_code = work.groupby("code")["date"].min()
    last_by_code = work.groupby("code")["date"].max()
    entrants = first_by_code[first_by_code > panel_first]
    leavers = last_by_code[last_by_code < panel_last]
    entrants_by_year = entrants.dt.year.value_counts()
    leavers_by_year = leavers.dt.year.value_counts()

    rows_by_year = work.groupby("year").size()
    codes_by_year = work.groupby("year")["code"].nunique()

    years = []
    for year in sorted(int(value) for value in work["year"].unique()):
        in_year = work["year"].eq(year)
        rows = int(rows_by_year.get(year, 0))
        entry = {
            "year": year,
            "rows": rows,
            "codes": int(codes_by_year.get(year, 0)),
            "first_seen": int(entrants_by_year.get(year, 0)),
            "disappeared": int(leavers_by_year.get(year, 0)),
            "st_count": None,
            "st_share": None,
            "st_codes": None,
            "bse_count": None,
            "bse_share": None,
            "bse_codes": None,
            "tradeable_pool": None,
            "tradeable_codes": None,
        }
        for prefix, flag in (("st", st_flag), ("bse", bse_flag)):
            if flag is None:
                continue
            count = int(flag[in_year].sum())
            entry[f"{prefix}_count"] = count
            entry[f"{prefix}_share"] = _share(count, rows)
            entry[f"{prefix}_codes"] = int(work.loc[in_year & flag, "code"].nunique())
        if tradeable is not None:
            entry["tradeable_pool"] = int(tradeable[in_year].sum())
            entry["tradeable_codes"] = int(
                work.loc[in_year & tradeable, "code"].nunique()
            )
        years.append(entry)

    panel_codes = set(work["code"].unique())
    missing_listed = None
    missing_listed_count = None
    listed_code_count = None
    listed_coverage = None
    if listed_codes is not None:
        if isinstance(listed_codes, str):
            # A single code, not an iterable of characters.
            listed_codes = [listed_codes]
        listed = sorted({_code_key(value) for value in listed_codes} - {""})
        listed_code_count = len(listed)
        missing_listed = [code for code in listed if code not in panel_codes]
        missing_listed_count = len(missing_listed)
        listed_coverage = _share(
            listed_code_count - missing_listed_count, listed_code_count
        )

    limitations = []
    survivorship_gap = len(leavers) == 0
    if survivorship_gap:
        limitations.append(
            "survivorship: the panel contains no delisted cohort, so this "
            "replay's returns are an upper bound"
        )
    if basis == ST_FLAG_BASIS_CURRENT:
        limitations.append(
            "st: is_st is a current snapshot, not point-in-time; historical ST "
            "membership is not reconstructed"
        )
    if st_flag is None:
        limitations.append(
            "st: the panel carries no is_st column; ST coverage is unknown"
        )
    if bse_flag is None:
        limitations.append(
            "bse: the panel carries no is_bse column; BSE coverage is unknown"
        )
    if missing_listed_count:
        limitations.append(
            "universe: %d supplied listed codes are absent from the panel"
            % missing_listed_count
        )

    return {
        "panel": {
            "rows": int(len(work)),
            "codes": len(panel_codes),
            "sessions": int(work["date"].nunique()),
            "from": panel_first.date().isoformat(),
            "to": panel_last.date().isoformat(),
            "years": len(years),
        },
        "years": years,
        "survivorship_gap": survivorship_gap,
        "delisted_cohort": int(len(leavers)),
        "delisted_years": sorted(int(value) for value in leavers_by_year.index),
        "first_seen_cohort": int(len(entrants)),
        "first_seen_years": sorted(int(value) for value in entrants_by_year.index),
        "st_flag_basis": basis,
        "st_flag_point_in_time": basis == ST_FLAG_BASIS_NAMECHANGE,
        "missing_listed_codes": missing_listed,
        "missing_listed_count": missing_listed_count,
        "listed_code_count": listed_code_count,
        "listed_coverage": listed_coverage,
        "dropped_rows": dropped,
        "limitations": limitations,
    }
