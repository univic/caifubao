# -*- coding: utf-8 -*-
"""Factor-research panel: multi-horizon labels from raw daily quote rows.

Four label conventions now coexist in this repository
----------------------------------------------------
| # | location | label / tradability rule | consumer |
|---|----------|--------------------------|----------|
| 1 | `autoresearch_h20_snapshot_runner._executable` | flat ±9.9 %, roll a blocked order forward | h20 autoresearch snapshot |
| 2 | `backend/app/services/backtest_service._can_trade` | flat ±9.9 %, roll forward | backtest service |
| 3 | `scoring_engine/factor_eval._build_dataset` | forward row = the h-th row inside an `int(h*1.5)` *calendar* window | `tech_factor_runner evaluate` |
| 4 | this module | board-aware limits, drop-on-untradable, position-checked trading-day offsets | the factor lab |

They must not share labels. Roll-forward answers "what would the strategy do" and
silently changes the holding period, so it cannot be used for IC statistics; a
calendar window cannot even resolve a Friday `h=1` observation (it needs three
calendar days); and a flat ±9.9 % limit misreads a ChiNext +15 % session as
limit-up. `openspec/changes/factor-lab-label-semantics/` records the decision and
the per-consumer authority.

Why not reuse the h20 snapshot
-----------------------------
It is shaped for one thing: the 8 scoring components of a *single* horizon, with
only four price columns per evaluation day. A short/medium-horizon factor lab
needs the opposite emphases:

* **labels at trading-day offsets**, for several horizons at once, with no
  calendar-window approximation (the legacy `int(h*1.5)` rule cannot resolve
  h=1 Friday observations at all);
* **volume/liquidity features**, so turnover and Amihud-style factors are
  computable;
* **board-aware price limits** (main ±10 %, ChiNext/STAR ±20 %, BSE ±30 %,
  ST ±5 % on the main board), resolved from the *open against the previous
  close* — the price an order would actually fill at — not from the session's
  close-to-close change;
* **drop-on-untradable** labelling: this measures a factor's predictive power.
  The paper/backtest path instead rolls a blocked order forward, which answers a
  different question ("what would the strategy do") and silently changes the
  holding period — so the two must not share a label.

Rows are pure dicts (``as_pymongo`` output), so the panel builds without
hydrating a single Document.
"""

import datetime
from collections import defaultdict

import pandas as pd

from app.lib.utilities.data_capability_helper import is_bse_stock_code

#: Percent daily price limits by board. ``st`` only tightens the **main** board
#: to 5 %; ChiNext/STAR ST names keep their 20 % band. The window the project can
#: currently score has no ``isST`` flag for 2026+ (see the pit-50k report), so
#: callers should treat the ST band as best-effort.
LIMIT_MAIN = 10.0
LIMIT_GROWTH = 20.0
LIMIT_BSE = 30.0
LIMIT_ST = 5.0

#: Blocking tolerance, matching the existing harness's ±9.9 % rule: a stock is
#: untradable when it sits within 0.1 pp of its limit.
LIMIT_EPSILON = 0.1

_PANEL_COLUMNS = (
    "date",
    "stock_code",
    "close",
    "close_hfq",
    "open",
    "open_hfq",
    "high_hfq",
    "low_hfq",
    "volume",
    "trade_amount",
    "change_rate",
    "previous_close",
    "trade_status",
    "is_bse",
    "is_st",
    "limit_up",
    "limit_down",
    "board_limit",
)

#: Label column families written per horizon. `fwd_h{h}`/`blocked_h{h}` are the
#: long leg; `fwd_short_h{h}`/`blocked_short_h{h}` the mirrored short leg.
LABEL_FAMILIES = ("fwd_h", "blocked_h", "fwd_short_h", "blocked_short_h")


def price_limit(stock_code: str, *, is_st: bool = False) -> float:
    """Percent daily price limit for one A-share code."""
    code = (stock_code or "").strip().lower()
    # `is_bse_stock_code` also matches the bare `[489]\\d{5}` form, which catches
    # Shanghai B-shares (900xxx). An exchange-prefixed sh/sz code is never BSE.
    if is_bse_stock_code(code) and not code.startswith(("sh", "sz", "900")):
        return LIMIT_BSE
    # Accept both exchange-prefixed (`sz300750`) and bare (`300750`) codes.
    if code.startswith(
        ("sh688", "sh689", "sz300", "sz301", "688", "689", "300", "301")
    ):
        return LIMIT_GROWTH
    return LIMIT_ST if is_st else LIMIT_MAIN


def _series_price(raw, hfq) -> pd.Series:
    """Tradable price used for labels: HFQ price, falling back to the raw one."""
    values = pd.to_numeric(hfq, errors="coerce")
    fallback = pd.to_numeric(raw, errors="coerce")
    return values.where(values > 0, fallback)


def _frame_from_rows(rows) -> pd.DataFrame:
    records = []
    for row in rows:
        code = row.get("code") or row.get("stock_code")
        if not code or row.get("date") is None:
            continue
        records.append(
            {
                "date": row["date"],
                "stock_code": code,
                "close": row.get("close"),
                "close_hfq": row.get("close_hfq"),
                "open": row.get("open"),
                "open_hfq": row.get("open_hfq"),
                "high_hfq": row.get("high_hfq"),
                "low_hfq": row.get("low_hfq"),
                "volume": row.get("volume"),
                "trade_amount": row.get("trade_amount"),
                "change_rate": row.get("change_rate"),
                "previous_close": row.get("previous_close"),
                "trade_status": row.get("trade_status", 1),
                "is_bse": bool(row.get("is_bse", is_bse_stock_code(code))),
                "is_st": bool(row.get("is_st", row.get("isST", 0))),
            }
        )
    return pd.DataFrame.from_records(
        records,
        columns=(
            "date",
            "stock_code",
            "close",
            "close_hfq",
            "open",
            "open_hfq",
            "high_hfq",
            "low_hfq",
            "volume",
            "trade_amount",
            "change_rate",
            "previous_close",
            "trade_status",
            "is_bse",
            "is_st",
        ),
    )


def _session_positions(frame: pd.DataFrame, sessions):
    """Map each row's session to its index in the trading calendar.

    ``sessions`` is the market calendar for the export window (usually
    ``trading_day_helper.get_a_stock_market_trade_calendar()``). When it is not
    supplied the panel derives one from its own distinct dates — exact for a
    multi-code export, but a single-stock export cannot see market sessions on
    which that stock has no quote row.
    """
    if sessions is None:
        calendar = sorted(pd.to_datetime(pd.Series(frame["date"].unique())))
    else:
        calendar = sorted({pd.Timestamp(value).normalize() for value in sessions})
    lookup = {value: index for index, value in enumerate(calendar)}
    return frame["date"].map(lookup), lookup


def _blocked_series(frame: pd.DataFrame, checks) -> pd.Series:
    """First matching reason per row, in the order given.

    ``checks`` is a list of ``(reason, mask)``; the first true mask wins, so the
    order encodes priority (a non-existent session before a missing price, an
    unknown price limit before a limit verdict, …).
    """
    blocked = pd.Series([None] * len(frame), index=frame.index, dtype="object")
    for reason, mask in checks:
        blocked = blocked.mask(blocked.isna() & mask, reason)
    return blocked


def build_panel(rows, horizons, sessions=None) -> pd.DataFrame:
    """Build a factor-research panel from raw quote rows.

    ``rows``: iterable of raw dicts for one or more stocks, any date order.
    ``horizons``: trading-day holding periods, e.g. ``(1, 5, 20, 60)``.
    ``sessions``: optional market trading calendar for the export window.

    Returns one row per ``(stock_code, date)`` with the raw features plus, per
    horizon ``h``:

    * ``fwd_h{h}`` — long-leg gross return from the T+1 open to the open ``h``
      sessions later, **NaN** when either session is missing or untradable (the
      reason is in ``blocked_h{h}``; NaN rather than None because the column is
      float64 and must survive a parquet round trip);
    * ``blocked_h{h}`` — why the long label is missing
      (``no_next_session``/``missing_price``/``suspended_entry``/
      ``missing_previous_close``/``limit_up_entry``/``no_exit_yet``/
      ``suspended_exit``/``limit_down_exit``/``missing_session_between``);
    * ``fwd_short_h{h}`` / ``blocked_short_h{h}`` — the mirrored **short** leg
      (sell the T+1 open, cover the h-th open), blocked on the opposite side:
      ``limit_down_entry`` and ``limit_up_exit``.

    Labels are positional offsets in *trading sessions*, and a label is dropped
    when a missing quote row would make the offset land on a different session
    than the calendar prescribes — otherwise a data gap would silently lengthen
    the holding period.
    """
    frame = _frame_from_rows(rows)
    horizons = sorted({int(h) for h in horizons})
    if frame.empty:
        columns = {}
        for horizon in horizons:
            columns[f"fwd_h{horizon}"] = pd.Series(dtype="float64")
            columns[f"blocked_h{horizon}"] = pd.Series(dtype="object")
            columns[f"fwd_short_h{horizon}"] = pd.Series(dtype="float64")
            columns[f"blocked_short_h{horizon}"] = pd.Series(dtype="object")
        return frame.assign(**columns)

    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    frame = frame.sort_values(["stock_code", "date"], kind="stable")
    frame = frame.drop_duplicates(["stock_code", "date"], keep="first")
    frame = frame.reset_index(drop=True)

    frame["board_limit"] = [
        price_limit(code, is_st=bool(st))
        for code, st in zip(frame["stock_code"], frame["is_st"])
    ]

    open_price = pd.to_numeric(frame["open"], errors="coerce")
    close_price = pd.to_numeric(frame["close"], errors="coerce")
    change = pd.to_numeric(frame["change_rate"], errors="coerce")
    # Prefer the quoted previous close; derive it from close/change_rate when the
    # feed omits it. Either way the limit verdict is "did the *open* sit at the
    # limit", which is what an order would face — not the close-to-close change.
    previous = pd.to_numeric(frame["previous_close"], errors="coerce")
    previous = previous.where(previous > 0)
    derived = (close_price / (1 + change / 100)).where(lambda values: values > 0)
    frame["previous_close"] = previous.fillna(derived)

    usable = frame["previous_close"].notna() & (open_price > 0)
    upper = frame["previous_close"] * (1 + (frame["board_limit"] - LIMIT_EPSILON) / 100)
    lower = frame["previous_close"] * (1 - (frame["board_limit"] - LIMIT_EPSILON) / 100)
    # `boolean` (not bool) so an underivable previous close stays visibly unknown
    # instead of silently reading as "tradable".
    frame["limit_up"] = pd.Series(pd.NA, index=frame.index, dtype="boolean")
    frame["limit_down"] = pd.Series(pd.NA, index=frame.index, dtype="boolean")
    frame.loc[usable, "limit_up"] = open_price[usable] >= upper[usable]
    frame.loc[usable, "limit_down"] = open_price[usable] <= lower[usable]

    positions, session_lookup = _session_positions(frame, sessions)

    grouped = frame.groupby("stock_code", sort=False)
    entry_raw = grouped["open"].shift(-1)
    entry_hfq = grouped["open_hfq"].shift(-1)
    entry_status = grouped["trade_status"].shift(-1)
    entry_limit_up = grouped["limit_up"].shift(-1).astype("boolean")
    entry_limit_down = grouped["limit_down"].shift(-1).astype("boolean")
    entry_exists = grouped["date"].shift(-1).notna()
    entry_gap = pd.Series(False, index=frame.index)
    if session_lookup:
        entry_pos = grouped["date"].shift(-1).map(session_lookup)
        entry_gap = entry_exists & (positions + 1 != entry_pos)

    entry = _series_price(entry_raw, entry_hfq)

    for horizon in horizons:
        exit_raw = grouped["open"].shift(-(1 + horizon))
        exit_hfq = grouped["open_hfq"].shift(-(1 + horizon))
        exit_status = grouped["trade_status"].shift(-(1 + horizon))
        exit_limit_up = grouped["limit_up"].shift(-(1 + horizon)).astype("boolean")
        exit_limit_down = grouped["limit_down"].shift(-(1 + horizon)).astype("boolean")
        exit_date = grouped["date"].shift(-(1 + horizon))
        exit_exists = exit_date.notna()

        exit_ = _series_price(exit_raw, exit_hfq)
        gap = entry_gap.copy()
        if session_lookup:
            exit_pos = exit_date.map(session_lookup)
            gap = gap | (exit_exists & (positions + 1 + horizon != exit_pos))

        gross = (exit_ - entry) / entry

        long_blocked = _blocked_series(
            frame,
            [
                ("no_next_session", ~entry_exists),
                ("missing_price", entry.isna() | (entry <= 0)),
                ("suspended_entry", entry_status != 1),
                ("missing_previous_close", entry_limit_up.isna()),
                ("limit_up_entry", entry_limit_up.fillna(False)),
                ("no_exit_yet", ~exit_exists),
                ("missing_price", exit_.isna() | (exit_ <= 0)),
                ("suspended_exit", exit_status != 1),
                ("missing_previous_close", exit_limit_down.isna()),
                ("limit_down_exit", exit_limit_down.fillna(False)),
                ("missing_session_between", gap),
            ],
        )
        frame[f"fwd_h{horizon}"] = gross.where(long_blocked.isna())
        frame[f"blocked_h{horizon}"] = long_blocked

        short_blocked = _blocked_series(
            frame,
            [
                ("no_next_session", ~entry_exists),
                ("missing_price", entry.isna() | (entry <= 0)),
                ("suspended_entry", entry_status != 1),
                ("missing_previous_close", entry_limit_down.isna()),
                ("limit_down_entry", entry_limit_down.fillna(False)),
                ("no_exit_yet", ~exit_exists),
                ("missing_price", exit_.isna() | (exit_ <= 0)),
                ("suspended_exit", exit_status != 1),
                ("missing_previous_close", exit_limit_up.isna()),
                ("limit_up_exit", exit_limit_up.fillna(False)),
                ("missing_session_between", gap),
            ],
        )
        frame[f"fwd_short_h{horizon}"] = (-gross).where(short_blocked.isna())
        frame[f"blocked_short_h{horizon}"] = short_blocked

    keep = [c for c in _PANEL_COLUMNS if c in frame.columns]
    keep += [
        c
        for h in horizons
        for c in (
            f"fwd_h{h}",
            f"blocked_h{h}",
            f"fwd_short_h{h}",
            f"blocked_short_h{h}",
        )
        if c in frame.columns
    ]
    return frame.loc[:, keep]


def coverage(panel: pd.DataFrame, horizon: int, *, leg: str = "long") -> dict:
    """Label coverage and the blocking-reason histogram for one horizon/leg."""
    prefix = "blocked_short_h" if leg == "short" else "blocked_h"
    label_column = f"fwd_short_h{horizon}" if leg == "short" else f"fwd_h{horizon}"
    column = f"{prefix}{horizon}"
    total = len(panel)
    resolved = int(panel[label_column].notna().sum()) if label_column in panel else 0
    reasons: dict = defaultdict(int)
    if column in panel.columns:
        counts = panel[column].value_counts(dropna=True)
        reasons.update({str(k): int(v) for k, v in counts.items()})
    return {
        "leg": leg,
        "rows": total,
        "resolved": resolved,
        "coverage": round(resolved / total, 4) if total else 0.0,
        "blocked_reasons": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
    }


def session_span(panel: pd.DataFrame) -> dict:
    """First/last session and the number of distinct sessions in the panel."""
    if panel.empty:
        return {"sessions": 0, "from": None, "to": None}
    dates = pd.to_datetime(panel["date"])
    return {
        "sessions": int(dates.nunique()),
        "from": dates.min().date().isoformat(),
        "to": dates.max().date().isoformat(),
    }


def split_by_date(panel: pd.DataFrame, start, end) -> pd.DataFrame:
    """Rows whose session falls inside ``[start, end]`` (inclusive)."""
    dates = pd.to_datetime(panel["date"])
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end)
    return panel.loc[(dates >= lower) & (dates <= upper)]


def default_splits(panel: pd.DataFrame) -> list[tuple[str, str, str]]:
    """Calendar splits for walk-forward: train / validation / test.

    Four fifths of the panel trains, then the remainder is halved into
    validation and a locked test window. Kept deliberately simple and derived
    from the panel itself so a different export range cannot silently reuse a
    hardcoded window.
    """
    span = session_span(panel)
    if span["sessions"] < 5:
        return []
    start = datetime.date.fromisoformat(span["from"])
    end = datetime.date.fromisoformat(span["to"])
    total = (end - start).days
    train_end = start + datetime.timedelta(days=int(total * 0.7))
    valid_end = start + datetime.timedelta(days=int(total * 0.85))
    return [
        ("train", start.isoformat(), train_end.isoformat()),
        (
            "validation",
            (train_end + datetime.timedelta(days=1)).isoformat(),
            valid_end.isoformat(),
        ),
        ("test", (valid_end + datetime.timedelta(days=1)).isoformat(), end.isoformat()),
    ]
