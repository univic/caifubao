# -*- coding: utf-8 -*-
"""Factor-research panel: multi-horizon labels from raw daily quote rows.

Relationship to the existing h20 harness
---------------------------------------
`autoresearch_h20_snapshot_runner` already exports a frozen parquet, but it is
shaped for one thing: the 8 scoring components of a *single* horizon, with only
four price columns per evaluation day, flat ±9.9 % price-limit detection and
roll-forward entry/exit resolution. A short/medium-horizon factor lab needs the
opposite emphases:

* **labels at trading-day offsets**, for several horizons at once, with no
  calendar-window approximation (the legacy `int(h*1.5)` rule cannot resolve
  h=1 Friday observations at all);
* **volume/liquidity features**, so turnover and Amihud-style factors are
  computable;
* **board-aware price limits** (main ±10 %, ChiNext/STAR ±20 %, BSE ±30 %,
  ST ±5 %), because on short horizons a limit-up open is the difference between
  a tradable and a fictive entry;
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

#: Percent daily price limits by board. ``st`` overrides to 5 %; the window the
#: project can currently score has no ``isST`` flag at all (see the pit-50k
#: report), so callers should treat the ST band as best-effort.
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
    "trade_status",
    "is_bse",
    "limit_up",
    "limit_down",
    "board_limit",
)


def price_limit(stock_code: str, *, is_st: bool = False) -> float:
    """Percent daily price limit for one A-share code."""
    if is_st:
        return LIMIT_ST
    code = (stock_code or "").lower()
    if is_bse_stock_code(code):
        return LIMIT_BSE
    if code.startswith(("sh688", "sh689", "sz300", "sz301")):
        return LIMIT_GROWTH
    return LIMIT_MAIN


def _eval_price(open_price, open_hfq):
    """Tradable price used for labels: HFQ open, falling back to the raw open."""
    if open_hfq:
        return float(open_hfq)
    if open_price:
        return float(open_price)
    return None


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
            "trade_status",
            "is_bse",
            "is_st",
        ),
    )


def build_panel(rows, horizons) -> pd.DataFrame:
    """Build a factor-research panel from raw quote rows.

    ``rows``: iterable of raw dicts for one or more stocks, any date order.
    ``horizons``: trading-day holding periods, e.g. ``(1, 5, 20, 60)``.

    Returns one row per ``(stock_code, date)`` with the raw features plus, per
    horizon ``h``:

    * ``fwd_h{h}`` — gross return from the T+1 open to the open ``h`` sessions
      later, **NaN** when either session is missing or untradable (the reason is
      in ``blocked_h{h}``; NaN rather than None because the column is float64 and
      must survive a parquet round trip);
    * ``blocked_h{h}`` — why the label is missing
      (``missing_price``/``suspended_entry``/``limit_up_entry``/
      ``suspended_exit``/``limit_down_exit``/``no_exit_yet``).

    Labels are positional (the h-th subsequent session), never calendar-based.
    """
    frame = _frame_from_rows(rows)
    horizons = sorted({int(h) for h in horizons})
    if frame.empty:
        return frame.assign(
            **{f"fwd_h{h}": pd.Series(dtype="float64") for h in horizons}
        )

    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values(["stock_code", "date"], kind="stable")
    frame = frame.drop_duplicates(["stock_code", "date"], keep="first")
    frame = frame.reset_index(drop=True)

    frame["board_limit"] = [
        price_limit(code, is_st=bool(st))
        for code, st in zip(frame["stock_code"], frame["is_st"])
    ]
    change = pd.to_numeric(frame["change_rate"], errors="coerce")
    # No change_rate (a gap in the feed) must not silently look tradable: treat
    # it as untradable so a limit session can never fabricate an entry.
    frame["limit_up"] = change.isna() | (change >= frame["board_limit"] - LIMIT_EPSILON)
    frame["limit_down"] = change.isna() | (
        change <= -(frame["board_limit"] - LIMIT_EPSILON)
    )

    grouped = frame.groupby("stock_code", sort=False)
    entry_price = grouped["open_hfq"].shift(-1)
    entry_raw = grouped["open"].shift(-1)
    entry_status = grouped["trade_status"].shift(-1)
    entry_limit_up = grouped["limit_up"].shift(-1)

    for horizon in horizons:
        exit_price = grouped["open_hfq"].shift(-(1 + horizon))
        exit_raw = grouped["open"].shift(-(1 + horizon))
        exit_status = grouped["trade_status"].shift(-(1 + horizon))
        exit_limit_down = grouped["limit_down"].shift(-(1 + horizon))

        entry = [_eval_price(raw, hfq) for raw, hfq in zip(entry_raw, entry_price)]
        exit_ = [_eval_price(raw, hfq) for raw, hfq in zip(exit_raw, exit_price)]
        entry = pd.Series(entry, index=frame.index, dtype="float64")
        exit_ = pd.Series(exit_, index=frame.index, dtype="float64")

        blocked = pd.Series([None] * len(frame), index=frame.index, dtype="object")
        blocked = blocked.mask(entry.isna() | (entry <= 0), "missing_price")
        blocked = blocked.mask(blocked.isna() & (entry_status != 1), "suspended_entry")
        blocked = blocked.mask(
            blocked.isna() & entry_limit_up.astype("boolean").fillna(True),
            "limit_up_entry",
        )
        blocked = blocked.mask(blocked.isna() & exit_.isna(), "no_exit_yet")
        blocked = blocked.mask(
            blocked.isna() & ((exit_ <= 0) | (exit_status != 1)), "suspended_exit"
        )
        blocked = blocked.mask(
            blocked.isna() & exit_limit_down.astype("boolean").fillna(True),
            "limit_down_exit",
        )

        gross = (exit_ - entry) / entry
        frame[f"fwd_h{horizon}"] = gross.where(blocked.isna())
        frame[f"blocked_h{horizon}"] = blocked

    keep = [c for c in _PANEL_COLUMNS if c in frame.columns]
    keep += [
        c
        for h in horizons
        for c in (f"fwd_h{h}", f"blocked_h{h}")
        if c in frame.columns
    ]
    return frame.loc[:, keep]


def forward_returns(panel: pd.DataFrame, horizon: int) -> pd.Series:
    """Net-of-cost label series for ``horizon`` (gross minus a round trip).

    Costs are applied at label level with the profile's rates
    (``slippage_per_side`` 10 bp, ``commission_rate`` 2.5 bp, ``sell_stamp_duty``
    10 bp) so a decile spread is directly comparable to the paper path. The
    minimum-commission floor and board lots are portfolio-level effects and are
    handled where positions are sized, not here.
    """
    from app.lib.factor_lab.metrics import round_trip_cost

    return panel[f"fwd_h{horizon}"] - round_trip_cost()


def coverage(panel: pd.DataFrame, horizon: int) -> dict:
    """Label coverage and the blocking-reason histogram for one horizon."""
    column = f"blocked_h{horizon}"
    total = len(panel)
    resolved = int(panel[f"fwd_h{horizon}"].notna().sum())
    reasons: dict = defaultdict(int)
    if column in panel.columns:
        counts = panel[column].value_counts(dropna=True)
        reasons.update({str(k): int(v) for k, v in counts.items()})
    return {
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
