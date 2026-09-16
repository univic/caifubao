"""Tushare pro.daily_basic handler — full-market valuation snapshot per date.

Normalization logic for pro.daily_basic responses. Fetching is delegated to
tushare_interface.daily_basic_by_trade_date (retry + pacing + empty check live
in the interface layer, mirroring adj_factor_by_trade_date).

Values are point-in-time by trade_date: tushare computes them at that trade
date's close from the latest published financials, so there is no look-ahead
when joining into research snapshots keyed by trade date.
"""

from __future__ import annotations

import datetime
import logging

import pandas

from app.lib.datahub.data_source.interface import tushare_interface

logger = logging.getLogger(__name__)


def normalize_daily_basic(raw_df, trade_date: str):
    """Map a raw tushare daily_basic frame to canonical stock_daily_basic rows.

    Returns a list of dicts ready for upsert:
        {code, date, pe_ttm, pb, ps_ttm, dv_ttm, total_mv, circ_mv,
         turnover_rate}
    Units kept as tushare reports them: total_mv/circ_mv 万元, dv_ttm/turnover
    rates as %, pe_ttm/pb/ps_ttm as plain ratios. NaN valuations (tushare
    leaves blanks for e.g. loss-making pe) are kept as None so research can
    filter explicitly instead of misreading a 0.0 sentinel as a real value.
    """
    if raw_df is None or raw_df.empty:
        return []

    if "ts_code" not in raw_df.columns:
        raise ValueError("tushare daily_basic response missing ts_code column")

    frame = raw_df.copy()
    frame["code"] = frame["ts_code"].map(tushare_interface.from_tushare_ts_code)
    frame["date"] = datetime.datetime.strptime(trade_date, "%Y%m%d")

    numeric_columns = [
        "pe_ttm",
        "pb",
        "ps_ttm",
        "dv_ttm",
        "total_mv",
        "circ_mv",
        "turnover_rate",
    ]
    for column in numeric_columns:
        if column not in frame:
            frame[column] = None
        else:
            frame[column] = pandas.to_numeric(frame[column], errors="coerce")

    canonical_columns = ["code", "date"] + numeric_columns
    frame = frame[canonical_columns]

    rows = []
    for _, row in frame.iterrows():
        doc = {"code": row["code"], "date": row["date"]}
        for column in numeric_columns:
            value = row[column]
            if pandas.isna(value):
                doc[column] = None
            else:
                doc[column] = float(value)
        rows.append(doc)
    return rows


def fetch_and_normalize(trade_date: str):
    """Fetch one trade date and normalize to canonical stock_daily_basic rows."""
    raw_df = tushare_interface.daily_basic_by_trade_date(trade_date)
    rows = normalize_daily_basic(raw_df, trade_date)
    logger.info(
        "daily_basic %s: fetched=%s normalized=%s",
        trade_date,
        0 if raw_df is None else len(raw_df),
        len(rows),
    )
    return rows
