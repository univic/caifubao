"""tushare on-exchange fund (ETF/LOF) normalization.

Turns ``pro.fund_basic`` / ``pro.fund_daily`` responses into the canonical
``exchange_traded_fund`` universe rows and ``stock_daily_quote`` quote rows.
The quote schema is intentionally the stock schema: ETF prices reuse the same
HFQ fields, so scoring/research readers keep working unchanged and the fund is
distinguished only by its ``object_type=etf`` master document.

Units follow the tushare payload: ``vol`` is 手 (kept as-is, like the stock
path) and ``amount`` is 千元, converted to CNY (×1000). Suspended funds are
absent from a ``fund_daily`` trade-date snapshot; that absence is preserved
(no synthetic zero-price row is invented here).
"""

from __future__ import annotations

import datetime

import pandas

from app.lib.datahub.data_source.interface import tushare_interface

#: Exchange-traded funds only. ``fund_daily`` also returns a few ``.OF``
#: (off-exchange) rows whose ts_code has no SH/SZ venue; they are not ETFs.
EXCHANGE_FUND_MARKETS = {"SH", "SZ"}

QUOTE_COLUMNS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "previous_close",
    "volume",
    "trade_amount",
    "turnover_rate",
    "change_rate",
    "change_amount",
    "trade_status",
    "peTTM",
    "pbMRQ",
    "psTTM",
    "pcfNcfTTM",
    "isST",
]


def _is_exchange_fund(ts_code: str) -> bool:
    return ts_code.rsplit(".", 1)[-1].upper() in EXCHANGE_FUND_MARKETS


def _parse_ts_date(value):
    if value is None or (isinstance(value, float) and pandas.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return datetime.datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None


def normalize_fund_universe(raw_df) -> list[dict]:
    """Map a ``fund_basic`` frame to exchange-traded-fund master rows.

    Only rows with a mappable SH/SZ code and a usable list date are returned;
    the caller decides which ``status`` values to keep. ``m_fee`` stays in the
    percent-per-year unit tushare reports.
    """
    if raw_df is None or raw_df.empty:
        return []

    required = {"ts_code"}
    missing = required - set(raw_df.columns)
    if missing:
        raise ValueError(
            f"Tushare fund_basic response missing required columns: {sorted(missing)}"
        )

    def _text(value):
        if value is None or (isinstance(value, float) and pandas.isna(value)):
            return None
        text = str(value).strip()
        return text or None

    def _float(value):
        number = pandas.to_numeric(value, errors="coerce")
        return None if pandas.isna(number) else float(number)

    rows = []
    for _, row in raw_df.iterrows():
        ts_code = _text(row.get("ts_code"))
        if not ts_code or "." not in ts_code or not _is_exchange_fund(ts_code):
            continue
        try:
            code = tushare_interface.from_tushare_ts_code(ts_code)
        except (ValueError, IndexError):
            continue
        rows.append(
            {
                "code": code,
                "ts_code": ts_code,
                "name": _text(row.get("name")) or ts_code,
                "fund_type": _text(row.get("fund_type")),
                "benchmark": _text(row.get("benchmark")),
                "management_fee": _float(row.get("m_fee")),
                "list_date": _parse_ts_date(row.get("list_date")),
                "status": _text(row.get("status")),
                "delist_date": _parse_ts_date(row.get("delist_date")),
            }
        )
    return rows


def normalize_tushare_fund_history(raw_df, code: str):
    """Map one fund's ``pro.fund_daily`` history to canonical quote rows."""
    if raw_df is None or raw_df.empty:
        return None

    normalized = raw_df.rename(
        columns={
            "trade_date": "date",
            "pre_close": "previous_close",
            "pct_chg": "change_rate",
            "change": "change_amount",
            "vol": "volume",
        }
    ).copy()
    required_columns = {"date", "open", "close", "high", "low", "volume"}
    missing_columns = required_columns - set(normalized.columns)
    if missing_columns:
        raise ValueError(
            "Tushare fund history response missing required columns: "
            f"{sorted(missing_columns)}"
        )

    normalized["date"] = pandas.to_datetime(normalized["date"], format="%Y%m%d")
    for column in (
        "open",
        "close",
        "high",
        "low",
        "previous_close",
        "trade_amount",
        "change_rate",
        "change_amount",
        "turnover_rate",
    ):
        if column not in normalized:
            normalized[column] = 0
        normalized[column] = pandas.to_numeric(
            normalized[column], errors="coerce"
        ).fillna(0)

    # tushare amount 单位千元 -> 元（与股票路径一致）
    normalized["trade_amount"] = (
        pandas.to_numeric(normalized["amount"], errors="coerce").fillna(0) * 1000
        if "amount" in normalized
        else 0
    )
    normalized["volume"] = normalized["volume"].astype("int64")
    normalized["code"] = code
    normalized["trade_status"] = 1
    normalized["peTTM"] = 0.0
    normalized["pbMRQ"] = 0.0
    normalized["psTTM"] = 0.0
    normalized["pcfNcfTTM"] = 0.0
    normalized["isST"] = 0

    return (
        normalized[QUOTE_COLUMNS]
        .sort_values("date")
        .drop_duplicates("date", keep="last")
        .reset_index(drop=True)
    )


def normalize_tushare_fund_snapshot(raw_df, trade_date: str) -> list[dict]:
    """Map one full-market ``fund_daily`` snapshot to quote documents.

    ``trade_date`` is the requested YYYYMMDD; every returned row is stamped
    with that date so a mislabeled provider payload cannot inject a different
    session.
    """
    if raw_df is None or raw_df.empty:
        return []

    target = datetime.datetime.strptime(trade_date, "%Y%m%d")
    rows = []
    for _, row in raw_df.iterrows():
        ts_code = str(row.get("ts_code") or "").strip()
        if not ts_code or "." not in ts_code or not _is_exchange_fund(ts_code):
            continue
        try:
            code = tushare_interface.from_tushare_ts_code(ts_code)
        except (ValueError, IndexError):
            continue

        def _num(key, default=0.0):
            value = pandas.to_numeric(row.get(key), errors="coerce")
            return default if pandas.isna(value) else float(value)

        rows.append(
            {
                "date": target,
                "code": code,
                "open": _num("open"),
                "high": _num("high"),
                "low": _num("low"),
                "close": _num("close"),
                "previous_close": _num("pre_close"),
                "volume": int(_num("vol")),
                "trade_amount": _num("amount") * 1000,
                "turnover_rate": 0.0,
                "change_rate": _num("pct_chg"),
                "change_amount": _num("change"),
                "trade_status": 1,
                "peTTM": 0.0,
                "pbMRQ": 0.0,
                "psTTM": 0.0,
                "pcfNcfTTM": 0.0,
                "isST": 0,
            }
        )
    return rows
