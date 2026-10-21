"""Adapt frozen vendor exports without inferring opening status from daily volume."""

import datetime
from decimal import Context, ROUND_HALF_UP, localcontext

from app.lib.strategy_engine.etf_benchmark import (
    VERSION,
    _day,
    _hash,
    _number,
    _object,
    _validate,
)

ADAPTER_VERSION = "etf-source-v1"


def _date(value):
    if isinstance(value, str) and len(value) == 8 and value.isdigit():
        value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return _day(value, "source date")


def _rows(value, name):
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return value


def adapt_etf_source(payload):
    """Return native replay input and full-source provenance, not certification."""
    with localcontext(Context(prec=40, rounding=ROUND_HALF_UP)):
        return _adapt(payload)


def _adapt(payload):
    fields = {
        "schema_version",
        "configuration",
        "start_date",
        "end_date",
        "calendar",
        "daily",
        "execution",
    }
    _object(payload, "source", fields, fields)
    if payload["schema_version"] != ADAPTER_VERSION:
        raise ValueError("unsupported source schema_version")
    config_fields = {
        "instrument",
        "initial_cash",
        "allocation",
        "fees",
        "corporate_actions",
    }
    config = _object(
        payload["configuration"],
        "configuration",
        config_fields,
        config_fields - {"initial_cash"},
    )
    inst = _object(
        config["instrument"],
        "instrument",
        {"code", "type", "tick_size"},
        {"code", "type", "tick_size"},
    )
    code = inst["code"]
    if not isinstance(code, str) or len(code) != 8 or code[:2] not in {"sh", "sz"}:
        raise ValueError("invalid instrument code")
    ts_code = code[2:] + (".SH" if code.startswith("sh") else ".SZ")
    exchange = "SSE" if code.startswith("sh") else "SZSE"
    start, end = _date(payload["start_date"]), _date(payload["end_date"])
    if start >= end:
        raise ValueError("source start_date must precede end_date")
    calendar = {}
    for row in _rows(payload["calendar"], "calendar"):
        _object(
            row,
            "calendar row",
            {"exchange", "cal_date", "is_open", "pretrade_date"},
            {"exchange", "cal_date", "is_open"},
        )
        date = _date(row["cal_date"])
        flag = row["is_open"]
        if type(flag) not in (str, int) or flag not in (0, 1, "0", "1"):
            raise ValueError("calendar is_open must be 0 or 1")
        if row["exchange"] != exchange or not start <= date <= end or date in calendar:
            raise ValueError("calendar exchange, range or duplicate mismatch")
        calendar[date] = int(flag)
    first, last = datetime.date.fromisoformat(start), datetime.date.fromisoformat(end)
    expected = {
        (first + datetime.timedelta(days=n)).isoformat()
        for n in range((last - first).days + 1)
    }
    if set(calendar) != expected:
        raise ValueError("calendar must cover every natural day in range")
    sessions = sorted(date for date, flag in calendar.items() if flag == 1)
    if len(sessions) < 2 or sessions[0] != start or sessions[-1] != end:
        raise ValueError("source bounds must be open with at least two sessions")

    def index_rows(name, allowed, required):
        indexed = {}
        for row in _rows(payload[name], name):
            _object(row, name + " row", allowed, required)
            date = _date(row["trade_date"])
            if row["ts_code"] != ts_code or date not in sessions or date in indexed:
                raise ValueError(f"{name} code, session or duplicate mismatch")
            indexed[date] = row
        return indexed

    daily = index_rows(
        "daily",
        {
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
        },
        {"ts_code", "trade_date"},
    )
    execution = index_rows(
        "execution",
        {"ts_code", "trade_date", "trade_status", "up_limit"},
        {"ts_code", "trade_date"},
    )
    quotes = []
    for date in sessions:
        source, opening = daily.get(date, {}), execution.get(date, {})
        quote = {
            "date": date,
            "trade_status": opening.get("trade_status"),
            "upper_limit": opening.get("up_limit"),
        }
        for field in ("open", "close"):
            value = source.get(field)
            quote[field] = (
                None if value is None or _number(value, field) == 0 else value
            )
        # Volume is only source audit material; it never authorizes an open fill.
        if source.get("vol") is not None:
            _number(source["vol"], "vol")
        quotes.append(quote)
    native = dict(
        config,
        schema_version=VERSION,
        price_basis="raw",
        decision_date=start,
        sessions=sessions,
        quotes=quotes,
    )
    normal, _ = _validate(native)
    return normal, {
        "adapter_version": ADAPTER_VERSION,
        "source_hash": _hash(payload),
        "status_basis": "operator_opening_declaration",
    }
