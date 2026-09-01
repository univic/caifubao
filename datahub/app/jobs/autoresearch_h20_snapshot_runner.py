"""Read-only, resource-bounded exporter for the H20 research snapshot."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.lib.scoring_engine.components import (
    breakout_or_position_component,
    clamp,
    momentum_component,
    relative_strength_component,
    risk_penalty,
    signal_strength_component,
    trend_alignment_component,
)
from app.lib.scoring_engine.config import (
    DEFAULT_MODEL_VERSION,
    get_effective_horizon_config,
)
from app.lib.scoring_engine.technical_factors import real_relative_strength
from app.lib.utilities.data_capability_helper import is_bse_stock_code

COMPONENT_IDS = (
    "signal_strength",
    "momentum",
    "trend_alignment",
    "breakout_or_position",
    "industry_momentum",
    "relative_strength",
    "real_relative_strength",
    "risk_penalty",
)

# These production components store their numeric signal in normalized_value;
# their raw_value is a list/dict used only as audit evidence.
NON_NUMERIC_RAW_COMPONENTS = {
    "signal_strength",
    "breakout_or_position",
    "industry_momentum",
}


def _raw_component_values(components, penalties):
    values = {}
    for item in components:
        component_id = item.get("id")
        if component_id not in COMPONENT_IDS:
            continue
        if component_id in NON_NUMERIC_RAW_COMPONENTS:
            values[component_id] = item.get(
                "normalized_value", item.get("raw_value", 0.0)
            )
        else:
            values[component_id] = item.get("raw_value", 0.0)
    for item in penalties:
        if item.get("id") == "risk_penalty":
            values["risk_penalty"] = item.get(
                "normalized_value", item.get("raw_value", 0.0)
            )
    return values


def _normalize_quote_prices(row):
    return {
        **row,
        "close_hfq": row.get("close_hfq"),
        "close": row.get("close"),
        "high_hfq": row.get("high_hfq"),
        "high": row.get("high"),
        "low_hfq": row.get("low_hfq"),
        "low": row.get("low"),
    }


def _pure_component_values(
    code,
    scoring_date,
    quote,
    history,
    factor,
    signals,
    index_quotes,
    industry_metric,
    config,
):
    """Build production-equivalent raw components without database queries."""
    quote_obj = SimpleNamespace(**_normalize_quote_prices(quote))
    history_objects = [
        SimpleNamespace(**_normalize_quote_prices(row)) for row in reversed(history)
    ]
    signal_objects = [SimpleNamespace(**row) for row in signals]
    bullish_today = any(
        item.direction == "BULLISH"
        and item.signal_name
        and _day(item.date) == scoring_date
        for item in signal_objects
    )
    days_since = None
    prior_strengths = prior_names = None
    if not bullish_today:
        prior = [
            item
            for item in signal_objects
            if item.direction == "BULLISH"
            and item.signal_name
            and _day(item.date) < scoring_date
            and (scoring_date - _day(item.date)).days <= config["signal_decay_max_days"]
        ]
        if prior:
            latest = max(_day(item.date) for item in prior)
            latest_items = [item for item in prior if _day(item.date) == latest]
            days_since = (scoring_date - latest).days
            prior_strengths = [float(item.strength or 1.0) for item in latest_items]
            prior_names = [item.signal_name for item in latest_items]
    current_signals = [
        item for item in signal_objects if _day(item.date) == scoring_date
    ]
    components = [
        signal_strength_component(
            current_signals,
            config["weights"]["signal_strength"],
            days_since_signal=days_since,
            last_signal_strengths=prior_strengths,
            last_signal_names=prior_names,
            decay_factor=config["signal_decay_factor"],
        ),
        trend_alignment_component(
            quote_obj,
            SimpleNamespace(**factor) if factor else None,
            20,
            config["weights"]["trend_alignment"],
        ),
        momentum_component(
            quote_obj,
            history_objects[: config["momentum_lookback"]],
            config["momentum_lookback"],
            config["weights"]["momentum"],
        ),
        breakout_or_position_component(
            quote_obj,
            history_objects[: config["breakout_lookback"]],
            config["weights"]["breakout_or_position"],
        ),
        relative_strength_component(
            quote_obj,
            history_objects[: config["momentum_lookback"]],
            config["weights"]["relative_strength"],
        ),
    ]
    stock_quotes = sorted([*history_objects, quote_obj], key=lambda item: item.date)
    index_objects = [
        SimpleNamespace(
            **{
                **row,
                "close_hfq": row.get("close_hfq"),
                "close": row.get("close"),
            }
        )
        for row in index_quotes
    ]
    alpha = real_relative_strength(
        stock_quotes, index_objects, config["momentum_lookback"]
    ).get(quote_obj.date.isoformat())
    if alpha is None:
        real_rs = relative_strength_component(
            quote_obj,
            history_objects[: config["momentum_lookback"]],
            config["weights"]["real_relative_strength"],
        )
        real_rs["id"] = "real_relative_strength"
    else:
        real_rs = {
            "id": "real_relative_strength",
            "raw_value": round(alpha, 6),
            "weight": config["weights"]["real_relative_strength"],
        }
    industry_raw = None
    if industry_metric and industry_metric.get("stock_count", 0) >= 3:
        industry_raw = {
            "industry": industry_metric.get("industry_name"),
            "avg_score": industry_metric["avg_score"],
            "stock_count": industry_metric["stock_count"],
        }
    industry_normalized = (
        clamp(float(industry_raw["avg_score"]) / 100.0) if industry_raw else 0.5
    )
    components.extend(
        [
            {
                "id": "industry_momentum",
                "raw_value": industry_raw,
                "normalized_value": industry_normalized,
                "weight": config["weights"]["industry_momentum"],
            },
            real_rs,
        ]
    )
    penalty = risk_penalty(
        quote_obj,
        history_objects[: config["risk_lookback"]],
        config["weights"]["risk_penalty"],
    )
    return _raw_component_values(components, [penalty])


def _day(value) -> pd.Timestamp:
    return pd.Timestamp(value).normalize().tz_localize(None)


def _records(rows: Iterable) -> list[dict]:
    result = []
    for row in rows:
        if isinstance(row, Mapping):
            result.append(dict(row))
        elif hasattr(row, "to_mongo"):
            result.append(dict(row.to_mongo().to_dict()))
        else:
            result.append(vars(row))
    return result


def _plain_rows(queryset):
    """Materialize projected Mongo rows without heavyweight Document objects."""
    return [dict(row) for row in queryset.as_pymongo()]


def build_trade_calendar(rows: Iterable) -> list[pd.Timestamp]:
    dates = set()
    for row in _records(rows):
        status = row.get("trade_status", 1)
        if status == 1 and row.get("date") is not None:
            dates.add(_day(row["date"]))
    return sorted(dates)


def build_date_batch(start, end, batch_days: int) -> list[list[pd.Timestamp]]:
    if batch_days < 1 or batch_days > 20:
        raise ValueError("batch_trading_days must be between 1 and 20")
    if isinstance(start, (str, dt.date, dt.datetime, pd.Timestamp)):
        lower, upper = _day(start), _day(end)
        dates = list(pd.bdate_range(lower, upper))
    else:
        dates = [_day(value) for value in start]
        if isinstance(end, (tuple, list)):
            lower, upper = _day(end[0]), _day(end[1])
        else:
            lower, upper = dates[0], _day(end)
    selected = [value for value in dates if lower <= value <= upper]
    return [
        selected[index : index + batch_days]
        for index in range(0, len(selected), batch_days)
    ]


def _executable(row: dict, side: str) -> bool:
    if (
        row.get("trade_status", 1) != 1
        or not row.get("open_hfq")
        or row["open_hfq"] <= 0
    ):
        return False
    change = row.get("change_rate")
    if change is None:
        return True
    return change < 9.9 if side == "BUY" else change > -9.9


def _resolve(quotes: list[dict], start_index: int, side: str):
    blocked = 0
    for row in quotes[start_index:]:
        if _executable(row, side):
            return row, blocked
        blocked += 1
    return None, blocked


def _eligibility_reason(row: dict) -> str:
    checks = (
        (row["is_bse"], "bse"),
        (row["is_st"], "st"),
        (row["listing_days"] < 60, "listing_age_below_60"),
        (row["trade_status"] != 1, "scoring_session_not_tradable"),
        (
            any(
                not row.get(name) or row[name] <= 0
                for name in ("open_hfq", "close_hfq", "high_hfq", "low_hfq")
            ),
            "missing_hfq",
        ),
        (row["actual_entry_date"] is None, "unresolved_entry"),
        (row["actual_exit_date"] is None, "unresolved_exit"),
    )
    return next((reason for failed, reason in checks if failed), "eligible")


def reconstruct_component_rows(
    source, scoring_dates, horizon: int = 20
) -> pd.DataFrame:
    """Reconstruct labels from injected rows; component_builder is scoring-service backed."""
    quotes = _records(source["quotes"])
    stocks = {row["code"]: row for row in _records(source.get("stocks", []))}
    builder = source.get("component_builder")
    by_code: dict[str, list[dict]] = {}
    for quote in quotes:
        quote = dict(quote)
        quote["date"] = _day(quote["date"])
        by_code.setdefault(quote.get("code") or quote.get("stock_code"), []).append(
            quote
        )
    for values in by_code.values():
        values.sort(key=lambda value: value["date"])
    output = []
    for scoring_date in map(_day, scoring_dates):
        supplied_breadth = source.get("market_breadth_by_date", {}).get(scoring_date)
        if supplied_breadth is None:
            breadth_rows = []
            for values in by_code.values():
                current = next(
                    (item for item in values if item["date"] == scoring_date), None
                )
                if current:
                    breadth_rows.append(current)
            above = [
                row["close_hfq"] > row["ma_60"]
                for row in breadth_rows
                if row.get("close_hfq") and row.get("ma_60")
            ]
            breadth = sum(above) / len(above) if above else 0.0
        else:
            breadth = float(supplied_breadth)
        for code, values in sorted(by_code.items()):
            indices = [
                i for i, item in enumerate(values) if item["date"] == scoring_date
            ]
            if not indices:
                continue
            index = indices[0]
            quote = values[index]
            history = values[:index]
            components = (
                builder(code, scoring_date, quote, history)
                if builder
                else quote.get("components", {})
            )
            requested_entry = values[index + 1] if index + 1 < len(values) else None
            entry, entry_blocked = _resolve(values, index + 1, "BUY")
            requested_exit = None
            exit_row = None
            exit_blocked = 0
            if entry is not None:
                entry_index = values.index(entry)
                requested_index = entry_index + horizon
                if requested_index < len(values):
                    requested_exit = values[requested_index]
                    exit_row, exit_blocked = _resolve(values, requested_index, "SELL")
            stock = stocks.get(code, {})
            listing_date = stock.get("listing_date") or stock.get("listed_at")
            listing_days = (
                (scoring_date - _day(listing_date)).days
                if listing_date
                else int(stock.get("listing_days", 9999))
            )
            row = {
                "date": scoring_date,
                "stock_code": code,
                "is_bse": bool(stock.get("is_bse", is_bse_stock_code(code))),
                "is_st": bool(quote.get("is_st", quote.get("isST", 0))),
                "listing_days": listing_days,
                "trade_status": quote.get("trade_status", 1),
                **{
                    name: quote.get(name)
                    for name in ("open_hfq", "close_hfq", "high_hfq", "low_hfq")
                },
                "requested_entry_date": requested_entry["date"]
                if requested_entry
                else None,
                "actual_entry_date": entry["date"] if entry else None,
                "actual_entry_open_hfq": entry.get("open_hfq") if entry else None,
                "entry_blocked_sessions": entry_blocked,
                "requested_exit_date": requested_exit["date"]
                if requested_exit
                else None,
                "actual_exit_date": exit_row["date"] if exit_row else None,
                "actual_exit_open_hfq": exit_row.get("open_hfq") if exit_row else None,
                "exit_blocked_sessions": exit_blocked,
                "market_fraction_above_ma60": breadth,
                "source_model_version": source.get(
                    "model_version", DEFAULT_MODEL_VERSION
                ),
                "factor_version": source.get("factor_version", "unknown"),
                "signal_version": source.get("signal_version", "unknown"),
            }
            for component in COMPONENT_IDS:
                value = components.get(component)
                row[component] = (
                    float(value)
                    if isinstance(value, (int, float)) and not isinstance(value, bool)
                    else float("nan")
                )
            row["eligibility_reason"] = _eligibility_reason(row)
            row["eligibility"] = row["eligibility_reason"] == "eligible"
            output.append(row)
    return pd.DataFrame(output)


def validate_export(frame: pd.DataFrame, expected_start, expected_end) -> dict:
    required = {
        "date",
        "stock_code",
        "eligibility",
        "eligibility_reason",
        "actual_entry_open_hfq",
        "actual_exit_open_hfq",
        *COMPONENT_IDS,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("export missing columns: " + ", ".join(missing))
    if {"next_open_hfq", "exit_open_hfq"}.intersection(frame.columns):
        raise ValueError("legacy execution labels are forbidden")
    if frame.duplicated(["date", "stock_code"]).any():
        raise ValueError("duplicate (date, stock_code) rows")
    dates = pd.to_datetime(frame["date"])
    if (
        dates.empty
        or dates.min().normalize() != _day(expected_start)
        or dates.max().normalize() != _day(expected_end)
    ):
        raise ValueError(
            "export date coverage does not match requested completed trading dates"
        )
    return {
        "row_count": len(frame),
        "eligible_count": int(frame["eligibility"].sum()),
        "date_min": dates.min().date().isoformat(),
        "date_max": dates.max().date().isoformat(),
    }


def _stable_export_types(frame):
    frame = frame.copy()
    for column in (
        "date",
        "requested_entry_date",
        "actual_entry_date",
        "requested_exit_date",
        "actual_exit_date",
    ):
        frame[column] = pd.to_datetime(frame[column])
    for column in (
        "open_hfq",
        "close_hfq",
        "high_hfq",
        "low_hfq",
        "actual_entry_open_hfq",
        "actual_exit_open_hfq",
        "market_fraction_above_ma60",
        *COMPONENT_IDS,
    ):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)
    return frame


class _MongoBatchSource:
    """Yield bounded Mongo query results for one scoring/code batch at a time."""

    def __init__(self, from_date, to_date, code_batch_size=50):
        if not 1 <= code_batch_size <= 200:
            raise ValueError("code_batch_size must be between 1 and 200")
        self.from_date = _day(from_date)
        self.to_date = _day(to_date)
        self.code_batch_size = code_batch_size
        self._listing_dates = None

    def calendar(self):
        from app.model.stock import FinanceMarket

        market = (
            FinanceMarket.objects(name="ChinaAStock").only("trade_calendar").first()
        )
        if not market:
            return []
        return [
            _day(value)
            for value in market.trade_calendar
            if self.from_date <= _day(value) <= self.to_date
        ]

    def iter_code_sources(self, scoring_dates, horizon):
        from app.model.factor import StockFactorDaily
        from app.model.industry import IndustryDailyMetrics, StockIndustryClassification
        from app.model.signal import StockSignalDaily
        from app.model.stock import IndividualStock, StockDailyQuote

        config = get_effective_horizon_config(horizon)
        if self._listing_dates is None:
            collection = StockDailyQuote._get_collection()
            self._listing_dates = {
                row["_id"]: row["listing_date"]
                for row in collection.aggregate(
                    [
                        {"$group": {"_id": "$code", "listing_date": {"$min": "$date"}}},
                        {"$project": {"_id": 1, "listing_date": 1}},
                    ],
                    allowDiskUse=True,
                )
            }
        breadth_counts = {date: [0, 0] for date in scoring_dates}
        for date_batch in build_date_batch(
            scoring_dates, (scoring_dates[0], scoring_dates[-1]), 20
        ):
            batch_datetimes = [date.to_pydatetime() for date in date_batch]
            breadth_factors = {
                (item["stock_code"], _day(item["date"])): item.get("ma_60")
                for item in _plain_rows(
                    StockFactorDaily.objects(date__in=batch_datetimes).only(
                        "stock_code", "date", "ma_60"
                    )
                )
            }
            breadth_quotes = _plain_rows(
                StockDailyQuote.objects(date__in=batch_datetimes).only(
                    "code", "date", "close_hfq"
                )
            )
            for quote in breadth_quotes:
                date = _day(quote["date"])
                ma_60 = breadth_factors.get((quote["code"], date))
                close_hfq = quote.get("close_hfq")
                if close_hfq is not None and ma_60 is not None:
                    breadth_counts[date][1] += 1
                    breadth_counts[date][0] += int(close_hfq > ma_60)
        market_breadth = {
            date: above / total if total else 0.0
            for date, (above, total) in breadth_counts.items()
        }
        scoring_start, scoring_end = min(scoring_dates), max(scoring_dates)
        scoring_datetimes = [date.to_pydatetime() for date in scoring_dates]
        # Historical membership comes from quotes that actually existed on the
        # scoring dates. Current IndividualStock.active_status would introduce
        # survivorship bias by excluding names delisted after that date.
        codes = sorted(
            StockDailyQuote.objects(date__in=scoring_datetimes).distinct("code")
        )
        query_start = (scoring_start - dt.timedelta(days=200)).to_pydatetime()
        query_end = (scoring_end + dt.timedelta(days=120)).to_pydatetime()
        index_quotes = _plain_rows(
            StockDailyQuote.objects(
                code="sh000300",
                date__gte=query_start,
                date__lte=scoring_end.to_pydatetime(),
            )
            .only("code", "date", "close_hfq", "close")
            .order_by("date")
        )
        index_quotes = [
            {
                **row,
                "close_hfq": row.get("close_hfq"),
                "close": row.get("close"),
            }
            for row in index_quotes
        ]
        for offset in range(0, len(codes), self.code_batch_size):
            code_batch = codes[offset : offset + self.code_batch_size]
            metadata = {
                stock["code"]: stock
                for stock in _plain_rows(
                    IndividualStock.objects(code__in=code_batch).only("code", "name")
                )
            }
            stocks = [
                {
                    "code": code,
                    "name": metadata.get(code, {}).get("name"),
                    "listing_date": self._listing_dates.get(code),
                }
                for code in code_batch
            ]
            quotes = _plain_rows(
                StockDailyQuote.objects(
                    code__in=code_batch, date__gte=query_start, date__lte=query_end
                )
                .only(
                    "code",
                    "date",
                    "open_hfq",
                    "close",
                    "close_hfq",
                    "high",
                    "high_hfq",
                    "low",
                    "low_hfq",
                    "change_rate",
                    "trade_status",
                    "isST",
                )
                .order_by("code", "date")
            )
            factors = {
                (item["stock_code"], _day(item["date"])): {
                    "ma_10": item.get("ma_10"),
                    "ma_20": item.get("ma_20"),
                    "ma_30": item.get("ma_30"),
                    "ma_60": item.get("ma_60"),
                    "ma_120": item.get("ma_120"),
                }
                for item in _plain_rows(
                    StockFactorDaily.objects(
                        stock_code__in=code_batch,
                        date__gte=query_start,
                        date__lte=scoring_end.to_pydatetime(),
                    ).only(
                        "stock_code",
                        "date",
                        "ma_10",
                        "ma_20",
                        "ma_30",
                        "ma_60",
                        "ma_120",
                    )
                )
            }
            signals: dict[str, list[dict]] = {}
            signal_rows = _plain_rows(
                StockSignalDaily.objects(
                    stock_code__in=code_batch,
                    date__gte=query_start,
                    date__lte=scoring_end.to_pydatetime(),
                ).only(
                    "stock_code",
                    "date",
                    "signal_name",
                    "signal_version",
                    "direction",
                    "strength",
                )
            )
            for item in signal_rows:
                signals.setdefault(item["stock_code"], []).append(
                    {
                        "date": _day(item["date"]),
                        "signal_name": item.get("signal_name"),
                        "direction": item.get("direction"),
                        "strength": item.get("strength"),
                    }
                )
            industries = {
                item["stock_code"]: {
                    "code": item.get("industry_code_sw_l1"),
                    "name": item.get("industry_name_sw_l1"),
                }
                for item in _plain_rows(
                    StockIndustryClassification.objects(stock_code__in=code_batch).only(
                        "stock_code", "industry_code_sw_l1", "industry_name_sw_l1"
                    )
                )
                if item.get("industry_code_sw_l1")
            }
            industry_codes = sorted({item["code"] for item in industries.values()})
            metric_rows = _plain_rows(
                IndustryDailyMetrics.objects(
                    industry_code__in=industry_codes,
                    date__lt=scoring_end.to_pydatetime() + dt.timedelta(days=1),
                    horizon=horizon,
                    model_version=DEFAULT_MODEL_VERSION,
                ).only(
                    "industry_code", "industry_name", "date", "avg_score", "stock_count"
                )
            )

            def prior_industry_metric(
                code, date, industry_map=industries, metrics=metric_rows
            ):
                industry = industry_map.get(code)
                if not industry:
                    return None
                prior = [
                    item
                    for item in metrics
                    if item["industry_code"] == industry["code"]
                    and _day(item["date"]) < date
                ]
                if not prior:
                    return None
                latest = max(prior, key=lambda item: item["date"])
                return {
                    "industry_name": industry["name"],
                    "avg_score": latest.get("avg_score"),
                    "stock_count": latest.get("stock_count"),
                }

            def component_builder(
                code, date, quote, history, factor_map=factors, signal_map=signals
            ):
                return _pure_component_values(
                    code,
                    date,
                    quote,
                    history,
                    factor_map.get((code, date)),
                    signal_map.get(code, []),
                    index_quotes,
                    prior_industry_metric(code, date),
                    config,
                )

            yield {
                "quotes": quotes,
                "stocks": stocks,
                "component_builder": component_builder,
                "market_breadth_by_date": market_breadth,
                "signal_version": "v1",
                "factor_version": "wide-daily-v1",
            }

    # Compatibility for parity and injected callers that request a single date slice.
    def iter_sources(self, scoring_dates, horizon):
        yield from self.iter_code_sources(scoring_dates, horizon)


def _database_source(from_date, to_date):
    from mongoengine import connect

    from app.conf import app_config

    connect(
        db=app_config.MONGODB_DB,
        host=app_config.MONGODB_HOST,
        port=app_config.MONGODB_PORT,
        username=app_config.MONGODB_USERNAME,
        password=app_config.MONGODB_PASSWORD,
        authentication_source="admin",
    )
    return _MongoBatchSource(from_date, to_date)


def export_snapshot(
    source, from_date, to_date, horizon, batch_days, output: Path, dry_run=False
):
    calendar = (
        source.calendar()
        if hasattr(source, "calendar")
        else build_trade_calendar(source.get("calendar") or source["quotes"])
    )
    batches = build_date_batch(calendar, (from_date, to_date), batch_days)
    if not batches:
        raise ValueError("requested range contains no completed trading sessions")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=output.parent, suffix=".parquet")
    os.close(fd)
    writer = None
    seen = set()
    row_count = eligible_count = 0
    date_min = date_max = None

    def write_frame(frame):
        nonlocal writer, row_count, eligible_count, date_min, date_max
        if frame.empty:
            return
        frame = _stable_export_types(frame)
        validate_export(frame, frame["date"].min(), frame["date"].max())
        keys = set(zip(frame["date"].map(_day), frame["stock_code"], strict=True))
        if seen.intersection(keys):
            raise ValueError("duplicate (date, stock_code) rows")
        seen.update(keys)
        dates = pd.to_datetime(frame["date"])
        date_min = dates.min() if date_min is None else min(date_min, dates.min())
        date_max = dates.max() if date_max is None else max(date_max, dates.max())
        row_count += len(frame)
        eligible_count += int(frame["eligibility"].sum())
        if not dry_run:
            table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(temporary, table.schema)
            writer.write_table(table)

    try:
        if hasattr(source, "iter_code_sources"):
            scoring_dates = [date for batch in batches for date in batch]
            for code_source in source.iter_code_sources(scoring_dates, horizon):
                for batch in batches:
                    write_frame(reconstruct_component_rows(code_source, batch, horizon))
        else:
            for batch in batches:
                sources = (
                    source.iter_sources(batch, horizon)
                    if hasattr(source, "iter_sources")
                    else (source,)
                )
                for batch_source in sources:
                    write_frame(
                        reconstruct_component_rows(batch_source, batch, horizon)
                    )
        if writer is not None:
            writer.close()
            writer = None
        if (
            row_count == 0
            or _day(date_min) != batches[0][0]
            or _day(date_max) != batches[-1][-1]
        ):
            raise ValueError(
                "export date coverage does not match requested completed trading dates"
            )
        summary = {
            "row_count": row_count,
            "eligible_count": eligible_count,
            "date_min": _day(date_min).date().isoformat(),
            "date_max": _day(date_max).date().isoformat(),
        }
        if dry_run:
            return {"output_path": str(output.resolve()), **summary, "sha256": None}
        digest = hashlib.sha256(Path(temporary).read_bytes()).hexdigest()
        os.replace(temporary, output)
    finally:
        if writer is not None:
            writer.close()
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {"output_path": str(output.resolve()), **summary, "sha256": digest}


def _compare_component_maps(vectorized, production, tolerance):
    mismatches = 0
    maximum_error = 0.0
    for component in COMPONENT_IDS:
        if component not in vectorized or component not in production:
            mismatches += 1
            continue
        left, right = vectorized[component], production[component]
        numeric_left = isinstance(left, (int, float)) and not isinstance(left, bool)
        numeric_right = isinstance(right, (int, float)) and not isinstance(right, bool)
        if numeric_left and numeric_right:
            if not math.isfinite(float(left)) or not math.isfinite(float(right)):
                mismatches += 1
                continue
            error = abs(float(left) - float(right))
            maximum_error = max(maximum_error, error)
            mismatches += int(error > tolerance)
        elif type(left) is not type(right) or (left is None) != (right is None):
            mismatches += 1
    return mismatches, maximum_error


def parity_check(source, from_date, to_date, horizon, sample_size, tolerance):
    from app.lib.scoring_engine.scoring_service import StockScoringService
    from app.model.stock import IndividualStock

    dates = [
        date for date in source.calendar() if _day(from_date) <= date <= _day(to_date)
    ]
    service = StockScoringService()
    compared_rows = compared_components = mismatch_count = 0
    maximum_error = 0.0
    sample_keys = []
    for batch_source in source.iter_sources(dates[:20], horizon):
        quotes = _records(batch_source["quotes"])
        by_code = {}
        for quote in quotes:
            quote["date"] = _day(quote["date"])
            by_code.setdefault(quote["code"], []).append(quote)
        for code in sorted(by_code):
            stock = IndividualStock.objects(code=code).first()
            if stock is None:
                continue
            values = sorted(by_code[code], key=lambda row: row["date"])
            for date in dates:
                current = next((row for row in values if row["date"] == date), None)
                if current is None:
                    continue
                history = [row for row in values if row["date"] < date]
                vectorized = batch_source["component_builder"](
                    code, date, current, history
                )
                config = service._get_horizon_config(horizon)
                quote = service._get_quote_on_date(stock.code, date.to_pydatetime())
                if quote is None:
                    continue
                factors = service._get_factor_on_date(stock.code, date.to_pydatetime())
                signals = service._get_signals_on_date(stock.code, date.to_pydatetime())
                history_quotes = service._get_previous_quotes(
                    stock.code,
                    date.to_pydatetime(),
                    max(
                        config["minimum_quote_count"],
                        config["breakout_lookback"],
                        config["risk_lookback"],
                    ),
                )
                components, penalties = service._build_components(
                    quote,
                    factors,
                    signals,
                    history_quotes,
                    date.to_pydatetime(),
                    horizon,
                    config,
                    stock.code,
                )
                production = _raw_component_values(components, penalties)
                mismatches, error = _compare_component_maps(
                    vectorized, production, tolerance
                )
                mismatch_count += mismatches
                maximum_error = max(maximum_error, error)
                compared_rows += 1
                compared_components += len(COMPONENT_IDS)
                sample_keys.append(f"{date.date().isoformat()}:{code}")
                if compared_rows >= sample_size:
                    break
            if compared_rows >= sample_size:
                break
        if compared_rows >= sample_size:
            break
    digest = hashlib.sha256("\n".join(sample_keys).encode()).hexdigest()
    return {
        "compared_rows": compared_rows,
        "compared_components": compared_components,
        "mismatch_count": mismatch_count,
        "maximum_absolute_error": maximum_error,
        "sample_sha256": digest,
    }


def build_parser():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export")
    export.add_argument("--from-date", required=True)
    export.add_argument("--to-date", required=True)
    export.add_argument("--horizon", type=int, default=20)
    export.add_argument("--batch-trading-days", type=int, default=20)
    export.add_argument("--output", required=True)
    export.add_argument("--dry-run", action="store_true")
    parity = commands.add_parser("parity")
    parity.add_argument("--from-date", required=True)
    parity.add_argument("--to-date", required=True)
    parity.add_argument("--horizon", type=int, default=20)
    parity.add_argument("--sample-size", type=int, default=50)
    parity.add_argument("--tolerance", type=float, default=0.000001)
    return parser


def main(argv=None, source=None):
    args = build_parser().parse_args(argv)
    source = source or _database_source(args.from_date, args.to_date)
    if args.command == "parity":
        result = parity_check(
            source,
            args.from_date,
            args.to_date,
            args.horizon,
            args.sample_size,
            args.tolerance,
        )
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return int(result["compared_rows"] == 0 or result["mismatch_count"] > 0)
    result = export_snapshot(
        source,
        args.from_date,
        args.to_date,
        args.horizon,
        args.batch_trading_days,
        Path(args.output),
        args.dry_run,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
