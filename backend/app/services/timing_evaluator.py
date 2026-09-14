"""Pure, research-only stock timing evaluation helpers.

This module deliberately accepts plain Python values and never imports a model,
database connection, Flask application, or persistence helper.  The database
and runner layers can inject score/quote evidence and a per-stock backtest
callable; the functions here only construct deterministic evidence records and
aggregate the returned rows.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

SCHEMA_VERSION = "1.0"
RESEARCH_STATUS = "UNVALIDATED"
RESEARCH_ONLY = True
DELISTED_COMPLETENESS_VALUES = frozenset({"VERIFIED", "NOT_VERIFIED", "UNKNOWN"})
USABLE_SCORE_STATUSES = frozenset(
    {"PENDING", "TRACKING", "VERIFIED", "INSUFFICIENT_DATA"}
)
MIN_REQUESTED_STOCKS = 50
MIN_EVIDENCE_ELIGIBLE = 50
MIN_COVERAGE = 0.90
MIN_OBSERVED_SESSIONS = 120
MIN_COMPLETED_SELLS = 5
SESSION_OPEN_UTC = time(1, 30)  # 09:30 China Standard Time
SESSION_CLOSE_UTC = time(7, 0)  # 15:00 China Standard Time


def _jsonable(value: Any) -> Any:
    """Convert supported values to deterministic JSON-compatible values."""
    if isinstance(value, Mapping):
        return {
            str(key): _jsonable(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable(item) for item in value), key=repr)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("canonical JSON does not accept NaN or infinite floats")
    return value


def canonical_json_sha256(value: Any) -> str:
    """Return the SHA-256 of canonical JSON for an identity or configuration."""
    payload = json.dumps(
        _jsonable(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def canonical_cohort(cohort: Iterable[str]) -> list[str]:
    """Normalize a caller-supplied cohort without inventing membership."""
    if isinstance(cohort, (str, bytes)):
        raise TypeError("cohort must be an iterable of stock codes")
    try:
        values = list(cohort)
    except TypeError as exc:
        raise TypeError("cohort must be an iterable of stock codes") from exc
    codes = {str(code).strip() for code in values}
    if "" in codes:
        raise ValueError("cohort contains an empty stock code")
    if not codes:
        raise ValueError("cohort must contain at least one stock code")
    return sorted(codes)


def _utc_timestamp(value: Any, field: str, *, required: bool = True) -> datetime | None:
    if value is None:
        if required:
            raise ValueError(f"{field} must be supplied")
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO timestamp") from exc
    else:
        raise TypeError(f"{field} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _rfc3339_utc(value: Any, field: str) -> str:
    """Normalize an aware timestamp to the canonical UTC ``Z`` spelling."""
    if isinstance(value, str) and "T" not in value:
        raise ValueError(f"{field} must be a UTC RFC3339 timestamp")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} must be a UTC RFC3339 timestamp") from exc
    else:
        raise TypeError(f"{field} must be a UTC RFC3339 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    if parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must use the UTC offset")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _session_date(value: Any, field: str = "date") -> date:
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value.astimezone(UTC).date()
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            if "T" in text or " " in text:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                    parsed = parsed.astimezone(UTC)
                return parsed.date()
            return date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO date") from exc
    raise ValueError(f"{field} must be a date")


def _calendar_dates(calendar: Sequence[Any] | Iterable[Any]) -> list[date]:
    if calendar is None:
        raise ValueError("trading calendar is required")
    try:
        days = sorted({_session_date(item, "calendar date") for item in calendar})
    except TypeError as exc:
        raise ValueError("trading calendar must be iterable") from exc
    if not days:
        raise ValueError("trading calendar is empty")
    return days


def _as_percentile(value: Any, field: str = "percentile") -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a finite number in [0, 1]")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number in [0, 1]") from exc
    if not math.isfinite(result) or not 0.0 <= result <= 1.0:
        raise ValueError(f"{field} must be a finite number in [0, 1]")
    return result


def _threshold(config: Mapping[str, Any], name: str) -> float:
    value = config.get(name)
    if value is None:
        value = config.get(name.replace("_percentile", "_threshold"))
    if value is None:
        raise ValueError(f"config requires {name}")
    return _as_percentile(value, name)


def _fresh(value: Any) -> bool:
    return isinstance(value, str) and value == "FRESH"


def _position_state(value: Any) -> str:
    if isinstance(value, Mapping):
        value = value.get("state", value.get("position_state"))
    state = str(value or "").strip().upper()
    if state not in {"FLAT", "HELD"}:
        raise ValueError("position_state must be FLAT or HELD")
    return state


def _iso_timestamp(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _resolve_expiry(
    *,
    signal_day: date,
    execution_day: date,
    days: list[date],
    config: Mapping[str, Any],
    explicit: Any,
) -> datetime:
    expected = datetime.combine(execution_day, SESSION_CLOSE_UTC, tzinfo=UTC)
    if explicit is not None:
        expires = _utc_timestamp(explicit, "expires_at")
        assert expires is not None
        if expires != expected:
            raise ValueError("expires_at must equal the execution session close")
        return expires

    raw_sessions = config.get("expiry_sessions", 1)
    try:
        expiry_sessions = int(raw_sessions)
    except (TypeError, ValueError) as exc:
        raise ValueError("expiry_sessions must be a positive integer") from exc
    if expiry_sessions < 1:
        raise ValueError("expiry_sessions must be a positive integer")
    try:
        signal_index = days.index(signal_day)
        expiry_day = days[signal_index + expiry_sessions]
    except (ValueError, IndexError) as exc:
        raise ValueError("trading calendar does not cover expires_at") from exc
    if expiry_day != execution_day:
        raise ValueError("expires_at must equal the execution session close")
    return expected


def build_timing_decision(
    evidence: Mapping[str, Any],
    *,
    position_state: str | Mapping[str, Any],
    config: Mapping[str, Any],
    calendar: Sequence[Any] | Iterable[Any],
    execution_date: Any = None,
    expires_at: Any = None,
) -> dict[str, Any]:
    """Construct one deterministic position-aware timing evidence record.

    ``evidence`` must contain a cross-sectional ``percentile``.  Absolute
    scores are intentionally ignored: entry and exit are percentile-only.  All
    timestamps in the artifact are normalized to UTC.  Missing/stale evidence
    produces ``NO_TRADE``; malformed causal inputs fail closed with
    ``ValueError`` before an artifact is emitted.
    """
    if not isinstance(evidence, Mapping):
        raise TypeError("evidence must be a mapping")
    if not isinstance(config, Mapping):
        raise TypeError("config must be a mapping")
    config_copy = _jsonable(config)
    assert isinstance(config_copy, dict)

    stock_code = str(evidence.get("stock_code", "")).strip()
    if not stock_code:
        raise ValueError("evidence requires stock_code")
    signal_day = _session_date(evidence.get("signal_date"), "signal_date")
    decision_at = _utc_timestamp(evidence.get("decision_at"), "decision_at")
    assert decision_at is not None
    signal_close_at = _utc_timestamp(evidence.get("signal_close_at"), "signal_close_at")
    assert signal_close_at is not None
    expected_signal_close = datetime.combine(signal_day, SESSION_CLOSE_UTC, tzinfo=UTC)
    if signal_close_at != expected_signal_close:
        raise ValueError("signal_close_at must equal the signal session close")

    days = _calendar_dates(calendar)
    if signal_day not in days:
        raise ValueError("signal date is not in the trading calendar")
    signal_index = days.index(signal_day)
    if signal_index + 1 >= len(days):
        raise ValueError("calendar does not cover the next trading session")
    next_day = days[signal_index + 1]
    if execution_date is not None:
        requested_execution = _session_date(execution_date, "execution_date")
        if requested_execution != next_day:
            raise ValueError("execution_date must be the next trading session")
    execution_at = datetime.combine(next_day, SESSION_OPEN_UTC, tzinfo=UTC)
    if decision_at < signal_close_at:
        raise ValueError("decision_at must be at or after signal close")
    if decision_at >= execution_at:
        raise ValueError("decision_at must be before the next session open")

    state = _position_state(position_state)
    entry = _threshold(config, "entry_percentile")
    exit_ = _threshold(config, "exit_percentile")
    if entry < exit_:
        raise ValueError("entry_percentile must be >= exit_percentile")
    expiry = _resolve_expiry(
        signal_day=signal_day,
        execution_day=next_day,
        days=days,
        config=config,
        explicit=expires_at if expires_at is not None else evidence.get("expires_at"),
    )
    if expiry <= decision_at:
        raise ValueError("expires_at must be after decision_at")

    raw_data_as_of = evidence.get("data_as_of")
    data_as_of = _utc_timestamp(raw_data_as_of, "data_as_of", required=False)
    freshness = evidence.get("freshness")
    reasons: list[str] = []
    raw_score_status = evidence.get("score_status", evidence.get("status"))
    score_status = (
        raw_score_status.strip().upper() if isinstance(raw_score_status, str) else None
    )
    if score_status not in USABLE_SCORE_STATUSES:
        reasons.append("unusable_score_status")
    raw_percentile = evidence.get("percentile")
    percentile: float | None = None
    if raw_percentile is None:
        reasons.append("missing_percentile")
    else:
        try:
            percentile = _as_percentile(raw_percentile)
        except (TypeError, ValueError):
            reasons.append("invalid_percentile")
    if not _fresh(freshness):
        reasons.append(
            "stale_evidence" if freshness is not None else "missing_freshness"
        )
    if data_as_of is None:
        reasons.append("missing_data_as_of")
    elif data_as_of > decision_at:
        reasons.append("future_data_as_of")

    model_version = str(evidence.get("model_version", "")).strip()
    if not model_version:
        reasons.append("missing_model_version")
    horizon = evidence.get("horizon")
    if horizon is None:
        reasons.append("missing_horizon")
    evidence_kind = str(evidence.get("evidence_kind", "REPLAY")).strip().upper()
    if not evidence_kind:
        evidence_kind = "REPLAY"

    if reasons:
        action = "NO_TRADE"
    elif state == "FLAT":
        if percentile >= entry:  # type: ignore[operator]
            action = "ENTER"
            reasons = ["entry_percentile"]
        else:
            action = "NO_TRADE"
            reasons = ["entry_not_met"]
    elif percentile <= exit_:  # type: ignore[operator]
        action = "EXIT"
        reasons = ["exit_percentile"]
    else:
        action = "HOLD"
        reasons = ["exit_not_met"]

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": RESEARCH_ONLY,
        "unvalidated": True,
        "status": RESEARCH_STATUS,
        "validation_status": RESEARCH_STATUS,
        "action": action,
        "stock_code": stock_code,
        "signal_date": signal_day.isoformat(),
        "signal_close_at": _iso_timestamp(signal_close_at),
        "decision_at": _iso_timestamp(decision_at),
        "execution_date": next_day.isoformat(),
        "execution_at": _iso_timestamp(execution_at),
        "expires_at": _iso_timestamp(expiry),
        "position_state": state,
        "percentile": percentile,
        "entry_percentile": entry,
        "exit_percentile": exit_,
        "data_as_of": _iso_timestamp(data_as_of),
        "freshness": freshness,
        "score_status": score_status,
        "model_version": model_version or None,
        "horizon": horizon,
        "config_hash": canonical_json_sha256(config_copy),
        "evidence_kind": evidence_kind,
        "reason_tokens": reasons,
        "execution_timing": "next_trading_day_open",
    }


def _window(
    window: Any, start_date: Any, end_date: Any
) -> tuple[dict[str, str], date, date]:
    if window is None:
        if start_date is None or end_date is None:
            raise ValueError("window or start_date/end_date is required")
        values = (start_date, end_date)
    elif isinstance(window, Mapping):
        values = (
            window.get("from", window.get("start", window.get("start_date"))),
            window.get("to", window.get("end", window.get("end_date"))),
        )
    else:
        try:
            values = tuple(window)
        except TypeError as exc:
            raise ValueError("window must contain start and end dates") from exc
        if len(values) != 2:
            raise ValueError("window must contain start and end dates")
    if values[0] is None or values[1] is None:
        raise ValueError("window must contain start and end dates")
    start = _session_date(values[0], "window start")
    end = _session_date(values[1], "window end")
    if start > end:
        raise ValueError("window start must be <= window end")
    normalized = {"from": start.isoformat(), "to": end.isoformat()}
    return normalized, start, end


def _delisted_status(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError(
            "delisted_completeness must be one of VERIFIED|NOT_VERIFIED|UNKNOWN"
        )
    status = value
    if status not in DELISTED_COMPLETENESS_VALUES:
        raise ValueError(
            "delisted_completeness must be one of VERIFIED|NOT_VERIFIED|UNKNOWN"
        )
    return status


def _runner_kwargs(
    *,
    code: str,
    config: Mapping[str, Any],
    model_version: str,
    start: date,
    end: date,
    initial_cash: float,
) -> dict[str, Any]:
    kwargs = {
        "stock_code": code,
        "start_date": datetime.combine(start, time.min, tzinfo=UTC),
        "end_date": datetime.combine(end, time.min, tzinfo=UTC),
        "initial_cash": initial_cash,
        "save_result": False,
        "model_version": model_version,
        "config": _jsonable(config),
    }
    # This keeps the adapter compatible with the existing run_backtest shape
    # without making this pure module import that service.
    for key, value in config.items():
        if isinstance(key, str):
            kwargs.setdefault(key, value)
    return kwargs


def _call_runner(runner: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
    """Call an injected runner without masking TypeErrors from its body."""
    try:
        signature = inspect.signature(runner)
    except (TypeError, ValueError):
        return runner(**kwargs)
    parameters = signature.parameters
    accepts_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    if accepts_kwargs:
        if "stock_code" in parameters:
            return runner(**kwargs)
        positional = next(
            (
                parameter
                for parameter in parameters.values()
                if parameter.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ),
            None,
        )
        if positional is not None:
            remaining = dict(kwargs)
            remaining.pop("stock_code", None)
            return runner(kwargs["stock_code"], **remaining)
        return runner(**kwargs)
    accepted = {name: value for name, value in kwargs.items() if name in parameters}
    if "stock_code" in parameters:
        return runner(**accepted)
    accepted.pop("stock_code", None)
    positional = next(
        (
            parameter
            for parameter in parameters.values()
            if parameter.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ),
        None,
    )
    if positional is not None:
        accepted.pop(positional.name, None)
        return runner(kwargs["stock_code"], **accepted)
    return runner(**accepted)


def _number(result: Mapping[str, Any], names: Sequence[str]) -> float | None:
    for name in names:
        value = result.get(name)
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _int_value(result: Mapping[str, Any], names: Sequence[str]) -> int | None:
    value = _number(result, names)
    if value is None or value < 0 or not value.is_integer():
        return None
    return int(value)


def _daily_dates(values: Any) -> set[date] | None:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        return None
    dates = set()
    for item in values:
        if not isinstance(item, Mapping):
            return None
        raw_date = item.get("date", item.get("day"))
        if raw_date is None:
            return None
        equity = _number(item, ("equity",))
        if equity is None or equity < 0:
            return None
        try:
            dates.add(_session_date(raw_date, "daily value date"))
        except (TypeError, ValueError):
            return None
    return dates


def _side_status(side: Mapping[str, Any], name: str) -> tuple[str | None, str | None]:
    raw_status = side.get("status")
    if side.get("error") is not None:
        return f"{name}_side_failed", str(side["error"])
    if raw_status is not None and str(raw_status).upper() not in {
        "SUCCESS",
        "COMPLETED",
        "OK",
    }:
        return f"{name}_side_failed", str(raw_status)
    return None, None


def _explicit_sides(
    result: Mapping[str, Any],
) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
    timing = result.get("timing")
    buy_hold = result.get("buy_hold")
    if isinstance(timing, Mapping) or isinstance(buy_hold, Mapping):
        return (
            timing if isinstance(timing, Mapping) else None,
            buy_hold if isinstance(buy_hold, Mapping) else None,
        )
    # Flat output remains supported only when both names explicitly identify
    # the two sides.  The legacy ``benchmark_return_pct`` is intentionally not
    # accepted as same-stock buy-and-hold evidence.
    if "timing_return_pct" not in result:
        return None, None
    if "buy_hold_return_pct" not in result:
        return result, None
    timing_side = {
        "return_pct": result.get("timing_return_pct"),
        "max_drawdown": result.get("timing_max_drawdown", result.get("max_drawdown")),
        "sharpe_ratio": result.get("timing_sharpe_ratio", result.get("sharpe_ratio")),
        "trades": result.get("timing_trades"),
        "daily_values": result.get("timing_daily_values"),
    }
    buy_hold_side = {
        "return_pct": result.get("buy_hold_return_pct"),
        "daily_values": result.get("buy_hold_daily_values"),
    }
    return timing_side, buy_hold_side


def _comparison_assumptions_error(
    code: str,
    timing_side: Mapping[str, Any],
    buy_hold_side: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> str | None:
    for side_name, side in (("timing", timing_side), ("buy_hold", buy_hold_side)):
        if str(side.get("stock_code", "")).strip() != code:
            return f"{side_name} stock_code does not match the requested stock"
        assumptions = side.get("assumptions")
        if not isinstance(assumptions, Mapping):
            return f"{side_name} assumptions are required"
    timing_assumptions = _jsonable(timing_side["assumptions"])
    buy_hold_assumptions = _jsonable(buy_hold_side["assumptions"])
    if timing_assumptions != buy_hold_assumptions:
        return "timing and buy_hold assumptions differ"
    assert isinstance(timing_assumptions, dict)
    for key in ("initial_cash", "window", "execution_timing", "valuation_timing"):
        if timing_assumptions.get(key) != expected[key]:
            return f"comparison assumption {key} does not match the pool"
    board_lot = timing_assumptions.get("board_lot")
    if isinstance(board_lot, bool) or not isinstance(board_lot, int) or board_lot < 1:
        return "comparison assumption board_lot must be a positive integer"
    friction = timing_assumptions.get("friction")
    if not isinstance(friction, Mapping):
        return "comparison assumption friction must be an object"
    for key in (
        "commission_rate",
        "minimum_commission",
        "stamp_duty_rate",
        "slippage_rate",
    ):
        value = _number(friction, (key,))
        if value is None or value < 0:
            return f"comparison assumption friction.{key} must be non-negative"
    return None


def _completed_sell(trade: Any) -> bool:
    if not isinstance(trade, Mapping):
        return False
    if str(trade.get("side", "")).upper() != "SELL":
        return False
    if str(trade.get("status", "")).upper() != "FILLED":
        return False
    quantity = _number(trade, ("quantity",))
    execution_price = _number(trade, ("exec_price", "execution_price"))
    return bool(quantity and quantity > 0 and execution_price and execution_price > 0)


def _trade_dates(values: Any) -> set[date] | None:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        return None
    dates = set()
    for trade in values:
        if not isinstance(trade, Mapping):
            return None
        raw_date = trade.get("date", trade.get("execution_date"))
        if raw_date is None:
            return None
        try:
            dates.add(_session_date(raw_date, "trade date"))
        except (TypeError, ValueError):
            return None
    return dates


def _normalize_row(
    code: str,
    result: Any,
    expected_assumptions: Mapping[str, Any],
    *,
    start: date,
    end: date,
) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "runner_non_mapping",
            "error": "runner returned a non-mapping",
        }
    if result.get("error") is not None or str(result.get("status", "")).upper() in {
        "FAILED",
        "ERROR",
    }:
        error = result.get("error") or result.get("status") or "runner failed"
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": str(result.get("reason_code") or "runner_error"),
            "error": str(error),
        }

    timing_side, buy_hold_side = _explicit_sides(result)
    if timing_side is None:
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "missing_timing_side",
            "error": "runner must provide an explicit timing side",
        }
    if buy_hold_side is None:
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "missing_buy_hold_side",
            "error": "runner must provide an explicit same-stock buy_hold side",
        }
    assumptions_error = _comparison_assumptions_error(
        code, timing_side, buy_hold_side, expected_assumptions
    )
    if assumptions_error is not None:
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "comparison_assumptions_mismatch",
            "error": assumptions_error,
        }
    if (
        buy_hold_side.get("benchmark_code") is not None
        or str(buy_hold_side.get("benchmark_type", "")).upper() == "INDEX"
    ):
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "buy_hold_not_same_stock",
            "error": "index benchmark cannot substitute for same-stock buy_hold",
        }
    side_errors = []
    timing_error, timing_detail = _side_status(timing_side, "timing")
    buy_hold_error, buy_hold_detail = _side_status(buy_hold_side, "buy_hold")
    if timing_error:
        side_errors.append((timing_error, timing_detail))
    if buy_hold_error:
        side_errors.append((buy_hold_error, buy_hold_detail))
    if side_errors:
        reason_code, detail = side_errors[0]
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": reason_code,
            "error": detail or reason_code,
        }

    timing = _number(
        timing_side, ("return_pct", "timing_return_pct", "total_return_pct")
    )
    buy_hold = _number(
        buy_hold_side,
        ("return_pct", "buy_hold_return_pct", "total_return_pct"),
    )
    max_drawdown = _number(timing_side, ("max_drawdown", "max_drawdown_pct"))
    sharpe = _number(timing_side, ("sharpe_ratio", "sharpe"))
    timing_dates = _daily_dates(timing_side.get("daily_values"))
    buy_hold_dates = _daily_dates(buy_hold_side.get("daily_values"))
    trades = timing_side.get("trades")
    if not isinstance(trades, Sequence) or isinstance(trades, (str, bytes)):
        trades = None
    trade_dates = _trade_dates(trades)
    completed_trades = (
        sum(1 for trade in trades if _completed_sell(trade))
        if trade_dates is not None
        else None
    )
    missing = []
    for name, value in (
        ("timing_return_pct", timing),
        ("buy_hold_return_pct", buy_hold),
        ("max_drawdown", max_drawdown),
        ("sharpe_ratio", sharpe),
        ("timing_daily_values", timing_dates),
        ("buy_hold_daily_values", buy_hold_dates),
        ("timing_trades", completed_trades),
    ):
        if value is None:
            missing.append(name)
    if missing:
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "missing_metrics",
            "error": "missing_metrics:" + ",".join(missing),
        }
    if any(day < start or day > end for day in timing_dates | buy_hold_dates):
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "observations_outside_window",
            "error": "daily observations must stay within the declared window",
        }
    if any(day < start or day > end for day in trade_dates):
        return {
            "stock_code": code,
            "status": "FAILED",
            "reason_code": "trades_outside_window",
            "error": "trades must stay within the declared window",
        }
    observed_sessions = len(timing_dates & buy_hold_dates)
    return {
        "stock_code": code,
        "status": "SUCCESS",
        "timing_return_pct": round(timing, 8),
        "buy_hold_return_pct": round(buy_hold, 8),
        "excess_return_pct": round(timing - buy_hold, 8),
        "max_drawdown": round(max_drawdown, 8),
        "sharpe_ratio": round(sharpe, 8),
        "completed_trades": completed_trades,
        "observed_sessions": observed_sessions,
        "evidence_eligible": (
            observed_sessions >= MIN_OBSERVED_SESSIONS
            and completed_trades >= MIN_COMPLETED_SELLS
        ),
        "anti_overfit_flags": [],
        "comparison_assumptions_hash": canonical_json_sha256(
            timing_side["assumptions"]
        ),
    }


def _quartiles(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "median": None, "q1": None, "q3": None}
    ordered = sorted(float(value) for value in values)

    def percentile(fraction: float) -> float:
        index = (len(ordered) - 1) * fraction
        lower = math.floor(index)
        upper = math.ceil(index)
        if lower == upper:
            return ordered[lower]
        weight = index - lower
        return ordered[lower] * (1 - weight) + ordered[upper] * weight

    return {
        "mean": round(sum(ordered) / len(ordered), 8),
        "median": round(percentile(0.5), 8),
        "q1": round(percentile(0.25), 8),
        "q3": round(percentile(0.75), 8),
    }


def evaluate_timing_pool(
    cohort: Iterable[str],
    runner: Callable[..., Any],
    *,
    model_version: str,
    config: Mapping[str, Any],
    window: Any = None,
    start_date: Any = None,
    end_date: Any = None,
    cohort_as_of: Any = None,
    cohort_source: str | None = None,
    delisted_completeness: str = "UNKNOWN",
    initial_cash: float = 100_000.0,
) -> dict[str, Any]:
    """Evaluate every member of an explicit cohort and return an unvalidated report.

    The injected runner may be a thin adapter around ``run_backtest``.  This
    function never chooses a winner, writes a result, or derives cohort
    membership from current database state.
    """
    codes = canonical_cohort(cohort)
    if not callable(runner):
        raise TypeError("runner must be callable")
    model = str(model_version or "").strip()
    if not model:
        raise ValueError("model_version is required")
    if not isinstance(config, Mapping):
        raise TypeError("config must be a mapping")
    if cohort_as_of is None:
        raise ValueError("cohort_as_of is required")
    normalized_cohort_as_of = _rfc3339_utc(cohort_as_of, "cohort_as_of")
    if not isinstance(cohort_source, str) or not cohort_source.strip():
        raise ValueError("cohort_source is required")
    normalized_cohort_source = cohort_source.strip()
    try:
        cash = float(initial_cash)
    except (TypeError, ValueError) as exc:
        raise ValueError("initial_cash must be positive") from exc
    if not math.isfinite(cash) or cash <= 0:
        raise ValueError("initial_cash must be positive")
    normalized_window, start, end = _window(window, start_date, end_date)
    config_copy = _jsonable(config)
    assert isinstance(config_copy, dict)
    delisted = _delisted_status(delisted_completeness)
    config_hash = canonical_json_sha256(config_copy)
    cohort_hash = canonical_json_sha256(
        {
            "codes": codes,
            "as_of": normalized_cohort_as_of,
            "source": normalized_cohort_source,
        }
    )
    identity_payload = {
        "cohort": codes,
        "cohort_as_of": normalized_cohort_as_of,
        "cohort_source": normalized_cohort_source,
        "model_version": model,
        "config": config_copy,
        "config_hash": config_hash,
        "window": normalized_window,
        "delisted_completeness": delisted,
    }
    identity_hash = canonical_json_sha256(identity_payload)
    expected_assumptions = {
        "initial_cash": cash,
        "window": normalized_window,
        "execution_timing": "next_trading_day_open",
        "valuation_timing": "last_close",
    }

    rows = []
    for code in codes:
        try:
            result = _call_runner(
                runner,
                _runner_kwargs(
                    code=code,
                    config=config,
                    model_version=model,
                    start=start,
                    end=end,
                    initial_cash=cash,
                ),
            )
            rows.append(
                _normalize_row(
                    code,
                    result,
                    expected_assumptions,
                    start=start,
                    end=end,
                )
            )
        except Exception as exc:  # noqa: BLE001 - failures are report data
            rows.append(
                {
                    "stock_code": code,
                    "status": "FAILED",
                    "reason_code": "runner_exception",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    successful = [row for row in rows if row["status"] == "SUCCESS"]
    failed = [row for row in rows if row["status"] == "FAILED"]
    excess_values = [float(row["excess_return_pct"]) for row in successful]
    distribution = _quartiles(excess_values)
    eligible = [row for row in successful if row["evidence_eligible"]]
    gates = {
        "requested": {
            "value": len(codes),
            "minimum": MIN_REQUESTED_STOCKS,
            "passed": len(codes) >= MIN_REQUESTED_STOCKS,
        },
        "evidence_eligible": {
            "value": len(eligible),
            "minimum": MIN_EVIDENCE_ELIGIBLE,
            "passed": len(eligible) >= MIN_EVIDENCE_ELIGIBLE,
        },
        "coverage": {
            "value": round(len(successful) / len(codes), 8),
            "minimum": MIN_COVERAGE,
            "passed": len(successful) / len(codes) >= MIN_COVERAGE,
        },
    }
    gate_passed = all(bool(gate["passed"]) for gate in gates.values())
    return {
        "schema_version": SCHEMA_VERSION,
        "status": RESEARCH_STATUS,
        "validation_status": RESEARCH_STATUS,
        "research_only": RESEARCH_ONLY,
        "unvalidated": True,
        "identity_sha256": identity_hash,
        "cohort_sha256": cohort_hash,
        "cohort_as_of": normalized_cohort_as_of,
        "cohort_source": normalized_cohort_source,
        "config_hash": config_hash,
        "provenance": {
            "cohort": codes,
            "cohort_size": len(codes),
            "cohort_as_of": normalized_cohort_as_of,
            "cohort_source": normalized_cohort_source,
            "model_version": model,
            "config": config_copy,
            "config_hash": config_hash,
            "window": normalized_window,
            "delisted_completeness": delisted,
        },
        "cohort": codes,
        "cohort_size": len(codes),
        "model_version": model,
        "window": normalized_window,
        "delisted_completeness": delisted,
        "requested_count": len(codes),
        "scanned_count": len(rows),
        "successful_count": len(successful),
        "failed_count": len(failed),
        "eligible_count": len(eligible),
        "evidence_eligible_count": len(eligible),
        "coverage": {
            "requested_count": len(codes),
            "successful_count": len(successful),
            "failed_count": len(failed),
            "success_rate": round(len(successful) / len(codes), 8),
        },
        "excess_distribution": distribution,
        "positive_excess_share": round(
            sum(value > 0 for value in excess_values) / len(excess_values), 8
        )
        if excess_values
        else 0.0,
        "total_completed_trades": sum(
            int(row["completed_trades"]) for row in successful
        ),
        "gates": gates,
        "gate_passed": gate_passed,
        "gate_reasons": sorted(
            name for name, gate in gates.items() if not bool(gate["passed"])
        ),
        "results": rows,
    }


# Explicit aliases make the boundary discoverable without introducing a class
# whose serialization semantics could drift from the dict contract.
build_timing_decision_record = build_timing_decision
build_timing_pool_report = evaluate_timing_pool
