"""Causal, research-only adapter for P0 stock-timing pool evaluation.

The core is deliberately database-free.  Callers inject frozen cohort,
prediction and quote evidence; the operator CLI owns read-only Mongo access.
"""

from __future__ import annotations

from copy import deepcopy
import datetime as dt
import hashlib
import json
import math
import re
from statistics import fmean, pstdev
from typing import Any, Mapping, Sequence


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_USABLE_SCORE_STATUSES = {"PENDING", "TRACKING", "VERIFIED", "INSUFFICIENT_DATA"}
_SHANGHAI_CLOSE_UTC = dt.time(7, 0, tzinfo=dt.UTC)
_SHANGHAI_OPEN_UTC = dt.time(1, 30, tzinfo=dt.UTC)


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _date(value: Any, field: str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field} must be YYYY-MM-DD") from exc
    raise ValueError(f"{field} must be YYYY-MM-DD")


def _instant(value: Any, field: str) -> dt.datetime:
    if isinstance(value, str):
        candidate = value.strip().replace("Z", "+00:00")
        try:
            value = dt.datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ValueError(f"{field} must be timezone-aware RFC3339") from exc
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware RFC3339")
    return value.astimezone(dt.UTC)


def _rfc3339(value: dt.datetime) -> str:
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _sha256(value: Any, field: str) -> str:
    normalized = _text(value, field)
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return normalized


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if normalized < 1 or normalized != value:
        raise ValueError(f"{field} must be a positive integer")
    return normalized


def _nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if normalized < 0 or normalized != value:
        raise ValueError(f"{field} must be a non-negative integer")
    return normalized


def _finite(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be finite")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(normalized) or (minimum is not None and normalized < minimum):
        raise ValueError(f"{field} must be finite and >= {minimum}")
    return normalized


def validate_replay_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize the frozen replay contract."""
    root = _mapping(manifest, "manifest")
    if root.get("schema_version") != "timing-replay-p1":
        raise ValueError("schema_version must be timing-replay-p1")

    cohort = _mapping(root.get("cohort"), "cohort")
    if cohort.get("membership_basis") != "point_in_time":
        raise ValueError("membership_basis must be point_in_time")
    if cohort.get("delisted_completeness") != "VERIFIED":
        raise ValueError("delisted_completeness must be VERIFIED")
    source = _text(cohort.get("source"), "cohort.source")
    as_of = _instant(cohort.get("as_of"), "cohort.as_of")

    provenance = _mapping(cohort.get("provenance"), "cohort.provenance")
    _text(provenance.get("artifact_uri"), "cohort.provenance.artifact_uri")
    artifact_hash = _sha256(
        provenance.get("artifact_sha256"), "cohort.provenance.artifact_sha256"
    )
    if provenance.get("includes_subsequently_delisted") is not True:
        raise ValueError("provenance must include subsequently delisted names")
    if provenance.get("includes_suspended") is not True:
        raise ValueError("provenance must include suspended names")

    raw_members = cohort.get("members")
    if not isinstance(raw_members, list) or not raw_members:
        raise ValueError("cohort.members must be a non-empty list")
    members = []
    seen_codes = set()
    for index, raw_member in enumerate(raw_members):
        member = _mapping(raw_member, f"cohort.members[{index}]")
        code = _text(member.get("stock_code"), f"cohort.members[{index}].stock_code")
        if code in seen_codes:
            raise ValueError(f"duplicate cohort member {code}")
        seen_codes.add(code)
        listed_on = _date(member.get("listed_on"), f"cohort.members[{index}].listed_on")
        delisted_raw = member.get("delisted_on")
        delisted_on = (
            _date(delisted_raw, f"cohort.members[{index}].delisted_on")
            if delisted_raw is not None
            else None
        )
        evidence_at = _instant(
            member.get("evidence_at"), f"cohort.members[{index}].evidence_at"
        )
        suspended = member.get("suspended_at_as_of")
        if not isinstance(suspended, bool):
            raise ValueError(
                f"cohort.members[{index}].suspended_at_as_of must be boolean"
            )
        if evidence_at > as_of:
            raise ValueError(f"cohort.members[{index}].evidence_at is after as_of")
        if listed_on > as_of.date():
            raise ValueError(f"cohort member {code} was not listed at as_of")
        if delisted_on is not None and delisted_on <= as_of.date():
            raise ValueError(f"cohort member {code} was delisted at as_of")
        members.append(
            {
                "stock_code": code,
                "listed_on": listed_on.isoformat(),
                "delisted_on": delisted_on.isoformat() if delisted_on else None,
                "suspended_at_as_of": suspended,
                "evidence_at": _rfc3339(evidence_at),
            }
        )

    members.sort(key=lambda item: item["stock_code"])
    expected_member_count = _positive_int(
        provenance.get("member_count"), "cohort.provenance.member_count"
    )
    if expected_member_count != len(members):
        raise ValueError("cohort.provenance.member_count does not reconcile")
    actual_delisted = sum(item["delisted_on"] is not None for item in members)
    expected_delisted = _nonnegative_int(
        provenance.get("subsequently_delisted_count"),
        "cohort.provenance.subsequently_delisted_count",
    )
    if expected_delisted != actual_delisted:
        raise ValueError(
            "cohort.provenance.subsequently_delisted_count does not reconcile"
        )
    actual_suspended = sum(item["suspended_at_as_of"] for item in members)
    expected_suspended = _nonnegative_int(
        provenance.get("suspended_count"), "cohort.provenance.suspended_count"
    )
    if expected_suspended != actual_suspended:
        raise ValueError("cohort.provenance.suspended_count does not reconcile")

    window = _mapping(root.get("window"), "window")
    start = _date(window.get("from"), "window.from")
    end = _date(window.get("to"), "window.to")
    if start > end:
        raise ValueError("window.from must be <= window.to")
    if as_of.date() > start:
        raise ValueError("cohort.as_of must be no later than window.from")

    raw_calendar = root.get("trading_calendar")
    if not isinstance(raw_calendar, list) or not raw_calendar:
        raise ValueError("trading_calendar must be a non-empty list")
    calendar = sorted({_date(day, "trading_calendar item") for day in raw_calendar})
    if len(calendar) != len(raw_calendar):
        raise ValueError("trading_calendar must not contain duplicates")
    if calendar[0] < start or calendar[-1] > end:
        raise ValueError("trading_calendar dates must fall inside window")
    if as_of >= dt.datetime.combine(calendar[0], _SHANGHAI_OPEN_UTC):
        raise ValueError("cohort.as_of must be before the first replay session open")

    raw_prediction_cohorts = _mapping(
        root.get("prediction_cohorts"), "prediction_cohorts"
    )
    prediction_cohorts = {}
    for day in calendar:
        day_text = day.isoformat()
        record = _mapping(
            raw_prediction_cohorts.get(day_text),
            f"prediction_cohorts[{day_text}]",
        )
        _text(
            record.get("artifact_uri"), f"prediction_cohorts[{day_text}].artifact_uri"
        )
        record_hash = _sha256(
            record.get("artifact_sha256"),
            f"prediction_cohorts[{day_text}].artifact_sha256",
        )
        fingerprint = _text(
            record.get("cohort_fingerprint"),
            f"prediction_cohorts[{day_text}].cohort_fingerprint",
        )
        member_count = _positive_int(
            record.get("member_count"),
            f"prediction_cohorts[{day_text}].member_count",
        )
        data_as_of = _instant(
            record.get("data_as_of"),
            f"prediction_cohorts[{day_text}].data_as_of",
        )
        session_close = dt.datetime.combine(day, _SHANGHAI_CLOSE_UTC)
        if data_as_of > session_close:
            raise ValueError(
                f"prediction_cohorts[{day_text}].data_as_of is after session close"
            )
        prediction_cohorts[day_text] = {
            "artifact_uri": record["artifact_uri"].strip(),
            "artifact_sha256": record_hash,
            "cohort_fingerprint": fingerprint,
            "member_count": member_count,
            "data_as_of": _rfc3339(data_as_of),
        }
    extra_dates = set(raw_prediction_cohorts) - set(prediction_cohorts)
    if extra_dates:
        raise ValueError("prediction_cohorts contains dates outside trading_calendar")

    model = _mapping(root.get("model"), "model")
    model_version = _text(model.get("model_version"), "model.model_version")
    config_hash = _sha256(model.get("config_hash"), "model.config_hash")
    horizon = _positive_int(model.get("horizon"), "model.horizon")
    if horizon not in {5, 20, 60}:
        raise ValueError("model.horizon must be one of 5, 20, 60")

    strategy = _mapping(root.get("strategy"), "strategy")
    entry = _finite(strategy.get("entry_percentile"), "strategy.entry_percentile")
    exit_ = _finite(strategy.get("exit_percentile"), "strategy.exit_percentile")
    if not 0 <= exit_ < entry <= 1:
        raise ValueError("strategy percentiles must satisfy 0 <= exit < entry <= 1")

    initial_cash = _finite(root.get("initial_cash"), "initial_cash", minimum=0.01)
    board_lot = _positive_int(root.get("board_lot"), "board_lot")
    friction = _mapping(root.get("friction"), "friction")
    normalized_friction = {
        key: _finite(friction.get(key), f"friction.{key}", minimum=0.0)
        for key in (
            "commission_rate",
            "minimum_commission",
            "stamp_duty_rate",
            "slippage_rate",
        )
    }
    if normalized_friction["slippage_rate"] >= 1:
        raise ValueError("friction.slippage_rate must be < 1")
    prediction_cohorts_hash = _canonical_sha256(prediction_cohorts)
    calendar_text = [day.isoformat() for day in calendar]
    calendar_hash = _canonical_sha256(calendar_text)

    return {
        "schema_version": "timing-replay-p1",
        "cohort_as_of": _rfc3339(as_of),
        "cohort_source": f"{source}#sha256={artifact_hash}",
        "delisted_completeness": "VERIFIED",
        "cohort_codes": [item["stock_code"] for item in members],
        "members": members,
        "cohort_provenance": deepcopy(dict(provenance)),
        "prediction_cohorts": prediction_cohorts,
        "model_version": model_version,
        "model_config_hash": config_hash,
        "horizon": horizon,
        "config": {
            "entry_percentile": entry,
            "exit_percentile": exit_,
            "horizon": horizon,
            "model_config_hash": config_hash,
            "cohort_artifact_sha256": artifact_hash,
            "prediction_cohorts_sha256": prediction_cohorts_hash,
            "trading_calendar_sha256": calendar_hash,
        },
        "window": {"from": start.isoformat(), "to": end.isoformat()},
        "trading_calendar": calendar_text,
        "initial_cash": initial_cash,
        "board_lot": board_lot,
        "friction": normalized_friction,
    }


def validate_model_pin(
    manifest: Mapping[str, Any], registry_record: Mapping[str, Any] | None
) -> dict[str, str]:
    """Fail closed unless the manifest names one exact ACTIVE ranked model."""
    if registry_record is None:
        raise ValueError("model_version is not registered")
    record = _mapping(registry_record, "registry_record")
    if record.get("model_version") != manifest.get("model_version"):
        raise ValueError("model_version does not match registry record")
    if record.get("status") != "ACTIVE":
        raise ValueError("status must be ACTIVE")
    if record.get("scoring_mode") != "ranked":
        raise ValueError("scoring_mode must be ranked")
    if record.get("config_hash") != manifest.get("model_config_hash"):
        raise ValueError("config_hash does not match replay manifest")
    return {
        "model_version": str(record["model_version"]),
        "status": "ACTIVE",
        "scoring_mode": "ranked",
        "config_hash": str(record["config_hash"]),
    }


def _quote_value(row: Any, field: str) -> Any:
    return row.get(field) if isinstance(row, Mapping) else getattr(row, field, None)


def _row_date(row: Any, field: str) -> dt.date:
    return _date(_quote_value(row, "date"), field)


def _tradable(row: Any, side: str) -> bool:
    open_price = _quote_value(row, "open_hfq")
    status = _quote_value(row, "trade_status")
    change_rate = _quote_value(row, "change_rate")
    if isinstance(open_price, bool) or isinstance(change_rate, bool):
        return False
    try:
        open_value = float(open_price)
        change_value = float(change_rate)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(open_value) or open_value <= 0:
        return False
    if type(status) is not int or status != 1:
        return False
    if not math.isfinite(change_value):
        return False
    if side == "BUY" and change_value >= 9.9:
        return False
    if side == "SELL" and change_value <= -9.9:
        return False
    return True


def _execution(open_price: float, side: str, friction: Mapping[str, float]) -> float:
    slippage = friction["slippage_rate"]
    return open_price * (1 + slippage if side == "BUY" else 1 - slippage)


def _commission(value: float, friction: Mapping[str, float]) -> float:
    return max(value * friction["commission_rate"], friction["minimum_commission"])


def _max_buy_quantity(
    cash: float,
    open_price: float,
    board_lot: int,
    friction: Mapping[str, float],
) -> int:
    price = _execution(open_price, "BUY", friction)
    quantity = int(cash / price) // board_lot * board_lot
    while quantity > 0:
        value = quantity * price
        if value + _commission(value, friction) <= cash:
            return quantity
        quantity -= board_lot
    return 0


def _filled_trade(
    day: str,
    side: str,
    quantity: int,
    open_price: float,
    friction: Mapping[str, float],
) -> tuple[dict[str, Any], float]:
    price = _execution(open_price, side, friction)
    value = price * quantity
    commission = _commission(value, friction)
    stamp = value * friction["stamp_duty_rate"] if side == "SELL" else 0.0
    cash_delta = -(value + commission) if side == "BUY" else value - commission - stamp
    return (
        {
            "date": day,
            "side": side,
            "status": "FILLED",
            "quantity": quantity,
            "price": round(open_price, 4),
            "exec_price": round(price, 4),
            "amount": round(value, 4),
            "commission": round(commission, 4),
            "stamp_duty": round(stamp, 4),
            "slippage": round(abs(price - open_price) * quantity, 4),
        },
        cash_delta,
    )


def _prediction_reason(
    prediction: Any,
    day: str,
    stock_code: str,
    manifest: Mapping[str, Any],
) -> tuple[float | None, str | None]:
    if prediction is None:
        return None, "missing_prediction"
    getter = (
        prediction.get
        if isinstance(prediction, Mapping)
        else lambda key, default=None: getattr(prediction, key, default)
    )
    if getter("stock_code") != stock_code:
        return None, "stock_code_mismatch"
    if _date(getter("date"), "prediction.date").isoformat() != day:
        return None, "prediction_date_mismatch"
    if getter("model_version") != manifest["model_version"]:
        return None, "model_version_mismatch"
    if getter("horizon") != manifest["horizon"]:
        return None, "horizon_mismatch"
    if getter("status") not in _USABLE_SCORE_STATUSES:
        return None, "unusable_score_status"
    snapshot = getter("input_snapshot")
    if not isinstance(snapshot, Mapping):
        return None, "missing_input_snapshot"
    if snapshot.get("freshness") != "FRESH":
        return None, "stale_prediction"
    if snapshot.get("status") != "RANKED" or snapshot.get("scoring_mode") != "ranked":
        return None, "not_ranked"
    provenance = manifest["prediction_cohorts"][day]
    if snapshot.get("cohort_fingerprint") != provenance["cohort_fingerprint"]:
        return None, "cohort_fingerprint_mismatch"
    if snapshot.get("cohort_artifact_sha256") != provenance["artifact_sha256"]:
        return None, "cohort_artifact_mismatch"
    try:
        snapshot_as_of = _instant(
            snapshot.get("data_as_of"), "input_snapshot.data_as_of"
        )
    except ValueError:
        return None, "missing_data_as_of"
    expected_as_of = _instant(provenance["data_as_of"], "prediction cohort data_as_of")
    session_close = dt.datetime.combine(dt.date.fromisoformat(day), _SHANGHAI_CLOSE_UTC)
    if snapshot_as_of > session_close:
        return None, "future_data_as_of"
    if snapshot_as_of != expected_as_of:
        return None, "data_as_of_mismatch"
    percentile = getter("percentile")
    if isinstance(percentile, bool):
        return None, "invalid_percentile"
    try:
        percentile = float(percentile)
    except (TypeError, ValueError):
        return None, "invalid_percentile"
    if not math.isfinite(percentile) or not 0 <= percentile <= 1:
        return None, "invalid_percentile"
    return percentile, None


def _metrics(initial_cash: float, daily_values: Sequence[Mapping[str, Any]]) -> dict:
    equities = [float(row["equity"]) for row in daily_values]
    total_return_pct = (equities[-1] / initial_cash - 1.0) * 100.0
    peak = equities[0]
    max_drawdown = 0.0
    returns = []
    for previous, current in zip(equities, equities[1:]):
        peak = max(peak, current)
        max_drawdown = min(max_drawdown, (current / peak - 1.0) * 100.0)
        if previous > 0:
            returns.append(current / previous - 1.0)
    volatility = pstdev(returns) if len(returns) >= 2 else 0.0
    sharpe = fmean(returns) / volatility * math.sqrt(252) if volatility > 0 else 0.0
    return {
        "return_pct": round(total_return_pct, 8),
        "max_drawdown": round(max_drawdown, 8),
        "sharpe_ratio": round(sharpe, 8),
    }


def replay_pair(
    stock_code: str,
    quotes: Sequence[Any],
    predictions: Sequence[Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Build normalized timing and same-stock buy/hold sides from real rows."""
    if stock_code not in manifest["cohort_codes"]:
        raise ValueError(f"{stock_code} is not in the frozen cohort")
    calendar = manifest["trading_calendar"]
    quote_by_date = {}
    for row in quotes:
        day = _row_date(row, "quote.date").isoformat()
        if day in quote_by_date:
            raise ValueError(f"duplicate quote for {day}")
        quote_by_date[day] = row
    for day in calendar:
        if day not in quote_by_date:
            raise ValueError(f"missing quote for authoritative session {day}")
        close = _quote_value(quote_by_date[day], "close_hfq")
        if isinstance(close, bool):
            raise ValueError(f"invalid close_hfq for {day}")
        try:
            close = float(close)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid close_hfq for {day}") from exc
        if not math.isfinite(close) or close <= 0:
            raise ValueError(f"invalid close_hfq for {day}")

    prediction_by_date = {}
    for row in predictions:
        day = _row_date(row, "prediction.date").isoformat()
        if day in prediction_by_date:
            raise ValueError(f"duplicate prediction for {day}")
        prediction_by_date[day] = row

    assumptions = {
        "initial_cash": manifest["initial_cash"],
        "window": deepcopy(manifest["window"]),
        "execution_timing": "next_trading_day_open",
        "valuation_timing": "last_close",
        "board_lot": manifest["board_lot"],
        "friction": deepcopy(manifest["friction"]),
    }
    states = {
        "timing": {
            "cash": manifest["initial_cash"],
            "shares": 0,
            "pending": None,
            "trades": [],
            "daily_values": [],
            "blocked_fills": 0,
        },
        "buy_hold": {
            "cash": manifest["initial_cash"],
            "shares": 0,
            "pending": "BUY",
            "trades": [],
            "daily_values": [],
            "blocked_fills": 0,
        },
    }
    rejected_predictions = []
    friction = manifest["friction"]
    board_lot = manifest["board_lot"]

    for day in calendar:
        quote = quote_by_date[day]
        open_price = _quote_value(quote, "open_hfq")
        for name, state in states.items():
            pending = state["pending"]
            if pending is not None:
                if _tradable(quote, pending):
                    if pending == "BUY":
                        quantity = _max_buy_quantity(
                            state["cash"], float(open_price), board_lot, friction
                        )
                    else:
                        quantity = state["shares"]
                    if quantity > 0:
                        trade, delta = _filled_trade(
                            day, pending, quantity, float(open_price), friction
                        )
                        state["trades"].append(trade)
                        state["cash"] += delta
                        state["shares"] = quantity if pending == "BUY" else 0
                    state["pending"] = None
                else:
                    state["blocked_fills"] += 1

        close = float(_quote_value(quote, "close_hfq"))
        for state in states.values():
            equity = state["cash"] + state["shares"] * close
            state["daily_values"].append({"date": day, "equity": round(equity, 4)})

        percentile, rejection = _prediction_reason(
            prediction_by_date.get(day), day, stock_code, manifest
        )
        if rejection is not None:
            rejected_predictions.append(rejection)
            continue
        timing = states["timing"]
        if timing["shares"] == 0:
            timing["pending"] = (
                "BUY" if percentile >= manifest["config"]["entry_percentile"] else None
            )
        else:
            timing["pending"] = (
                "SELL" if percentile <= manifest["config"]["exit_percentile"] else None
            )

    pair = {}
    for name, state in states.items():
        side = {
            "stock_code": stock_code,
            "assumptions": deepcopy(assumptions),
            "trades": state["trades"],
            "daily_values": state["daily_values"],
            "diagnostics": {
                "blocked_fills": state["blocked_fills"],
                "open_position": state["shares"] > 0,
            },
        }
        side.update(_metrics(manifest["initial_cash"], state["daily_values"]))
        pair[name] = side
    pair["timing"]["diagnostics"]["rejected_predictions"] = sorted(
        set(rejected_predictions)
    )
    return pair
