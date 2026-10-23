"""Read-only paper-trading readiness assessment for the research environment.

Answers one question before an operator opens (or continues) a paper window on
real market data: *is today's chain complete enough that a run today is
forward evidence rather than a silent replay?*

The assessment never writes: it reads the quote/scoring/signal freshness, the
FQ coverage of the tradable universe, the certified forward window, the halt
switch and the (new) ETF market data, then returns PASS/WARN/FAIL per check.
A FAIL means "do not treat today's output as forward evidence"; a WARN is a
known limitation the operator must acknowledge.

Two shapes keep this testable: ``load_inputs`` performs the Mongo reads and
``evaluate`` is a pure function over the resulting ``ReadinessInputs``.
"""

from __future__ import annotations

import datetime
import json
import os
from dataclasses import dataclass, field

QUOTE_COLLECTION = "stock_daily_quote"
SCORE_COLLECTION = "stock_score_predictions"
SIGNAL_COLLECTION = "stock_signal_daily"
STOCK_COLLECTION = "basic_stock"
WINDOW_COLLECTION = "strategy_forward_windows"
RUN_COLLECTION = "strategy_paper_runs"
MARKET_COLLECTION = "finance_market"
ETF_COLLECTION = "exchange_traded_fund"
ETF_QUOTE_COLLECTION = "etf_daily_quote"

HALT_PATH_ENV = "CAIFUBAO_STRATEGY_HALT_FILE"
MARKET_NAME = "ChinaAStock"
SESSION_CLOSE_MINUTE = 15 * 60

LIMITATIONS = [
    "the current (pre-#307) paper NAV marks positions at raw open/close: cash "
    "distributions are not credited, so total return is understated across "
    "ex-dividend dates. This limitation is scoped to the NAV implementation "
    "the check observes; the coordinated execution-model change retires it "
    "when the ledger credits distributions and share multipliers",
    "paper target weights are not an account: no broker, no order, no fill "
    "against a real account",
    "forward-window gaps are permanent (a missed session is never backfilled)",
    "ETF prices are distribution-adjusted (fund_adj) for analysis, but ETF "
    "cash distributions are subject to the same paper-NAV gap above",
]


def _as_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value)[:10])


def expected_session(calendar, now_beijing) -> datetime.date | None:
    """Latest session whose close has already happened.

    Before 15:00 Beijing on a session, the expected completed session is the
    previous one; on a non-session day it is the most recent past session.
    """
    sessions = sorted({day for day in (_as_date(v) for v in calendar or []) if day})
    today = _as_date(now_beijing)
    if not sessions or today is None:
        return None
    past = [day for day in sessions if day <= today]
    if not past:
        return None
    if today in sessions and now_beijing.hour * 60 + now_beijing.minute < (
        SESSION_CLOSE_MINUTE
    ):
        return past[-2] if len(past) >= 2 else past[-1]
    return past[-1]


@dataclass
class ReadinessInputs:
    now_beijing: datetime.datetime
    calendar: list = field(default_factory=list)
    active_stock_count: int = 0
    latest_quote_date: datetime.date | None = None
    quoted_active_count: int = 0
    missing_fq_count: int = 0
    latest_score_date: datetime.date | None = None
    latest_signal_date: datetime.date | None = None
    active_window: dict | None = None
    forward_dates: list = field(default_factory=list)
    halt_path: str | None = None
    halt_state: dict | None = None
    etf_count: int = 0
    etf_latest_quote_date: datetime.date | None = None
    etf_active_count: int = 0
    etf_missing_fq_count: int = 0


def _check(check_id: str, status: str, summary: str, detail=None) -> dict:
    return {
        "id": check_id,
        "status": status,
        "summary": summary,
        "detail": detail or {},
    }


def _session_lag(latest, expected) -> int | None:
    if latest is None or expected is None:
        return None
    return (expected - latest).days


def _quote_check(inputs: ReadinessInputs, expected) -> dict:
    if inputs.active_stock_count <= 0:
        return _check("quote_freshness", "FAIL", "no active stock universe")
    if inputs.latest_quote_date is None:
        return _check("quote_freshness", "FAIL", "no daily quote rows")
    detail = {
        "active_stock_count": inputs.active_stock_count,
        "quoted_active_count": inputs.quoted_active_count,
        "latest_quote_date": str(inputs.latest_quote_date),
        "expected_session": str(expected) if expected else None,
        "lag_days": _session_lag(inputs.latest_quote_date, expected),
    }
    if expected is not None and inputs.latest_quote_date < expected:
        return _check(
            "quote_freshness",
            "FAIL",
            f"latest quote {inputs.latest_quote_date} is behind the expected "
            f"session {expected}",
            detail,
        )
    coverage = inputs.quoted_active_count / inputs.active_stock_count
    detail["coverage"] = round(coverage, 4)
    if coverage < 0.95:
        return _check(
            "quote_freshness",
            "WARN",
            f"only {coverage:.1%} of active stocks have a quote on "
            f"{inputs.latest_quote_date}",
            detail,
        )
    return _check(
        "quote_freshness",
        "PASS",
        f"{inputs.quoted_active_count}/{inputs.active_stock_count} active "
        f"stocks quoted on {inputs.latest_quote_date}",
        detail,
    )


def _fq_check(inputs: ReadinessInputs) -> dict:
    detail = {
        "missing_fq_count": inputs.missing_fq_count,
        "latest_quote_date": (
            str(inputs.latest_quote_date) if inputs.latest_quote_date else None
        ),
    }
    if inputs.latest_quote_date is None:
        return _check("fq_coverage", "FAIL", "no latest session to assess", detail)
    if inputs.missing_fq_count > 0:
        return _check(
            "fq_coverage",
            "FAIL",
            f"{inputs.missing_fq_count} active quote rows on the latest "
            "session have no fq_factor",
            detail,
        )
    return _check(
        "fq_coverage",
        "PASS",
        "every active quote on the latest session has an adjusted price",
        detail,
    )


def _is_behind(latest, expected) -> bool:
    """True when a required session is newer than what the chain produced."""
    return expected is not None and latest is not None and latest < expected


def _chain_check(check_id, label, latest, expected) -> dict:
    detail = {
        "latest": str(latest) if latest else None,
        "expected_session": str(expected) if expected else None,
        "lag_days": _session_lag(latest, expected),
    }
    if latest is None:
        return _check(check_id, "FAIL", f"no {label} rows", detail)
    if _is_behind(latest, expected):
        return _check(
            check_id,
            "FAIL",
            f"latest {label} {latest} is behind the expected session {expected}",
            detail,
        )
    return _check(check_id, "PASS", f"{label} latest {latest}", detail)


def _window_check(inputs: ReadinessInputs, expected) -> dict:
    window = inputs.active_window
    if not window:
        return _check(
            "forward_window",
            "WARN",
            "no ACTIVE forward window: certify one before counting evidence",
            {},
        )
    start = _as_date(window.get("start_date"))
    forward_dates = sorted(
        {day for day in (_as_date(v) for v in inputs.forward_dates) if day}
    )
    sessions = [
        day
        for day in (_as_date(v) for v in inputs.calendar)
        if day and start and start <= day and (expected is None or day <= expected)
    ]
    # The current expected session has not been run yet when readiness is
    # checked just before the daily run, so only strictly older sessions count
    # as a gap; the current one is reported separately.
    missing = sorted(
        day for day in set(sessions) - set(forward_dates) if expected and day < expected
    )
    detail = {
        "model_version": window.get("model_version"),
        "horizon": window.get("horizon"),
        "config_hash": window.get("config_hash"),
        "start_date": str(start) if start else None,
        "forward_sessions": len(forward_dates),
        "current_session_recorded": bool(expected and expected in forward_dates),
        "missing_sessions": len(missing),
        "missing_examples": [str(day) for day in missing[:5]],
    }
    if missing:
        return _check(
            "forward_window",
            "WARN",
            f"window has {len(missing)} missing session(s) since {start}",
            detail,
        )
    return _check(
        "forward_window",
        "PASS",
        f"active window started {start} with {len(forward_dates)} recorded session(s)",
        detail,
    )


def _halt_check(inputs: ReadinessInputs) -> dict:
    if not inputs.halt_path:
        return _check(
            "halt_switch",
            "WARN",
            "no halt-file path configured: order generation fails closed",
            {"path_configured": False},
        )
    state = inputs.halt_state or {}
    detail = {
        "path_configured": True,
        "halted": bool(state.get("halted")),
        "changed_by": state.get("changed_by"),
        "reason": state.get("reason"),
    }
    if detail["halted"]:
        return _check(
            "halt_switch",
            "WARN",
            "halt switch is engaged (no order generation)",
            detail,
        )
    return _check("halt_switch", "PASS", "halt switch is OFF", detail)


def _etf_check(inputs: ReadinessInputs, expected) -> dict:
    detail = {
        "etf_count": inputs.etf_count,
        "active_etf_count": inputs.etf_active_count,
        "latest_etf_quote_date": (
            str(inputs.etf_latest_quote_date) if inputs.etf_latest_quote_date else None
        ),
        "expected_session": str(expected) if expected else None,
        "missing_fq_count": inputs.etf_missing_fq_count,
    }
    if inputs.etf_count <= 0:
        return _check(
            "etf_market_data",
            "WARN",
            "no ETF universe synced yet; ETF strategy evidence is unavailable",
            detail,
        )
    if inputs.etf_latest_quote_date is None:
        return _check(
            "etf_market_data", "FAIL", "ETF universe exists but has no quotes", detail
        )
    if expected is not None and inputs.etf_latest_quote_date < expected:
        return _check(
            "etf_market_data",
            "FAIL",
            f"ETF quotes are behind the expected session {expected}",
            detail,
        )
    if inputs.etf_missing_fq_count > 0:
        return _check(
            "etf_market_data",
            "FAIL",
            f"{inputs.etf_missing_fq_count} ETF quote rows lack fund_adj FQ",
            detail,
        )
    return _check(
        "etf_market_data",
        "PASS",
        f"{inputs.etf_count} ETF(s) quoted through "
        f"{inputs.etf_latest_quote_date} with adjusted prices",
        detail,
    )


def evaluate(inputs: ReadinessInputs) -> dict:
    expected = expected_session(inputs.calendar, inputs.now_beijing)
    checks = [
        _quote_check(inputs, expected),
        _fq_check(inputs),
        _chain_check(
            "scoring_freshness", "score predictions", inputs.latest_score_date, expected
        ),
        _chain_check(
            "signal_freshness", "signals", inputs.latest_signal_date, expected
        ),
        _window_check(inputs, expected),
        _halt_check(inputs),
        _etf_check(inputs, expected),
    ]
    statuses = {check["status"] for check in checks}
    if "FAIL" in statuses:
        overall = "NOT_READY"
    elif "WARN" in statuses:
        overall = "DEGRADED"
    else:
        overall = "READY"
    return {
        "as_of": inputs.now_beijing.isoformat(),
        "expected_session": str(expected) if expected else None,
        "overall": overall,
        "checks": checks,
        "limitations": list(LIMITATIONS),
    }


def load_inputs(
    db,
    *,
    model_version: str | None = None,
    horizon: int | None = None,
    now_beijing: datetime.datetime | None = None,
    halt_path: str | None = None,
) -> ReadinessInputs:
    """Read the readiness inputs from MongoDB (read-only)."""
    from caifubao_contracts.time import beijing_now_naive

    now_beijing = now_beijing or beijing_now_naive()
    inputs = ReadinessInputs(now_beijing=now_beijing)

    market = db[MARKET_COLLECTION].find_one(
        {"name": MARKET_NAME}, {"_id": 0, "trade_calendar": 1}
    )
    inputs.calendar = list((market or {}).get("trade_calendar") or [])

    # BSE stocks are not a supported universe (daily_quote=false); including
    # them would make the coverage check warn forever.
    active_codes = set(
        db[STOCK_COLLECTION].distinct(
            "code",
            {
                "object_type": "individual_stock",
                "active_status": 0,
                "data_capabilities.daily_quote": {"$ne": False},
            },
        )
    )
    inputs.active_stock_count = len(active_codes)

    # Anchor the session on the supported stock universe: an index row (which
    # is not a stock) must not make a fully stale stock session look current.
    latest = (
        db[QUOTE_COLLECTION].find_one(
            {"code": {"$in": sorted(active_codes)}}, {"date": 1}, sort=[("date", -1)]
        )
        if active_codes
        else None
    )
    inputs.latest_quote_date = _as_date((latest or {}).get("date"))
    if inputs.latest_quote_date is not None:
        quote_day = datetime.datetime.combine(
            inputs.latest_quote_date, datetime.time.min
        )
        quoted = set(db[QUOTE_COLLECTION].distinct("code", {"date": quote_day}))
        quoted_active = quoted & active_codes
        inputs.quoted_active_count = len(quoted_active)
        if quoted_active:
            inputs.missing_fq_count = db[QUOTE_COLLECTION].count_documents(
                {
                    "code": {"$in": sorted(quoted_active)},
                    "date": quote_day,
                    "fq_factor": None,
                }
            )

    score_query = {}
    if model_version:
        score_query["model_version"] = model_version
    score = db[SCORE_COLLECTION].find_one(score_query, {"date": 1}, sort=[("date", -1)])
    inputs.latest_score_date = _as_date((score or {}).get("date"))
    signal = db[SIGNAL_COLLECTION].find_one({}, {"date": 1}, sort=[("date", -1)])
    inputs.latest_signal_date = _as_date((signal or {}).get("date"))

    window_query = {"status": "ACTIVE"}
    if model_version:
        window_query["model_version"] = model_version
    if horizon is not None:
        window_query["horizon"] = int(horizon)
    window = db[WINDOW_COLLECTION].find_one(window_query, {"_id": 0})
    inputs.active_window = window
    if window:
        run_query = {
            "model_version": window.get("model_version"),
            "horizon": window.get("horizon"),
            "config_hash": window.get("config_hash"),
            "evidence_kind": "FORWARD",
            "date": {"$gte": window.get("start_date")},
        }
        inputs.forward_dates = list(db[RUN_COLLECTION].distinct("date", run_query))

    inputs.halt_path = halt_path or os.getenv(HALT_PATH_ENV) or None
    if inputs.halt_path and os.path.exists(inputs.halt_path):
        try:
            with open(inputs.halt_path, encoding="utf-8") as handle:
                inputs.halt_state = json.load(handle)
        except (OSError, ValueError):
            inputs.halt_state = {"halted": True, "reason": "unreadable halt file"}

    etf_codes = set(db[ETF_COLLECTION].distinct("code", {"active_status": 0}))
    inputs.etf_count = len(etf_codes)
    if etf_codes:
        etf_latest = db[ETF_QUOTE_COLLECTION].find_one(
            {"code": {"$in": sorted(etf_codes)}}, {"date": 1}, sort=[("date", -1)]
        )
        inputs.etf_latest_quote_date = _as_date((etf_latest or {}).get("date"))
        if inputs.etf_latest_quote_date is not None:
            etf_day = datetime.datetime.combine(
                inputs.etf_latest_quote_date, datetime.time.min
            )
            quoted_etfs = set(
                db[ETF_QUOTE_COLLECTION].distinct(
                    "code", {"code": {"$in": sorted(etf_codes)}, "date": etf_day}
                )
            )
            if quoted_etfs:
                inputs.etf_missing_fq_count = db[ETF_QUOTE_COLLECTION].count_documents(
                    {
                        "code": {"$in": sorted(quoted_etfs)},
                        "date": etf_day,
                        "fq_factor": None,
                    }
                )
    return inputs
