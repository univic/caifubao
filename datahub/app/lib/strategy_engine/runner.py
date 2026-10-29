# -*- coding: utf-8 -*-
"""Daily-run orchestration for the paper-first strategy runner.

Pure, dependency-injected: the caller (jobs/strategy_runner) supplies concrete
query functions for usable predictions and stock flags; this module assembles
a daily plan (eligible universe -> target holdings -> rebalance vs previous),
decides skip, and never touches Mongo itself. So the full decision path is
unit-testable without a database.
"""

from __future__ import annotations

import datetime
import math
from collections import Counter

from app.lib.strategy_engine.config import (
    DEFAULT_HORIZON,
    validate_strategy_config,
)
from app.lib.strategy_engine.selection import compute_rebalance, plan_target_holdings

# Prediction record field for the eligibility source of truth. The runner maps
# quote/stock rows onto this shape.
STOCK_FLAG_KEYS = ("is_st", "is_bse", "trade_status")


def eligible_codes_from_flags(
    flags: dict[str, dict],  # stock_code -> {is_st?, is_bse?, trade_status?}
    config: dict,
    *,
    exclusion_reasons: dict[str, str] | None = None,
) -> set[str]:
    """Return stock codes that pass the configured eligibility constraints.

    flags maps stock_code to a dict with optional keys is_st (bool/int),
    is_bse (bool/int), trade_status (1 = tradable). Codes missing from flags
    are NOT eligible (unknown liquidity/status is fail-closed). A code with no
    flags entry but present in the prediction set is excluded — the runner must
    pass the full tradable-universe flag map for the date.
    """
    config = validate_strategy_config(config)
    constraints = config["constraints"]
    affordability_spec = None
    if constraints.get("exclude_infeasible_buys"):
        from app.lib.strategy_engine.spec import legacy_to_spec
        from caifubao_contracts.strategy_spec import (
            OpeningTradeInfeasible,
            preflight_execution_feasibility,
        )

        affordability_spec = legacy_to_spec(config)["spec"]

    def _truthy(value) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        return bool(int(value))

    eligible = set()
    min_trade_amount = float(constraints.get("min_trade_amount_cny") or 0.0)
    for code, row in flags.items():
        if constraints.get("exclude_st") and _truthy(row.get("is_st")):
            continue
        if constraints.get("exclude_bse") and _truthy(row.get("is_bse")):
            continue
        if constraints.get("exclude_suspended") and _truthy(
            row.get("trade_status") != 1
        ):
            continue
        # Liquidity floor: a name may only be selected when its traded amount on
        # the signal date is known and meets the floor. Unknown or non-finite
        # amount is not selectable, so missing evidence can never widen the
        # tradable set (NaN would otherwise pass `nan < floor` and fail open).
        if min_trade_amount > 0:
            traded = row.get("trade_amount")
            if (
                isinstance(traded, bool)
                or not isinstance(traded, (int, float))
                or not math.isfinite(float(traded))
                or float(traded) < min_trade_amount
            ):
                continue
        if affordability_spec is not None:
            price = row.get("reference_close")
            reason = None
            if (
                isinstance(price, bool)
                or not isinstance(price, (int, float))
                or not math.isfinite(float(price))
                or price < 0.000001
            ):
                reason = "invalid_signal_close"
            else:
                try:
                    preflight_execution_feasibility(
                        affordability_spec, reference_prices={code: price}
                    )
                except OpeningTradeInfeasible as exc:
                    reason = exc.reason_code
            if reason:
                if exclusion_reasons is not None:
                    exclusion_reasons[code] = reason
                continue
        eligible.add(code)
    return eligible


def _has_active_eligibility_constraint(config: dict) -> bool:
    """True when the config asks for filtering that needs a universe flag map."""
    constraints = config.get("constraints") or {}
    if any(
        constraints.get(flag)
        for flag in (
            "exclude_st",
            "exclude_bse",
            "exclude_suspended",
            "exclude_infeasible_buys",
        )
    ):
        return True
    try:
        return float(constraints.get("min_trade_amount_cny") or 0.0) > 0
    except (TypeError, ValueError):
        return True


def assemble_daily_plan(
    *,
    config: dict,
    date: datetime.datetime,
    predictions,  # iterable of usable predictions for the configured version
    previous_holdings: list[dict] | None,
    flags: dict[str, dict] | None = None,
    industry_by_code: dict[str, str] | None = None,
    horizon: int | None = None,
    held_meta: dict[str, dict] | None = None,
    exit_buffer: float = 1.0,
    min_hold_sessions: int = 0,
    decision_state: dict | None = None,
    exit_policy: dict | None = None,
    family: str = "rank_selection",
    marks: dict | None = None,
) -> dict:
    """Build one day's paper plan.

    Returns {"skipped": bool, "reason"?: str, "date", "horizon",
    "target_holdings": [...], "rebalance": {...}}. When no usable predictions
    exist for the configured model version, the plan is skipped (no empty
    portfolio is written). industry_by_code is required only when the config
    sets constraints.max_industry_pct.
    """
    config = validate_strategy_config(config)
    horizon = int(horizon or config.get("horizon", DEFAULT_HORIZON))
    prediction_list = list(predictions)
    if not prediction_list:
        return {
            "skipped": True,
            "reason": (
                f"no usable predictions for model_version="
                f"{config['score_model_version']} on {date.date()} "
                f"(horizon {horizon})"
            ),
            "date": date,
            "horizon": horizon,
        }

    if flags is None and _has_active_eligibility_constraint(config):
        raise ValueError(
            "cannot assemble a plan without the universe flag map while an "
            "eligibility constraint is active (exclusion flags or "
            "min_trade_amount_cny); refusing to select from the whole "
            "prediction set with the constraint silently skipped"
        )

    excluded = {}
    eligible = (
        eligible_codes_from_flags(flags, config, exclusion_reasons=excluded)
        if flags is not None
        else None
    )
    diagnostics = {}
    if config["constraints"].get("exclude_infeasible_buys"):
        diagnostics["eligibility"] = {
            "price_date": date.date().isoformat(),
            "excluded_count": len(excluded),
            "reason_counts": dict(Counter(excluded.values())),
            "excluded_examples": [
                {"stock_code": code, "reason": excluded[code]}
                for code in sorted(excluded)[:20]
            ],
        }
    if decision_state is not None:
        # A v2 declaration carries its exit contract and is planned from the
        # account state, so a stop-loss and a maximum holding period are read
        # rather than approximated from the previous target book.
        from app.lib.strategy_engine.stateful_planner import plan_from_state

        planned = plan_from_state(
            state=decision_state,
            predictions=prediction_list,
            config=config,
            eligible_codes=eligible,
            industry_by_code=industry_by_code,
            exit_buffer=exit_buffer,
            min_hold_sessions=min_hold_sessions,
            family=family,
            exit_policy=exit_policy,
            # The stop-loss is measured against the decision-session close, so the
            # planner needs it; the state only carries the cost basis.
            marks=marks,
        )
    else:
        planned = plan_target_holdings(
            prediction_list,
            config,
            eligible_codes=eligible,
            industry_by_code=industry_by_code,
            previous_holdings=previous_holdings,
            held_meta=held_meta,
            exit_buffer=exit_buffer,
            min_hold_sessions=min_hold_sessions,
        )
    target = planned["holdings"]
    if not target:
        return {
            "skipped": True,
            "reason": "selection produced no eligible holdings",
            **diagnostics,
            "date": date,
            "horizon": horizon,
        }

    return {
        "skipped": False,
        **diagnostics,
        "date": date,
        "horizon": horizon,
        "target_holdings": target,
        # Only the state-driven path produces a per-name reason map; surfacing it
        # keeps a stop-out explainable instead of merely absent.
        "reasons": dict(planned.get("reasons") or {}),
        "hold_meta": planned["hold_meta"],
        "exit_reasons": planned["reasons"],
        "rebalance": compute_rebalance(previous_holdings, target),
    }


def next_execution_date(signal_date, trade_calendar):
    """Resolve a strictly later market session; never guess beyond coverage."""
    days = sorted({_date_key(day) for day in trade_calendar})
    signal_key = _date_key(signal_date)
    if signal_key not in days:
        raise ValueError("signal date is not in the trading calendar")
    later = [day for day in days if day > signal_key]
    if not later:
        raise ValueError("calendar does not cover the next trading session")
    return datetime.datetime.fromisoformat(later[0])


# A-share session open is 09:30 CST == 01:30 UTC; the causal boundary for a
# FORWARD decision is that its decision timestamp precedes this instant.
SESSION_OPEN_UTC = datetime.time(1, 30)


def _session_open_utc(execution_date) -> datetime.datetime:
    day = (
        execution_date.date()
        if isinstance(execution_date, datetime.datetime)
        else execution_date
    )
    return datetime.datetime.combine(day, SESSION_OPEN_UTC, tzinfo=datetime.UTC)


def classify_evidence_kind(
    *,
    date,
    decision_at,
    execution_date,
    window,  # StrategyForwardWindow or None (ACTIVE certified window)
    config_hash: str,
    existing_status: str | None,
    replace: bool,
    hash_kind: str = "legacy",
) -> str:
    """Certify a strategy run record as FORWARD or REPLAY (NEXT.1).

    FORWARD requires ALL of: an ACTIVE window exists and its config_hash
    matches this run's; the signal date is at/after the window's start_date;
    no COMPLETED plan is being replaced (a replacement is retrospective —
    FORWARD plans are immutable, and a REPLAY/legacy COMPLETED plan replaced
    stays REPLAY); and decision_at precedes the open of the calendar
    execution date (next_execution_date + ChinaAStock calendar). Anything
    else — missing window, config mismatch, backdated signal date, late
    decision, replacement — is REPLAY (legacy/missing provenance default).
    A SKIPPED/FAILED existing document may be rewritten (same-day rerun)
    and still certify FORWARD when all conditions hold.
    """
    if window is None or getattr(window, "status", None) != "ACTIVE":
        return "REPLAY"
    if getattr(window, "config_hash", None) != config_hash:
        return "REPLAY"
    # Missing kind on historical Mongo rows identifies legacy config hashes.
    # A matching raw sha256 value alone cannot cross the two namespaces.
    if (getattr(window, "hash_kind", None) or "legacy") != hash_kind:
        return "REPLAY"
    if _date_key(date) < _date_key(window.start_date):
        return "REPLAY"
    if existing_status == "COMPLETED" and replace:
        # Replacing a completed plan is retrospective; FORWARD plans are
        # immutable (the runner additionally fails closed on that path).
        return "REPLAY"
    try:
        if decision_at is None or not (decision_at < _session_open_utc(execution_date)):
            return "REPLAY"
    except (TypeError, ValueError):
        return "REPLAY"
    return "FORWARD"


def schedule_from_runs(runs) -> list[dict]:
    """Translate signal records into explicit execution-session schedules."""
    schedule = []
    seen = set()
    for run in runs:
        holdings = {
            h["stock_code"]: float(h["weight"])
            for h in (run.target_holdings or [])
            if h.get("stock_code")
        }
        if not holdings:
            continue
        execution = getattr(run, "execution_date", None)
        if execution is None or _date_key(execution) <= _date_key(run.date):
            raise ValueError(
                "run requires execution_date after signal date; replay legacy records"
            )
        day = _date_key(execution)
        if day in seen:
            raise ValueError("duplicate execution_date in paper track")
        seen.add(day)
        schedule.append({"date": day, "holdings": holdings})
    return sorted(schedule, key=lambda item: item["date"])


def attach_nav_points(runs, curve: list[dict]) -> dict:
    """Merge simulate_paper_nav curve points back onto their runs.

    Returns a map date.isoformat() -> {date, nav, daily_return, turnover,
    drawdown, benchmark_return?, positions_count} plus a list of dates with no
    matching curve point. The caller persists each point into the matching
    StrategyPaperRun.nav_snapshot.
    """
    by_date = {}
    for point in curve:
        key = _date_key(point["date"])
        by_date[key] = point

    matched = {}
    unmatched_dates = []
    for run in runs:
        key = _date_key(run.date)
        execution_key = _date_key(getattr(run, "execution_date", None))
        if execution_key in by_date:
            matched[key] = by_date[execution_key]
        else:
            unmatched_dates.append(run.date)
    return {"points_by_date": matched, "unmatched_dates": unmatched_dates}


def _date_key(value) -> str:
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    return str(value)
