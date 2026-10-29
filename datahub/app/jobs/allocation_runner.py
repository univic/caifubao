# -*- coding: utf-8 -*-
"""Daily allocation plan for the stable sleeve: research only, never an order.

The runner reads the v2 declaration and the ETF quotes already published, then
produces one plan per session through the same pure functions the contract tests
cover:

    ETf quotes + declaration -> plan_allocation -> AllocationPlan

Three deliberate properties:

* **No scoring model.** An allocation sleeve is driven by its declared weights
  and band, so ``signal`` is absent from the declaration and nothing here queries
  the scoring pipeline.
* **No order.** A plan records target weights; execution does not exist in this
  path, and the trading runtime is untouched.
* **Refuses to guess.** A missing quote, a missing asset in the declaration, or a
  session with no data produces SKIPPED rather than a plan built from stale marks.

The plan carries the cost basis it used for the per-asset stop, because the stop
is only reproducible if the reference price is recorded with it.
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from caifubao_contracts.strategy_allocation import (
    plan_allocation,
    should_rebalance,
    validate_allocation,
)
from caifubao_contracts.strategy_exit_policy import validate_exit_policy

BEIJING = ZoneInfo("Asia/Shanghai")


class AllocationRunError(RuntimeError):
    """The session cannot be planned; the caller must not invent a plan."""


def _session_date(value) -> dt.date:
    if value is None:
        return dt.datetime.now(BEIJING).date()
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def sessions_between(trading_days: list[str], start, end) -> int:
    """Number of trading sessions strictly after ``start`` up to ``end``."""
    if start is None:
        return 10**6
    start_day = _session_date(start).isoformat()
    end_day = _session_date(end).isoformat()
    return sum(1 for day in trading_days if start_day < day <= end_day)


def build_plan(
    *,
    spec: dict,
    strategy_id: str,
    session_date,
    quotes: dict[str, float],
    trading_days: list[str],
    previous: dict | None = None,
) -> dict:
    """Return the plan document fields for one session, or raise."""
    allocation = validate_allocation(spec["allocation"])
    exit_policy = validate_exit_policy(spec["family"], spec["selection"]["exit"])
    cadence = int(spec["rebalance"]["cadence_days"])
    day = _session_date(session_date).isoformat()

    targets = allocation["assets"]
    missing = sorted(code for code in targets if quotes.get(code) is None)
    if missing:
        raise AllocationRunError(
            f"no usable quote for {missing} on {day}; refusing to plan from stale marks"
        )

    previous = previous or {}
    # Keep undeclared holdings in the state: an asset that left the declaration
    # must be *closed*, not silently forgotten.
    previous_targets = {
        code: float(weight) for code, weight in (previous.get("targets") or {}).items()
    }
    reference = {
        code: float(price)
        for code, price in (previous.get("reference_prices") or {}).items()
        if code in targets
    }
    for code in targets:
        reference.setdefault(code, quotes[code])

    since = sessions_between(trading_days, previous.get("date"), day)
    rebalance_session = should_rebalance(since, cadence)
    plan = plan_allocation(
        targets=targets,
        band_pct=allocation["band_pct"],
        current_weights=previous_targets,
        marks=quotes,
        cost_basis=reference,
        stop_loss_pct=exit_policy["stop_loss_pct"],
        rebalance_session=rebalance_session,
    )
    # A stop or a rebalance resets the cost basis for the assets it touched.
    reference = {
        code: (
            quotes[code]
            if plan["actions"].get(code) in ("open", "rebalance")
            else reference.get(code, quotes[code])
        )
        for code in targets
    }
    return {
        "strategy_id": strategy_id,
        "spec_hash": spec.get("_spec_hash"),
        "date": day,
        "targets": plan["targets"],
        "actions": plan["actions"],
        "cash_weight": plan["cash_weight"],
        "reference_prices": reference,
        "rebalance_session": plan["rebalance_session"],
        "status": "PLANNED",
    }


def load_quotes(codes, session) -> dict[str, float]:
    """Adjusted closes for the session; a missing asset is simply absent."""
    from app.model.stock import EtfDailyQuote

    day = _session_date(session)
    start = dt.datetime.combine(day, dt.time.min)
    end = dt.datetime.combine(day, dt.time.max)
    quotes = {}
    for quote in EtfDailyQuote.objects(
        code__in=list(codes), date__gte=start, date__lte=end
    ):
        if getattr(quote, "trade_status", 1) in (None, 1) and quote.close_hfq:
            quotes[quote.code] = float(quote.close_hfq)
    return quotes


def trading_days_for(code: str) -> list[str]:
    from app.model.stock import EtfDailyQuote

    return sorted(
        str(value)[:10] for value in EtfDailyQuote.objects(code=code).distinct("date")
    )


def previous_plan(strategy_id: str, spec_hash: str, before) -> dict | None:
    from app.model.strategy import AllocationPlan

    day = _session_date(before)
    record = (
        AllocationPlan.objects(
            strategy_id=strategy_id,
            spec_hash=spec_hash,
            date__lt=dt.datetime.combine(day, dt.time.min),
            status="PLANNED",
        )
        .order_by("-date")
        .first()
    )
    if record is None:
        return None
    return {
        "date": str(record.date)[:10],
        "targets": dict(record.targets or {}),
        "reference_prices": dict(record.reference_prices or {}),
    }


def run(
    *,
    spec: dict,
    strategy_id: str,
    session_date=None,
    dry_run: bool = False,
) -> dict:
    """Build one plan; write it unless ``dry_run``."""
    from app.model.strategy import AllocationPlan

    day = _session_date(session_date)
    codes = list(validate_allocation(spec["allocation"])["assets"])
    quotes = load_quotes(codes, day)
    days = trading_days_for(codes[0])
    previous = previous_plan(strategy_id, spec["_spec_hash"], day)
    plan = build_plan(
        spec=spec,
        strategy_id=strategy_id,
        session_date=day,
        quotes=quotes,
        trading_days=days,
        previous=previous,
    )
    plan["execution_date"] = next(
        (value for value in days if value > plan["date"]), None
    )
    if dry_run:
        plan["dry_run"] = True
        return plan

    record = AllocationPlan(
        strategy_id=plan["strategy_id"],
        spec_hash=plan["spec_hash"],
        date=dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc),
        execution_date=(
            dt.datetime.fromisoformat(plan["execution_date"])
            if plan.get("execution_date")
            else None
        ),
        decision_at=dt.datetime.now(dt.timezone.utc),
        targets=plan["targets"],
        actions=plan["actions"],
        cash_weight=float(plan["cash_weight"]),
        reference_prices={
            code: float(price) for code, price in plan["reference_prices"].items()
        },
        rebalance_session=bool(plan["rebalance_session"]),
        status="PLANNED",
    )
    record.save()
    plan["saved"] = True
    return plan
