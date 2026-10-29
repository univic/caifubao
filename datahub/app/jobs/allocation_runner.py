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
* **Refuses to guess.** A missing quote, a missing asset in the declaration or a
  limit it cannot measure raises `AllocationRunError` and the job exits non-zero.
  It does not write a SKIPPED plan: a skip that nobody acts on would let the
  evidence clock accrue a gap silently, and the gap cannot be filled later.

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
from caifubao_contracts.strategy_risk_budget import REGIME_NONE, SIDEWAYS, check_budget
from caifubao_contracts.strategy_spec import spec_hash, validate_strategy_spec

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

    # Drift the previous target weights by the actual returns since the previous
    # session. Passing the raw targets would make the band blind to drift, which
    # is the only thing it exists to measure.
    drifted, previous_cash = drift_book(
        targets=previous_targets,
        previous_marks=previous.get("marks") or {},
        quotes=quotes,
        cash_weight=float(previous.get("cash_weight") or 0.0),
    )

    # The cadence counts from the last session that actually rebalanced, not from
    # yesterday's plan date; otherwise a daily plan loop keeps the gap at one and
    # a five-session cadence can never fire.
    since = sessions_between(trading_days, previous.get("last_rebalance_date"), day)
    rebalance_session = should_rebalance(since, cadence)
    plan = plan_allocation(
        targets=targets,
        band_pct=allocation["band_pct"],
        current_weights=drifted,
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
    plan = apply_risk_budget(spec, plan, previous_targets)
    return {
        "strategy_id": strategy_id,
        "spec_hash": spec.get("_spec_hash"),
        "date": day,
        "targets": plan["targets"],
        "actions": plan["actions"],
        "cash_weight": plan["cash_weight"],
        "reference_prices": reference,
        "marks": {
            code: float(quote) for code, quote in quotes.items() if code in targets
        },
        "last_rebalance_date": (
            day if plan["rebalance_session"] else previous.get("last_rebalance_date")
        ),
        "rebalance_session": plan["rebalance_session"],
        "risk_verdict": plan.get("risk_verdict"),
        "status": "PLANNED",
    }


def drift_book(
    *,
    targets: dict,
    previous_marks: dict,
    quotes: dict,
    cash_weight: float,
) -> tuple[dict, float]:
    """Grow the previous book by realised returns, cash included.

    Dividing by the invested part alone would rescale a half-invested book to a
    fully invested one and silently erase the reserve, so cash takes part in the
    normalisation and comes back out as a weight.
    """
    marks = {code: float(previous_marks.get(code) or 0.0) for code in targets}
    if not targets or any(not price for price in marks.values()):
        return dict(targets), cash_weight
    if any(not quotes.get(code) for code in targets):
        return dict(targets), cash_weight
    asset_growth = sum(
        float(targets[code]) * float(quotes[code]) / marks[code] for code in targets
    )
    gross = asset_growth + max(0.0, float(cash_weight))
    if gross <= 0:
        return dict(targets), cash_weight
    drifted = {
        code: float(targets[code]) * float(quotes[code]) / marks[code] / gross
        for code in targets
    }
    return drifted, max(0.0, float(cash_weight)) / gross


def assert_risk_evaluable(risk: dict) -> None:
    """Refuse limits this runner cannot measure, before any I/O happens.

    Two ``risk`` fields need inputs the sleeve does not have yet: a drawdown stop
    needs a high-water mark, and a regime policy needs a classifier. Declaring one
    and then ignoring it is the defect this program exists to remove, so the job
    stops here instead.
    """
    if risk.get("max_drawdown_pct") is not None:
        raise AllocationRunError(
            "risk.max_drawdown_pct is declared but this runner has no high-water "
            "mark to measure drawdown against; refusing rather than ignoring it"
        )
    if risk.get("regime_policy", REGIME_NONE) != REGIME_NONE:
        raise AllocationRunError(
            f"risk.regime_policy={risk['regime_policy']!r} needs a regime classifier "
            "this runner does not have; refusing rather than assuming a regime"
        )


def apply_risk_budget(spec: dict, plan: dict, previous_weights: dict) -> dict:
    """Refuse or scale the plan against the declared ``risk`` budget.

    Two limits cannot be evaluated by this runner yet — a drawdown stop needs a
    high-water mark, and a regime policy needs a classifier. Both are *refused*
    when configured rather than silently ignored: a limit that is declared but
    not measured is the defect this program exists to remove.
    """
    risk = spec.get("risk") or {}
    assert_risk_evaluable(risk)
    weights = plan["targets"]

    codes = set(weights) | set(previous_weights)
    turnover = 0.5 * sum(
        abs(float(weights.get(code, 0.0)) - float(previous_weights.get(code, 0.0)))
        for code in codes
    )
    verdict = check_budget(
        risk=risk,
        position_count=sum(1 for weight in weights.values() if weight > 0),
        gross_exposure_pct=sum(weights.values()),
        cash_pct=plan["cash_weight"],
        turnover_pct=turnover,
        drawdown_pct=0.0,  # no drawdown stop is configured; guarded above
        largest_weight_pct=max(weights.values()) if weights else 0.0,
        regime=SIDEWAYS,
    )
    if not verdict["admissible"]:
        raise AllocationRunError(
            "risk budget refused the plan: "
            f"stops={verdict['stops']} violations={verdict['violations']}"
        )
    truncate_to = verdict.get("truncate_to")
    if truncate_to is not None and len(weights) > truncate_to:
        # Keep the largest positions; a count cap cannot be met by shrinking.
        keep = sorted(weights.items(), key=lambda item: (-item[1], item[0]))[
            :truncate_to
        ]
        weights = dict(keep)
        # The dropped names release their weight to cash; keeping the old cash
        # figure would claim a fully invested book that no longer exists.
        plan = dict(
            plan, targets=weights, cash_weight=max(0.0, 1.0 - sum(weights.values()))
        )
    if verdict["scale"] < 1.0:
        scaled = {code: weight * verdict["scale"] for code, weight in weights.items()}
        plan = dict(
            plan,
            targets=scaled,
            cash_weight=max(0.0, 1.0 - sum(scaled.values())),
        )
    plan["risk_verdict"] = verdict
    return plan


MARKET_NAME = "ChinaAStock"


def next_execution_date(decision_day) -> str:
    """The next exchange session after the decision date.

    It cannot be derived from the quotes: on the decision session the next
    session has not been quoted yet, so a live plan would get ``None`` and never
    count as forward evidence. The exchange calendar is the authority.
    """
    from app.model.stock import FinanceMarket

    day = _session_date(decision_day)
    market = FinanceMarket.objects(name=MARKET_NAME).only("trade_calendar").first()
    calendar = sorted(
        str(value)[:10] for value in (market.trade_calendar if market else [])
    )
    upcoming = next((value for value in calendar if value > day.isoformat()), None)
    if upcoming is None:
        raise AllocationRunError(
            f"the exchange calendar has no session after {day.isoformat()}; refusing "
            "to record a plan that could never be counted as forward evidence"
        )
    return upcoming


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


def session_calendar(codes) -> list[str]:
    """Sessions where *every* declared asset has a quote.

    Deriving the calendar from one asset would let a gap in that one asset shift
    the execution date for the whole plan.
    """
    per_code = [set(trading_days_for(code)) for code in codes]
    if not per_code:
        return []
    return sorted(set.intersection(*per_code))


class PlanStore:
    """Persistence seam for allocation plans.

    The runner talks to this interface instead of a MongoEngine document, so the
    write path — create, retry, refuse-after-execution — is testable without a
    database, and the durable behaviour lives in one thin adapter.
    """

    def latest(self, strategy_id: str, spec_hash: str, before) -> dict | None:
        raise NotImplementedError

    def find(self, strategy_id: str, day) -> dict | None:
        raise NotImplementedError

    def save(self, plan: dict, *, decision_at, execution_date) -> None:
        raise NotImplementedError

    def list_decided(self, strategy_id: str, spec_hash: str) -> list[str]:
        raise NotImplementedError


class MongoPlanStore(PlanStore):
    """Durable adapter over the ``allocation_plans`` collection."""

    def _model(self):
        from app.model.strategy import AllocationPlan

        return AllocationPlan

    def latest(self, strategy_id: str, spec_hash: str, before) -> dict | None:
        day = _session_date(before)
        record = (
            self._model()
            .objects(
                strategy_id=strategy_id,
                spec_hash=spec_hash,
                date__lt=dt.datetime.combine(day, dt.time.min),
                status="PLANNED",
            )
            .order_by("-date")
            .first()
        )
        return _record_to_dict(record)

    def find(self, strategy_id: str, day) -> dict | None:
        record = (
            self._model()
            .objects(
                strategy_id=strategy_id,
                date=dt.datetime.combine(
                    _session_date(day), dt.time.min, tzinfo=dt.timezone.utc
                ),
            )
            .first()
        )
        return _record_to_dict(record)

    def save(self, plan: dict, *, decision_at, execution_date) -> None:
        model = self._model()
        record_date = dt.datetime.combine(
            _session_date(plan["date"]), dt.time.min, tzinfo=dt.timezone.utc
        )
        record = model.objects(
            strategy_id=plan["strategy_id"], date=record_date
        ).first()
        if record is None:
            record = model(strategy_id=plan["strategy_id"], date=record_date)
        record.spec_hash = plan["spec_hash"]
        record.execution_date = execution_date
        record.decision_at = decision_at
        record.targets = plan["targets"]
        record.actions = plan["actions"]
        record.cash_weight = float(plan["cash_weight"])
        record.reference_prices = {
            code: float(price) for code, price in plan["reference_prices"].items()
        }
        record.marks = {code: float(price) for code, price in plan["marks"].items()}
        record.last_rebalance_date = (
            dt.datetime.fromisoformat(plan["last_rebalance_date"])
            if plan.get("last_rebalance_date")
            else None
        )
        record.rebalance_session = bool(plan["rebalance_session"])
        record.status = "PLANNED"
        record.save()

    def list_decided(self, strategy_id: str, spec_hash: str) -> list[str]:
        records = self._model().objects(
            strategy_id=strategy_id, spec_hash=spec_hash, status="PLANNED"
        )
        return sorted(
            {
                str(record.date)[:10]
                for record in records
                if record.decision_at
                and record.execution_date
                and record.decision_at.date() < record.execution_date.date()
            }
        )


def _record_to_dict(record) -> dict | None:
    if record is None:
        return None
    return {
        "date": str(record.date)[:10],
        "spec_hash": record.spec_hash,
        "targets": dict(record.targets or {}),
        "reference_prices": dict(record.reference_prices or {}),
        "marks": dict(record.marks or {}),
        "cash_weight": record.cash_weight,
        "last_rebalance_date": (
            str(record.last_rebalance_date)[:10] if record.last_rebalance_date else None
        ),
        "decision_at": record.decision_at,
        "execution_date": record.execution_date,
    }


def run(
    *,
    spec: dict,
    strategy_id: str,
    session_date=None,
    dry_run: bool = False,
    store: "PlanStore | None" = None,
    execution_date_resolver=next_execution_date,
) -> dict:
    """Build one plan; write it unless ``dry_run``."""
    store = store or MongoPlanStore()

    day = _session_date(session_date)
    spec = dict(spec)
    spec["_spec_hash"] = spec_hash(
        validate_strategy_spec(
            {key: value for key, value in spec.items() if key != "_spec_hash"}
        )
    )
    assert_risk_evaluable(spec.get("risk") or {})
    codes = list(validate_allocation(spec["allocation"])["assets"])
    quotes = load_quotes(codes, day)
    days = session_calendar(codes)
    previous = store.latest(strategy_id, spec["_spec_hash"], day)
    plan = build_plan(
        spec=spec,
        strategy_id=strategy_id,
        session_date=day,
        quotes=quotes,
        trading_days=days,
        previous=previous,
    )
    plan["execution_date"] = execution_date_resolver(day)
    if dry_run:
        plan["dry_run"] = True
        return plan

    now = dt.datetime.now(dt.timezone.utc)
    execution_date = (
        dt.datetime.fromisoformat(plan["execution_date"])
        if plan.get("execution_date")
        else None
    )
    existing = store.find(strategy_id, day)
    if existing is not None and existing.get("execution_date") is not None:
        # A correction is allowed only before the execution session opens. After
        # that the plan is history: rewriting it would let a late edit look like a
        # timely decision and turn a backfill into forward evidence.
        if now.date() >= existing["execution_date"].date():
            raise AllocationRunError(
                f"plan for {plan['date']} already executed on "
                f"{str(existing['execution_date'])[:10]}; refusing to rewrite it"
            )
    store.save(plan, decision_at=now, execution_date=execution_date)
    plan["saved"] = True
    return plan


def load_declaration(path) -> dict:
    """Read and validate a v2 declaration from JSON, computing its identity."""
    import json
    from pathlib import Path

    declaration = json.loads(Path(path).read_text(encoding="utf-8"))
    declaration.pop("_spec_hash", None)
    try:
        validated = validate_strategy_spec(declaration)
    except (ValueError, TypeError) as exc:
        raise AllocationRunError(
            f"declaration {path} is not admissible: {exc}"
        ) from exc
    validated["_spec_hash"] = spec_hash(validated)
    return validated


def main(argv=None) -> int:
    """Entry point: one plan, or a loud refusal.

    Exits non-zero on every failure. A job that exits 0 without writing a plan
    would let a scheduler report success while the evidence clock accrues gaps.
    """
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Daily allocation plan (research only)"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run", help="Build and persist one plan")
    run_parser.add_argument("--spec", required=True, help="v2 declaration JSON path")
    run_parser.add_argument("--strategy-id", required=True)
    run_parser.add_argument("--date", default=None, help="decision session (ISO date)")
    run_parser.add_argument("--dry-run", action="store_true")
    progress_parser = sub.add_parser("progress", help="Report the forward window")
    progress_parser.add_argument(
        "--spec", required=True, help="v2 declaration JSON path"
    )
    progress_parser.add_argument("--strategy-id", required=True)
    args = parser.parse_args(argv)

    if args.command == "progress":
        from app.lib.strategy_engine.allocation_window import (
            allocation_forward_progress,
        )
        from app.model.strategy import AllocationForwardWindow

        window = AllocationForwardWindow.objects(
            strategy_id=args.strategy_id, status="ACTIVE"
        ).first()
        if window is None:
            print(
                json.dumps({"error": "no ACTIVE allocation window"}, ensure_ascii=False)
            )
            return 1
        try:
            declaration = load_declaration(args.spec)
        except AllocationRunError as exc:
            print(
                json.dumps(
                    {"status": "REFUSED", "reason": str(exc)}, ensure_ascii=False
                )
            )
            return 2
        decided = MongoPlanStore().list_decided(args.strategy_id, window.spec_hash)
        report = allocation_forward_progress(
            window={
                "strategy_id": window.strategy_id,
                "start_date": str(window.start_date)[:10],
                "status": window.status,
                "closed_at": str(window.closed_at)[:10] if window.closed_at else None,
            },
            decided_dates=decided,
            trading_days=session_calendar(list(declaration["allocation"]["assets"])),
        )
        print(json.dumps(report, ensure_ascii=False))
        return 0

    try:
        spec = load_declaration(args.spec)
        result = run(
            spec=spec,
            strategy_id=args.strategy_id,
            session_date=args.date,
            dry_run=args.dry_run,
        )
    except AllocationRunError as exc:
        print(json.dumps({"status": "REFUSED", "reason": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
