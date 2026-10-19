# -*- coding: utf-8 -*-
"""Build and validate a read-only strategy replay artifact.

The artifact is the research record the API and UI render. It is deliberately
separate from the frozen ``factor_lab_paper_run.py`` ledger and from
``StrategyPaperRun`` / ``StrategyForwardWindow``: it never reads their state and
never advances forward evidence. This module is pure; persistence is the
operator-run runner's job.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import math
import re

SCHEMA_VERSION = "strategy-replay-v1"
EVIDENCE_KIND = "REPLAY"
RESEARCH_GRADE = "RESEARCH"
FORWARD_REQUIRED = 120
MAX_SESSIONS = 5_000
DISCLAIMER = "研究记录，不构成投资建议。历史回放不是前瞻验证，不能作为上钱依据。"
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,127}$")


def canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_input_hash(
    *, strategy: str, start_date: str, end_date: str, aum: float, config: dict
) -> str:
    payload = {
        "strategy": strategy,
        "start_date": start_date,
        "end_date": end_date,
        "aum": round(float(aum), 6),
        "config": config,
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _parse_date(value) -> datetime.date:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value)[:10])


def _finite(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _metrics(curve: list[dict], fills: list[dict]) -> dict:
    navs = [float(point["nav"]) for point in curve]
    first, last = navs[0], navs[-1]
    total_return = last / first - 1.0 if first else None
    days = (_parse_date(curve[-1]["date"]) - _parse_date(curve[0]["date"])).days
    annualised = None
    if days > 0 and first > 0 and last > 0:
        annualised = (last / first) ** (365.25 / days) - 1.0
    returns = [
        navs[index] / navs[index - 1] - 1.0
        for index in range(1, len(navs))
        if navs[index - 1]
    ]
    sharpe = None
    if len(returns) > 1:
        mean = sum(returns) / len(returns)
        variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
        std = math.sqrt(variance)
        if std > 0:
            sharpe = mean / std * math.sqrt(252.0)
    peak = -math.inf
    max_drawdown = 0.0
    for nav in navs:
        peak = max(peak, nav)
        if peak > 0:
            max_drawdown = min(max_drawdown, nav / peak - 1.0)
    cash_shares = [
        float(point["cash"]) / float(point["nav"])
        for point in curve
        if _finite(point.get("cash")) and float(point.get("nav") or 0) > 0
    ]
    fees = 0.0
    traded = 0.0
    for fill in fills:
        if _finite(fill.get("quantity")) and _finite(fill.get("price")):
            traded += float(fill["quantity"]) * float(fill["price"])
        if _finite(fill.get("total_cost")):
            fees += float(fill["total_cost"])
            continue
        for key in ("commission", "stamp_duty", "transfer_fee", "slippage"):
            value = fill.get(key)
            if _finite(value):
                fees += float(value)
    benchmark = [
        float(point["benchmark_nav"])
        for point in curve
        if _finite(point.get("benchmark_nav"))
    ]
    benchmark_return = None
    if len(benchmark) >= 2 and benchmark[0]:
        benchmark_return = benchmark[-1] / benchmark[0] - 1.0
    average_nav = sum(navs) / len(navs) if navs else 0.0
    years = days / 365.25 if days > 0 else 0.0
    annual_turnover = (
        traded / average_nav / years if average_nav > 0 and years > 0.5 else None
    )
    return {
        "total_return": round(total_return, 6) if total_return is not None else None,
        "annualised": round(annualised, 6) if annualised is not None else None,
        "sharpe": round(sharpe, 6) if sharpe is not None else None,
        "max_drawdown": round(max_drawdown, 6),
        "annual_turnover": round(annual_turnover, 6)
        if annual_turnover is not None
        else None,
        "total_fees": round(fees, 2),
        "average_cash_share": round(sum(cash_shares) / len(cash_shares), 6)
        if cash_shares
        else None,
        "benchmark_return": round(benchmark_return, 6)
        if benchmark_return is not None
        else None,
    }


def _validate_curve(curve: list[dict]) -> None:
    if not curve:
        raise ValueError("curve must not be empty")
    if len(curve) > MAX_SESSIONS:
        raise ValueError(
            f"curve has {len(curve)} sessions; the artifact is bounded at "
            f"{MAX_SESSIONS}"
        )
    dates = [str(point.get("date")) for point in curve]
    if dates != sorted(dates):
        raise ValueError("curve dates must be sorted ascending")
    if len(set(dates)) != len(dates):
        raise ValueError("curve dates must be unique")
    required = ("cash", "benchmark_nav", "positions_count")
    for point in curve:
        missing = [key for key in required if key not in point]
        if missing:
            raise ValueError(
                f"curve point {point.get('date')} is missing {missing}; the "
                "artifact contract requires cash, benchmark_nav and positions_count"
            )
        if not _finite(point.get("nav")) or float(point["nav"]) <= 0:
            raise ValueError(f"invalid nav at {point.get('date')}")


def _validate_traceability(decisions: list[dict], fills: list[dict]) -> None:
    ids = []
    for decision in decisions:
        decision_id = str(decision.get("decision_id") or "")
        if not decision_id:
            raise ValueError("every decision needs a decision_id")
        if decision_id in ids:
            raise ValueError(f"duplicate decision_id {decision_id}")
        ids.append(decision_id)
        if "date" not in decision or "execution_date" not in decision:
            raise ValueError(f"decision {decision_id} needs date and execution_date")
        date = _parse_date(decision["date"])
        execution = _parse_date(decision["execution_date"])
        if execution <= date:
            raise ValueError(
                f"decision {decision_id} execution_date must be strictly later"
            )
        total = 0.0
        for row in decision.get("target_holdings") or []:
            weight = float(row.get("weight", 0))
            if weight < 0 or not math.isfinite(weight):
                raise ValueError(f"invalid target weight in {decision_id}")
            total += weight
        if total > 1.0 + 1e-9:
            raise ValueError(f"target weights exceed 1 in {decision_id}")
    known = set(ids)
    for fill in fills:
        decision_id = str(fill.get("decision_id") or "")
        if decision_id not in known:
            raise ValueError(f"fill references unknown decision_id {decision_id}")
        if not _finite(fill.get("quantity")) or float(fill["quantity"]) <= 0:
            raise ValueError(f"invalid fill quantity in {decision_id}")
        if not _finite(fill.get("price")) or float(fill["price"]) <= 0:
            raise ValueError(f"invalid fill price in {decision_id}")


def build_replay_artifact(
    *,
    replay_id: str,
    name: str,
    strategy: str,
    start_date: str,
    end_date: str,
    aum: float,
    config: dict,
    config_hash: str,
    source_artifact: str,
    curve: list[dict],
    decisions: list[dict],
    fills: list[dict],
    blocked_orders: list[dict] | None = None,
    limitations: list[str] | None = None,
    survivorship_gap: bool = True,
    description: str = "",
    metrics: dict | None = None,
    generated_at: datetime.datetime | None = None,
) -> dict:
    if not _ID_RE.match(replay_id or ""):
        raise ValueError("replay_id must be a lowercase slug")
    _validate_curve(curve)
    _validate_traceability(decisions, fills)
    if not config_hash:
        raise ValueError("config_hash is required")
    if not source_artifact:
        raise ValueError("source_artifact is required")
    data_as_of = max(str(point["date"]) for point in curve)
    normalized_fills = []
    for fill in fills:
        copied = dict(fill)
        copied.setdefault("execution_date", copied.get("date"))
        normalized_fills.append(copied)
    fills = normalized_fills
    input_hash = compute_input_hash(
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        aum=aum,
        config=config,
    )
    computed = _metrics(curve, fills)
    if metrics:
        computed.update(
            {key: value for key, value in metrics.items() if value is not None}
        )
    artifact = {
        "replay_id": replay_id,
        "schema_version": SCHEMA_VERSION,
        "source_artifact": source_artifact,
        "name": name,
        "strategy": strategy,
        "description": description,
        "start_date": start_date,
        "end_date": end_date,
        "data_as_of": data_as_of,
        "aum": round(float(aum), 2),
        "config": config,
        "config_hash": config_hash,
        "input_hash": input_hash,
        "evidence_kind": EVIDENCE_KIND,
        "research_grade": RESEARCH_GRADE,
        "forward_sessions": 0,
        "forward_required": FORWARD_REQUIRED,
        "forward_eligible": False,
        "survivorship_gap": bool(survivorship_gap),
        "limitations": list(limitations or []),
        "disclaimer": DISCLAIMER,
        "metrics": computed,
        "curve": curve,
        "decisions": decisions,
        "fills": fills,
        "blocked_orders": list(blocked_orders or []),
        "generated_at": (
            generated_at or datetime.datetime.now(datetime.timezone.utc)
        ).isoformat(),
    }
    if artifact["survivorship_gap"]:
        marker = "上界"
        if not any(marker in str(item) for item in artifact["limitations"]):
            artifact["limitations"].append(
                "面板存在生存者偏差（无退市 cohort），历史收益应视为上界；"
                "补全退市股后的 OOS 重跑尚未完成。"
            )
    artifact["content_hash"] = hashlib.sha256(
        canonical_json(
            {
                "replay_id": replay_id,
                "name": name,
                "description": description,
                "config_hash": config_hash,
                "input_hash": input_hash,
                "survivorship_gap": bool(survivorship_gap),
                "limitations": artifact["limitations"],
                "data_as_of": data_as_of,
                "metrics": computed,
                "curve": curve,
                "decisions": decisions,
                "fills": fills,
                "blocked_orders": artifact["blocked_orders"],
            }
        ).encode("utf-8")
    ).hexdigest()
    return artifact
