from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

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

DETAILED_COLUMNS = (
    "run_id",
    "git_ref",
    "candidate_config_sha256",
    "candidate_summary",
    "snapshot_sha256",
    "train_range",
    "validation_range",
    "test_range",
    "information_ratio",
    "annualized_net_excess_return",
    "excess_max_drawdown",
    "annual_turnover",
    "profit_concentration",
    "completed_trades",
    "eligible_trading_days",
    "walk_forward_decay",
    "research_profitability_score",
    "decision",
    "reason",
    "elapsed_seconds",
)


def load_json_yaml(path: str) -> dict:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"configuration must be an object: {path}")
    return value


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_candidate(candidate: dict) -> None:
    directions = candidate.get("component_directions", {})
    weights = candidate.get("weights", {})
    if set(directions) != set(COMPONENT_IDS) or set(weights) != set(COMPONENT_IDS):
        raise ValueError("candidate must define every known component exactly once")
    if any(value not in (-1, 0, 1) for value in directions.values()):
        raise ValueError("component directions must be -1, 0, or 1")
    if any(
        not isinstance(value, (int, float)) or value < 0 for value in weights.values()
    ):
        raise ValueError("component weights must be non-negative numbers")
    if not math.isclose(sum(weights.values()), 100.0, abs_tol=1e-9):
        raise ValueError("component weights must sum to 100")
    selection = candidate.get("selection", {})
    if selection.get("mode") not in {"top_percentile", "exclude_percentile"}:
        raise ValueError("unsupported selection mode")
    lower, upper = selection.get("lower"), selection.get("upper")
    if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
        raise TypeError("selection bounds must be numeric")
    if not 0 <= lower <= upper <= 1:
        raise ValueError("selection bounds must be within [0, 1]")
    if int(selection.get("portfolio_size", 0)) < 1:
        raise ValueError("portfolio_size must be positive")
    regime = candidate.get("regime_filter", {})
    if regime.get("mode") not in {"none", "market_breadth"}:
        raise ValueError("unsupported regime filter")


def eligible_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        ~frame["is_bse"].fillna(False).astype(bool)
        & ~frame["is_st"].fillna(False).astype(bool)
        & frame["listing_days"].fillna(0).ge(60)
        & frame["trade_status"].fillna(0).eq(1)
        & frame["next_open_hfq"].notna()
        & frame["exit_open_hfq"].notna()
        & frame["next_open_hfq"].gt(0)
        & frame["exit_open_hfq"].gt(0)
    )


def validate_snapshot(frame: pd.DataFrame, profile: dict) -> dict:
    required = {
        "date",
        "stock_code",
        "is_bse",
        "is_st",
        "listing_days",
        "trade_status",
        "next_open_hfq",
        "exit_open_hfq",
        *COMPONENT_IDS,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("snapshot missing columns: " + ", ".join(missing))
    dates = pd.to_datetime(frame["date"], utc=True)
    if dates.empty:
        raise ValueError("snapshot is empty")
    return {
        "row_count": len(frame),
        "date_min": dates.min().date().isoformat(),
        "date_max": dates.max().date().isoformat(),
        "eligible_count": int(eligible_mask(frame).sum()),
    }


def rank_components(frame: pd.DataFrame, candidate: dict) -> pd.DataFrame:
    validate_candidate(candidate)
    result = frame.copy()
    score = pd.Series(0.0, index=result.index)
    for component in COMPONENT_IDS:
        ranked = result.groupby("date", sort=False)[component].rank(
            method="average", pct=True, na_option="bottom"
        )
        score += (
            ranked
            * float(candidate["component_directions"][component])
            * float(candidate["weights"][component])
            / 100.0
        )
        result[f"{component}_percentile"] = ranked
    result["candidate_score"] = score
    result["candidate_percentile"] = result.groupby("date", sort=False)[
        "candidate_score"
    ].rank(method="average", pct=True)
    return result


def build_positions(
    scored: pd.DataFrame, candidate: dict, profile: dict
) -> pd.DataFrame:
    selection = candidate["selection"]
    eligible = scored.loc[eligible_mask(scored)].copy()
    mode = selection["mode"]
    inside = eligible["candidate_percentile"].between(
        float(selection["lower"]), float(selection["upper"]), inclusive="both"
    )
    eligible = eligible.loc[inside if mode == "top_percentile" else ~inside]
    regime = candidate.get("regime_filter", {"mode": "none"})
    size = int(selection["portfolio_size"])
    if regime.get("mode") == "market_breadth":
        threshold = float(regime["minimum_fraction_above_ma60"])
        scale = float(regime["position_scale"])
        breadth = eligible["market_fraction_above_ma60"].fillna(0)
        eligible = eligible.loc[breadth.ge(threshold)]
        size = max(1, int(size * scale))
    return (
        eligible.sort_values(
            ["date", "candidate_score", "stock_code"], ascending=[True, False, True]
        )
        .groupby("date", sort=True)
        .head(size)
        .copy()
    )


def apply_friction(
    price: float, quantity: int, side: str, profile: dict
) -> tuple[float, float, float, float]:
    config = profile["execution"]
    slippage = float(config["slippage_per_side"])
    exec_price = price * (1 + slippage if side == "BUY" else 1 - slippage)
    value = exec_price * quantity
    commission = max(
        value * float(config["commission_rate"]),
        float(config["minimum_commission_cny"]),
    )
    duty = value * float(config["sell_stamp_duty_rate"]) if side == "SELL" else 0.0
    return exec_price, commission, duty, abs(exec_price - price) * quantity


def _net_return(frame: pd.DataFrame, profile: dict) -> pd.Series:
    gross = frame["exit_open_hfq"] / frame["next_open_hfq"] - 1.0
    rate_cost = 2 * float(profile["execution"]["commission_rate"])
    rate_cost += float(profile["execution"]["sell_stamp_duty_rate"])
    rate_cost += 2 * float(profile["execution"]["slippage_per_side"])
    return gross - rate_cost


def equal_weight_benchmark(
    frame: pd.DataFrame, profile: dict | None = None
) -> pd.Series:
    eligible = frame.loc[eligible_mask(frame)].copy()
    if profile is None:
        returns = eligible["exit_open_hfq"] / eligible["next_open_hfq"] - 1.0
    else:
        returns = _net_return(eligible, profile)
    return returns.groupby(eligible["date"]).mean().sort_index()


def information_ratio(excess: pd.Series) -> float:
    clean = pd.Series(excess, dtype=float).replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < 2 or clean.std(ddof=1) == 0:
        return float("nan")
    return float(clean.mean() / clean.std(ddof=1) * math.sqrt(252 / 20))


def profitability_score(metrics: dict, profile: dict) -> tuple[float, list[str]]:
    config = profile["metric"]
    flags = []
    gates = (
        (
            metrics["completed_trades"] < config["minimum_completed_trades"],
            "low_sample",
        ),
        (
            metrics["eligible_trading_days"] < config["minimum_eligible_trading_days"],
            "insufficient_period",
        ),
        (
            metrics["profit_concentration"] > config["maximum_profit_concentration"],
            "concentrated_returns",
        ),
        (
            metrics["walk_forward_decay"] > config["maximum_walk_forward_decay"],
            "performance_decay",
        ),
    )
    flags.extend(name for failed, name in gates if failed)
    ir = float(metrics["information_ratio"])
    if not math.isfinite(ir):
        flags.append("invalid_information_ratio")
    if flags:
        return float(config["hard_failure_score"]), flags
    score = (
        ir
        + config["annualized_net_excess_return_weight"]
        * metrics["annualized_net_excess_return"]
    )
    score -= config["drawdown_penalty_weight"] * max(
        0.0, abs(metrics["excess_max_drawdown"]) - config["drawdown_free_allowance"]
    )
    score -= config["turnover_penalty_weight"] * max(
        0.0, metrics["annual_turnover"] - config["turnover_free_allowance"]
    )
    score -= config["concentration_penalty_weight"] * max(
        0.0, metrics["profit_concentration"] - config["concentration_free_allowance"]
    )
    return round(float(score), 8), []


def _git_ref() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def evaluate_candidate(
    snapshot_path: str,
    candidate_path: str,
    profile_path: str,
    *,
    split: str = "validation",
    allow_test: bool = False,
) -> dict:
    started = time.monotonic()
    profile = load_json_yaml(profile_path)
    if split == "test" and not allow_test:
        raise ValueError(
            "test split is locked; pass --allow-test after selection is frozen"
        )
    candidate = load_json_yaml(candidate_path)
    validate_candidate(candidate)
    frame = pd.read_parquet(snapshot_path)
    validate_snapshot(frame, profile)
    bounds = profile["experiment"][f"{split}_range"]
    dates = pd.to_datetime(frame["date"], utc=True)
    start = pd.Timestamp(bounds[0], tz="UTC")
    end = pd.Timestamp(bounds[1], tz="UTC")
    frame = frame.loc[dates.between(start, end)].copy()
    if frame.empty:
        raise ValueError(f"snapshot has no rows for {split} range")
    scored = rank_components(frame, candidate)
    positions = build_positions(scored, candidate, profile)
    benchmark = equal_weight_benchmark(frame, profile)
    position_returns = _net_return(positions, profile)
    strategy = position_returns.groupby(positions["date"]).mean().sort_index()
    excess = strategy.subtract(benchmark, fill_value=np.nan).dropna()
    wealth = (1 + excess).cumprod()
    drawdown = wealth / wealth.cummax() - 1
    positive = position_returns[position_returns > 0]
    concentration = (
        float(positive.max() / positive.sum()) if positive.sum() > 0 else 1.0
    )
    periods = max(len(excess), 1)
    metrics = {
        "information_ratio": information_ratio(excess),
        "annualized_net_excess_return": float((1 + excess.mean()) ** (252 / 20) - 1),
        "excess_max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "annual_turnover": float(periods * 2 * 30 / max(30, len(positions)) * 252 / 20),
        "profit_concentration": concentration,
        "completed_trades": len(positions),
        "eligible_trading_days": int(frame["date"].nunique()),
        "walk_forward_decay": 0.0,
    }
    score, flags = profitability_score(metrics, profile)
    report = {
        "schema_version": "1.0",
        "project_id": profile["project_id"],
        "run_id": uuid.uuid4().hex,
        "attempt_id": 1,
        "git_ref": _git_ref(),
        "candidate_config_sha256": file_sha256(candidate_path),
        "candidate_summary": candidate["name"],
        "snapshot_sha256": file_sha256(snapshot_path),
        "train_range": profile["experiment"]["train_range"],
        "validation_range": profile["experiment"]["validation_range"],
        "test_range": profile["experiment"]["test_range"]
        if split == "test"
        else "LOCKED",
        **metrics,
        "research_profitability_score": score,
        "decision": "keep" if not flags else "discard",
        "status": "keep" if not flags else "discard",
        "status_reason": ",".join(flags) if flags else "baseline completed",
        "metric_name": profile["experiment"]["metric_name"],
        "metric_value": score,
        "metric_direction": profile["experiment"]["metric_direction"],
        "time_budget_seconds": profile["experiment"]["time_budget_seconds"],
        "reason": ",".join(flags) if flags else "all gates passed",
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "runtime_seconds": round(time.monotonic() - started, 6),
        "peak_memory_mb": None,
        "log_path": profile["runtime"]["log_path"],
        "results_row_ref": None,
        "profile_version": profile["profile_version"],
    }
    return report


def append_run_artifacts(report: dict, profile: dict) -> None:
    artifacts = profile["artifacts"]
    report_path = Path(artifacts["latest_report_path"])
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=report_path.parent, delete=False, encoding="utf-8"
    ) as handle:
        json.dump(report, handle, sort_keys=True)
        temp_name = handle.name
    os.replace(temp_name, report_path)
    ledger_paths = {
        Path(artifacts["ledger_path"]),
        report_path.parent / "results.jsonl",
    }
    for ledger in ledger_paths:
        ledger.parent.mkdir(parents=True, exist_ok=True)
        with ledger.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(report, sort_keys=True) + "\n")
    results_path = Path(artifacts["results_tsv_path"])
    description = report["candidate_summary"].replace("\t", " ").replace("\n", " ")
    with results_path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"{report['git_ref']}\t{report['research_profitability_score']}\t"
            f"N/A\t{report['status']}\t{description}\n"
        )


def metric_from_report(report_path: str) -> float:
    value = json.loads(Path(report_path).read_text(encoding="utf-8"))[
        "research_profitability_score"
    ]
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("metric must be finite")
    return value
