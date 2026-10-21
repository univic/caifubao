# -*- coding: utf-8 -*-
"""Build the balanced 50k core-satellite replay payload.

Inputs:
  * a satellite JSON produced by ``factor_lab_account_replay.py --detail``
    (its ``decision_time_daily`` arm), and
  * a wide ETF close parquet, or a live tushare fetch for the three core ETFs.

Output: a JSON payload consumed by ``app.jobs.strategy_replay_runner``. This is
research tooling; it never places an order and never writes Mongo itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.strategy_engine.core_satellite import (  # noqa: E402
    DEFAULT_CORE_WEIGHTS,
    combine_sleeves,
    simulate_core,
)
from app.lib.strategy_engine.replay import canonical_json  # noqa: E402

CORE_AUM = 25_000.0
SATELLITE_AUM = 20_000.0
CASH_AUM = 5_000.0
TOTAL_AUM = CORE_AUM + SATELLITE_AUM + CASH_AUM
FROZEN_COMPONENTS = frozenset(
    {"reversal_10", "amihud_20", "trend_60", "volatility_20", "rsi_14"}
)
FROZEN_NAMES = 10
FROZEN_LABEL = "fwd_h20"
FROZEN_LAG = 21


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--satellite-json", required=True)
    parser.add_argument("--core-prices", default=None, help="Wide close parquet")
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--replay-id", default=None)
    parser.add_argument("--cadence", type=int, default=60)
    parser.add_argument(
        "--audit-json",
        default=None,
        help="Optional research_universe_audit.py output: carries the "
        "survivorship_gap flag and limitations into the artifact.",
    )
    return parser.parse_args(argv)


def load_satellite(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    arm = payload.get("decision_time_daily") or {}
    curve = arm.get("nav") or []
    decisions = arm.get("decisions") or []
    fills = arm.get("fills") or []
    if not curve or not decisions:
        raise ValueError("satellite JSON lacks decision_time_daily nav/decisions")
    return {
        "payload": payload,
        "arm": arm,
        "curve": curve,
        "decisions": decisions,
        "fills": fills,
    }


def assert_frozen_satellite(satellite: dict, cadence: int) -> dict:
    """Refuse a satellite build that is not the frozen riq_v1 configuration."""
    payload = satellite["payload"]
    aum = float(payload.get("aum") or 0)
    if aum != SATELLITE_AUM:
        raise ValueError(
            f"satellite AUM is {aum!r}, expected {SATELLITE_AUM:.0f}; rebuild "
            "the satellite at --aum 20000 instead of scaling another curve"
        )
    names = int(payload.get("names") or 0)
    if names != FROZEN_NAMES:
        raise ValueError(f"satellite names is {names}, expected {FROZEN_NAMES}")
    label = payload.get("weight_label")
    if label != FROZEN_LABEL:
        raise ValueError(
            f"satellite weight_label is {label!r}, expected {FROZEN_LABEL!r}"
        )
    lag = int(payload.get("weight_label_lag_sessions") or 0)
    if lag != FROZEN_LAG:
        raise ValueError(f"satellite weight lag is {lag}, expected {FROZEN_LAG}")
    step = int(payload.get("step_sessions") or 0)
    if step != int(cadence):
        raise ValueError(
            f"satellite step is {step}, expected the combined cadence {cadence}"
        )
    components = frozenset(payload.get("components") or [])
    if components != FROZEN_COMPONENTS:
        raise ValueError(
            f"satellite components are {sorted(components)}, expected "
            f"{sorted(FROZEN_COMPONENTS)}"
        )
    return {
        "aum": aum,
        "names": names,
        "weight_label": label,
        "weight_label_lag_sessions": lag,
        "step_sessions": step,
        "components": sorted(components),
    }


def fetch_core_prices(from_date: str, to_date: str) -> pd.DataFrame:
    import os

    import tushare as ts

    from app.lib.etf_lab.pipeline import fetch_prices

    pro = ts.pro_api(os.environ["TUSHARE_TOKEN"])
    long = fetch_prices(
        pro,
        list(DEFAULT_CORE_WEIGHTS),
        from_date.replace("-", ""),
        to_date.replace("-", ""),
    )
    if long.empty:
        raise ValueError("tushare returned no core ETF prices")
    long["date"] = pd.to_datetime(long["trade_date"], format="%Y%m%d")
    long["close_hfq"] = pd.to_numeric(long["close"], errors="coerce") * pd.to_numeric(
        long["adj_factor"], errors="coerce"
    )
    wide = long.pivot(index="date", columns="ts_code", values="close_hfq").sort_index()
    missing = [code for code in DEFAULT_CORE_WEIGHTS if code not in wide.columns]
    if missing:
        raise ValueError(f"core ETF price frame is missing {missing}")
    return wide


def _next_date(dates: list[str], day: str) -> str:
    index = dates.index(day)
    if index + 1 >= len(dates):
        return day
    return dates[index + 1]


def _core_activity(
    core: dict, dates: list[str]
) -> tuple[list[dict], list[dict], list[dict]]:
    """Core decisions/fills/skips with traceable decision ids."""
    fills_by_date: dict[str, list[dict]] = {}
    for fill in core["fills"]:
        fills_by_date.setdefault(fill["date"], []).append(fill)
    decisions = []
    fills = []
    blocked = []
    previous_codes: set[str] = set()
    for row in core["decisions"]:
        day = row["date"]
        if dates.index(day) + 1 >= len(dates):
            # No later execution session to name; the last-session decision is
            # not executable and must not be invented as an artifact decision.
            continue
        decision_id = "core_" + day
        codes = sorted((row.get("targets") or {}).keys())
        cash_after = float(row["cash_after"])
        day_fills = []
        for fill in fills_by_date.get(day, []):
            copied = dict(fill)
            total_cost = float(fill.get("commission") or 0) + float(
                fill.get("transfer_fee") or 0
            )
            copied["decision_id"] = decision_id
            copied["total_cost"] = round(total_cost, 2)
            copied["sleeve"] = "core"
            fills.append(copied)
            day_fills.append(copied)
        decisions.append(
            {
                "decision_id": decision_id,
                "date": day,
                "execution_date": _next_date(dates, day),
                "nav_before": float(row["nav_before"]),
                "cash_before": float(row["cash_before"]),
                "cash_after": round(cash_after, 2),
                "target_holdings": [
                    {"code": code, "weight": float(row["targets"][code])}
                    for code in codes
                ],
                "added": [code for code in codes if code not in previous_codes],
                "removed": [
                    code for code in sorted(previous_codes) if code not in codes
                ],
                "estimated_cost": round(
                    sum(float(item["total_cost"]) for item in day_fills), 2
                ),
                "sleeve": "core",
            }
        )
        previous_codes = set(codes)
    valid_days = set(dates[:-1])
    for skip in core.get("skipped") or []:
        if skip["date"] not in valid_days:
            continue
        blocked.append(
            {
                "decision_id": "core_" + skip["date"],
                "date": skip["date"],
                "code": skip["code"],
                "side": "BUY",
                "reason": "core_board_minimum_skip: " + skip["reason"],
            }
        )
    return decisions, fills, blocked


def _satellite_activity(satellite: dict) -> tuple[list[dict], list[dict], list[dict]]:
    decisions = []
    for row in satellite["decisions"]:
        copied = dict(row)
        copied["sleeve"] = "satellite"
        decisions.append(copied)
    fills = []
    for row in satellite["fills"]:
        copied = dict(row)
        copied["sleeve"] = "satellite"
        fills.append(copied)
    blocked = []
    for row in satellite["arm"].get("blocked_orders") or []:
        copied = dict(row)
        copied["sleeve"] = "satellite"
        blocked.append(copied)
    return decisions, fills, blocked


def _transfer_activity(
    combined: dict, dates: list[str], curve_by_date: dict[str, dict]
) -> tuple[list[dict], list[dict]]:
    """Cross-sleeve rebalances as decisions plus a costed transfer fill."""
    decisions = []
    fills = []
    for row in combined.get("transfers") or []:
        if float(row.get("moved_notional") or 0) <= 0:
            continue
        day = row["date"]
        if dates.index(day) + 1 >= len(dates):
            continue
        decision_id = "sleeve_" + day
        after = row.get("weights_after") or {}
        before = row.get("weights_before") or {}
        point = curve_by_date.get(day) or {}
        total = float(point.get("nav") or 0)
        decisions.append(
            {
                "decision_id": decision_id,
                "date": day,
                "execution_date": _next_date(dates, day),
                "nav_before": round(total, 2),
                "cash_before": round(float(before.get("cash", 0.0)) * total, 2),
                "cash_after": round(float(point.get("cash") or 0.0), 2),
                "target_holdings": [
                    {"code": "SLEEVE_CORE", "weight": float(after.get("core", 0))},
                    {
                        "code": "SLEEVE_SATELLITE",
                        "weight": float(after.get("satellite", 0)),
                    },
                    {"code": "SLEEVE_CASH", "weight": float(after.get("cash", 0))},
                ],
                "added": [],
                "removed": [],
                "estimated_cost": float(row.get("cost") or 0),
                "moved_notional": float(row.get("moved_notional") or 0),
                "sleeve": "sleeve",
            }
        )
        fills.append(
            {
                "decision_id": decision_id,
                "date": day,
                "code": "SLEEVE",
                "side": "TRANSFER",
                "quantity": 1,
                "price": round(float(row["moved_notional"]), 2),
                "commission": 0.0,
                "stamp_duty": 0.0,
                "transfer_fee": 0.0,
                "total_cost": round(float(row.get("cost") or 0), 2),
                "reason": "cross_sleeve_rebalance",
                "sleeve": "sleeve",
            }
        )
    return decisions, fills


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    satellite = load_satellite(args.satellite_json)
    frozen = assert_frozen_satellite(satellite, args.cadence)
    satellite_curve = satellite["curve"]
    start = args.from_date or satellite_curve[0]["date"]
    end = args.to_date or satellite_curve[-1]["date"]

    if args.core_prices:
        wide = pd.read_parquet(args.core_prices)
        wide.index = pd.to_datetime(wide.index)
    else:
        wide = fetch_core_prices(start, end)

    satellite_dates = pd.DatetimeIndex(
        [pd.Timestamp(row["date"]) for row in satellite_curve]
    )
    core_prices = wide.reindex(satellite_dates)
    if core_prices.isna().any().any():
        missing = core_prices.isna().any(axis=1)
        first = core_prices.index[missing][0].date().isoformat()
        raise ValueError(f"core ETF price missing on a satellite session: {first}")

    core = simulate_core(
        core_prices, aum=CORE_AUM, weights=DEFAULT_CORE_WEIGHTS, cadence=args.cadence
    )
    benchmark_by_date = {
        day.date().isoformat(): float(value)
        for day, value in core_prices["510300.SH"].items()
        if pd.notna(value) and float(value) > 0
    }
    combined = combine_sleeves(
        core["curve"],
        satellite_curve,
        cash=CASH_AUM,
        cadence=args.cadence,
        benchmark_by_date=benchmark_by_date,
    )

    audit = {}
    if args.audit_json:
        audit = json.loads(Path(args.audit_json).read_text(encoding="utf-8"))
    survivorship_gap = bool(audit.get("survivorship_gap", True))
    replay_id = args.replay_id or (
        "balanced_50k_"
        + start.replace("-", "")
        + "_"
        + end.replace("-", "")
        + "_50000_v1"
    )
    satellite_config_hash = hashlib.sha256(
        canonical_json(frozen).encode("utf-8")
    ).hexdigest()
    config = {
        "profile": "balanced",
        "core": {
            "weights": DEFAULT_CORE_WEIGHTS,
            "aum": CORE_AUM,
            "cadence": args.cadence,
            "execution": "session close (research approximation)",
        },
        "satellite": {
            "strategy": "riq_v1",
            "config_hash": satellite_config_hash,
            **frozen,
            "cadence": args.cadence,
        },
        "cash": {"aum": CASH_AUM, "yield": 0.0},
        "sleeve_weights": {"core": 0.5, "satellite": 0.4, "cash": 0.1},
        "benchmark": {"code": "510300.SH", "kind": "price_index_no_dividends"},
        "universe": {
            "survivorship_gap": survivorship_gap,
            "st_flag_basis": audit.get("st_flag_basis", "current_snapshot"),
        },
    }
    config_hash = hashlib.sha256(canonical_json(config).encode("utf-8")).hexdigest()
    limitations = [
        "REPLAY 研究记录，不是前瞻验证；不计入 120 日证据。",
        "核心 ETF 使用 tushare fund_daily/fund_adj 价格，不是生产行情库。",
        "核心按季度、收盘价模拟；研究中的静态配置证据是月频，二者不可直接等同。",
        "窗口化回放：因子权重使用全面板 IC，结果含样本内成分；不是滚动 OOS。",
        "核心决策的 execution_date 记为下一交易日用于追溯，成交价仍按决策日收盘模拟。",
        "基准为 510300 价格指数（不含分红），仅作参照。",
        "QDII（513100）的溢价/折价与申购限额未建模。",
        "卫星为 20,000 元的新构建（top-10/fwd_h20/21 会话 lag），整手与 5 元最低佣金按 20k 账户计；不能按 50k 曲线缩放。",
        "面板存在幸存者偏差（无退市股、ST 为当前快照），收益应视为上界。",
        "跨 sleeve 调仓按名义额转移计费，不是 ETF 整手成交。",
        "若最后一个会话恰好是核心调仓日，该决策没有更晚的执行日，artifact 不记录它（净值影响仍在 curve 内）。",
    ]
    for item in audit.get("limitations") or []:
        if item not in limitations:
            limitations.append(item)

    dates = [row["date"] for row in combined["curve"]]
    curve_by_date = {row["date"]: row for row in combined["curve"]}
    core_decisions, core_fills, core_blocked = _core_activity(core, dates)
    satellite_decisions, satellite_fills, satellite_blocked = _satellite_activity(
        satellite
    )
    transfer_decisions, transfer_fills = _transfer_activity(
        combined, dates, curve_by_date
    )
    decisions = sorted(
        core_decisions + satellite_decisions + transfer_decisions,
        key=lambda row: (row["date"], row["decision_id"]),
    )
    fills = core_fills + satellite_fills + transfer_fills
    blocked_orders = core_blocked + satellite_blocked

    payload = {
        "replay_id": replay_id,
        "name": "平衡 50/40/10 · 核心-卫星回放",
        "strategy": "core_satellite",
        "description": "50% 三 ETF 核心 + 40% riq_v1 卫星 + 10% 现金",
        "start_date": combined["curve"][0]["date"],
        "end_date": combined["curve"][-1]["date"],
        "aum": TOTAL_AUM,
        "config": config,
        "config_hash": config_hash,
        "source_artifact": "datahub/scripts/core_satellite_backtest.py",
        "curve": combined["curve"],
        "decisions": decisions,
        "fills": fills,
        "blocked_orders": blocked_orders,
        "limitations": limitations,
        "survivorship_gap": survivorship_gap,
    }
    Path(args.output).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    summary = {
        "replay_id": replay_id,
        "sessions": len(combined["curve"]),
        "decisions": len(decisions),
        "fills": len(fills),
        "blocked": len(blocked_orders),
        "core_terminal": core["terminal_nav"],
        "satellite_terminal": satellite_curve[-1]["nav"],
        "combined_terminal": combined["terminal_nav"],
        "output": args.output,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
