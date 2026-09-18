"""Read-only composite-factor research backtest over the factor-lab panel.

This is research measurement tooling, not production code: it is never imported
by scoring, signal, strategy or API modules, it writes nothing to MongoDB, and
it reads only the parquet panel produced by ``factor_lab_runner export`` on the
``factor-lab-panel`` PVC. Results are recorded in
``docs/operations/strategy-experiments-2026-08.md``; see
``docs/operations/factor-lab.md`` for invocation.

What it does
------------
1. Computes the five registered factors (``reversal_10``, ``amihud_20``,
   ``trend_60``, ``volatility_20``, ``rsi_14``) and their daily Spearman IC
   against the panel's tradeable forward label (T+1 open -> T+1+h open, with
   limit-up entries, limit-down exits, suspensions and missing sessions already
   blocked by the panel builder; blocked labels are never rolled forward).
2. Builds a **walk-forward** composite: at each rebalance date the weights are
   the signed ICIR estimated only from ICs realised strictly before that date
   (244-session lookback, at least 120 sessions), so no future information
   enters the score. Known-at-close signals fill at the next session's open.
3. Reports, per mode:
   * ``composite``  equal-weight and walk-forward ICIR books for several sizes,
     with per-year and per-period metrics;
   * ``liquidity``  a liquidity-threshold sweep (keep the top X% by trailing
     20-session mean trade amount);
   * ``capacity``   net CAGR under a square-root impact model
     ``2 * k * sqrt(order_notional / ADV20)`` for AUM levels and k values
     (``k`` is a documented parameter, not a measured constant);
   * ``overlay``    a market-breadth filter (share of names with MA20 < MA60,
     computed at the rebalance close) at several thresholds/exposures;
   * ``accounts``   a small-account simulation with 100-share lots sized from
     the RAW T+1 open (returns stay on the HFQ total-return label), a CNY 5
     per-trade minimum commission and zero-yield leftover cash.

Output is a single JSON document on stdout (or ``--output``).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_COMPONENTS = (
    "reversal_10",
    "amihud_20",
    "trend_60",
    "volatility_20",
    "rsi_14",
)
DEFAULT_PANEL = "/data/lab_2026q3.parquet"
TRADING_DAYS = 244
LOT = 100
LIQUIDITY_WINDOW = 20
FLOORS = (0.0, 0.30, 0.50, 0.70, 0.85)
AUM_LEVELS = (10_000, 30_000, 100_000, 300_000, 1_000_000, 10_000_000)
ACCOUNT_SIZES = (3, 5, 8, 10, 15, 20, 30, 50)
IMPACT_K = (0.05, 0.10, 0.20)
BREADTH_THRESHOLDS = (0.50, 0.60, 0.70)
PERIODS = {"2020-2023": ("2020", "2023"), "2024-2026": ("2024", "2026")}
_PANEL_FIELDS = (
    "date",
    "stock_code",
    "close",
    "close_hfq",
    "open",
    "open_hfq",
    "high_hfq",
    "low_hfq",
    "volume",
    "trade_amount",
    "is_bse",
    "is_st",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", default=DEFAULT_PANEL, help="Parquet panel path.")
    parser.add_argument(
        "--mode",
        action="append",
        choices=("composite", "liquidity", "capacity", "overlay", "accounts"),
        default=[],
        help="Report to produce; repeatable. Default: every report.",
    )
    parser.add_argument("--label", default="fwd_h20", help="Forward label column.")
    parser.add_argument(
        "--weight-label",
        default="fwd_h20",
        help="Label used to estimate the walk-forward ICIR weights.",
    )
    parser.add_argument("--step", type=int, default=20, help="Sessions per holding.")
    parser.add_argument(
        "--sizes",
        default="30,50,100",
        help="Comma-separated book sizes for the composite report.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=100,
        help="Book size used by the liquidity/capacity/overlay reports.",
    )
    parser.add_argument(
        "--buffer-multiple",
        type=int,
        default=2,
        help="Hold while the name stays inside top-(multiple * size).",
    )
    parser.add_argument("--lookback", type=int, default=TRADING_DAYS)
    parser.add_argument("--min-ic-dates", type=int, default=120)
    parser.add_argument("--output", default=None, help="Write JSON here as well.")
    return parser.parse_args(argv)


def load_panel(path: str, labels: tuple[str, ...]) -> pd.DataFrame:
    columns = [field for field in _PANEL_FIELDS]
    frame = pd.read_parquet(
        path, columns=columns + [c for c in labels if c not in columns]
    )
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values(["date", "stock_code"]).reset_index(drop=True)
    raw_open = pd.to_numeric(frame["open"], errors="coerce")
    frame["__raw_entry"] = raw_open.groupby(frame["stock_code"], sort=False).shift(-1)
    amount = pd.to_numeric(frame["trade_amount"], errors="coerce")
    frame["__adv20"] = amount.groupby(frame["stock_code"], sort=False).transform(
        lambda series: series.rolling(LIQUIDITY_WINDOW, min_periods=5).mean()
    )
    price = pd.to_numeric(frame["close_hfq"], errors="coerce").where(
        lambda series: series > 0, pd.to_numeric(frame["close"], errors="coerce")
    )
    by_stock = price.groupby(frame["stock_code"], sort=False)
    ma20 = by_stock.transform(lambda series: series.rolling(20, min_periods=20).mean())
    ma60 = by_stock.transform(lambda series: series.rolling(60, min_periods=60).mean())
    valid = ma20.notna() & ma60.notna()
    breadth = ((ma20 < ma60) & valid).groupby(frame["date"]).mean()
    frame["__breadth"] = frame["date"].map(breadth)
    frame["__liquid_rank"] = frame["__adv20"].groupby(frame["date"]).rank(pct=True)
    return frame


def zscore_by_date(values: pd.Series, dates: pd.Series) -> pd.Series:
    grouped = values.groupby(dates)
    std = grouped.transform("std").replace(0.0, np.nan)
    return (values - grouped.transform("mean")) / std


def daily_ic(frame: pd.DataFrame, values: pd.Series, label: str) -> pd.Series:
    dates = frame["date"]
    rank_factor = values.groupby(dates).rank()
    rank_label = frame[label].groupby(dates).rank()
    covariance = (
        (
            (rank_factor - rank_factor.groupby(dates).transform("mean"))
            * (rank_label - rank_label.groupby(dates).transform("mean"))
        )
        .groupby(dates)
        .mean()
    )
    denominator = rank_factor.groupby(dates).std() * rank_label.groupby(dates).std()
    return (covariance / denominator).replace([np.inf, -np.inf], np.nan)


def walk_forward_composite(
    frame: pd.DataFrame,
    zscores: dict[str, pd.Series],
    ic_by_date: dict[str, pd.Series],
    components: tuple[str, ...],
    lookback: int,
    min_ic_dates: int,
    weighting: str = "icir",
) -> pd.Series:
    """Composite score using only ICs realised before each rebalance date.

    ``weighting="icir"`` weights each factor by mean(IC)/std(IC) (a negative
    ICIR turns into a negative, i.e. inverted, weight); ``weighting="sign"``
    keeps the direction but gives every factor an equal magnitude.
    """
    dates = sorted(frame["date"].unique())
    reference = ic_by_date[components[0]]
    score = pd.Series(np.nan, index=frame.index, dtype="float64")
    for date in dates:
        stamp = pd.Timestamp(date)
        history = reference.loc[:stamp].iloc[:-1].tail(lookback)
        if len(history) < min_ic_dates:
            continue
        weights: dict[str, float] = {}
        for name in components:
            series = ic_by_date[name].reindex(history.index)
            mean = series.mean()
            std = series.std(ddof=1)
            if not np.isfinite(mean):
                continue
            if weighting == "sign":
                weights[name] = float(np.sign(mean))
            elif std and not np.isnan(std):
                weights[name] = float(mean / std)
        weights = {name: value for name, value in weights.items() if value}
        if not weights:
            continue
        total = sum(abs(value) for value in weights.values()) or 1.0
        mask = (frame["date"] == date).to_numpy()
        names = list(weights)
        block = np.column_stack([zscores[name].to_numpy()[mask] for name in names])
        vector = np.array([weights[name] / total for name in names])
        score.loc[mask] = block @ vector
    return score


def curve_stats(net: np.ndarray, benchmark: np.ndarray, step: int) -> dict:
    if len(net) == 0:
        return {}
    per_year = TRADING_DAYS / step
    equity = np.cumprod(1.0 + net)
    years = len(net) / per_year
    vol = float(net.std(ddof=1) * np.sqrt(per_year)) if len(net) > 1 else 0.0
    peak = np.maximum.accumulate(equity)
    bench_equity = np.cumprod(1.0 + benchmark)
    bench_vol = (
        float(benchmark.std(ddof=1) * np.sqrt(per_year)) if len(benchmark) > 1 else 0.0
    )
    bench_peak = np.maximum.accumulate(bench_equity)
    return {
        "cagr_net": float(equity[-1] ** (1.0 / years) - 1.0) if years > 0 else None,
        "sharpe_net": float(net.mean() * per_year / vol) if vol > 0 else None,
        "max_drawdown": float((equity / peak - 1.0).min()),
        "win_rate": float(np.mean(net > 0)),
        "total_net": float(equity[-1] - 1.0),
        "benchmark_cagr": float(bench_equity[-1] ** (1.0 / years) - 1.0)
        if years > 0
        else None,
        "benchmark_sharpe": float(benchmark.mean() * per_year / bench_vol)
        if bench_vol > 0
        else None,
        "benchmark_max_drawdown": float((bench_equity / bench_peak - 1.0).min()),
    }


def collect_blocks(
    frame: pd.DataFrame,
    score: pd.Series,
    label: str,
    size: int,
    buffer_n: int,
    step: int,
    keep_fraction: float = 1.0,
) -> list[dict]:
    """One entry per rebalance: tradeable label, benchmark and the traded book."""
    from app.lib.factor_lab import metrics

    cost = metrics.round_trip_cost()
    dates = sorted(frame["date"].unique())
    score_values = score.to_numpy(dtype="float64")
    label_values = frame[label].to_numpy(dtype="float64")
    date_values = frame["date"].to_numpy()
    codes = frame["stock_code"].to_numpy()
    liquid = frame["__liquid_rank"].to_numpy(dtype="float64")
    adv = frame["__adv20"].to_numpy(dtype="float64")
    raw_entry = frame["__raw_entry"].to_numpy(dtype="float64")
    boundaries = frame["__breadth"].to_numpy(dtype="float64")
    eligible = (
        (~frame["is_bse"].fillna(False))
        & (~frame["is_st"].fillna(False))
        & frame[label].notna()
        & (liquid >= (1.0 - keep_fraction))
    ).to_numpy()
    blocks: list[dict] = []
    previous: set[str] = set()
    for index in range(0, len(dates) - 1, step):
        target = dates[index]
        mask = (date_values == target) & eligible & np.isfinite(score_values)
        if mask.sum() < size:
            continue
        positions = np.flatnonzero(mask)
        ranked = positions[np.argsort(-score_values[positions])]
        rank_of = {int(position): rank for rank, position in enumerate(ranked)}
        if previous:
            held = set(previous)
            keep = [
                position
                for position in ranked
                if codes[position] in held and rank_of[int(position)] < buffer_n
            ]
            additions = [position for position in ranked if codes[position] not in held]
            order = np.array(keep[:size] + additions[: max(0, size - len(keep))])
        else:
            order = ranked[:size]
        current = set(codes[order])
        blocks.append(
            {
                "date": str(pd.Timestamp(target).date()),
                "gross": float(np.nanmean(label_values[order])),
                "benchmark": float(np.nanmean(label_values[positions])),
                "turnover": 1.0
                if not previous
                else len(current - previous) / len(current),
                "universe": int(len(positions)),
                "breadth": float(np.nanmean(boundaries[positions]))
                if positions.size
                else float("nan"),
                "returns": label_values[order].tolist(),
                "raw_entries": raw_entry[order].tolist(),
                "adv": adv[order].tolist(),
            }
        )
        previous = current
    for block in blocks:
        block["fixed_net"] = block["gross"] - cost
    return blocks


def yearly(blocks: list[dict], key: str) -> dict[str, float]:
    groups: dict[str, list[float]] = {}
    for block in blocks:
        groups.setdefault(block["date"][:4], []).append(block[key])
    return {
        year: float(np.prod(1.0 + np.array(values)) - 1.0)
        for year, values in sorted(groups.items())
    }


def period_stats(blocks: list[dict], net: np.ndarray, step: int) -> dict:
    out = {}
    for name, (start, end) in PERIODS.items():
        mask = np.array([start <= b["date"][:4] <= end for b in blocks], dtype=bool)
        if mask.any():
            benchmark = np.array([b["benchmark"] for b in blocks])[mask]
            out[name] = {
                "blocks": int(mask.sum()),
                **curve_stats(net[mask], benchmark, step),
            }
    return out


def impact_cost(block: dict, aum: float, size: int, k: float) -> float:
    notional = aum / size * max(block["turnover"], 1e-9)
    adv = np.array(block["adv"], dtype="float64")
    adv = adv[adv > 0]
    if adv.size == 0:
        return 0.0
    return float(2.0 * (k * np.sqrt(notional / adv)).mean())


def report_composite(
    frame: pd.DataFrame,
    scores: dict[str, pd.Series],
    label: str,
    sizes: tuple[int, ...],
    buffer_multiple: int,
    step: int,
) -> dict:
    out: dict[str, dict] = {}
    for name, score in scores.items():
        for size in sizes:
            blocks = collect_blocks(
                frame, score, label, size, buffer_multiple * size, step
            )
            net = np.array([b["fixed_net"] for b in blocks])
            benchmark = np.array([b["benchmark"] for b in blocks])
            out[f"{name}_top{size}"] = {
                "blocks": len(blocks),
                "avg_turnover": float(np.mean([b["turnover"] for b in blocks])),
                **curve_stats(net, benchmark, step),
                "yearly_net": yearly(blocks, "fixed_net"),
                "periods": period_stats(blocks, net, step),
            }
    return out


def report_liquidity(
    frame: pd.DataFrame,
    score: pd.Series,
    label: str,
    size: int,
    buffer_multiple: int,
    step: int,
) -> dict:
    out: dict[str, dict] = {}
    for floor in FLOORS:
        blocks = collect_blocks(
            frame, score, label, size, buffer_multiple * size, step, 1.0 - floor
        )
        if not blocks:
            continue
        net = np.array([b["fixed_net"] for b in blocks])
        benchmark = np.array([b["benchmark"] for b in blocks])
        out[f"floor_{floor:.2f}"] = {
            "liquidity_floor": floor,
            "blocks": len(blocks),
            "avg_universe": float(np.mean([b["universe"] for b in blocks])),
            "median_book_adv_cny": float(
                np.median([np.median(b["adv"]) for b in blocks])
            ),
            **curve_stats(net, benchmark, step),
        }
    return out


def report_capacity(
    frame: pd.DataFrame,
    score: pd.Series,
    label: str,
    size: int,
    buffer_multiple: int,
    step: int,
    aum_levels: tuple[float, ...],
) -> dict:
    from app.lib.factor_lab import metrics

    blocks = collect_blocks(frame, score, label, size, buffer_multiple * size, step)
    benchmark = np.array([b["benchmark"] for b in blocks])
    cost = metrics.round_trip_cost()
    out: dict[str, dict] = {}
    for aum in aum_levels:
        row = {}
        for k in IMPACT_K:
            net = np.array(
                [b["gross"] - cost - impact_cost(b, aum, size, k) for b in blocks]
            )
            row[f"k={k}"] = curve_stats(net, benchmark, step)
        out[f"aum_{int(aum)}"] = row
    return out


def report_overlay(
    frame: pd.DataFrame,
    score: pd.Series,
    label: str,
    size: int,
    buffer_multiple: int,
    step: int,
) -> dict:
    blocks = collect_blocks(frame, score, label, size, buffer_multiple * size, step)
    benchmark = np.array([b["benchmark"] for b in blocks])
    out: dict[str, dict] = {}
    for threshold in BREADTH_THRESHOLDS:
        for exposure in (0.5, 0.0):
            net = np.array(
                [
                    b["fixed_net"] * (exposure if b["breadth"] > threshold else 1.0)
                    for b in blocks
                ]
            )
            exposures = [exposure if b["breadth"] > threshold else 1.0 for b in blocks]
            out[f"threshold_{threshold:.2f}_exposure_{exposure:.1f}"] = {
                "avg_exposure": float(np.mean(exposures)),
                "de_risked_blocks": int(sum(1 for value in exposures if value < 1.0)),
                **curve_stats(net, benchmark, step),
            }
    continuous = np.array(
        [b["fixed_net"] * float(np.clip(1.0 - b["breadth"], 0.2, 1.0)) for b in blocks]
    )
    out["continuous_clip_0.2_1.0"] = {
        "avg_exposure": float(
            np.mean([float(np.clip(1.0 - b["breadth"], 0.2, 1.0)) for b in blocks])
        ),
        **curve_stats(continuous, benchmark, step),
    }
    return out


def report_accounts(
    frame: pd.DataFrame,
    score: pd.Series,
    label: str,
    buffer_multiple: int,
    step: int,
    min_commission: float,
) -> dict:
    from app.lib.factor_lab import metrics

    out: dict[str, dict] = {}
    for aum in AUM_LEVELS:
        for size in ACCOUNT_SIZES:
            if aum / size < LOT:
                continue
            blocks = collect_blocks(
                frame, score, label, size, buffer_multiple * size, step
            )
            net = []
            cash_drag = []
            commission_drag = []
            held_counts = []
            for block in blocks:
                returns = np.array(block["returns"], dtype="float64")
                entries = np.array(block["raw_entries"], dtype="float64")
                valid = np.isfinite(entries) & (entries > 0) & np.isfinite(returns)
                lots = np.zeros(len(entries))
                lots[valid] = np.floor((aum / size) / (entries[valid] * LOT))
                lots = np.where(lots > 0, lots, 0.0)
                prices = np.where(valid, entries, 0.0)
                invested = float(np.sum(lots * LOT * prices))
                # The panel label is a total return (HFQ, T+1 open to exit
                # open); apply it to the raw position value so dividends and
                # splits stay in the result while lot sizes use real prices.
                exit_value = float(np.sum(lots * LOT * prices * (1.0 + returns)))
                traded = invested * block["turnover"]
                held = int(np.count_nonzero(lots))
                trades = 2.0 * block["turnover"] * max(held, 1)
                per_trade = traded / (trades / 2.0) if traded else 0.0
                commission = trades * max(min_commission, 0.00025 * max(per_trade, 0.0))
                end_value = (
                    aum
                    - invested
                    + exit_value
                    - metrics.round_trip_cost() * traded
                    - commission
                )
                net.append(end_value / aum - 1.0)
                cash_drag.append(1.0 - invested / aum)
                commission_drag.append(commission / aum)
                held_counts.append(held)
            benchmark = np.array([b["benchmark"] for b in blocks])
            out[f"aum_{int(aum)}_names_{size}"] = {
                "avg_names_with_lots": float(np.mean(held_counts)),
                "avg_cash_drag": float(np.mean(cash_drag)),
                "avg_commission_drag_per_year": float(
                    np.mean(commission_drag) * (TRADING_DAYS / step)
                ),
                **curve_stats(np.array(net), benchmark, step),
            }
    return out


def main(argv: list[str] | None = None) -> None:
    from app.lib.factor_lab import factors, metrics

    args = parse_args(argv)
    labels = tuple({args.label, args.weight_label})
    if not Path(args.panel).exists():
        raise FileNotFoundError(
            f"panel not found: {args.panel} (run factor_lab_runner export first)"
        )
    frame = load_panel(args.panel, labels)

    zscores: dict[str, pd.Series] = {}
    ic_by_date: dict[str, pd.Series] = {}
    for name in DEFAULT_COMPONENTS:
        values = factors.compute(frame, name)
        zscores[name] = zscore_by_date(values, frame["date"])
        ic_by_date[name] = daily_ic(frame, values, args.weight_label)

    components = tuple(DEFAULT_COMPONENTS)
    raw_equal = sum(zscores[name] for name in components) / len(components)
    sign_equal = walk_forward_composite(
        frame,
        zscores,
        ic_by_date,
        components,
        args.lookback,
        args.min_ic_dates,
        weighting="sign",
    )
    icir = walk_forward_composite(
        frame, zscores, ic_by_date, components, args.lookback, args.min_ic_dates
    )
    scores = {
        "equal_weight_raw": raw_equal,
        "equal_weight_ic_sign": sign_equal,
        "icir_walk_forward": icir,
    }

    sizes = tuple(int(part) for part in args.sizes.split(",") if part.strip())
    modes = args.mode or ["composite", "liquidity", "capacity", "overlay", "accounts"]
    report: dict = {
        "panel": args.panel,
        "label": args.label,
        "weight_label": args.weight_label,
        "step_sessions": args.step,
        "top": args.top,
        "buffer_multiple": args.buffer_multiple,
        "round_trip_cost": metrics.round_trip_cost(),
        "components": list(components),
        "factor_daily_ic_mean": {
            name: float(ic_by_date[name].mean()) for name in components
        },
        "notes": [
            "equal_weight_raw applies no direction: it documents that naive "
            "equal weighting of these factors is not a strategy.",
            "equal_weight_ic_sign and icir_walk_forward estimate direction (and "
            "ICIR weighting) only from ICs realised before each rebalance date.",
            "Impact k is a parameter, not a measured constant; capacity results "
            "are model-based and this output is research, not investment advice.",
        ],
        "reports": {},
    }
    if "composite" in modes:
        report["reports"]["composite"] = report_composite(
            frame, scores, args.label, sizes, args.buffer_multiple, args.step
        )
    if "liquidity" in modes:
        report["reports"]["liquidity"] = report_liquidity(
            frame, icir, args.label, args.top, args.buffer_multiple, args.step
        )
    if "capacity" in modes:
        report["reports"]["capacity"] = report_capacity(
            frame,
            icir,
            args.label,
            args.top,
            args.buffer_multiple,
            args.step,
            AUM_LEVELS,
        )
    if "overlay" in modes:
        report["reports"]["overlay"] = report_overlay(
            frame, icir, args.label, args.top, args.buffer_multiple, args.step
        )
    if "accounts" in modes:
        report["reports"]["accounts"] = report_accounts(
            frame, icir, args.label, args.buffer_multiple, args.step, 5.0
        )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
