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
     per-trade minimum commission and zero-yield leftover cash;
   * ``sprint``     the extreme small-account sweep over holding steps that have
     a matching ``fwd_h{step}`` label, with and without friction;
   * ``oos``        one stitched rolling out-of-sample curve: consecutive
     ``--train-sessions``/``--test-sessions`` folds, fold weights purged of every
     IC whose forward label could reach the test window (plus ``--embargo``
     extra sessions) and the test window replayed from cash.

Output is a single JSON document on stdout (or ``--output``). The app package
installs a stdout log handler on import; ``main`` binds it to stderr so stdout
carries only that JSON. The "existing modes are unchanged" guarantee therefore
covers the JSON document / ``--output`` payload, not the stdout stream, which no
longer carries the environment and startup log lines the pre-change build leaked.
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
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

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
# The capacity report looks further out along the AUM axis than the
# small-account grid, where lot rounding is the binding constraint.
CAPACITY_AUM_LEVELS = (10_000_000, 50_000_000, 100_000_000, 500_000_000, 1_000_000_000)
ACCOUNT_SIZES = (3, 5, 8, 10, 15, 20, 30, 50)
IMPACT_K = (0.05, 0.10, 0.20)
#: Extreme small-account experiment: how fast can CNY 10k compound when the
#: book is deliberately tiny? Steps must have a matching ``fwd_h{step}`` label
#: in the panel, so the sweep only uses horizons the export produced.
SPRINT_STEPS = (1, 2, 3, 5, 10, 20, 60)
SPRINT_SIZES = (1, 2, 3, 5, 8)
SPRINT_AUM = 10_000
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


def _non_negative_sessions(value: str) -> int:
    """argparse type: a session count that may be zero but never negative."""
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError(
            f"expected zero or a positive session count, got {number}"
        )
    return number


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", default=DEFAULT_PANEL, help="Parquet panel path.")
    parser.add_argument(
        "--mode",
        action="append",
        choices=(
            "composite",
            "liquidity",
            "capacity",
            "overlay",
            "accounts",
            "sprint",
            "oos",
        ),
        default=[],
        help="Report to produce; repeatable. Default: every report.",
    )
    parser.add_argument("--label", default="fwd_h20", help="Forward label column.")
    parser.add_argument(
        "--weight-label",
        default=None,
        help="Label used to estimate the walk-forward weights (default: --label).",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=None,
        help="Sessions per holding (default: 20, or 60 for --mode oos).",
    )
    parser.add_argument(
        "--names",
        type=int,
        default=10,
        help="Book size for --mode oos (equal weight, 2N buffer).",
    )
    parser.add_argument(
        "--aum",
        type=float,
        default=50_000.0,
        help="Account size for --mode oos.",
    )
    parser.add_argument(
        "--train-sessions",
        type=int,
        default=500,
        help="Training sessions per rolling out-of-sample fold.",
    )
    parser.add_argument(
        "--test-sessions",
        type=int,
        default=250,
        help="Test sessions per rolling out-of-sample fold.",
    )
    parser.add_argument(
        "--embargo",
        type=_non_negative_sessions,
        default=0,
        help="Extra purge sessions between the training tail and the test window "
        "(zero or more; a negative value would re-admit overlapping-label ICs).",
    )
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
    parser.add_argument(
        "--sprint-aum",
        type=float,
        default=SPRINT_AUM,
        help="Account size for the sprint (extreme small-account) report.",
    )
    parser.add_argument(
        "--sprint-steps",
        default=",".join(str(value) for value in SPRINT_STEPS),
        help="Comma-separated holding/rebalance steps for the sprint report.",
    )
    parser.add_argument(
        "--sprint-sizes",
        default=",".join(str(value) for value in SPRINT_SIZES),
        help="Comma-separated book sizes for the sprint report.",
    )
    parser.add_argument(
        "--components",
        default=None,
        help="Comma-separated registered factors for the composite report "
        "(default: the five-factor RIQ set). The report's equal_weight_raw row "
        "is the un-signed book, i.e. long high factor values.",
    )
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
    """Exact per-date Spearman: ranks over the common non-NaN subset.

    Ranks are computed only where both the factor and the label exist, and the
    correlation uses population moments (ddof=0) on those ranks, which is what
    Spearman is; dates with fewer than two pairs are dropped.
    """
    dates = frame["date"]
    valid = values.notna() & frame[label].notna()
    if not valid.any():
        return pd.Series(dtype="float64")
    rank_factor = values.where(valid).groupby(dates).rank()
    rank_label = frame[label].where(valid).groupby(dates).rank()
    centered_factor = rank_factor - rank_factor.groupby(dates).transform("mean")
    centered_label = rank_label - rank_label.groupby(dates).transform("mean")
    pairs = valid.groupby(dates).sum()
    covariance = (centered_factor * centered_label).groupby(dates).sum() / pairs
    denominator = rank_factor.groupby(dates).std(ddof=0) * rank_label.groupby(
        dates
    ).std(ddof=0)
    ic = (covariance / denominator).replace([np.inf, -np.inf], np.nan)
    return ic[pairs >= 2].dropna()


def _icir_weights(
    history: pd.Series,
    components: tuple[str, ...],
    ic_by_date: dict[str, pd.Series],
    weighting: str,
) -> dict[str, float]:
    """Signed, self-normalising factor weights over one IC history window."""
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
        return {}
    total = sum(abs(value) for value in weights.values()) or 1.0
    return {name: value / total for name, value in weights.items()}


def walk_forward_composite(
    frame: pd.DataFrame,
    zscores: dict[str, pd.Series],
    ic_by_date: dict[str, pd.Series],
    components: tuple[str, ...],
    lookback: int,
    min_ic_dates: int,
    label_lag: int,
    weighting: str = "icir",
) -> pd.Series:
    """Composite score using only ICs whose labels were already realised.

    The panel labels session ``t`` with open(t+1) -> open(t+1+h), so the IC
    computed at ``t`` is only observable at ``t+1+h``. Weight estimation for a
    rebalance at session D therefore stops at ``D - h - 1``
    (``label_lag = h + 1``); using later ICs would leak future prices into the
    score. ``weighting="icir"`` weights each factor by mean(IC)/std(IC) (a
    negative ICIR becomes a negative, i.e. inverted, weight);
    ``weighting="sign"`` keeps the direction with equal magnitudes.
    """
    dates = sorted(frame["date"].unique())
    positions = {pd.Timestamp(date): index for index, date in enumerate(dates)}
    reference = ic_by_date[components[0]]
    score = pd.Series(np.nan, index=frame.index, dtype="float64")
    for date in dates:
        stamp = pd.Timestamp(date)
        cutoff_index = positions[stamp] - label_lag
        if cutoff_index < 0:
            continue
        history = reference.loc[: pd.Timestamp(dates[cutoff_index])].tail(lookback)
        if len(history) < min_ic_dates:
            continue
        weights = _icir_weights(history, components, ic_by_date, weighting)
        if not weights:
            continue
        mask = (frame["date"] == date).to_numpy()
        names = list(weights)
        block = np.column_stack([zscores[name].to_numpy()[mask] for name in names])
        vector = np.array([weights[name] for name in names])
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
            if not blocks:
                out[f"{name}_top{size}"] = {"blocks": 0}
                continue
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


def simulate_account(
    blocks: list[dict],
    aum: float,
    size: int,
    min_commission: float,
    step: int,
    charge_costs: bool = True,
) -> dict:
    """Lot-aware account simulation over one book's rebalance blocks.

    Lots are sized from the RAW T+1 open (what the account really pays) while
    value growth uses the panel's HFQ total-return label. ``charge_costs=False``
    returns the fee-free upper bound used to separate signal from friction.
    """
    from app.lib.factor_lab import metrics

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
        exit_value = float(np.sum(lots * LOT * prices * (1.0 + returns)))
        held = int(np.count_nonzero(lots))
        if charge_costs:
            traded = invested * block["turnover"]
            trades = 2.0 * block["turnover"] * max(held, 1)
            per_trade = traded / (trades / 2.0) if traded else 0.0
            commission = trades * max(min_commission, 0.00025 * max(per_trade, 0.0))
            cost = metrics.round_trip_cost() * traded + commission
        else:
            commission = 0.0
            cost = 0.0
        net.append((aum - invested + exit_value - cost) / aum - 1.0)
        cash_drag.append(1.0 - invested / aum)
        commission_drag.append(commission / aum)
        held_counts.append(held)
    benchmark = np.array([b["benchmark"] for b in blocks]) if blocks else np.array([])
    stats = curve_stats(np.array(net), benchmark, step)
    if stats.get("cagr_net") and stats["cagr_net"] > 0:
        stats["years_to_double"] = float(np.log(2.0) / np.log(1.0 + stats["cagr_net"]))
        stats["months_to_double"] = stats["years_to_double"] * 12.0
    else:
        stats["years_to_double"] = None
        stats["months_to_double"] = None
    stats.update(
        {
            "blocks": len(blocks),
            "avg_names_with_lots": float(np.mean(held_counts)) if held_counts else 0.0,
            "avg_cash_drag": float(np.mean(cash_drag)) if cash_drag else 0.0,
            "avg_commission_drag_per_year": float(
                np.mean(commission_drag) * TRADING_DAYS / step
            )
            if commission_drag
            else 0.0,
            "avg_turnover": float(np.mean([b["turnover"] for b in blocks]))
            if blocks
            else 0.0,
        }
    )
    return stats


def report_accounts(
    frame: pd.DataFrame,
    score: pd.Series,
    label: str,
    buffer_multiple: int,
    step: int,
    min_commission: float,
) -> dict:
    out: dict[str, dict] = {}
    median_entry = float(np.nanmedian(frame["__raw_entry"].to_numpy(dtype="float64")))
    for aum in AUM_LEVELS:
        for size in ACCOUNT_SIZES:
            # A slice that cannot buy even one typical lot produces an
            # all-cash book; report it as skipped instead of a fake strategy.
            if aum / size < LOT or aum / size < median_entry * LOT:
                out[f"aum_{int(aum)}_names_{size}"] = {"skipped": "slice < 1 lot"}
                continue
            blocks = collect_blocks(
                frame, score, label, size, buffer_multiple * size, step
            )
            out[f"aum_{int(aum)}_names_{size}"] = simulate_account(
                blocks, aum, size, min_commission, step
            )
    return out


def label_horizon(label: str) -> int:
    """Sessions in a ``fwd_h{N}`` label (0 when the name carries no horizon)."""
    suffix = label.rsplit("h", 1)[-1]
    return int(suffix) if suffix.isdigit() else 0


def report_sprint(
    frame: pd.DataFrame,
    zscores: dict[str, pd.Series],
    components: tuple[str, ...],
    lookback: int,
    min_ic_dates: int,
    aum: float,
    steps: tuple[int, ...],
    sizes: tuple[int, ...],
    buffer_multiple: int,
    min_commission: float,
) -> dict:
    """How fast can a tiny account compound, and what does friction cost?

    For every holding period with a matching label the report builds the causal
    ICIR composite (weights lagged by step + 1 sessions), trades the top N with
    an N*buffer_multiple band, and simulates the account twice: once with the
    fixed round-trip cost plus the per-trade minimum commission, and once with
    no friction at all, so the gap is visible.
    """
    out: dict[str, dict] = {}
    median_entry = float(np.nanmedian(frame["__raw_entry"].to_numpy(dtype="float64")))
    for step in steps:
        label = f"fwd_h{step}"
        if label not in frame.columns:
            continue
        ic_by_date = {
            name: daily_ic(frame, values, label) for name, values in zscores.items()
        }
        for weighting in ("icir", "sign"):
            score = walk_forward_composite(
                frame,
                zscores,
                ic_by_date,
                components,
                lookback,
                min_ic_dates,
                step + 1,
                weighting=weighting,
            )
            for size in sizes:
                key = f"step{step}_{weighting}_top{size}"
                if aum / size < LOT or aum / size < median_entry * LOT:
                    out[key] = {"skipped": "slice < 1 lot"}
                    continue
                blocks = collect_blocks(
                    frame,
                    score,
                    label,
                    size,
                    max(size, buffer_multiple * size),
                    step,
                )
                if not blocks:
                    out[key] = {"blocks": 0}
                    continue
                out[key] = {
                    "with_costs": simulate_account(
                        blocks, aum, size, min_commission, step
                    ),
                    "no_costs": simulate_account(
                        blocks, aum, size, min_commission, step, charge_costs=False
                    ),
                }
    return out


def plan_oos_folds(
    n_sessions: int, train_sessions: int, test_sessions: int
) -> list[tuple[int, int, int, int]]:
    """Consecutive ``(train_start, train_end, test_start, test_end)`` folds.

    Every fold trains on ``train_sessions`` sessions and tests on the following
    ``test_sessions``, then rolls forward by exactly ``test_sessions``, so the
    test windows are consecutive and non-overlapping. Raises instead of silently
    reporting when the panel cannot hold one full fold.
    """
    if train_sessions < 1 or test_sessions < 1:
        raise ValueError(
            "--train-sessions and --test-sessions must both be positive "
            f"(got {train_sessions} and {test_sessions})"
        )
    folds: list[tuple[int, int, int, int]] = []
    start = 0
    while start + train_sessions + test_sessions <= n_sessions:
        folds.append(
            (
                start,
                start + train_sessions - 1,
                start + train_sessions,
                start + train_sessions + test_sessions - 1,
            )
        )
        start += test_sessions
    if not folds:
        raise ValueError(
            f"panel has {n_sessions} sessions, fewer than one full rolling "
            f"out-of-sample fold of {train_sessions} train + {test_sessions} test "
            "sessions"
        )
    return folds


def attach_execution_columns(frame: pd.DataFrame, path: str) -> pd.DataFrame:
    """Add the T+1 execution gates the daily replay needs.

    The composite panel projection does not carry the limit/suspension flags, so
    the ``oos`` mode reads them from the parquet; a panel without them is treated
    as always tradeable rather than failing the run.
    """
    missing = [
        name
        for name in ("trade_status", "limit_up", "limit_down")
        if name not in frame.columns
    ]
    if missing:
        try:
            extra = pd.read_parquet(path, columns=["date", "stock_code", *missing])
        except (KeyError, ValueError):
            extra = pd.read_parquet(path, columns=["date", "stock_code"])
        extra["date"] = pd.to_datetime(extra["date"])
        for name in missing:
            if name not in extra.columns:
                extra[name] = 1 if name == "trade_status" else False
        try:
            frame = frame.merge(
                extra,
                on=["date", "stock_code"],
                how="left",
                validate="one_to_one",
            )
        except pd.errors.MergeError as exc:
            raise ValueError(
                "cannot attach execution columns: the panel has duplicate "
                "(date, stock_code) keys, so the left merge would multiply rows"
            ) from exc
    frame["trade_status"] = pd.to_numeric(
        frame["trade_status"], errors="coerce"
    ).fillna(1)
    for name in ("limit_up", "limit_down"):
        frame[name] = frame[name].fillna(False).astype(bool)
    return frame


def report_oos(
    frame: pd.DataFrame,
    zscores: dict[str, pd.Series],
    ic_by_date: dict[str, pd.Series],
    components: tuple[str, ...],
    *,
    panel: str,
    step: int,
    names: int,
    buffer_multiple: int,
    aum: float,
    lookback: int,
    min_ic_dates: int,
    label: str,
    label_lag: int,
    train_sessions: int,
    test_sessions: int,
    embargo: int,
    liquidity_floor: float = 0.0,
) -> dict:
    """One stitched out-of-sample curve from purged, embargoed folds.

    For every fold the composite weights are frozen from the IC dates at or
    before ``test_start_index - label_horizon - 1 - embargo`` (the same causal
    cutoff ``walk_forward_composite`` uses, plus the embargo), never from an IC
    whose forward label could reach into the test window. The frozen book is then
    replayed on the test window only and the daily returns are stitched.
    """
    from app.lib.factor_lab import metrics
    from factor_lab_account_replay import (
        COMMISSION_RATE,
        MIN_COMMISSION,
        replay_book,
    )

    dates = list(sorted(frame["date"].unique()))
    folds = plan_oos_folds(len(dates), train_sessions, test_sessions)
    frame["__raw_close"] = pd.to_numeric(frame["close"], errors="coerce")
    reference = ic_by_date[components[0]]

    fold_rows: list[dict] = []
    stitched = [1.0]
    cash_shares: list[float] = []
    traded_total = 0.0
    sessions_stitched = 0
    for number, (train_start, train_end, test_start, test_end) in enumerate(folds, 1):
        # Purge: the IC at session d labels the open-to-open move over
        # d+1 .. d+1+h, so only d <= test_start - h - 1 can be fully observed
        # before the test window opens. --embargo adds extra spacing.
        if embargo < 0:
            raise ValueError(
                "--embargo must be zero or a positive session count; a negative "
                "embargo would move the cutoff later and re-admit the "
                "overlapping-label ICs the purge exists to remove"
            )
        cutoff_index = test_start - label_lag - embargo
        if cutoff_index > test_start - label_lag:
            raise ValueError(
                f"fold {number} would use IC dates after the purge cutoff "
                f"(embargo {embargo}); refusing to leak the test window"
            )
        if cutoff_index < 0:
            raise ValueError(
                f"fold {number} has no purgeable training history: its test "
                f"window starts at session {test_start} but the label lag and "
                f"embargo need {label_lag + embargo} preceding sessions"
            )
        cutoff = pd.Timestamp(dates[cutoff_index])
        history = reference.loc[:cutoff].tail(lookback)
        if len(history) < min_ic_dates:
            raise ValueError(
                f"fold {number} has only {len(history)} IC dates at or before "
                f"{cutoff.date()}, fewer than --min-ic-dates {min_ic_dates}"
            )
        weights = _icir_weights(history, components, ic_by_date, "icir")
        if not weights:
            raise ValueError(
                f"fold {number} produced no usable factor weights at or before "
                f"{cutoff.date()}"
            )
        frame["__score"] = sum(
            weight * zscores[name] for name, weight in weights.items()
        )
        window = [pd.Timestamp(value) for value in dates[test_start : test_end + 1]]
        book = replay_book(
            frame,
            sessions=window,
            step=step,
            names=names,
            buffer_multiple=buffer_multiple,
            aum=aum,
            liquidity_floor=liquidity_floor,
            label=label,
            require_label=False,
            daily_marks=True,
            include_nav=True,
        )
        nav_rows = book["nav"]
        levels = [row["nav"] / aum for row in nav_rows]
        # The first test session is all cash: the book is bought at the next
        # session's open, so its return is exactly zero and no training-window
        # position is carried into the fold.
        returns = [0.0] + [
            levels[index] / levels[index - 1] - 1.0 for index in range(1, len(levels))
        ]
        for value in returns:
            stitched.append(stitched[-1] * (1.0 + value))
        cash_shares.extend(
            row["cash_share"] for row in nav_rows if row["cash_share"] is not None
        )
        traded_total += book["traded_notional"]
        sessions_stitched += len(nav_rows)
        purged = (
            int(
                (
                    (reference.index > cutoff)
                    & (reference.index <= dates[test_start - 1])
                ).sum()
            )
            if test_start > 0
            else 0
        )
        fold_rows.append(
            {
                "fold": number,
                "train_start": str(pd.Timestamp(dates[train_start]).date()),
                "train_end": str(pd.Timestamp(dates[train_end]).date()),
                "test_start": str(pd.Timestamp(dates[test_start]).date()),
                "test_end": str(pd.Timestamp(dates[test_end]).date()),
                "ic_first_date": str(pd.Timestamp(history.index[0]).date())
                if len(history)
                else None,
                "ic_cutoff": str(cutoff.date()),
                "ic_dates_used": int(len(history)),
                "purged_ic_dates": purged,
                "factor_weights": {
                    name: round(float(value), 6) for name, value in weights.items()
                },
                "test_total_return": book["total_return"],
                "test_annualised": book["annualised"],
                "test_max_drawdown": book["max_drawdown"],
                "test_turnover_annual": book["turnover_annual"],
                "test_cash_share_avg": book["cash_share_avg"],
                "test_traded_notional": book["traded_notional"],
                "test_sessions": len(nav_rows),
            }
        )

    equity = np.array(stitched, dtype="float64")
    years = sessions_stitched / TRADING_DAYS
    peak = np.maximum.accumulate(equity)
    average_nav = float(np.mean(equity[1:])) if sessions_stitched else 1.0
    config = {
        "panel": panel,
        "mode": "oos",
        "step": step,
        "names": names,
        "buffer_multiple": buffer_multiple,
        "components": list(components),
        "lookback": lookback,
        "min_ic_dates": min_ic_dates,
        "label": label,
        "label_lag": label_lag,
        "purge_sessions": label_lag,
        "train_sessions": train_sessions,
        "test_sessions": test_sessions,
        "embargo": embargo,
        "aum": aum,
        "liquidity_floor": liquidity_floor,
        "weighting": "icir",
        "costs": {
            "round_trip_cost": metrics.round_trip_cost(),
            "min_commission_cny": MIN_COMMISSION,
            "commission_rate": COMMISSION_RATE,
        },
    }
    summary = {
        "folds": len(fold_rows),
        "sessions": sessions_stitched,
        "total_return": float(equity[-1] - 1.0),
        "annualised": float(equity[-1] ** (1.0 / years) - 1.0)
        if years > 0 and equity[-1] > 0
        else None,
        "max_drawdown": float((equity / peak - 1.0).min()),
        "turnover_annual": round(traded_total / (average_nav * aum * years), 4)
        if years > 0 and average_nav > 0
        else None,
        "cash_share_avg": round(float(np.mean(cash_shares)), 4)
        if cash_shares
        else None,
        "first_test_start": fold_rows[0]["test_start"],
        "last_test_end": fold_rows[-1]["test_end"],
    }
    return {
        "config": config,
        "folds": fold_rows,
        "summary": summary,
        "note": "Rolling out-of-sample protocol: fold weights are purged of every "
        "IC whose forward label could reach the test window, with the optional "
        "embargo on top, and each fold's test window is replayed from cash so no "
        "training-window position carries over. This is still a replay on a "
        "survival-biased panel, not forward evidence or investment advice.",
        "history_note": "Each fold's train_start/train_end are only the folder "
        "bounds. The weights actually use the IC dates in [ic_first_date, "
        "ic_cutoff] -- a window capped by --lookback -- so --train-sessions "
        "shifts the folds while --lookback caps the IC history and "
        "purged_ic_dates reports the leaky IC dates removed between the cutoff "
        "and the test window.",
    }


def main(argv: list[str] | None = None) -> None:
    # Importing the app package installs a stdout log handler; keep stdout for
    # the JSON payload by letting that handler bind to stderr instead (the same
    # pattern factor_lab_paper_run uses).
    real_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        from app.lib.factor_lab import factors, metrics
    finally:
        sys.stdout = real_stdout

    args = parse_args(argv)
    modes = args.mode or ["composite", "liquidity", "capacity", "overlay", "accounts"]
    block_modes = [mode for mode in modes if mode != "oos"]
    if "oos" in modes and block_modes and args.step is None:
        # The two families have different natural steps (20 vs 60); a single
        # shared --step cannot default both honestly, so make the choice explicit.
        raise ValueError(
            "--mode oos rebalances quarterly (60) by default while the "
            f"label-based modes ({', '.join(block_modes)}) default to 20; a mixed "
            "run needs an explicit --step."
        )
    # The daily out-of-sample book is not tied to a forward label's horizon, so
    # it rebalances quarterly by default; the label-based modes keep 20.
    step = (
        args.step
        if args.step is not None
        else (60 if "oos" in modes and not block_modes else 20)
    )
    weight_label = args.weight_label or args.label
    label_sessions = label_horizon(args.label)
    if block_modes and label_sessions and label_sessions != step:
        raise ValueError(
            f"--label {args.label} holds {label_sessions} sessions but --step is "
            f"{step}; the label horizon and the rebalance step must match "
            "(a mismatch double-counts the holding-period return)."
        )
    label_lag = label_horizon(weight_label) + 1
    sprint_steps = tuple(
        int(part) for part in args.sprint_steps.split(",") if part.strip()
    )
    labels = {args.label, weight_label}
    if "sprint" in (args.mode or []):
        # The sprint sweep needs every step's label column loaded.
        labels.update(f"fwd_h{sprint_step}" for sprint_step in sprint_steps)
    labels = tuple(labels)
    if not Path(args.panel).exists():
        raise FileNotFoundError(
            f"panel not found: {args.panel} (run factor_lab_runner export first)"
        )
    frame = load_panel(args.panel, labels)
    if "oos" in modes:
        frame = attach_execution_columns(frame, args.panel)

    zscores: dict[str, pd.Series] = {}
    ic_by_date: dict[str, pd.Series] = {}
    components = (
        tuple(part for part in args.components.split(",") if part.strip())
        if args.components
        else tuple(DEFAULT_COMPONENTS)
    )
    for name in components:
        values = factors.compute(frame, name)
        zscores[name] = zscore_by_date(values, frame["date"])
        ic_by_date[name] = daily_ic(frame, values, weight_label)

    raw_equal = sum(zscores[name] for name in components) / len(components)
    sign_equal = walk_forward_composite(
        frame,
        zscores,
        ic_by_date,
        components,
        args.lookback,
        args.min_ic_dates,
        label_lag,
        weighting="sign",
    )
    icir = walk_forward_composite(
        frame,
        zscores,
        ic_by_date,
        components,
        args.lookback,
        args.min_ic_dates,
        label_lag,
    )
    scores = {
        "equal_weight_raw": raw_equal,
        "equal_weight_ic_sign": sign_equal,
        "icir_walk_forward": icir,
    }

    sizes = tuple(int(part) for part in args.sizes.split(",") if part.strip())
    report: dict = {
        "panel": args.panel,
        "label": args.label,
        "weight_label": weight_label,
        "weight_label_lag_sessions": label_lag,
        "step_sessions": step,
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
            "max_drawdown is sampled once per rebalance (every --step sessions), "
            "so it omits intra-holding-period troughs and understates a daily "
            "drawdown; it is not a daily equity-path figure.",
            "Eligibility requires a non-null forward label, so names whose exit "
            "turns out untradeable (limit-down exit, suspension) are excluded "
            "ex-post; that is the panel's conservative label semantics but it "
            "still biases basket returns upward. Blocked labels are never rolled "
            "forward.",
        ],
        "reports": {},
    }
    if "composite" in modes:
        report["reports"]["composite"] = report_composite(
            frame, scores, args.label, sizes, args.buffer_multiple, step
        )
    if "liquidity" in modes:
        report["reports"]["liquidity"] = report_liquidity(
            frame, icir, args.label, args.top, args.buffer_multiple, step
        )
    if "capacity" in modes:
        report["reports"]["capacity"] = report_capacity(
            frame,
            icir,
            args.label,
            args.top,
            args.buffer_multiple,
            step,
            CAPACITY_AUM_LEVELS,
        )
    if "overlay" in modes:
        report["reports"]["overlay"] = report_overlay(
            frame, icir, args.label, args.top, args.buffer_multiple, step
        )
    if "accounts" in modes:
        report["reports"]["accounts"] = report_accounts(
            frame, icir, args.label, args.buffer_multiple, step, 5.0
        )
    if "sprint" in modes:
        report["reports"]["sprint"] = report_sprint(
            frame,
            zscores,
            components,
            args.lookback,
            args.min_ic_dates,
            args.sprint_aum,
            sprint_steps,
            tuple(int(part) for part in args.sprint_sizes.split(",") if part.strip()),
            args.buffer_multiple,
            5.0,
        )
    if "oos" in modes:
        oos = report_oos(
            frame,
            zscores,
            ic_by_date,
            components,
            panel=args.panel,
            step=step,
            names=args.names,
            buffer_multiple=args.buffer_multiple,
            aum=args.aum,
            lookback=args.lookback,
            min_ic_dates=args.min_ic_dates,
            label=weight_label,
            label_lag=label_lag,
            train_sessions=args.train_sessions,
            test_sessions=args.test_sessions,
            embargo=args.embargo,
        )
        report["reports"]["oos"] = oos
        report["config"] = oos["config"]
    payload = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
