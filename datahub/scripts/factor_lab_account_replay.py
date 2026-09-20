"""Daily account replay without ex-post screening (research-only, REPLAY).

The composite backtest disclosed two credibility gaps: basket eligibility
required a non-null forward label (so names whose exit turned out untradeable
were dropped ex post), and drawdown was sampled once per rebalance, hiding
intra-holding losses. This script replays the same strategy as a real account:

* **Selection uses only information available at the decision close** - price,
  volume/turnover, ST/BSE flags and an optional liquidity floor. No forward
  label is required; a name is never dropped because its future turned out
  awkward.
* **The account is marked every session** from the execution open onward, with
  closes (HFQ, raw close as fallback), so drawdown is a daily figure.
* **Execution reality stays inside the account**: a limit-up or suspended entry
  is not filled and the cash stays idle (no back-fill), a blocked exit is retried
  every session until it fills, a name with no market row carries its last price
  and raises after a long gap, and every failed fill, cash balance and fee is
  reported.

Both variants are reported side by side: the old ex-post-screened book (forward
label required, rebalance-sampled drawdown) and the decision-time book with
daily marks, so the size of the bias is visible.
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

TRADING_DAYS = 244
LOT = 100
MIN_COMMISSION = 5.0
COMMISSION_RATE = 0.00025
PANEL_FIELDS = (
    "date",
    "stock_code",
    "close",
    "close_hfq",
    "open",
    "open_hfq",
    "volume",
    "trade_amount",
    "is_bse",
    "is_st",
    "trade_status",
    "limit_up",
    "limit_down",
    "previous_close",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", required=True)
    parser.add_argument(
        "--start-date",
        default=None,
        help="First evaluated session. Factors, ICs and walk-forward weights "
        "still use the full panel, so a windowed run is the in-sample arm for a "
        "rolling out-of-sample run on the same window.",
    )
    parser.add_argument(
        "--end-date", default=None, help="Last evaluated session (inclusive)."
    )
    parser.add_argument("--label", default="fwd_h20", help="Weight-estimation label.")
    parser.add_argument("--step", type=int, default=60, help="Rebalance sessions.")
    parser.add_argument("--names", type=int, default=10)
    parser.add_argument("--buffer-multiple", type=int, default=2)
    parser.add_argument("--aum", type=float, default=50_000.0)
    parser.add_argument(
        "--liquidity-floor",
        type=float,
        default=0.0,
        help="Drop the bottom fraction by 20-session mean turnover.",
    )
    parser.add_argument("--components", default=None)
    parser.add_argument("--lookback", type=int, default=TRADING_DAYS)
    parser.add_argument("--min-ic-dates", type=int, default=120)
    parser.add_argument("--output", default=None)
    return parser.parse_args(argv)


def flag(value) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        return False
    return bool(value)


def side_cost(notional: float) -> float:
    from app.lib.factor_lab import metrics

    half = metrics.round_trip_cost() / 2.0 * notional
    return half + max(0.0, MIN_COMMISSION - COMMISSION_RATE * notional)


def tradeable_open(row, side: str) -> float | None:
    if row is None:
        return None
    price = row.get("open")
    if price is None or not np.isfinite(price) or price <= 0:
        return None
    status = row.get("trade_status")
    try:
        if status is None or pd.isna(status) or int(status) != 1:
            return None
    except (TypeError, ValueError):
        return None
    if side == "buy" and flag(row.get("limit_up")):
        return None
    if side == "sell" and flag(row.get("limit_down")):
        return None
    return float(price)


def mark_price(row) -> float | None:
    if row is None:
        return None
    for column in ("close_hfq", "close"):
        value = row.get(column)
        if value is not None and np.isfinite(value) and value > 0:
            return float(value)
    return None


def build_target(
    frame: pd.DataFrame,
    value: pd.Timestamp,
    previous: list[str],
    *,
    names: int,
    buffer_multiple: int,
    liquidity_floor: float,
) -> list[str]:
    """Top-``names`` basket at ``value`` with the hold-while-in-band buffer.

    Selection uses only data known at the decision close: ``__score``,
    ``__raw_close``, the ST/BSE flags and the optional trailing liquidity floor.
    """
    day = frame[frame["date"] == value]
    price = pd.to_numeric(day["__raw_close"], errors="coerce")
    eligible = (
        (~day["is_bse"].fillna(False))
        & (~day["is_st"].fillna(False))
        & np.isfinite(day["__score"])
        & np.isfinite(price)
        & (price > 0)
    )
    if liquidity_floor > 0:
        eligible &= day["__liquid_rank"] >= (1.0 - liquidity_floor)
    day = day[eligible].sort_values("__score", ascending=False)
    codes = list(day["stock_code"])
    rank_of = {code: rank for rank, code in enumerate(codes)}
    if previous:
        keep = [
            code
            for code in codes
            if code in set(previous) and rank_of[code] < names * buffer_multiple
        ]
        additions = [code for code in codes if code not in set(previous)]
        return keep[:names] + additions[: max(0, names - len(keep))]
    return codes[:names]


def replay_book(
    frame: pd.DataFrame,
    *,
    sessions: list[pd.Timestamp],
    step: int,
    names: int,
    buffer_multiple: int,
    aum: float,
    liquidity_floor: float = 0.0,
    label: str | None = None,
    require_label: bool = False,
    daily_marks: bool = True,
    include_nav: bool = False,
) -> dict:
    """Daily account replay of the book ranked by ``frame['__score']``.

    ``frame`` carries ``__score``, ``__raw_close`` and ``__liquid_rank`` plus the
    execution columns ``open``, ``trade_status``, ``limit_up`` and
    ``limit_down``. ``sessions`` is the replay window and the rebalance schedule
    is anchored to its first session, so a caller can replay a single fold of a
    rolling out-of-sample split; ``include_nav`` also returns the session rows.
    """
    lookup = frame.set_index(["date", "stock_code"], drop=False).sort_index()

    def quote(value: pd.Timestamp, code: str):
        try:
            return lookup.loc[(value, code)]
        except KeyError:
            return None

    cash = aum
    positions: dict[str, int] = {}
    pending_sell: set[str] = set()
    last_price: dict[str, float] = {}
    missing: dict[str, int] = {}
    entry_raw: dict[str, float] = {}
    entry_hfq: dict[str, float] = {}
    written_off: dict[str, float] = {}
    delisted: list[str] = []
    failed_entries = 0
    failed_exits = 0
    attempted_entries = 0
    traded = 0.0
    fees = 0.0
    nav_rows = []
    previous: list[str] = []
    rebalance_positions = [position for position in range(0, len(sessions) - 1, step)]
    for position, value in enumerate(sessions):
        if position in rebalance_positions:
            target = build_target(
                frame,
                value,
                previous,
                names=names,
                buffer_multiple=buffer_multiple,
                liquidity_floor=liquidity_floor,
            )
            execution = sessions[position + 1] if position + 1 < len(sessions) else None
            if execution is not None and require_label:
                # the old book required a realised, tradeable exit label
                target = [
                    code
                    for code in target
                    if np.isfinite(
                        frame[(frame["date"] == value) & (frame["stock_code"] == code)][
                            label
                        ].iloc[0]
                    )
                ]
            previous = target
            if execution is not None:
                for code in list(positions):
                    if code not in set(target):
                        pending_sell.add(code)
                for code in sorted(pending_sell):
                    price = tradeable_open(quote(execution, code), "sell")
                    if price is None:
                        failed_exits += 1
                        continue
                    lots = positions.pop(code)
                    notional = lots * LOT * price
                    cost = side_cost(notional)
                    cash += notional - cost
                    fees += cost
                    traded += notional
                    pending_sell.discard(code)
                book_now = sum(
                    lots * LOT * last_price.get(code, 0.0)
                    for code, lots in positions.items()
                )
                slice_amount = (cash + book_now) / max(len(target), 1)
                for code in target:
                    attempted_entries += 1
                    price = tradeable_open(quote(execution, code), "buy")
                    if price is None:
                        failed_entries += 1
                        continue
                    desired = int(np.floor(slice_amount / (price * LOT)))
                    delta = desired - positions.get(code, 0)
                    if delta <= 0:
                        continue
                    notional = delta * LOT * price
                    cost = side_cost(notional)
                    if notional + cost > cash:
                        failed_entries += 1
                        continue
                    cash -= notional + cost
                    fees += cost
                    traded += notional
                    positions[code] = positions.get(code, 0) + delta
                    entry_raw[code] = price
                    row = quote(execution, code)
                    hfq_entry = row.get("open_hfq") if row is not None else None
                    if (
                        hfq_entry is None
                        or not np.isfinite(hfq_entry)
                        or hfq_entry <= 0
                    ):
                        hfq_entry = row.get("close_hfq") if row is not None else None
                    entry_hfq[code] = (
                        float(hfq_entry)
                        if hfq_entry and np.isfinite(hfq_entry) and hfq_entry > 0
                        else None
                    )
        else:
            for code in sorted(pending_sell):
                price = tradeable_open(quote(value, code), "sell")
                if price is None:
                    failed_exits += 1
                    continue
                lots = positions.pop(code)
                notional = lots * LOT * price
                cost = side_cost(notional)
                cash += notional - cost
                fees += cost
                traded += notional
                pending_sell.discard(code)
        if not daily_marks and position not in rebalance_positions:
            continue
        for code in positions:
            row_today = quote(value, code)
            hfq_today = row_today.get("close_hfq") if row_today is not None else None
            if (
                hfq_today is not None
                and np.isfinite(hfq_today)
                and hfq_today > 0
                and entry_hfq.get(code)
                and entry_raw.get(code)
            ):
                last_price[code] = entry_raw[code] * float(hfq_today) / entry_hfq[code]
                missing[code] = 0
                continue
            price = mark_price(row_today)
            if price is None:
                missing[code] = missing.get(code, 0) + 1
                if missing[code] > 20 and not written_off.get(code):
                    # Delisting / long suspension: the position stays in
                    # the account and is written off at the last known
                    # price, never dropped from the book.
                    written_off[code] = last_price.get(code, 0.0)
                    last_price[code] = 0.0
                    delisted.append(code)
                continue
            missing[code] = 0
            last_price[code] = price
        book = sum(
            lots * LOT * last_price.get(code, 0.0) for code, lots in positions.items()
        )
        if cash < -1e-6:
            raise ValueError(f"negative cash at {value.date()}: {cash}")
        book_check = sum(
            lots * LOT * last_price.get(code, 0.0) for code, lots in positions.items()
        )
        if any(lots <= 0 for lots in positions.values()):
            raise ValueError(f"non-positive position at {value.date()}")
        nav_rows.append(
            {
                "date": str(value.date()),
                "book_check": round(book_check, 2),
                "nav": round(cash + book, 2),
                "cash_share": round(cash / (cash + book), 4) if cash + book else None,
                "positions": len(positions),
            }
        )
    nav = np.array([row["nav"] for row in nav_rows], dtype="float64") / aum
    if len(nav_rows) > 1:
        span_days = (
            pd.Timestamp(nav_rows[-1]["date"]) - pd.Timestamp(nav_rows[0]["date"])
        ).days
        span_years = span_days / 365.25
    else:
        span_years = 0.0
    peak = np.maximum.accumulate(nav)
    years = len(nav) / (TRADING_DAYS / (1 if daily_marks else step))
    result = {
        "sessions": len(nav_rows),
        "final_nav": round(float(nav[-1]) * aum, 2) if nav.size else aum,
        "total_return": round(float(nav[-1] - 1.0), 4) if nav.size else 0.0,
        "annualised": round(float(nav[-1] ** (1.0 / years) - 1.0), 4)
        if years > 0.5 and nav.size
        else None,
        "max_drawdown": round(float((nav / peak - 1.0).min()), 4) if nav.size else 0.0,
        "turnover": round(traded / (np.mean(nav) * aum), 4) if nav.size else None,
        "turnover_annual": round(traded / (np.mean(nav) * aum * span_years), 4)
        if nav.size and span_years > 0.5
        else None,
        "cash_share_avg": round(
            float(
                np.mean(
                    [
                        row["cash_share"]
                        for row in nav_rows
                        if row["cash_share"] is not None
                    ]
                )
            ),
            4,
        )
        if nav_rows
        else None,
        "failed_entry_fills": failed_entries,
        "failed_exit_fills": failed_exits,
        "attempted_entries": attempted_entries,
        "fees_paid": round(fees, 2),
        "final_positions": len(positions),
        "pending_exits": len(pending_sell),
        "delisted_positions": sorted(delisted),
        "delisted_write_off_cny": round(float(sum(written_off.values())), 2),
    }
    if include_nav:
        result["nav"] = nav_rows
        result["traded_notional"] = round(traded, 2)
    return result


def main(argv: list[str] | None = None) -> None:
    import logging

    for handler in list(logging.getLogger().handlers):
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)

    args = parse_args(argv)
    import factor_lab_composite_backtest as core
    from app.lib.factor_lab import factors

    labels = tuple({args.label})
    frame = core.load_panel(args.panel, labels)
    extra = pd.read_parquet(args.panel, columns=list(PANEL_FIELDS))
    extra["date"] = pd.to_datetime(extra["date"])
    extra = extra[["date", "stock_code", "trade_status", "limit_up", "limit_down"]]
    frame = frame.merge(extra, on=["date", "stock_code"], how="left")
    frame["__raw_close"] = pd.to_numeric(frame["close"], errors="coerce")

    components = (
        tuple(part for part in args.components.split(",") if part.strip())
        if args.components
        else tuple(core.DEFAULT_COMPONENTS)
    )
    zscores = {}
    ic_by_date = {}
    for name in components:
        values = factors.compute(frame, name)
        zscores[name] = core.zscore_by_date(values, frame["date"])
        ic_by_date[name] = core.daily_ic(frame, values, args.label)
    lag = core.label_horizon(args.label) + 1
    score = core.walk_forward_composite(
        frame, zscores, ic_by_date, components, args.lookback, args.min_ic_dates, lag
    )
    frame["__score"] = score

    sessions = [pd.Timestamp(value) for value in sorted(frame["date"].unique())]
    if args.start_date or args.end_date:
        # Restrict only the evaluated sessions. Factors, ICs and walk-forward
        # weights stay full-panel, so a windowed run reproduces the same model
        # as a full run and can be compared with a rolling out-of-sample arm.
        lower = pd.Timestamp(args.start_date) if args.start_date else sessions[0]
        upper = pd.Timestamp(args.end_date) if args.end_date else sessions[-1]
        sessions = [value for value in sessions if lower <= value <= upper]
        if len(sessions) < 2:
            raise ValueError(
                f"the requested window {lower.date()}..{upper.date()} holds "
                f"{len(sessions)} panel sessions; at least 2 are required"
            )

    def replay(require_label: bool, daily_marks: bool) -> dict:
        return replay_book(
            frame,
            sessions=sessions,
            step=args.step,
            names=args.names,
            buffer_multiple=args.buffer_multiple,
            aum=args.aum,
            liquidity_floor=args.liquidity_floor,
            label=args.label,
            require_label=require_label,
            daily_marks=daily_marks,
        )

    report = {
        "panel": args.panel,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "step_sessions": args.step,
        "names": args.names,
        "aum": args.aum,
        "liquidity_floor": args.liquidity_floor,
        "components": list(components),
        "weight_label": args.label,
        "weight_label_lag_sessions": lag,
        "ex_post_screened_rebalance_sampled": replay(True, False),
        "decision_time_rebalance_sampled": replay(False, False),
        "decision_time_daily": replay(False, True),
        "note": "Selection in 'decision_time_daily' uses only close-of-decision "
        "information (price, ST/BSE flags, optional liquidity floor); the old "
        "'ex_post_screened_rebalance_sampled' variant additionally required a "
        "realised forward label and sampled drawdown once per rebalance. "
        "Research measurement only.",
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
