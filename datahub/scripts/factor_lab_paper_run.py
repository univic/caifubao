"""Research paper-run ledger for the RIQ composite (research-only, REPLAY).

``riq_v1`` is the factor-composite book recorded in
``docs/operations/strategy-experiments-2026-08.md``: five registered
factor-lab factors, walk-forward weights estimated only from ICs whose labels
were already realised, a quarterly rebalance with a 2N hold-while-in-band
buffer, equal weight and 100-share lots.

This tool runs that book on real market data with virtual money. It is
research-only: it writes nothing to MongoDB, is never imported by production
code, and every record it writes is ``evidence_kind=REPLAY`` — a replay of a
decision that was recorded before its outcome existed, but **not** forward
evidence and never counted toward the 120-session immutable-forward window that
``strategy-forward-evidence-capture`` governs.

Two steps, deliberately separated:

``decide``
    Run after a session's close (or later, with ``--as-of``): compute the causal
    composite, apply the buffer against the previously recorded basket, size it
    for the virtual account and write an **immutable** decision record. The
    record states the intended execution session (the next session's open); the
    final share counts are fixed by ``mark`` from that open, because the
    decision cannot know it.

``mark``
    Replay the recorded decisions against the panel: fill at the execution
    session's open, skip entries that were untradeable (limit-up at the open or
    suspended) with the cash left idle, roll exits that were blocked, charge the
    fixed round-trip cost plus the per-trade minimum commission, and report the
    daily NAV path, drawdown, per-decision P&L and the equal-weight-universe
    benchmark.
"""

from __future__ import annotations

import argparse
import hashlib
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

MODEL_NAME = "riq_v1"
EVIDENCE_KIND = "REPLAY"
TRADING_DAYS = 244
LOT = 100
MIN_COMMISSION = 5.0
COMMISSION_RATE = 0.00025
EXECUTION_COLUMNS = ("limit_up", "limit_down", "trade_status", "previous_close")


def side_cost(notional: float) -> float:
    """One side of a trade: half the documented round trip + the min commission.

    ``metrics.round_trip_cost()`` already covers a full buy+sell, so charging it
    per side would double the documented friction; half per side keeps one round
    trip per round trip while the CNY 5 minimum still applies to every trade.
    """
    from app.lib.factor_lab import metrics

    return metrics.round_trip_cost() / 2.0 * notional + max(
        MIN_COMMISSION, COMMISSION_RATE * notional
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    decide = sub.add_parser("decide", help="Record one paper decision.")
    decide.add_argument("--panel", required=True)
    decide.add_argument("--state", required=True, help="Paper state directory.")
    decide.add_argument(
        "--ledger-id",
        default=MODEL_NAME,
        help="Research ledger identifier; not a registered score model version.",
    )
    decide.add_argument(
        "--as-of", default=None, help="Decision session (default: latest)."
    )
    decide.add_argument(
        "--execution-session",
        default=None,
        help="Intended execution session (default: the next panel session; "
        "required when the panel ends at the decision date).",
    )
    decide.add_argument("--aum", type=float, default=50_000.0)
    decide.add_argument("--names", type=int, default=8)
    decide.add_argument("--buffer-multiple", type=int, default=2)
    decide.add_argument("--label", default="fwd_h60")
    decide.add_argument("--weighting", choices=("sign", "icir"), default="sign")
    decide.add_argument("--lookback", type=int, default=TRADING_DAYS)
    decide.add_argument("--min-ic-dates", type=int, default=120)
    decide.add_argument("--rebalance-sessions", type=int, default=60)
    decide.add_argument("--components", default=None)
    decide.add_argument("--dry-run", action="store_true")

    mark = sub.add_parser("mark", help="Replay decisions into a NAV path.")
    mark.add_argument("--panel", required=True)
    mark.add_argument("--state", required=True)
    mark.add_argument("--label", default="fwd_h60")
    mark.add_argument("--output", default=None)
    return parser.parse_args(argv)


def core():  # pragma: no cover - thin import shim
    import factor_lab_composite_backtest as module

    return module


def load_frame(panel: str, labels: tuple[str, ...]) -> pd.DataFrame:
    backtest = core()
    frame = backtest.load_panel(panel, labels)
    extra = pd.read_parquet(
        panel,
        columns=["date", "stock_code", *EXECUTION_COLUMNS],
    )
    extra["date"] = pd.to_datetime(extra["date"])
    return frame.merge(extra, on=["date", "stock_code"], how="left")


def sessions(frame: pd.DataFrame) -> list[pd.Timestamp]:
    return [pd.Timestamp(value) for value in sorted(frame["date"].unique())]


def composite_for(frame: pd.DataFrame, args: argparse.Namespace) -> dict:
    backtest = core()
    components = (
        tuple(part for part in args.components.split(",") if part.strip())
        if getattr(args, "components", None)
        else backtest.DEFAULT_COMPONENTS
    )
    from app.lib.factor_lab import factors

    zscores = {}
    ic_by_date = {}
    for name in components:
        values = factors.compute(frame, name)
        zscores[name] = backtest.zscore_by_date(values, frame["date"])
        ic_by_date[name] = backtest.daily_ic(frame, values, args.label)
    lag = backtest.label_horizon(args.label) + 1
    score = backtest.walk_forward_composite(
        frame,
        zscores,
        ic_by_date,
        components,
        args.lookback,
        args.min_ic_dates,
        lag,
        weighting=getattr(args, "weighting", "sign"),
    )
    return {
        "score": score,
        "components": list(components),
        "lag_sessions": lag,
        "factor_ic_mean": {name: float(ic_by_date[name].mean()) for name in components},
    }


def read_state(state: Path) -> list[dict]:
    if not state.exists():
        return []
    records = []
    for path in sorted(state.glob("*.decision.json")):
        records.append(json.loads(path.read_text(encoding="utf-8")))
    return records


def previous_basket(records: list[dict]) -> list[str]:
    if not records:
        return []
    return list(records[-1]["target_basket"])


def decide(args: argparse.Namespace) -> dict:
    state = Path(args.state)
    state.mkdir(parents=True, exist_ok=True)
    labels = tuple({args.label})
    frame = load_frame(args.panel, labels)
    all_sessions = sessions(frame)
    as_of = pd.Timestamp(args.as_of) if args.as_of else all_sessions[-1]
    if as_of not in set(all_sessions):
        as_of = max(value for value in all_sessions if value <= as_of)
    future = [value for value in all_sessions if value > as_of]
    if args.execution_session:
        execution_session = pd.Timestamp(args.execution_session)
        if execution_session <= as_of:
            raise ValueError(
                f"--execution-session {execution_session.date()} must be strictly "
                f"after the decision date {as_of.date()}"
            )
    elif future:
        execution_session = future[0]
    else:
        raise ValueError(
            "the panel ends at the decision date, so the execution session is "
            "not knowable from it; pass --execution-session (next session per "
            "the trade calendar)."
        )

    bundle = composite_for(frame, args)
    score = bundle["score"]
    today = frame[frame["date"] == as_of].copy()
    # Selection only needs a usable close at D: the execution price is the
    # *next* session's open and is deliberately unknown here (the last session
    # of the panel has no next open at all). Execution-time availability is
    # handled by mark().
    close = pd.to_numeric(today["close"], errors="coerce")
    eligible = (
        (~today["is_bse"].fillna(False))
        & (~today["is_st"].fillna(False))
        & np.isfinite(close)
        & (close > 0)
    )
    today = today.assign(__score=score[today.index])
    today = today[eligible & np.isfinite(today["__score"])]

    previous = previous_basket(read_state(state))
    ranked = today.sort_values("__score", ascending=False)
    rank_of = {code: rank for rank, code in enumerate(ranked["stock_code"])}
    if previous:
        keep = [
            code
            for code in ranked["stock_code"]
            if code in set(previous)
            and rank_of[code] < args.names * args.buffer_multiple
        ]
        additions = [code for code in ranked["stock_code"] if code not in set(previous)]
        basket = keep[: args.names] + additions[: max(0, args.names - len(keep))]
    else:
        basket = list(ranked["stock_code"][: args.names])
    if len(basket) < args.names:
        raise ValueError(
            f"only {len(basket)} names survived selection for {args.names} slots"
        )

    book = today[today["stock_code"].isin(basket)].set_index("stock_code")
    slice_amount = args.aum / args.names
    weights = {}
    indicative_lots = {}
    for code in basket:
        close = float(book.loc[code, "close"])
        lots = int(np.floor(slice_amount / (close * LOT)))
        weights[code] = round(1.0 / args.names, 6)
        indicative_lots[code] = max(lots, 0)
    order = []
    for code in basket:
        if code not in previous:
            order.append({"side": "BUY", "stock_code": code})
    for code in previous:
        if code not in basket:
            order.append({"side": "SELL", "stock_code": code})

    expected_invested = sum(
        lots * LOT * float(book.loc[code, "close"])
        for code, lots in indicative_lots.items()
    )
    idempotency_key = hashlib.sha256(
        "|".join(
            [
                args.ledger_id,
                str(as_of.date()),
                str(execution_session.date()) if execution_session else "",
                ",".join(basket),
                f"{args.aum:.2f}",
            ]
        ).encode("utf-8")
    ).hexdigest()
    payload = {
        "ledger_id": args.ledger_id,
        "identifier_note": "research ledger identifier; NOT a registered score "
        "model version and NOT the score-driven paper track",
        "evidence_kind": EVIDENCE_KIND,
        "decision_date": str(as_of.date()),
        "execution_session": str(execution_session.date())
        if execution_session
        else None,
        "execution_note": "fill at the execution session's open, subject to "
        "limit-up/suspension availability; final share counts are fixed by mark",
        "aum": args.aum,
        "names": args.names,
        "buffer_multiple": args.buffer_multiple,
        "weighting": args.weighting,
        "label": args.label,
        "components": bundle["components"],
        "weight_label_lag_sessions": bundle["lag_sessions"],
        "factor_ic_mean_at_decision": bundle["factor_ic_mean"],
        "rebalance_sessions": args.rebalance_sessions,
        "universe_size": int(len(today)),
        "median_book_adv_cny": float(
            np.nanmedian(book["__adv20"].to_numpy(dtype="float64"))
        ),
        "target_basket": basket,
        "target_weights": weights,
        "indicative_lots_at_decision_close": indicative_lots,
        "expected_invested": round(expected_invested, 2),
        "expected_cash": round(args.aum - expected_invested, 2),
        "idempotency_key": idempotency_key,
        "indicative_prices_close": {
            code: float(book.loc[code, "close"]) for code in basket
        },
        "orders": order,
        "previous_basket": previous,
        "dry_run": bool(args.dry_run),
    }
    payload["payload_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    path = state / f"{as_of.date()}.decision.json"
    if not args.dry_run:
        if path.exists():
            raise FileExistsError(f"decision already recorded (append-only): {path}")
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    payload["record_path"] = None if args.dry_run else str(path)
    return payload


def _entry_price(row: pd.Series) -> float | None:
    if not np.isfinite(row.get("open", np.nan)) or row.get("open", 0) <= 0:
        return None
    if bool(row.get("limit_up", False)):
        return None
    if int(row.get("trade_status", 1) or 0) != 1:
        return None
    return float(row["open"])


def _exit_price(row: pd.Series) -> float | None:
    if not np.isfinite(row.get("open", np.nan)) or row.get("open", 0) <= 0:
        return None
    if bool(row.get("limit_down", False)):
        return None
    if int(row.get("trade_status", 1) or 0) != 1:
        return None
    return float(row["open"])


def mark(args: argparse.Namespace) -> dict:
    state = Path(args.state)
    records = read_state(state)
    if not records:
        raise ValueError(f"no decision records in {state}")
    frame = load_frame(args.panel, (args.label,))
    all_sessions = sessions(frame)
    index = {value: position for position, value in enumerate(all_sessions)}

    def next_session(value: pd.Timestamp) -> pd.Timestamp | None:
        position = index[value]
        return all_sessions[position + 1] if position + 1 < len(all_sessions) else None

    def row_at(value: pd.Timestamp, code: str) -> pd.Series | None:
        subset = frame[(frame["date"] == value) & (frame["stock_code"] == code)]
        return None if subset.empty else subset.iloc[0]

    aum = float(records[0]["aum"])
    cash = aum
    positions: dict[str, int] = {}
    nav_rows: list[dict] = []
    decision_reports: list[dict] = []
    fees_total = 0.0

    executions = []
    for record in records:
        decision = pd.Timestamp(record["decision_date"])
        execution = next_session(decision) if decision in index else None
        executions.append((record, execution))
    benchmark_start = None
    benchmark_units: dict[str, float] = {}
    last_price: dict[str, float] = {}
    benchmark_aum = 0.0

    current_record = None
    for position, value in enumerate(all_sessions):
        for record, execution in executions:
            if execution != value:
                continue
            if current_record is not None:
                # Liquidate the previous book at this open where tradeable.
                for code, lots in list(positions.items()):
                    row = row_at(value, code)
                    price = _exit_price(row) if row is not None else None
                    if price is None:
                        continue
                    notional = lots * LOT * price
                    cost = side_cost(notional)
                    cash += notional - cost
                    fees_total += cost
                    positions.pop(code, None)
            target = record["target_basket"]
            slice_amount = cash / max(len(target), 1)
            unfilled = []
            for code in target:
                row = row_at(value, code)
                price = _entry_price(row) if row is not None else None
                if price is None:
                    unfilled.append(code)
                    continue
                lots = int(np.floor(slice_amount / (price * LOT)))
                if lots <= 0:
                    unfilled.append(code)
                    continue
                notional = lots * LOT * price
                cost = side_cost(notional)
                if notional + cost > cash:
                    continue
                cash -= notional + cost
                fees_total += cost
                positions[code] = lots
            current_record = record
            decision_reports.append(
                {
                    "decision_date": record["decision_date"],
                    "execution_session": str(value.date()),
                    "target": target,
                    "unfilled": unfilled,
                    "cash_after": round(cash, 2),
                    "positions": {code: lots for code, lots in positions.items()},
                }
            )
            if benchmark_start is None:
                benchmark_start = value
                benchmark_aum = aum
                universe = frame[
                    (frame["date"] == value)
                    & (~frame["is_bse"].fillna(False))
                    & (~frame["is_st"].fillna(False))
                    & frame["open"].notna()
                ]
                for code in universe["stock_code"]:
                    row = row_at(value, code)
                    price = _entry_price(row) if row is not None else None
                    if price is None:
                        continue
                    benchmark_units[code] = (aum / len(universe)) / price

        if benchmark_start is None or value < benchmark_start:
            continue
        day = frame[frame["date"] == value]
        closes = {
            code: float(price)
            for code, price in zip(day["stock_code"], day["close_hfq"], strict=False)
            if price is not None and np.isfinite(price) and price > 0
        }
        last_price.update(closes)
        book_value = 0.0
        for code, lots in positions.items():
            book_value += lots * LOT * last_price.get(code, 0.0)
        benchmark_value = 0.0
        for code, units in benchmark_units.items():
            benchmark_value += units * last_price.get(code, 0.0)
        nav_rows.append(
            {
                "date": str(value.date()),
                "cash": round(cash, 2),
                "book_value": round(book_value, 2),
                "nav": round(cash + book_value, 2),
                "benchmark": round(benchmark_value, 2),
                "positions": len(positions),
            }
        )
    nav = np.array([row["nav"] for row in nav_rows], dtype="float64")
    benchmark = np.array([row["benchmark"] for row in nav_rows], dtype="float64")
    nav_series = nav / aum
    peak = np.maximum.accumulate(nav_series)
    drawdown = float((nav_series / peak - 1.0).min()) if len(nav_series) else 0.0
    benchmark_series = (
        benchmark / benchmark_aum if benchmark_aum else np.zeros_like(benchmark)
    )
    benchmark_peak = np.maximum.accumulate(benchmark_series)
    years = len(nav_series) / TRADING_DAYS if nav_series.size else 0.0
    report = {
        "ledger_id": records[0]["ledger_id"],
        "evidence_kind": EVIDENCE_KIND,
        "decisions": len(records),
        "nav_sessions": len(nav_rows),
        "from": nav_rows[0]["date"] if nav_rows else None,
        "to": nav_rows[-1]["date"] if nav_rows else None,
        "aum": aum,
        "final_nav": float(nav[-1]) if nav.size else aum,
        "total_return": float(nav_series[-1] - 1.0) if nav_series.size else 0.0,
        "annualised": float(nav_series[-1] ** (1.0 / years) - 1.0)
        if years > 0
        else None,
        "daily_max_drawdown": drawdown,
        "benchmark_total_return": float(benchmark_series[-1] - 1.0)
        if benchmark_series.size
        else 0.0,
        "benchmark_daily_max_drawdown": float(
            (benchmark_series / benchmark_peak - 1.0).min()
        )
        if benchmark_series.size
        else 0.0,
        "fees_paid": round(fees_total, 2),
        "cash": round(cash, 2),
        "book": {code: lots for code, lots in positions.items()},
        "decision_reports": decision_reports,
        "nav": nav_rows,
        "note": "REPLAY-only research ledger: records were written before their "
        "outcomes existed, but this is not forward evidence and does not count "
        "toward the immutable-forward window. Blocked entries are skipped with "
        "the cash left idle; blocked exits are rolled to the next session.",
    }
    if args.output:
        Path(args.output).write_text(
            json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    return report


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "decide":
        payload = decide(args)
    else:
        payload = mark(args)
    print(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
