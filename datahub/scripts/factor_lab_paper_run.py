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


def route_logs_to_stderr() -> None:
    """Keep stdout clean for the JSON payload (the app config logs to stdout)."""
    import logging

    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)


def side_cost(notional: float) -> float:
    """One side of a trade: half the documented round trip + the min commission.

    ``metrics.round_trip_cost()`` already embeds the proportional commission for
    both sides, so only the *shortfall* up to the CNY 5 per-trade minimum is
    added on each side; charging the whole minimum would double count it.
    """
    from app.lib.factor_lab import metrics

    half_round_trip = metrics.round_trip_cost() / 2.0 * notional
    minimum_top_up = max(0.0, MIN_COMMISSION - COMMISSION_RATE * notional)
    return half_round_trip + minimum_top_up


def flag(value) -> bool:
    """Boolean panel flag that tolerates None/NaN/pandas NA."""
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        return False
    return bool(value)


def tradeable_open(row: pd.Series | None, side: str) -> float | None:
    """Open price when the session allows that side, else None (blocked)."""
    if row is None:
        return None
    price = row.get("open")
    if price is None or not np.isfinite(price) or price <= 0:
        return None
    status = row.get("trade_status")
    if status is None:
        return None
    try:
        if pd.isna(status) or int(status) != 1:
            return None
    except (TypeError, ValueError):
        return None
    if side == "buy" and flag(row.get("limit_up")):
        return None
    if side == "sell" and flag(row.get("limit_down")):
        return None
    return float(price)


def close_price(row: pd.Series | None) -> float | None:
    """Mark price: HFQ close with the raw close as fallback, else None."""
    if row is None:
        return None
    for column in ("close_hfq", "close"):
        value = row.get(column)
        if value is not None and np.isfinite(value) and value > 0:
            return float(value)
    return None


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
    # Report the IC mean over the ICs the weights could actually have used, not
    # a whole-panel mean that would look ahead.
    all_sessions = sorted(frame["date"].unique())
    as_of = (
        pd.Timestamp(args.as_of) if getattr(args, "as_of", None) else all_sessions[-1]
    )
    cutoff_index = None
    for position, value in enumerate(all_sessions):
        if pd.Timestamp(value) <= as_of:
            cutoff_index = position
    cutoff = (
        pd.Timestamp(all_sessions[max(0, cutoff_index - lag)])
        if cutoff_index is not None
        else None
    )
    history = (
        ic_by_date[components[0]].loc[:cutoff]
        if cutoff is not None
        else ic_by_date[components[0]]
    ).tail(args.lookback)
    return {
        "score": score,
        "components": list(components),
        "lag_sessions": lag,
        "ic_cutoff": str(cutoff.date()) if cutoff is not None else None,
        "factor_ic_mean": {
            name: float(ic_by_date[name].reindex(history.index).mean())
            for name in components
        },
    }


def read_state(state: Path) -> list[dict]:
    """Read decision records, refusing any whose payload hash does not match."""
    if not state.exists():
        return []
    records = []
    for path in sorted(state.glob("*.decision.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        recorded = payload.pop("payload_sha256", None)
        recomputed = hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        if recorded != recomputed:
            raise ValueError(
                f"decision record failed its integrity check: {path} "
                f"(recorded {recorded}, recomputed {recomputed})"
            )
        payload["payload_sha256"] = recorded
        payload["record_path"] = str(path)
        records.append(payload)
    return records


def previous_basket(records: list[dict]) -> list[str]:
    if not records:
        return []
    return list(records[-1]["target_basket"])


def decide(args: argparse.Namespace) -> dict:
    state = Path(args.state)
    labels = tuple({args.label})
    frame = load_frame(args.panel, labels)
    all_sessions = sessions(frame)
    as_of = pd.Timestamp(args.as_of) if args.as_of else all_sessions[-1]
    if as_of not in set(all_sessions):
        as_of = max(value for value in all_sessions if value <= as_of)
    horizon = core().label_horizon(args.label)
    if horizon and horizon != args.rebalance_sessions:
        raise ValueError(
            f"--label {args.label} holds {horizon} sessions but "
            f"--rebalance-sessions is {args.rebalance_sessions}; they must match"
        )
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
        price = float(book.loc[code, "close"])
        indicative_lots[code] = max(int(np.floor(slice_amount / (price * LOT))), 0)
        weights[code] = round(1.0 / args.names, 6)
    intended_trades = [
        {"intent": "increase-to-target", "stock_code": code}
        for code in basket
        if code not in previous
    ] + [
        {"intent": "exit", "stock_code": code}
        for code in previous
        if code not in basket
    ]
    expected_invested = sum(
        lots * LOT * float(book.loc[code, "close"])
        for code, lots in indicative_lots.items()
    )
    idempotency_key = hashlib.sha256(
        "|".join(
            [
                args.ledger_id,
                str(as_of.date()),
                str(execution_session.date()),
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
        "research_intent_only": "these entries describe research intent; they are "
        "not orders, execution instructions or advice",
        "decision_date": str(as_of.date()),
        "execution_session": str(execution_session.date()),
        "execution_note": "fill at the execution session's open, subject to "
        "limit-up/suspension availability; final share counts are fixed by mark",
        "aum": args.aum,
        "names": args.names,
        "buffer_multiple": args.buffer_multiple,
        "weighting": args.weighting,
        "label": args.label,
        "components": bundle["components"],
        "weight_label_lag_sessions": bundle["lag_sessions"],
        "factor_ic_mean_through_cutoff": bundle["factor_ic_mean"],
        "ic_cutoff": bundle["ic_cutoff"],
        "rebalance_sessions": args.rebalance_sessions,
        "universe_size": int(len(today)),
        "median_book_adv_cny": float(
            np.nanmedian(book["__adv20"].to_numpy(dtype="float64"))
        ),
        "target_basket": basket,
        "target_weights": weights,
        "indicative_lots_at_decision_close": indicative_lots,
        "indicative_prices_close": {
            code: float(book.loc[code, "close"]) for code in basket
        },
        "intended_trades": intended_trades,
        "previous_basket": previous,
        "expected_invested": round(expected_invested, 2),
        "expected_cash": round(args.aum - expected_invested, 2),
        "idempotency_key": idempotency_key,
        "dry_run": bool(args.dry_run),
    }
    payload["payload_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    path = state / f"{as_of.date()}.decision.json"
    if not args.dry_run:
        state.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise FileExistsError(f"decision already recorded (append-only): {path}")
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    payload["record_path"] = None if args.dry_run else str(path)
    return payload


def mark(args: argparse.Namespace) -> dict:
    state = Path(args.state)
    records = read_state(state)
    if not records:
        raise ValueError(f"no decision records in {state}")
    frame = load_frame(args.panel, (args.label,))
    all_sessions = sessions(frame)
    index = {value: position for position, value in enumerate(all_sessions)}

    decisions = {}
    for record in records:
        decision = pd.Timestamp(record["decision_date"])
        execution = pd.Timestamp(record["execution_session"])
        if decision not in index:
            raise ValueError(
                f"decision date {decision.date()} of {record['record_path']} is "
                "not a session in this panel"
            )
        if execution not in index:
            raise ValueError(
                f"recorded execution session {execution.date()} of "
                f"{record['record_path']} is not a session in this panel"
            )
        decisions[execution] = record

    lookup = frame.set_index(["date", "stock_code"], drop=False).sort_index()

    def quote(value: pd.Timestamp, code: str) -> pd.Series | None:
        try:
            return lookup.loc[(value, code)]
        except KeyError:
            return None

    aum = float(records[0]["aum"])
    first_execution = min(decisions)
    cash = aum
    positions: dict[str, int] = {}
    pending_sell: set[str] = set()
    last_price: dict[str, float] = {}
    missing_sessions: dict[str, int] = {}
    nav_rows: list[dict] = []
    decision_reports: list[dict] = []
    fees_total = 0.0
    traded_total = 0.0
    benchmark_units: dict[str, float] = {}
    benchmark_aum = 0.0
    missing_limit = 20

    for value in all_sessions:
        if value < first_execution:
            continue
        record = decisions.get(value)
        if not benchmark_units and record is not None:
            benchmark_aum = aum
            day = frame[frame["date"] == value]
            universe = day[
                (~day["is_bse"].fillna(False))
                & (~day["is_st"].fillna(False))
                & day["open"].notna()
            ]
            for code in universe["stock_code"]:
                price = tradeable_open(quote(value, code), "buy")
                if price is None:
                    continue
                benchmark_units[code] = (aum / len(universe)) / price

        if record is not None:
            target = set(record["target_basket"])
            for code in list(positions):
                if code not in target:
                    pending_sell.add(code)

        # Sell anything pending, every session, until it becomes executable.
        for code in sorted(pending_sell):
            price = tradeable_open(quote(value, code), "sell")
            if price is None:
                continue
            lots = positions.pop(code)
            notional = lots * LOT * price
            cost = side_cost(notional)
            cash += notional - cost
            fees_total += cost
            traded_total += notional
            pending_sell.discard(code)

        traded_decision = 0.0
        if record is not None:
            target = list(record["target_basket"])
            slice_amount = cash / max(len(target), 1)
            unfilled: list[str] = []
            cash_skipped: list[str] = []
            for code in target:
                price = tradeable_open(quote(value, code), "buy")
                if price is None:
                    unfilled.append(code)
                    continue
                desired = int(np.floor(slice_amount / (price * LOT)))
                delta = desired - positions.get(code, 0)
                if delta <= 0:
                    continue
                notional = delta * LOT * price
                cost = side_cost(notional)
                if notional + cost > cash:
                    cash_skipped.append(code)
                    continue
                cash -= notional + cost
                fees_total += cost
                traded_total += notional
                traded_decision += notional
                positions[code] = positions.get(code, 0) + delta
            nav_here = cash + sum(
                lots * LOT * last_price.get(code, 0.0)
                for code, lots in positions.items()
            )
            decision_reports.append(
                {
                    "decision_date": record["decision_date"],
                    "execution_session": str(value.date()),
                    "target": target,
                    "unfilled": unfilled,
                    "cash_skipped": cash_skipped,
                    "cash_after": round(cash, 2),
                    "traded_notional": round(traded_decision, 2),
                    "turnover": round(traded_decision / nav_here, 4)
                    if nav_here
                    else None,
                    "pending_exits": sorted(pending_sell),
                    "positions": {code: lots for code, lots in positions.items()},
                }
            )

        for code in positions:
            price = close_price(quote(value, code))
            if price is None:
                # Suspension: carry the last known price but count the gap; a
                # name that never comes back is a delisting the operator must
                # handle, never something to value at zero.
                missing_sessions[code] = missing_sessions.get(code, 0) + 1
                if missing_sessions[code] > missing_limit:
                    raise ValueError(
                        f"{code} has no usable close for "
                        f"{missing_sessions[code]} sessions; delisting needs an "
                        "explicit write-off"
                    )
                continue
            missing_sessions[code] = 0
            last_price[code] = price
        book_value = sum(
            lots * LOT * last_price.get(code, 0.0) for code, lots in positions.items()
        )
        benchmark_value = sum(
            units * last_price.get(code, 0.0) for code, units in benchmark_units.items()
        )
        nav_rows.append(
            {
                "date": str(value.date()),
                "cash": round(cash, 2),
                "book_value": round(book_value, 2),
                "nav": round(cash + book_value, 2),
                "benchmark": round(benchmark_value, 2),
                "positions": len(positions),
                "pending_exits": len(pending_sell),
            }
        )

    nav = np.array([row["nav"] for row in nav_rows], dtype="float64")
    benchmark = np.array([row["benchmark"] for row in nav_rows], dtype="float64")
    nav_series = nav / aum if aum else nav
    peak = np.maximum.accumulate(nav_series)
    drawdown = float((nav_series / peak - 1.0).min()) if len(nav_series) else 0.0
    years = len(nav_series) / TRADING_DAYS if nav_series.size else 0.0
    benchmark_series = (
        benchmark / benchmark_aum if benchmark_aum else np.zeros_like(benchmark)
    )
    benchmark_peak = np.maximum.accumulate(benchmark_series)
    nav_average = float(np.mean(nav)) if nav.size else aum
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
        "turnover": round(traded_total / nav_average, 4) if nav_average else None,
        "traded_notional_total": round(traded_total, 2),
        "fees_paid": round(fees_total, 2),
        "benchmark_total_return": float(benchmark_series[-1] - 1.0)
        if benchmark_series.size
        else 0.0,
        "benchmark_daily_max_drawdown": float(
            (benchmark_series / benchmark_peak - 1.0).min()
        )
        if benchmark_series.size
        else 0.0,
        "cash": round(cash, 2),
        "book": {code: lots for code, lots in positions.items()},
        "pending_exits": sorted(pending_sell),
        "decision_reports": decision_reports,
        "nav": nav_rows,
        "note": "REPLAY-only research ledger: records were written before their "
        "outcomes existed, but this is not forward evidence and does not count "
        "toward the immutable-forward window. Retained names are traded on the "
        "delta; blocked entries are skipped with the cash left idle and never "
        "back-filled; blocked exits are retried every session until executable.",
    }
    if args.output:
        Path(args.output).write_text(
            json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    return report


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    route_logs_to_stderr()
    # Importing the app package installs a stdout log handler, so keep stdout
    # reserved for the JSON payload: everything logged during the run goes to
    # stderr, and only the final document is printed here.
    real_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        payload = decide(args) if args.command == "decide" else mark(args)
    finally:
        sys.stdout = real_stdout
    print(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
