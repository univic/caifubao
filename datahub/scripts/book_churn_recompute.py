# -*- coding: utf-8 -*-
"""Read-only recomputation of book churn across widths and exit bands (W0-A).

Why this script exists
----------------------
``docs/autoresearch/runs/small-book/`` reported narrow-book results that were
later invalidated: the signal had to be *reconstructed* because the frozen
factor-lab panel carries only price/volume features, and the reconstruction's
top-50 overlapped the production model by as few as 7/50 names. Its numbers must
not be cited. This script replaces them with a measurement that uses
**production** ``flip_wide_shadow_v1`` scores stored in ``stock_score_predictions``
and the **production** selection code (``plan_target_holdings``), so no part of
the decision is re-implemented here.

What it measures
----------------
One-way replacement turnover per scored session, and the friction that turnover
implies at the shared round-trip cost, for several book widths and exit bands.
Turnover is measured as ``replaced_names / width``: replacing ``k`` of ``N``
equal-weight names trades ``k/N`` of the book on each side, so one replacement
costs one round trip on that name's notional.

What it does NOT measure
------------------------
Returns. The available production score history is a few tens of sessions, which
cannot settle whether the signal has an edge in either direction. Any return
number read off this window is noise and MUST NOT be reported as evidence.

The script only reads Mongo. It never writes a document, never opens a forward
window and never places an order.

Usage
-----
    python -m scripts.book_churn_recompute --model-version flip_wide_shadow_v1
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from pathlib import Path
from types import SimpleNamespace

_HERE = Path(__file__).resolve()
PROJECT_ROOT = _HERE.parents[1] if len(_HERE.parents) > 1 else Path.cwd()
if not (PROJECT_ROOT / "app").is_dir():
    # Running from stdin (e.g. ``kubectl exec -- python -``); use the image root.
    PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.strategy_engine.selection import (  # noqa: E402
    compute_rebalance,
    plan_target_holdings,
)
from caifubao_contracts.friction import (  # noqa: E402
    COMMISSION_RATE,
    SELL_STAMP_DUTY_RATE,
    SLIPPAGE_PER_SIDE,
    TRANSFER_FEE_RATE,
)

#: One replacement = sell one name + buy one name.
ROUND_TRIP_COST = (
    2 * SLIPPAGE_PER_SIDE
    + 2 * COMMISSION_RATE
    + 2 * TRANSFER_FEE_RATE
    + SELL_STAMP_DUTY_RATE
)
#: Trading sessions per year, used only to annualise the observed cadence.
SESSIONS_PER_YEAR = 244.0
#: BSE codes are excluded by every strategy config in this repo.
EXCLUDED_PREFIXES = ("bj",)


def _db(args):
    import pymongo

    uri = args.uri or "mongodb://%s:%s@%s:%s/%s?authSource=admin" % (
        os.environ.get("MONGODB_USER", ""),
        os.environ.get("MONGODB_PASS", ""),
        os.environ.get("MONGODB_HOST", "localhost"),
        os.environ.get("MONGODB_PORT", "27017"),
        os.environ.get("MONGODB_NAME", "caifubao"),
    )
    return pymongo.MongoClient(
        uri, serverSelectionTimeoutMS=8000
    ).get_default_database()


def _load_scores(db, model_version, horizon):
    """Production scores grouped by session date, BSE and blank scores removed."""
    query = {"model_version": model_version, "horizon": horizon}
    projection = {"_id": 0, "date": 1, "stock_code": 1, "score": 1, "percentile": 1}
    grouped: dict[str, list[dict]] = {}
    for doc in db["stock_score_predictions"].find(query, projection):
        code = doc.get("stock_code") or ""
        if not code or code.startswith(EXCLUDED_PREFIXES):
            continue
        score = doc.get("score")
        if score is None:
            continue
        day = str(doc.get("date"))[:10]
        grouped.setdefault(day, []).append(
            {
                "stock_code": code,
                "score": float(score),
                "percentile": doc.get("percentile"),
            }
        )
    return grouped


def _with_percentile(rows):
    """Fill a missing percentile from the cross-section rank (1.0 = best)."""
    ordered = sorted(rows, key=lambda r: (-r["score"], r["stock_code"]))
    n = len(ordered)
    out = []
    for index, row in enumerate(ordered):
        percentile = row["percentile"]
        if percentile is None:
            percentile = 1.0 if n <= 1 else 1.0 - index / (n - 1)
        out.append(
            SimpleNamespace(
                stock_code=row["stock_code"],
                score=row["score"],
                percentile=float(percentile),
            )
        )
    return out


def _config(model_version, horizon, width):
    return {
        "score_model_version": model_version,
        "horizon": horizon,
        "selection": {
            "mode": "top_percentile",
            "lower": 0.20,
            "upper": 1.00,
            "portfolio_size": width,
        },
    }


def _load_trading_days(db, start_day, end_day):
    """Trading sessions with a published quote, as an ordered ISO-day list."""
    import datetime as dt

    lo = dt.datetime.fromisoformat(start_day)
    hi = dt.datetime.fromisoformat(end_day) + dt.timedelta(days=1)
    raw = db["stock_daily_quote"].distinct("date", {"date": {"$gte": lo, "$lt": hi}})
    return sorted(str(value)[:10] for value in raw)


def _chain(grouped, dates, trading_index, model_version, horizon, width, exit_buffer):
    """Walk the book through the scored sessions and record replacement churn.

    Turnover is reported twice: over every scored-date pair, and over the subset
    where the pair is **adjacent trading sessions**. The second set is the only
    one that measures a daily re-sort; a pair separated by a gap carries the
    accumulated change of the whole gap and would overstate daily churn.
    """
    config = _config(model_version, horizon, width)
    previous = None
    previous_day = None
    adjacent: list[float] = []
    gapped: list[float] = []
    sold_percentiles: list[float] = []
    per_session = []

    for day in dates:
        predictions = _with_percentile(grouped[day])
        percentile_by_code = {p.stock_code: p.percentile for p in predictions}
        plan = plan_target_holdings(
            predictions,
            config,
            previous_holdings=previous,
            exit_buffer=exit_buffer,
        )
        target = plan["holdings"]
        if previous is not None:
            rebalance = compute_rebalance(previous, target)
            replaced = len(rebalance["added"])
            # A partial book (thin eligible universe) trades at most what it holds.
            base = max(1, min(width, max(len(previous), len(target))))
            turnover = replaced / base
            is_adjacent = (
                trading_index.get(day) is not None
                and trading_index.get(previous_day) is not None
                and trading_index[day] == trading_index[previous_day] + 1
            )
            (adjacent if is_adjacent else gapped).append(turnover)
            sold_percentiles.extend(
                percentile_by_code.get(code, 0.0) for code in rebalance["removed"]
            )
            per_session.append(
                {
                    "date": day,
                    "adjacent": is_adjacent,
                    "held_before": len(previous),
                    "held_after": len(target),
                    "replaced": replaced,
                    "one_way_turnover": round(turnover, 6),
                    "friction_pct_of_book": round(turnover * ROUND_TRIP_COST * 100, 6),
                }
            )
        previous = target
        previous_day = day

    every = adjacent + gapped
    if not every:
        return {"width": width, "exit_buffer": exit_buffer, "sessions": 0}

    def _mean(values):
        return round(statistics.fmean(values), 6) if values else None

    def _annualised(values):
        return (
            round(statistics.fmean(values) * SESSIONS_PER_YEAR, 4) if values else None
        )

    adjacent_mean = statistics.fmean(adjacent) if adjacent else None
    adjacent_annual = (
        adjacent_mean * SESSIONS_PER_YEAR if adjacent_mean is not None else None
    )
    return {
        "width": width,
        "exit_buffer": exit_buffer,
        "sessions": len(every),
        "adjacent_pairs": len(adjacent),
        "gapped_pairs": len(gapped),
        "mean_one_way_turnover_adjacent": _mean(adjacent),
        "mean_one_way_turnover_gapped": _mean(gapped),
        "median_one_way_turnover_adjacent": (
            round(statistics.median(adjacent), 6) if adjacent else None
        ),
        "annualised_one_way_turnover_adjacent": _annualised(adjacent),
        "annualised_friction_pct_adjacent": (
            round(adjacent_annual * ROUND_TRIP_COST * 100, 4)
            if adjacent_annual is not None
            else None
        ),
        "mean_one_way_turnover_all_pairs": _mean(every),
        "annualised_friction_pct_all_pairs": (
            round(
                statistics.fmean(every) * SESSIONS_PER_YEAR * ROUND_TRIP_COST * 100, 4
            )
        ),
        "median_replaced_percentile": (
            round(statistics.median(sold_percentiles), 4) if sold_percentiles else None
        ),
        "detail": per_session,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--horizon", type=int, default=20)
    parser.add_argument("--widths", default="5,10,20,50,100,800")
    parser.add_argument("--buffers", default="1.0,2.0")
    parser.add_argument("--uri", default=None)
    parser.add_argument("--detail", action="store_true")
    args = parser.parse_args(argv)

    widths = [int(v) for v in args.widths.split(",") if v.strip()]
    buffers = [float(v) for v in args.buffers.split(",") if v.strip()]

    db = _db(args)
    grouped = _load_scores(db, args.model_version, args.horizon)
    dates = sorted(grouped)
    if len(dates) < 3:
        raise SystemExit("not enough scored sessions to measure churn")

    trading_days = _load_trading_days(db, dates[0], dates[-1])
    trading_index = {day: index for index, day in enumerate(trading_days)}

    results = [
        _chain(
            grouped,
            dates,
            trading_index,
            args.model_version,
            args.horizon,
            width,
            buffer,
        )
        for width in widths
        for buffer in buffers
    ]
    payload = {
        "schema": "book-churn-recompute-v1",
        "model_version": args.model_version,
        "horizon": args.horizon,
        "scored_sessions": len(dates),
        "trading_sessions_in_range": len(trading_days),
        "session_range": [dates[0], dates[-1]],
        "round_trip_cost_pct": round(ROUND_TRIP_COST * 100, 6),
        "sessions_per_year_assumed": SESSIONS_PER_YEAR,
        "results": results,
        "limitations": [
            "Measures turnover and its friction only; it does NOT measure returns.",
            "The available production score history cannot settle whether the "
            "signal has an edge in either direction.",
            "Headline figures use only adjacent-trading-session pairs: a pair "
            "separated by a gap carries the accumulated change of the whole gap.",
            "Annualisation assumes one rebalance per trading session.",
            "ST, suspended and limit states are not filtered here; eligibility "
            "is applied by the live runner, so absolute turnover may differ.",
        ],
    }
    if not args.detail:
        for item in results:
            item.pop("detail", None)
    print("CHURN_RESULT " + json.dumps(payload, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
