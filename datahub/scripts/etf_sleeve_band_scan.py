# -*- coding: utf-8 -*-
"""Recompute the rebalance band x cadence trade-off for the stable sleeve (W3).

The band and the cadence are the two parameters that decide how expensive a
diversified sleeve is, and both are measurable. This script measures them on the
assets the repository actually has, so the choice is evidence rather than taste.

What it measures, for the declared 25/25/25/25 sleeve:

* **turnover** — one-way notional traded per year;
* **friction** — turnover times the ETF round-trip cost (ETFs are exempt from
  stamp duty, so this is cheaper than the stock model);
* **drift** — the mean and maximum deviation of actual weights from target,
  which is what a band is trading against.

What it does NOT measure: returns. Bands do not create return here; they decide
how much tracking error you pay to save friction. Any return claim needs a
separate study and a forward window.

Read-only: it never writes to the database.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics

#: The static allocation the sleeve is built to hold.
TARGET = {
    "sh510300": 0.25,  # CSI 300
    "sh518880": 0.25,  # gold
    "sh513100": 0.25,  # Nasdaq 100
    "sh511010": 0.25,  # treasury bond
}
#: ETF round trip: two sides of slippage, commission and transfer fee, no stamp duty.
ETF_ROUND_TRIP_COST = 2 * 0.001 + 2 * 0.00025 + 2 * 0.00001
SESSIONS_PER_YEAR = 244


def load_closes(db, codes):
    """Return {code: {iso_date: close_hfq}} from the research ETF quotes."""
    series = {}
    for code in codes:
        rows = db["etf_daily_quote"].find(
            {"code": code}, {"_id": 0, "date": 1, "close_hfq": 1, "trade_status": 1}
        )
        per_code = {}
        for row in rows:
            if row.get("close_hfq") in (None, 0):
                continue
            if row.get("trade_status") not in (None, 1):
                continue
            per_code[str(row["date"])[:10]] = float(row["close_hfq"])
        series[code] = per_code
    common = (
        sorted(set.intersection(*(set(v) for v in series.values()))) if series else []
    )
    return {code: {day: series[code][day] for day in common} for code in codes}, common


def simulate(days, prices, *, band, cadence):
    """Run the band-and-cadence rule and return turnover and drift statistics."""
    codes = list(TARGET)
    weights = dict(TARGET)
    one_way_total = 0.0
    rebalances = 0
    drifts = []
    for index, day in enumerate(days):
        close = {code: prices[code][day] for code in codes}
        if index > 0:
            previous_day = days[index - 1]
            prev_close = {code: prices[code][previous_day] for code in codes}
            gross = sum(
                weights[code] * close[code] / prev_close[code] for code in codes
            )
            weights = {
                code: weights[code] * close[code] / prev_close[code] / gross
                for code in codes
            }
        drifts.append(0.5 * sum(abs(weights[code] - TARGET[code]) for code in codes))
        scheduled = cadence and index > 0 and index % cadence == 0
        outside = any(abs(weights[code] - TARGET[code]) > band for code in codes)
        if index > 0 and (scheduled and outside):
            traded = 0.5 * sum(abs(TARGET[code] - weights[code]) for code in codes)
            one_way_total += traded
            rebalances += 1
            weights = dict(TARGET)
    years = len(days) / SESSIONS_PER_YEAR if days else 0
    annual_turnover = one_way_total / years if years else 0.0
    return {
        "band_pct": band,
        "cadence_sessions": cadence,
        "sessions": len(days),
        "rebalances": rebalances,
        "annual_one_way_turnover": round(annual_turnover, 4),
        "annual_friction_pct": round(annual_turnover * ETF_ROUND_TRIP_COST * 100, 4),
        "mean_drift_pct": round(statistics.fmean(drifts) * 100, 4) if drifts else None,
        "max_drift_pct": round(max(drifts) * 100, 4) if drifts else None,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=None)
    parser.add_argument(
        "--bands", default="0.02,0.03,0.05,0.10", help="absolute weight bands"
    )
    parser.add_argument(
        "--cadences", default="5,20,60", help="rebalance cadence in sessions"
    )
    args = parser.parse_args(argv)

    import pymongo

    uri = args.uri or "mongodb://%s:%s@%s:%s/%s?authSource=admin" % (
        os.environ.get("MONGODB_USER", ""),
        os.environ.get("MONGODB_PASS", ""),
        os.environ.get("MONGODB_HOST", "localhost"),
        os.environ.get("MONGODB_PORT", "27017"),
        os.environ.get("MONGODB_NAME", "caifubao"),
    )
    db = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000).get_default_database()

    prices, days = load_closes(db, list(TARGET))
    if len(days) < 250:
        raise SystemExit(f"only {len(days)} common sessions; need at least 250")

    bands = [float(value) for value in args.bands.split(",") if value.strip()]
    cadences = [int(value) for value in args.cadences.split(",") if value.strip()]
    results = [
        simulate(days, prices, band=band, cadence=cadence)
        for band in bands
        for cadence in cadences
    ]
    payload = {
        "schema": "etf-sleeve-band-scan-v1",
        "target_weights": TARGET,
        "sessions": len(days),
        "first_session": days[0],
        "last_session": days[-1],
        "etf_round_trip_cost_pct": round(ETF_ROUND_TRIP_COST * 100, 6),
        "results": results,
        "limitations": [
            "Turnover, friction and drift only; returns are not measured here.",
            "The rule rebalances to target only when a scheduled session is also "
            "outside the band; other sessions trade nothing.",
            "The sleeve is 25/25/25/25 because those four assets are the ones with "
            "history in this repository; the asset set is not part of this scan.",
        ],
    }
    print("BAND_SCAN " + json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
