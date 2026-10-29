# -*- coding: utf-8 -*-
"""Measure whether a per-asset stop-loss actually helps the stable sleeve.

The exit contract requires a per-asset stop, but the threshold was a guess. This
script asks the only question that can settle it: when the stop fires, what
happens next?

For each threshold it runs the declared rule — 25/25/25/25, rebalance on a
scheduled session when an asset has drifted beyond the band, and close any asset
that falls below its recorded cost basis by more than the stop — then measures,
over the following sessions:

* **how often it fires** (events per asset per year);
* **whether it was a false trigger** — the price recovered above the reference
  within 20 or 60 sessions, meaning the stop sold a local bottom;
* **what it avoided** — the worst drawdown over the same horizon.

A threshold is only justified if it strands few positions and avoids real
drawdown. "It feels safer" is not a measurement.

Read-only: it never writes to the database.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics

TARGET = {
    "sh510300": 0.25,  # CSI 300
    "sh518880": 0.25,  # gold
    "sh513100": 0.25,  # Nasdaq 100
    "sh511010": 0.25,  # treasury bond
}
ETF_ROUND_TRIP_COST = 2 * 0.001 + 2 * 0.00025 + 2 * 0.00001
SESSIONS_PER_YEAR = 244
HORIZONS = (20, 60)


def load_closes(db, codes):
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
    return {code: [series[code][day] for day in common] for code in codes}, common


def simulate(days, prices, *, band, cadence, stop_pct):
    """Run the sleeve and collect one record per stop event."""
    codes = list(TARGET)
    weights = dict(TARGET)
    reference = {code: prices[code][0] for code in codes}  # cost basis
    stopped = {code: False for code in codes}
    events = []

    for index in range(len(days)):
        close = {code: prices[code][index] for code in codes}

        # Drift first: weights follow prices between rebalances.
        if index > 0:
            prev = {code: prices[code][index - 1] for code in codes}
            gross = sum(weights[code] * close[code] / prev[code] for code in codes)
            weights = {c: weights[c] * close[c] / prev[c] / gross for c in codes}

        # Stop check against the recorded cost basis, every session.
        for code in codes:
            if stopped[code] or stop_pct is None:
                continue
            if close[code] < reference[code] * (1.0 - stop_pct):
                stopped[code] = True
                weights[code] = 0.0
                outcome = {"day_index": index, "code": code, "price": close[code]}
                for horizon in HORIZONS:
                    if index + horizon < len(days):
                        future = prices[code][index : index + horizon + 1]
                        outcome[f"fwd_{horizon}"] = future[-1] / close[code] - 1.0
                        outcome[f"dd_{horizon}"] = min(future) / close[code] - 1.0
                        outcome[f"recovered_{horizon}"] = (
                            prices[code][index + horizon] > reference[code]
                        )
                events.append(outcome)

        # Scheduled rebalance restores the target and resets the cost basis.
        if cadence and index > 0 and index % cadence == 0:
            outside = any(abs(weights[code] - TARGET[code]) > band for code in codes)
            if outside:
                weights = dict(TARGET)
                reference = dict(close)
                stopped = {code: False for code in codes}
    return events


def summarise(events, sessions, codes, stop_pct):
    years = sessions / SESSIONS_PER_YEAR
    out = {
        "stop_pct": stop_pct,
        "events": len(events),
        "events_per_year": round(len(events) / years, 3) if years else None,
    }
    for horizon in HORIZONS:
        key_fwd = f"fwd_{horizon}"
        key_dd = f"dd_{horizon}"
        key_rec = f"recovered_{horizon}"
        usable = [e for e in events if key_fwd in e]
        if not usable:
            out[f"fwd_{horizon}_mean_pct"] = None
            out[f"false_trigger_{horizon}_pct"] = None
            out[f"avoided_dd_{horizon}_mean_pct"] = None
            continue
        out[f"fwd_{horizon}_mean_pct"] = round(
            statistics.fmean(e[key_fwd] for e in usable) * 100, 3
        )
        out[f"avoided_dd_{horizon}_mean_pct"] = round(
            statistics.fmean(e[key_dd] for e in usable) * 100, 3
        )
        out[f"false_trigger_{horizon}_pct"] = round(
            100.0 * sum(1 for e in usable if e[key_rec]) / len(usable), 1
        )
    out["by_asset"] = {
        code: sum(1 for e in events if e["code"] == code) for code in codes
    }
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=None)
    parser.add_argument("--band", type=float, default=0.02)
    parser.add_argument("--cadence", type=int, default=5)
    parser.add_argument("--stops", default="0.05,0.08,0.10,0.15,0.20")
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
    if len(days) < 500:
        raise SystemExit(f"only {len(days)} common sessions; need at least 500")

    stops = [float(value) for value in args.stops.split(",") if value.strip()]
    results = []
    for stop_pct in stops:
        events = simulate(
            days, prices, band=args.band, cadence=args.cadence, stop_pct=stop_pct
        )
        results.append(summarise(events, len(days), list(TARGET), stop_pct))

    payload = {
        "schema": "etf-sleeve-stop-scan-v1",
        "target_weights": TARGET,
        "band_pct": args.band,
        "cadence_sessions": args.cadence,
        "sessions": len(days),
        "first_session": days[0],
        "last_session": days[-1],
        "etf_round_trip_cost_pct": round(ETF_ROUND_TRIP_COST * 100, 6),
        "results": results,
        "limitations": [
            "The stop is measured against the recorded cost basis, which resets at "
            "each rebalance, exactly as the exit contract specifies.",
            "A stopped asset is restored at the next scheduled rebalance; the "
            "interval between stop and restore is not modelled as a separate trade.",
            "Returns after a stop are measured for the stopped asset, not for the "
            "rebuilt sleeve, so this is a property of the stop, not of a strategy.",
            "One window (2013-2026) and four assets; not split by regime.",
        ],
    }
    print("STOP_SCAN " + json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
