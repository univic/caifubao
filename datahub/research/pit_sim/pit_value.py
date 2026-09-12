# -*- coding: utf-8 -*-
"""Value each 50k paper book at the window end (2026-09-11).

`strategy_runner nav` marks the curve at *execution dates* only, so with a
5-session cadence and T+1 execution the last point is 2026-09-08. This replays
the engine's own trades to recover the exact closing cash and share counts, then
marks the held basket at the 2026-09-11 close (the same raw `close` convention
`simulate_paper_nav` uses). No prices after 2026-09-11 are read, and no decision
is taken here — this is valuation only.
"""

import datetime
import json
import os

VARIANTS = [
    ("A_production_raw", "pit_baseline_h20_v1", "b9ce2434"),
    ("B_flip_ranked", "pit_flip_ranked_h20_v1", "091539f2"),
    ("C_ranked_control", "pit_baseline_ranked_h20_v1", "349b6c99"),
]
WINDOW_FROM = datetime.datetime(2026, 8, 1)
VALUATION_DATE = datetime.datetime(2026, 9, 11)
VALUATION_KEY = "2026-09-11"


def _assert_research_db() -> None:
    """Refuse to run against anything but the research database (read-only tool)."""
    name = os.getenv("MONGODB_NAME", "")
    if "research" not in name.lower():
        raise SystemExit(
            f"refusing to run: MONGODB_NAME={name!r} is not a research database"
        )


def main() -> int:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    _assert_research_db()
    mongo_watcher.get_db_connection()
    from app.lib.strategy_engine.nav import _tradable, _valid_price
    from app.jobs.strategy_runner import (
        _benchmark_returns_for_dates,
        _load_quotes_for_codes,
    )
    from app.lib.strategy_engine.nav import simulate_paper_nav
    from app.lib.strategy_engine.runner import schedule_from_runs
    from app.model.scoring import StockScorePrediction  # noqa: F401 (conn warmup)
    from app.model.strategy import StrategyPaperRun

    out = {}
    for label, model_version, prefix in VARIANTS:
        runs = [
            r
            for r in StrategyPaperRun.objects(
                model_version=model_version, horizon=20, status="COMPLETED"
            ).order_by("date")
            if (r.config_hash or "").startswith(prefix)
        ]
        schedule = schedule_from_runs(runs)
        codes = sorted({c for d in schedule for c in d["holdings"]})
        prices = _load_quotes_for_codes(codes, WINDOW_FROM, VALUATION_DATE)
        benchmark = _benchmark_returns_for_dates(WINDOW_FROM, VALUATION_DATE)
        result = simulate_paper_nav(
            prices=prices,
            schedule=schedule,
            benchmark_returns=benchmark,
            initial_nav=50000.0,
        )

        # Replay the engine's trades to recover exact closing cash + share counts.
        cash = 50000.0
        qty: dict[str, int] = {}
        for t in result["trades"]:
            signed = t["price"] * t["quantity"]
            costs = t.get("costs") or {}
            if t["side"] == "SELL":
                cash += (
                    signed - costs.get("commission", 0.0) - costs.get("stamp_duty", 0.0)
                )
                qty[t["stock_code"]] = qty.get(t["stock_code"], 0) - t["quantity"]
            else:
                cash -= (
                    signed + costs.get("commission", 0.0) + costs.get("stamp_duty", 0.0)
                )
                qty[t["stock_code"]] = qty.get(t["stock_code"], 0) + t["quantity"]
        held = {c: q for c, q in qty.items() if q > 0}

        marks = {}
        for code, shares in held.items():
            series = prices.get(code) or {}
            # Match nav.py `_mark`: only a tradable row refreshes the mark, so a
            # name suspended at the window end carries its last tradable close.
            quote = series.get(VALUATION_KEY)
            if not _tradable(quote) or not _valid_price(getattr(quote, "close", None)):
                earlier = sorted(k for k in series if k <= VALUATION_KEY)
                quote = None
                for key in reversed(earlier):
                    if _tradable(series[key]) and _valid_price(series[key].close):
                        quote = series[key]
                        break
            marks[code] = float(quote.close) if quote is not None else None
        missing = [c for c, m in marks.items() if m is None]
        final_nav = cash + sum(marks[c] * s for c, s in held.items() if marks[c])

        curve = result["curve"]
        out[label] = {
            "model_version": model_version,
            "config_hash": prefix,
            "runs": len(runs),
            "curve": [
                {
                    "date": p["date"],
                    "nav": p["nav"],
                    "daily_return": p.get("daily_return"),
                    "drawdown": p.get("drawdown"),
                    "positions_count": p.get("positions_count"),
                    "benchmark_return": p.get("benchmark_return"),
                }
                for p in curve
            ],
            "terminal_nav_last_execution": result["terminal_nav"],
            "last_execution_date": curve[-1]["date"],
            "valuation_date": VALUATION_DATE.date().isoformat(),
            "valuation_nav": round(final_nav, 2),
            "valuation_return_pct": round((final_nav / 50000.0 - 1) * 100, 3),
            "positions_at_valuation": len(held),
            "cash_at_last_execution": round(cash, 2),
            "missing_marks": missing,
        }

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
