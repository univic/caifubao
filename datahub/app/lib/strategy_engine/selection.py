# -*- coding: utf-8 -*-
"""Selection + rebalance logic for the paper-first strategy runner.

Pure functions over plain records so they are unit-testable without Mongo:
callers pass VERIFIED predictions as lightweight objects with score /
percentile / stock_code and an eligibility map stock_code -> bool. The runner
layer maps Mongo StockScorePrediction documents and quote/stock eligibility
onto these shapes.
"""

from __future__ import annotations

from collections import OrderedDict

from app.lib.strategy_engine.config import (
    DEFAULT_HORIZON,
    validate_strategy_config,
)


def select_target_holdings(
    predictions,
    config: dict,
    *,
    eligible_codes: set[str] | None = None,
    max_size: int | None = None,
) -> list[dict]:
    """Select an equal-weight target holdings list for one date.

    Always "buys high": sorts by score descending (direction semantics live in
    the scoring construction layer, never here). Applies eligibility and the
    configured selection rule, then caps at portfolio_size.

    predictions: iterable of objects with stock_code, score, percentile.
    Returns list of {"stock_code", "weight"} equal-weight entries (weight
    sums to 1 minus cash_reserve_pct); empty list when nothing is eligible.
    """
    config = validate_strategy_config(config)
    selection = config["selection"]
    mode = selection["mode"]
    size = int(selection.get("portfolio_size"))
    if max_size is not None and max_size >= 1:
        size = min(size, max_size)

    candidates = []
    for item in predictions:
        code = item.stock_code
        if eligible_codes is not None and code not in eligible_codes:
            continue
        score = item.score if item.score is not None else -float("inf")
        percentile = (
            item.percentile if getattr(item, "percentile", None) is not None else 0.0
        )
        if mode == "top_percentile":
            lower = float(selection["lower"])
            upper = float(selection["upper"])
            if not (lower <= float(percentile) <= upper):
                continue
        candidates.append(
            {
                "stock_code": code,
                "score": score,
                "percentile": percentile,
            }
        )

    # Rank by score desc (buy high). Stable tie-break by stock_code so runs
    # are deterministic.
    candidates.sort(key=lambda c: (-c["score"], c["stock_code"]))
    holdings = candidates[:size]

    cash_reserve = float(config.get("cash_reserve_pct", 0.0))
    investable = 1.0 - cash_reserve
    per_stock = investable / len(holdings) if holdings else 0.0
    return [
        {"stock_code": h["stock_code"], "weight": round(per_stock, 8)} for h in holdings
    ]


def compute_rebalance(
    previous_holdings: list[dict] | None,
    target_holdings: list[dict],
) -> dict:
    """Diff previous vs target holdings into a rebalance list.

    previous_holdings/target_holdings are lists of {"stock_code", "weight"}.
    Returns {"added": [codes...], "removed": [codes...], "unchanged": [...]}.
    """
    previous = OrderedDict(
        (h["stock_code"], h.get("weight")) for h in (previous_holdings or [])
    )
    target = OrderedDict(
        (h["stock_code"], h.get("weight")) for h in (target_holdings or [])
    )
    prev_codes = set(previous)
    target_codes = set(target)
    return {
        "added": sorted(target_codes - prev_codes),
        "removed": sorted(prev_codes - target_codes),
        "unchanged": sorted(prev_codes & target_codes),
    }


def holdings_by_code(holdings: list[dict]) -> dict[str, float]:
    """Map holdings list to {stock_code: weight} for price-based valuation."""
    return OrderedDict((h["stock_code"], h["weight"]) for h in (holdings or []))


def default_horizon_from_config(config: dict) -> int:
    return int(config.get("horizon", DEFAULT_HORIZON))
