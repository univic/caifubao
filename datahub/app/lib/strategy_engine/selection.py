# -*- coding: utf-8 -*-
"""Selection + rebalance logic for the paper-first strategy runner.

Pure functions over plain records so they are unit-testable without Mongo:
callers pass VERIFIED predictions as lightweight objects with score /
percentile / stock_code and an eligibility map stock_code -> bool. The runner
layer maps Mongo StockScorePrediction documents and quote/stock eligibility
onto these shapes.
"""

from __future__ import annotations

from collections import Counter, OrderedDict
from math import floor

from app.lib.strategy_engine.config import (
    DEFAULT_HORIZON,
    validate_strategy_config,
)

# Names with no industry classification are bucketed together so missing
# classification data cannot hide concentration from the industry cap.
UNKNOWN_INDUSTRY = "UNKNOWN"

_WEIGHT_SCALE = 100_000_000  # 8 decimal places


def _emit_weight(weight: float) -> float:
    """Round a weight DOWN to 8 dp.

    Rounding down means emission can never inflate a weight past a cap or push
    the emitted total above the investable fraction; the shortfall stays in cash.
    """
    return floor(weight * _WEIGHT_SCALE) / _WEIGHT_SCALE


def _bounded_weight(
    investable: float,
    n: int,
    constraints: dict,
    holdings: list[dict],
    industry_by_code: dict[str, str] | None,
) -> float:
    """Weight per name after the single-position and industry caps.

    Equal weight is scaled *down* to honour either cap, leaving the remainder in
    cash: names are never dropped to force the average down and never scaled up.
    A configured industry cap without industry data fails closed — an
    unverifiable limit must not silently degrade.
    """
    weight = investable / n
    max_single = constraints.get("max_single_stock_pct")
    if max_single is not None:
        weight = min(weight, float(max_single))
    max_industry = constraints.get("max_industry_pct")
    if max_industry is not None:
        if industry_by_code is None:
            raise ValueError(
                "constraints.max_industry_pct is configured but no industry "
                "classification was supplied; refusing to run without it"
            )
        # `or UNKNOWN_INDUSTRY`: a present-but-falsy value (None/"") is just as
        # unresolvable as a missing key, and must not become its own bucket.
        counts = Counter(
            (industry_by_code.get(h["stock_code"]) or UNKNOWN_INDUSTRY)
            for h in holdings
        )
        largest = max(counts.values()) if counts else 0
        if largest:
            weight = min(weight, float(max_industry) / largest)
    return weight


def select_target_holdings(
    predictions,
    config: dict,
    *,
    eligible_codes: set[str] | None = None,
    max_size: int | None = None,
    industry_by_code: dict[str, str] | None = None,
) -> list[dict]:
    """Select a target holdings list for one date under the configured limits.

    Always "buys high": sorts by score descending (direction semantics live in
    the scoring construction layer, never here). Applies eligibility and the
    configured selection rule, caps at portfolio_size, then applies the
    single-position and industry caps by scaling the equal weight down (the
    shortfall stays in cash).

    predictions: iterable of objects with stock_code, score, percentile.
    industry_by_code: optional stock_code -> industry code map; required only
    when constraints.max_industry_pct is configured.
    Returns list of {"stock_code", "weight"} entries; empty list when nothing is
    eligible.
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
    per_stock = (
        _bounded_weight(
            investable,
            len(holdings),
            config["constraints"],
            holdings,
            industry_by_code,
        )
        if holdings
        else 0.0
    )
    return [
        {"stock_code": h["stock_code"], "weight": _emit_weight(per_stock)}
        for h in holdings
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
