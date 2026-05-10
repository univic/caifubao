# -*- coding: utf-8 -*-

import math
from statistics import pstdev


from app.model.industry import IndustryDailyMetrics


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def quote_price(quote, field: str = "close") -> float | None:
    value = getattr(quote, f"{field}_hfq", None)
    if value is None:
        value = getattr(quote, field, None)
    return float(value) if value is not None else None


def build_component(
    component_id: str,
    group: str,
    label: str,
    raw_value,
    normalized_value: float,
    weight: float,
    evidence: dict | None = None,
) -> dict:
    normalized = clamp(float(normalized_value))
    contribution = round(normalized * float(weight), 4)
    return {
        "id": component_id,
        "group": group,
        "label": label,
        "raw_value": raw_value,
        "normalized_value": round(normalized, 4),
        "weight": float(weight),
        "contribution": contribution,
        "direction": "positive",
        "evidence": evidence or {},
    }


def build_penalty(
    penalty_id: str,
    label: str,
    raw_value,
    normalized_value: float,
    weight: float,
    evidence: dict | None = None,
) -> dict:
    normalized = clamp(float(normalized_value))
    contribution = round(-normalized * float(weight), 4)
    return {
        "id": penalty_id,
        "group": "risk",
        "label": label,
        "raw_value": raw_value,
        "normalized_value": round(normalized, 4),
        "weight": float(weight),
        "contribution": contribution,
        "direction": "negative",
        "evidence": evidence or {},
    }


def signal_strength_component(signals: list, weight: float) -> dict:
    bullish_signals = [
        signal
        for signal in signals
        if getattr(signal, "direction", None) == "BULLISH"
        and getattr(signal, "signal_name", None)
    ]
    if not bullish_signals:
        normalized = 0.0
    else:
        strengths = [
            float(getattr(signal, "strength", 1.0) or 1.0) for signal in bullish_signals
        ]
        normalized = clamp(sum(strengths) / max(len(strengths), 1))

    return build_component(
        "signal_strength",
        "signal",
        "Bullish signal strength",
        [getattr(signal, "signal_name", None) for signal in bullish_signals],
        normalized,
        weight,
        {
            "signals": [
                {
                    "name": getattr(signal, "signal_name", None),
                    "strength": getattr(signal, "strength", None),
                    "reason": getattr(signal, "reason", None),
                }
                for signal in bullish_signals
            ]
        },
    )


def trend_alignment_component(quote, factors, horizon: int, weight: float) -> dict:
    close = quote_price(quote)
    if close is None or factors is None:
        return build_component(
            "trend_alignment",
            "trend",
            "Moving-average trend alignment",
            None,
            0.0,
            weight,
        )

    checks = []
    ma_20 = getattr(factors, "ma_20", None)
    ma_60 = getattr(factors, "ma_60", None)
    ma_120 = getattr(factors, "ma_120", None)

    if ma_20:
        checks.append(close > ma_20)
    if ma_60:
        checks.append(close > ma_60)
        if ma_20:
            checks.append(ma_20 > ma_60)
    if horizon >= 60 and ma_120:
        checks.append(close > ma_120)
        if ma_60:
            checks.append(ma_60 > ma_120)

    normalized = sum(1 for item in checks if item) / len(checks) if checks else 0.0
    return build_component(
        "trend_alignment",
        "trend",
        "Moving-average trend alignment",
        normalized,
        normalized,
        weight,
        {
            "close": close,
            "ma_20": ma_20,
            "ma_60": ma_60,
            "ma_120": ma_120,
        },
    )


def momentum_component(
    quote, previous_quotes: list, lookback: int, weight: float
) -> dict:
    close = quote_price(quote)
    if close is None or len(previous_quotes) < 1:
        return build_component(
            "momentum", "momentum", "Recent momentum", None, 0.0, weight
        )

    oldest = previous_quotes[-1]
    old_close = quote_price(oldest)
    if not old_close:
        return build_component(
            "momentum", "momentum", "Recent momentum", None, 0.0, weight
        )

    change = (close - old_close) / old_close
    normalized = clamp(change / 0.1)
    return build_component(
        "momentum",
        "momentum",
        "Recent momentum",
        round(change, 6),
        normalized,
        weight,
        {"lookback": lookback, "close": close, "old_close": old_close},
    )


def breakout_or_position_component(quote, history_quotes: list, weight: float) -> dict:
    close = quote_price(quote)
    if close is None or not history_quotes:
        return build_component(
            "breakout_or_position",
            "position",
            "Recent range position",
            None,
            0.0,
            weight,
        )

    highs = [quote_price(item, "high") for item in history_quotes]
    lows = [quote_price(item, "low") for item in history_quotes]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    if not highs or not lows:
        return build_component(
            "breakout_or_position",
            "position",
            "Recent range position",
            None,
            0.0,
            weight,
        )

    high = max(highs)
    low = min(lows)
    if close >= high:
        normalized = 1.0
    elif high > low:
        normalized = clamp((close - low) / (high - low))
    else:
        normalized = 0.0

    return build_component(
        "breakout_or_position",
        "position",
        "Recent range position",
        {"close": close, "range_high": high, "range_low": low},
        normalized,
        weight,
        {"history_count": len(history_quotes)},
    )


def relative_strength_component(quote, previous_quotes: list, weight: float) -> dict:
    close = quote_price(quote)
    if close is None or not previous_quotes:
        return build_component(
            "relative_strength",
            "relative_strength",
            "Relative strength placeholder",
            None,
            0.0,
            weight,
        )

    old_close = quote_price(previous_quotes[-1])
    if not old_close:
        normalized = 0.0
        change = None
    else:
        change = (close - old_close) / old_close
        normalized = clamp((change + 0.05) / 0.15)

    return build_component(
        "relative_strength",
        "relative_strength",
        "Relative strength proxy",
        round(change, 6) if change is not None else None,
        normalized,
        weight,
        {"note": "Universe/index relative strength can replace this proxy later."},
    )


def risk_penalty(quote, history_quotes: list, weight: float) -> dict:
    closes = [quote_price(item) for item in reversed(history_quotes)]
    current_close = quote_price(quote)
    if current_close is not None:
        closes.append(current_close)
    closes = [value for value in closes if value is not None and value > 0]

    returns = []
    for idx in range(1, len(closes)):
        returns.append((closes[idx] - closes[idx - 1]) / closes[idx - 1])

    volatility = pstdev(returns) if len(returns) > 1 else 0.0
    trade_status = getattr(quote, "trade_status", 1)
    is_st = getattr(quote, "isST", 0)
    abnormal = trade_status == 0 or is_st == 1
    raw_penalty = volatility / 0.06
    if abnormal:
        raw_penalty += 1.0
    if math.isnan(raw_penalty):
        raw_penalty = 0.0

    return build_penalty(
        "risk_penalty",
        "Volatility and tradeability risk",
        round(volatility, 6),
        raw_penalty,
        weight,
        {"trade_status": trade_status, "is_st": is_st, "return_count": len(returns)},
    )


def industry_momentum_component(
    stock_code: str,
    date,
    horizon: int,
    weight: float,
) -> dict:
    """Industry momentum component.

    Looks up the latest IndustryDailyMetrics for the stock's Shenwan L1 industry
    and normalizes the industry's average score into a 0-1 value. Returns a
    neutral 0.5 when no industry data is available (no penalty, no boost).
    """
    try:
        from app.model.industry import StockIndustryClassification

        industry = StockIndustryClassification.objects(stock_code=stock_code).first()
        if not industry or not industry.industry_code_sw_l1:
            return build_component(
                "industry_momentum",
                "industry",
                "Industry average score momentum",
                None,
                0.5,
                weight,
                evidence={"note": "No industry classification available"},
            )

        metrics = (
            IndustryDailyMetrics.objects(
                industry_code=industry.industry_code_sw_l1,
            )
            .order_by("-date")
            .first()
        )

        if not metrics or metrics.stock_count < 3:
            return build_component(
                "industry_momentum",
                "industry",
                "Industry average score momentum",
                None,
                0.5,
                weight,
                evidence={
                    "industry": industry.industry_name_sw_l1,
                    "note": "Insufficient industry data (need >=3 scored stocks)",
                },
            )

        normalized = clamp(metrics.avg_score / 100.0)
        return build_component(
            "industry_momentum",
            "industry",
            "Industry average score momentum ({})".format(industry.industry_name_sw_l1),
            {
                "industry": industry.industry_name_sw_l1,
                "avg_score": metrics.avg_score,
                "stock_count": metrics.stock_count,
            },
            normalized,
            weight,
            evidence={
                "industry_code": industry.industry_code_sw_l1,
                "industry_name": industry.industry_name_sw_l1,
                "avg_score": metrics.avg_score,
                "stock_count": metrics.stock_count,
                "buy_count": metrics.buy_count,
                "watch_count": metrics.watch_count,
            },
        )
    except Exception as exc:
        return build_component(
            "industry_momentum",
            "industry",
            "Industry momentum (error)",
            str(exc),
            0.5,
            weight,
            evidence={"error": str(exc)},
        )


def aggregate_industry_metrics(
    date,
    predictions: list,
    model_version: str,
) -> list[dict]:
    """Aggregate score predictions into IndustryDailyMetrics per L1 industry.

    Called after scoring is complete for a given date. Persists the aggregated
    metrics so industry_momentum_component can reference them on subsequent runs.
    """
    from collections import defaultdict
    from app.model.industry import StockIndustryClassification

    # Map stock_code → industry_code
    industries = {
        doc.stock_code: doc
        for doc in StockIndustryClassification.objects(
            stock_code__in=[p.stock_code for p in predictions]
        )
    }

    groups = defaultdict(list)
    for pred in predictions:
        ind = industries.get(pred.stock_code)
        if not ind or not ind.industry_code_sw_l1:
            continue
        groups[ind.industry_code_sw_l1].append(
            {
                "score": getattr(pred, "score", 0) or 0,
                "percentile": getattr(pred, "percentile", 0) or 0,
                "rank": getattr(pred, "rank", 0) or 0,
                "recommendation": getattr(pred, "recommendation", "NONE"),
            }
        )

    results = []
    for code, preds in groups.items():
        scores = [p["score"] for p in preds]
        if not scores:
            continue

        recos = [p["recommendation"] for p in preds]

        doc = IndustryDailyMetrics(
            industry_code=code,
            industry_name=industries[next(iter(preds))].industry_name_sw_l1
            if preds
            else "",
            date=date,
            stock_count=len(scores),
            avg_score=round(sum(scores) / len(scores), 2),
            max_score=round(max(scores), 2),
            min_score=round(min(scores), 2),
            avg_percentile=round(sum(p["percentile"] for p in preds) / len(preds), 4),
            avg_rank=round(sum(p["rank"] for p in preds) / len(preds), 2),
            buy_count=sum(1 for r in recos if r == "BUY"),
            watch_count=sum(1 for r in recos if r == "WATCH"),
            avoid_count=sum(1 for r in recos if r == "AVOID"),
        )
        doc.save()
        results.append(doc)

    return results
