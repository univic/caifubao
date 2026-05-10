# -*- coding: utf-8 -*-

import datetime

from app.lib.scoring_engine.components import quote_price
from app.lib.scoring_engine.config import DEFAULT_MODEL_VERSION, get_horizon_config
from app.lib.scoring_engine.scoring_service import normalize_date
from app.model.scoring import StockScorePrediction
from app.model.stock import StockDailyQuote


class ScoreVerificationService:
    """Update tracking and verified outcomes for score predictions."""

    def __init__(
        self,
        quote_model=StockDailyQuote,
        prediction_model=StockScorePrediction,
        model_version: str = DEFAULT_MODEL_VERSION,
    ):
        self.quote_model = quote_model
        self.prediction_model = prediction_model
        self.model_version = model_version

    def verify_predictions(
        self,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        horizon: int | None = None,
        today: datetime.datetime | None = None,
    ) -> dict:
        query = {
            "status__in": ["PENDING", "TRACKING"],
            "model_version": self.model_version,
        }
        today = normalize_date(today or datetime.datetime.now(datetime.UTC))
        # Only verify predictions whose target date has already passed
        # (skips freshly-generated predictions that aren't due yet)
        query["target_date__lte"] = today

        if start_date:
            query["date__gte"] = normalize_date(start_date)
        if end_date:
            query["date__lte"] = normalize_date(end_date)
        if horizon:
            query["horizon"] = horizon

        predictions = list(self.prediction_model.objects(**query))
        counts = {"TRACKING": 0, "VERIFIED": 0, "INSUFFICIENT_DATA": 0, "PENDING": 0}
        for prediction in predictions:
            status = self.verify_single_prediction(prediction, today=today)
            counts[status] = counts.get(status, 0) + 1
        return {"checked_count": len(predictions), "status_counts": counts}

    def verify_single_prediction(self, prediction, today=None) -> str:
        if prediction.status == "BLOCKED":
            return "BLOCKED"
        today = normalize_date(today or datetime.datetime.now(datetime.UTC))
        future_quotes = list(
            self.quote_model.objects(
                code=prediction.stock_code,
                date__gt=prediction.date,
                date__lte=prediction.target_date,
            ).order_by("date")
        )

        if not future_quotes:
            prediction.verification = {
                **(prediction.verification or {}),
                "status": "PENDING",
                "verified_quote_count": 0,
            }
            prediction.status = "PENDING"
            prediction.save()
            return "PENDING"

        metrics = self._build_metrics(prediction, future_quotes)
        expected = prediction.horizon
        target_reached = len(future_quotes) >= expected
        target_date_passed = prediction.target_date and prediction.target_date <= today
        if target_reached:
            status = "VERIFIED"
        elif target_date_passed:
            status = "INSUFFICIENT_DATA"
        else:
            status = "TRACKING"

        prediction.verification = {
            **(prediction.verification or {}),
            **metrics,
            "status": status,
            "target_date": prediction.target_date.isoformat()
            if prediction.target_date
            else None,
            "expected_quote_count": expected,
            "verified_at": datetime.datetime.now(datetime.UTC).isoformat()
            if status == "VERIFIED"
            else None,
        }
        prediction.status = status
        prediction.save()
        return status

    def _build_metrics(self, prediction, future_quotes: list) -> dict:
        base_price = prediction.base_price
        if not base_price:
            return {"verified_quote_count": len(future_quotes)}

        closes = [quote_price(quote) for quote in future_quotes]
        highs = [quote_price(quote, "high") for quote in future_quotes]
        lows = [quote_price(quote, "low") for quote in future_quotes]
        closes = [value for value in closes if value is not None]
        highs = [value for value in highs if value is not None]
        lows = [value for value in lows if value is not None]
        if not closes:
            return {"verified_quote_count": len(future_quotes)}

        max_price = max(highs or closes)
        min_price = min(lows or closes)
        actual_price = closes[-1]
        max_return = (max_price - base_price) / base_price
        min_return = (min_price - base_price) / base_price
        return_at_target = (actual_price - base_price) / base_price
        days_to_max_return = self._days_to_price(future_quotes, max_price)
        config = get_horizon_config(prediction.horizon)
        return {
            "verified_quote_count": len(future_quotes),
            "current_price": closes[-1],
            "actual_price": actual_price,
            "max_price": max_price,
            "min_price": min_price,
            "return_at_target": round(return_at_target, 6),
            "max_return": round(max_return, 6),
            "min_return": round(min_return, 6),
            "max_drawdown": round(min_return, 6),
            "days_to_max_return": days_to_max_return,
            "hit_target": max_return >= config["effective_threshold"],
            "hit_stop_loss": min_return <= config["stop_loss_threshold"],
            "effective_threshold": config["effective_threshold"],
            "stop_loss_threshold": config["stop_loss_threshold"],
        }

    def _days_to_price(self, quotes, target_price):
        for idx, quote in enumerate(quotes, start=1):
            high = quote_price(quote, "high")
            close = quote_price(quote)
            if high == target_price or close == target_price:
                return idx
        return None
