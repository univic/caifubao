# -*- coding: utf-8 -*-

import datetime

from app.lib.scoring_engine.components import quote_price
from app.lib.scoring_engine.config import DEFAULT_MODEL_VERSION, get_horizon_config
from app.lib.scoring_engine.scoring_service import normalize_date
from app.model.scoring import StockScorePrediction
from app.model.stock import StockDailyQuote


def _bisect_right_date(quotes: list, boundary: datetime.datetime) -> int:
    """Return the index of the first quote whose date is > boundary.

    quotes must be ascending by date. Used to slice a prediction's future
    window (date, target_date] from a per-code ordered quote list without a
    linear scan per prediction (perf task 2.7).
    """
    lo, hi = 0, len(quotes)
    while lo < hi:
        mid = (lo + hi) // 2
        if normalize_date(quotes[mid].date) <= normalize_date(boundary):
            lo = mid + 1
        else:
            hi = mid
    return lo


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
        return self._verify_many(predictions, today=today)

    def verify_predictions_batch(
        self,
        predictions,
        today: datetime.datetime | None = None,
    ) -> dict:
        """Verify a caller-supplied prediction list (batch path).

        Perf task 2.7: verifies in bulk — future quotes are fetched per stock
        code in one query each (not per prediction), and status/verification
        updates are bulk_written. Returns the same shape as
        verify_predictions.
        """
        return self._verify_many(list(predictions), today=today)

    def _verify_many(self, predictions, today=None) -> dict:
        today = normalize_date(today or datetime.datetime.now(datetime.UTC))
        today = today.replace(tzinfo=None)
        counts = {"TRACKING": 0, "VERIFIED": 0, "INSUFFICIENT_DATA": 0, "PENDING": 0}
        if not predictions:
            return {"checked_count": 0, "status_counts": counts}

        # Group by stock code, then load each code's whole future window in
        # ONE query (instead of one query per prediction) and slice per
        # prediction. Only the price/date fields are projected.
        by_code: dict[str, list] = {}
        for prediction in predictions:
            if prediction.status == "BLOCKED":
                continue
            if not prediction.date or not prediction.target_date:
                # No window to verify (target_date__lte excludes these in the
                # normal query path; caller-supplied lists may still carry
                # them). Leave untouched, matching the old per-prediction
                # path which simply matched nothing.
                continue
            by_code.setdefault(prediction.stock_code, []).append(prediction)

        quotes_by_code: dict[str, list] = {}
        for code in by_code:
            preds = by_code[code]
            min_date = min(p.date for p in preds)
            max_target = max(p.target_date for p in preds)
            quotes_by_code[code] = list(
                self.quote_model.objects(
                    code=code,
                    date__gt=normalize_date(min_date),
                    date__lte=normalize_date(max_target),
                )
                # Project the HFQ fields too: quote_price() prefers
                # {field}_hfq and falls back to raw only when absent. The old
                # per-prediction path and verify_single_prediction load full
                # docs and therefore use HFQ-adjusted prices; dropping the
                # hfq fields here would silently compute metrics on raw
                # prices whenever fq_factor != 1.
                .only(
                    "date",
                    "open",
                    "close",
                    "high",
                    "low",
                    "close_hfq",
                    "high_hfq",
                    "low_hfq",
                )
                .order_by("date")
            )

        # Slice each code's ordered quote list per prediction and compute
        # status; collect updates for one bulk_write.
        from pymongo import UpdateOne

        operations = []
        results: dict[str, int] = {}
        for code, preds in by_code.items():
            quotes = quotes_by_code.get(code) or []
            for prediction in preds:
                # quotes are date-ordered; slice this prediction's window
                # (date, target_date] via bisect so an earlier prediction
                # never counts quotes past its own target date (spec: each
                # prediction verifies only its own horizon window).
                pd = normalize_date(prediction.date)
                target = normalize_date(prediction.target_date)
                start_idx = _bisect_right_date(quotes, pd)
                end_idx = _bisect_right_date(quotes, target)
                future = quotes[start_idx:end_idx]
                status = self._status_for(prediction, future, today)
                metrics = (
                    self._build_metrics(prediction, future)
                    if future
                    else {"verified_quote_count": 0}
                )
                verification = {
                    **(prediction.verification or {}),
                    **metrics,
                    "status": status,
                    "target_date": prediction.target_date.isoformat()
                    if prediction.target_date
                    else None,
                    "expected_quote_count": prediction.horizon,
                    "verified_at": datetime.datetime.now(datetime.UTC).isoformat()
                    if status == "VERIFIED"
                    else None,
                }
                operations.append(
                    UpdateOne(
                        {"_id": prediction.id},
                        {"$set": {"status": status, "verification": verification}},
                    )
                )
                results[status] = results.get(status, 0) + 1
        if operations:
            self.prediction_model._get_collection().bulk_write(
                operations, ordered=False
            )
        for key in counts:
            counts[key] = results.get(key, 0)
        return {"checked_count": len(predictions), "status_counts": counts}

    @staticmethod
    def _status_for(prediction, future_quotes: list, today) -> str:
        if not future_quotes:
            return "PENDING"
        expected = prediction.horizon
        target_reached = len(future_quotes) >= expected
        target_date_passed = prediction.target_date and normalize_date(
            prediction.target_date
        ).replace(tzinfo=None) <= today.replace(tzinfo=None)
        if target_reached:
            return "VERIFIED"
        if target_date_passed:
            return "INSUFFICIENT_DATA"
        return "TRACKING"

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
        target_date_passed = (
            prediction.target_date
            and prediction.target_date <= today.replace(tzinfo=None)
        )
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
            "hit_target_close": return_at_target >= config["effective_threshold"],
            "hit_target_intra": max_return >= config["effective_threshold"],
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
