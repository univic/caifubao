# -*- coding: utf-8 -*-

from collections import defaultdict

from app.lib.scoring_engine.config import DEFAULT_MODEL_VERSION
from app.lib.scoring_engine.scoring_service import normalize_date
from app.model.scoring import StockScorePrediction


SCORE_BUCKETS = (
    (0, 20),
    (20, 40),
    (40, 60),
    (60, 80),
    (80, 100),
)


class ScoreCalibrationReport:
    """Build lightweight calibration summaries from verified predictions."""

    def __init__(
        self,
        prediction_model=StockScorePrediction,
        model_version: str = DEFAULT_MODEL_VERSION,
    ):
        self.prediction_model = prediction_model
        self.model_version = model_version

    def generate(self, start_date, end_date, horizon: int) -> dict:
        predictions = list(
            self.prediction_model.objects(
                date__gte=normalize_date(start_date),
                date__lte=normalize_date(end_date),
                horizon=horizon,
                model_version=self.model_version,
                status="VERIFIED",
            ).order_by("date", "-score")
        )
        return {
            "horizon": horizon,
            "model_version": self.model_version,
            "from": normalize_date(start_date).isoformat(),
            "to": normalize_date(end_date).isoformat(),
            "prediction_count": len(predictions),
            "score_buckets": self._bucket_summary(predictions),
            "top_n": self._top_n_summary(predictions),
            "component_summary": self._component_summary(predictions),
            "false_positives": self._false_positives(predictions),
            "false_negatives": self._false_negatives(predictions),
        }

    def _bucket_summary(self, predictions):
        result = []
        for low, high in SCORE_BUCKETS:
            bucket_items = [
                item
                for item in predictions
                if low <= (item.score or 0) < high
                or (high == 100 and (item.score or 0) == 100)
            ]
            result.append(
                {
                    "bucket": f"{low}-{high}",
                    **self._metric_summary(bucket_items),
                }
            )
        return result

    def _top_n_summary(self, predictions):
        by_date = defaultdict(list)
        for prediction in predictions:
            by_date[prediction.date].append(prediction)

        result = {}
        for top_n in (10, 30, 50):
            selected = []
            for items in by_date.values():
                selected.extend(
                    sorted(items, key=lambda item: item.score, reverse=True)[:top_n]
                )
            result[f"top_{top_n}"] = self._metric_summary(selected)
        return result

    def _component_summary(self, predictions):
        grouped = defaultdict(list)
        for prediction in predictions:
            explanation = prediction.explanation or {}
            for component in explanation.get("components", []):
                grouped[component.get("id")].append(prediction)

        return {
            component_id: self._metric_summary(items)
            for component_id, items in sorted(grouped.items())
            if component_id
        }

    def _false_positives(self, predictions):
        items = [
            item
            for item in predictions
            if (item.score or 0) >= 70
            and (item.verification or {}).get("return_at_target", 0) < 0
        ]
        return self._sample(items)

    def _false_negatives(self, predictions):
        items = [
            item
            for item in predictions
            if (item.score or 0) < 40
            and (item.verification or {}).get("max_return", 0) >= 0.08
        ]
        return self._sample(items)

    def _metric_summary(self, predictions):
        if not predictions:
            return {
                "count": 0,
                "avg_score": None,
                "avg_return_at_target": None,
                "avg_max_return": None,
                "avg_min_return": None,
                "avg_max_drawdown": None,
                "hit_rate": None,
                "stop_loss_hit_rate": None,
            }

        return {
            "count": len(predictions),
            "avg_score": self._avg([item.score for item in predictions]),
            "avg_return_at_target": self._avg_metric(predictions, "return_at_target"),
            "avg_max_return": self._avg_metric(predictions, "max_return"),
            "avg_min_return": self._avg_metric(predictions, "min_return"),
            "avg_max_drawdown": self._avg_metric(predictions, "max_drawdown"),
            "hit_rate": self._rate(predictions, "hit_target"),
            "stop_loss_hit_rate": self._rate(predictions, "hit_stop_loss"),
        }

    def _sample(self, predictions, limit=10):
        return [
            {
                "stock_code": item.stock_code,
                "stock_name": item.stock_name,
                "date": item.date.isoformat() if item.date else None,
                "score": item.score,
                "rank": item.rank,
                "verification": item.verification,
            }
            for item in predictions[:limit]
        ]

    def _avg_metric(self, predictions, metric):
        return self._avg(
            [(item.verification or {}).get(metric) for item in predictions]
        )

    def _avg(self, values):
        filtered = [value for value in values if value is not None]
        if not filtered:
            return None
        return round(sum(filtered) / len(filtered), 6)

    def _rate(self, predictions, metric):
        values = [(item.verification or {}).get(metric) for item in predictions]
        values = [value for value in values if value is not None]
        if not values:
            return None
        return round(sum(1 for value in values if value) / len(values), 6)
