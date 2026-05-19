# -*- coding: utf-8 -*-

from collections import Counter, defaultdict

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

MISCALIBRATION_BUY_RATE_MIN = 0.03   # flag when BUY percentage < 3%
MISCALIBRATION_AVOID_RATE_MAX = 0.50  # flag when AVOID percentage > 50%


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
            "distribution": self._distribution_stats(predictions),
            "score_buckets": self._bucket_summary(predictions),
            "top_n": self._top_n_summary(predictions),
            "component_summary": self._component_summary(predictions),
            "false_positives": self._false_positives(predictions),
            "false_negatives": self._false_negatives(predictions),
        }

    def _distribution_stats(self, predictions):
        """Compute score distribution, recommendation counts, and miscalibration
        flags.
        """
        scores = sorted(
            [item.score for item in predictions if (item.score or 0) >= 0]
        )
        if not scores:
            return {
                "count": 0,
                "min": None,
                "max": None,
                "mean": None,
                "std": None,
                "percentiles": {},
                "recommendations": {},
                "miscalibration_flags": ["no_data"],
            }

        n = len(scores)
        mean_val = round(sum(scores) / n, 2)
        std_val = round(
            (sum((x - mean_val) ** 2 for x in scores) / n) ** 0.5, 2
        )

        def pct(p: int) -> float:
            idx = max(0, min(n - 1, int(round(n * p / 100.0))))
            return round(scores[idx], 2)

        percentiles = {
            "p5": pct(5),
            "p10": pct(10),
            "p25": pct(25),
            "p50": pct(50),
            "p75": pct(75),
            "p90": pct(90),
            "p95": pct(95),
        }

        # Recommendation counts
        rec_counter = Counter()
        for item in predictions:
            rec_counter[item.recommendation or "NONE"] += 1
        total = sum(rec_counter.values())

        def _rec_entry(key: str) -> dict:
            cnt = rec_counter.get(key, 0)
            pct_val = round(cnt / total * 100, 2) if total else 0
            return {"count": cnt, "pct": pct_val}

        recommendations = {
            "BUY": _rec_entry("BUY"),
            "WATCH": _rec_entry("WATCH"),
            "NONE": _rec_entry("NONE"),
            "AVOID": _rec_entry("AVOID"),
        }

        # Miscalibration flags
        flags = []
        buy_pct = recommendations["BUY"]["pct"]
        avoid_pct = recommendations["AVOID"]["pct"]
        if buy_pct < MISCALIBRATION_BUY_RATE_MIN * 100:
            threshold_pct = MISCALIBRATION_BUY_RATE_MIN * 100
            flags.append(
                f"BUY_rate_too_low:{buy_pct:.1f}% < {threshold_pct:.1f}%"
            )
        if avoid_pct > MISCALIBRATION_AVOID_RATE_MAX * 100:
            threshold_pct = MISCALIBRATION_AVOID_RATE_MAX * 100
            flags.append(
                f"AVOID_rate_too_high:{avoid_pct:.1f}% > {threshold_pct:.1f}%"
            )
        if median_val := percentiles.get("p50", 0):
            if median_val <= 25:
                flags.append(f"median_score_low:{median_val}")

        return {
            "count": n,
            "min": round(scores[0], 2),
            "max": round(scores[-1], 2),
            "mean": mean_val,
            "std": std_val,
            "percentiles": percentiles,
            "recommendations": recommendations,
            "miscalibration_flags": flags,
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
        # Use max_return (aggressive/intra metric) intentionally:
        # a false negative is a stock we scored low that had ANY opportunity
        # (even if only intraday), so we use the aggressive threshold.
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
                "hit_rate_intra": None,
                "stop_loss_hit_rate": None,
            }

        return {
            "count": len(predictions),
            "avg_score": self._avg([item.score for item in predictions]),
            "avg_return_at_target": self._avg_metric(predictions, "return_at_target"),
            "avg_max_return": self._avg_metric(predictions, "max_return"),
            "avg_min_return": self._avg_metric(predictions, "min_return"),
            "avg_max_drawdown": self._avg_metric(predictions, "max_drawdown"),
            "hit_rate": self._rate(predictions, "hit_target_close"),
            "hit_rate_intra": self._rate(predictions, "hit_target_intra"),
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
