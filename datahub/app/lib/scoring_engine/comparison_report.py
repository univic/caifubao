# -*- coding: utf-8 -*-
"""Side-by-side experiment comparison report for model-version or scoring-config pairs."""

from collections import defaultdict
from typing import Any

from app.lib.scoring_engine.calibration_report import SCORE_BUCKETS
from app.lib.scoring_engine.scoring_service import normalize_date
from app.model.scoring import StockScorePrediction


def _avg(values):
    filtered = [v for v in values if v is not None]
    if not filtered:
        return None
    return round(sum(filtered) / len(filtered), 6)


def _avg_metric(predictions, metric: str):
    return _avg([(p.verification or {}).get(metric) for p in predictions])


def _rate(predictions, metric: str):
    values = [(p.verification or {}).get(metric) for p in predictions]
    values = [v for v in values if v is not None]
    if not values:
        return None
    return round(sum(1 for v in values if v) / len(values), 6)


def _metric_summary(predictions):
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
        "avg_score": _avg([p.score for p in predictions]),
        "avg_return_at_target": _avg_metric(predictions, "return_at_target"),
        "avg_max_return": _avg_metric(predictions, "max_return"),
        "avg_min_return": _avg_metric(predictions, "min_return"),
        "avg_max_drawdown": _avg_metric(predictions, "max_drawdown"),
        "hit_rate": _rate(predictions, "hit_target_close"),
        "hit_rate_intra": _rate(predictions, "hit_target_intra"),
        "stop_loss_hit_rate": _rate(predictions, "hit_stop_loss"),
    }


def _top_n_summary(predictions):
    by_date = defaultdict(list)
    for p in predictions:
        by_date[p.date].append(p)

    result = {}
    for top_n in (10, 30, 50):
        selected = []
        for items in by_date.values():
            selected.extend(
                sorted(items, key=lambda x: x.score or 0, reverse=True)[:top_n]
            )
        result[f"top_{top_n}"] = _metric_summary(selected)
    return result


def _bucket_summary(predictions):
    result = []
    for low, high in SCORE_BUCKETS:
        bucket_items = [
            p
            for p in predictions
            if low <= (p.score or 0) < high or (high == 100 and (p.score or 0) == 100)
        ]
        result.append({"bucket": f"{low}-{high}", **_metric_summary(bucket_items)})
    return result


class ExperimentComparisonReport:
    """Build a side-by-side comparison of two model versions over a shared date range."""

    def __init__(self, prediction_model=StockScorePrediction):
        self.prediction_model = prediction_model

    def compare(
        self,
        candidate_model_version: str,
        baseline_model_version: str,
        start_date,
        end_date,
        horizon: int,
    ) -> dict[str, Any]:
        """Return a comparison dict with candidate/baseline summaries, deltas, and verdict."""

        candidate_preds = list(
            self.prediction_model.objects(
                model_version=candidate_model_version,
                date__gte=normalize_date(start_date),
                date__lte=normalize_date(end_date),
                horizon=horizon,
                status="VERIFIED",
            ).order_by("date", "-score")
        )
        baseline_preds = list(
            self.prediction_model.objects(
                model_version=baseline_model_version,
                date__gte=normalize_date(start_date),
                date__lte=normalize_date(end_date),
                horizon=horizon,
                status="VERIFIED",
            ).order_by("date", "-score")
        )

        candidate_summary = {
            "model_version": candidate_model_version,
            "overall": _metric_summary(candidate_preds),
            "score_buckets": _bucket_summary(candidate_preds),
            "top_n": _top_n_summary(candidate_preds),
        }
        baseline_summary = {
            "model_version": baseline_model_version,
            "overall": _metric_summary(baseline_preds),
            "score_buckets": _bucket_summary(baseline_preds),
            "top_n": _top_n_summary(baseline_preds),
        }

        deltas = self._compute_deltas(candidate_summary, baseline_summary)
        verdict = self._verdict(deltas)

        return {
            "horizon": horizon,
            "start_date": normalize_date(start_date).isoformat(),
            "end_date": normalize_date(end_date).isoformat(),
            "candidate": candidate_summary,
            "baseline": baseline_summary,
            "deltas": deltas,
            "verdict": verdict,
        }

    @staticmethod
    def _compute_deltas(candidate: dict, baseline: dict) -> dict:
        c = candidate["overall"]
        b = baseline["overall"]

        def delta(key: str):
            cv = c.get(key)
            bv = b.get(key)
            if cv is not None and bv is not None:
                return round(cv - bv, 6)
            return None

        result = {
            "count": delta("count"),
            "avg_score": delta("avg_score"),
            "avg_return_at_target": delta("avg_return_at_target"),
            "avg_max_return": delta("avg_max_return"),
            "avg_min_return": delta("avg_min_return"),
            "avg_max_drawdown": delta("avg_max_drawdown"),
            "hit_rate": delta("hit_rate"),
            "hit_rate_intra": delta("hit_rate_intra"),
            "stop_loss_hit_rate": delta("stop_loss_hit_rate"),
        }

        # top-N deltas
        c_top = candidate.get("top_n") or {}
        b_top = baseline.get("top_n") or {}
        top_deltas = {}
        for key in c_top:
            top_deltas[key] = {}
            for metric in ("hit_rate", "avg_return_at_target", "avg_max_return"):
                cv = (c_top[key] or {}).get(metric)
                bv = (b_top[key] or {}).get(metric)
                top_deltas[key][f"{metric}_delta"] = (
                    round(cv - bv, 6) if cv is not None and bv is not None else None
                )
        result["top_n"] = top_deltas
        return result

    @staticmethod
    def _verdict(deltas: dict) -> str:
        hit_delta = deltas.get("hit_rate") or 0
        ret_delta = deltas.get("avg_return_at_target") or 0

        if hit_delta > 0.02 and ret_delta > 0.002:
            return "Candidate clearly wins on both hit rate and return."
        if hit_delta > 0.02 and ret_delta >= -0.002:
            return "Candidate wins on hit rate, return is comparable."
        if hit_delta >= -0.02 and ret_delta > 0.005:
            return "Candidate wins on return, hit rate is comparable."
        if hit_delta < -0.05 or ret_delta < -0.01:
            return "Baseline wins."
        if abs(hit_delta) <= 0.01 and abs(ret_delta) <= 0.003:
            return "The two versions are not significantly different."
        if hit_delta > 0:
            return "Candidate shows modest improvement in hit rate."
        if ret_delta > 0:
            return "Candidate shows modest improvement in return."
        return "Candidate shows mixed results — review in detail."
