# -*- coding: utf-8 -*-
"""Seed a small, deterministic score-research dataset for local UI/API feedback."""

import datetime
import math
import os
import sys

sys.path.insert(0, os.getcwd())

from app.api.v1.score_experiments import _build_report
from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.model.scoring import ScoreExperiment, StockScorePrediction


BASE_DATE = datetime.datetime(2026, 4, 1)
MODEL_CANDIDATE = "score_demo_candidate"
MODEL_BASELINE = "score_demo_baseline"
STOCKS = [
    ("sh600000", "浦发银行"),
    ("sz000001", "平安银行"),
    ("sh600519", "贵州茅台"),
    ("sz300750", "宁德时代"),
    ("sh601318", "中国平安"),
    ("sz002594", "比亚迪"),
    ("sh600036", "招商银行"),
    ("sh688981", "中芯国际"),
]


def _date(offset):
    return BASE_DATE + datetime.timedelta(days=offset)


def _target_date(date, horizon):
    return date + datetime.timedelta(days=math.ceil(horizon * 1.5))


def _verification(score, horizon, variant_bonus):
    base_return = (score - 50) / 1000 + variant_bonus
    max_return = base_return + horizon / 1000
    min_return = base_return - 0.035
    threshold = {5: 0.02, 20: 0.05, 60: 0.08}[horizon]
    stop_loss = {5: -0.05, 20: -0.08, 60: -0.12}[horizon]
    return {
        "status": "VERIFIED",
        "expected_quote_count": horizon,
        "verified_quote_count": horizon,
        "return_at_target": round(base_return, 6),
        "max_return": round(max_return, 6),
        "min_return": round(min_return, 6),
        "max_drawdown": round(min_return, 6),
        "hit_target_close": base_return >= threshold,
        "hit_target_intra": max_return >= threshold,
        "hit_stop_loss": min_return <= stop_loss,
        "effective_threshold": threshold,
        "stop_loss_threshold": stop_loss,
    }


def _prediction_payload(stock_code, stock_name, date, horizon, idx, model_version):
    variant_bonus = 0.012 if model_version == MODEL_CANDIDATE else 0.0
    horizon_bias = {5: 0, 20: -4, 60: -8}[horizon]
    score = max(15, min(95, 86 - idx * 6 + horizon_bias + int(variant_bonus * 300)))
    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "date": date,
        "horizon": horizon,
        "score": float(score),
        "rank": idx + 1,
        "percentile": round(1 - idx / len(STOCKS), 4),
        "recommendation": "BUY" if score >= 70 else "WATCH" if score >= 50 else "NONE",
        "base_price": round(10 + idx * 3.2, 2),
        "target_date": _target_date(date, horizon),
        "status": "VERIFIED",
        "explanation": {
            "summary": "Demo score generated for local research feedback.",
            "horizon": horizon,
            "score": float(score),
            "components": [
                {
                    "id": "trend_alignment",
                    "group": "trend",
                    "label": "Moving-average trend alignment",
                    "weight": 30,
                    "contribution": round(score * 0.28, 4),
                    "evidence": {"demo": True},
                },
                {
                    "id": "momentum",
                    "group": "momentum",
                    "label": "Recent momentum",
                    "weight": 20,
                    "contribution": round(score * 0.18, 4),
                    "evidence": {"demo": True},
                },
            ],
            "penalties": [],
        },
        "verification": _verification(score, horizon, variant_bonus),
        "input_snapshot": {"status": "READY", "source": "demo_seed"},
        "model_version": model_version,
    }


def seed_predictions():
    StockScorePrediction.objects(
        model_version__in=[MODEL_CANDIDATE, MODEL_BASELINE]
    ).delete()
    dates = [_date(0), _date(1), _date(2)]
    for model_version in (MODEL_CANDIDATE, MODEL_BASELINE):
        for date in dates:
            for horizon in (5, 20, 60):
                for idx, (stock_code, stock_name) in enumerate(STOCKS):
                    StockScorePrediction(
                        **_prediction_payload(
                            stock_code,
                            stock_name,
                            date,
                            horizon,
                            idx,
                            model_version,
                        )
                    ).save()


def seed_experiment():
    ScoreExperiment.objects(name="Demo Score Experiment").delete()
    experiment = ScoreExperiment(
        name="Demo Score Experiment",
        description="Local seeded experiment for fast ScoreExperiment UI feedback.",
        model_version=MODEL_CANDIDATE,
        baseline_model_version=MODEL_BASELINE,
        start_date=_date(0),
        end_date=_date(2),
        horizons=[5, 20, 60],
        config={
            "5": {"signal_strength": 30, "momentum": 25, "trend_alignment": 20},
            "20": {"trend_alignment": 30, "relative_strength": 15},
            "60": {"trend_alignment": 35, "relative_strength": 25},
        },
        status="COMPLETED",
    )
    experiment.report = _build_report(experiment)
    experiment.completed_at = datetime.datetime.now(datetime.UTC)
    experiment.save()
    return experiment


def main():
    mongo_watcher.get_db_connection()
    seed_predictions()
    experiment = seed_experiment()
    print("Seeded score demo data")
    print(f"  experiment_id={experiment.id}")
    print(f"  candidate_model={MODEL_CANDIDATE}")
    print(f"  baseline_model={MODEL_BASELINE}")


if __name__ == "__main__":
    main()
