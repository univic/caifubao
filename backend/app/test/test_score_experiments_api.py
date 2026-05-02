import datetime
from types import SimpleNamespace


class FakePredictionQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def order_by(self, *_fields):
        return self

    def __iter__(self):
        return iter(self.rows)


def _prediction(**overrides):
    defaults = {
        "stock_code": "sh600000",
        "stock_name": "浦发银行",
        "date": datetime.datetime(2026, 4, 13),
        "horizon": 5,
        "score": 76.0,
        "rank": 1,
        "model_version": "candidate",
        "status": "VERIFIED",
        "verification": {
            "return_at_target": 0.03,
            "max_return": 0.08,
            "min_return": -0.01,
            "max_drawdown": -0.02,
            "hit_target": True,
            "hit_stop_loss": False,
        },
        "explanation": {
            "components": [
                {"id": "trend_alignment", "contribution": 20},
                {"id": "momentum", "contribution": 15},
            ]
        },
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_build_report_compares_candidate_with_baseline(monkeypatch):
    from app.api.v1 import score_experiments

    rows = [
        _prediction(
            model_version="candidate",
            score=76.0,
            verification={
                "return_at_target": 0.03,
                "max_return": 0.08,
                "min_return": -0.01,
                "max_drawdown": -0.02,
                "hit_target": True,
                "hit_stop_loss": False,
            },
        ),
        _prediction(
            model_version="baseline",
            score=70.0,
            verification={
                "return_at_target": 0.01,
                "max_return": 0.04,
                "min_return": -0.03,
                "max_drawdown": -0.04,
                "hit_target": False,
                "hit_stop_loss": False,
            },
        ),
    ]

    def fake_objects(**kwargs):
        filtered = [
            row
            for row in rows
            if row.model_version == kwargs["model_version"]
            and row.horizon == kwargs["horizon"]
            and row.status == kwargs["status"]
            and kwargs["date__gte"] <= row.date <= kwargs["date__lte"]
        ]
        return FakePredictionQuery(filtered)

    monkeypatch.setattr(
        score_experiments,
        "StockScorePrediction",
        SimpleNamespace(objects=fake_objects),
    )

    experiment = SimpleNamespace(
        model_version="candidate",
        baseline_model_version="baseline",
        start_date=datetime.datetime(2026, 4, 1),
        end_date=datetime.datetime(2026, 4, 30),
        horizons=[5],
    )

    report = score_experiments._build_report(experiment)

    horizon_report = report["horizons"]["5"]
    assert horizon_report["overall"]["count"] == 1
    assert horizon_report["overall"]["hit_rate"] == 1.0
    assert horizon_report["baseline"]["overall"]["hit_rate"] == 0.0
    assert horizon_report["comparison"]["avg_return_at_target_delta"] == 0.02
    assert horizon_report["component_summary"]["trend_alignment"]["count"] == 1
