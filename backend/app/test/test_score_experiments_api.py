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
            "hit_target_close": True,
            "hit_target_intra": True,
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
                "hit_target_close": True,
                "hit_target_intra": True,
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
                "hit_target_close": False,
                "hit_target_intra": False,
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


def test_build_report_uses_percentile_for_positive_only_flipped_config(monkeypatch):
    from app.api.v1 import score_experiments

    rows = [
        _prediction(
            model_version="partial_flip",
            score=10.0,
            percentile=0.9,
        )
    ]

    def fake_objects(**kwargs):
        return FakePredictionQuery(
            [row for row in rows if row.model_version == kwargs["model_version"]]
        )

    monkeypatch.setattr(
        score_experiments,
        "StockScorePrediction",
        SimpleNamespace(objects=fake_objects),
    )
    experiment = SimpleNamespace(
        model_version="partial_flip",
        baseline_model_version="",
        start_date=datetime.datetime(2026, 4, 1),
        end_date=datetime.datetime(2026, 4, 30),
        horizons=[5],
        config={"5": {"directions": {"momentum": -1}}},
    )

    report = score_experiments._build_report(experiment)

    horizon_report = report["horizons"]["5"]
    assert horizon_report["bucket_basis"] == "percentile"
    assert horizon_report["score_buckets"][-1]["count"] == 1


def test_build_report_rejects_invalid_signed_percentile(monkeypatch):
    import pytest

    from app.api.v1 import score_experiments

    rows = [
        _prediction(
            model_version="partial_flip",
            score=-10.0,
            percentile=float("nan"),
        )
    ]
    monkeypatch.setattr(
        score_experiments,
        "StockScorePrediction",
        SimpleNamespace(objects=lambda **_kwargs: FakePredictionQuery(rows)),
    )
    experiment = SimpleNamespace(
        model_version="partial_flip",
        baseline_model_version="",
        start_date=datetime.datetime(2026, 4, 1),
        end_date=datetime.datetime(2026, 4, 30),
        horizons=[5],
        config={"5": {"directions": {"momentum": -1}}},
    )

    with pytest.raises(score_experiments.ScoreReportInputError, match="percentile"):
        score_experiments._build_report(experiment)


def test_build_comparison_report_aligns_bases_without_datahub_import(monkeypatch):
    from app.api.v1 import score_experiments

    rows = [
        _prediction(
            model_version="partial_flip",
            score=10.0,
            percentile=0.9,
        ),
        _prediction(
            model_version="baseline",
            score=80.0,
            percentile=0.9,
        ),
    ]

    def fake_objects(**kwargs):
        return FakePredictionQuery(
            [row for row in rows if row.model_version == kwargs["model_version"]]
        )

    monkeypatch.setattr(
        score_experiments,
        "StockScorePrediction",
        SimpleNamespace(objects=fake_objects),
    )

    report = score_experiments._build_comparison_report(
        "partial_flip",
        "baseline",
        datetime.datetime(2026, 4, 1),
        datetime.datetime(2026, 4, 30),
        5,
        candidate_config={"5": {"directions": {"momentum": -1}}},
        baseline_config={},
    )

    assert report["comparison_basis"] == "percentile"
    assert report["candidate"]["score_buckets"][-1]["count"] == 1
    assert report["baseline"]["score_buckets"][-1]["count"] == 1
    assert report["deltas"]["avg_score"] is None


def test_compare_endpoint_returns_stable_422_for_invalid_percentile(
    client, monkeypatch
):
    from app.api.v1 import score_experiments

    monkeypatch.setattr(
        score_experiments,
        "_resolve_comparison_target",
        lambda identifier: {"model_version": identifier, "config": {}},
    )

    def fail_comparison(*_args, **_kwargs):
        raise score_experiments.ScoreReportInputError("invalid percentile")

    monkeypatch.setattr(score_experiments, "_build_comparison_report", fail_comparison)

    response = client.get(
        "/api/score-experiments/compare",
        query_string={
            "id_a": "candidate",
            "id_b": "baseline",
            "start_date": "2026-04-01",
            "end_date": "2026-04-30",
            "horizon": 20,
        },
    )

    assert response.status_code == 422
    assert response.get_json() == {
        "success": False,
        "message": "invalid percentile",
        "data": None,
    }


def test_compare_endpoint_exposes_basis_metadata(client, monkeypatch):
    from app.api.v1 import score_experiments

    monkeypatch.setattr(
        score_experiments,
        "_resolve_comparison_target",
        lambda identifier: {"model_version": identifier, "config": {}},
    )
    monkeypatch.setattr(
        score_experiments,
        "_build_comparison_report",
        lambda *_args, **_kwargs: {
            "comparison_basis": "percentile",
            "comparison_status": "ok",
        },
    )

    response = client.get(
        "/api/score-experiments/compare",
        query_string={
            "id_a": "candidate",
            "id_b": "baseline",
            "start_date": "2026-04-01",
            "end_date": "2026-04-30",
            "horizon": 20,
        },
    )

    assert response.status_code == 200
    assert response.get_json()["data"]["comparison_basis"] == "percentile"


def test_run_endpoint_returns_422_and_failed_experiment(client, monkeypatch):
    from app.api.v1 import score_experiments

    experiment = SimpleNamespace(status="FAILED")
    monkeypatch.setattr(
        score_experiments,
        "_experiment_or_404",
        lambda _experiment_id: (experiment, None),
    )

    def fail_run(_experiment):
        raise score_experiments.ScoreReportInputError("invalid percentile")

    monkeypatch.setattr(score_experiments, "_run_experiment", fail_run)
    monkeypatch.setattr(
        score_experiments,
        "_serialize_experiment",
        lambda item: {"status": item.status},
    )

    response = client.post("/api/score-experiments/experiment-1/run")

    assert response.status_code == 422
    assert response.get_json() == {
        "success": False,
        "message": "invalid percentile",
        "data": {"status": "FAILED"},
    }
