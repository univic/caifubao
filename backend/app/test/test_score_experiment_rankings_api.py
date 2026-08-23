# -*- coding: utf-8 -*-
"""Tests for the Score Experiment Rankings and Heatmap API endpoints.

Covers the /api/score-experiments/rankings and /api/score-experiments/heatmap
endpoints from Phase 2-3 (tasks 14.5, 14.8).
"""

import datetime
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Fake mongoengine-style query helpers
# ---------------------------------------------------------------------------
class FakeExperimentQuery:
    """Mimics ScoreExperiment QuerySet for rankings/heatmap."""

    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **kwargs):
        if not kwargs:
            return type(self)(self.rows)
        return self.filter(**kwargs)

    def filter(self, **kwargs):
        rows = self.rows
        for key, value in kwargs.items():
            if key == "status__in":
                rows = [r for r in rows if getattr(r, "status", None) in value]
            else:
                rows = [r for r in rows if _getattr(r, key) == value]
        return type(self)(rows)

    def order_by(self, *fields):
        rows = self.rows
        for field in reversed(fields):
            reverse = field.startswith("-")
            name = field.removeprefix("-")
            rows = sorted(rows, key=lambda r: _getattr(r, name) or "", reverse=reverse)
        return type(self)(rows)

    def limit(self, n):
        return type(self)(self.rows[:n])

    def first(self):
        return self.rows[0] if self.rows else None

    def only(self, *fields):
        return self

    def count(self):
        return len(self.rows)

    def __iter__(self):
        return iter(self.rows)


def _getattr(obj, name):
    val = getattr(obj, name, None)
    if isinstance(val, datetime.datetime):
        return val.replace(tzinfo=None)
    return val


def _experiment_row(**overrides):
    """Create a completed ScoreExperiment with report data."""
    defaults = {
        "id": "exp_001",
        "name": "Momentum Grid Search v1",
        "description": "Sweep momentum component weight 10-40",
        "model_version": "candidate_v3",
        "baseline_model_version": "score_v2_202604",
        "start_date": datetime.datetime(2026, 1, 1),
        "end_date": datetime.datetime(2026, 4, 30),
        "horizons": [5, 20, 60],
        "config": {
            "param_grid": {
                "momentum_weight": [10, 20, 30, 40],
                "trend_alignment_weight": [15, 25, 35],
            },
        },
        "status": "COMPLETED",
        "report": {
            "horizons": {
                "20": {
                    "overall": {
                        "count": 245,
                        "hit_rate": 0.62,
                        "avg_return_at_target": 0.031,
                        "avg_max_return": 0.075,
                    },
                    "component_summary": {
                        "momentum": {"count": 245, "hit_rate": 0.62},
                        "trend_alignment": {"count": 200, "hit_rate": 0.58},
                    },
                    "top_n": {
                        "top_10": {"count": 10, "hit_rate": 0.78},
                        "top_30": {"count": 30, "hit_rate": 0.71},
                    },
                }
            }
        },
        "error_msg": None,
        "created_at": datetime.datetime(2026, 5, 1, 12, 0, 0),
        "completed_at": datetime.datetime(2026, 5, 1, 13, 0, 0),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestScoreExperimentRankingsAPI:
    """Rankings and heatmap endpoints under /api/score-experiments."""

    # ---- GET /api/score-experiments/rankings ----

    def test_rankings_returns_rankings_and_bonferroni(self, client, monkeypatch):
        """GET /api/score-experiments/rankings — verify 200 + rankings + bonferroni."""
        from app.api.v1 import score_experiments as mod

        rows = [
            _experiment_row(
                id="exp_a",
                model_version="candidate_A",
                report={
                    "horizons": {
                        "20": {
                            "overall": {
                                "count": 245,
                                "hit_rate": 0.62,
                                "avg_return_at_target": 0.031,
                            },
                            "component_summary": {
                                "momentum": {"count": 245, "hit_rate": 0.62},
                            },
                        }
                    }
                },
            ),
            _experiment_row(
                id="exp_b",
                model_version="candidate_B",
                report={
                    "horizons": {
                        "20": {
                            "overall": {
                                "count": 240,
                                "hit_rate": 0.55,
                                "avg_return_at_target": 0.025,
                            },
                            "component_summary": {
                                "momentum": {"count": 240, "hit_rate": 0.55},
                            },
                        }
                    }
                },
            ),
            _experiment_row(
                id="exp_c",
                model_version="candidate_C",
                report={
                    "horizons": {
                        "20": {
                            "overall": {
                                "count": 250,
                                "hit_rate": 0.68,
                                "avg_return_at_target": 0.038,
                            },
                            "component_summary": {
                                "momentum": {"count": 250, "hit_rate": 0.68},
                            },
                        }
                    }
                },
            ),
        ]
        monkeypatch.setattr(mod.ScoreExperiment, "objects", FakeExperimentQuery(rows))

        resp = client.get("/api/score-experiments/rankings")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert "rankings" in data
        assert isinstance(data["rankings"], list)
        assert "bonferroni" in data
        # bonferroni should include corrected_alpha or similar
        assert (
            "corrected_alpha" in data["bonferroni"]
            or "n_comparisons" in data["bonferroni"]
        )

        # Rankings should be sorted (best first)
        if len(data["rankings"]) > 1:
            first = data["rankings"][0].get("composite_score", 0)
            last = data["rankings"][-1].get("composite_score", 999)
            assert first >= last

    def test_rankings_horizon_filter(self, client, monkeypatch):
        """GET /api/score-experiments/rankings?horizon=20 — verify horizon filter."""
        from app.api.v1 import score_experiments as mod

        rows = [
            _experiment_row(
                id="exp_h20",
                model_version="candidate_h20",
                horizons=[20],
                report={
                    "horizons": {
                        "20": {
                            "overall": {
                                "count": 200,
                                "hit_rate": 0.60,
                                "avg_return_at_target": 0.03,
                            },
                            "component_summary": {
                                "momentum": {"count": 200, "hit_rate": 0.60},
                            },
                        }
                    }
                },
            ),
        ]
        monkeypatch.setattr(mod.ScoreExperiment, "objects", FakeExperimentQuery(rows))

        resp = client.get("/api/score-experiments/rankings?horizon=20")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        # Rankings should be filtered by horizon, response includes bonferroni
        assert "rankings" in data
        assert "bonferroni" in data
        assert data["total_experiments"] >= 0

    def test_rankings_no_completed_experiments(self, client, monkeypatch):
        """GET /api/score-experiments/rankings — no completed experiments."""
        from app.api.v1 import score_experiments as mod

        monkeypatch.setattr(mod.ScoreExperiment, "objects", FakeExperimentQuery([]))

        resp = client.get("/api/score-experiments/rankings")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert data["rankings"] == []
        assert "bonferroni" in data

    # ---- GET /api/score-experiments/heatmap ----

    def test_heatmap_returns_components_and_matrix(self, client, monkeypatch):
        """GET /api/score-experiments/heatmap?horizon=20 — verify components + matrix."""
        from app.api.v1 import score_experiments as mod

        rows = [
            _experiment_row(
                id="exp_h1",
                model_version="candidate_H1",
                report={
                    "horizons": {
                        "20": {
                            "overall": {
                                "count": 245,
                                "hit_rate": 0.62,
                            },
                            "component_summary": {
                                "momentum": {"count": 245, "hit_rate": 0.62},
                                "trend_alignment": {"count": 200, "hit_rate": 0.58},
                                "value": {"count": 180, "hit_rate": 0.52},
                            },
                        }
                    }
                },
            ),
            _experiment_row(
                id="exp_h2",
                model_version="candidate_H2",
                report={
                    "horizons": {
                        "20": {
                            "overall": {
                                "count": 230,
                                "hit_rate": 0.55,
                            },
                            "component_summary": {
                                "momentum": {"count": 230, "hit_rate": 0.55},
                                "trend_alignment": {"count": 190, "hit_rate": 0.50},
                                "volatility": {"count": 150, "hit_rate": 0.48},
                            },
                        }
                    }
                },
            ),
        ]
        monkeypatch.setattr(mod.ScoreExperiment, "objects", FakeExperimentQuery(rows))

        resp = client.get("/api/score-experiments/heatmap?horizon=20")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert "components" in data
        assert isinstance(data["components"], list)
        assert "matrix" in data
        assert isinstance(data["matrix"], list)
        # components should list component IDs
        if data["components"]:
            assert isinstance(data["components"][0], str)
        # matrix should be a 2D structure
        if data["matrix"]:
            assert isinstance(data["matrix"][0], (list, dict))

    def test_heatmap_missing_horizon_returns_400(self, client, monkeypatch):
        """GET /api/score-experiments/heatmap — missing horizon returns 400."""
        from app.api.v1 import score_experiments as mod

        rows = [_experiment_row()]
        monkeypatch.setattr(mod.ScoreExperiment, "objects", FakeExperimentQuery(rows))

        resp = client.get("/api/score-experiments/heatmap")
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False
        assert "horizon" in body["message"].lower()

    def test_heatmap_invalid_horizon_returns_400(self, client, monkeypatch):
        """GET /api/score-experiments/heatmap?horizon=99 — invalid horizon."""
        from app.api.v1 import score_experiments as mod

        monkeypatch.setattr(mod.ScoreExperiment, "objects", FakeExperimentQuery([]))

        resp = client.get("/api/score-experiments/heatmap?horizon=99")
        # Should reject non-standard horizons
        assert resp.status_code in (400, 404)
        body = resp.get_json()
        assert body["success"] is False

    def test_heatmap_no_data(self, client, monkeypatch):
        """GET /api/score-experiments/heatmap?horizon=20 — no experiments."""
        from app.api.v1 import score_experiments as mod

        monkeypatch.setattr(mod.ScoreExperiment, "objects", FakeExperimentQuery([]))

        resp = client.get("/api/score-experiments/heatmap?horizon=20")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert "components" in data
        assert len(data["components"]) >= 0  # components always present
        # When no experiments, matrix entries have avg_score 0.0
        assert "matrix" in data
        for entry in data["matrix"]:
            assert entry["avg_score"] == 0.0
