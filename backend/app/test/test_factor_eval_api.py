# -*- coding: utf-8 -*-
"""Tests for the Factor Evaluation API endpoints."""

import datetime
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Fake mongoengine-style query helpers
# ---------------------------------------------------------------------------
class FakeFactorEvalQuery:
    """Mimics mongoengine QuerySet for FactorEvalReport."""

    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **kwargs):
        if not kwargs:
            return type(self)(self.rows)
        return self.filter(**kwargs)

    def filter(self, **kwargs):
        rows = self.rows
        for key, value in kwargs.items():
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

    def __iter__(self):
        return iter(self.rows)


class FakePredictionQuery:
    """Mimics StockScorePrediction QuerySet for components listing."""

    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **kwargs):
        return self

    def only(self, *fields):
        return self

    def limit(self, n):
        return self

    def __iter__(self):
        return iter(self.rows)


def _getattr(obj, name):
    val = getattr(obj, name, None)
    if isinstance(val, datetime.datetime):
        return val.replace(tzinfo=None)
    return val


def _report_row(**overrides):
    defaults = {
        "id": "fer_001",
        "factor_name": "momentum",
        "factor_description": "Price momentum factor",
        "start_date": datetime.datetime(2025, 1, 1),
        "end_date": datetime.datetime(2025, 12, 31),
        "observation_count": 250,
        "ic_summary": {
            "5": {"ic_mean": 0.03, "ic_std": 0.08},
            "20": {"ic_mean": 0.05, "ic_std": 0.10},
            "60": {"ic_mean": 0.04, "ic_std": 0.12},
        },
        "icir_summary": {
            "5": 0.375,
            "20": 0.5,
            "60": 0.333,
        },
        "quintile_analysis": {
            "20": [
                {"quintile": 1, "avg_return": -0.01},
                {"quintile": 5, "avg_return": 0.06},
            ],
        },
        "correlation_matrix": {"trend_alignment": 0.45, "value": 0.12},
        "decay_curve": {"1": 0.06, "5": 0.05, "10": 0.04, "20": 0.03, "60": 0.01},
        "regime_ic": {},
        "component_contribution": {},
        "win_rate_by_component": {},
        "status": "COMPLETED",
        "error_msg": None,
        "created_at": datetime.datetime(2026, 5, 1, 12, 0, 0),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestFactorEvalAPI:
    """Factor Evaluation API endpoints under /api/factor-eval."""

    # ---- GET /api/factor-eval/reports ----

    def test_list_reports_returns_items(self, client, monkeypatch):
        """GET /api/factor-eval/reports — list reports, verify 200 + items array."""
        from app.api.v1 import factor_eval as mod

        rows = [
            _report_row(id="fer_001", factor_name="momentum"),
            _report_row(id="fer_002", factor_name="volume_ratio"),
            _report_row(id="fer_003", factor_name="rsi_14"),
        ]
        monkeypatch.setattr(mod.FactorEvalReport, "objects", FakeFactorEvalQuery(rows))

        resp = client.get("/api/factor-eval/reports")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert "items" in data
        assert data["total"] == 3
        assert len(data["items"]) == 3
        assert data["items"][0]["factor_name"] is not None
        assert "ic_summary" in data["items"][0]
        assert "icir_summary" in data["items"][0]

    def test_list_reports_filter_by_factor_name(self, client, monkeypatch):
        """GET /api/factor-eval/reports?factor_name=momentum — filter works."""
        from app.api.v1 import factor_eval as mod

        rows = [
            _report_row(id="fer_a", factor_name="momentum"),
            _report_row(id="fer_b", factor_name="volume_ratio"),
            _report_row(id="fer_c", factor_name="momentum"),
        ]
        monkeypatch.setattr(mod.FactorEvalReport, "objects", FakeFactorEvalQuery(rows))

        resp = client.get("/api/factor-eval/reports?factor_name=momentum")
        assert resp.status_code == 200
        body = resp.get_json()
        data = body["data"]
        assert data["total"] == 2
        for item in data["items"]:
            assert item["factor_name"] == "momentum"

    def test_list_reports_empty(self, client, monkeypatch):
        """GET /api/factor-eval/reports — no reports returns empty list."""
        from app.api.v1 import factor_eval as mod

        monkeypatch.setattr(mod.FactorEvalReport, "objects", FakeFactorEvalQuery([]))

        resp = client.get("/api/factor-eval/reports")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["data"]["items"] == []
        assert body["data"]["total"] == 0

    # ---- GET /api/factor-eval/reports/<id> ----

    def test_get_report_detail_returns_200(self, client, monkeypatch):
        """GET /api/factor-eval/reports/<id> — get report detail, verify 200."""
        from app.api.v1 import factor_eval as mod

        row = _report_row(
            id="fer_detail_001",
            factor_name="momentum",
            correlation_matrix={"trend_alignment": 0.45, "value": 0.12},
            decay_curve={"1": 0.06, "5": 0.05, "20": 0.03},
        )
        monkeypatch.setattr(mod.FactorEvalReport, "objects", FakeFactorEvalQuery([row]))

        resp = client.get("/api/factor-eval/reports/fer_detail_001")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert data["factor_name"] == "momentum"
        assert data["status"] == "COMPLETED"
        # Detail includes correlation_matrix and decay_curve
        assert "correlation_matrix" in data
        assert isinstance(data["correlation_matrix"], list)
        assert "decay_curve" in data
        assert isinstance(data["decay_curve"], list)
        # Detail also includes regime_ic, component_contribution, win_rate_by_component
        assert "regime_ic" in data
        assert "component_contribution" in data
        assert "win_rate_by_component" in data

    def test_get_report_nonexistent_returns_404(self, client, monkeypatch):
        """GET /api/factor-eval/reports/<id> — nonexistent returns 404."""
        from app.api.v1 import factor_eval as mod

        monkeypatch.setattr(mod.FactorEvalReport, "objects", FakeFactorEvalQuery([]))

        resp = client.get("/api/factor-eval/reports/nonexistent")
        assert resp.status_code == 404
        body = resp.get_json()
        assert body["success"] is False
        assert "not found" in body["message"].lower()

    # ---- GET /api/factor-eval/components ----

    def test_list_components_returns_items(self, client, monkeypatch):
        """GET /api/factor-eval/components — list components, verify 200 + items."""
        from app.model import scoring

        # Create fake score predictions with different components
        preds = [
            SimpleNamespace(
                explanation={
                    "components": [
                        {"id": "momentum", "label": "动量"},
                        {"id": "trend_alignment", "label": "趋势对齐"},
                    ]
                }
            ),
            SimpleNamespace(
                explanation={
                    "components": [
                        {"id": "momentum", "label": "动量"},
                        {"id": "value", "label": "价值"},
                    ]
                }
            ),
            SimpleNamespace(
                explanation={
                    "components": [
                        {"id": "trend_alignment", "label": "趋势对齐"},
                        {"id": "volatility", "label": "波动率"},
                    ]
                }
            ),
        ]
        monkeypatch.setattr(
            scoring.StockScorePrediction,
            "objects",
            FakePredictionQuery(preds),
        )

        resp = client.get("/api/factor-eval/components")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert "items" in data
        assert data["total"] > 0
        # momentum appears in 2 predictions
        momentum = next(
            (c for c in data["items"] if c["component_id"] == "momentum"),
            None,
        )
        assert momentum is not None
        assert momentum["prediction_count"] == 2
        assert momentum["label"] == "动量"
        # Items sorted by count descending
        assert (
            data["items"][0]["prediction_count"]
            >= data["items"][-1]["prediction_count"]
        )

    def test_list_components_empty(self, client, monkeypatch):
        """GET /api/factor-eval/components — no predictions returns empty."""
        from app.model import scoring

        monkeypatch.setattr(
            scoring.StockScorePrediction, "objects", FakePredictionQuery([])
        )

        resp = client.get("/api/factor-eval/components")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["data"]["items"] == []
        assert body["data"]["total"] == 0
