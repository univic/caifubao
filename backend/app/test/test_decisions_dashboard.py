# -*- coding: utf-8 -*-
"""Tests for the Decisions Dashboard API — enriched fields."""

import datetime
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Fake mongoengine-style query helpers
# ---------------------------------------------------------------------------
class FakeScorePredQuery:
    """Mimics StockScorePrediction QuerySet for dashboard."""

    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **kwargs):
        if not kwargs:
            return type(self)(self.rows)
        return self.filter(**kwargs)

    def filter(self, **kwargs):
        rows = self.rows
        for key, value in kwargs.items():
            if key.endswith("__gte"):
                field = key.removesuffix("__gte")
                rows = [
                    r
                    for r in rows
                    if _getattr(r, field) and _getattr(r, field) >= value
                ]
            elif key.endswith("__lte"):
                field = key.removesuffix("__lte")
                rows = [
                    r
                    for r in rows
                    if _getattr(r, field) and _getattr(r, field) <= value
                ]
            elif key.endswith("__lt"):
                field = key.removesuffix("__lt")
                rows = [
                    r for r in rows if _getattr(r, field) and _getattr(r, field) < value
                ]
            elif key.endswith("__in"):
                field = key.removesuffix("__in")
                rows = [r for r in rows if getattr(r, field) in value]
            else:
                rows = [r for r in rows if _getattr(r, key) == value]
        return type(self)(rows)

    def order_by(self, *fields):
        return self

    def only(self, *fields):
        return self

    def limit(self, n):
        return type(self)(self.rows[:n])

    def first(self):
        return self.rows[0] if self.rows else None

    def count(self):
        return len(self.rows)

    def __iter__(self):
        return iter(self.rows)


class FakeQuoteQuery:
    """Mimics StockDailyQuote QuerySet for dashboard."""

    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **kwargs):
        if not kwargs:
            return type(self)(self.rows)
        return self.filter(**kwargs)

    def filter(self, **kwargs):
        code = kwargs.get("code")
        if code:
            matching = [r for r in self.rows if getattr(r, "code", None) == code]
            return type(self)(matching)
        return self

    def order_by(self, *fields):
        return self

    def only(self, *fields):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


def _getattr(obj, name):
    val = getattr(obj, name, None)
    if isinstance(val, datetime.datetime):
        return val.replace(tzinfo=None)
    return val


def _make_score_pred(**overrides):
    defaults = {
        "stock_code": "sh600519",
        "stock_name": "贵州茅台",
        "date": datetime.datetime(2026, 5, 15),
        "horizon": 5,
        "score": 82.5,
        "rank": 1,
        "percentile": 0.99,
        "recommendation": "BUY",
        "base_price": 1650.0,
        "status": "TRACKING",
        "verification": {"hit_target_close": True},
        "explanation": {
            "components": [{"id": "momentum", "contribution": 20}],
        },
        "model_version": "score_v2_202604",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_quote(code="sh600519", close_hfq=1650.0, close=1645.0):
    return SimpleNamespace(
        code=code,
        date=datetime.datetime(2026, 5, 15),
        close_hfq=close_hfq,
        close=close,
        turnover_rate=0.5,
        volume=1000000,
    )


class TestDecisionsDashboard:
    """Decisions Dashboard endpoint under /api/decisions/dashboard."""

    def _setup_mocks(self, monkeypatch, score_preds, quotes=None):
        """Set up common mocks for dashboard tests."""
        import app.api.v1.decisions as mod

        monkeypatch.setattr(
            mod.StockScorePrediction,
            "objects",
            FakeScorePredQuery(score_preds),
        )
        if quotes is not None:
            monkeypatch.setattr(
                mod.StockDailyQuote,
                "objects",
                FakeQuoteQuery(quotes),
            )
        # preflight_check is imported inside _position_sizing from
        # app.services.backtest_service — mock at the source
        import app.services.backtest_service as bts

        monkeypatch.setattr(
            bts,
            "preflight_check",
            lambda *a, **kw: {
                "pass": True,
                "capacity_ok": True,
                "checks": [],
                "summary": "All checks passed",
                "stock_code": a[0] if a else "unknown",
            },
        )

    def test_dashboard_includes_position_sizing(self, client, monkeypatch):
        """GET /api/decisions/dashboard — verify position_sizing field on items."""
        preds = [
            _make_score_pred(
                stock_code="sh600519",
                score=82.5,
                rank=1,
                horizon=5,
            ),
            _make_score_pred(
                stock_code="sz000001",
                score=76.0,
                rank=2,
                horizon=5,
                stock_name="平安银行",
            ),
        ]
        quotes = [
            _make_quote("sh600519", 1650.0),
            _make_quote("sz000001", 12.5),
        ]
        self._setup_mocks(monkeypatch, preds, quotes)

        resp = client.get("/api/decisions/dashboard")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]

        # Verify score5 horizon data
        assert "score5" in data
        score5 = data["score5"]
        assert score5["horizon"] == 5
        assert len(score5["items"]) == 2

        # Each item should have position_sizing
        for item in score5["items"]:
            assert "position_sizing" in item, (
                f"position_sizing missing from item {item.get('stock_code')}"
            )
            sizing = item["position_sizing"]
            assert "target_weight_pct" in sizing
            assert "max_shares" in sizing
            assert "capacity_check" in sizing
            assert "current_price" in sizing
            assert sizing["capacity_check"] is True

        # Also check score20 and score60 are present (even if empty)
        assert "score20" in data
        assert "score60" in data

    def test_dashboard_primary_horizon(self, client, monkeypatch):
        """GET /api/decisions/dashboard?horizon=20 — verify primary_horizon."""
        preds = [
            _make_score_pred(
                stock_code="sh600519",
                score=82.0,
                rank=1,
                horizon=20,
                recommendation="BUY",
            ),
            _make_score_pred(
                stock_code="sz000001",
                score=65.0,
                rank=2,
                horizon=20,
                stock_name="平安银行",
                recommendation="WATCH",
            ),
        ]
        quotes = [
            _make_quote("sh600519", 1650.0),
            _make_quote("sz000001", 12.5),
        ]
        self._setup_mocks(monkeypatch, preds, quotes)

        resp = client.get("/api/decisions/dashboard?horizon=20")
        assert resp.status_code == 200
        body = resp.get_json()
        data = body["data"]
        assert data["primary_horizon"] == 20

        # score20 should have items
        score20 = data["score20"]
        assert score20["horizon"] == 20
        assert len(score20["items"]) == 2

    def test_dashboard_no_data(self, client, monkeypatch):
        """GET /api/decisions/dashboard — no predictions returns empty data."""
        self._setup_mocks(monkeypatch, [], [])

        resp = client.get("/api/decisions/dashboard")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        # When there's no data, score5 has date=None, count=0
        assert data["score5"]["date"] is None
        assert data["score5"]["count"] == 0

    def test_dashboard_items_have_invalidation_fields(self, client, monkeypatch):
        """GET /api/decisions/dashboard — each item includes invalidation metadata."""
        preds = [
            _make_score_pred(
                stock_code="sh600519",
                score=82.5,
                rank=1,
                horizon=5,
                recommendation="BUY",
            ),
        ]
        quotes = [_make_quote("sh600519", 1650.0)]
        self._setup_mocks(monkeypatch, preds, quotes)

        resp = client.get("/api/decisions/dashboard")
        assert resp.status_code == 200
        body = resp.get_json()
        items = body["data"]["score5"]["items"]
        assert len(items) > 0
        item = items[0]
        assert "invalidation" in item
        inv = item["invalidation"]
        assert "exit_threshold" in inv
        assert "stop_loss_pct" in inv
        assert "expiry_days" in inv
