# -*- coding: utf-8 -*-
"""Tests for the Watchlist API endpoints."""

import datetime
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Fake mongoengine-style query helpers
# ---------------------------------------------------------------------------
class FakeWatchlistQuery:
    """Mimics mongoengine QuerySet for Watchlist."""

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

    def only(self, *fields):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class FakeStockQuery:
    """Mimics mongoengine QuerySet for IndividualStock / StockScorePrediction / StockDailyQuote."""

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


def _watchlist_row(**overrides):
    defaults = {
        "id": "wl_001",
        "name": "My Watchlist",
        "stock_codes": ["sh600519", "sz000001"],
        "user_id": "test-user",
        "created_at": datetime.datetime(2026, 5, 1, 12, 0, 0),
        "updated_at": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _fake_stock(name_map=None):
    """Return a callable that returns SimpleNamespace for IndividualStock.first()."""
    name_map = name_map or {
        "sh600519": "贵州茅台",
        "sz000001": "平安银行",
    }

    class FakeStockQueryCtx(FakeStockQuery):
        def filter(self, **kwargs):
            code = kwargs.get("code")
            name = name_map.get(code)
            if name:
                return FakeStockQueryCtx([SimpleNamespace(code=code, name=name)])
            return FakeStockQueryCtx([])

    return FakeStockQueryCtx([])


def _fake_scores():
    """Return a callable that returns fake StockScorePrediction data."""
    data = {
        ("sh600519", 5): SimpleNamespace(
            stock_code="sh600519",
            horizon=5,
            score=76.5,
            recommendation="BUY",
            date=datetime.datetime(2026, 5, 15),
        ),
        ("sh600519", 20): SimpleNamespace(
            stock_code="sh600519",
            horizon=20,
            score=82.0,
            recommendation="BUY",
            date=datetime.datetime(2026, 5, 15),
        ),
        ("sh600519", 60): SimpleNamespace(
            stock_code="sh600519",
            horizon=60,
            score=68.0,
            recommendation="WATCH",
            date=datetime.datetime(2026, 5, 15),
        ),
        ("sz000001", 5): SimpleNamespace(
            stock_code="sz000001",
            horizon=5,
            score=55.0,
            recommendation="WATCH",
            date=datetime.datetime(2026, 5, 15),
        ),
        ("sz000001", 20): SimpleNamespace(
            stock_code="sz000001",
            horizon=20,
            score=60.0,
            recommendation="WATCH",
            date=datetime.datetime(2026, 5, 15),
        ),
        ("sz000001", 60): SimpleNamespace(
            stock_code="sz000001",
            horizon=60,
            score=42.0,
            recommendation="AVOID",
            date=datetime.datetime(2026, 5, 15),
        ),
    }

    class FakeScoreQueryCtx(FakeStockQuery):
        def filter(self, **kwargs):
            stock_code = kwargs.get("stock_code")
            horizon = kwargs.get("horizon")
            key = (stock_code, horizon)
            if key in data:
                return FakeScoreQueryCtx([data[key]])
            return FakeScoreQueryCtx([])

    return FakeScoreQueryCtx([])


def _fake_quotes():
    """Return a callable that returns fake StockDailyQuote data."""
    quote_data = {
        "sh600519": SimpleNamespace(
            code="sh600519",
            close_hfq=1650.0,
            close=1645.0,
            date=datetime.datetime(2026, 5, 15),
        ),
        "sz000001": SimpleNamespace(
            code="sz000001",
            close_hfq=12.5,
            close=12.3,
            date=datetime.datetime(2026, 5, 15),
        ),
    }

    class FakeQuoteQueryCtx(FakeStockQuery):
        def filter(self, **kwargs):
            code = kwargs.get("code")
            if code in quote_data:
                return FakeQuoteQueryCtx([quote_data[code]])
            return FakeQuoteQueryCtx([])

    return FakeQuoteQueryCtx([])


class TestWatchlistAPI:
    """Watchlist endpoints under /api/decisions/watchlists."""

    # ---- POST /api/decisions/watchlists ----

    def test_create_watchlist_returns_201(self, client, monkeypatch):
        """POST /api/decisions/watchlists — create watchlist, verify 201."""
        import app.api.v1.decisions as mod

        saved = {}

        class FakeWatchlist:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.id = "wl_new_001"
                self.created_at = datetime.datetime(2026, 5, 22, 12, 0, 0)

            def save(self):
                saved["watchlist"] = self

        monkeypatch.setattr(mod, "Watchlist", FakeWatchlist)
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.post(
            "/api/decisions/watchlists",
            json={
                "name": "Test Watchlist",
                "stock_codes": ["sh600519", "sz000001"],
            },
        )
        assert resp.status_code == 201
        body = resp.get_json()
        assert body["success"] is True
        assert body["data"]["name"] == "Test Watchlist"
        assert "stock_codes" in body["data"]
        assert "watchlist" in saved

    def test_create_watchlist_missing_name_returns_400(self, client, monkeypatch):
        """POST /api/decisions/watchlists — missing name returns 400."""
        monkeypatch.setattr(
            "app.api.v1.decisions.get_jwt_identity", lambda: "test-user"
        )

        resp = client.post(
            "/api/decisions/watchlists",
            json={"stock_codes": ["sh600519"]},
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False
        assert "name" in body["message"]

    def test_create_watchlist_updates_existing(self, client, monkeypatch):
        """POST /api/decisions/watchlists — updating existing watchlist."""
        import app.api.v1.decisions as mod

        saved = {}

        class FakeExistingWatchlist:
            def __init__(self, **kwargs):
                pass  # Not called for the find path

        existing = SimpleNamespace(
            id="wl_existing_001",
            name="Test Watchlist",
            stock_codes=["sh600519"],
            user_id="test-user",
            created_at=datetime.datetime(2026, 5, 1),
        )

        class FakeWatchlistUpdate:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.id = "wl_existing_001"
                self.created_at = existing.created_at

            def save(self):
                saved["updated"] = self.stock_codes

        # Mock the query to return the existing watchlist
        monkeypatch.setattr(mod.Watchlist, "objects", FakeWatchlistQuery([existing]))
        # The code path: finds existing, then does wl.stock_codes = ..., wl.save()
        # We need to ensure the existing watchlist's save works
        existing.save = lambda: saved.update({"updated_existing": True})
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.post(
            "/api/decisions/watchlists",
            json={
                "name": "Test Watchlist",
                "stock_codes": ["sh600519", "sz000001", "sz000002"],
            },
        )
        # Should be 201 or 200 for update
        assert resp.status_code in (200, 201)
        body = resp.get_json()
        assert body["success"] is True
        # The existing watchlist save should have been called
        assert "updated_existing" in saved

    # ---- GET /api/decisions/watchlists ----

    def test_list_watchlists_returns_items(self, client, monkeypatch):
        """GET /api/decisions/watchlists — list watchlists, verify 200 + items."""
        import app.api.v1.decisions as mod

        rows = [
            _watchlist_row(id="wl_1", name="Tech Stocks"),
            _watchlist_row(id="wl_2", name="Blue Chips"),
            _watchlist_row(id="wl_3", name="Small Caps"),
        ]
        monkeypatch.setattr(mod.Watchlist, "objects", FakeWatchlistQuery(rows))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/watchlists")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert "items" in data
        assert data["total"] == 3
        assert len(data["items"]) == 3
        assert data["items"][0]["name"] == "Blue Chips"
        assert "stock_count" in data["items"][0]

    def test_list_watchlists_empty(self, client, monkeypatch):
        """GET /api/decisions/watchlists — no watchlists returns empty."""
        import app.api.v1.decisions as mod

        monkeypatch.setattr(mod.Watchlist, "objects", FakeWatchlistQuery([]))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/watchlists")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["data"]["items"] == []
        assert body["data"]["total"] == 0

    # ---- GET /api/decisions/watchlists/<id> ----

    def test_get_watchlist_with_scores(self, client, monkeypatch):
        """GET /api/decisions/watchlists/<id> — returns watchlist with scores."""
        import app.api.v1.decisions as mod

        wl = _watchlist_row(id="wl_view_1", name="Score Watch")
        monkeypatch.setattr(mod.Watchlist, "objects", FakeWatchlistQuery([wl]))

        # Mock IndividualStock for stock name lookups
        monkeypatch.setattr(
            mod, "IndividualStock", SimpleNamespace(objects=_fake_stock())
        )
        # Mock StockScorePrediction for score lookups
        monkeypatch.setattr(
            mod, "StockScorePrediction", SimpleNamespace(objects=_fake_scores())
        )
        # Mock StockDailyQuote for price lookups
        monkeypatch.setattr(
            mod, "StockDailyQuote", SimpleNamespace(objects=_fake_quotes())
        )
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/watchlists/wl_view_1")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert data["name"] == "Score Watch"
        assert "stocks" in data
        assert len(data["stocks"]) == 2
        # Verify stock 1 has scores
        stock0 = data["stocks"][0]
        assert "stock_code" in stock0
        assert "stock_name" in stock0
        assert "current_price" in stock0
        assert "scores" in stock0
        # Should have score5, score20, score60
        scores = stock0["scores"]
        assert "score5" in scores or "score20" in scores

    def test_get_watchlist_not_found_returns_404(self, client, monkeypatch):
        """GET /api/decisions/watchlists/<id> — nonexistent returns 404."""
        import app.api.v1.decisions as mod

        monkeypatch.setattr(mod.Watchlist, "objects", FakeWatchlistQuery([]))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/watchlists/nonexistent_id")
        assert resp.status_code == 404
        body = resp.get_json()
        assert body["success"] is False
