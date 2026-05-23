# -*- coding: utf-8 -*-
"""Tests for the Decision Journal API endpoints."""

import datetime
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Fake mongoengine-style query helpers
# ---------------------------------------------------------------------------
class FakeJournalQuery:
    """Mimics mongoengine QuerySet for DecisionJournal."""

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

    def skip(self, n):
        return type(self)(self.rows[n:])

    def limit(self, n):
        return type(self)(self.rows[:n])

    def count(self):
        return len(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def only(self, *fields):
        return self

    def __iter__(self):
        return iter(self.rows)


def _getattr(obj, name):
    val = getattr(obj, name, None)
    if isinstance(val, datetime.datetime):
        return val.replace(tzinfo=None)
    return val


def _journal_row(**overrides):
    defaults = {
        "id": "journal_001",
        "stock_code": "sh600519",
        "stock_name": "\u8d35\u5dde\u8305\u53f0",
        "date": datetime.datetime(2026, 5, 15),
        "horizon": 20,
        "score": 82.5,
        "recommendation": "BUY",
        "recommended_action": "BUY",
        "confidence": "high",
        "entry_price": 1650.0,
        "target_price": 1750.0,
        "stop_loss": 1500.0,
        "position_size_pct": 10.0,
        "executed": True,
        "executed_at": datetime.datetime(2026, 5, 15, 10, 30, 0),
        "executed_price": 1655.0,
        "executed_quantity": 100,
        "execution_type": "followed",
        "realized_pnl": 5000.0,
        "realized_pnl_pct": 3.0,
        "exit_date": datetime.datetime(2026, 5, 20),
        "exit_price": 1705.0,
        "exit_reason": "target hit",
        "dominant_component": "momentum",
        "notes": "Test entry",
        "created_at": datetime.datetime(2026, 5, 15, 12, 0, 0),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestDecisionJournalAPI:
    """Decision Journal endpoints under /api/decisions/journal."""

    @staticmethod
    def _make_fake_document_class(rows):
        """Create a fake mongoengine Document class with mock objects() chain."""

        class FakeDoc:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.id = kwargs.get("id", "fake_id")
                self.created_at = kwargs.get(
                    "created_at", datetime.datetime(2026, 5, 22)
                )

            def save(self):
                pass

        class FakeQuerySet:
            def __init__(self, items):
                self._items = items

            def filter(self, **kwargs):
                return self

            def order_by(self, *_):
                return self

            def limit(self, n):
                return FakeQuerySet(self._items[:n])

            def only(self, *_):
                return self

            def first(self):
                return self._items[0] if self._items else None

            def count(self):
                return len(self._items)

            def __iter__(self):
                return iter(self._items)

        FakeDoc.objects = FakeQuerySet([FakeDoc(**r) for r in rows])
        return FakeDoc

    def _patch_module(self, monkeypatch, rows):
        """Patch DecisionJournal in the decisions module."""
        import app.api.v1.decisions as mod

        monkeypatch.setattr(
            mod, "DecisionJournal", self._make_fake_document_class(rows)
        )
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

    # ---- POST /api/decisions/journal ----

    def test_create_journal_entry_returns_201(self, client, monkeypatch):
        """POST /api/decisions/journal — create a journal entry."""
        import app.api.v1.decisions as mod

        saved = {}

        class FakeEntry:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.id = "journal_new_001"
                self.created_at = datetime.datetime(2026, 5, 22, 12, 0, 0)

            def save(self):
                saved["entry"] = self

        monkeypatch.setattr(mod, "DecisionJournal", FakeEntry)
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.post(
            "/api/decisions/journal",
            json={
                "stock_code": "sh600519",
                "stock_name": "\u8d35\u5dde\u8305\u53f0",
                "date": "2026-05-15",
                "horizon": 20,
                "score": 82.5,
                "recommendation": "BUY",
                "recommended_action": "BUY",
                "confidence": "high",
            },
        )
        assert resp.status_code == 201
        body = resp.get_json()
        assert body["success"] is True
        assert body["data"]["id"] is not None
        assert body["data"]["stock_code"] == "sh600519"
        assert "entry" in saved

    def test_create_journal_missing_stock_code_returns_400(self, client, monkeypatch):
        """POST /api/decisions/journal — missing stock_code returns 400."""
        monkeypatch.setattr(
            "app.api.v1.decisions.get_jwt_identity", lambda: "test-user"
        )

        resp = client.post(
            "/api/decisions/journal",
            json={
                "date": "2026-05-15",
                "horizon": 20,
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False
        assert "stock_code" in body["message"]

    # ---- GET /api/decisions/journal ----

    def test_list_journal_returns_items(self, client, monkeypatch):
        """GET /api/decisions/journal — list entries, verify 200 + items array."""
        import app.api.v1.decisions as mod

        rows = [_journal_row(id=f"journal_{i:03d}") for i in range(3)]
        monkeypatch.setattr(mod.DecisionJournal, "objects", FakeJournalQuery(rows))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/journal")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert isinstance(data["items"], list)
        assert data["total"] == 3
        assert len(data["items"]) == 3
        assert data["items"][0]["stock_code"] == "sh600519"

    def test_list_journal_filter_by_execution_type(self, client, monkeypatch):
        """GET /api/decisions/journal?execution_type=missed — filter works."""
        import app.api.v1.decisions as mod

        rows = [
            _journal_row(
                id="journal_001", execution_type="followed", stock_code="sh600000"
            ),
            _journal_row(
                id="journal_002", execution_type="missed", stock_code="sz000001"
            ),
            _journal_row(
                id="journal_003", execution_type="deviated", stock_code="sh600519"
            ),
        ]
        monkeypatch.setattr(mod.DecisionJournal, "objects", FakeJournalQuery(rows))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/journal?execution_type=missed")
        assert resp.status_code == 200
        body = resp.get_json()
        data = body["data"]
        assert data["total"] == 1
        assert data["items"][0]["execution_type"] == "missed"
        assert data["items"][0]["stock_code"] == "sz000001"

    def test_list_journal_empty(self, client, monkeypatch):
        """GET /api/decisions/journal — no entries returns empty items."""
        import app.api.v1.decisions as mod

        monkeypatch.setattr(mod.DecisionJournal, "objects", FakeJournalQuery([]))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/journal")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        assert body["data"]["items"] == []
        assert body["data"]["total"] == 0

    # ---- GET /api/decisions/journal/summary ----

    def test_journal_summary_returns_quality_fields(self, client, monkeypatch):
        """GET /api/decisions/journal/summary — verify model_quality + execution_discipline."""
        import app.api.v1.decisions as mod

        rows = [
            _journal_row(id="j1", score=85.0, executed=True, realized_pnl=5000.0),
            _journal_row(id="j2", score=45.0, executed=False, realized_pnl=None),
            _journal_row(id="j3", score=72.0, executed=True, realized_pnl=-1000.0),
            _journal_row(id="j4", score=68.0, executed=True, realized_pnl=3000.0),
        ]
        monkeypatch.setattr(mod.DecisionJournal, "objects", FakeJournalQuery(rows))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/journal/summary")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert data["total_entries"] == 4
        assert "model_quality" in data
        assert "execution_discipline" in data
        # 3 of 4 entries have score >= 60
        assert data["model_quality"] == 0.75
        # 3 of 4 entries were executed
        assert data["execution_discipline"] == 0.75

    def test_journal_summary_no_entries(self, client, monkeypatch):
        """GET /api/decisions/journal/summary — empty returns None quality."""
        import app.api.v1.decisions as mod

        monkeypatch.setattr(mod.DecisionJournal, "objects", FakeJournalQuery([]))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/journal/summary")
        assert resp.status_code == 200
        body = resp.get_json()
        data = body["data"]
        assert data["total_entries"] == 0
        assert data["model_quality"] is None
        assert data["execution_discipline"] is None

    # ---- GET /api/decisions/journal/attribution ----

    def test_journal_attribution_returns_by_component_and_horizon(
        self, client, monkeypatch
    ):
        """GET /api/decisions/journal/attribution — verify by_component + by_horizon."""
        import app.api.v1.decisions as mod

        rows = [
            _journal_row(
                id="ja1",
                dominant_component="momentum",
                horizon=5,
                realized_pnl=3000.0,
            ),
            _journal_row(
                id="ja2",
                dominant_component="trend_alignment",
                horizon=20,
                realized_pnl=5000.0,
            ),
            _journal_row(
                id="ja3",
                dominant_component="momentum",
                horizon=60,
                realized_pnl=-1000.0,
            ),
            _journal_row(
                id="ja4",
                dominant_component="value",
                horizon=5,
                realized_pnl=2000.0,
            ),
        ]
        monkeypatch.setattr(mod.DecisionJournal, "objects", FakeJournalQuery(rows))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/journal/attribution")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert "by_component" in data
        assert "by_horizon" in data
        assert isinstance(data["by_component"], list)
        assert isinstance(data["by_horizon"], list)
        # Verify component breakdown
        momentum = next(
            (c for c in data["by_component"] if c["component"] == "momentum"),
            None,
        )
        assert momentum is not None
        assert momentum["trade_count"] == 2
        assert momentum["total_pnl"] == 2000.0

    def test_journal_attribution_no_entries(self, client, monkeypatch):
        """GET /api/decisions/journal/attribution — empty returns empty arrays."""
        import app.api.v1.decisions as mod

        monkeypatch.setattr(mod.DecisionJournal, "objects", FakeJournalQuery([]))
        monkeypatch.setattr(mod, "get_jwt_identity", lambda: "test-user")

        resp = client.get("/api/decisions/journal/attribution")
        assert resp.status_code == 200
        body = resp.get_json()
        data = body["data"]
        assert data["by_component"] == []
        assert data["by_horizon"] == []
