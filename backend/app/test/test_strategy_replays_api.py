# -*- coding: utf-8 -*-
"""Tests for the read-only strategy replay API.

The API reads the ``strategy_replays`` collection; these tests replace the
MongoEngine manager with an in-memory fake so no live MongoDB is required.
"""

import datetime
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Fake mongoengine query helpers
# ---------------------------------------------------------------------------


class FakeQuery:
    """Minimal mongoengine-style query chain for replay lookups."""

    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **filters):
        if not filters:
            return type(self)(self.rows)
        return self.filter(**filters)

    def filter(self, **kwargs):
        rows = self.rows
        for key, value in kwargs.items():
            rows = [row for row in rows if _getattr(row, key) == value]
        return type(self)(rows)

    def order_by(self, *fields):
        rows = self.rows
        for field in reversed(fields):
            reverse = field.startswith("-")
            name = field.removeprefix("-")
            rows = sorted(
                rows,
                key=lambda row: _sort_key(_getattr(row, name)),
                reverse=reverse,
            )
        return type(self)(rows)

    def skip(self, count):
        return type(self)(self.rows[count:])

    def limit(self, count):
        return type(self)(self.rows[:count])

    def count(self):
        return len(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def only(self, *fields):
        return self

    def __iter__(self):
        return iter(self.rows)


def _getattr(obj, name):
    return getattr(obj, name, None)


def _sort_key(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if value is None:
        return ""
    return str(value)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_row(index: int = 0, **overrides) -> SimpleNamespace:
    data = {
        "replay_id": f"riq_v1_2018-2026_50k_v{index + 1}",
        "name": "RIQ v1 balanced core-satellite",
        "strategy": "riq_v1",
        "description": "50k balanced replay (core/satellite/cash)",
        "start_date": datetime.datetime(2018, 1, 2),
        "end_date": datetime.datetime(2026, 4, 30),
        "aum": 50000.0,
        "config": {
            "core_weights": {"510300.SH": 0.4, "518880.SH": 0.3, "513100.SH": 0.3},
            "cash_floor": 0.1,
            "rebalance": "quarterly",
        },
        "config_hash": "cfg-deadbeef",
        "input_hash": "input-cafebabe",
        "content_hash": "content-1234",
        "schema_version": "1.0.0",
        "source_artifact": "datahub:strategy_replay_runner",
        "evidence_kind": "REPLAY",
        "forward_sessions": 0,
        "forward_required": 120,
        "forward_eligible": False,
        "data_as_of": "2026-04-30",
        "generated_at": datetime.datetime(2026, 5, 1, 9, 30),
        "research_grade": "RESEARCH",
        "disclaimer": (
            "Research use only. Historical replay is not forward validation "
            "and not investment advice."
        ),
        "survivorship_gap": True,
        "limitations": ["survivorship gap", "QDII premium unmodelled"],
        "metrics": {
            "total_return": 0.62,
            "annualised": 0.065,
            "sharpe": 0.71,
            "max_drawdown": -0.21,
            "annual_turnover": 0.42,
            "total_fees": 1234.5,
            "average_cash_share": 0.1,
            "benchmark_return": 0.33,
        },
        "curve": [
            {
                "date": datetime.datetime(2018, 1, 2),
                "nav": 1.0,
                "benchmark_nav": 1.0,
                "drawdown": 0.0,
                "cash": 5000.0,
                "positions_count": 3,
                "gross_exposure": 0.9,
            },
            {
                "date": datetime.datetime(2018, 1, 3),
                "nav": 1.004,
                "benchmark_nav": 1.001,
                "drawdown": -0.001,
                "cash": 4900.0,
                "positions_count": 3,
                "gross_exposure": 0.902,
            },
        ],
        "decisions": [
            {
                "decision_id": "d1",
                "date": datetime.datetime(2018, 3, 30),
                "execution_date": datetime.datetime(2018, 4, 2),
                "target_holdings": [
                    {"code": "510300.SH", "weight": 0.4},
                    {"code": "518880.SH", "weight": 0.3},
                ],
                "added": ["518880.SH"],
                "removed": [],
                "cash_before": 0.2,
                "cash_after": 0.1,
                "estimated_cost": 12.5,
            }
        ],
        "fills": [
            {
                "decision_id": "d1",
                "date": datetime.datetime(2018, 4, 2),
                "code": "518880.SH",
                "side": "BUY",
                "quantity": 1000,
                "price": 3.5,
                "commission": 5.0,
                "stamp_duty": 0.0,
                "transfer_fee": 0.03,
                "reason": "rebalance",
            }
        ],
        "blocked_orders": [
            {
                "decision_id": "d1",
                "date": "2018-04-02T00:00:00",
                "code": "513100.SH",
                "side": "BUY",
                "reason": "QDII purchase limit",
            }
        ],
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _auth_headers(app):
    from flask_jwt_extended import create_access_token

    with app.app_context():
        token = create_access_token(identity="test-user")
    return {"Authorization": f"Bearer {token}"}


def _patch_rows(monkeypatch, rows):
    from app.api.v1 import strategy_replays as api_mod

    # Replace the module-level reference instead of the Document class
    # attribute: reading StrategyReplay.objects would open a Mongo connection
    # (even under monkeypatch.setattr, which resolves the old value first).
    monkeypatch.setattr(
        api_mod, "StrategyReplay", SimpleNamespace(objects=FakeQuery(rows))
    )


# ---------------------------------------------------------------------------
# List endpoint
# ---------------------------------------------------------------------------


class TestStrategyReplayListAPI:
    def test_list_requires_user_jwt(self, client):
        response = client.get("/api/strategy/replays")

        assert response.status_code == 401

    def test_list_rejects_service_token(self, client):
        response = client.get(
            "/api/strategy/replays",
            headers={"Authorization": "Bearer st_openclaw_token"},
        )

        assert response.status_code == 403
        body = response.get_json()
        assert body["success"] is False
        assert body["error_code"] == "SERVICE_TOKEN_BLOCKED"

    def test_list_empty(self, app, client, monkeypatch):
        _patch_rows(monkeypatch, [])

        response = client.get("/api/strategy/replays", headers=_auth_headers(app))

        assert response.status_code == 200
        body = response.get_json()
        assert body["success"] is True
        assert set(body) == {"success", "message", "request_id", "generated_at", "data"}
        assert body["data"] == {"total": 0, "page": 1, "page_size": 20, "items": []}

    def test_list_returns_summaries_without_process_arrays(
        self, app, client, monkeypatch
    ):
        rows = [_make_row(0), _make_row(1)]
        _patch_rows(monkeypatch, rows)

        response = client.get("/api/strategy/replays", headers=_auth_headers(app))

        assert response.status_code == 200
        body = response.get_json()
        data = body["data"]
        assert data["total"] == 2
        assert data["page"] == 1
        assert data["page_size"] == 20
        assert len(data["items"]) == 2

        item = data["items"][0]
        # Same generated_at for both rows -> deterministic replay_id tie-break.
        assert item["replay_id"] == "riq_v1_2018-2026_50k_v2"
        assert item["strategy"] == "riq_v1"
        assert item["evidence_kind"] == "REPLAY"
        assert item["forward_sessions"] == 0
        assert item["forward_required"] == 120
        assert item["forward_eligible"] is False
        assert item["data_as_of"] == "2026-04-30"
        assert item["start_date"] == "2018-01-02"
        assert item["end_date"] == "2026-04-30"
        assert item["generated_at"] == "2026-05-01T09:30:00"
        assert item["metrics"]["total_return"] == 0.62
        assert item["survivorship_gap"] is True

        # The list contract is summary-only: no decision/fill/curve payloads.
        for key in ("curve", "decisions", "fills", "blocked_orders", "config"):
            assert key not in item

    def test_list_pagination_defaults_and_clamps(self, app, client, monkeypatch):
        rows = [_make_row(i) for i in range(250)]
        _patch_rows(monkeypatch, rows)
        headers = _auth_headers(app)

        response = client.get("/api/strategy/replays?page_size=10000", headers=headers)
        assert response.status_code == 200
        data = response.get_json()["data"]
        assert data["page"] == 1
        assert data["page_size"] == 200
        assert len(data["items"]) == 200

        response = client.get(
            "/api/strategy/replays?page=0&page_size=0", headers=headers
        )
        data = response.get_json()["data"]
        assert data["page"] == 1
        assert data["page_size"] == 1

        response = client.get(
            "/api/strategy/replays?page=abc&page_size=abc", headers=headers
        )
        data = response.get_json()["data"]
        assert data["page"] == 1
        assert data["page_size"] == 20

        response = client.get(
            "/api/strategy/replays?page=99999999999999999999", headers=headers
        )
        assert response.status_code == 200
        data = response.get_json()["data"]
        assert data["page"] == 1_000_000
        assert data["items"] == []

    def test_list_page_slice(self, app, client, monkeypatch):
        rows = [_make_row(i) for i in range(5)]
        _patch_rows(monkeypatch, rows)

        response = client.get(
            "/api/strategy/replays?page=2&page_size=2", headers=_auth_headers(app)
        )

        assert response.status_code == 200
        data = response.get_json()["data"]
        assert data["total"] == 5
        assert data["page"] == 2
        assert data["page_size"] == 2
        assert [item["replay_id"] for item in data["items"]] == [
            "riq_v1_2018-2026_50k_v3",
            "riq_v1_2018-2026_50k_v2",
        ]

    def test_collection_is_read_only(self, app, client, monkeypatch):
        _patch_rows(monkeypatch, [_make_row(0)])
        headers = _auth_headers(app)

        for method in (client.post, client.put, client.patch, client.delete):
            response = method("/api/strategy/replays", headers=headers)
            assert response.status_code == 405


# ---------------------------------------------------------------------------
# Detail endpoint
# ---------------------------------------------------------------------------


class TestStrategyReplayDetailAPI:
    def test_detail_requires_user_jwt(self, client):
        response = client.get("/api/strategy/replays/riq_v1_2018-2026_50k_v1")

        assert response.status_code == 401

    def test_detail_rejects_service_token(self, client):
        response = client.get(
            "/api/strategy/replays/riq_v1_2018-2026_50k_v1",
            headers={"Authorization": "Bearer st_openclaw_token"},
        )

        assert response.status_code == 403

    def test_detail_not_found_envelope(self, app, client, monkeypatch):
        _patch_rows(monkeypatch, [])

        response = client.get(
            "/api/strategy/replays/missing_replay",
            headers=_auth_headers(app),
        )

        assert response.status_code == 404
        body = response.get_json()
        assert body["success"] is False
        assert body["data"] is None
        assert "request_id" in body
        assert "generated_at" in body
        assert "missing_replay" in body["message"]

    def test_detail_returns_full_artifact_with_iso_dates(
        self, app, client, monkeypatch
    ):
        row = _make_row(0)
        _patch_rows(monkeypatch, [row])

        response = client.get(
            f"/api/strategy/replays/{row.replay_id}",
            headers=_auth_headers(app),
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body["success"] is True
        data = body["data"]

        assert data["replay_id"] == row.replay_id
        assert data["config"]["cash_floor"] == 0.1
        assert data["config_hash"] == "cfg-deadbeef"
        assert data["input_hash"] == "input-cafebabe"
        assert data["content_hash"] == "content-1234"
        assert data["evidence_kind"] == "REPLAY"
        assert data["research_grade"] == "RESEARCH"
        assert data["limitations"] == ["survivorship gap", "QDII premium unmodelled"]
        assert data["disclaimer"].startswith("Research use only.")
        assert len(data["curve"]) == 2
        assert len(data["decisions"]) == 1
        assert len(data["fills"]) == 1
        assert len(data["blocked_orders"]) == 1

        # All date-like values cross the wire as ISO strings.
        assert data["curve"][0]["date"] == "2018-01-02"
        assert data["decisions"][0]["date"] == "2018-03-30"
        assert data["decisions"][0]["execution_date"] == "2018-04-02"
        assert data["fills"][0]["date"] == "2018-04-02"
        assert data["blocked_orders"][0]["date"] == "2018-04-02"
        assert data["generated_at"] == "2026-05-01T09:30:00"
        assert isinstance(data["curve"][0]["date"], str)
        assert isinstance(data["generated_at"], str)

    def test_detail_reasserts_replay_forward_fields(self, app, client, monkeypatch):
        row = _make_row(
            0,
            forward_sessions=7,
            forward_required=999,
            forward_eligible=True,
        )
        _patch_rows(monkeypatch, [row])

        response = client.get(
            f"/api/strategy/replays/{row.replay_id}",
            headers=_auth_headers(app),
        )

        assert response.status_code == 200
        data = response.get_json()["data"]
        assert data["forward_sessions"] == 0
        assert data["forward_required"] == 120
        assert data["forward_eligible"] is False
        assert data["evidence_kind"] == "REPLAY"

    def test_endpoint_is_read_only(self, app, client, monkeypatch):
        _patch_rows(monkeypatch, [_make_row(0)])
        headers = _auth_headers(app)

        for method in (client.post, client.put, client.patch, client.delete):
            response = method(
                "/api/strategy/replays/riq_v1_2018-2026_50k_v1",
                headers=headers,
            )
            assert response.status_code == 405


def test_read_model_declares_the_datahub_timestamps():
    """The datahub writer persists created_at/updated_at; the read model must
    declare them or a real Mongo fetch raises FieldDoesNotExist."""
    from app.model.strategy_replay import StrategyReplay

    assert "created_at" in StrategyReplay._fields
    assert "updated_at" in StrategyReplay._fields
