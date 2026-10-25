"""Milestone-B paper read API never fabricates forward evidence or fills."""

import datetime
from types import SimpleNamespace

from flask_jwt_extended import create_access_token

from app.api.v1 import research_paper as api


class Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, name, direction):
        self.docs.sort(key=lambda d: d.get(name) or datetime.datetime.min, reverse=direction == -1)
        return self

    def limit(self, count):
        self.docs = self.docs[:count]
        return self

    def __iter__(self):
        return iter(self.docs)


class Collection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, filters, projection=None):
        matches = [d.copy() for d in self.docs if self._matches(d, filters)]
        if projection and projection.get("_id") == 0:
            for d in matches:
                d.pop("_id", None)
        return Cursor(matches)

    def find_one(self, filters, projection=None):
        return next(iter(self.find(filters, projection)), None)

    @staticmethod
    def _matches(doc, filters):
        return all(doc.get(k) == v for k, v in filters.items())


def _data(date="2026-10-23", evidence_kind="FORWARD", hash_kind="spec"):
    return {
        "date": datetime.datetime.fromisoformat(date),
        "strategy_name": "flip_wide_paper",
        "model_version": "flip_wide_shadow_v1",
        "horizon": 20,
        "config_hash": "hash123",
        "hash_kind": hash_kind,
        "status": "COMPLETED",
        "evidence_kind": evidence_kind,
        "decision_at": datetime.datetime.fromisoformat(date + "T10:00:00"),
        "execution_date": datetime.datetime(2026, 10, 26),
        "target_holdings": [{"stock_code": "sh600000", "weight": 0.5}],
        "rebalance": {"added": ["sh600000"], "removed": []},
        "nav_snapshot": {
            "date": datetime.datetime(2026, 10, 26),
            "nav": 100_500,
            "daily_return": 0.005,
            "drawdown": -0.01,
        },
    }


def _setup(monkeypatch, *, runs, window=True):
    windows = (
        [
            {
                "status": "ACTIVE",
                "model_version": "flip_wide_shadow_v1",
                "horizon": 20,
                "config_hash": "hash123",
                "hash_kind": "spec",
                "start_date": datetime.datetime(2026, 10, 23),
            }
        ]
        if window
        else []
    )
    db = {
        "strategy_paper_runs": Collection(runs),
        "strategy_forward_windows": Collection(windows),
    }
    monkeypatch.setattr(api, "_db", lambda: db)


def _auth(app):
    with app.app_context():
        token = create_access_token(identity="paper-api-test")
    return {"Authorization": f"Bearer {token}"}


Q = "?model_version=flip_wide_shadow_v1&horizon=20&config_hash=hash123&hash_kind=spec"


def test_research_paper_requires_auth(client):
    for path in ("/api/strategy/paper/identities", "/api/strategy/paper/overview" + Q):
        assert client.get(path).status_code == 401


def test_research_paper_blocks_service_tokens(client):
    response = client.get(
        "/api/strategy/paper/identities",
        headers={"Authorization": "Bearer st_openclaw_test"},
    )
    assert response.status_code == 403


def test_overview_counts_only_exact_window_forward_and_warns_nav(
    app, client, monkeypatch
):
    rows = [
        _data(date="2026-10-23"),
        _data(date="2026-10-26", evidence_kind="REPLAY"),
        _data(date="2026-10-27", hash_kind="legacy"),
    ]
    _setup(monkeypatch, runs=rows)
    response = client.get("/api/strategy/paper/overview" + Q, headers=_auth(app))
    assert response.status_code == 200
    result = response.get_json()["data"]
    assert result["forward_observed_sessions"] == 1
    assert result["forward_eligible"] is False
    assert result["accounting_status"] == "UNVERIFIED"
    assert result["account_type"] == "RESEARCH_SIMULATION_NOT_TRADING_ACCOUNT"
    assert len(result["curve"]) == 2  # legacy cohort is isolated
    assert result["decisions"][0]["execution_trace"] == "NOT_LINKED"
    assert result["decisions"][0]["nav_snapshot"]["nav"] == 100_500


def test_no_window_never_claims_forward_progress(app, client, monkeypatch):
    _setup(monkeypatch, runs=[_data()], window=False)
    result = client.get(
        "/api/strategy/paper/overview" + Q, headers=_auth(app)
    ).get_json()["data"]
    assert result["forward_observed_sessions"] == 0
    assert result["forward_status"] == "NO_ACTIVE_WINDOW"
    assert result["curve"][0]["nav"] == 100_500  # still labeled unverified


def test_no_snapshot_no_fabricated_curve(app, client, monkeypatch):
    row = _data()
    row["nav_snapshot"] = {}
    _setup(monkeypatch, runs=[row])
    result = client.get(
        "/api/strategy/paper/overview" + Q, headers=_auth(app)
    ).get_json()["data"]
    assert result["curve"] == []
    assert result["decisions"][0]["nav_snapshot"] is None


def test_bad_identity_never_performs_implicit_cross_strategy_scan(
    app, client, monkeypatch
):
    _setup(monkeypatch, runs=[_data()])
    for query in (
        "",
        "?model_version=flip_wide_shadow_v1&horizon=7&config_hash=hash123",
        "?model_version=flip_wide_shadow_v1&horizon=20",
    ):
        assert client.get(
            "/api/strategy/paper/overview" + query,
            headers=_auth(app),
        ).status_code == 400


def test_decision_detail_exact_date_and_hash(app, client, monkeypatch):
    _setup(monkeypatch, runs=[_data()])
    good = client.get(
        "/api/strategy/paper/decisions/2026-10-23" + Q, headers=_auth(app)
    )
    assert good.status_code == 200
    assert good.get_json()["data"]["decision"]["target_holdings"][0]["weight"] == 0.5
    assert client.get(
        "/api/strategy/paper/decisions/2026-10-24" + Q, headers=_auth(app)
    ).status_code == 404
    assert client.get(
        "/api/strategy/paper/decisions/2026-10-99" + Q, headers=_auth(app)
    ).status_code == 400


def test_identities_reports_distinct_pinned_cohorts(app, client, monkeypatch):
    _setup(monkeypatch, runs=[_data(), _data(date="2026-10-26")])
    response = client.get(
        "/api/strategy/paper/identities", headers=_auth(app)
    )
    body = response.get_json()["data"]
    assert len(body["items"]) == 1
    assert body["items"][0]["config_hash"] == "hash123"
    assert body["items"][0]["latest_date"] == "2026-10-26"
