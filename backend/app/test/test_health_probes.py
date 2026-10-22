# -*- coding: utf-8 -*-
"""Liveness and readiness mean different things and must not be conflated."""


def test_liveness_does_not_touch_the_database(client, monkeypatch):
    import app.api.health as health

    class ExplodingDocument:
        @property
        def objects(self):
            raise AssertionError("liveness must not query MongoDB")

    monkeypatch.setattr(health, "ConnTestDocument", ExplodingDocument)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_readiness_reports_ok_when_mongodb_answers(client, monkeypatch):
    import app.api.health as health

    class FakeQuery:
        def first(self):
            return None

    class ReachableDocument:
        objects = FakeQuery()

    monkeypatch.setattr(health, "ConnTestDocument", ReachableDocument)

    response = client.get("/health/ready")

    assert response.status_code == 200
    body = response.get_json()
    assert body["status"] == "ok"
    assert body["database"] == "reachable"


def test_readiness_returns_503_when_mongodb_fails(client, monkeypatch):
    import app.api.health as health

    class FailingQuery:
        def first(self):
            raise ConnectionError("mongodb is down")

    class UnreachableDocument:
        objects = FailingQuery()

    monkeypatch.setattr(health, "ConnTestDocument", UnreachableDocument)

    response = client.get("/health/ready")

    assert response.status_code == 503
    body = response.get_json()
    assert body["status"] == "unavailable"
    assert body["database"] == "unreachable"
