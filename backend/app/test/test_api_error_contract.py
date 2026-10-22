# -*- coding: utf-8 -*-
"""API error responses are a contract: always JSON, never an HTML error page."""


def test_unknown_route_returns_json(client):
    response = client.get("/api/definitely-not-a-route")

    assert response.status_code == 404
    assert response.is_json
    body = response.get_json()
    assert body["success"] is False
    assert body["error_code"] == "HTTP_404"
    assert body["data"] is None
    assert body["request_id"]


def test_unsupported_method_returns_json(client):
    response = client.put("/api/system/env")

    assert response.status_code == 405
    assert response.is_json
    assert response.get_json()["success"] is False


def test_unhandled_exception_returns_json_500(app):
    app.config["PROPAGATE_EXCEPTIONS"] = False

    def boom():
        raise RuntimeError("kaboom")

    app.add_url_rule("/api/_test_boom", "test_boom", boom)

    response = app.test_client().get("/api/_test_boom")

    assert response.status_code == 500
    assert response.is_json
    body = response.get_json()
    assert body["success"] is False
    assert body["error_code"] == "INTERNAL_ERROR"
    # The internal message must not leak to the client.
    assert "kaboom" not in body["message"]
