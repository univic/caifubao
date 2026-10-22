# -*- coding: utf-8 -*-
"""Prometheus endpoint and request-id propagation."""


def test_metrics_endpoint_is_scrapable(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"
    body = response.get_data(as_text=True)
    assert "caifubao_api_requests_total" in body
    assert "caifubao_compute_tasks" in body


def test_incoming_request_id_is_preserved(client):
    response = client.get("/api/system/env", headers={"X-Request-ID": "trace-abc"})

    assert response.headers["X-Request-ID"] == "trace-abc"


def test_request_id_is_generated_when_missing(client):
    response = client.get("/api/system/env")

    assert response.headers.get("X-Request-ID")


def test_error_responses_carry_the_request_id(client):
    response = client.get("/api/not-a-route", headers={"X-Request-ID": "trace-404"})

    assert response.status_code == 404
    assert response.headers["X-Request-ID"] == "trace-404"
    assert response.get_json()["request_id"] == "trace-404"
