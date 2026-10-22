# -*- coding: utf-8 -*-
"""Prometheus metrics and request correlation for the API.

The metrics endpoint is intentionally unauthenticated (scrapers do not carry
user JWTs) and lives on the same port as the API. Multi-process gunicorn is
supported through ``PROMETHEUS_MULTIPROC_DIR``: when the directory is set, the
scrape handler aggregates the per-worker mmap files; otherwise it serves the
in-process registry (single-worker / local development).

Every request gets an ``X-Request-ID`` (incoming header is preserved) that is
also attached to the JSON logs, so a slow request can be traced across workers.
"""

import os
import threading
import time
import uuid

from flask import Flask, Response, g, request
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    generate_latest,
)

REQUEST_COUNT = Counter(
    "caifubao_api_requests_total",
    "API requests by method, route template and status",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "caifubao_api_request_duration_seconds",
    "API request latency by method and route template",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
COMPUTE_TASKS = Gauge(
    "caifubao_compute_tasks",
    "Compute tasks currently in the given status",
    ["status"],
    multiprocess_mode="max",
)


def _registry() -> CollectorRegistry:
    if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
        from prometheus_client import multiprocess

        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return registry
    return REGISTRY


def _route_label() -> str:
    rule = request.url_rule
    # The route template (not the concrete URL) keeps label cardinality bounded.
    return getattr(rule, "rule", None) or "unknown"


def _refresh_queue_gauges() -> None:
    try:
        from app.model.compute_task import ComputeTask

        for status in ("PENDING", "RUNNING"):
            COMPUTE_TASKS.labels(status=status).set(
                ComputeTask.objects(status=status).count()
            )
    except Exception:  # pragma: no cover - metrics must never break a scrape
        pass


def _queue_gauge_loop(interval_seconds: int = 30) -> None:
    while True:
        _refresh_queue_gauges()
        time.sleep(interval_seconds)


def init_metrics(app: Flask) -> None:
    @app.before_request
    def _start_request():
        g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        g.request_started_at = time.perf_counter()

    @app.after_request
    def _finish_request(response):
        elapsed = time.perf_counter() - getattr(
            g, "request_started_at", time.perf_counter()
        )
        endpoint = _route_label()
        if endpoint != "/metrics":
            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=endpoint,
                status=str(response.status_code),
            ).inc()
            REQUEST_LATENCY.labels(method=request.method, endpoint=endpoint).observe(
                elapsed
            )
        response.headers.setdefault("X-Request-ID", g.request_id)
        return response

    # Queue depth needs a MongoDB query, which must never run on the scrape
    # path (a database outage would stall every scrape). Refresh it from a
    # background daemon instead; it is skipped in tests.
    if os.getenv("APP_ENV", "").upper() not in {"TEST"}:
        threading.Thread(target=_queue_gauge_loop, daemon=True).start()

    @app.route("/metrics", methods=["GET"])
    def metrics():
        return Response(generate_latest(_registry()), mimetype=CONTENT_TYPE_LATEST)
