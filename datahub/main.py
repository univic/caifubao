#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import logging
import os
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from app.lib.datahub import Datahub
from app.jobs.quote_catchup import run_startup_quote_catchup
from app.lib.utilities.logger_utils import is_health_log_enabled

# logging.basicConfig() 已移除
# 日志配置由 app/__init__.py 的 setup_logging() 统一处理 (K8S + CLS 兼容)
# main.py 启动时 app 包会被导入，setup_logging() 会先执行

logger = logging.getLogger(__name__)

# Global variable to track health status. `healthy` is liveness (the process and
# its scheduler thread are alive); `ready` additionally requires MongoDB.
_health_status = {
    "healthy": True,
    "ready": True,
    "message": "OK",
    "mode": "unknown",
    "last_check": None,
}
_health_lock = threading.Lock()


def set_health_status(healthy, message="OK"):
    """Thread-safe function to update health status"""
    with _health_lock:
        _health_status["healthy"] = healthy
        # An unhealthy process is never ready.
        _health_status["ready"] = bool(healthy) and _health_status.get("ready", False)
        _health_status["message"] = message
        _health_status["last_check"] = time.time()
        if not healthy:
            logger.error(f"Health status changed to unhealthy: {message}")


def _mongo_reachable() -> bool:
    """Best-effort MongoDB ping used by the readiness check."""
    try:
        from app.lib.db_watcher.mongoengine_tool import ConnTestDocument, mongo_watcher

        if mongo_watcher.db_conn is None:
            mongo_watcher.get_db_connection()
        ConnTestDocument.objects.first()
        return True
    except SystemExit:
        # `get_db_connection` gives up with exit(1); inside the health thread
        # that would silently kill the checker, so treat it as not ready.
        logger.warning("Readiness check: MongoDB connection gave up")
        return False
    except Exception:
        logger.warning("Readiness check: MongoDB is not reachable", exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Prometheus metrics (scraped from the same HTTP server as /health)
# ---------------------------------------------------------------------------

SERVICE_READY = Gauge(
    "caifubao_datahub_ready", "1 when the datahub service is ready (Mongo reachable)"
)
JOB_LAST_SUCCESS = Gauge(
    "caifubao_datahub_job_last_success_timestamp_seconds",
    "Unix timestamp of the most recent SUCCESS run per watched job",
    ["job_name"],
)
ASSETS_BY_STATUS = Gauge(
    "caifubao_datahub_assets",
    "Data asset status counts (quote/factor/signal freshness)",
    ["status"],
)
QUOTE_LATEST_DATA_DATE = Gauge(
    "caifubao_datahub_quote_latest_data_date_timestamp_seconds",
    "Unix timestamp of the newest quote date recorded in data_asset_status",
)


def refresh_metrics() -> None:
    """Refresh DB-backed gauges; never raises (a scrape must not fail)."""
    with _health_lock:
        SERVICE_READY.set(1 if _health_status["ready"] else 0)
    try:
        from app.jobs.health_watcher import WATCHED_JOB_NAMES
        from app.model.data_asset_status import (
            STATUS_NO_DATA,
            STATUS_STALE,
            DataAssetStatus,
        )
        from app.model.datahub_job_run import DatahubJobRun

        for job_name in WATCHED_JOB_NAMES:
            record = (
                DatahubJobRun.objects(job_name=job_name, status="SUCCESS")
                .order_by("-completed_at")
                .only("completed_at")
                .first()
            )
            if record and record.completed_at is not None:
                JOB_LAST_SUCCESS.labels(job_name=job_name).set(
                    record.completed_at.replace(
                        tzinfo=datetime.timezone.utc
                    ).timestamp()
                )
            else:
                JOB_LAST_SUCCESS.labels(job_name=job_name).set(0)

        ASSETS_BY_STATUS.labels(status=STATUS_STALE).set(
            DataAssetStatus.objects(status=STATUS_STALE).count()
        )
        ASSETS_BY_STATUS.labels(status=STATUS_NO_DATA).set(
            DataAssetStatus.objects(status=STATUS_NO_DATA).count()
        )

        latest = (
            DataAssetStatus.objects(asset_type="quote", asset_name="daily_quote")
            .order_by("-latest_data_date")
            .only("latest_data_date")
            .first()
        )
        if latest and latest.latest_data_date is not None:
            QUOTE_LATEST_DATA_DATE.set(
                latest.latest_data_date.replace(
                    tzinfo=datetime.timezone.utc
                ).timestamp()
            )
    except Exception:
        logger.debug("Metrics refresh skipped", exc_info=True)


def check_datahub_health(datahub_instance):
    """Update liveness/readiness state.

    Liveness stays true while this process answers requests. Readiness requires
    MongoDB and, when the legacy scheduler is enabled, a running scheduler. In
    cronjob mode the Deployment does not run the pipeline itself, so scheduler
    state must not be mistaken for pipeline health.
    """
    try:
        if datahub_instance.scheduler:
            mode = "scheduler"
            healthy = bool(datahub_instance.scheduler.running)
            message = "Scheduler is running" if healthy else "Scheduler is not running"
        else:
            mode = "cronjob"
            healthy = True
            message = "CronJob mode (scheduler disabled)"

        ready = healthy and _mongo_reachable()
        with _health_lock:
            _health_status["healthy"] = healthy
            _health_status["ready"] = ready
            _health_status["mode"] = mode
            _health_status["message"] = message
            _health_status["last_check"] = time.time()
        if not healthy:
            logger.error("Health status changed to unhealthy: %s", message)
    except Exception as e:
        set_health_status(False, f"Health check failed: {str(e)}")


class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks"""

    def log_message(self, format, *args):
        if is_health_log_enabled():
            logger.info(
                "Health check request: %s - %s",
                self.address_string(),
                format % args,
            )

    def do_GET(self):
        if self.path == "/health":
            # Liveness: the process answers; do not fail on a database outage
            # (restarting the pod cannot fix MongoDB).
            with _health_lock:
                body = {
                    "status": "healthy",
                    "message": _health_status["message"],
                    "mode": _health_status["mode"],
                    "ready": _health_status["ready"],
                    "last_check": _health_status["last_check"],
                }
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(payload)
        elif self.path == "/metrics":
            # Gauges are refreshed by the background health worker; the scrape
            # path itself must never wait on MongoDB.
            payload = generate_latest()
            self.send_response(200)
            self.send_header("Content-type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(payload)
        elif self.path == "/health/ready":
            with _health_lock:
                ready = bool(_health_status["ready"])
                body = {
                    "status": "ready" if ready else "unavailable",
                    "message": _health_status["message"],
                    "mode": _health_status["mode"],
                    "last_check": _health_status["last_check"],
                }
            payload = json.dumps(body).encode()
            self.send_response(200 if ready else 503)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(404)
            self.end_headers()


def start_health_server(port=8000):
    """Start a simple HTTP server for health checks"""
    try:
        server = HTTPServer(("0.0.0.0", port), HealthHandler)
        logger.info(f"Health check server started on port {port}")

        # Run server in a separate thread
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        return server
    except Exception as e:
        logger.error(
            f"Failed to start health server on port {port}: {e}", exc_info=True
        )
        return None


def health_check_worker(datahub_instance, interval=30):
    """Background worker to periodically check datahub health"""
    logger.info(f"Health check worker started, checking every {interval} seconds")
    while True:
        try:
            check_datahub_health(datahub_instance)
            refresh_metrics()
        except Exception as e:
            logger.error(f"Health check worker error: {e}", exc_info=True)
        time.sleep(interval)


def reap_stale_job_runs():
    """Mark orphan RUNNING job_run records as FAILED before catch-up starts.

    Records left RUNNING by a dead process would otherwise linger until
    manual cleanup and could confuse has_active_job_run checks.
    """
    try:
        from app.lib.db_watcher.mongoengine_tool import mongo_watcher
        from app.lib.utilities import job_run_helper

        mongo_watcher.get_db_connection()
        job_run_helper.mark_stale_running_job_runs_failed()
    except Exception:
        logger.exception("Stale RUNNING job-run cleanup failed; continuing")


def startup_quote_catchup_worker():
    """Run a one-time quote catch-up when the service boots behind schedule."""
    try:
        result = run_startup_quote_catchup()
        logger.info(
            "Startup quote catch-up completed: status=%s reason=%s",
            result.get("status"),
            result.get("reason"),
        )
    except Exception as e:
        logger.error(f"Startup quote catch-up failed: {e}", exc_info=True)


def main():
    logger.info("=" * 60)
    logger.info("Starting Datahub service")
    logger.info("=" * 60)

    # Start health check server
    health_server = start_health_server(8000)
    if not health_server:
        logger.error(
            "Health check server failed to start, service may not be properly monitored"
        )
        set_health_status(False, "Health check server failed to start")

    try:
        datahub_instance = Datahub()

        if len(sys.argv) > 1 and sys.argv[1] == "--scheduled":
            logger.info("Starting scheduled datahub service")
            datahub_instance.start_scheduled()

            # Start health check worker
            health_worker = threading.Thread(
                target=health_check_worker, args=(datahub_instance, 30), daemon=True
            )
            health_worker.start()
            logger.info("Health check worker started")

            # Reap orphan RUNNING job runs regardless of the catch-up flag:
            # cron pods rely on this service-side pass too, and it must run
            # before the catch-up check so stale records cannot skew
            # has_active_job_run or block the uniqueness claim.
            reap_stale_job_runs()

            if os.getenv("DATAHUB_STARTUP_CATCHUP_ENABLED", "true").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }:
                catchup_worker = threading.Thread(
                    target=startup_quote_catchup_worker,
                    daemon=True,
                )
                catchup_worker.start()
                logger.info("Startup quote catch-up worker started")

            try:
                while True:
                    time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                logger.info("Shutting down scheduled datahub service")
                datahub_instance.stop_scheduled()
                set_health_status(False, "Service is shutting down")
                if health_server:
                    health_server.shutdown()
        else:
            logger.info("Running datahub once")
            datahub_instance.start()
            logger.info("Datahub execution completed")
            set_health_status(False, "Execution completed")
            if health_server:
                health_server.shutdown()

    except Exception as e:
        logger.error(f"Datahub service failed: {e}", exc_info=True)
        set_health_status(False, f"Service failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
