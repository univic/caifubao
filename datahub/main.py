#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from app.lib.datahub import Datahub
from app.jobs.quote_catchup import run_startup_quote_catchup
from app.lib.utilities.logger_utils import is_health_log_enabled

# logging.basicConfig() 已移除
# 日志配置由 app/__init__.py 的 setup_logging() 统一处理 (K8S + CLS 兼容)
# main.py 启动时 app 包会被导入，setup_logging() 会先执行

logger = logging.getLogger(__name__)

# Global variable to track health status
_health_status = {"healthy": True, "message": "OK", "last_check": None}
_health_lock = threading.Lock()


def set_health_status(healthy, message="OK"):
    """Thread-safe function to update health status"""
    with _health_lock:
        _health_status["healthy"] = healthy
        _health_status["message"] = message
        _health_status["last_check"] = time.time()
        if not healthy:
            logger.error(f"Health status changed to unhealthy: {message}")


def check_datahub_health(datahub_instance):
    """Check if datahub is healthy by checking scheduler status"""
    try:
        if datahub_instance.scheduler:
            if datahub_instance.scheduler.running:
                set_health_status(True, "Scheduler is running")
            else:
                set_health_status(False, "Scheduler is not running")
        else:
            set_health_status(True, "Datahub initialized")
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
            with _health_lock:
                if _health_status["healthy"]:
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        f'{{"status": "healthy", "message": "{_health_status["message"]}", "last_check": {_health_status["last_check"]}}}'.encode()
                    )
                else:
                    self.send_response(503)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        f'{{"status": "unhealthy", "message": "{_health_status["message"]}", "last_check": {_health_status["last_check"]}}}'.encode()
                    )
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

            if os.getenv("DATAHUB_STARTUP_CATCHUP_ENABLED", "true").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }:
                reap_stale_job_runs()
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
