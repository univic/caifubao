#!/bin/bash
set -e

# Datahub runs as its own service; the backend container serves the API only.
# Gunicorn (instead of the Werkzeug development server) gives us multiple
# worker processes, graceful shutdown and real request concurrency.
PORT="${FLASK_PORT:-8000}"
WORKERS="${GUNICORN_WORKERS:-2}"
TIMEOUT="${GUNICORN_TIMEOUT:-300}"
GRACEFUL_TIMEOUT="${GUNICORN_GRACEFUL_TIMEOUT:-30}"

# Gunicorn runs several workers; the Prometheus client needs a shared mmap
# directory so /metrics aggregates all workers. Clear it once, before the
# master forks.
if [ -n "${PROMETHEUS_MULTIPROC_DIR:-}" ]; then
    rm -rf "${PROMETHEUS_MULTIPROC_DIR}"
    mkdir -p "${PROMETHEUS_MULTIPROC_DIR}"
fi

echo "Starting API (gunicorn) on 0.0.0.0:${PORT} with ${WORKERS} workers..."

exec gunicorn main:app \
    --bind "0.0.0.0:${PORT}" \
    --workers "${WORKERS}" \
    --timeout "${TIMEOUT}" \
    --graceful-timeout "${GRACEFUL_TIMEOUT}" \
    --access-logfile - \
    --error-logfile -
