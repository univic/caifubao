#!/bin/bash
set -e

# Datahub runs as its own service; the backend container serves the API only.
# Gunicorn (instead of the Werkzeug development server) gives us multiple
# worker processes, graceful shutdown and real request concurrency.
PORT="${FLASK_PORT:-8000}"
WORKERS="${GUNICORN_WORKERS:-2}"
TIMEOUT="${GUNICORN_TIMEOUT:-300}"
GRACEFUL_TIMEOUT="${GUNICORN_GRACEFUL_TIMEOUT:-30}"

echo "Starting API (gunicorn) on 0.0.0.0:${PORT} with ${WORKERS} workers..."

exec gunicorn main:app \
    --bind "0.0.0.0:${PORT}" \
    --workers "${WORKERS}" \
    --timeout "${TIMEOUT}" \
    --graceful-timeout "${GRACEFUL_TIMEOUT}" \
    --access-logfile - \
    --error-logfile -
