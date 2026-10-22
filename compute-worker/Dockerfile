# Compute-worker image.
#
# Build context is the repository root (the worker imports both the backend
# services and the datahub research libraries):
#
#   docker build -f compute-worker/Dockerfile -t caifubao/compute-worker:local .
#
# The image bakes a *merged* `app` package at /app/app: backend code first, then
# the datahub model/lib overlay, then the backend db_watcher restored on top.
# worker_app.worker also assembles the same layout from a bare checkout.
FROM python:3.12-slim

WORKDIR /app

# Build dependencies for the Python wheels (numpy/pandas/numba/akshare).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /tmp/backend-requirements.txt
COPY datahub/requirements.txt /tmp/datahub-requirements.txt
RUN pip install --no-cache-dir \
    -r /tmp/backend-requirements.txt \
    -r /tmp/datahub-requirements.txt

# Worker code (imported as `worker_app`).
COPY compute-worker/worker_app /app/worker_app
COPY compute-worker/requirements.txt /app/worker_app/requirements.txt

# Merged `app` package: backend application code, then the datahub model and
# research libraries it depends on, then the backend connection layer.
COPY backend/app /app/app
COPY datahub/app/model /app/app/model
COPY datahub/app/lib /app/app/lib
COPY backend/app/lib/db_watcher /app/app/lib/db_watcher

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

CMD ["python", "-m", "worker_app.worker"]
