FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY compute-worker/app /app/worker_app/
COPY compute-worker/requirements.txt /app/

# Copy shared model layer
COPY backend/app/model /app/backend/app/model/
COPY datahub/app/model /app/datahub/app/model/
COPY datahub/app/lib /app/datahub/app/lib/

# Copy worker code
COPY compute-worker/worker_app /app/worker_app/

# Copy backend services needed by handlers
COPY backend/app/services /app/backend/app/services/

CMD ["python", "-m", "worker_app.worker"]
