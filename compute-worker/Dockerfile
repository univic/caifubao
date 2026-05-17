FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY compute-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared model layer
COPY backend/app/model /app/backend/app/model/
COPY datahub/app/model /app/datahub/app/model/
COPY datahub/app/lib /app/datahub/app/lib/

# Copy worker code
COPY compute-worker/app /app/app/

# Copy backend services needed by handlers
COPY backend/app/services /app/backend/app/services/

CMD ["python", "-m", "app.worker"]
