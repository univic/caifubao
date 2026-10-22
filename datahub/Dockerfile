# Datahub image.
#
# Build context is the repository root (the image installs the shared contracts
# package):
#
#   docker build -f datahub/Dockerfile -t caifubao-datahub:local .
FROM python:3.12-slim

ARG CAIFUBAO_BUILD_REVISION=""

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY datahub/requirements.txt .
COPY shared /tmp/shared

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir /tmp/shared

COPY datahub /app

ENV PYTHONUNBUFFERED=1 \
    CAIFUBAO_BUILD_REVISION=${CAIFUBAO_BUILD_REVISION}

CMD ["python", "main.py", "--scheduled"]
