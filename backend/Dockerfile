# Backend API image.
#
# Build context is the repository root (the image installs the shared contracts
# package):
#
#   docker build -f backend/Dockerfile -t caifubao-backend:local .
FROM python:3.12-slim AS builder

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY backend/requirements.txt .
COPY shared /tmp/shared

# Install dependencies and the shared contracts package into the user site.
RUN pip install --no-cache-dir --user -r requirements.txt -i https://pypi.org/simple \
    && pip install --no-cache-dir --user /tmp/shared

# 运行阶段 - 使用更小的基础镜像
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/root/.local/bin:$PATH"

WORKDIR /app

COPY --from=builder /root/.local /root/.local

ENV PYTHONPATH="/root/.local/lib/python3.12/site-packages"

COPY backend/app ./app
COPY backend/main.py .
COPY backend/setup.py .

RUN mkdir -p /app/app/log

COPY backend/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

RUN rm -rf /root/.cache/pip \
    && rm -rf /tmp/* \
    && find /root/.local -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true \
    && find /root/.local -type f -name '*.pyc' -delete 2>/dev/null || true \
    && find /root/.local -type d -name '*.egg-info' -exec rm -rf {} + 2>/dev/null || true

EXPOSE 8000

CMD ["./docker-entrypoint.sh"]
