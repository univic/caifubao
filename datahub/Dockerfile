FROM python:3.12-slim

ARG CAIFUBAO_BUILD_REVISION=""

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    CAIFUBAO_BUILD_REVISION=${CAIFUBAO_BUILD_REVISION}

CMD ["python", "main.py", "--scheduled"]
