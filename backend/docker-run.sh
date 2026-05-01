#!/bin/bash

echo "Starting caifubao-backend container..."

# 自动加载 .env 文件（如果存在）
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    set -a
    source .env
    set +a
fi

# 从系统环境变量读取配置，如果不存在则使用默认值
MONGODB_HOST=${MONGODB_HOST:-""}
MONGODB_PORT=${MONGODB_PORT:-"27017"}
MONGODB_NAME=${MONGODB_NAME:-""}
MONGODB_USER=${MONGODB_USER:-""}
MONGODB_PASS=${MONGODB_PASS:-""}
APP_ENV=${APP_ENV:-"production"}
FLASK_HOST=${FLASK_HOST:-"0.0.0.0"}
FLASK_PORT=${FLASK_PORT:-"8000"}
FLASK_DEBUG=${FLASK_DEBUG:-"false"}

# 检查必需的环境变量
if [ -z "$MONGODB_HOST" ] || \
   [ -z "$MONGODB_NAME" ] || \
   [ -z "$MONGODB_USER" ] || \
   [ -z "$MONGODB_PASS" ]; then
    echo "ERROR: Please set required environment variables:"
    echo "  MONGODB_HOST"
    echo "  MONGODB_NAME"
    echo "  MONGODB_USER"
    echo "  MONGODB_PASS"
    echo ""
    echo "You can set them in one of the following ways:"
    echo ""
    echo "1. Create a .env file:"
    echo "   cp .env.docker.example .env"
    echo "   vim .env"
    echo "   ./docker-run.sh"
    echo ""
    echo "2. Set environment variables in your shell:"
    echo "   export MONGODB_HOST=localhost"
    echo "   export MONGODB_NAME=caifubao"
    echo "   export MONGODB_USER=root"
    echo "   export MONGODB_PASS=your_password"
    echo "   ./docker-run.sh"
    echo ""
    echo "3. Pass them inline:"
    echo "   MONGODB_HOST=localhost MONGODB_NAME=caifubao MONGODB_USER=root MONGODB_PASS=your_password ./docker-run.sh"
    exit 1
fi

# 启动容器
docker run -d \
  --name caifubao-backend \
  -p ${FLASK_PORT}:8000 \
  -e MONGODB_HOST=${MONGODB_HOST} \
  -e MONGODB_PORT=${MONGODB_PORT} \
  -e MONGODB_NAME=${MONGODB_NAME} \
  -e MONGODB_USER=${MONGODB_USER} \
  -e MONGODB_PASS=${MONGODB_PASS} \
  -e APP_ENV=${APP_ENV} \
  -e FLASK_HOST=${FLASK_HOST} \
  -e FLASK_PORT=${FLASK_PORT} \
  -e FLASK_DEBUG=${FLASK_DEBUG} \
  caifubao-backend:latest

echo "Container started with following configuration:"
echo "  MONGODB_HOST: ${MONGODB_HOST}"
echo "  MONGODB_PORT: ${MONGODB_PORT}"
echo "  MONGODB_NAME: ${MONGODB_NAME}"
echo "  MONGODB_USER: ${MONGODB_USER}"
echo "  APP_ENV: ${APP_ENV}"
echo "  FLASK_HOST: ${FLASK_HOST}"
echo "  FLASK_PORT: ${FLASK_PORT}"
echo "  FLASK_DEBUG: ${FLASK_DEBUG}"
echo ""
echo "Check logs with: docker logs -f caifubao-backend"
echo "Stop container with: docker stop caifubao-backend"
echo "Remove container with: docker rm caifubao-backend"