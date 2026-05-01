# 多阶段构建 - 构建阶段
FROM python:3.12-slim AS builder

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装依赖到临时目录
RUN pip install --no-cache-dir --user -r requirements.txt -i https://pypi.org/simple

# 运行阶段 - 使用更小的基础镜像
FROM python:3.12-slim

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/root/.local/bin:$PATH"

# 设置工作目录
WORKDIR /app

# 从构建阶段复制已安装的包
COPY --from=builder /root/.local /root/.local

# 设置 Python 路径以包含用户站点包
ENV PYTHONPATH="/root/.local/lib/python3.12/site-packages"

# 复制项目源码（排除不必要的文件）
COPY app ./app
COPY main.py .
COPY setup.py .

# 创建必要的目录
RUN mkdir -p /app/app/log

# 复制并设置启动脚本权限
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

# 清理不必要的文件（保留 dist-info 以支持包元数据）
RUN rm -rf /root/.cache/pip \
    && rm -rf /tmp/* \
    && find /root/.local -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true \
    && find /root/.local -type f -name '*.pyc' -delete 2>/dev/null || true \
    && find /root/.local -type d -name '*.egg-info' -exec rm -rf {} + 2>/dev/null || true

# 声明容器运行时监听的端口
EXPOSE 8000

# 容器启动命令
CMD ["./docker-entrypoint.sh"]