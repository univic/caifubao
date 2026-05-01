# Docker 部署指南

## 构建镜像

在 `backend` 目录下执行：

```bash
docker build -t caifubao-backend:latest .
```

## 运行容器

### 方式1：使用 docker-run.sh 脚本（推荐）

**直接运行（自动加载 .env 文件）**：

```bash
# 1. 创建 .env 文件（如果还没有）
cp .env.docker.example .env

# 2. 编辑 .env 文件，填写配置
vim .env

# 3. 直接运行脚本（会自动加载 .env 文件）
./docker-run.sh
```

**从系统环境变量读取**：

```bash
# 设置环境变量
export MONGODB_HOST=localhost
export MONGODB_NAME=caifubao
export MONGODB_USER=root
export MONGODB_PASS=your_password

# 运行脚本
./docker-run.sh
```

**内联传递环境变量**：

```bash
MONGODB_HOST=localhost MONGODB_NAME=caifubao MONGODB_USER=root MONGODB_PASS=your_password ./docker-run.sh
```

### 方式2：使用 docker run 命令

```bash
docker run -d \
  --name caifubao-backend \
  -p 8000:8000 \
  -e MONGODB_HOST=your_mongodb_host \
  -e MONGODB_PORT=27017 \
  -e MONGODB_NAME=your_database_name \
  -e MONGODB_USER=your_username \
  -e MONGODB_PASS=your_password \
  -e APP_ENV=production \
  -e FLASK_HOST=0.0.0.0 \
  -e FLASK_PORT=8000 \
  -e FLASK_DEBUG=false \
  caifubao-backend:latest
```

## 环境变量说明

| 变量名 | 说明 | 必需 | 默认值 |
|--------|------|------|--------|
| MONGODB_HOST | MongoDB 主机地址 | 是 | - |
| MONGODB_PORT | MongoDB 端口 | 是 | - |
| MONGODB_NAME | 数据库名称 | 是 | - |
| MONGODB_USER | MongoDB 用户名 | 是 | - |
| MONGODB_PASS | MongoDB 密码 | 是 | - |
| APP_ENV | 环境类型 (dev/production) | 否 | DEV |
| FLASK_HOST | Flask 监听地址 | 否 | 0.0.0.0 |
| FLASK_PORT | Flask 监听端口 | 否 | 8000 |
| FLASK_DEBUG | Flask 调试模式 | 否 | False |

## 常用命令

### 查看日志

```bash
docker logs -f caifubao-backend
```

### 停止容器

```bash
docker stop caifubao-backend
```

### 启动已停止的容器

```bash
docker start caifubao-backend
```

### 删除容器

```bash
docker rm caifubao-backend
```

### 进入容器

```bash
docker exec -it caifubao-backend /bin/bash
```

## 镜像信息

- **基础镜像**: python:3.12-slim
- **镜像大小**: ~1GB
- **工作目录**: /app
- **暴露端口**: 8000

## 注意事项

1. **MongoDB 连接**: 确保容器可以访问 MongoDB 服务器
2. **网络配置**: 如果 MongoDB 在其他容器中，使用 Docker 网络
3. **日志持久化**: 建议挂载日志目录到宿主机
4. **数据持久化**: APScheduler 的定时任务已持久化到 MongoDB

### 挂载日志目录

```bash
docker run -d \
  --name caifubao-backend \
  -p 8000:8000 \
  -v /path/to/logs:/app/app/log \
  -e MONGODB_HOST=your_mongodb_host \
  -e MONGODB_PORT=27017 \
  -e MONGODB_NAME=your_database_name \
  -e MONGODB_USER=your_username \
  -e MONGODB_PASS=your_password \
  -e APP_ENV=production \
  -e FLASK_HOST=0.0.0.0 \
  -e FLASK_PORT=8000 \
  -e FLASK_DEBUG=false \
  caifubao-backend:latest
```

### 使用 Docker 网络

```bash
# 创建网络
docker network create caifubao-network

# 启动 MongoDB 容器
docker run -d \
  --name caifubao-mongodb \
  --network caifubao-network \
  -p 27017:27017 \
  mongo:latest

# 启动应用容器
docker run -d \
  --name caifubao-backend \
  --network caifubao-network \
  -p 8000:8000 \
  -e MONGODB_HOST=caifubao-mongodb \
  -e MONGODB_PORT=27017 \
  -e MONGODB_NAME=caifubao \
  -e MONGODB_USER=root \
  -e MONGODB_PASS=your_password \
  -e APP_ENV=production \
  -e FLASK_HOST=0.0.0.0 \
  -e FLASK_PORT=8000 \
  -e FLASK_DEBUG=false \
  caifubao-backend:latest
```

## 故障排查

### 容器无法启动

1. 检查日志：`docker logs caifubao-backend`
2. 检查环境变量是否正确
3. 确认 MongoDB 连接是否正常

### 无法连接 MongoDB

1. 检查 MongoDB 是否运行
2. 确认网络连接
3. 验证用户名和密码

### 定时任务不执行

1. 检查 MongoDB 中的 `apscheduler_jobs` 集合
2. 确认 APScheduler 日志
3. 验证时区设置