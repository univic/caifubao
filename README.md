# Caifubao — 大A量化分析平台

大A量化分析系统后端服务

## 项目结构

```
caifubao/
├── backend/                 # 后端 API 服务
│   ├── app/                 # 应用主目录
│   │   ├── api/           # API 接口
│   │   ├── conf/          # 配置文件
│   │   ├── lib/           # 核心库
│   │   │   ├── db_watcher/    # 数据库监控
│   │   │   ├── task_controller/ # 任务控制器
│   │   │   ├── messenger/     # 消息通知
│   │   │   └── ...
│   │   ├── model/         # 数据模型
│   │   ├── utilities/     # 工具函数
│   │   └── ...
│   ├── Dockerfile          # Docker 镜像构建
│   ├── docker-entrypoint.sh # Docker 容器启动脚本
│   ├── dev-entrypoint.sh   # 本地开发启动脚本
│   ├── main.py           # 应用入口
│   └── requirements.txt   # Python 依赖
├── datahub/                # 数据获取服务（独立微服务）
│   ├── app/                 # 应用主目录
│   │   ├── conf/          # 配置文件
│   │   ├── lib/           # 核心库
│   │   │   ├── datahub/      # 数据获取模块
│   │   │   ├── data_source/  # 数据源接口
│   │   │   ├── processors/   # 数据处理器
│   │   │   ├── task_controller/ # 任务控制器
│   │   │   ├── periodic_task_dispatcher/ # 定时任务调度
│   │   │   └── utilities/     # 工具函数
│   │   └── model/         # 数据模型
│   ├── Dockerfile          # Docker 镜像构建
│   ├── main.py           # 应用入口
│   └── requirements.txt   # Python 依赖
├── frontend/               # 前端应用
├── k8s/                    # Kubernetes 部署配置
│   ├── base/              # 基础配置
│   ├── overlays/          # 环境特定配置
│   │   ├── development/   # 开发环境
│   │   └── production/    # 生产环境
│   └── deploy.sh          # 部署脚本
└── README.md            # 项目说明
```

## 环境要求

- Python 3.12+
- MongoDB 4.0+
- Redis (可选，用于缓存)

## 配置说明

### 环境变量

创建 `.env` 文件在 `backend/` 目录下：

```bash
# MongoDB 配置
MONGODB_NAME=caifubao
MONGODB_HOST=localhost
MONGODB_PORT=27017
MONGODB_USER=root
MONGODB_PASS=your_password

# Flask 配置
FLASK_HOST=0.0.0.0
FLASK_PORT=8000
FLASK_DEBUG=False

# 应用环境
APP_ENV=DEV  # DEV 或 PRODUCTION

# 邮件配置（可选）
SMTP_SENDER_EMAIL=your_email@example.com
SMTP_SENDER_PASSWORD=your_password

# Bark 通知（可选）
BARK_URL=https://your.bark.url
```

### 配置文件

配置文件位于 `app/conf/` 目录：

- `__init__.py` - 基础配置
- `dev_config.py` - 开发环境配置
- `production_config.py` - 生产环境配置

## 运行方式

### 快速开发反馈流

推荐优先使用仓库根目录的 `Makefile`，这样可以减少“改代码-启动服务-看效果”的手动步骤。

#### 1. 准备本地配置

```bash
cp .env.example .env.local
```

`.env.local` 已被 `.gitignore` 忽略，可以放本机私有配置。启动时会从这个文件读取 MongoDB、Flask 和前端 mock 配置。

根目录 `.env.example` 是推荐的本地开发主模板。`backend/.env.example` 和 `datahub/.env.example` 仅用于单独进入对应服务目录运行时参考；日常整仓开发优先使用根目录 `.env.local`。前端 Vite 已配置为读取仓库根目录 env，因此不需要维护 `frontend/.env.local`。

#### 2. 纯前端快速看评分实验页

如果只想调整和确认评分实验页面，不需要启动后端或连接 MongoDB。把 `.env.local` 里的开关改成：

```bash
VITE_USE_MOCK_API=true
```

然后运行：

```bash
cd frontend
npm run dev
```

访问：

```text
http://127.0.0.1:5173/score-experiments
```

这会使用前端内置 mock `ScoreExperiment` 数据，适合快速反馈 UI、布局、字段展示和交互文案。

#### 3. 本地 API + 演示数据

如果要验证真实 backend API 和前端联调，先确认 `.env.local` 指向本地 MongoDB，然后写入一组稳定的评分实验演示数据：

```bash
make seed-score-demo
make dev
```

`make seed-score-demo` 会生成：

- `score_demo_candidate`
- `score_demo_baseline`
- 多个 `StockScorePrediction`
- 一个 `Demo Score Experiment`

`make dev` 会同时启动 backend 和 frontend，并打印当前连接的 MongoDB：

```text
Mongo=<host>:<port>/<db>
Frontend mock API=<true|false>
```

#### 4. 线上 dev 库联调

如需连接线上 dev MongoDB，不要把真实配置提交到仓库。只修改本机 `.env.local`：

```bash
MONGODB_HOST=<dev-mongo-host>
MONGODB_PORT=27017
MONGODB_NAME=<dev-db-name>
MONGODB_USER=<dev-user>
MONGODB_PASS=<dev-password>
VITE_USE_MOCK_API=false
```

连接线上 dev 库时，评分实验建议使用独立 `model_version`，例如：

```text
score_exp_20260502_v1
```

避免覆盖现有 `score_v2_202604` 的预测结果。

#### 5. 常用检查命令

```bash
make check
make test-backend
make test-frontend
```

当前 `make check` 会跑评分实验相关后端测试，以及前端 lint/build。

### 本地开发

#### 1. 创建虚拟环境

```bash
cd backend
python3 -m venv venv312
```

#### 2. 安装依赖

```bash
cd backend
source venv312/bin/activate
pip install -r requirements.txt
```

#### 3. 配置环境变量

复制 `.env.example` 到 `.env` 并修改配置：

```bash
cp .env.example .env
# 编辑 .env 文件
```

#### 4. 启动应用

```bash
cd backend
./dev-entrypoint.sh
```

这将启动：
- datahub 定时任务（每天下午 6 点，交易日执行）
- Flask Web 应用（监听 0.0.0.0:8000）

## API 接口说明

### Datahub 状态概览

`GET /api/datahub/status`

用途：
- 快速查看指数和个股的最新行情日期、freshness 日期和完成情况
- 适合运维排障、CLS/告警联动、人工巡检

请求参数：
- 无

请求示例：

```bash
curl http://localhost:8000/api/datahub/status
```

响应字段说明：

- `generated_at`：接口生成时间
- `index`：指数行情概览
  - `total_count`：当前系统中的指数标的数量
  - `quote_records_count`：指数行情记录数
  - `latest_quote_date`：指数最新行情日期
  - `freshness_records_count`：指数 freshness 记录数
  - `latest_freshness_date`：指数最新 freshness 日期
  - `up_to_date_count`：与最新行情日期一致的 freshness 数量
  - `is_up_to_date`：指数是否整体已更新到最新
- `stock`：个股行情概览
  - 字段含义同 `index`

响应示例：

```json
{
  "generated_at": "2026-04-02T00:00:00",
  "index": {
    "total_count": 562,
    "quote_records_count": 562,
    "latest_quote_date": "2026-04-02T00:00:00",
    "freshness_records_count": 562,
    "latest_freshness_date": "2026-04-02T00:00:00",
    "up_to_date_count": 562,
    "is_up_to_date": true
  },
  "stock": {
    "total_count": 5827,
    "quote_records_count": 915,
    "latest_quote_date": "2026-04-01T00:00:00",
    "freshness_records_count": 915,
    "latest_freshness_date": "2026-04-01T00:00:00",
    "up_to_date_count": 915,
    "is_up_to_date": false
  }
}
```

### Docker 容器

#### 1. 构建镜像

```bash
# Backend
cd backend
docker build -t caifubao-backend .

# Datahub
cd datahub
docker build -t caifubao-datahub .
```

#### 2. 运行容器

```bash
# Backend
docker run -p 8000:8000 \
  -e MONGODB_HOST=mongodb \
  -e MONGODB_PORT=27017 \
  -e MONGODB_NAME=caifubao \
  -e MONGODB_USER=root \
  -e MONGODB_PASS=your_password \
  -e APP_ENV=PRODUCTION \
  caifubao-backend

# Datahub
docker run \
  -e MONGODB_HOST=mongodb \
  -e MONGODB_PORT=27017 \
  -e MONGODB_NAME=caifubao \
  -e MONGODB_USER=root \
  -e MONGODB_PASS=your_password \
  -e APP_ENV=PRODUCTION \
  caifubao-datahub
```

#### 3. Kubernetes 示例部署

公开仓库只保留脱敏后的 Kubernetes 示例。真实环境 overlay、私有 registry
配置、域名和运维脚本应保存在私有仓库或本地 `caifubao-private/` 中。

渲染示例配置：

```bash
kubectl kustomize k8s/overlays/example-development
kubectl kustomize k8s/overlays/example-production
```

应用到真实集群前，必须替换所有 `change-me-*`、`registry.example.com`
和 `example.com` 占位值。更多说明见 `k8s/README.md`。

如果需要在公开仓库合并后自动触发私有部署 overlay，请在 public GitHub
repository 中配置：

- `PRIVATE_REPO_DISPATCH_TOKEN`: 有权向 private overlay repo 发送
  `repository_dispatch` 的 token
- `CONTAINER_REGISTRY_HOST`
- `CONTAINER_REGISTRY_NAMESPACE`
- `CONTAINER_REGISTRY_USERNAME`
- `CONTAINER_REGISTRY_PASSWORD`

公开仓库中的：

- `.github/workflows/backend-publish.yml`
- `.github/workflows/frontend-publish.yml`
- `.github/workflows/datahub-publish.yml`

会在 `develop` / `main` 分支构建并推送镜像。

`.github/workflows/deploy-dispatch.yml` 会在 `develop` 分支镜像发布成功后向
private `caifubao-private` 发送 development 部署事件，也会在 `main`
分支镜像发布成功后发送 production 部署事件。建议在 private repo 的
`production` environment 上启用 required reviewers 或手动 approval，
把自动 dispatch 和生产准入控制分开。

## 启动脚本说明

### dev-entrypoint.sh（本地开发）

- **用途**：本地开发环境启动
- **Python 版本**：使用 venv312 (Python 3.12.12)
- **特点**：
  - 强制要求虚拟环境存在
  - 自动激活虚拟环境
  - 如果虚拟环境不存在，给出明确的错误提示

### docker-entrypoint.sh（Docker 容器）

- **用途**：Docker 容器内启动
- **Python 版本**：使用系统 Python (3.12)
- **特点**：
  - 不需要虚拟环境
  - 同时启动 datahub 和 Flask 应用
  - 优雅关闭处理

## 应用架构

### 微服务架构

项目采用微服务架构，将系统拆分为以下独立服务：

1. **Backend API 服务** (`backend/`)
   - Flask REST API
   - 用户认证和授权
   - 数据查询接口
   - 业务逻辑处理

2. **Datahub 数据服务** (`datahub/`)
   - 定时获取股票行情数据
   - 支持多个数据源（akshare、baostock）
   - 数据完整性检查
   - 独立部署和扩展

3. **Frontend 前端应用** (`frontend/`)
   - Vue 3 + TypeScript
   - 用户界面
   - 数据可视化

4. **MongoDB 数据库**
   - 存储行情数据
   - 用户数据
   - 系统配置

### 服务通信

```
┌─────────────────────────────────────────────────────────────┐
│                         前端应用                           │
│                      (Vue 3 + Nginx)                       │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ HTTP/HTTPS
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Traefik Ingress                       │
│                      (K8S 集群入口)                         │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌──────────────────────┐    ┌──────────────────────┐
│   Backend API 服务    │    │   Frontend 服务       │
│   (Flask + Gunicorn)  │    │   (Nginx + Vue)      │
└──────────┬───────────┘    └──────────────────────┘
           │
           │ MongoDB
           ▼
┌──────────────────────┐
│     MongoDB          │
│   (数据存储)         │
└──────────────────────┘
           ▲
           │
┌──────────┴───────────┐
│   Datahub 数据服务   │
│  (定时数据更新)      │
└──────────────────────┘
```

### 核心模块

1. **datahub** - 数据获取模块
   - 定时获取股票行情数据
   - 支持多个数据源（akshare、baostock）
   - 数据完整性检查

2. **web_server** - Web 服务
   - Flask REST API
   - 用户认证和授权
   - 数据查询接口

3. **task_controller** - 任务控制器
   - 异步任务执行
   - 任务队列管理
   - 任务状态跟踪

4. **db_watcher** - 数据库监控
   - MongoDB 连接管理
   - 连接池管理
   - 自动重连机制

### 数据流

```
数据源 (akshare/baostock)
    ↓
datahub (定时任务)
    ↓
MongoDB (行情数据)
    ↓
web_server (API)
    ↓
前端应用
```

## 开发指南

### 代码规范

- 遵循 PEP 8 编码规范
- 使用类型注解
- 编写单元测试
- 添加文档注释

### 提交规范

```bash
git add .
git commit -m "feat: 添加新功能"
```

提交类型：
- `feat:` 新功能
- `fix:` 修复 bug
- `docs:` 文档更新
- `style:` 代码格式调整
- `refactor:` 重构
- `test:` 测试相关
- `chore:` 构建/工具相关

## 故障排查

### 常见问题

1. **虚拟环境不存在**
   ```
   ERROR: Virtual environment not found at /path/to/venv312
   ```
   解决：创建虚拟环境
   ```bash
   python3 -m venv /path/to/venv312
   ```

2. **MongoDB 连接失败**
   ```
   CRITICAL ERROR: Failed to establish MongoDB connection
   ```
   解决：检查 `.env` 中的 MongoDB 配置

3. **端口被占用**
   ```
   Address already in use
   ```
   解决：修改 `FLASK_PORT` 或停止占用端口的进程

## 许可证

MIT License

## 联系方式

- 项目地址：[GitHub](https://github.com/univic/caifubao)
- 问题反馈：提交 Issue
