# 可观测性（Metrics / 健康检查 / 请求关联）

> 状态：现行。适用 backend（Flask + gunicorn）与 datahub（CronJob 执行器 +
> 常驻 Deployment）。本文描述**已实现**的能力；集群侧的告警路由（钉钉/邮件/
> Prometheus 部署）属于私有仓库或集群运维范围。

## 1. 健康检查

| 服务 | 端点 | 语义 |
|:---|:---|:---|
| backend | `GET /health` | **liveness**：进程能响应 HTTP，不访问 MongoDB |
| backend | `GET /health/ready` | **readiness**：MongoDB 可达（`ConnTestDocument` 查询） |
| datahub | `GET /health` | **liveness**：进程响应；返回 `mode`（`scheduler`/`cronjob`）与 `ready` |
| datahub | `GET /health/ready` | **readiness**：MongoDB 可达；若启用 legacy scheduler，还要求 scheduler 运行 |

K8s readiness probe 使用 `/health/ready`，liveness 使用 `/health`。数据库故障时
pod 退出 Service endpoints 但不被重启（重启无法修复 MongoDB）；进程卡死仍由
liveness 覆盖。

## 2. Metrics（Prometheus）

两个服务都暴露 `GET /metrics`（文本格式，无需鉴权，供集群内抓取）：

| 指标 | 类型 | 标签 | 说明 |
|:---|:---|:---|:---|
| `caifubao_api_requests_total` | counter | `method` / `endpoint` / `status` | API 请求数；`endpoint` 是路由模板（非具体 URL），基数可控 |
| `caifubao_api_request_duration_seconds` | histogram | `method` / `endpoint` | API 延迟 |
| `caifubao_compute_tasks` | gauge | `status` | `compute_tasks` 中 PENDING/RUNNING 数量（后台每 30s 刷新） |
| `caifubao_datahub_ready` | gauge | — | 1 = readiness 通过 |
| `caifubao_datahub_job_last_success_timestamp_seconds` | gauge | `job_name` | 每日链路各 job 最近一次 SUCCESS 的时间戳 |
| `caifubao_datahub_assets` | gauge | `status` | `data_asset_status` 中 STALE / NO_DATA 数量 |
| `caifubao_datahub_quote_latest_data_date_timestamp_seconds` | gauge | — | `data_asset_status` 中最新行情日期 |

K8s 的 backend/datahub Pod 模板已带 `prometheus.io/scrape=true` 注解（端口 8000，
路径 `/metrics`），支持该注解的采集器可直接发现；使用 Prometheus Operator 时在
私有 overlay 中补 `ServiceMonitor`。

**多进程说明**：backend 由 gunicorn 多 worker 运行，`/metrics` 通过
`PROMETHEUS_MULTIPROC_DIR` 聚合各 worker 的 mmap 文件（entrypoint 在 master
启动前清理该目录）。单进程/本地开发不需要该变量。

## 3. 请求关联（轻量 tracing）

- 每个 backend 请求都有 `X-Request-ID`：优先使用入口传入的同名 header，否则生成
  UUID；响应会回写该 header。
- JSON 日志的每条记录带 `request_id` 字段（`RequestIdFilter`），错误响应体也带
  `request_id`。这样可以在 CLS/日志系统里按 id 串起一次请求的日志与错误。
- 未接入 OpenTelemetry/Jaeger；跨服务（backend → Mongo → datahub CronJob）的关联
  依赖 job run 记录（`datahub_job_runs`）与 `request_id` 日志，而不是分布式 span。

## 4. 失败告警

`datahub/app/jobs/health_watcher.py`（CronJob `caifubao-datahub-health-watcher`）
检查近 26h 的 FAILED/DEGRADED job run 与 STALE/NO_DATA 资产，`--fail-on-issues`
让 CronJob 以非零退出码暴露问题；`HEALTH_WEBHOOK_URL` 可接钉钉/邮件 webhook
（私有 overlay 配置真实地址）。

## 5. 已验证的本地命令

```bash
curl -s localhost:8000/health
curl -s localhost:8000/health/ready
curl -s localhost:8000/metrics | head
curl -s -H 'X-Request-ID: demo-1' localhost:8000/api/system/env -i | grep -i x-request-id
```
