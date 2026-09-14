# Caifubao 文档索引

本文件是 `docs/` 的入口，说明每份文档的定位与状态（现行 / 历史）。仓库级
治理文档在仓库根目录，不放在 `docs/` 下。

## 治理与规则（仓库根目录）

| 文档 | 作用 |
|:---|:---|
| [`RULES.md`](../RULES.md) | **规则权威**：安全、模块边界、外科纪律、验证、spec-gate |
| [`AGENTS.md`](../AGENTS.md) | Agent 操作指南与开发工作流（角色、评审门禁） |
| [`.project-rules.md`](../.project-rules.md) | Git 工作流与 OpenSpec 操作参考 |
| [`DESIGN.md`](../DESIGN.md) | 架构设计 |
| [`SECURITY.md`](../SECURITY.md) | 安全政策 |
| [`README.md`](../README.md) | 项目总览、环境、运行方式 |

## 操作 Runbook — `docs/operations/`

| 文档 | 作用 |
|:---|:---|
| [`agent-cli.md`](./operations/agent-cli.md) | **统一运维 CLI**（`./scripts/caifubao`）的 canonical 指南（K3s dev） |
| [`cli-reference.md`](./cli-reference.md) | datahub runner CLI 全量参考（仓库根执行，需 datahub 环境） |
| [`data-lake.md`](./operations/data-lake.md) | 研究数据湖 |
| [`factor-lab.md`](./operations/factor-lab.md) | 因子实验室（research-only） |
| [`mongodb-resilience.md`](./operations/mongodb-resilience.md) | MongoDB 备份 / 恢复 / bootstrap |
| [`mongodb-node-migration.md`](./operations/mongodb-node-migration.md) | 单副本 MongoDB 节点迁移 runbook |
| [`service-tokens.md`](./operations/service-tokens.md) | Service token 运维 |
| [`strategy-forward-window.md`](./operations/strategy-forward-window.md) | 前瞻证据窗口 operator runbook |
| [`tailscale-k8s-deploy.md`](./operations/tailscale-k8s-deploy.md) | Tailscale Kubernetes API server 部署路径 |
| [`perf-analysis-2026-08.md`](./operations/perf-analysis-2026-08.md) | 性能 / CPU 热点分析与修复路线（被 `openspec/changes/datahub-perf-optimization/` 引用） |
| [`roadmap-2026-08.md`](./operations/roadmap-2026-08.md) | 稳定性与实盘指导方向（部分待办仍开放） |
| [`strategy-experiments-2026-08.md`](./operations/strategy-experiments-2026-08.md) | 策略实验记录（2026-08） |

## 参考

| 文档 | 作用 |
|:---|:---|
| [`capability-inventory.md`](./capability-inventory.md) | 系统能力清单。**最后更新 2026-05-22，已过期**；刷新任务见 `openspec/changes/production-capability-roadmap/tasks.md` 0.5 |
| [`technical-factors.md`](./technical-factors.md) | 技术因子参考 |
| [`integrations/openclaw.md`](./integrations/openclaw.md) | OpenClaw 集成指南 |

## 研究记录 — `docs/autoresearch/`（research-only，历史记录）

- `plans/` — 研究计划
- `runs/` — 研究运行报告，按日期归档
- `specs/` — 研究设计

这些是**研究证据**，不是当前系统描述；引用时务必带上其数据快照与日期。

## 共享进度

- [`agent-progress.md`](./agent-progress.md) — 跨会话共享进度日志（append-only）

## 已过期文档

- [`archive/`](./archive/README.md) — 已被取代的文档，清单与替代关系见 archive 的 README

## 测试布局

清理审计未发现冗余测试（无孤儿 import、无 skip/xfail、无空用例）。测试位置：

| 位置 | 内容 | 运行 |
|:---|:---|:---|
| `backend/app/test/` | Flask API / 服务 / 模型测试 | `pytest backend/app/test/` |
| `datahub/app/test/` | 数据 / 评分 / 策略引擎测试 | `datahub/.venv/bin/python -m pytest datahub/app/test/` |
| `frontend/src/**/__tests__/` | 前端单元测试 | `cd frontend && npm run test`（配置见 `frontend/vitest.config.ts`） |
