# Environment Model — Tasks

## 1. 权威文档

- [x] 1.1 新增 `docs/architecture/environment-model.md`：两轴定义、有效/无效组合、
  运行目标当前/目标对照、数据所有权、制品流、dev 快照导入边界、FQ/HFQ 职责、
  `prod` 未启用声明、技术名称对照、快速问答
- [x] 1.2 实盘门槛与 promotion 细节以引用 `production-capability-roadmap` 为主，
  不建立第二套规范性来源

## 2. 入口文档链接与冲突修正

- [x] 2.1 `README.md`：部署/发布段落链接环境模型；修正 `production` Environment
  建议以匹配 TASK-303 切流后的现状（research 部署使用受保护的 `research`
  Environment）
- [x] 2.2 `docs/README.md`：新增架构与环境入口；修正 capability-inventory 的过期
  状态注记
- [x] 2.3 `k8s/README.md`：链接环境模型，标注 `example-production` 等为示例名
- [x] 2.4 `docs/operations/agent-cli.md`：同步来源改标为旧 stable（迁移期），
  "prod" 字样改为技术名称标注；命令行为不变
- [x] 2.5 `docs/capability-inventory.md`：环境相关条目对齐两轴口径并链接环境模型
- [x] 2.6 `openspec/changes/production-capability-roadmap/tasks.md` 0.4：注明
  "生产环境" 指 trading/production 目标态且当前未启用

## 3. 历史材料注记

- [x] 3.1 `docs/operations/roadmap-2026-08.md` 顶部加历史状态注记
- [x] 3.2 `docs/operations/mongodb-resilience.md` 时间线段落加历史注记
- [x] 3.3 不重写历史事实；`docs/agent-progress.md` 只追加本轮进度

## 4. 验证与评审

- [x] 4.1 `openspec validate --all --strict` 通过
- [x] 4.2 变更文件的相对链接与路径检查通过
- [x] 4.3 本轮新增内容未新增私有部署敏感值（域名、节点名、对象存储、真实地址；
  既有行中的历史提及不在本轮范围）
- [x] 4.4 spec-guardian gate 结论已记录（GATE REQUIRED，独立最小 change）
- [x] 4.5 contract-reviewer / qa-reviewer 结论已记录（按任务要求追加调用）
- [x] 4.6 分支冲突检查通过；Draft PR 已创建
- [x] 4.7 `docs/agent-progress.md` 追加本轮进度（含 PR 链接与剩余漂移）
