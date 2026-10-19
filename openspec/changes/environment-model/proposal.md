# Environment Model — Two-Axis Domain/Stage Vocabulary

## Why

活跃文档对环境的描述仍在漂移：research stable 被误写成实盘 production、dev 的
在线 `data sync` 被写成目标架构、`production`/`prod` 等技术名称被当成业务语义。
单轴 `development/production` 词汇把「软件生命周期成熟度」与「真实资金风险」压在
一个名字里，是误读的根源。私有仓库
（`caifubao-private` `docs/architecture/target-architecture.md`）已经确立
development / data / research / trading 的目标域模型，但公共仓库缺少对应的权威
公共描述，导致 README、`docs/operations/agent-cli.md`、`docs/capability-inventory.md`
等入口各自为政。

本 change 为公共文档建立统一的两轴环境模型（domain × stage）作为**文档语义契约**，
并修正入口文档中的冲突描述。它不改变任何运行时行为。

## What Changes

- 新增权威文档 `docs/architecture/environment-model.md`：domain（development /
  data / research / trading）与 stage（dev / stable / paper / production）两轴定义、
  有效/无效组合、运行目标（`dev`、`research`、`trading-paper`、`prod`）当前态与
  目标态对照、数据所有权、Data → Research → Trading 制品流、dev 快照导入边界、
  FQ/HFQ 外部输入与派生职责、`prod` 未启用声明，以及「技术名称 ≠ 业务语义」对照。
- 从 `README.md`、`docs/README.md`、`k8s/README.md`、
  `docs/operations/agent-cli.md`、`docs/capability-inventory.md` 与
  `openspec/changes/production-capability-roadmap/tasks.md`（0.4 任务注释）链接到
  权威文档，并修正与两轴模型冲突的表述（research stable ≠ 实盘 production；
  dev 在线同步标注为 TASK-404 前的迁移期遗留；`production`/`prod` 技术名称显式
  标注）。
- 在易误读的历史文档（`docs/operations/roadmap-2026-08.md`、
  `docs/operations/mongodb-resilience.md` 时间线）顶部加「历史状态/已被取代」注记，
  不重写历史。
- 新增本 capability spec（`environment-model`），约束**公共文档**必须使用的环境
  词汇与必答问题，不约束运行时。

## Non-goals

- 不修改任何代码、workflow、部署行为、数据库或集群状态。已核实的运行时漂移仅
  **记录**不修复：publish workflow 的 job 级 GitHub Environment 仍按分支解析为
  `development`/`production`（`.github/workflows/backend-publish.yml` 等第 21 行）
  而 dispatch payload 已发送 `dev`/`research`；`main` 构建仍推送 `prod`/`latest`
  channel tag 而部署目标是 research；`caifubao-datahub-data-sync` CronJob 仍在线
  直连旧 stable（TASK-404 前的迁移期遗留）。
- 不重复 `production-capability` capability 的实盘门槛与 promotion 细节
  （≥120 日不可变前瞻证据、默认关闭、kill-switch 等已是
  `openspec/changes/production-capability-roadmap/` 的规范性要求）；权威文档只做
  摘要并引用。
- 不对 `production-capability` 或 `datahub-factors`（`fq-adj-factor-fix`）做任何
  MODIFIED delta：FQ/HFQ 管线的当前实现位置在 datahub 模块仍然有效，目标
  research ownership 以「迁移期实现位置」的形式记录。
- 不重写历史/归档材料，只加注记。

## Impact

- 受影响文件：`docs/architecture/environment-model.md`（新增）、上述 6 个入口文档
  与 2 个历史文档的标注、`openspec/changes/environment-model/`（新增）。
- 读者影响：新读者可从单一权威文档回答 dev/research/trading-paper/prod 职责、
  券商凭证边界、FQ/HFQ 重算位置、dev 数据获取方式与 prod 是否存在。
- 运行时影响：无。
