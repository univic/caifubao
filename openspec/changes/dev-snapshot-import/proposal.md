# Dev Snapshot Import — 受控快照导入替代在线直连（TASK-404 切片 1 契约）

## Why

dev 是最后一个硬性依赖旧 stable（retired production）在线 MongoDB 的环境：
`data_sync_runner`/`sync_engine.py` 经 `MONGODB_SRC_*` 在线直连读取 6 个
collection（`stock_daily_quote`、`stock_daily_basic`、`stock_factor_daily`、
`stock_signal_daily`（dev_only）、`finance_market`、`stock_industry`），由
CronJob `caifubao-datahub-data-sync`（工作日 19:15）驱动。这是 P0-5 的核心
所有权缺口，也是退役旧 stable 的前置条件（TASK-404）。

`environment-model`（`openspec/changes/environment-model/`）已确立目标态边界：
dev 数据可丢弃/抽样/受控快照导入，目标态不得直接依赖 research/trading 在线库。
本 change 把该边界落成**可实现的导入契约**（allow-list、manifest+checksum、
fail-closed、幂等、freshness 传播、CLI 过渡），并给出分阶段任务；writer 切换的
操作设计见私有仓库 `docs/architecture/data-authority-cutover.md`。

## What Changes

- 新增 capability spec `dev-snapshot-import`（全部为 ADDED requirements，针对
  slice 3 将落地的导入路径）：
  1. **封闭 allow-list**：导入器只接受 6 个既有同步面 collection + 一个取代
     `data_sync_state` 的 dev 导入状态元数据 collection，其余一律拒绝；
  2. **版本化 manifest**：逐 collection 文件、sha256、文档计数、`data_as_of`
     水位、producer 镜像 SHA；导入前先校验后应用，并在 dev 留下可追溯记录；
  3. **fail-closed**：manifest/校验和/计数不一致即中止，不得部分应用；
  4. **幂等**：日期分区 collection 走业务键 upsert，快照型 collection 走显式
     `--drop` 语义，重复执行结果一致；
  5. **freshness 传播**：`data_as_of` 随导入写入 dev 的 freshness/status 元数据；
  6. **CLI/配置过渡（条件性 SHALL）**：导入路径激活并验证后，`data sync` 语义
     切换为快照导入且不再建立在线 DB 连接；`MONGODB_SRC_*` 与在线同步路径
     deprecated 后移除；文档同步更新。
- `tasks.md` 记录三个切片：切片 1（本设计）、切片 2（writer 切换执行，仅作为
  需逐项批准的门控执行步骤）、切片 3（导入实现与在线路径退役）。

## Non-goals

- **不授权任何集群变更**：本 change 的任何 requirement 都不构成对 CronJob
  挂起/恢复、Secret、restore 执行或命名空间操作的授权；这些属私有
  `docs/architecture/data-authority-cutover.md` 的逐项批准执行清单（与
  migration-contract §P0-5「创建目标库/用户不等于切换任务」同口径）。
- **不含 writer 切换的规范性要求**（顺序、与旧 stable 的比对指标、验证窗口）：
  切片 2 是一次性迁移操作，写入私有设计文档与任务清单，公开 spec 不预声明
  尚未发生的迁移。
- 不在本 change 内复述环境词汇；沿用
  [`environment-model`](../environment-model/proposal.md) 的 domain/stage 口径
  （单一规范性来源）。
- 与 `datahub-perf-optimization` 的关系：该 change 的 `datahub-runners` spec
  现含 "Watermark-Based Incremental Prod-to-Dev Sync" 在线同步要求；本 change
  不修改它，slice 3 落地时以 MODIFIED delta 收敛为单一口径（见 tasks 3.x）。

## Impact

- 受影响文件：本 change 新增；公开文档零改动（`agent-progress.md` 进度追加
  除外）。slice 3 预期影响：`datahub/app/lib/datahub/sync_engine.py`、
  `datahub/app/jobs/data_sync_runner.py`、`scripts/caifubao`、dev overlay 的
  `MONGODB_SRC_*` 注入、`docs/operations/agent-cli.md`。
- 运行时影响：本 slice 为零；导入契约 requirement 在 slice 3 实现并验证后才
  视为满足，届时保持本 change 开放（先例：`fq-adj-factor-fix` 5.x）。
- 读者影响：新读者可从 spec 回答「dev 以后如何获得真实规模数据、在线直连何时
  消失、导入失败时的行为」。
