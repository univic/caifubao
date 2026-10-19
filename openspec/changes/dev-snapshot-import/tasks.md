# Dev Snapshot Import — Tasks

> 切片划分与私有设计文档
> `caifubao-private docs/architecture/data-authority-cutover.md` 对应。
> 切片 2 的执行步骤是**需要用户逐项批准的集群操作**，勾选仅表示对应步骤已
> 获批准并完成，不构成对未来操作的授权。

## 1. 切片 1 — 数据权威切换设计（本 PR）

- [x] 1.1 盘点同步面 collection 的 owner/writer/reader（私有设计文档 §3）
- [x] 1.2 设计 research/data 每日生产链与缺口（daily_basic、health-watcher、
  data_asset_status 覆盖）（私有设计文档 §4）
- [x] 1.3 设计 writer 切换顺序、验证指标、回滚窗口与批准门（私有设计文档 §5+§8）
- [x] 1.4 设计受控快照导入管线与在线路径退役步骤（私有设计文档 §6）
- [x] 1.5 本 change：导入契约 ADDED requirements（spec-guardian：GATE REQUIRED）

## 2. 切片 2 — research 独立生产链启用（门控执行，逐项批准）

- [x] 2.1 步骤 0：只读盘点 research/旧 stable 的 CronJob 实际 suspend 状态与
  最近运行记录（无集群变更；2026-09-16 完成：research 全部 CronJob 处于
  挂起——无双写风险；旧 stable quote-index/quote-stock/signal/scoring/
  industry-sync 活跃；dev data-sync 活跃。细节见私有设计文档 §2.2）
- [ ] 2.2 对齐修复：research `stock_industry` 规范键迁移（#247 后代码 vs 09-11
  快照数据）；research 侧 FQ 全市场重算（`fq-adj-factor-fix` 5.2）
- [ ] 2.3 补齐 research 缺失的调度型 writer：`daily_basic`（tushare
  daily_basic）、`health-watcher` CronJob、`data_asset_status` 覆盖
- [ ] 2.4 逐 writer 切换（每次一个，验证后关旧）：quote-index → quote-stock
  （含 factor/FQ 阶段产出，不单独切换）→ daily_basic → signal → scoring →
  industry-sync；每个 writer ≥1 交易日验证窗口
- [ ] 2.5 每个 writer 的验证指标达标：freshness/`data_as_of` 一致、计数差在
  容差内、抽样数值逐字段一致（样本 ≥20）
- [ ] 2.6 回滚演练：至少一次「关新开旧」回滚窗口验证

## 3. 切片 3 — 受控快照导入替代在线同步（实现 PR）

- [ ] 3.1 快照导出：逐 allow-list collection 导出（JSONL.gz + BSON 安全编码）
  + manifest（sha256、计数、`data_as_of`、producer 镜像 SHA）——**代码已落地**
  （`datahub/app/lib/datahub/snapshot_transfer.py` +
  `snapshot_export_runner.py` + `./scripts/caifubao data snapshot-export`），
  对象存储传输（`--upload-uri`，键 `<prefix>/<snapshot_id>/<file>`）已落地；
  私有侧 Job 接线已完成（run-datahub-job.sh snapshot 类型），首次实跑待批准
- [ ] 3.2 dev 导入 Job：manifest/校验和/计数 fail-closed 校验（两遍式，先核验
  后写入）+ 幂等应用（业务键 upsert / 快照类 staging→原子
  `renameCollection`）+ 导入状态记录（`snapshot_import_state`）+ freshness
  复用既有 initializer 重算——**代码已落地**
  （`snapshot_import_runner.py` + `./scripts/caifubao data snapshot-import`），
  私有侧导入 Job 清单待私有 PR
- [ ] 3.3 `data sync` CLI 语义切换为快照导入；移除 `MONGODB_SRC_*` 注入；停用
  `caifubao-datahub-data-sync` CronJob（cutover 门 = 3.6 观察窗完成）
- [x] 3.4 `datahub-perf-optimization` 的 "Watermark-Based Incremental
  Prod-to-Dev Sync" requirement 以 MODIFIED delta 收敛为单一口径：同步语义
  切换为快照导入，并修正其陈旧的「按 `_id` 幂等 upsert」表述（现行为业务键
  upsert，见 `SYNC_UPSERT_KEYS`）——**delta 已落地**
  （`specs/datahub-runners/spec.md`：在线引擎定性为冻结的迁移期遗留；
  upsert 口径对日期分区集合收敛为业务键、`finance_market` 等无业务键的
  快照类集合保留 `_id` upsert；补 3 日重放窗口、dev-only signal 跳过规则
  与"不得扩列"场景；validate 1.1.1 通过）
- [x] 3.5 文档更新：`docs/operations/agent-cli.md` data sync 段落、环境模型
  §12 迁移期标注收口——**已落地**（data sync 段标题标为 migration-period
  legacy；新增 snapshot-export/snapshot-import 两节；环境模型 §12 指向已
  落地的契约与工具，保留迁移期定性）
- [ ] 3.6 观察窗口（≥5 交易日）快照导入的 freshness/计数对齐验证
- [ ] 3.7 旧 stable 退役判据核验（无读取方、research 权威 ≥5 交易日、备份与
  restore drill 通过）并提交退役变更（独立批准）
- [ ] 3.8 切片 3 验收（实现评审后新增）：对真实 dev mongod 验证
  `renameCollection` 经 admin 库下发且 dev Mongo 角色具备
  renameCollectionSameDB 权限（快照类换入的前提）

## 4. 验证与评审

- [x] 4.1 `openspec validate --all --strict` 通过
- [x] 4.2 spec-guardian gate 结论已记录（GATE REQUIRED，独立 change
  `dev-snapshot-import`）
- [x] 4.3 contract-reviewer（PASS，0 P1 · 2 P2 · 6 P3，全修复）/
  qa-reviewer（PASS，0 P1 · 1 P2 · 4 P3，全修复）结论已记录于 PR 正文
- [x] 4.4 分支冲突检查通过（public vs `origin/develop`、private vs
  `origin/main` 均 clean）；Draft PR：public
  https://github.com/univic/caifubao/pull/256 、private
  https://github.com/univic/caifubao-private/pull/83
- [x] 4.5 `docs/agent-progress.md` 记录本轮进度（含 PR 链接）
