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

- [ ] 2.1 步骤 0：只读盘点 research/旧 stable 的 CronJob 实际 suspend 状态与
  最近运行记录（无集群变更）
- [ ] 2.2 对齐修复：research `stock_industry` 规范键迁移（#247 后代码 vs 09-11
  快照数据）；research 侧 FQ 全市场重算（`fq-adj-factor-fix` 5.2）
- [ ] 2.3 补齐 research 缺失的调度型 writer：`daily_basic`（tushare
  daily_basic）、`health-watcher` CronJob、`data_asset_status` 覆盖
- [ ] 2.4 逐 writer 切换（每次一个，验证后关旧）：quote-index → quote-stock
  （含 FQ）→ daily_basic → factor（随 quote-stock）→ signal → scoring →
  industry-sync；每个 writer ≥1 交易日验证窗口
- [ ] 2.5 每个 writer 的验证指标达标：freshness/`data_as_of` 一致、计数差在
  容差内、抽样数值逐字段一致（样本 ≥20）
- [ ] 2.6 回滚演练：至少一次「关新开旧」回滚窗口验证

## 3. 切片 3 — 受控快照导入替代在线同步（实现 PR）

- [ ] 3.1 快照导出：逐 allow-list collection mongodump + manifest（sha256、
  计数、`data_as_of`、producer 镜像 SHA）
- [ ] 3.2 dev 导入 Job：manifest/校验和/计数 fail-closed 校验 + 幂等应用
  （业务键 upsert / 快照类 `--drop`）+ 导入状态记录（取代 `data_sync_state`）
- [ ] 3.3 `data sync` CLI 语义切换为快照导入；移除 `MONGODB_SRC_*` 注入；停用
  `caifubao-datahub-data-sync` CronJob
- [ ] 3.4 `datahub-perf-optimization` 的 "Watermark-Based Incremental
  Prod-to-Dev Sync" requirement 以 MODIFIED delta 收敛为单一口径
- [ ] 3.5 文档更新：`docs/operations/agent-cli.md` data sync 段落、环境模型
  §12 迁移期标注收口
- [ ] 3.6 观察窗口（≥5 交易日）快照导入的 freshness/计数对齐验证
- [ ] 3.7 旧 stable 退役判据核验（无读取方、research 权威 ≥5 交易日、备份与
  restore drill 通过）并提交退役变更（独立批准）

## 4. 验证与评审

- [x] 4.1 `openspec validate --all --strict` 通过
- [x] 4.2 spec-guardian gate 结论已记录（GATE REQUIRED，独立 change
  `dev-snapshot-import`）
- [ ] 4.3 contract-reviewer / qa-reviewer 结论已记录
- [ ] 4.4 分支冲突检查通过；Draft PR 已创建（public + private 各一）
- [ ] 4.5 `docs/agent-progress.md` 记录本轮进度（含 PR 链接）
