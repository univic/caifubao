# Tasks

> #306 已合并契约；#307 是**分阶段实现中的 Draft PR**。勾选表示相应代码/文档切片已提交并有覆盖，
> **不等于阶段级验收、完整生产能力、前瞻证据认证或授权 promote**。按 S0 → S3 渐进收口。

## 0. 契约与本 PR 收口

- [x] 0.1 编写 `proposal.md`（Why / What Changes / Capabilities / Impact / Non-goals）
- [x] 0.2 编写 `design.md`（目标形态、现状证据、成熟度差距、S0–S3 迁移、取舍与风险）
- [x] 0.3 编写 4 份 capability specs（definition / ledger / risk-gate / execution-model）
- [x] 0.4 编写 `openspec/README.md` 索引与写作约定，并链接到本 change
- [ ] 0.5 评审：spec-guardian 复核新增 Requirement、legacy/spec 双哈希、分域账本与 environment-model 边界；确认既有契约 supersession 只在实现 PR 发生
- [x] 0.6 #306 已合并；OpenSpec strict 校验通过。后续实现由 #307 按阶段推进

## 1. S0 — 定义统一（`strategy-definition`）

- [x] 1.1 定义 `StrategySpec` schema：
  `spec_version`、`signal`、`selection`（entry/exit/replacement）、`sizing`、
  `constraints`、`rebalance`、`risk`、`execution`、`evidence`
- [x] 1.2 实现未知键拒绝与缺省值显式化，复用现有 `validate_strategy_config` 的错误风格
- [x] 1.3 实现 canonical `spec_hash`（字段排序、类型规范化），并锁定 golden 向量；旧 `config_hash` 只保留为 `legacy_config_hash`
- [x] 1.4 实现 AUM × 宽度 × 板块最小手数 × 最低佣金的可行性校验（Spec 路径按决策日参考收盘价和目标权重执行；legacy 路径保持兼容）
- [x] 1.5 实现 `DEFAULT_STRATEGY_CONFIG` → `StrategySpec` 的确定性迁移函数
- [x] 1.6 golden 测试：迁移后的 spec 产生与旧 config 相同的 target book / rebalance / 声明的执行行为；分别验证 `legacy_config_hash` 原样保留与 `spec_hash` 稳定，不要求两个哈希相等
- [x] 1.7 让 `strategy_runner run` 支持 `--spec`（与 `--config-json` 并存），输出 `spec_hash`
- [x] 1.8 让 replay 路径消费同一 spec，新增 paper↔replay 的 target book 对拍测试
- [x] 1.9 随 S0 实现 PR 为 `strategy-forward-evidence` 提交 MODIFIED delta：窗口显式 pin `legacy_config_hash` 或 `spec_hash`，禁止跨类型比较；并在 `docs/operations/strategy-forward-window.md` 说明「Spec 变更 = 新哈希 = 新窗口」

## 2. S1 — 账本（`strategy-portfolio-ledger`）

- [x] 2.1 定义共享 ledger event schema；实现 `ResearchSimulationLedger` 的事件/投影，不在 datahub 创建 trading 账户集合。未来 `TradingPortfolioLedger` 的存储与投影由 trading runtime 独立拥有
- [x] 2.2 实现 `Ledger.apply(event)` 与稳定 `intent_id` / `order_id` / `fill_id`；允许一个订单多笔 partial fills，重放同一事件幂等
- [x] 2.3 实现由有序事件流重建研究模拟账本的 `rebuild()`，并加「重建 == 原投影」测试
- [x] 2.4 `nav.py` 改为消费研究模拟账本持仓；目标权重仅作为 intent，不把研究模拟账本当作 trading 账户账本
- [x] 2.4a 随 2.4 提交 `strategy-engine` 的 MODIFIED delta（NAV 由账本推导，费用/手数/T+1 语义不变）
- [x] 2.5 把研究模拟 `blocked_orders` 从 CLI 临时返回值改为可追溯的研究事件/工件，并在 `report` 中可见；trading 拒绝事件由 trading runtime 持久化
- [ ] 2.6 明确对账迁移边界：当前只读 CLI 保留既有输出契约；目标 trading 账户对账的计划侧来自 trading ledger projection，不得读取 research ledger
- [ ] 2.6a 随 2.6 提交 `strategy-live-loop-minimal` 的 MODIFIED delta（对账计划侧来源变更）
- [ ] 2.7 双跑对照：旧 NAV 曲线 vs 账本 NAV 曲线，逐日差异必须可解释
- [x] 2.8 文档：研究模拟账本事件 schema、重建 runbook，以及未来 trading ledger/对账的域边界契约

## 3. S2 — 风控门（`strategy-risk-gate`）

- [x] 3.1 定义 `risk` spec 块：turnover budget、concentration、cash floor、
  drawdown breaker、regime policy
- [x] 3.2 实现纯函数 `RiskGate.evaluate(spec, intents, ledger, market)`，返回
  approved / rejected(reason)
- [x] 3.3 实现换手/再平衡预算的确定性削减规则（按优先级）
- [x] 3.4 实现组合回撤熔断：只拦开仓，不拦减仓
- [ ] 3.5 接入 `market_regime`（60 日趋势）作为可选 regime 门控
- [ ] 3.6 保留 halt 契约的优先级语义；运维 halt 状态与操作历史归 trading domain，research replay 只消费显式冻结的 halt snapshot，不读写运行中的 halt store
- [x] 3.7 replay 对拍：同一输入重放得到相同 approved/rejected 与原因码
- [x] 3.8 文档：风控参数默认值、熔断语义与原因码表

## 4. S3 — 共享执行规则与域内适配器（`strategy-execution-model`）

- [x] 4.1 将 `shared/caifubao_contracts/friction.py` 明确为费用算术唯一来源；保留 `datahub/app/lib/execution/` 为兼容 re-export，不向其中添加完整撮合、订单状态或账户逻辑
- [x] 4.2 将板块最小量、T+1、涨跌停/停牌判定与未知策略收敛为可测试的共享纯规则；规则只接收显式输入，不读数据库、不执行 job、不连接券商
- [ ] 4.3 明确并实现 research-owned simulator 与未来 trading-owned adapter 的边界；BACKTEST/REPLAY/RESEARCH_SIMULATION 使用研究侧模拟器，TRADING_PAPER/LIVE 的订单、风控、账本、halt 与对账必须位于 trading runtime
- [ ] 4.4 迁移 `timing_replay` 与 factor lab 的成交计算以消费共享规则（至少共享费用与整手规则）；补跨域 conformance tests
- [x] 4.5 冻结 `backend/app/services/backtest_service.py` 为 legacy：加注释与只读策略清单，不再新增策略分支
- [ ] 4.6 把 paper / replay / backtest 产物统一挂 `artifact_id` / `artifact_hash` /
  `input_snapshot`（replay 现有 `input_hash` 只做映射；落地 `artifact-contract`）
- [ ] 4.7 交叉对拍：同一 spec 下 BACKTEST、REPLAY、RESEARCH_SIMULATION 的共享规则一致；未来 TRADING_PAPER 以同一组 conformance vectors 验证，不把 research runtime 当作 trading 依赖
- [x] 4.8 文档：`docs/architecture/strategy-engine.md`（把 `design.md` 的目标形态固化为
  现行架构文档）

## 5. 验证与收口（每阶段重复）

- [ ] 5.1 运行共享契约测试与受影响的 research/datahub 兼容测试；trading runtime 建立后增加其独立测试套件
- [ ] 5.2 `datahub/.venv/bin/ruff check --select E4,E7,E9,F datahub/` 与 format check，并检查 shared package 的 lint/类型兼容
- [x] 5.3 `openspec validate --all --strict`
- [x] 5.4 更新 `docs/capability-inventory.md` 对应行（策略引擎/账本/风控/执行）
- [ ] 5.5 记录阶段结论到 `docs/autoresearch/runs/.../manual-experiments-ledger.md`（涉及
  指标变化时）
