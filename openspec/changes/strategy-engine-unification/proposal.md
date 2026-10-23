# Strategy Engine Unification — 一个 Spec、一套撮合、一个账本

## Why

策略层已经完成 paper 闭环（评分 → 选择 → 调仓 → paper NAV → 前瞻证据），但**能力是
按切片累加的，没有收敛成一套引擎**。继续往上叠功能之前必须先收口，否则分裂会被固化：

1. **目标组合被当成持仓。** `simulate_paper_nav` 用「目标权重 × NAV」推演持仓
   （`datahub/app/lib/strategy_engine/nav.py`），`StrategyPaperRun` 只存
   `target_holdings`，没有 fills、现金账本、逐日持仓或 P&L。后果是部分成交无从表达、
   盈亏无法归因、对账靠操作者手填 JSON（`reconcile.py`）、多策略共用一个账户不可能、
   blocked orders 只存在于 CLI 返回值（`run_nav` 算完即丢）。
2. **同一语义多处实现。** 可交易性在 `tradability.py` 对「涨跌停判定不可得」**fail
   closed**，研究侧 `factor_lab_account_replay.tradeable_open` **fail open**（文档明说
   "intentional divergence"）；paper 用合格股等权基准、backend 回测用沪深 300，两者
   的「超额/IR」不可比；撮合逻辑散落在 `backend/app/services/backtest_service.py`
   （3,129 行，`_simulate` 单体约 1,090 行）、`nav.py`、`timing_replay.py` 与
   factor lab 四处。
3. **策略定义耦合在代码里。** 加一种策略 = 改 `if strategy == ...` 分支；`selection`
   只支持 `top_percentile` / `top_n` 与等权（`_WEIGHTINGS = {"equal"}`），滞回、替代
   门槛、仓位算法、止损都无处声明；研究侧 `candidate.yaml` 表达的语义在生产侧没有
   对应物，"研究的 = 跑生产的"目前靠人工对齐。
4. **证据分散在四条轨。** `BacktestResult`、`StrategyReplay`、`StrategyPaperRun` +
   `StrategyForwardWindow`、`DecisionJournal` 互不连通；`artifact-contract` 定义了统一
   工件契约但 0/13 任务落地。

为什么是现在：账本、风控、执行是 roadmap 1.3 / 2.x 的前置（没有真实持仓，风控无从
计算、对账无意义）；而 `flip_wide_shadow_v1` 的前瞻窗口一旦开启，任何语义变更都会让
120 日时钟从头开始。先把「唯一 Spec + 唯一撮合 + 账本」定下来，后续切片才有稳定地基。

## What Changes

本 change 只交付**契约与迁移计划**（proposal + specs + design + tasks），不改变任何
运行时行为；实现按 S0–S3 分阶段落地，每阶段独立可验证。

- **StrategySpec（S0）**：把 `DEFAULT_STRATEGY_CONFIG` 升格为版本化、可哈希、可 diff
  的声明式策略定义，覆盖 universe / signal / selection（含 entry/exit/替代门槛）/
  sizing / constraints / rebalance / risk / execution / evidence。未知键拒绝、缺省值
  显式、AUM × 宽度 × 手数 × 最低佣金的可行性在定义期校验。
- **PortfolioLedger（S1）**：新增持久化账本（positions / cash / lots / fills /
  rejections / P&L），由 fills 重建；目标组合与持仓彻底分离；blocked orders 落库；
  对账从「手填 JSON」变成「读库」。
- **RiskGate（S2）**：独立的交易前/交易后风控阶段，覆盖换手与再平衡预算、集中度、
  流动性、现金下限、组合回撤熔断、regime 门控与 halt；拒绝订单带原因码，可在 replay
  中复现。
- **ExecutionModel（S3）**：收敛为唯一撮合语义（整手/板块最小量、T+1、涨跌停、停牌、
  费用会计），四种执行模式（BACKTEST / REPLAY / PAPER / LIVE）共用；未知可交易性
  判定改为**显式策略参数**（默认 fail closed，研究复现按需 opt-in），而不是第二份实现；
  券商适配器以 default-off 接口预留，不改变 Spec 语义。
- **Legacy 冻结**：`backend/app/services/backtest_service.py` 的 6 种策略冻结为
  legacy（只读、不再新增策略），策略语义只在 datahub 策略引擎演进。
- **证据收敛**：paper/replay/backtest 产物统一引用 `artifact-contract` 的
  `artifact_id` / `input_hash` 契约，前瞻窗口只认这一种证据。

## Capabilities

### New Capabilities

- `strategy-definition`: 版本化、可哈希、可迁移的 StrategySpec，以及「一份 Spec 驱动
  全部执行模式」的语义约束与可行性校验。
- `strategy-portfolio-ledger`: 由 fills 重建的持仓/现金/批次/P&L 账本，目标组合与持仓
  分离，拒绝订单持久化，可与账户快照对账。
- `strategy-risk-gate`: 交易前/交易后的组合级风控阶段（换手预算、集中度、流动性、
  回撤熔断、regime、halt），拒绝可解释、可重放。
- `strategy-execution-model`: 所有模式共用的唯一撮合与费用语义，未知判定策略显式化，
  成交幂等，券商适配器默认关闭。

### Modified Capabilities

无。本仓库没有独立的 `openspec/specs/` 主规格目录，现有 `strategy-engine` 等能力只以
change 内的 delta specs 存在；本 change 新增的是上层契约，不修改任何已合入 change 的
Requirement 文本。两者关系与迁移映射见 `design.md` §5。

## Impact

- **代码（后续阶段，非本 PR）**：`datahub/app/lib/strategy_engine/`（新增
  spec / ledger / risk / execution 模块，`nav.py` 改为消费账本）、
  `datahub/app/jobs/strategy_runner.py`（装配新阶段）、
  `datahub/app/model/strategy.py`（新增账本集合），`shared/caifubao_contracts/`（Spec
  与账本 schema 的单一来源）。
- **兼容性**：S0 保证现有 config → Spec 的确定性迁移，迁移后 `config_hash` 语义不变；
  已有 `StrategyPaperRun` / `StrategyForwardWindow` 记录只读兼容，不重写历史。
- **不涉及**：API 端点、鉴权、评分语义、方向语义、前瞻证据判定口径、券商真实下单。
- **文档**：新增 `openspec/README.md`（契约账本索引与本 change 的入口）；
  `docs/README.md` 增加指向该索引的一行链接。

## Non-goals

- 不接券商、不自动下单、不做订单状态机的真实成交回报（S3 只预留 default-off 接口）。
- 不改评分构造方向、不改选择「买高」语义、不重开或推进任何前瞻窗口。
- 不重写历史 replay / paper 产物，不修改已归档契约。
- 不新增数据源、不做日内/实时行情、不做多策略资金分配（只预留 sleeve 边界）。
- 本 change 不授权 promote，也不构成投资建议。
