# Strategy Engine Unification — 一份 Spec、共享执行语义、分域账本

## Why

策略层已经完成 paper 闭环（评分 → 选择 → 调仓 → paper NAV → 前瞻证据），但**能力是
按切片累加的，没有收敛成一套引擎**。继续往上叠功能之前必须先收口，否则分裂会被固化：

1. **目标组合被当成持仓。** `simulate_paper_nav` 用「目标权重 × NAV」推演持仓
   （`datahub/app/lib/strategy_engine/nav.py`），`StrategyPaperRun` 只存
   `target_holdings` / `rebalance` / `nav_snapshot`，没有 fills、现金账本、逐日持仓或
   P&L。后果是部分成交无从表达、盈亏无法归因、对账的账户侧只能靠操作者手填 JSON
   （计划侧由 `account_plan` 生成）、多策略共用一个账户不可能、blocked orders 只存在
   于 CLI 返回值（`run_nav` 算完即丢）。
2. **执行规则分散，但费用算术已有共享入口。** 涨跌停可交易性、板块最小量、T+1、
   停牌与成交时序分散在 `nav.py`、`timing_replay.py`、Factor Lab 与 Backend 回测中。
   费用算术的权威实现已经在 `shared/caifubao_contracts/friction.py`；
   `datahub/app/lib/execution/costs.py` 是兼容转导，不是另一份费用实现。真正的问题是
   其余执行规则尚未统一，不能因此把有状态的订单执行、风控、账本都继续塞进 datahub。
   Paper 与 Backend 的基准口径也不同，超额收益/IR 不能不加说明地横向比较。
3. **策略定义耦合在代码里。** 加一种策略 = 改 `if strategy == ...` 分支；`selection`
   只支持 `top_percentile` / `top_n` 与等权（`_WEIGHTINGS = {"equal"}`），滞回、替代
   门槛、仓位算法、止损都无处声明；研究侧 `candidate.yaml` 表达的语义在生产侧没有
   对应物，"研究的 = 跑生产的"目前靠人工对齐。
4. **证据分散在四条轨。** `BacktestResult`、`StrategyReplay`、`StrategyPaperRun` +
   `StrategyForwardWindow`、`DecisionJournal` 互不连通；`artifact-contract` 定义了统一
   工件契约但 0/13 任务落地。

为什么是现在：策略定义、模拟账务和执行规则是 roadmap 1.3 / 2.x 的前置；而
`flip_wide_shadow_v1` 的前瞻窗口一旦开启，语义变更会影响证据可比性。先把「唯一 Spec +
共享确定性执行规则 + 分域账本」定下来，后续切片才有稳定地基。这里的“统一”指规则与契约
统一，不代表 research 与 trading 共用一个运行时、数据库或账户。

## What Changes

本 change 只交付**契约与迁移计划**（proposal + specs + design + tasks），不改变任何
运行时行为；实现按 S0–S3 分阶段落地，每阶段独立可验证。

- **StrategySpec（S0）**：把 `DEFAULT_STRATEGY_CONFIG` 升格为版本化、可哈希、可 diff
  的声明式策略定义，覆盖 universe / signal / selection（含 entry/exit/替代门槛）/
  sizing / constraints / rebalance / risk / execution / evidence。未知键拒绝、缺省值
  显式、AUM × 宽度 × 手数 × 最低佣金的可行性在定义期校验。
- **PortfolioLedger（S1）**：引入可由成交事件重建的账务模型，但按 domain 分开状态。
  research 侧使用 `ResearchSimulationLedger` 计算回测/回放/前瞻模拟 NAV，输出不可变研究
  工件；未来 trading/paper 的持仓、现金、订单、成交与对账由 trading domain 自己持有。
  两者可共享事件 schema 与记账规则，但不得共用持久化账户状态或让 research 写 trading。
- **RiskGate（S2）**：风控策略表达和确定性判定可复用；research 中用于回放/模拟评估，
  trading 中由 trading runtime 对真实 paper/live 订单执行权威风控。运维 halt、风险状态和
  订单拒绝的持久化归 trading 所有。
- **ExecutionModel（S3）**：统一共享的纯确定性执行规则（整手、T+1、涨跌停、停牌、
  成本与费用算术），各 domain 使用自己的编排与适配器。共享纯函数/契约放在
  `shared/caifubao_contracts/`；`datahub/app/lib/execution/` 保持兼容转导，不扩展为完整
  有状态撮合引擎。research 的 BACKTEST/REPLAY/模拟执行与 trading 的 TRADING_PAPER/LIVE
  分域运行；未知可交易性策略显式配置，默认 fail closed。
- **Legacy 冻结**：`backend/app/services/backtest_service.py` 的 6 种策略冻结为
  legacy（只读、不再新增策略）。新策略语义由 research domain 的策略引擎演进；当前
  `datahub/app/lib/strategy_engine/` 是迁移期物理位置，不代表 data domain 的目标所有权。
- **证据收敛**：research 的 backtest/replay/前瞻模拟产物统一引用 `artifact-contract` 的
  `artifact_id` / `artifact_hash` / `input_snapshot` 契约（replay 现有 `input_hash` 只做映射）。
  现有 `StrategyPaperRun` 是 research 的目标组合/前瞻计划证据，不等同于 trading/paper
  的订单与成交账本；本 change 不静默改变现有 FORWARD 认证口径。若未来 promotion 门槛要求
  trading/paper 的实际模拟成交证据，必须另提明确的契约变更。

## Capabilities

### New Capabilities

- `strategy-definition`: 版本化、可哈希、可迁移的 StrategySpec，以及「一份 Spec 驱动
  全部执行模式」的语义约束与可行性校验。
- `strategy-portfolio-ledger`: 统一账务事件与重建语义，但 research simulation ledger
  与 trading portfolio ledger 分域持有；目标组合与持仓分离，拒绝事件可追溯，对账只读取
  trading ledger。
- `strategy-risk-gate`: 可复现的组合级风险政策（换手预算、集中度、流动性、回撤熔断、
  regime）；研究侧只做模拟评估，交易侧由 trading runtime 权威执行并持有运维 halt 状态。
- `strategy-execution-model`: 所有模式共用版本化的确定性执行规则和费用语义，域内 adapter
  独立编排；未知判定策略显式化，部分成交幂等，券商适配器默认关闭。

### Modified Capabilities

本 change 的 delta 全部是 `ADDED`，不修改任何已合入 change 的 Requirement 文本。
但 S1 落地时会**必然**触及两条既有契约，届时必须先让 `openspec/specs/` 可用（归档，
见 `openspec/README.md` §7.1），再在实现 PR 中附 `## MODIFIED Requirements` delta
（完整清单见 `design.md` §5）：

- `strategy-engine`：paper NAV 由「目标权重推演」改为「账本投影」；
- `strategy-live-loop-minimal`：对账的计划侧由「计划目标组合」改为「账本投影」。

在这两个 MODIFIED delta 合并之前，既有 Requirement 仍然是现行契约，本 change 的
specs 只描述目标态。

## Impact

- **共享契约与纯函数**：`shared/caifubao_contracts/` 是版本化 `StrategySpec` schema、
  canonical `spec_hash`、费用算术（`friction.py`）及跨域纯确定性规则的共享边界；
  不在这里放 Mongo/SQL 访问、任务编排、账户状态或券商 SDK。
- **Research domain**：拥有策略构建、回测、Replay、研究模拟账本与研究证据。当前
  `datahub/app/lib/strategy_engine/` 和相关 job 是历史实现位置/迁移债务；本 PR 不移动
  运行时代码，但后续不得把 trading 订单状态、账户账本、运维 halt 或券商适配器新增到
  datahub。Research 模拟账本用于可复现的模拟结果，不是可操作账户。
- **Trading domain（目标态，当前未启用）**：拥有 TRADING_PAPER/LIVE 的风险执行、订单、
  成交、持仓、资金流水、运维 halt、对账与适配器。Trading 必须消费不可变策略工件，并能在
  research 服务不可用时独立运行；不得依赖 research 在线数据库。
- **兼容转导**：`datahub/app/lib/execution/costs.py` 继续作为现有调用方的兼容 façade；
  其权威费用算术在 `shared/caifubao_contracts/friction.py`。后续迁移逐步让调用方依赖
  共享纯函数，但不在 datahub façade 中实现第二套规则。
- **Backend**：保留既有 API 与旧回测兼容路径；不把完整策略执行引擎迁入 Flask API 服务。
- **兼容性与哈希**：历史 `config_hash` 原样保留并作为 `legacy_config_hash` 关联字段；
  新的完整 `StrategySpec` 使用独立 `spec_hash`。迁移要求目标组合、调仓和已声明执行行为
  对拍一致，不要求两种不同规范化算法产出相同哈希。旧运行/旧窗口只读；新 Spec 不匹配旧
  窗口时按现有契约归类为 `REPLAY`，不得推进原窗口计数。
- **契约变更**：S1 落地研究模拟 NAV 时，需要为现有 `strategy-engine` Requirement 提交
  `MODIFIED` delta；若修改 trading 账户对账计划侧来源，也必须在相应实现 PR 提交
  `MODIFIED` delta。当前 change 只定义目标态，不覆盖已生效契约。
- **本 PR**：仅更新契约与迁移计划，不改变运行时行为，不接入券商，不启用 trading/paper。
- **文档**：`openspec/README.md` 为契约账本入口，`docs/README.md` 指向该索引。
  
## Non-goals

- 不接券商、不自动下单、不做订单状态机的真实成交回报（S3 只预留 default-off 接口）。
- 不改评分构造方向、不改选择「买高」语义、不重开或推进任何前瞻窗口。
- 不重写历史 replay / paper 产物，不修改已归档契约。
- 不在本 PR 内迁移运行时代码或创建 trading/paper 部署；本 PR 只明确目标域边界与后续迁移约束。
- 不新增数据源、不做日内/实时行情、不做多策略资金分配（只预留 sleeve 边界）。
- 本 change 不授权 promote，也不构成投资建议。
