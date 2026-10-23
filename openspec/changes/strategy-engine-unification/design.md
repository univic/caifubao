# Design: Strategy Engine Unification

> 研究/学习/演示级设计文档，不构成投资建议。运行时行为在本 change 中**不变**；本文定义
> 目标形态、现状证据与迁移顺序。

## 1. 目标形态

目标是**一份不可变策略契约、共享的确定性规则、分域运行时与分域状态**，而不是把所有模式
塞进 datahub 的同一个进程或共用一份数据库。以下为目标架构：

```
data domain
  版本化行情 / 交易日历 / 股票主数据
                 │ 只读 PIT snapshot
                 ▼
research domain
  StrategySpec → Builder → ResearchSimulator → ResearchSimulationLedger
       │                 回测 / Replay / 研究前瞻模拟 → 不可变研究证据
       │
       └── promotion：StrategySpec + hash + 验证证据（不可变工件）
                              │
                              ▼
trading domain（当前 trading-paper / prod 均未启用）
  工件消费者 → Trading RiskGate → domain-local ExecutionAdapter
             → Trading Portfolio Ledger → 对账 / 审计 / 运维 halt
```

共享边界：

- `shared/caifubao_contracts/`：StrategySpec schema、规范化/哈希契约、费用算术，以及
  必须跨域一致的纯确定性执行规则。共享函数只接受显式输入并返回结果，不读数据库、不调度
  job、不持有账户状态、不连接券商。
- research 的模拟账本与 trading 的账户账本可复用事件 schema、成本定义和对拍向量，但
  **必须是不同的状态实例与持久化边界**。Research 不能读写 trading 在线库；trading
  不能依赖 research 服务在线。
- `datahub/app/lib/execution/` 当前是共享费用模型的兼容 façade，不是完整撮合引擎；
  它可以暂时保留旧 import 路径，但不得继续承载有状态订单执行、持仓账本、风控状态或
  券商适配器。Backend 是 API 层，不是新的交易引擎归宿。

| 模式 | 所属 domain | 数据/输入 | 产物与状态 |
|:---|:---|:---|:---|
| BACKTEST | research | 历史面板 + PIT 约束 | 研究指标与模拟账本工件 |
| REPLAY | research | 冻结工件/快照 | `StrategyReplay`（REPLAY） |
| RESEARCH_SIMULATION | research | 按时点可用的每日快照 | 研究模拟成交事件、NAV 与证据 |
| TRADING_PAPER | trading（目标态） | 已 promotion 的 Spec + 交易时点市场数据 | paper 订单/成交/持仓/现金账本 |
| LIVE | trading（目标态，default-off） | 已 promotion 的 Spec + 实时市场数据 | 券商订单与实际成交回报 |

现有 `StrategyPaperRun` 名称是历史命名：它目前保存策略目标组合与前瞻计划证据，不代表
已经有 trading/paper 订单账本。新契约应把研究模拟（`RESEARCH_SIMULATION`）与未来的
`TRADING_PAPER` 明确区分；不要求本 PR 立即重命名历史集合或重写历史记录。

设计原则（沿用现有约定，不新造）：

1. **方向只在评分构造层**，策略层永远「选高」
   （`docs/autoresearch/runs/h20-excess-alpha/architecture-layers-strategy-design.md` §4.3）。
   本 change 不碰。
2. **纯函数 + 适配器边界**：共享规则不 import Mongo、不做 I/O；research runner 只负责
   研究数据读取与工件落盘，trading runner 只负责订单/账户状态。现有
   `selection.py` / `runner.py` 可作为迁移起点，但物理目录不改变 domain ownership。
3. **配置即契约**：完整 Spec 使用独立 `spec_hash`；历史 `config_hash` 作为
   `legacy_config_hash` 保留映射，不强求两个不同 canonicalizer 的哈希相等。语义变化必须
   bump Spec 版本，不允许静默改变默认值。
4. **证据不可变**：FORWARD 产物只能由窗口内、决策早于执行日开盘的运行产生；本 change
   只让证据引用统一 artifact id / hash，不改判定口径。

## 2. 现状盘点（证据）

| # | 链路 | 位置 | 规模 | 角色 |
|:---|:---|:---|:---|:---|
| 1 | 后端回测 | `backend/app/services/backtest_service.py` | 3,129 行；`_simulate` 约 1,090 行 | 6 种策略，单票/多票，CSI300 基准 |
| 2 | Paper 策略 | `datahub/app/lib/strategy_engine/` + `jobs/strategy_runner.py` | 引擎 5,148 行 + job 约 1,000 行 | top-N → 调仓 → paper NAV → 前瞻窗口 |
| 3 | 研究回放 | `factor_lab/` + `autoresearch/` | — | 冻结面板、标签语义、walk-forward、硬门 |
| 4 | 单票择时 | `timing_replay.py`、`backend/app/services/timing_evaluator.py` | 688 + 1,019 行 | entry/exit 分位、成对证据 |
| 5 | ETF 账本 | `etf_benchmark.py`、`core_satellite.py`、`etf_lab/` | — | cent 精确、核心-卫星、多资产轮动 |
| 6 | PIT 模拟 | `datahub/research/pit_sim/` | — | 50k 纸面模拟 |
| 7 | H20 快照 | `autoresearch_h20*_runner` | — | train/val/test 快照 |

策略定义同样分散：`DEFAULT_STRATEGY_CONFIG`（dict）、autoresearch `candidate.yaml`、
backend 回测请求 JSON、timing replay manifest、factor lab `profile.yaml`。**没有一处
把「一个策略」定义为可被所有模式消费的对象。**

### 成熟度评估（0 无 – 3 生产可用）

| 维度 | 目标 | 现状 | 成熟度 | 差距 | 关键证据 |
|:---|:---|:---|:---:|:---|:---|
| 定义/版本化 | 一份 Spec 驱动四模式 | 5 套 DSL | 1.5 | 中 | `config.py` 的 `_KNOWN_SELECTION_KEYS` |
| 组合构建 | entry/exit/替代门槛/多种仓位 | top-N + 静态约束 | 1.5 | 中 | `selection.py:compute_rebalance` 集合差 |
| 撮合语义 | 唯一实现 | 3+ 份，fail-open/closed 分裂 | 1.5 | 中 | `tradability.py` 文档 |
| 账户账本 | 持久化 positions/cash/P&L | 无，NAV 从目标权重推导 | 0.5 | **大** | `nav.py:198` |
| 组合风险 | 换手预算/回撤/暴露/regime | 仅单票/行业/流动性静态约束 | 1.0 | **大** | `_KNOWN_CONSTRAINT_KEYS` |
| 执行接入 | 订单状态机 + default-off 适配器 | 无（仅 halt + 只读对账） | 0.5 | 大（可延后） | `halt.py`、`reconcile.py` |
| 证据/治理 | 单一 artifact + 升格门 | 前瞻门可用，但四轨并存 | 2.0 | 小-中 | `strategy-forward-window.md` |
| 编排/运维 | 幂等运行图 + 调度 | operator cadence，非交易日异常 | 1.0 | **大** | forward-window §6 |
| 多策略/资金分配 | sleeve 接口 | 仅研究侧 `combine_sleeves` | 0.5 | 中（MVP 可不做） | `core_satellite.py` |
| 规模可行性 | 定义期校验 AUM×宽度×成本 | 无 | 0.5 | 小但致命 | `PAPER_EXECUTION` 最低佣金 5 元 |

**结论：决策与模拟内核接近完成，状态与执行基本空白。** 对「可信 paper 研究闭环」差距
小-中（2–3 个切片）；对「可运营 paper 系统」差中；对「可实盘」差大，剩余工作量约六到
七成，且主体是状态机与运维，不是预测模型。

## 3. 三个结构性缺陷

### 3.1 目标组合 = 持仓（状态混同）

`simulate_paper_nav` 以目标权重推演持仓，`StrategyPaperRun` 不存 fills（只存
`target_holdings` / `rebalance` / `nav_snapshot`）。于是：部分成交无法表达、P&L 无法
归因、blocked orders 不落库、对账的账户侧只能读操作者手填 JSON（计划侧由
`account_plan` 生成）、一个账户挂多个策略不可能。**这是 S1 必须先做的原因。**

### 3.2 一个语义，多处实现

- 可交易性：生产 fail closed，研究 fail open（`tradability.py` 明确记录）。
- 基准：paper 合格股等权 vs backend 沪深 300，超额/IR 不可比。
- 策略分支：6 种策略在单体 `_simulate` 内以 `if/elif` 实现，新增策略 = 改单体。
- 成本：`shared/caifubao_contracts/friction.py` 是费用算术的权威实现；
  `datahub/app/lib/execution/costs.py` 仅 re-export 该实现。尚未统一的是整手、T+1、
  涨跌停/停牌和执行时序等规则。应扩展共享纯函数/契约并让域内 adapter 调用，不应把
  `datahub/app/lib/execution/` 扩展为完整的有状态执行模块。

**结论：不要求立刻删除任何一份，但必须指定唯一权威实现，其余显式标注 legacy / 兼容
用途（S3）。**

### 3.3 定义与执行耦合

`selection` 只支持 `top_percentile`/`top_n` 与 `equal` 权重；`rebalance` 只有
`cadence_days`。滞回、替代门槛、时间退出、仓位算法、止损都无法声明。研究侧的候选语义
无法直接在生产执行，导致「研究的 = 跑生产的」只能靠人工保证。

## 4. 迁移顺序（S0–S3）

每阶段独立可验收；后续实现不得用迁移便利绕过 domain 边界。

### S0 — 定义统一

- 新增 `StrategySpec` schema（版本、显式默认值、未知键拒绝、canonical `spec_hash`）。
- 为旧 config 建立确定性映射：保留旧 `config_hash` 为 `legacy_config_hash`，另算
  `spec_hash`；golden tests 验证目标组合、调仓和声明的执行行为一致，而不是强求哈希相等。
- entry/exit/替代门槛、sizing、risk 与 execution policy 进入 Spec；所有会影响决策的字段进入
  `spec_hash`。
- 可行性校验：AUM × 宽度 × 板块最小手数 × 最低佣金。
- **验收**：paper/replay 兼容路径在相同输入下 target book / intents 一致；Spec 哈希向量稳定；
  新 Spec 不匹配旧前瞻窗口时按现有契约记为 REPLAY，不推进旧窗口。

### S1 — 研究模拟账本（不是 trading 账户账本）

- 新增可由模拟成交事件重建的 `ResearchSimulationLedger`，用于 NAV、现金、持仓、批次和 P&L；
  研究输出以不可变工件/事件记录保存，不创建可操作的 trading 账户状态。
- NAV 改为消费研究模拟账本，而非由目标权重直接推导（随附 `strategy-engine` 的
  `MODIFIED` delta）；blocked simulated intents/fills/rejections 可由研究报告重建。
- 研究模拟账本保留 T+1、现金守恒、持仓上限、公司行为、退市显式退出、长停牌估值陈旧标记等
  已有账本不变量；同一账户跨窗口连续模拟，不得隐式重置资金。
- Trading 账户账本、运维 halt 与真实账户对账状态归 trading domain；如果启用 trading/paper，
  必须在独立运行时/存储内实现，不复用 research ledger 的数据库。
- **验收**：同输入重跑逐笔一致；由成交事件从零重建与原投影一致；旧 NAV vs 新 NAV 的差异
  逐日可解释；拒绝订单不改变持仓/现金。

### S2 — 风控门

- 风险政策和可复现的纯判定逻辑可共享；research 使用冻结 ledger/snapshot 做历史回放与模拟，
  trading runtime 对 paper/live 订单执行权威风险门控。
- 覆盖换手预算、集中度、流动性、现金下限、组合回撤熔断与 regime；运维 halt 的状态、操作
  历史和优先级由 trading domain 拥有，research 只能消费明确提供的快照，不得修改该状态。
- 拒绝订单带原因码和限额版本，可在 replay 中复现。
- **验收**：预算耗尽/回撤触发的开仓被拒绝且可解释；减风险退出仍可通过；相同输入的风险决策
  与拒绝原因完全一致。

### S3 — 共享执行规则与域内适配器

- 费用算术以 `shared/caifubao_contracts/friction.py` 为唯一来源；保留
  `datahub/app/lib/execution/` 为兼容 re-export，禁止在那里增加第二套成本逻辑或有状态撮合。
- 将板块最小量、T+1、涨跌停、停牌和可交易性未知策略整理为显式、可测试的共享纯规则；
  若共享包不适合承载某条规则，必须在设计评审中说明理由并提供跨域 conformance tests。
- research 的 BACKTEST/REPLAY/RESEARCH_SIMULATION 使用 research-owned simulator；未来
  TRADING_PAPER/LIVE 使用 trading-owned adapter / order state machine / ledger。两者共享规则和
  测试向量，不共享进程、数据库或运行时依赖。
- backend 六种旧策略冻结为 legacy；不将新策略分支或有状态执行引擎搬入 Flask API 服务。
- 统一工件引用 `artifact_id` / `artifact_hash` / `input_snapshot`；历史 replay 的
  `input_hash` 只映射、不重写语义。
- **验收**：共享规则 conformance tests 通过；跨域相同输入的确定性决策/成本规则一致；LIVE 无
  adapter 时 fail loud，且任何模式差异均可由显式配置或实际成交事实解释。

## 5. 与现有 change 的关系

| 现有 change | 关系 |
|:---|:---|
| `strategy-paper-runner` | 本 change 的 S0/S1 是其 config/runner 契约的演进，不推翻 |
| `strategy-portfolio-risk-limits` | S2 在其静态约束之上增加预算/回撤/regime |
| `strategy-live-loop-minimal` | S3 复用其 halt / tradability / reconcile 的判定与输出契约；S1 只改对账的**计划侧来源** |
| `strategy-forward-evidence-capture` | 证据判定口径不变；S6 只统一 artifact 引用 |
| `artifact-contract` | 落地 `artifact_id` / `artifact_hash` / `input_snapshot` 契约（replay 现有 `input_hash` 只做映射，不改语义） |
| `production-capability-roadmap` | 本 change 是阶段 1（决策与治理地基）的引擎侧细化 |

### 5.1 计划中的 supersession（S1 实现时必须补 MODIFIED delta）

本 change 的 specs 描述目标态，但 S1 落地会改变两条**已经生效**的 Requirement。按
OpenSpec 的 delta 规则，实现 PR 必须为它们提交 `## MODIFIED Requirements`（完整复制并
更新原文，而不是在本 change 里静默覆盖）：

| 目标能力 | 现行 Requirement（来源） | S1 后的变化 |
|:---|:---|:---|
| `strategy-engine` | `strategy-live-loop-minimal/specs/strategy-engine/spec.md`「Paper NAV simulation uses realistic T+1 cost semantics」 | NAV 由「按 `StrategyPaperRun` 目标权重推演」改为「由账本投影推导」；费用/手数/T+1 语义不变 |
| `strategy-live-loop-minimal` | `strategy-live-loop-minimal/specs/strategy-live-loop-minimal/spec.md`「Plan-versus-account reconciliation MUST be structured and fail loud」 | 对账的计划侧由「计划目标组合」改为「账本投影」；漂移/缺失/breaks/非零退出语义不变 |

在对应实现 PR 的 MODIFIED delta 合并前，上述 Requirement 仍是现行契约；本 change 不得被
当作已生效的替代。另：当前 FORWARD 认证仍针对 research-side `StrategyPaperRun` 计划证据，
不是 trading/paper 的成交账本证据；本 change 不改变该认证口径。若未来要把 promotion 的
120-session 门槛改为 trading/paper 成交证据，必须另提明确的 MODIFIED delta。

## 6. 关键取舍

- **为什么不是重写？** 现有纯函数模块测试充分（策略相关测试约 6,000 行），重写会把
  已验证的语义（费用感知 sizing、板块最小量、fail-closed 交易性）置于风险中。采用
  「加一层 Spec + 换内部状态源」的演进式路径。
- **为什么账本优先于风控？** 没有真实持仓，回撤/暴露/预算都算不出来；风控必须先有状态。
- **为什么保留 backend 回测？** 它服务单票教学/演示与历史 API 契约；冻结比删除便宜，
  也避免 API 破坏。
- **为什么不做多策略资金分配？** 账本与 Spec 未稳定前抽象 sleeve 会把错误固化；只预留
  边界（`combine_sleeves` 研究实现）。

## 7. 风险与缓解

| 风险 | 缓解 |
|:---|:---|
| Spec 迁移改变既有 `config_hash`，使历史 run 不可比 | 保留 `legacy_config_hash`，另算 `spec_hash`；golden 对拍行为；新 Spec 不继承旧窗口资格；历史产物只读 |
| 账本引入后 NAV 与旧曲线不一致 | 双跑对照 + 差异报告，差异必须可解释（费用/手数/成交） |
| 风控参数化诱发过拟合 | 预算/门槛走 replay 网格并记录；默认值保守 |
| 收敛撮合时悄悄改变研究结论或越过 domain 边界 | 共享纯规则 + 跨域 conformance tests；研究/交易分域状态；研究兼容策略必须显式 opt-in |
| 阶段依赖导致长期半成品 | 每阶段独立可交付、可回滚；S0 之后即可停 |
