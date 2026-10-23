# Design: Strategy Engine Unification

> 研究/学习/演示级设计文档，不构成投资建议。运行时行为在本 change 中**不变**；本文定义
> 目标形态、现状证据与迁移顺序。

## 1. 目标形态

四个执行模式共用同一套决策与账务语义，只有「数据来源」和「成交适配器」不同：

```
不可变事实层  数据(PIT) · 因子 · 信号 · 评分(model_version + config_hash)
                              │ 只读快照 snapshot(as_of)
                    ┌─────────▼─────────┐
                    │ S1 StrategySpec   │ 版本化、可 diff、可哈希、未知键拒绝
                    └─────────┬─────────┘
                    ┌─────────▼─────────┐
                    │ S2 Builder (纯)   │ (spec, snapshot, ledger) → TargetBook + Intents
                    └─────────┬─────────┘
                    ┌─────────▼─────────┐
                    │ S3 RiskGate (纯)  │ 换手预算/集中度/流动性/回撤/regime/halt
                    └─────────┬─────────┘
                    ┌─────────▼─────────┐
                    │ S4 ExecutionModel │ 唯一撮合: 整手/T+1/涨跌停/停牌/费用
                    └─────────┬─────────┘
                    ┌─────────▼─────────┐
                    │ S5 Ledger         │ 持仓/现金/批次/P&L（持久化、可重建）
                    └─────────┬─────────┘
                    ┌─────────▼─────────┐
                    │ S6 Evidence       │ artifact(input_hash) · replay/forward · 升格门
                    └───────────────────┘
```

| 模式 | 数据源 | 成交 | 产物 |
|:---|:---|:---|:---|
| BACKTEST | 历史全量面板 | ExecutionModel | 研究指标（research） |
| REPLAY | 冻结工件 | ExecutionModel | `StrategyReplay`（REPLAY） |
| PAPER | 每日 PIT 快照 | ExecutionModel（模拟） | `StrategyPaperRun`（REPLAY/FORWARD） |
| LIVE | 收盘/实时行情 | 券商适配器（default-off） | 订单 + 成交回报 |

设计原则（沿用现有约定，不新造）：

1. **方向只在评分构造层**，策略层永远「选高」（`architecture-layers-strategy-design.md`
   §4.3）。本 change 不碰。
2. **纯函数 + 适配器边界**：S2–S4 不 import Mongo；只有 runner/job 触碰持久化。现有
   `selection.py` / `runner.py` 已是这个形态。
3. **配置即契约**：Spec 进入 `config_hash`；语义变化必须 bump Spec 版本，不允许静默
   改默认值（现有 `validate_strategy_config` 已拒绝未知键）。
4. **证据不可变**：FORWARD 产物只能由窗口内、决策早于执行日开盘的运行产生；本 change
   只让证据引用统一 artifact id，不改判定口径。

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

`simulate_paper_nav` 以目标权重推演持仓，`StrategyPaperRun` 不存 fills。于是：部分成交
无法表达、P&L 无法归因、blocked orders 不落库、对账只能读操作者手填 JSON、一个账户
挂多个策略不可能。**这是 S1 必须先做的原因。**

### 3.2 一个语义，多处实现

- 可交易性：生产 fail closed，研究 fail open（`tradability.py` 明确记录）。
- 基准：paper 合格股等权 vs backend 沪深 300，超额/IR 不可比。
- 策略分支：6 种策略在单体 `_simulate` 内以 `if/elif` 实现，新增策略 = 改单体。
- 成本：已在 `shared/caifubao_contracts/friction.py` 收敛，可复用的先例已经存在。

**结论：不要求立刻删除任何一份，但必须指定唯一权威实现，其余显式标注 legacy / 兼容
用途（S3）。**

### 3.3 定义与执行耦合

`selection` 只支持 `top_percentile`/`top_n` 与 `equal` 权重；`rebalance` 只有
`cadence_days`。滞回、替代门槛、时间退出、仓位算法、止损都无法声明。研究侧的候选语义
无法直接在生产执行，导致「研究的 = 跑生产的」只能靠人工保证。

## 4. 迁移顺序（S0–S3）

每阶段独立可验收；后一阶段依赖前一阶段落地。

### S0 — 定义统一

- 新增 `StrategySpec` schema（版本、字段、默认值、未知键拒绝、哈希）。
- `DEFAULT_STRATEGY_CONFIG` → Spec 的确定性迁移；迁移后行为与 `config_hash` 兼容。
- entry/exit/替代门槛、sizing（equal 之外至少留接口）、risk 块进入 Spec。
- 可行性校验：AUM × 宽度 × 板块最小手数 × 最低佣金。
- **验收**：同一 Spec 分别驱动 paper 与 replay，模块边界测试绿；golden 对比证明
  「旧 config ≡ 迁移后 Spec」。

### S1 — 账本

- `strategy_fills` / `strategy_positions` / `strategy_cash`（或事件表 + 投影）。
- `Ledger.apply(fills)` 可重建；`nav` 改为消费账本而非目标权重。
- blocked orders 落库；`reconcile` 直接读账本。
- **验收**：同输入重跑逐笔一致；账本能由 fills 从零重建；对账无手填 JSON。

### S2 — 风控门

- `RiskGate`：换手/再平衡预算、集中度、流动性、现金下限、组合回撤熔断、regime
  （复用 `market_regime.py`）、halt（复用 `halt.py`）。
- 拒绝订单带原因码，可在 replay 中复现。
- **验收**：预算耗尽/回撤触发的订单被拒绝且可解释；同一输入重放结果一致。

### S3 — 执行与收敛

- `ExecutionModel` 收敛唯一撮合语义；未知判定改为显式策略参数（默认 fail closed，
  研究复现 opt-in）。
- `ExecutionAdapter` 接口 + SIM 实现（默认）+ BROKER 占位（default-off）。
- backend 6 策略冻结为 legacy；新增策略只走 Spec。
- **验收**：paper 与 replay 走同一状态机；适配器不改变 Spec 语义；legacy 路径只读。

## 5. 与现有 change 的关系

| 现有 change | 关系 |
|:---|:---|
| `strategy-paper-runner` | 本 change 的 S0/S1 是其 config/runner 契约的演进，不推翻 |
| `strategy-portfolio-risk-limits` | S2 在其静态约束之上增加预算/回撤/regime |
| `strategy-live-loop-minimal` | S3 复用其 halt / tradability / reconcile，收敛而非替换 |
| `strategy-forward-evidence-capture` | 证据判定口径不变；S6 只统一 artifact 引用 |
| `artifact-contract` | S6 直接落地其 `artifact_id` / `input_hash` 契约 |
| `production-capability-roadmap` | 本 change 是阶段 1（决策与治理地基）的引擎侧细化 |

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
| Spec 迁移改变既有 `config_hash`，使历史 run 不可比 | 迁移函数保证旧 config 生成同一哈希；golden 测试；历史产物只读 |
| 账本引入后 NAV 与旧曲线不一致 | 双跑对照 + 差异报告，差异必须可解释（费用/手数/成交） |
| 风控参数化诱发过拟合 | 预算/门槛走 replay 网格并记录；默认值保守 |
| 收敛撮合时悄悄改变研究结论 | 研究侧 fail-open 作为显式 opt-in 保留，默认策略不变 |
| 阶段依赖导致长期半成品 | 每阶段独立可交付、可回滚；S0 之后即可停 |
