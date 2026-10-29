# Strategy Program 2026 — 统一策略声明、研究流水线与双袖资金结构（P0–P4）

## Why

现有策略体系有三件稀缺资产：可哈希的 `StrategySpec`、域内独立的账本契约、fail-closed 的
前瞻证据门禁。但它缺三样东西，且互相纠缠：

1. **声明与行为脱节。** `rebalance.cadence_days` 通过校验、进入 `spec_hash`、写进文档，
   但没有任何代码消费它——名义周频、实际日频（`docs/capability-inventory.md` 与
   `strategy-exit-hysteresis/proposal.md` 均已记录）。`risk.*` 的六个字段同样全部定义并
   校验，但 `jobs/strategy_runner.py` 不调用 `shared/caifubao_contracts/strategy_risk.py`
   的 `evaluate_risk`，因此每日链路一个都不生效。spec 在语义上撒谎，比缺字段更危险。
2. **决策逻辑有多个副本。** research 与未来 trading 各写一套模拟器，靠
   `GOLDEN_EXECUTION_V1`（3 条费用向量）对齐；费用算术能对齐，选股与组合构建对齐不了。
   现有 `StrategySpec` 还用等值断言固定 `universe == {"market":"CN_A","as_of":"PIT"}`，
   连 ETF 与多资产都装不进去。
3. **研究没有统一验证流程。** 同一个 idea 可从任一处发起，用不同标签口径、不同基准、
   不同窗口跑出不同数字。`docs/autoresearch/runs/small-book/` 记录过一次方向搞反与至少
   两次结果整体作废。**这些报告已自行声明失效，其数值（包括持有期扫描里 h60 那一行与
   各宽度的回撤）必须视为待重算，不得作为设计依据。**

同时有两项资金结构判断已经确定（见 `docs/architecture/strategy-program.md` §2）：稳健
半仓改为 ETF 多资产；`flip` 簿维持现有窗口继续观察。前者要求新的能力契约，后者要求明确
「v1 窗口不得因本次改动而改变——不只是 hash 不变，而是**执行语义不变**」。

## What Changes

按依赖顺序交付五个能力，每个阶段独立 PR、独立验收：

- **P0 `strategy-spec-completeness`** — 策略声明必须「声明即行为」：新增 `spec_version: "2"`
  的分层字段（身份假设、数据*要求*、模型引用、策略族、出场/再平衡语义、会计口径、基准、
  失效判据）；用 **consumption manifest** + CI 校验消灭未消费字段；加结构门禁拒绝注定
  失败的配置（最小持仓数、年换手预算、集中簿必须有止损、回撤预算）；把数据、模型与决策
  状态拆成三层身份；`spec_version: "1"` **保留旧执行语义**（`cadence_days` 仍不消费），
  并用连续多日回归而非 hash 相等来验证。
- **P1 `strategy-signal-providers`** — 统一的 `SignalProvider` 契约与三个实现
  （规则 / 机器学习 / 大模型），全部输出同一 schema；新增「记录-重放」一致性不变量，
  使不可复现的模型也能产出可审计证据；确立「AI 只出分不出单」与「推理成本进会计」。
- **P2 `strategy-research-pipeline`** — 六阶段研究流水线（预注册 → 可行性 → 探索 →
  确认 → 结构与容量 → 前瞻 → 判定），配**确认集数据隔离**（purge/embargo 按标签期限）、
  **按假设族累计的试验计数**（换 registration 不能重置预算）、确认次数上限、结构/容量
  门禁与不可变 `verdict.json`。
- **P3 `stable-multi-asset-sleeve`** — ETF 多资产稳健半仓：预先固定的资产集、目标权重与
  带宽、**单资产止损**（组合级降险不得替代）、波动目标、回撤预算、低换手再平衡与失效判据。
  其收益/回撤画像与基准选择属**待验证预期**，不引用已失效报告数值。
- **P4 `aggressive-sleeve-research`** — 激进袖研究：期限维度显式化（不预设哪个期限正确）、
  历史数字在真实回放口径下**先行重算**、新信息集数据落地、大模型二次筛选的成本与范围约束、
  与稳健袖的资金与风险隔离。

本 change 只定义契约与分阶段任务，不含实现；实现按 P0→P1→P2→P3/P4 顺序另开 PR。

## Capabilities

### New Capabilities

- `strategy-spec-completeness` — 策略声明的完备性、消费者门禁与结构准入
- `strategy-signal-providers` — 跨规则/ML/LLM 的统一信号源契约与可审计重放
- `strategy-research-pipeline` — 预注册、试验计数与六阶段判定
- `stable-multi-asset-sleeve` — ETF 多资产稳健袖的画像、门禁与失效判据
- `aggressive-sleeve-research` — 激进袖的期限纪律、数据前置与风险隔离

### Modified Capabilities

无。本 change 不修改已归档或已合入 change 的既有 Requirement 文本；`spec_version: "1"`
的行为、`strategy-exit-hysteresis` 的可选字段语义与已认证窗口 `01c196b8…` 均保持不变。

## Non-goals

- 不实现真实券商执行、不启用 `trading-paper` / `prod`、不授权任何真实资金动作。
- 不改变已认证前瞻窗口 `01c196b8…` 的任何字节；不把 v1 窗口升级为 v2 证据。
- 不为机器学习/大模型单独发明风控或账本；它们必须复用同一 `decide()` 与 `RiskGate`。
- 不在本 change 内扩大激进袖资金，也不承诺任何收益目标。
- 不统一 `factor_lab` 与 `autoresearch` 的既有标签口径（另需独立 change）。

## Impact

- 契约：`shared/caifubao_contracts/strategy_spec.py`（v2 字段、策略族与**按版本分派的执行
  语义**）、新增 `strategy_provider.py`、`strategy_decision_state.py`、`strategy_verdict.py`；
  `shared/caifubao_contracts/strategy_risk.py` 由「已定义」变为「被每日链路消费」——
  但**只对 v2 声明生效**。
- 研究实现：`datahub/app/lib/strategy_engine/`（出场语义、配置族的权重与带宽）、
  `datahub/app/jobs/strategy_runner.py`（RiskGate 接线、v1/v2 语义分派）。
- 运行记录：每次运行新增 `decision_state_hash` 与 `resolved_inputs`（当日快照 id +
  解析后的模型工件 digest），使跨模式分叉可归因、前瞻每日新数据不改策略身份。
- 门禁与工具：`openspec/specs/strategy-declaration/consumers.yaml`（新建）、CI 校验脚本、
  连续多日回归基线、研究流水线 harness。
- 数据：P4 需要新增事件类数据源，按 datahub `fetch → normalize → validate → persist →
  publish` 流程落地；不在本 change 内写入。研究 `dataset_id` 由数据快照独立寻址。
- 不受影响：评分语义、`flip_wide_shadow_v1` 注册表内容、现有前瞻窗口计数与 v1 执行语义、
  OpenClaw 契约、backend API 形状。
- 运行环境：本 change 不改变任何 `k8s/` 清单，不启用任何 CronJob。
