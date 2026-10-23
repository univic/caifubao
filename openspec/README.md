# OpenSpec — 契约账本索引与写作约定

> 本文件是 `openspec/` 的导航入口，不新增规则。规则权威仍是
> [`RULES.md#spec-gate`](../RULES.md#spec-gate)；项目上下文在
> [`config.yaml`](./config.yaml)；工作流与角色见 [`AGENTS.md`](../AGENTS.md)。
>
> 最后整理：2026-10-23。

## 1. 这个仓库怎么用 OpenSpec

本仓库**没有独立的 `openspec/specs/` 主规格目录**。契约以「change 内的 delta
specs」形式存在，历史账本在 `openspec/archive/`。因此：

- 一份「当前契约」= `openspec/archive/` 中已归档的账本 + `openspec/changes/` 中所有
  已合入 change 的 specs；
- 新工作先建 change，写 `## ADDED Requirements` 之类的 delta，而不是修改别处的
  Requirement 文本；
- 只有 `openspec archive <change>` 才会把 delta 合并进 `openspec/specs/`。本仓库
  尚未启用该目录（见 §7 的归档积压）。

```
openspec/
├── README.md          ← 本文件（索引与约定）
├── config.yaml        ← schema 与项目上下文（会被 CLI 注入到写作提示）
├── changes/           ← 活跃 change：proposal / specs / design / tasks
└── archive/           ← 已归档账本（不要再修改）
```

## 2. 生命周期

1. **Gate**：先按 `RULES.md#spec-gate` 判断是否需要 change。内部重构、测试、格式化、
   等价的小修复不需要。
2. **新建**：`openspec new change <kebab-name>`（会生成 `.openspec.yaml`）。
3. **写作顺序**：`proposal.md` → `specs/` → `design.md`（需要时）→ `tasks.md`。
4. **实现**：按 `tasks.md` 勾选；每个 change 保持单一目标。
5. **验证**：`openspec validate <change> --strict`，合并前 `--all --strict`。
6. **归档**：完成后 `openspec archive <change>`（本仓库当前未执行，见 §7）。

状态口径：

| 标记 | 含义 |
|:---|:---|
| ✅ 任务已勾选 | `tasks.md` 无未完成项；**不等于**已验收或已归档 |
| 🟡 进行中 | 仍有未完成任务 |
| 📋 契约先行 | 只有 proposal/specs，实现未开始（如 `artifact-contract`） |

## 3. 写作约定（与 CLI 1.1.1 对齐）

CI 使用 `@fission-ai/openspec@1.1.1` 执行 `openspec validate --all --strict`。以下
是**会被严格校验**的点：

- `proposal.md` 需要 `## Why`、`## What Changes`、`## Capabilities`（含
  `### New Capabilities` / `### Modified Capabilities`）、`## Impact`。
- proposal 列出的每个新能力，必须在 `specs/<capability>/spec.md` 有对应文件；
  capability 名用 kebab-case。
- delta 文件用 `## ADDED Requirements` / `## MODIFIED Requirements` /
  `## REMOVED Requirements` / `## RENAMED Requirements` 之一或多个。
- 每条 `### Requirement: <名称>` 至少有一个 `#### Scenario:`（**必须 4 个 `#`**）。
- **Requirement 正文的第一行必须包含 `SHALL` 或 `MUST`**——CLI 只把第一行当作规范性
  文本做检查，后续行不参与该判断。这是最容易踩的格式坑。
- `MODIFIED` 要求 `openspec/specs/<capability>/spec.md` 已存在。本仓库没有该目录，
  所以**新增契约一律用 `ADDED`**；等启用归档后再按 `MODIFIED` 写增量。
- `tasks.md` 用 `- [ ]` / `- [x]`，每条控制在约 2 小时；需要跨阶段时拆成
  `## 1. S0 — ...` 这样的段。
- 语言：中文为主，规范句可用英文 `MUST`；金额/费率/口径必须写清单位与来源。

## 4. 领域与能力（capability）命名

能力按「契约边界」命名，不按文件命名。当前活跃能力（括号内为定义它的 change）：

- **数据/管道**：`datahub-runners`（多个）、`datahub-factors`（fq-adj-factor-fix）、
  `datahub-industry-classification`（industry-classification-code-normalization）、
  `datahub-pipeline-degradation`（balanced-core-satellite）、`dev-snapshot-import`、
  `signals-mvp`（datahub-perf-optimization）
- **评分**：`scoring-engine`（scoring-direction-versioning / scoring-model-registry /
  scoring-percentile-rank / datahub-perf-optimization）、`stock-scoring`
  （industry-classification-code-normalization）
- **研究**：`factor-lab-labels`、`factor-lab-metrics`、`factor-lab-oos`、
  `factor-lab-significance`、`autoresearch-profitability`、
  `financial-quality-factors`、`research-ledger-correctness`、
  `research-universe-survivorship`
- **策略/执行**：`strategy-engine`、`strategy-nav-fee-budget`、`strategy-paper-timing`、
  `strategy-forward-evidence`、`strategy-live-loop-minimal`、
  `strategy-paper-research-ledger`、`strategy-portfolio-risk-limits`、
  `strategy-target-export`、`strategy-replay-artifact`、`backtest-mvp`、
  `core-satellite-allocation`、`small-account-execution-plan`、
  `etf-benchmark-paper`、`stock-timing-*`
- **治理**：`environment-model`、`artifact-contract`、`production-capability`、
  `architecture-integrity`

本索引新增（尚未实现）：

- **策略引擎统一**：`strategy-definition`、`strategy-portfolio-ledger`、
  `strategy-risk-gate`、`strategy-execution-model`
  → [`changes/strategy-engine-unification/`](./changes/strategy-engine-unification/proposal.md)

## 5. 活跃 change 索引

共 37 个（36 个既有 + 1 个新增），未完成任务合计 147。按领域分组，组内按 change 名排序。

### 5.1 数据与管道

| Change | 内容 | 主要能力 | 未完成 |
|:---|:---|:---|:---:|
| `datahub-perf-optimization` | 性能/CPU 热点修复与分阶段路线 | `datahub-runners`、`scoring-engine`、`signals-mvp` | 28 |
| `dev-snapshot-import` | 受控快照导入替代在线直连（TASK-404） | `dev-snapshot-import`、`datahub-runners` | 11 |
| `datahub-snapshot-daily-update` | 每日增量快照（UPD）路径 | `datahub-runners` | 0 |
| `datahub-tushare-source` | tushare 数据源（行情/复权/每日指标） | `datahub-runners` | 0 |
| `datahub-tushare-universe` | tushare 全市场股票池 | `datahub-runners` | 0 |
| `fq-adj-factor-fix` | 复权因子改用真实 `adj_factor` | `datahub-factors` | 3 |
| `industry-classification-code-normalization` | 行业分类代码规范化 | `datahub-industry-classification`、`stock-scoring` | 18 |

### 5.2 评分

| Change | 内容 | 主要能力 | 未完成 |
|:---|:---|:---|:---:|
| `scoring-percentile-rank` | 百分位排名评分（ranked 模式） | `scoring-engine` | 2 |
| `scoring-direction-versioning` | 分量方向版本化（flip_wide 的前提） | `scoring-engine` | 0 |
| `scoring-model-registry` | 不可变模型版本注册表 | `scoring-engine` | 0 |

### 5.3 研究

| Change | 内容 | 主要能力 | 未完成 |
|:---|:---|:---|:---:|
| `factor-lab-label-semantics` | 因子实验室标签语义（T+1 可执行） | `factor-lab-labels`、`factor-lab-metrics` | 1 |
| `factor-lab-multiple-testing` | 多重检验校正（Newey-West 与显著性证据） | `factor-lab-significance` | 2 |
| `factor-lab-rolling-oos` | 滚动样本外 | `factor-lab-oos` | 0 |
| `financial-quality-factors` | 财务质量因子（research-only） | `financial-quality-factors` | 0 |
| `h20-autoresearch-replay-semantics` | H20 研究回放前视语义 | `autoresearch-profitability` | 0 |
| `research-ledger-correctness` | 研究账本正确性（P0） | `research-ledger-correctness` | 0 |
| `balanced-core-satellite` | 50k 核心-卫星、降级管道、回放查看器 | `core-satellite-allocation`、`strategy-replay-artifact` 等 6 项 | 0 |
| `small-account-strategy-research` | 小账户策略研究工具 | `small-account-strategy-research` | 0 |

### 5.4 策略与执行

| Change | 内容 | 主要能力 | 未完成 |
|:---|:---|:---|:---:|
| `strategy-engine-unification` | **本索引新增**：一个 Spec、一套撮合、一个账本 | `strategy-definition`、`strategy-portfolio-ledger`、`strategy-risk-gate`、`strategy-execution-model` | 40 |
| `artifact-contract` | 不可变研究工件契约（S2） | `artifact-contract` | 13 |
| `score-backtest-execution-integrity` | 评分驱动回测的执行时序安全 | `backtest-mvp` | 2 |
| `strategy-forward-evidence-capture` | 不可变前瞻证据 + 120 会话计数 | `strategy-forward-evidence` | 1 |
| `strategy-live-loop-minimal` | 最小实盘闭环（涨跌停/对账/熔断） | `strategy-live-loop-minimal`、`strategy-engine`、`strategy-nav-fee-budget` | 2 |
| `strategy-paper-research-ledger` | REPLAY-only 研究账本 | `strategy-paper-research-ledger` | 1 |
| `strategy-paper-runner` | paper-first 策略 runner | `strategy-engine` | 1 |
| `strategy-portfolio-risk-limits` | 组合风控限制（单票/行业/流动性） | `strategy-portfolio-risk-limits` | 1 |
| `strategy-target-export` | 目标组合导出（研究级） | `strategy-target-export` | 1 |
| `strategy-fee-aware-opening-budget` | 费用感知开盘买入预算 | `strategy-nav-fee-budget` | 0 |
| `strategy-paper-causal-timing` | 因果 paper 时序与证据边界 | `strategy-paper-timing` | 0 |
| `etf-benchmark-paper` | 单 ETF paper 基准 | `etf-benchmark-paper` | 0 |
| `stock-timing-evaluator-p0` | 单股择时评估器（成对证据） | `stock-timing-evaluator` | 0 |
| `stock-timing-replay-p1` | 单股择时回放适配器 | `stock-timing-replay` | 0 |
| `stock-timing-pit-evidence-p2` | 单股 PIT 输入证据 P2a | `stock-timing-pit-evidence` | 0 |
| `stock-timing-pit-consumer-p2b` | 单股 PIT 工件消费 P2b | `stock-timing-pit-consumer`、`stock-timing-replay` | 0 |

### 5.5 治理

| Change | 内容 | 主要能力 | 未完成 |
|:---|:---|:---|:---:|
| `production-capability-roadmap` | 研究/MVP → 实盘能力的分阶段闸门 | `production-capability` | 20 |
| `architecture-hardening` | 契约、运行时诚实性与死代码清理 | `architecture-integrity` | 0 |
| `environment-model` | 两轴环境模型（domain × stage） | `environment-model` | 0 |

> 「未完成」只统计 `tasks.md` 中未勾选的复选框。任务全勾选不代表已验收或已具备
> 前瞻证据；以各 change 的 specs 与运行手册为准。

## 6. 归档账本

| 账本 | 内容 | 状态 |
|:---|:---|:---|
| [`archive/mvp-quant-demo/`](./archive/mvp-quant-demo/) | MVP 契约账本（11 份 spec） | 已归档，只读 |
| [`archive/frontend/`](./archive/frontend/) | Django/Celery/AkQuant 旧架构 | 历史参考 |
| [`archive/homepage-20260321/`](./archive/homepage-20260321/) | 首页提案 | 未实施 |

归档账本**不要修改**。新契约写在活跃 change 里；若必须纠正历史，另开 change 说明。

## 7. 已知整理缺口

1. **归档积压**：20 个 change 的 `tasks.md` 已全部勾选但未 `openspec archive`。首次归档
   会创建 `openspec/specs/` 并合并全部 delta，属于结构性动作，建议单开一个
   `chore/openspec-archive` 分支执行，不与内容变更混在一起。
2. **实现进度与任务勾选不完全等价**：`artifact-contract`、`production-capability-roadmap`
   等以契约先行，任务勾选不代表能力已上线。
3. **本索引是人工维护的**：新增/归档 change 后必须同步 §5 的表与 §4 的能力列表；CI 只
   校验 change 自身的格式，不校验本索引是否过期。
4. **早期 proposal 没有 H1**：部分 change 的 `proposal.md` 直接以 `## Why` 开头（不是
   CLI 错误）。§5 中的一句话说明由本索引补充，未改动原文标题。

## 8. 验证

```bash
# CLI 版本与 CI 对齐（CI 用 1.1.1）
npx @fission-ai/openspec@1.1.1 validate --all --strict

# 单个 change
npx @fission-ai/openspec@1.1.1 validate <change-name> --strict
```

CI 在 `openspec/**` 变更时运行 `openspec-check`（见 `.github/workflows/ci.yml`）。
