# Caifubao 环境模型（Environment Model）

> 状态：**权威文档（authoritative）**。本文是 Caifubao 环境、数据所有权与制品流
> 的统一参考；其他文档与本文冲突时，以本文为准。发现冲突请修正对应文档或在本文
> 补充说明，而不是在别处再造一套口径。
>
> 对外定位保持不变：**research / learning / demonstration MVP, not investment
> advice（研究 / 学习 / 演示 MVP，不构成投资建议）。** 本系统当前**没有任何真实
> 资金执行能力**（见 [§10](#10-prod-未启用声明)）。

## 1. 为什么需要两轴

历史上本仓库只有一套单轴环境词汇：`development` / `production`。它把两个正交的
问题压在了一个名字里：

1. **这段软件/数据运行在生命周期的哪个成熟度？**（迭代 Playground，还是长期稳定
   运行？）
2. **它是否接触真实资金？**（崩了能重来，还是会亏钱？）

结果就是 `production` 一词同时指代「长期稳定的研究运行环境」和「真实资金执行」，
导致文档把 research stable 误写成实盘 production。本仓库的 `production` 命名
（GitHub Environment、镜像 tag、K8s namespace、部署输入）全部是**历史/技术名称**，
不代表业务语义上的实盘能力。

因此引入**两轴模型**：

- **domain（职责轴）**：这段运行时负责什么、拥有哪些数据、风险边界在哪。
- **stage（成熟度轴）**：这段运行时的运行成熟度，以及它对真实资金的影响。

`stable ≠ production`。`production` 只用于 `trading/production`（真实资金阶段），
或在显式标注为 GitHub Environment / 历史名称 / 镜像 tag / 应用配置常量时出现。

## 2. domain 轴：职责、数据所有权与风险边界

| domain | 职责 | 拥有的数据（目标态） | 风险边界 |
|:---|:---|:---|:---|
| **development** | 软件平台开发、API/UI/Schema/部署集成验证 | 丢弃型/抽样数据，受控快照导入 | 无真实资金；无真实券商凭证；不得直接依赖 research/trading 在线库 |
| **data** | 外部市场数据采集、标准化、校验、发布（`fetch → normalize → validate → persist → publish`） | 权威市场数据与外部原始数据：行情（个股/指数）、交易日历、股票主数据、外部 `adj_factor`、估值/基本面参考数据、freshness/发布状态 | 不产生投资观点；不做因子/评分/策略 |
| **research** | 因子、评分、模型、策略、回测与验证证据 | 因子定义与评估、`stock_factor_daily`、评分预测、实验、回测、模型版本、研究工件 | 无真实资金；无真实券商凭证；不得直接写 trading 状态 |
| **trading** | 风险控制、订单、成交、持仓、资金、对账 | 策略发布/实例、订单与回报、成交、持仓、资金流水、风控状态、对账记录 | 唯一允许接触真实资金与券商凭证的 domain；必须默认关闭、显式授权 |

`data` 是职责边界，不要求第四个 K8s namespace：迁移期内 data 职责由稳定运行
环境中的 datahub 服务承载（见 [§8 迁移期实现位置](#8-迁移期实现位置)）。

## 3. stage 轴：运行成熟度与资金影响

| stage | 含义 | 资金影响 | 数据策略 |
|:---|:---|:---|:---|
| **dev** | 快速迭代、可随时重建 | 无 | 可丢弃、可抽样、可受控快照导入 |
| **stable** | 长期稳定运行，**不涉及真实资金** | 无 | 权威/长期数据，有备份 |
| **paper** | 模拟交易（订单路径被模拟/记录） | 无真实资金 | 模拟账本与 paper 记录 |
| **production** | 真实资金执行 | **真实资金** | 强一致账本、审计、对账 |

## 4. 有效组合与无效/不推荐组合

| 组合 | 判定 | 说明 |
|:---|:---|:---|
| development / dev | ✅ 有效 | 即运行目标 `dev` |
| research / stable | ✅ 有效 | 即运行目标 `research` |
| trading / paper | ✅ 有效（目标态） | 即运行目标 `trading-paper`；当前仅有清单骨架，未启用 |
| trading / production | ✅ 有效（目标态） | 即运行目标 `prod`；**当前未启用** |
| data / stable | ✅ 有效 | 权威数据发布环境（迁移期由稳定环境内的 datahub 承载） |
| research / paper | ⚠️ 不推荐 | paper 订单语义属于 trading domain；research 只做回测/模拟证据，不应自建 paper 订单簿 |
| development / stable | ⚠️ 不推荐 | dev 的价值在于可重建，不要把长期状态沉淀在 dev |
| development / production、research / production、data / production | ❌ 无效 | 除 trading 外任何 domain 不得处于 production stage（真实资金） |
| trading / dev | ❌ 无效 | 涉资 domain 不允许「快速迭代/可丢弃」成熟度 |

## 5. 运行目标（running targets）

| 运行目标 | 两轴定位 | 当前状态（2026-09-16） | 目标状态 |
|:---|:---|:---|:---|
| **`dev`** | development / dev | ✅ 已运行（CLI 默认 namespace `caifubao-dev`）。数据目前经 `data sync` 从旧 stable 在线库只读同步 | 受控快照导入/重建，不再直连 research/stable 在线库（TASK-404，未完成） |
| **`research`** | research / stable | ✅ 已运行（TASK-303 切流已落地：research overlay + 受保护的 `research` GitHub Environment）。承载因子/评分/回测/研究工作负载 | 同左，且完成 Data/Research 数据所有权拆分（P0-5/TASK-401..404） |
| **`trading-paper`** | trading / paper | ❌ 未启用。当前只有 default-deny NetworkPolicy 的清单骨架，无工作负载 | paper 交易运行时（模拟撮合、账本、对账） |
| **`prod`** | trading / production | ❌ **不存在、未启用** | 未来实盘运行目标；须满足 §11 门槛 |

旧 **stable**（历史名 `production`）：**已退役（retired production）**，
TASK-303 切流后不再有部署路径，冻结保留用于回滚。迁移期内它仍承载每日
quote→signal→scoring 例行任务，并作为 `dev` 的 `data sync` 只读来源；这是
**迁移期遗留**，不是目标架构，由 TASK-404 关闭。它**不是**未来的实盘 `prod`，
也**不属于** trading domain。

## 6. 当前状态 vs 目标状态

| 维度 | 当前（迁移期） | 目标 |
|:---|:---|:---|
| dev 数据来源 | 在线直连旧 stable Mongo（`MONGODB_SRC_*` 只读） | Data 发布 → 受控快照导入 dev（可抽样） |
| 数据权威环境 | 旧 stable（retired production）仍在采集发布 | data domain（由 datahub 服务承载）发布版本化数据资产 |
| `stock_factor_daily` / FQ/HFQ | 实现位于 datahub 模块 | research 拥有派生因子数据（实现位置随后续迁移调整） |
| research 与 trading 关系 | trading 未启用 | research → 不可变策略工件 + 验证证据 → trading promotion |
| 实盘 | 不存在 | `prod`（trading/production），默认关闭 + 显式授权 + 全套执行门禁 |
| 部署输入 | `dev` / `research`（`development`/`production` 仅作过渡别名，`production` 已退役并 fail loudly） | 同左 |
| GitHub Environment | `development`、`research`（`production` 仅剩 tailscale operator bootstrap 在用）；publish workflow 的 job 级 environment 仍按分支解析为 `development`/`production`（漂移项，未修复） | 与 domain/stage 语义对齐（见 §13） |

## 7. 数据所有权

| 数据 | 所有 domain | 说明 |
|:---|:---|:---|
| 行情（个股/指数）、交易日历、股票主数据（`basic_stock`）、外部 `adj_factor`、daily_basic 等外部参考数据、freshness/`data_asset_status` | **data** | 权威市场数据与外部原始数据；以版本化发布契约对外提供 |
| `stock_factor_daily`、因子定义/评估、`stock_signal_daily` | **research** | 派生因子数据。迁移期实现位于 datahub 模块（见 §8） |
| `stock_score_predictions`、评分实验、模型版本注册表、回测/验证工件 | **research** | |
| 策略发布/实例、订单、成交、持仓、资金流水、风控、对账 | **trading** | 目标态 PostgreSQL 账本；当前 trading 未启用 |

## 8. 迁移期实现位置

以下**实现位置**是迁移期现状，**不改变** §7 的目标所有权：

- `stock_factor_daily`、FQ/HFQ 派生逻辑、`stock_signal_daily` 等仍由 `datahub/`
  模块计算和写入（`fq-adj-factor-fix` 等既有契约仍以 datahub 为当前实现位置）；
- dev 的 `data sync` 仍直连旧 stable Mongo（TASK-404 完成前）；
- 旧 stable 仍承载每日采集/信号/评分调度。

凡引用这些现状的文档，必须标注「迁移期遗留」，不得写成目标架构。

## 9. FQ/HFQ：外部输入与派生数据职责

- **外部输入**：复权因子 `adj_factor` 来自外部数据供应商，属于 **data domain 的
  外部原始数据**，由 data 职责负责采集、校验与发布。
- **派生数据**：FQ/HFQ 价格是由「原始行情 × 外部复权因子」派生的**研究数据**，
  目标所有者是 **research**。
- **独立重算要求**：research 必须能在**自己的数据与凭证边界内**独立完成 FQ/HFQ
  派生重算——只要拿到 data 发布的行情与 `adj_factor`，research 就能自行重建
  FQ/HFQ 历史。**不得**要求 prod（或任何 trading/旧 stable 运行时）先计算再同步
  给 research/dev。
- 迁移期内该派生逻辑仍由 datahub 模块实现并写入 `stock_factor_daily` /
  `stock_daily_quote` 的 hfq 字段；这是实现位置，不是所有权变更。

## 10. `prod` 未启用声明

**`prod`（trading/production）当前不存在、未启用。**

- 当前系统**没有**真实券商执行能力：无券商适配器、无订单幂等、无成交/资金/持仓
  对账、无 kill-switch。
- GitHub `production` Environment、`prod`/`latest` 镜像 tag、旧 stable namespace、
  `APP_ENV=PRODUCTION`、`k8s/overlays/example-production` 都只是**技术名称/历史
  名称/示例名称**，不代表实盘能力。
- 任何文档不得把当前 research stable 或旧 stable 写成「已用于实盘」。
- 对外表述一律保持「research / learning / demonstration MVP, not investment
  advice」。

## 11. 制品流、promotion 与实盘门槛（摘要）

```text
data: fetch → normalize → validate → publish（版本化数据资产）
                  │
                  ▼
research: 因子 → 评分/模型 → 回测/验证 → 候选策略工件（含证据）
                  │  promotion：仅通过不可变策略工件 + 验证证据
                  ▼
trading: 风控 → 订单（幂等） → 成交/持仓/资金 → 对账 → 审计
```

规则（本节为摘要；**规范性来源**是
[`openspec/changes/production-capability-roadmap/`](../../openspec/changes/production-capability-roadmap/proposal.md)）：

1. **promotion 只走不可变工件**：research 只能向 trading promotion 不可变策略
   制品及验证证据（git SHA、镜像 digest、模型版本/哈希、数据快照、验证指标、
   审批状态）。**不得**通过复制 research 数据库或运行时调用来「发布」策略。
2. **trading 独立运行**：trading 必须在 research 服务不可用时仍可独立运行；
   不得依赖 research 在线库、DSH、LiteLLM 等。
3. **实盘门槛**：`prod` 的启用前提（默认关闭 + 显式授权、不可变发布、风控、
   订单幂等、成交/资金/持仓对账、审计与 kill-switch、≥120 个不可变前瞻纸面
   交易日证据等）以 production-capability-roadmap 的 requirements 为准；
   replay/backfill/replacement/NAV recompute/job SUCCESS 均不构成前瞻证据。
4. **禁止事项**：
   - dev 与 research **不得持有真实券商凭证**，不得访问券商实盘端点；
   - research 不得直接写 trading 状态；
   - 不得用 repo dispatch 自动 promote 到实盘；
   - 不得把 paper/replay 输出表述为可交易建议。

## 12. dev 快照导入边界

- dev 是软件平台开发/集成环境，**不是**正式策略研究环境；研究结论不应以 dev
  数据为准。
- dev 数据**可以丢弃、抽样，或通过受控快照导入**；目标态 dev **不得**直接依赖
  research/trading 在线数据库。
- 当前 `data sync`（旧 stable 在线库 → dev 在线直连）是 **TASK-404 完成前的迁移
  期遗留**；目标流程为「data/stable 环境生成受控快照 → dev 导入/恢复」，并配
  显式工具（控制样本规模、可重复、可校验）。
- 需要真实规模测试数据时：从稳定环境的**备份/导出**（MongoDB 归档或 Parquet
  数据湖导出）做受控导入，而不是让 dev 直连在线库。

## 13. 技术名称 ≠ 业务语义

| 名称 | 类型 | 语义 |
|:---|:---|:---|
| GitHub Environment `development` | 技术名称 | dev 域部署的 secrets/RBAC 作用域；不代表成熟生产 |
| GitHub Environment `production` | 技术名称/历史名称 | 仅剩 tailscale operator bootstrap 在用；publish workflow 的 job 级 environment 仍用它取 secrets（漂移项）。**不代表实盘** |
| GitHub Environment `research` | 技术名称 | research stable 部署作用域；**不代表实盘** |
| 镜像 tag `prod` / `latest` | 技术名称 | `main` 分支构建的 channel tag，部署目标是 research stable；**不代表实盘** |
| 镜像 tag `develop` | 技术名称 | `develop` 分支构建的 channel tag，部署目标是 dev |
| namespace `caifubao-dev` | 技术名称 | dev 运行目标（CLI 默认 namespace） |
| 部署输入 `development`/`production` | 过渡别名 | 部署输入现为 `dev`/`research`；旧值仅作过渡别名（`production` 已退役并 fail loudly） |
| `APP_ENV=PRODUCTION` | 技术名称 | 应用进程配置常量，与部署环境/资金阶段无关 |
| `k8s/overlays/example-production` | 示例名称 | 公共仓库的脱敏示例 overlay |

## 14. 快速问答

- **dev / research / trading-paper / prod 分别负责什么？**
  dev = 软件平台开发与集成（development/dev）；research = 因子、评分、模型、策略
  与验证证据（research/stable）；trading-paper = 模拟交易（trading/paper，未启
  用）；prod = 真实资金执行（trading/production，未启用）。
- **哪些环境允许真实券商凭证？** 只有 trading domain（且 production stage 还需
  满足 §11 门槛）。dev 和 research 一律不允许。
- **哪个环境负责策略研究？** `research`（research/stable）。
- **哪个环境负责真实执行？** 未来 `prod`（trading/production）；当前**不存在**。
- **当前 prod 是否已经存在？** 不存在。当前没有任何真实资金执行能力。
- **FQ/HFQ 在哪里可以独立重算？** research——在自己的数据与凭证边界内，用 data
  发布的行情与外部 `adj_factor` 自行派生（见 §9）。
- **dev 如何获得真实规模测试数据？** 目标态通过受控快照导入（备份归档/Parquet
  导出 → dev 恢复）；当前在线直连旧 stable 的 `data sync` 是 TASK-404 前的迁移
  期遗留（见 §12）。

## 15. 相关文档

- [`openspec/changes/environment-model/`](../../openspec/changes/environment-model/proposal.md)
  （本模型的契约记录）
- [`openspec/changes/production-capability-roadmap/`](../../openspec/changes/production-capability-roadmap/proposal.md)
  （实盘能力路线图：promotion 与执行门槛的规范性来源）
- [`docs/capability-inventory.md`](../capability-inventory.md)（能力清单）、
  [`docs/operations/agent-cli.md`](../operations/agent-cli.md)（dev 运维 CLI）、
  [`docs/README.md`](../README.md)（文档索引）
- 私有仓库 `caifubao-private`（不链接，避免指向私有资产）：`docs/architecture/
  target-architecture.md`（目标架构）、`docs/architecture/migration-contract.md`
  （迁移契约）、`docs/architecture/refactoring-roadmap.md`（迁移路线图，含
  TASK-303/TASK-404）。
