# Strategy Target Portfolio Export (roadmap 2.1)

## Why

`production-capability-roadmap` 2.1：A 模式（人工确认后自行下单）需要一份**人工可核对**
的每日「目标持仓 + 调仓清单」。当前 `strategy-paper-runner` 已经把每日目标写入
`StrategyPaperRun.target_holdings` / `rebalance`，但 operator 侧唯一的读取命令
`strategy_runner report` 只输出 `target_holdings_count`（计数），**拿不到逐标的的
权重、金额与买卖方向**。人工执行因此没有可用产物：知道"有 800 只"，不知道"买什么、
卖什么、买多少"。

本 change 补这条只读导出路径。它不改任何选择/评分/NAV 语义，不写库，不下单，不启动
前瞻窗口，也不承诺实盘可用性。

## What Changes

- 新增只读命令 `strategy_runner export`：读取指定 signal date 的 COMPLETED paper run，
  输出逐标的「目标持仓 + 调仓清单」——`BUY`（本轮新增）、`SELL`（本轮移出）、
  `HOLD`（未变动）。每行含该 signal date 的 score / percentile（缺失即为 null，
  不臆造）与理由；有目标权重的行按基准 NAV 折算目标金额。
- `BUY`/`SELL`/`HOLD` 只描述该 run 的调仓 diff，**不是**推荐、不是下单指令，
  导出不得被表述为 actionable 或 tradable。
- `SELL` 行**不给出卖出数量/金额**：paper track 的目标权重不是操作者真实账户的持仓，
  卖出定量属于 roadmap 1.1/2.3（DecisionJournal / 对账）的职责，本 change 不代劳。
- 输出**无条件**携带研究级/观察级标注与"非投资建议"免责声明；该标注不得由
  `evidence_kind`、前瞻交易日计数、`forward progress`、job 状态或 NAV 记录推断。
  roadmap 0.3 对任何模型版本都尚未满足，因此当前所有导出都是研究级。
- 支持 `--format csv`（默认，人工对照表）与 `--format json`（完整结构），
  `--output PATH` 可落盘。CSV 产物**自身**以首行 `#` 注释携带标注与免责声明
  （便于单独流转），元数据另在 stderr 打印完整副本。
- 同名同期的多个 COMPLETED `config_hash` 视为歧义，导出 fail closed 而不是默默挑选
  最新一条。
- 只读约束：不写任何集合、不改 paper run / 前瞻证据、不触发调度或下单。

## Non-goals

- 不做组合/风控规则引擎（roadmap 1.3 独立切片）；本 change 不新增或强制任何风险约束。
- 不做卖出/仓位定量：paper 目标权重不等于操作者真实持仓，卖出定量与对账属于
  roadmap 1.1/2.3。
- 不做券商适配器、对账、kill-switch、真实执行（roadmap 2.2-2.4）。
- 不落任何审计/决策记录（roadmap 1.1）；本命令只读，不写库、不建 job run。
- 不改评分/方向/选择/再平衡/NAV 语义，不改 `StrategyPaperRun` 既有字段语义，
  不回写任何历史记录。
- 不 promote 任何模型版本，不开前瞻窗口，不改变合规对外定位；不在 backend /
  OpenClaw 暴露该导出。
