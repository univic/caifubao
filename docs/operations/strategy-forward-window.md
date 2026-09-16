# Strategy Forward-Evidence Window — Operator Runbook

> 项目定位：A 股量化研究/学习/演示 MVP，**不构成投资建议**。本文只描述如何操作
> **paper**（纸面）证据窗口，不授权任何真实资金执行、不授权 promote。

本文是 `production-capability-roadmap` 0.1 与 `strategy-forward-evidence-capture`
（NEXT.1）的 operator 手册：如何把 `flip_wide_shadow_v1` 的**不可变前瞻纸面证据**
跑到 120 个交易日，以及哪些东西**不算数**。

## 1. 这个窗口是什么

- 只有**在结果未知之前写下**的记录才算前瞻证据。窗口由 `forward certify` 显式开启，
  `start_date` = **认证当天的交易日**，**永不回填**。
- 计数 = 窗口内 `distinct` 的 `FORWARD` + `COMPLETED` signal date（`forward progress`）。
- **不算数**（无论 job 是否 SUCCESS）：`REPLAY` 记录、历史回填、`--replace` 重跑、
  NAV 重算、`2026-06-10` / `2026-09-04` 这类示例日期。
- 窗口按 `(model_version, horizon)` 唯一；配置（含风控参数）变化会改变 `config_hash`，
  使**新产生的 run 归类为 `REPLAY`**。ACTIVE 窗口**不会因为配置变化自动关闭**：它只会
  在 **(a) `strategy forward close` 显式关闭**，或 **(b) 认证一个新窗口**时关闭
  （certify 会先关闭同 `(model_version, horizon)` 的 ACTIVE 前身）。两种情况计数都从
  新窗口重新开始。
- 因此：**改模型或改风控参数 = 该候选的前瞻时钟重新开始**。研究与升格是两条线：
  研究层可以天天迭代，只有你准备上钱的候选才值得占用窗口。

## 2. 开窗前置检查（全部满足才开窗）

- [ ] 影子版本已注册且 ACTIVE：`./scripts/caifubao strategy report <DATE> --model-version flip_wide_shadow_v1`
      能读到 run（注册用 `model_registry_runner register`，见 task-3.3 runbook）。
- [ ] 全市场 replay 与校准对比已执行、结论已回填 `manual-experiments-ledger.md`。
- [ ] 每日数据链健康：`./scripts/caifubao system health`，且当日 quote → signal →
      scoring 无断链（08-28/08-31 断链教训）。
- [ ] **每日运行方式已确定并验证过一天**：手工运行也可以（当前 spec 允许 operator
      cadence），但必须先确认这条命令当天能跑通、产出 `FORWARD` 而不是 `REPLAY`。
- [ ] 已获**显式授权**开窗（见 §5）。
- [ ] `initial_nav` 与真实账户规模匹配：宽书 `portfolio_size=800` 时每股预算过小会导致
      大量标的买不进一手（见 task-4.4 runbook §1）。

> ⚠️ **开窗前最重要的一条**：窗口一旦开启，**漏跑的交易日是永久缺口**（`forward progress`
> 只报告缺口，不伪造）。所以先用一天验证自动化/命令可靠，再开窗——否则会浪费不可重来的
> 120 天时钟。

## 3. 开窗

```bash
./scripts/caifubao strategy forward certify \
  --model-version flip_wide_shadow_v1 --horizon 20 \
  --config-json '{"timing_version":"paper_causal_v1","score_model_version":"flip_wide_shadow_v1","horizon":20,"initial_nav":20000000,"selection":{"mode":"top_percentile","lower":0.20,"upper":1.00,"portfolio_size":800},"constraints":{"exclude_st":true,"exclude_bse":true,"exclude_suspended":true,"max_single_stock_pct":0.05,"min_trade_amount_cny":0.0},"rebalance":{"cadence_days":5},"weighting":"equal","cash_reserve_pct":0.0}' \
  --yes
```

`--yes` 是**强制**的：没有它 CLI 会拒绝执行（certify 是升格门禁，且会关闭同键的 ACTIVE
前身）。`certify` 会打印窗口的 `start_date` / `config_hash`。**记下 `config_hash`**：之后
每天必须用**完全相同的配置**（同哈希）运行，否则记录会变成 `REPLAY`。

暂停或需要重开计数时显式关闭：

```bash
./scripts/caifubao strategy forward close --model-version flip_wide_shadow_v1 --horizon 20
```

## 4. 每日运行与进度

每交易日**在当日评分完成之后**运行（评分未完成时记录 `SKIPPED`，不算证据）：

```bash
./scripts/caifubao strategy run <DATE> --config-json '<与开窗完全相同的 JSON>'
```

- 同日先 `SKIPPED` 后晚间重跑成功 → 该日算 `FORWARD`（设计即如此）。
- 迟跑（`execution_date` 已开盘）→ 自动归类 `REPLAY`，**不计入**。
- 每周记录进度与 NAV：

```bash
./scripts/caifubao strategy forward progress --model-version flip_wide_shadow_v1 --horizon 20
./scripts/caifubao strategy nav --from <START> --to <TODAY> --config-json '<同一 JSON>'
```

把每周的 NAV / 超额 / 回撤 / 换手记入
`docs/autoresearch/runs/h20-excess-alpha/manual-experiments-ledger.md`，并标明证据类型。

## 5. 授权与边界（不可绕过）

- **开窗、每日写库、以及任何 promote 都需要显式人工授权**；本 runbook 不构成授权。
- 窗口只产出**研究级/观察级**输出。未跑满 120 日并对照研究 walk-forward 预期之前，
  模型**不得**被标注为 tradable/actionable。
- **不下任何真实订单**；本路径没有执行能力（roadmap 2.2 起才有，且默认关闭）。
- 对外表述一律保持"研究/学习/演示 MVP，非投资建议"。

## 6. 已知未完成（不要假装已具备）

- **自动化尚未落地**：`strategy_daily` CronJob 未在任何 overlay 中定义，当前是 operator
  cadence。注意 `strategy_runner run --date` 的日期在 CronJob 中需要额外计算，且非交易日
  会以异常结束（fail loud，不是静默跳过）——决定"优雅跳过"还是"接受告警"之前不要挂调度。
- **缺口不可补**：漏一天就是少一天，只能用 `forward progress` 如实记录。
- **paper 不等于真实账户**：目标权重不是你的持仓；卖出定量与对账属 roadmap 1.1/2.3。
- **风控仍有缺口**：换手/再平衡预算未落地（roadmap 1.3 后续切片）。宽书下
  `max_single_stock_pct` 通常不 binding，真正的集中度护栏是 `max_industry_pct`。
