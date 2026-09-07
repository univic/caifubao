# Task 4.4 — flip_wide paper-first ≥120 交易日 operator runbook

openspec `strategy-paper-runner` task 4.4：以 paper 模式（只记录不交易）运行
flip_wide 宽书策略，积累 **≥120 个交易日** 的 T+1 安全证据后再谈任何真实执行。
本文是 operator 执行手册；执行需要 dev/prod MongoDB 与数据链可用（同 task-3.3
runbook 前置）。当前因果时序修正的所有新产物都标记为 `evidence_kind=REPLAY`，即使
使用最新日期或 job 成功，也不能计入 120 个前瞻交易日。不可变的前瞻捕获与计数属于
后续独立切片；本 runbook 不授权真实执行或模型升格。

> 项目定位：A 股量化研究/学习/演示 MVP，不构成投资建议。
> 语义约束：策略层永远是「选高买入」（方向只发生在评分构造层）；paper 只记录，
> 不下任何真实订单。本 runbook 不授权任何真实执行。

## 0. 前置条件（与 task 3.3 共用）

1. **注册 flip_wide 影子版本**（若尚未注册）：
   ```bash
   PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.model_registry_runner register \
     --model-version flip_wide_shadow_v1 \
     --description "flip_wide shadow: construction-layer reversal h20 only; research-only, NOT default" \
     --config-json "$(cat datahub/research/autoresearch/h20_excess_alpha/flip_wide_registry_config.json)"
   ```
2. **准备可用分数**：策略可读取同一 signal date、model version 和 horizon 下的
   `PENDING`、`TRACKING`、`VERIFIED`、`INSUFFICIENT_DATA` 分数；`BLOCKED`、`FAILED`
   行不可用。verification 结果不参与排序，也不再要求等待未来结果才作出 paper
   decision。`scoring_runner backfill` 或 replacement 产生的结果仍是 `REPLAY`，不能
   作为前瞻证据。
3. 确认数据链健康：`./scripts/caifubao system health`；每日 quote→signal→scoring
   链路无断链（08-28/08-31 断链教训）。
4. 确认 signal date 是 shared ChinaAStock calendar 的交易日；runner 会严格解析其后
   的下一个交易日为 `execution_date`，不能把 signal date 当作执行日。

## 1. 初始 NAV 与 book 规模匹配（重要）

默认配置 `portfolio_size=800`、`initial_nav=1,000,000` → 每股预算 ≈ 1,250 CNY ≈
1 手零股，多数股票 1 手都买不进（QA 实测仅约一半可买）。**operator 必须按真实 A 股
账户规模选择初始 NAV**，使每股预算 ≥ 数个整手。经验法则：

- 宽书 top-800 等权：目标 NAV ≈ 800 × 每股预算；每股预算取 20,000–50,000 CNY
  （中价股 1–3 手）→ 建议 `initial_nav` 2,000 万–4,000 万 CNY 档（paper 模拟，
  不涉及真实资金）。
- 缩小持股数需要独立研究验证，不能直接继承宽书结论。`--dry-run` 仅预览目标组合，
  不模拟成交；买入率和资金利用率需用 NAV 成交模拟另行核验。

## 2. 每日运行（paper 记录）

按 signal date 的交易日执行，读取上述可用状态，不需要等待 horizon 完结。**示例给出
完整且固定的 `paper_causal_v1` 配置**（此处以 2,000 万 CNY 为例）：

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.strategy_runner run \
  --date 2026-09-04 \
  --config-json '{"timing_version":"paper_causal_v1",
                  "score_model_version":"flip_wide_shadow_v1",
                  "horizon":20,
                  "initial_nav":20000000,
                  "selection":{"mode":"top_percentile","lower":0.20,"upper":1.00,"portfolio_size":800},
                  "constraints":{"exclude_st":true,"exclude_bse":true,"exclude_suspended":true,"max_single_stock_pct":0.05,"min_trade_amount_cny":0.0},
                  "rebalance":{"cadence_days":5},
                  "weighting":"equal",
                  "cash_reserve_pct":0.0}'
```

- `2026-09-04` 在这里仅是 replay 示例；它与历史记录中的 `2026-06-10` 都不是已认证
  的前瞻起点。
- 没有可用分数（四种可用状态均无）→ 记 SKIPPED（freshness 可见，不会伪装成 fresh）。
- 每个交易日一条 `StrategyPaperRun`；`--replace` 可重跑单日。
- 记录 signal date、实际 UTC `decision_at` 和严格晚于 signal date 的下一个
  `execution_date`；规范化 config 固定 `timing_version=paper_causal_v1`，记录带有
  `evidence_kind=REPLAY`。
- runner 的 `SUCCESS` 只表示 job 完成，不能把该记录算作前瞻天数。
- 调度化后应把该命令挂到 CronJob（job_family `strategy_daily`），与 scoring 链
  错开（当日评分完成后记录目标；未来收益验证独立运行，不是策略前置条件）。

## 3. NAV 曲线回算（可每周/每月）

**必须把 §2 的完整配置原样传给 `nav`，并命中同一个 `config_hash`**；可省略仍取相同默认值的字段，但不能
依赖最早 run 的 `initial_nav` 继承。`initial_nav`、`timing_version`、分数源、horizon、
selection、constraints、rebalance、weighting 和 `cash_reserve_pct` 都属于匹配条件：

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.strategy_runner nav \
  --from 2026-03-01 --to 2026-08-31 \
  --config-json '{"timing_version":"paper_causal_v1",
                  "score_model_version":"flip_wide_shadow_v1",
                  "horizon":20,
                  "initial_nav":20000000,
                  "selection":{"mode":"top_percentile","lower":0.20,"upper":1.00,"portfolio_size":800},
                  "constraints":{"exclude_st":true,"exclude_bse":true,"exclude_suspended":true,"max_single_stock_pct":0.05,"min_trade_amount_cny":0.0},
                  "rebalance":{"cadence_days":5},
                  "weighting":"equal",
                  "cash_reserve_pct":0.0}'
```

- `--from/--to` 仍是 signal-date 边界；quote 和 benchmark 加载到所选记录中最后一个
  `execution_date`，再把 execution-day 的 NAV 点挂回其 originating signal run。
- 只读取同一 `config_hash`、同一 `timing_version` 的 COMPLETED replay run；legacy 日期、
  混合配置、重复 execution session 或 execution-day quote 缺失都必须 fail closed，不能
  写入误导性的 curve。
- 开盘订单只能使用 execution date 的 open 或此前已知 mark 来定量；交易完成后才按 close
  做日终估值。停牌或 unknown status 不得转换成可交易。
- 输出含 `initial_nav/terminal_nav/curve_points/benchmark_dates`。同日可交易全市场等权
  基准目前只作诊断比较，不能把 replay NAV 或基准超额当作前瞻认证。

## 4. 进度与记录

- **120 个前瞻交易日计数尚未由本切片启动**：绝不按 `datahub_job_runs` 的
  `strategy_daily` `SUCCESS` 数量计数；SUCCESS 只表示 job 成功，SKIPPED/FAILED 也不能
  变成前瞻证据。
- 只有后续独立切片写入的不可变 forward-capture 记录，且满足固定 config hash、
  `paper_causal_v1`、实际 decision/execution 时间和完整执行日证据，才可进入 120 日计数。
  本切片的所有 `REPLAY`、历史 backfill、replacement 和 NAV recompute 永远不计入。
- 每周可把 replay NAV 曲线、相对基准的超额、回撤、换手记入
  `docs/autoresearch/runs/h20-excess-alpha/manual-experiments-ledger.md`，明确标注
  `REPLAY`；`autoresearch/ledger.jsonl` 仍只记录官方 runner profile run，不混入人工 replay。
- 未来是否进入任何 promote 流程，需要另一个 Spec Gate 和人工审阅；本 runbook 不承诺
  实盘可用性，也不授权 promote。

## 5. 执行阻塞（operator）

- 需要 dev/prod MongoDB 与数据链；写库前需用户显式授权（同 08-31 prod 补跑先例）。
- `flip_wide_shadow_v1` 未注册或 horizon 未覆盖时 runner **fail-closed**（拒绝跑在
  未翻转分数上）——这是设计，不是故障。

## 6. 明确延期的口径缺口

- **cadence / rebalance**：`cadence_days=5` 只是本 replay 配置的记录值；完整的调度、
  缺口日处理与权重再平衡预算仍待独立切片验证。
- **risk**：单票、行业、总仓、换手预算和可用资金等组合风控仍未形成可认证的 paper
  约束；ST/BSE/停牌过滤不等于完整风控。
- **cash sizing**：订单未预先按含费用预算缩量；超出可用现金时仍跳过整笔订单，
  因而不能据目标权重推断真实资金利用率。
- **weight**：当前示例固定等权；集中度、流动性容量和动态权重没有被证明。
- **corporate actions**：除权除息、拆分合并和复权一致性尚未纳入这条证据口径；相关
  NAV 不能用于前瞻认证。
- **daily benchmark**：日频基准的交易日、可交易集合、执行日对齐和数据质量仍待独立
  校验；当前等权基准只支持诊断，不构成已完成的超额评估。
