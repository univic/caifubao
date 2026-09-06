# Task 4.4 — flip_wide paper-first ≥120 交易日 operator runbook

openspec `strategy-paper-runner` task 4.4：以 paper 模式（只记录不交易）运行
flip_wide 宽书策略，积累 **≥120 个交易日** 的 T+1 安全证据后再谈任何真实执行。
本文是 operator 执行手册；执行需要 dev/prod MongoDB 与数据链可用（同 task-3.3
runbook 前置）。

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
2. **回放/补齐 VERIFIED 分数**：task 3.3 已 backfill 的窗口可直接用；新日期需
   `scoring_runner backfill --model-version flip_wide_shadow_v1 --horizon 20 --from/--to`，
   且评分日期须 **T+1 完结**（预测 date D 约在 D+20 交易日后才被 verify 为 VERIFIED）。
3. 确认数据链健康：`./scripts/caifubao system health`；每日 quote→signal→scoring
   链路无断链（08-28/08-31 断链教训）。

## 1. 初始 NAV 与 book 规模匹配（重要）

默认配置 `portfolio_size=800`、`initial_nav=1,000,000` → 每股预算 ≈ 1,250 CNY ≈
1 手零股，多数股票 1 手都买不进（QA 实测仅约一半可买）。**operator 必须按真实 A 股
账户规模选择初始 NAV**，使每股预算 ≥ 数个整手。经验法则：

- 宽书 top-800 等权：目标 NAV ≈ 800 × 每股预算；每股预算取 20,000–50,000 CNY
  （中价股 1–3 手）→ 建议 `initial_nav` 2,000 万–4,000 万 CNY 档（paper 模拟，
  不涉及真实资金）。
- 或用窄书（top-100/200）降低所需 NAV。**先 `--dry-run` 验证买入率**再正式跑。

## 2. 每日运行（paper 记录）

按交易日（滞后 ≥1 天，取当日已有 VERIFIED 分数的日期）执行。**示例按 §1 要求显式
给出与 book 规模匹配的 `initial_nav`**（此处以 2,000 万 CNY 为例；该值只在 `nav`
回算时生效，`run` 本身只记录目标组合）：

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.strategy_runner run \
  --date 2026-09-04 \
  --config-json '{"score_model_version":"flip_wide_shadow_v1",
                  "horizon":20,
                  "initial_nav":20000000,
                  "selection":{"mode":"top_percentile","lower":0.20,"upper":1.00,"portfolio_size":800},
                  "constraints":{"exclude_st":true,"exclude_bse":true,"exclude_suspended":true},
                  "rebalance":{"cadence_days":5}}'
```

- 无 VERIFIED 分数 → 记 SKIPPED（freshness 可见，不会伪装成 fresh）。
- 每个交易日一条 `StrategyPaperRun`；`--replace` 可重跑单日。
- 调度化后应把该命令挂到 CronJob（job_family `strategy_daily`），与 scoring 链
  错开（score 18:35 → verify 夜间 → strategy 次日）。

## 3. NAV 曲线回算（可每周/每月）

**必须与 `run` 使用相同 `initial_nav`**（§2 示例已包含；省略则退回 1,000,000 默认，
在 800 宽书下大多数股票买不进 1 手，曲线会严重失真）：

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.strategy_runner nav \
  --from 2026-03-01 --to 2026-08-31 \
  --config-json '{"score_model_version":"flip_wide_shadow_v1","initial_nav":20000000}'
```

- 读取窗口内全部 COMPLETED run → 按日期排序成 rebalance schedule → 加载
  StockDailyQuote 价格 + 同日等权基准 → `simulate_paper_nav`（T+1 开盘执行、
  佣金/滑点/印花税/手数/停牌 roll-forward）→ 把每个 curve 点写回对应 run 的
  `nav_snapshot`。
- 输出含 `initial_nav/terminal_nav/curve_points/benchmark_dates`；基准 = 同日
  可交易全市场等权收益（与研究 profile benchmark 口径一致）。

## 4. 进度与记录

- **≥120 个安全交易日**：按 `datahub_job_runs`（job_family `strategy_daily`）的
  SUCCESS 记录数核对；SKIPPED/FAILED 日不计。
- 每周把 NAV 曲线、相对基准的超额、回撤、换手记入
  `docs/autoresearch/runs/h20-excess-alpha/manual-experiments-ledger.md` 与
  `autoresearch/ledger.jsonl`（同 #187/#189 先例），注明生产数据口径。
- 120 天窗口结束后，评估 paper 结果 vs 研究 walk-forward 预期；通过后才进入
  version bump + Spec Gate 的 promote 流程（本 runbook 不授权 promote）。

## 5. 执行阻塞（operator）

- 需要 dev/prod MongoDB 与数据链；写库前需用户显式授权（同 08-31 prod 补跑先例）。
- `flip_wide_shadow_v1` 未注册或 horizon 未覆盖时 runner **fail-closed**（拒绝跑在
  未翻转分数上）——这是设计，不是故障。
