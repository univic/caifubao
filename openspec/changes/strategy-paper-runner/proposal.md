# Strategy Paper Runner (paper-first)

## Why

评分构造层已具备方向版本化（#183）、注册表（#185）与 percentile 校准消费端（#188/
#189），但**评分→策略之间没有生产产物**：没有每日"读生产评分 → 按配置选股 → 输出
目标组合/调仓清单 + 真实成本 NAV 模拟"的闭环。研究已证明 flip_wide 构造层翻转 + 宽书
（top ~20%，~800 只）优于窄书（#177/#187），但该语义只存在于研究候选
（flip_wide.yaml），生产链不消费。

本 change 落地架构文档 4.4 的第 3 步：**paper-first 最小闭环 strategy_runner**——
先只记录不交易；真实执行不在本文档承诺或授权范围内。

## Causal-timing correction and evidence boundary

`strategy-paper-causal-timing` supersedes the pre-correction decision and evidence
wording in this change. The runner consumes usable score rows in
`PENDING`/`TRACKING`/`VERIFIED`/`INSUFFICIENT_DATA`; `BLOCKED` and `FAILED` rows are
excluded, and later outcome verification is never a ranking input. Each decision
records the signal date, the actual UTC `decision_at`, and the strictly later
next-calendar-session `execution_date`. The normalized config includes
`timing_version=paper_causal_v1`, and NAV requires the exact full config and
matching `config_hash`, including `initial_nav`.

Every record produced by this first causal slice is `evidence_kind=REPLAY`,
including current-date runs, replacements, historical backfills, and NAV
recomputations. Replay output cannot count toward the 120-session forward gate.
Immutable forward capture and its counter are a separate later change. The
checked historical gate records for #190/#191/#192 remain historical records of
their earlier scope; they do not certify this correction or a forward start.

## What Changes

- **策略配置（versioned，生产链可复现）**：model_version（分数源，默认指向 flip_wide
  影子分数 `flip_wide_shadow_v1`，可配置任意已注册版本）、selection（top_percentile
  宽书：lower/upper/portfolio_size，或 top_n）、约束（单票 max pct、流动性下限、
  ST/BSE/停牌排除；行业上限为后续切片，schema 暂不包含）、再平衡（默认每周）、排序
  语义固定"选高买入"（方向只在构造层）。未知配置键（含嵌套）一律拒绝，防 typo 静默
  落入默认值。分数源必须显式声明，绝不静默默认。
- **strategy_runner（datahub 每日 job）**：读某日某 model_version 的可用评分
  （`PENDING`/`TRACKING`/`VERIFIED`/`INSUFFICIENT_DATA`，排除 `BLOCKED`/`FAILED`）→
  应用选择与约束 → 输出目标组合与调仓清单；写策略新鲜度记录（沿用 datahub_job_runs
  模式）。决策记录实际 `decision_at` 和下一个交易日 `execution_date`；paper 模式只
  记录，不下任何真实订单。
- **Paper NAV 模拟**：按 `paper_causal_v1` 在下一个交易日开盘执行，真实成本
  （commission/滑点/印花税/手数/T+1/停牌 roll-forward，口径与 autoresearch profile
  execution 一致），只用开盘信息或此前已知 mark 定量，输出每日 NAV/回撤/换手与基准
  对比。
- **产物**：策略目标组合、调仓清单、paper NAV 快照的持久化集合（datahub model）。

## Non-goals

- **不做真实执行/下单/券商对接**；任何未来执行都需另行授权、Spec Gate 和验证，本文档
  不承诺实盘可用性。
- 不改变 scoring 数学、DEFAULT_MODEL_VERSION、decisions API。
- 估值/市值因子暂不进组合（研究 #178 已否决混入 flip_wide；独立验证另行 change）。
- 不改后端 Portfolio 手工记账模型（那是用户手工组合，与 paper 策略产物分离）。
