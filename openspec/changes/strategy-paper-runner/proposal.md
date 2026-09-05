# Strategy Paper Runner (paper-first)

## Why

评分构造层已具备方向版本化（#183）、注册表（#185）与 percentile 校准消费端（#188/
#189），但**评分→策略之间没有生产产物**：没有每日"读生产评分 → 按配置选股 → 输出
目标组合/调仓清单 + 真实成本 NAV 模拟"的闭环。研究已证明 flip_wide 构造层翻转 + 宽书
（top ~20%，~800 只）优于窄书（#177/#187），但该语义只存在于研究候选
（flip_wide.yaml），生产链不消费。

本 change 落地架构文档 4.4 的第 3 步：**paper-first 最小闭环 strategy_runner**——
先只记录不交易，积累 T+1 证据 ≥120 天后才谈真实执行（本文档不授权任何真实执行）。

## What Changes

- **策略配置（versioned，生产链可复现）**：model_version（分数源，默认指向 flip_wide
  影子分数 `flip_wide_shadow_v1`，可配置任意已注册版本）、selection（top_percentile
  宽书：lower/upper/portfolio_size，或 top_n）、约束（单票 max pct、流动性下限、
  ST/BSE/停牌排除；行业上限为后续切片，schema 暂不包含）、再平衡（默认每周）、排序
  语义固定"选高买入"（方向只在构造层）。未知配置键（含嵌套）一律拒绝，防 typo 静默
  落入默认值。分数源必须显式声明，绝不静默默认。
- **strategy_runner（datahub 每日 job）**：读某日某 model_version 的 VERIFIED 评分 →
  应用选择与约束 → 输出目标组合与调仓清单；写策略新鲜度记录（沿用 datahub_job_runs
  模式）。paper 模式只记录，不下任何真实订单。
- **Paper NAV 模拟**：真实成本（commission/滑点/印花税/手数/T+1/停牌 roll-forward，
  口径与 autoresearch profile execution 一致），输出每日 NAV/回撤/换手与基准对比。
- **产物**：策略目标组合、调仓清单、paper NAV 快照的持久化集合（datahub model）。

## Non-goals

- **不做真实执行/下单/券商对接**（paper-first，≥120 天 T+1 证据后才另行评估）。
- 不改变 scoring 数学、DEFAULT_MODEL_VERSION、decisions API。
- 估值/市值因子暂不进组合（研究 #178 已否决混入 flip_wide；独立验证另行 change）。
- 不改后端 Portfolio 手工记账模型（那是用户手工组合，与 paper 策略产物分离）。
