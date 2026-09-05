## Why

H20 研究（#177）证明当前 8 个技术分量在 2019-2026 全 regime 反预测（买 top 全亏），构造层
翻转候选 `flip_wide`（8 分量方向 −1 + 宽书）是唯一全窗口正、decay 0.00 的候选。但生产
scoring 的方向是硬编码的（components 加分、penalties 减分），研究候选无法作为生产评分
模型版本运行——评分引擎需要支持**按模型版本声明分量方向**，才能把 flip_wide 变成可回测/
可执行策略（研究结论的 production bridge）。

## What Changes

- **`get_effective_horizon_config` 支持 per-horizon `directions` 覆盖**：
  `{component_id: -1|0|1}`，解析为完整 per-component map（默认 components +1 /
  risk_penalty −1 = 现语义；key 必须是被评分分量、值 ∈ {-1,0,1}；缺省键保持默认方向）。
  未提供 directions override 时不产生 `directions` 键，分数数学与 develop 完全一致。
- **`score_all_stocks_ranked` 按解析方向应用贡献**：每分量/惩罚的 rank 贡献乘以方向
  （component 默认 +1、penalty 默认 −1，与现行为逐位一致）；持久化 explanation 的
  contribution 符号与分数一致。
- **分数保序不塌缩**：仅钳制上界（≤100），**下界开放**——完整翻转模型（全分量 −1）的
  分数保持带符号负值，横截面可严格排序；默认全正模型分数恒 ≥0 不受影响。语义对齐研究
  评估器（h20_excess_alpha）：原始加权和保留符号，cohort percentile 由该和的排名导出。
- **语义契约（scoring 层）**：flipped-direction model version 是**独立模型**，其分数/
  percentile 含义相对默认版本反转（高分 ↔ 原 bullishness 低）；replay/calibration/
  backtest 对比必须限制在同方向 model version 内。翻转版本经全市场 replay + calibration
  与 baseline 对比后方可 promote（沿用 archived stock-scoring 要求）。`DEFAULT_MODEL_VERSION`
  不变。

## Non-goals

- 不改变 API 契约、鉴权、新鲜度、数据所有权、公开文档、前端/OpenClaw/调度器行为。
- 不改变 raw（非 ranked）评分路径；不改变 recommendation 阈值/calibration 度量口径。
- 不自动 promote 任何新版本；不引入模型注册表（model_version → config 的自动绑定，
  后续单独 change）。
- 不改变「选高买入」usage 语义——翻转发生在构造层，usage 层仍按分数从高到低买。
