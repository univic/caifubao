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
  percentile 含义相对默认版本反转（高分 ↔ 原 bullishness 低）。同一 score basis 的模型
  可以比较原始分；不同 direction/basis 的 promotion comparison 必须统一使用 percentile。
  翻转版本经全市场 replay + calibration 与 baseline 对比后方可 promote（沿用 archived
  stock-scoring 要求）。`DEFAULT_MODEL_VERSION` 不变。
- **校准与实验报告支持 signed score**：分桶基准由版本化 config 决定；存在非惩罚分量翻转
  时使用持久化 percentile（0-100），而不是依赖本次窗口是否恰好出现负分。跨 basis 比较
  两侧统一为 percentile，并禁止报告无意义的 raw-score delta。
- **同步 API 与前端显式呈现 basis**：实验运行报告返回 `bucket_basis`，比较接口返回
  `comparison_basis` / `comparison_status`；缺失、非有限或越界 percentile 以稳定 422
  响应拒绝，前端按返回 basis 标注“原始分/百分位分桶”。异步校准任务以清理后的领域错误
  标记失败，不暴露 traceback。

## Non-goals

- 不改变鉴权、新鲜度、数据所有权、OpenClaw 或调度器行为。
- 不改变 raw（非 ranked）评分路径；不改变 recommendation 阈值，校准仅把 signed model
  的 scale-dependent 度量切换到已有 percentile 口径。
- 不自动 promote 任何新版本；不引入模型注册表（model_version → config 的自动绑定，
  后续单独 change）。
- 不改变「选高买入」usage 语义——翻转发生在构造层，usage 层仍按分数从高到低买。
