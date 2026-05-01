## Context

当前仓库的真实技术栈已经是：

- backend: Flask
- datahub: Python 任务与定时运行器
- frontend: Vue 3 + Vite + Pinia + Element Plus
- storage: MongoDB
- delivery: k8s + GitHub Actions

OpenSpec 需要和这套实际结构保持一致，尤其要避免继续把 Django、Celery、AkQuant 这类旧规划写成当前执行目标。

## Architecture

```
datahub  ->  MongoDB  ->  backend API  ->  frontend
   |            |             |
   |            |             +-- 聚合查询、鉴权、简单业务编排
   |            +-- 行情 / 因子 / freshness / 质量数据
   +-- 采集、计算、落库

OpenClaw ---------------> backend API
   +-- 下游消费行情 / 因子 / 信号 / 评分 / 数据质量，用于投资分析
```

### Boundary Rules

- datahub 只负责生产数据，不负责页面展示。
- backend 只负责 API 和轻量聚合，不承担定时采集。
- frontend 只消费 API，不直接依赖 Mongo 集合结构。
- OpenClaw 是下游系统，只通过 backend 的稳定只读 API 消费数据，不直接连接 Mongo。
- caifubao 不承载 OpenClaw 的投资分析逻辑，只负责提供可追溯、可判断 freshness 的数据契约。
- 标的评分由 datahub 生产，backend 只读聚合，frontend 展示分数、解释和闭环追踪结果。
- 评分记录必须以预测为中心建模，而不是以页面展示字段或 Mongo 旧字段为中心建模。

## MVP Strategy

MVP 只保留最小闭环：

1. 登录
2. 市场总览
3. 历史行情
4. 数据质量
5. 简单信号
6. 简单回测

## Next Phase: Multi-horizon Scoring

评分机制当前尚未真正进入生产演示闭环，因此下一阶段可以直接重塑数据模型和接口契约，而不是围绕已有 T+5 字段做保守扩展。

目标是将评分从一个单字段展示能力升级为可追溯的预测系统：

- `Score5` 表示未来 5 个交易日的短线机会强度。
- `Score20` 表示未来 20 个交易日的波段机会强度。
- `Score60` 表示未来 60 个交易日的中期机会强度。

推荐数据建模方式是一条记录表示一次预测：

```
StockScorePrediction
  stock_code
  date
  horizon: 5 | 20 | 60
  score
  rank
  recommendation
  explanation
  input_snapshot
  verification
  model_version
```

这比在旧 `StockDailyScore` 上继续堆 `score_5`、`score_20`、`score_60` 字段更激进，但更适合后续做模型版本、分周期胜率、失败样本分析和下游消费。

实施原则：

- 先规则化，不先引入机器学习训练流水线。
- 评分回放与校准先使用项目内轻量实现，不引入外部交易回测框架。
- 先保存解释和输入快照，再追求复杂因子。
- 每个周期独立配置权重、阈值和有效性标准。
- 历史回放必须防止 look-ahead bias：评分只能读取评估日及之前的数据。
- 评分校准先回答“高分是否更有效”，不要过早扩展成完整交易撮合引擎。
- 验证任务每天更新 `PENDING` / `TRACKING` 记录，不只在到期日一次性回填。
- 高分失败样本必须可复盘，不能只展示成功推荐。
- 评分 API 是稳定契约，Mongo 结构不是外部契约。

### Replay vs Backtest

后续实施中需要区分两层能力：

- Scoring replay / calibration：由 `datahub` 负责，使用历史数据重放 `Score5`、`Score20`、`Score60`，验证评分区分度。第一版用项目内 Python 服务和 pandas/Mongo 查询实现。
- Trading backtest：由 `backend` 负责，面向用户提交的简单交易策略，MVP 只做单股票日线策略。暂不引入 `backtrader`、`vectorbt`、`zipline`、`rqalpha` 等框架。

只有当系统进入多股票组合、调仓、手续费、滑点、仓位约束和撮合规则阶段时，才重新评估外部回测框架。

## Next Phase: OpenClaw Data Access

MVP 闭环稳定后，下一步任务是保障 OpenClaw 可以调取本项目的数据进行投资分析。该阶段的重点不是扩展 caifubao 的分析能力，而是把 caifubao 打造成可靠的数据提供方：

- 提供稳定的股票主数据、日线行情、复权价格、因子、信号、评分和数据质量 API。
- 每个依赖行情的数据响应都要暴露数据日期、生成时间或 freshness 状态。
- 明确 `missing`、`stale`、`not applicable`、`blocked by quote` 等状态，避免下游靠空值猜测。
- 下游只读访问，避免 OpenClaw 触发采集、回填或因子计算任务。
- OpenClaw 使用独立 service token 鉴权，不复用普通用户 JWT，不直连 Mongo。
- service token 只授予只读 scope，backend 记录 request id、token id、endpoint、状态码和 data-as-of，方便追溯投资分析输入。
- 保持 backend API 为集成契约，避免 OpenClaw 依赖 Mongo 集合细节。

## Historical Reference

`openspec/changes/frontend` 保留为早期前端蓝图参考，不再作为当前执行清单。
