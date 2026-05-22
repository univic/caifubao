## Why

财富宝当前已经具备行情采集、因子计算、数据质量检查和基础页面壳子，但 OpenSpec 仍停留在早期“大前端蓝图”阶段，和当前 Flask + datahub + Vue 3 的 MVP 实施状态不一致。我们需要把规格收束到真正会在两周内交付的闭环上，避免文档继续偏离代码。

## What Changes

- 新增一个面向当前 MVP 的 OpenSpec change
- 明确 datahub、backend、frontend 的职责边界
- 用最小规格覆盖数据质量、datahub 运行器、简单信号和简单回测
- 将标的评分从单一 T+5 MVP 草案升级为 `Score5`、`Score20`、`Score60` 多周期预测与闭环追踪机制
- ~评分机制尚未成为生产依赖，允许用更激进的模型重塑替代字段级兼容~ ✅ 已完成（`StockScorePrediction` 模型 + `scoring_engine` 服务 + API + 前端）
- ~评分结果具备结构化解释、输入快照、模型版本和按周期验证结果~ ✅ 已完成
- ~评分历史回放与校准作为评分第一阶段核心能力，用项目内轻量实现~ ✅ 已完成
- ~评分校准不同于完整交易回测；MVP 阶段不引入外部回测框架~ ✅ 已完成
- 补充 OpenClaw 作为下游消费者的数据访问规格，明确 caifubao 提供稳定数据 API，OpenClaw 负责投资分析
- 补充 OpenClaw service token 鉴权方案，明确只读 scope、hashed token 存储、请求审计、过期与吊销预期
- 补充 Karpathy autoresearch skill suite 的项目适配规格，用于把评分、因子、阈值和策略研究收束为可验证实验循环
- 明确 autoresearch 只能优化研究指标，不能生成收益承诺、投资建议或绕过全市场验证
- 将旧 `openspec/changes/frontend` 视为历史参考，而不是当前执行清单

## Scope

- `openspec/config.yaml`
- `openspec/changes/mvp-quant-demo/*`

## Non-goals

- 不重写旧的 `openspec/changes/frontend`
- 不把 OpenSpec 扩展成完整平台规划
- 不引入新基础设施或新的微服务拆分
- 不把 OpenClaw 的分析逻辑内置到 caifubao
- 不直接暴露 Mongo 集合作为 OpenClaw 的集成契约
- 不复用普通用户 JWT 作为 OpenClaw 的服务间鉴权方式
- 不在 MVP 阶段引入完整 OAuth/OIDC client-credential 流程，除非后续安全评审明确要求
- 不在评分第一版引入机器学习训练流水线；先用可解释、可回放、可校准的规则评分闭环积累样本
- 不把评分机制伪装成收益承诺；评分表示机会强度，必须展示验证口径和风险状态
- 不在评分回放/校准阶段引入 `backtrader`、`vectorbt`、`zipline`、`rqalpha` 等完整交易回测框架
- 不在评分第一阶段实现组合调仓、撮合、手续费、滑点、仓位约束或策略参数网格优化
- 不允许 autoresearch 自动修改生产默认模型版本、公开 API 契约、鉴权、OpenClaw 集成或部署配置
