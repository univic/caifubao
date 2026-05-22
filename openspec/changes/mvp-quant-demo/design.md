## Context

当前仓库的真实技术栈已经是：

- backend: Flask
- datahub: Python 任务与定时运行器
- frontend: Vue 3 + Vite + Pinia + Element Plus
- storage: MongoDB
- delivery: k8s + GitHub Actions

OpenSpec 需要和这套实际结构保持一致，尤其要避免继续把 Django、Celery、AkQuant 这类旧规划写成当前执行目标。

## Architecture

Module boundaries are defined in `RULES.md#module-boundaries`. Summary:

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

## Multi-horizon Scoring (Implemented ✅)

评分引擎已经实现 `StockScorePrediction` 模型，覆盖 Score5/20/60 三个周期。实施原则全部满足：

- 规则化评分（7 组件 × 3 周期），无 ML 训练流水线。
- 项目内轻量回放/校准实现（`ScoreReplayService`, `ScoreCalibrationReport`）。
- 每条预测包含解释和输入快照。
- 每个周期独立配置权重、阈值和有效性标准（`config.py`）。
- 回放防止 look-ahead bias（`date__lt` 严格查询）。
- 校准回答"高分是否更有效"（top-N 聚合、假阳性/假阴性筛查）。
- 验证任务每天更新 `PENDING`/`TRACKING` 记录。
- 评分 API 是稳定契约（`/api/v1/integrations/openclaw/scores`）。

未完成：全市场回填 + 新旧模型版本校准对比 (12d.5)。

### Replay vs Backtest

后续实施中需要区分两层能力：

- Scoring replay / calibration：由 `datahub` 负责，使用历史数据重放 `Score5`、`Score20`、`Score60`，验证评分区分度。第一版用项目内 Python 服务和 pandas/Mongo 查询实现。
- Trading backtest：由 `backend` 负责，面向用户提交的简单交易策略，MVP 只做单股票日线策略。暂不引入 `backtrader`、`vectorbt`、`zipline`、`rqalpha` 等框架。

只有当系统进入多股票组合、调仓、手续费、滑点、仓位约束和撮合规则阶段时，才重新评估外部回测框架。

## OpenClaw Data Access (Implemented ✅)

OpenClaw 集成已完成，caifubao 作为可靠数据提供方的全部要素已就位：

- 提供稳定的股票主数据、日线行情、复权价格、因子、信号、评分和数据质量 API（9 端点）。
- 评分端点包含解释、输入快照新鲜度、验证指标。
- 独立 service token 鉴权（SHA-256 哈希，双 scope），不复用用户 JWT。
- 所有 compute/mutation 端点对 service token 返回 403。
- 标准化响应格式（request_id、generated_at）。

未完成：
- `data_as_of` 字段从未在响应中填充（10.3）。
- 无速率限制保护（10.8）。
- 无持久化审计日志表，仅覆盖写 last_used_at/last_used_ip。

## Production Readiness Gap

当前项目已实现研究闭环的核心骨架（多周期评分、带摩擦回测、OpenClaw 集成、前端展示），
但距离真正在市场上盈利还差四个阶段的跨越。以下按工作量递增排列，每阶段映射到现有
`tasks.md` 中对应的任务编号。

### 阶段 A：研究可靠性（4–6 周）

验证当前评分和策略是否真的有预测力，而不是回测过拟合。

| 能力 | 对应任务 | 现状 |
|:-----|:---------|:-----|
| 因子评估管线 | 15.1–15.12 | ✅ FactorEvaluationService 已实现（IC/ICIR/五分组/相关性/衰减），✅ tech_factor_runner CLI 可用。❌ 市场状态分类器(15.12)未实现。❌ 前端仪表板(15.11)未建。 |
| 市场状态分类 | 15.12 | ❌ 未实现。牛/熊/震荡市分类是所有分状态报告的前置依赖。 |
| 滚动窗口验证 + Bootstrap | 17.1–17.7 | ✅ 滚动验证(17.1)、衰减检测(17.2)、permutation test + bootstrap CI(17.5) 已实现。❌ 市场状态分拆(17.3)、稳定性检验(17.4)、景观可视化(17.6)、最终推荐(17.7)未实现。 |
| 反过拟合护栏 | 13.6–13.8 | ✅ composite ranking(13.6)、集中度检测、流动性过滤已实现。❌ Bonferroni 校正(13.7)未实现。✅ 交易可执行性约束(13.8)已实现。 |
| 全市场校准 | 12d.1, 20.2 | ✅ 混合分位数+绝对阈值已实施(12d.1)。❌ 全市场回填 + 新旧模型对比(12d.5)未执行。❌ 20.2 全市场成功标准未验证。 |

### 阶段 B：执行基础设施（6–8 周）

从 "信号研究实验室" 跨越到 "交易执行手术台"。

| 能力 | 说明 |
|:-----|:-----|
| 券商 API 对接 | A 股市场可选 QMT（迅投）、CTP/XTP、PTrade；需要委托下单、撤单、成交回报、持仓查询 |
| 实时行情接入 | 当前评分引擎是日线级别，实盘需要盘中数据流（至少分钟线）支持日内执行决策 |
| 订单管理 + 成交回报 | 从下单到成交到持仓更新的完整生命周期，含委托状态机、部分成交、废单处理 |
| 持仓追踪 + P&L 归因 | 实盘持仓 ≠ 回测持仓；需要每日 mark-to-market、已实现/未实现盈亏、分股票归因 |
| 组合级风险管理 | 从单票 stop-loss 升级为 VaR 约束、相关性约束、行业集中度限制、回撤熔断、黑名单 |

> 阶段 B 在 `tasks.md` 中无对应条目，属于当前 scope 之外的全新能力。

### 阶段 C：生产化部署（4 周）

从单机脚本到可运维的生产系统。

| 能力 | 对应任务 | 说明 |
|:-----|:---------|:-----|
| Compute Worker 生产部署 | 11.1–11.6 | ✅ 评分/回填/校准/验证 worker 已实现 8 种 handler + K3s Deployment manifest。❌ 11.6 node-role 文档未写。 |
| 数据质量自动监控 + 告警 | 2.1–2.3, 10.3–10.4 | ✅ 评分质量监控 `/api/decisions/quality` 已实现（命中率、偏移、漂移）。❌ `data_as_of` 未填充(10.3)。❌ 无持久化审计日志。 |
| 决策日志 + 复盘工具 | 18.6–18.12 | ❌ DecisionJournal 模型未建。✅ 评分预警(18.3)和质量监控(18.4-18.5)已实现。❌ 日志追踪/归因/再平衡预览(18.6-18.12)未实现。 |
| 监控 / 告警 / 灾备 | — | ❌ Prometheus + Grafana + MongoDB 备份；评分任务失败、数据延迟的自动通知 |

### 阶段 D：纸上交易验证（2–3 个月）

模拟盘验证是实盘前的最后防线。核心检验：

- 模拟盘至少跑 2–3 个月，覆盖至少一轮完整的市场涨跌周期
- 对比每日系统推荐 vs 实际执行偏离（是否因仓位、资金、滑点等因素无法执行推荐）
- 分别计算 "模型质量"（推荐本身对不对）和 "执行纪律"（推荐是否被正确执行）作为独立指标
- 在实盘前完成最后一次参数修正，并锁定模型版本

### Non-goals（明确排除）

以下能力不在当前规划中，待上述四个阶段完成后再评估：

- 高频 / 日内交易：当前以日线为基础，扩展到分钟/ tick 级别需要全新数据管道
- 多资产类别：只做 A 股；商品期货/期权/可转债需要独立的风控和定价模型
- 机器学习模型替代规则评分：先积累足够多 `VERIFIED` 样本（至少数千条）再评估
- 自动交易（无人值守）：实盘初期必须人工复核每笔交易，逐步建立信任

## Historical Reference

`openspec/changes/frontend` 保留为早期前端蓝图参考，不再作为当前执行清单。
