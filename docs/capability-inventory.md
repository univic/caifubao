# Caifubao 系统能力清单

> 最后更新：2026-09-14

本文档以分层视角列出 caifubao 系统的全部能力，标注各项能力的现状：✅ 已实现 / 📋 已规划 / ❌ 未实现。

> 项目定位：A 股量化研究/学习/演示 MVP，**不构成投资建议**。本清单不主张任何生产级
> 实盘交易能力；真实执行、promote 与前瞻证据认证均有独立门禁（见
> `openspec/changes/production-capability-roadmap/`）。自 2026-05-22（#100）以来
> develop 新增 118 个提交、PR 编号至 #237，本轮据此刷新。
>
> 环境口径：本文中「prod→dev 同步」「prod」等表述指**旧 stable（retired
> production，迁移期数据权威环境）→ dev 的在线同步**，是 TASK-404 完成前的迁移期
> 遗留，不代表实盘 production——`prod`（trading/production）目标态**尚未启用**。
> 环境模型以 [`architecture/environment-model.md`](./architecture/environment-model.md)
> 为准。

---

## 第一层 — 基础数据

| 能力 | 现状 | 说明 |
|------------|--------|-------------|
| 股票主数据 | ✅ 已实现 | 全部 A 股代码、名称、上市日期、交易状态、ST 标记 |
| 指数主数据 | ✅ 已实现 | 上证指数、深证成指、沪深 300、创业板指等 |
| 行业分类（证监会 CSRC 一级） | ✅ 已实现 | `StockIndustryClassification`（`datahub/app/model/industry.py`）；数据实际为 baostock 的 CSRC 一级行业（如 `J66`/`货币金融服务`），`industry_code_sw_l1` 是历史遗留字段名而非申万，列映射由 #172 修正 |
| 交易日历 | ✅ 已实现 | `FinanceMarket.trade_calendar`，驱动 T+N 目标日期计算 |
| 日线 OHLCV 行情 | ✅ 已实现 | 开盘/最高/最低/收盘/成交量/成交额/换手率/涨跌幅 |
| 复权价格 (HFQ) | ✅ 已实现 | 后复权开盘/收盘/最高/最低；#135 起改用 tushare 真实 `adj_factor`（原为近似口径） |
| 停牌/交易状态 | ✅ 已实现 | `trade_status` 字段，风险惩罚与 paper/回测可交易性判定使用 |
| 数据新鲜度追踪 | ✅ 已实现 | `data_as_of`、`generated_at`、`freshness` 元数据 |
| 数据质量检查 | ✅ 已实现 | 覆盖率、缺失日统计、BSE 排除、新股 MA 窗口适用性 |
| 估值/市值基本面 | ✅ 已实现 | `stock_daily_basic`（`datahub/app/model/daily_basic.py`）：pe_ttm/pb/ps_ttm/dv_ttm/total_mv/circ_mv/turnover_rate，按 trade_date 的 point-in-time 快照；`daily_basic_backfill` 支持断点续传，已接入 stable→dev 数据同步（#175/#176；在线同步为迁移期遗留） |
| Tushare 数据源 | ✅ 已实现 | 历史行情、`adj_factor`、`daily_basic`、股票池四个接口；全部调用按 300/min 限速节流、带重试与空结果显式失败（`datahub/app/lib/datahub/data_source/interface/tushare_interface.py`） |
| 全市场股票池 | ✅ 已实现 | `stock_basic_active`（tushare `pro.stock_basic`，`list_status='L'`），用于股票池刷新（#130） |
| 财务报表（ROE、营收增速等三表） | ❌ 未实现 | 仅采集 daily_basic 估值/市值，利润表/资产负债表/现金流量表未采集 |
| 融资融券数据 | ❌ 未实现 | 需要新采集器 |
| 北向资金流向 | ❌ 未实现 | 需要新采集器 |
| 实时/日内行情 | ❌ 未实现 | 超出 MVP 范围 |

---

## 第二层 — 派生数据与因子

| 能力 | 现状 | 说明 |
|------------|--------|-------------|
| MA 均线因子 | ✅ 已实现 | MA10/20/30/60/120，趋势对齐组件使用 |
| 金叉/死叉信号 | ✅ 已实现 | `StockSignalDaily`，含方向和强度 |
| 信号历史查询 | ✅ 已实现 | 按日期/信号名称/方向筛选 |
| 评分预测 (Score5/20/60) | ✅ 已实现 | 8 个分量（含 `real_relative_strength`）× 3 个时间周期（#183 前文档记为 7 个，已更正），含解释和输入快照 |
| 评分排名/分位数 | ✅ 已实现 | 按日期/时间周期分配 rank + percentile |
| 分量方向版本化 | ✅ 已实现 | `get_effective_horizon_config` 支持 per-horizon `directions` 覆盖 `{component_id: -1\|0\|1}`，完整方向图进入 `config_hash`；研究候选 `flip_wide`（7 个 alpha 分量翻转、宽书）由此可作独立模型版本运行（#183、`scoring-direction-versioning`） |
| 不可变评分模型版本注册表 | ✅ 已实现 | `ScoreModelVersion`（collection `score_model_versions`）+ `jobs/model_registry_runner`（register/list/retire）；model_version 绑定 per-horizon override，canonical `config_hash`（`model_config_hash`），版本不可变（#185、`scoring-model-registry`） |
| 百分位排名评分（ranked 模式） | ✅ 已实现 | 横截面按分数排序 + 各分量横截面 rank 加权（`DATAHUB_SCORING_MODE=ranked` / 注册表 `scoring_mode`）；推荐改为百分位驱动（BUY=top `buy_percentile` 等），signed score 校准统一用 percentile basis（#137/#188/#189） |
| 行业聚合指标 | ✅ 已实现 | `IndustryDailyMetrics`，驱动行业动量组件与 `max_industry_pct` |
| 评分解释溯源 | ✅ 已实现 | 每个分量：原始值、归一化值、权重、贡献值、证据 |
| 输入快照 | ✅ 已实现 | 记录的行情/因子/信号新鲜度及阻塞原因 |
| RSI(14) | ✅ 已实现 | 标准 RSI(14)，含 Wilder 平滑 |
| 布林带位置 | ✅ 已实现 | BB(20,2)，(close - lower) / (upper - lower) |
| ATR 归一化波动率 | ✅ 已实现 | ATR(14) / close |
| 成交量比 | ✅ 已实现 | volume / MA20(volume) |
| 连续阳线天数 | ✅ 已实现 | 统计连续 close > open 的天数 |
| 换手率加速度 | ✅ 已实现 | turnover_rate / MA5(turnover_rate) |
| 跳空缺口比 | ✅ 已实现 | (open - prev_close) / prev_close |
| 52 周区间位置 | ✅ 已实现 | (close - 52w_low) / (52w_high - 52w_low) |
| 因子评估管道 | ✅ 已实现 | FactorEvaluationService：IC/ICIR/分层收益/相关性矩阵/衰减曲线 + regime IC 拆解；`tech_factor_runner` CLI 支持 compute/evaluate/list |
| Factor Lab（冻结面板 + 单因子评估） | ✅ 已实现 | `datahub/app/lib/factor_lab/`（panel/factors/metrics）+ `jobs/factor_lab_runner` export/evaluate/list：parquet 面板冻结、多 horizon 交易日偏移标签（含 short 腿）、IC/ICIR、净分层价差、换手、walk-forward、硬门；面板落 PVC，Job 示例见 `k8s/base/factor-lab-*.example.yaml`（#232–#235） |
| 真实相对强弱 | ✅ 已实现 | 对标沪深 300/中证 500 指数的 alpha；已注册并接入评分管道（权重 10/10/8），依赖 index_quotes/alpha_map 输入 |
| 基本面估值因子（EP/BP/DV 等） | 📋 已规划 | `stock_daily_basic` 已入库、研究审计已完成（估值正 IC 且 size 中性后不消失）；结论为作独立/regime 分量，**未注册为评分分量**，也未混入 flip_wide（`docs/autoresearch/runs/h20-excess-alpha/fundamental-factor-audit-2019-2026.md`） |
| 持有期 × buffer 扫描 | ✅ 已实现 | `datahub/app/lib/strategy_engine/holding_scan.py` + `factor_lab_runner holding-scan`（#236）；持有期扫描已合并并附实测报告。注意 `--buffers` 轴被 QA 判定**结构性无效**（每期硬到期先清空账本，滞回永不生效），实际等价于纯持有期扫描 |
| ETF lab / 小账户研究模块 | ✅ 已实现 | `datahub/app/lib/etf_lab`（pool/panel/measure + 多资产 rotation）与 `datahub/app/lib/small_book`（8 分量评分重建）已随 #236 合入；研究结论为负结果/配置型，不构成 alpha |
| 不可变研究工件契约 | 📋 已规划 | `openspec/changes/artifact-contract/` 已定义 artifact_id/artifact_hash/`input_snapshot` 契约，但 0/13 任务实现；当前各报告/导出的哈希与溯源语义尚未统一 |

---

## 第三层 — 分析引擎

| 能力 | 现状 | 说明 |
|------------|--------|-------------|
| 回测引擎 | ✅ 已实现 | 支持 MA_CROSS、BUY_HOLD、SCORE_THRESHOLD、SCORE_MOMENTUM、MULTI_HORIZON_CONSENSUS、TOP_N_ROTATION 六个策略 |
| 回测指标 | ✅ 已实现 | 总收益、年化收益(CAGR)、夏普比率、最大回撤、最大回撤持续天数、胜率、信息比率、超额收益 |
| 回测交易明细 | ✅ 已实现 | 逐笔交易(buy/sell 价格/数量/金额/PnL)、每日权益曲线 |
| 回测结果持久化 | ✅ 已实现 | `BacktestResult` 集合，支持列表/详情/删除 |
| 摩擦成本建模 | ✅ 已实现 | 佣金 0.025%(最低 5 元)、印花税 0.1%(卖方)、滑点 0.1% |
| 涨跌停板约束 | ✅ 已实现 | `trade_status` 拦截：一字涨停买入失败、一字跌停卖出失败、连续涨停重试 |
| 多股票回测 | ✅ 已实现 | 多只股票统一日期对齐、等权/分数加权/仓位上限、整手 100 股取整 |
| 指数基准对比 | ✅ 已实现 | CSI 300 基准，超额收益和信息比率 |
| 评分验证（闭环） | ✅ 已实现 | PENDING → TRACKING → VERIFIED → INSUFFICIENT_DATA → BLOCKED |
| 验证指标 | ✅ 已实现 | 目标收益、最大收益、最小收益、回撤、触及目标价日数、hit_target、hit_stop_loss |
| 校准报告 | ✅ 已实现 | 5 桶评分分档、Top-10/30/50 聚合、假阳性/假阴性筛查；signed/翻转模型报告 `bucket_basis=percentile`（#188） |
| 评分实验 (A/B 测试) | ✅ 已实现 | 手动创建、回填、验证、报告生成，支持配置覆盖和基线对比 |
| 评分历史回放 | ✅ 已实现 | 按日期范围/股票代码/时间周期回填，支持 dry-run 和 replace |
| 前视偏差防护 | ✅ 已实现 | `date__lt` 严格查询、行业指标先于评分日、独立 lookback 窗口；`score-backtest-execution-integrity` change 另修 score 驱动回测的执行时序 |
| 评分驱动回测策略 | ✅ 已实现 | SCORE_THRESHOLD、TOP_N_ROTATION、SCORE_MOMENTUM、MULTI_HORIZON_CONSENSUS |
| 执行安全评分回测 | ✅ 已实现 | #170：score 驱动回测的执行日/可见信息/成交价语义修正，禁止未来信息影响成交 |
| 评分方案网格搜索 | ✅ 已实现 | GridSearchService 权重 × 阈值自动搜索，compute-worker handler 支持 |
| 滚动交叉验证 | ✅ 已实现 | RollingValidationTask：年滚动训练/测试，衰减检测(>20% = 过拟合) |
| 统计显著性检验 | ✅ 已实现 | Permutation test + bootstrap CI，exposed via API |
| 多周期共识/分歧检测 | ✅ 已实现 | `GET /api/score-experiments/consensus`：对同一股票+日期检测 Score5/20/60 共识或分歧（要求 3 个 horizon 均有预测） |
| 市场状态检测 | ✅ 已实现 | `datahub/app/lib/market_regime.py`：CSI 300 60 日趋势分类 bull/bear/sideways；接入因子评估 regime IC、滚动验证分状态报告与回测 `/regime` 端点（#103） |
| 实验比较报告 | ✅ 已实现 | 并排指标对比(comparison_report.py)，含统计概要；跨方向模型统一 percentile basis |
| Compute-Worker 异步计算 | ✅ 已实现 | 独立服务 9 种任务类型（BACKTEST_SINGLE/MULTI/SCAN、GRID_SEARCH、SCORE_REPLAY、SCORE_VERIFY、CALIBRATION_REPORT、FACTOR_EVAL、ROLLING_VALIDATION），K3s Deployment + node-type=compute affinity |
| 策略发现与多重比较校正 | ✅ 已实现 | 策略发现/可执行性筛选/异步 scan/CSV 导出 + Bonferroni 校正（`bonferroni_correction`，实验比较/扫描元数据）；前端 DiscoveryView（#103） |
| H20 超额 alpha 自动研究 | ✅ 已实现 | `jobs/autoresearch_h20_runner`（prepare/run/metric）+ `lib/autoresearch/h20_excess_alpha.py`：不可变快照校验、train/validation/test 切分、IR/净超额/回撤/换手/集中度/decay、research_profitability_score 与 keep/discard（#169） |
| H20 研究快照导出 | ✅ 已实现 | `jobs/autoresearch_h20_snapshot_runner`：只读、资源受限的全市场 H20 快照导出（8 分量 + 可执行 entry/exit 标签 + 阻断计数），parquet + manifest 落盘（#169、`h20-autoresearch-replay-semantics`） |
| H20 replay / 前视语义 | ✅ 已实现 | 选型只看 validation 与 quarterly walk-forward，最终 test 冻结后只读一次；信号收盘后产生、次交易日开盘首次尝试成交，持有期从**实际 entry** 起算，停牌/涨跌停顺延，快照记录 requested/actual 标签（`h20-autoresearch-replay-semantics`） |
| 策略 Paper Runner | ✅ 已实现 | `lib/strategy_engine` + `jobs/strategy_runner`：按已注册 model_version 读可用评分 → selection 约束 → 目标组合与调仓清单 + 真实成本 paper NAV（commission/滑点/印花税/手数/T+1/停牌 roll-forward）（#190–#192） |
| 组合风控限制 | ✅ 已实现 | `max_single_stock_pct` 真实生效、新增行业上限 `max_industry_pct`（CSRC 一级，UNKNOWN 桶同样受限）、`min_trade_amount_cny` 流动性下限；分类须 `assigned_at <= signal date`；配置缺失时 fail closed（#221、`strategy-portfolio-risk-limits`） |
| 费用感知开盘买入预算 | ✅ 已实现 | `nav.py _fit_buy_quantity`：整手二分，使「含滑点成交额 + commission ≤ min(现金, 目标预算)」，仅当一手都放不下才跳过（#204、`strategy-fee-aware-opening-budget`） |
| 因果 paper 时序 | ✅ 已实现 | `paper_causal_v1`：消费 PENDING/TRACKING/VERIFIED/INSUFFICIENT_DATA、记录真实 UTC `decision_at` 与下一交易日 `execution_date`、按完整 config_hash 隔离轨道、只用开盘信息定量，产物标记 `evidence_kind=REPLAY`（#202） |
| 前瞻证据认证 + 120 交易日计数器 | ✅ 已实现 | `StrategyForwardWindow`（append-only，按 model_version+horizon ACTIVE 唯一）+ `strategy forward certify/close/progress`：仅窗口内决策先于执行日开盘的 COMPLETED 记录标 FORWARD，计数 distinct 信号日/120，缺口如实报告；FORWARD 计划不可改写，NAV 仅为派生（#207、`strategy-forward-evidence-capture`） |
| 研究级目标组合导出 | ✅ 已实现 | `lib/strategy_engine/export.py` + `strategy_runner export`：COMPLETED run 的 BUY/SELL/HOLD 目标权重与金额、CSV/JSON；固定 `grade=RESEARCH` + 非投资建议免责声明，歧义 config_hash fail closed；不写库不下单（#220、`strategy-target-export`） |
| 可信股票择时评估器 | ✅ 已实现 | `backend/app/services/timing_evaluator.py`（依赖自由、纯函数）+ `backtest_runner timing-pool` CLI：对显式冻结 cohort 聚合 timing vs buy_hold 成对证据，未达门槛输出 UNVALIDATED/研究级（#237、`stock-timing-evaluator-p0`） |
| Point-in-time 前视审计模拟 | ✅ 已实现 | `datahub/research/pit_sim/`（pit_sim/pit_value/pit_drift_check）：50k 纸面模拟只读 `date <= D` 的行，窗口内价格仅用于估值；2026-08-01→09-11 实测报告（#231） |
| 组合层面整体回测 | ❌ 未实现 | 多股回测存在，但未接入 Portfolio 持仓做组合整体回测 |
| 换手/再平衡预算 | ❌ 未实现 | roadmap 1.3 剩余：再平衡的换手/交易预算未落地（`strategy-forward-window.md` §6 明示） |
| 真实执行（券商对接/对账/kill-switch） | ❌ 未实现 | roadmap 2.2-2.4：无券商适配器、成交对账、下单幂等或 kill-switch；paper 路径明确不下真实订单 |
| 大盘择时信号 | ❌ 未实现 | 市场状态分类器已实现，但尚无输出择时信号/仓位建议的层 |

---

## 第四层 — 用户界面

| 能力 | 现状 | 说明 |
|------------|--------|-------------|
| 用户登录 | ✅ 已实现 | JWT 鉴权 |
| 注册/找回密码/个人资料 | ✅ 已实现 | RegisterView、ForgotPasswordView、ResetPasswordView、ProfileView |
| 用户管理（管理员） | ✅ 已实现 | UserManagementView + `/admin` API，`requiresAdmin` 路由守卫 |
| 市场总览面板 | ✅ 已实现 | 股票/指数分 Tab，多时间周期评分筛选排序，多周期评分标签 |
| 历史行情查看 | ✅ 已实现 | K 线图(ECharts) + 数据表格 |
| 股票评分详情 | ✅ 已实现 | 时间周期卡片(分数/推荐/分位数)、验证指标、评分历史图表 |
| 评分解释查看 | ✅ 已实现 | 分量分解(原始值/归一化值/权重/贡献值/证据)、输入新鲜度 |
| 回测创建/结果查看 | ✅ 已实现 | 参数表单、指标展示、交易列表、每日权益 |
| 回测权益曲线图 | ✅ 已实现 | BacktestResultView 权益曲线图表（#182） |
| 策略发现工作台 | ✅ 已实现 | DiscoveryView（1123 行）：策略扫描/可执行性/参数景观/CSV 导出（#103） |
| 投资组合管理 | ✅ 已实现 | 手动记账(BUY/SELL/CASH_IN/CASH_OUT/DIVIDEND)、持仓、交易历史、快照 |
| 评分实验管理 | ✅ 已实现 | 创建/运行/查看报告 |
| 评分方案排名/热力图 | ✅ 已实现 | ScoreExperimentsView：Top 排名表 + 权重热力图 + Bonferroni 阈值标注（#107，原文档记为 📋） |
| 因子评估仪表板 | ✅ 已实现 | FactorEvalView（451 行）：IC/分层/衰减图表（#104，原文档记为 📋） |
| 每日决策面板 | ✅ 已实现 | 今日最高分、评分变化(涨跌箭头+差值)、持仓匹配(持有/建议买入/建议卖出)、可操作推荐 |
| 评分预警通知 | ✅ 已实现 | 分数跳升超过阈值时触发，前端 Dashboard + Decisions 页面 |
| 决策日志界面 | ✅ 已实现 | DecisionsView（1286 行）内 journal 列表/摘要/归因（#108；原文档记为「模型未建」） |
| 评分驱动再平衡预览 | ✅ 已实现 | `POST /api/decisions/rebalance-preview` + DecisionsView 现金/持仓输入（原文档记为 📋） |
| 自选股/关注列表 | ✅ 已实现 | WatchlistsView + `/watchlists` API + Pinia `watchlist` store（#108） |
| 指数视图 | ✅ 已实现 | IndicesView + `/indices` API |
| 数据质量视图 | ✅ 已实现 | DataQualityView + DashboardDataStatus |
| 参数景观可视化 | 📋 已规划 | 后端 `POST /api/backtest/landscape` 已实现（flat vs sharp optima），前端无对应组件（17.6） |
| 行业/板块轮动热力图 | 📋 已规划 | `IndustryDailyMetrics` 数据已有，前端行业热力图未建 |

---

## 第五层 — 集成与运维

| 能力 | 现状 | 说明 |
|------------|--------|-------------|
| OpenClaw 服务令牌鉴权 | ✅ 已实现 | 独立 service token、SHA-256 哈希、双 scope(`openclaw:data-read` + `openclaw:score-read`) |
| OpenClaw 只读数据 API | ✅ 已实现 | 股票/行情/因子/信号/质量/评分/推荐 共 9 个端点 |
| OpenClaw 请求审计 | ✅ 已实现 | request_id、generated_at、last_used_at/last_used_ip（注意：无持久化审计日志表） |
| 持久化审计日志链 | ❌ 未实现 | roadmap 1.2：无 `RequestAudit`/操作审计链，last_used_* 是覆盖写而非审计 |
| Service token 限流 | ❌ 未实现 | roadmap 1.2：service token 无 rate-limit 保护 |
| 数据新鲜度契约 | ✅ 已实现 | 下游可判断 missing/stale/blocked 状态，避免靠空值猜测；`data_as_of` 已在 OpenClaw 各端点填充（#103） |
| CI/CD | ✅ 已实现 | GitHub Actions 基础流水线；部署环境按 dev/research 分派（#210） |
| K8s 部署示例 | ✅ 已实现 | `k8s/` 目录，含 base/overlays/services + compute-worker |
| 研究环境 overlay | ✅ 已实现 | `k8s/overlays/example-research/`（#214）；研究集群迁移已收尾，research 激活仍待修复 bootstrap 镜像 tag 不变量（#215） |
| 数据管道调度 | ✅ 已实现 | CronJob 依赖链(信号 → 评分 → 验证)，上游失败则跳过；quote/signal/scoring 已拆分为独立 CronJob（#144） |
| 任务依赖管理 | ✅ 已实现 | `job_run_helper` 检查上游任务 SUCCESS 状态 |
| Dry-run 支持 | ✅ 已实现 | 评分回填支持 dry-run 模式 |
| 幂等性保证 | ✅ 已实现 | 默认 skip 已有记录，显式 replace 才覆盖；stable→dev 同步按业务键 upsert（#151） |
| 评分质量自动监控 | ✅ 已实现 | `/api/decisions/quality`：滚动命中率、分布偏移检测、模型漂移检测(P50/P90) |
| 决策日志（DecisionJournal） | ✅ 已实现 | `DecisionJournal` 模型含推荐内容/信心/目标价/止损 + `executed`/成交价量/`realized_pnl` 全链字段，`/api/decisions/journal*` 四端点（#104/#108） |
| Compute-Worker K3s 部署 | ✅ 已实现 | node-type=compute 亲和性、资源限制、存活探针 |
| OpenClaw 评分/推荐端点 | ✅ 已实现 | `/scores` (含解释/验证) + `/recommendations/daily` + `/recommendations/performance` |
| Health Watcher | ✅ 已实现 | `jobs/health_watcher.py`：近 26h FAILED/SKIPPED 任务 + STALE/NO_DATA 资产，JSON 报告、`--fail-on-issues`、可选 webhook；dev overlay 有 CronJob（#148） |
| MongoDB 备份/恢复 + 恢复演练 | ✅ 已实现 | `k8s/base/mongodb-backup.yaml`、`mongodb-restore-job.example.yaml`，runbook 见 `docs/operations/mongodb-resilience.md`；13/13 集合非破坏性恢复演练通过（2026-09-13） |
| MongoDB 启动连接重试 | ✅ 已实现 | backend（#149）与 datahub（#153）`connect_to_db` 10×5s 重试，耗尽后 exit(1) |
| 研究数据湖 | ✅ 已实现 | `jobs/parquet_export_runner`（daily_quotes/factors/signals，按 trade_date 分区，S3 兼容）+ `k8s/base/data-lake-export.yaml`；research export smoke 通过（2026-09-12）；文档 `docs/operations/data-lake.md` |
| 模型治理（promote/rollback/停用） | ❌ 未实现 | roadmap 1.4：已有 config_hash/ACTIVE-RETIRED，但无审批与审计记录流程 |

---

## 第六层 — 数据管道

| 能力 | 现状 | 说明 |
|------------|--------|-------------|
| 行情采集 (Quote Runner) | ✅ 已实现 | 日线数据落库，含 dry-run |
| 快照驱动每日行情更新 | ✅ 已实现 | UPD 路径：tushare 全市场按 trade_date 快照增量更新（#132） |
| 因子计算 (Factor Runner) | ✅ 已实现 | MA 因子计算并落库 |
| 真实复权因子 (adj_factor) | ✅ 已实现 | FQ 因子改用 tushare `adj_factor`；每日 FQ 更新批量加固（#135/#139、`fq-adj-factor-fix`） |
| 信号生成 (Signal Runner) | ✅ 已实现 | 金叉/死叉检测并落库 |
| 增量信号计算 | ✅ 已实现 | 以 `(code, signal_name)` 的 `latest_data_date` 为 anchor，只重算锚点之后的新日期，保留历史命中（#141） |
| 评分生成 (Scoring Runner) | ✅ 已实现 | 依赖信号完成，3 个时间周期全量打分，含排名/行业聚合 |
| 批量评分读写（C1） | ✅ 已实现 | `score_all_stocks[_ranked]` 按天预取（quote/factor/signal/existing/行业/CSI300），原始 dict + `_Row` 装配免 Document 水合，`bulk_write` 落库；等价性由 `test_scoring_batch_equivalence.py` 逐字段锁定（#227/#229） |
| 增量评分 | ✅ 已实现 | 跳过已完整的评分 cohort（#143） |
| 评分一致性校验 | ✅ 已实现 | `scoring_runner equivalence-check`：新旧路径全市场逐字段 diff（dev/research 均 0 diff），实际拦下信号读取顺序分歧（#229） |
| 验证运行 (Verification) | ✅ 已实现 | 评分后触发，批量更新 PENDING/TRACKING 记录 |
| 批量验证 | ✅ 已实现 | `verify_predictions_batch`：未来行情按 stock_code 单次拉取 + bisect 切片 + `bulk_write`（#197） |
| 回填命令 | ✅ 已实现 | `--from`/`--to`/`--horizon`/`--stock-code`/`--replace`/`--dry-run` |
| 校准报告命令 | ✅ 已实现 | JSON/TEXT 输出 |
| daily_basic 回填 | ✅ 已实现 | `jobs/daily_basic_backfill`：逐交易日全市场 upsert，按日期完成标记断点续传（#175） |
| 研究数据湖导出 Runner | ✅ 已实现 | `parquet_export_runner export --dataset all/daily_quotes/factors/signals` |
| 行情/信号/评分任务拆分 | ✅ 已实现 | 拆分出独立 CronJob，避免单任务超时被 kill 拖垮全链（#144） |
| 任务进度持久化 + 陈旧 RUNNING 回收 | ✅ 已实现 | 阶段级 `phase_stats` 持久化、原子 catchup claim、启动时回收 stale RUNNING（#142/#144） |
| 数据同步（stable→dev） | ✅ 已实现 | 增量、按业务键幂等 upsert；MongoDB 声明漂移已收敛（#151/#164/#176）。来源为旧 stable（retired production）；在线直连为迁移期遗留，目标为受控快照导入（TASK-404） |
| 数据源健康与快速失败 | ✅ 已实现 | 批量新鲜度刷新；死掉的历史数据源 fail fast（#140） |
| 因子评估向量化 | ✅ 已实现 | `evaluate`/`_build_dataset` 接受预载 `quote_frame`，前向收益 `groupby.shift(-h)`，decay 复用同一数据集；dev 实测 434.4s → 49.9s（8.7×），DB 往返归零（#232） |
| data-sync 失败自动补跑 | ❌ 未实现 | roadmap P2：dev data-sync 失败后无自动补跑，次日调度才重试 |

---

## 当前薄弱环节与待改进项

以下项目已确认存在，按严重程度和处置阶段排列。状态基于 develop @ `7b8e90d`（2026-09-14）。

### ✅ 已修复（合并于 develop）

| 问题 | 状态 | 处置 |
|--------|------|------|
| 校准报告的 hit_target 使用 max_return | ✅ 已修复 (12a.1) | 拆分为 `hit_target_close`（保守口径）和 `hit_target_intra`（激进口径） |
| 回测无摩擦成本 | ✅ 已修复 (12a.2) | 佣金 0.025%（最低 5 元）+ 印花税 0.1%（卖方）+ 滑点 0.1% |
| 回测无涨跌停板约束 | ✅ 已修复 (12a.4) | `trade_status` 拦截一字涨跌停，连续涨停重试 |
| 回测无指数基准对比 | ✅ 已修复 (12a.6) | CSI 300 基准，超额收益和信息比率 |
| 市场状态分类器未实现 | ✅ 已修复 (#103) | `lib/market_regime.py` + 因子评估 regime IC + 滚动验证分状态报告 + 回测 `/regime` 端点 |
| Bonferroni 多重比较校正未实现 | ✅ 已修复 (#103) | `bonferroni_correction` 接入实验比较/策略扫描元数据与前端排名 |
| 因子评估仪表板（前端）缺失 | ✅ 已修复 (#104) | FactorEvalView 已建成（IC/分层/衰减图表） |
| 网格搜索热力图（前端）缺失 | ✅ 已修复 (#107) | 排名表 + 权重热力图 + Bonferroni 阈值展示 |
| 决策日志(DecisionJournal)模型未建 | ✅ 已修复 (#104/#108) | 模型 + 四端点 + DecisionsView journal 区；含 executed/realized_pnl 全链字段 |
| 决策工作区前端不足 | ✅ 已修复 (#108) | DecisionsView 由 232 行扩到 1286 行，含信心元数据、journal、再平衡预览 |
| 评分驱动再平衡预览 | ✅ 已修复 | `POST /api/decisions/rebalance-preview` + 前端现金/持仓输入 |
| 多周期共识/分歧检测 | ✅ 已修复 | `GET /api/score-experiments/consensus`（需 3 horizon 均有预测） |
| data_as_of 从未填充 | ✅ 已修复 (#103) | OpenClaw 全部端点按数据边界填充 `data_as_of` |
| 行业分类口径错误（文档写「申万」） | ✅ 已修复 (#172) | 数据实为 baostock CSRC 一级行业；列映射修正，本清单已更正说明 |
| 信号/评分停更无人知晓（3 日断链） | ✅ 已修复 (#144/#148) | 任务拆分 + Health Watcher + `--fail-on-issues` CronJob |
| MongoDB 单点/连接崩溃循环 | ✅ 已部分修复 (#149/#153) | 启动连接重试；节点迁移与 HA 仍见 P1 |

### P0 — 当前立即行动项

| 问题 | 严重程度 | 处置 |
|--------|----------|------|
| 前瞻证据窗口尚未认证、未跑满 120 交易日 | **高** | roadmap 0.1/0.3：机制已于 #207 落地，但当前无 ACTIVE 窗口、计数为 0；在跑满 120 个不可变 FORWARD 交易日并对照研究 walk-forward 之前，任何模型不得标注 tradable/actionable，也不得 promote |
| 全市场 baseline 对等校准对比未完成 | **高** | task 3.3/4.4：flip_wide 影子版本已全市场 replay 并 vs baseline 对比，但 dev baseline VERIFIED 仅覆盖旧 50 只子集，**非对等比较**；需回补全市场 baseline 后重跑 |
| 评分模型成功标准未在全市场验证 | **高** | 20.x：研究层 walk-forward 已做，但「SCORE 策略是否优于 BUY_HOLD」等生产链验收标准在全市场仍未闭环 |
| 真实执行能力零实现，且默认必须保持关闭 | **高** | roadmap 2.2-2.4：无券商适配器/对账/下单幂等/kill-switch；任何执行路径都需另行授权、Spec Gate 与验证 |
| 持久化审计日志 + service token 限流未实现 | 中 | roadmap 1.2：无 RequestAudit 审计链、无 rate-limit；当前 last_used_* 是覆盖写 |
| 研究环境（research overlay）激活阻塞 | 中 | #215：research 的激活路径缺失，需先修 bootstrap 镜像 tag 不变量；research 集群迁移本身已完成 |

### P1 — 阶段修正

| 问题 | 严重程度 | 处置 |
|--------|----------|------|
| 组合换手/再平衡预算未落地 | 高 | roadmap 1.3 剩余：需把上一期持仓纳入选择语义；宽书下 `max_single_stock_pct` 通常不 binding |
| 真实相对强弱依赖指数数据 | 中 | 16.9：分量已注册并接入评分管道，但依赖 index_quotes/alpha_map，指数数据缺失时贡献为 0，需确认指数行情采集覆盖 |
| 投资组合层面整体回测缺失 | 中 | 多股回测存在，但未接入 Portfolio 持仓做组合整体回测 |
| H20 快照导出语义 change 任务未回填 | 中 | `h20-autoresearch-replay-semantics` 的 2.1-2.4/3.x 复选框仍为空，但实现与测试已在 develop；需回填任务状态以免误判 |
| C1 评分内存峰值未收尾 | 中 | perf 3.1b「按 ~900–1000 code 分组预取」未实现；单份全市场窗口 RSS 峰值 ~1.06GiB，dev 1Gi limit 曾 OOMKill（现暂调 2Gi） |
| 全市场 FQ 重算与 50 股复测未执行 | 中 | `fq-adj-factor-fix` 5.1-5.3：新镜像发布、全市场 FQ 重算、50 只评分+验证对比仍待 operator 执行 |
| percentile-rank 部署与复测未执行 | 中 | `scoring-percentile-rank` 5.1/5.2：镜像发布、50 只评分+验证与基线 corr 对比未完成 |
| 不可变研究工件契约未实现 | 中 | `artifact-contract` 0/13：artifact_id/hash/`input_snapshot` 跨模块契约尚未统一，溯源语义不一致 |
| 参数景观可视化（前端）缺失 | 低 | 17.6：后端 `/landscape` 已实现，前端无 flat vs sharp optima 展示 |
| 行业/板块轮动分析 | 低 | `IndustryDailyMetrics` 已有，前端行业热力图未建 |
| 大盘择时信号 | 低 | 市场状态分类器已实现，但尚无仓位/择时信号层 |

### P2 — 后续修正

| 问题 | 严重程度 | 处置 |
|--------|----------|------|
| 小账户研究前瞻验证未完成 | 中 | 需 ≥120 交易日前瞻样本；`docs/autoresearch/runs/small-book/forward-ledger.jsonl` 已启动（首条为 REPLAY，不计入前瞻窗口），自动累积需把 `rotation.py` 部署进 datahub 镜像并加月度 CronJob（属新授权范围） |
| `scoring_runner` 的 raw/ranked 选择由环境变量决定 | 中 | `DATAHUB_SCORING_MODE` 决定路径，模型注册表的 `scoring_mode=ranked` 不参与判断；影子模型未显式设置该变量时 `directions` 翻转会**静默失效**（#236 实测：翻转与不翻转得到逐位相同分数） |
| `rebalance.cadence_days` 未被消费 | 中 | 该字段只被校验/记录，`assemble_daily_plan` / `strategy_runner` 不消费它，实际是每日换仓（#236 发现） |
| 模型治理 promote/rollback 流程缺失 | 中 | roadmap 1.4：已有 config_hash 与 ACTIVE/RETIRED，缺审批/回滚/停用记录 |
| FAILED 验证状态实际判定 | 低 | `verification_service` 无 FAILED 终态判定逻辑 |
| 稳定性检验（权重扰动） | 低 | 17.4：小权重扰动是否导致大结果变化，未实现 |
| signal decay 专项测试缺失 | 低 | 12d.6 部分修复：hybrid 阈值测试已补齐，signal decay 逻辑仍无专门测试 |
| data-sync 失败自动补跑 | 低 | 失败后次日 19:15 才重试；方案见 `docs/operations/roadmap-2026-08.md` |
| strategy_daily CronJob 未接线 | 中 | `strategy_daily` 未在任何 overlay 定义，当前为 operator cadence；非交易日会异常结束（fail loud），需先决策「优雅跳过 vs 接受告警」 |
| MongoDB 运维遗留（旧 stable 迁移、副本集 HA、定时备份启用） | 中 | dev 已迁 vm-8-15；旧 stable（retired production，非实盘 `prod`）暂留 vm-4-12，副本集 HA 与 backup CronJob 解挂待决策（`docs/operations/mongodb-node-migration.md`） |

---

## 覆盖度统计

| 层级 | 已实现 | 已规划 | 未实现 | 完成率 |
|------|--------|--------|--------|--------|
| 第一层 基础数据 | 12 | 0 | 4 | 75% |
| 第二层 派生数据与因子 | 24 | 2 | 0 | 92% |
| 第三层 分析引擎 | 35 | 0 | 4 | 90% |
| 第四层 用户界面 | 21 | 2 | 0 | 91% |
| 第五层 集成与运维 | 19 | 0 | 3 | 86% |
| 第六层 数据管道 | 21 | 0 | 1 | 95% |
| **总计** | **132** | **4** | **12** | **89%** |
