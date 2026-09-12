# Datahub Performance Optimization Tasks

> 编号（S1/Q1-Q5/F1-F5/G1-G3/C1-C8/W1-W2/R1-R7）与
> [`docs/operations/perf-analysis-2026-08.md`](../../../docs/operations/perf-analysis-2026-08.md)
> §3 发现清单一一对应。每完成一项须在本文勾选并在该报告对应条目补记实测结果。

## 1. 阶段 0 — 度量基线

- [ ] 1.1 采集一个完整交易日的 `datahub_job_runs` 各 job/phase 耗时基线（18:00–19:30 prod 全链路）
- [ ] 1.2 pod 内 py-spy 火焰图各一份：score-all、data sync、FQ 全历史回填（验证 C1/Q2/F1 热点占比）
- [ ] 1.3 Mongo profiler（slowms=100）观察一晚慢查询分布并归档结论
- [ ] 1.4 将实测数字回填到分析报告 §4，修正各发现量级

## 2. 阶段 1 — 快赢

### 同步（S1）

- [ ] 2.1 sync_engine 增加每集合水位线（meta 集合记录上次同步最大 date/count），默认增量
- [ ] 2.2 CLI `data sync` 默认带最近交易日；全量需显式 `--allow-full-sync`（复用 sync_data.py 闸门思路）
- [ ] 2.3 每周低峰全量对账 CronJob（或 runbook 手动步骤）；冷启动 runbook 改 mongodump/mongorestore
- [ ] 2.4 sync 幂等回归测试：重复增量运行零重复写、全量显式路径可用、dev_only signal 规则不变

### 评分写路径（C5/C6/C2）

- [x] 2.5 `assign_ranks` 改 `bulk_write($set:{rank,percentile})`，值未变跳过（C5）
- [x] 2.6 ranked 评分收尾消费内存结果：assign_ranks 接受已持久化对象列表
  （predictions=）免 cohort 重查；_require_complete_prediction_set 接受
  persisted_codes 集合做内存完整性校验；BLOCKED 行不入内存排名（C5）。
  注：本任务未处理 replay_service 与 _upgrade_recommendations 的全量重读；
  ranked 收尾的 _upgrade_recommendations 重读已由后续 PR #199 内存化消除
  （predictions= 并同步改写对象），replay/raw 侧重读与行业/指数缓存仍留待
  后续（见 perf-analysis 文档 C5 行；dev 实测 87 s/日 @2 核）
- [x] 2.7 验证服务批量路径（verify_predictions_batch/_verify_many）：未来行情按
  stock_code 一次拉取（.only() 投影）后按预测切片；status/verification 用
  bulk_write 更新（C6）。注：「仅验到期未验且自上次验证后有新行情」的增量门槛
  未实现（verify_predictions 仍查 status__in PENDING/TRACKING 全量候选），留待后续
- [x] 2.8 消除 prod 评分双跑：服务层按 horizon 验证完整 cohort 后在逐股取数前跳过（C2，无需私有 overlay 变更）

### 日历与杂项（R4/F4/Q4/G3）

- [ ] 2.9 四处日历排序/线性扫描改 bisect/预排序缓存：scoring_service.py:67-69、fq_factor.py:302-306、trading_day_helper.py:129-138、data_asset_status_helper.py:206-210
  注：`scoring_service.py` 一处**已完成**（`_sorted_calendar()` 缓存 + `get_t_plus_n_day`
  改 bisect，实测 8.5k 天日历 15,600 次调用 54.0s → 0.011s，即每交易日约 54s 纯 CPU）；
  `fq_factor.py` / `trading_day_helper.py` / `data_asset_status_helper.py` 三处未动，
  故本项保持未勾选。
- [x] 2.10 factor_runner MA stale 分支改调 `update_market`（对齐 FQ 分支）（F4）
- [ ] 2.11 `get_hist_stock_quote_data`：`stock_obj` 直传参数 + 删除重复查询死代码（Q4）
- [x] 2.12 信号 `generated_at`/`source_freshness` 提为循环外常量或 `$setOnInsert`（G3，语义见 specs/signals-mvp delta）
- [ ] 2.13 阶段 1 回归：datahub pytest 全绿 + 一次 dev 全链路跑批对比基线

## 3. 阶段 2 — 结构性改造（每项独立 PR、独立验证）

### 评分批量取数（C1）

- [x] 3.1 `score_all_stocks` 按天批量预取：当日 quote/factor/signal/existing + 全市场 lookback 窗口行情（code→DataFrame）
  注：`_DayPrefetch` 每次 `score_all_stocks(...)` 调用一份，raw/ranked 共用；当日
  quote/factor/signal 各 1 查、`code__in` 窗口 1 查、信号衰减窗 1 查、CSI300 1 查、
  行业分类/指标各 1 查、当日 existing 1 查。runner 已合并为单次 `horizon=None` 调用
  （见 3.1a，完整交易日 1 份预取）；`replay_service.backfill_predictions` 亦已合并为
  每日期一次调用。窗口按**交易日**
  `max(minimum_quote_count, breakout_lookback, risk_lookback)+10` 取（design 的
  `d-120` 是日历日，覆盖不了 h60 的 120 根历史）；窗口内行数不足的稀疏/停牌 code
  回退逐股精确查询，绝不截断历史。行以 `as_pymongo()` 原始 dict + mongoengine
  字段默认骨架装配为 `_Row`（免 Document 水合），窗口分块建帧后用
  `groupby("code").indices` 切片。`DATAHUB_SCORING_BATCH=0` 回退逐股路径。
- [x] 3.1a 预取粒度合并（C1 收尾）：runner 与 replay/backfill 每个日期只调用一次
  `score_all_stocks`（`horizon=None` 覆盖全部请求 horizon），完整交易日由 3 份预取
  降为 1 份
  注：`scoring_runner.run_scoring` 与 `ScoreReplayService.backfill_predictions` 各自
  删除了逐 horizon 循环（`horizon=args.horizon` 原样透传，单 horizon 语义不变）；
  回归测试 `test_run_scoring_makes_one_call_covering_all_horizons`、
  `test_replay_backfill_makes_one_call_per_date_for_all_horizons` 断言「每日期只调
  一次」；all-horizons 单次调用的输出等价性由
  `test_raw_all_horizons_batch_matches_per_stock` 保证。`datahub_job_runs.summary`
  的 `results` 由「按 horizon 字符串分键的 dict」变为单次服务结果（backend 只读
  `count_documents`，无外部消费者）。
- [x] 3.2 分量计算 pandas 化（保持各组件 raw_value/weight 逐股一致）
  注：数值内核仍是原 Python 实现（`statistics.pstdev`、逐项 `round` 逐位一致），
  改变的是输入装配——组件改吃 dict/DataFrame 回读的 `_Row`/`_HistoryWindow`
  （`len`/正负索引/切片/`+[quote]`/`reversed` 全兼容）。NaN/NaT 回填 None、
  Timestamp 还原 datetime。内存（1Gi pod）：窗口/衰减窗/行业按实读字段投影，窗口
  按 5 万行分块建帧、concat 后释放 chunk 帧——702k 行 × 8 投影字段合成实测
  tracemalloc 峰值 ~102MB、保留 ~57MB（未分块单次建帧 ~420–460MB；QA 独立复测
  分块收益 ~2.3–3.7×）；新增字段读取须同步白名单，等价性 harness 为兜底（已实际
  拦下 `buy_count` 漏投影）。
- [x] 3.3 持久化改 `bulk_write(upsert)`，替换逐文档 save（新值语义与 `_persist_prediction` 一致）
  注：自然键 `{stock_code,date,horizon,model_version}`（唯一索引）upsert，`$set`
  全部业务字段 + `updated_at`，`$setOnInsert.generated_at`；BLOCKED 行显式
  `rank/percentile=None`（mongoengine save 省略 None 字段、bulk 写显式 null，读回
  语义相同；bulk 亦跳过模型校验，payload 全部由引擎生成）。raw/ranked 每 horizon
  一次 bulk，整批失败直接抛出不降级为逐股错误。ranked 收尾改为 bulk 落库后读回
  cohort 一次再排名（每 horizon 1 查）；完整性校验仍用内存 code 集合。
- [x] 3.4 行业分类/行业指标/CSI300 按天缓存（省 4.5 万次/天）
  注：`industry_momentum_component(industry_lookup=)` 与
  `real_relative_strength_component(index_quotes=)` 接受按天预取；指标取每
  (行业, horizon) 最新一行后再判 `stock_count>=3`（与逐股语义一致，不向前找更老的
  行）。CSI300 由 `index_quotes_for(code, history, quote)` 按该股自身
  `[min(quote dates), d]` 过滤下发；稀疏股回退到窗口之前时各自读一次自身区间
  （每 code 一次并缓存），否则会把评估日静默降级为 self-proxy。预取失败返回
  `None` 回退组件自身逐股读，不会静默按"无行业/无指数"计分。
- [x] 3.5 等价性验证（harness 与工具部分）：批量化路径与逐股路径对同一天全市场产出逐字段 diff 为空（score/rank/percentile/recommendation/explanation）
  注：`datahub/app/test/test_scoring_batch_equivalence.py` 对同一天全市场逐字段
  （含 `stock` 引用编码）比对 raw 全 horizon/单 horizon、ranked、replace=False
  部分修复（raw 保留 PENDING + ranked 保留 BLOCKED）、dry_run 报告、CSI300/行业
  预取失败回退；另有反向对照（人为扰动批量分必须报差异）与非空断言（真实分量/
  rank/percentile/CSI300 alpha）防止 vacuous；`test_batch_reads_are_constant_in_
  cohort_size` + `test_sparse_history_costs_bounded_extra_reads` 断言读次数与
  cohort 规模无关（稀疏股仅多出自身回退 + 自身指数区间各 1 查）。该 harness 已
  实际拦下两处分歧：pandas NaN 泄漏与稀疏股 CSI300 self-proxy 降级。
  以上均基于 fake 模型 / mongomock 后端，**不是** dev/prod 真实库的全市场比对。
  验证工具已就绪：`./scripts/caifubao score equivalence-check <DATE>
  [--horizons ...] [--mode raw|ranked] [--apply]`（默认只读预览；`--apply` 才跑
  两次 replace 比对并给 before/after 耗时，逐字段 diff 非空则 exit 1）。
- [ ] 3.5a dev 真实库全市场同日逐字段比对（`equivalence-check`，operator 执行）
  注：须与 5.3 的 before/after 耗时采集同批完成 —— 在此之前 3.5 的验收标准
  「对同一天全市场产出 diff 为空」只有 harness 证据，没有真实库证据。

### 信号增量（G1）

- [x] 3.6 引入信号 anchor：cross 信号只算 `date > anchor` 窗口（含 shift(1) lookback）；状态型信号只写最新交易日
- [x] 3.7 状态刷新批量化（仿 `refresh_market_statuses`）；count 改一次 `$group` 聚合（G2 一并完成）
- [x] 3.8 等价性验证：增量与 force 全量对同一最新日的业务 payload 逐字段相等（`generated_at`/`source_freshness` 仅为插入快照）

### FQ 回填改造（F1/Q5）

- [ ] 3.9 回填/历史修复改按交易日 `adj_factor_by_trade_date` 快照（250 次/年）+ 令牌桶 2–4 并发
- [ ] 3.10 `build_fq_factor_frame` 的 adj_factor 对齐向量化（`to_datetime+reindex+ffill/bfill`，语义不变）
- [ ] 3.11 `update_code` 支持 anchor/`date_gt` 增量；状态刷新复用批量路径
- [ ] 3.12 用改造后路径执行交接文档待办 5.2 全市场 FQ 重算（operator），记录实测耗时

### 行情摄取（Q2/Q3/Q1/S2）

- [ ] 3.13 quote bootstrap/INC 向量化构造 UpdateOne（`to_dict("records")` + numpy 列预算）
- [ ] 3.14 INC/FULL/bootstrap 拉取线程池（8–16）+ 主线程批量写；熔断计数加锁；baostock 会话隔离
- [ ] 3.15 完整性检查改单聚合粗筛 + 可疑股 distinct 集合差；日历 bisect
- [ ] 3.16 Parquet 导出流式分区 + 向量化规范化 + boto3 client 复用（S2/S3-P2 一并）
- [ ] 3.17 sync_data.py 改 bulk upsert 或仅重试 BulkWriteError 涉及文档；删除 `_document_size` 字符串化（S3）
- [ ] 3.18 阶段 2 回归：相关 pytest + dev 一次 bootstrap/INC 演练对比基线

### factor_eval 与研究链路（C4）

- [ ] 3.19 `evaluate` 接受预载 `quote_frame`；前向收益 `groupby+shift` 向量化；decay 复用数据集
- [ ] 3.20 tech_factor_runner 全市场 hydrate 改 `.only()` + as_pymongo/frame（C7/C8 内存项）

## 4. 阶段 3 — 研究链路与基建（不阻塞阶段 1/2 合并）

- [ ] 4.1 网格搜索分量矩阵化：分量 raw 值采集一次存矩阵，组合评估改 `R@w - P@wp`（C3）；等价性验证矩阵法与全量重算分数一致
- [ ] 4.2 校准/对比报告 `.only()` + `$unwind/$group` 聚合管道，消除 4–10GB 峰值（C7）
- [ ] 4.3 compute-worker `MAX_CONCURRENT_TASKS` 并发生效（IO 线程池/CPU 进程池，保持原子 claim）（W1）
- [ ] 4.4 评分作业资源归属调整：独立 CronJob pod 或挪 compute-worker（W2）
- [ ] 4.5 抽公共骨架 `wide_frame_bulk_writer` / `select_stale_codes`，factor/signal 迁移复用
- [ ] 4.6 backtest optimize/scan 数据外提 + 基准/股票名循环外提 + `.only()`（C8）

## 5. Review + Merge（每阶段/每独立 PR）

- [ ] 5.1 阶段 1：qa-reviewer（全量）+ contract-reviewer（C2/G3 触及 freshness/timestamps 语义）+ branch conflict + Draft PR CI green
- [ ] 5.2 阶段 2 各 PR：同上；C1/G1/F1 另需 spec-guardian 确认 specs delta 与实现一致
- [ ] 5.3 每阶段合并后在 `docs/operations/perf-analysis-2026-08.md` 对应条目回填 before/after 实测
- [ ] 5.4 部署 dev 验证 → prod 部署（operator）；更新 `docs/capability-inventory.md` 与本文状态
