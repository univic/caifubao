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

- [ ] 2.5 `assign_ranks` 改 `bulk_write($set:{rank,percentile})`，值未变跳过（C5）
- [ ] 2.6 replay/评分收尾（rank/upgrade/aggregate）消费内存结果，消除 3 次全量重读（C5）
- [ ] 2.7 验证服务：仅验「到期未验且自上次验证后有新行情」；未来行情按天批量拉；`bulk_write` 更新；`.only()` 投影（C6）
- [ ] 2.8 消除 prod 评分双跑：18:00 相位链去 `update_scoring` 或 18:35 跳过已评分日（C2，prod overlay 变更）

### 日历与杂项（R4/F4/Q4/G3）

- [ ] 2.9 四处日历排序/线性扫描改 bisect/预排序缓存：scoring_service.py:67-69、fq_factor.py:302-306、trading_day_helper.py:129-138、data_asset_status_helper.py:206-210
- [x] 2.10 factor_runner MA stale 分支改调 `update_market`（对齐 FQ 分支）（F4）
- [ ] 2.11 `get_hist_stock_quote_data`：`stock_obj` 直传参数 + 删除重复查询死代码（Q4）
- [x] 2.12 信号 `generated_at`/`source_freshness` 提为循环外常量或 `$setOnInsert`（G3，语义见 specs/signals-mvp delta）
- [ ] 2.13 阶段 1 回归：datahub pytest 全绿 + 一次 dev 全链路跑批对比基线

## 3. 阶段 2 — 结构性改造（每项独立 PR、独立验证）

### 评分批量取数（C1）

- [ ] 3.1 `score_all_stocks` 按天批量预取：当日 quote/factor/signal/existing + 全市场 lookback 窗口行情（code→DataFrame）
- [ ] 3.2 分量计算 pandas 化（保持各组件 raw_value/weight 逐股一致）
- [ ] 3.3 持久化改 `bulk_write(upsert)`，替换逐文档 save（新值语义与 `_persist_prediction` 一致）
- [ ] 3.4 行业分类/行业指标/CSI300 按天缓存（省 4.5 万次/天）
- [ ] 3.5 等价性验证：批量化路径与逐股路径对同一天全市场产出逐字段 diff 为空（score/rank/percentile/recommendation/explanation）

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
