# Datahub Performance Optimization

## Why

2026-08-28 对 prod→dev 同步、factor、signal、scoring 四条链路（含上游行情摄取与
回测交互）完成了一次全量性能与 CPU 热点审查（三路并行代码深审 + 第一手交叉验证）。
完整发现清单见 [`docs/operations/perf-analysis-2026-08.md`](../../../docs/operations/perf-analysis-2026-08.md)。

核心结论：

- **prod→dev 同步无变更水位**：夜间 CronJob 不带 `--from-date`，每晚把
  quote/factor/signal 全历史（3,000 万+ 文档）重新读出并逐条重 upsert，
  30–90 分钟/晚；而真正的日增量只有 ~5,000 条（<1 分钟）。
- **signal 无增量语义**：每股每次全量读 factor+quote 全历史并全量 upsert
  所有历史命中行，且 `generated_at` 每次必变导致旧文档必然被改写——
  常态日约 7,000 万行读 + 3,000 万级 upsert 写，是全管道最大写放大源。
- **scoring 主路径逐股 N+1**：每股票×horizon ~8 次 Mongo 查询 + 1 次逐文档
  save，全市场 × 3 horizon ≈ 18–20 万次往返/天（~6 分钟，与 datahub pod
  500m CPU 限额定吻合）；年度 replay ≈ 5,000 万次往返。Prod 每日评分实际
  双跑（18:00 `--include-factors` 相位链内一次 + 18:35 CronJob 一次）。
- **FQ 全历史回填路径**（逐股 `adj_factor` + 双重 `iterrows`）被 tushare
  pacing 钉死在 45–60 分钟以上，而交接文档待办 5.2「全市场 FQ 重算」
  正好要走这条路径，是当前路线图上的硬阻塞。
- **CPU 热点集中在逐行 Python**：`iterrows` + 逐行 mongoengine Document
  构造（行情 bootstrap 时 25–50 分钟纯 CPU）、交易日历反复排序/线性扫描
  （4 处，每处每晚 ~4,500 万–1 亿次元素操作）。
- **全链路零并行 + 资源错配**：重 CPU 作业跑在 500m CPU 的 datahub pod；
  compute-worker 配了 2–6 CPU 但实际一次只跑一个任务。

本 change 承载上述问题的分阶段修复。所有性能改造的**结果语义必须与现状
收敛**（分数、信号、因子、新鲜度状态的最终值不变），改变的只是写入批量、
往返次数与重复计算量；少数确有语义变化的点（同步默认增量、信号
`generated_at` 稳定化、prod 评分双跑消除）在本 change 的 specs delta 中显式约定。

## What Changes

分四个阶段，每项独立可验证（编号与任务清单、分析报告一致）：

**阶段 0 — 度量基线（改码前）**
- 采集一个完整交易日的 `datahub_job_runs` 耗时基线；pod 内 py-spy 火焰图 +
  Mongo profiler 钉死 C1/Q2/F1 热点占比。

**阶段 1 — 快赢（小改动）**
- S1：sync engine 增加每集合水位线，默认增量同步，全量需显式
  `--allow-full-sync`；每周低峰全量对账；冷启动建议 mongodump/mongorestore
- C5：`assign_ranks` 改 `bulk_write($set:{rank,percentile})`；replay 复用
  内存结果，消除重复全量重读
- C6：验证服务仅验「到期未验且有新行情」，未来行情按天批量拉，更新走
  `bulk_write`，查询加 `.only()`
- C2：消除 prod 评分双跑（18:00 相位链去掉 `update_scoring` 或 18:35 跳过
  已评分日）
- R4：四处交易日历排序/线性扫描改 bisect/预排序缓存
  （scoring_service.py:67-69、fq_factor.py:302-306、trading_day_helper.py:129-138、
  data_asset_status_helper.py:206-210）
- F4：factor_runner 的 MA stale 分支对齐 `update_market` 批量路径
- Q4：删除 `get_hist_stock_quote_data` 内重复查询死代码，`stock_obj` 直传
- G3：信号 `generated_at`/`source_freshness` 提为循环外常量（行为语义见 specs delta）

**阶段 2 — 结构性改造（每项独立可验证）**
- C1：评分按「天」批量取数（~4 条查询/天替代 18–20 万次逐股往返）+
  分量 pandas 化 + `bulk_write` 持久化
- G1：信号增量 anchor（cross 信号只算 anchor 后窗口；状态型信号只写最新
  交易日），消除每日全历史重算与全量 upsert
- F1：FQ 回填改按交易日全市场 `adj_factor_by_trade_date` 快照 + 因子对齐
  向量化 + `update_code` anchor 增量（解锁待办 5.2 全市场 FQ 重算）
- Q2+Q3：行情 bootstrap/INC 向量化构造（`to_dict("records")`）+ 线程池拉取
- Q1：完整性检查单聚合粗筛（`$group by code` + 可疑股 distinct 集合差）
- C4：factor_eval 接受预载行情 frame，前向收益 `groupby+shift` 向量化
- S2：Parquet 导出流式分区（游标按 date 排序天然分区连续）

**阶段 3 — 研究链路与基建**
- C3：网格搜索改「分量矩阵算一次 + numpy 重新加权」，权重组合评估从
  不可行变为秒级
- C7：校准/对比报告全面 `.only()` 与聚合管道，消除 4–10GB 内存峰值
- W1/W2：compute-worker 并发生效；评分作业资源归属调整
- 抽公共骨架 `wide_frame_bulk_writer` / `select_stale_codes`，统一
  factor/signal 两套同构实现

## Non-goals

- 不改变评分/因子/信号的**计算结果语义**（分数、推荐、rank/percentile、
  FQ/MA 因子值、信号命中集的最终收敛态与现状一致）
- 不改变数据源（tushare/akshare 混合现状）、Mongo schema 主键与唯一索引
- 不改变 freshness 语义本身（status 状态机、`data_as_of`、新鲜度判定规则）
- 不在本 change 内引入多 pod 数据面并行或新的存储引擎（水位线、批量、
  向量化、线程池为主）
- 不改变 API 契约与鉴权；`stock_signal_daily` 的 dev_only 同步规则保持不变
- 阶段 3 的 C3/W1/W2 不阻塞阶段 1/2 的独立合并
