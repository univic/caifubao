# Design

> 详细发现与量级推算见 [`docs/operations/perf-analysis-2026-08.md`](../../../docs/operations/perf-analysis-2026-08.md)。
> 本文件只记录跨条目的技术决策与不变量。所有改造的统一验收标准：
> **最终数据收敛态与现状逐字段一致**（除 specs delta 显式约定的 generated_at 稳定化）。

## 1. 同步水位线（S1）

```python
# data_sync_state（新 meta 集合，每集合一条）
{ "_id": "stock_daily_quote",
  "watermark_date": datetime("2026-08-28"),
  "watermark_count": 31_246_802,       # 水位当日源库 count，用于对账
  "updated_at": ... }
```

- 默认路径：`date >= watermark_date`（含水位日重跑，靠 `_id` upsert 幂等去重）；
  成功后水位推进到本次最大 date。
- 全量：仅显式 `--allow-full-sync`；`dry_run` 不推进水位。
- 对账：低峰周任务比对源/目标 count 与 max(date)，漂移即告警并提示全量。
- `finance_market`/`stock_industry` 保持恒全量（小集合）；`stock_signal_daily`
  的 `dev_only` 门控不变。
- 冷启动（水位不存在）：视为全量，但 runbook 建议 mongodump/mongorestore。

## 2. 批量写范式（R3，统一照抄现有实现）

- 预测/排名/验证更新：`bulk_write([UpdateOne({...}, {"$set": {...}})], ordered=False)`
  —— 范式源 `scoring_service._upgrade_recommendations`（:309-311）。
- 状态刷新：`aggregate_stats_by_code` + `bulk_upsert_asset_status`
  （data_asset_status_helper.py:379-445）—— 范式源 `_flush_batched_quote_updates`。
- 分块统一 1 万 op/批（现状 500 偏小）。
- 不变量：upsert 键与唯一索引一致（`{stock_code,date,horizon,model_version}` /
  `{code,date}` / `{stock_code,date,signal_name}`）；`ordered=False` 保持；
  失败以 BulkWriteError 定位单项，不做整批回退。

## 3. 评分按天批量取数（C1）

- 每天恒定 ~4 条查询：
  1. `StockDailyQuote.objects(date=d)` 当日行情；
  2. `date__gte=d-120` 全市场窗口行情 → `groupby("code")` 供 momentum/breakout/risk；
  3. `StockFactorDaily.objects(date=d)` 当日因子；
  4. `StockSignalDaily.objects(date__gte=d-decay_max)` 近窗信号（衰减回看合并进来）。
  existing predictions 用第 5 条 `date=d` 一次取回。
- 分量计算输入从「Document 对象列表」换「DataFrame/dict」：
  组件函数保持纯函数签名，接受 `quote_row: dict` 与 `history: DataFrame`，
  raw_value/weight 输出与现状逐股一致（等价性测试保证）。
- 持久化 payload 构造与 `_persist_prediction` 字段集完全一致。
- `DATAHUB_SCORING_MODE=ranked` 路径共享同一批预取与同一套分量函数，
  仅聚合阶段不同（两阶段 rank 归一化保留）。

## 4. 信号增量 anchor（G1/G3）

- anchor = 该 `(code, signal_name)` 的 `DataAssetStatus.latest_data_date`，
  表示“已评估至”；即使当日零命中也推进。不以稀疏的
  `StockSignalDaily` 最大 date 作为水位；任一 signal status 缺失时，
  该 code 回退到全量读取以完成冷启动。
- MA10_CROSS_MA20：读 `date >= anchor 前一交易日` 的 factor/quote（shift(1)
  需要 1 行 lookback），只计算 `date > anchor` 的窗口。
- PRICE_ABOVE_MA60 / MA20_ABOVE_MA60（状态型）：评估并写入各自
  anchor 之后的所有新交易日；常态日仅一日，多日积压不丢历史命中。
- `generated_at`：`$setOnInsert`（新文档记录生成时间，重算不触碰旧文档）——
  这是 specs/signals-mvp delta 显式约定的唯一语义变化（旧文档生成时间冻结）。
- `source_freshness`：每 code 计算一份，与 `generated_at` 一样仅在插入时
  写入，表示生成时的来源快照，重跑不触碰旧文档。
- stale 增量仅用于 anchor 之前上游未被修正的日常路径。`force`
  为权威全历史重建：先 upsert 完整因子输入对应的命中集，再精确删除
  不再命中的旧键，避免写入中途失败时先破坏旧权威集；
  落库成功后才推进 status。任何计算、bulk write 或 status write 失败都
  向调用方传播，不把该 code 记为成功。
- 全市场 status 刷新从最终落库数据一次 `$group`，再批量 upsert；
  `data_count` 是实际持久化命中数，而不是本次构造的 operation 数。
- 语义不变量：任何日期的信号命中集、strength、direction、factor_snapshot
  与全量重算逐字段一致（等价性测试：增量重跑 vs 全量重算 diff 为空）。

## 5. FQ 回填按交易日快照（F1/Q5）

- 回填循环改为**日期外层**：对每个历史交易日调
  `tushare_interface.adj_factor_by_trade_date(date)`（全市场一次调用），
  与该日已落库 quote 一对一 merge（复用 `build_market_snapshot_frame`，
  fq_factor.py:146-223，已是向量化的），按日 bulk_write。
- pacing：令牌桶（300/min 配额，2–4 并发），替代固定 `sleep(0.25)` 串行。
- 顺序无关：快照路径逐日写 `{code,date}` 字段，任意中断可重跑（幂等）。
- 单股 `update_code`（增量修复路径）保留，但支持 anchor：
  `_load_quote_df(code, date_gt=anchor)` + `adj_factor(ts_code, anchor, end)`。
- 因子对齐向量化（:76-91 替换）：
  `pd.to_datetime(trade_date, format="%Y%m%d")` → `to_numeric` →
  `drop_duplicates("trade_date").set_index` → `reindex(union).ffill().bfill()`，
  与现 fallback anchor 语义一致（已由现有测试 test_fq_factor.py 覆盖语义）。

## 6. 并发纪律（R5/Q3/W1）

- 先向量化、后并行：逐行 Python CPU（iterrows/Document 水合）不并行化，
  一律先改 `to_dict("records")`/numpy/pandas；I/O 段才用线程。
- 线程池边界：拉取/查询/S3 上传用 `ThreadPoolExecutor(8-16)`；
  `bulk_write` 与状态刷新收敛回主线程，保证每批一次事务性推进。
- pymongo client 进程内共享线程安全；baostock 单会话非线程安全——
  baostock 源保持串行或每线程独立 login。
- tushare：令牌桶 300/min，2–4 并发打满配额即停，不超发。
- compute-worker（W1）：保持 `_fetch_next_task` 原子 claim；并发执行时
  `MAX_CONCURRENT_TASKS` 个 worker 线程各自跑 `_run_task`（任务为 IO 型
  居多，线程池即可；CPU 型任务（组件矩阵、向量化后）单进程足够）。

## 7. 网格搜索分量矩阵（C3）

- 采集：对 (date, stock, horizon) 全网格把各分量 raw_value 采一次，
  存 `numpy` 矩阵（研究进程内存）或临时 parquet；权重组合评估 =
  `(R @ w - P @ wp)`，归一化在矩阵层完成。
- 等价性：矩阵法分数与对任一组合跑全量 replay 的分数一致（浮点容差 1e-9）。
- 阈值网格只影响 recommendation 映射，事后向量化重算，零重评。

## 8. 资源归属（W2）

- 目标态：scoring/网格/replay 类重 CPU 作业以独立 Job/CronJob 跑
  （compute-worker 同款 2–6 CPU 配额），datahub 常驻 pod 保持轻量健康检查
  与调度；短期缓解可先给 scoring 独立 Job 提升 CPU limit。
- 不改变 `datahub_job_runs` 记账语义（job_name/family/trigger 原样）。
