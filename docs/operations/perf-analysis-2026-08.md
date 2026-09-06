# Caifubao 性能与 CPU 热点分析报告（2026-08-28）

> **用途**：本文是 datahub 四条链路（prod→dev 同步、factor、signal、scoring，含上游行情摄取）
> 的性能问题盘点与解决方案路线图，供后续 agent 按任务推进时查阅细节。
> 配套 OpenSpec change：`openspec/changes/datahub-perf-optimization/`（proposal/design/tasks/specs）。
>
> **方法**：三路并行代码深审（sync / factor+signal / scoring）+ 全部关键断言的第一手交叉验证。
> 全程只读，未修改任何代码。规模基准：~5,200 只活跃 A 股，老股全历史 ~6,000–8,500 个交易日，
> `stock_daily_quote` 约 3,000 万+ 文档；Mongo 往返 ~0.3–3ms；tushare 配额 300 次/分钟。

---

## 1. 全景：夜间流水线与成本地图

**Prod（工作日，Asia/Shanghai，`k8s/overlays/example-production/datahub-cronjobs.yaml`）**：

```
18:00 quote-index(CronJob)
18:00 quote-stock(CronJob, --include-factors) → run_stock_job 相位链(china_a_stock.py:156-166):
     quote同步 → FQ因子 → MA因子 → 信号 → 【评分 score_all_stocks 全市场×3 horizon】
18:30 signal(CronJob)   → 信号兜底重跑（stale 集通常为空，代价小）
18:35 scoring(CronJob)  → 再跑一遍评分（existing 短路 + assign_ranks 全量重写）
12日/月 industry-sync
```

**Dev**：18:10 quote-stock（`--include-factors`，suspend 默认）→ 19:15 data-sync CronJob（**不带 `--from-date`，见 S1**，prod signal/scoring 之后拿到当日信号）；
评分不同步到 dev，靠 `./scripts/caifubao score score-all`（kubectl exec 进 datahub pod，**CPU limit 500m**，
`k8s/base/datahub.yaml:95-101`）。

| 环节 | 每日常态成本 | 主要瓶颈 |
|---|---|---|
| prod→dev sync | 增量仅需 ~5,000 条/<1min，实际全量重灌 3,000 万+ 条/30–90min | 无水位线 |
| quote 摄取 | 正常日快照路径已优化（~3 次调用/分钟级）；bootstrap/INC 路径小时级 | iterrows + 逐行 mongoengine 构造 |
| factor | 正常日增量良好；FQ 历史回填/修复路径 45–60min 起（tushare pacing 钉死） | 逐股调用模式 |
| signal | **每日约 7,000 万行读 + 3,000 万级 upsert 写**，全管道最重写放大 | 无增量、全历史重算 |
| scoring | ~18–20 万次 Mongo 往返/天（文档口径 ~6 分钟，与 500m CPU 限额定吻合）；年度 replay ≈ 5,000 万往返 | 逐股 N+1 + 逐文档 save |

---

## 2. 横切根因（所有发现的归纳）

- **R1 逐股 N+1 查询**：scoring 每股票×horizon ~8 次查询（scoring_service.py:184-210、:718-726、
  components.py:353/365-374/575-579）；signal 每股 ~10 次（signal_factory/moving_average.py:132-182、:234-256）；
  完整性检查每股 1 次全历史读（stock_quote.py:96-101）；factor_eval 逐观测点 ~4 次（factor_eval.py:104-125）。
  索引齐备，**问题全在往返次数与 mongoengine Document 水合 CPU，不是索引缺失**。
- **R2 `iterrows`/逐行 mongoengine 构造**：china_a_stock.py:1095-1104（每行 new Document +
  `to_mongo().to_dict()`，bootstrap 时 3,000–4,000 万行 ≈ 25–50 分钟纯 CPU）；
  fq_factor.py:76/128、factor_factory/moving_average.py:131、signal_factory/moving_average.py:193。
  `iterrows` 每行装箱 Series，比 `itertuples`/`to_dict("records")` 慢 ~50×。
- **R3 逐文档 `save()` 而非 bulk_write**：assign_ranks（scoring_service.py:257-270，1.5 万次/天）、
  persist（:646-657）、verify（verification_service.py:74/104）、signal 状态刷新。
  仓库内已有正确范式可照抄：`_upgrade_recommendations`（scoring_service.py:309-311）、
  `bulk_upsert_asset_status`（data_asset_status_helper.py:379-445）。
  *澄清：mongoengine `save()` 走 `_get_update_doc()` 增量 `$set`（mongoengine/document.py:551-574，已核实），
  并非全文档重写；真实代价是往返次数而非字节量。*
- **R4 交易日历反复排序/线性扫描（4 处，每处每晚 ~4,500 万–1 亿次元素操作）**：
  scoring_service.py:67-69（`get_t_plus_n_day`）、fq_factor.py:302-306（`_is_next_trading_day`，逐股重建排序）、
  trading_day_helper.py:129-138（`determine_trading_date_diff`）、data_asset_status_helper.py:206-210。
  日历本身升序，全部可换 `bisect`/预排序缓存。
- **R5 全串行 + 资源错配**：数据路径零并行（唯一线程池是 APScheduler 调度用，datahub/__init__.py:203）；
  评分跑在 500m CPU 的 datahub pod；compute-worker 配了 2–6 CPU 但主循环一次只跑一个任务，
  `MAX_CONCURRENT_TASKS` 配置实际未被使用（compute-worker/worker.py:96-112）。
- **R6 逐股调用 vs 全市场快照**：tushare 每次调用强制 `sleep(0.25)`（tushare_interface.py:95/139/155）→
  逐股模式吞吐上限 ~240 次/分钟。日快照模式（`adj_factor_by_trade_date`、`pro.daily(trade_date)`）已证明正确
  且已用于 quote UPD 与 FQ stale 路径，**但历史回填/修复路径未接入**。
- **R7 同步无变更水位**：sync_engine.py:131-150 `query={}` 全表读 + 全量 upsert。

**并发策略结论**（GIL 相关）：
- I/O 段（Mongo 读、HTTP 拉取、S3 上传）→ `ThreadPoolExecutor(8-16)` 有效（pymongo 线程安全、
  网络等待释放 GIL）；pymongo client 可进程内共享，baostock 单会话非线程安全需隔离。
- 逐行 Python CPU（iterrows、Document 水合）→ 线程无效，必须先向量化，之后线程扩展才线性。
- 进程池仅对逐股 pandas 回填可能有意义（worker 内须新建 pymongo client），优先级低于向量化。

---

## 3. 发现清单

> 严重度：P0=阻塞/每晚大额浪费；P1=显著；P2=顺手修。编号在各环节内保持唯一，
> OpenSpec change 的 tasks.md 按此编号引用。

### 3.1 prod→dev 同步（S 系列）

| ID | 严重度 | 位置 | 问题 | 修复 |
|---|---|---|---|---|
| S1 | **P0** | sync_engine.py:131-150、:168-192；k8s dev overlay datahub-cronjobs.yaml L238-251（CronJob 不带 `--from-date`） | 夜间同步无变更水位：`query={}` 全表读 + 全量 upsert。stock_daily_quote ≈ 3,000 万+ 条 → 30–90 分钟/晚、10–60GB 传输；日增量仅 ~5,000 条。幂等但每次全量重传；未改动文档不产生 oplog 却照付 BSON 解码+往返 | ①每集合水位线（meta 集合记上次最大 date/count），默认增量 `date >= watermark`；②CLI 默认带最近交易日，全量需显式 `--allow-full-sync`（sync_data.py:284-290 已有此闸门思路）；③每周低峰一次全量对账；④冷启动改 mongodump/mongorestore（快一个数量级） |
| S2 | P1 | parquet_export_runner.py:154-164、:182-184、:89-118 | 全集合一次性物化 list[dict]（5–15GB 内存）+ `_normalize_value` 逐值递归（全量 2.5 亿次函数调用）+ `pd.DataFrame(list-of-dicts)` 慢路径 | 游标按 date 排序流式切分区（排序游标天然分区连续，内存 O(单日 5,000 行)）；规范化 pandas 向量化或服务端 `$project/$dateToString`；`pyarrow.Table.from_pylist` 列式构造 |
| S3 | P2 | sync_data.py:184-198、:128-131、:102-125 | `insert_many` 撞唯一键整批回退逐条 `update_one`（重复同步恰是常态→必走慢路径，千万级单条往返）；`_document_size=len(str(doc))` 逐条字符串化；`$or [datetime,字符串]` 双分支日期过滤 | 改 `bulk_write(UpdateOne upsert)` 或只重试 `BulkWriteError` 涉及文档；删字符串化或抽样；统一日期类型 |

### 3.2 行情摄取（Q 系列；sync 的上游，决定 factor 启动时间）

| ID | 严重度 | 位置 | 问题 | 修复 |
|---|---|---|---|---|
| Q1 | **P0** | data_integrity_keeper/handler/stock_quote.py:94-129（dispatch_check :66-75 对全部 5,000 只循环） | 完整性检查逐股物化全部行情日期（老股 8,000+ Document，`.only("date")` 仍水合 Document）×3 次遍历（bool/min/max）+ 2 次 `trade_calendar.index()` 线性扫描 → 全市场 ~1.2 亿次元素操作，数十分钟级 | 一条 `$group by code`（min/max/count）全局粗筛 → 仅可疑股取 `distinct("date")`（as_pymongo）做集合差；日历 bisect。预计 <10 秒 |
| Q2 | P1 | china_a_stock.py:1095-1104 + :73-91（`_build_stock_quote_upsert_operation`） | `iterrows` + 每行 new `StockDailyQuote()` + 18 次 setattr 描述符 + `to_mongo().to_dict()` 构造 upsert——bootstrap/INC 补历史时 3,000–4,000 万行 ≈ 25–50 分钟**纯 CPU**（全仓最热 CPU 点） | `quote_df.to_dict("records")`（~1-2µs/行）直构 UpdateOne；amplitude/change_amount 用 numpy 列预算；缺省列 `fillna/astype` 预处理。CPU 降 20–40 倍。索引断言：`{code,-date}` 唯一索引已存在（model/stock.py:50-51） |
| Q3 | P1 | china_a_stock.py:490-638（逐股循环）+ zh_a_daily.py:427-447 | UPD 之外的 INC/FULL/bootstrap 路径逐股串行 HTTP（akshare 300–500ms/股）≈ 25–40 分钟纯等待。**正常日 UPD 快照路径已被 #132 优化为分钟级，此项只影响补历史/非 tushare 源** | `ThreadPoolExecutor(8-16)` 包拉取（网络等待释放 GIL），bulk_write 与状态刷新留主线程批量做；熔断计数 `consecutive_history_failures` 加锁；baostock 单会话需线程隔离或保持串行；tushare 源用令牌桶（见 Q5） |
| Q4 | P1 | 三处日历 O(N×C)（见 R4）；china_a_stock.py:940/1110（akshare 路径逐股状态重读 + 全历史聚合 + save，批量基建已存在未接入）；china_a_stock.py:1063-1067 vs :1084-1089（同条查询逐字重复的死代码） | 每晚多付 1.5 万+ 往返与 ~1 亿次日历元素操作 | 状态刷新复用 `_flush_batched_quote_updates` 的批量范式（:977-1052）；`get_hist_stock_quote_data` 加 `stock_obj=None` 可选参数直传，删重复块；日历 bisect |
| Q5 | P1 | tushare_interface.py:95/139/155 | 每次调用 `sleep(0.25)` 且全串行：FQ 逐股历史修复/回填被钉死在 45–60 分钟（见 F1） | 按交易日全市场快照（调用量降 ~300 倍）+ 令牌桶/滑动窗口允许 2–4 并发打满 300/min 配额 |

### 3.3 factor（F 系列）

| ID | 严重度 | 位置 | 问题 | 修复 |
|---|---|---|---|---|
| F1 | **P0** | fq_factor.py:240（`_load_quote_df` 全历史）、:275-289（逐股 `adj_factor`）、:76-91（iterrows 建因子映射）、:126-144（iterrows 建 ops）；历史修复 :502-508、backfill_all :523-543 | FQ 全历史路径：每股全历史 quote 读 + 逐股 tushare `adj_factor`（0.25s sleep，1990 年起 ≈2 窗口调用）+ 双重 iterrows（~8,500 行/股）→ 全市场回填 ≈ 8,500 万行 iterrows + 1 万次 tushare 调用（仅 API ≥42 分钟）+ 每股 3 次状态往返。**交接文档待办 5.2「全市场 FQ 重算」正好踩中此路径，是当前路线图硬阻塞** | ①:76 向量化：`pd.to_datetime(trade_date, format="%Y%m%d")`+`to_numeric`+`drop_duplicates.set_index` 后 `reindex+ffill/bfill`（与现 fallback anchor 语义一致）；②回填改**按交易日**取 `adj_factor_by_trade_date` 快照（250 次/年 vs 5,000×窗口次）；③`update_code` 支持 anchor/`date_gt` 增量（正常日已由 snapshot 写入，历史路径只补缺失日）；④状态刷新复用 :389-428 批量路径；⑤`_build_bulk_operations` 改 `to_dict("index")`/`itertuples` |
| F2 | P1 | fq_factor.py:291-311（调用点 :356-363 逐 code 一次） | `_is_next_trading_day` 每股重建+排序整张日历（每元素 2 次 `pd.Timestamp` 构造+normalize）→ ~4,500 万次构造，1–2 分钟纯 CPU/次 stock job，GC 压力大 | `_get_market_update_plan` 开头一次性 `np.sort(pd.DatetimeIndex(calendar).normalize().values)`，循环内 `np.searchsorted` 判定前邻交易日 |
| F3 | P1 | factor_factory/moving_average.py:334-360（读链每股 ~6 次往返，~3 万次/晚）、:103-122、:201-209 | MA 链路逐股串行；anchor 退化为 None 时全历史重算（4,200 万行 iterrows）；`refresh_market_statuses` 5 窗口各一次 `$in` 5,000 code 聚合（`ma_N $exists` 无法走索引，每次扫 ~3,500 万索引项） | ①全市场一趟向量化：一条近 130 交易日查询（命中 `(date,code)` 索引，~65 万行）→ `groupby("code")["close_hfq"].rolling(w).mean()` → 只写 anchor 后的行 → 1 万 op/块 bulk_write（3 万往返→~5 次）；②单趟 `$group` 聚合用 `$sum:{$cond:[{$gt:["$ma_10",null]},1,0]}` 同时统计 5 窗口计数 + `$max/$min` date |
| F4 | P1 | jobs/factor_runner.py:155-167 + moving_average.py:265-266、:161-175 | CLI `--factor ma --mode stale`（默认）逐股 `update_code(refresh_statuses=True)` → 每股 11 次状态往返（全量 5.5 万次），绕开 cron 路径已有的批量刷新——同一业务两套性能不同的实现 | **已实现，待 dev 实测**：stale 路由 `update_market(selected_codes=...)` 批处理，`--code/--limit/market` 选择集原样传递，dry-run/force 语义保持 |
| F5 | P2 | factor_factory/moving_average.py:70-93、:95-101 | `_get_incremental_anchor_date` 每股 2 次状态查询（`get_codes_requiring_update` 已批量读过一遍，重复） | 复用批量 plan 查询结果传参 |

*正面确认：MA anchor 增量 + 119 行 lookback 语义正确（moving_average.py:95-122）；FQ 正常日 snapshot 路径设计良好（fq_factor.py:430-486，1 次 tushare + 2 次大查询 + 1 次 bulk write + 批量状态刷新），均无需动。*

### 3.4 signal（G 系列；全管道最大写放大源）

| ID | 严重度 | 位置 | 问题 | 修复 |
|---|---|---|---|---|
| G1 | **P0** | signal_factory/moving_average.py:139-164（全历史读）、:276-287（全历史重算 3 信号）、:193（iterrows 全量 ops）、:226（`generated_at: datetime.now()`） | **无增量语义**：每股每次全量读 factor+quote 全历史（~2×7,000 行），3 个信号对整段历史重算并全量 upsert 所有命中行；`generated_at` 每次必变使旧文档必然被改写。常态日 5,000 股 ≈ **7,000 万行读 + 3,000 万级 upsert 写**（`PRICE_ABOVE_MA60`/`MA20_ABOVE_MA60` 为状态型，~40–50% 历史日命中），写放大、oplog、WiredTiger cache 全承压；也是 18:00 相位链最重相位，推迟同链 scoring | **已实现，待 dev 实测**：以 `(code, signal_name)` 的 status `latest_data_date` 作“已评估至” anchor；cross 仅读 anchor 前一交易日起的窗口，三类信号均只写各自 anchor 后的新日期（多日积压不丢历史命中）；`generated_at/source_freshness` 仅插入时写；force 先 upsert 权威集再删除旧键 |
| G2 | P1 | signal_factory/moving_average.py:132-137（每股 stock 查询）、:166-182（status 查询）、:234-256（每信号 count+update）；jobs/signal_runner.py:120-132（串行） | 逐股 ~10 次串行往返（stock 1 + factor 1 + quote 1 + status 1 + 3×(count+update)=6）≈ 5 万次/晚；per-signal `count()` 可合并 | **已实现，待 dev 实测**：成功 code 的持久化命中数以一次 `$group` 聚合，所有 signal status 一次 bulk upsert；单 code/bulk 失败不推进 freshness，runner 返回 failed codes |
| G3 | P2 | signal_factory/moving_average.py:85、:225-226 | `build_signal_frame` 每信号 `copy().sort_index()` 一次全历史帧（数据已保序，冗余）；`source_freshness` 3 个 status 快照逐行内嵌 | `_load_factor_df` 末尾排序/`to_numeric` 一次，各信号复用只读视图；freshness 每 code 一份或入 run 级文档 |

### 3.5 scoring（C 系列）

| ID | 严重度 | 位置 | 问题 | 修复 |
|---|---|---|---|---|
| C1 | **P0** | scoring_service.py:184-210（每股 5 查）+ :718-726（信号衰减回看）+ components.py:353/365-374（行业×2）+ :575-579（CSI300）+ :646-657（逐文档 save） | **主路径逐股 N+1**：每股票×horizon ~8 读 + 1 写 → 15,000 次 `score_single_stock`/天 ≈ **18–20 万往返/天**（本地 3–5 分钟，500m CPU pod 下 ~6 分钟）；年度 replay ≈ 5,000 万往返、十几小时起。CPU 主因是每查询的 mongoengine Document 水合 | 按「天」批量化：每天 4 条查询拿全市场数据（当日 quote/factor/signal/未来 N 日行情窗口，`date__lte=d` 按 code 取窗口或一次拉 250 天做 code→DataFrame），分量计算 pandas 化；持久化 `bulk_write(UpdateOne({stock_code,date,horizon,model_version},{$set:...},upsert=True))`。往返可降 ~95% |
| C2 | **P0** | china_a_stock.py:164（相位链含 `update_scoring`）+ k8s prod overlay L134（`--include-factors`）与 L313-340（18:35 scoring CronJob） | **Prod 每日评分双跑**：18:00 相位链内全量真算（此时无 existing）→ 18:35 CronJob 再跑：existing 短路逐股查询 1.5 万次，但 `assign_ranks` 仍 45k 次读+delta save、`_upgrade_recommendations`/`aggregate_industry_metrics` 各 3 次全量重读 | **结构性消除（2026-08 拆分 CronJob）**：`run_stock_job` 相位链移除 `update_signals`/`update_scoring`，信号/评分只由独立 signal/scoring CronJob 承担（18:30/18:35，依赖门基于已落库数据放行），双跑不再发生；服务层 skip-complete-cohort（#143）仍保留以防手动重复触发 |
| C3 | **P0** | grid_search.py:150-167（笛卡尔积）× experiment_service.py:43-55 × replay_service.py:38-66 | **网格搜索指数放大**：权重组合只作用于最终加权和（scoring_service.py:480-488），**raw 分量对组合不变**，目前却每组合完整重算 replay。8 分量×3 档=6,561 组合/horizon → 完全不可行 | 「分量矩阵算一次 + numpy 重新加权」：先对 (date,stock,horizon) 采一次各分量 raw 值存矩阵（numpy/parquet/专用 collection），网格退化为 `R@w - P@wp` 矩阵乘，6,561 组合从月级到秒级；利用现有 normalize（:106-119）去重等价组合；阈值网格只影响 recommendation，可事后零成本重算 |
| C4 | **P0** | factor_eval.py:104-106（base 行情逐观测）、:117-125（每 horizon 未来行情）、:306-315（`_compute_decay` 用 6 个 horizon 重跑整个 `_build_dataset`）；调用方 tech_factor_runner.py:133-135 已把全市场行情载入内存 | 逐观测点 N+1：全市场年评估 1.25M 观测 ×(1+3) + decay 1.25M×(1+6) ≈ **1,375 万次查询**（1ms 也要 ~4 小时），而行情已在内存 | `evaluate` 增加可选 `quote_frame`（code→按日排序 close 表）；前向收益 `groupby("code")["close_hfq"].shift(-h)/close - 1` 向量化；decay 复用同一数据集只补 1/3/10 三个 horizon 的 shift。DB 查询归零 |
| C5 | P1 | scoring_service.py:257-270（assign_ranks 逐文档 save，1.5 万次/天×delta `$set`）、replay_service.py:38-66（每 horizon 重新拉股票全集 + 3 次重读刚写完的当日结果 :140-146/:258-265/:279-286）、components.py:353/365-374/575-579（行业分类、行业指标、CSI300 可按天缓存，各省 1.5 万次/天）、scoring_service.py:67-69（`get_t_plus_n_day` 每次排序日历） | 高频重复开销合计每晚数万往返 + 分钟级纯 CPU | **部分已实现，待 dev 实测**：`assign_ranks` 仅为 rank/percentile 实际变化行生成 `UpdateOne`，空 delta 零 bulk write（2.5）；ranked 收尾消费内存结果——`assign_ranks(predictions=)` 免 cohort 重查、`_require_complete_prediction_set(persisted_codes=)` 做内存完整性校验（含 replace=False 修复路径下存留的既有 BLOCKED 行，不误报不完整）、BLOCKED 行不入内存排名（2.6）。遗留：replay_service 与 `_upgrade_recommendations` 的收尾全量重读、行业/指数缓存与日历优化（后续 C1/C5 工作） |
| C6 | P1 | verification_service.py:32-48、:59-65、:74/:104；入口 scoring_runner.py:427-434（from_date=None） | 验证逐条「1 查未来行情 + 1 save」。*量级修正：查询含 `target_date__lte=today`，在途 TRACKING（target 未到）被挡在外面，稳态日处理量是「当日到期未验」~1.5 万条（≈3 万往返），并非深审初报的 42.5 万条/85 万往返；但 verify 中断、停牌积压、`--replace` 重置后池子会放大* | **部分已实现，待 dev 实测**：新增批量路径 `verify_predictions_batch`/`_verify_many`——未来行情按 stock_code 单次拉取（`.only()` 投影含 hfq 价格字段）后按 (date, target_date] 双界 bisect 切片、逐预测判定，status/verification 更新走 `bulk_write($set)`；`verify_predictions` 路由到批量路径（2.7）。遗留：候选查询仍全量水合（预测侧 `.only()` 未应用）、「仅验到期未验且自上次验证后有新行情」增量门槛（`last_verified_date` 或分桶）未实现 |
| C7 | P1 | calibration_report.py:34-42、:167-178；comparison_report.py:101-118；factor_eval.py:263-268 | 报告全历史加载胖文档：1 年×5,000×3 ≈ 125 万条（**4–10GB**），`_component_summary` 按 8 分量各 append 整个 prediction（引用 ×8）；comparison 双版本同时载入 | 全面 `.only()`；分量统计改聚合管道（`$unwind:"$explanation.components"` + `$group`）或按月分块流式累加；comparison 改流水式 |
| C8 | P2 | scoring_service.py:448-467（rank 分量提取 `next()` 嵌套线性扫描，O(C²N)≈40 万次/天）、:575（blocked O(n) 查 stock）、config.py:99（每股票每 horizon deepcopy config，1.5 万次/天）、components.py:471-482（aggregate 逐行业重扫 predictions）、technical_factors.py:26-185（纯 Python O(n×window)：yearly_position 全市场 ≈3 亿次运算，仅研究链路）、backtest_service.py:602-701（optimize 27 组合=54 次相同数据加载；scan 3,000 股串行且基准/股票名重复查） | 小热点集合 | 构造 `raw_by_component[cid][code]` 索引、`stocks_by_code`、config 按 horizon 缓存、industry groupby 一次；技术因子换 pandas rolling；optimize/scan 外层加载一次内层换参重跑 `_simulate`，基准/股票名提到循环外，分数查询加 `.only("date","score","stock_code")` |

*正面确认：回测**不重算评分**，直接读已落库 `StockScorePrediction`（backtest_service.py:685-717），模拟循环 `_simulate`/`_simulate_multi` 全内存无逐日查询——设计正确，重复仅在数据加载与串行执行。`technical_factors.py` 不在每日评分主路径（趋势分量读预计算 `StockFactorDaily` 均线）。索引完备（scoring.py:109-120 等）。*

### 3.6 compute-worker / 编排（W 系列）

| ID | 严重度 | 位置 | 问题 | 修复 |
|---|---|---|---|---|
| W1 | P1 | compute-worker/worker.py:96-112、config.py | 主循环一次只跑一个任务，`MAX_CONCURRENT_TASKS` 配置未被使用 → 回测/网格/验证任务在 2–6 CPU pod 上实际串行 | 循环改为按 `MAX_CONCURRENT_TASKS` 并发（IO 型任务线程池即可；CPU 型任务进程池），保持 `_fetch_next_task` 原子 claim 语义 |
| W2 | P2 | k8s/base/datahub.yaml:95-101（500m CPU）+ compute-worker.yaml:47-53（2–6 CPU） | 评分/信号等重 CPU 作业跑在 500m datahub pod；compute-worker 空有配额 | 评分挪到 compute-worker 或独立 scoring CronJob pod（配合 C1/C2）；或临时调高 datahub scoring 作业的 limit（独立 Job 而非 kubectl exec 进常驻 pod） |

---

## 4. 量级估算与实测方法（阶段 0）

所有数字基于代码结构推算（假设见文件头），落地前应实测钉死：

1. **基线**：`datahub_job_runs` 已记录每 job 的 `elapsed_seconds` 与 phase_stats；先采集一个完整
   交易日（18:00–19:30）各 job 的耗时作为 before 基线。
2. **CPU 火焰图**：pod 内 `py-spy dump --pid <pid>` / `py-spy record -o perf.svg -- <cmd>`，
   对 score-all、data sync、FQ 回填各采一次，验证 C1/Q2/F1 的热点占比。
3. **Mongo 侧**：`db.setProfilingLevelForMongo(1, {slowms: 100})` 观察慢查询分布（预期：
   scoring 的 `_get_previous_quotes`/`assign_ranks` 全量排序、signal 全历史读、sync 全表扫）。
4. **每项修复后**：同法复测 + 相关 pytest（scoring/factor/signal/sync 测试均在
   `datahub/app/test/`），确认结果语义不变。

---

## 5. 已核实为健康、无需动的部分

- sync_engine 的流式读 + 500/批 `bulk_write(ordered=False)`（除 S1 水位线外）。
- quote UPD 快照路径（#132，正常日分钟级）；FQ 正常日 snapshot 路径（fq_factor.py:430-486）。
- MA anchor 增量语义与 119 行 lookback；rolling 已用 pandas。
- 三个服务的 `get_codes_requiring_update` 批量状态比对；`bulk_upsert_asset_status` 分块与幂等。
- `StockDailyQuote` `{code,-date}` 唯一索引、factor/signal/prediction 索引完备。
- retry（retry.py 指数退避）与连续失败熔断（china_a_stock.py:39）。
- 回测读已落库分数、模拟全内存；`_upgrade_recommendations` 的 bulk_write 范式。

---

## 6. 路线图（与 `openspec/changes/datahub-perf-optimization/tasks.md` 对应）

- **阶段 0（度量）**：§4 的基线采集。
- **阶段 1（快赢，小改动）**：S1 水位线；C5/C6/G1 写路径 bulk 化；R4 四处日历 bisect；
  C2 消除评分双跑；F4 CLI 对齐；Q4 死查询清理；G3 常量外提。
- **阶段 2（结构性，每项独立可验证）**：C1 评分按天批量取数；G1 信号增量 anchor；
  F1 FQ 回填按交易日快照+向量化（解锁交接文档待办 5.2 全市场 FQ 重算）；
  Q2+Q3 行情向量化+线程池；Q1 完整性检查聚合粗筛；C4 factor_eval 预载数据。
- **阶段 3（研究链路与基建）**：C3 网格分量矩阵；C7 报告内存；W1/W2 并发与资源；
  抽公共骨架（`wide_frame_bulk_writer` / `select_stale_codes`）统一 factor/signal 同构实现。

涉及评分语义、freshness/generated timestamps、数据所有权的实现改动（阶段 1 的 C2/G3、
阶段 2 全部）按 `RULES.md#P3` 需过 Spec Gate——本 change 即其载体。
