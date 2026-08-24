---
name: datahub-data-quality
description: datahub 数据质量与新鲜度语义——freshness 状态机、BSE 排除、HFQ 缺口、确定性 as-of-date 与幂等引导
license: MIT
compatibility: opencode, dsh
metadata:
  audience: contributors
  project: caifubao
---

## 1. 定位

datahub 拥有行情/因子/信号/评分/freshness/数据质量的生产逻辑：`datahub/app/jobs/*_runner.py`（入口 CLI）、`datahub/app/lib/utilities/`（状态计算）、`datahub/app/lib/factor_factory|signal_factory/`、`datahub/app/lib/datahub/processors/china_a_stock.py`（quote 落库与校验）。

外部契约是 API 响应（`backend/app/api/v1/` 的 data_quality、datahub_status、integrations/openclaw/*），Mongo 集合形状不是。`DataAssetStatus` 的 docstring 明确它是 `data_freshness_meta` 的目标替代品，迁移期内两者共存；新质量/读取路径应优先读 `data_asset_status`。

数据依赖链（docs/operations/agent-cli.md）：quote → FQ factor → MA factor → signal → scoring → verification。

## 2. Freshness 语义

状态常量（`datahub/app/model/data_asset_status.py`）：`OK` / `STALE` / `AHEAD` / `NO_DATA` / `NOT_APPLICABLE`。`data_asset_status` 集合唯一索引 `(code, object_type, asset_type, asset_name)`，字段含 `first/latest_data_date`、`data_count`、`expected_count`、`coverage_rate`、`status_reason`、`last_success_at`（仅 OK 时写）。

行情判定 `classify_quote_status`（`datahub/app/lib/utilities/data_asset_status_helper.py`）：
- `data_count <= 0` 或 `latest_data_date` 为空 → `NO_DATA` / `no_source_data`
- 无 `expected_latest_date` → `OK`
- `latest < expected` → `STALE` / `behind_expected_quote_date`
- `expected < latest` → `AHEAD` / `ahead_of_expected_quote_date`
- 否则 → `OK`

`expected_quote_count` = data_count + 按 `trade_calendar` 统计的 `latest < date <= expected` 缺失交易日数。因子侧 `refresh_fq_factor_status`：无 FQ 行 → NO_DATA；FQ 最新日 < 行情最新日 → `STALE`/`behind_daily_quote`。`refresh_ma_factor_status`：`quote_count < window` → `NOT_APPLICABLE`/`insufficient_quote_history`；无 MA 行 → NO_DATA；MA 最新日 < 行情最新日 → STALE；`expected_count = max(quote_count - window + 1, 0)`。能力被禁用的标的由 `data_asset_status_initializer` 写 `NOT_APPLICABLE`/`capability_disabled`。

`data sync` 不更新 `data_asset_status`，必须随后 `./scripts/caifubao data refresh-status`。

API 侧：OpenClaw 响应经 `wrap_response` 带 `data_as_of`/`generated_at`（`backend/app/api/v1/integrations/openclaw/utils.py`）。已核实缺口：capability-inventory.md 的 P0 记录 data_as_of「从未填充」（10.3），各响应为 None —— 修好前下游不能依赖该字段。

## 3. 范围与排除

BSE 排除（`datahub/app/lib/utilities/data_capability_helper.py`）：`BSE_CODE_PATTERN = ^(?:bj\d{6}|[489]\d{5})$`（不区分大小写）命中即视为北交所，默认能力 payload `daily_quote`/`fq_factor`/`ma_factor` 全 False；`stock_supports` 在 `data_capabilities` 缺失时回退该默认。`backend/app/api/v1/datahub_status.py` 也只返回支持 daily_quote 的活跃股票（排除北交所）。

新股 MA 窗口适用性：MA 窗口 10/20/30/60/120；行情天数不足窗口时状态为 `NOT_APPLICABLE`/`insufficient_quote_history`；`MovingAverageFactorService.get_codes_requiring_update` 只对 `data_count >= window` 的窗口要求最新。capability-inventory 第一层把「数据质量检查」定义为：覆盖率、缺失日统计、BSE 排除、新股 MA 窗口适用性。

## 4. HFQ 与缺口防护

- FQ 因子（`datahub/app/lib/factor_factory/fq_factor.py`）：`_validate_quote_df` 拒绝 `previous_close`/`close` 为 NaN 或 ≤0（复权链断裂防护）；增量锚定最新一条 `fq_factor`/`close_hfq` 作 base，只算其后的区间；结果写回 `stock_daily_quote` 的 `fq_factor`/`close_hfq`/`open_hfq`/`high_hfq`/`low_hfq`。
- MA 因子：必须 `close_hfq`；缺列 → `ValueError("Missing price field for MA calculation")` → SKIP；全 NaN → `ValueError("No valid close_hfq ...")`。
- 回测前防护（`backend/app/services/backtest_service.py`）：截断到最后一个有 HFQ 的交易日（raw-close fallback 会腐蚀收益），过滤中间 HFQ 缺口；模拟前 `_data_coverage_report` 产出 warnings（HFQ 缺口、因子 vs 行情覆盖、评分覆盖）附在结果。
- 零行写入即失败（`processors/china_a_stock.py` `check_data_integrity`）：allow_update 且本地无数据但尝试更新、`validated_quote_count == 0` 时 raise `RuntimeError("...wrote zero quote rows...")`；remote spot 为空同样 raise。
- 停牌豁免：`is_allowed_suspension_gap` —— 源成功且缺目标日行可归因于临时停牌时，允许该股保持 STALE 而不使全市场 run 失败。
- 陈旧检测：`check_data_freshness` 返回 GOOD/UPD/INC/WARN/FULL/SKIP；行情日超前 → WARN「Quote date ahead of time!」。`DataIntegrityKeeper.check_stock_quote_missing` 对照 trade_calendar 找中间缺失日并把 freshness_meta 重置到最后一个有效日。

## 5. 确定性引导（bootstrap）

docs/operations/mongodb-resilience.md 的确定性 quote bootstrap gate：
- 选一个已完成交易日作为逻辑 run 的冻结 `as_of_date`；quote CronJob 保持 suspend；一次性 Job 用 `backoffLimit: 0` + `restartPolicy: Never`。
- 每次续跑/重放都用同一 `--as-of-date`；quote 持久化按 `(code, date)` 幂等。
- 不 resume 旧镜像创建的 Job；源失败与 NO_DATA 视为 fatal（仅临时停牌导致的缺行可豁免）。
- 运行形态：`python -m app.jobs.quote_runner --target stock --as-of-date YYYY-MM-DD`。

日期校验 `perform_date_check`：显式 as_of_date 必须在 trade_calendar 且 ≤ 最新完整交易日，否则 `ValueError("Explicit as_of_date must be a completed market trading day")`；无完整交易日 → RuntimeError。时区为 Asia/Shanghai（`job_run_helper.BEIJING_TZ_NAME`、`trading_day_helper.BEIJING_TIMEZONE`、quote_runner `--scheduled-timezone` 默认值）。

空库 bootstrap 顺序：secrets → 部署 → 股票主数据 → 历史行情 → FQ/MA/tech 因子 → 信号 → 评分 → 刷新 data_asset_status 与 freshness → `system bootstrap-check` → 健康/质量/OpenClaw 检查。信号 run 有依赖门：`signal_runner` 要求 `quote_daily` 家族当日 SUCCESS（查 `datahub_job_runs`），否则 SKIPPED/`dependency_failed`；startup quote catch-up（`quote_catchup.py`）用 data_asset_status 滞后判定 + 180 分钟活跃 job 防重入。

## 6. 命令地图

统一 CLI（scripts/caifubao）：
- `data sync [FROM_DATE] [COLLECTIONS]` / `data refresh-status [LIMIT]` / `data status <STOCK>`
- `system cron [status|trigger|suspend|resume] <name>` / `system bootstrap-check` / `system backup|restore ...`

runner CLI（datahub pod 内 `python -m app.jobs.*`）：
- `quote_runner --target index|stock|all [--include-factors] [--as-of-date YYYY-MM-DD] [--job-* --scheduled-*]`（核实：parse_args 无 `--dry-run`；capability-inventory 声称含 dry-run，未在代码中核实到）
- `factor_runner --factor fq|ma|all --mode stale|force [--code --limit --dry-run --market ChinaAStock]`
- `signal_runner --signal ma-cross|all --mode stale|force [--code --limit --dry-run]`
- `data_asset_status_initializer [--code --limit --dry-run --batch-size 100]`
- `parquet_export_runner export --dataset all|daily_quotes|factors|signals [--from-date --to-date --lookback-days --dry-run]`
- `data_sync_runner`/`sync_data`：prod→dev 同步，用 `MONGODB_SRC_*`，集合 stock_daily_quote/stock_factor_daily/stock_signal_daily/finance_market/stock_industry
- `backtest_runner single|multi|compare|optimize|compare-all|scan|walk-forward`；`tech_factor_runner compute|evaluate|list`

## 7. 测试模式

- `datahub/app/test/test_data_asset_status_helper.py`：`classify_quote_status` 四态断言（NO_DATA/STALE/OK/AHEAD）；`expected_quote_count` 计入缺失交易日。
- `test_data_asset_status_initializer.py`：集合名/字段；STALE `behind_daily_quote`；`NOT_APPLICABLE insufficient_quote_history`；能力禁用（bj920118）→ `capability_disabled`；dry-run 不写。
- `test_china_a_stock.py`：`test_stock_bootstrap_fails_when_every_quote_write_is_zero`（"wrote zero quote rows"）；"spot list is empty"；停牌零收盘不标 inactive；新停牌股陈旧历史不使 bootstrap 失败。
- `test_factor_runner.py`：默认 ma+stale；dry-run 只选 stale codes 不写；skip/fail 计数。
- `test_ma_factor.py`：MA 用 close_hfq；缺列/全空 close_hfq 必须报错。
- `test_data_capability_helper.py`：`test_bse_codes_default_to_unsupported_capabilities`。
- `test_data_integrity_keeper.py`：freshness_meta 读/upsert 与 OK/UPD 判定。
- `backend/app/test/test_data_quality_api.py`：BSE 样本 bj430047 排除、`generated_at` 时区断言。

## 8. 自检清单

- [ ] data sync 之后是否跑了 `data refresh-status`？否则 data_asset_status 不更新
- [ ] 引导/重放是否全程同一 `--as-of-date`，且该日确为已完成的交易日？
- [ ] 全市场 quote 刷新是否可能「尝试更新但零写入」？如是必须失败而不是静默通过
- [ ] 新增因子/信号 runner 是否默认 stale 模式并先 dry-run？
- [ ] 是否误把 BSE 代码（bj/4/8 开头）当普通股票处理？
- [ ] 回测/评分输入的最后交易日是否有 close_hfq？HFQ 链是否完整？
- [ ] 新质量路径是否优先读 `data_asset_status` 而非 `data_freshness_meta`？
- [ ] OpenClaw 响应 data_as_of 为 None 是已知缺口，是否被误判为「没有数据」？
- [ ] 信号 run 前是否确认 quote_daily 当日 SUCCESS（datahub_job_runs）？
