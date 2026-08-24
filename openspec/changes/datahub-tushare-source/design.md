## Design

### Source selection

`DATAHUB_STOCK_HISTORY_SOURCE`（env）取值扩展为 `akshare | baostock | tushare`。
`get_stock_history_source()` 校验集合同步更新；缺省 `akshare` 行为不变。

### Interface

`data_source/interface/tushare_interface.py`（镜像 `akshare_interface.py` 风格）：

- `to_tushare_ts_code(code)`：`sh600519 → 600519.SH`、`sz000977 → 000977.SZ`、
  `bj920000 → 920000.BJ`
- `tushare_daily(ts_code, start_date, end_date)`：token 取 `env TUSHARE_TOKEN`
  （缺失即 `RuntimeError` 明确报错）；`pro.daily()` 返回列
  `ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg,
  vol(手), amount(千元)`；**按 18 年日历窗口分页拉取并拼接**（规避单次
  6000 行上限对老股票历史的静默截断），窗口间 0.1s 间隔
- 未知代码前缀（非 sh/sz/bj）抛 `ValueError`，不静默按 SH 处理

### Normalization（`zh_a_daily._normalize_tushare_stock_history`）

| tushare 列 | 目标字段 | 处理 |
|---|---|---|
| `trade_date` (YYYYMMDD) | `date` | `to_datetime(format="%Y%m%d")` |
| `open/high/low/close` | 同名 | 直接 |
| `vol` (手) | `volume` (int) | 与东财单位一致，直接 |
| `amount` (千元) | `trade_amount` | **×1000 → 元**（与东财成交额一致） |
| `pre_close` | `previous_close` | 直接 |
| `change` | `change_amount` | 直接 |
| `pct_chg` (%) | `change_rate` | 直接 |
| — | `turnover_rate` | 0（tushare daily 不返回换手率） |
| — | `trade_status` / `peTTM` / `pbMRQ` / `psTTM` / `pcfNcfTTM` / `isST` | 固定值（同 akshare 归一化） |

输出按 `date` 升序；缺失数值列补 0；`code` 写入系统内部代码。

### Handler 分发

`get_zh_a_stock_hist_daily_quote()` 在 akshare 分支后增加 tushare 分支：
`_call_with_retry(lambda: tushare_daily(...), label=f"tushare_daily:{code}")`，
复用现有重试/超时语义；随后按冻结 `end_date` 过滤（`<= as_of_date`）。

### Secret 接线（私有仓库）

`TUSHARE_TOKEN` 走与 `MONGODB_SRC_PASSWORD` 相同的链路：
GitHub Actions secret → `env/root/.env`（write-actions-env.sh）→
`prepare-worktree.sh` 生成 `.env.datahub-secret` → kustomize secretGenerator →
datahub pod env `TUSHARE_TOKEN`；**并显式注入所有执行行情历史的 workload**：
datahub Deployment（startup catch-up）、quote-stock CronJob、一次性 bootstrap
Job（私有部署 patch + 公开 example 清单占位）。

### 验证

- 单测：归一化 schema/单位/排序/end-date 过滤；源选择；ts_code 映射；
  空响应 → None（不崩溃）；分页窗口；限流消息重试
- 手工冒烟：`DATAHUB_STOCK_HISTORY_SOURCE=tushare` 拉单只股票全量历史
- 全市场 bootstrap：一次性 Job 配 `--as-of-date`，监控 symbol 进度与 JobRun
