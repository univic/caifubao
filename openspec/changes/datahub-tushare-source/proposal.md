## Why

2026-08-24 集群恢复期间，默认历史行情源（东财 `push2his` kline）持续被拒
（RemoteDisconnected，数小时未恢复），全市场行情 bootstrap 被阻塞。已实测
备选源：腾讯 `stock_zh_a_hist_tx` 可用但全市场串行约 40 小时；而 Tushare
`pro.daily`（已有 token、积分 ≥2000）按股票逐只、按 18 年窗口分页拉全量历史
（约 2-3 次调用/只，全市场约 1.1-1.7 万次调用），限流 500 次/分钟下约
30-60 分钟可完成全市场，是最快的可用替代源。

## What Changes

- `DATAHUB_STOCK_HISTORY_SOURCE` 支持新增取值 `tushare`（默认仍为 `akshare`，不改变现有行为）
- 新增 `datahub/app/lib/datahub/data_source/interface/tushare_interface.py`：
  从 env `TUSHARE_TOKEN` 读取凭据（私有 secret，与 `MONGODB_SRC_PASSWORD` 同模式），
  提供按股票日线历史拉取与 ts_code 映射
- `zh_a_daily.py`：`SUPPORTED_STOCK_HISTORY_SOURCES` 增加 `tushare`，
  新增 tushare → 内部 quote schema 的归一化（列名/单位/派生字段）
- 仅替换**历史数据**来源；股票列表/spot/停牌检测仍走东财 spot（未被阻塞）
- 配套测试、文档、私有仓库 secret 接线

## Non-goals

- 不切换股票列表/universe 来源（spot 仍用东财）
- 不改变默认数据源（`akshare` 仍为默认，仅在运维显式设置 `tushare` 时启用）
- 不引入 Tushare 的因子/财务/实时接口，仅日线历史
