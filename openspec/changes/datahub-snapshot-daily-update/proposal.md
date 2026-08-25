## Why

每日增量更新当前对每只"差 1 天"（UPD）的股票逐只拉历史（tushare 下约
5,200 次调用、30-40 分钟、贴近 300 次/分限流）。但全市场当日快照
（东财 spot 或 tushare `daily(trade_date=...)`）一次调用即含全部股票的
官方结算行情——UPD 场景用快照写当日行即可，只有差 >1 天（INC/FULL）才
需要历史。这正是系统原设计意图（"正常每日更新从快照更新"），本次把
UPD 分支从历史拉取改为快照写入。

## What Changes

- `check_data_integrity` 的 UPD 分支：**股票 + 非停牌 + universe 源为
  tushare** → 用 as-of 市场快照行写当日 quote（`write_snapshot_quote`），
  不再逐只拉历史；spot universe（实时快照）保持历史回退
- INC/FULL、以及 UPD 但停牌（close=0）→ 维持历史拉取（现有语义不变）
- tushare universe 构建器扩展输出完整日线字段（open/high/low/close/
  volume/trade_amount/previous_close/change_amount/change_rate），支撑快照写入
- **取代说明**：本变更取代兄弟 change（datahub-tushare-universe）中
  "不写 spot 快照作为行情行" 的非目标——UPD 场景改为写 tushare
  `daily(trade_date)` 的官方结算快照（与历史接口同源，非实时 spot）

## Non-goals

- 不改 INC/FULL 的历史拉取
- 不改指数路径（指数少，历史拉取代价低）
- 不写停牌股（close=0）的行情行（保持防污染）
