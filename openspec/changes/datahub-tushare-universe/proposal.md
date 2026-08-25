## Why

2026-08-25 集群恢复期间，股票列表/行情快照端点（东财 `push2` spot，回退
新浪 spot）持续封锁/抖动（RemoteDisconnected、HTTP 456、偶发截断为 80 行），
导致每日增量行情更新无法启动——quote 管线把 spot 作为股票列表与停牌检测的
唯一驱动。tushare 历史源（`pro.daily`）已验证可用且写入 1,660 万行无问题，
但 universe 仍依赖东财/新浪。本变更让**股票列表/universe 也可切换为
tushare**，整条 quote 管线完全绕开东财/新浪。

## What Changes

- 新增 env `DATAHUB_STOCK_UNIVERSE_SOURCE` ∈ `spot | tushare`（默认 `spot`，不改变现有行为）
- `zh_a_daily.get_zh_a_stock_universe(as_of_date)`：`spot` 走现有
  `get_zh_a_stock_spot()`；`tushare` 用 `pro.stock_basic(list_status='L')`
  取活跃股票列表 + `pro.daily(trade_date=as_of)` 取当日行情/停牌，产出
  `[code, name, close]` DataFrame（close=0 表示停牌，语义与 spot 一致）
- `china_a_stock.check_stock_data_integrity` 改调 universe getter（传入
  frozen `as_of_date`）
- 仅影响股票 quote 路径；指数 spot（新浪）保持不变

## Non-goals

- 不改变历史数据源语义（tushare 历史分支已存在）
- 不写 spot 快照作为行情行（沿用"历史写入"设计）
- 不改指数 spot
