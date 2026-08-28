## Why

2026-08-27 全链路实验（3 只股票评分+验证）发现评分与 20 日实际收益强负相关
（corr=-0.676，BUY 平均 -42.8%）。深入检查发现根因是**除权除息处理错误**：
`fq_factor.py` 用 `close/previous_close`（每日涨跌比）的累计值当作复权因子，
导致 `fq_factor` 每个交易日都变化（实测 sh600309 一年跳变 124 次），
`close_hfq` 变成"累计收益率 × 基准价"的虚构价格（sh600309 真实股价 ~75 元，
close_hfq 却为 3255）。对比 tushare 官方 `adj_factor` 接口（同期恒定 42.74），
确认该实现从项目初始就是错误的。

由于评分、验证、信号全部通过 `quote_price()`（优先取 `close_hfq`）计算，
**所有下游结果都建立在虚构复权价上，完全失真**。

## What Changes

- `tushare_interface.py`：新增 `adj_factor(ts_code, start_date, end_date)`，
  调用 `pro.adj_factor` 拉取真实后复权因子（ts_code → trade_date → adj_factor）
- `fq_factor.py`：重写 `build_fq_factor_frame` 与 `update_code`：
  - 从 tushare `adj_factor` 获取每个交易日的真实复权因子（数据缺失时回退到
    最近已知因子，即"因子只在除权日变化"的语义）
  - `fq_factor = adj_factor`（真实复权因子）
  - `close_hfq = close × adj_factor`（真实后复权价）
  - open/high/low_hfq 同理按 close 的比例缩放
  - 全历史无除权的股票：adj_factor 恒定，close_hfq 与 close 等比
- 保留幂等 upsert 与 `(code, date)` 主键语义不变
- `adj_factor` 的网络、解码与限流错误按单个窗口请求进行有限重试；重试耗尽、
  空响应或全无效响应时该股票失败且不写 FQ/HFQ 字段，不再静默回退 factor=1
- 日常 stale market refresh 按目标交易日一次获取全市场 `adj_factor` 快照，
  与已落库的当日 quote 一对一合并且只写当日 FQ/HFQ；逐股全历史请求仅保留
  给 force/backfill，避免每日约 5,000 次请求和全历史重写

## Non-goals

- 不改变行情来源/采集逻辑（仍 tushare/akshare 混合）
- 不改变评分/验证的业务逻辑（它们在复权价修复后自动获得正确输入）
- 不引入前复权（qfq）模式；统一使用后复权（hfq）
- 不在本次引入 Tushare 并发、跨股票批量接口或新的缓存语义
- 不使用当前账号无权限的 `stk_factor`，不改变 quote 来源或 Mongo schema
