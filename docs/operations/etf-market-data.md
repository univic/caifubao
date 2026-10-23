# Tushare ETF 行情能力（research/data 数据资产）

> 本文描述已实现的 **场内基金（ETF/LOF）行情 + 除复权** 能力：如何运行、数据落在
> 哪里、口径与已知约束。它是数据能力，不是策略、不是交易，也不构成投资建议。

契约账本：[`openspec/changes/datahub-tushare-etf/`](../../openspec/changes/datahub-tushare-etf/proposal.md)
（能力名 `datahub-etf-market-data`）。

## 1. 边界

| 项目 | 说明 |
|:---|:---|
| 数据来源 | tushare `pro.fund_basic(market='E')`、`pro.fund_daily`、`pro.fund_adj` |
| 主数据 | 独立集合 `exchange_traded_fund`（`object_type=etf`） |
| 行情 | 独立集合 `etf_daily_quote`，主键 `(code, date)`；ETF 代码如 `sh510300` |
| 除复权 | `fq_factor` / `*_hfq` 来自 `fund_adj`，与股票 FQ 同一套算术和写入路径 |
| 新鲜度 | `data_asset_status`，`object_type=etf` |
| 不包含 | 基金净值、折溢价、份额、实时行情、券商/下单；北交所无场内基金 |

ETF 使用**独立集合**（`exchange_traded_fund` / `etf_daily_quote`），不是
`basic_stock` / `stock_daily_quote` 的子类。`BasicStock.objects()` 会返回全部子类，
按日期扫描行情也不会过滤 `object_type`，共享集合会让 ETF 静默混进股票池、研究面板、
评分/信号和公开股票 API；独立集合从结构上排除这种泄漏。两个集合已登记进
online-sync / snapshot（别名 `etf`、`etf_quote`）。

## 2. 运行时口径（实测 2026-10-23 research 账号）

- `fund_basic(market='E')`：3,007 条（含 `.OF` 场外行）。
- `fund_daily(trade_date=…)`：2,177 条；其中 1 条为 `158008.OF`（场外，被拒绝）。
- `fund_adj(trade_date=…)`：2,205 条，覆盖全部当日有行情的 SH/SZ 基金。
- **单次调用上限**：`fund_adj` 2,600 行、`fund_daily` 5,000 行（均为最新优先）。
  18 年窗口会让老基金丢掉最早年份（实测 510300.SH 2005–2026 窗口只返回到
  2016-02-04）。因此 `fund_adj` 按 **10 年窗口**分页，并在“命中上限且首行明显晚于
  请求起点”时直接报错，不落库截断序列。

## 3. 操作

所有命令通过统一 CLI 在 datahub pod 内执行（默认 dev，`CFB_NAMESPACE` 可切 research）：

```bash
export CFB_NAMESPACE=caifubao-research

# 1) 同步基金主数据（首次或新基金上市后）
./scripts/caifubao etf universe

# 2) 回补单只/全部历史（逐只：fund_daily 全历史 + fund_adj）
./scripts/caifubao etf backfill sh510300 sh159915
./scripts/caifubao etf backfill --all          # ~1,000 只活跃基金，耗时较长

# 3) 每个交易日：当日快照 + 当日 fund_adj 除复权（先写行情，再写 FQ）
./scripts/caifubao etf daily 2026-10-23

# 4) 只读查看覆盖
./scripts/caifubao etf status
./scripts/caifubao etf status sh510300
```

底层模块：`python -m app.jobs.etf_sync_runner {universe,daily,backfill,status}`；
`daily` / `backfill` 写 `datahub_job_runs`，任一阶段失败（含 `backfill --all`
出现失败 fund）都以非零退出并记录 `FAILED`，不会把部分成功报成 SUCCESS。

调度：`k8s/overlays/example-research/datahub-cronjobs.yaml` 中的
`caifubao-datahub-etf-daily`（18:40，`suspend: true`）。按 TASK-404 的
one-writer-at-a-time 规则，由私有 overlay 显式启用。

## 4. 口径

- `vol` 为手、`amount` 千元 → 元（与股票 tushare 路径一致）。
- `previous_close = pre_close`；`trade_status=1`（tushare 当日快照缺行 = 未交易，
  **不**补 0 价行）。
- 当日快照按“已入库的全部基金”匹配（含退市整理期 `status=D` 但仍在交易的基金）；
  未知代码（不在 `fund_basic`）仍然 fail loud。
- `fq_factor = fund_adj`；`close_hfq = close × fq_factor`，open/high/low 同比例。
- `daily` 的 FQ 阶段是 all-or-nothing：`fund_adj` 缺任一当日有行情代码的因子 →
  整批不写、状态不前进、运行失败；不回退 `factor=1`。
- `backfill` 走共享 FQ 算术的缺口语义：同一代码区间内缺失的因子行**向前沿用**最近
  已知因子（历史对齐规则），因此“缺一天”不会让回补失败；只有整个区间无有效因子
  才失败。需要逐日严格覆盖时用 `daily` 快照路径。
- `fund_adj` 与 `fund_daily` 共用 200 次/分钟预算，调用间隔 0.35s。

## 5. 失败模式与处理

| 现象 | 原因 | 处理 |
|:---|:---|:---|
| `fund_daily returned no rows` | 非交易日 / 供应商尚未发布 | 确认交易日历；盘后重跑 |
| `fund_adj window truncated` | 分页窗口过宽（命中 2,600 上限） | 代码已固定 10 年窗口；出现即说明供应商上限变化，先停写再调查 |
| `snapshot contains codes missing from the ETF universe` | 新基金未入池 | 先跑 `etf universe` |
| `missing valid factors` | 个别基金当日无因子 | 运行失败但不写 FQ；查该基金是否停牌/退市，勿放宽校验 |

## 6. 当前状态

- 代码、单测、CronJob 模板、CLI 已完成并通过本地校验。
- **尚未部署到 research**：镜像发布后需按顺序执行 `etf universe` → 抽样
  `etf backfill` → `etf daily <最新交易日>`，然后再接 `paper readiness` 的 ETF 检查。
- 本次未回补全市场（约 1,000 只活跃基金）；由运维决定窗口与节奏。
