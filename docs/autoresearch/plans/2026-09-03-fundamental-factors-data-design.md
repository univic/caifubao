# 基本面因子数据接入设计（估值 + 质量）

> 目的：为 H20 因子重构（B 路径）提供**点位（point-in-time）**的估值与质量数据。
> 现状：仓库无任何基本面数据（行情归一化时 peTTM/pbMRQ 被显式置 0；
> FinancialStatus/ValuationIndicator 仅模型定义、从未同步/消费）。tushare 集成
> （token + pacing）已存在，`pro.daily`/`adj_factor` 在用，可平滑扩展。

## 一、估值因子 —— tushare `daily_basic`（优先，干净）

**数据**：`pro.daily_basic(trade_date=YYYYMMDD)` 每交易日全市场返回：
`pe / pe_ttm / pb / ps / ps_ttm / dv_ratio / dv_ttm / total_share / float_share /
total_mv / circ_mv / turnover_rate / turnover_rate_f`。

**点位语义**：`daily_basic` 的值是「该交易日收盘后按最新财报 + 当日价」计算的——
**用 trade_date 即无前视**（pe_ttm 的财报滞后由 tushare 处理）。

**落库**：新集合 `stock_daily_basic`（code, date, pe_ttm, pb, ps_ttm, dv_ttm,
total_mv, circ_mv, turnover_rate）。独立于行情，避免污染/依赖 quote 归一化。

**同步**：镜像现有 tushare 每日同步模式——
- `tushare_interface.py` 加 `daily_basic_by_trade_date(trade_date)`（带 pacing/重试，
  同 `daily_by_trade_date`）；
- 新 handler `zh_a_daily_basic.py`：逐交易日拉取 → 归一化 → 批量 upsert；
- 注册进 data-sync（prod→dev）+ `data_asset_status` 新鲜度。

**回填**：2019-01-01~2026-07-31 ≈ 1850 个交易日 × ~1 次调用/日（每调用 ~6000 行，
够全市场）。按仓库现有限速（<300/min）≈ **~15-25 分钟 API 时间** + 写入。

## 二、质量数据 —— tushare `fina_indicator`（季度，前视处理复杂）

**数据**：`pro.fina_indicator(ts_code=..., start_date=..., end_date=...)` 季度指标：
`roe / roa / netprofit_margin / grossprofit_margin / debt_to_assets / yoynetprofit /
or_yoy / update_flag` 等。关键字段：**end_date（报告期）+ ann_date（公告日）**。

**点位语义（必须）**：报告只在 `ann_date ≤ 评分日` 后才可知。**绝不能用 end_date
选报告**（会把 Q2 财报提前用到 Q2 结束日）。快照取每股「ann_date ≤ date 的最新一份」。

**落库**：`stock_financial_indicator`（code, end_date, ann_date, roe,
grossprofit_margin, netprofit_margin, debt_to_assets, yoynetprofit）。保留 ann_date
供点位查询。

**同步**：按报告期（每季度 ~1 次批量拉全市场该期指标，~30 期 × 分页）回填 +
后续按公告增量更新。比 daily_basic 重：分页 + 点位校验。

## 三、因子构造（研究侧，先进快照再谈生产）

| 因子 | 来源 | 构造（研究用，避免前视/极端值） |
|---|---|---|
| 估值 | stock_daily_basic | 1/pe_ttm、1/pb（**排除 pe≤0 与极端值**，A 股特色：负 PE/微利失真）；dv_ttm |
| 市值 | stock_daily_basic | log(total_mv)（**必须做 size 中性**——等权基准偏小盘，价值因子常是 size 代理） |
| 流动性 | stock_daily_basic | turnover_rate |
| 质量 | stock_financial_indicator | roe、净利率、毛利率、负债率、净利同比（ann_date ≤ 日） |

**验证顺序**：daily_basic 到位 → 快照导出器加估值/市值列 → 2019-2023 轻量导出 →
per-regime IC + size 中性检验 → 有正 IC 再上 fina_indicator 质量因子。

## 四、工程落地顺序（建议）

1. `stock_daily_basic` 集合 + tushare `daily_basic_by_trade_date` + handler + 回填
   （dev 先行，只读研究；prod 同步后续）。
2. 快照导出器读 `stock_daily_basic`，输出估值/市值因子列。
3. per-regime 审计新因子。
4. 确认后：`fina_indicator`（质量）+ 快照再加质量列 → 再审计。
5. 生产路径（研究确认后才走）：新组件 + 权重重配 + Spec Gate + 校准 + version bump。

## 五、风险与预期管理

- A 股价值因子历史上**偏弱且被污染**（负 PE、微利、小盘）；「低 PE 组合」常是小盘/
  反转代理。**size 中性检验是必须的，别把 size 当 alpha**。
- `daily_basic` 的 pe_ttm 依赖财报更新节奏，单点偶有缺失/异常——研究侧要 winsorize。
- quote 派生因子（低波、距 52 周高）与已发现的均值回归高度重叠，**真正增量在
  基本面估值/质量**，但需要上述数据工程；若只想快速试 quote 派生因子，可先不加数据。
- 当前 2019-2023 导出是旧 schema（无新因子列）——若 B 走通需带新列重导。
