# FQ/HFQ 链路审计（除复权）

> 只读审计。本文记录 2026-10-23 在 **research** 环境对 `stock_daily_quote` 除复权
> 链路（`adj_factor → fq_factor → *_hfq`）的实测结论、残留风险与复跑方法。
> 项目定位不变：研究/学习/演示 MVP，不构成投资建议。

## 1. 结论先行

| 维度 | 结论 | 证据 |
|:---|:---|:---|
| 活跃股票覆盖 | ✅ 可靠 | 支持域活跃股票 5,225，当日有行情 5,217（99.85%）；有行情的活跃股票缺 `fq_factor` = **0** |
| 算术一致性 | ✅ 可靠 | 4 只样本共 19,089 行、76,356 个 OHLC 字段，`*_hfq == raw × fq_factor` 偏差 >0.02 的 = **0**；非正/非有限因子 = **0** |
| 与源头一致性 | ✅ 在容差内 | 4 只样本（sh600000/sh603429/sz000812/sz300118）逐日比对 tushare `adj_factor`，最大绝对差 **0.0004**，容忍度 **0.0005**；无源缺口、无“行情早于首个因子” |
| 全市场因子跳变 | ✅ 正常 | 近 90 天 16 个跳变日，每个交易日只有 1–2 只股票跳变（除权/除息），最大单日占比 0.036%；不存在 2026-08-31 式全市场跳变 |
| FQ 新鲜度状态 | ✅ 正常 | `data_asset_status` 中 `FQ_FACTOR` 全部 OK（5,227），无 STALE/NO_DATA |
| 残留 | ⚠️ 1 行 | 1 只已退市（`sz002731` 萃华退）在退市整理期的最新行情行没有 `fq_factor`（不在 FQ 计划内） |

**判定：WARN（非 FAIL）**——活跃支持域内链路可靠、数据准确（在供应商重发布的
0.0005 舍入容差内）；WARN 仅来自 1 行退市整理期行情与供应商重发布漂移，不影响
评分/研究的主域。

## 2. 审计方法

```bash
# 在 research 的 datahub pod 内（只读；不写任何集合）
python scripts/fq_chain_audit.py --jump-scan --sample-size 4
# 指定样本 / 降低或提高容差
python scripts/fq_chain_audit.py --sample-code sh600519 --sample-code sz000001 --tolerance 5e-4
# 离线（跳过 tushare 源比对）
python scripts/fq_chain_audit.py --skip-source --jump-scan
```

- 口径：活跃支持域 = `basic_stock.object_type=individual_stock` 且
  `active_status=0` 且 `data_capabilities.daily_quote != false`（北交所按设计排除，
  否则覆盖率永远只有 ~93.5%）。
- 源比对：按样本股票的已存日期区间拉取 tushare `pro.adj_factor`，逐日比较；
  容差 5e-4 与 writer-switch 验收（`fq-adj-factor-fix` 6.5）一致。
- 跳变扫描：复用 `fq_factor_integrity.scan_fq_factor_jumps`，以已落库交易日历做
  相邻日判定。
- 退出码：PASS=0，WARN=1，FAIL=2（FAIL 用于缺因子或超出容差的样本）。

实现：`datahub/scripts/fq_chain_audit.py` +
`datahub/app/lib/datahub/data_integrity_keeper/handler/fq_chain_audit.py`。

## 3. 实测明细（2026-10-23，镜像 sha-f3f29c8）

覆盖率：

```json
{"active_stock_count": 5225, "quoted_active_count": 5217,
 "quoted_active_coverage": 0.9985, "missing_fq_count": 0,
 "latest_quote_date": "2026-10-23",
 "inactive_quoted_count": 1, "inactive_missing_fq_count": 1}
```

样本源比对：

| 代码 | 行数 | 逐日比较 | 超容差 | 最大绝对差 | 源缺口 | 行情早于首因子 |
|:---|---:|---:|---:|---:|---:|:---|
| sh600000 | 6,530 | 6,530 | 0 | 0.0003 | 0 | 否 |
| sh603429 | 2,314 | 2,314 | 0 | 0.0004 | 0 | 否 |
| sz000812 | 6,482 | 6,482 | 0 | 0.0004 | 0 | 否 |
| sz300118 | 3,763 | 3,763 | 0 | 0.0002 | 0 | 否 |

算术检查：4 只样本 19,089 行 × 4 个价格字段 = 76,356 个字段，全部满足
`*_hfq == raw × fq_factor`。

跳变扫描：`jump_dates=16`，最大相对跳变 0.501（`sz002961`，2026-09-23，送转/除权），
当日跳变股票占比 0.018%–0.036%，与“真实除权日只动几十只股票”的基线一致。

## 4. 已知残留风险（不阻塞研究，但不要假装不存在）

1. **供应商重发布漂移（已量化）**：tushare 会在不同快照间以略不同的小数位重发布
   同一因子（实测 2.4765 vs 2.476，差 0.0005）。写入时“抄当天因子、历史不回头修”
   意味着历史行可能停留在旧快照值。量级 ≤ 5e-4（相对 ~2e-4），对 20 日收益
   （~5%）不可见；但**不能**声称“历史存储值逐位等于当前源头”。需要时用
   `fq_factor` 全历史回填（force/backfill）刷新，而不是日常快照路径。
2. **退市整理期行情**：`_get_market_update_plan` 只选 `active_status=0`，但全市场
   `daily` 快照仍会给退市整理期股票写入行情，因此其最新行没有 FQ（实测 1 行）。
   研究层的退市组合（`research_merge_delisted_panel.py`）自行用 `adj_factor` 计算
   HFQ，不依赖该字段；库存行情里的这 1 行是“可见但不完整”，不是链路错误。
3. **前导对齐**：`build_fq_factor_frame` 对“行情早于首个因子行”的前导区间使用
   最早因子（历史语义，测试 `test_factor_before_first_factor_row_uses_earliest_factor`
   固化）。本次 4 只样本均无该情况；审计逐样本报告 `leading_before_source`，出现时
   WARN 而不是静默通过。
4. **Paper NAV 用原始价**：`strategy_engine/nav.py` 与基准收益都用原始 open/close，
   **不把现金分红计入账户**，除息日会出现假跌。研究回放/评分用 HFQ；paper 账本
   没有。这是 paper 结果能否当“实盘有效性”证据的关键缺口，已在
   `strategy-paper-readiness` 的 limitations 中显式列出，修复属于执行模型（PR #307
   重构范围），不在本次审计内改。
5. **ETF 此前完全缺失**：research 库没有任何 ETF 行情。本次新增
   `datahub-tushare-etf` 能力（`fund_daily` + `fund_adj`），ETF 除复权从此与股票
   同口径；上线前该维度仍是“无数据”。

## 5. 复跑与验收

- 建议节奏：每次改 FQ/复权代码或升级数据源后跑一次；每次全量 FQ 回填后跑一次。
- 正式验收仍以 `writer_switch_verify` 的 R1 源校验与
  `check_fq_factor_integrity.py --jump-scan` 为准；本审计是第三条独立证据，不替代
  它们。
- 若审计 FAIL：先看 `sample[].source.mismatch_examples` 与
  `fq_asset_status`，再决定是单只 `backfill` 还是全市场重算；不要用改容差的方式
  让 FAIL 变 PASS。
