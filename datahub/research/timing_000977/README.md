# 000977 浪潮信息 — 个股择时研究

研究工具与产出，答案见 [`REPORT.md`](./REPORT.md)。

> 仅用于研究与教学，不构成投资建议。脚本只读本地 parquet，不写 Mongo、不下单。

## 结论速览（修正后，含成本，2005-01-04 ～ 2026-10-21）

| | 年化 | 最大回撤 | Calmar | Sharpe |
|---|---:|---:|---:|---:|
| 择时（固定参数，价格契约面板） | 17.3% | -35.6% | 0.49 | 0.87 |
| 滚动前进样本外 2011-2026（top3 集成） | 14.1% | -32.5% | 0.44 | 0.73 |
| 买入持有 | 19.4% | -69.8% | 0.28 | 0.58 |

进阶研究见 [`RISK_AND_ALPHA.md`](./RISK_AND_ALPHA.md)：熊市降仓/波动率控制/资金曲线控制的对比，以及超跌、趋势、市场状态信号的可重复增量预测力检验。

跨股票验证见 [`CROSS_SECTION.md`](./CROSS_SECTION.md)：94 只 A 股、同仓位配对对照，检验这些规则能否系统性改善表现。

行业层验证见 [`SECTOR.md`](./SECTOR.md)：中证 10 个一级行业的轮动检验，以及行业状态叠加到个股后的增量。

**结果快照与交叉检查**：[`RESULTS_MANIFEST.md`](./RESULTS_MANIFEST.md) 记录环境、数据版本、全部核心指标与 208 个文件的 SHA256；运行 `python verify_results.py` 可一键校验（哈希 + 重算最终策略/基准/跨股票汇总）。

**价格契约**：`datahub/app/lib/prices/` + `datahub/scripts/build_price_panel.py`；000977 使用 `data/corporate_actions_000977.csv` 的 20 条事件，构建 0 违规。研究只读 `data/000977_panel.parquet`。

**审计回应**：见 [`AUDIT_RESPONSE.md`](./AUDIT_RESPONSE.md) 与 [`RETRACTIONS.md`](./RETRACTIONS.md)。公司行动、预测回归 purge、Reality Check、行业双重滞后均已修正并重算。

**重要**：上一版的 21.1%/-35.8% 基于东财后复权序列，该序列有 51 天越过 ±10% 涨跌停（最差 -26.84%），已作废。现在使用“原始价 × 送转倍数”重建序列（0 次越界）。详见 `REPORT.md` §0。

## 文件

| 文件 | 作用 |
|---|---|
| `fetch_data.py` | 拉取 000977 原始价、东财前/后复权（审计对照）与沪深300等指数，缓存 parquet + manifest |
| `build_adjusted.py` | 原始价 × 5 次送转倍数 → `000977_adj`（主序列），带涨跌停一致性校验 |
| `engine.py` | 无前视回测引擎：t 日收盘出信号、t+1 开盘成交、佣金/印花税/滑点/整手/无交易带/可选回撤刹车 |
| `signals.py` | 因果特征与策略族：均线、双均线、动量、唐奇安、趋势+移动止损、波动率目标、RSI、超跌反转、波动区间、市场闸门 |
| `scan.py` | 基线扫描：74 个策略族 × 全样本/样本内/样本外 |
| `strategy.py` | 最终策略 `FINAL`、候选网格与暴露缓存 |
| `walkforward.py` | 通用滚动前进（470 候选网格上的选参噪声实验） |
| `walkforward_focused.py` | 最终结构族内的滚动前进，并保存逐块仓位路径 |
| `report.py` | 分区间统计、自助法、图 1-4 |
| `audit.py` | 账目恒等式、逐笔成交/成本、仓位跟踪、因果性、价格序列检验、2024-2026 专项、图 5 |
| `benchmark.py` | 统一 OOS 对照（固定仓位/波动率目标/随机安慰剂）、图 6 |
| `ablation.py` | 规则消融（超跌/熊市降仓/低波动闸门/波动率上限） |
| `risk_overlay.py` | 风险管理实验：熊市降仓/波动率控制/CPPI/净值均线/回撤刹车，含 CVaR、Ulcer、水下天数 |
| `alpha_study.py` | 预测力实验：IC 分段稳定性、事件研究、扩张窗口 OOS 回归、Reality Check |
| `RISK_AND_ALPHA.md` | 风险管理与择时 Alpha 的结论文档 |
| `universe_fetch.py` | 随机抽取 A 股样本，腾讯后复权逐只抓取并做涨跌停一致性过滤 |
| `cross_section.py` | 94 只股票的配对检验：每条规则 vs 同平均仓位固定仓位，IS/OOS 分段 |
| `CROSS_SECTION.md` | 跨股票验证的结论文档 |
| `sector_fetch.py` | 抓取中证 10 个一级行业指数与个股行业归属 |
| `sector_study.py` | 行业轮动（动量/低波/反转）与 000977 行业叠加的增量检验 |
| `SECTOR.md` | 行业层验证的结论文档 |
| `AUDIT_RESPONSE.md` | 对外部审计的逐条回应与修正后数字 |
| `RETRACTIONS.md` | 已撤回/更正的结论清单 |
| `make_manifest.py` | 生成结果快照、`SHA256SUMS` 与 `SHA256SUMS.data` |
| `verify_results.py` | 交叉检查：校验哈希 + 重算策略/基准/汇总，15 项 PASS/FAIL |
| `RESULTS_MANIFEST.md` | 结果快照（环境、数据、指标、文件清单） |
| `show_signal.py` | 打印最新交易日的四个闸门与目标仓位（`--refresh`、`--json`） |
| `out/` | `audit_report.json`、`benchmark_oos.csv`、`ablation.csv`、`headline.json`、walk-forward 净值等 |
| `figures/` | fig1 净值/回撤、fig2 仓位、fig3 年度、fig4 参数热力图、fig5 审计、fig6 基准、fig7 风险前沿、fig8 预测力证据、fig9 跨股票配对分布、fig10 行业轮动净值 |
| `data/` | 行情快照 + `manifest.json`（含 `000977_adj` 的 5 个送转事件） |

## 复现

```bash
cd datahub/research/timing_000977
../../.venv/bin/python fetch_data.py          # 已缓存则不重复抓取
../../.venv/bin/python build_adjusted.py      # 重建主序列
../../.venv/bin/python scan.py
../../.venv/bin/python walkforward_focused.py
MPLCONFIGDIR=/tmp/mpl ../../.venv/bin/python report.py
MPLCONFIGDIR=/tmp/mpl ../../.venv/bin/python audit.py
MPLCONFIGDIR=/tmp/mpl ../../.venv/bin/python benchmark.py
../../.venv/bin/python ablation.py
../../.venv/bin/python show_signal.py
```

依赖：`pandas / numpy / matplotlib / requests / pyarrow`（均在 `datahub/.venv`）。

## 策略一句话

低波动时持有 50%，超跌（120 日 -25%）加到 100%，沪深300 深度回撤时减半；偏离 5% 才调仓，次日开盘执行。
