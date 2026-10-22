# 浪潮信息择时研究 — 结果快照与交叉检查清单
生成时间（UTC）：`2026-10-22T05:02:31+00:00`
节点：`ubuntu-5700x`　｜　目录：`/home/ops/develop/caifubao/datahub/research/timing_000977`
Git HEAD：`e20231a6902b1d47c718496ceb2690dd687a0213`　｜　分支：`develop`
工作区状态：`?? ./`

> 本文件由 `make_manifest.py` 生成，用于冻结已有输出。`verify_results.py` 只校验：全文件哈希、固定策略三段×三指标、已保存 walk-forward 仓位路径、以及跨股票均值；它**不**重跑年度选模、公司行动审计、bootstrap 区间、预测力或行业实验。

## 1. 数据快照
| 数据集 | 复权 | 行数 | 起始 | 结束 | 抓取时间 |
|---|---|---|---|---|---|
| 000977_hfq | 2 | 5179 | 2005-01-04 | 2026-10-21 | 2026-10-21T11:51:44+00:00 |
| 000977_raw | 0 | 5179 | 2005-01-04 | 2026-10-21 | 2026-10-21T11:51:44+00:00 |
| csi300 | 2 | 5292 | 2005-01-04 | 2026-10-21 | 2026-10-21T11:51:45+00:00 |
| sse | 2 | 5292 | 2005-01-04 | 2026-10-21 | 2026-10-21T11:51:46+00:00 |
| chinext | 2 | 3978 | 2010-06-01 | 2026-10-21 | 2026-10-21T11:51:47+00:00 |
| csi500 | 2 | 5292 | 2005-01-04 | 2026-10-21 | 2026-10-21T11:51:47+00:00 |
| 000977_qfq | 1 | 5179 | 2005-01-04 | 2026-10-21 | 2026-10-21T14:12:00+00:00 |
| 000977_panel | None | None | None | None | None |
重建序列 `000977_adj` 的 5 个送转事件：
| 除权日 | 倍数 | 依据 |
|---|---|---|
## 2. 核心结果
| 区间 | CAGR | MaxDD | Calmar | Sharpe | 平均仓位 |
|---|---|---|---|---|---|
| 全样本 2005-2026 | 17.32% | -35.55% | 0.49 | 0.87 | 32.91% |
| 样本内 2005-2016 | 17.15% | -25.83% | 0.66 | 0.95 | 27.34% |
| 样本外 2017-2026 | 17.29% | -35.24% | 0.49 | 0.79 | 39.29% |
| 2011 起 | 14.82% | -35.41% | 0.42 | 0.75 | 36.15% |
| 买入持有 | 19.37% | -69.79% | 0.28 | 0.60 | 99.04% |
滚动前进（2011-2026，固定结构族内每年重选）：
| 集成档位 | CAGR | MaxDD | Calmar | Sharpe |
|---|---|---|---|---|
| top1 | 13.23% | -37.08% | 0.36 | 0.68 |
| top3 | 14.13% | -32.47% | 0.44 | 0.73 |
| top5 | 14.25% | -33.42% | 0.43 | 0.73 |
## 3. 统一基准（2011-01-04 起，同一引擎）
| 组合 | CAGR | MaxDD | Calmar | Sharpe | 平均仓位 |
|---|---|---|---|---|---|
| Buy & hold | 17.75% | -65.44% | 0.271 | 0.576 | 97.94% |
| Constant 60% | 13.73% | -43.15% | 0.318 | 0.573 | 60.13% |
| Constant 40% | 10.68% | -29.97% | 0.356 | 0.600 | 39.72% |
| Constant 30% | 7.68% | -22.93% | 0.335 | 0.562 | 29.61% |
| Constant 37% (matched) | 9.60% | -27.63% | 0.347 | 0.581 | 36.68% |
| Timing (walk-forward) | 13.24% | -38.27% | 0.346 | 0.680 | 36.72% |
| Vol target 20% | 9.73% | -41.12% | 0.237 | 0.529 | 47.79% |
| Vol target 25% | 12.35% | -44.40% | 0.278 | 0.562 | 58.73% |
## 4. 风险覆盖层（节选）
| 基底 | 覆盖层 | CAGR | MaxDD | Calmar | 最差年 | Ulcer |
|---|---|---|---|---|---|---|
| bh | none | 19.37% | -69.79% | 0.278 | -48.77% | 0.365 |
| bh | vol_regime70+csi_dd20_x0 | 22.47% | -55.02% | 0.408 | -33.91% | 0.218 |
| bh | combo+vt30 | 16.15% | -43.45% | 0.372 | -22.93% | 0.149 |
| core | none | 19.04% | -42.07% | 0.453 | -31.45% | 0.138 |
| core | csi_dd20_x0 | 18.21% | -39.88% | 0.457 | -20.88% | 0.118 |
| bh | cppi_75_2 | 11.55% | -65.56% | 0.176 | -38.98% | 0.347 |
| bh | dd_soft_10_60 | 7.86% | -50.80% | 0.155 | -27.08% | 0.358 |
| bh | eqma_150 | -1.57% | -44.75% | -0.035 | 0.00% | 0.310 |
## 5. 消融（全样本）
| 变体 | 平均仓位 | CAGR | MaxDD | Calmar |
|---|---|---|---|---|
| Full (calm + oversold + override) | 33.20% | 16.61% | -40.25% | 0.413 |
| No oversold boost | 25.55% | 11.59% | -27.85% | 0.416 |
| No market override | 43.58% | 18.38% | -47.49% | 0.387 |
| No calm-vol gate | 50.15% | 17.12% | -40.81% | 0.419 |
| Calm gate only | 32.18% | 12.24% | -35.74% | 0.342 |
| Oversold only | 29.41% | 15.93% | -48.13% | 0.331 |
| Override only | 80.83% | 17.01% | -60.23% | 0.282 |
| Full + vol cap 55% | 33.09% | 16.60% | -40.29% | 0.412 |
## 6. 跨股票配对检验（94 只）
| 区间 | 规则 | n | ΔCalmar 均值 | 95% CI | ΔCAGR | ΔMaxDD | 胜率 |
|---|---|---|---|---|---|---|---|
| full | composite | 91 | +0.0226 | [-0.0037, +0.0485] | +0.67pp | -8.57pp | 61.5% |
| full | oversold | 91 | +0.0207 | [+0.0099, +0.0316] | +0.71pp | -2.71pp | 62.6% |
| full | calm | 91 | +0.0101 | [-0.0129, +0.0318] | +0.52pp | -8.03pp | 59.3% |
| full | bear_zero | 91 | +0.0058 | [-0.0079, +0.0186] | +0.44pp | -3.16pp | 54.9% |
| full | bear_half | 91 | +0.0044 | [-0.0033, +0.0126] | +0.27pp | -0.71pp | 52.7% |
| full | trend_ma200 | 91 | -0.1493 | [-0.1735, -0.1256] | -7.82pp | -33.14pp | 5.5% |
| is | composite | 83 | +0.0305 | [-0.0385, +0.1004] | -0.10pp | -6.82pp | 53.0% |
| is | calm | 83 | +0.0262 | [-0.0299, +0.0830] | +0.42pp | -5.47pp | 57.8% |
| is | oversold | 83 | +0.0205 | [-0.0014, +0.0433] | +0.41pp | -2.47pp | 60.2% |
| is | bear_half | 83 | -0.0162 | [-0.0263, -0.0067] | -0.46pp | -1.42pp | 25.3% |
| is | bear_zero | 83 | -0.0289 | [-0.0452, -0.0117] | -0.79pp | -3.48pp | 25.3% |
| is | trend_ma200 | 83 | -0.1587 | [-0.1897, -0.1284] | -6.94pp | -26.98pp | 9.6% |
| oos | composite | 91 | +0.0620 | [+0.0086, +0.1159] | +1.66pp | -4.28pp | 65.9% |
| oos | bear_zero | 91 | +0.0462 | [+0.0210, +0.0729] | +1.52pp | -1.77pp | 64.8% |
| oos | oversold | 91 | +0.0389 | [+0.0204, +0.0566] | +1.24pp | -1.25pp | 70.3% |
| oos | calm | 91 | +0.0298 | [-0.0168, +0.0723] | +1.30pp | -6.19pp | 57.1% |
| oos | bear_half | 91 | +0.0239 | [+0.0085, +0.0391] | +0.78pp | -0.21pp | 67.0% |
| oos | trend_ma200 | 91 | -0.2228 | [-0.2589, -0.1883] | -9.39pp | -24.53pp | 3.3% |
## 7. 行业层
| 策略 | 全样本 CAGR | 全样本 MaxDD | Calmar | IS Calmar | OOS Calmar |
|---|---|---|---|---|---|
| Equal weight | 3.03% | -44.03% | 0.069 | -0.008 | 0.260 |
| Top3 mom120 | 3.81% | -57.15% | 0.067 | -0.035 | 0.305 |
| Top3 mom250 | 8.22% | -49.83% | 0.165 | 0.068 | 0.493 |
| Top3 lowvol60 | 1.49% | -47.52% | 0.031 | 0.040 | 0.037 |
| Top3 reversal250 | 0.76% | -44.36% | 0.017 | -0.062 | 0.177 |
## 8. 预测力
| 信号 | IC(20d) | 95% CI | 同号段数 |
|---|---|---|---|
| oversold | +0.095 | [-0.000, +0.194] | 2 |
| oversold_bin | +0.070 | [-0.009, +0.150] | 2 |
| rsi_low | +0.065 | [+0.012, +0.117] | 3 |
| trend_ma60 | -0.028 | [-0.114, +0.046] | 2 |
| trend_ma200 | -0.024 | [-0.115, +0.070] | 1 |
| trend_dist200 | -0.076 | [-0.173, +0.024] | 2 |
| mom250 | -0.037 | [-0.144, +0.055] | 2 |
| csi_ma200 | +0.044 | [-0.058, +0.124] | 2 |
| csi_dd250 | +0.006 | [-0.088, +0.095] | 2 |
| vol20 | -0.024 | [-0.101, +0.066] | 1 |
| csi_vol60 | +0.028 | [-0.064, +0.122] | 2 |
Reality Check：11 个信号，max|t| = 1.44，bootstrap p = 0.61。

OOS 回归（20d）：
| 模型 | OOS R² | 命中率 |
|---|---|---|
| baseline_vol | 0.0190 | 0.523 |
| vol+market | -0.0075 | 0.494 |
| vol+market+oversold | -0.0313 | 0.515 |
| vol+market+trend | -0.0403 | 0.514 |
| vol+market+oversold+trend | -0.0483 | 0.511 |
## 9. 文件清单与校验
校验方式：`sha256sum -c SHA256SUMS`（文档/代码/图表/结果表）与 `sha256sum -c SHA256SUMS.data`（行情缓存）。
文档与结果文件：96 个；行情数据文件：117 个。

### 文档、代码、图表与结果
| 文件 | 字节 | SHA256 前缀 |
|---|---|---|
| AUDIT_RESPONSE.md | 9,510 | 43d97d15255f7ae5… |
| CROSS_SECTION.md | 7,343 | 5c973669fe916f1d… |
| README.md | 5,869 | 4f98e2245b31931c… |
| REPORT.md | 12,742 | a724d7718ac05ddc… |
| RETRACTIONS.md | 2,200 | b7a648787569bcb7… |
| RISK_AND_ALPHA.md | 10,945 | 131d75ff14f247e1… |
| SECTOR.md | 5,665 | 7eaafbfd6638929a… |
| ablation.py | 3,657 | 04a0a6ff72404ce0… |
| alpha_study.py | 13,401 | 91567063107b9154… |
| audit.py | 10,863 | fb7e8fd0d95f5185… |
| benchmark.py | 6,474 | 297a165c83740ca8… |
| build_adjusted.py | 1,383 | fa55c3d1519cffe9… |
| cross_section.py | 8,418 | 12c19b80e804fa57… |
| cross_section_bootstrap.py | 1,290 | 3d535b93ba351081… |
| data/000977_panel.manifest.json | 4,991 | acd94be9c704cfd9… |
| data/corporate_actions_000977.csv | 1,913 | 2eeee915d156e369… |
| data/manifest.json | 1,716 | 9cdd6e191d4d1e5a… |
| data/sectors/sector_names.json | 234 | 80f8490f5f650bd1… |
| data/universe/industry_map.csv | 1,633 | d8f7e75ce163669a… |
| data/universe/universe.csv | 7,830 | 7c0d20cfc0e35d21… |
| engine.py | 10,716 | 1565d3f92f115482… |
| explore.py | 4,592 | 4945c3fd14b818e1… |
| fetch_data.py | 4,172 | 162ccd01a0d6a789… |
| figures/fig10_sector_rotation.png | 89,948 | 9392790d18f57655… |
| figures/fig1_equity_drawdown.png | 204,446 | c14a6a8217a5e3c5… |
| figures/fig2_exposure.png | 81,635 | 440d62c105f5ee51… |
| figures/fig3_yearly.png | 45,991 | 1adee9d93b43c2d2… |
| figures/fig4_heatmap.png | 91,441 | 89637ccdd962de46… |
| figures/fig5_audit_2024.png | 102,205 | 289b14ff1a83bb6b… |
| figures/fig6_benchmark.png | 146,221 | 2b5883539a49e55f… |
| figures/fig7_risk_frontier.png | 63,000 | 4013e8281dba835c… |
| figures/fig8_alpha_evidence.png | 61,101 | b13f1aca2f94b3d6… |
| figures/fig9_cross_section.png | 47,632 | 000e4aade3d248cf… |
| make_manifest.py | 12,412 | d1c5f03dda895406… |
| out/ablation.csv | 3,246 | 4703b35c6911a81a… |
| out/alpha_events20.csv | 866 | a6f401bd416105fb… |
| out/alpha_events60.csv | 855 | 708d2d99180e95ca… |
| out/alpha_ic20.csv | 1,884 | 3742123b0c2c13ae… |
| out/alpha_ic60.csv | 1,886 | 2bd1d76395de6412… |
| out/alpha_oos_regression.csv | 701 | da9f3995a9f31b20… |
| out/alpha_reality_check.json | 516 | a4f83548dbd5ceb4… |
| out/audit_2024_monthly.csv | 2,321 | 84ca293edda65f3e… |
| out/audit_2024_trades.csv | 9,072 | 9ba235e154990478… |
| out/audit_report.json | 3,744 | 0229fb55bb993719… |
| out/benchmark_oos.csv | 1,244 | 5310b4ec95021fce… |
| out/benchmark_summary.json | 651 | f95a3714d612ce5a… |
| out/bootstrap.json | 270 | f0e2032e5c2af24f… |
| out/buy_hold.csv | 885 | 865f09f6e781992e… |
| out/candidate_table.csv | 129,165 | 64eba48b1e995cbf… |
| out/candidate_table_adj.csv | 156,427 | 037da0b4ebd435f9… |
| out/candidate_table_qfq.csv | 166,485 | 34bfb52e23c827b2… |
| out/composite_grid.csv | 120,991 | 030a2ee69755b7fd… |
| out/cross_section.csv | 526,819 | 741a412d5964ae81… |
| out/cross_section_bootstrap.csv | 2,610 | 187005f63235ddaa… |
| out/cross_section_meta.json | 34 | 875fa54b7532b79e… |
| out/cross_section_summary.csv | 5,624 | c3e15304217636bd… |
| out/final_stats.json | 2,817 | a7db81d479a55354… |
| out/frontier.json | 810 | cdd1350e95e7aeea… |
| out/headline.json | 2,032 | a3542490c105b186… |
| out/is_oos_merged.csv | 18,242 | 05461bfd5205271e… |
| out/max_volreg_rev.csv | 8,252 | 5661ae66a5e50fda… |
| out/retrev_grid.csv | 31,757 | fbe5675cd9ddcfe9… |
| out/risk_overlays.csv | 12,034 | bd176dca627240c3… |
| out/risk_overlays.json | 22,247 | 005c41c506e6c44f… |
| out/scan_full.csv | 14,312 | 5f32ef54fe78226d… |
| out/scan_is.csv | 14,082 | 6d27eeb5b43b6afe… |
| out/scan_oos.csv | 14,476 | c28a2d508bb3c1cb… |
| out/scan_reversal.csv | 10,673 | 04e52a079efea09a… |
| out/sector_overlay_000977.csv | 2,962 | 3f618b7a9f9cefd7… |
| out/sector_rotation.csv | 740 | 3a084964b923148c… |
| out/walkforward_summary.json | 2,260 | c7e1456ba3713e01… |
| out/wf_calmar_choices.csv | 3,020 | d26c7fc876c0dba6… |
| out/wf_calmar_equity.csv | 110,268 | 8e96533bf175c71b… |
| out/wf_dd_budget_choices.csv | 3,051 | d751e0ffb73cd496… |
| out/wf_dd_budget_equity.csv | 110,337 | c88fe90697fe4f83… |
| out/wf_final_choices.csv | 1,042 | 6c10163e1c36a91b… |
| out/wf_final_exposure.csv | 56,809 | 3cb2e720b95bc2ec… |
| out/wf_final_summary.json | 3,257 | 1cbe03132a72d31c… |
| out/wf_final_top1.csv | 110,723 | 5355e3b760d04000… |
| out/wf_final_top3.csv | 110,987 | c45230cd47446d8a… |
| out/wf_final_top5.csv | 110,893 | 5f0ddb84434c9fd1… |
| out/wf_focused_k1_equity.csv | 110,362 | 6f78133ec23442e8… |
| out/wf_focused_k3_equity.csv | 110,202 | ea94a4c5830d4ec2… |
| out/wf_focused_k5_equity.csv | 110,069 | 1b4c76aceb7fd7ce… |
| report.py | 8,019 | c3399f66bcb64bf6… |
| risk_overlay.py | 10,385 | e3f745de89c7353d… |
| scan.py | 7,909 | 249175ec62313341… |
| sector_fetch.py | 3,450 | 6389a14e7fa451bc… |
| sector_study.py | 7,451 | 708cab1a3bbfecab… |
| show_signal.py | 2,386 | 960f877f735cb623… |
| signals.py | 11,318 | 1dcc91e1be6a68a3… |
| strategy.py | 7,437 | 74a979f8b9981e43… |
| universe_fetch.py | 5,824 | 970dca9cb2fa990b… |
| verify_results.py | 3,912 | f497c4c52b273b24… |
| walkforward.py | 5,035 | 93924cc91ee1af5a… |
| walkforward_focused.py | 3,861 | 0a7fc8e5c24c7c27… |
## 10. 交叉检查步骤
```bash
cd datahub/research/timing_000977
sha256sum -c SHA256SUMS && sha256sum -c SHA256SUMS.data
../../.venv/bin/python verify_results.py
../../.venv/bin/python build_adjusted.py
../../.venv/bin/python report.py
../../.venv/bin/python cross_section.py
../../.venv/bin/python risk_overlay.py
```

`verify_results.py` 会重算最终策略、基准与跨股票汇总，并与本清单中的文件逐项比对，输出 PASS/FAIL。

## 11. 已知局限
- 单票样本（000977）约 5 个牛熊周期；跨股票样本 94 只且为当前上市股票（幸存者偏差）。
- 重建价格序列有 3 个送转倍数为推断值（2006/2017/2020），现金分红未计入主序列。
- 规则参数来自 000977 样本内研究，跨股票检验未重新拟合，但存在设计偏差。
- 事件研究与 IC 使用重叠窗口，有效样本小于名义样本。
- 未建模涨跌停无法成交、停牌、冲击成本、融券。
