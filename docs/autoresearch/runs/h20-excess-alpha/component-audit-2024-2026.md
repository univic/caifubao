# H20 分量级隔离审计 —— 2024-2026 阶段结论（多 regime 扩展进行中）

> Research-only。数据：冻结快照（T+1 实际入场/出场开盘价，20 交易日持有，无前视）。
> 指标：dailyIC=分量值对前向 20 日收益的日均截面 Spearman 秩相关；
> LSspreadIR=五分位多空价差超额 IR；buyTop20/buyBot20IR=买 top/bottom 五分位 vs 当日
> 等权基准的超额 IR（按评估器惯例 ×√(252/20)）。industry_momentum 因行业数据在导出时
> 损坏而全部中性(NaN)——该项留待行业修复后重导验证。

## 结果（按年 regime）

| 分量（权重） | REGIME 2024 (mixed+stimulus) | REGIME 2025 (bear/chop) | REGIME 2026H1 |
|---|---|---|---|
| signal_strength (15) | IC −0.037 / LS −0.58 / top −0.67 / **bot +0.35** | IC −0.074 / LS −1.07 / top −1.18 / **bot +0.86** | IC −0.082 / top −0.57 / **bot +0.34** |
| momentum (15) | IC −0.088 / top −2.11 / **bot +0.97** | IC −0.068 / top −1.33 / **bot +0.39** | IC −0.047 / top −0.35 / bot −0.28 |
| trend_alignment (30) | IC −0.065 / top −1.27 / **bot +1.04** | IC −0.078 / top −1.12 / **bot +0.86** | IC −0.086 / top −0.33 / **bot +0.16** |
| breakout_or_position (5) | IC −0.078 / top −1.70 / **bot +0.91** | IC −0.069 / top −1.05 / bot +0.20 | IC −0.077 / top −0.23 / bot +0.27 |
| relative_strength (15) | IC −0.088（与 momentum 相同） | IC −0.068（同） | IC −0.047（同） |
| real_relative_strength (10) | IC −0.088（同） | IC −0.068（同） | IC −0.047（同） |
| risk_penalty (15) | IC −0.087 / top −0.84 / **bot +0.20** | IC −0.067 / top −1.00 / bot **−1.09** | IC −0.103 / top −0.35 / **bot +0.34** |
| **COMPOSITE (current_h20)** | buyTop20IR **−1.50** | **−1.37** | **−0.34** |

## 阶段结论（2024-2026，三个 regime 内一致）

1. **7 个正向分量全部显著反向且跨 regime 稳定**：momentum/trend/breakout/signal/relative
   strength 的 dailyIC 在 2024、2025、2026H1 均为显著负值（t 值量级 −5~−12，未列）。
   「高分分量 → 低前向收益」是结构性的，不是某一年的偶然。
2. **momentum / relative_strength / real_relative_strength 的 IC 完全相同** —— 三者本质是
   同一个 20 日收益信号（信息冗余被证实），合计权重 40% 其实是同一笔押注。
3. **买 top 五分位在任何 regime 都亏**（buyTop20IR 全负）→ 在当前「选高买入」语义下，
   组合的选股方向是系统性的反预测；但按既定决策**不改分数语义**（不做避高剔除反转）。
4. **买 bottom 五分位在 2024/2025 大多为正**（+0.2~+1.0），但**全反向（买 bottom 5%）失败**
   ——说明「低分 ≠ 可买」：最低的 5% 是下跌飞刀，只有「相对低分的中部」才有边际。这解释
   了 full_reversal 为什么失败、exclude_d8_d9 为什么只是「最不坏」。
5. risk_penalty 作为减项在 2024 有效、2025 失效（2025 高/低风险都亏）——方向不稳定，需谨慎。

## 含义与下一步

- 在「选高买入」语义内，要让分数有效，需要**按证据翻转/重配分量方向与权重**（而非反转分数
  用途）。候选方向：对 7 个趋势分量做方向审计、剥离 momentum 三重冗余、按 regime 检验
  risk_penalty。
- **多 regime 扩展导出（2019-2023）在后台进行**：完成后补上 2019 反弹/2020 疫情/2021
  结构牛/2022 熊这几个 regime 的行，验证上述反向结论是否跨牛熊成立（尤其牛市里趋势分量
  是否转正——若牛市转正，则结论是「regime 依赖」而非「恒反向」，对是否改权重是决定性证据）。
- industry_momentum 待行业数据修复后的重导验证（当前窗口内恒中性）。
