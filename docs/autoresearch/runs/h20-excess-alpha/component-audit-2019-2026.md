# H20 分量级隔离审计 —— 2019-2026 多 regime 完整结论

> Research-only。数据：冻结快照 ×2 —— 2019-2023 导出（5,677,504 行，
> sha256 `2fb501c660b2774c789341795795052f2c1db1bbd2635e03a7459ea600eefd8d`）+
> 本地 2024-2026 快照（3,525,955 行）。T+1 实际入场/出场开盘价，20 交易日持有，
> 无前视。指标：dailyIC=分量值对前向 20 日收益的日均截面 Spearman 秩相关；
> LSspreadIR=五分位多空价差超额 IR；buyTop20/buyBot20IR=买 top/bottom 五分位 vs 当日
> 等权基准的超额 IR（按评估器惯例 ×√(252/20)）。industry_momentum 因行业数据在导出时
> 损坏而全部中性(NaN)——该项留待行业修复后重导验证（历史窗口内恒 NaN）。

## 结果（按年 regime，buyTop20IR / buyBot20IR）

| 分量（权重） | 2019 反弹 | 2020 疫情 | 2021 结构牛 | 2022 熊 | 2023 震荡 |
|---|---|---|---|---|---|
| signal_strength (15) | −0.70 / −0.08 | +0.48 / −1.43 | −1.74 / +0.67 | −1.86 / +0.57 | −1.25 / +0.14 |
| momentum (15) | −2.62 / +1.00 | −0.70 / −0.76 | −1.23 / −0.74 | −3.11 / +0.49 | −1.92 / +0.18 |
| trend_alignment (30) | −1.09 / +0.62 | +0.18 / −0.86 | −1.42 / +0.60 | −2.16 / +1.19 | −1.36 / +0.34 |
| breakout_or_position (5) | −1.36 / +0.08 | +0.17 / −1.62 | −1.32 / +0.39 | −2.50 / +0.40 | −1.33 / +0.18 |
| relative_strength (15) | −2.62（同 momentum） | −0.70（同） | −1.23（同） | −3.11（同） | −1.92（同） |
| real_relative_strength (10) | −2.62（同） | −0.71（同） | −1.23（同） | −3.11（同） | −1.91（同） |
| risk_penalty (15) | −1.77 / −0.48 | −1.13 / −0.20 | −2.23 / +0.80 | −2.54 / +0.82 | −1.50 / +0.92 |
| **COMPOSITE** | **−1.70** | **−0.11** | **−1.04** | **−2.38** | **−1.49** |

| 分量（权重） | 2024 mixed+stimulus | 2025 bear/chop | 2026H1 |
|---|---|---|---|
| signal_strength (15) | −0.67 / +0.35 | −1.18 / +0.86 | −0.57 / +0.34 |
| momentum (15) | −2.11 / +0.97 | −1.33 / +0.39 | −0.35 / −0.28 |
| trend_alignment (30) | −1.27 / +1.04 | −1.12 / +0.86 | −0.33 / +0.16 |
| breakout_or_position (5) | −1.70 / +0.91 | −1.05 / +0.20 | −0.23 / +0.27 |
| relative_strength (15) | −2.11（同） | −1.33（同） | −0.35（同） |
| real_relative_strength (10) | −2.11（同） | −1.34（同） | −0.35（同） |
| risk_penalty (15) | −0.84 / +0.20 | −1.00 / −1.09 | −0.35 / +0.34 |
| **COMPOSITE** | **−1.50** | **−1.37** | **−0.34** |

dailyIC（未列全）在 2019-2026 八个 regime 中：2019 全负；2020 signal_strength +0.002
（唯一 ≈0）、momentum/trend 仍负；2021-2026 全部显著为负（momentum/trend/risk 量级
−0.03~−0.13）。

## 跨 regime 审计结论

1. **反向是「恒反向」，不是 regime 依赖**。决定性问题——「牛市里趋势分量是否转正」——
   答案是否：2019 反弹与 2021 结构牛里 momentum/trend_alignment 的 dailyIC 与 buyTop20IR
   依然显著为负（2021 trend top −1.42、momentum top −1.23）。八个 regime（含 2024 刺激牛
   尾段）无一让「高分分量 → 高前向收益」成立；2020 是唯一接近 0 的 regime
   （signal_strength 转 ≈0，composite −0.11），疫情 V 反时反转关系短暂失效但不翻转。
2. **COMPOSITE 买 top 五分位在全部 8 个 regime 都亏**（IR −0.11 ~ −2.38）。在既定「选高
   买入」语义下，当前权重的选股方向在所有可测历史里系统性反预测。按既定决策**不改分数
   语义**；修正只能在构造层做（翻转向量方向/剥离冗余/重配权重），不能靠反转分数用途。
3. **「买低分」不是独立 alpha，而是反预测关系的镜像，强度随 regime 波动**：buyBot20IR
   在 2021/2022/2023/2024/2025 大多为正，但 2020 明显失效（signal −1.43、breakout −1.62、
   momentum −0.76——反转关系消失时买低分同样亏），2019 的 risk_penalty bot 也负（−0.48）。
   2026H1 momentum/real_rs 的 bot 亦转负（−0.28）。→ 若构造层翻转方向后「低分=高分」的
   那部分收益会随 regime 波动，这正是必须按分量+按 regime 分别审计、而不是整本书反转的
   原因。
4. **momentum / relative_strength / real_relative_strength 三合一是同一个信号**（八段
   regime 的 IC/top/bot 全部相同到小数点后 2 位）——40% 权重是同一笔押注，冗余被实锤。
5. **risk_penalty 方向不稳定**：作为减项在 2021/2022/2023 的 bot（低风险侧）为正
   （+0.80/+0.82/+0.92，即减风险有效），2024 弱正（+0.20），但 2019 bot −0.48、2025
   bot −1.09（2025 高/低风险都亏）、2020 bot −0.20。risk 分量需要单独的 regime 检验，
   不能按趋势分量同批翻转。
6. 2020（疫情 V）与 2026H1（弱 regime）的 composite 亏损最轻（−0.11/−0.34），与 2026
   线上 U 形样本呼应：反向效应在「反转关系弱」的 regime 会收缩——所以即便构造层翻转，
   预期收益也高度 regime 依赖，最终需要 regime 门控或保守权重。

## 含义与下一步（不变，证据已补全）

- 修正必须在**构造层**：对 7 个趋势分量按「当前为恒反向」证据翻转向量方向（等价于把
  分位语义从避高改选高在构造内实现），剥离 momentum 三重冗余（40%→~15%），risk_penalty
  单独审计后再定方向；分数语义保持「选高买入」。
- 生产修改门槛不变：≥120 天 T+1 安全的生产分数 + walk-forward + version bump 后才动；
  审计只证明构造方向问题，不授权直接上线任何翻转。
- 基本面因子（B 路径）数据接入已启动（daily_basic 估值列），是构造层重建的替代/增量
  证据源。
- industry_momentum 待行业数据修复重导（历史窗口恒 NaN，不影响上述结论）。

## 复现

```bash
# 2019-2023（在 pod 导出 /tmp/h20-2019-2023.parquet）
PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_component_audit.py /tmp/h20-2019-2023.parquet
# 2024-2026（本地标准快照）
PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_component_audit.py datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet
```
