# 估值因子与 flip_wide 的融合实验 —— 结论：不加

> Research-only。在构造层翻转 walk-forward 验证（#177）基础上测试：size 中性估值
> （EP/BP）+ 低换手作为附加正向分位与 flip_wide（8 技术分量全翻转、宽书）按权重 w 混合，
> 是否能提升评估器口径 IR。数据同前：合并快照 9,203,459 行 + daily_basic 8,695,127 行。

## 结果（runner 口径：每日 top-800 宽书、净摩擦、vs 当日等权基准）

| w_value（估值块权重） | train 2019-23 | val 2024 | val 2025 | test 2026H1 |
|---|---|---|---|---|
| **0.0 = 纯 flip_wide** | **+0.485** | **+0.975** | **+0.385** | +0.426 |
| 0.2 | +0.524 | +0.871 | +0.017 | +0.545 |
| 0.3 | +0.519 | +0.791 | −0.256 | +0.595 |
| 0.5 | +0.500 | +0.619 | −0.742 | +0.713 |

（w=0 行与 #177 的 flip_wide 数值一致，作为口径对账。）

## 结论

1. **估值融合不改善 flip_wide**：任何 w>0 都使 **2025 异常年从 +0.385 崩向负值**
   （w=0.3 → −0.256，w=0.5 → −0.742）。这正是单因子审计已知的「2025 价值失效」：
   估值因子在 2025 是八段 regime 中唯一失效段，叠加它就把 flip_wide 唯一在 2025 仍正的
   稳健性破坏掉了。
2. 估值融合**只**在 test 2026H1（+0.426→+0.713 @w=0.5）和 train 2019-23 微升，代价是
   2024/2025 双降——跨 regime 稳健性整体变差，不符合「保持选高买入语义 + 全窗口稳定」的
   目标。
3. **推荐：flip_wide 保持纯 8 技术分量方向翻转，不并入估值块**。估值因子的正确用法不是
   与翻转技术分等权混合，而是：
   - 作为**独立的 regime 条件**（价值仅在非 2025 类 regime 生效——需要 regime 识别，
     当前市场 breadth 门控只区分强弱市，不区分价值风格）；
   - 或作为**生产路径的独立组件**经 ≥120 天 T+1 安全分数单独验证后再定去留，而非在研究
     层强行融合。
4. 与 #177 一致：flip_wide（纯构造层翻转宽书）仍是当前唯一全窗口正、decay 0.00 的候选；
   估值因子审计结论（IC 正、size 中性成立）不变，但**组合层面它对翻转宽书是负贡献**——
   单因子正 IC 不等于组合增量（与 IC/IR 区别的典型实例）。

## 复现

```bash
PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_value_blend_validate.py \
  --snapshot19 /tmp/h20-2019-2023.parquet \
  --snapshot24 datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet \
  --daily-basic /tmp/daily_basic_all.parquet \
  --profile autoresearch/profile.yaml
```
