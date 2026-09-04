# 构造层翻转候选的 Walk-Forward 验证 —— 2019-2026

> Research-only。目的：把多 regime 审计发现的「8 分量恒反向」转化为**可评估的候选配置**，
> 用评估器正式口径（rank → top 分位选股 → 净摩擦净收益 vs 当日等权基准）做跨 regime
> walk-forward（train 2019-2023 → val 2024/2025 → test 2026H1）。数据：合并冻结快照
> 9,203,459 行（2019-2023 + 2024-2026 两个快照 schema 一致拼接，/tmp/h20-2019-2026-merged.parquet）。

## 一、先厘清一个口径陷阱：审计 vs 评估器

- **审计（scripts/h20_component_audit.py）**：无摩擦、top 五分位（~20% 宽书）等权截面。
- **评估器（h20_excess_alpha）**：rank 后按 selection 选股，`_net_return` 扣佣金/印花税/
  滑点，组合 = 每日所选股票等权，超额 vs 当日全部 eligible 等权基准。

两者对同一候选的结论可以**相反**（见下表 2025 年 flip）：审计 +0.50 而评估器 top5%/30只
口径 −0.92。原因是评估器默认 selection 是 **top 5% 只买 30 只**——30 只极端小样本的
超额 IR 噪声极大，且摩擦吃掉换手收益。因此必须显式扫描 selection 宽度，不能只看默认配置。

## 二、候选集合

| 候选 | 构造方向 | selection |
|---|---|---|
| current_h20（baseline） | 8 分量原方向 | top 5% / 30 只 |
| flip（=full_reversal.yaml） | 全分量 direction −1 | top 5% / 30 只 |
| flip_top20 | 全分量 direction −1 | top 20% / 200 只 |
| **flip_wide（flip_wide.yaml）** | 全分量 direction −1 | **翻转后 top 80% 中的前 800 只**（排除翻转后 bottom 20%，≈ 原 h20 最低 ~16% 宽书） |

注：full_reversal.yaml 的 risk_penalty direction=−1 与 baseline 相同——之前审计文档称
full_reversal「未真正翻转 risk」；本验证的全翻转候选沿用该文件（含 risk −1），语义为
**构造层方向取反**（非分数用途反转；分数仍「越高越买」）。

## 三、Walk-Forward 结果（评估器口径，净摩擦，IR）

| 候选 | train 2019-23 | val 2024 | val 2025 | test 2026H1 | val 2y (24-25) | WF decay |
|---|---|---|---|---|---|---|
| current_h20 (top5/30) | +0.185 | +0.167 | −0.859 | −0.474 | −0.205 | **2.11 (FAIL)** |
| flip (top5/30) | −0.330 | +0.700 | −0.918 | +0.322 | −0.041 | 0.00 |
| flip_top20 (200只) | +0.083 | +0.560 | −0.080 | +0.440 | +0.280 | 0.00 |
| **flip_wide (800只)** | **+0.485** | **+0.975** | **+0.385** | **+0.426** | **+0.714** | **0.00** |

正式 evaluate_candidate（扩展 profile，train 2019-23 → val 2024-25 → test 2026H1）：

| 候选 | split | IR | 年化净超额 | maxDD | trades | decay | 决策 |
|---|---|---|---|---|---|---|---|
| current_h20 | validation | −0.205 | −4.6% | −99.8% | 14500 | 2.11 | **discard** (performance_decay) |
| flip_wide | validation | **+0.714** | +6.0% | −48.0% | 388000 | 0.00 | keep |
| flip_wide | test 2026H1 | **+0.426** | +6.2% | −79.9% | 111200 | 0.00 | keep |

## 四、结论

1. **selection 宽度是决定性的**：同一 flip 方向，30 只窄书 val 2024-25 IR −0.04（噪声），
   200 只 +0.28，800 只宽书 +0.71。评估器默认 top5%/30只是「小样本噪声配置」，用它否定
   一个方向是误判（这正是此前 full_reversal 记录 −999 的根源之一——它其实是 decay gate
   在看 2024→2025 regime 断裂，见下）。
2. **baseline current_h20 在扩展 walk-forward 下 decay 2.11 → 硬失败**：train(2019-23)
   +0.185、val(2024-25) −0.205，方向在 2024-2025 段反号。这再次确认「8 分量原方向在近期
   regime 反预测」，与审计一致。
3. **flip_wide 是唯一全窗口稳定为正的候选**：train +0.485、val2024 +0.975、val2025
   **+0.385（唯一在 2025 异常年仍为正）**、test2026H1 +0.426，decay 0.00——跨牛（2024）、
   熊/震荡（2025）、反弹（2026H1）不衰减。年化净超额 +6%（相对等权），回撤 −48%~−80%
   （宽书等权本身波动大）。
4. **语义边界（重要）**：flip 系列通过构造层方向取反实现「高分=低动量/低趋势」。flip_wide
   选出的 800 只是**原 h20 分数最低的 ~16%**（抽样日 mean orig percentile 0.105）——效果上
   等价于「买低动量宽书」。按既定决策分数语义仍为「选高买入」（候选分数越高越买），但
   结果持仓是原体系下的低分组。若该结果要进入生产路径，需要在 Spec Gate 时显式声明这一
   构造层语义（不是 usage 反转：usage 仍是选高分，是构造把「高」定义改成了低动量）。
5. **与估值因子的衔接**：flip_wide 目前只翻转既有 8 分量，**不含** daily_basic 估值因子
   （EP/BP/低换手）——估值因子 IC 在 size 中性后仍正（+0.03~+0.08），可作为 flip_wide 的
   增量分量叠加测试（快照需重导出带估值列，或先在候选层做 rank 融合实验），是下一步。
6. 2025 仍是全体系最差年（flip_wide +0.385 是唯一幸存者）——单年归因未完成，任何生产
   决策仍需 ≥120 天 T+1 安全生产分数 + walk-forward + version bump。

## 五、复现

```bash
# 合并快照（已生成 /tmp/h20-2019-2026-merged.parquet）
# 扩展 profile（train 2019-23 / val 2024-25）见 /tmp/profile-ext.json
PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_walkforward_validate.py   # 或直接调 evaluate_candidate
```
