# H20 研究阶段总结 —— 2026-09-04

> Research-only。本文串起 2026-08-29 至今（H20 autoresearch bootstrap → daily_basic 数据接入 →
> 多 regime 审计 → 构造层翻转候选 walk-forward → 估值融合实验）的证据链、已合并的工程
> 变更、当前结论与待办。分报告链接见文末。所有研究基于冻结快照（T+1 入场/出场、20 日
> 持有、无前视）；分数语义按既定决策维持「选高买入」（高分=买入优先），本阶段所有修正
> 均发生在构造层，不是分数用途反转。

## 一、数据基础设施（已完成并合并）

| PR | 内容 | 状态 |
|---|---|---|
| #175 | tushare `daily_basic` 接入：新集合 `stock_daily_basic`（pe_ttm/pb/ps_ttm/dv_ttm/total_mv/circ_mv/turnover_rate），接口+模型+handler+回填 runner（per-date 完成标记断点续跑、单日失败隔离） | merged |
| #176 | `stock_daily_basic` 注册进 prod→dev data-sync（SYNCABLE_COLLECTIONS + alias + date-first 索引） | merged |

- dev **与** prod 均已回填 **8,695,127 行 / 1,838 交易日**（2019-01-02 ~ 2026-07-31），两库一致；
  完成后 dev data-sync 端到端验证通过（status GOOD，幂等无冲突）。
- prod 回填选择**从 tushare 重新拉取**而非 dev→prod 反向同步（data-sync 是单向 prod→dev，
  prod 是权威源；重拉幂等且与 dev 同源同值）。
- 每日增量/新鲜度接入（data_asset_status + 生产每日拉取）**按设计暂缓**：等研究确认新因子
  价值后再上，避免为未验证因子上线生产任务。

## 二、证据链 1：8 个技术分量 2019-2026 全程恒反向（#174）

分报告：`component-audit-2019-2026.md`（取代 `component-audit-2024-2026.md` 的部分结论）

- 冻结快照 2019-2023（5,677,504 行）导出完成并与本地 2024-2026 快照（3,525,955 行）审计对齐，
  共 9,203,459 行 / 八段 regime。
- **7 个正向分量（signal_strength/momentum/trend_alignment/breakout/industry_momentum/
  relative_strength/real_relative_strength）dailyIC 在全部 8 个 regime 显著为负**——包括
  2019 反弹与 2021 结构牛（牛市里趋势分量同样反预测）→ **反向是结构性的，不是 regime 依赖**。
- **COMPOSITE 买 top 五分位在全部 8 个 regime 亏损**（IR −0.11 ~ −2.38），当前「选高买入」
  语义下组合方向系统性反预测。
- **momentum / relative_strength / real_relative_strength 三合一是同一信号**（IC/top/bot
  全部一致）——40% 权重是同一笔押注（冗余实锤）。
- industry_momentum 历史窗口恒 NaN（无历史行业数据），不影响结论。
- 关键修正在审计中被证伪/证实：`full_reversal`（旧版 IR −0.92）失败 ≠ 「翻转无效」——
  它只翻转了 7 个正向分量而 risk_penalty 未真正翻转；方向修正本身有效（见证据链 3）。

## 三、证据链 2：基本面估值因子是首个方向天然正确的因子源（#177 前半）

分报告：`fundamental-factor-audit-2019-2026.md`

- join daily_basic（8.7M 行）到快照，per-regime 审计单因子：
  | 因子 | 2019-2026 dailyIC | size 中性后 | 备注 |
  |---|---|---|---|
  | ep_ttm (1/pe) | 7/8 正（2025 微正） | 不变或增强 | 最强单因子之一 |
  | bp (1/pb) | 2021/2023 强正（+0.10/+0.12） | 不变 | — |
  | sp_ttm / dv_ttm | 弱到中等正 | 不变 | — |
  | turnover | **恒负**（−0.08~−0.14） | 不变 | 低换手方向正确 |
  | log_mv | regime 大幅摆动 | — | 不可当稳定方向 |
- **估值 alpha 不是 size 代理**（size 中性后 IC 不消失）——设计文档最担心的污染未出现。
- decile（2023-2026H1）：D0 显著跑输（超额 −1.16%），D3-D9 平坦（+0.1~+0.17%）→ 价值
  信息主要在**尾部剔除**（避开最差），符合 A 股价值因子弱/污染的历史经验。
- 估值与现有 H20 组合低相关（2024 秩相关 +0.06）→ 真实增量信息源（但在组合层见证据链 4）。

## 四、证据链 3：构造层翻转 + 宽书 = 唯一全窗口正候选（#177 后半）

分报告：`flip-walkforward-2019-2026.md`

**关键方法学修正**：审计（无摩擦 top 五分位宽书）与评估器默认配置（**top5% 只买 30 只 +
净摩擦**）对同一方向结论相反——30 只小样本噪声 + 摩擦是旧记录（full_reversal −999、
summary.md 的早期结论）误导的根源。必须扫描 selection 宽度。

合并快照 2019-2026 上，评估器口径 walk-forward（train 2019-23 → val 2024/2025 → test
2026H1，净摩擦 vs 当日等权基准）：

| 候选 | train19-23 | val2024 | val2025 | test26H1 | WF decay |
|---|---|---|---|---|---|
| current_h20 (top5/30) | +0.185 | +0.167 | −0.859 | −0.474 | 2.11 硬失败 |
| flip (top5/30) | −0.330 | +0.700 | −0.918 | +0.322 | 0.00（噪声） |
| flip_top20 (200只) | +0.083 | +0.560 | −0.080 | +0.440 | 0.00 |
| **flip_wide (800只)** | **+0.485** | **+0.975** | **+0.385** | **+0.426** | **0.00** |

- **flip_wide（8 分量 direction 全 −1，宽书排除翻转后 bottom 20%）是唯一全窗口稳定正、
  decay 0.00 的候选**；2025 异常年仍 +0.385（baseline −0.859）。年化净超额 ~+6%。
- baseline current_h20 扩展 walk-forward **decay 2.11 硬失败**——再次确认原方向在近期 regime
  反预测。
- **语义边界（重要）**：flip_wide 持仓是原 h20 最低 ~16% 分位（低动量/低趋势）。实现是
  **构造层方向取反**（候选分数「越高越买」不变），不是 usage 反转——「选高买入」语义在
  usage 层保留，但「高」的定义被构造改成了低动量。生产化时需在 Spec Gate 显式声明。

## 五、证据链 4：估值块混入 flip_wide 不加（#178，PR open）

分报告：`value-flip-blend-2019-2026.md`

| w_value（估值块权重） | train19-23 | val2024 | val2025 | test26H1 |
|---|---|---|---|---|
| 0.0 = 纯 flip_wide | +0.485 | +0.975 | **+0.385** | +0.426 |
| 0.2 | +0.524 | +0.871 | +0.017 | +0.545 |
| 0.3 | +0.519 | +0.791 | −0.256 | +0.595 |
| 0.5 | +0.500 | +0.619 | **−0.742** | +0.713 |

- **任何估值权重都破坏 flip_wide 在 2025 的稳健性**（2025 是已知价值失效年）——估值只救
  test 2026H1 与 train 微升，2024/2025 双降，跨 regime 稳健性整体变差。
- **结论：flip_wide 保持纯构造层翻转，不并入估值块**。估值因子正确用法 = regime 条件块
  （需风格识别）或独立生产组件走 ≥120 天 T+1 单独验证，而非等权混合。
- 这是单因子 IC 正 ≠ 组合增量的典型实例（IC 说方向、IR 说能否稳定赚钱）。

## 六、当前结论（截至 2026-09-04）

1. **方向修正已从研究到候选验证闭环**：8 分量构造层全翻转 + 宽书（flip_wide）是当前唯一
   全窗口正、decay 0.00 的候选，直接回答「分数为什么恒反」并给出可评估的修正配置。
2. **估值因子数据基建完整可用**（prod+dev 8.7M 行、data-sync 跟进），单因子方向正确，但
   组合层不作为 flip_wide 的混合成分。
3. **2025 仍是未归因的异常年**：flip_wide 是唯一幸存者（+0.385），baseline/估值/窄书全部
   转负；需单独归因后才能放心推广（2025 微小盘风格极端 + 反转动能收缩的假设待验证）。
4. **生产化门槛未变**：≥120 天 T+1 安全的生产分数 + walk-forward + version bump + Spec
   Gate。当前 flip_wide 是研究证据，不是上线授权。

## 七、待办（决策点）

- [ ] 合并 #178（value-blend 实验落地，research-only）
- [ ] 生产数据路径：daily_basic 每日增量 + data_asset_status 新鲜度（研究确认后）
- [ ] 快照导出器加估值/市值列（为 regime 门控版估值或独立组件准备，需重导）
- [ ] 2025 异常年归因（风格暴露分解：小盘/反转/价值失效）
- [ ] flip_wide 语义在 Spec Gate 的显式声明文档（若走生产）
- [ ] ≥120 天 T+1 安全生产分数的累计起点（flip_wide 若要上线，需从部署日开始）

## 附：分报告索引

| 文档 | 内容 |
|---|---|
| `plans/2026-08-29-h20-excess-alpha-plan.md` | H20 研究总计划 |
| `plans/2026-09-03-fundamental-factors-data-design.md` | 基本面因子数据接入设计（A/B 路径） |
| `component-audit-2019-2026.md` | 8 分量多 regime 隔离审计（完整版，取代 2024-2026 部分版） |
| `fundamental-factor-audit-2019-2026.md` | 估值/市值/换手单因子审计 |
| `flip-walkforward-2019-2026.md` | 构造层翻转候选 walk-forward 验证 |
| `value-flip-blend-2019-2026.md` | 估值融合实验（不加） |
