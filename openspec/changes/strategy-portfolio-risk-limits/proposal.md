# Strategy Portfolio Risk Limits (roadmap 1.3)

## Why

`production-capability-roadmap` 1.3 要求把组合/风控规则落到 paper 路径。当前
`strategy_engine` 的约束块里有两个**声明了但不生效**的项，以及一个缺失的约束：

- `max_single_stock_pct` 在 `config.py` 里有校验、也进了 `config_hash`，但
  `selection.py` 只做等权，注释直接写着 “ignored while equal-weight wide book”
  —— **配置谎报**：operator 以为有单票上限，实际没有。
- `min_trade_amount_cny` 注释为 “liquidity floor; 0 = unenforced”，但**从未被读取**
  —— 低分位反转策略买的正是被打下去的小票，人工执行时最容易被流动性吃掉，这条恰恰
  最重要。
- 没有任何**行业集中度**上限：宽书等权下 800 只分散度看似够，但反转信号可能整体押在
  同一风格/板块上，组合层没有护栏。

人工执行（A 模式）下，operator 拿到的是最终目标组合，配置层面承诺的风控必须真实成立，
否则“研究级输出 + 人工判断”的风险边界是假的。

## What Changes

- **单票上限真实生效**：`max_single_stock_pct` 约束每个目标权重；等权算出的人均权重
  超过上限时按上限缩减，差额留在现金（不丢标的、不放大其他标的）。
- **新增行业集中度上限** `max_industry_pct`：约束单一**一级行业**的合计目标权重。
  可用分类只有 baostock 的**证监会（CSRC）一级行业**，存在
  `StockIndustryClassification.industry_code_sw_l1`（字段名的 `sw` 是历史遗留，**不是
  申万**，见 `model/industry.py` 模块说明）。缺少可解析分类的标的归入独立 `UNKNOWN`
  桶并同样受上限约束（缺失数据不能成为隐藏集中的避风港）。
- **分类必须可证明早于 signal date**：分类表是「每股一行」的当前快照，因此只有
  `assigned_at <= signal date` 时才采用；无法证明的视为不可解析 → 归入 `UNKNOWN`，
  历史 replay 不会读到"未来才生效"的分类。
- **流动性下限真实生效**：`min_trade_amount_cny > 0` 时，signal date 当日成交额低于
  该阈值的标的**不可入选**；成交额缺失视为不可用（fail closed）。
- **禁止静默绕过资格约束**：配置了流动性下限（或任一排除开关）时，必须携带 universe
  标记表才能装配计划；不提供标记表直接装配 **fail closed**，避免出现"配置了但被跳过"
  这一类问题（与 `max_single_stock_pct` 的原始缺陷同类）。
- 三条限制只用 **signal date 及之前**的信息；**不引入任何基于当前快照的退市/状态判断**
  （`active_status` 是当前值，用于历史回放会构成前视偏差）。
- 限制通过 `validate_strategy_config` 归一化后进入 `config_hash`，因此**改风控参数即改
  配置哈希**：新产生的 run 会因哈希不匹配而归类为 `REPLAY`，**不会**自动关闭已认证的
  ACTIVE 窗口——窗口只有在认证一个新窗口时才关闭。`max_industry_pct` **不写入**
  `DEFAULT_STRATEGY_CONFIG`，以保证默认配置哈希不变、既有 pin 住的哈希可复现。

### 行为变更（需周知）

默认配置声明了 `max_single_stock_pct = 0.05`，此前**完全不生效**。本 change 让它真实
生效后：**持股数少于 20 只的组合**（人均权重 > 5%）会被压到 5%，余额留现金。
宽书（`portfolio_size=800`，人均 0.125%）不受影响，因此线上日常链路行为不变；
变的是窄组合与相关测试的期望值（`test_top_percentile_selects_high_scores_within_band`
已按新语义更新并注明原因）。

## Non-goals

- 不加 `max_gross_exposure_pct`：`cash_reserve_pct` 已经是总仓控制，新增同义参数属于
  冗余。
- 不加退市过滤：唯一可用字段 `active_status` 是当前快照，会向前视偏差；需要 point-in-time
  来源，另行 change。
- 不做换手/再平衡预算：需要把“上一期持仓”引入选择语义，属独立切片。
- 不做真实执行、不 promote、不改评分/方向/NAV 语义、不开前瞻窗口。
