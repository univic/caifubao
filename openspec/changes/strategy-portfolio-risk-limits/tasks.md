# Strategy Portfolio Risk Limits Tasks

> 基线：develop。范围：`datahub/app/lib/strategy_engine/{config,selection,runner}.py`
> + `datahub/app/jobs/strategy_runner.py` 的数据装配 + 新测试文件 + 本 change。
> roadmap 1.3（组合/风控规则落 paper 路径）。
> 不写库、不下单、不 promote、不开前瞻窗口、不改评分/方向/NAV 语义。

## 1. Spec + 失败测试

- [x] 1.1 本 change spec 经 spec-guardian 审阅：首轮 **GATE_BLOCKED**（无 P1；4 项 P2 +
      4 项 P3）。已全部处理：
      - P2 分类法错误 → 改为「一级行业（baostock CSRC 证监会 L1，字段名 `sw` 为历史遗留）」
      - P2 前视 → 新增 as-of 规则：只有 `assigned_at <= signal date` 的分类才采用，
        否则视为不可解析归 `UNKNOWN`（实现见 `_assigned_on_or_before` / `_query_industry_map`）
      - P2 静默绕过 → 新增 requirement：有生效的资格约束（排除开关或流动性下限）时
        必须提供 universe 标记表，否则 fail closed（实现见 `_has_active_eligibility_constraint`）
      - P2 语义二义 → 明确 `None`（无映射）fail closed、空/部分映射归 `UNKNOWN`，各配 scenario
      - P3 行业上限不得丢标的、合并双上限公式、typo、proposal 对窗口语义的措辞（配置变化
        只是让新 run 归类 REPLAY，不会自动关窗）
- [x] 1.2 失败测试先行（红→绿）：单票上限（等权 ≤ 上限不变 / 超限缩到上限、余额留现金、
      不丢标的）；行业上限（超限缩小、缺分类归 UNKNOWN 同样受限、未配置不改变权重、
      双上限取更严者）；流动性下限（低于阈值剔除、成交额缺失剔除、阈值为 0 不约束）；
      分类 as-of（晚于 signal date 不采用、`assigned_at` 缺失不采用、naive/aware 混用不报错）；
      资格约束不被绕过（无标记表且约束生效 → fail closed；无约束 → 允许；无预测 → 仍 SKIPPED）；
      风控参数进入 config_hash；默认不含 `max_industry_pct`
- [x] 1.3 测试放在**新文件** `test_strategy_risk_limits.py`，避免与在审的
      `strategy-target-export`(#220) 在同一测试文件尾部冲突

## 1b. 流程说明（spec-guardian 指出）

- [x] 1b.1 spec-guardian 指出「实现先于 gate 清除」不符合 RULES P3 顺序。实际情况是
      spec 评审与实现**并行**启动；结果：gate 的全部 P2/P3 都在**开 PR 之前**修复并复审，
      未带病进入评审流程。后续切片改为**等 gate 结论后再动代码**

## 2. 实现

- [x] 2.1 `config.py`：新增 `max_industry_pct` 到已知键与范围校验（`(0, 1]`）；
      **不写入 `DEFAULT_STRATEGY_CONFIG`**，保证默认配置哈希可复现
- [x] 2.2 `selection.py`：`_bounded_weight` 一次性缩放到
      `min(等权, 单票上限, 行业上限/最大桶计数)`，余额留现金、不丢标的、不放大小权重；
      `industry_by_code` 为 `None` 且配置了行业上限 → fail closed，缺失/不可解析归 `UNKNOWN`
- [x] 2.3 `runner.py`：`eligible_codes_from_flags` 增加流动性下限判定（`trade_amount`
      低于阈值或缺失即不可选）；`assemble_daily_plan` 透传行业映射，并在
      `_has_active_eligibility_constraint` 为真且未提供 universe 标记表时 fail closed
- [x] 2.4 `jobs/strategy_runner.py`：`_query_flags` 增补当日成交额；`_query_industry_map`
      **按 `assigned_at <= signal date` 做 as-of 解析**（`_assigned_on_or_before`，按日历日
      比较以兼容 naive/aware 混用），不可证明归 `UNKNOWN`（未触碰 export 子命令区域）
- [x] 2.5 focused/full datahub pytest 全绿（611）、Ruff、OpenSpec strict（16/16）

## 3. Gates

- [x] 3.1 spec-guardian **GATE_OK**（`12f893d` 后按 P3 收口注释与勾选）；qa-reviewer 审阅。
      contract-reviewer 不触发：不改 API/鉴权/freshness/OpenClaw
- [ ] 3.2 branch-conflict vs develop；Draft PR CI green
- [ ] 3.3 agent-progress 回填；本切片不授权合并、写库、promote 或真实下单
