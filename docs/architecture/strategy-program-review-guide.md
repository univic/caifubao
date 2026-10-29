# Strategy Program 2026 — 评审指南

> 用途：这个 PR 有 40 个提交、60+ 个文件，按提交读会失去主线。本指南把改动
> **按关注点重排**，每个关注点给：它解决什么问题、看哪几个文件、哪几个测试钉住它、
> 以及**这里最容易错的地方**（前两轮评审实际抓到的缺陷类型）。
>
> 规范性来源仍是 `openspec/changes/strategy-program-2026/`；本文件只是阅读索引。

## 0. 先看三句话

1. 这是一个**契约**改动，大部分文件是纯函数与测试，没有运行时行为变更——除
   `allocation_runner`（新作业）与 `runner.py`（v2 分支）两处。
2. **v1 的行为被钉死**：`test_strategy_v1_regression.py` 用五个连续生产会话复现，
   qa-reviewer 另做过 400 次随机差分对比，0 处不一致。
3. 已认证窗口 `01c196b8…` 的哈希由测试直接复现
   （`test_the_live_window_hash_is_still_reproducible`）。

## 1. 关注点 A — 声明与消费者（本 PR 的核心主张）

**要解决的问题**：字段被校验、入 hash、写进文档，但**没有任何代码读它**。
`rebalance.cadence_days` 是原型，后来发现 `risk.*` 七个与 `sizing.weighting` 同病。

| 文件 | 作用 |
|:---|:---|
| `shared/caifubao_contracts/strategy_spec_consumers.json` | 消费者清单：每个字段的名字 → 谁读、哪个版本读、哪个测试钉住 |
| `shared/caifubao_contracts/strategy_spec_consumers.py` | 校验器：清单 ↔ 允许字段双向一致、消费者文件必须存在且提及该字段、**golden 测试必须存在且引用该字段**（AST 解析，改名/删除即失败） |
| `scripts/check_strategy_consumers.py` + `.github/workflows/ci.yml` | 门禁入口与 CI job |
| `shared/caifubao_contracts/strategy_spec.py::check_manifest_coverage` | **v2 声明在校验时就要过清单**，不再只是 CI 注释 |

**钉住它的测试**：`test_strategy_spec_consumers.py`、`test_strategy_spec_semantics.py`

**最容易错的地方**（已发生两次）：
- 清单是**数据文件**，必须被打包进 wheel。`shared/pyproject.toml` 的
  `[tool.setuptools.package-data]` 就是修这个；缺了它，部署后**所有 v2 校验都会崩**。
- 契约文本曾要求「golden 测试 pins its effect」，而实现只做文本匹配——现在 spec
  已改成如实描述（存在 + 引用），不再过度声明。

## 2. 关注点 B — 版本分派与 v1 冻结

**要解决的问题**：给 v1 接线新消费者会在 **hash 完全不变**的情况下改变已认证窗口的行为。

| 文件 | 作用 |
|:---|:---|
| `shared/caifubao_contracts/strategy_spec_semantics.py` | `V1_CONSUMED` 冻结在代码里；`field_is_consumed("1", …)` 恒为 False（即使清单声称已消费） |
| `shared/caifubao_contracts/strategy_spec.py` | 接受 `1\|2`；**v1 拒绝 v2-only 字段**（不吸收进 hash）；v2 必填 family/exit/accounting/benchmark/kill_criteria |
| `datahub/app/test/fixtures/strategy_v1_golden.json` | 五会话生产分数基线 |
| `test_strategy_v1_regression.py` | 目标组合 + 调仓 diff 逐字段；并复现 `01c196b871ea` |

**最容易错的地方**：只钉 hash 不钉行为。`test_cadence_days_does_not_skip_sessions`
曾经**永远不会失败**（`plan_target_holdings` 没有 cadence 入参）——这类"看起来在测"的
断言要看它是否真的能翻红。

## 3. 关注点 C — 出场契约与规划

**要解决的问题**：此前只有"排名跌出带"一种退出；止损与最长持有根本不存在，而打分的
逻辑是"越跌分越高"。

| 文件 | 作用 |
|:---|:---|
| `shared/caifubao_contracts/strategy_exit_policy.py` | 两族必填 `stop_loss_pct`；排名族另必填 rank_below / max_hold / on_ineligible；配置族**拒绝**这些；`min_hold > max_hold` 拒绝 |
| `datahub/app/lib/strategy_engine/selection.py` | `mandatory_exits` 优先级高于出口带与最短持有；被强制退出的名字**不会在补空缺时被买回** |
| `shared/caifubao_contracts/strategy_allocation.py` | 配置族语义：带宽内不交易；**cadence 的唯一读点之一**；止损与移出资产集每期评估 |
| `datahub/app/lib/strategy_engine/stateful_planner.py` | `plan_from_state`：从账户状态出计划 |
| `datahub/app/lib/strategy_engine/rank_state.py` | 排名族的账户状态构建器（已由 `strategy_runner` 接线） |

**最容易错的地方**：
- 止损用**记录成本价**，不是当日价；没有 mark 时**不臆造**止损（当日无收盘→该期延后，不拒绝整轮）。
- 「确认」是**证据**不是计数：记录必须带 `spec_hash`/`dataset_id`/`p_value`/当时阈值/`passed`，
  `passed` 还要与自身数字自洽；晋级要求该确认**通过**且阈值没被后续试验放松。
- 注册必须带**交易日历**：数工作日会把节假日当 session，cutoff 在纸面上合规、实际标签重叠。
- 成本价只能来自**上一轮落库的价格**；缺失时拒绝构建状态，因为"用当日价兜底"会让止损永远不触发。
- 更隐蔽的一半（评审才发现）：**已有持仓必须沿用开仓价，只有新开仓才记当日收盘**。
  早期实现给每个持仓每天刷新成本价 → 止损退化成"单日跌幅"，而主板 ±10% 限制下
  15% 的单日止损**永远不可能触发**：声明了、落库了、但永久失效。
- v2 声明曾经**根本跑不起来**：`to_legacy_config` 把 `selection.exit` 原样投影进 legacy config，
  而 legacy 校验器拒绝未知 `selection` 键 → 每次 `run --spec` 在加载阶段就崩。投影必须丢出场契约
  （作业层从声明里读，不从 config 读），身份校验也随之改为**以声明为准**而非从投影反推。
- 强制退出必须**同时**排除于保留循环与补空缺循环（早期实现会先卖后买回来）。
- `min_hold_sessions` 曾可在 `rebalance` 与 `selection.exit` 同时声明，后者静默胜出 → 已拒绝。

## 4. 关注点 D — 研究流水线

**要解决的问题**：同一 idea 可从任一处发起、口径各异，且**没人记录挑出赢家前试了多少配置**。

| 文件 | 作用 |
|:---|:---|
| `shared/caifubao_contracts/strategy_research.py` | 预注册契约；`cutoff` 按**会话**而非自然日；有日历则精确 |
| `datahub/app/lib/strategy_engine/research_pipeline.py` | 阶段机、族级试验计数、确认上限、**结论/晋级需本注册对同一候选的确认证据**（不是计数）；注册必须带交易日历 |
| `datahub/app/lib/strategy_engine/research_store.py` | Mongo 账本（append-only、内容寻址） |
| `shared/caifubao_contracts/strategy_structure.py` | 结构与容量门禁；**晋级 verdict 必须携带两项 passed** |

**最容易错的地方**：标签隔离用自然日会泄漏约一周标签；确认计数按"族"会被兄弟注册借用。

## 5. 关注点 E — 信号源

| 文件 | 作用 |
|:---|:---|
| `shared/caifubao_contracts/strategy_provider.py` | 三类 provider 一个 schema；**拒绝 side/quantity/weight 等指令字段**；缺失 percentile 不默认 0 |
| `datahub/app/lib/strategy_engine/providers.py` | 规则适配器 + 录制重放；重放路径**没有发起外部调用的能力** |

**注意**：`RuleScoreProvider` 尚未被 `strategy_runner` 调用——runner 仍直接读 Mongo。
这是已知缺口，不是遗漏。

## 6. 关注点 F — 稳健袖与窗口

| 文件 | 作用 |
|:---|:---|
| `datahub/app/jobs/allocation_runner.py` | 每日计划：漂移**含现金**、cadence 从**上次实际调仓**算、执行日来自**交易所日历**、写入可注入、执行后拒绝重写 |
| `datahub/app/lib/strategy_engine/allocation_window.py` | 按 `strategy_id` 键的独立窗口；只关同策略前身；**没有计划路径时拒绝认证** |
| `datahub/research/strategy/etf-sleeve-stable-v1.json` | 冻结的声明（band 0.02 / cadence 5 / stop 0.20 / 50% 资金） |
| `datahub/scripts/etf_sleeve_{band,stop}_scan.py` | 参数是按证据选的，不是拍的 |

**最容易错的地方**（全部实际发生过）：执行日取自"下一条行情"→ 实盘永远为空；
漂移归一化漏掉现金 → 半仓被放大成满仓；cadence 从昨日计划日算 → 周频永不触发。

## 7. 关注点 G — 研究记录与文档

| 文件 | 说明 |
|:---|:---|
| `docs/autoresearch/runs/etf-sleeve/*-scan-2026-10-29.md` | 带宽与止损的实测；**只测换手/摩擦/漂移，不测收益** |
| `docs/architecture/strategy-program.md` §8 | 里程碑实际状态与六处偏差，含"哪些还没做" |

**阅读提示**：`§8` 是我自己写的差异说明，若与代码不符以代码为准。

## 8. 建议的评审顺序

1. 先看 `openspec/changes/strategy-program-2026/specs/` 的 6 份 delta spec（读 20 分钟，得到"应该做什么"）。
2. 再读关注点 A 的两个文件 + 其测试（判断门禁是否真的能拦住缺陷）。
3. 然后关注点 F 的 `allocation_runner.py`（唯一有真实运行时的新作业，前两轮 P1 全在这里）。
4. 其余按关注点抽样；纯函数模块的测试密度高，可只看测试名。

## 9. 已知未完成（不必再报）

| 缺口 | 原因 |
|:---|:---|
| 计划链仍读上一版 `target_holdings`，不读账本 | `plan_from_state` 只拿到"上一轮目标组合"构造的状态；真正不可卖持仓要等账本/账户投影（P0.16 剩余） |
| 确认记录里的 `p_value` 由调用方提供 | 管线没有求值引擎，只能保证**身份/当时阈值/自洽**可核对，不能复算这个 p 值；复算属于执行层的责任 |
| `blocked_exits` 在排名族恒为空 | 研究规划路径观察不到"成交被拒"，把它算作已成交是既有简化；配置族由 `allocation_runner` 承担 |
| 成本价是决策日收盘，不是成交价 | 计划链只规划目标权重，没有成交账本；止损因此是"相对决策日收盘"的近似，复盘时需带上这个口径 |
| 运行记录没有落"实际生效的出场契约" | 只能通过 `spec_hash` 找到声明的工件；`decision_state` 只含持仓/成本/现金 |
| `RuleScoreProvider` 未被 runner 使用 | 等第二个 provider 出现再切，避免过度抽象 |
| `sizing.weighting` 的 score 分支只有测试可达 | legacy 投影只接受 `equal`，作业层又只从投影读 `weighting` → 声明 `score` 会在投影阶段被拒；要到达需作业层直接从声明传 `weighting`，且尚无扫描结论支撑该口径 |
| 波动目标未可声明 | 需要真正按波动定仓的执行器，不能只加字段 |
