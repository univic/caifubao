# Strategy Program 2026 Tasks

> 契约先行：本 change 只定义契约与分阶段任务，实现按下方 §0 的顺序另开 PR。
> 每个切片独立验收。

## 0. 实施顺序（work order）

排序原则：**先取回决策所需的事实，再建契约，最后让资金结构使用契约。** 同一波次内可并行。

| 波次 | 内容 | 为什么在这个位置 |
|:---|:---|:---|
| **W0** | `P4.1` 真实回放口径重算 + `P4.4` 事件数据可行性 | 只读、不改契约、不阻塞；**三条决定依赖它**：D1 的理由、结构门禁阈值、P4 方向。事件数据是 P4 的长周期前置 |
| **W1** | P0 五个切片（见下） | 后续全部依赖。切片顺序不可乱：先立门禁与基线，再动行为，最后加字段 |
| **W2** | `P2.1–P2.11` 研究流水线 | 用 W0 的实际产物定义 verdict 字段，避免凭空设计；P3 的窗口认证需要它 |
| **W3** | `P3.0` → `P3.1–P3.9` 稳健袖 | 真实使用 allocation 家族，反过来检验 P0 的家族抽象 |
| **W4** | `P1.1–P1.9` 信号源抽象 | **延后**：P3 只用规则 provider，提前造抽象没有第二个实现来检验它 |
| **W5** | `P4.2–P4.9` 激进袖 | 依赖 P1/P2 与 W0 的数据结论 |

**W1 内部切片顺序**（每一片一个 PR）：

1. `P0.1–P0.3` consumption manifest + CI —— 零运行时风险，先消灭「未消费字段」这一类缺陷。
2. `P0.5`、`P0.12`、`P0.14` 版本分派语义 + **v1 连续多日回归基线** + hash 钉住 —— **必须早于任何新消费者**：基线一旦被改动就再也测不出来。
3. `P0.11`、`P0.13`、`P0.16` `DecisionState` + 账户投影计划链 + 占位与 sizing + blocked 原因 —— 修掉「卖不掉却被当成卖掉」的真实缺陷。
4. `P0.6–P0.8` 家族化 + 完整出场契约（排名族）与配置族契约（含单资产止损）。
5. `P0.4`、`P0.9`、`P0.10`、`P0.15`、`P0.17` v2 字段、结构门禁、风控接线（仅 v2）、身份分层与索引。

**结构门禁阈值（`P0.9`）等待 W0 的重算结果**再定，避免用失效数字设门槛；在此之前只落地「字段必填 + 形态校验」。

**P1 延后的复核触发点**：当出现第一个非规则 provider 的真实需求（例如 P4 要接 ML/LLM，或 P3 需要 ML 信号）时，立即提前启动 P1，不再等到 W4。

## 1. P0 — 策略声明完备性（`strategy-spec-completeness`）

- [x] P0.1 定义 consumption manifest 的格式与校验语义（`shared/caifubao_contracts/strategy_spec_consumers.json`，纯 stdlib 校验器；golden 绑定见 P0.3 后续）
- [x] P0.2 盘点 v1 全部允许字段的实际消费者：38 个字段，29 已消费、9 未消费（`cadence_days` + `risk.*` 6 项 + **`sizing.weighting`**）
- [x] P0.3 CI 校验脚本 `scripts/check_strategy_consumers.py` + `Strategy Contract Check` job：双向一致、消费者文件必须存在且提及该字段、未消费必须指明 owner（13 个回归测试）
- [ ] P0.4 定义 `spec_version: "2"` 字段集（identity / data 要求 / signal 引用 / family / accounting / benchmark / evidence.kill_criteria）
- [ ] P0.5 校验器实现 v2 严格校验，并按 `spec_version` 分派执行语义（**不得按字段存在与否分派**）
      —— 分派原语已落地 `shared/caifubao_contracts/strategy_spec_semantics.py`
      （`V1_CONSUMED` 冻结 + `field_is_consumed`）；消费者接线仍待做
- [ ] P0.6 策略族校验：`rank_selection` 必填排名出场语义，`asset_allocation` 必填目标权重/带宽/被真实消费的再平衡节奏；两族均拒绝不适用的语义
- [ ] P0.7 `rank_selection` 消费显式出场语义（rank_below / stop_loss_pct / max_hold_sessions / on_ineligible）并记录卖出原因
- [ ] P0.8 `asset_allocation` 消费目标权重、再平衡带宽、cadence 与**单资产止损**，带宽内不交易回目标权重
- [ ] P0.9 结构门禁：最小持仓数、年换手预算、集中簿必须有止损、回撤预算，并在开窗前评估
- [ ] P0.10 把 `evaluate_risk` 接进每日链路，**仅对 v2 声明生效**（v1 保持不消费）
- [x] P0.11 定义 `DecisionState`（持仓/成本价/hold_count/峰值/现金/未完成订单）并记录 `decision_state_hash`
      —— `shared/caifubao_contracts/strategy_decision_state.py`（校验 + 内容哈希 + `blocked_slots`）；
      在 run 文档中落字段随 v2 runner 接线（P0.16 剩余）
- [x] P0.12 连续多日回归基线：`datahub/app/test/fixtures/strategy_v1_golden.json`（2026-08-03..08-07 生产分数，5 个连续交易日）+ `test_strategy_v1_regression.py` 断言目标组合与调仓 diff 逐字段不变；订单/账本投影部分随 P0.16 补
- [ ] P0.13 跨模式 conformance 向量改为比对 `DecisionState` 链，分叉必须归因到具体成交
- [ ] P0.14 回归测试钉住已认证窗口 `01c196b8…` 的 `spec_hash` 与 `legacy_config_hash`，并断言 v1 语义未变
- [ ] P0.15 运行记录新增 `resolved_inputs`（当日快照 id + 解析后的模型工件 digest）与 `run_id`
- [ ] P0.16 计划链改由账本/账户投影驱动（当前 research 读上一版 `target_holdings`），不可卖持仓占用名额，blocked 退出记录 reason 并进入下一期 `DecisionState`
      —— **契约与规划器已落地**：`selection.plan_target_holdings` 新增 `reserved_slots` / `sizing_basis`（默认保持 v1 行为），
      `strategy_engine/stateful_planner.plan_from_state` 从状态选股并按 cap 定权；**runner 接线待 v2 可表达后做**
- [ ] P0.17 `openspec/README.md` 能力索引与本文档同步更新

## 2. P1 — 统一信号源（`strategy-signal-providers`）

- [ ] P1.1 定义 provider 结果 schema（date / stock / score / percentile / rationale）与越权字段拒绝
- [ ] P1.2 `RuleScoreProvider` 适配现有评分引擎，产出与 v1 逐位一致的结果
- [ ] P1.3 `MLModelProvider` 契约：工件 digest、特征集版本、标签口径、有效区间、滚动 refit 的逐 session 解析
- [ ] P1.4 `LLMProvider` 契约：prompt digest、模型标识、请求/响应录制存储
- [ ] P1.5 记录-重放：第二次求值零外部调用，且结果与录制一致
- [ ] P1.6 无录制结果不得归类 FORWARD 的证据门禁
- [ ] P1.7 工件注册表 append-only，历史 session 重放解析到同一工件
- [ ] P1.8 provider 成本记账接入 NAV 与净收益口径（与佣金/税/过户费/滑点并列）
- [ ] P1.9 provider 种类不得影响 selection / sizing / risk 的回归测试

## 3. P2 — 研究流水线（`strategy-research-pipeline`）

- [ ] P2.1 预注册 schema（假设/假设族/数据/程序/成功判据/失效判据/试验上限/确认集切片）与账本存储
- [ ] P2.2 试验计数按**假设族**累计，新 registration 继承族内累计数；registration 级计数单独保留
- [ ] P2.3 接入既有 Bonferroni/多重检验工具，输出按族计数调整后的阈值
- [ ] P2.4 确认集数据隔离：探索期不可读确认区间的价格与结果，读取即标记污染
- [ ] P2.5 cutoff = 确认起点 − 标签期限 − 1 − embargo；重叠即拒绝，负 embargo 直接拒绝
- [ ] P2.6 确认次数上限按族强制，超限拒绝并留痕
- [ ] P2.7 六阶段 runner：registration → feasibility → exploration → confirmation → structural → forward → verdict
- [ ] P2.8 不可变 `verdict.json`：引用 registration、假设族、spec hash、`dataset_id`、代码 SHA、族级/registration 级试验数、确认次数、cutoff 与标签期限
- [ ] P2.9 结构与容量门禁：分散度/换手/回撤/止损存在性/资金×10 容量测试
- [ ] P2.10 探索阶段禁止产出结论与晋级候选的强制校验
- [ ] P2.11 用一个小样本 idea 端到端跑通并留下 verdict 样例

## 4. P3 — 稳健半仓（`stable-multi-asset-sleeve`）

- [ ] P3.0 在真实回放口径重算宽度/换手/回撤与多资产基准数字后，复核决策 D1（结论不变则更新理由，约束消失则重新评估载体）
- [ ] P3.1 声明 allocation 族：预先固定资产集、目标权重、再平衡带宽、被真实消费的 cadence、**单资产止损**、波动目标 10–12%、回撤预算 15–20%、年换手 ≤ 4、基准
- [ ] P3.2 数据准备：宽基 ETF + 国债 + 商品类 ETF 的行情与复权完整性与 PIT 校验
- [ ] P3.3 回测：波动目标 + 带区间再平衡，费用按 ETF 口径（免印花税）
- [ ] P3.4 组合级降险规则实现（触发/削减步长/最大削减/恢复条件）
- [ ] P3.5 失效判据评估器：滚动超额 vs 基准 + 最大回撤，按复核节奏评估
- [ ] P3.6 资产事后替换检测：替换资产即新声明与新证据身份
- [ ] P3.7 不可负担宽度校验：拒绝依赖小资金买不到的宽度的单股信号
- [ ] P3.8 拒绝配置族中的排名出场字段（不适用语义必须报错，不得静默忽略）
- [ ] P3.9 `paper readiness` 无 FAIL 后由人工授权认证前瞻窗口

## 5. P4 — 激进袖研究（`aggressive-sleeve-research`）

- [ ] P4.1 在真实回放口径下重算历史数字（持有期 × 出口带 × 宽度），把现有报告数值替换为待验证基线
      —— **换手与摩擦部分已完成**（W0-A，2026-10-29）：见
      `docs/autoresearch/runs/small-book/w0a-book-churn-recompute-2026-10-29.md`；
      旧「年化摩擦 42%」实测为 N=20 的 11.4%–16.9%，「窄簿最贵」被否定（中宽度 50–100 最高）。
      仍待重算：持有期 × 收益、各宽度收益/回撤、多资产基准数字。
- [ ] P4.2 声明激进袖资金上限、独立会计身份与独立证据身份
- [ ] P4.3 期限维度显式化：声明即评估，禁止跨期限借用结论
- [ ] P4.4 事件数据源选型（业绩预告 / 龙虎榜 / 公告），评估可得性、PIT 性与成本
- [ ] P4.5 事件数据按 datahub `fetch → normalize → validate → persist → publish` 落地
- [ ] P4.6 事件信号的 PIT 证据：发布时间戳与可交易性（停牌/一字板）处理
- [ ] P4.7 大模型二次筛选：候选池上界、每会话调用上界、推理成本记录与净额扣减
- [ ] P4.8 风险隔离：激进袖亏损不得侵蚀稳健袖分配，越界即报告
- [ ] P4.9 首轮事件回测并产出 verdict（允许结论为“不成立”）

## 6. Gates

- [ ] G.1 `spec-guardian` 审阅本 change 的 requirements/scenarios 与合规口径
- [ ] G.2 每阶段独立 PR：按 `RULES.md` 触发对应 reviewer + 分支冲突检查 + Draft PR CI green
- [ ] G.3 确认本 change 不触碰已认证窗口 `01c196b8…` 的任何字节，且 v1 执行语义不变
