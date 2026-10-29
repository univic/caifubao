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

**当前状态（2026-10-29）**：W0-A、W1.1–W1.5、W2、W3、W4、W5 上半均已完成；剩余任务集中在
W5 的数据侧（需数据授权）与运维侧（需写库/开窗授权）。逐项差异见
`docs/architecture/strategy-program.md` §8。

**P1 延后的复核触发点**：当出现第一个非规则 provider 的真实需求（例如 P4 要接 ML/LLM，或 P3 需要 ML 信号）时，立即提前启动 P1，不再等到 W4。

## 1. P0 — 策略声明完备性（`strategy-spec-completeness`）

- [x] P0.1 定义 consumption manifest 的格式与校验语义（`shared/caifubao_contracts/strategy_spec_consumers.json`，纯 stdlib 校验器；golden 绑定见 P0.3 后续）
- [x] P0.2 盘点 v1 全部允许字段的实际消费者：38 个字段，29 已消费、9 未消费（`cadence_days` + `risk.*` 6 项 + **`sizing.weighting`**）
- [x] P0.3 CI 校验脚本 `scripts/check_strategy_consumers.py` + `Strategy Contract Check` job：双向一致、消费者文件必须存在且提及该字段、
      **每个字段必须绑定一个存在且引用它的 golden 测试**（按 AST 解析，改名/删除即失败）、未消费必须指明 owner
- [x] **v2 读 manifest**：`validate_strategy_spec` 对 v2 校验「每个出现的字段都在 manifest 中且被 v2 消费者读取」，
      manifest 从 CI 注释变成**加载即生效的契约**（`check_manifest_coverage`）
- [x] **配置族收窄**：`asset_allocation` 不得声明排名语义（`selection.mode/lower/upper/portfolio_size`）、
      `sizing`、`constraints`；已交付声明同步收窄（`spec_hash=6c31978e22c1b724`）
- [ ] P0.4 定义 `spec_version: "2"` 字段集（identity / data 要求 / signal 引用 / family / accounting / benchmark / evidence.kill_criteria）
      —— **已落地 `family` + `selection.exit`**（v2 声明首次可被校验）；
      identity / data / accounting / benchmark / kill_criteria 待做，且必须先有消费者才允许进 v2
- [x] P0.5 校验器实现 v2 严格校验，并按 `spec_version` 分派执行语义（**不得按字段存在与否分派**）
      —— `strategy_spec.py` 接受 `1|2`；v1 拒绝 v2-only 字段（保持旧严格度与旧 hash），
      v2 必须声明 `family` + `selection.exit` 并委托 `strategy_exit_policy` 校验；
      manifest 增加 `consumed_by_versions` 维度，v2 只允许被 v2 消费者读取的字段
      —— 分派原语已落地 `shared/caifubao_contracts/strategy_spec_semantics.py`
      （`V1_CONSUMED` 冻结 + `field_is_consumed`）；消费者接线仍待做
- [ ] P0.6 策略族校验：`rank_selection` 必填排名出场语义，`asset_allocation` 必填目标权重/带宽/被真实消费的再平衡节奏；两族均拒绝不适用的语义
      —— **出场维度已落地** `shared/caifubao_contracts/strategy_exit_policy.py`（两族均强制 `stop_loss_pct`；排名族必填四项；配置族拒绝排名出场；
      `min_hold > max_hold` 拒绝）；配置族的权重/带宽/cadence 仍待做
- [x] P0.7 `rank_selection` 消费显式出场语义（rank_below / stop_loss_pct / max_hold_sessions / on_ineligible）并记录卖出原因
      —— `plan_target_holdings` 新增 `mandatory_exits` / `exit_rank_override`（默认保持 v1 行为），
      `plan_from_state` 按策略计算强制退出；原因 `sold_stop_loss` / `sold_max_hold` 优先于带内保留与失资格
- [x] P0.8 `asset_allocation` 消费目标权重、再平衡带宽、cadence 与**单资产止损**，带宽内不交易回目标权重
      —— `shared/caifubao_contracts/strategy_allocation.py`：`validate_allocation`（权重和 ≤ 1、带宽上界）、
      `should_rebalance`（**cadence 首次真正被读取**）、`plan_allocation`（带内不交易、止损与移出资产集每期评估）
- [ ] P0.9 结构门禁：最小持仓数、年换手预算、集中簿必须有止损、回撤预算，并在开窗前评估
- [x] P0.10 把风险预算接进 v2 链路（v1 保持不消费）
      —— `shared/caifubao_contracts/strategy_risk_budget.py`：`check_budget` 消费全部 7 个 `risk.*` 字段
      （现金下限/回撤/regime 是 stop，持仓数/总暴露/单票上限是 scale，换手超限直接拒绝）；
      `sizing.weighting` 由 `selection` 消费（equal / score），**manifest 未消费字段归零**
      - **评审修正**：此前 `check_budget` 无生产调用方（manifest 声称已消费，实为 false positive）。
      现已接入 `jobs/allocation_runner.build_plan`：换手/持仓数/总暴露/单票上限→缩小或拒绝，
      现金下限→停止；**无法度量的限制（回撤/regime）显式拒绝**而非忽略。同时修两个 bug：
      `regime` 默认值不可用、NaN 回撤绕过止损（fail-open）
- [x] P0.11 定义 `DecisionState`（持仓/成本价/hold_count/峰值/现金/未完成订单）并记录 `decision_state_hash`
      —— `shared/caifubao_contracts/strategy_decision_state.py`（校验 + 内容哈希 + `blocked_slots`）；
      在 run 文档中落字段随 v2 runner 接线（P0.16 剩余）
- [x] P0.12 连续多日回归基线：`datahub/app/test/fixtures/strategy_v1_golden.json`（2026-08-03..08-07 生产分数，5 个连续交易日）+ `test_strategy_v1_regression.py` 断言目标组合与调仓 diff 逐字段不变；订单/账本投影部分随 P0.16 补
- [ ] P0.13 跨模式 conformance 向量改为比对 `DecisionState` 链，分叉必须归因到具体成交
- [ ] P0.14 回归测试钉住已认证窗口 `01c196b8…` 的 `spec_hash` 与 `legacy_config_hash`，并断言 v1 语义未变
- [ ] P0.15 运行记录新增 `resolved_inputs`（当日快照 id + 解析后的模型工件 digest）与 `run_id`
- [ ] P0.16 计划链改由账本/账户投影驱动（当前 research 读上一版 `target_holdings`），不可卖持仓占用名额，blocked 退出记录 reason 并进入下一期 `DecisionState`
      —— **契约与规划器已落地**：`selection.plan_target_holdings` 新增 `reserved_slots` / `sizing_basis`（默认保持 v1 行为），
      `strategy_engine/stateful_planner.plan_from_state` 从状态选股并按 cap 定权；
      **排名族 runner 已接线**（`strategy_runner._build_rank_state`：声明里的 `family`/`selection.exit` 穿到 `assemble_daily_plan`，
      决策日收盘作为 marks，`reference_prices` / `decision_state`/`decision_state_hash` 落库；
      调度入口 `strategy_forward_daily.run_forward_plan` 也传声明，否则自动路径只拿到 hash 会必然失败；
      已有持仓沿用开仓价，只有新开仓记当日收盘——每日刷新会让止损退化成单日跌幅而永不触发）；
      剩余：计划链仍从上一版 `target_holdings` 起步而非账本/账户投影，`blocked_exits` 在排名族恒空
- [ ] P0.17 `openspec/README.md` 能力索引与本文档同步更新

## 2. P1 — 统一信号源（`strategy-signal-providers`）

- [x] P1.1 定义 provider 结果 schema（date / stock / score / percentile / rationale）与越权字段拒绝
      —— `shared/caifubao_contracts/strategy_provider.py`：`side/quantity/target_weight/weight/order_id/cash/action` 一律拒绝
- [x] P1.2 `RuleScoreProvider` 适配现有评分引擎，产出与 v1 逐位一致的结果
      —— `strategy_engine/providers.py`；缺失 percentile 时按横截面排名补算，与生产口径一致
- [x] P1.3 `MLModelProvider` 契约：工件 digest、特征集版本、标签口径、有效区间、滚动 refit 的逐 session 解析
      —— 身份层已强制 artifact_digest + feature_set_version；逐 session 解析随 W5 接
- [x] P1.4 `LLMProvider` 契约：prompt digest、模型标识、请求/响应录制存储
      —— `RecordedProvider`：prompt digest 不符即拒绝；无录制即拒绝
- [x] P1.5 记录-重放：第二次求值零外部调用，且结果与录制一致
      —— `assert_replay_is_free` 守卫；重放路径没有任何发起外部调用的能力
- [ ] P1.6 无录制结果不得归类 FORWARD 的证据门禁
- [ ] P1.7 工件注册表 append-only，历史 session 重放解析到同一工件
- [x] P1.8 provider 成本记账接入 NAV 与净收益口径（与佣金/税/过户费/滑点并列）
      —— `provider_cost` + `net_return`：先扣推理成本再报净收益
- [ ] P1.9 provider 种类不得影响 selection / sizing / risk 的回归测试

## 3. P2 — 研究流水线（`strategy-research-pipeline`）

- [x] P2.1 预注册 schema（假设/假设族/数据/程序/成功判据/失效判据/试验上限/确认集切片）与账本存储
      —— `shared/caifubao_contracts/strategy_research.py` + `strategy_engine/research_pipeline.py`（注入式 store，内存实现用于测试；
      `research_store.MongoResearchStore` 已落地并单测（append-only、内容寻址、族级计数）；
      已在 research 库实测写权限：insert / read-back / delete / drop 均通过且零残留
      （该凭据**不能建临时库**，故冒烟在授权库内自清理完成）
- [x] P2.2 试验计数按**假设族**累计，新 registration 继承族内累计数；registration 级计数单独保留
      —— 换 registration 重置预算已被测试拦下（`test_trials_accumulate_per_family_across_registrations`）
- [ ] P2.3 接入既有 Bonferroni/多重检验工具，输出按族计数调整后的阈值
- [x] P2.4 确认集数据隔离：探索期不可读确认区间的价格与结果，读取即标记污染
      —— `assert_exploration_range` 越界即 `ResearchError`
- [x] P2.5 cutoff = 确认起点 − 标签期限 − 1 − embargo；重叠即拒绝，负 embargo 直接拒绝
      —— **评审修正（P2）**：`register` 此前允许省略交易日历，退回"数工作日"的近似，
      而节假日会把不交易的日子当成 session → 2026-08-03 这类 cutoff 在业务日口径下合规、
      在真实日历下只有 16 个 session（标签重叠）。现在 `register` **强制要求日历**，
      并把 session 精确校验的证明（`required_sessions`/`latest_allowed_cutoff`）落库；
      没有该证明的 registration 不得支撑结论或晋级 verdict
- [x] P2.6 确认次数上限按族强制，超限拒绝并留痕
      —— **评审修正（P2）**：此前 `verdict` 只要求"本 registration 至少有一次确认计数"，
      而 `record_confirmation()` 不携带任何证据，等于"有人说过确认过"就能晋级。
      现在确认记录必须写明 `spec_hash`/`dataset_id`/`p_value`/当时的调整阈值/`passed`，
      且 `passed` 必须与自身数字自洽；结论要求同候选的确认记录存在，
      晋级要求该记录**通过**且阈值未因后续试验变松（否则要求重跑确认）
- [ ] P2.7 六阶段 runner：registration → feasibility → exploration → confirmation → structural → forward → verdict
- [x] P2.8 不可变 `verdict.json`：引用 registration、假设族、spec hash、`dataset_id`、代码 SHA、族级/registration 级试验数、确认次数、cutoff 与标签期限
      —— 内容寻址（`verdict_hash`），篡改即换地址
- [x] P2.9 结构与容量门禁：分散度/换手/回撤/止损存在性/资金×10 容量测试
      —— `strategy_structure.py`：`check_structure`（四项全查，逐项 finding）与 `evaluate_capacity`（保留比例下限）；
      **晋级 verdict 必须携带两项 passed**，否则 `ResearchError`——门禁不是建议
- [x] P2.10 探索阶段禁止产出结论与晋级候选的强制校验
      —— 契约层（stage/outcome 矩阵）与 runner 层双重拒绝
- [x] P2.11 用一个小样本 idea 端到端跑通并留下 verdict 样例
      —— `test_strategy_research_end_to_end.py`：W0-A 的换手重算登记为 idea、累计 12 次试验、
      过结构门禁、产出 **exploratory** verdict（33 个会话不足以定收益，拒绝晋级）

## 4. P3 — 稳健半仓（`stable-multi-asset-sleeve`）

- [ ] P3.0 在真实回放口径重算宽度/换手/回撤与多资产基准数字后，复核决策 D1（结论不变则更新理由，约束消失则重新评估载体）
- [ ] P3.1 声明 allocation 族：预先固定资产集、目标权重、再平衡带宽、被真实消费的 cadence、**单资产止损**、波动目标 10–12%、回撤预算 15–20%、年换手 ≤ 4、基准
      —— 资产集/权重/带宽/cadence/单资产止损/**accounting/benchmark/kill_criteria** 均已可声明并消费；
      波动目标与回撤预算仍待（需要一个真实消费它们的风险预算执行器）
- [ ] P3.2 数据准备：宽基 ETF + 国债 + 商品类 ETF 的行情与复权完整性与 PIT 校验
- [ ] P3.3 回测：波动目标 + 带区间再平衡，费用按 ETF 口径（免印花税）
      —— **带宽 × cadence 已实测**（`datahub/scripts/etf_sleeve_band_scan.py`，3,220 个共同交易日）：
      年化摩擦全部在 0.009%–0.033%（ETF 往返 0.252%，免印花税），因此带宽应按跟踪精度选而非省成本；
      证据建议 band 0.02 / cadence 5（摩擦 0.033%、平均偏离 1.27%）。记录见
      `docs/autoresearch/runs/etf-sleeve/w3-etf-band-cadence-scan-2026-10-29.md`。
      收益与波动目标执行器仍待做
      - **单资产止损已实测**（`etf_sleeve_stop_scan.py`）：5%–10% 的止损触发后 20 日均**上涨 1.8%–2.4%**、
      60 日 +4.2%–7.4%，同期只避开 3.7%–4.8% 回撤 → 紧止损**毁价值**；证据支持 0.20–0.25 作为
      灾难刹车（13 年触发 0–2 次，n 太小不足以称已验证）。记录见
      `docs/autoresearch/runs/etf-sleeve/w3-etf-stop-scan-2026-10-29.md`
- [ ] P3.4 组合级降险规则实现（触发/削减步长/最大削减/恢复条件）
- [x] P3.5 失效判据评估器：滚动超额 vs 基准 + 最大回撤，按复核节奏评估
      —— `strategy_benchmark.evaluate_kill_criteria`：历史不足即 `insufficient_evidence`，越界即 `breach` + 退役建议
- [ ] P3.6 资产事后替换检测：替换资产即新声明与新证据身份
- [ ] P3.7 不可负担宽度校验：拒绝依赖小资金买不到的宽度的单股信号
- [ ] P3.8 拒绝配置族中的排名出场字段（不适用语义必须报错，不得静默忽略）
      —— 已在 v2 校验实现（`allocation` 仅配置族可用，排名族拒绝）
- [x] **窗口键方案 C**：为 allocation 族另建独立窗口机制（`strategy-allocation-window`）
      —— `AllocationForwardWindow`（独立集合，按 `strategy_id` 键，不占用 `model_version`）+ `allocation_window.py`
      （`validate_certification` / `plan_certification` / `allocation_forward_progress`，19 个测试）；
      **开窗仍被拒**：`has_plan_path=False` 时拒绝——没有 runner 就开窗等于烧掉 120 日时钟
- [x] **allocation runner**：`jobs/allocation_runner.py`（`build_plan` / `run`）+ `AllocationPlan` 模型
      —— 声明已入库 `datahub/research/strategy/etf-sleeve-stable-v1.json`（band 0.02 / cadence 5 / stop 0.20 /
      资金 50% / 退役复核 60 日 / 回撤 20%），`spec_hash=f4f29aa7c816dbce`；
      用真实行情 dry-run 通过（2026-10-29，四资产 open，cash 0）
- [ ] **开窗前置（未完成）**：runner 必须进入运行镜像——当前 pod 里没有该模块，调度器无法产出计划；
      在此之前认证窗口会立刻开始积累缺口
- [ ] P3.9 `paper readiness` 无 FAIL 后由人工授权认证前瞻窗口

## 5. P4 — 激进袖研究（`aggressive-sleeve-research`）

- [ ] P4.1 在真实回放口径下重算历史数字（持有期 × 出口带 × 宽度），把现有报告数值替换为待验证基线
      —— **换手与摩擦部分已完成**（W0-A，2026-10-29）：见
      `docs/autoresearch/runs/small-book/w0a-book-churn-recompute-2026-10-29.md`；
      旧「年化摩擦 42%」实测为 N=20 的 11.4%–16.9%，「窄簿最贵」被否定（中宽度 50–100 最高）。
      仍待重算：持有期 × 收益、各宽度收益/回撤、多资产基准数字。
- [x] P4.2 声明激进袖资金上限、独立会计身份与独立证据身份
      —— `strategy_sleeve.py`：`validate_sleeves`（份额和 ≤ 1、身份唯一）、`allocate_sleeves`、
      `apply_losses`（单个袖子的亏损只能吃自己的份额，超出记 shortfall 不外溢）
- [x] P4.3 期限维度显式化：声明即评估，禁止跨期限借用结论
      —— `validate_horizon_claim`：声明期限与证据期限不一致即拒绝（v2 声明层强制）
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
- [ ] G.4 **跟进**：上游恢复发布 `posthog-node@5.60.0` 后，去掉 `.github/workflows/ci.yml`
      中 `npm install -g --omit=optional @fission-ai/openspec@1.1.1` 的 `--omit=optional`
      （操作者决定随本 PR 一并合入，触发 qa-reviewer；该 flag 只是上游 404 的临时绕过）
