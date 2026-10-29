# Strategy Program 2026 Tasks

> 契约先行：本 change 只定义契约与分阶段任务，实现按 P0 → P1 → P2 → P3/P4 顺序另开 PR。
> 每阶段独立验收；P0 未合入前不启动 P1。

## 1. P0 — 策略声明完备性（`strategy-spec-completeness`）

- [ ] P0.1 定义 consumption manifest 的格式与校验语义（字段路径 → 消费模块 + golden 测试）
- [ ] P0.2 盘点 v1 全部允许字段的实际消费者，产出 `consumers.yaml` 初版与已知未消费清单
- [ ] P0.3 CI 校验脚本：manifest ↔ 校验器允许字段集合双向一致，缺项即失败
- [ ] P0.4 定义 `spec_version: "2"` 字段集（identity / data / signal / selection.exit / accounting / benchmark / evidence.kill_criteria）
- [ ] P0.5 校验器实现 v2 严格校验与版本分支：v1 只读可重放、不得开新窗口
- [ ] P0.6 `selection` 消费显式出场语义（rank_below / stop_loss_pct / max_hold_sessions / on_ineligible）并记录卖出原因
- [ ] P0.7 结构门禁：最小持仓数、年换手预算、集中簿必须有止损、回撤预算，并在开窗前评估
- [ ] P0.8 把 `evaluate_risk` 接进每日 paper 链路（当前只定义未消费）
- [ ] P0.9 回归测试钉住已认证窗口 `01c196b8…` 的 `spec_hash` 与 `legacy_config_hash`
- [ ] P0.10 跨模式 golden TargetBook 向量：同一 spec + 快照在 research 与 trading 端点产出逐字段相同目标组合
- [ ] P0.11 `openspec/README.md` 能力索引与本文档同步更新

## 2. P1 — 统一信号源（`strategy-signal-providers`）

- [ ] P1.1 定义 provider 结果 schema（date / stock / score / percentile / rationale）与越权字段拒绝
- [ ] P1.2 `RuleScoreProvider` 适配现有评分引擎，产出与 v1 逐位一致的结果
- [ ] P1.3 `MLModelProvider` 契约：工件 digest、特征集版本、标签口径、有效区间
- [ ] P1.4 `LLMProvider` 契约：prompt digest、模型标识、请求/响应录制存储
- [ ] P1.5 记录-重放：第二次求值零外部调用，且结果与录制一致
- [ ] P1.6 无录制结果不得归类 FORWARD 的证据门禁
- [ ] P1.7 provider 成本记账接入 NAV 与净收益口径（与佣金/税/过户费/滑点并列）
- [ ] P1.8 provider 种类不得影响 selection / sizing / risk 的回归测试

## 3. P2 — 研究流水线（`strategy-research-pipeline`）

- [ ] P2.1 预注册 schema（假设/数据/程序/成功判据/失效判据/试验上限）与账本存储
- [ ] P2.2 试验计数器：每评估一个配置 +1，确认阶段门槛按计数调整
- [ ] P2.3 接入既有 Bonferroni/多重检验工具，输出调整后阈值
- [ ] P2.4 六阶段 runner：registration → feasibility → exploration → confirmation → structural → forward → verdict
- [ ] P2.5 不可变 `verdict.json`：引用 registration、spec hash、快照、代码 SHA、试验数、阶段、结论
- [ ] P2.6 结构与容量门禁：分散度/换手/回撤/止损存在性/资金×10 容量测试
- [ ] P2.7 探索阶段禁止产出结论与晋级候选的强制校验
- [ ] P2.8 用一个小样本 idea 端到端跑通并留下 verdict 样例

## 4. P3 — 稳健半仓（`stable-multi-asset-sleeve`）

- [ ] P3.1 声明稳健袖：预先固定资产集、波动目标 10–12%、回撤预算 15–20%、年换手 ≤ 4、基准
- [ ] P3.2 数据准备：宽基 ETF + 国债 + 商品类 ETF 的行情与复权完整性与 PIT 校验
- [ ] P3.3 回测：波动目标 + 带区间季度再平衡，费用按 ETF 口径（免印花税）
- [ ] P3.4 组合级降险规则实现（触发/削减步长/最大削减/恢复条件）
- [ ] P3.5 失效判据评估器：滚动超额 vs 基准 + 最大回撤，按复核节奏评估
- [ ] P3.6 资产事后替换检测：替换资产即新声明与新证据身份
- [ ] P3.7 不可负担宽度校验：拒绝依赖小资金买不到的宽度的单股信号
- [ ] P3.8 `paper readiness` 无 FAIL 后由人工授权认证前瞻窗口

## 5. P4 — 激进袖研究（`aggressive-sleeve-research`）

- [ ] P4.1 声明激进袖资金上限、独立会计身份与独立证据身份
- [ ] P4.2 60 日持有期框架：期限声明、评估与证据绑定，禁止跨期限借用结论
- [ ] P4.3 事件数据源选型（业绩预告 / 龙虎榜 / 公告），评估可得性、PIT 性与成本
- [ ] P4.4 事件数据按 datahub `fetch → normalize → validate → persist → publish` 落地
- [ ] P4.5 事件信号的 PIT 证据：发布时间戳与可交易性（停牌/一字板）处理
- [ ] P4.6 大模型二次筛选：候选池上界、每会话调用上界、推理成本记录与净额扣减
- [ ] P4.7 风险隔离：激进袖亏损不得侵蚀稳健袖分配，越界即报告
- [ ] P4.8 首轮事件回测并产出 verdict（允许结论为“不成立”）

## 6. Gates

- [ ] G.1 `spec-guardian` 审阅本 change 的 requirements/scenarios 与合规口径
- [ ] G.2 每阶段独立 PR：按 `RULES.md` 触发对应 reviewer + 分支冲突检查 + Draft PR CI green
- [ ] G.3 确认本 change 不触碰已认证窗口 `01c196b8…` 的任何字节
