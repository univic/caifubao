# Production Capability Roadmap Tasks

> 合规口径：产品对外始终称「研究/学习/演示 MVP、非投资建议」；本文只规划**实盘能力**
> 的内部路线。本 change 不授权真实资金执行。

> **证据修正顺序：** P0 causal replay integrity 已随 #202 合并（`87a0ca2`）；再另行
> 设计不可变 forward capture/count（`NEXT`）。strategy-paper-runner 的历史 gate
> checkmarks 保留作历史记录；因果修正的实现和测试在此处不标记为已完成。

## 0. 证据与定版门槛（因果修正优先）

- [x] P0.1 **causal replay integrity — MERGED #202 (`87a0ca2`)**：完成 `paper_causal_v1` 可用分数
      消费、实际 UTC `decision_at`、下一个交易日 `execution_date`、完整 config hash
      隔离、开盘信息定量和 `evidence_kind=REPLAY` 标记；本切片产物不进入 120 日计数。
- [ ] NEXT.1 **immutable forward capture/count — NEXT**：在 P0 完成后由独立 change 设计
      不可变前瞻捕获和 120-session counter；不得把历史 replay、backfill、replacement、
      NAV recompute 或 job SUCCESS 当作前瞻证据。
- [ ] 0.1 task 4.4 纸面 ≥120 交易日 operator 窗口：`2026-06-10` 和 `2026-09-04` 均为
      历史/replay 日期，不是已认证的前瞻起点；在 NEXT 可用前不计数，也不按
      `strategy_daily` SUCCESS 数量计数。
- [ ] 0.2 每周纸面 NAV/超额/回撤/换手记入 autoresearch ledger（同 manual-experiments-ledger），
      并明确标注 `REPLAY`，不得写成前瞻证据
- [ ] 0.3 promote 检查清单：仅在 NEXT 的不可变前瞻 120 日窗口结束后，将 paper 结果
      与研究 walk-forward 预期对照；通过后再走 version bump + Spec Gate 的 promote
      流程（本 change 不授权 promote）
- [ ] 0.4 生产环境部署验证：datahub prod 配额（2 核/2Gi，私有 #60）dispatch 部署并跑一次
      单日 ranked 计时回填对比
- [ ] 0.5 MVP 收尾：capability-inventory.md 刷新到当前（H20/flip/strategy/perf 均已合入）；
      openspec 已完成 change 逐个 close/archive

## 1. 决策与治理地基

- [ ] 1.1 DecisionJournal：推荐 → 是否执行 → 成交 → 盈亏 全链持久化（capability P2 高项）
- [ ] 1.2 持久化审计日志（RequestAudit/操作审计链）+ service token 限流（capability P0 中项）
- [ ] 1.3 组合/风控规则引擎落 paper 路径：单票/行业/总仓上限、ST/退市/停牌/流动性过滤、
      换手与再平衡预算、可用预算校验；每日产出「目标持仓 + 调仓清单」供人工核对
- [ ] 1.4 模型治理：promote/rollback/停用流程与记录（config_hash 已有，补审批/审计记录）

## 2. 执行能力（默认关闭、人工确认优先）

- [ ] 2.1 目标组合导出（csv/报告，含权重/预算/理由）——最小实盘路径的第一步（人工下单）
- [ ] 2.2 券商 API 适配器以 default-off 标志接入（QMT/Ptrade 或等价通道）；执行路径
      paper/audit-first + 显式环境授权
- [ ] 2.3 对账：成交回报 vs 目标、资金/持仓对账、下单幂等去重
- [ ] 2.4 kill-switch：一键停止订单生成/清仓（默认关闭的应急路径）
- [ ] 2.5 告警体系：数据缺口、评分/分数分布漂移、回撤阈值、流水线失败、执行异常

## 3. 工程加固

- [ ] 3.1 确定性可重放：评分/验证/策略 NAV 全链 bit-identical 可审计重放
- [ ] 3.2 数据 SLA 与降级策略：数据源故障切换/延迟告警、行情正确性校验（复权/除权除息）
- [ ] 3.3 交易日历权威源对齐与节假日处理（策略/回测/评分共用同一日历语义）

## 4. Backlog（另行 change，不在本路线图落地）

- [ ] 4.1 新数据源：财报/基本面、融资融券、北向资金、实时/日内行情
- [ ] 4.2 市场状态分类器、行业轮动、大盘择时（依赖 4.1/1.3 组合层）

## 5. Gates

- [ ] 5.1 本 change：spec-guardian 审阅 requirements/scenarios 与合规口径
- [ ] 5.2 每阶段独立 PR：按 RULES 触发对应 reviewer + branch conflict + Draft PR CI green
- [ ] 5.3 每阶段合并后刷新 capability-inventory.md 与本文状态

## DSH 交接：下一独立切片（已分配，尚未执行）

用户于 2026-09-07 批准 #202 合并，并由 DSH 接续一部分工作。本次只分配
**1.3a 含费用的开盘买入预算**；1.3 其余风控、NEXT.1 前瞻捕获仍未完成。

- [ ] 1.3a DSH：从最新 develop 新建独立分支，先创建费用预算语义 OpenSpec change，
      完成 Spec Gate 后再改代码。以 #202 (`87a0ca2`) 为最低基线。
- [ ] 给定目标权重、开盘可用组合估值和现金，整手数量应满足
      `成交额（含滑点） + commission <= min(现金, 目标预算)`；数量不足一手才跳过，
      不能因初始整手数量未预留手续费就跳过原本可缩量成交的整笔订单。
- [ ] 先写失败测试：100% 目标、最低佣金边界、比例佣金边界、现金不足一手、多个标的
      依序买入不透支；保留已有未来收盘不改变当日开盘成交的回归。
- [ ] 只改 `datahub/app/lib/strategy_engine/nav.py`、相关策略测试及对应 spec/docs；
      如确需扩大范围，先报告原因，不能顺手改评分、方向、券商执行或前瞻证据口径。
- [ ] 校验 focused/full datahub pytest、Ruff、OpenSpec strict；实现后调用
      spec-guardian / contract-reviewer / qa-reviewer，做 branch-conflict 与 Draft PR CI。
      回填 agent-progress；本交接不授权合并后续代码 PR、operator 写库、promote 或真实下单。

验收产物：独立 PR、修复前失败/修复后通过的测试证据、费用预算口径、遗留风险。
本文件是可供 DSH 读取的交接；未通过本会话工具启动 DSH，也不声称其已开始执行。
