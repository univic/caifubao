# Production Capability Roadmap Tasks

> 合规口径：产品对外始终称「研究/学习/演示 MVP、非投资建议」；本文只规划**实盘能力**
> 的内部路线。本 change 不授权真实资金执行。

## 0. 证据与定版门槛（进行中）

- [ ] 0.1 task 4.4 纸面 ≥120 交易日 operator 日更已启动（day-1 = 2026-06-10，
      flip_wide_shadow_v1 宽书 top-800、等权、排除 ST/BSE/停牌、initial_nav 2,000 万）；
      每日补跑最新 VERIFIED 日期并计入 strategy_daily SUCCESS 计数
- [ ] 0.2 每周纸面 NAV/超额/回撤/换手记入 autoresearch ledger（同 manual-experiments-ledger）
- [ ] 0.3 promote 检查清单：120 日窗口结束后 paper 结果 vs 研究 walk-forward 预期对照，
      通过后走 version bump + Spec Gate 的 promote 流程（本 change 不授权 promote）
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
