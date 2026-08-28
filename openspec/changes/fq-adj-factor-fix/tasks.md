# FQ Adj Factor Fix Tasks

## 1. Interface

- [x] 1.1 `tushare_interface.adj_factor(ts_code, start_date, end_date)` → DataFrame(ts_code, trade_date, adj_factor)
- [x] 1.2 Token/错误处理与 `tushare_daily` 一致（缺 token 报错、逐请求限流重试）
- [x] 1.3 adj_factor 空响应、无有效行或重试耗尽时该 code 失败且不写入
- [x] 1.4 `adj_factor_by_trade_date(trade_date)` 一次返回全市场当日因子并复用重试

## 2. FQ Factor Rewrite

- [x] 2.1 `build_fq_factor_frame` 改为基于真实 adj_factor（不再用 close/previous_close 累积）
- [x] 2.2 `update_code` 拉取 adj_factor 并合并到 quote 行；缺失日期回退最近因子
- [x] 2.3 close/open/high/low_hfq 按 `close × adj_factor` 与同比例缩放
- [x] 2.4 幂等 upsert `(code, date)` 语义不变
- [x] 2.5 stale market refresh 按交易日批量合并，只写当日；force/backfill 保留全历史
- [x] 2.6 完整覆盖、同日、唯一 code、有限正因子门禁在任何写入前执行
- [x] 2.7 停牌/无当日 quote 股票不进入覆盖分母且不生成虚构记录

## 3. Tests

- [x] 3.1 无除权股：fq_factor 恒定、close_hfq/close 比例恒定
- [x] 3.2 除权股：因子仅在除权日变化，close_hfq 与真实 adj_factor 一致
- [x] 3.3 缺失 adj_factor 行回退最近因子
- [x] 3.4 现有 FQ/评分/验证测试不回归（6 passed，本地环境仅 apscheduler 依赖测试待 CI）
- [x] 3.5 限流重试/耗尽、分窗空响应、非正或非有限响应和失败不写入测试
- [x] 3.6 因子日期对齐由逐行扫描改为排序后 ffill/bfill，保持结果语义不变
- [x] 3.7 全市场快照单次调用、当日写入、覆盖失败零写入、额外/停牌代码测试
- [x] 3.8 一日缺口走快照；NO_DATA/多日缺口保留全历史修复测试

## 4. Review + Merge

- [x] 4.1 spec-guardian + qa-reviewer（均 PASS，P1 已修复并有测试覆盖）
- [x] 4.2 Branch conflict check + Draft PR + CI green（PR #135，全绿后 merged）
- [x] 4.3 本轮可靠性修复 spec-guardian + qa-reviewer（P1 清零）
- [x] 4.4 Branch conflict check + Draft PR #139 + CI green
- [x] 4.5 快照增量路径 spec/contract/qa review + branch conflict + PR #139 CI green
- [x] 4.6 5,547 行本地合成基准：join 0.44s，构造单日 UpdateOne 0.14s

## 5. Deploy + Recompute (operator)

- [ ] 5.1 发布新镜像；部署 dev → 验证 → 部署 prod
- [ ] 5.2 重算全市场 FQ 因子（真实复权）
- [ ] 5.3 重跑 50 只股票评分+验证实验
