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

## 6. Acceptance instrument (R1/R2, writer_switch_verify)

- [x] 6.1 R2：按集合/行类声明 required/derived/research-populated/stable-only/excluded 字段作用域，未声明字段名点失败（抽样 + 全量当日键发现）
- [x] 6.2 R2：derived 默认排除并记录 excluded class、原因（legacy stable frozen pre-fq-adj-factor-fix）与稳定 `scope_version`；`--compare-derived` 打开后不一致即 FAIL
- [x] 6.3 R2：research-populated 字段必须在 research 非空可访问，记录 writer/source/code revision 及 revision 来源；stable 缺省为信息性，但两侧非空不一致 FAIL
- [x] 6.4 R2：计数按已声明类分别判定（individual_stock/stock_index/unsupported_universe），零行一侧 FAIL；index 用严格 superset + `min_research_rows` 562 + code 级 superset；总数不作为判据
- [x] 6.5 R1：tushare `adj_factor` 源校验默认开启（`--skip-source-tushare` 仅离线调试，PASS 不算 R1 验收）；错误/空/无可用因子 FAIL，无效行按 writer 语义跳过；5e-4 舍入容差入报告
- [x] 6.6 时效语义：`--trade-date` 显式钉住；省略时以 `finance_market.trade_calendar` + 15:00 Asia/Shanghai 推导应完成会话，避免双方共同陈旧通过
- [x] 6.7 抽样池不再 1000 上限截断；池完整性（pool_size vs 当日行数）入报告并 FAIL 校验
- [x] 6.8 修复合同/QA 复核发现：默认开启源校验、index 塌缩、抽样偏置、声明作用域断言、unsupported 类、数值类型严格、CLI/job-run 与评分抽样测试
