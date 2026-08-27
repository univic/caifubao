# FQ Adj Factor Fix Tasks

## 1. Interface

- [x] 1.1 `tushare_interface.adj_factor(ts_code, start_date, end_date)` → DataFrame(ts_code, trade_date, adj_factor)
- [ ] 1.2 Token/错误处理与 `tushare_daily` 一致（缺 token 报错、限流标记）

## 2. FQ Factor Rewrite

- [x] 2.1 `build_fq_factor_frame` 改为基于真实 adj_factor（不再用 close/previous_close 累积）
- [x] 2.2 `update_code` 拉取 adj_factor 并合并到 quote 行；缺失日期回退最近因子
- [x] 2.3 close/open/high/low_hfq 按 `close × adj_factor` 与同比例缩放
- [x] 2.4 幂等 upsert `(code, date)` 语义不变

## 3. Tests

- [x] 3.1 无除权股：fq_factor 恒定、close_hfq/close 比例恒定
- [x] 3.2 除权股：因子仅在除权日变化，close_hfq 与真实 adj_factor 一致
- [x] 3.3 缺失 adj_factor 行回退最近因子
- [x] 3.4 现有 FQ/评分/验证测试不回归（6 passed，本地环境仅 apscheduler 依赖测试待 CI）

## 4. Review + Merge

- [x] 4.1 spec-guardian + qa-reviewer（均 PASS，P1 已修复并有测试覆盖）
- [x] 4.2 Branch conflict check + Draft PR + CI green（PR #135，全绿后 merged）

## 5. Deploy + Recompute (operator)

- [ ] 5.1 发布新镜像；部署 dev → 验证 → 部署 prod
- [ ] 5.2 重算全市场 FQ 因子（真实复权）
- [ ] 5.3 重跑 50 只股票评分+验证实验
