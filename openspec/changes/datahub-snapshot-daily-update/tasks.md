# Snapshot Daily Update Tasks

## 1. Handler

- [ ] 1.1 `_build_tushare_universe` 输出完整日线字段（open/high/low/close/volume/trade_amount/previous_close/change_amount/change_rate/turnover_rate）
- [ ] 1.2 `ChinaAStock.write_snapshot_quote`：快照行 → 单行 upsert + freshness 更新（与历史路径同形返回）

## 2. Processor

- [ ] 2.1 `check_data_integrity` UPD 分支：股票+非停牌 → 快照写入；INC/FULL/停牌/指数 → 历史

## 3. Tests

- [ ] 3.1 UPD 走快照写入（历史不被调用）
- [ ] 3.2 INC/FULL 仍走历史
- [ ] 3.3 停牌 UPD 走历史且不写 close=0 行
- [ ] 3.4 tushare universe 含完整日线字段
- [ ] 3.5 快照写入更新 freshness

## 4. Review + Merge

- [ ] 4.1 spec-guardian + qa-reviewer
- [ ] 4.2 Branch conflict check + Draft PR + CI green

## 5. Deploy + Daily Update (operator)

- [ ] 5.1 Publish new image; deploy dev → verify → deploy prod
- [ ] 5.2 每日增量 Job（全 tushare）：~3 次调用、分钟级完成、0 限流
- [ ] 5.3 验证 08-25 全市场行写入与 freshness
