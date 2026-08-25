# Datahub Tushare Universe Tasks

## 1. Interface

- [x] 1.1 `tushare_interface`: `from_tushare_ts_code`（600519.SH → sh600519）
- [x] 1.2 `tushare_interface`: `stock_basic_active()`（list_status='L'）
- [x] 1.3 `tushare_interface`: `daily_by_trade_date(trade_date)`（全市场当日截面）

## 2. Handler + Processor

- [x] 2.1 `zh_a_daily`: `DATAHUB_STOCK_UNIVERSE_SOURCE`（默认 spot）+ `get_zh_a_stock_universe(as_of_date)`
- [x] 2.2 `zh_a_daily`: `_build_tushare_universe`（stock_basic + daily 合并，close=0 停牌语义）
- [x] 2.3 `china_a_stock.check_stock_data_integrity` 改调 universe getter（传 frozen as_of_date）

## 3. Tests

- [x] 3.1 Universe dispatch（spot 默认不变 / tushare 生效）
- [x] 3.2 Tushare universe：code/name/close 映射、停牌（缺行 / trade_status=0 → close=0）
- [x] 3.3 ts_code 反映射（sh/sz/bj）
- [x] 3.4 现有 spot 路径测试不回归

## 4. Review + Merge

- [ ] 4.1 spec-guardian + qa-reviewer
- [ ] 4.2 Branch conflict check + Draft PR + CI green

## 5. Deploy + Daily Update (operator)

- [ ] 5.1 Publish new image; deploy dev → verify → deploy prod
- [ ] 5.2 每日增量 Job：`DATAHUB_STOCK_HISTORY_SOURCE=tushare` + `DATAHUB_STOCK_UNIVERSE_SOURCE=tushare`，`--as-of-date` 当日
- [ ] 5.3 验证 08-25 行写入与 freshness（无超期行、AHEAD=0）
