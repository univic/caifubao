## Design

### UPD 分支（check_data_integrity）

```python
if flag in ["UPD", "INC", "FULL"]:
    if not is_temporarily_suspended:
        required_quote_attempt_count += 1
    if (
        flag == "UPD"
        and not is_temporarily_suspended
        and obj_type == "stock"
        and zh_a_daily.get_stock_universe_source() == "tushare"
    ):
        hist_result = self.write_snapshot_quote(
            stock_obj=stock_obj,
            snapshot_row=remote_stock_item,
            expected_date=self.most_recent_trading_day,
        )
    else:
        hist_result = self.get_hist_quote_data(...)
```

- **UPD + 非停牌 + 股票 + universe 源为 tushare** → 快照写入；其余
  （INC/FULL、停牌、指数、spot universe）→ 历史
- 快照路径仅对 tushare universe 开放：`daily(trade_date)` 是 as-of 结算价；
  spot（实时快照）盘中会与 as-of 日期错位，保持历史回退
- 停牌股（close=0）不进快照写入，避免写入 close=0 的污染行（沿用悬挂容忍）

### write_snapshot_quote

用 `_build_stock_quote_upsert_operation` 构造单行 upsert
（date=as-of，字段取自快照行，缺省 0），bulk_write 后
`refresh_quote_status(expected_latest_date=as-of)`；返回
`{code, written_count, validated_count, freshness_status}` 与历史路径同形，
下游 `quote_validation_failed` / `failed_quote_codes` / 零行门 逻辑不变。

### tushare universe 扩展

`_build_tushare_universe` 输出列从 `[code, name, close]` 扩展为
`[code, name, close, open, high, low, volume, trade_amount,
previous_close, change_amount, change_rate, turnover_rate]`
（trade_amount = amount×1000；停牌行全 0）。快照写入仅限 tushare
universe（spot 为实时快照，保持历史回退）。

### 验证

- 单测：UPD+tushare 走快照写入（历史不被调用）；INC/FULL/停牌/spot-universe
  走历史；tushare universe 含完整日线字段；`write_snapshot_quote` 直接单测
  （行构造/数值强转/freshness/异常→FAIL）
- 生产：每日增量 Job（全 tushare）应 ~3 次调用 + 分钟级完成，0 限流
