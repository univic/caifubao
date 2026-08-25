## Design

### Universe source selection

`DATAHUB_STOCK_UNIVERSE_SOURCE`（env）∈ `spot | tushare`，默认 `spot`。
`get_zh_a_stock_universe(as_of_date=None)` 按该值分发；`spot` 分支与现行为
完全一致（`get_zh_a_stock_spot()` 东财→新浪回退）。

### Tushare universe（`zh_a_daily._build_tushare_universe`）

1. `pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')`
   → 全部活跃 A 股（ts_code 映射回内部 code，如 `600519.SH → sh600519`）
2. `pro.daily(trade_date=<as_of YYYYMMDD>)` → 当日全市场行情
   （`ts_code, close, ...`；**tushare 省略停牌股**——该日无数据即停牌）
3. 合并：name 取 stock_basic；close 取 daily；**该 code 不在 daily 响应中 →
   close=0（停牌）**，与 spot 的 close==0 语义一致
4. **空快照守卫**：daily 为空 → `RuntimeError`（否则全市场误判停牌、
   静默零写入），与"spot 列表为空"守卫对齐
4. 输出 DataFrame 列：`code, name, close`

### Handler 分发

`get_zh_a_stock_universe(as_of_date)`：
- `spot` → `get_zh_a_stock_spot()`
- `tushare` → `_build_tushare_universe(as_of_date)`（`as_of_date` 缺省取
  `date.today()`，即"今天"的完整截面）

`china_a_stock.check_stock_data_integrity` 改为：
```python
remote_stock_list = zh_a_daily.get_zh_a_stock_universe(
    as_of_date=(
        self.most_recent_trading_day.strftime("%Y-%m-%d")
        if self.most_recent_trading_day
        else None
    )
)
```

### 验证

- 单测：universe 分发（spot/tushare）；tushare universe 的 code/name/close 映射
  与停牌（缺席 → close=0）；空快照报错；ts_code 反映射；
  处理器级：universe 快照日期 = 解析后的冻结交易日
- 注：`pro.daily(trade_date=...)` 单次 6000 行上限，当前市场 ~5.4k 接近上限，
  市场扩容（如主板拆细）时需分片
- 手工：dev 以 `DATAHUB_STOCK_UNIVERSE_SOURCE=tushare` 跑单只/全市场
- 生产：每日增量 Job 配 `DATAHUB_STOCK_HISTORY_SOURCE=tushare` +
  `DATAHUB_STOCK_UNIVERSE_SOURCE=tushare`，全链路不再触碰东财/新浪
