# 10万元单ETF基准：本地可核对闭环

本切片把初始买入、费用、现金、份额和每日估值接起来，作为主动策略的比较基础。
结果全部是历史 REPLAY；不写数据库、认证前瞻窗口或连接券商。

## 1. 跑通合成样例

`datahub/examples/etf-benchmark-100k.json` 的价格和费用是**合成测试值**，股票代码仅
作接口示例，不是实际行情、产品推荐或券商报价。100% allocation 用于验证手续费
不会使现金透支，不表示真实账户应采用该仓位。

在仓库根目录运行（本地 datahub/.venv 为 Python 3.12）：

```bash
./scripts/caifubao strategy benchmark \
  --input datahub/examples/etf-benchmark-100k.json \
  --output /tmp/etf-benchmark-result.json \
  --halt-file /tmp/etf-benchmark-halt.json
```

这个子命令在本机执行，不调用 kubectl。独立工作区没有 venv 时，设置
`CFB_LOCAL_PYTHON` 为已有 Python 3.12 虚拟环境的绝对解释器路径。也可直接运行：

```bash
PYTHONPATH=datahub datahub/.venv/bin/python \
  -m app.lib.strategy_engine.etf_benchmark \
  --input datahub/examples/etf-benchmark-100k.json \
  --halt-file /tmp/etf-benchmark-halt.json
```

halt 路径必须显式提供，或使用 `CAIFUBAO_STRATEGY_HALT_FILE`。复用既有 halt
语义：配置的文件不存在表示 OFF，损坏文件失败关闭；不能通过删除文件来安全保持
停机状态。测试使用单独的 halt 文件，不操作任何运行环境的停机开关。

## 2. 手工核对

2026-09-01 收盘后形成初始买入决定，下一交易日以4元模拟成交：

| 项目 | 计算结果 |
|---|---:|
| 初始资金 | 100,000.00元 |
| 买入份额 | 24,900份 |
| 成交金额 | 99,600.00元 |
| 佣金 | max(99,600 × 0.025%, 5) = 24.90元 |
| 剩余现金 | 375.10元 |
| 首日收盘净值 | 375.10 + 24,900 × 4.10 = 102,465.10元 |
| 最后收盘净值 | 375.10 + 24,900 × 4.20 = 104,955.10元 |

此后两天均为 HOLD，无额外交易和佣金。买入当天可卖份额为0，下一给定交易日为
24,900份。输出 `trades`、`curve` 和 `final_account` 可独立重建。

## 3. 输入契约

- 输入必须为 `etf-benchmark-v1`，仅一只声明为 `domestic_equity_etf` 的ETF；代码
  格式只作格式检查，不能证明真实产品类别。参数声明随输出保存。
- `price_basis` 必须是 `raw`。**实际份额乘未复权价格**估值，不使用研究面板的
  `close_hfq`、`fund_adj` 或 forward labels。此命令不自动转换 ETF panel。
- 日历必须独立给出：`decision_date` 是 `sessions` 首日，每个后续市场交易日都会
  输出账本行；报价缺日不能把市场交易日压缩成“有报价日”。日历由输入提供，尚未
  接入权威交易所日历校验。报价需唯一、有序且属于日历。
- `corporate_actions: []` 是操作员对该区间无分红/拆分的显式声明，不是系统检测。
  非空动作拒绝处理；跨分红/拆分区间须等后续现金与份额变动支持，不能删动作后
  把价格跌幅当作投资损失。
- `allocation` 是包含佣金的初始资金预算占比，范围[0,1]；资金默认100,000元，
  精确到分。`fees` 必须给出小数比例的 `commission_rate`、精确到分的
  `minimum_commission` 和 `slippage_rate`。使用实际券商参数；没有股票费税默认。
  allocation、佣金率和滑点率最多接受12位小数，超出则拒绝，避免舍入改变预算或成交价。
- 价格和金额推荐以字符串输入，拒绝bool、NaN、Infinity、未知字段及重复JSON键。
  `tick_size` 为正、≤1、最多4位小数，价格须符合tick。买入滑点不利于投资者并
  向上取至tick，佣金四舍五入至分，预算向下取至分，份额按100份取整。
- 报价字段为 `date/open/close/trade_status/upper_limit`；价格、状态或涨停价未知
  会阻断对应操作；非法已给值则拒绝输入。交易状态只接受0/1或null。

## 4. 阻断、重试与估值

上一交易日收盘不可用时，不生成可执行初始买单；当前开盘/状态/涨停价缺失、涨停
开盘、滑点后超过涨停价或预算不足一手，记录 BLOCKED 并保留现金。可在下一市场
交易日按其前一日信息重试，预算不随未来净值变动。一次成功买入后只保持。

当天收盘缺失不会影响已可成交的开盘买入。新持仓暂以当日原始开盘估值、来源为
OPEN，并显示 STALE；已有持仓延用最后有效价格，保留其 `mark_as_of/mark_source`。
有有效收盘才显示 OK。尚未买入或零仓位显示 CASH，不假装拥有最新ETF估值。

持仓期间缺报价仍输出 HOLD 和 STALE；没有实现卖出，T+1字段仅报告份额可卖日期。
完整 input/config 哈希用于回放核对；trade ID不依赖未来价格。相同输入输出相同，
不代表已有真实账户的持久化去重能力。

输出包含 `target_holdings` 与 `final_account`，可把它们分别导出为计划和独立重建
账户，调用已有 `app.lib.strategy_engine.reconcile`。账户需包含显式 `planned_cash`。
旧对账器只接受数值现金，在接口处将 `cash/planned_cash` 字符串转为JSON数值；原始
账本继续保留精确到分的字符串。测试已覆盖这一转换，不改变旧对账接口。
用输出自身对自身核对只能确认格式；实盘对账仍需独立的真实账户快照。

## 5. 验收与后续

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m pytest -q \
  datahub/app/test/test_etf_benchmark.py
```

本切片验收是手算一致、现金不透支、仅一次建仓、无收盘前瞻、缺行情不伪装新鲜、
halt生效、输出原子写入且不覆盖输入/停机文件。没有收益优越性结论。

接下来依次补：源数据真实性与公司行为校验、分红/份额变动、持久化账户与成交导入、
每日调度与最小账户页面。主动策略在基准闭环稳定后单独验证。

## 6. 冻结行情与日历导出适配

命令增加 `--source-format tushare-json`，输入为单个 `etf-source-v1` bundle：
`configuration` 包含 instrument、initial_cash（可省略）、allocation、fees、
corporate_actions；其余字段为 start_date、end_date、calendar、daily、execution。
日期接受 YYYYMMDD 或 YYYY-MM-DD。配置和无公司行为声明沿用前述契约。

合成源格式示例（不是真实行情）：

```bash
./scripts/caifubao strategy benchmark \
  --input datahub/examples/etf-source-100k.json --source-format tushare-json \
  --output /tmp/etf-source-result.json --halt-file /tmp/etf-demo-halt.json
```

- `calendar` 使用 [trade_cal](https://tushare.pro/document/2?doc_id=26) 形状，
  每行 exchange、cal_date、is_open（0/1，可为字符串）。区间每个自然日都必须有行，
  包括闭市日；两端必须开市，至少两个开市日。不要导出只含 is_open=1 的日历。
- `daily` 使用 [fund_daily](https://tushare.pro/document/2?doc_id=127) 原始
  open/close、ts_code、trade_date，可保留标准日线字段。复权字段拒绝。
  沪市使用510300.SH形状，深市159xxx.SZ形状，须与instrument一致。
  0价格转为缺失；负价或非有限价格拒绝。缺行情的开市日仍保留在账本中。
- `execution` 独立提供 ts_code、trade_date、trade_status（整数0/1或null）、
  up_limit（有效原始涨停价或null）。这些必须是开盘前已知的操作员声明；
  缺行即未知。不能用日成交量推断开盘可交易，系统也不从昨收推算涨停价。
  日线vol只作非负有限数校验，不影响买入。

输入记录允许倒序，但重复日期、混合代码/交易所、范围外行和闭市日报价拒绝。
输出新增 source_provenance，保留适配版本、整个bundle的规范JSON SHA256及
operator_opening_declaration状态来源。同样保留 REPLAY 标记。
这条路径离线，不自动调用供应商或认证数据真实性、交易日已完成状态；回放不依赖
运行当天的时钟，未来合成区间也不代表真实已发生行情。真实导出须保留在本地，不提交
供应商付费行情或任何token。修改成交量会改变源哈希，但不会改变成交。
