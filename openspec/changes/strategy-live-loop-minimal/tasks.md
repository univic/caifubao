# Strategy Live Loop Minimal Tasks

> roadmap 2.2-2.4（pre-broker 最小实盘闭环）。范围：`datahub/app/lib/strategy_engine/`
> （`tradability.py` 新增、`nav.py`、`config.py`、`reconcile.py` 新增、`halt.py` 新增）
> + `datahub/app/jobs/strategy_runner.py` 装配/CLI + 测试 + 本 change + 操作文档。
> 不接券商、不下单、不写库、不 promote、不改评分/方向。

## 1. Spec + 失败测试

- [x] 1.1 本 change 四个 requirement 覆盖 2.2-2.4 缺口（涨跌停门禁、板块最小量+过户费、
      对账、熔断），每个 requirement 带 GIVEN/WHEN/THEN scenario
- [x] 1.2 失败测试先行（红→绿）：`test_strategy_nav_causality.py` 扩涨停拒买/跌停拒卖/
      未知 flag 保守 + 报告；`test_strategy_fee_budget.py` 扩过户费影响 sizing、板块最小量、
      SELL 豁免、费用进 `config_hash`；新 `test_strategy_reconcile.py`；新
      `test_strategy_halt.py`
- [x] 1.3 既有 fixture 补 `previous_close`（新契约要求涨跌停可判定；否则 fail closed），
      并更新因费用/门禁变化而必须变的数值期望

## 2. 实现

- [x] 2.1 `tradability.py`：复用 `factor_lab.panel.price_limit` 的 board 分类 + 0.1pp 容差，
      按 `open vs previous_close` 判 `limit_up/limit_down`；`tradeable_open(quote, side)`
      统一 trade_status/open/涨跌停判定并返回结构化拒绝原因
- [x] 2.2 `nav.py`：`QuoteView` 承载 `limit_up/limit_down/previous_close/stock_code/is_st`；
      BUY 拒绝涨停、SELL 拒绝跌停、未知 flag fail closed；被拒订单写入 `blocked`/
      `blocked_count`；`board_lot_rule`（科创板 200 起/1 递增，其余 100 整手）；
      `_order_cost` 双向过户费；sizing 预留佣金+过户费；SELL 豁免买入最小量
- [x] 2.3 `config.py`：费率常量唯一来源（`factor_lab.metrics`），`PAPER_EXECUTION` 引用；
      可选 `execution` 块校验后进入 `config_hash`（缺省不写入默认配置）；
      `resolve_execution`
- [x] 2.4 `jobs/strategy_runner.py`：quote 装载带 `previous_close/isST/code`（门禁可判定）；
      `run_nav` 用 `resolve_execution` 并输出 `blocked_orders`
- [x] 2.5 `reconcile.py`：`reconcile_plan_vs_account` + 薄 CLI（容差越界非零退出，只读）
- [x] 2.6 `halt.py`：文件持久化、默认 OFF、路径无静默默认、who/when/why + history、
      `HaltError`；`run_strategy` 第一件事检查熔断（dry-run 也拒），CLI
      `halt engage|resume|status` + `run --halt-file`
- [x] 2.7 focused/full datahub pytest 全绿（1150）、Ruff、OpenSpec strict

## 2b. Review fixes

- [x] 2b.1 spec-guardian MAJOR 1：缺 `cash` 的账户快照不再 `ok=true`，改为显式
      `cash_drift_unverifiable` break；CLI 非零退出；测试改为「有现金才通过」
- [x] 2b.2 spec-guardian MAJOR 2/3：本 change 内以 `## MODIFIED Requirements` 修正
      `strategy-nav-fee-budget`（零费用场景 + 过户费）与 `strategy-engine`（T+1 成本语义
      含过户费、印花税 0.0005、涨跌停门禁）
- [x] 2b.3 spec-guardian MINOR 4-8：混合批次未知涨跌停场景、CLI 层 HALTED 场景、
      真实零股 SELL（科创板 201 股）、spec 去掉结构性措辞、roadmap 2.3/2.4 记录残留范围
- [x] 2b.4 spec-guardian MINOR 9/10 + qa P3：研究/生产未知判定差异与 ST 位缺口显式记录；
      `as_of` 归一化为 ISO 字符串；门禁只在成交/NAV 时点、熔断文件删除即恢复等写进文档
- [x] 2b.5 qa P1：`reconcile._as_float` 拒绝 NaN/inf（容差与持仓/现金），CLI 对非法输入
      非零退出；补 NaN/inf 测试
- [x] 2b.6 qa P2：印花税 0.001 → 0.0005（`round_trip_cost` = 0.00302）；`holding_scan`
      的 `ROUND_TRIP_COST` 改为从 `metrics` 派生，测试与模块值对齐

## 3. Gates

- [ ] 3.1 spec-guardian 复核本 change（first round 待跑）
- [ ] 3.2 QA reviewer 复核 P1/P2 回归与 fail-closed 语义
