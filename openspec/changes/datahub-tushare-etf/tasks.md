# Tushare ETF Market Data Tasks

## 1. Contract

- [x] 1.1 Freeze the ETF universe, quote, fund_adj FQ, freshness and boundary
  semantics in the delta spec.
- [x] 1.2 Probe the provider caps and document them (fund_adj 2,600 rows,
  fund_daily 5,000 rows; `.OF` rows are not ETFs).

## 2. Model and interface

- [x] 2.1 Add `ExchangeTradedFund` (`object_type=etf`) to the shared model and
  re-export it from `app.model.stock`.
- [x] 2.2 Add `fund_basic_all`, `fund_daily_by_trade_date`, `tushare_fund_daily`,
  `fund_adj` and `fund_adj_by_trade_date` with the 200/min pacing budget.
- [x] 2.3 Paginate `fund_adj` in 10-year windows and reject capped/truncated
  windows (also guard `fund_daily` at its 5,000-row cap).

## 3. Normalization and service

- [x] 3.1 Add `zh_etf_daily` universe/history/snapshot normalization with
  SH/SZ-only filtering and the documented unit conversions.
- [x] 3.2 Add `EtfFQFactorService` reusing the stock FQ arithmetic with
  `fund_adj` as its source.
- [x] 3.3 Add `EtfMarketService` universe upsert, daily quote + FQ sync,
  per-fund history backfill and status summary.
- [x] 3.4 Record quote/FQ freshness under `data_asset_status` with
  `object_type=etf`.

## 4. Operations

- [x] 4.1 Add `app.jobs.etf_sync_runner`
  (`universe|daily|backfill|status`) with job-run tracking.
- [x] 4.2 Add `scripts/caifubao etf ...` commands and help text.
- [x] 4.3 Add a suspended-by-default research CronJob for the daily ETF sync.
- [x] 4.4 Document the operator flow and the provider caps.

## 5. Validation

- [x] 5.1 Unit tests: universe/history/snapshot normalization, `.OF` filtering,
  10-year pagination, truncation rejection, fund_adj FQ wiring, quote upsert
  and the daily orchestration order.
- [x] 5.2 Ruff (CI rule set + format) and the focused pytest run pass.
- [x] 5.3 Real-provider smoke check in the research pod: fund_basic shape,
  fund_daily/fund_adj coverage on the latest session, and full-history
  pagination for an old fund.

## 6. Review and delivery

- [ ] 6.1 Spec, contract and QA reviewer gates pass.
- [ ] 6.2 Branch conflict check against the target base (including the open
  `feature/strategy-engine-unification` PR #307).
- [ ] 6.3 Draft PR; CI green before marking ready (only when delivery includes
  a PR).
