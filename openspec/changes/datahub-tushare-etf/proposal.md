## Why

The research chain only ingests stocks. ETF research (the 100k small-account
rotation study, the single-ETF paper benchmark and the core-satellite study)
currently pulls `fund_basic` / `fund_daily` / `fund_adj` ad hoc from a local
script, freezes a parquet panel and throws the data away; nothing is persisted,
nothing has freshness/quality status, and no scheduled job keeps it current.
The paper-trading readiness work also needs an instrument-level data source that
is as auditable as the stock quote chain.

While building this capability, a real provider constraint was measured (research
account, 2026-10-23): `pro.fund_adj` returns at most **2,600 latest-first rows**
per call, so an 18-year window silently drops the oldest years (510300.SH lost
everything before 2016-02-04) and `pro.fund_daily` caps at 5,000 rows. The new
source must paginate within those caps and fail loudly instead of storing a
truncated factor series.

## What Changes

- Add an `exchange_traded_fund` master document (`object_type=etf`) to the
  shared model, with `fund_type`, `benchmark`, `management_fee` and `list_date`.
- Extend `tushare_interface` with `fund_basic_all`, `fund_daily_by_trade_date`,
  `tushare_fund_daily`, `fund_adj` and `fund_adj_by_trade_date`, paced at the
  fund endpoints' 200 calls/minute budget.
- Paginate fund factors in 10-year windows and reject a capped window whose
  earliest row starts far after the requested start (no silent truncation).
- Add `zh_etf_daily` normalization: SH/SZ exchange funds only (the `.OF` rows
  that appear in `fund_daily` are not ETFs), `vol` in 手, `amount` 千元 → 元,
  canonical stock-quote columns.
- Add `EtfMarketService` + `EtfFQFactorService`: universe upsert, per-session
  quote snapshot, per-fund history backfill, and `fq_factor`/`*_hfq` derived
  from `fund_adj` through the existing, tested FQ pipeline.
- Write ETF quotes into `stock_daily_quote` (shared schema) and ETF
  freshness/quality into `data_asset_status` with `object_type=etf`.
- Add `python -m app.jobs.etf_sync_runner {universe,daily,backfill,status}` and
  the `scripts/caifubao etf {universe,daily,backfill,status}` CLI.
- Add a suspended-by-default research CronJob for the daily ETF sync.

## Impact

- `shared/caifubao_contracts/models/stock.py`, `datahub/app/model/stock.py`
- `datahub/app/lib/datahub/data_source/interface/tushare_interface.py`
- `datahub/app/lib/datahub/data_source/handler/zh_etf_daily.py` (new)
- `datahub/app/lib/datahub/etf_market.py` (new)
- `datahub/app/jobs/etf_sync_runner.py` (new)
- `scripts/caifubao`, `k8s/overlays/example-research/datahub-cronjobs.yaml`
- Tests and operator documentation.

No stock universe, scoring, signal, strategy or forward-evidence semantics
change. ETF data is a research/data asset; nothing here trades or promotes.

## Non-goals

- ETF selection, rotation strategy changes or portfolio construction.
- Fund NAV, premiums/discounts, or valuation (`fund_nav`/`fund_share`) ingestion.
- LOF/closed-end fund coverage beyond what `market='E'` returns with an SH/SZ
  venue; `.OF` (off-exchange) rows are rejected.
- Real-time quotes, broker access or any order path.
- Backfilling the full 3,000-fund universe in this change; the runner supports
  it, the operator decides when to run it.
