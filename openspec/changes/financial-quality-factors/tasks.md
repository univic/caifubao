# Financial quality factor tasks

## 1. Contract and implementation

- [x] 1.1 Add `stock_financial_indicator` model and a Tushare `fina_indicator` runner.
- [x] 1.2 Normalize/store `code/ann_date/end_date/report_type` + selected quality fields; reject missing dates.
- [x] 1.3 Idempotent upsert and summary; research-only (no API/schedule/promotion).
- [x] 1.4 Tests with a fake Tushare client: idempotency, missing dates, field mapping.

## 2. Backfill and PIT merge

- [x] 2.1 Backfill 2020Q1..2026Q2 report periods into the research collection.
- [x] 2.2 Add `research_merge_financials.py`: `merge_asof` by stock on `ann_date <= date`, greatest `end_date` on ties, no forward fill.
- [x] 2.3 Tests: later announcement invisible, pre-announcement null, no duplication.

## 3. Factors and evaluation

- [x] 3.1 Add current-snapshot industry/size quality ranks (ROE, gross margin, net margin, asset turnover, low debt, growth, operating cash) and a composite.
- [x] 3.2 Run rolling OOS on the delisted-supplemented panel; record negative/positive result with limitations.
- [x] 3.3 Validate tests, Ruff, OpenSpec; record in the experiment log and progress.

## 4. Review

- [x] 4.1 `spec-guardian` and `qa-reviewer` read-only review; resolve P1/P2.
