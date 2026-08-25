# Datahub Tushare Source Tasks

## 1. Interface

- [x] 1.1 Add `data_source/interface/tushare_interface.py`: `to_tushare_ts_code` + `tushare_daily` (token from `TUSHARE_TOKEN`, clear error when unset; unknown prefix raises `ValueError`)
- [x] 1.2 Add `tushare==1.4.26` to `datahub/requirements.txt`
- [x] 1.3 Paginate `tushare_daily` by 18-year windows (tushare per-call 6000-row cap; old listings must not truncate)

## 2. Handler + Normalization

- [x] 2.1 Add `tushare` to `SUPPORTED_STOCK_HISTORY_SOURCES`
- [x] 2.2 Add `_normalize_tushare_stock_history` (columns, units: amount 千元→元, vol 手; derived/fixed fields; ascending by date)
- [x] 2.3 Add tushare branch in `get_zh_a_stock_hist_daily_quote` with `_call_with_retry` and `as_of_date` cap
- [x] 2.4 Empty history returns `None` instead of crashing the run (guarded in akshare + tushare branches)
- [x] 2.5 Retry tushare rate-limit errors (marker "每分钟最多" in transient set)

## 3. Tests

- [x] 3.1 Normalization schema/units/order/end-date filter (red before, green after)
- [x] 3.2 Source selection (`DATAHUB_STOCK_HISTORY_SOURCE=tushare` accepted; default unchanged)
- [x] 3.3 ts_code mapping (sh/sz/bj) + unknown-prefix ValueError
- [x] 3.4 Missing token fails with clear error
- [x] 3.5 Empty history → None; missing required column → ValueError; transient error retried; year-window pagination

## 4. Review + Merge

- [ ] 4.1 spec-guardian + qa-reviewer (re-review after P1 fixes)
- [ ] 4.2 Branch conflict check + Draft PR + CI green

## 5. Private Wiring + Deploy (private repo, operator)

- [x] 5.1 `TUSHARE_TOKEN` GitHub secret → `env/root/.env` → datahub secret → pod env (private PR #54)
- [ ] 5.2 Publish new image; deploy dev → verify → deploy prod (full public_ref)
- [ ] 5.3 Bootstrap smoke: single stock via tushare; then full-market one-shot Job with frozen `as_of_date`
