# Datahub Tushare Source Tasks

## 1. Interface

- [ ] 1.1 Add `data_source/interface/tushare_interface.py`: `to_tushare_ts_code` + `tushare_daily` (token from `TUSHARE_TOKEN`, clear error when unset)
- [ ] 1.2 Add `tushare==1.4.26` to `datahub/requirements.txt`

## 2. Handler + Normalization

- [ ] 2.1 Add `tushare` to `SUPPORTED_STOCK_HISTORY_SOURCES`
- [ ] 2.2 Add `_normalize_tushare_stock_history` (columns, units: amount 千元→元, vol 手; derived/fixed fields; ascending by date)
- [ ] 2.3 Add tushare branch in `get_zh_a_stock_hist_daily_quote` with `_call_with_retry` and `as_of_date` cap

## 3. Tests

- [ ] 3.1 Normalization schema/units/order/end-date filter (red before, green after)
- [ ] 3.2 Source selection (`DATAHUB_STOCK_HISTORY_SOURCE=tushare` accepted; default unchanged)
- [ ] 3.3 ts_code mapping (sh/sz/bj)
- [ ] 3.4 Missing token fails with clear error

## 4. Review + Merge

- [ ] 4.1 spec-guardian + qa-reviewer
- [ ] 4.2 Branch conflict check + Draft PR + CI green

## 5. Private Wiring + Deploy (private repo, operator)

- [ ] 5.1 `TUSHARE_TOKEN` GitHub secret → `env/root/.env` → datahub secret → pod env
- [ ] 5.2 Publish new image; deploy dev → verify → deploy prod (full public_ref)
- [ ] 5.3 Bootstrap smoke: single stock via tushare; then full-market one-shot Job with frozen `as_of_date`
