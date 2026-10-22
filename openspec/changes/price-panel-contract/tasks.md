# Price Panel Contract — Tasks

## 1. Ledger and arithmetic

- [x] 1.1 Add `app/lib/prices/corporate_actions.py` with the event schema, validation,
      share multiplier, account value, event return, and theoretical ex-price factor.
- [x] 1.2 Reject invalid events: unknown action type, negative ratios, rights without
      ratio/price, announcement after ex-date.
- [x] 1.3 Load a CSV ledger with required columns and a stable sort.

## 2. Return index and invariants

- [x] 2.1 Build a positive total-return index from raw OHLC plus the ledger.
- [x] 2.2 Replace only ex-date returns with the event formula; non-event returns must
      equal the raw return exactly.
- [x] 2.3 Apply the board limit to non-event days only (event returns may legitimately
      exceed the raw limit).
- [x] 2.4 Report violations for missing ex-dates, non-positive prices, limit breaches,
      non-positive index, and rebuild mismatch.
- [x] 2.5 Derive adjusted OHLC as `raw * (index / raw_close)` so the factor changes
      only on ex-dates.

## 3. CLI and manifest

- [x] 3.1 Add `scripts/build_price_panel.py` with `--raw/--actions/--code/--output/
      --manifest/--tolerance`.
- [x] 3.2 Write the manifest with input hashes, output hash, event list, and
      violations; exit non-zero on violations.

## 4. Tests

- [x] 4.1 Synthetic bonus, cash, rights, and share-reform cases with exact expected
      returns.
- [x] 4.2 Non-event invariance, constant factor between events, limit breach,
      negative price, missing ex-date, invalid rights event, announcement ordering.
- [ ] 4.3 Add a cross-vendor reconciliation test once a second series is available in
      CI (currently no network).

## 5. Research migration

- [x] 5.1 Seed the 000977 ledger with 20 sourced events.
- [x] 5.2 Make `research/timing_000977/build_adjusted.py` a thin wrapper over the
      contract; `scan.py` reads the canonical panel.
- [x] 5.3 Re-run the research pipeline and record the corrected numbers.
- [ ] 5.4 Replace the remaining vendor series in the 94-stock universe with the same
      contract (needs per-stock ledgers; tracked as a follow-up).

## 6. Validation

- [x] 6.1 `ruff check --select E4,E7,E9,F datahub/` and `ruff format --check datahub/`.
- [x] 6.2 `pytest datahub/app/test/test_price_contract.py` — 12 passed.
- [x] 6.3 000977 panel builds with 0 violations and a manifest hash.
