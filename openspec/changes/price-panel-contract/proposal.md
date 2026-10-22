## Why

Repeated research failures trace back to adjustment (复权) handling:

- vendor 后复权 violated the ±10% limit on 51 days and inflated 000977's CAGR;
- vendor 前复权 went negative for high-dividend stocks and compressed 2005-2013 returns;
- research inferred adjustment factors from price gaps, misclassifying the 2017/2020
  rights issues as bonus issues;
- the adjusted series changed mid-study (hfq → qfq → reconstructed), so results were
  not comparable and had to be retracted.

The production path already uses tushare `adj_factor` (`fq-adj-factor-fix`), but the
research path bypasses it. There is no contract that says which series is canonical,
how rights issues are treated, or what a valid adjusted series must satisfy.

## What Changes

- Add `datahub/app/lib/prices/`:
  - `corporate_actions.py`: the corporate-action ledger schema and account-based
    event arithmetic for bonus, transfer, share reform, rights, and cash.
  - `return_index.py`: build a positive total-return index from raw prices + the
    ledger, with contract invariants (limit on non-event days, event reconciliation,
    constant factor between events, rebuild equality).
- Add `datahub/scripts/build_price_panel.py`: a CLI that writes a versioned panel and
  a manifest, and fails on any violation.
- Add `datahub/app/test/test_price_contract.py`: synthetic cases covering bonus,
  cash, rights, share reform, missing ex-date, limit breach, negative price, invalid
  rights event, and announcement-after-ex-date.
- Seed the 000977 ledger at
  `datahub/research/timing_000977/data/corporate_actions_000977.csv` (20 events with
  sources) and migrate the research build to call the contract.
- Document that production keeps tushare `adj_factor` as the primary factor source,
  and the offline ledger path must match the same invariants.

## Non-goals

- No change to MongoDB schemas, collection names, or the existing FQ writer.
- No new vendor integration; no full-universe backfill in this change.
- No change to scoring, signals, or strategy parameters.
- Does not model rights subscription cash in the research account yet; the index is
  an account-based total-return proxy, and the real-share ledger remains a follow-up.
