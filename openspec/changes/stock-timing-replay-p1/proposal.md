# Stock Timing Replay Adapter P1

## Why

P0 can judge a normalized timing-versus-buy-and-hold result pool, but it still
requires callers to manufacture the cohort and both result sides. The existing
single-stock backtest cannot fill that gap safely: score strategies trade on
absolute `score` rather than the P0 full-cohort `percentile`, and its buy-and-
hold path enters at a session close rather than through the same next-open
execution model.

P1 adds the missing research adapter so a frozen point-in-time cohort can be
replayed against real stored quotes and ranked predictions without quietly
falling back to the current active universe or persisting backtest documents.

## What Changes

- Define and validate a frozen point-in-time cohort manifest. Its provenance
  must explicitly cover suspended and subsequently delisted names, and every
  member carries listing/delisting evidence known by the manifest `as_of`.
- Add a deterministic percentile timing simulator and same-stock buy-and-hold
  baseline using the same window, cash, board lot, next-tradable-open fills,
  last-close valuation, limit/suspension rules, and friction.
- Pin an ACTIVE ranked model registry record, its configuration hash, horizon,
  and timing thresholds before replay starts.
- Add a research-only `timing-replay` CLI that loads real stored evidence,
  adapts each stock into the P0 normalized pair, and invokes the P0 pooled
  evaluator. It writes only an optional JSON report file.

## Non-goals

- No current-active universe discovery, cohort backfill, score generation,
  model promotion, parameter search, public API, frontend, scheduler, order, or
  account mutation.
- No alpha or tradability claim. Output remains `research_only=true` and
  `validation_status=UNVALIDATED` even when mechanical evidence gates pass.
- No attempt to infer delisted-name completeness from successful database rows.

## Module Impact

- `datahub/`: frozen-cohort validation, replay adapter, CLI, and focused tests.
- `openspec/`: P1 replay and look-ahead/survivorship-bias contract.
- `backend/`: no public API or persistence change.
