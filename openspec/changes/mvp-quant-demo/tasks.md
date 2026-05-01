# MVP Quant Demo Tasks

## 1. Spec Alignment

- [ ] 1.1 Update `openspec/config.yaml` to match the current stack
- [ ] 1.2 Keep legacy frontend OpenSpec as historical reference only
- [ ] 1.3 Confirm MVP scope with current AGENTS.md

## 2. Data Quality

- [ ] 2.1 Document data quality summary and detail rules
- [ ] 2.2 Document BSE exclusion and unsupported stock scope
- [ ] 2.3 Document MA window applicability for new listings

## 3. Datahub Runners

- [ ] 3.1 Document quote runner behavior and invocation
- [ ] 3.2 Document factor runner behavior and invocation
- [ ] 3.3 Document freshness updates and dry-run support

## 4. Signals MVP

- [ ] 4.1 Document MA cross signal rules
- [ ] 4.2 Document signal storage shape and freshness
- [ ] 4.3 Document backend query API shape

## 5. Backtest MVP

- [ ] 5.1 Document single-stock daily backtest flow
- [ ] 5.2 Document supported strategy set
- [ ] 5.3 Document result payload and UI flow
- [ ] 5.4 Document that MVP trading backtest uses lightweight internal backend code, not an external backtest framework
- [ ] 5.5 Document separation between scoring replay/calibration and user-facing trading backtest

## 6. Review

- [ ] 6.1 Review docs against current code paths
- [ ] 6.2 Trim any over-designed or outdated content

## 7. Multi-horizon Stock Scoring

- [ ] 7.1 Replace the T+5-oriented scoring spec with a `Score5` / `Score20` / `Score60` prediction model
- [ ] 7.2 Define `StockScorePrediction` as one stock/date/horizon/model-version prediction record
- [ ] 7.3 Define unique and query indexes for stock/date/horizon/model-version, rankings, stock history, and verification jobs
- [ ] 7.4 Define structured explanation contract with components, penalties, evidence, thresholds, and model version
- [ ] 7.5 Define input snapshot contract for quote, factor, signal, freshness, and blocked-input states
- [ ] 7.6 Define horizon-specific scoring configuration for weights, thresholds, risk caps, and minimum quote requirements
- [ ] 7.7 Define initial rule-based components for signal strength, trend alignment, momentum, breakout/position, relative strength, and risk penalty
- [ ] 7.8 Define closed-loop tracking metrics for target return, max return, min return, drawdown, days to max return, quote count, and hit flags
- [ ] 7.9 Define verification statuses: `PENDING`, `TRACKING`, `VERIFIED`, `INSUFFICIENT_DATA`, `BLOCKED`, and `FAILED`
- [ ] 7.10 Define daily datahub workflow order for quote, factor, signal, score generation, verification, and score summary metrics
- [ ] 7.11 Define manual and backfill commands for one date, one horizon, date ranges, verification, and dry-run scoring
- [ ] 7.12 Define backend score APIs for list, stock history, explanation, performance, and market comprehensive summaries
- [ ] 7.13 Define frontend market and stock-detail requirements for horizon sorting, explanations, tracking state, and verified outcomes
- [ ] 7.14 Define performance review metrics by horizon, score bucket, top-N group, model version, false positives, and false negatives
- [ ] 7.15 Mark old single-score `StockDailyScore` T+5 fields as legacy/disposable unless a short-lived adapter is needed
- [ ] 7.16 Define scoring replay/calibration as a lightweight internal datahub implementation using MongoEngine queries and optional pandas aggregation
- [ ] 7.17 Define that scoring replay/calibration does not use `backtrader`, `vectorbt`, `zipline`, `rqalpha`, or another full trading framework
- [ ] 7.18 Define look-ahead bias guardrails for historical scoring: scoring reads only evaluation-date-or-earlier inputs
- [ ] 7.19 Define scoring backfill overwrite policy: default no overwrite, explicit replace required for same stock/date/horizon/model-version
- [ ] 7.20 Define calibration report outputs for JSON and future API consumption

## 8. Scoring Implementation Path

- [ ] 8.1 Add `StockScorePrediction` model to datahub and backend model trees
- [ ] 8.2 Add scoring config module with horizon-specific weights, thresholds, risk caps, and model version
- [ ] 8.3 Add scoring component module for signal strength, trend alignment, momentum, breakout/position, relative strength, and risk penalty
- [ ] 8.4 Refactor scoring service to generate one prediction per stock/date/horizon/model-version
- [ ] 8.5 Add input snapshot builder for quote, factor, signal, freshness, and blocked reasons
- [ ] 8.6 Add explanation builder with reproducible components, penalties, evidence, thresholds, and summary
- [ ] 8.7 Add rank and percentile assignment per date/horizon/model-version
- [ ] 8.8 Add verification service for `PENDING`, `TRACKING`, `VERIFIED`, and `INSUFFICIENT_DATA` transitions
- [ ] 8.9 Add replay service for historical date-range scoring backfill
- [ ] 8.10 Add calibration report service for score buckets, Top-N groups, component contribution analysis, false positives, and false negatives
- [ ] 8.11 Update `scoring_runner.py` with `run`, `backfill`, `verify`, and `report` commands
- [ ] 8.12 Add command options for date ranges, horizon filters, stock-code filters, model-version filters, dry-run, and replace
- [ ] 8.13 Add tests for model indexes, target trading dates, score generation, blocked inputs, explanation reproducibility, ranking, verification, replay, reports, and look-ahead bias
- [ ] 8.14 Run datahub Python validation: `ruff check`, `ruff format --check`, and focused scoring tests

## 9. Score API and UI Implementation Path

- [ ] 9.1 Add backend score list API
- [ ] 9.2 Add backend stock score history API
- [ ] 9.3 Add backend score explanation API
- [ ] 9.4 Add backend score performance API backed by calibration summaries or prediction aggregation
- [ ] 9.5 Update `/api/market/comprehensive` to return `Score5`, `Score20`, and `Score60` summaries
- [ ] 9.6 Add backend API tests for score list, history, explanation, performance, and comprehensive market summaries
- [ ] 9.7 Update frontend market API types for multi-horizon score summaries
- [ ] 9.8 Update Market view to display and sort by `Score5`, `Score20`, and `Score60`
- [ ] 9.9 Update stock detail flow to show score history, explanation components, input freshness, and verification status
- [ ] 9.10 Run relevant frontend lint/build validation

## 10. OpenClaw Data Access

- [ ] 10.1 Document OpenClaw as a downstream read-only consumer of caifubao data
- [ ] 10.2 Define required API domains: stock master data, quotes, adjusted prices, factors, signals, scores, and data quality
- [ ] 10.3 Define freshness and blocked-by-quote semantics for downstream analysis gating
- [ ] 10.4 Identify backend API gaps for OpenClaw consumption
- [ ] 10.5 Keep OpenClaw analysis logic out of caifubao and avoid direct Mongo coupling
- [ ] 10.6 Define OpenClaw service-token authentication with hashed token storage and read-only scopes
- [ ] 10.7 Define request audit fields for OpenClaw access, including request id, token id, endpoint, status code, and data-as-of
- [ ] 10.8 Document token expiry, revocation, and future rate-limit expectations
