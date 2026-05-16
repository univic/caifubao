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

- [x] 5.1 Document single-stock daily backtest flow (implemented: `backend/app/services/backtest_service.py` + `backend/app/api/v1/backtest.py`)
- [x] 5.2 Document supported strategy set (MA_CROSS golden/dead cross, BUY_HOLD baseline)
- [x] 5.3 Document result payload and UI flow (API returns metrics, trades, daily equity; frontend has Create/List/Result views)
- [x] 5.4 Document that MVP trading backtest uses lightweight internal backend code, not an external backtest framework
- [x] 5.5 Document separation between scoring replay/calibration (datahub) and user-facing trading backtest (backend)

## 6. Review

- [ ] 6.1 Review docs against current code paths
- [ ] 6.2 Trim any over-designed or outdated content

## 7. Multi-horizon Stock Scoring

- [x] 7.1 Replace the T+5-oriented scoring spec with a `Score5` / `Score20` / `Score60` prediction model
- [x] 7.2 Define `StockScorePrediction` as one stock/date/horizon/model-version prediction record
- [x] 7.3 Define unique and query indexes for stock/date/horizon/model-version, rankings, stock history, and verification jobs
- [x] 7.4 Define structured explanation contract with components, penalties, evidence, thresholds, and model version
- [x] 7.5 Define input snapshot contract for quote, factor, signal, freshness, and blocked-input states
- [x] 7.6 Define horizon-specific scoring configuration for weights, thresholds, risk caps, and minimum quote requirements
- [x] 7.7 Define initial rule-based components for signal strength, trend alignment, momentum, breakout/position, relative strength, and risk penalty
- [x] 7.8 Define closed-loop tracking metrics for target return, max return, min return, drawdown, days to max return, quote count, and hit flags
- [x] 7.9 Define verification statuses: `PENDING`, `TRACKING`, `VERIFIED`, `INSUFFICIENT_DATA`, `BLOCKED`, and `FAILED`
- [x] 7.10 Define daily datahub workflow order for quote, factor, signal, score generation, verification, and score summary metrics
- [x] 7.11 Define manual and backfill commands for one date, one horizon, date ranges, verification, and dry-run scoring
- [x] 7.12 Define backend score APIs for list, stock history, explanation, performance, and market comprehensive summaries
- [x] 7.13 Define frontend market and stock-detail requirements for horizon sorting, explanations, tracking state, and verified outcomes
- [x] 7.14 Define performance review metrics by horizon, score bucket, top-N group, model version, false positives, and false negatives
- [x] 7.15 Mark old single-score `StockDailyScore` T+5 fields as legacy/disposable unless a short-lived adapter is needed
- [x] 7.16 Define scoring replay/calibration as a lightweight internal datahub implementation using MongoEngine queries and optional pandas aggregation
- [x] 7.17 Define that scoring replay/calibration does not use `backtrader`, `vectorbt`, `zipline`, `rqalpha`, or another full trading framework
- [x] 7.18 Define look-ahead bias guardrails for historical scoring: scoring reads only evaluation-date-or-earlier inputs
- [x] 7.19 Define scoring backfill overwrite policy: default no overwrite, explicit replace required for same stock/date/horizon/model-version
- [x] 7.20 Define calibration report outputs for JSON and future API consumption

## 8. Scoring Implementation Path

- [x] 8.1 Add `StockScorePrediction` model to datahub and backend model trees
- [x] 8.2 Add scoring config module with horizon-specific weights, thresholds, risk caps, and model version
- [x] 8.3 Add scoring component module for signal strength, trend alignment, momentum, breakout/position, relative strength, and risk penalty
- [x] 8.4 Refactor scoring service to generate one prediction per stock/date/horizon/model-version
- [x] 8.5 Add input snapshot builder for quote, factor, signal, freshness, and blocked reasons
- [x] 8.6 Add explanation builder with reproducible components, penalties, evidence, thresholds, and summary
- [x] 8.7 Add rank and percentile assignment per date/horizon/model-version
- [x] 8.8 Add verification service for `PENDING`, `TRACKING`, `VERIFIED`, and `INSUFFICIENT_DATA` transitions
- [x] 8.9 Add replay service for historical date-range scoring backfill
- [x] 8.10 Add calibration report service for score buckets, Top-N groups, component contribution analysis, false positives, and false negatives
- [x] 8.11 Update `scoring_runner.py` with `run`, `backfill`, `verify`, and `report` commands
- [x] 8.12 Add command options for date ranges, horizon filters, stock-code filters, model-version filters, dry-run, and replace
- [x] 8.13 Add tests for model indexes, target trading dates, score generation, blocked inputs, explanation reproducibility, ranking, verification, replay, reports, and look-ahead bias
- [x] 8.14 Run datahub Python validation: `ruff check`, `ruff format --check`, and focused scoring tests

## 9. Score API and UI Implementation Path

- [x] 9.1 Add backend score list API (`GET /api/scores`)
- [x] 9.2 Add backend stock score history API (`GET /api/scores/<code>`)
- [x] 9.3 Add backend score explanation API (`GET /api/scores/<code>/<date>/explanation`)
- [x] 9.4 Add backend score performance API backed by calibration summaries or prediction aggregation (via `/api/score-experiments`)
- [x] 9.5 Update `/api/market/comprehensive` to return `Score5`, `Score20`, and `Score60` summaries
- [x] 9.6 Add backend API tests for score list, history, explanation, performance, and comprehensive market summaries
- [x] 9.7 Update frontend market API types for multi-horizon score summaries
- [x] 9.8 Update Market view to display and sort by `Score5`, `Score20`, and `Score60`
- [x] 9.9 Update stock detail flow to show score history, explanation components, input freshness, and verification status
- [x] 9.10 Run relevant frontend lint/build validation

## 10. OpenClaw Data Access

- [x] 10.1 Document OpenClaw as a downstream read-only consumer of caifubao data (docs/integrations/openclaw.md, AGENTS.md)
- [x] 10.2 Define required API domains: stock master data, quotes, adjusted prices, factors, signals, scores, and data quality (5 domains + recommendations documented)
- [ ] 10.3 Define freshness and blocked-by-quote semantics for downstream analysis gating
- [ ] 10.4 Identify backend API gaps for OpenClaw consumption (no formal gap analysis yet)
- [x] 10.5 Keep OpenClaw analysis logic out of caifubao and avoid direct Mongo coupling (read-only API only, no analysis logic in caifubao)
- [x] 10.6 Define OpenClaw service-token authentication with hashed token storage and read-only scopes (backend/app/model/service_token.py, auth_decorators.py)
- [x] 10.7 Define request audit fields for OpenClaw access, including request id, token id, endpoint, status code, and data-as-of (utils.py request_id, auth decorator tracks last_used_at/last_used_ip)
- [ ] 10.8 Document token expiry, revocation, and future rate-limit expectations (rate-limit docs still needed)

## 11. Phase 0 — Compute-Worker Infrastructure (2 days)

- [ ] 11.1 Create `compute-worker/` service directory with independent Python project (shared model layer from datahub + backend)
- [ ] 11.2 Define `ComputeTask` MongoDB collection schema (task_type, params, status, result, error, timestamps)
- [ ] 11.3 Implement worker loop: poll PENDING tasks → dispatch by task_type → execute → write result
- [ ] 11.4 Add backend API endpoints: `POST /api/tasks` (create), `GET /api/tasks/<id>` (poll result)
- [ ] 11.5 Add K3s Deployment manifest with 5600X nodeAffinity and resource limits
- [ ] 11.6 Define node-role split: cloud node (Flask API, MongoDB, daily pipeline), 5600X node (batch compute only)

## 12. Phase 1 — Score-driven Backtest + Hardening (6 days)

### 12a. Backtest Realism (P0 — immediate fix)

- [ ] 12a.1 Split verification hit_target into `hit_target_close` (conservative, based on `return_at_target`) and `hit_target_intra` (aggressive, based on `max_return`); update calibration reports to use `hit_target_close` as primary
- [ ] 12a.2 Add friction model to backtest engine: commission (0.025%, min 5 CNY), stamp duty (0.1% sell side), slippage (default 0.1%)
- [ ] 12a.3 Report gross return (before friction) and net return (after friction) in backtest output
- [ ] 12a.4 Enable `trade_status` limit-up/down constraints in backtest engine: buy blocked at limit-up, sell blocked at limit-down
- [ ] 12a.5 Handle consecutive limit-day retries: attempt execution each day until filled or condition clears
- [ ] 12a.6 Add CSI 300 buy-and-hold benchmark to all backtest results; report strategy excess return and tracking error

### 12b. Score-driven Strategies

- [ ] 12b.1 Implement `SCORE_THRESHOLD` strategy: buy when Score(N) >= entry_threshold, sell when Score(N) < exit_threshold or stop-loss hit; configurable horizon and thresholds
- [ ] 12b.2 Implement `SCORE_MOMENTUM` strategy: buy when score delta >= N vs previous day, sell on reverse delta
- [ ] 12b.3 Add look-ahead bias guard: score-driven strategies SHALL only read `StockScorePrediction` with `date__lt` or `date__lte` relative to current trading day
- [ ] 12b.4 Add `score_config` and `horizon` fields to `BacktestResult` for traceability
- [ ] 12b.5 Frontend: add score-driven strategy selectors, horizon picker, and parameter fields to backtest create form

### 12c. Multi-stock Backtest

- [ ] 12c.1 Upgrade backtest engine to load quotes for N stocks simultaneously with common trading-day alignment
- [ ] 12c.2 Implement position sizing: equal-weight, score-weighted, max-position-cap rules
- [ ] 12c.3 Implement 100-share lot rounding (整手) for A-share compliance
- [ ] 12c.4 Track per-stock contribution metrics (realized PnL, max drawdown, win rate) alongside portfolio aggregates
- [ ] 12c.5 Add `TOP_N_ROTATION` strategy: rank by score each rebalance day, hold top N with configurable rebalance interval

## 13. Phase 2 — Scoring Scheme Combinatorial Optimization (4 days)

- [ ] 13.1 Implement `GridSearchTask`: given weight-grid + threshold-grid, auto-generate N `ScoreExperiment` runs
- [ ] 13.2 Within grid search, run score-driven backtest (Phase 1) per experiment to capture trading metrics (Sharpe, hit rate, max drawdown)
- [ ] 13.3 Enforce weight-sum constraint (weights must sum to 100; only vary enabled components)
- [ ] 13.4 Implement `ExperimentComparisonReport`: side-by-side metrics table with deltas and statistical significance (DeLong or bootstrap test)
- [ ] 13.5 Rank top-20 configs by Sharpe ratio / hit rate with weight heatmap visualization
- [ ] 13.6 Add multi-horizon consensus/divergence detection (e.g., Score5=BUY ∧ Score60=AVOID → "short-long divergence")
- [ ] 13.7 Backend API: `GET /api/score-experiments/compare?id_a=X&id_b=Y`
- [ ] 13.8 Frontend: experiment comparison view, grid-search result table with heatmap

## 14. Phase 3 — Factor Evaluation Pipeline (4 days)

- [ ] 14.1 Implement `FactorEvaluationService`: compute rank IC, ICIR, quintile returns, correlation matrix, decay curve
- [ ] 14.2 IC time-series: rolling mean and std of rank IC across all evaluation dates
- [ ] 14.3 Quintile analysis: group stocks by factor value quintile, compute mean forward return per quintile; test monotonicity
- [ ] 14.4 Correlation matrix: pairwise Pearson correlation with existing 7 scoring components; flag >0.7 redundancies
- [ ] 14.5 Market-regime split: compute factor IC separately in trending / ranging / volatile regimes
- [ ] 14.6 Factor decay curve: IC of factor vs forward 1/3/5/10/20/60 day returns
- [ ] 14.7 `FactorEvalReport` model: persist IC stats, quintile results, correlation, decay; expose via API
- [ ] 14.8 Frontend: factor evaluation dashboard (IC chart, quintile bar chart, correlation heatmap)
- [ ] 14.9 Implement market-regime classifier (trending / ranging / volatile) based on index data for use in phases 3-5

## 15. Phase 4 — New Technical Factors (5 days)

Each factor follows the same pipeline: compute → factor eval (Phase 3) → integrate as component → grid-search weight (Phase 2) → rolling validate (Phase 5).

- [ ] 15.1 `volume_ratio`: volume / MA20(volume) — interest intensity. Source: `StockDailyQuote`
- [ ] 15.2 `bb_position`: (close − BB_lower) / (BB_upper − BB_lower), BB(20, 2). Source: `StockDailyQuote`
- [ ] 15.3 `atr_ratio`: ATR(14) / close — normalized volatility distinct from raw std-dev. Source: `StockDailyQuote`
- [ ] 15.4 `consecutive_up`: consecutive days where close > open — trend persistence. Source: `StockDailyQuote`
- [ ] 15.5 `turnover_accel`: turnover_rate / MA5(turnover_rate). Source: `StockDailyQuote`
- [ ] 15.6 `gap_ratio`: (open − prev_close) / prev_close — overnight gap strength. Source: `StockDailyQuote`
- [ ] 15.7 `yearly_position`: (close − 52w_low) / (52w_high − 52w_low). Source: `StockDailyQuote`
- [ ] 15.8 `rsi_14`: standard RSI(14). Source: `StockDailyQuote`
- [ ] 15.9 `real_relative_strength`: replace self-proxy with alpha vs CSI 300 / CSI 500 index. Run through Phase 3 eval before integration
- [ ] 15.10 Add FactorRunner steps to compute and store each new factor; update `StockFactorDaily` or create new collection as needed

## 16. Phase 5 — Rolling Cross-validation + Robustness (3 days)

- [ ] 16.1 Implement `RollingValidationTask`: train on year Y, test on Y+1, slide forward, repeat
- [ ] 16.2 Decay analysis: compare train-period Sharpe vs test-period Sharpe per config; flag >20% drop as overfit
- [ ] 16.3 Market-regime split reporting: Bull / Bear / Ranging performance per config; flag regime-specific failures
- [ ] 16.4 Stability check: small weight perturbation → large outcome change? Flag unstable configs
- [ ] 16.5 Generate final recommendation: best config per horizon with confidence interval and regime robustness score

## 17. Phase 6 — Decision Dashboard + Alerts (3 days)

- [ ] 17.1 Daily decision dashboard view: today's top scores, score changes (Δ arrows), position match (held vs recommended), actionable buy/sell
- [ ] 17.2 Score alert detection: score jump ≥ threshold (e.g., Score5: 45 → 82); push to frontend
- [ ] 17.3 Score quality monitoring: rolling 30-day hit rate, decay detection, auto-alert when metrics drop below historical baseline
- [ ] 17.4 Decision journal: log recommended vs executed; track adopted-suggestion P&L
- [ ] 17.5 Score-driven rebalance preview: map today's scores to existing portfolio positions, recommend entries/exits/holds

## 18. Phase 7 — OpenClaw Score-Read + Completion (1 day)

- [ ] 18.1 Add `openclaw:score-read` scope to service-token model and auth decorators
- [ ] 18.2 Expose score-prediction endpoints under OpenClaw integration API (`/api/v1/integrations/openclaw/scores`)
- [ ] 18.3 Include per-component explanation, input-snapshot freshness, and verification metrics in OpenClaw score response
- [ ] 18.4 Enforce that OpenClaw cannot trigger backtests, experiments, or scoring runs (403 on compute endpoints)
- [ ] 18.5 Document complete OpenClaw API surface in `docs/integrations/openclaw.md`
