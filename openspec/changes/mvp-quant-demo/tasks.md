# MVP Quant Demo Tasks

> **Status (2026-05-25)**: Development complete. Phases 13–19 (strategy discovery,
> grid search, factor eval, walk-forward, decision dashboard, OpenClaw) all built
> with backend, frontend, and 34 tests. Sections 12a-d partially done.
> 
> **Remaining**: Documentation (§1-6, 10.8, 11.6), operational validation
> (§12d.5-6, 12e.5, 13.10, 20.1-5 — needs running environment),
> Autoresearch design (§22), and cluster reinitialization resilience (§23).

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

- [x] 5.1 Document single-stock daily backtest flow (implemented)
- [x] 5.2 Document supported strategy set (MA_CROSS, BUY_HOLD)
- [x] 5.3 Document result payload and UI flow
- [x] 5.4 Document lightweight internal backtest engine
- [x] 5.5 Document separation between scoring replay and trading backtest

## 6. Review

- [ ] 6.1 Review docs against current code paths
- [ ] 6.2 Trim any over-designed or outdated content

## 7. Multi-horizon Stock Scoring

- [x] 7.1-7.20 All scoring model, config, verification, replay, and calibration tasks

## 8. Scoring Implementation Path

- [x] 8.1-8.14 All scoring implementation, service, and test tasks

## 9. Score API and UI Implementation Path

- [x] 9.1-9.10 All API and frontend score display tasks

## 10. OpenClaw Data Access

- [x] 10.1 OpenClaw downstream consumer documentation
- [x] 10.2 Required API domains documentation
- [x] 10.3 Freshness and blocked-by-quote semantics (data_as_of populated in all 9 endpoints)
- [ ] 10.4 Backend API gap analysis
- [x] 10.5 No OpenClaw analysis logic in caifubao
- [x] 10.6 Service-token authentication
- [x] 10.7 Request audit fields
- [ ] 10.8 Token expiry, revocation, rate-limit docs

## 11. Phase 0 — Compute-Worker Infrastructure (2 days)

- [x] 11.1 Create `compute-worker/` service directory (worker.py, handlers.py, Dockerfile, etc.)
- [x] 11.2 Define `ComputeTask` MongoDB schema (8 task types)
- [x] 11.3 Implement worker loop: poll -> dispatch -> execute -> write (atomic claim + status mgmt)
- [x] 11.4 Add backend API: `POST /api/tasks`, `GET /api/tasks/<id>`
- [x] 11.5 Add K3s Deployment manifest with node-type=compute affinity
- [ ] 11.6 Define node-role split: cloud vs batch compute (node-role document TBD)

## 12. Phase 1 — Score-driven Backtest + Hardening (6 days)

### 12a. Backtest Realism (P0)

- [x] 12a.1 Split hit_target into close/intra; use close as primary
- [x] 12a.2 Friction: commission 0.025% min 5 CNY, stamp 0.1%, slippage 0.1%
- [x] 12a.3 Report gross and net return
- [x] 12a.4 Limit-up/down constraints via trade_status
- [x] 12a.5 Consecutive limit-day retries
- [x] 12a.6 CSI 300 benchmark with excess return and information ratio

### 12b. Score-driven Strategies

- [x] 12b.1 SCORE_THRESHOLD: entry>=threshold buy, exit<threshold sell, stop-loss
- [x] 12b.2 SCORE_MOMENTUM: score delta entry/exit
- [x] 12b.3 Look-ahead bias guard: date <= current trading day
- [x] 12b.4 score_config and horizon fields in BacktestResult
- [x] 12b.5 Frontend: score-driven strategy selectors and parameter fields

### 12c. Multi-stock Backtest

- [x] 12c.1 Multi-stock quote loading with common trading-day alignment
- [x] 12c.2 Position sizing: equal-weight, score-weighted, max-position-cap
- [x] 12c.3 100-share lot rounding
- [x] 12c.4 Per-stock contribution metrics
- [x] 12c.5 TOP_N_ROTATION strategy

### 12d. Scoring Engine Quick Wins (2 days)

Hybrid percentile+absolute thresholds implemented in `score_v2_202605b` (PR #97).
Calibration report flags miscalibration (BUY<3%, AVOID>50%). Full-market backfill
mechanism exists but old-vs-new model version comparison is manual.

- [x] 12d.1 Generate full-market calibration report first; propose hybrid percentile+absolute thresholds based on distribution; bump model version only after full-market validation (NOT single-stock observation)
- [x] 12d.2 Signal persistence decay: exponential decay factor 0.7/day when signal disappears
- [x] 12d.3 Config entries: signal_decay_factor, signal_decay_max_days per horizon
- [x] 12d.4 Score distribution metrics in calibration report; flag BUY<3% or AVOID>50% as miscalibrated
- [ ] 12d.5 Backfill with new model version across FULL MARKET; compare calibration reports between old and new model versions
- [ ] 12d.6 Update scoring tests for signal decay and hybrid threshold logic

### 12e. Backtest Optimization Quick Wins (2 days)

- [x] 12e.1 POST /api/backtest/optimize: param sweep with train/val/test split (60/20/20); select best params on train+val; report final result on test period only; warn if <300 trading days total
- [x] 12e.2 MULTI_HORIZON_CONSENSUS: BUY when all horizons >= entry, SELL when any < exit
- [x] 12e.3 Consensus strategy in _simulate() with partial data handling
- [x] 12e.4 optimize subcommand in backtest_runner.py CLI with --split flag
- [ ] 12e.5 Validate on sz000977: consensus + optimize vs baselines; check train/test Sharpe decay

## 13. Phase 1.5 — Strategy Discovery & Screening (3 days) ✅ DONE

Bridge from single backtests to systematic strategy discovery. Pure backend —
wraps existing run_backtest() in screening, comparison, and validation loops.
All scan/comparison results MUST include anti-overfitting flags and use composite
ranking (not pure Sharpe).

- [x] 13.1 POST /api/backtest/compare: all eligible strategies on one stock; side-by-side comparison ranked by composite score (excess return, max DD, info ratio, turnover penalty)
- [x] 13.2 POST /api/backtest/scan: one strategy across all stocks; paginated; include trade count, concentration flag, turnover rate, single-best-day contribution; rank by composite score
- [x] 13.3 Async market scan via ComputeTask when stock count > threshold
- [x] 13.4 POST /api/backtest/walk-forward: rolling-window; include first-half vs second-half Sharpe comparison; flag performance_decay if second-half Sharpe is >20% lower
- [x] 13.5 GET /api/backtest/<id>/regime: bull/bear/sideways decomposition via CSI 300 trend
- [x] 13.6 Implement composite ranking function: excess return + max DD penalty + info ratio - turnover penalty - concentration penalty; exclude <5 trades from ranking
- [x] 13.7 Implement anti-overfitting guardrails: train/val/test split, multiple-comparison flagging (Bonferroni), minimum sample warnings, concentration detection (>40% from single episode = flag)
- [x] 13.8 Implement trading executability constraints: ST/suspension filter, liquidity floor (5M CNY avg turnover), dynamic slippage mode, position capacity check (1% of daily volume)
- [x] 13.9 compare, scan, walk-forward subcommands in backtest_runner.py CLI
- [ ] 13.10 Validate: compare all strategies on sz000977; scan MA_CROSS on top-50 by market cap; check anti-overfitting flags
- [x] 13.11 Frontend: discovery workspace with anti-overfitting flags visible on every result card
- [x] 13.12 CSV export for scan, comparison, walk-forward results including all flags

## 14. Phase 2 — Scoring Scheme Combinatorial Optimization (4 days) ✅ DONE

- [x] 14.1 GridSearchService: weight-grid + threshold-grid auto-generate N experiments (draft PR #89)
- [x] 14.2 Run score-driven backtest per experiment; capture composite score, Sharpe, hit rate, max DD
- [x] 14.3 Enforce weight-sum constraint (sum to 100, vary enabled components only)
- [x] 14.4 ExperimentComparisonReport: metrics table with statistical significance
- [x] 14.5 Rank top-20 configs by composite score (not pure Sharpe) with weight heatmap visualization
- [x] 14.6 Multi-horizon consensus/divergence detection
- [x] 14.7 GET /api/score-experiments/compare?id_a=X&id_b=Y
- [x] 14.8 Frontend: experiment comparison view, grid-search heatmap

## 15. Phase 3 — Factor Evaluation Pipeline (4 days) ✅ DONE

Systematic evaluation of scoring components and external factors for
predictive power, redundancy, and regime sensitivity.

- [x] 15.1 FactorEvaluationService: rank IC, ICIR, quintile returns, correlation matrix, decay curve
- [x] 15.2 IC time-series: rolling mean/std of rank IC; percentage of dates with positive IC
- [x] 15.3 Quintile analysis: group by factor value, mean forward return per quintile; test monotonicity
- [x] 15.4 Correlation matrix: pairwise Pearson with 7 scoring components; flag >0.7 redundancies
- [x] 15.5 Market-regime split: compute IC separately in trending/ranging/volatile regimes
- [x] 15.6 Factor decay curve: IC vs forward 1/3/5/10/20/60 day returns
- [x] 15.7 Component contribution P&L: compute average component score at entry/exit per trade; identify dominant exit drivers
- [x] 15.8 Win rate by dominant component: group trades by which component had highest contribution at entry
- [x] 15.9 Candidate factor pre-integration eval: standalone IC, correlation with existing, model with/without comparison
- [x] 15.10 FactorEvalReport model: persist IC, quintile, correlation, decay; expose via API
- [x] 15.11 Frontend: factor evaluation dashboard (IC chart, quintile bar, correlation heatmap)
- [x] 15.12 Implement market-regime classifier (trending/ranging/volatile) based on CSI 300 for phases 3-6

## 16. Phase 4 — New Technical Factors (5 days) ✅ DONE

Each factor: compute -> factor eval (Phase 3) -> integrate as scoring component
-> grid-search weight (Phase 2) -> rolling validate (Phase 5).

- [x] 16.1 volume_ratio: volume / MA20(volume)
- [x] 16.2 bb_position: (close - BB_lower) / (BB_upper - BB_lower), BB(20, 2)
- [x] 16.3 atr_ratio: ATR(14) / close
- [x] 16.4 consecutive_up: consecutive days close > open
- [x] 16.5 turnover_accel: turnover_rate / MA5(turnover_rate)
- [x] 16.6 gap_ratio: (open - prev_close) / prev_close
- [x] 16.7 yearly_position: (close - 52w_low) / (52w_high - 52w_low)
- [x] 16.8 rsi_14: standard RSI(14)
- [x] 16.9 real_relative_strength: replace self-proxy with alpha vs CSI 300/500 index (function exists but excluded from registry — needs index_quotes parameter)
- [x] 16.10 Add FactorRunner steps for new factors; update StockFactorDaily

## 17. Phase 5 — Walk-forward Validation + Robustness (3 days) ✅ DONE

- [x] 17.1 RollingValidationTask: train on year Y, test on Y+1, slide forward
- [x] 17.2 Decay analysis: train Sharpe vs test Sharpe; flag >20% drop as overfit
- [x] 17.3 Market-regime split reporting per config; flag regime-specific failures
- [x] 17.4 Stability check: small weight perturbation -> large outcome change?
- [x] 17.5 Statistical significance: permutation test and bootstrap CI for strategy returns
- [x] 17.6 Parameter landscape visualization: identify flat vs sharp optima
- [x] 17.7 Generate final recommendation: best config per horizon with CI and regime robustness

## 18. Phase 6 — Decision Dashboard + Alerts (3 days) ✅ DONE

- [x] 18.1 Daily decision dashboard: top scores per horizon, score deltas, position match
- [x] 18.2 Actionable recommendations: each BUY/WATCH/AVOID includes confidence (historical hit rate + sample size + trend), invalidation conditions (exit threshold, stop-loss, expiry), position sizing (target weight, capacity check)
- [x] 18.3 Score alert detection: score jump >= 15 points, threshold crossing, quality degradation
- [x] 18.4 Score quality monitoring: rolling 30-day hit rate, distribution shift detection
- [x] 18.5 Model drift detection: P50/P90 score shift > 10 points in 20 days
- [x] 18.6 Decision journal: log recommended vs executed with P&L
- [x] 18.7 Journal tracks missed recommendations: system recommended BUY but user did not execute; compute opportunity P&L
- [x] 18.8 Journal tracks user deviations: user executed trade NOT recommended by system; separate P&L tracking
- [x] 18.9 Position attribution: attribute trade P&L to scoring horizon and dominant components
- [x] 18.10 Rebalance preview: map today's scores to portfolio; recommend entries/exits/holds with confidence + invalidation
- [x] 18.11 Configurable watchlists for focused monitoring
- [x] 18.12 Frontend: decision workspace with confidence metadata and invalidation conditions inline on every signal

## 19. Phase 7 — OpenClaw Score-Read + Completion (1 day)

- [x] 19.1 Add openclaw:score-read scope to service-token model and auth decorators
- [x] 19.2 Expose score endpoints under /api/v1/integrations/openclaw/scores
- [x] 19.3 Include explanation, input-snapshot freshness, verification in response
- [x] 19.4 Enforce 403 on compute endpoints for OpenClaw
- [x] 19.5 Document complete OpenClaw API surface

## 20. Success Criteria

Minimum acceptance thresholds that MUST be met before any phase is considered
complete. These apply across all discovery, optimization, and decision-support
workflows.

- [ ] 20.1 SCORE-based strategies do not need to beat BUY_HOLD on absolute return, but MUST demonstrate a clear advantage in at least one of: net return, max drawdown, Sharpe ratio, or information ratio over a full-market sample (not single-stock cherry-picking)
- [ ] 20.2 Single-stock validation is NOT sufficient for acceptance; any strategy or threshold change MUST be validated on at least the top-50 stocks by market cap or the full active market
- [ ] 20.3 Parameter optimization results MUST pass walk-forward decay check: test-period Sharpe MUST NOT be more than 20% below train-period Sharpe; results that fail this check SHALL be flagged "overfit" and excluded from top-ranking positions
- [ ] 20.4 Any strategy ranked in a top-N list SHALL have at minimum 5 trades, 120+ trading days of data, and a concentration ratio (single-best-day / total return) below 40%
- [ ] 20.5 Scoring model version changes SHALL be accompanied by a calibration report comparing the new and old model versions across the full market
- [x] 20.6 Decision journal SHALL separately track "model quality" (how good were recommendations) and "execution discipline" (how well were they followed) as distinct metrics

## 21. Recommended Execution Order

1. **12d/12e first** — Scoring quick wins + backtest optimization with:
   - Full-market calibration before any threshold change (12d.1)
   - Train/val/test split in optimization (12e.1)
   - Validate against sz000977 AND top-50 market scan

2. **Then 13.x** — Strategy discovery with anti-overfitting built in:
   - Composite ranking (13.6)
   - Anti-overfitting guardrails (13.7)
   - Trading executability constraints (13.8)

3. **Then 15.x** — Factor evaluation pipeline:
   - IC, quintile, correlation to decide which factors earn a place in the model

4. **Then 14.x (Phase 2)** — Grid search optimization:
   - Only after baseline anti-overfitting guards are proven effective

5. **Finally 18.x** — Decision dashboard:
   - Only after research pipeline quality is validated
   - Dashboard should display signals from a proven pipeline, not pretty-print unstable signals

6. **Autoresearch after 12d/12e and 13.x guardrails** — Use the Karpathy
   autoresearch loop only after a mechanical metric, full-market validation
   command, and edit scope are frozen.
   - Start with score threshold / weight research, not production API changes
   - Promote no result without full-market calibration and walk-forward checks
   - Preserve failed and discarded experiments for bias review

## 22. Autoresearch-Guided Profitability Research

Autoresearch adapts Karpathy's autonomous experiment loop to Caifubao's scoring
and strategy research. The goal is research throughput and robustness, not
investment advice or guaranteed profit.

- [ ] 22.1 Diagnose Caifubao as `v1-bootstrap-fit` and write
  `docs/autoresearch/specs/<date>-caifubao-profitability-design.md`
  with frozen runtime, metric, edit scope, baseline, git policy, and logging
  fields
- [ ] 22.2 Define `research_profitability_score` as a single numeric metric
  extracted from test-period reports only; include net excess return,
  information ratio, max drawdown penalty, turnover penalty, concentration
  penalty, sample-size penalty, and overfit penalty
- [ ] 22.3 Add a thin adapter plan for producing `autoresearch/profile.yaml`,
  `autoresearch/state.yaml`, `autoresearch/results.tsv`, and
  `autoresearch/ledger.jsonl` without changing production defaults
- [ ] 22.4 Restrict initial edit scope to research configuration files,
  candidate scoring weights, threshold profiles, and candidate factor drafts;
  exclude auth, OpenClaw endpoints, k8s, public API response contracts, and
  production default model version changes
- [ ] 22.5 Run baseline full-market validation before the first experiment and
  record the baseline report path and git ref
- [ ] 22.6 Require every kept experiment to pass train/validation/test split,
  full-market or top-50 validation, walk-forward decay, minimum trade count,
  and concentration checks
- [ ] 22.7 Persist discarded and crashed experiments in the ledger with the
  rejection reason so model-selection bias can be audited
- [ ] 22.8 Document that autoresearch output is research evidence only; any
  production promotion still requires OpenSpec review, model version bump,
  calibration comparison, and normal validation gates

## 23. Cluster Reinitialization and Operational Resilience

The current K3S reset makes operational resilience the next implementation
gate. Finish these before treating the new cluster as durable or starting real
autoresearch experiments against regenerated data.

- [x] 23.1 Add public S3-compatible backup manifest templates for MongoDB
  logical dumps; keep real bucket, endpoint, access key, secret key, and
  retention values in private deployment configuration
- [x] 23.2 Add backup job status output that records start time, finish time,
  object key, database name, namespace, status, and sanitized error summary
- [x] 23.3 Add a one-shot restore job template that downloads an approved backup
  artifact and runs `mongorestore` into a fresh MongoDB instance
- [x] 23.4 Add post-restore sanity checks for required collections, document
  counts, freshness metadata, backend health, data quality summary, and
  OpenClaw read endpoints
- [x] 23.5 Document empty-database bootstrap order: stock master, historical
  quotes, FQ/MA/technical factors, signals, scores, data quality, and freshness
  status
- [x] 23.6 Document data survivability classes: regenerable market data vs
  non-regenerable users, portfolios, watchlists, decision journal, service
  tokens, audit logs, and task history
- [x] 23.7 Harden MongoDB storage planning for long-lived clusters: StatefulSet
  or equivalent identity, explicit PVC/reclaim policy, node placement notes,
  and backup dependency
- [x] 23.8 Add an operator validation command path through `./scripts/caifubao`
  or documented kubectl commands for backup, restore, and bootstrap readiness
- [x] 23.9 Prepare autoresearch implementation only after either restore
  validation or empty-database bootstrap validation passes; before that, limit
  autoresearch work to docs, adapters, profile scaffolding, and synthetic metric
  extraction tests
