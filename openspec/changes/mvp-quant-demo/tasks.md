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
- [ ] 10.3 Freshness and blocked-by-quote semantics
- [ ] 10.4 Backend API gap analysis
- [x] 10.5 No OpenClaw analysis logic in caifubao
- [x] 10.6 Service-token authentication
- [x] 10.7 Request audit fields
- [ ] 10.8 Token expiry, revocation, rate-limit docs

## 11. Phase 0 — Compute-Worker Infrastructure (2 days)

- [ ] 11.1 Create `compute-worker/` service directory
- [ ] 11.2 Define `ComputeTask` MongoDB schema
- [ ] 11.3 Implement worker loop: poll -> dispatch -> execute -> write
- [ ] 11.4 Add backend API: `POST /api/tasks`, `GET /api/tasks/<id>`
- [ ] 11.5 Add K3s Deployment manifest with 5600X affinity
- [ ] 11.6 Define node-role split: cloud vs batch compute

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
- [ ] 12b.5 Frontend: score-driven strategy selectors and parameter fields

### 12c. Multi-stock Backtest

- [x] 12c.1 Multi-stock quote loading with common trading-day alignment
- [x] 12c.2 Position sizing: equal-weight, score-weighted, max-position-cap
- [x] 12c.3 100-share lot rounding
- [ ] 12c.4 Per-stock contribution metrics
- [x] 12c.5 TOP_N_ROTATION strategy

### 12d. Scoring Engine Quick Wins (2 days)

Based on live backtest findings (2026-05-18: sz000977 Score5 median=21, only 1% BUY).

- [ ] 12d.1 Lower Score5 default thresholds 70/50 -> 60/40; bump model version to score_v2_202606
- [ ] 12d.2 Signal persistence decay: exponential decay factor 0.7/day when signal disappears
- [ ] 12d.3 Config entries: signal_decay_factor, signal_decay_max_days per horizon
- [ ] 12d.4 Score distribution metrics in calibration report; flag BUY<5% or AVOID>40%
- [ ] 12d.5 Backfill with new model version and re-run SCORE_THRESHOLD validation on sz000977
- [ ] 12d.6 Update scoring tests for signal decay and new thresholds

### 12e. Backtest Optimization Quick Wins (2 days)

- [ ] 12e.1 POST /api/backtest/optimize: param sweep, best by Sharpe
- [ ] 12e.2 MULTI_HORIZON_CONSENSUS: BUY when all horizons >= entry, SELL when any < exit
- [ ] 12e.3 Consensus strategy in _simulate() with partial data handling
- [ ] 12e.4 optimize subcommand in backtest_runner.py CLI
- [ ] 12e.5 Validate on sz000977: consensus + optimize vs baselines

## 13. Phase 1.5 — Strategy Discovery & Screening (3 days)

Bridge from single backtests to systematic strategy discovery. Pure backend —
wraps existing run_backtest() in screening, comparison, and validation loops.

- [ ] 13.1 POST /api/backtest/compare: all eligible strategies on one stock; side-by-side comparison
- [ ] 13.2 POST /api/backtest/scan: one strategy across all stocks; paginated, sorted by Sharpe
- [ ] 13.3 Async market scan via ComputeTask when stock count > threshold
- [ ] 13.4 POST /api/backtest/walk-forward: rolling-window with configurable window/step; stability score
- [ ] 13.5 GET /api/backtest/<id>/regime: bull/bear/sideways decomposition via CSI 300 trend
- [ ] 13.6 compare and scan subcommands in backtest_runner.py CLI
- [ ] 13.7 Validate: compare all strategies on sz000977; scan MA_CROSS on top-50; regime breakdown on best strategy
- [ ] 13.8 Frontend: discovery workspace with screen -> compare -> walk-forward -> regime workflow
- [ ] 13.9 CSV export for scan, comparison, walk-forward results

## 14. Phase 2 — Scoring Scheme Combinatorial Optimization (4 days)

- [ ] 14.1 GridSearchTask: weight-grid + threshold-grid auto-generate N experiments
- [ ] 14.2 Run score-driven backtest per experiment; capture Sharpe, hit rate, max DD
- [ ] 14.3 Enforce weight-sum constraint (sum to 100, vary enabled components only)
- [ ] 14.4 ExperimentComparisonReport: metrics table with statistical significance
- [ ] 14.5 Rank top-20 configs by Sharpe with weight heatmap visualization
- [ ] 14.6 Multi-horizon consensus/divergence detection
- [ ] 14.7 GET /api/score-experiments/compare?id_a=X&id_b=Y
- [ ] 14.8 Frontend: experiment comparison view, grid-search heatmap

## 15. Phase 3 — Factor Evaluation Pipeline (4 days)

Systematic evaluation of scoring components and external factors for
predictive power, redundancy, and regime sensitivity.

- [ ] 15.1 FactorEvaluationService: rank IC, ICIR, quintile returns, correlation matrix, decay curve
- [ ] 15.2 IC time-series: rolling mean/std of rank IC; percentage of dates with positive IC
- [ ] 15.3 Quintile analysis: group by factor value, mean forward return per quintile; test monotonicity
- [ ] 15.4 Correlation matrix: pairwise Pearson with 7 scoring components; flag >0.7 redundancies
- [ ] 15.5 Market-regime split: compute IC separately in trending/ranging/volatile regimes
- [ ] 15.6 Factor decay curve: IC vs forward 1/3/5/10/20/60 day returns
- [ ] 15.7 Component contribution P&L: compute average component score at entry/exit per trade; identify dominant exit drivers
- [ ] 15.8 Win rate by dominant component: group trades by which component had highest contribution at entry
- [ ] 15.9 Candidate factor pre-integration eval: standalone IC, correlation with existing, model with/without comparison
- [ ] 15.10 FactorEvalReport model: persist IC, quintile, correlation, decay; expose via API
- [ ] 15.11 Frontend: factor evaluation dashboard (IC chart, quintile bar, correlation heatmap)
- [ ] 15.12 Implement market-regime classifier (trending/ranging/volatile) based on CSI 300 for phases 3-6

## 16. Phase 4 — New Technical Factors (5 days)

Each factor: compute -> factor eval (Phase 3) -> integrate as scoring component
-> grid-search weight (Phase 2) -> rolling validate (Phase 5).

- [ ] 16.1 volume_ratio: volume / MA20(volume)
- [ ] 16.2 bb_position: (close - BB_lower) / (BB_upper - BB_lower), BB(20, 2)
- [ ] 16.3 atr_ratio: ATR(14) / close
- [ ] 16.4 consecutive_up: consecutive days close > open
- [ ] 16.5 turnover_accel: turnover_rate / MA5(turnover_rate)
- [ ] 16.6 gap_ratio: (open - prev_close) / prev_close
- [ ] 16.7 yearly_position: (close - 52w_low) / (52w_high - 52w_low)
- [ ] 16.8 rsi_14: standard RSI(14)
- [ ] 16.9 real_relative_strength: replace self-proxy with alpha vs CSI 300/500 index
- [ ] 16.10 Add FactorRunner steps for new factors; update StockFactorDaily

## 17. Phase 5 — Walk-forward Validation + Robustness (3 days)

- [ ] 17.1 RollingValidationTask: train on year Y, test on Y+1, slide forward
- [ ] 17.2 Decay analysis: train Sharpe vs test Sharpe; flag >20% drop as overfit
- [ ] 17.3 Market-regime split reporting per config; flag regime-specific failures
- [ ] 17.4 Stability check: small weight perturbation -> large outcome change?
- [ ] 17.5 Statistical significance: permutation test and bootstrap CI for strategy returns
- [ ] 17.6 Parameter landscape visualization: identify flat vs sharp optima
- [ ] 17.7 Generate final recommendation: best config per horizon with CI and regime robustness

## 18. Phase 6 — Decision Dashboard + Alerts (3 days)

- [ ] 18.1 Daily decision dashboard: top scores per horizon, score deltas, position match
- [ ] 18.2 Score alert detection: score jump >= 15 points, threshold crossing, quality degradation
- [ ] 18.3 Score quality monitoring: rolling 30-day hit rate, distribution shift detection
- [ ] 18.4 Model drift detection: P50/P90 score shift > 10 points in 20 days
- [ ] 18.5 Decision journal: log recommended vs executed with P&L tracking
- [ ] 18.6 Position attribution: attribute trade P&L to scoring horizon and dominant components
- [ ] 18.7 Rebalance preview: map today's scores to portfolio; recommend entries/exits/holds
- [ ] 18.8 Configurable watchlists for focused monitoring
- [ ] 18.9 Frontend: integrated decision workspace with dashboard, alerts, journal

## 19. Phase 7 — OpenClaw Score-Read + Completion (1 day)

- [ ] 19.1 Add openclaw:score-read scope to service-token model and auth decorators
- [ ] 19.2 Expose score endpoints under /api/v1/integrations/openclaw/scores
- [ ] 19.3 Include explanation, input-snapshot freshness, verification in response
- [ ] 19.4 Enforce 403 on compute endpoints for OpenClaw
- [ ] 19.5 Document complete OpenClaw API surface
