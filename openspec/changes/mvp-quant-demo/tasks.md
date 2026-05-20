# MVP Quant Demo Tasks

## Status Key
- `[x]` = implemented + merged into develop + deployed
- `[~]` = implemented, in draft PR, not yet merged
- `[ ]` = not started / pending

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

- [x] 5.1-5.5 All backtest MVP tasks

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

- [x] 10.1, 10.2, 10.5-10.7 Core docs + auth
- [ ] 10.3, 10.4, 10.8 Remaining docs

## 11. Phase 0 — Compute-Worker Infrastructure

- [ ] 11.1-11.6 Not started — entire compute-worker module

## 12. Phase 1 — Score-driven Backtest + Hardening

### 12a. Backtest Realism (P0)
- [x] 12a.1-12a.6 All done — friction, limits, benchmark

### 12b. Score-driven Strategies
- [x] 12b.1-12b.4 SCORE_THRESHOLD, SCORE_MOMENTUM, look-ahead, traceability
- [ ] 12b.5 Frontend: score-driven selectors

### 12c. Multi-stock Backtest
- [x] 12c.1-12c.3, 12c.5 Multi-stock engine, sizing, lots, TOP_N_ROTATION
- [ ] 12c.4 Per-stock contribution metrics

### 12d. Scoring Engine Quick Wins (merged PR #83)
- [~] 12d.1 Full-market calibration report — code ready (distribution stats in calibration_report.py), needs K3s run with enough VERIFIED predictions
- [x] 12d.2 Signal persistence decay in components.py
- [x] 12d.3 Config entries: signal_decay_factor / max_days per horizon
- [x] 12d.4 Score distribution metrics + miscalibration flags in calibration report
- [ ] 12d.5 Backfill full market with new model version, compare reports
- [ ] 12d.6 Update scoring tests for signal decay

### 12e. Backtest Optimization (merged PR #84)
- [x] 12e.1 POST /api/backtest/optimize with train/val/test split
- [x] 12e.2 MULTI_HORIZON_CONSENSUS strategy
- [x] 12e.3 Consensus in _simulate() with partial data handling
- [x] 12e.4 optimize subcommand in backtest_runner.py CLI
- [ ] 12e.5 Validate on sz000977 — deploy + run on K3s

## 13. Phase 1.5 — Strategy Discovery (PR #86 draft)

- [~] 13.1 POST /api/backtest/compare — with composite ranking
- [~] 13.2 POST /api/backtest/scan — with anti-overfitting flags
- [ ] 13.3 Async market scan via ComputeTask
- [~] 13.4 POST /api/backtest/walk-forward — with stability + decay flag
- [~] 13.5 GET /api/backtest/<id>/regime — with per-regime return_pct
- [~] 13.6 Composite ranking function in backtest_service.py
- [~] 13.7 Anti-overfitting flags (concentration, low_sample, insufficient_period)
- [ ] 13.8 Trading executability constraints (ST filter, liquidity, dynamic slippage)
- [~] 13.9 CLI compare-all / scan subcommands
- [ ] 13.10 Validate — deploy + run on K3s
- [ ] 13.11 Frontend: discovery workspace
- [ ] 13.12 CSV export

## 14. Phase 2 — Grid Search (PR #89 draft)

- [~] 14.1 GridSearchService: weight-grid × threshold-grid → experiments
- [ ] 14.2 Backtest per experiment (via ScoreExperimentService)
- [ ] 14.3 Weight-sum constraint enforcement
- [x] 14.4 ExperimentComparisonReport (existing score_experiments.py)
- [ ] 14.5 Rank top-20 by composite score with heatmap
- [ ] 14.6 Multi-horizon consensus/divergence detection
- [x] 14.7 GET /api/score-experiments/compare (existing)
- [ ] 14.8 Frontend: experiment heatmap

## 15. Phase 3 — Factor Evaluation (PR #87 draft)

- [x] 15.1 FactorEvaluationService (factor_eval.py, pre-existing 299 lines)
- [x] 15.2-15.6 IC time-series, quintile, correlation, decay (in factor_eval.py)
- [~] 15.7 GET /api/backtest/<id>/component-contribution
- [ ] 15.8 Win rate by dominant component (partially in component-contribution)
- [~] 15.9 POST /api/backtest/evaluate-factor (API for FactorEvaluationService)
- [ ] 15.10 FactorEvalReport model persist + API
- [ ] 15.11 Frontend: factor evaluation dashboard
- [ ] 15.12 Market-regime classifier

## 16. Phase 4 — New Technical Factors

- [ ] 16.1-16.10 All not started — 9 new factors (volume_ratio, RSI, BB, etc.)

## 17. Phase 5 — Validation + Robustness

- [ ] 17.1 RollingValidationTask
- [ ] 17.2-17.7 All not started — decay analysis, significance, parameter landscape

## 18. Phase 6 — Decision Dashboard (PR #88 draft)

- [~] 18.1 GET /api/decisions/dashboard — with confidence + invalidation
- [~] 18.2 Actionable recommendations (confidence metadata, invalidation)
- [x] 18.3 Score alert detection (existing /api/decisions/alerts)
- [x] 18.4 Score quality monitoring (existing /api/decisions/quality)
- [~] 18.5 Model drift detection (P50/P90 shift, added to quality)
- [ ] 18.6-18.12 Not started — journal, attribution, rebalance, watchlists, frontend

## 19. Phase 7 — OpenClaw Score-Read

- [ ] 19.1-19.5 Not started

## 20. Success Criteria

- [ ] 20.1-20.6 None met — require K3s deployment + full-market validation

---

## Current Draft PRs (4 open)

| PR | Phase | Key Features | Status |
|----|-------|-------------|--------|
| #86 | 13.x | compare/scan/walk-forward/regime + composite | review |
| #87 | 15.x | component-contribution + factor-evaluate | review |
| #88 | 18.x | dashboard + confidence + model drift | review |
| #89 | 14.x | grid-search auto-generate experiments | review |

## Blockers

1. **12d.1/12d.5**: Insufficient VERIFIED predictions in dev — verification only ran once (17 records). Need regular verification cron + backfill to get meaningful calibration reports.
2. **12e.5 + 13.10**: All features work in CI but haven't been deployed + validated on K3s dev with real data.

## Recommended Next Steps

1. **Merge draft PRs** (86→87→88→89) in dependency order
2. **Fix dev data pipeline**: regular verification runs, data sync cron health
3. **Deploy & validate**: run full-market calibration report → hybrid thresholds
4. **Phase 4 (16.x)**: Implement 1-2 new technical factors (volume_ratio, RSI)
5. **Phase 5 (17.x)**: Rolling cross-validation + significance tests
