# Scoring Direction Versioning Tasks

## 1. Config resolution

- [x] 1.1 `get_effective_horizon_config` resolves optional per-horizon `directions`
  into a full per-component map with defaults (components +1, risk_penalty -1).
- [x] 1.2 Validate keys are scored components and values in {-1, 0, 1}; invalid raises.
- [x] 1.3 Absent override => no `directions` key => current math unchanged (locked by tests).

## 2. Ranked application + score semantics

- [x] 2.1 `score_all_stocks_ranked` multiplies each component/penalty rank contribution
  by its resolved direction (defaults identical to develop).
- [x] 2.2 Persisted explanation contribution signs match the score sign.
- [x] 2.3 Floor clamp is polarity-aware: default (no component flip) models keep the
  develop max(0, .) floor so penalized bottom stocks stay at 0.0 (bit-identical
  re-runs); a real component flip (any non-penalty direction -1) opens the floor so
  the model keeps signed, strictly-sortable scores (no all-zero tie). Mirrors research
  evaluator: signed weighted sum, percentile from ranking that sum.
- [x] 2.4 Ranked e2e direction flip: high-momentum stock outranks by default, inverts
  when flipped; full-flip parity test (8 components -1) keeps strict order and inverts
  ranking exactly; default-with-penalty floor test; flip-with-penalty signed test.
- [x] 2.5 Direction override validation: raw values validated in {-1,0,1} BEFORE int
  coercion (floats/bools rejected); non-dict directions rejected at resolution time.

## 3. Flipped-version semantics + operator validation

- [x] 3.1 Flipped-version score/percentile meaning defined (higher score <-> lower raw
  bullishness); percentile-driven BUY/WATCH/AVOID stays well-defined for a full-flip
  model (no tie degeneracy) - locked by tests (flip-with-penalty signed order).
- [x] 3.2 Consumer tooling for flipped (non-positive) scores: calibration_report
  and comparison_report bucket by config-resolved percentile before any
  flipped-direction experiment is validated or promoted; positive-only partial windows
  retain flipped semantics, invalid percentiles fail explicitly, backend run/compare
  paths use the same basis, and frontend labels the returned basis. Absolute-score
  threshold consumers (SCORE_THRESHOLD backtest entry/exit, consensus/openclaw
  thresholds) remain default-direction-only (raw-score semantics); they are out of
  scope for flipped percentile scores.
- [ ] 3.3 Operator validation of one flipped experiment model version: full-market replay
  + calibration comparison vs baseline before any promotion.
- [x] 3.4 Raw-score comparisons stay within same-direction model versions;
  cross-direction promotion comparisons align scale-dependent metrics by percentile.

## 4. Gates

- [x] 4.1 spec-guardian: triggered (scoring semantics) — this change entry.
- [x] 4.2 qa-reviewer on the diff (flag full-flip degeneracy finding — resolved by 2.3/2.4);
  spec-guardian/qa-reviewer/contract-reviewer all GATE_OK on the 3.2 full-chain
  (config-resolved percentile basis), P2 empty-config registry fallback fixed in 518eb48.
- [x] 4.3 branch-conflict check against develop before merge.
- [ ] 4.4 CI green; merge.
