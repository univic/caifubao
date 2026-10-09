## Why

The Factor Lab sweeps many registered factors and forward-return horizons. It
reports Newey-West t-statistics per pair, but currently has no machine-readable
multiple-testing correction. Reading a nominal 5% p-value after trying many
pairs exaggerates the evidence for a historical pattern. A single-factor rerun
also must not implicitly erase previously attempted hypotheses.

## What Changes

- Annotate the research-only `evaluate` JSON with two-sided, asymptotic
  normal p-values derived from its existing Newey-West t-statistics, and a
  Bonferroni family-wise correction across all evaluated factor/horizon pairs.
- Count all attempted pairs, including pairs with unavailable statistics;
  optionally declare a larger full search family with
  `--hypotheses-count` to account for prior/unreported trials. Reject a
  declared count smaller than the pairs actually evaluated.
- Add `--alpha` (default 0.05), validation, and per-pair significance status.
  Insufficient IC dates or missing/nonfinite Newey-West t-values never pass.
- Distinguish exploratory significance from existing economic and walk-forward
  gates. Do not auto-promote any factor to trading.
- Add deterministic tests and operator documentation of approximations and
  limitations.

## Impact

Datahub factor-lab CLI/report and tests; research documentation only.
Frozen panel and its label semantics remain unchanged. No Mongo mutations,
production signal/scoring changes, backend API, trading, or K8s changes.

## Non-goals

- No claim that Bonferroni significance demonstrates a tradable, net-positive
  strategy, market-regime stability, or genuine forward performance.
- No reconstruction of the complete history of previously tried hypotheses.
- No replacement for pre-registration, purge/embargo, or rolling OOS replay.
