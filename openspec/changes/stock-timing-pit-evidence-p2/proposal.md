# Stock Timing PIT Input Evidence P2a

## Why

The P1 timing replay adapter intentionally rejects ranked predictions that
cannot prove their historical cohort and input cutoff. Retrospectively reading
mutable quote, factor, signal, and stock-master rows cannot close that gap: a
cohort-only hash would still allow corrected or future-recomputed inputs to be
mislabelled `FRESH`.

## What Changes

- Add operator-only two-phase capture: freeze D's universe before D opens, then
  freeze D's complete ranked-scoring read set after D closes and before the
  next session opens.
- Publish the read set as a research-grade immutable artifact conforming to the
  repository's shared artifact contract, including source hashes, config and
  build revision, field manifest, canonical hash, and deterministic identity.
- Capture the full current-active universe in the pre-open phase, so
  suspended names remain explicit members and later delisting cannot erase
  them from the frozen evidence.
- Do not backdate capture, mutate predictions, or mark any prediction `FRESH`
  in this slice. A follow-on P2b will consume the artifact to generate bound
  ranked predictions for P1.

## Impact

- Affected code: a new pure artifact/capture module under
  `datahub/app/lib/scoring_engine/`, `datahub/app/jobs/scoring_runner.py`, tests,
  datahub image revision injection, and operator docs.
- No public API, authentication, scoring formula, database schema, scheduled
  scoring path, or existing prediction changes.
- The output is research-only evidence, not a strategy result or investment
  recommendation.
