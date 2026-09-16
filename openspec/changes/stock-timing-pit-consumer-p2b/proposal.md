# Stock Timing PIT Artifact Consumer P2b

## Why

P2a can freeze the complete point-in-time ranked-scoring read set, but it does
not consume that evidence or create predictions. P1 therefore still has no
way to accept a historical ranked prediction as `FRESH`: existing scoring
commands re-read mutable Mongo rows and do not bind their output to the frozen
universe and input artifact.

## What Changes

- Add an artifact-only ranked-scoring adapter that fully revalidates the P2a
  universe and input artifacts, verifies their model/session/universe binding,
  and computes every score without reading quote, factor, signal, industry, or
  stock-master collections.
- Produce P1-compatible prediction provenance. Each prediction separately
  binds the immutable P2a input artifact and proves inclusion in a per-horizon
  Merkle root over the complete generated cohort; D close, model/config, both
  artifact identities, and producer/consumer revisions are covered by each
  leaf. Usable ranked predictions are marked `FRESH` only after all checks pass.
- Keep calculation dry-run by default. An explicit publish option preflights
  that the captured model pin is still ACTIVE/ranked with the same config and
  rejects every natural-key collision before a single insert-only Mongo write.
- Emit a deterministic result artifact. After exclusive serialization, emit
  the exact P1 daily `prediction_cohorts[D]` handoff record containing both the
  result file SHA-256 and the horizon prediction root.
- Strengthen P1 to verify a prediction's versioned Merkle inclusion proof
  before using a `FRESH` signal. Do not alter scheduled/live scoring, backfill,
  verification, API, or strategy execution.

## Impact

- Affected code: a new consumer/integrity helper under
  `datahub/app/lib/scoring_engine/`, `datahub/app/jobs/scoring_runner.py`, P1
  `timing_replay.py`, focused P1/P2 tests, operator documentation, and the
  shared progress log.
- Scoring formula and ranked normalization semantics remain the existing
  production semantics; only the input source and provenance/publish path are
  new.
- This is an intentional P1 evidence-contract strengthening: a `FRESH` row
  without the new proof fails closed. There is no database schema migration or
  automatic trading. Output remains research evidence, not an investment
  recommendation.
