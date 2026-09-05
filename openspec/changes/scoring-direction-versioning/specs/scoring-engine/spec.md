# Scoring Component-Direction Versioning

## ADDED Requirements

### Requirement: Per-horizon component directions are configurable

The scoring config MUST accept an optional per-horizon `directions` override
mapping each scored component/penalty to a direction in {-1, 0, 1}, resolved
into a complete per-component direction map. Absent an override, no directions
key exists and scoring math is unchanged.

#### Scenario: Default model has no directions key

- GIVEN a horizon config without a directions override
- WHEN `get_effective_horizon_config` resolves it
- THEN the returned config has no `directions` key
- AND component contributions use the default polarity (components positive,
  risk_penalty negative)

#### Scenario: Flip override resolves for every component

- GIVEN a directions override flipping three components to -1
- WHEN the horizon config is resolved
- THEN those components resolve to -1
- AND untouched components keep their default polarity
- AND risk_penalty stays -1

#### Scenario: Invalid direction rejected

- GIVEN a directions override with an unknown component or a value outside
  {-1, 0, 1}
- WHEN the horizon config is resolved
- THEN resolution raises a ValueError

### Requirement: Ranked score applies resolved component directions

The ranked market-wide scoring path MUST multiply each component's and each
penalty's rank contribution by its resolved direction (default: components
+1, penalties -1, identical to prior behavior), and MUST sign the persisted
explanation contributions to match the score.

#### Scenario: Flipped direction inverts ranking

- GIVEN two stocks where stock A has higher raw momentum than stock B
- WHEN momentum (and its aliases) are flipped to -1 and market-wide scoring runs
- THEN stock A scores LOWER than stock B
- AND the same two stocks score in the opposite order under default directions

#### Scenario: Full flip stays sortable

- GIVEN every component flipped to -1 (flip_wide full reversal)
- WHEN market-wide scoring runs
- THEN scores remain signed (non-positive) and strictly ordered across the
  cohort - never a tie of zeros
- AND percentile-based BUY/WATCH/AVOID remains well-defined

### Requirement: Flipped-direction models are distinct and compared within direction

A model version configured with flipped component directions MUST be treated
as a distinct model whose score/percentile meaning is inverted relative to the
default direction (higher score means lower raw bullishness). Raw-score
comparisons MUST restrict to same-direction model versions. A promotion
comparison between flipped and default directions MUST align scale-dependent
metrics by percentile. Promotion of a flipped model requires full-market replay
and calibration comparison against the baseline default model. Absolute-score
threshold consumers (SCORE_THRESHOLD backtests, consensus/openclaw thresholds)
remain default-direction-only and are undefined for flipped percentile scores.

#### Scenario: Raw-score comparison stays within same direction

- GIVEN two model versions with the same component directions
- WHEN a comparison report is generated
- THEN both sides resolve to the `score` basis
- AND raw score values may be compared directly

### Requirement: Signed-score calibration uses percentile semantics

Calibration and comparison reports MUST preserve the legacy 0-100 score basis
for default-direction model configurations. A model configuration with any
non-penalty component direction set to -1 MUST use the persisted cross-sectional
percentile normalized to 0-100, including windows that happen to contain only
positive scores. Observed negative scores remain a defensive signal to select
percentile when configuration metadata is unavailable. Reports MUST label the
basis. A percentile-basis report MUST fail rather than silently omit observations
when a required percentile is missing, non-finite, boolean, or outside [0, 1].

#### Scenario: Signed cohort retains its negative tail

- GIVEN a flipped model cohort with negative scores and complete percentiles
- WHEN a calibration report is generated
- THEN `bucket_basis` is `percentile`
- AND every verified observation is included in the distribution and buckets
- AND false-positive and false-negative samples use percentile thresholds

#### Scenario: Positive-only partial window keeps configured semantics

- GIVEN a flipped model configuration and a requested window whose scores are all positive
- WHEN a calibration report is generated
- THEN `bucket_basis` is still `percentile`
- AND basis selection does not depend on the observed score range

#### Scenario: Cross-direction comparison has one common basis

- GIVEN a signed candidate cohort and a default-direction baseline cohort
- WHEN a comparison report is generated
- THEN both sides are bucketed by percentile on the same 0-100 scale
- AND `comparison_basis` is `percentile`
- AND no raw average-score delta is reported across the two directions

#### Scenario: Signed cohort is missing percentile data

- GIVEN a signed-score prediction without a persisted percentile
- WHEN calibration or comparison is requested
- THEN the report fails with an explicit percentile requirement
- AND it does not return a partial or score-filtered result

### Requirement: Experiment APIs expose report basis and stable validation failures

The score-experiment run report MUST expose `bucket_basis` for each horizon.
The `/api/score-experiments/compare` response MUST expose `comparison_basis` and
`comparison_status`. A synchronous request that requires invalid percentile data
or lacks verified predictions on either side MUST return HTTP 422 with a stable
`success`, `message`, and `data` error envelope. It MUST NOT expose internal
tracebacks. The frontend MUST label score and percentile buckets according to
this metadata.

#### Scenario: Cross-basis API comparison

- GIVEN a flipped candidate and default-direction baseline with valid verified data
- WHEN `/api/score-experiments/compare` succeeds
- THEN both summaries declare `bucket_basis: percentile`
- AND the response declares `comparison_basis: percentile`
- AND the frontend labels the buckets as percentile-based

#### Scenario: Comparison cannot be evaluated

- GIVEN either side has no verified predictions or has invalid required percentiles
- WHEN `/api/score-experiments/compare` is requested
- THEN the response status is 422
- AND the response contains a sanitized domain message and `data: null`
