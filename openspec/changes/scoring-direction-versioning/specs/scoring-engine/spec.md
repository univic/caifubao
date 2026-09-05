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
and calibration comparison against the baseline default model.

#### Scenario: Raw-score comparison stays within same direction

- GIVEN two model versions with the same component directions
- WHEN a comparison report is generated
- THEN both sides use the same resolved direction set
- AND raw score values may be compared directly

### Requirement: Signed-score calibration uses percentile semantics

Calibration and comparison reports MUST preserve the legacy 0-100 score basis
for cohorts without negative scores. When either cohort contains a negative
score, distribution and bucket metrics MUST instead use the persisted
cross-sectional percentile normalized to 0-100, and the report MUST label that
basis. A signed-score report MUST fail rather than silently omit observations
when a required percentile is missing.

#### Scenario: Signed cohort retains its negative tail

- GIVEN a flipped model cohort with negative scores and complete percentiles
- WHEN a calibration report is generated
- THEN `bucket_basis` is `percentile`
- AND every verified observation is included in the distribution and buckets
- AND false-positive and false-negative samples use percentile thresholds

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
