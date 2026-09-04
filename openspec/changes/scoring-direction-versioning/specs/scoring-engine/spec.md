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
default direction (higher score means lower raw bullishness). Replay,
calibration, and backtest comparisons MUST restrict to same-direction model
versions. Promotion of a flipped model requires full-market replay and
calibration comparison against the baseline default model.

#### Scenario: Comparison stays within same direction

- GIVEN a default-direction model version and a flipped model version
- WHEN a comparison report is generated
- THEN both sides use the same resolved direction set
- AND raw score/percentile values are not mixed across directions without
  labeling the direction difference
