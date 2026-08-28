# Scoring Percentile-Rank Behavior

## ADDED Requirements

### Requirement: Recommendation is driven by cross-sectional percentile

The scoring engine MUST derive BUY/WATCH/AVOID recommendations from the
cross-sectional score percentile within the same evaluation date and horizon,
not from absolute score thresholds.

#### Scenario: Top-decile stock gets BUY

- GIVEN a date/horizon cohort of predictions ranked by score
- WHEN a prediction's percentile is at or above `buy_percentile`
- THEN its recommendation is BUY

#### Scenario: Mid-range percentile stays NONE

- GIVEN a prediction whose percentile is between `watch_percentile` and
  `avoid_percentile`
- WHEN recommendations are assigned
- THEN its recommendation is NONE

#### Scenario: Bottom-decile stock gets AVOID

- GIVEN a prediction whose percentile is at or below `avoid_percentile`
- WHEN recommendations are assigned
- THEN its recommendation is AVOID

#### Scenario: Single-stock fallback

- GIVEN a single-stock scoring path with no cross-sectional cohort
- WHEN a recommendation is needed
- THEN the engine falls back to absolute thresholds (existing behavior)

### Requirement: Component values are rank-normalized before weighting

The market-wide scoring path MUST rank-normalize each raw component value
across the day's cohort before applying weights, so the final score is
cross-sectionally comparable.

#### Scenario: Market-wide scoring uses rank-normalized components

- GIVEN `score_all_stocks` is called for a date/horizon
- WHEN raw component values are collected for every stock in the cohort
- THEN each component is rank-normalized to [0, 1] across the cohort
- AND the final score is the weighted sum of the rank-normalized components

#### Scenario: Single-stock path keeps raw component weighting

- GIVEN `score_single_stock` is called directly
- WHEN scoring a single stock
- THEN raw component weighting is used (backward compatible)

#### Scenario: Ranked mode is gated by an environment flag

- GIVEN the scoring engine is invoked for market-wide scoring
- WHEN env `DATAHUB_SCORING_MODE` equals `ranked`
- THEN `score_all_stocks` uses the cross-sectional rank-normalized path
- AND when `DATAHUB_SCORING_MODE` is unset or not `ranked`
- THEN the legacy component-weighted path is used (default behavior)
