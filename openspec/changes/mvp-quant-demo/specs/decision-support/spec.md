## ADDED Requirements

### Requirement: Daily Decision Dashboard

The system SHALL provide a daily dashboard that translates research outputs
into actionable trading signals, bridging the gap between backtest discovery
and live decision-making.

#### Scenario: Dashboard shows today's top signals

- **GIVEN** Score5, Score20, and Score60 predictions exist for today
- **WHEN** the daily decision dashboard is requested
- **THEN** the dashboard SHALL show for each horizon the top-N stocks
  by score, with score change (delta from previous trading day), current
  recommendation, and verification status of the most recent verified
  prediction
- **AND** stocks with score increases >= 15 points SHALL be flagged as
  "significant change" alerts.

#### Scenario: Dashboard shows position match

- **GIVEN** the user has configured a tracked portfolio (list of held stocks)
- **WHEN** the dashboard is rendered
- **THEN** it SHALL show for each held stock: current score across all
  three horizons, whether the held position matches the current
  recommendation (BUY/WATCH/AVOID), and the date of the last score change
- **AND** flag positions where the recommendation contradicts the holding
  (e.g., holding but Score5 is AVOID).

#### Scenario: Dashboard supports configurable watchlists

- **GIVEN** a user wants to monitor specific stocks
- **WHEN** they add stocks to a watchlist
- **THEN** the dashboard SHALL show those stocks at the top with full
  multi-horizon score history and alerts
- **AND** support multiple named watchlists (e.g., "tech stocks",
  "dividend stocks").

### Requirement: Actionable Decision Recommendations

Every recommendation presented in the dashboard SHALL include sufficient
context for the user to make an informed execution decision, not just a
BUY/WATCH/AVOID label.

#### Scenario: Recommendation includes confidence metadata

- **GIVEN** a score recommendation for a stock
- **WHEN** the recommendation is displayed
- **THEN** the display SHALL include: (a) the historical hit rate for
  predictions in the same score bucket over the last 90 days, (b) the
  sample size backing that hit rate, and (c) whether the hit rate has
  been trending up or down in the last 30 days
- **AND** recommendations with fewer than 10 historical samples in the
  same bucket SHALL be marked "low_confidence".

#### Scenario: Recommendation includes invalidation conditions

- **GIVEN** a BUY recommendation for a stock at score 75
- **WHEN** the recommendation is presented
- **THEN** it SHALL specify explicit invalidation conditions: the score
  level below which the recommendation is void (exit threshold), the stop-loss
  percentage, and the maximum number of trading days after which the
  recommendation expires if not acted upon
- **AND** these conditions SHALL be derived from the horizon-specific
  scoring config used to generate the prediction.

#### Scenario: Recommendation includes position sizing guidance

- **GIVEN** a BUY recommendation and the user's portfolio state
- **WHEN** the recommendation is displayed
- **THEN** it SHALL suggest a target position weight (as % of portfolio)
  based on the score percentile and the effective thresholds
- **AND** indicate whether the current portfolio has room for the position
  or needs to free capital first
- **AND** flag if the suggested position would exceed the stock's estimated
  capacity limit (based on 1% of average daily turnover).

### Requirement: Score Alert Detection

The system SHALL detect and surface significant score changes that warrant
a trader's attention.

#### Scenario: Score jump alert is triggered

- **GIVEN** a stock's Score5 was 35 yesterday and 68 today
- **WHEN** the scoring daily run completes
- **THEN** the system SHALL generate an alert of type "score_jump" with
  the stock code, horizon, old score, new score, and delta
- **AND** the alert SHALL be visible in the dashboard and available via API.

#### Scenario: Threshold crossing alert is triggered

- **GIVEN** a stock's Score20 crosses above the BUY threshold (e.g., from
  55 to 72)
- **WHEN** the scoring run completes
- **THEN** the system SHALL generate an alert of type "threshold_cross"
  with the stock, horizon, old recommendation, new recommendation, and score
- **AND** include the primary components driving the score increase.

#### Scenario: Score quality alert is triggered

- **GIVEN** the rolling 30-day hit rate for a horizon drops below its
  90-day historical baseline by more than 20%
- **WHEN** the quality monitor runs
- **THEN** the system SHALL generate an alert of type "quality_degradation"
  with the horizon, current hit rate, baseline hit rate, and drop percentage.

### Requirement: Decision Journal

The system SHALL maintain a decision journal that records what was
recommended and what was actually executed, enabling P&L attribution
and process improvement.

#### Scenario: Decision is logged

- **GIVEN** a score recommendation exists (BUY/WATCH/AVOID) for a stock
- **WHEN** a user records an actual trading decision (bought/sold/held)
- **THEN** the journal entry SHALL record: date, stock code, recommended
  action, actual action, score at decision time, model version, data
  freshness at time of decision, reasoning (free text), and the user or
  system that made the entry
- **AND** the entry SHALL be queryable by date range, stock, and
  recommendation type.

#### Scenario: Journal tracks missed recommendations

- **GIVEN** the system generated a BUY recommendation that the user did
  not execute
- **WHEN** the opportunity window closes (score drops below exit threshold
  or target date passes)
- **THEN** the journal SHALL record a "missed_recommendation" entry with:
  the stock, recommendation date, entry score, the maximum favorable
  return that would have been achieved had the recommendation been followed,
  and the final outcome at window close
- **AND** aggregate statistics SHALL show missed opportunity P&L and the
  ratio of executed vs missed recommendations.

#### Scenario: Journal tracks user-initiated deviations

- **GIVEN** a user executes a trade that was NOT recommended by the system
- **WHEN** the trade is recorded
- **THEN** the journal SHALL create a "user_deviation" entry with the
  trade details, the system's recommendation at that time (if any), and
  the delta between the system's recommendation and the user's action
- **AND** track the P&L of user deviations separately from
  system-recommended trades.

#### Scenario: Journal tracks decision outcomes

- **GIVEN** a journal entry for a buy decision with entry price
- **WHEN** the position is later closed
- **THEN** the journal SHALL support updating the entry with exit date,
  exit price, realized P&L, and return percentage
- **AND** aggregate statistics SHALL show adoption rate, win rate, and
  cumulative P&L by recommendation source
- **AND** separate metrics SHALL distinguish between "model quality"
  (how good were the recommendations) and "execution discipline"
  (how well were the recommendations followed).

### Requirement: Score Quality Monitoring

The system SHALL continuously monitor score prediction quality to detect
model degradation before it impacts trading decisions.

#### Scenario: Rolling hit rate is tracked

- **GIVEN** verified score predictions exist over a 90-day window
- **WHEN** quality monitoring runs daily
- **THEN** the system SHALL compute the rolling 30-day hit_rate_close
  and hit_rate_intra per horizon
- **AND** store the daily quality metric for trend visualization.

#### Scenario: Model drift is detected

- **GIVEN** score distribution percentiles are tracked daily
- **WHEN** the P50 or P90 score shifts more than 10 points over a
  20-day window
- **THEN** the system SHALL flag "model_drift" with the affected
  horizon, old and new distribution statistics
- **AND** recommend re-evaluation of component weights.

#### Scenario: Quality metrics are exposed via API

- **GIVEN** quality monitoring data exists
- **WHEN** the quality API is queried
- **THEN** it SHALL return per-horizon hit rate time-series, distribution
  shifts, alert count, and the date of the last model version change
- **AND** support a date range filter for historical analysis.

### Requirement: Position Attribution and Rebalance Preview

The system SHALL attribute portfolio P&L to specific signals and preview
recommended rebalancing actions.

#### Scenario: P&L is attributed to signal sources

- **GIVEN** a closed trade has an entry triggered by Score5 >= 60
- **WHEN** P&L attribution is computed
- **THEN** the system SHALL attribute the trade P&L to the scoring horizon
  and the dominant components at entry time
- **AND** aggregate attribution SHALL show which signal sources (Score5,
  Score20, Score60, MA_CROSS) contributed most to cumulative P&L.

#### Scenario: Rebalance preview is generated

- **GIVEN** today's scores and a current portfolio
- **WHEN** a rebalance preview is requested
- **THEN** the system SHALL recommend: stocks to enter (score >= BUY
  threshold, not currently held), stocks to exit (held but score <= AVOID
  threshold), and stocks to hold or trim
- **AND** each recommendation SHALL include confidence metadata, invalidation
  conditions, and position sizing per the Actionable Decision Recommendations
  requirement.

### Requirement: Frontend Decision Workspace

The frontend SHALL provide an integrated decision workspace that combines
all decision-support capabilities.

#### Scenario: User opens the decision workspace

- **GIVEN** the user is authenticated
- **WHEN** they navigate to the decision workspace
- **THEN** the page SHALL show a dashboard with: top score signals, active
  alerts, position match summary, and a link to the decision journal
- **AND** each section SHALL support drill-down to detailed views
- **AND** each signal SHALL display its confidence metadata, invalidation
  conditions, and position sizing inline.

#### Scenario: Alert feed is real-time

- **GIVEN** new score alerts are generated by the daily scoring run
- **WHEN** the user has the decision workspace open
- **THEN** the frontend SHALL poll or receive push updates for new alerts
- **AND** display a notification badge with unread alert count.
