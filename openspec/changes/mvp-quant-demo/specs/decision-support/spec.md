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
  action, actual action, score at decision time, reasoning (free text),
  and the user or system that made the entry
- **AND** the entry SHALL be queryable by date range, stock, and
  recommendation type.

#### Scenario: Journal tracks decision outcomes

- **GIVEN** a journal entry for a buy decision with entry price
- **WHEN** the position is later closed
- **THEN** the journal SHALL support updating the entry with exit date,
  exit price, realized P&L, and return percentage
- **AND** aggregate statistics SHALL show adoption rate (decisions taken
  vs recommendations made), win rate, and cumulative P&L by recommendation
  source.

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
- **AND** each recommendation SHALL include the confidence (based on
  verification hit rate for that score bucket).

### Requirement: Frontend Decision Workspace

The frontend SHALL provide an integrated decision workspace that combines
all decision-support capabilities.

#### Scenario: User opens the decision workspace

- **GIVEN** the user is authenticated
- **WHEN** they navigate to the decision workspace
- **THEN** the page SHALL show a dashboard with: top score signals, active
  alerts, position match summary, and a link to the decision journal
- **AND** each section SHALL support drill-down to detailed views.

#### Scenario: Alert feed is real-time

- **GIVEN** new score alerts are generated by the daily scoring run
- **WHEN** the user has the decision workspace open
- **THEN** the frontend SHALL poll or receive push updates for new alerts
- **AND** display a notification badge with unread alert count.
