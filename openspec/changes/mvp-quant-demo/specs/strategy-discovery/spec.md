## ADDED Requirements

### Requirement: Market-wide Strategy Screening

The system SHALL support scanning one strategy across the entire stock universe
to identify which stocks a given strategy is most effective on.

#### Scenario: User screens MA_CROSS across the market

- **GIVEN** quote and factor data exists for all active A-share stocks
- **WHEN** a user submits a market scan for `MA_CROSS` over a date range
- **THEN** the engine SHALL run the strategy on each stock independently
- **AND** return results sorted by Sharpe ratio or excess return vs benchmark
- **AND** include for each stock: return, Sharpe, max drawdown, win rate,
  trade count, and benchmark excess
- **AND** support optional filtering by minimum trade count, minimum
  Sharpe, sector/industry, and market cap.

#### Scenario: Scan is paginated for large result sets

- **GIVEN** 4000+ active stocks exist
- **WHEN** a market scan is requested
- **THEN** results SHALL be paginated with configurable page size (default 100)
- **AND** the scan SHALL report total stocks evaluated, stocks with zero
  trades (skipped due to insufficient data), and error count.

#### Scenario: Scan uses compute-worker for heavy workloads

- **GIVEN** a market scan for all stocks over a 2-year range
- **WHEN** the computation would exceed a 30-second timeout
- **THEN** the system SHALL support offloading the scan to the compute-worker
  as an asynchronous `ComputeTask`
- **AND** the user SHALL receive a task ID for polling the result.

### Requirement: Strategy Comparison on a Single Stock

The system SHALL compare all eligible strategies on one stock simultaneously
so users can identify the best strategy for a given instrument.

#### Scenario: User compares all strategies on one stock

- **GIVEN** quote, factor, and score data exists for a stock
- **WHEN** a user requests a strategy comparison for that stock over a date range
- **THEN** the engine SHALL run all eligible strategies (BUY_HOLD, MA_CROSS,
  SCORE_THRESHOLD, SCORE_MOMENTUM, and MULTI_HORIZON_CONSENSUS when score
  data exists)
- **AND** return a side-by-side comparison with return, Sharpe, max drawdown,
  win rate, trade count, benchmark excess, and information ratio
- **AND** highlight the best strategy by Sharpe ratio.

#### Scenario: Score-driven strategies are excluded when score data is absent

- **GIVEN** no `StockScorePrediction` data exists for a stock
- **WHEN** strategy comparison is requested
- **THEN** only non-score strategies (BUY_HOLD, MA_CROSS) SHALL be evaluated
- **AND** score-driven strategy rows SHALL be marked "unavailable" with
  reason "no_score_data".

### Requirement: Walk-forward Validation

The system SHALL support rolling-window backtest evaluation to detect
whether a strategy's performance is stable across different time periods
or driven by a single favorable episode.

#### Scenario: User runs walk-forward validation

- **GIVEN** sufficient historical data for a stock and strategy
- **WHEN** a walk-forward validation is requested with window size W and step S
- **THEN** the engine SHALL run the strategy on each window [start, start+W],
  [start+S, start+S+W], etc.
- **AND** return for each window: start date, end date, return, Sharpe,
  max drawdown, win rate, and trade count
- **AND** include an overall stability score (standard deviation of
  window Sharpe ratios across all windows).

#### Scenario: Walk-forward detects regime dependency

- **GIVEN** a walk-forward result with 12+ windows
- **WHEN** the result is analyzed
- **THEN** the system SHALL flag windows where performance deviates more
  than 2 standard deviations from the mean
- **AND** label the worst-performing windows with their date range for
  further investigation.

#### Scenario: Window size is validated

- **GIVEN** a walk-forward request with window W days
- **WHEN** W is less than the minimum required trading days for the strategy
- **THEN** the request SHALL be rejected with a clear error indicating
  the minimum required window size.

### Requirement: Market Regime Decomposition

The system SHALL decompose backtest performance by market regime so users
can understand when a strategy works and when it fails.

#### Scenario: Backtest result is decomposed by regime

- **GIVEN** a completed backtest with daily values and CSI 300 data
- **WHEN** regime decomposition is requested
- **THEN** the system SHALL classify each holding period into one of:
  bull (index up > 10% over 60 days), bear (index down > 10% over 60 days),
  or sideways (otherwise)
- **AND** compute separate return, Sharpe, and win rate for each regime
- **AND** return the percentage of time spent in each regime.

#### Scenario: Regime labels use a configurable classifier

- **GIVEN** a regime decomposition request
- **WHEN** the regime is determined
- **THEN** the system SHALL use the CSI 300 index as the reference
- **AND** regime thresholds (up/down percentages, lookback window) SHALL
  be configurable
- **AND** SHALL support a "custom" mode where the user provides the
  benchmark code.

### Requirement: Factor Predictive Power Evaluation

The system SHALL evaluate whether individual scoring components and external
factors have predictive power for future returns, enabling systematic factor
discovery.

#### Scenario: Factor IC is computed across the market

- **GIVEN** a scoring component (e.g., momentum) has been computed for all
  stocks over a date range
- **WHEN** a factor evaluation is requested
- **THEN** the system SHALL compute the rank Information Coefficient (IC)
  between the factor value and forward N-day returns for each evaluation date
- **AND** return the mean IC, IC standard deviation, ICIR (IC / std), and
  IC time-series for plotting.

#### Scenario: Quintile analysis shows factor monotonicity

- **GIVEN** factor values and forward returns exist for all stocks on
  an evaluation date
- **WHEN** quintile analysis is requested
- **THEN** the system SHALL sort stocks into quintiles by factor value
- **AND** compute the mean forward return for each quintile
- **AND** flag when the relationship is non-monotonic (e.g., Q3 > Q5).

#### Scenario: Component contribution is attributed to P&L

- **GIVEN** a backtest result that used score-driven entries
- **WHEN** component contribution analysis is requested
- **THEN** the system SHALL compute the average component score at entry
  and at exit for each trade
- **AND** report which components had the largest contribution change
  between entry and exit
- **AND** compute win rate and average P&L grouped by which component
  triggered the entry signal.

### Requirement: Discovery Workflow Integration

The discovery capabilities SHALL be accessible through both the API and
the frontend research workspace.

#### Scenario: Researcher follows a discovery workflow

- **GIVEN** the researcher wants to find effective strategies
- **WHEN** they follow the workflow: scan strategies on a stock -> identify
  best strategy -> walk-forward validate -> examine regime breakdown -> drill
  into component contribution
- **THEN** each step SHALL produce results that can be bookmarked or saved
  as a named analysis
- **AND** the frontend SHALL provide navigation between discovery steps
  with the stock and strategy context preserved.

#### Scenario: Discovery results are exportable

- **GIVEN** a scan, comparison, or walk-forward result
- **WHEN** the user exports the result
- **THEN** the system SHALL support CSV export
- **AND** include all metrics columns plus metadata (date range, strategy,
  model version, generated timestamp).
