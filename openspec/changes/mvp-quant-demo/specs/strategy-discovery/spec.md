## ADDED Requirements

### Requirement: Market-wide Strategy Screening

The system SHALL support scanning one strategy across the entire stock universe
to identify which stocks a given strategy is most effective on.

#### Scenario: User screens MA_CROSS across the market

- **GIVEN** quote and factor data exists for all active A-share stocks
- **WHEN** a user submits a market scan for `MA_CROSS` over a date range
- **THEN** the engine SHALL run the strategy on each stock independently
- **AND** return results sorted by a composite score (see Composite Strategy
  Ranking requirement), NOT by Sharpe alone
- **AND** include for each stock: return, Sharpe, max drawdown, win rate,
  trade count, benchmark excess, turnover rate, and the single-best-day
  contribution to total return
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

### Requirement: Composite Strategy Ranking

Strategy results from screening, comparison, and optimization SHALL be ranked
by a composite score that penalizes overfitting-prone configurations, not by
Sharpe ratio alone.

#### Scenario: Composite score is computed

- **GIVEN** a backtest result with full metrics
- **WHEN** the composite score is computed
- **THEN** the score SHALL incorporate at minimum: net excess return (strategy
  minus benchmark), maximum drawdown magnitude, information ratio, and a
  penalty proportional to turnover rate
- **AND** configurations with fewer than 5 trades SHALL receive a severe
  penalty or be excluded from ranking
- **AND** the component weights and formula SHALL be documented and
  configurable.

#### Scenario: Composite score penalizes concentration

- **GIVEN** a strategy result where a single trade or a single 5-day window
  accounts for more than 40% of total P&L
- **WHEN** the composite score is computed
- **THEN** the score SHALL receive a concentration penalty
- **AND** the result SHALL be flagged with "concentrated_returns" in the
  output.

#### Scenario: Composite score penalizes extreme drawdowns

- **GIVEN** a strategy with max drawdown exceeding 30%
- **WHEN** the composite score is computed
- **THEN** the drawdown penalty SHALL increase non-linearly above 20%
- **AND** the result SHALL be flagged "high_drawdown" above 30%.

### Requirement: Anti-overfitting Guardrails

The system SHALL embed minimum anti-overfitting protections into all
screening, comparison, and optimization workflows so that discovered
strategies are not artifacts of data snooping.

#### Scenario: Train / validation / test split is enforced for optimization

- **GIVEN** a parameter optimization or market scan that will report
  performance results
- **WHEN** the optimization is configured
- **THEN** the date range SHALL be splittable into train (earliest 60%),
  validation (next 20%), and test (latest 20%) periods
- **AND** parameter selection SHALL use only train + validation data
- **AND** the final reported result SHALL use only test-period data
- **AND** the split dates SHALL be included in the output for auditability.

#### Scenario: Multiple-comparison risk is flagged

- **GIVEN** a market scan evaluated 2000 stocks with one strategy
- **WHEN** the top-20 results are displayed
- **THEN** the output SHALL include the total number of candidates evaluated
- **AND** compute and display the expected number of false positives under
  a null hypothesis (Bonferroni-corrected or Benjamini-Hochberg estimate)
- **AND** label results that do not survive the multiple-comparison
  correction as "not_significant_after_multi_testing".

#### Scenario: Walk-forward decay is detected

- **GIVEN** a walk-forward validation result
- **WHEN** the stability analysis runs
- **THEN** the system SHALL compare the average Sharpe of the first half
  of windows to the average Sharpe of the second half
- **AND** flag the result as "performance_decay" if the second-half Sharpe
  is more than 20% lower than the first-half Sharpe
- **AND** require the decay flag to be visible in any exported or displayed
  result.

#### Scenario: Minimum sample requirements

- **GIVEN** a backtest result
- **WHEN** it is displayed in a comparison, scan, or optimization ranking
- **THEN** results with fewer than 5 trades SHALL be marked "low_sample"
  and excluded from top-ranking positions
- **AND** results covering fewer than 120 trading days SHALL be marked
  "insufficient_period".

### Requirement: Trading Executability Constraints

Market scans and strategy evaluations SHALL incorporate real-world trading
constraints beyond the existing friction model to ensure discovered strategies
are executable at scale.

#### Scenario: Un-tradable stocks are filtered

- **GIVEN** a market scan is executed
- **WHEN** stocks are evaluated for inclusion
- **THEN** the system SHALL by default exclude stocks with trade_status
  indicating suspension, ST designation, or listing within the last 60
  trading days
- **AND** these filters SHALL be configurable (on/off) with clear labels
  in the output.

#### Scenario: Liquidity floor is enforced

- **GIVEN** a strategy that would trade a stock
- **WHEN** the stock's average daily turnover over the last 20 days is
  below a configurable threshold (default 5M CNY)
- **THEN** the stock SHALL be flagged "low_liquidity" in scan results
- **AND** the user SHALL have the option to exclude low-liquidity stocks
  from ranking.

#### Scenario: Dynamic slippage model is available

- **GIVEN** a stock with high volatility or low liquidity
- **WHEN** slippage is computed for a trade
- **THEN** the system SHALL support a dynamic slippage mode that scales
  with volatility (e.g., 0.1% base + 0.5 * daily_volatility) in addition
  to the fixed 0.1% default
- **AND** the slippage mode SHALL be selectable per backtest or scan.

#### Scenario: Position capacity is estimated

- **GIVEN** a strategy would allocate X CNY to a stock
- **WHEN** the capacity check runs
- **THEN** the system SHALL estimate whether X exceeds 1% of the stock's
  average daily turnover over 20 days
- **AND** flag the stock as "capacity_limited" if the position would
  consume more than 1% of daily volume.

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
  win rate, trade count, benchmark excess, information ratio, and composite
  score
- **AND** rank strategies by composite score, not by Sharpe alone
- **AND** include concentration and decay flags per strategy.

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

#### Scenario: Walk-forward triggers performance decay alert

- **GIVEN** a walk-forward result with at least 6 windows
- **WHEN** the stability analysis runs
- **THEN** the system SHALL compare average Sharpe of the first half of
  windows to the second half
- **AND** flag "performance_decay" if second-half Sharpe is >20% lower
- **AND** the flag SHALL be prominent in the output and persisted.

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
the frontend research workspace, with anti-overfitting checks visible at
every step.

#### Scenario: Researcher follows a discovery workflow with guardrails

- **GIVEN** the researcher wants to find effective strategies
- **WHEN** they follow the workflow: scan strategies on a stock -> identify
  best strategy -> check anti-overfitting flags -> walk-forward validate ->
  examine regime breakdown -> drill into component contribution
- **THEN** each step SHALL produce results that can be bookmarked or saved
  as a named analysis
- **AND** the frontend SHALL provide navigation between discovery steps
  with the stock and strategy context preserved
- **AND** anti-overfitting flags (concentration, decay, low sample,
  multiple-comparison) SHALL be visible on every result card without
  requiring a drill-down.

#### Scenario: Discovery results are exportable

- **GIVEN** a scan, comparison, or walk-forward result
- **WHEN** the user exports the result
- **THEN** the system SHALL support CSV export
- **AND** include all metrics columns plus metadata (date range, strategy,
  model version, generated timestamp)
- **AND** include all anti-overfitting flags in exported data.
