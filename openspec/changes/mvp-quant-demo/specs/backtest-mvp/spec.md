## ADDED Requirements

### Requirement: Lightweight Single-stock Backtest

The system SHALL provide an MVP backtest capability for one stock and one simple daily-bar strategy.

#### Scenario: User runs an MA-cross backtest

- **GIVEN** daily quote and moving-average factor data exists for one stock
- **WHEN** a user submits a backtest request for the MA-cross strategy
- **THEN** the backend SHALL evaluate the strategy with project-owned daily-bar logic
- **AND** return metrics, an equity curve, and trade history.

### Requirement: Backtest Boundary

The backtest MVP SHALL remain separate from score replay and calibration.

#### Scenario: Score replay is evaluated separately

- **GIVEN** historical score predictions exist
- **WHEN** the system evaluates whether scores predicted future outcomes
- **THEN** it SHALL use scoring replay/calibration services
- **AND** SHALL NOT treat that evaluation as a trading backtest.

### Requirement: Backtest Engine Scope

The MVP SHALL NOT introduce a full external backtest framework.

#### Scenario: Backtest dependencies are reviewed

- **GIVEN** the MVP only needs single-stock daily-bar simulation
- **WHEN** implementing the backtest service
- **THEN** the implementation SHALL use small project-owned code
- **AND** SHALL NOT add `backtrader`, `vectorbt`, `zipline`, `rqalpha`, or similar frameworks.

### Requirement: Score-driven Backtest Strategies

The backtest engine SHALL support strategies that consume `StockScorePrediction` data
to validate whether scoring signals produce profitable trading outcomes.

#### Scenario: User backtests a score-threshold strategy

- **GIVEN** historical `StockScorePrediction` records exist for a stock
- **WHEN** a user submits a backtest for the `SCORE_THRESHOLD` strategy with horizon 5 and threshold 80
- **THEN** the engine SHALL buy when today's Score5 >= 80 and sell when Score5 < 50 or stop-loss is hit
- **AND** SHALL only read predictions whose `date` is strictly less than or equal to the current trading day (no look-ahead bias)
- **AND** return metrics, equity curve, trades, and the scoring config used.

#### Scenario: User backtests a top-N rotation strategy

- **GIVEN** historical score predictions exist for multiple stocks
- **WHEN** a user submits a backtest for `TOP_N_ROTATION` with N=10 and rebalance interval K=5
- **THEN** the engine SHALL rank stocks by score each rebalance day
- **AND** hold the top N stocks with equal weight or score-weighted allocation
- **AND** liquidate stocks that fall out of the top N.

#### Scenario: User backtests a score-momentum strategy

- **GIVEN** historical score predictions exist
- **WHEN** a user submits a backtest for `SCORE_MOMENTUM` with delta-threshold N
- **THEN** the engine SHALL buy when today's score exceeds yesterday's score by N or more
- **AND** sell when the score drops below entry score minus exit-delta.

### Requirement: Backtest Friction Model

The backtest engine SHALL model real-world trading frictions so that results are
not misleadingly optimistic.

#### Scenario: Commission is deducted

- **GIVEN** a buy order is executed at price P for Q shares
- **WHEN** the backtest engine records the trade
- **THEN** it SHALL deduct commission at 0.025% of trade value (minimum 5 CNY) from available cash.

#### Scenario: Stamp duty is deducted on sells

- **GIVEN** a sell order is executed
- **WHEN** the backtest engine records the trade
- **THEN** it SHALL deduct stamp duty at 0.1% of trade value from the proceeds.

#### Scenario: Slippage is applied

- **GIVEN** a trade signal is generated
- **WHEN** the engine determines the execution price
- **THEN** it SHALL apply a configurable slippage factor (default 0.1%) against the signal direction
- **AND** buy orders SHALL execute at close x (1 + slippage), sell orders at close x (1 - slippage).

#### Scenario: Friction is reported transparently

- **GIVEN** a backtest completes
- **WHEN** the result is returned
- **THEN** it SHALL include total commission, total stamp duty, total slippage cost
- **AND** net return (after all frictions) alongside gross return (before frictions).

### Requirement: Limit-up / Limit-down Constraints

The backtest engine SHALL respect A-share price-limit rules using the existing
`trade_status` field from `StockDailyQuote`.

#### Scenario: Buy order fails on limit-up day

- **GIVEN** a buy signal is generated for a stock
- **WHEN** the stock's `trade_status` indicates a limit-up condition (price movement = +10%)
- **THEN** the engine SHALL NOT execute the buy order
- **AND** SHALL record a skipped-trade entry with reason "limit_up_blocked".

#### Scenario: Sell order fails on limit-down day

- **GIVEN** a sell signal is generated for a stock
- **WHEN** the stock's `trade_status` indicates a limit-down condition (price movement = -10%)
- **THEN** the engine SHALL NOT execute the sell order
- **AND** SHALL record a skipped-trade entry with reason "limit_down_blocked".

#### Scenario: Consecutive limit days

- **GIVEN** a stock hits limit-up for multiple consecutive days
- **WHEN** a pending buy order exists
- **THEN** the engine SHALL attempt execution each day until either the order fills or the limit condition clears.

### Requirement: Index Benchmark Comparison

The backtest engine SHALL compare strategy returns against a buy-and-hold index
benchmark to distinguish skill (Alpha) from market exposure (Beta).

#### Scenario: Benchmark is computed alongside strategy

- **GIVEN** a backtest runs over a date range
- **WHEN** the result is computed
- **THEN** the engine SHALL simulate a buy-and-hold strategy on the specified benchmark index over the same date range
- **AND** the default benchmark SHALL be CSI 300 (sh000300) unless overridden.

#### Scenario: Alpha and Beta are reported

- **GIVEN** a completed backtest with benchmark data
- **WHEN** metrics are computed
- **THEN** the result SHALL include benchmark total return and annualized return
- **AND** strategy excess return (strategy return - benchmark return)
- **AND** information ratio (excess return / tracking error) when sufficient daily data exists.

### Requirement: Multi-stock Backtest

The backtest engine SHALL support simultaneous simulation across multiple stocks.

#### Scenario: User runs a multi-stock backtest

- **GIVEN** a list of stock codes, a strategy, and a date range
- **WHEN** a multi-stock backtest is requested
- **THEN** the engine SHALL load quotes for all stocks and align them to a common trading-day index
- **AND** evaluate signals across all stocks on each trading day
- **AND** enforce position limits, capital allocation rules, and per-stock constraints.

#### Scenario: Position sizing is configurable

- **GIVEN** a multi-stock backtest with N stocks and initial capital C
- **WHEN** the allocation rule is "equal_weight"
- **THEN** each active position SHALL receive approximately C / N in capital
- **AND** the engine SHALL round down to 100-share lots.

#### Scenario: Per-stock contribution is tracked

- **GIVEN** a multi-stock backtest completes
- **WHEN** the result is returned
- **THEN** it SHALL include per-stock contribution metrics (realized PnL, max drawdown, win rate)
- **AND** portfolio-level aggregate metrics.

### Requirement: Backtest Parameter Optimization

The backtest system SHALL support automated parameter sweep to find optimal
strategy thresholds without manual trial-and-error.

#### Scenario: User optimizes SCORE_THRESHOLD parameters

- **GIVEN** score predictions and quote data exist for a stock
- **WHEN** a user submits a `POST /api/backtest/optimize` request with a
  threshold grid (e.g., entry=[50,60,70,80], exit=[30,40,50])
- **THEN** the engine SHALL run backtests for each parameter combination
  on the specified date range
- **AND** return results sorted by Sharpe ratio (primary) and excess
  return (secondary)
- **AND** include the full metrics for the best configuration.

#### Scenario: Optimization respects look-ahead bias

- **GIVEN** a parameter optimization request for date range [D1, D2]
- **WHEN** each backtest is executed
- **THEN** the engine SHALL use the same look-ahead guard for score
  consumption as single backtests
- **AND** SHALL NOT optimize parameters on data outside the requested range.

#### Scenario: Optimization supports strategy-appropriate grids

- **GIVEN** an optimization request for `SCORE_THRESHOLD`
- **WHEN** the parameter grid is validated
- **THEN** valid parameters SHALL include entry_threshold, exit_threshold,
  stop_loss_pct, and horizon
- **AND** invalid or irrelevant parameters for the strategy SHALL be
  rejected with a clear error.

### Requirement: Multi-horizon Consensus Strategy

The backtest engine SHALL support a strategy that requires consensus
across multiple score horizons to reduce false signals from short-term
noise.

#### Scenario: User backtests multi-horizon consensus

- **GIVEN** Score5, Score20, and Score60 predictions exist
- **WHEN** a user submits a backtest for `MULTI_HORIZON_CONSENSUS` with
  per-horizon thresholds
- **THEN** the engine SHALL generate a BUY signal only when all configured
  horizons meet or exceed their respective entry thresholds
- **AND** generate a SELL signal when ANY horizon drops below its exit
  threshold or stop-loss is hit.

#### Scenario: Consensus strategy handles partial horizon data

- **GIVEN** Score5 data exists but Score60 is missing for a trading day
- **WHEN** the consensus strategy evaluates a signal
- **THEN** it SHALL require only the configured horizons that have data
  for that day
- **AND** SHALL skip the day if fewer than 2 horizons have data
- **AND** SHALL record skipped days in the backtest result.

### Requirement: Strategy Comparison Endpoint

The backtest system SHALL support comparing all eligible strategies on a
single stock simultaneously.

#### Scenario: User compares all strategies on one stock

- **GIVEN** quote, factor, and score data exists for a stock
- **WHEN** a `POST /api/backtest/compare` request is submitted
- **THEN** the engine SHALL run all eligible strategies for that stock
  over the date range
- **AND** return a side-by-side comparison table with total return, Sharpe,
  max drawdown, win rate, trade count, benchmark excess, and information
  ratio for each strategy
- **AND** highlight the best strategy by Sharpe ratio.

#### Scenario: Score-driven strategies excluded when no score data

- **GIVEN** no `StockScorePrediction` data exists for the stock
- **WHEN** comparison is requested
- **THEN** score-driven strategy rows SHALL show "unavailable" with
  reason "no_score_data"
- **AND** BUY_HOLD and MA_CROSS SHALL still be evaluated.

### Requirement: Market-wide Strategy Scan

The backtest system SHALL support scanning one strategy across the entire
stock universe.

#### Scenario: User scans MA_CROSS across the market

- **GIVEN** quote and factor data exists for all active stocks
- **WHEN** a `POST /api/backtest/scan` request is submitted with a
  strategy and date range
- **THEN** the engine SHALL run the strategy on each stock independently
- **AND** return results sorted by Sharpe ratio, paginated (default 100 per page)
- **AND** for each stock include return, Sharpe, max drawdown, win rate,
  trade count, and benchmark excess.

#### Scenario: Offload heavy scans to compute-worker

- **GIVEN** a market scan would run for 3000+ stocks
- **WHEN** the scan is submitted
- **THEN** the system SHALL support async execution via `ComputeTask`
  with task type `MARKET_SCAN`
- **AND** the response SHALL include a task ID for polling.

### Requirement: Walk-forward Validation Endpoint

The backtest system SHALL support rolling-window validation to detect
regime-dependent performance.

#### Scenario: User runs walk-forward on a strategy

- **GIVEN** sufficient historical data exists
- **WHEN** a `POST /api/backtest/walk-forward` request is submitted
  with window size W and step S
- **THEN** the engine SHALL run the strategy on each rolling window
  and return per-window metrics (return, Sharpe, max DD, win rate)
- **AND** compute a stability score (standard deviation of window Sharpe ratios)
- **AND** flag windows where Sharpe deviates >2 sigma from the mean.

### Requirement: Market Regime Breakdown

The backtest system SHALL decompose performance by market regime.

#### Scenario: Backtest result is decomposed by regime

- **GIVEN** a completed backtest with daily equity and CSI 300 data
- **WHEN** regime breakdown is requested via `GET /api/backtest/<id>/regime`
- **THEN** the system SHALL classify periods into bull, bear, and sideways
  using index trend thresholds
- **AND** return separate metrics (return, Sharpe, win rate) per regime
- **AND** report the percentage of time spent in each regime.
