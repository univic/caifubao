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
- **AND** buy orders SHALL execute at close × (1 + slippage), sell orders at close × (1 − slippage).

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
- **WHEN** the stock's `trade_status` indicates a limit-down condition (price movement = −10%)
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
- **AND** the default benchmark SHALL be CSI 300 (沪深 300, code `sh000300`) unless overridden.

#### Scenario: Alpha and Beta are reported

- **GIVEN** a completed backtest with benchmark data
- **WHEN** metrics are computed
- **THEN** the result SHALL include benchmark total return and annualized return
- **AND** strategy excess return (strategy return − benchmark return)
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
- **AND** the engine SHALL round down to 100-share lots (整手).

#### Scenario: Per-stock contribution is tracked

- **GIVEN** a multi-stock backtest completes
- **WHEN** the result is returned
- **THEN** it SHALL include per-stock contribution metrics (realized PnL, max drawdown, win rate)
- **AND** portfolio-level aggregate metrics.
