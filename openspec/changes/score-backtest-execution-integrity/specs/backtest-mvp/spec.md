## MODIFIED Requirements

### Requirement: Score-driven Backtest Strategies

The backtest engine SHALL support strategies that consume eligible,
version-isolated `StockScorePrediction` data to test whether scoring signals
produce profitable trading outcomes without look-ahead bias.

#### Scenario: Score-threshold signal executes after its score date

- **GIVEN** an eligible Score5 prediction for trading day D and a later trading day D1
- **WHEN** the D score meets a `SCORE_THRESHOLD` entry or exit threshold
- **THEN** the engine SHALL form the order after D closes
- **AND** SHALL NOT execute the order on D
- **AND** SHALL first attempt execution on D1 using D1 `open_hfq` plus directional slippage.

#### Scenario: Score-momentum signal executes after its score date

- **GIVEN** eligible consecutive predictions for a fixed model version
- **WHEN** a `SCORE_MOMENTUM` signal is generated from the latest score available at D close
- **THEN** the order SHALL first be eligible for execution on the next actual trading day
- **AND** SHALL NOT use D's close as its execution price.

#### Scenario: Multi-horizon consensus executes after its score date

- **GIVEN** at least two eligible configured horizons are present for D
- **WHEN** `MULTI_HORIZON_CONSENSUS` generates an order
- **THEN** the order SHALL first be eligible on the next actual trading day
- **AND** `BLOCKED` or `FAILED` horizons SHALL NOT count as available horizons.

#### Scenario: Top-N rotation executes after ranking date

- **GIVEN** eligible predictions exist for multiple stocks on ranking day D
- **WHEN** `TOP_N_ROTATION` selects its target holdings
- **THEN** the resulting rebalance orders SHALL first execute on the next actual trading day open
- **AND** SHALL use only the explicitly selected model version.

#### Scenario: Score order is blocked or has no later trading day

- **GIVEN** a score-driven order is pending
- **WHEN** the next trading day is suspended or blocked by the existing price-limit rules
- **THEN** the engine SHALL retain the pending order and retry on a later tradable open
- **AND** if the requested range contains no later trading day, SHALL leave the order unexecuted
- **AND** SHALL NOT fall back to the score day's close.

### Requirement: Score Model-version Isolation

Every score-driven backtest SHALL consume one explicit scoring model version.

#### Scenario: Public score-driven request omits model version

- **WHEN** a public backtest entry point requests a score-driven strategy without a non-empty `model_version`
- **THEN** the API SHALL return a stable client validation error
- **AND** SHALL NOT select a model version implicitly.

#### Scenario: Multiple model versions exist on the same date

- **GIVEN** multiple predictions exist for the same stock, date, and horizon
- **WHEN** a score-driven backtest specifies model version V
- **THEN** the engine SHALL consume only predictions whose `model_version` equals V
- **AND** SHALL record V and `execution_timing=next_trading_day_open` in `score_config`.

#### Scenario: Non-score strategy omits model version

- **WHEN** a user runs a non-score-driven strategy without `model_version`
- **THEN** the request SHALL continue to use the existing validation and execution behavior.

#### Scenario: Result records the scoring model version

- **GIVEN** a score-driven backtest completes with an explicit model version V
- **WHEN** the result is persisted
- **THEN** it SHALL store V in a top-level `model_version` field indexed for lookups
- **AND** the persisted-result list and detail responses SHALL expose `model_version` as a top-level field.

### Requirement: Usable Score Status

Score-driven backtests SHALL exclude predictions that were not valid scoring
outputs while retaining valid predictions independently of verification maturity.

#### Scenario: Unusable predictions carry numeric scores

- **GIVEN** `BLOCKED` or `FAILED` predictions contain numeric score values
- **WHEN** a score-driven backtest loads predictions
- **THEN** those predictions SHALL NOT generate orders or contribute a consensus horizon.

#### Scenario: No usable score remains

- **GIVEN** strict model-version and status filters remove all predictions in the requested range
- **WHEN** the backtest starts
- **THEN** it SHALL return a clear no-usable-score error
- **AND** SHALL NOT fall back to another version or unusable status.

### Requirement: Backtest Friction Model

The backtest engine SHALL model real-world trading frictions and SHALL use an
execution price consistent with when each strategy's signal becomes available.

#### Scenario: Commission is deducted

- **GIVEN** a buy order is executed at price P for Q shares
- **WHEN** the backtest engine records the trade
- **THEN** it SHALL deduct commission at 0.025% of trade value with the existing minimum commission.

#### Scenario: Stamp duty is deducted on sells

- **GIVEN** a sell order is executed
- **WHEN** the backtest engine records the trade
- **THEN** it SHALL deduct the configured sell-side stamp duty from the proceeds.

#### Scenario: Slippage is applied to score-driven next-open execution

- **GIVEN** a score-driven signal is formed after trading day D closes
- **WHEN** the order executes on a later tradable day D1
- **THEN** the base execution price SHALL be D1 `open_hfq`
- **AND** the engine SHALL apply directional slippage against the order
- **AND** SHALL skip execution when D1 has no valid adjusted open rather than using D close.

#### Scenario: Friction is reported transparently

- **GIVEN** a backtest completes
- **WHEN** the result is returned
- **THEN** it SHALL include commission, stamp duty, slippage, gross return, and net return.
