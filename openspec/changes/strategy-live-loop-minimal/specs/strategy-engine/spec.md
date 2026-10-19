## MODIFIED Requirements

### Requirement: Paper NAV simulation uses realistic T+1 cost semantics

Paper NAV MUST simulate entry at the next market-calendar-session open after a
signal decision, per-side commission (with minimum), sell stamp duty (0.0005),
slippage, the two-way A-share transfer fee (0.00001 per side), board-aware
minimum order sizes, and suspension roll-forward, using the same execution
parameters as the paper/backtest model (commission_rate 0.00025,
minimum_commission_cny 5.0, sell_stamp_duty_rate 0.0005, transfer_fee_rate
0.00001, slippage_per_side 0.001). Opening order sizing and turnover
denominators MUST use only the execution-day open or prior known marks; same-day
closes are available only for post-trade marking. Suspended or unknown statuses
MUST NOT be converted to tradable, and a session whose open is at its price
limit MUST NOT fill the blocked side. NAV, drawdown, daily return, and turnover
are computed per rebalance cycle; the same-date tradable-universe equal-weight
return is a diagnostic baseline whose daily alignment remains subject to a
separate validation slice.

#### Scenario: Suspended names hold last observed valuation

- GIVEN a held name that suspends after a trading day with a known close
- WHEN paper NAV is computed for the suspension day (no executable quote)
- THEN the name is valued at its last observed close, not its entry price
- AND no forced mark or valuation failure occurs
- AND an unknown or suspended status is not converted to tradable

#### Scenario: NAV marks to market with costs

- GIVEN a target portfolio and a subsequent price series
- WHEN paper NAV is computed
- THEN realized trades deduct commission, slippage, sell stamp duty and the
  two-way transfer fee
- AND the target decision executes on its `execution_date`, with suspended names
  rolling forward to the next executable open instead of failing the valuation
- AND an order at a limit-locked open is refused and reported rather than filled
- AND per-cycle turnover (buy + sell notional / pre-cycle NAV) is reported

#### Scenario: Equity curve is comparable to equal-weight baseline

- GIVEN a paper NAV series over ≥2 rebalance cycles
- WHEN compared to the same-date equal-weight benchmark
- THEN the strategy equity curve and the benchmark curve are both recorded
  with per-date values

#### Scenario: NAV curve recomputes from persisted runs

- GIVEN a range with ≥1 COMPLETED StrategyPaperRun carrying target holdings
- WHEN the operator runs `strategy_runner nav --from <date> --to <date>`
- THEN the operator supplies the exact full strategy config used by those runs,
  including `initial_nav`, and the matching `config_hash` is selected
- AND the runs are sorted by signal date while execution dates form the
  rebalance schedule
- AND quote prices (open/close/trade_status/previous_close) plus the diagnostic
  equal-weight benchmark are loaded through the last selected `execution_date`
- AND `simulate_paper_nav` runs with realistic T+1 costs
- AND each curve point (nav/daily_return/turnover/drawdown/benchmark_return) is
  attached to its originating signal record and written into that run's
  `nav_snapshot`
- AND only the same `timing_version=paper_causal_v1` track is used

#### Scenario: NAV recompute with no runs is reported, not crashed

- GIVEN a range with no COMPLETED runs for the configured model version
- WHEN the operator runs `strategy_runner nav --from <date> --to <date>`
- THEN the command returns a no-runs result with a reason
- AND nothing is written
- AND a range whose held codes have no quote rows also returns a reason
  instead of writing a flat cash nav_snapshot
