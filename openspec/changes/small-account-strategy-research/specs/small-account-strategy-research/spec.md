# Small-account strategy research specification

## ADDED Requirements

### Requirement: Holding scan is causal and uses existing labels

The holding scan SHALL consume the frozen panel's `fwd_h{h}` labels without
rebuilding labels or rolling blocked entry/exit dates.  Rebalance dates SHALL be
anchored to the sessions inside the evaluated window and spaced by the selected
horizon.

#### Scenario: Leading history does not shift the schedule

- GIVEN two inputs with identical evaluation windows and sufficient indicators
- WHEN one input contains additional leading history
- THEN their rebalance dates and results inside the window are identical

#### Scenario: Blocked labels are not rolled

- GIVEN a stock whose horizon label is unresolved on a rebalance date
- WHEN the scan constructs the target and equal-weight control
- THEN that observation is not moved to a later date
- AND it does not contribute a return on the original date

### Requirement: Buffer is genuine hysteresis

Entry SHALL require rank percentile at or inside `entry_pct`.  A currently held
name SHALL remain held while its rank is at or inside
`min(1, entry_pct * buffer)` and SHALL exit only after leaving that wider band
or becoming unmeasurable on that rebalance date.  The horizon SHALL define the
rebalance cadence, not force liquidation of every holding.

#### Scenario: Wider buffer retains a name

- GIVEN a held name whose next rank is outside the entry band but inside the
  wider exit band
- WHEN the same panel is scanned with buffer 1 and with a wider buffer
- THEN the wider-buffer target retains the name
- AND its replacement turnover is lower

### Requirement: Friction follows replaced weight

One-way turnover SHALL be the fraction of the current target book not present
in the previous target book.  The initial target SHALL have turnover 1.
Round-trip friction SHALL be charged in proportion to that turnover for both
the strategy basket and equal-weight control, rather than once per full book on
every rebalance.

#### Scenario: Unchanged target does not pay another round trip

- GIVEN identical consecutive target books
- WHEN the second rebalance is measured
- THEN strategy replacement turnover is zero
- AND no additional strategy round-trip friction is deducted for that period

### Requirement: Research replay cannot become forward evidence

ETF and small-book simulations SHALL apply a new target no earlier than the
following return period. Rebalance schedules SHALL be invariant to additional
leading history. The ETF pool SHALL identify its liquidity snapshot date; the
CLI SHALL NOT claim a survivorship-complete PIT universe because it does not
reconstruct historical membership, delisted instruments, or exchange-calendar
complete labels. Ledger entries created from historical or current-as-of
reconstruction SHALL be labelled `REPLAY`, SHALL copy their input payloads, and
SHALL NOT count toward the production immutable-forward 120-session gate.

#### Scenario: Replay ledger entry is non-promotional

- GIVEN a historical or current-as-of research signal
- WHEN it is appended to the local research ledger
- THEN the entry records `evidence_kind=REPLAY`
- AND it is not treated as an executable order or certified forward evidence

#### Scenario: Current-snapshot replay is not presented as PIT

- GIVEN a historical rotation run using a pool frozen from one liquidity date
- WHEN the operator reads the CLI contract or public documentation
- THEN the run is identified as a current-snapshot replay
- AND no historical-membership or exchange-calendar completeness is claimed

### Requirement: Tools remain operator-only research surfaces

The new helpers and CLI SHALL NOT expose a public REST endpoint, schedule
themselves, persist Mongo backtests, promote a model, or mutate an account.
Research reports SHALL state material limitations and SHALL NOT claim reliable
timing or selection alpha when the controls do not support such a conclusion.

#### Scenario: Historical control matches or beats timing

- GIVEN a replay where a static allocation matches or beats the timing overlay
- WHEN the result is documented
- THEN the report identifies the result as a negative timing-alpha finding
- AND leaves promotion and forward certification incomplete
