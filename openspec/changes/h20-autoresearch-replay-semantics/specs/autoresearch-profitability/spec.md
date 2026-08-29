# H20 Autoresearch Replay Semantics

## ADDED Requirements

### Requirement: H20 candidate selection preserves final-test isolation

The H20 autoresearch loop SHALL use validation or quarterly walk-forward
validation metrics for candidate keep/discard decisions and SHALL keep the
final test split unread until candidate selection is frozen.

#### Scenario: Candidate is compared with the current best

- **GIVEN** an H20 candidate and baseline have been evaluated
- **WHEN** autoresearch decides whether to keep the candidate
- **THEN** it SHALL compare their mechanical profitability metrics using only
  validation or quarterly walk-forward validation observations
- **AND** it SHALL NOT read or derive any test-period candidate metric.

#### Scenario: Frozen candidate receives final evaluation

- **GIVEN** train and validation selection has produced one frozen candidate
- **WHEN** the final H20 evaluation is authorized
- **THEN** the system SHALL evaluate the locked test split exactly once
- **AND** the test result SHALL be reported as final research evidence rather
  than fed back into candidate selection.

### Requirement: H20 replay starts holding time at actual entry

The H20 research replay SHALL attempt execution using only information
available by each attempted session and SHALL measure the 20-session holding
period from the actual entry session.

#### Scenario: Entry executes on the next trading session

- **GIVEN** a signal is generated after the scoring-date close
- **AND** the next trading session is executable for a buy
- **WHEN** H20 replay resolves the entry
- **THEN** it SHALL use that session's open as the actual entry price
- **AND** it SHALL record requested and actual entry dates as that session.

#### Scenario: Blocked buy rolls forward

- **GIVEN** a pending H20 buy order
- **WHEN** an attempted session is suspended or is limit-up according to the
  existing A-share execution rule
- **THEN** replay SHALL not fill the order on that session
- **AND** it SHALL retry at each subsequent trading-session open until the
  first executable buy session
- **AND** it SHALL record the actual entry date and entry blocked-session count.

#### Scenario: Exit is anchored to actual entry

- **GIVEN** an H20 position was filled on actual entry session E
- **WHEN** replay schedules its normal exit
- **THEN** the requested exit SHALL be the open of the twentieth trading
  session strictly after E
- **AND** no scoring-date or pre-entry observation SHALL shorten that holding
  clock.

#### Scenario: Blocked sell rolls forward

- **GIVEN** a normal H20 exit is due
- **WHEN** an attempted session is suspended or is limit-down according to the
  existing A-share execution rule
- **THEN** replay SHALL not fill the sell on that session
- **AND** it SHALL retry at each subsequent trading-session open until the
  first executable sell session
- **AND** it SHALL record the actual exit date and exit blocked-session count.

#### Scenario: Required future execution cannot be observed

- **GIVEN** an entry or exit cannot be resolved within the immutable snapshot
  range
- **WHEN** snapshot eligibility is evaluated
- **THEN** the row SHALL remain auditable with a non-eligible reason
- **AND** it SHALL not contribute a return or candidate metric.

### Requirement: H20 snapshot separates features from execution labels

The immutable H20 snapshot SHALL keep scoring-date features separate from
future entry and exit labels and SHALL expose enough metadata to audit replay.

#### Scenario: Component reconstruction prevents look-ahead

- **GIVEN** a component row for scoring date T
- **WHEN** any scoring component is reconstructed
- **THEN** every quote, factor, signal, industry, and benchmark input to that
  component SHALL have an effective date less than or equal to T
- **AND** requested/actual entry and exit dates and prices SHALL not enter the
  component calculation.

#### Scenario: Execution labels are auditable

- **GIVEN** an exported H20 snapshot row
- **WHEN** the row is inspected
- **THEN** it SHALL include requested_entry_date, actual_entry_date,
  actual_entry_open_hfq, entry_blocked_sessions, requested_exit_date,
  actual_exit_date, actual_exit_open_hfq, exit_blocked_sessions, eligibility,
  and eligibility_reason
- **AND** actual entry and exit fields SHALL be the only price labels used for
  eligibility, benchmark returns, strategy returns, and profitability metrics
- **AND** the manifest SHALL record the snapshot checksum and covered range.
