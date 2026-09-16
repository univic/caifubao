## ADDED Requirements

### Requirement: Paper decisions do not require future outcome verification
The runner SHALL consume usable scoring outputs without conditioning on future
verification results. PENDING, TRACKING, VERIFIED and INSUFFICIENT_DATA SHALL be
eligible lifecycle states; BLOCKED and FAILED scoring rows SHALL be excluded.

#### Scenario: Later verification cannot select the cohort
- WHEN a usable score moves from PENDING to VERIFIED or INSUFFICIENT_DATA
- THEN selection is unchanged for identical scores and signal-day eligibility

### Requirement: Execution follows the signal session
The runner SHALL persist the signal date, actual UTC decision_at and a strictly
later next market-calendar execution_date. Calendar gaps SHALL fail closed.

#### Scenario: Friday signal crosses a holiday
- WHEN the next market-calendar session after a Friday is Tuesday
- THEN the paper order executes Tuesday and never Friday or Monday

#### Scenario: Calendar is exhausted
- WHEN no later session is available in the calendar
- THEN the runner rejects the request without a completed target record

### Requirement: Opening trades use only opening information
NAV SHALL size opening orders and turnover using opening prices or prior known
marks, never same-day closing prices. Suspended or unknown status SHALL NOT
be converted to tradable; invalid opening prices SHALL NOT produce orders.

#### Scenario: Future close is changed
- WHEN only an execution day's closing price changes
- THEN that day's opening trades and opening turnover denominator are unchanged
- AND closing NAV may change

### Requirement: Paper tracks and NAV dates are isolated
Normalized config SHALL include paper_causal_v1 timing semantics. Previous
holdings and NAV SHALL use the exact same config hash. NAV signal-date bounds
SHALL load quotes through the last execution date and attach each execution
point to its originating signal record. Missing execution coverage SHALL fail
closed without writing a misleading curve.

#### Scenario: Another capital or strategy config exists
- WHEN NAV or previous holdings are queried for one config hash
- THEN another config hash and legacy timing rows are not consumed

### Requirement: Retrospective records cannot certify forward evidence
This runner version SHALL persist evidence_kind REPLAY for all new records.
Legacy rows without provenance, historical backfills, replacements and NAV
recomputations SHALL NOT count as frozen forward sessions. Job SUCCESS counts
SHALL NOT be interpreted as the 120-session promotion gate.

#### Scenario: Historical replay succeeds
- WHEN an operator backfills a historical date and computes its NAV
- THEN the result remains REPLAY regardless of success or date count
