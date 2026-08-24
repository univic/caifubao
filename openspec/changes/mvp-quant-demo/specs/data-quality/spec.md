## ADDED Requirements

### Requirement: Data Quality Summary

The system SHALL expose a data quality summary for the supported active A-share universe.

#### Scenario: User opens data quality page

- **GIVEN** quote, FQ factor, MA factor, and freshness metadata exists
- **WHEN** the frontend requests data quality
- **THEN** the backend SHALL return overall status, latest quote date, generated time, and coverage metrics.

### Requirement: Freshness State Classification

The data quality summary SHALL distinguish missing, stale, ahead, not-applicable, and blocked-by-quote states.

#### Scenario: Quote data is stale

- **GIVEN** quote data lags the expected latest trading day
- **WHEN** FQ or MA freshness is evaluated
- **THEN** the factor state SHALL be reported as blocked by quote freshness
- **AND** SHALL NOT be counted as an independent factor failure.

#### Scenario: Quote freshness is evaluated against a run snapshot

- **GIVEN** a quote run has frozen an expected `as_of_date`
- **WHEN** persisted quote status is recalculated
- **THEN** no quote data SHALL be classified as missing
- **AND** a latest quote date before `as_of_date` SHALL be classified as stale
- **AND** a latest quote date equal to `as_of_date` SHALL be classified as OK
- **AND** a latest quote date after `as_of_date` SHALL be classified as ahead
- **AND** the presence of any historical quote alone SHALL NOT imply OK.

#### Scenario: Suspended stock has no target-date quote

- **GIVEN** a supported stock is still listed but is temporarily suspended
- **WHEN** it has no quote row on the frozen `as_of_date`
- **THEN** its quote freshness SHALL remain stale for observability
- **AND** temporary suspension SHALL NOT change its active-universe membership.

### Requirement: Supported Universe Filtering

The data quality denominator SHALL include only supported active A-share stocks.

#### Scenario: Unsupported symbols exist

- **GIVEN** BSE or unsupported symbols exist in stock master data
- **WHEN** coverage metrics are calculated
- **THEN** those symbols SHALL be excluded from the denominator.
