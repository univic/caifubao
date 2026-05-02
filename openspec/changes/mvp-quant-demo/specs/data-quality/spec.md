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

### Requirement: Supported Universe Filtering

The data quality denominator SHALL include only supported active A-share stocks.

#### Scenario: Unsupported symbols exist

- **GIVEN** BSE or unsupported symbols exist in stock master data
- **WHEN** coverage metrics are calculated
- **THEN** those symbols SHALL be excluded from the denominator.
