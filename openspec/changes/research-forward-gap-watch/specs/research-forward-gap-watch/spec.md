## ADDED Requirements

### Requirement: Research FORWARD watcher SHALL report missing certified signal sessions

The watcher SHALL compare every China A-share trading day at or after an
operator-certified ACTIVE forward window's start date with distinct completed
FORWARD strategy records matching that window's exact spec hash, model version,
horizon and hash kind. It SHALL reject REPLAY, SKIPPED, legacy or wrong-spec
records as evidence and return a nonzero status if eligible days are absent.

#### Scenario: Missing strategy session

- **GIVEN** an ACTIVE spec-certified window spanning multiple completed trade days
- **WHEN** any of those sessions has no matching FORWARD+COMPLETED record
- **THEN** the watcher identifies the missing dates and returns an unhealthy result

#### Scenario: Retrospective plan cannot fill a gap

- **GIVEN** a REPLAY or wrong-hash plan on a missing trading day
- **WHEN** the watcher counts legitimate evidence
- **THEN** that plan is excluded from the FORWARD count

#### Scenario: Market holiday

- **GIVEN** the calendar excludes today's date and includes future sessions
- **WHEN** the daily checker starts on a weekday market holiday
- **THEN** it reports NON_TRADING_DAY without generating a missing evidence alert

### Requirement: Research monitoring SHALL be disabled by default and read-only

The public Research template SHALL suspend the watcher until a separate
operator-controlled private deployment enables it. The watcher SHALL NOT
write plans, alter the certified window, fill trading orders or backdate
research evidence.

#### Scenario: Alerting an absent capture

- **GIVEN** a scheduled monitor after the daily capture cutoff and missing evidence
- **WHEN** it evaluates the Research DB
- **THEN** it returns a nonzero process code and optionally sends the existing
  webhook report if the operator has configured a valid destination
