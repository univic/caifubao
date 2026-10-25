## ADDED Requirements

### Requirement: Research NAV SHALL accrue distributions on ex-date but spend cash only on payable date

The Research NAV engine SHALL apply supplied, explicitly dated corporate
actions only alongside an explicit daily valuation calendar. It SHALL accrue
cash entitlement on the ex-date using record-date holdings, include unpaid
receivables in NAV while excluding them from spendable order cash, and move
those receivables to cash no earlier than the payment date. It SHALL adjust
held share quantities for splits before ex-date opening fills.

#### Scenario: Ex-dividend raw-price drop

- **GIVEN** a held position recorded before an ex-date cash distribution
- **WHEN** its raw share price falls by exactly the dividend per share
- **THEN** research NAV remains constant while free cash does not increase

#### Scenario: Payable date after the simulated horizon

- **GIVEN** a dividend entitlement whose settlement is in the future
- **WHEN** NAV is rebuilt as of the last included session
- **THEN** outstanding receivables remain in NAV without premature cash credit

#### Scenario: Share transfer produces a split event

- **GIVEN** a 2:1 share multiplier on ex-date
- **WHEN** the simulation processes the raw 50-percent lower close
- **THEN** share count doubles and cost basis/total NAV are conserved

### Requirement: Incomplete corporate-action inputs SHALL fail closed

The opt-in corporate-action path SHALL reject missing record/ex trading
sessions, invalid or duplicate event identifiers, inconsistent dates and
fractional shares the research ledger cannot represent. Without an explicit
valuation calendar, no action processing may be silently assumed.

#### Scenario: Missing record-date snapshot

- **GIVEN** an ex-date dividend in an incomplete daily valuation calendar
- **WHEN** the simulator would credit its receivable
- **THEN** it raises an error rather than manufacturing a holder quantity
