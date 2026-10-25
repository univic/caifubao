## MODIFIED Requirements

### Requirement: Research ledger SHALL distinguish dividend entitlement from spendable cash

The Research simulation ledger SHALL accrue cash distributions as separately
identified receivables on the ex-date, using a verifiable record-date holding
quantity. It SHALL include accrued receivables in account NAV, but SHALL NOT
allow receivables to fund new buys until a matching settlement event at or
after the payable date. Event IDs SHALL be idempotent; conflicting duplicates
and invalid dates or amounts SHALL fail without state mutation.

#### Scenario: Ex-date price fall is offset by the dividend receivable

- **GIVEN** a held stock entitles its record-date owner to 0.5 CNY per share
- **WHEN** a 100-share holding falls from 10.0 to 9.5 at ex-date
- **THEN** the Research ledger books a 50 CNY receivable and preserves NAV,
  without increasing cash available to the execution model

#### Scenario: The shareholder sells after ex-date but before payment

- **GIVEN** a correctly accrued dividend entitlement and an intervening sale
- **WHEN** the payable-date settlement is applied
- **THEN** the Research ledger converts that receivable into cash and does not
  require the original position to remain held

#### Scenario: Invalid or duplicate corporate actions

- **GIVEN** an early settlement, mismatched entitled share count or conflicting event ID
- **WHEN** the ledger applies the event
- **THEN** the operation fails atomically, without inventing a dividend or
  silently duplicating cash

#### Scenario: Replay exactly reconstructs receivables

- **GIVEN** a sequence of fills, entitlements, settlements and marks
- **WHEN** a new Research ledger replays those events
- **THEN** its cash, positions, receivables, realized P&L and NAV match exactly
