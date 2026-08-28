## ADDED Requirements

### Requirement: Portfolio Accounting Models

The system SHALL store research portfolio accounting state in portfolio, position, transaction, and snapshot records.

#### Scenario: Portfolio data is persisted

- **GIVEN** a user creates and trades in a research portfolio
- **WHEN** the backend persists the state
- **THEN** it SHALL store portfolio cash, positions, transaction history, and snapshots
- **AND** SHALL NOT treat the portfolio as a brokerage account or financial service.

### Requirement: Manual Transaction Processing

The backend SHALL process manual portfolio transactions and update cash and positions.

#### Scenario: User records a buy transaction

- **GIVEN** a portfolio has enough cash
- **WHEN** the user records a `BUY` transaction with stock code, quantity, price, and fee
- **THEN** cash SHALL decrease
- **AND** the position quantity and average cost SHALL update.

#### Scenario: User records a sell transaction

- **GIVEN** a portfolio has enough quantity in the position
- **WHEN** the user records a `SELL` transaction
- **THEN** cash SHALL increase
- **AND** the position quantity and realized PnL SHALL update.

#### Scenario: User records a cash transaction

- **GIVEN** a portfolio exists
- **WHEN** the user records `CASH_IN`, `CASH_OUT`, or `DIVIDEND`
- **THEN** cash SHALL be adjusted without changing stock quantity.

### Requirement: Portfolio Read APIs

The backend SHALL expose portfolio summary, holdings, transactions, and snapshots.

#### Scenario: User opens a portfolio

- **GIVEN** a portfolio exists
- **WHEN** the frontend requests portfolio detail
- **THEN** the backend SHALL return total value, cash, positions value, return, return percentage, and position count.

#### Scenario: User views current holdings

- **GIVEN** positions exist for a portfolio
- **WHEN** the frontend requests positions
- **THEN** the backend SHALL include quantity, average cost, latest market price, market value, unrealized PnL, realized PnL, weight, and quote date.

### Requirement: Portfolio Frontend

The frontend SHALL provide a portfolio management page for the MVP accounting workflow.

#### Scenario: User manages a portfolio

- **GIVEN** the user opens the portfolio page
- **WHEN** portfolios, positions, and transactions exist
- **THEN** the page SHALL show portfolio metrics, holdings, transaction entry, transaction history, and snapshot action.

### Requirement: Score-to-Portfolio Next Step

The system SHALL treat score-driven rebalance preview as the next integration after manual portfolio accounting.

#### Scenario: Rebalance preview is requested later

- **GIVEN** score predictions and a portfolio exist
- **WHEN** a future feature generates a rebalance preview
- **THEN** it SHALL explain candidate entries, exits, holds, and target weights before creating transactions.
