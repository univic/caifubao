# Signals and Opportunities Specification

## ADDED Requirements

### Requirement: Signals list page
The system SHALL display a list of trading signals with:
- Signal type (信号类型): Buy/Sell/Hold
- Stock code and name
- Signal date
- Signal strength/confidence
- Current status

#### Scenario: Load signals list
- **WHEN** user navigates to `/signals`
- **THEN** a table of current signals is displayed

#### Scenario: Filter signals
- **WHEN** user selects a filter (e.g., "只看买入")
- **THEN** the list updates to show only matching signals

#### Scenario: Sort signals
- **WHEN** user clicks column header "信号强度"
- **THEN** signals are sorted by strength descending

### Requirement: Real-time signal status
The system SHALL indicate whether a signal is:
- Active (持仓中)
- Closed (已平仓)
- Expired (已过期)

#### Scenario: Status indicator
- **WHEN** signal data loads
- **THEN** each signal shows its current status with colored badge

### Requirement: Signal detail popup
The system SHALL show detailed information when clicking on a signal.

#### Scenario: View signal details
- **WHEN** user clicks on a signal row
- **THEN** a modal/drawer shows: signal rationale, technical indicators, related news

### Requirement: Opportunities discovery page
The system SHALL display AI-recommended or factor-screened investment opportunities.

#### Scenario: Load opportunities
- **WHEN** user navigates to opportunities section
- **THEN** a list of potential opportunities is displayed with scores

### Requirement: Opportunity scoring
The system SHALL display a multi-dimensional score for each opportunity:
- Trend score (趋势得分)
- Valuation score (估值得分)
- Momentum score (动量得分)
- Overall score (综合得分)

#### Scenario: Score visualization
- **WHEN** opportunity data loads
- **THEN** each opportunity shows radar chart or progress bars for each dimension

### Requirement: Watchlist
The system SHALL allow users to add stocks to a personal watchlist.

#### Scenario: Add to watchlist
- **WHEN** user clicks "加入自选" on a stock
- **THEN** the stock is added to the user's watchlist
- **AND** a toast confirms "已加入自选"

#### Scenario: Remove from watchlist
- **WHEN** user clicks "移除自选" on a watched stock
- **THEN** the stock is removed from watchlist

### Requirement: Alert/Notification settings
The system SHALL allow users to set price alerts or signal notifications.

#### Scenario: Create alert
- **WHEN** user clicks "创建提醒" and fills form
- **THEN** an alert is created and saved

#### Scenario: Alert triggered
- **WHEN** market price hits the alert condition
- **THEN** a notification is shown to the user

### Requirement: Opportunity export
The system SHALL allow exporting opportunities to CSV.

#### Scenario: Export opportunities
- **WHEN** user clicks "导出" button
- **THEN** a CSV file downloads with opportunity data
