> **⚠️ ARCHIVED — HISTORICAL REFERENCE ONLY**
> This file describes an abandoned architecture (Django, Celery, AkQuant).
> The current system uses Flask + datahub + Vue 3. Do NOT use as a current spec.
> See `openspec/archive/mvp-quant-demo/` for the archived specification.

# Market Dashboard Specification

## ADDED Requirements

### Requirement: Market overview display
The system SHALL display real-time market overview data including major indices, market cap, trading volume, and turnover rate.

#### Scenario: Load dashboard
- **WHEN** user navigates to the home page (`/`)
- **THEN** market overview cards load and display current market data

#### Scenario: Auto-refresh data
- **WHEN** 30 seconds have passed since last refresh
- **THEN** the dashboard automatically fetches latest data in the background

### Requirement: Major indices panel
The system SHALL display major stock indices (Shanghai, Shenzhen, ChiNext, CSI 300) with:
- Current price
- Change amount
- Change percentage
- Trend direction indicator (up/down/neutral)

#### Scenario: Index data display
- **WHEN** index data is available
- **THEN** each index card shows: name, price, change, change %, and colored indicator

### Requirement: Market breadth indicators
The system SHALL display market breadth data:
- Advance/Decline count (上涨/下跌家数)
- New highs/New lows
-涨停/跌停数量

#### Scenario: Breadth refresh
- **WHEN** breadth data updates
- **THEN** the numbers animate to reflect changes

### Requirement: Sector performance heatmap
The system SHALL display a sector performance heatmap showing:
- Sector name
- Change percentage
- Color coding (green for up, red for down)

#### Scenario: Sector heatmap render
- **WHEN** sector data loads
- **THEN** a treemap or block visualization shows all sectors with color intensity based on change magnitude

### Requirement: Top gainers/losers list
The system SHALL display lists of:
- Top 10 gainers (涨幅榜)
- Top 10 losers (跌幅榜)

Each list item shows: stock code, name, price, change percentage.

#### Scenario: List navigation
- **WHEN** user clicks on a stock in the list
- **THEN** user is navigated to the historical quote page for that stock

### Requirement: Market capital flow
The system SHALL display capital flow information:
- Northbound funds (北向资金)
- Main funds (主力资金)
- Retail funds (散户资金)

#### Scenario: Capital flow display
- **WHEN** capital flow data loads
- **THEN** net inflow/outflow amounts are displayed with directional indicators

### Requirement: Data freshness indicator
The system SHALL display the last update timestamp for all dashboard data.

#### Scenario: Timestamp display
- **WHEN** dashboard loads
- **THEN** footer shows "数据更新时间: HH:mm:ss"
