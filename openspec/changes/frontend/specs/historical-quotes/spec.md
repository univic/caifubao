# Historical Quotes Specification

## ADDED Requirements

### Requirement: Stock symbol search
The system SHALL allow users to search for stocks by symbol code or name.

#### Scenario: Search by code
- **WHEN** user enters stock code "000001" in search box
- **THEN** matching stocks are displayed in a dropdown

#### Scenario: Search by name
- **WHEN** user enters "平安" in search box
- **THEN** stocks containing "平安" in name are displayed

#### Scenario: Select stock from results
- **WHEN** user clicks on a stock in search results
- **THEN** the page navigates to that stock's detail page

### Requirement: Stock detail header
The stock detail page SHALL display:
- Stock code and name
- Current price
- Change amount and percentage
- Open, High, Low, Close prices
- Volume and amount
- Market cap

#### Scenario: Header data display
- **WHEN** stock data loads
- **THEN** all header fields are populated with real-time data

### Requirement: K-line chart display
The system SHALL display an interactive K-line (candlestick) chart with:
- OHLC (Open, High, Low, Close) candlesticks
- Volume bars below
- Zoom and pan functionality
- Crosshair with price/date tooltip

#### Scenario: K-line render
- **WHEN** historical data loads
- **THEN** candlesticks are drawn with green (up) and red (down) colors

#### Scenario: Time range selection
- **WHEN** user selects a different time range (1D, 1W, 1M, 3M, 6M, 1Y, ALL)
- **THEN** the chart updates to show data for the selected range

### Requirement: Technical indicators overlay
The system SHALL support overlaying technical indicators on the K-line chart:
- MA (Moving Average): MA5, MA10, MA20, MA60
- MACD
- KDJ
- BOLL (Bollinger Bands)

#### Scenario: Toggle indicator
- **WHEN** user clicks "MA5" toggle button
- **THEN** MA5 line is added to the chart; clicking again removes it

### Requirement: K-line type switching
The system SHALL allow switching between different chart types:
- K-line (Candlestick)
- Line chart
- OHLC (Bar)

#### Scenario: Chart type change
- **WHEN** user selects "Line chart" from dropdown
- **THEN** the chart re-renders as a line chart

### Requirement: Historical quote table
The system SHALL display a table of historical daily quotes below the chart:
- Date
- Open
- High
- Low
- Close
- Volume
- Change %

#### Scenario: Table pagination
- **WHEN** user clicks "Next" in pagination
- **THEN** the next page of historical data loads

### Requirement: Data export
The system SHALL allow users to export historical quotes as CSV.

#### Scenario: Export CSV
- **WHEN** user clicks "Export" button
- **THEN** a CSV file downloads containing all visible historical data
