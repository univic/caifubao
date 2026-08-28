> **⚠️ ARCHIVED — HISTORICAL REFERENCE ONLY**
> This file describes an abandoned architecture (Django, Celery, AkQuant).
> The current system uses Flask + datahub + Vue 3. Do NOT use as a current spec.
> See `openspec/archive/mvp-quant-demo/` for the archived specification.

# Backtest System Specification

## ADDED Requirements

### Requirement: Backtest list page
The system SHALL display a list of all backtest runs with their status and summary.

#### Scenario: Load backtest list
- **WHEN** user navigates to `/backtest`
- **THEN** a table of past backtests is displayed with columns: Name, Status, Created, Period, Return %

#### Scenario: Backtest status display
- **WHEN** a backtest is running
- **THEN** status shows "运行中" with a spinner
- **AND** when completed, status shows "完成" or "失败"

### Requirement: Create new backtest
The system SHALL provide a form to create a new backtest with:
- Strategy name (策略名称)
- Stock symbol (股票代码): e.g., sh600000, sz000001
- Time period (回测区间): start date and end date
- Initial capital (初始资金): default 1,000,000 CNY
- Strategy selection (策略): pre-built strategies (MA Cross, Momentum, etc.)
- Parameters configuration (参数配置): strategy-specific parameters

#### Scenario: Form validation
- **WHEN** user submits form with missing required fields
- **THEN** validation errors are displayed on respective fields

#### Scenario: Submit backtest
- **WHEN** user clicks "开始回测" button
- **THEN** a task is submitted to backend (AkQuant)
- **AND** user is redirected to the backtest result page

#### Scenario: Invalid stock symbol
- **WHEN** user enters non-existent stock code
- **THEN** an error message "股票代码不存在" is shown

### Requirement: Backtest task tracking
The system SHALL track the status of a running backtest task.

#### Scenario: Task status polling
- **WHEN** user is on a running backtest page
- **THEN** the page polls for task status every 5 seconds
- **AND** progress percentage is displayed

#### Scenario: Task completion
- **WHEN** the backtest task completes successfully
- **THEN** the page displays "回测完成"
- **AND** shows a link to view results

### Requirement: Backtest result - Equity curve
The system SHALL display an equity curve (收益曲线) showing:
- Portfolio value over time
- Benchmark (e.g., CSI 300) for comparison

#### Scenario: Equity curve render
- **WHEN** backtest results load
- **THEN** a line chart displays portfolio value vs benchmark

### Requirement: Backtest result - Performance metrics
The system SHALL display key performance metrics:
- Total return (总收益率)
- Annualized return (年化收益率)
- Sharpe ratio (夏普比率)
- Max drawdown (最大回撤)
- Win rate (胜率)
- Profit factor (盈利因子)
- Number of trades (交易次数)

#### Scenario: Metrics display
- **WHEN** results load
- **THEN** all metrics are calculated and displayed in cards

### Requirement: Backtest result - Position history
The system SHALL display the position changes over time.

#### Scenario: Position timeline
- **WHEN** user scrolls to position section
- **THEN** a timeline shows buy/sell events with dates and prices

### Requirement: Backtest result - Trade history
The system SHALL display a detailed trade history table:
- Date
- Action (Buy/Sell)
- Symbol
- Price
- Quantity
- Amount

#### Scenario: Trade history pagination
- **WHEN** there are many trades
- **THEN** table supports pagination

### Requirement: Backtest result - Holdings analysis
The system SHALL display holdings analysis:
- Sector allocation (行业分布)
- Stock weight distribution (持仓权重)

#### Scenario: Holdings charts
- **WHEN** user views holdings analysis
- **THEN** pie/bar charts show sector and stock distribution

### Requirement: Delete backtest
The system SHALL allow users to delete a backtest.

#### Scenario: Delete confirmation
- **WHEN** user clicks delete button
- **THEN** a confirmation dialog appears
- **AND** on confirm, the backtest is deleted from the list

### Requirement: Backtest report download
The system SHALL provide an option to download the AkQuant-generated HTML report.

#### Scenario: Download report
- **WHEN** user clicks "下载报告" button
- **THEN** an HTML file is downloaded containing the full backtest analysis
- **AND** report includes equity curve, drawdown, monthly returns heatmap

#### Scenario: View report inline
- **WHEN** user clicks "查看报告" button
- **THEN** the HTML report is displayed in an iframe or new tab
