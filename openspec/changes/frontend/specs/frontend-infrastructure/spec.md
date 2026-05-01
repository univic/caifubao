# Frontend Infrastructure Specification

## ADDED Requirements

### Requirement: Project initialization
The system SHALL provide a ready-to-use Vue 3 + Vite project scaffold with all necessary dependencies configured.

#### Scenario: Fresh clone setup
- **WHEN** developer runs `npm install` in the frontend directory
- **THEN** all dependencies install without errors and `npm run dev` starts the development server

#### Scenario: Build production bundle
- **WHEN** developer runs `npm run build`
- **THEN** a production-ready bundle is generated in the `dist/` directory

### Requirement: Routing system
The system SHALL support client-side routing with the following routes:

| Path | Component | Description |
|------|------------|-------------|
| `/` | DashboardView | 市场总览 |
| `/history` | HistoricalQuotesView | 历史行情 |
| `/history/:symbol` | QuoteDetailView | 股票详情/K线 |
| `/backtest` | BacktestListView | 回测列表 |
| `/backtest/new` | BacktestCreateView | 创建回测 |
| `/backtest/:id` | BacktestResultView | 回测结果 |
| `/signals` | SignalsView | 信号与机会 |

#### Scenario: Navigate to route
- **WHEN** user clicks a navigation link or enters a URL
- **THEN** the corresponding page component renders without full page reload

### Requirement: Global layout
The system SHALL provide a consistent layout with:
- Fixed sidebar navigation (collapsible on mobile)
- Top header with logo and user actions
- Main content area with breadcrumb

#### Scenario: Responsive layout
- **WHEN** user resizes window to mobile width (<768px)
- **THEN** sidebar collapses to hamburger menu

### Requirement: API service layer
The system SHALL provide a centralized API service for HTTP requests to the backend.

#### Scenario: Successful API call
- **WHEN** component calls `api.get('/stocks/search?q=平安')`
- **THEN** the response data is returned to the caller

#### Scenario: API error handling
- **WHEN** API returns non-2xx status
- **THEN** an error toast is displayed with the error message

### Requirement: State management
The system SHALL manage application state using Pinia stores.

#### Scenario: Store persists across navigation
- **WHEN** user navigates from Dashboard to History page
- **THEN** the shared state (e.g., selected market) remains available

### Requirement: Loading and error states
The system SHALL provide visual feedback during data loading and error conditions.

#### Scenario: Loading indicator
- **WHEN** component is fetching data from API
- **THEN** a skeleton/spinner is displayed in the content area

#### Scenario: Error state
- **WHEN** API call fails
- **THEN** an error message with retry button is displayed
