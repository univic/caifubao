> **⚠️ ARCHIVED — HISTORICAL REFERENCE ONLY**
> This file describes an abandoned architecture (Django, Celery, AkQuant).
> The current system uses Flask + datahub + Vue 3. Do NOT use as a current spec.
> See  for the active specification.

# Frontend Implementation Tasks

## 1. Project Setup

- [x] 1.1 Initialize Vite + Vue 3 + TypeScript project
- [x] 1.2 Install dependencies: element-plus, echarts, pinia, vue-router, axios
- [x] 1.3 Configure vite.config.ts (proxy, aliases)
- [x] 1.4 Set up project directory structure
- [ ] 1.5 Configure ESLint and Prettier

## 2. Infrastructure

- [x] 2.1 Implement Vue Router configuration
- [x] 2.2 Create global layout component (Sidebar, Header)
- [x] 2.3 Set up Pinia stores (appStore, marketStore)
- [x] 2.4 Create API service layer (axios wrapper)
- [x] 2.5 Implement loading/error state components
- [x] 2.6 Add CSS variables and global styles

## 3. Authentication Module

- [x] 3.1 Create Login page
- [x] 3.2 Create Register page
- [x] 3.3 Implement JWT token storage and retrieval
- [x] 3.4 Add request interceptor (attach token)
- [x] 3.5 Add response interceptor (handle 401)
- [x] 3.6 Implement token refresh logic
- [x] 3.7 Create logout functionality
- [x] 3.8 Add route guards (protected routes)
- [x] 3.9 Create password reset flow

## 4. User & Permissions Module

- [ ] 4.1 Create user store with auth state
- [ ] 4.2 Implement role-based menu rendering
- [ ] 4.3 Create Profile page (view/edit)
- [ ] 4.4 Create Admin user management page (list)
- [ ] 4.5 Implement user edit/disable/delete actions
- [ ] 4.6 Add permission-based component rendering
- [ ] 4.7 Create "Access Denied" page

## 5. Dashboard Module

- [ ] 5.1 Create Dashboard page layout
- [ ] 5.2 Implement index overview cards (上证/深证/创业板/科创50)
- [ ] 5.3 Implement market breadth indicators (涨跌停/涨跌家数)
- [ ] 5.4 Build sector performance heatmap
- [ ] 5.5 Create top gainers/losers lists
- [ ] 5.6 Add capital flow display
- [ ] 5.7 Implement auto-refresh (30s interval)
- [ ] 5.8 Add data freshness timestamp

## 6. Historical Quotes Module

- [ ] 6.1 Create stock search component with autocomplete
- [ ] 6.2 Implement stock detail header component
- [ ] 6.3 Build K-line chart with ECharts
- [ ] 6.4 Add time range selector (1D/1W/1M/3M/6M/1Y/ALL)
- [ ] 6.5 Implement technical indicators (MA, MACD, KDJ, BOLL)
- [ ] 6.6 Add chart type switching (K-line/Line/OHLC)
- [ ] 6.7 Create historical quote table with pagination
- [ ] 6.8 Implement CSV export functionality

## 7. Backtest Module

- [ ] 7.1 Create backtest list page with table
- [ ] 7.2 Implement backtest creation form
- [ ] 7.3 Add form validation (stock pool, date range, parameters)
- [ ] 7.4 Implement task submission to backend
- [ ] 7.5 Build task status polling mechanism
- [ ] 7.6 Create equity curve chart
- [ ] 7.7 Implement performance metrics display
- [ ] 7.8 Build position history timeline
- [ ] 7.9 Create trade history table
- [ ] 7.10 Add holdings analysis charts (sector/stock distribution)
- [ ] 7.11 Implement delete backtest with confirmation

## 8. Signals & Opportunities Module

- [ ] 8.1 Create signals list page
- [ ] 8.2 Implement signal filtering (type, status)
- [ ] 8.3 Add signal sorting functionality
- [ ] 8.4 Build signal detail modal/drawer
- [ ] 8.5 Create opportunities discovery page
- [ ] 8.6 Implement opportunity scoring visualization (radar/progress bars)
- [ ] 8.7 Add watchlist functionality (add/remove)
- [ ] 8.8 Implement alert/notification settings
- [ ] 8.9 Add opportunities export to CSV

## 9. Testing & Polish

- [ ] 9.1 Add responsive layout testing
- [ ] 9.2 Implement error boundary components
- [ ] 9.3 Add loading skeletons for better UX
- [ ] 9.4 Optimize bundle size (code splitting)
- [ ] 9.5 Test API error handling flows

## 10. Build & Deploy

- [ ] 10.1 Configure production build
- [ ] 10.2 Create Dockerfile for frontend
- [ ] 10.3 Set up environment variables handling
- [ ] 10.4 Test production build locally

---

## Implementation Notes

### Task Dependencies
- Tasks in Section 1-2 must be completed before any module work
- Section 3-4 should be completed before protected pages
- Section 5-8 can be worked on in parallel after auth is ready
- Section 9 should be done after all features are implemented
- Section 10 is final step

### Verification Criteria
- Each task is complete when:
  - Code is written and passes lint
  - Feature works as specified in SPEC.md
  - No console errors in browser

### Time Estimate (Optional)
- Section 1-2: ~1 week
- Section 3-4: ~1 week
- Section 5: ~1 week
- Section 6: ~1 week
- Section 7: ~1 week
- Section 8: ~1 week
- Section 9-10: ~1 week

Total: ~7 weeks
