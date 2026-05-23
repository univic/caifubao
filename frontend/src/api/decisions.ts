import api from './index'

export interface DecisionDashboardItem {
  stock_code: string
  stock_name: string
  score: number
  rank: number
  recommendation: string // BUY | WATCH | AVOID | NONE
  confidence: string // high | medium | low
  sample_size: number
  hit_rate: number | null
  trend: string // improving | declining | stable
  invalidation: {
    exit_threshold: number | null
    stop_loss_pct: number | null
    expiry_days: number | null
  }
  position_sizing: {
    target_weight_pct: number | null
    max_shares: number | null
    capacity_check: string | null
  }
}

export interface DecisionDashboardHorizon {
  horizon: number
  date: string
  count: number
  items: DecisionDashboardItem[]
}

export interface DecisionDashboardResponse {
  primary_horizon: number
  score5: DecisionDashboardHorizon | null
  score20: DecisionDashboardHorizon | null
  score60: DecisionDashboardHorizon | null
}

// -------- Journal types --------

export interface JournalEntry {
  id: string
  date: string
  stock_code: string
  stock_name: string
  recommended_action: string // BUY | SELL | HOLD | WATCH
  confidence: string // high | medium | low
  entry_price: number | null
  target_price: number | null
  stop_loss: number | null
  position_size_pct: number | null
  executed: boolean
  executed_price: number | null
  executed_quantity: number | null
  execution_type: string | null // followed | deviated | missed
  realized_pnl: number | null
  realized_pnl_pct: number | null
  notes: string | null
  created_at: string
}

export interface JournalListResponse {
  items: JournalEntry[]
  total: number
  page: number
  per_page: number
}

export interface JournalSummary {
  model_quality: {
    hit_rate: number
    total_recommendations: number
    effective_recommendations: number
  }
  execution_discipline: {
    follow_through_rate: number
    followed: number
    deviated: number
    missed: number
  }
  total_pnl: number
  total_trades: number
  total_realized_pnl: number
  avg_realized_pnl: number
}

export interface JournalAttributionRow {
  component_id?: string
  horizon?: number
  pnl: number
  pnl_pct: number
  trade_count: number
}

export interface JournalAttribution {
  by_component: JournalAttributionRow[]
  by_horizon: JournalAttributionRow[]
}

export interface JournalCreatePayload {
  stock_code: string
  recommended_action: string
  confidence: string
  entry_price?: number
  target_price?: number
  stop_loss?: number
  position_size_pct?: number
  executed: boolean
  executed_price?: number
  executed_quantity?: number
  notes?: string
}

// -------- Rebalance types --------

export interface RebalancePreviewItem {
  stock_code: string
  stock_name: string
  current_score: number
  recommendation: string
  action: string // BUY_MORE | SELL | REDUCE | HOLD
  reason: string
}

export interface RebalancePreviewPayload {
  portfolio_stocks: string[]
  cash: number
}

// -------- Watchlist types --------

export interface WatchlistItem {
  id: string
  name: string
  stock_count: number
  created_at: string
}

export interface WatchlistStock {
  stock_code: string
  stock_name: string
  score5: number | null
  score20: number | null
  score60: number | null
  recommendation: string
  current_price: number | null
}

export interface WatchlistDetail {
  id: string
  name: string
  stock_codes: string[]
  stocks: WatchlistStock[]
  created_at: string
}

export interface WatchlistCreatePayload {
  name: string
  stock_codes: string[]
}

export const decisionsApi = {
  getDashboard(params: { horizon?: number; limit?: number }) {
    return api.get<any>('/decisions/dashboard', { params }) as unknown as Promise<{
      data: DecisionDashboardResponse
    }>
  },

  // Journal
  getJournal(params?: {
    page?: number
    per_page?: number
    execution_type?: string
    stock_code?: string
    start_date?: string
    end_date?: string
  }) {
    return api.get<any>('/decisions/journal', { params }) as unknown as Promise<{
      data: JournalListResponse
    }>
  },

  postJournal(payload: JournalCreatePayload) {
    return api.post<any>('/decisions/journal', payload) as unknown as Promise<{
      data: { id: string }
    }>
  },

  getJournalSummary() {
    return api.get<any>('/decisions/journal/summary') as unknown as Promise<{
      data: JournalSummary
    }>
  },

  getJournalAttribution() {
    return api.get<any>('/decisions/journal/attribution') as unknown as Promise<{
      data: JournalAttribution
    }>
  },

  // Rebalance
  postRebalancePreview(payload: RebalancePreviewPayload) {
    return api.post<any>('/decisions/rebalance-preview', payload) as unknown as Promise<{
      data: { items: RebalancePreviewItem[] }
    }>
  },

  // Watchlists
  getWatchlists() {
    return api.get<any>('/decisions/watchlists') as unknown as Promise<{
      data: { items: WatchlistItem[]; total: number }
    }>
  },

  createWatchlist(payload: WatchlistCreatePayload) {
    return api.post<any>('/decisions/watchlists', payload) as unknown as Promise<{
      data: { id: string }
    }>
  },

  getWatchlist(id: string) {
    return api.get<any>(`/decisions/watchlists/${id}`) as unknown as Promise<{
      data: WatchlistDetail
    }>
  },

  deleteWatchlist(id: string) {
    return api.delete<any>(`/decisions/watchlists/${id}`) as unknown as Promise<{
      data: { success: boolean }
    }>
  }
}
