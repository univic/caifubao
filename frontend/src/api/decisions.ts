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

export const decisionsApi = {
  getDashboard(params: { horizon?: number; limit?: number }) {
    return api.get<any>('/decisions/dashboard', { params }) as unknown as Promise<{
      data: DecisionDashboardResponse
    }>
  }
}
