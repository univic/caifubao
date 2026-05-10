import api from './index'
import type { ScoreSummary } from './scores'

export interface MarketComprehensiveItem {
  code: string
  name: string
  ohlcv: {
    open: number | null
    high: number | null
    low: number | null
    close: number | null
    volume: number | null
    change_rate: number | null
  }
  evaluation: {
    primary_horizon: number
    score: number | null
    rank: number
    display_rank: number
    percentile: number | null
    recommendation: string
    basis?: {
      signals?: string[]
      trend?: string[]
    }
    status: string | null
    verification: Record<string, unknown>
    model_version: string | null
    profit_percentage_t5: number | null
    max_profit_percentage: number | null
    is_effective: boolean | null
    scores: Record<string, ScoreSummary>
  }
}

export interface MarketComprehensiveResponse {
  success: boolean
  date: string
  total: number
  page: number
  per_page: number
  items: MarketComprehensiveItem[]
}

export const marketApi = {
  getComprehensiveData(params: {
    date?: string
    type: 'stock' | 'index'
    horizon?: number
    page?: number
    per_page?: number
    q?: string
  }) {
    return api.get<any, MarketComprehensiveResponse>('/market/comprehensive', { params })
  },
  
  getMarketOverview() {
    return api.get('/market/overview')
  }
}
