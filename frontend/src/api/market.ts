import api from './index'

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
    score: number | null
    rank: number
    display_rank: number
    recommendation: string
    basis?: any
    status: string | null
    profit_percentage_t5: number | null
    max_profit_percentage: number | null
    is_effective: boolean | null
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
