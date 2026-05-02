import api from './index'

export interface PortfolioSummary {
  cash: number
  positions_value: number
  total_value: number
  total_return: number
  total_return_pct: number | null
  position_count: number
}

export interface Portfolio {
  id: string
  name: string
  description: string
  base_currency: string
  benchmark: string
  initial_cash: number
  cash: number
  status: string
  summary: PortfolioSummary
  created_at: string
  updated_at: string
}

export interface PortfolioPosition {
  id: string
  stock_code: string
  stock_name: string
  quantity: number
  avg_cost: number
  market_price: number
  market_value: number
  cost_value: number
  unrealized_pnl: number
  unrealized_pnl_pct: number | null
  realized_pnl: number
  weight: number | null
  quote_date: string | null
  updated_at: string | null
}

export interface PortfolioTransaction {
  id: string
  portfolio_id: string
  stock_code: string | null
  stock_name: string | null
  side: string
  quantity: number
  price: number
  fee: number
  amount: number
  trade_date: string
  reason: string | null
  source_score_id: string | null
  created_at: string
}

export interface PortfolioSnapshot {
  id: string
  date: string
  total_value: number
  cash: number
  positions_value: number
  daily_return: number | null
  drawdown: number | null
  holdings: PortfolioPosition[]
  created_at: string
}

export interface CreatePortfolioPayload {
  name: string
  description?: string
  initial_cash: number
  benchmark?: string
}

export interface CreateTransactionPayload {
  side: string
  stock_code?: string
  stock_name?: string
  quantity?: number
  price: number
  fee?: number
  trade_date?: string
  reason?: string
  source_score_id?: string
}

export const portfolioApi = {
  listPortfolios() {
    return api.get<{ items: Portfolio[] }>('/portfolios') as unknown as Promise<{ items: Portfolio[] }>
  },
  createPortfolio(payload: CreatePortfolioPayload) {
    return api.post<Portfolio>('/portfolios', payload) as unknown as Promise<Portfolio>
  },
  getPortfolio(id: string) {
    return api.get<Portfolio>(`/portfolios/${id}`) as unknown as Promise<Portfolio>
  },
  getPositions(id: string) {
    return api.get<{ summary: PortfolioSummary; items: PortfolioPosition[] }>(`/portfolios/${id}/positions`) as unknown as Promise<{ summary: PortfolioSummary; items: PortfolioPosition[] }>
  },
  getTransactions(id: string) {
    return api.get<{ items: PortfolioTransaction[] }>(`/portfolios/${id}/transactions`) as unknown as Promise<{ items: PortfolioTransaction[] }>
  },
  createTransaction(id: string, payload: CreateTransactionPayload) {
    return api.post<PortfolioTransaction>(`/portfolios/${id}/transactions`, payload) as unknown as Promise<PortfolioTransaction>
  },
  createSnapshot(id: string) {
    return api.post<PortfolioSnapshot>(`/portfolios/${id}/snapshots`) as unknown as Promise<PortfolioSnapshot>
  }
}
