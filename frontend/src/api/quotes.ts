import api from './index'

export interface QuoteSearchItem {
  code: string
  name: string
  object_type: string | null
  market_name: string | null
  market_code: string | null
  exchange_name: string | null
  exchange_code: string | null
  active_status: number | null
  watch_level: number | null
  pre_name: string[]
  total_equity: number | null
  outstanding_share: number | null
}

export interface QuoteSearchResponse {
  query: string
  total: number
  items: QuoteSearchItem[]
}

export interface QuoteStockDetail {
  code: string
  name: string
  object_type: string | null
  market_name: string | null
  market_code: string | null
  exchange_name: string | null
  exchange_code: string | null
  active_status: number | null
  watch_level: number | null
  pre_name: string[]
  total_equity: number | null
  outstanding_share: number | null
}

export interface QuoteDailyItem {
  date: string
  open: number | null
  close: number | null
  previous_close: number | null
  high: number | null
  low: number | null
  volume: number | null
  trade_amount: number | null
  change_amount: number | null
  change_rate: number | null
  turnover_rate: number | null
}

export interface QuoteDetailResponse {
  symbol: string
  normalized_symbol: string
  stock: QuoteStockDetail
  freshness: {
    freshness_datetime: string | null
    calculated_at: string | null
    status: string | null
  } | null
  latest_quote: QuoteDailyItem | null
}

export interface QuoteDailyResponse {
  symbol: string
  normalized_symbol: string
  count: number
  quotes: QuoteDailyItem[]
}

export const quoteApi = {
  searchQuotes(query: string, limit = 10) {
    return api.get<QuoteSearchResponse>('/quotes/search', {
      params: { q: query, limit }
    }) as unknown as Promise<QuoteSearchResponse>
  },

  getQuoteDetail(symbol: string) {
    return api.get<QuoteDetailResponse>(`/quotes/${encodeURIComponent(symbol)}`) as unknown as Promise<QuoteDetailResponse>
  },

  getQuoteDaily(symbol: string, params?: { start?: string; end?: string; limit?: number }) {
    return api.get<QuoteDailyResponse>(`/quotes/${encodeURIComponent(symbol)}/daily`, {
      params
    }) as unknown as Promise<QuoteDailyResponse>
  }
}
