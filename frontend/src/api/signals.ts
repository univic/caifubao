import api from './index'

export interface SignalItem {
  stock_code: string
  stock_name: string | null
  category: string | null
  date: string
  signal_name: string
  signal_version: string | null
  direction: string
  signal_type: string
  strength: number | null
  reason: string | null
  price_snapshot: Record<string, unknown>
  factor_snapshot: Record<string, unknown>
  source_freshness: Record<string, unknown>
  generated_at: string | null
}

export interface SignalListResponse {
  date: string | null
  requested_date: string | null
  total: number
  limit: number
  offset: number
  items: SignalItem[]
}

export interface SignalListParams {
  date?: string
  signal_name?: string
  direction?: string
  limit?: number
  offset?: number
}

export const signalApi = {
  listSignals(params?: SignalListParams) {
    return api.get<SignalListResponse>('/signals', { params }) as unknown as Promise<SignalListResponse>
  }
}
