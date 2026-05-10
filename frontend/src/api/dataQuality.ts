import api from './index'

export type DataQualityStatus =
  | 'OK'
  | 'WARN'
  | 'ERROR'
  | 'STALE'
  | 'MISSING'
  | 'AHEAD'
  | 'BLOCKED_BY_QUOTE'

export interface DataQualityCoverage {
  total: number
  ok: number
  missing: number
  stale: number
  ahead: number
  blocked: number
  ok_rate: number
}

export interface DataQualityScope {
  total_active: number
  excluded_unsupported: number
  effective_total: number
  unsupported_markets: string[]
}

export interface IndustryCoverage {
  total_classified: number
  industry_count: number
  last_sync: string | null
}

export interface DataQualitySummary {
  status: 'OK' | 'WARN' | 'ERROR'
  generated_at: string
  latest_quote_date: string | null
  scope: DataQualityScope
  coverage: {
    overall: DataQualityCoverage
    quote: DataQualityCoverage
    fq_factor: DataQualityCoverage
    ma_factor: DataQualityCoverage
    industry: IndustryCoverage
  }
}

export interface DataQualityItem {
  code: string
  name: string
  object_type: string
  active_status: number | null
  quote_date: string | null
  fq_factor_date: string | null
  fq_factor_status: 'OK' | 'STALE' | 'MISSING' | 'AHEAD' | 'NOT_APPLICABLE' | 'BLOCKED_BY_QUOTE'
  ma_dates: Record<string, string | null>
  ma_statuses: Record<
    string,
    'OK' | 'STALE' | 'MISSING' | 'AHEAD' | 'NOT_APPLICABLE' | 'BLOCKED_BY_QUOTE'
  >
  status: 'OK' | 'STALE' | 'MISSING' | 'AHEAD'
  issues: string[]
}

export interface DataQualityItemsResponse {
  total: number
  limit: number
  offset: number
  items: DataQualityItem[]
}

export interface DataQualityItemsParams {
  status?: 'all' | 'abnormal' | 'ok' | 'stale' | 'missing' | 'ahead'
  q?: string
  limit?: number
  offset?: number
}

export const dataQualityApi = {
  getSummary() {
    return api.get<DataQualitySummary>('/data-quality/summary') as unknown as Promise<DataQualitySummary>
  },

  getItems(params: DataQualityItemsParams = {}) {
    return api.get<DataQualityItemsResponse>('/data-quality/items', {
      params
    }) as unknown as Promise<DataQualityItemsResponse>
  }
}
