import api from './index'

export interface IndexOverview {
  code: string
  name: string
  price: number
  previousClose: number
  change: number
  changePct: number
  open?: number
  high?: number
  low?: number
  volume?: number
  tradeDate?: string
}

export interface IndexListItem {
  code: string
  name: string
  close: number | null
  previousClose: number | null
  open: number | null
  high: number | null
  low: number | null
  changeRate: number
  changeAmount: number
  volume: number | null
  tradeDate?: string | null
}

export interface IndicesOverviewResponse {
  data: IndexOverview[]
}

export interface IndicesListResponse {
  items: IndexListItem[]
  total: number
  page: number
  page_size: number
}

export interface IndicesListParams {
  page?: number
  page_size?: number
  sort_by?: string
  order?: 'asc' | 'desc'
}

export async function getIndicesOverview(): Promise<IndicesOverviewResponse> {
  return (await api.get('/v1/indices/overview')) as unknown as IndicesOverviewResponse
}

export async function getIndicesList(params: IndicesListParams = {}): Promise<IndicesListResponse> {
  return (await api.get('/v1/indices', { params })) as unknown as IndicesListResponse
}
