import api from './index'

export interface ScoreSummary {
  score: number
  rank: number | null
  percentile: number | null
  recommendation: string
  status: string | null
  verification: Record<string, unknown>
  model_version: string | null
}

export interface ScorePrediction {
  stock_code: string
  stock_name: string | null
  date: string
  horizon: number
  score: number
  rank: number | null
  percentile: number | null
  recommendation: string
  base_price: number | null
  target_date: string | null
  status: string
  verification: Record<string, unknown>
  model_version: string
  generated_at: string
  updated_at: string
  explanation?: Record<string, unknown>
  input_snapshot?: Record<string, unknown>
}

export interface ScoreListResponse {
  date: string | null
  requested_date: string | null
  horizon: number
  total: number
  limit: number
  offset: number
  items: ScorePrediction[]
}

export interface ScoreHistoryResponse {
  stock_code: string
  horizon: number
  total: number
  limit: number
  offset: number
  items: ScorePrediction[]
}

export interface ScoreExplanationResponse extends ScorePrediction {
  explanation: Record<string, unknown>
  input_snapshot: Record<string, unknown>
}

export interface ScoreGenerateRequest {
  date?: string
  horizon?: number
  stock_code?: string
  model_version?: string
  replace?: boolean
}

export interface ScoreGenerateResponse {
  success: boolean
  message: string
  date: string
  horizon: string | number
  scored_count: number
  model_version: string
  results?: Array<{
    stock_code: string
    horizon: number
    score: number | null
    recommendation: string | null
  }>
}

export const scoreApi = {
  /** List scores for a given horizon and date (ranking board) */
  listScores(params: {
    horizon: number
    date?: string
    limit?: number
    offset?: number
    min_score?: number
    recommendation?: string
    status?: string
  }) {
    return api.get<ScoreListResponse>('/scores', { params }) as unknown as Promise<ScoreListResponse>
  },

  /** Get score history for a specific stock */
  getStockScoreHistory(
    stockCode: string,
    params: {
      horizon: number
      from?: string
      to?: string
      limit?: number
      offset?: number
    }
  ) {
    return api.get<ScoreHistoryResponse>(
      `/scores/${encodeURIComponent(stockCode)}`,
      { params }
    ) as unknown as Promise<ScoreHistoryResponse>
  },

  /** Get detailed score explanation for a stock on a specific date */
  getScoreExplanation(
    stockCode: string,
    date: string,
    params: { horizon: number; model_version?: string }
  ) {
    return api.get<ScoreExplanationResponse>(
      `/scores/${encodeURIComponent(stockCode)}/${encodeURIComponent(date)}/explanation`,
      { params }
    ) as unknown as Promise<ScoreExplanationResponse>
  },

  /** Generate score predictions on demand */
  generateScores(body: ScoreGenerateRequest) {
    return api.post<ScoreGenerateResponse>(
      '/scores/generate',
      body
    ) as unknown as Promise<ScoreGenerateResponse>
  }
}
