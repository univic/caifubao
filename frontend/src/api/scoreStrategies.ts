import api from './index'

export interface BacktestRequest {
  horizon: number
  top_n: number
  start_date: string
  end_date: string
  model_version?: string
}

export interface DailyResult {
  date: string
  position_count: number
  avg_return: number | null
  avg_max_return: number | null
  hit_rate: number | null
  top_stocks: Array<{
    stock_code: string
    stock_name: string | null
    score: number | null
    return_at_target: number | null
    hit_target: boolean | null
  }>
}

export interface EquityPoint {
  date: string
  value: number
}

export interface BacktestSummary {
  total_trading_days: number
  total_positions: number
  avg_return_per_position: number | null
  overall_hit_rate: number | null
  total_return: number
  annualized_return: number
  max_drawdown: number
  sharpe_ratio: number | null
  win_rate: number | null
}

export interface BacktestStrategy {
  horizon: number
  top_n: number
  model_version: string
  start_date: string
  end_date: string
}

export interface BacktestResponse {
  success: boolean
  message?: string
  strategy?: BacktestStrategy
  summary?: BacktestSummary
  equity_curve?: EquityPoint[]
  daily_results?: DailyResult[]
}

export interface BucketCalibration {
  bucket: string
  count: number
  avg_score: number | null
  avg_return: number | null
  avg_max_return: number | null
  hit_rate: number | null
  stop_loss_hit_rate: number | null
  confidence: 'high' | 'medium' | 'low' | null
  suggested_stop_loss: number | null
  suggested_take_profit: number | null
}

export interface CalibrationResponse {
  success: boolean
  horizon: number
  model_version: string
  lookback_days: number
  prediction_count: number
  overall: {
    avg_return: number | null
    hit_rate: number | null
  }
  buckets: BucketCalibration[]
}

export interface TradeSuggestions {
  stop_loss: number | null
  take_profit: number | null
  basis: string
}

export interface ConfidenceResponse {
  success: boolean
  stock_code: string
  date: string
  horizon: number
  score: number
  score_bucket: string
  confidence: 'high' | 'medium' | 'low' | null
  bucket_hit_rate: number | null
  bucket_sample_count: number
  trade_suggestions: TradeSuggestions | null
  prediction_status: string
  prediction_verification: {
    return_at_target: number | null
    max_return: number | null
    hit_target: boolean | null
  } | null
}

export const scoreStrategiesApi = {
  runBacktest(params: BacktestRequest) {
    return api.post('/score-strategies/backtest', params) as unknown as Promise<BacktestResponse>
  },
  getCalibration(params: { horizon: number; model_version?: string; days?: number }) {
    return api.get('/score-strategies/calibration', { params }) as unknown as Promise<CalibrationResponse>
  },
  getConfidence(params: { stock_code: string; date: string; horizon: number; model_version?: string }) {
    return api.get('/score-strategies/confidence', { params }) as unknown as Promise<ConfidenceResponse>
  }
}
