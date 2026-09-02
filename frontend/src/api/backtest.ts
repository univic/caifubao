import api from './index'

export const SCORE_DRIVEN_STRATEGIES = [
  'SCORE_THRESHOLD',
  'SCORE_MOMENTUM',
  'MULTI_HORIZON_CONSENSUS',
  'TOP_N_ROTATION',
] as const

export type ScoreDrivenStrategy = (typeof SCORE_DRIVEN_STRATEGIES)[number]
export type NonScoreStrategy = 'MA_CROSS' | 'BUY_HOLD'

export function isScoreDrivenStrategy(strategy: string): strategy is ScoreDrivenStrategy {
  return SCORE_DRIVEN_STRATEGIES.some((item) => item === strategy)
}

export interface BacktestTrade {
  date: string
  side: string // BUY or SELL
  price: number
  quantity: number
  amount: number
  reason: string
  pnl?: number
}

export interface DailyValue {
  date: string
  close: number
  cash: number
  shares: number
  equity: number
  value: number
  positions_value: number
}

export interface BacktestResult {
  id: string
  name: string
  stock_code: string
  stock_name?: string
  strategy: string
  start_date: string
  end_date: string
  initial_cash: number
  final_value: number
  total_return: number
  total_return_pct: number
  annualized_return: number
  max_drawdown: number
  max_drawdown_duration: number
  sharpe_ratio: number
  win_rate: number
  total_trades: number
  profit_trades: number
  loss_trades: number
  status: string
  error_message?: string
  trades: BacktestTrade[]
  daily_values: DailyValue[]
  created_at: string
  completed_at?: string
  // Friction costs
  total_commission?: number
  total_stamp_duty?: number
  total_slippage?: number
  gross_return?: number
  gross_return_pct?: number
  // Benchmark
  benchmark_code?: string
  benchmark_return?: number
  benchmark_return_pct?: number
  benchmark_annualized_return?: number
  excess_return?: number
  excess_return_pct?: number
  information_ratio?: number
  // Score-driven
  horizon?: number
  model_version?: string
  score_config?: {
    horizon?: number
    entry_threshold?: number
    exit_threshold?: number
    stop_loss_pct?: number
    score_delta?: number
    model_version?: string
    execution_timing?: 'next_trading_day_open'
  }
  // Multi-stock
  per_stock_contributions?: Array<{
    stock_code: string
    stock_name?: string
    realized_pnl: number
    trades: number
  }>
  top_n?: number
  rebalance_interval?: number
  allocation?: string
}

interface RunBacktestPayloadBase {
  stock_code: string
  start_date: string
  end_date: string
  initial_cash?: number
  benchmark_code?: string
  entry_threshold?: number
  exit_threshold?: number
  stop_loss_pct?: number
  score_delta?: number
}

export type RunBacktestPayload =
  | (RunBacktestPayloadBase & {
      strategy: 'SCORE_THRESHOLD' | 'SCORE_MOMENTUM'
      horizon: number
      model_version: string
    })
  | (RunBacktestPayloadBase & {
      strategy: 'MULTI_HORIZON_CONSENSUS'
      model_version: string
      horizon?: never
    })
  | (RunBacktestPayloadBase & {
      strategy: NonScoreStrategy
      model_version?: never
      horizon?: never
    })

export interface RunMultiBacktestPayload {
  stock_codes: string[]
  strategy: 'TOP_N_ROTATION'
  start_date: string
  end_date: string
  initial_cash?: number
  benchmark_code?: string
  horizon?: number
  top_n?: number
  rebalance_interval?: number
  allocation?: string
  max_position_pct?: number
  stop_loss_pct?: number
  model_version: string
}

// Strategy Discovery types

export interface CompareResult {
  strategy: string
  total_return_pct: number
  sharpe_ratio: number
  max_drawdown: number
  win_rate: number
  total_trades: number
  excess_return_pct: number
  information_ratio: number
  composite_score?: number
  composite_breakdown?: Record<string, number>
  flags?: string[]
  rankable?: boolean
  error?: string
}

export interface ScanItem {
  stock_code: string
  stock_name: string
  total_return_pct: number
  sharpe_ratio: number
  max_drawdown: number
  total_trades: number
  win_rate: number
  excess_return_pct: number
  information_ratio: number
  composite_score?: number
  composite_breakdown?: Record<string, number>
  flags?: string[]
  rankable?: boolean
}

export interface ComparePayload {
  stock_code: string
  start_date: string
  end_date: string
  initial_cash?: number
  benchmark_code?: string
  model_version: string
}

interface ScanPayloadBase {
  start_date: string
  end_date: string
  initial_cash?: number
  page?: number
  per_page?: number
  min_trades?: number
}

export type ScanPayload =
  | (ScanPayloadBase & {
      strategy: 'SCORE_THRESHOLD' | 'SCORE_MOMENTUM'
      horizon: number
      model_version: string
    })
  | (ScanPayloadBase & {
      strategy: 'MULTI_HORIZON_CONSENSUS'
      model_version: string
      horizon?: never
    })
  | (ScanPayloadBase & {
      strategy: NonScoreStrategy
      model_version?: never
      horizon?: never
    })

export const backtestApi = {
  list() {
    return api.get<any>('/backtest').then((res: any) => res.data) as Promise<{ total: number; limit: number; offset: number; items: BacktestResult[] }>
  },
  run(payload: RunBacktestPayload) {
    return api.post<any>('/backtest/run', payload).then((res: any) => res.data) as Promise<BacktestResult>
  },
  runMulti(payload: RunMultiBacktestPayload) {
    return api.post<any>('/backtest/run-multi', payload).then((res: any) => res.data) as Promise<BacktestResult>
  },
  get(id: string) {
    return api.get<any>(`/backtest/${id}`).then((res: any) => res.data) as Promise<BacktestResult>
  },
  delete(id: string) {
    return api.delete<any>(`/backtest/${id}`).then((res: any) => res.data) as Promise<{ message: string }>
  },
  compare(payload: ComparePayload) {
    return api.post<any>('/backtest/compare', payload).then((res: any) => res.data)
  },
  scan(payload: ScanPayload) {
    return api.post<any>('/backtest/scan', payload)
  },
  scanExport(payload: any) {
    return api.post<any>('/backtest/export/scan', payload)
  },
  getTask(taskId: string) {
    return api.get<any>(`/tasks/${taskId}`).then((res: any) => res.data)
  }
}
