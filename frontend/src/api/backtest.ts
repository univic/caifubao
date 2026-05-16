import api from './index'

export interface BacktestTrade {
  date: string
  side: string // BUY or SELL
  price: number
  quantity: number
  amount: number
  reason: string
  cash_after: number
  position_after: number
  pnl?: number
}

export interface DailyValue {
  date: string
  value: number
  cash: number
  positions_value: number
  return_pct?: number
  drawdown_pct?: number
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
}

export interface RunBacktestPayload {
  stock_code: string
  strategy: string
  start_date: string
  end_date: string
  initial_cash?: number
}

export const backtestApi = {
  list() {
    return api.get<{ items: BacktestResult[] }>('/backtest') as unknown as Promise<{ items: BacktestResult[] }>
  },
  run(payload: RunBacktestPayload) {
    return api.post<BacktestResult>('/backtest/run', payload) as unknown as Promise<BacktestResult>
  },
  get(id: string) {
    return api.get<BacktestResult>(`/backtest/${id}`) as unknown as Promise<BacktestResult>
  },
  delete(id: string) {
    return api.delete<{ message: string }>(`/backtest/${id}`) as unknown as Promise<{ message: string }>
  }
}
