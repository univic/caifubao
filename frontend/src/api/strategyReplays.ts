import api from './index'

/**
 * Read-only strategy replay research artifact.
 *
 * A replay is historical research evidence (`evidence_kind: 'REPLAY'`), never
 * forward validation, and the UI must not present it as investment advice.
 */
export interface StrategyReplayMetrics {
  total_return: number | null
  annualised: number | null
  sharpe: number | null
  max_drawdown: number | null
  annual_turnover: number | null
  total_fees: number | null
  average_cash_share: number | null
  benchmark_return: number | null
}

export interface StrategyReplayCurvePoint {
  date: string
  nav: number | null
  benchmark_nav: number | null
  drawdown: number | null
  cash: number | null
  positions_count: number | null
  gross_exposure: number | null
}

export interface StrategyReplayTargetHolding {
  code: string
  weight: number
}

export interface StrategyReplayDecision {
  decision_id: string
  date: string
  execution_date: string
  sleeve?: string
  moved_notional?: number
  target_holdings: StrategyReplayTargetHolding[]
  added: string[]
  removed: string[]
  cash_before: number | null
  cash_after: number | null
  estimated_cost: number | null
}

export interface StrategyReplayFill {
  decision_id: string
  date: string
  code: string
  side: string
  sleeve?: string
  total_cost?: number
  quantity: number | null
  price: number | null
  commission: number | null
  stamp_duty: number | null
  transfer_fee: number | null
  reason?: string | null
}

export interface StrategyReplayBlockedOrder {
  decision_id: string
  date: string
  code: string
  side: string
  reason: string
}

export interface StrategyReplaySummary {
  replay_id: string
  name: string
  strategy: string
  description: string
  start_date: string
  end_date: string
  aum: number | null
  config_hash: string
  input_hash: string
  content_hash?: string | null
  schema_version: string
  source_artifact: string
  evidence_kind: 'REPLAY'
  forward_sessions: number
  forward_required: number
  forward_eligible: boolean
  data_as_of: string | null
  generated_at: string | null
  research_grade: string
  survivorship_gap: boolean
  disclaimer: string
  limitations: string[]
  metrics: Partial<StrategyReplayMetrics>
}

export interface StrategyReplayDetail extends StrategyReplaySummary {
  config: Record<string, unknown>
  curve: StrategyReplayCurvePoint[]
  decisions: StrategyReplayDecision[]
  fills: StrategyReplayFill[]
  blocked_orders: StrategyReplayBlockedOrder[]
}

export interface StrategyReplayListParams {
  page?: number
  page_size?: number
}

export interface StrategyReplayListResult {
  total: number
  page: number
  page_size: number
  items: StrategyReplaySummary[]
}

/** GET /api/strategy/replays — paginated summaries (default 20, max 200). */
export async function listStrategyReplays(
  params: StrategyReplayListParams = {}
): Promise<StrategyReplayListResult> {
  const response = await api.get<any>('/strategy/replays', { params })
  return response.data as StrategyReplayListResult
}

/** GET /api/strategy/replays/<replay_id> — one full artifact. */
export async function getStrategyReplay(replayId: string): Promise<StrategyReplayDetail> {
  const response = await api.get<any>(`/strategy/replays/${encodeURIComponent(replayId)}`)
  return response.data as StrategyReplayDetail
}

export const strategyReplayApi = {
  list: listStrategyReplays,
  get: getStrategyReplay
}
