import api from './index'

/** Research simulation only. NAV snapshots are NOT certified account returns. */
export interface ResearchPaperIdentity {
  model_version: string
  horizon: number
  config_hash: string
  hash_kind: 'spec' | 'legacy'
}

export interface ResearchPaperCohort extends ResearchPaperIdentity {
  latest_date: string
  last_record_kind: 'REPLAY' | 'FORWARD'
}

export interface ResearchPaperNavPoint {
  date: string | null
  nav: number | null
  daily_return: number | null
  drawdown: number | null
  benchmark_return: number | null
  stale_positions_count: number | null
}

export interface ResearchPaperDecision {
  date: string
  decision_at: string | null
  execution_date: string | null
  status: string
  skip_reason: string | null
  evidence_kind: 'REPLAY' | 'FORWARD'
  hash_kind: 'legacy' | 'spec'
  model_version: string
  config_hash: string
  horizon: number
  strategy_name: string
  target_holdings: Array<{ stock_code: string; weight: number }>
  rebalance: Record<string, unknown>
  nav_snapshot: ResearchPaperNavPoint | null
  execution_trace: 'NOT_LINKED'
  accounting_status: 'UNVERIFIED'
}

export interface ResearchSimulationEvent {
  type: 'FILL' | 'REJECTED' | 'MARK' | 'DIVIDEND_ENTITLEMENT' | 'DIVIDEND_SETTLEMENT'
  event_id: string
  stock_code?: string
  side?: string
  order_id?: string
  fill_id?: string
  intent_id?: string
  quantity?: number
  price?: number
  reference_price?: number
  fees?: { commission?: number; stamp_duty?: number; transfer_fee?: number }
  reason_code?: string
  execution_date?: string
  decision_date?: string
  as_of?: string
  ex_date?: string
  pay_date?: string
  record_date?: string
  cash_per_share?: number
  entitlement_id?: string
}

export interface ResearchSimulationTrace {
  status: 'LINKED_SIMULATION' | 'NOT_LINKED' | 'INVALID_PROVENANCE' | 'AMBIGUOUS_ARTIFACT'
  research_only: true
  artifact_hash: string | null
  fills: ResearchSimulationEvent[]
  rejections: ResearchSimulationEvent[]
  session_events: ResearchSimulationEvent[]
  undated_corporate_events: number
  truncated: boolean
  explanation: string
}

export interface ResearchPaperDecisionDetail {
  decision: ResearchPaperDecision
  trace: ResearchSimulationTrace
}

export interface ResearchPaperOverview {
  identity: ResearchPaperIdentity
  window: { status: 'ACTIVE'; start_date: string } | null
  forward_observed_sessions: number
  forward_required: number
  forward_eligible: false
  forward_status: string
  accounting_status: 'UNVERIFIED'
  account_type: 'RESEARCH_SIMULATION_NOT_TRADING_ACCOUNT'
  provenance_note: string
  curve: ResearchPaperNavPoint[]
  decisions: ResearchPaperDecision[]
  has_more_runs: boolean
  total_loaded: number
}

export async function listResearchPaperCohorts(): Promise<ResearchPaperCohort[]> {
  const response = await api.get<any>('/strategy/paper/identities')
  return response.data.items as ResearchPaperCohort[]
}

export async function getResearchPaperOverview(
  identity: ResearchPaperIdentity
): Promise<ResearchPaperOverview> {
  const response = await api.get<any>('/strategy/paper/overview', {
    params: identity
  })
  return response.data as ResearchPaperOverview
}

export async function getResearchPaperDecision(
  identity: ResearchPaperIdentity,
  date: string
): Promise<ResearchPaperDecisionDetail> {
  const response = await api.get<any>(
    '/strategy/paper/decisions/' + encodeURIComponent(date),
    { params: identity }
  )
  return response.data as ResearchPaperDecisionDetail
}
