import api from './index'

export interface ScoreMetricSummary {
  count: number
  avg_score: number | null
  avg_return_at_target: number | null
  avg_max_return: number | null
  avg_min_return: number | null
  avg_max_drawdown: number | null
  hit_rate: number | null
  stop_loss_hit_rate: number | null
}

export interface ScoreBucketSummary extends ScoreMetricSummary {
  bucket: string
}

export type ScoreBasis = 'score' | 'percentile'

export interface ScoreExperimentHorizonReport {
  bucket_basis: ScoreBasis
  comparison_basis?: ScoreBasis
  overall: ScoreMetricSummary
  score_buckets: ScoreBucketSummary[]
  top_n: Record<string, ScoreMetricSummary>
  component_summary: Record<string, ScoreMetricSummary>
  false_positives: Array<Record<string, unknown>>
  false_negatives: Array<Record<string, unknown>>
  baseline?: ScoreExperimentHorizonReport
  comparison?: Record<string, number | null>
}

export interface ScoreExperimentReport {
  model_version: string
  baseline_model_version: string | null
  start_date: string
  end_date: string
  horizons: Record<string, ScoreExperimentHorizonReport>
}

export interface ScoreExperiment {
  id: string
  name: string
  description: string
  model_version: string
  baseline_model_version: string | null
  start_date: string
  end_date: string
  horizons: number[]
  config: Record<string, unknown>
  status: 'CREATED' | 'RUNNING' | 'COMPLETED' | 'FAILED'
  report: ScoreExperimentReport | Record<string, never>
  error_msg: string | null
  created_at: string
  updated_at: string
  completed_at: string | null
}

export interface ScoreExperimentListResponse {
  items: ScoreExperiment[]
}

export interface CreateScoreExperimentPayload {
  name: string
  description?: string
  model_version: string
  baseline_model_version?: string
  start_date: string
  end_date: string
  horizons: number[]
  config?: Record<string, unknown>
  run_now?: boolean
}

const useMockApi = import.meta.env.VITE_USE_MOCK_API === 'true'

const mockExperiment: ScoreExperiment = {
  id: 'mock-score-experiment',
  name: 'Mock Score20 trend-heavy',
  description: 'Frontend mock data for fast research-page feedback.',
  model_version: 'score_mock_candidate',
  baseline_model_version: 'score_mock_baseline',
  start_date: '2026-04-01T00:00:00',
  end_date: '2026-04-30T00:00:00',
  horizons: [5, 20, 60],
  config: {
    5: { signal_strength: 30, momentum: 25, trend_alignment: 20 },
    20: { trend_alignment: 34, relative_strength: 20, momentum: 16 },
    60: { trend_alignment: 38, relative_strength: 26, risk_penalty: 14 }
  },
  status: 'COMPLETED',
  report: {
    model_version: 'score_mock_candidate',
    baseline_model_version: 'score_mock_baseline',
    start_date: '2026-04-01T00:00:00',
    end_date: '2026-04-30T00:00:00',
    horizons: {
      '5': buildMockHorizonReport(5, 0.028),
      '20': buildMockHorizonReport(20, 0.061),
      '60': buildMockHorizonReport(60, 0.093)
    }
  },
  error_msg: null,
  created_at: '2026-05-02T09:00:00',
  updated_at: '2026-05-02T09:03:00',
  completed_at: '2026-05-02T09:03:00'
}

function buildMockMetric(count: number, avgReturn: number): ScoreMetricSummary {
  return {
    count,
    avg_score: 72.4,
    avg_return_at_target: avgReturn,
    avg_max_return: avgReturn + 0.026,
    avg_min_return: avgReturn - 0.041,
    avg_max_drawdown: avgReturn - 0.052,
    hit_rate: Math.min(0.86, 0.48 + avgReturn * 4),
    stop_loss_hit_rate: Math.max(0.03, 0.16 - avgReturn)
  }
}

function buildMockHorizonReport(horizon: number, avgReturn: number): ScoreExperimentHorizonReport {
  const overall = buildMockMetric(240, avgReturn)
  return {
    bucket_basis: 'score',
    overall,
    score_buckets: [
      { bucket: '0-20', ...buildMockMetric(18, -0.018) },
      { bucket: '20-40', ...buildMockMetric(42, -0.004) },
      { bucket: '40-60', ...buildMockMetric(72, 0.012) },
      { bucket: '60-80', ...buildMockMetric(80, avgReturn) },
      { bucket: '80-100', ...buildMockMetric(28, avgReturn + 0.021) }
    ],
    top_n: {
      top_10: buildMockMetric(30, avgReturn + 0.019),
      top_30: buildMockMetric(90, avgReturn + 0.011),
      top_50: buildMockMetric(150, avgReturn + 0.006)
    },
    component_summary: {
      trend_alignment: buildMockMetric(168, avgReturn + 0.013),
      momentum: buildMockMetric(132, avgReturn + 0.007),
      relative_strength: buildMockMetric(96, avgReturn + 0.016),
      risk_penalty: buildMockMetric(80, avgReturn - 0.006)
    },
    false_positives: [],
    false_negatives: [],
    baseline: {
      bucket_basis: 'score',
      overall: buildMockMetric(240, avgReturn - 0.012),
      score_buckets: [],
      top_n: {},
      component_summary: {},
      false_positives: [],
      false_negatives: []
    },
    comparison: {
      count_delta: 0,
      avg_return_at_target_delta: 0.012,
      avg_max_return_delta: 0.014,
      avg_min_return_delta: 0.006,
      avg_max_drawdown_delta: 0.007,
      hit_rate_delta: horizon === 20 ? 0.09 : 0.05,
      stop_loss_hit_rate_delta: -0.03
    }
  }
}

export interface CompareResult {
  horizon: number
  start_date: string
  end_date: string
  comparison_basis: ScoreBasis
  comparison_status: 'ok' | 'insufficient_data'
  verdict: string
  candidate: {
    model_version: string
    bucket_basis: ScoreBasis
    overall: ScoreMetricSummary
    score_buckets: ScoreBucketSummary[]
    top_n: Record<string, ScoreMetricSummary>
  }
  baseline: {
    model_version: string
    bucket_basis: ScoreBasis
    overall: ScoreMetricSummary
    score_buckets: ScoreBucketSummary[]
    top_n: Record<string, ScoreMetricSummary>
  }
  deltas: Record<string, number | null | Record<string, Record<string, number | null>>>
}

export interface CompareParams {
  id_a: string
  id_b: string
  start_date: string
  end_date: string
  horizon: number
}

// ─── Rankings & Heatmap (Task 14.8) ───────────────────────────────────────

export interface RankingEntry {
  rank: number
  experiment_id: string
  name: string
  model_version: string
  horizon: number | null
  composite_score: number
  breakdown: Record<string, number>
  metrics: {
    excess_return_pct: number
    max_drawdown: number
    information_ratio: number
    total_trades: number
    trading_days: number
  }
  weights: Record<string, number>
  flags: string[]
  rankable: boolean
}

export interface RankingsResponse {
  rankings: RankingEntry[]
  total_experiments: number
  bonferroni: {
    alpha: number
    corrected_alpha: number
    num_comparisons: number
  }
  total?: number
}

export interface HeatmapMatrixEntry {
  component_x: string
  component_y: string
  avg_score: number
  experiment_count: number
  best_config: {
    experiment_id: string
    name: string
    composite_score: number
    weights: Record<string, number>
  } | null
}

export interface HeatmapResponse {
  horizon: number
  components: string[]
  matrix: HeatmapMatrixEntry[]
  entries?: HeatmapMatrixEntry[]
}

export interface RankingsParams {
  horizon?: number
  limit?: number
}

export interface HeatmapParams {
  horizon?: number
}

export const scoreExperimentApi = {
  listExperiments() {
    if (useMockApi) {
      return Promise.resolve({ items: [mockExperiment] })
    }
    return api.get<ScoreExperimentListResponse>('/score-experiments') as unknown as Promise<ScoreExperimentListResponse>
  },
  createExperiment(payload: CreateScoreExperimentPayload) {
    if (useMockApi) {
      return Promise.resolve({
        ...mockExperiment,
        id: `mock-${Date.now()}`,
        name: payload.name,
        description: payload.description || mockExperiment.description,
        model_version: payload.model_version,
        baseline_model_version: payload.baseline_model_version || null,
        start_date: `${payload.start_date}T00:00:00`,
        end_date: `${payload.end_date}T00:00:00`,
        horizons: payload.horizons,
        config: payload.config || {}
      })
    }
    return api.post<ScoreExperiment>('/score-experiments', payload) as unknown as Promise<ScoreExperiment>
  },
  runExperiment(id: string) {
    if (useMockApi) {
      return Promise.resolve({ ...mockExperiment, id })
    }
    return api.post<ScoreExperiment>(`/score-experiments/${id}/run`) as unknown as Promise<ScoreExperiment>
  },
  compare(params: CompareParams) {
    return api.get<{ success: boolean; data: CompareResult }>('/score-experiments/compare', { params })
  },
  getRankings(params?: RankingsParams) {
    if (useMockApi) {
      return Promise.resolve(mockRankings(params?.horizon ?? 20))
    }
    return api.get('/score-experiments/rankings', { params }) as unknown as Promise<RankingsResponse>
  },
  getHeatmap(params?: HeatmapParams) {
    if (useMockApi) {
      return Promise.resolve(mockHeatmap(params?.horizon ?? 20))
    }
    return api.get('/score-experiments/heatmap', { params }) as unknown as Promise<HeatmapResponse>
  }
}

// ─── Mock data for Rankings & Heatmap ─────────────────────────────────────

function mockRankings(horizon: number): RankingsResponse {
  const items: RankingEntry[] = [
    {
      rank: 1, experiment_id: 'exp-1', name: 'Score20 trend-heavy v2', model_version: 'score_v2_202605', horizon,
      composite_score: 1.82,
      breakdown: { excess_contrib: 0.5, ir_contrib: 0.3, dd_penalty: 0.1, turnover_penalty: 0.05, concentration_penalty: 0 },
      metrics: { excess_return_pct: 8.4, max_drawdown: 12.3, information_ratio: 0.93, total_trades: 34, trading_days: 200 },
      flags: [],
      weights: { trend_alignment: 35, momentum: 20, relative_strength: 20, signal_strength: 15, risk_penalty: 10 },
      rankable: true
    },
    {
      rank: 2, experiment_id: 'exp-2', name: 'Score20 balanced v1', model_version: 'score_v2_202604', horizon,
      composite_score: 1.56,
      breakdown: { excess_contrib: 0.4, ir_contrib: 0.25, dd_penalty: 0.15, turnover_penalty: 0.03, concentration_penalty: 0 },
      metrics: { excess_return_pct: 5.2, max_drawdown: 15.1, information_ratio: 0.71, total_trades: 28, trading_days: 180 },
      flags: ['low_sample'],
      weights: { trend_alignment: 25, momentum: 25, relative_strength: 25, signal_strength: 15, risk_penalty: 10 },
      rankable: true
    },
    {
      rank: 3, experiment_id: 'exp-3', name: 'Score20 momentum-heavy', model_version: 'score_v2_202605b', horizon,
      composite_score: 1.23,
      breakdown: { excess_contrib: 0.3, ir_contrib: 0.2, dd_penalty: 0.2, turnover_penalty: 0.08, concentration_penalty: 0.1 },
      metrics: { excess_return_pct: 3.8, max_drawdown: 18.7, information_ratio: 0.55, total_trades: 42, trading_days: 220 },
      flags: ['high_drawdown', 'concentrated_returns:0.45'],
      weights: { trend_alignment: 15, momentum: 40, relative_strength: 15, signal_strength: 10, risk_penalty: 20 },
      rankable: true
    },
    {
      rank: 4, experiment_id: 'exp-4', name: 'Score20 risk-averse', model_version: 'score_v2_202604_risk', horizon,
      composite_score: 0.98,
      breakdown: { excess_contrib: 0.2, ir_contrib: 0.15, dd_penalty: 0.05, turnover_penalty: 0.02, concentration_penalty: 0 },
      metrics: { excess_return_pct: 2.1, max_drawdown: 9.2, information_ratio: 0.42, total_trades: 18, trading_days: 140 },
      flags: ['low_sample', 'insufficient_period'],
      weights: { trend_alignment: 20, momentum: 15, relative_strength: 15, signal_strength: 10, risk_penalty: 40 },
      rankable: true
    },
    {
      rank: 5, experiment_id: 'exp-5', name: 'Score20 signal-first', model_version: 'score_v2_202604_sig', horizon,
      composite_score: 0.45,
      breakdown: { excess_contrib: -0.1, ir_contrib: -0.05, dd_penalty: 0.3, turnover_penalty: 0.06, concentration_penalty: 0 },
      metrics: { excess_return_pct: -1.5, max_drawdown: 22.1, information_ratio: -0.18, total_trades: 38, trading_days: 210 },
      flags: ['high_drawdown', 'performance_decay'],
      weights: { trend_alignment: 10, momentum: 15, relative_strength: 10, signal_strength: 45, risk_penalty: 20 },
      rankable: false
    }
  ]
  return {
    rankings: items,
    total_experiments: 150,
    bonferroni: { alpha: 0.05, corrected_alpha: 3.3e-4, num_comparisons: 150 }
  }
}

function mockHeatmap(horizon: number): HeatmapResponse {
  const components = ['trend_alignment', 'momentum', 'relative_strength', 'signal_strength', 'risk_penalty', 'breakout_or_pos', 'volume_ratio']
  const matrix: HeatmapMatrixEntry[] = []

  for (let i = 0; i < components.length; i++) {
    for (let j = i; j < components.length; j++) {
      const count = i === j ? 80 + Math.floor(Math.random() * 40) : 30 + Math.floor(Math.random() * 50)
      let avg: number
      if (i === j) {
        avg = 1.0 + Math.random() * 1.0
      } else {
        avg = -0.4 + Math.random() * 1.8
      }
      avg = Math.round(avg * 100) / 100
      const entry: HeatmapMatrixEntry = {
        component_x: components[i]!,
        component_y: components[j]!,
        avg_score: avg,
        experiment_count: count,
        best_config: null
      }
      matrix.push(entry)
      if (i !== j) {
        matrix.push({ ...entry, component_x: components[j]!, component_y: components[i]! })
      }
    }
  }

  return { horizon, components, matrix }
}
