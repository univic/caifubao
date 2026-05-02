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

export interface ScoreExperimentHorizonReport {
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
  }
}
