import api from './index'

export interface FactorEvalReportSummary {
  id: string
  factor_name: string
  observation_count: number
  ic_mean_score20: number | null
  icir_score20: number | null
  created_at: string
  status: string
}

export interface IcSummary {
  ic_mean: number
  ic_std: number
}

export interface QuintileItem {
  quintile: number
  avg_return: number
}

export interface QuintileAnalysis {
  [horizon: string]: QuintileItem[]
}

export interface CorrelationItem {
  component: string
  corr: number
}

export interface DecayCurveItem {
  horizon: number
  ic_mean: number
}

export interface RegimeIcItem {
  bull: { ic_mean: number; ic_std: number } | null
  bear: { ic_mean: number; ic_std: number } | null
  sideways: { ic_mean: number; ic_std: number } | null
}

export interface FactorEvalReportDetail {
  id: string
  factor_name: string
  observation_count: number
  status: string
  created_at: string
  ic_summary: Record<string, IcSummary>
  icir_summary: Record<string, number>
  quintile_analysis: QuintileAnalysis
  correlation_matrix: CorrelationItem[]
  decay_curve: DecayCurveItem[]
  regime_ic: RegimeIcItem | null
}

export const factorEvalApi = {
  getReports(params?: { page?: number; per_page?: number }) {
    return api.get<any>('/factor-eval/reports', { params }) as unknown as Promise<{
      data: { items: FactorEvalReportSummary[]; total: number }
    }>
  },

  getReport(id: string) {
    return api.get<any>(`/factor-eval/reports/${id}`) as unknown as Promise<{
      data: FactorEvalReportDetail
    }>
  }
}
