import { defineStore } from 'pinia'
import { ref } from 'vue'
import api from '@/api'

export interface IndexInfo {
  code: string
  name: string
  price: number
  change: number
  changePct: number
  sparkline?: number[]
}

export interface MarketBreadth {
  advances: number      // 上涨家数
  declines: number      // 下跌家数
  limitUp: number       // 涨停数
  limitDown: number     // 跌停数
}

export interface SectorPerformance {
  name: string
  changePct: number
}

export interface StockItem {
  code: string
  name: string
  price: number
  changePct: number
}

export interface CapitalFlow {
  northbound: number    // 北向资金
  main: number          // 主力资金
  retail: number        // 散户资金
}

export interface MarketOverviewResponse {
  indices: IndexInfo[]
  breadth: MarketBreadth
  sectors: SectorPerformance[]
  top_gainers: StockItem[]
  top_losers: StockItem[]
  capital_flow: CapitalFlow
}

export interface DataStatusReferenceDates {
  latest_complete_trading_day: string | null
  previous_complete_trading_day: string | null
}

export interface DataStatusCategory {
  total_count: number
  quote_records_count: number
  latest_quote_date: string | null
  freshness_records_count: number
  latest_freshness_date: string | null
  up_to_date_count: number
  lag_1_day_count: number
  expired_count: number
  no_data_count: number
  is_up_to_date: boolean
}

export interface DataStatusResponse {
  generated_at: string
  reference_dates: DataStatusReferenceDates
  index: DataStatusCategory
  stock: DataStatusCategory
  signal_run_today?: boolean
  scoring_run_today?: boolean
}

async function getMarketOverview() {
  return (await api.get('/market/overview')) as unknown as MarketOverviewResponse
}

async function getDataStatus() {
  return (await api.get('/datahub/status')) as unknown as DataStatusResponse
}

export const useMarketStore = defineStore('market', () => {
  const indices = ref<IndexInfo[]>([])
  const marketBreadth = ref<MarketBreadth>({ advances: 0, declines: 0, limitUp: 0, limitDown: 0 })
  const sectors = ref<SectorPerformance[]>([])
  const topGainers = ref<StockItem[]>([])
  const topLosers = ref<StockItem[]>([])
  const capitalFlow = ref<CapitalFlow>({ northbound: 0, main: 0, retail: 0 })
  const lastUpdateTime = ref<string>('')
  const dataStatus = ref<DataStatusResponse | null>(null)
  const marketLoading = ref(false)
  const statusLoading = ref(false)
  const marketError = ref<string | null>(null)
  const statusError = ref<string | null>(null)

  async function fetchMarketOverview() {
    marketLoading.value = true
    try {
      const res = await getMarketOverview()
      indices.value = res.indices
      marketBreadth.value = res.breadth
      sectors.value = res.sectors
      topGainers.value = res.top_gainers
      topLosers.value = res.top_losers
      capitalFlow.value = res.capital_flow
      lastUpdateTime.value = new Date().toLocaleString('zh-CN')
      marketError.value = null
    } catch (err) {
      marketError.value = '市场总览加载失败，请稍后重试'
      throw err
    } finally {
      marketLoading.value = false
    }
  }

  async function fetchDataStatus() {
    statusLoading.value = true
    try {
      const res = await getDataStatus()
      dataStatus.value = res
      statusError.value = null
    } catch (err) {
      statusError.value = '数据状态加载失败，请稍后重试'
      throw err
    } finally {
      statusLoading.value = false
    }
  }

  async function fetchDashboardData() {
    await Promise.allSettled([fetchMarketOverview(), fetchDataStatus()])
  }

  return {
    indices,
    marketBreadth,
    sectors,
    topGainers,
    topLosers,
    capitalFlow,
    lastUpdateTime,
    dataStatus,
    marketLoading,
    statusLoading,
    marketError,
    statusError,
    fetchMarketOverview,
    fetchDataStatus,
    fetchDashboardData
  }
})
