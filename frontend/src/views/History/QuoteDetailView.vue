<template>
  <div class="quote-detail-page">
    <section class="topbar">
      <div class="title-block">
        <p class="eyebrow">Quote Detail</p>
        <h1>{{ detail?.stock.name || '股票详情' }}</h1>
        <p class="subtitle">
          {{ detail?.stock.code || symbol }}
          <span v-if="detail?.stock.market_name">· {{ detail.stock.market_name }}</span>
          <span v-if="detail?.freshness?.freshness_datetime">· 更新于 {{ formatDate(detail.freshness.freshness_datetime) }}</span>
        </p>
      </div>
      <el-button plain @click="goBack">返回查询</el-button>
    </section>

    <el-card class="search-card" shadow="never">
      <el-form class="inline-form" @submit.prevent="handleSearch">
        <el-form-item class="search-field">
          <el-input
            v-model="searchSymbol"
            clearable
            placeholder="输入股票代码，例如 600519 / sh600519"
            @keyup.enter="handleSearch"
          />
        </el-form-item>
        <el-button type="primary" :loading="loading" @click="handleSearch">重新查询</el-button>
      </el-form>
    </el-card>

    <el-skeleton v-if="loading && !detail" :rows="8" animated />

    <template v-else>
      <!-- Summary Card -->
      <el-card class="summary-card" shadow="hover">
        <div class="summary-grid">
          <div class="summary-item emphasis">
            <div class="label">最新价</div>
            <div class="value">{{ formatPrice(detail?.latest_quote?.close) }}</div>
            <div class="hint">{{ formatQuoteChange(detail?.latest_quote) }}</div>
          </div>
          <div class="summary-item">
            <div class="label">开盘</div>
            <div class="value">{{ formatPrice(detail?.latest_quote?.open) }}</div>
          </div>
          <div class="summary-item">
            <div class="label">最高</div>
            <div class="value">{{ formatPrice(detail?.latest_quote?.high) }}</div>
          </div>
          <div class="summary-item">
            <div class="label">最低</div>
            <div class="value">{{ formatPrice(detail?.latest_quote?.low) }}</div>
          </div>
          <div class="summary-item">
            <div class="label">成交量</div>
            <div class="value">{{ formatVolume(detail?.latest_quote?.volume) }}</div>
          </div>
          <div class="summary-item">
            <div class="label">数据状态</div>
            <div class="value">
              <el-tag v-if="detail?.freshness?.status" size="small" type="success">
                {{ detail.freshness.status }}
              </el-tag>
              <span v-else>未知</span>
            </div>
          </div>
        </div>
      </el-card>

      <!-- Info Card -->
      <el-card class="info-card" shadow="hover">
        <div class="section-header">
          <h2>基本信息</h2>
        </div>
        <div class="info-grid">
          <div class="info-row"><span class="key">代码</span><span class="value">{{ detail?.stock.code }}</span></div>
          <div class="info-row"><span class="key">名称</span><span class="value">{{ detail?.stock.name }}</span></div>
          <div class="info-row"><span class="key">类型</span><span class="value">{{ stockTypeLabel(detail?.stock.object_type) }}</span></div>
          <div class="info-row"><span class="key">交易所</span><span class="value">{{ detail?.stock.exchange_name || '未知' }}</span></div>
          <div class="info-row"><span class="key">市场</span><span class="value">{{ detail?.stock.market_name || '未知' }}</span></div>
          <div class="info-row"><span class="key">关注等级</span><span class="value">{{ detail?.stock.watch_level ?? '未知' }}</span></div>
        </div>
      </el-card>

      <!-- Tabbed Content: K-line / Score -->
      <el-tabs v-model="detailTab" class="detail-tabs">
        <!-- K-line Tab -->
        <el-tab-pane label="K 线" name="kline">
          <el-card class="chart-card" shadow="hover">
            <div class="section-header">
              <div>
                <h2>日线图</h2>
                <p class="section-desc">最近 {{ history.length }} 条日线</p>
              </div>
              <div class="section-meta" v-if="latestHistoryItem">
                最新行情 {{ formatDate(latestHistoryItem.date) }}
              </div>
            </div>
            <div ref="chartRef" class="chart"></div>
            <el-empty v-if="!history.length && !loading" description="暂无日线数据" />
          </el-card>

          <el-card class="table-card" shadow="hover">
            <div class="section-header">
              <h2>最近行情</h2>
            </div>
            <el-table v-if="history.length" :data="recentRows" size="small" stripe>
              <el-table-column prop="date" label="日期" width="120">
                <template #default="{ row }">{{ formatDate(row.date) }}</template>
              </el-table-column>
              <el-table-column prop="open" label="开盘" />
              <el-table-column prop="close" label="收盘" />
              <el-table-column prop="high" label="最高" />
              <el-table-column prop="low" label="最低" />
              <el-table-column prop="volume" label="成交量" />
              <el-table-column prop="change_rate" label="涨跌幅">
                <template #default="{ row }">{{ formatPercent(row.change_rate) }}</template>
              </el-table-column>
            </el-table>
            <el-empty v-else description="暂无行情数据" />
          </el-card>
        </el-tab-pane>

        <!-- Score Tab -->
        <el-tab-pane label="评分" name="score">
          <!-- Horizon Score Cards -->
          <div class="score-horizon-grid">
            <el-card
              v-for="h in scoreHorizons"
              :key="h"
              class="horizon-score-card"
              :class="{ active: scoreHorizon === h }"
              shadow="hover"
              @click="scoreHorizon = h"
            >
              <div class="horizon-card-header">
                <span class="horizon-badge">Score{{ h }}</span>
                <span class="horizon-desc">{{ horizonLabel(h) }}</span>
              </div>
              <div v-if="latestScores[h]" class="horizon-card-body">
                <div class="big-score" :class="scoreColorClass(latestScores[h]?.score)">
                  {{ formatScore(latestScores[h]?.score) }}
                </div>
                <div class="score-meta">
                  <el-tag v-if="latestScores[h]?.recommendation" size="small" :type="recTagType(latestScores[h]?.recommendation)">
                    {{ latestScores[h]?.recommendation }}
                  </el-tag>
                  <span v-if="latestScores[h]?.percentile !== null" class="percentile">
                    P{{ ((latestScores[h]?.percentile ?? 0) * 100).toFixed(0) }}
                  </span>
                </div>
                <div class="score-status">
                  <el-tag v-if="latestScores[h]?.status" size="small" :type="statusTagType(latestScores[h]?.status)">
                    {{ latestScores[h]?.status }}
                  </el-tag>
                </div>
              </div>
              <div v-else class="horizon-card-body empty">
                <span class="text-dim">暂无数据</span>
              </div>
            </el-card>
          </div>

          <!-- Score Verification Metrics -->
          <el-card v-if="selectedScoreDetail" class="verification-card" shadow="hover">
            <div class="section-header">
              <h2>验证指标</h2>
              <span class="model-version">模型: {{ selectedScoreDetail.model_version || '--' }}</span>
            </div>
            <div class="verification-grid">
              <div class="verification-item">
                <span class="v-label">基准价格</span>
                <span class="v-value">{{ formatPrice(selectedScoreDetail.base_price) }}</span>
              </div>
              <div class="verification-item">
                <span class="v-label">目标日期</span>
                <span class="v-value">{{ formatDate(selectedScoreDetail.target_date) }}</span>
              </div>
              <div class="verification-item">
                <span class="v-label">预测得分</span>
                <span class="v-value" :class="scoreColorClass(selectedScoreDetail.score)">
                  {{ formatScore(selectedScoreDetail.score) }}
                </span>
              </div>
              <div class="verification-item">
                <span class="v-label">状态</span>
                <span class="v-value">
                  <el-tag size="small" :type="statusTagType(selectedScoreDetail.status)">
                    {{ selectedScoreDetail.status }}
                  </el-tag>
                </span>
              </div>
              <div class="verification-item">
                <span class="v-label">目标收益</span>
                <span class="v-value" :class="getPriceClass(selectedScoreDetail.verification?.profit_percentage_t5 as number)">
                  {{ formatVerificationMetric(selectedScoreDetail.verification, 'profit_percentage_t5') }}
                </span>
              </div>
              <div class="verification-item">
                <span class="v-label">最大收益</span>
                <span class="v-value" :class="getPriceClass(selectedScoreDetail.verification?.max_profit_percentage as number)">
                  {{ formatVerificationMetric(selectedScoreDetail.verification, 'max_profit_percentage') }}
                </span>
              </div>
              <div class="verification-item">
                <span class="v-label">最大回撤</span>
                <span class="v-value down">
                  {{ formatVerificationMetric(selectedScoreDetail.verification, 'max_drawdown') }}
                </span>
              </div>
              <div class="verification-item">
                <span class="v-label">命中率</span>
                <span class="v-value">
                  {{ formatVerificationMetric(selectedScoreDetail.verification, 'hit_rate') }}
                </span>
              </div>
              <div class="verification-item">
                <span class="v-label">预测有效</span>
                <span class="v-value">
                  <el-icon v-if="selectedScoreDetail.verification?.is_effective" class="effect-icon"><CircleCheck /></el-icon>
                  <el-icon v-else-if="selectedScoreDetail.verification && 'is_effective' in selectedScoreDetail.verification" class="no-effect-icon"><CircleClose /></el-icon>
                  <span v-else class="text-dim">--</span>
                </span>
              </div>
            </div>
          </el-card>

          <!-- Score History Chart -->
          <el-card class="score-chart-card" shadow="hover">
            <div class="section-header">
              <div>
                <h2>评分历史 · Score{{ scoreHorizon }}</h2>
                <p class="section-desc" v-if="scoreHistory.length">共 {{ scoreHistoryTotal }} 条记录</p>
              </div>
              <div class="section-meta">
                <el-button size="small" :loading="scoreLoading" @click="fetchScoreHistory">刷新</el-button>
              </div>
            </div>
            <div ref="scoreChartRef" class="chart score-chart"></div>
            <el-empty v-if="!scoreHistory.length && !scoreLoading" description="暂无评分历史数据" />
          </el-card>

          <!-- Score History Table -->
          <el-card v-if="scoreHistory.length" class="score-table-card" shadow="hover">
            <el-table :data="scoreHistory" size="small" stripe>
              <el-table-column prop="date" label="日期" width="120">
                <template #default="{ row }">{{ formatDate(row.date) }}</template>
              </el-table-column>
              <el-table-column prop="score" label="评分" width="100">
                <template #default="{ row }">
                  <span :class="scoreColorClass(row.score)">{{ formatScore(row.score) }}</span>
                </template>
              </el-table-column>
              <el-table-column prop="recommendation" label="建议" width="100">
                <template #default="{ row }">
                  <el-tag v-if="row.recommendation !== 'NONE'" size="small" :type="recTagType(row.recommendation)">
                    {{ row.recommendation }}
                  </el-tag>
                  <span v-else class="text-dim">--</span>
                </template>
              </el-table-column>
              <el-table-column prop="rank" label="排名" width="80" />
              <el-table-column prop="status" label="状态" width="110">
                <template #default="{ row }">
                  <el-tag size="small" :type="statusTagType(row.status)">
                    {{ row.status }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="目标收益" width="100">
                <template #default="{ row }">
                  <span :class="getPriceClass(row.verification?.profit_percentage_t5 as number)">
                    {{ formatVerificationPct(row.verification, 'profit_percentage_t5') }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column label="最大收益" width="100">
                <template #default="{ row }">
                  <span :class="getPriceClass(row.verification?.max_profit_percentage as number)">
                    {{ formatVerificationPct(row.verification, 'max_profit_percentage') }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column label="模型版本" width="140">
                <template #default="{ row }">{{ row.model_version }}</template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>
      </el-tabs>
    </template>
  </div>
</template>

<script setup lang="ts">
import * as echarts from 'echarts'
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { CircleCheck, CircleClose } from '@element-plus/icons-vue'
import { quoteApi, type QuoteDailyItem, type QuoteDetailResponse } from '@/api/quotes'
import { scoreApi, type ScorePrediction } from '@/api/scores'

const route = useRoute()
const router = useRouter()

const symbol = ref(normalizeRouteSymbol(route.params.symbol))
const searchSymbol = ref(symbol.value)
const detail = ref<QuoteDetailResponse | null>(null)
const history = ref<QuoteDailyItem[]>([])
const loading = ref(false)
const chartRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

// Score tab state
const detailTab = ref('kline')
const scoreHorizons = [5, 20, 60] as const
const scoreHorizon = ref<number>(5)
const scoreLoading = ref(false)
const latestScores = ref<Record<number, ScorePrediction | null>>({ 5: null, 20: null, 60: null })
const scoreHistory = ref<ScorePrediction[]>([])
const scoreHistoryTotal = ref(0)
const scoreChartRef = ref<HTMLDivElement | null>(null)
let scoreChartInstance: echarts.ECharts | null = null

const recentRows = computed(() => history.value.slice(-12).reverse())
const latestHistoryItem = computed(() => {
  if (!history.value.length) return null
  return history.value[history.value.length - 1] ?? null
})

const selectedScoreDetail = computed(() => latestScores.value[scoreHorizon.value])

// Lifecycle
onMounted(() => {
  loadQuote(symbol.value)
})

watch(
  () => route.params.symbol,
  (value) => {
    const nextSymbol = normalizeRouteSymbol(value)
    if (nextSymbol && nextSymbol !== symbol.value) {
      symbol.value = nextSymbol
      searchSymbol.value = nextSymbol
      loadQuote(nextSymbol)
    }
  }
)

watch(history, async () => {
  await nextTick()
  renderChart()
})

watch(scoreHistory, async () => {
  await nextTick()
  renderScoreChart()
})

watch(scoreHorizon, () => {
  fetchScoreHistory()
})

watch(detailTab, async (tab) => {
  if (tab === 'score' && detail.value) {
    await Promise.all([
      fetchLatestScores(),
      fetchScoreHistory()
    ])
  }
})

onBeforeUnmount(() => {
  disposeChart()
  disposeScoreChart()
  window.removeEventListener('resize', handleResize)
})

// Quote loading
async function loadQuote(target: string) {
  const normalized = target.trim()
  if (!normalized) return

  loading.value = true
  try {
    const [detailRes, dailyRes] = await Promise.all([
      quoteApi.getQuoteDetail(normalized),
      quoteApi.getQuoteDaily(normalized, { limit: 180 })
    ])
    detail.value = detailRes
    history.value = dailyRes.quotes
    symbol.value = detailRes.normalized_symbol
    searchSymbol.value = detailRes.normalized_symbol

    // Reset score data on new symbol
    latestScores.value = { 5: null, 20: null, 60: null }
    scoreHistory.value = []

    await nextTick()
    renderChart()
  } catch (error: any) {
    detail.value = null
    history.value = []
    ElMessage.error(error?.response?.data?.message || '股票详情加载失败')
  } finally {
    loading.value = false
  }
}

// Score loading
async function fetchLatestScores() {
  if (!detail.value?.stock.code) return
  const code = detail.value.stock.code

  const results = await Promise.allSettled(
    scoreHorizons.map((h) =>
      scoreApi.getStockScoreHistory(code, { horizon: h, limit: 1 })
    )
  )

  results.forEach((result, idx) => {
    const horizon = scoreHorizons[idx] ?? 5
    if (result.status === 'fulfilled' && result.value.items.length > 0) {
      latestScores.value[horizon] = result.value.items[0] ?? null
    } else {
      latestScores.value[horizon] = null
    }
  })
}

async function fetchScoreHistory() {
  if (!detail.value?.stock.code) return
  const code = detail.value.stock.code

  scoreLoading.value = true
  try {
    const res = await scoreApi.getStockScoreHistory(code, {
      horizon: scoreHorizon.value,
      limit: 60
    })
    scoreHistory.value = res.items
    scoreHistoryTotal.value = res.total
    await nextTick()
    renderScoreChart()
  } catch {
    scoreHistory.value = []
    ElMessage.error('评分历史加载失败')
  } finally {
    scoreLoading.value = false
  }
}

async function handleSearch() {
  const target = searchSymbol.value.trim()
  if (!target) {
    ElMessage.warning('请输入股票代码')
    return
  }
  await router.push({ name: 'QuoteDetail', params: { symbol: target } })
}

// K-line chart
function renderChart() {
  if (!chartRef.value) return
  if (!history.value.length) {
    disposeChart()
    return
  }

  disposeChart()
  chartInstance = echarts.init(chartRef.value)
  const categories = history.value.map(item => formatDate(item.date))
  const candleData = history.value.map(item => [
    item.open ?? '-',
    item.close ?? '-',
    item.low ?? '-',
    item.high ?? '-'
  ])
  const closeLine = history.value.map(item => item.close ?? '-')

  chartInstance.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: { left: 48, right: 24, top: 40, bottom: 72 },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: '#191a1b',
      borderColor: 'rgba(255, 255, 255, 0.08)',
      textStyle: { color: '#f7f8f8' }
    },
    xAxis: {
      type: 'category',
      data: categories,
      boundaryGap: true,
      axisLine: { lineStyle: { color: '#62666d' } },
      axisLabel: { color: '#8a8f98' }
    },
    yAxis: {
      scale: true,
      splitArea: { show: false },
      axisLine: { lineStyle: { color: '#62666d' } },
      splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.05)' } },
      axisLabel: { color: '#8a8f98' }
    },
    dataZoom: [
      { type: 'inside', start: 60, end: 100 },
      {
        show: true,
        type: 'slider',
        top: '90%',
        start: 60,
        end: 100,
        backgroundColor: 'rgba(255, 255, 255, 0.03)',
        fillerColor: 'rgba(113, 112, 255, 0.18)',
        borderColor: 'rgba(255, 255, 255, 0.08)',
        handleStyle: { color: '#7170ff' }
      }
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: candleData,
        itemStyle: {
          color: '#ef4444',
          color0: '#22c55e',
          borderColor: '#ef4444',
          borderColor0: '#22c55e'
        }
      },
      {
        name: '收盘价',
        type: 'line',
        data: closeLine,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 1.5, color: '#7170ff' },
        areaStyle: { opacity: 0.08, color: '#7170ff' }
      }
    ]
  })

  window.removeEventListener('resize', handleResize)
  window.addEventListener('resize', handleResize)
}

// Score history chart
function renderScoreChart() {
  if (!scoreChartRef.value) return
  if (!scoreHistory.value.length) {
    disposeScoreChart()
    return
  }

  disposeScoreChart()
  scoreChartInstance = echarts.init(scoreChartRef.value)

  const sorted = [...scoreHistory.value].sort(
    (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
  )
  const dates = sorted.map(item => formatDate(item.date))
  const scores = sorted.map(item => item.score)
  const recColors = sorted.map(item => {
    if (item.recommendation === 'BUY') return '#10b981'
    if (item.recommendation === 'WATCH') return '#f59e0b'
    if (item.recommendation === 'AVOID') return '#ef4444'
    return '#8a8f98'
  })

  scoreChartInstance.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: { left: 48, right: 24, top: 40, bottom: 72 },
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#191a1b',
      borderColor: 'rgba(255, 255, 255, 0.08)',
      textStyle: { color: '#f7f8f8' },
      formatter: (params: any) => {
        const p = Array.isArray(params) ? params[0] : params
        if (!p) return ''
        const rec = sorted[p.dataIndex]?.recommendation || ''
        return `${p.axisValue}<br/>评分: ${p.value.toFixed(1)} ${rec ? '· ' + rec : ''}`
      }
    },
    xAxis: {
      type: 'category',
      data: dates,
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#62666d' } },
      axisLabel: { color: '#8a8f98' }
    },
    yAxis: {
      min: 0,
      max: 100,
      splitArea: { show: false },
      axisLine: { lineStyle: { color: '#62666d' } },
      splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.05)' } },
      axisLabel: { color: '#8a8f98' }
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      {
        show: true,
        type: 'slider',
        top: '90%',
        start: 0,
        end: 100,
        backgroundColor: 'rgba(255, 255, 255, 0.03)',
        fillerColor: 'rgba(113, 112, 255, 0.18)',
        borderColor: 'rgba(255, 255, 255, 0.08)',
        handleStyle: { color: '#7170ff' }
      }
    ],
    series: [
      {
        name: '评分',
        type: 'line',
        data: scores,
        smooth: true,
        showSymbol: true,
        symbolSize: 6,
        lineStyle: { width: 2, color: '#7170ff' },
        areaStyle: { opacity: 0.1, color: '#7170ff' },
        itemStyle: {
          color: (params: any) => recColors[params.dataIndex] || '#7170ff'
        }
      },
      {
        name: '80 线',
        type: 'line',
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { color: 'rgba(16, 185, 129, 0.3)', type: 'dashed' },
          data: [{ yAxis: 80, label: { formatter: '80', color: '#10b981' } }]
        }
      },
      {
        name: '60 线',
        type: 'line',
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { color: 'rgba(245, 158, 11, 0.3)', type: 'dashed' },
          data: [{ yAxis: 60, label: { formatter: '60', color: '#f59e0b' } }]
        }
      }
    ]
  })

  window.addEventListener('resize', handleResize)
}

function handleResize() {
  chartInstance?.resize()
  scoreChartInstance?.resize()
}

function disposeChart() {
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
}

function disposeScoreChart() {
  if (scoreChartInstance) {
    scoreChartInstance.dispose()
    scoreChartInstance = null
  }
}

// Helpers
function normalizeRouteSymbol(value: unknown) {
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return value[0] || ''
  return ''
}

function formatDate(value: string | null | undefined) {
  if (!value) return '-'
  return value.slice(0, 10)
}

function formatPrice(value: number | null | undefined) {
  if (value === null || value === undefined) return '-'
  return value.toFixed(2)
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined) return '-'
  const sign = value > 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

function formatScore(value: number | null | undefined) {
  if (value === null || value === undefined) return '--'
  return value.toFixed(1)
}

function formatVolume(value: number | null | undefined) {
  if (value === null || value === undefined) return '-'
  if (value >= 100000000) return `${(value / 100000000).toFixed(2)}亿`
  if (value >= 10000) return `${(value / 10000).toFixed(2)}万`
  return `${value}`
}

function formatQuoteChange(quote: QuoteDailyItem | null | undefined) {
  if (!quote) return '-'
  const changeAmount = quote.change_amount ?? calcChangeAmount(quote)
  const changeRate = quote.change_rate ?? calcChangeRate(quote)
  if (changeAmount === null || changeRate === null) return '-'
  const sign = changeAmount >= 0 ? '+' : ''
  return `${sign}${changeAmount.toFixed(2)} (${changeRate >= 0 ? '+' : ''}${changeRate.toFixed(2)}%)`
}

function calcChangeAmount(quote: QuoteDailyItem) {
  if (quote.close === null || quote.previous_close === null) return null
  return quote.close - quote.previous_close
}

function calcChangeRate(quote: QuoteDailyItem) {
  const amount = calcChangeAmount(quote)
  if (amount === null || quote.previous_close === null || quote.previous_close === 0) return null
  return (amount / quote.previous_close) * 100
}

function formatVerificationMetric(verification: Record<string, unknown> | null | undefined, key: string) {
  if (!verification) return '--'
  const val = verification[key]
  if (val === null || val === undefined) return '--'
  if (typeof val === 'number') {
    if (['profit_percentage_t5', 'max_profit_percentage', 'max_drawdown'].includes(key)) {
      return formatPercent(val * 100)
    }
    if (key === 'hit_rate') {
      return (val * 100).toFixed(1) + '%'
    }
    return val.toFixed(2)
  }
  return String(val)
}

function formatVerificationPct(verification: Record<string, unknown> | null | undefined, key: string) {
  if (!verification) return '--'
  const val = verification[key]
  if (val === null || val === undefined) return '--'
  if (typeof val === 'number') {
    return formatPercent(val * 100)
  }
  return String(val)
}

function stockTypeLabel(objectType: string | null | undefined) {
  if (!objectType) return '股票'
  if (objectType === 'stock_index') return '指数'
  if (objectType === 'individual_stock') return '个股'
  return objectType
}

function horizonLabel(horizon: number) {
  if (horizon === 5) return '短期'
  if (horizon === 20) return '中期'
  if (horizon === 60) return '长期'
  return ''
}

function scoreColorClass(score: number | null | undefined) {
  if (score === null || score === undefined) return ''
  if (score >= 80) return 'score-high'
  if (score >= 60) return 'score-med'
  return 'score-low'
}

function getPriceClass(val: number | null | undefined) {
  if (val === null || val === undefined || val === 0) return ''
  return val > 0 ? 'text-up' : 'text-down'
}

function recTagType(rec: string) {
  if (rec === 'BUY') return 'success'
  if (rec === 'WATCH') return 'warning'
  if (rec === 'AVOID') return 'danger'
  return 'info'
}

function statusTagType(status: string | null | undefined) {
  if (!status) return 'info'
  if (status === 'VERIFIED') return 'success'
  if (status === 'FAILED' || status === 'BLOCKED') return 'danger'
  if (status === 'TRACKING') return 'warning'
  return 'info'
}

function goBack() {
  router.push({ name: 'History' })
}
</script>

<style scoped lang="scss">
.quote-detail-page {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.topbar {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  padding: 26px 28px;
  border-radius: 22px;
  background:
    radial-gradient(circle at top right, rgba(113, 112, 255, 0.18), transparent 34%),
    linear-gradient(135deg, #0f1011 0%, #191a1b 55%, #0f1011 100%);
  border: 1px solid rgba(255, 255, 255, 0.08);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.28);
}

.title-block {
  .eyebrow {
    margin: 0 0 8px;
    color: #828fff;
    font-size: 12px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
  }

  h1 {
    margin: 0 0 8px;
    font-size: 32px;
  }

  .subtitle {
    margin: 0;
    color: var(--color-text-secondary);
    line-height: 1.7;
  }
}

.search-card,
.summary-card,
.info-card,
.chart-card,
.table-card,
.verification-card,
.score-chart-card,
.score-table-card {
  border-radius: 18px;
}

.inline-form {
  display: flex;
  gap: 12px;
  align-items: flex-start;

  @media (max-width: 768px) {
    flex-direction: column;
  }
}

.search-field {
  flex: 1;
  margin-bottom: 0;
}

.summary-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 12px;

  @media (max-width: 1200px) {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  @media (max-width: 768px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

.summary-item {
  padding: 16px;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.08);

  &.emphasis {
    background:
      radial-gradient(circle at top right, rgba(113, 112, 255, 0.12), transparent 32%),
      rgba(255, 255, 255, 0.04);
  }

  .label {
    color: var(--color-text-secondary);
    font-size: 13px;
    margin-bottom: 8px;
  }

  .value {
    font-size: 22px;
    font-weight: 700;
  }

  .hint {
    margin-top: 6px;
    color: var(--color-text-secondary);
    font-size: 13px;
  }
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 14px;
}

.section-header h2 {
  margin: 0;
  font-size: 18px;
}

.section-desc,
.section-meta {
  margin: 4px 0 0;
  color: var(--color-text-secondary);
  font-size: 13px;
}

.model-version {
  font-size: 12px;
  color: var(--color-text-secondary);
  font-family: ui-monospace, SF Mono, monospace;
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;

  @media (max-width: 900px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  @media (max-width: 640px) {
    grid-template-columns: 1fr;
  }
}

.info-row {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  padding: 14px 16px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.03);

  .key {
    color: var(--color-text-secondary);
  }

  .value {
    font-weight: 600;
  }
}

.chart {
  width: 100%;
  height: 420px;
}

.score-chart {
  height: 350px;
}

// Detail tabs
:deep(.detail-tabs) {
  .el-tabs__nav-wrap::after { display: none; }
  .el-tabs__item {
    color: var(--color-text-secondary);
    font-size: 15px;
    font-weight: 510;
    &.is-active { color: #fff; }
  }
  .el-tabs__active-bar { background-color: #7170ff; }
}

// Score horizon cards
.score-horizon-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 16px;

  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
}

.horizon-score-card {
  cursor: pointer;
  transition: all 0.2s ease;
  border: 2px solid transparent;

  &:hover {
    border-color: rgba(113, 112, 255, 0.3);
  }

  &.active {
    border-color: var(--el-color-primary);
    background: rgba(113, 112, 255, 0.06);
  }

  .horizon-card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;

    .horizon-badge {
      font-size: 14px;
      font-weight: 700;
      color: #fff;
    }

    .horizon-desc {
      font-size: 11px;
      color: var(--color-text-secondary);
    }
  }

  .horizon-card-body {
    .big-score {
      font-size: 36px;
      font-weight: 700;
      margin-bottom: 8px;

      &.score-high { color: #10b981; }
      &.score-med { color: #f59e0b; }
      &.score-low { color: var(--color-text-secondary); }
    }

    .score-meta {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 6px;
    }

    .score-status {
      margin-top: 4px;
    }

    .percentile {
      font-size: 12px;
      color: var(--color-text-secondary);
    }

    &.empty {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 80px;
    }
  }
}

// Verification grid
.verification-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;

  @media (max-width: 900px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  @media (max-width: 640px) {
    grid-template-columns: 1fr;
  }
}

.verification-item {
  padding: 12px 16px;
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.06);
  display: flex;
  justify-content: space-between;
  align-items: center;

  .v-label {
    color: var(--color-text-secondary);
    font-size: 13px;
  }

  .v-value {
    font-weight: 600;
    font-size: 14px;

    &.down { color: #ef4444; }
  }
}

// Utility
.text-up { color: #10b981; }
.text-down { color: #ef4444; }
.text-dim { color: var(--color-text-secondary); }

.effect-icon {
  font-size: 18px;
  color: #10b981;
}

.no-effect-icon {
  font-size: 18px;
  color: #ef4444;
}

.score-high { color: #10b981; }
.score-med { color: #f59e0b; }
.score-low { color: var(--color-text-secondary); }

:deep(.el-card__body) {
  .verification-card &,
  .score-chart-card &,
  .score-table-card & {
    padding: 20px;
  }
}
</style>
