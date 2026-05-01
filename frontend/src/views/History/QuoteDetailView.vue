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

      <el-card class="chart-card" shadow="hover">
        <div class="section-header">
          <div>
            <h2>K 线</h2>
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
    </template>
  </div>
</template>

<script setup lang="ts">
import * as echarts from 'echarts'
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { quoteApi, type QuoteDailyItem, type QuoteDetailResponse } from '@/api/quotes'

const route = useRoute()
const router = useRouter()

const symbol = ref(normalizeRouteSymbol(route.params.symbol))
const searchSymbol = ref(symbol.value)
const detail = ref<QuoteDetailResponse | null>(null)
const history = ref<QuoteDailyItem[]>([])
const loading = ref(false)
const chartRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

const recentRows = computed(() => history.value.slice(-12).reverse())
const latestHistoryItem = computed(() => {
  if (!history.value.length) {
    return null
  }
  return history.value[history.value.length - 1] ?? null
})

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

onBeforeUnmount(() => {
  disposeChart()
  window.removeEventListener('resize', handleResize)
})

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

async function handleSearch() {
  const target = searchSymbol.value.trim()
  if (!target) {
    ElMessage.warning('请输入股票代码')
    return
  }
  await router.push({ name: 'QuoteDetail', params: { symbol: target } })
}

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

function handleResize() {
  chartInstance?.resize()
}

function disposeChart() {
  window.removeEventListener('resize', handleResize)
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
}

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

function stockTypeLabel(objectType: string | null | undefined) {
  if (!objectType) return '股票'
  if (objectType === 'stock_index') return '指数'
  if (objectType === 'individual_stock') return '个股'
  return objectType
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
.table-card {
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
</style>
