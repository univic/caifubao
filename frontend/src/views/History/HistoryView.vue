<template>
  <div class="history-page">
    <section class="hero">
      <div class="hero-copy">
        <p class="eyebrow">Quote Explorer</p>
        <h1>股票查询</h1>
        <p class="hero-desc">
          在同一页面内完成股票搜索、候选结果选择和详情查看，默认展示最近 180 天 K 线。
        </p>
      </div>

      <el-card class="search-card" shadow="hover">
        <el-form @submit.prevent="handleSearch">
          <el-form-item>
            <el-input
              v-model="keyword"
              size="large"
              clearable
              placeholder="例如：600519、贵州茅台、sh600519"
              @keyup.enter="handleSearch"
            >
              <template #prefix>
                <span class="search-prefix">代码</span>
              </template>
            </el-input>
          </el-form-item>

          <div class="search-actions">
            <el-button type="primary" :loading="searchLoading" @click="handleSearch">查询</el-button>
            <el-button :disabled="searchLoading" @click="applyExample('600519')">示例：600519</el-button>
            <el-button :disabled="searchLoading" @click="applyExample('贵州茅台')">示例：贵州茅台</el-button>
          </div>
        </el-form>

        <div class="search-note">
          支持股票代码、带交易所前缀代码和股票名称。唯一命中会直接展示详情，多结果时可在下方候选区选择。
        </div>
      </el-card>
    </section>

    <section v-if="recentSearches.length" class="recent-searches">
      <div class="section-header">
        <h2>最近查看</h2>
      </div>
      <div class="chip-list">
        <el-tag
          v-for="item in recentSearches"
          :key="item"
          effect="dark"
          round
          class="search-chip"
          @click="selectSymbol(item)"
        >
          {{ item }}
        </el-tag>
      </div>
    </section>

    <section class="result-section">
      <div class="section-header">
        <div>
          <h2>搜索结果</h2>
          <p class="section-desc">
            {{ selectedResult ? '当前已选中股票，详情在下方展示。' : '搜索后可在此选择股票。' }}
          </p>
        </div>
        <span class="section-meta" v-if="searched">共 {{ results.length }} 条</span>
      </div>

      <el-skeleton v-if="searchLoading" :rows="4" animated />
      <el-empty v-else-if="searched && !results.length" description="没有找到相关股票" />
      <el-empty v-else-if="!searched" description="输入股票代码或关键词后查看结果" />

      <div v-else class="result-list">
        <el-card
          v-for="item in results"
          :key="item.code"
          class="result-card"
          :class="{ active: item.code === selectedSymbolCode }"
          shadow="hover"
          @click="selectResult(item)"
        >
          <div class="result-main">
            <div>
              <div class="stock-code">{{ item.code }}</div>
              <div class="stock-name">{{ item.name }}</div>
            </div>
            <div class="stock-meta">
              <el-tag size="small" type="info">{{ stockTypeLabel(item.object_type) }}</el-tag>
              <span>{{ item.market_name || '未知市场' }}</span>
            </div>
          </div>
          <div class="result-sub">
            <span v-if="item.exchange_name">交易所: {{ item.exchange_name }}</span>
            <span v-if="item.watch_level !== null">关注等级: {{ item.watch_level }}</span>
            <span v-if="item.active_status !== null">状态: {{ item.active_status }}</span>
          </div>
        </el-card>
      </div>
    </section>

    <section class="detail-section">
      <div class="section-header">
        <div>
          <h2>股票详情</h2>
          <p class="section-desc">
            {{ detail ? `${detail.stock.name} · ${detail.stock.code}` : '选择股票后查看详情' }}
          </p>
        </div>
      </div>

      <el-skeleton v-if="detailLoading && !detail" :rows="8" animated />
      <el-empty v-else-if="!detail" description="请选择一只股票查看详情" />

      <template v-else>
        <el-card class="summary-card" shadow="hover">
          <div class="summary-grid">
            <div class="summary-item emphasis">
              <div class="label">最新价</div>
              <div class="value">{{ formatPrice(detail.latest_quote?.close) }}</div>
              <div class="hint">{{ formatQuoteChange(detail.latest_quote) }}</div>
            </div>
            <div class="summary-item">
              <div class="label">开盘</div>
              <div class="value">{{ formatPrice(detail.latest_quote?.open) }}</div>
            </div>
            <div class="summary-item">
              <div class="label">最高</div>
              <div class="value">{{ formatPrice(detail.latest_quote?.high) }}</div>
            </div>
            <div class="summary-item">
              <div class="label">最低</div>
              <div class="value">{{ formatPrice(detail.latest_quote?.low) }}</div>
            </div>
            <div class="summary-item">
              <div class="label">成交量</div>
              <div class="value">{{ formatVolume(detail.latest_quote?.volume) }}</div>
            </div>
            <div class="summary-item">
              <div class="label">数据状态</div>
              <div class="value">
                <el-tag v-if="detail.freshness?.status" size="small" type="success">
                  {{ detail.freshness.status }}
                </el-tag>
                <span v-else>未知</span>
              </div>
            </div>
          </div>
        </el-card>

        <div class="detail-grid">
          <el-card class="info-card" shadow="hover">
            <div class="section-header">
              <h2>基本信息</h2>
            </div>
            <div class="info-grid">
              <div class="info-row"><span class="key">代码</span><span class="value">{{ detail.stock.code }}</span></div>
              <div class="info-row"><span class="key">名称</span><span class="value">{{ detail.stock.name }}</span></div>
              <div class="info-row"><span class="key">类型</span><span class="value">{{ stockTypeLabel(detail.stock.object_type) }}</span></div>
              <div class="info-row"><span class="key">交易所</span><span class="value">{{ detail.stock.exchange_name || '未知' }}</span></div>
              <div class="info-row"><span class="key">市场</span><span class="value">{{ detail.stock.market_name || '未知' }}</span></div>
              <div class="info-row"><span class="key">关注等级</span><span class="value">{{ detail.stock.watch_level ?? '未知' }}</span></div>
            </div>
          </el-card>

          <el-card class="freshness-card" shadow="hover">
            <div class="section-header">
              <h2>数据状态</h2>
            </div>
            <div class="freshness-list">
              <div class="freshness-item">
                <span class="key">最新 quote 日期</span>
                <span class="value">{{ formatDate(latestHistoryItem?.date) }}</span>
              </div>
              <div class="freshness-item">
                <span class="key">最新 freshness</span>
                <span class="value">{{ formatDate(detail.freshness?.freshness_datetime) }}</span>
              </div>
              <div class="freshness-item">
                <span class="key">统计生成时间</span>
                <span class="value">{{ formatDateTime(detail.freshness?.calculated_at) }}</span>
              </div>
            </div>
          </el-card>
        </div>

        <el-card class="chart-card" shadow="hover">
          <div class="section-header">
            <div>
              <h2>K 线与成交量</h2>
              <p class="section-desc">默认展示最近 180 天，可按区间切换；下方柱状图为成交量。</p>
            </div>
            <div class="chart-toolbar">
              <el-radio-group v-model="range" size="small" @change="handleRangeChange">
                <el-radio-button label="60d">60天</el-radio-button>
                <el-radio-button label="180d">180天</el-radio-button>
                <el-radio-button label="1y">1年</el-radio-button>
              </el-radio-group>
              <span class="section-meta" v-if="latestHistoryItem">
                最新行情 {{ formatDate(latestHistoryItem.date) }}
              </span>
            </div>
          </div>
          <el-skeleton v-if="detailLoading && !history.length" :rows="5" animated />
          <div v-else ref="chartRef" class="chart"></div>
          <el-empty v-if="!history.length && !detailLoading" description="暂无日线数据" />
        </el-card>
      </template>
    </section>
  </div>
</template>

<script setup lang="ts">
import * as echarts from 'echarts'
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { quoteApi, type QuoteDailyItem, type QuoteDetailResponse, type QuoteSearchItem } from '@/api/quotes'

type QuoteRange = '60d' | '180d' | '1y'
type ChartTooltipParam = {
  seriesType?: string
  dataIndex?: number
  axisValueLabel?: string
}

const RANGE_LIMITS: Record<QuoteRange, number> = {
  '60d': 60,
  '180d': 180,
  '1y': 365
}

const route = useRoute()
const router = useRouter()

const keyword = ref('')
const searched = ref(false)
const searchLoading = ref(false)
const detailLoading = ref(false)
const results = ref<QuoteSearchItem[]>([])
const recentSearches = ref<string[]>(loadRecentSearches())
const selectedSymbolCode = ref('')
const selectedResult = ref<QuoteSearchItem | null>(null)
const detail = ref<QuoteDetailResponse | null>(null)
const history = ref<QuoteDailyItem[]>([])
const range = ref<QuoteRange>('180d')
const chartRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

const latestHistoryItem = computed(() => {
  if (!history.value.length) {
    return null
  }
  return history.value[history.value.length - 1] ?? null
})

onMounted(() => {
  void syncFromRoute()
})

watch(
  () => [route.query.q, route.query.symbol, route.query.range],
  () => {
    void syncFromRoute()
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

async function syncFromRoute() {
  const query = normalizeQueryValue(route.query.q)
  const symbol = normalizeQueryValue(route.query.symbol)
  const nextRange = normalizeRange(route.query.range)

  range.value = nextRange
  keyword.value = query || symbol

  if (!query && !symbol) {
    searched.value = false
    results.value = []
    selectedSymbolCode.value = ''
    selectedResult.value = null
    detail.value = null
    history.value = []
    return
  }

  if (query) {
    await performSearch(query, symbol)
    return
  }

  if (symbol) {
    searched.value = true
    results.value = []
    await loadQuote(symbol, true)
  }
}

async function handleSearch() {
  const query = keyword.value.trim()
  if (!query) {
    ElMessage.warning('请输入股票代码或名称')
    return
  }

  await router.push({
    path: '/history',
    query: {
      q: query,
      range: range.value
    }
  })
}

function applyExample(value: string) {
  keyword.value = value
  void handleSearch()
}

async function performSearch(query: string, preferredSymbol: string) {
  searchLoading.value = true
  searched.value = true
  try {
    const res = await quoteApi.searchQuotes(query, 12)
    results.value = res.items

    if (!res.items.length) {
      selectedResult.value = null
      selectedSymbolCode.value = ''
      detail.value = null
      history.value = []
      return
    }

    const preferred = preferredSymbol
      ? res.items.find((item) => item.code === preferredSymbol) ?? null
      : null

    if (preferred) {
      await loadQuote(preferred.code, false, preferred)
      return
    }

    if (res.items.length === 1) {
      const [single] = res.items
      if (!single) {
        return
      }
      await loadQuote(single.code, false, single)
      await pushHistoryQuery(query, single.code)
      return
    }

    selectedResult.value = null
    selectedSymbolCode.value = ''
    detail.value = null
    history.value = []
  } catch (error: any) {
    results.value = []
    ElMessage.error(error?.response?.data?.message || '股票查询失败')
  } finally {
    searchLoading.value = false
  }
}

async function selectResult(item: QuoteSearchItem) {
  await loadQuote(item.code, false, item)
  await pushHistoryQuery(keyword.value.trim() || item.code, item.code)
}

async function selectSymbol(symbol: string) {
  keyword.value = symbol
  await router.push({
    path: '/history',
    query: {
      q: symbol,
      symbol,
      range: range.value
    }
  })
}

async function loadQuote(target: string, fromDirectRoute = false, item: QuoteSearchItem | null = null) {
  const normalized = target.trim()
  if (!normalized) return

  detailLoading.value = true
  try {
    const [detailRes, dailyRes] = await Promise.all([
      quoteApi.getQuoteDetail(normalized),
      quoteApi.getQuoteDaily(normalized, { limit: RANGE_LIMITS[range.value] })
    ])
    detail.value = detailRes
    history.value = dailyRes.quotes
    selectedSymbolCode.value = detailRes.normalized_symbol
    selectedResult.value =
      item ??
      results.value.find((result) => result.code === detailRes.normalized_symbol) ??
      null
    rememberSearch(detailRes.normalized_symbol)

    if (fromDirectRoute && !keyword.value) {
      keyword.value = detailRes.normalized_symbol
    }

    await nextTick()
    renderChart()
  } catch (error: any) {
    detail.value = null
    history.value = []
    selectedResult.value = null
    selectedSymbolCode.value = ''
    ElMessage.error(error?.response?.data?.message || '股票详情加载失败')
  } finally {
    detailLoading.value = false
  }
}

async function handleRangeChange() {
  const symbol = detail.value?.normalized_symbol || selectedSymbolCode.value
  const query = keyword.value.trim()

  await router.push({
    path: '/history',
    query: {
      ...(query ? { q: query } : {}),
      ...(symbol ? { symbol } : {}),
      range: range.value
    }
  })
}

async function pushHistoryQuery(query: string, symbol: string) {
  await router.replace({
    path: '/history',
    query: {
      ...(query ? { q: query } : {}),
      symbol,
      range: range.value
    }
  })
}

function renderChart() {
  if (!chartRef.value) return
  if (!history.value.length) {
    disposeChart()
    return
  }

  disposeChart()
  chartInstance = echarts.init(chartRef.value)
  const categories = history.value.map((item) => formatDate(item.date))
  const candleData = history.value.map((item) => [
    item.open ?? '-',
    item.close ?? '-',
    item.low ?? '-',
    item.high ?? '-'
  ])
  const closeLine = history.value.map((item) => item.close ?? '-')
  const volumeData = history.value.map((item) => item.volume ?? '-')

  chartInstance.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: [
      { left: 48, right: 24, top: 40, height: 300 },
      { left: 48, right: 24, top: 360, height: 80 }
    ],
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: '#191a1b',
      borderColor: 'rgba(255, 255, 255, 0.08)',
      textStyle: { color: '#f7f8f8' },
      formatter(params: ChartTooltipParam | ChartTooltipParam[]) {
        const items = Array.isArray(params) ? params : [params]
        const candle = items.find((item) => item.seriesType === 'candlestick')
        const line = items.find((item) => item.seriesType === 'line')
        const volume = items.find((item) => item.seriesType === 'bar')
        const dataIndex = candle?.dataIndex ?? line?.dataIndex ?? volume?.dataIndex ?? -1
        const quote = dataIndex >= 0 ? history.value[dataIndex] : null

        const open = formatPrice(quote?.open)
        const close = formatPrice(quote?.close)
        const low = formatPrice(quote?.low)
        const high = formatPrice(quote?.high)
        const volumeText = formatVolume(quote?.volume)

        return [
          `${candle?.axisValueLabel ?? line?.axisValueLabel ?? volume?.axisValueLabel ?? '-'}`,
          `开盘: ${open}`,
          `收盘: ${close}`,
          `最低: ${low}`,
          `最高: ${high}`,
          `成交量: ${volumeText}`
        ].join('<br/>')
      }
    },
    xAxis: [
      {
        type: 'category',
        data: categories,
        boundaryGap: true,
        axisLine: { lineStyle: { color: '#62666d' } },
        axisLabel: { show: false },
        axisTick: { show: false },
        splitLine: { show: false }
      },
      {
        type: 'category',
        data: categories,
        gridIndex: 1,
        boundaryGap: true,
        axisLine: { lineStyle: { color: '#62666d' } },
        axisLabel: { show: true },
        axisTick: { show: false },
        splitLine: { show: false }
      }
    ],
    yAxis: [
      {
        scale: true,
        splitArea: { show: false },
        axisLine: { lineStyle: { color: '#62666d' } },
        splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.05)' } },
        axisLabel: { color: '#8a8f98' }
      },
      {
        gridIndex: 1,
        scale: true,
        axisLabel: {
          color: '#8a8f98',
          formatter(value: number) {
            return formatVolume(value)
          }
        },
        splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.05)' } }
      }
    ],
    dataZoom: [
      { type: 'inside', start: 60, end: 100, xAxisIndex: [0, 1] },
      {
        show: true,
        type: 'slider',
        top: '90%',
        start: 60,
        end: 100,
        xAxisIndex: [0, 1],
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
        xAxisIndex: 0,
        yAxisIndex: 0,
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
        xAxisIndex: 0,
        yAxisIndex: 0,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 1.5, color: '#7170ff' },
        areaStyle: { opacity: 0.08, color: '#7170ff' }
      },
      {
        name: '成交量',
        type: 'bar',
        data: volumeData.map((value, index) => {
          const quote = history.value[index]
          const color =
            quote && quote.close !== null && quote.open !== null && quote.close >= quote.open
              ? '#ef4444'
              : '#22c55e'
          return {
            value,
            itemStyle: {
              color
            }
          }
        }),
        xAxisIndex: 1,
        yAxisIndex: 1,
        barWidth: '60%',
        itemStyle: {
          opacity: 0.85
        }
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

function normalizeQueryValue(value: unknown) {
  if (typeof value === 'string') return value.trim()
  if (Array.isArray(value)) return (value[0] || '').trim()
  return ''
}

function normalizeRange(value: unknown): QuoteRange {
  const candidate = normalizeQueryValue(value)
  if (candidate === '60d' || candidate === '180d' || candidate === '1y') {
    return candidate
  }
  return '180d'
}

function formatDate(value: string | null | undefined) {
  if (!value) return '-'
  return value.slice(0, 10)
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '-'
  return new Date(value).toLocaleString('zh-CN')
}

function formatPrice(value: number | null | undefined) {
  if (value === null || value === undefined) return '-'
  return value.toFixed(2)
}

function formatVolume(value: number | null | undefined) {
  if (value === null || value === undefined) return '-'
  if (!Number.isFinite(value)) return '-'
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

function loadRecentSearches() {
  try {
    const raw = localStorage.getItem('quote_recent_searches')
    const parsed = raw ? JSON.parse(raw) : []
    return Array.isArray(parsed) ? parsed.slice(0, 8) : []
  } catch {
    return []
  }
}

function rememberSearch(value: string) {
  const next = [value, ...recentSearches.value.filter((item) => item !== value)].slice(0, 8)
  recentSearches.value = next
  localStorage.setItem('quote_recent_searches', JSON.stringify(next))
}
</script>

<style scoped lang="scss">
.history-page {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.hero {
  display: grid;
  grid-template-columns: 1.1fr 1fr;
  gap: 24px;
  align-items: stretch;

  @media (max-width: 960px) {
    grid-template-columns: 1fr;
  }
}

.hero-copy {
  padding: 28px;
  border-radius: 20px;
  background:
    radial-gradient(circle at top right, rgba(113, 112, 255, 0.18), transparent 34%),
    linear-gradient(135deg, #0f1011 0%, #191a1b 55%, #0f1011 100%);
  color: #f7f8f8;
  border: 1px solid rgba(255, 255, 255, 0.08);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.28);

  .eyebrow {
    margin: 0 0 12px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: #828fff;
    font-size: 12px;
  }

  h1 {
    margin: 0 0 12px;
    font-size: 34px;
    line-height: 1.1;
  }

  .hero-desc {
    margin: 0;
    color: #d0d6e0;
    max-width: 42ch;
    line-height: 1.8;
  }
}

.search-card,
.summary-card,
.info-card,
.freshness-card,
.chart-card {
  border-radius: 20px;
}

.search-prefix {
  color: #828fff;
  font-size: 12px;
  font-weight: 600;
}

.search-actions {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}

.search-note {
  margin-top: 14px;
  color: var(--color-text-secondary);
  font-size: 13px;
  line-height: 1.6;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 14px;

  h2 {
    margin: 0;
    font-size: 20px;
  }
}

.section-desc,
.section-meta {
  margin: 4px 0 0;
  font-size: 13px;
  color: var(--color-text-secondary);
}

.recent-searches,
.result-section,
.detail-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.chip-list {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.search-chip {
  cursor: pointer;
}

.result-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;

  @media (max-width: 960px) {
    grid-template-columns: 1fr;
  }
}

.result-card {
  cursor: pointer;
  border-radius: 18px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  background: rgba(255, 255, 255, 0.03);

  &.active {
    border-color: rgba(113, 112, 255, 0.4);
    box-shadow: 0 12px 28px rgba(113, 112, 255, 0.16);
  }
}

.result-main {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.stock-code {
  color: var(--color-text-secondary);
  font-size: 13px;
}

.stock-name {
  margin-top: 6px;
  font-size: 20px;
  font-weight: 700;
}

.stock-meta {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 8px;
  color: var(--color-text-secondary);
  font-size: 13px;
}

.result-sub {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  margin-top: 14px;
  color: var(--color-text-secondary);
  font-size: 13px;
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

.detail-grid {
  display: grid;
  grid-template-columns: 1.4fr 1fr;
  gap: 16px;

  @media (max-width: 960px) {
    grid-template-columns: 1fr;
  }
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;

  @media (max-width: 640px) {
    grid-template-columns: 1fr;
  }
}

.info-row,
.freshness-item {
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
    text-align: right;
  }
}

.freshness-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.chart-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.chart {
  width: 100%;
  height: 540px;

  @media (max-width: 768px) {
    height: 620px;
  }
}
</style>
