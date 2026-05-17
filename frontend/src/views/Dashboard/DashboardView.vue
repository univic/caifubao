<template>
  <div class="dashboard">
    <!-- Page Header -->
    <div class="page-header">
      <div class="header-left">
        <p class="eyebrow">Workbench</p>
        <h1 class="page-title">今日决策台</h1>
        <p class="page-desc">把市场温度、候选机会、自选变化和数据健康收拢到一个决策入口</p>
      </div>
      <div class="header-right" v-if="marketStore.lastUpdateTime">
        <span class="update-time">更新于 {{ marketStore.lastUpdateTime }}</span>
        <el-button class="btn-refresh" :loading="marketStore.marketLoading" circle @click="refreshAll">
          <el-icon><Refresh /></el-icon>
        </el-button>
      </div>
    </div>

    <!-- Error Banner -->
    <div v-if="marketStore.marketError" class="dashboard-banner warning">
      {{ marketStore.marketError }}
    </div>

    <section class="decision-strip">
      <div class="decision-card market-temperature">
        <span class="decision-label">市场温度</span>
        <strong>{{ marketMood.label }}</strong>
        <p>{{ marketMood.detail }}</p>
        <button type="button" @click="router.push('/signals')">查看今日信号</button>
      </div>

      <div class="decision-card primary-opportunity">
        <span class="decision-label">首要机会</span>
        <template v-if="topOpportunity">
          <strong>{{ topOpportunity.name || topOpportunity.code }}</strong>
          <p>
            {{ topOpportunity.code }} · {{ recommendationLabel(topOpportunity.evaluation.recommendation) }}
            · Score{{ oppHorizon }} {{ formatScore(topOpportunity.evaluation.scores?.[String(oppHorizon)]?.score) }}
          </p>
          <button type="button" @click="navigateToStock(topOpportunity.code)">进入详情</button>
        </template>
        <template v-else>
          <strong>暂无候选</strong>
          <p>当前筛选周期还没有可行动标的。</p>
          <button type="button" @click="router.push('/market')">去机会筛选</button>
        </template>
      </div>

      <div class="decision-card watchlist-action">
        <span class="decision-label">我的待处理</span>
        <strong>{{ watchActionCount }} 项</strong>
        <p>{{ watchActionText }}</p>
        <button type="button" @click="router.push('/portfolio')">查看自选与组合</button>
      </div>

      <div class="decision-card data-health">
        <span class="decision-label">数据健康</span>
        <strong>{{ dataHealth.label }}</strong>
        <p>{{ dataHealth.detail }}</p>
        <button type="button" @click="router.push('/data-quality')">检查数据</button>
      </div>
    </section>

    <!-- Index Cards -->
    <div class="index-cards">
      <div
        v-for="index in marketStore.indices"
        :key="index.code"
        class="index-card-wrapper"
        @click="navigateToIndex(index.code)"
      >
        <div class="index-card" :class="index.changePct >= 0 ? 'up-bg' : 'down-bg'">
          <div class="card-left">
            <div class="index-name">{{ index.name }}</div>
            <div class="index-price">{{ index.price.toFixed(2) }}</div>
            <div class="index-change" :class="index.changePct >= 0 ? 'up' : 'down'">
              {{ index.changePct >= 0 ? '+' : '' }}{{ index.change.toFixed(2) }}
              ({{ index.changePct >= 0 ? '+' : '' }}{{ index.changePct.toFixed(2) }}%)
            </div>
          </div>
          <div class="card-right">
            <div :ref="el => setSparklineRef(el, index.code)" class="sparkline-container"></div>
          </div>
        </div>
      </div>
    </div>

    <!-- Main Dashboard Grid -->
    <div class="dashboard-grid">
      <!-- Column 1: Market Breadth Chart -->
      <div class="grid-col card">
        <div class="card-header">
          <h3>市场涨跌分布</h3>
          <div class="breadth-summary">
            <span class="up">{{ marketStore.marketBreadth.advances }} 涨</span>
            <span class="divider">/</span>
            <span class="down">{{ marketStore.marketBreadth.declines }} 跌</span>
          </div>
        </div>
        <div ref="breadthChartRef" class="chart-container"></div>
        <div class="breadth-stats">
          <div class="stat-item up-bg-stat">
            <span class="label">涨停</span>
            <span class="value">{{ marketStore.marketBreadth.limitUp }}</span>
          </div>
          <div class="stat-item down-bg-stat">
            <span class="label">跌停</span>
            <span class="value">{{ marketStore.marketBreadth.limitDown }}</span>
          </div>
        </div>
      </div>

      <!-- Column 2: Watchlist + Opportunities -->
      <div class="grid-col right-col">
        <!-- Watchlist Card -->
        <div class="card watchlist-card">
          <div class="card-header">
            <div class="header-title-group">
              <h3>
                <el-icon class="header-icon"><StarFilled /></el-icon>
                我的关注
              </h3>
              <span class="badge-count">{{ watchlistStore.count }} 只</span>
            </div>
            <router-link to="/market" class="more-link">标的看板 →</router-link>
          </div>
          <div v-if="watchlistStore.count === 0" class="watchlist-empty">
            <p>暂无关注标的</p>
            <p class="hint">在标的看板或信号页面点击 ☆ 添加自选</p>
            <el-button size="small" class="btn-ghost-sm" @click="router.push('/market')">
              去标的看板
            </el-button>
          </div>
          <div v-else class="watchlist-items" v-loading="watchlistLoading">
            <div
              v-for="item in enrichedWatchlist"
              :key="item.code"
              class="watchlist-row"
              @click="navigateToStock(item.code)"
            >
              <div class="wl-info">
                <span class="wl-name">{{ item.name }}</span>
                <span class="wl-code">{{ item.code }}</span>
              </div>
              <div class="wl-scores" v-if="item.scores">
                <ScoreChip :horizon="5" :score="item.scores['5']?.score" />
                <ScoreChip :horizon="20" :score="item.scores['20']?.score" />
                <ScoreChip :horizon="60" :score="item.scores['60']?.score" />
              </div>
              <div class="wl-scores wl-scores--loading" v-else-if="watchlistLoading">
                <span class="loading-text">加载中...</span>
              </div>
              <div class="wl-change" v-if="item.changePct !== null" :class="changeClass(item.changePct)">
                {{ formatChangePct(item.changePct) }}
              </div>
            </div>
          </div>
        </div>

        <!-- Today's Opportunities -->
        <div class="card opportunities-card">
          <div class="card-header">
            <div class="header-title-group">
              <h3>
                <el-icon class="header-icon"><TrendCharts /></el-icon>
                今日机会
              </h3>
            </div>
            <div class="horizon-toggle">
              <button
                v-for="h in [5, 20, 60]"
                :key="h"
                class="hz-btn"
                :class="{ active: oppHorizon === h }"
                @click="oppHorizon = h; fetchOpportunities()"
              >
                S{{ h }}
              </button>
            </div>
          </div>
          <div v-loading="oppLoading" class="opp-list">
            <div v-if="!opportunities.length && !oppLoading" class="opp-empty">
              暂无数据
            </div>
            <div
              v-for="(item, idx) in opportunities"
              :key="item.code"
              class="opp-row"
              @click="navigateToStock(item.code)"
            >
              <span class="opp-rank" :class="{ 'top-rank': idx < 3 }">#{{ idx + 1 }}</span>
              <div class="opp-info">
                <span class="opp-name">{{ item.name }}</span>
                <span class="opp-code">{{ item.code }}</span>
              </div>
              <div class="opp-scores">
                <ScoreChip :horizon="5" :score="item.evaluation?.scores?.['5']?.score" />
                <ScoreChip :horizon="20" :score="item.evaluation?.scores?.['20']?.score" />
                <ScoreChip :horizon="60" :score="item.evaluation?.scores?.['60']?.score" />
              </div>
              <span class="opp-rec" v-if="item.evaluation?.recommendation && item.evaluation.recommendation !== 'NONE'">
                {{ item.evaluation.recommendation === 'BUY' ? '买入' : item.evaluation.recommendation === 'WATCH' ? '关注' : '回避' }}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Recent Signals (full width) -->
    <div class="card signals-card">
      <div class="card-header">
        <div class="header-title-group">
          <h3>
            <el-icon class="header-icon"><Bell /></el-icon>
            最新信号
          </h3>
        </div>
        <router-link to="/signals" class="more-link">全部信号 →</router-link>
      </div>
      <div v-loading="signalsLoading" class="signals-table-wrap">
        <table v-if="recentSignals.length" class="mini-table">
          <thead>
            <tr>
              <th>股票</th>
              <th>信号</th>
              <th>方向</th>
              <th>评分</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="signal in recentSignals"
              :key="signal.stock_code + signal.date"
              class="signal-row"
              @click="navigateToStock(signal.stock_code)"
            >
              <td>
                <div class="stock-info">
                  <strong>{{ signal.stock_name || signal.stock_code }}</strong>
                  <small>{{ signal.stock_code }}</small>
                </div>
              </td>
              <td>
                <span class="signal-name-text">{{ signalLabel(signal.signal_name) }}</span>
              </td>
              <td>
                <span class="direction-tag" :class="signal.direction.toLowerCase()">
                  {{ signal.direction === 'BULLISH' ? '看多' : '看空' }}
                </span>
              </td>
              <td>
                <div class="signal-scores" v-if="signalScores[signal.stock_code]">
                  <ScoreChip :horizon="5" :score="signalScores[signal.stock_code]?.['5']" />
                  <ScoreChip :horizon="20" :score="signalScores[signal.stock_code]?.['20']" />
                  <ScoreChip :horizon="60" :score="signalScores[signal.stock_code]?.['60']" />
                </div>
                <span v-else class="text-dim">--</span>
              </td>
              <td>
                <WatchlistButton
                  :code="signal.stock_code"
                  :name="signal.stock_name || signal.stock_code"
                  size="small"
                  @click.stop
                />
              </td>
            </tr>
          </tbody>
        </table>
        <el-empty v-else description="暂无近期信号" :image-size="60" />
      </div>
    </div>

    <!-- Data Status Section -->
    <DashboardDataStatus
      :generating-scores="generatingScores"
      :score-gen-message="scoreGenMessage"
      @generate-scores="triggerScoreGeneration"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useMarketStore } from '@/stores/market'
import { useWatchlistStore } from '@/stores/watchlist'
import { signalApi, type SignalItem } from '@/api/signals'
import { marketApi, type MarketComprehensiveItem } from '@/api/market'
import { scoreApi } from '@/api/scores'
import { Refresh, StarFilled, Bell, TrendCharts } from '@element-plus/icons-vue'
import WatchlistButton from '@/components/common/WatchlistButton.vue'
import ScoreChip from '@/components/common/ScoreChip.vue'
import DashboardDataStatus from './DashboardDataStatus.vue'
import * as echarts from 'echarts'

const marketStore = useMarketStore()
const watchlistStore = useWatchlistStore()
const router = useRouter()

let refreshTimer: number | null = null

// Signals
const signalsLoading = ref(false)
const recentSignals = ref<SignalItem[]>([])
const signalScores = ref<Record<string, Record<string, number>>>({})

// Watchlist enrichment
const watchlistLoading = ref(false)
const enrichedWatchlist = ref<Array<{
  code: string
  name: string
  changePct: number | null
  scores: Record<string, { score: number }> | null
}>>([])

// Opportunities
const oppHorizon = ref(5)
const oppLoading = ref(false)
const opportunities = ref<MarketComprehensiveItem[]>([])
const topOpportunity = computed(() => opportunities.value[0] ?? null)
const watchActionCount = computed(() => enrichedWatchlist.value.filter(item => item.scores || item.changePct !== null).length)
const watchActionText = computed(() => {
  if (!watchlistStore.count) return '先把关注标的加入自选，后续信号和评分变化会在这里出现。'
  if (!watchActionCount.value) return '自选列表已建立，等待最新评分和行情同步。'
  return '自选标的已有行情或评分变化，适合进入组合页逐项确认。'
})
const marketMood = computed(() => {
  const { advances, declines, limitUp, limitDown } = marketStore.marketBreadth
  const total = advances + declines
  if (!total) return { label: '等待数据', detail: '市场宽度尚未同步，先检查数据健康。' }
  const ratio = advances / total
  if (ratio >= 0.58) return { label: '偏强', detail: `${advances} 涨 / ${declines} 跌，涨停 ${limitUp} 只，适合优先看多头候选。` }
  if (ratio <= 0.42) return { label: '偏弱', detail: `${advances} 涨 / ${declines} 跌，跌停 ${limitDown} 只，候选需要更严格验证。` }
  return { label: '震荡', detail: `${advances} 涨 / ${declines} 跌，市场分歧较高，适合等待评分确认。` }
})
const dataHealth = computed(() => {
  const status = marketStore.dataStatus
  if (!status) return { label: '未知', detail: '数据链路状态尚未返回。' }
  const healthy = status.index.is_up_to_date && status.stock.is_up_to_date
  if (healthy) return { label: '已同步', detail: `最新完整交易日 ${formatDate(status.reference_dates.latest_complete_trading_day) || '未知'}。` }
  return { label: '需检查', detail: `指数过期 ${status.index.expired_count}，个股过期 ${status.stock.expired_count}。` }
})

// Breadth chart
const breadthChartRef = ref<HTMLElement | null>(null)
let chartInstance: echarts.ECharts | null = null

// Sparklines
const sparklineRefs = new Map<string, HTMLElement>()
const sparklineInstances = new Map<string, echarts.ECharts>()

const setSparklineRef = (el: any, code: string) => {
  if (el) sparklineRefs.set(code, el as HTMLElement)
}

function initSparkline(code: string, data: number[], isUp: boolean) {
  const el = sparklineRefs.get(code)
  if (!el) return
  let instance = sparklineInstances.get(code)
  if (!instance) {
    instance = echarts.init(el)
    sparklineInstances.set(code, instance)
  }
  const option = {
    grid: { left: 0, right: 0, top: 10, bottom: 10 },
    xAxis: { type: 'category', show: false },
    yAxis: { type: 'value', show: false, min: 'dataMin', max: 'dataMax' },
    series: [{
      data,
      type: 'line',
      smooth: true,
      symbol: 'none',
      lineStyle: { width: 2, color: isUp ? '#ef4444' : '#22c55e' },
      areaStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: isUp ? 'rgba(239, 68, 68, 0.2)' : 'rgba(34, 197, 94, 0.2)' },
          { offset: 1, color: 'transparent' }
        ])
      }
    }]
  }
  instance.setOption(option)
}

function initAllSparklines() {
  marketStore.indices.forEach(idx => {
    if (idx.sparkline && idx.sparkline.length) {
      initSparkline(idx.code, idx.sparkline, idx.changePct >= 0)
    }
  })
}

function initChart() {
  if (!breadthChartRef.value) return
  if (!chartInstance) chartInstance = echarts.init(breadthChartRef.value)
  const { advances, declines } = marketStore.marketBreadth
  chartInstance.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: { orient: 'vertical', right: 10, top: 'center', textStyle: { color: '#8a8f98' } },
    series: [{
      name: '市场涨跌',
      type: 'pie',
      radius: ['50%', '80%'],
      avoidLabelOverlap: false,
      itemStyle: { borderRadius: 4, borderColor: '#191a1b', borderWidth: 2 },
      label: { show: false, position: 'center' },
      emphasis: { label: { show: true, fontSize: 16, fontWeight: 'bold', color: '#f7f8f8' } },
      labelLine: { show: false },
      data: [
        { value: advances, name: '上涨', itemStyle: { color: '#ef4444' } },
        { value: declines, name: '下跌', itemStyle: { color: '#22c55e' } }
      ]
    }]
  })
}

// Watch enrichment
watch(() => watchlistStore.items, () => {
  enrichWatchlist()
}, { deep: true, immediate: true })

function navigateToStock(code: string) {
  router.push({ name: 'QuoteDetail', params: { symbol: code } })
}

function navigateToIndex(code: string) {
  router.push({ name: 'QuoteDetail', params: { symbol: code } })
}

function changeClass(val: number | null) {
  if (val === null || val === 0) return ''
  return val > 0 ? 'up' : 'down'
}

function formatChangePct(val: number | null) {
  if (val === null) return '--'
  return (val > 0 ? '+' : '') + val.toFixed(2) + '%'
}

function formatDate(value: string | null) {
  if (!value) return ''
  return value.slice(0, 10)
}

function formatScore(val: number | null | undefined) {
  if (val === null || val === undefined) return '--'
  return val.toFixed(1)
}

function recommendationLabel(value: string) {
  if (value === 'BUY') return '买入观察'
  if (value === 'WATCH') return '关注'
  if (value === 'AVOID') return '回避'
  return '无建议'
}

function signalLabel(value: string) {
  if (value === 'MA10_CROSS_MA20') return 'MA10 上穿 MA20'
  return value
}

// Data fetching
async function refreshAll() {
  await marketStore.fetchDashboardData()
  await Promise.allSettled([
    fetchRecentSignals(),
    fetchOpportunities(),
    enrichWatchlist()
  ])
  nextTickAllCharts()
}

function nextTickAllCharts() {
  setTimeout(() => {
    initAllSparklines()
    initChart()
  }, 50)
}

async function fetchRecentSignals() {
  signalsLoading.value = true
  try {
    const res = await signalApi.listSignals({ limit: 10 })
    recentSignals.value = res.items
    // Fetch scores for signal stocks
    await fetchSignalScores(res.items.map(s => s.stock_code))
  } catch (err) {
    console.error('Failed to fetch signals:', err)
  } finally {
    signalsLoading.value = false
  }
}

async function fetchSignalScores(codes: string[]) {
  if (!codes.length) return
  const uniqueCodes = [...new Set(codes)]
  try {
    // Use market comprehensive to get scores for signal stocks
    const res = await marketApi.getComprehensiveData({
      type: 'stock',
      q: uniqueCodes.slice(0, 20).join(','),
      per_page: uniqueCodes.length
    })
    const scoresMap: Record<string, Record<string, number>> = {}
    res.items.forEach(item => {
      if (item.evaluation?.scores) {
        const s: Record<string, number> = {}
        Object.entries(item.evaluation.scores).forEach(([h, v]) => {
          s[h] = (v as { score: number }).score
        })
        scoresMap[item.code] = s
      }
    })
    signalScores.value = scoresMap
  } catch (err) {
    console.error('Failed to fetch signal scores:', err)
  }
}

async function enrichWatchlist() {
  if (watchlistStore.count === 0) {
    enrichedWatchlist.value = []
    return
  }
  watchlistLoading.value = true
  try {
    // Fetch comprehensive data for watched stocks
    const codes = watchlistStore.items.map(i => i.code).join(',')
    const compRes = await marketApi.getComprehensiveData({
      type: 'stock',
      q: codes,
      per_page: watchlistStore.count + 10
    })

    const enriched: typeof enrichedWatchlist.value = []
    watchlistStore.items.forEach(wlItem => {
      const compItem = compRes.items.find(i => i.code === wlItem.code)
      const changePct = compItem?.ohlcv?.change_rate ?? null

      enriched.push({
        code: wlItem.code,
        name: wlItem.name,
        changePct,
        scores: compItem?.evaluation?.scores
          ? (compItem.evaluation.scores as Record<string, { score: number }>)
          : null
      })
    })
    enrichedWatchlist.value = enriched
  } catch (err) {
    console.error('Failed to enrich watchlist:', err)
    enrichedWatchlist.value = watchlistStore.items.map(item => ({
      code: item.code,
      name: item.name,
      changePct: null,
      scores: null
    }))
  } finally {
    watchlistLoading.value = false
  }
}

async function fetchOpportunities() {
  oppLoading.value = true
  try {
    const res = await marketApi.getComprehensiveData({
      type: 'stock',
      horizon: oppHorizon.value,
      per_page: 10
    })
    opportunities.value = res.items
  } catch (err) {
    console.error('Failed to fetch opportunities:', err)
  } finally {
    oppLoading.value = false
  }
}

// Score generation
const generatingScores = ref(false)
const scoreGenMessage = ref('')

async function triggerScoreGeneration() {
  generatingScores.value = true
  scoreGenMessage.value = ''
  try {
    const res = await scoreApi.generateScores({})
    if (res.success) {
      scoreGenMessage.value = `${res.message}`
      await refreshAll()
    } else {
      scoreGenMessage.value = res.message || '生成失败'
    }
  } catch (err: any) {
    const msg = err?.response?.data?.message || err?.message || '生成失败，请检查后台日志'
    scoreGenMessage.value = msg
    console.error('Score generation failed:', err)
  } finally {
    generatingScores.value = false
  }
}

function handleResize() {
  if (chartInstance) chartInstance.resize()
  sparklineInstances.forEach(ins => ins.resize())
}

watch(() => marketStore.marketBreadth, () => initChart(), { deep: true })
watch(() => marketStore.indices, () => setTimeout(initAllSparklines, 0), { deep: true })

onMounted(() => {
  refreshAll()
  refreshTimer = window.setInterval(() => {
    if (!marketStore.marketLoading && !marketStore.statusLoading) {
      marketStore.fetchDashboardData()
      fetchRecentSignals()
    }
  }, 60000)
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer)
  window.removeEventListener('resize', handleResize)
  if (chartInstance) chartInstance.dispose()
  sparklineInstances.forEach(ins => ins.dispose())
})
</script>

<style scoped lang="scss">
.dashboard {
  --color-brand: #5e6ad2;
  --color-brand-accent: #7170ff;
  --color-up: #ef4444;
  --color-down: #22c55e;
  --color-gold: #f59e0b;
  --color-text-primary: #f7f8f8;
  --color-text-secondary: #d0d6e0;
  --color-text-tertiary: #8a8f98;
  --color-text-quaternary: #62666d;
  --color-border: rgba(255, 255, 255, 0.08);
  --color-border-subtle: rgba(255, 255, 255, 0.05);
  --font-inter: 'Inter Variable', Inter, sans-serif;
  --font-mono: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace;

  max-width: 1400px;
  margin: 0 auto;
  font-family: var(--font-inter);
  font-feature-settings: "cv01", "ss03";
}

/* Page Header */
.page-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 28px;

  .header-right {
    display: flex;
    align-items: center;
    gap: 12px;
    padding-top: 4px;
  }

  .update-time {
    font-size: 13px;
    color: var(--color-text-quaternary);
    font-family: var(--font-mono);
  }
}

.eyebrow {
  font-size: 13px;
  font-weight: 510;
  color: var(--color-brand-accent);
  margin-bottom: 8px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

.page-title {
  font-size: 40px;
  font-weight: 510;
  line-height: 1;
  letter-spacing: -0.88px;
  margin: 0 0 8px 0;
  color: var(--color-text-primary);
}

.page-desc {
  font-size: 15px;
  color: var(--color-text-tertiary);
  margin: 0;
}

.btn-refresh {
  background: rgba(255, 255, 255, 0.03) !important;
  border: 1px solid var(--color-border) !important;
  color: var(--color-text-tertiary) !important;
  &:hover { color: var(--color-text-primary) !important; background: rgba(255, 255, 255, 0.06) !important; }
}

.dashboard-banner {
  margin-bottom: 16px;
  padding: 12px 16px;
  border-radius: 10px;
  font-size: 13px;
  &.warning {
    background: rgba(245, 158, 11, 0.1);
    color: #fde68a;
    border: 1px solid rgba(245, 158, 11, 0.2);
  }
}

.decision-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
  margin-bottom: 24px;
}

.decision-card {
  min-height: 172px;
  padding: 18px;
  border: 1px solid var(--color-border);
  border-radius: 12px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.045), rgba(255, 255, 255, 0.02)),
    #0f1011;
  display: flex;
  flex-direction: column;
  gap: 10px;

  strong {
    color: var(--color-text-primary);
    font-size: 24px;
    font-weight: 590;
    line-height: 1.1;
  }

  p {
    min-height: 44px;
    margin: 0;
    color: var(--color-text-tertiary);
    font-size: 13px;
    line-height: 1.55;
  }

  button {
    align-self: flex-start;
    margin-top: auto;
    border: 1px solid rgba(113, 112, 255, 0.28);
    border-radius: 7px;
    background: rgba(113, 112, 255, 0.12);
    color: #f7f8f8;
    cursor: pointer;
    font-size: 13px;
    font-weight: 510;
    padding: 7px 10px;
  }

  button:hover {
    background: rgba(113, 112, 255, 0.18);
  }
}

.decision-label {
  color: var(--color-text-quaternary);
  font-size: 12px;
  font-weight: 590;
}

/* Index Cards */
.index-cards {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 14px;
  margin-bottom: 24px;

  @media (max-width: 1400px) { grid-template-columns: repeat(4, 1fr); }
  @media (max-width: 1100px) { grid-template-columns: repeat(3, 1fr); }
  @media (max-width: 768px) { grid-template-columns: repeat(2, 1fr); }
  @media (max-width: 480px) { grid-template-columns: 1fr; }
}

.index-card-wrapper {
  cursor: pointer;
  transition: transform 0.2s;
  &:hover { transform: translateY(-2px); }
}

.index-card {
  border-radius: 12px;
  border: 1px solid var(--color-border);
  padding: 16px 18px;
  background: rgba(255, 255, 255, 0.03);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.24);
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;

  &.up-bg { background: linear-gradient(135deg, rgba(239, 68, 68, 0.05) 0%, rgba(255, 255, 255, 0.03) 100%); }
  &.down-bg { background: linear-gradient(135deg, rgba(34, 197, 94, 0.05) 0%, rgba(255, 255, 255, 0.03) 100%); }

  .card-left { flex: 1; min-width: 0; }
  .card-right { width: 90px; height: 45px; }
  .index-name { font-size: 12px; color: var(--color-text-tertiary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .index-price { font-size: 22px; font-weight: 590; color: var(--color-text-primary); letter-spacing: -0.288px; }
  .index-change { font-size: 12px; &.up { color: var(--color-up); } &.down { color: var(--color-down); } }
}

.sparkline-container { width: 100%; height: 100%; }

/* Dashboard Grid */
.dashboard-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  margin-bottom: 20px;

  @media (max-width: 1024px) { grid-template-columns: 1fr; }
}

.grid-col { min-width: 0; }

.right-col {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

/* Cards */
.card {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  padding: 24px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;

  h3 {
    font-size: 15px;
    font-weight: 590;
    color: var(--color-text-primary);
    margin: 0;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .header-icon {
    color: var(--color-brand-accent);
  }

  .header-title-group {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .badge-count {
    font-size: 12px;
    font-weight: 510;
    color: var(--color-text-quaternary);
    background: rgba(255, 255, 255, 0.06);
    padding: 2px 10px;
    border-radius: 999px;
  }

  .more-link {
    font-size: 13px;
    font-weight: 510;
    color: var(--color-brand-accent);
    text-decoration: none;
    &:hover { opacity: 0.8; }
  }
}

.chart-container { height: 200px; width: 100%; }
.breadth-summary { font-size: 14px; font-weight: 510; .up { color: var(--color-up); } .down { color: var(--color-down); } .divider { margin: 0 6px; color: var(--color-text-quaternary); } }
.breadth-stats { display: flex; justify-content: space-around; margin-top: 14px; padding-top: 14px; border-top: 1px solid var(--color-border-subtle); }
.stat-item {
  display: flex; flex-direction: column; align-items: center; padding: 8px 20px; border-radius: 10px;
  .label { font-size: 11px; color: var(--color-text-quaternary); }
  .value { font-size: 20px; font-weight: 590; margin-top: 2px; }
  &.up-bg-stat { background: rgba(239, 68, 68, 0.06); .value { color: var(--color-up); } }
  &.down-bg-stat { background: rgba(34, 197, 94, 0.06); .value { color: var(--color-down); } }
}

/* Watchlist */
.watchlist-card { flex: 1; }
.watchlist-empty {
  text-align: center; padding: 24px 0;
  p { color: var(--color-text-tertiary); font-size: 14px; margin: 0 0 4px 0; }
  .hint { font-size: 12px; color: var(--color-text-quaternary); margin-bottom: 12px; }
}

.watchlist-items {
  display: flex; flex-direction: column; gap: 6px;
}

.watchlist-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 14px;
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid transparent;
  cursor: pointer;
  transition: all 0.15s;

  &:hover {
    background: rgba(255, 255, 255, 0.04);
    border-color: var(--color-border-subtle);
  }

  .wl-info {
    display: flex; flex-direction: column; min-width: 100px;
    .wl-name { font-size: 14px; font-weight: 510; color: var(--color-text-primary); }
    .wl-code { font-size: 11px; color: var(--color-text-quaternary); font-family: var(--font-mono); }
  }

  .wl-scores {
    display: flex; gap: 6px;
    &--loading { .loading-text { font-size: 11px; color: var(--color-text-quaternary); } }
  }

  .wl-change {
    font-size: 13px; font-weight: 510; min-width: 60px; text-align: right;
    &.up { color: var(--color-up); }
    &.down { color: var(--color-down); }
  }
}

/* Opportunities */
.horizon-toggle {
  display: flex; gap: 4px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid var(--color-border-subtle);
  border-radius: 8px;
  padding: 3px;
  .hz-btn {
    font-size: 11px; font-weight: 590; padding: 4px 10px;
    border: none; border-radius: 6px;
    background: transparent; color: var(--color-text-quaternary);
    cursor: pointer; transition: all 0.15s;
    &.active { background: var(--color-brand); color: #f7f8f8; }
    &:hover:not(.active) { color: var(--color-text-secondary); }
  }
}

.opp-list { display: flex; flex-direction: column; gap: 6px; }
.opp-empty { text-align: center; padding: 20px; color: var(--color-text-quaternary); }

.opp-row {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 14px; border-radius: 10px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid transparent;
  cursor: pointer; transition: all 0.15s;

  &:hover { background: rgba(255, 255, 255, 0.04); border-color: var(--color-border-subtle); }

  .opp-rank {
    font-size: 11px; color: var(--color-text-quaternary); font-weight: 510; min-width: 28px;
    &.top-rank { color: var(--color-gold); font-weight: 590; }
  }
  .opp-info {
    display: flex; flex-direction: column; flex: 1; min-width: 0;
    .opp-name { font-size: 13px; font-weight: 510; color: var(--color-text-primary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .opp-code { font-size: 11px; color: var(--color-text-quaternary); font-family: var(--font-mono); }
  }
  .opp-scores { display: flex; gap: 4px; }
  .opp-rec {
    font-size: 11px; font-weight: 590; padding: 3px 8px; border-radius: 5px;
    background: rgba(16, 185, 129, 0.1); color: #10b981;
    white-space: nowrap;
  }
}

/* Signals Card */
.signals-card {
  margin-bottom: 20px;
}

.signals-table-wrap {
  overflow-x: auto;
}

.mini-table {
  width: 100%; border-collapse: collapse;
  th {
    text-align: left; font-size: 11px; font-weight: 510; color: var(--color-text-quaternary);
    padding: 10px 12px; border-bottom: 1px solid var(--color-border-subtle);
    text-transform: uppercase; letter-spacing: 0.05em;
  }
  td { padding: 10px 12px; border-bottom: 1px solid rgba(255, 255, 255, 0.03); font-size: 13px; }
}

.signal-row {
  cursor: pointer; transition: background 0.15s;
  &:hover { background: rgba(255, 255, 255, 0.02); }
}

.stock-info {
  display: flex; flex-direction: column;
  strong { font-weight: 510; color: var(--color-text-primary); }
  small { font-size: 11px; color: var(--color-text-quaternary); font-family: var(--font-mono); }
}

.signal-name-text { color: var(--color-text-secondary); font-size: 13px; }

.direction-tag {
  font-size: 10px; font-weight: 590; padding: 3px 8px; border-radius: 4px;
  &.bullish { color: #10b981; background: rgba(16, 185, 129, 0.1); }
  &.bearish { color: #ef4444; background: rgba(239, 68, 68, 0.1); }
}

.signal-scores { display: flex; gap: 4px; }

.text-dim { color: var(--color-text-quaternary); font-size: 12px; }

/* Utility */
.btn-ghost-sm {
  background: rgba(255, 255, 255, 0.03) !important;
  border: 1px solid var(--color-border) !important;
  color: var(--color-text-secondary) !important;
}

/* Responsive */
@media (max-width: 1024px) {
  .page-title { font-size: 28px; }
  .decision-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}

@media (max-width: 768px) {
  .page-header { flex-direction: column; gap: 12px; }
  .right-col { gap: 14px; }
}

@media (max-width: 560px) {
  .decision-strip { grid-template-columns: 1fr; }
}
</style>
