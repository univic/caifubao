<template>
  <div class="dashboard">
    <div class="page-header">
      <div class="header-left">
        <h1 class="page-title">市场总览</h1>
        <p class="page-desc">实时掌握 A 股市场动态</p>
      </div>
      <div class="header-right" v-if="marketStore.lastUpdateTime">
        <span class="update-time">数据更新时间: {{ marketStore.lastUpdateTime }}</span>
        <el-button type="primary" :loading="marketStore.marketLoading" circle @click="refreshData">
          <el-icon><Refresh /></el-icon>
        </el-button>
      </div>
    </div>

    <div v-if="marketStore.marketError" class="dashboard-banner warning">
      {{ marketStore.marketError }}
    </div>
    
    <!-- Index Cards -->
    <div class="index-cards">
      <div v-for="index in marketStore.indices" :key="index.code" class="index-card-wrapper">
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

    <div class="dashboard-grid">
      <!-- Market Breadth Chart -->
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
          <div class="stat-item up">
            <span class="label">涨停</span>
            <span class="value">{{ marketStore.marketBreadth.limitUp }}</span>
          </div>
          <div class="stat-item down">
            <span class="label">跌停</span>
            <span class="value">{{ marketStore.marketBreadth.limitDown }}</span>
          </div>
        </div>
      </div>

      <!-- Recent Signals -->
      <div class="grid-col card">
        <div class="card-header">
          <h3>最新信号预览</h3>
          <router-link to="/signals" class="more-link">全部信号</router-link>
        </div>
        <div v-loading="signalsLoading" class="mini-table-container">
          <table v-if="recentSignals.length" class="mini-table">
            <thead>
              <tr>
                <th>股票</th>
                <th>信号</th>
                <th>方向</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="signal in recentSignals" :key="signal.stock_code + signal.date">
                <td>
                  <div class="stock-info">
                    <strong>{{ signal.stock_name || signal.stock_code }}</strong>
                    <small>{{ signal.stock_code }}</small>
                  </div>
                </td>
                <td>{{ signal.signal_name }}</td>
                <td>
                  <el-tag :type="signal.direction === 'BULLISH' ? 'success' : 'danger'" size="small" effect="plain">
                    {{ signal.direction === 'BULLISH' ? '看多' : '看空' }}
                  </el-tag>
                </td>
              </tr>
            </tbody>
          </table>
          <el-empty v-else description="暂无近期信号" :image-size="60" />
        </div>
      </div>

      <!-- Top Movers -->
      <div class="grid-col card">
        <div class="card-header">
          <h3>领涨个股</h3>
          <span class="card-subtitle">实时涨幅前列</span>
        </div>
        <div class="movers-list">
          <div v-for="stock in marketStore.topGainers" :key="stock.code" class="mover-item">
            <div class="stock-info">
              <span class="name">{{ stock.name }}</span>
              <span class="code">{{ stock.code }}</span>
            </div>
            <div class="stock-price">{{ stock.price.toFixed(2) }}</div>
            <div class="stock-change up">+{{ stock.changePct.toFixed(2) }}%</div>
          </div>
          <el-empty v-if="!marketStore.topGainers.length" description="暂无领涨数据" :image-size="60" />
        </div>
      </div>

      <!-- Capital Flow -->
      <div class="grid-col card">
        <div class="card-header">
          <h3>资金流向与工具</h3>
          <span class="card-subtitle">今日动态</span>
        </div>
        <div class="capital-flow">
          <div class="flow-pill">
            <span class="label">北向资金</span>
            <span class="value" :class="marketStore.capitalFlow.northbound >= 0 ? 'up' : 'down'">
              {{ marketStore.capitalFlow.northbound >= 0 ? '+' : '' }}{{ marketStore.capitalFlow.northbound.toFixed(2) }} 亿
            </span>
          </div>
          <div class="flow-pill">
            <span class="label">主力资金</span>
            <span class="value" :class="marketStore.capitalFlow.main >= 0 ? 'up' : 'down'">
              {{ marketStore.capitalFlow.main >= 0 ? '+' : '' }}{{ marketStore.capitalFlow.main.toFixed(2) }} 亿
            </span>
          </div>
          <div class="flow-pill">
            <span class="label">散户资金</span>
            <span class="value" :class="marketStore.capitalFlow.retail >= 0 ? 'up' : 'down'">
              {{ marketStore.capitalFlow.retail >= 0 ? '+' : '' }}{{ marketStore.capitalFlow.retail.toFixed(2) }} 亿
            </span>
          </div>
        </div>
        
        <div class="quick-actions">
          <h3>快速入口</h3>
          <div class="action-buttons">
            <el-button @click="router.push('/market')">标的看板</el-button>
            <el-button @click="router.push('/backtest/new')">策略回测</el-button>
          </div>
        </div>
      </div>
    </div>

    <!-- Data Status Section -->
    <div class="data-status card">
      <div class="section-header">
        <div>
          <h3>数据处理引擎状态</h3>
          <p v-if="marketStore.dataStatus" class="section-desc">
            最新完整交易日:
            {{ formatDate(marketStore.dataStatus.reference_dates.latest_complete_trading_day) || '未知' }}
          </p>
        </div>
        <div v-if="marketStore.dataStatus" class="section-meta">
          统计于 {{ formatDateTime(marketStore.dataStatus.generated_at) }}
        </div>
      </div>

      <div v-if="marketStore.statusLoading && !marketStore.dataStatus" class="status-loading">
        加载中...
      </div>

      <div v-else-if="marketStore.dataStatus" class="status-grid">
        <div class="status-panel">
          <div class="status-panel-header">
            <h4>指数数据</h4>
            <span class="status-badge" :class="{ healthy: marketStore.dataStatus.index.is_up_to_date }">
              {{ marketStore.dataStatus.index.is_up_to_date ? '已同步' : '落后' }}
            </span>
          </div>
          <div class="status-metrics">
            <div class="metric-pill healthy">
              <span class="label">正常</span>
              <span class="value">{{ marketStore.dataStatus.index.up_to_date_count }}</span>
            </div>
            <div class="metric-pill danger">
              <span class="label">过期</span>
              <span class="value">{{ marketStore.dataStatus.index.expired_count }}</span>
            </div>
          </div>
        </div>

        <div class="status-panel">
          <div class="status-panel-header">
            <h4>个股行情</h4>
            <span class="status-badge" :class="{ healthy: marketStore.dataStatus.stock.is_up_to_date }">
              {{ marketStore.dataStatus.stock.is_up_to_date ? '已同步' : '落后' }}
            </span>
          </div>
          <div class="status-metrics">
            <div class="metric-pill healthy">
              <span class="label">正常</span>
              <span class="value">{{ marketStore.dataStatus.stock.up_to_date_count }}</span>
            </div>
            <div class="metric-pill danger">
              <span class="label">过期</span>
              <span class="value">{{ marketStore.dataStatus.stock.expired_count }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useMarketStore } from '@/stores/market'
import { signalApi, type SignalItem } from '@/api/signals'
import { Refresh } from '@element-plus/icons-vue'
import * as echarts from 'echarts'

const marketStore = useMarketStore()
const router = useRouter()
let refreshTimer: number | null = null

const signalsLoading = ref(false)
const recentSignals = ref<SignalItem[]>([])
const breadthChartRef = ref<HTMLElement | null>(null)
let chartInstance: echarts.ECharts | null = null

// Sparklines management
const sparklineRefs = new Map<string, HTMLElement>()
const sparklineInstances = new Map<string, echarts.ECharts>()

const setSparklineRef = (el: any, code: string) => {
  if (el) {
    sparklineRefs.set(code, el as HTMLElement)
  }
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
    series: [
      {
        data: data,
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
      }
    ]
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

onMounted(() => {
  refreshData()
  // Auto refresh every 60 seconds
  refreshTimer = window.setInterval(() => {
    if (!marketStore.marketLoading && !marketStore.statusLoading) {
      refreshData()
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

async function refreshData() {
  await marketStore.fetchDashboardData()
  fetchRecentSignals()
  initAllSparklines()
}

async function fetchRecentSignals() {
  signalsLoading.value = true
  try {
    const res = await signalApi.listSignals({ limit: 5 })
    recentSignals.value = res.items
  } catch (err) {
    console.error('Failed to fetch signals:', err)
  } finally {
    signalsLoading.value = false
  }
}

function initChart() {
  if (!breadthChartRef.value) return
  
  if (!chartInstance) {
    chartInstance = echarts.init(breadthChartRef.value)
  }
  
  const { advances, declines } = marketStore.marketBreadth
  
  const option = {
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c} ({d}%)'
    },
    legend: {
      orient: 'vertical',
      right: 10,
      top: 'center',
      textStyle: { color: '#8a8f98' }
    },
    series: [
      {
        name: '市场涨跌',
        type: 'pie',
        radius: ['50%', '80%'],
        avoidLabelOverlap: false,
        itemStyle: {
          borderRadius: 4,
          borderColor: '#191a1b',
          borderWidth: 2
        },
        label: {
          show: false,
          position: 'center'
        },
        emphasis: {
          label: {
            show: true,
            fontSize: 16,
            fontWeight: 'bold',
            color: '#f7f8f8'
          }
        },
        labelLine: {
          show: false
        },
        data: [
          { value: advances, name: '上涨', itemStyle: { color: '#ef4444' } },
          { value: declines, name: '下跌', itemStyle: { color: '#22c55e' } }
        ]
      }
    ]
  }
  
  chartInstance.setOption(option)
}

watch(() => marketStore.marketBreadth, () => {
  initChart()
}, { deep: true })

watch(() => marketStore.indices, () => {
  setTimeout(initAllSparklines, 0)
}, { deep: true })

function handleResize() {
  if (chartInstance) chartInstance.resize()
  sparklineInstances.forEach(ins => ins.resize())
}

function formatDate(value: string | null) {
  if (!value) return ''
  return value.slice(0, 10)
}

function formatDateTime(value: string | null) {
  if (!value) return ''
  return new Date(value).toLocaleString('zh-CN')
}
</script>

<style scoped lang="scss">
.dashboard {
  .page-header {
    margin-bottom: 24px;
    display: flex;
    justify-content: space-between;
    align-items: flex-end;

    .header-right {
      display: flex;
      align-items: center;
      gap: 16px;
      margin-bottom: 4px;

      .update-time {
        font-size: 13px;
        color: var(--color-text-secondary);
      }
    }
  }
}

.index-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin-bottom: 24px;
  
  @media (max-width: 1200px) {
    grid-template-columns: repeat(2, 1fr);
  }
  
  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
}

.index-card-wrapper {
  transition: transform 0.2s;
  &:hover {
    transform: translateY(-2px);
  }
}

.index-card {
  border-radius: 16px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  padding: 16px 20px;
  background: rgba(255, 255, 255, 0.03);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.24);
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;

  &.up-bg {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.05) 0%, rgba(255, 255, 255, 0.03) 100%);
  }

  &.down-bg {
    background: linear-gradient(135deg, rgba(34, 197, 94, 0.05) 0%, rgba(255, 255, 255, 0.03) 100%);
  }

  .card-left {
    flex: 1;
    min-width: 0;
  }

  .card-right {
    width: 100px;
    height: 50px;
  }

  .index-name {
    font-size: 13px;
    color: var(--color-text-secondary);
    margin-bottom: 4px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  
  .index-price {
    font-size: 24px;
    font-weight: 600;
    color: var(--color-text-primary);
    margin-bottom: 2px;
  }
  
  .index-change {
    font-size: 13px;
    white-space: nowrap;
    &.up { color: #ef4444; }
    &.down { color: #22c55e; }
  }
}

.sparkline-container {
  width: 100%;
  height: 100%;
}

.dashboard-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 24px;
  margin-bottom: 24px;

  @media (max-width: 1024px) {
    grid-template-columns: 1fr;
  }
}

.card {
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 20px;
  padding: 24px;
  display: flex;
  flex-direction: column;

  .card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;

    h3 {
      font-size: 16px;
      font-weight: 600;
      margin: 0;
    }

    .card-subtitle {
      font-size: 12px;
      color: var(--color-text-secondary);
    }

    .more-link {
      font-size: 13px;
      color: var(--color-primary);
      text-decoration: none;
      &:hover { text-decoration: underline; }
    }
  }
}

.chart-container {
  height: 200px;
  width: 100%;
}

.breadth-summary {
  font-size: 14px;
  font-weight: 500;
  .up { color: #ef4444; }
  .down { color: #22c55e; }
  .divider { margin: 0 8px; color: var(--color-text-placeholder); }
}

.breadth-stats {
  display: flex;
  justify-content: space-around;
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid rgba(255, 255, 255, 0.06);

  .stat-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    .label { font-size: 12px; color: var(--color-text-secondary); }
    .value { font-size: 20px; font-weight: 600; margin-top: 4px; }
  }
}

.mini-table-container {
  flex: 1;
}

.mini-table {
  width: 100%;
  border-collapse: collapse;
  
  th {
    text-align: left;
    font-size: 12px;
    color: var(--color-text-secondary);
    padding: 8px 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  }

  td {
    padding: 12px 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.04);
    font-size: 14px;

    .stock-info {
      display: flex;
      flex-direction: column;
      strong { font-weight: 500; }
      small { font-size: 11px; color: var(--color-text-secondary); }
    }
  }
}

.movers-list {
  display: flex;
  flex-direction: column;
  gap: 12px;

  .mover-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 14px;
    background: rgba(255, 255, 255, 0.02);
    border-radius: 12px;
    border: 1px solid rgba(255, 255, 255, 0.04);

    .stock-info {
      display: flex;
      flex-direction: column;
      width: 120px;
      .name { font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .code { font-size: 11px; color: var(--color-text-secondary); }
    }

    .stock-price { font-family: monospace; }
    .stock-change.up { color: #ef4444; font-weight: 600; }
  }
}

.capital-flow {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
  margin-bottom: 24px;

  .flow-pill {
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 12px;
    padding: 12px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;

    .label { font-size: 11px; color: var(--color-text-secondary); }
    .value { 
      font-size: 15px; 
      font-weight: 600;
      &.up { color: #ef4444; }
      &.down { color: #22c55e; }
    }
  }
}

.quick-actions {
  h3 { font-size: 14px; margin-bottom: 12px; }
  .action-buttons {
    display: flex;
    gap: 12px;
    .el-button { flex: 1; }
  }
}

.data-status {
  margin-top: 24px;
  .section-header {
    display: flex;
    justify-content: space-between;
    margin-bottom: 18px;
    .section-desc { font-size: 13px; color: var(--color-text-secondary); margin-top: 4px; }
    .section-meta { font-size: 12px; color: var(--color-text-secondary); }
  }

  .status-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 16px;
    @media (max-width: 768px) { grid-template-columns: 1fr; }
  }

  .status-panel {
    padding: 16px;
    border-radius: 12px;
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid rgba(255, 255, 255, 0.06);
    
    .status-panel-header {
      display: flex;
      justify-content: space-between;
      margin-bottom: 12px;
      h4 { margin: 0; font-size: 14px; }
    }
    .status-badge {
      font-size: 11px;
      padding: 2px 8px;
      border-radius: 10px;
      &.healthy { background: rgba(34, 197, 94, 0.1); color: #4ade80; }
    }
  }

  .status-metrics {
    display: flex;
    gap: 12px;
  }

  .metric-pill {
    flex: 1;
    display: flex;
    justify-content: space-between;
    padding: 8px 12px;
    border-radius: 8px;
    font-size: 12px;
    &.healthy { background: rgba(34, 197, 94, 0.08); color: #4ade80; }
    &.danger { background: rgba(239, 68, 68, 0.08); color: #fb7185; }
  }
}

.dashboard-banner {
  margin-bottom: 16px;
  padding: 12px 16px;
  border-radius: 10px;
  font-size: 13px;
  background: rgba(245, 158, 11, 0.1);
  color: #fde68a;
  border: 1px solid rgba(245, 158, 11, 0.2);
}
</style>
