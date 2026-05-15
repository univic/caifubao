<template>
  <div class="backtest-result-page">
    <section class="topbar">
      <div class="title-block">
        <p class="eyebrow">Backtest Result</p>
        <h1>回测结果</h1>
        <p v-if="result?.strategy" class="subtitle">
          Score{{ result.strategy.horizon }} · Top {{ result.strategy.top_n }}
          · {{ result.strategy.start_date }} ~ {{ result.strategy.end_date }}
          · 模型 {{ result.strategy.model_version }}
        </p>
      </div>
      <div class="topbar-actions">
        <el-button plain @click="goCreate">新建回测</el-button>
        <el-button plain @click="goList">返回列表</el-button>
      </div>
    </section>

    <!-- Summary Metrics -->
    <el-card v-if="result?.summary" class="summary-card" shadow="hover">
      <div class="section-header">
        <h2>绩效总览</h2>
      </div>
      <div class="summary-grid">
        <div class="metric-card">
          <span class="metric-label">总收益</span>
          <span class="metric-value" :class="result.summary.total_return >= 0 ? 'up' : 'down'">
            {{ formatPct(result.summary.total_return) }}
          </span>
        </div>
        <div class="metric-card">
          <span class="metric-label">年化收益</span>
          <span class="metric-value" :class="result.summary.annualized_return >= 0 ? 'up' : 'down'">
            {{ formatPct(result.summary.annualized_return) }}
          </span>
        </div>
        <div class="metric-card">
          <span class="metric-label">最大回撤</span>
          <span class="metric-value down">{{ formatPct(result.summary.max_drawdown) }}</span>
        </div>
        <div class="metric-card">
          <span class="metric-label">夏普比率</span>
          <span class="metric-value">{{ formatNum(result.summary.sharpe_ratio) }}</span>
        </div>
        <div class="metric-card">
          <span class="metric-label">命中率</span>
          <span class="metric-value">{{ formatPct(result.summary.overall_hit_rate) }}</span>
        </div>
        <div class="metric-card">
          <span class="metric-label">胜率</span>
          <span class="metric-value">{{ formatPct(result.summary.win_rate) }}</span>
        </div>
      </div>
    </el-card>

    <!-- Equity Curve -->
    <el-card v-if="result?.equity_curve?.length" class="chart-card" shadow="hover">
      <div class="section-header">
        <h2>权益曲线</h2>
      </div>
      <div ref="chartRef" class="chart"></div>
    </el-card>

    <!-- Daily Results Table -->
    <el-card v-if="result?.daily_results?.length" class="table-card" shadow="hover">
      <div class="section-header">
        <h2>每日明细</h2>
        <span class="section-meta">共 {{ result.daily_results.length }} 个交易日</span>
      </div>
      <el-table :data="result.daily_results" size="small" stripe>
        <el-table-column prop="date" label="日期" width="120">
          <template #default="{ row }">{{ row.date?.slice(0, 10) }}</template>
        </el-table-column>
        <el-table-column prop="position_count" label="持仓数" width="100" align="center" />
        <el-table-column label="平均收益" width="110" align="right">
          <template #default="{ row }">
            <span :class="(row.avg_return ?? 0) >= 0 ? 'up' : 'down'">
              {{ formatPct(row.avg_return) }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="最大收益" width="110" align="right">
          <template #default="{ row }">
            <span :class="(row.avg_max_return ?? 0) >= 0 ? 'up' : 'down'">
              {{ formatPct(row.avg_max_return) }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="命中率" width="100" align="right">
          <template #default="{ row }">{{ formatPct(row.hit_rate) }}</template>
        </el-table-column>
        <el-table-column label="头部标的" min-width="200">
          <template #default="{ row }">
            <div class="top-stocks-cell">
              <el-tag
                v-for="s in (row.top_stocks || []).slice(0, 3)"
                :key="s.stock_code"
                size="small"
                :type="s.hit_target ? 'success' : 'info'"
                class="stock-tag"
              >
                {{ s.stock_code }}
                <span v-if="s.return_at_target !== null" class="tag-return">
                  {{ formatPct(s.return_at_target) }}
                </span>
              </el-tag>
              <span v-if="(row.top_stocks || []).length > 3" class="more-hint">
                +{{ row.top_stocks.length - 3 }}
              </span>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- Empty State -->
    <el-empty v-if="!result" description="暂无回测结果">
      <el-button type="primary" @click="goCreate">创建回测</el-button>
    </el-empty>
  </div>
</template>

<script setup lang="ts">
import * as echarts from 'echarts'
import { nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import type { BacktestResponse } from '@/api/scoreStrategies'

const router = useRouter()
const chartRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

const result = ref<BacktestResponse | null>(null)

onMounted(() => {
  const raw = sessionStorage.getItem('backtest_result')
  if (raw) {
    try {
      result.value = JSON.parse(raw)
    } catch {
      result.value = null
    }
  }
  nextTick(() => renderChart())
})

onBeforeUnmount(() => {
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
  window.removeEventListener('resize', handleResize)
})

function renderChart() {
  if (!chartRef.value || !result.value?.equity_curve?.length) return

  if (chartInstance) chartInstance.dispose()
  chartInstance = echarts.init(chartRef.value)

  const data = [...result.value.equity_curve].sort(
    (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
  )
  const dates = data.map((d) => d.date.slice(0, 10))
  const values = data.map((d) => d.value)
  const firstValue = values[0] ?? 1
  const isUp = values.length > 0 && (values[values.length - 1] ?? 1) >= firstValue

  chartInstance.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: { left: 60, right: 24, top: 32, bottom: 64 },
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#191a1b',
      borderColor: 'rgba(255, 255, 255, 0.08)',
      textStyle: { color: '#f7f8f8', fontSize: 13 },
      formatter: (params: any) => {
        const p = Array.isArray(params) ? params[0] : params
        if (!p) return ''
        return `${p.axisValue}<br/>净值: ${Number(p.value).toFixed(4)}`
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
      scale: true,
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
        name: '权益曲线',
        type: 'line',
        data: values,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: isUp ? '#10b981' : '#ef4444' },
        areaStyle: {
          opacity: 0.1,
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: isUp ? '#10b981' : '#ef4444' },
            { offset: 1, color: 'transparent' }
          ])
        }
      },
      {
        name: '基准线',
        type: 'line',
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { color: 'rgba(255, 255, 255, 0.15)', type: 'dashed' },
          data: [{ yAxis: 1, label: { formatter: '1.00', color: '#8a8f98' } }]
        }
      }
    ]
  })

  window.addEventListener('resize', handleResize)
}

function handleResize() {
  chartInstance?.resize()
}

function formatPct(value: number | null | undefined): string {
  if (value == null) return '--'
  return (value * 100).toFixed(2) + '%'
}

function formatNum(value: number | null | undefined): string {
  if (value == null) return '--'
  return value.toFixed(2)
}

function goCreate() {
  router.push({ name: 'BacktestCreate' })
}

function goList() {
  router.push({ name: 'BacktestList' })
}
</script>

<style scoped lang="scss">
.backtest-result-page {
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

.topbar-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  padding-top: 4px;
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

.summary-card,
.chart-card,
.table-card {
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 16px;

  h2 {
    margin: 0;
    font-size: 18px;
  }
}

.section-meta {
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

.metric-card {
  padding: 18px 16px;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.08);
  display: flex;
  flex-direction: column;
  gap: 8px;

  .metric-label {
    color: var(--color-text-secondary);
    font-size: 13px;
  }

  .metric-value {
    font-size: 22px;
    font-weight: 700;

    &.up { color: #10b981; }
    &.down { color: #ef4444; }
  }
}

.chart {
  width: 100%;
  height: 420px;
}

.top-stocks-cell {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
}

.stock-tag {
  .tag-return {
    margin-left: 4px;
    opacity: 0.85;
  }
}

.more-hint {
  font-size: 12px;
  color: #8a8f98;
}

.up { color: #10b981; }
.down { color: #ef4444; }

:deep(.el-card__body) {
  .chart-card &,
  .table-card & {
    padding: 20px;
  }
}
</style>
