<template>
  <section class="research-paper">
    <header class="headline">
      <div>
        <div class="eyebrow">RESEARCH / PAPER OBSERVABILITY</div>
        <h1>研究模拟盘</h1>
        <p>研究策略计划、净值快照与前瞻证据。非券商账户；未经核验的净值不可用于策略晋级。</p>
      </div>
      <el-button :loading="loading" @click="loadCohorts">刷新</el-button>
    </header>

    <el-alert
      title="仅研究模拟：分红/送转等公司行动仍在对账，NAV 标记为 UNVERIFIED；不代表实盘收益或已认证策略。"
      type="warning"
      :closable="false"
      show-icon
      class="notice"
    />

    <div class="filter-row">
      <span>策略 / 模型 / 固定配置</span>
      <el-select
        v-model="selected"
        placeholder="选择已记录的策略配置"
        style="min-width: 360px; max-width: 100%;"
        @change="loadOverview"
      >
        <el-option
          v-for="(cohort, index) in cohorts"
          :key="keyFor(cohort)"
          :label="labelFor(cohort)"
          :value="String(index)"
        />
      </el-select>
    </div>

    <el-alert
      v-if="errorMessage"
      :title="errorMessage"
      type="error"
      :closable="false"
      show-icon
      class="notice"
    />

    <el-skeleton v-if="loading && !overview" :rows="7" animated />
    <el-empty
      v-else-if="!loading && !overview && !errorMessage"
      description="尚无可查看的研究策略运行记录。请先完成受控的 Research 策略运行。"
    />

    <template v-if="overview">
      <div class="metrics">
        <div class="metric">
          <div class="metric-label">最近净值快照 · 未核验</div>
          <div class="metric-value">{{ money(latest?.nav) }}</div>
          <div class="metric-note">{{ latest?.date || '没有已写入的净值快照' }}</div>
        </div>
        <div class="metric">
          <div class="metric-label">当前快照回撤</div>
          <div class="metric-value">{{ fraction(latest?.drawdown) }}</div>
          <div class="metric-note">不作为已验证收益指标</div>
        </div>
        <div class="metric">
          <div class="metric-label">观察到的前瞻交易日</div>
          <div class="metric-value">
            {{ overview.forward_observed_sessions }} / {{ overview.forward_required }}
          </div>
          <div class="metric-note">
            {{ overview.window ? 'ACTIVE 窗口 · 仍未获最终认证' : '没有匹配的 ACTIVE 前瞻窗口' }}
          </div>
        </div>
        <div class="metric">
          <div class="metric-label">最近策略记录</div>
          <div class="metric-value">{{ overview.total_loaded }}</div>
          <div class="metric-note">
            {{ overview.has_more_runs ? '最近 400 条（已截断）' : '固定配置范围内的已加载记录' }}
          </div>
        </div>
      </div>

      <div class="panel">
        <div class="panel-title">
          <div>
            <h2>模拟净值走势</h2>
            <p>仅绘制数据库中实际存在的快照；不补造交易日或收益。使用原始人民币 NAV。</p>
          </div>
          <el-tag type="warning">UNVERIFIED</el-tag>
        </div>
        <div v-if="overview.curve.length" ref="chartEl" class="chart" aria-label="研究模拟净值曲线" />
        <el-empty v-else description="尚无净值快照；不会用目标持仓推算虚假净值。" />
        <p v-if="overview.curve.some(item => (item.stale_positions_count || 0) > 0)" class="warning">
          包含使用陈旧行情估值的交易日；具体数量见数据快照。
        </p>
      </div>

      <div class="panel">
        <div class="panel-title">
          <div>
            <h2>决策历史</h2>
            <p>按信号决策日排序，展示目标持仓和调仓差异；目标不是实际成交。</p>
          </div>
          <el-tag type="info">READ ONLY</el-tag>
        </div>
        <el-table :data="overview.decisions" row-key="date" @row-click="openDecision">
          <el-table-column prop="date" label="信号日" width="120" />
          <el-table-column prop="execution_date" label="计划执行日" width="125" />
          <el-table-column label="证据" width="110">
            <template #default="{ row }">
              <el-tag :type="row.evidence_kind === 'FORWARD' && overview.window ? 'success' : 'info'">
                {{ row.evidence_kind }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="status" label="状态" width="120" />
          <el-table-column label="目标数量" width="100">
            <template #default="{ row }">{{ row.target_holdings.length }}</template>
          </el-table-column>
          <el-table-column label="目标 / 净值溯源" min-width="240">
            <template #default="{ row }">
              {{ row.hash_kind }} · {{ row.config_hash.slice(0, 12) }} ·
              {{ row.nav_snapshot ? '有快照，未核验' : '暂无快照' }}
            </template>
          </el-table-column>
        </el-table>
        <p class="panel-foot">
          点击决策查看目标权重与增删清单。执行意图、风控、模拟成交尚未与本时间线建立一对一链路。
        </p>
      </div>
    </template>

    <el-dialog v-model="decisionVisible" title="策略决策详情 · 只读" width="680px">
      <template v-if="decision">
        <p>决策日 {{ decision.date }} · 计划执行日 {{ decision.execution_date || '--' }}</p>
        <p>证据 {{ decision.evidence_kind }} · 哈希类型 {{ decision.hash_kind }}</p>
        <p>配置哈希 <code>{{ decision.config_hash }}</code></p>
        <p v-if="decision.skip_reason">跳过原因：{{ decision.skip_reason }}</p>
        <h3>目标持仓（非真实持仓）</h3>
        <el-table :data="decision.target_holdings" size="small">
          <el-table-column prop="stock_code" label="代码" />
          <el-table-column label="目标权重">
            <template #default="{ row }">{{ fraction(row.weight) }}</template>
          </el-table-column>
        </el-table>
        <h3>调仓差异</h3>
        <pre class="json-block">{{ JSON.stringify(decision.rebalance, null, 2) }}</pre>
        <el-alert type="warning" :closable="false" title="此决策没有已关联的成交/风险事件；不得把目标持仓视为已成交。" />
      </template>
      <template #footer>
        <el-button @click="decisionVisible = false">关闭</el-button>
      </template>
    </el-dialog>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import * as echarts from 'echarts'
import {
  getResearchPaperDecision,
  getResearchPaperOverview,
  listResearchPaperCohorts,
  type ResearchPaperCohort,
  type ResearchPaperDecision,
  type ResearchPaperOverview,
  type ResearchPaperNavPoint
} from '@/api/researchPaper'

const loading = ref(false)
const errorMessage = ref('')
const cohorts = ref<ResearchPaperCohort[]>([])
const selected = ref('')
const overview = ref<ResearchPaperOverview | null>(null)
const decision = ref<ResearchPaperDecision | null>(null)
const decisionVisible = ref(false)
const chartEl = ref<HTMLElement | null>(null)
let chart: echarts.ECharts | null = null
let requestId = 0

const latest = computed<ResearchPaperNavPoint | null>(() => {
  const points = overview.value?.curve || []
  return points.length ? points[points.length - 1] : null
})

function money(value: number | null | undefined) {
  return value == null || !Number.isFinite(value)
    ? '--' : value.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}
function fraction(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? '--' : (value * 100).toFixed(2) + '%'
}
function keyFor(item: ResearchPaperCohort) {
  return [item.model_version, item.horizon, item.hash_kind, item.config_hash].join(':')
}
function labelFor(item: ResearchPaperCohort) {
  return item.model_version + ' / ' + item.horizon + '日 / ' + item.hash_kind
    + ' / ' + item.config_hash.slice(0, 10) + '…'
}

function renderChart() {
  if (chart) {
    chart.dispose()
    chart = null
  }
  if (!chartEl.value || !overview.value?.curve.length) return
  const points = overview.value.curve
  chart = echarts.init(chartEl.value)
  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 72, right: 28, top: 22, bottom: 45 },
    xAxis: {
      type: 'category',
      data: points.map(p => p.date || '--'),
      axisLabel: { color: '#a3a8b5' },
      axisLine: { lineStyle: { color: '#414651' } }
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: { color: '#a3a8b5' },
      splitLine: { lineStyle: { color: '#2b2e35' } }
    },
    series: [{
      name: 'NAV（未经核验）',
      type: 'line',
      showSymbol: points.length < 35,
      smooth: false,
      data: points.map(p => p.nav),
      lineStyle: { width: 2 },
      areaStyle: { opacity: 0.08 }
    }]
  })
}

async function loadCohorts() {
  const id = ++requestId
  loading.value = true
  errorMessage.value = ''
  try {
    const list = await listResearchPaperCohorts()
    if (id !== requestId) return
    cohorts.value = list
    if (!list.length) {
      overview.value = null
      selected.value = ''
      chart?.dispose()
      chart = null
    } else {
      const oldKey = cohorts.value[Number(selected.value)]
      selected.value = String(list.findIndex(item => keyFor(item) === keyFor(oldKey || list[0])))
      if (selected.value === '-1') selected.value = '0'
      await loadOverview()
    }
  } catch {
    if (id === requestId) {
      errorMessage.value = '读取 Research 模拟盘配置失败'
      overview.value = null
    }
  } finally {
    if (id === requestId) loading.value = false
  }
}

async function loadOverview() {
  const item = cohorts.value[Number(selected.value)]
  if (!item) return
  const id = ++requestId
  loading.value = true
  errorMessage.value = ''
  try {
    const result = await getResearchPaperOverview(item)
    if (id !== requestId) return
    overview.value = result
    await nextTick()
    renderChart()
  } catch {
    if (id === requestId) {
      overview.value = null
      errorMessage.value = '读取净值和决策历史失败'
    }
  } finally {
    if (id === requestId) loading.value = false
  }
}

async function openDecision(row: ResearchPaperDecision) {
  const item = cohorts.value[Number(selected.value)]
  if (!item) return
  try {
    decision.value = await getResearchPaperDecision(item, row.date)
    decisionVisible.value = true
  } catch {
    errorMessage.value = '决策详情读取失败'
  }
}

onMounted(loadCohorts)
onBeforeUnmount(() => {
  requestId += 1
  chart?.dispose()
})
</script>

<style scoped lang="scss">
.research-paper { max-width: 1280px; margin: 0 auto; color: #f4f4f5; }
.headline { display: flex; justify-content: space-between; align-items: center; gap: 18px; }
.headline h1 { font-size: 30px; margin: 6px 0 8px; }
.headline p, .panel p, .metric-note { color: #9ca3af; }
.eyebrow { color: #a5b4fc; font-size: 12px; letter-spacing: 0.12em; }
.notice { margin: 18px 0; }
.filter-row { display: flex; align-items: center; gap: 18px; margin: 24px 0; flex-wrap: wrap; }
.metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }
.metric, .panel {
  background: rgba(255,255,255,0.035);
  border: 1px solid rgba(255,255,255,0.09);
  border-radius: 14px;
  padding: 24px;
}
.metric-label { color: #a3a8b5; font-size: 13px; }
.metric-value { font-size: 26px; font-weight: 650; margin: 16px 0 10px; font-variant-numeric: tabular-nums; }
.metric-note { font-size: 12px; line-height: 1.6; }
.panel { margin-top: 22px; }
.panel-title { display: flex; justify-content: space-between; align-items: center; gap: 10px; }
.panel-title h2 { font-size: 19px; margin: 0; }
.panel-title p { font-size: 13px; }
.chart { min-height: 340px; width: 100%; }
.panel-foot, .warning { font-size: 12px; }
.warning { color: #f0b56e; }
.json-block { max-height: 220px; overflow: auto; padding: 12px; background: #15171b; border-radius: 8px; }
@media(max-width: 1000px) { .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media(max-width: 640px) {
  .headline { align-items: flex-start; }
  .metrics { grid-template-columns: 1fr; }
  .metric, .panel { padding: 16px; }
}
</style>
