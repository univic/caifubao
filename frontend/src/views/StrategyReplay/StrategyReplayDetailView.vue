<template>
  <div class="strategy-replay-detail-page">
    <!-- Loading -->
    <div v-if="loading" class="loading-state">
      <el-skeleton :rows="10" animated />
    </div>

    <!-- Error -->
    <el-alert
      v-if="errorMessage"
      class="error-alert"
      type="error"
      :title="errorMessage"
      show-icon
      :closable="false"
    />

    <template v-if="replay">
      <!-- Hero -->
      <header class="page-hero">
        <div class="hero-top">
          <router-link to="/strategy-replays" class="back-link">
            <el-icon><ArrowLeft /></el-icon> 返回列表
          </router-link>
          <div class="hero-badges">
            <span class="replay-badge">REPLAY</span>
            <span class="forward-badge">
              前视 {{ replay.forward_sessions }}/{{ replay.forward_required }} · 不计入
            </span>
          </div>
        </div>
        <div class="hero-main">
          <h1 class="page-title">{{ replay.name || replay.replay_id }}</h1>
          <div class="hero-meta">
            <span class="meta-strategy">{{ strategyLabel(replay.strategy) }}</span>
            <span class="meta-divider">·</span>
            <span class="meta-date">
              {{ formatDate(replay.start_date) }} — {{ formatDate(replay.end_date) }}
            </span>
            <span class="meta-divider">·</span>
            <span>AUM {{ formatMoney(replay.aum) }}</span>
            <span class="meta-divider">·</span>
            <span>数据截至 {{ formatDate(replay.data_as_of) }}</span>
          </div>
          <p class="hero-id mono-text">{{ replay.replay_id }}</p>
          <p v-if="replay.description" class="hero-desc">{{ replay.description }}</p>
        </div>
        <div class="hero-actions">
          <el-button class="btn-ghost" :icon="Refresh" :loading="loading" @click="fetchReplay">
            刷新
          </el-button>
        </div>
      </header>

      <!-- Research disclaimer -->
      <div class="notice-panel">
        <div class="notice-head">
          <span class="notice-tag">{{ replay.research_grade || 'RESEARCH' }}</span>
          <span class="notice-title">研究级历史回放 · 非前瞻验证 · 不构成投资建议</span>
        </div>
        <p class="notice-text">{{ replay.disclaimer || fallbackDisclaimer }}</p>
      </div>

      <!-- Metrics -->
      <section class="section">
        <div class="section-header">
          <h2>核心指标</h2>
          <span class="section-sub">研究级重放，非实盘业绩</span>
        </div>
        <div class="metrics-grid">
          <div class="metric-card">
            <span class="metric-label">总收益</span>
            <span class="metric-value mono" :class="pnlClass(metrics.total_return)">
              {{ formatFractionPercent(metrics.total_return) }}
            </span>
            <span class="metric-sub muted">区间累计</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">年化收益</span>
            <span class="metric-value mono" :class="pnlClass(metrics.annualised)">
              {{ formatFractionPercent(metrics.annualised) }}
            </span>
            <span class="metric-sub muted">重放区间年化</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">夏普比率</span>
            <span class="metric-value mono">{{ formatNumber(metrics.sharpe) }}</span>
            <span class="metric-sub muted">未年化无风险利率</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">最大回撤</span>
            <span class="metric-value mono danger">
              {{ formatFractionPercent(metrics.max_drawdown) }}
            </span>
            <span class="metric-sub muted">峰谷最大跌幅</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">年化换手</span>
            <span class="metric-value mono">
              {{ formatFractionPercent(metrics.annual_turnover) }}
            </span>
            <span class="metric-sub muted">成交额 / 平均净值 / 年</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">总费用</span>
            <span class="metric-value mono">{{ formatMoney2(metrics.total_fees) }}</span>
            <span class="metric-sub muted">佣金 + 印花税 + 过户费</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">平均现金占比</span>
            <span class="metric-value mono">
              {{ formatFractionPercent(metrics.average_cash_share) }}
            </span>
            <span class="metric-sub muted">现金拖累参考</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">基准收益</span>
            <span class="metric-value mono" :class="pnlClass(metrics.benchmark_return)">
              {{ formatFractionPercent(metrics.benchmark_return) }}
            </span>
            <span class="metric-sub muted">同区间基准</span>
          </div>
        </div>
      </section>

      <!-- Limitations next to the metrics -->
      <section class="limitations-card">
        <div class="limitations-head">
          <h3>研究限制与证据边界</h3>
          <el-tag v-if="replay.survivorship_gap" type="warning" size="small">
            存在生存者偏差缺口（survivorship_gap）
          </el-tag>
          <el-tag v-else type="success" size="small">survivorship_gap=false</el-tag>
        </div>
        <ul v-if="replay.limitations.length" class="limitations-list">
          <li v-for="(item, index) in replay.limitations" :key="`${index}-${item}`">
            {{ item }}
          </li>
        </ul>
        <p v-else class="limitations-empty">未记录额外限制。</p>
        <p class="limitations-note">
          重放固定为 {{ replay.forward_sessions }}/{{ replay.forward_required }}，
          forward_eligible={{ replay.forward_eligible }}：历史回放不会推进前视进度，
          前视认证仍需独立的 {{ replay.forward_required }} 会话前视窗口。QDII 溢价、
          申购限额与规模约束可能未被完全建模，请结合限制清单解读指标。
        </p>
      </section>

      <!-- NAV / benchmark / drawdown chart -->
      <div class="content-card">
        <div class="card-header">
          <div>
            <h3 class="card-title">净值 · 基准 · 回撤</h3>
            <p class="card-desc">
              {{ replay.curve.length }} 个交易日 · NAV 以区间起点归一化为 1.0
            </p>
          </div>
        </div>
        <div v-if="!replay.curve.length" class="empty-inline">该重放没有净值曲线。</div>
        <div v-else ref="navChartRef" class="nav-chart"></div>
      </div>

      <!-- Decision timeline -->
      <div class="content-card">
        <div class="card-header">
          <div>
            <h3 class="card-title">决策时间线</h3>
            <p class="card-desc">
              共 {{ replay.decisions.length }} 次调仓 · 展开查看目标权重、成交与受阻订单
            </p>
          </div>
        </div>

        <div v-if="!replay.decisions.length" class="empty-inline">暂无决策记录。</div>
        <div v-else class="table-wrapper">
          <el-table
            :data="replay.decisions"
            row-key="decision_id"
            class="linear-table decision-table"
          >
            <el-table-column type="expand">
              <template #default="{ row }">
                <div class="decision-expand">
                  <div class="expand-block">
                    <h4>目标权重 <span class="muted">（sleeve 内权重）</span></h4>
                    <el-table
                      :data="row.target_holdings || []"
                      size="small"
                      class="linear-table nested-table"
                      empty-text="无目标持仓"
                    >
                      <el-table-column prop="code" label="代码" min-width="140">
                        <template #default="{ row: holding }">
                          <span class="mono-text">{{ holding.code }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="目标权重" align="right" width="140">
                        <template #default="{ row: holding }">
                          <span class="mono-text">{{ formatWeight(holding.weight) }}</span>
                        </template>
                      </el-table-column>
                    </el-table>
                  </div>

                  <div class="expand-block">
                    <h4>成交 · {{ fillsForDecision(row.decision_id).length }} 笔</h4>
                    <el-table
                      :data="fillsForDecision(row.decision_id)"
                      size="small"
                      class="linear-table nested-table"
                      empty-text="无成交记录"
                    >
                      <el-table-column label="执行日" width="120">
                        <template #default="{ row: fill }">
                          <span class="mono-text">{{ formatDate(fill.date) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column prop="code" label="代码" min-width="120">
                        <template #default="{ row: fill }">
                          <span class="mono-text">{{ fill.code }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="方向" width="80">
                        <template #default="{ row: fill }">
                          <span class="side-tag" :class="sideClass(fill.side)">
                            {{ sideLabel(fill.side) }}
                          </span>
                        </template>
                      </el-table-column>
                      <el-table-column label="Sleeve" width="90">
                        <template #default="{ row: fill }">
                          <span class="code-chip">{{ sleeveLabel(fill.sleeve) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="数量" width="100" align="right">
                        <template #default="{ row: fill }">
                          <span class="mono-text">{{ formatFillQuantity(fill) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="价格" width="100" align="right">
                        <template #default="{ row: fill }">
                          <span class="mono-text">{{ formatFillPrice(fill) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="佣金" width="90" align="right">
                        <template #default="{ row: fill }">
                          <span class="mono-text">{{ formatMoney2(fill.commission) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="印花税" width="90" align="right">
                        <template #default="{ row: fill }">
                          <span class="mono-text">{{ formatMoney2(fill.stamp_duty) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="过户费" width="90" align="right">
                        <template #default="{ row: fill }">
                          <span class="mono-text">{{ formatMoney2(fill.transfer_fee) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="原因" min-width="140">
                        <template #default="{ row: fill }">
                          <span class="reason-text">{{ fill.reason || '—' }}</span>
                        </template>
                      </el-table-column>
                    </el-table>
                  </div>

                  <div class="expand-block">
                    <h4>受阻订单 · {{ blockedForDecision(row.decision_id).length }} 笔</h4>
                    <el-table
                      v-if="blockedForDecision(row.decision_id).length"
                      :data="blockedForDecision(row.decision_id)"
                      size="small"
                      class="linear-table nested-table"
                    >
                      <el-table-column label="日期" width="120">
                        <template #default="{ row: order }">
                          <span class="mono-text">{{ formatDate(order.date) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column prop="code" label="代码" min-width="120">
                        <template #default="{ row: order }">
                          <span class="mono-text">{{ order.code }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="方向" width="80">
                        <template #default="{ row: order }">
                          <span class="side-tag" :class="sideClass(order.side)">
                            {{ sideLabel(order.side) }}
                          </span>
                        </template>
                      </el-table-column>
                      <el-table-column label="原因" min-width="220">
                        <template #default="{ row: order }">
                          <span class="reason-text">{{ order.reason }}</span>
                        </template>
                      </el-table-column>
                    </el-table>
                    <p v-else class="expand-empty">无受阻订单。</p>
                  </div>
                </div>
              </template>
            </el-table-column>

            <el-table-column label="决策日" width="120">
              <template #default="{ row }">
                <span class="mono-text">{{ formatDate(row.date) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="执行日" width="120">
              <template #default="{ row }">
                <span class="mono-text">{{ formatDate(row.execution_date) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="Sleeve" width="96">
              <template #default="{ row }">
                <span class="code-chip">{{ sleeveLabel(row.sleeve) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="目标持仓" min-width="240">
              <template #default="{ row }">
                <div class="holding-list">
                  <span
                    v-for="holding in sortedHoldings(row)"
                    :key="holding.code"
                    class="code-chip"
                  >
                    {{ holding.code }} {{ formatWeight(holding.weight) }}
                  </span>
                  <span v-if="!row.target_holdings?.length" class="muted">—</span>
                </div>
              </template>
            </el-table-column>

            <el-table-column label="新增" min-width="150">
              <template #default="{ row }">
                <div class="holding-list">
                  <span v-for="code in row.added || []" :key="`add-${code}`" class="code-chip add">
                    {{ code }}
                  </span>
                  <span v-if="!row.added?.length" class="muted">—</span>
                </div>
              </template>
            </el-table-column>

            <el-table-column label="移除" min-width="150">
              <template #default="{ row }">
                <div class="holding-list">
                  <span v-for="code in row.removed || []" :key="`rm-${code}`" class="code-chip remove">
                    {{ code }}
                  </span>
                  <span v-if="!row.removed?.length" class="muted">—</span>
                </div>
              </template>
            </el-table-column>

            <el-table-column label="现金 before → after" width="180" align="right">
              <template #default="{ row }">
                <span class="mono-text">
                  {{ formatMoney2(row.cash_before) }} → {{ formatMoney2(row.cash_after) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column label="估算成本" width="120" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatMoney2(row.estimated_cost) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>

      <!-- Config -->
      <div class="content-card">
        <div class="card-header">
          <div>
            <h3 class="card-title">策略配置</h3>
            <p class="card-desc">本次重放使用的已验证配置快照</p>
          </div>
        </div>
        <div v-if="configEntries.length" class="config-grid">
          <div v-for="entry in configEntries" :key="entry[0]" class="config-item">
            <span class="config-label">{{ entry[0] }}</span>
            <span class="config-value mono-text">{{ formatConfigValue(entry[1]) }}</span>
          </div>
        </div>
        <div v-else class="empty-inline">该重放没有配置快照。</div>
      </div>

      <!-- Artifact metadata -->
      <div class="meta-card">
        <div class="meta-item">
          <span class="meta-label">replay_id</span>
          <span class="meta-value mono-text small">{{ replay.replay_id }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">schema_version</span>
          <span class="meta-value mono-text small">{{ replay.schema_version || '--' }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">source_artifact</span>
          <span class="meta-value mono-text small">{{ replay.source_artifact || '--' }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">evidence_kind</span>
          <span class="meta-value mono-text small">{{ replay.evidence_kind }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">research_grade</span>
          <span class="meta-value mono-text small">{{ replay.research_grade }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">input_hash</span>
          <span class="meta-value mono-text small">{{ replay.input_hash || '--' }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">config_hash</span>
          <span class="meta-value mono-text small">{{ replay.config_hash || '--' }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">content_hash</span>
          <span class="meta-value mono-text small">{{ replay.content_hash || '--' }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">generated_at</span>
          <span class="meta-value mono-text small">{{ formatDateTime(replay.generated_at) }}</span>
        </div>
      </div>
    </template>

    <!-- Not Found -->
    <el-empty
      v-if="!loading && !errorMessage && !replay"
      description="未找到该策略重放"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ArrowLeft, Refresh } from '@element-plus/icons-vue'
import * as echarts from 'echarts'
import {
  getStrategyReplay,
  type StrategyReplayDecision,
  type StrategyReplayDetail,
  type StrategyReplayFill,
  type StrategyReplayBlockedOrder,
  type StrategyReplayMetrics
} from '@/api/strategyReplays'

const route = useRoute()
const loading = ref(false)
const errorMessage = ref('')
const replay = ref<StrategyReplayDetail | null>(null)
const navChartRef = ref<HTMLElement | null>(null)
let navChart: echarts.ECharts | null = null

const fallbackDisclaimer =
  '研究记录，不构成投资建议。历史回放不是前瞻验证，不能作为上钱依据。'

const metrics = computed<Partial<StrategyReplayMetrics>>(
  () => replay.value?.metrics || {}
)

const configEntries = computed(() =>
  Object.entries(replay.value?.config || {}) as Array<[string, unknown]>
)

const fillsByDecision = computed(() => {
  const map = new Map<string, StrategyReplayFill[]>()
  for (const fill of replay.value?.fills || []) {
    const key = fill.decision_id
    const bucket = map.get(key)
    if (bucket) bucket.push(fill)
    else map.set(key, [fill])
  }
  return map
})

const blockedByDecision = computed(() => {
  const map = new Map<string, StrategyReplayBlockedOrder[]>()
  for (const order of replay.value?.blocked_orders || []) {
    const key = order.decision_id
    const bucket = map.get(key)
    if (bucket) bucket.push(order)
    else map.set(key, [order])
  }
  return map
})

function fillsForDecision(decisionId: string): StrategyReplayFill[] {
  return fillsByDecision.value.get(decisionId) || []
}

function blockedForDecision(decisionId: string): StrategyReplayBlockedOrder[] {
  return blockedByDecision.value.get(decisionId) || []
}

function sortedHoldings(row: StrategyReplayDecision) {
  return [...(row.target_holdings || [])].sort((a, b) => b.weight - a.weight)
}

function formatDate(value: string | null | undefined) {
  if (!value) return '--'
  return value.slice(0, 10)
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '--'
  return value.replace('T', ' ').slice(0, 19)
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toFixed(2)
}

function formatMoney(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `¥${value.toLocaleString('zh-CN', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  })}`
}

function formatMoney2(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `¥${value.toFixed(2)}`
}

function formatQuantity(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function formatPrice(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toFixed(3)
}

/** Replay metrics are decimal fractions (0.62 => 62.00%). */
function formatFractionPercent(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `${(value * 100).toFixed(2)}%`
}

function formatWeight(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `${(value * 100).toFixed(1)}%`
}

function pnlClass(value: number | null | undefined) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return ''
  if (numeric > 0) return 'positive'
  if (numeric < 0) return 'negative'
  return ''
}

function sideLabel(value: string) {
  if (value === 'BUY') return '买入'
  if (value === 'SELL') return '卖出'
  if (value === 'TRANSFER') return '跨仓'
  return value
}

function sideClass(value: string) {
  if (value === 'BUY') return 'buy'
  if (value === 'SELL') return 'sell'
  if (value === 'TRANSFER') return 'transfer'
  return ''
}

function sleeveLabel(value: string | undefined) {
  if (value === 'core') return '核心'
  if (value === 'satellite') return '卫星'
  if (value === 'sleeve') return '跨仓'
  return value || '—'
}

function formatFillQuantity(fill: StrategyReplayFill) {
  if (fill.side === 'TRANSFER') return '—'
  return formatQuantity(fill.quantity)
}

function formatFillPrice(fill: StrategyReplayFill) {
  if (fill.side === 'TRANSFER') return formatMoney2(fill.price)
  return formatPrice(fill.price)
}

function strategyLabel(value: string) {
  const map: Record<string, string> = {
    riq_v1: 'RIQ v1（冻结）',
    MA_CROSS: '均线交叉',
    BUY_HOLD: '买入持有',
    SCORE_THRESHOLD: '评分阈值',
    SCORE_MOMENTUM: '评分动量',
    TOP_N_ROTATION: 'Top-N 轮动',
    BALANCED_CORE_SATELLITE: '平衡核心-卫星',
    core_satellite: '平衡核心-卫星'
  }
  return map[value] || value
}

function formatConfigValue(value: unknown): string {
  if (value === null || value === undefined) return '--'
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  if (typeof value === 'string') return value
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function renderNavChart() {
  const artifact = replay.value
  const element = navChartRef.value
  if (!artifact || !element || !artifact.curve.length) return

  if (navChart) {
    navChart.dispose()
    navChart = null
  }

  const dates = artifact.curve.map((point) => formatDate(point.date))
  const rawNav = artifact.curve.map((point) =>
    point.nav === null || point.nav === undefined ? null : Number(point.nav)
  )
  const rawBenchmark = artifact.curve.map((point) =>
    point.benchmark_nav === null || point.benchmark_nav === undefined
      ? null
      : Number(point.benchmark_nav)
  )
  const navBase = rawNav.find((value) => value !== null && value > 0) || 1
  const benchmarkBase = rawBenchmark.find((value) => value !== null && value > 0) || 1
  const nav = rawNav.map((value) => (value === null ? null : value / navBase))
  const benchmark = rawBenchmark.map((value) =>
    value === null ? null : value / benchmarkBase
  )
  const drawdown = artifact.curve.map((point) =>
    point.drawdown === null || point.drawdown === undefined
      ? null
      : Number((point.drawdown * 100).toFixed(4))
  )

  navChart = echarts.init(element)
  navChart.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: { left: 56, right: 64, top: 48, bottom: 76 },
    tooltip: { trigger: 'axis' },
    legend: {
      data: ['策略 NAV', '基准 NAV', '策略回撤'],
      textStyle: { color: '#d0d6e0' },
      top: 0
    },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0 },
      {
        type: 'slider',
        xAxisIndex: 0,
        height: 18,
        bottom: 14,
        borderColor: 'rgba(255,255,255,0.08)',
        backgroundColor: 'rgba(255,255,255,0.02)',
        fillerColor: 'rgba(113,112,255,0.16)',
        handleStyle: { color: '#7170ff' },
        textStyle: { color: '#8a8f98' }
      }
    ],
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: { color: '#8a8f98' },
      axisLine: { lineStyle: { color: 'rgba(255,255,255,0.15)' } }
    },
    yAxis: [
      {
        type: 'value',
        name: 'NAV',
        scale: true,
        nameTextStyle: { color: '#8a8f98' },
        axisLabel: { color: '#8a8f98' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } }
      },
      {
        type: 'value',
        name: '回撤 %',
        nameTextStyle: { color: '#8a8f98' },
        axisLabel: { color: '#8a8f98', formatter: '{value}%' },
        splitLine: { show: false }
      }
    ],
    series: [
      {
        name: '策略 NAV',
        type: 'line',
        data: nav,
        showSymbol: false,
        lineStyle: { width: 2, color: '#7170ff' },
        areaStyle: { color: 'rgba(113,112,255,0.08)' }
      },
      {
        name: '基准 NAV',
        type: 'line',
        data: benchmark,
        showSymbol: false,
        lineStyle: { width: 1.5, color: '#8a8f98', type: 'dashed' }
      },
      {
        name: '策略回撤',
        type: 'line',
        yAxisIndex: 1,
        data: drawdown,
        showSymbol: false,
        lineStyle: { width: 1, color: '#fb7185' },
        areaStyle: { color: 'rgba(251,113,133,0.12)' }
      }
    ]
  })
}

function handleResize() {
  navChart?.resize()
}

function disposeNavChart() {
  if (navChart) {
    navChart.dispose()
    navChart = null
  }
}

async function fetchReplay() {
  const replayId = String(route.params.replayId || '')
  if (!replayId) {
    errorMessage.value = '缺少重放 ID。'
    return
  }
  loading.value = true
  errorMessage.value = ''
  disposeNavChart()
  try {
    replay.value = await getStrategyReplay(replayId)
    await nextTick()
    renderNavChart()
  } catch (error: any) {
    console.error(error)
    replay.value = null
    disposeNavChart()
    if (error?.response?.status === 404) {
      errorMessage.value = '该策略重放不存在，或已被移除。'
    } else {
      errorMessage.value = '策略重放详情加载失败，请稍后重试。'
    }
  } finally {
    loading.value = false
  }
}

watch(
  () => route.params.replayId,
  () => {
    fetchReplay()
  }
)

onMounted(() => {
  fetchReplay()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (navChart) {
    navChart.dispose()
    navChart = null
  }
})
</script>

<style scoped lang="scss">
.strategy-replay-detail-page {
  --color-bg: #08090a;
  --color-panel: #0f1011;
  --color-surface: #191a1b;
  --color-brand: #5e6ad2;
  --color-brand-accent: #7170ff;
  --color-text-primary: #f7f8f8;
  --color-text-secondary: #d0d6e0;
  --color-text-tertiary: #8a8f98;
  --color-text-quaternary: #62666d;
  --color-border: rgba(255, 255, 255, 0.08);
  --color-border-subtle: rgba(255, 255, 255, 0.05);
  --font-inter: 'Inter Variable', Inter, sans-serif;
  --font-mono: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace;

  min-height: 100vh;
  padding: 40px 60px;
  background-color: var(--color-bg);
  color: var(--color-text-primary);
  font-family: var(--font-inter);
  font-feature-settings: "cv01", "ss03";
}

/* Loading */
.loading-state {
  padding: 40px 0;
}

/* Buttons */
:deep(.el-button) {
  height: 36px;
  padding: 0 16px;
  border-radius: 6px;
  font-weight: 510;
  font-size: 14px;
  transition: all 0.2s;
}

.btn-ghost {
  background: rgba(255, 255, 255, 0.02) !important;
  border: 1px solid var(--color-border) !important;
  color: var(--color-text-secondary) !important;

  &:hover {
    background: rgba(255, 255, 255, 0.05) !important;
    border-color: var(--color-text-tertiary) !important;
  }
}

/* Alerts */
.error-alert {
  margin-bottom: 24px;
  background-color: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.2);
  color: #fb7185;
}

/* Hero */
.page-hero {
  margin-bottom: 24px;
}

.hero-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  margin-bottom: 20px;
}

.back-link {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 14px;
  font-weight: 510;
  color: var(--color-text-tertiary);
  text-decoration: none;

  &:hover {
    color: var(--color-text-primary);
  }
}

.hero-badges {
  display: flex;
  align-items: center;
  gap: 8px;
}

.replay-badge {
  font-size: 10px;
  font-weight: 590;
  letter-spacing: 0.08em;
  padding: 3px 8px;
  border-radius: 4px;
  color: #a9a8ff;
  background: rgba(113, 112, 255, 0.14);
  border: 1px solid rgba(113, 112, 255, 0.28);
}

.forward-badge {
  font-family: var(--font-mono);
  font-size: 11px;
  padding: 3px 10px;
  border-radius: 999px;
  color: var(--color-text-tertiary);
  border: 1px solid var(--color-border);
  background: rgba(255, 255, 255, 0.03);
}

.hero-main {
  margin-bottom: 8px;
}

.page-title {
  font-size: 36px;
  font-weight: 510;
  line-height: 1.15;
  letter-spacing: -0.792px;
  margin: 0 0 12px 0;
  color: var(--color-text-primary);
}

.hero-meta {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  font-size: 15px;
  color: var(--color-text-tertiary);

  .meta-strategy {
    font-weight: 510;
    color: var(--color-text-secondary);
  }

  .meta-divider {
    color: var(--color-text-quaternary);
  }

  .meta-date {
    font-family: var(--font-mono);
    font-size: 14px;
  }
}

.hero-id {
  margin: 10px 0 0;
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--color-text-quaternary);
}

.hero-desc {
  margin: 8px 0 0;
  max-width: 760px;
  font-size: 14px;
  line-height: 1.6;
  color: var(--color-text-tertiary);
}

.hero-actions {
  display: flex;
  gap: 12px;
  margin-top: 16px;
}

/* Research disclaimer */
.notice-panel {
  background:
    linear-gradient(90deg, rgba(113, 112, 255, 0.08), rgba(113, 112, 255, 0.02)),
    var(--color-panel);
  border: 1px solid rgba(113, 112, 255, 0.2);
  border-radius: 12px;
  padding: 16px 20px;
  margin-bottom: 28px;
}

.notice-head {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.notice-tag {
  font-family: var(--font-mono);
  font-size: 10px;
  letter-spacing: 0.08em;
  padding: 2px 8px;
  border-radius: 4px;
  color: #a9a8ff;
  background: rgba(113, 112, 255, 0.16);
  border: 1px solid rgba(113, 112, 255, 0.3);
}

.notice-title {
  font-size: 14px;
  font-weight: 590;
  color: var(--color-text-primary);
}

.notice-text {
  margin: 8px 0 0;
  font-size: 13px;
  line-height: 1.7;
  color: var(--color-text-tertiary);
}

/* Sections */
.section {
  margin-bottom: 8px;
}

.section-header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;

  h2 {
    margin: 0;
    font-size: 20px;
    font-weight: 590;
    letter-spacing: -0.24px;
  }
}

.section-sub {
  font-size: 12px;
  color: var(--color-text-quaternary);
}

/* Metrics Grid */
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin-bottom: 24px;

  @media (max-width: 1200px) {
    grid-template-columns: repeat(2, 1fr);
  }

  @media (max-width: 640px) {
    grid-template-columns: 1fr;
  }
}

.metric-card {
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.metric-label {
  font-size: 12px;
  font-weight: 510;
  color: var(--color-text-quaternary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.metric-value {
  font-size: 26px;
  font-weight: 590;
  color: var(--color-text-primary);
  line-height: 1.2;

  &.mono {
    font-family: var(--font-mono);
  }

  &.positive { color: #ef4444; }
  &.negative { color: #22c55e; }
  &.danger { color: #fb7185; }
}

.metric-sub {
  font-size: 12px;
  color: var(--color-text-tertiary);

  &.positive { color: #ef4444; }
  &.negative { color: #22c55e; }
  &.muted { color: var(--color-text-quaternary); }
}

/* Limitations */
.limitations-card {
  background: rgba(251, 191, 36, 0.04);
  border: 1px solid rgba(251, 191, 36, 0.18);
  border-radius: 12px;
  padding: 20px 24px;
  margin-bottom: 24px;
}

.limitations-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;

  h3 {
    margin: 0;
    font-size: 16px;
    font-weight: 590;
  }
}

.limitations-list {
  margin: 12px 0 0;
  padding-left: 20px;
  color: var(--color-text-secondary);
  font-size: 13px;
  line-height: 1.8;

  li::marker {
    color: #fbbf24;
  }
}

.limitations-empty {
  margin: 12px 0 0;
  font-size: 13px;
  color: var(--color-text-tertiary);
}

.limitations-note {
  margin: 14px 0 0;
  padding-top: 12px;
  border-top: 1px solid rgba(251, 191, 36, 0.14);
  font-size: 12px;
  line-height: 1.8;
  color: var(--color-text-tertiary);
}

/* Content Card */
.content-card {
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  overflow: hidden;
  margin-bottom: 24px;
}

.card-header {
  padding: 24px 32px;
  border-bottom: 1px solid var(--color-border-subtle);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card-title {
  font-size: 20px;
  font-weight: 590;
  letter-spacing: -0.24px;
  margin: 0 0 4px 0;
}

.card-desc {
  font-size: 14px;
  color: var(--color-text-tertiary);
  margin: 0;
}

.nav-chart {
  width: 100%;
  height: 460px;
  padding: 8px 12px 12px;
  box-sizing: border-box;
}

.empty-inline {
  padding: 28px 32px;
  font-size: 13px;
  color: var(--color-text-quaternary);
}

/* Table Overrides */
.table-wrapper {
  padding: 0 12px 12px;
}

:deep(.linear-table) {
  background: transparent !important;
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: transparent;
  --el-table-border-color: var(--color-border-subtle);
  --el-table-text-color: var(--color-text-secondary);
  --el-table-header-text-color: var(--color-text-tertiary);

  &::before { display: none; }

  th.el-table__cell {
    font-size: 12px;
    font-weight: 510;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 16px 8px;
  }

  td.el-table__cell {
    padding: 12px 8px;
    border-bottom: 1px solid var(--color-border-subtle);
  }

  .el-table__row:hover > td {
    background-color: rgba(255, 255, 255, 0.02) !important;
  }
}

.decision-table {
  :deep(.el-table__expanded-cell) {
    background: rgba(255, 255, 255, 0.015);
    padding: 8px 20px 20px;
  }
}

.mono-text {
  font-family: var(--font-mono);
  font-size: 13px;

  &.small {
    font-size: 12px;
    color: var(--color-text-quaternary);
  }
}

.return-value {
  font-family: var(--font-mono);
  font-size: 14px;
  font-weight: 590;

  &.positive { color: #ef4444; }
  &.negative { color: #22c55e; }
}

.danger-text {
  color: #fb7185;
}

.muted {
  color: var(--color-text-quaternary);
}

/* Decision cells */
.holding-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.code-chip {
  font-family: var(--font-mono);
  font-size: 11px;
  padding: 2px 7px;
  border-radius: 4px;
  color: var(--color-text-secondary);
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid var(--color-border-subtle);

  &.add {
    color: #fca5a5;
    background: rgba(239, 68, 68, 0.08);
    border-color: rgba(239, 68, 68, 0.2);
  }

  &.remove {
    color: #86efac;
    background: rgba(34, 197, 94, 0.08);
    border-color: rgba(34, 197, 94, 0.2);
  }
}

/* Expanded decision */
.decision-expand {
  display: flex;
  flex-direction: column;
  gap: 20px;
  padding: 12px 4px 4px;
}

.expand-block {
  h4 {
    margin: 0 0 8px;
    font-size: 13px;
    font-weight: 590;
    color: var(--color-text-secondary);
  }
}

.nested-table {
  --el-table-header-bg-color: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--color-border-subtle);
  border-radius: 8px;
  overflow: hidden;
}

.expand-empty {
  margin: 0;
  font-size: 13px;
  color: var(--color-text-quaternary);
}

.reason-text {
  font-size: 12px;
  color: var(--color-text-tertiary);
}

/* Side Tags */
.side-tag {
  font-size: 11px;
  font-weight: 510;
  padding: 2px 8px;
  border-radius: 4px;

  &.buy {
    color: #ef4444;
    background: rgba(239, 68, 68, 0.1);
  }

  &.sell {
    color: #22c55e;
    background: rgba(34, 197, 94, 0.1);
  }

  &.transfer {
    color: #a9a8ff;
    background: rgba(113, 112, 255, 0.12);
  }
}

/* Config */
.config-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 1px;
  background: var(--color-border-subtle);

  @media (max-width: 1024px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  @media (max-width: 640px) {
    grid-template-columns: 1fr;
  }
}

.config-item {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 16px 24px;
  background: var(--color-panel);
}

.config-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-quaternary);
}

.config-value {
  font-size: 13px;
  color: var(--color-text-secondary);
  word-break: break-word;
}

/* Meta Card */
.meta-card {
  display: flex;
  gap: 32px;
  padding: 20px 24px;
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  margin-bottom: 24px;
  flex-wrap: wrap;
}

.meta-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-width: 360px;
}

.meta-label {
  font-size: 11px;
  color: var(--color-text-quaternary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.meta-value {
  font-size: 13px;
  color: var(--color-text-secondary);
  word-break: break-all;
}

@media (max-width: 1024px) {
  .strategy-replay-detail-page {
    padding: 24px;
  }

  .page-title {
    font-size: 28px;
  }

  .card-header {
    padding: 20px 24px;
  }
}
</style>
