<template>
  <div class="factor-eval-page">
    <header class="page-hero">
      <p class="eyebrow">Factor Research</p>
      <h1 class="page-title">因子评估</h1>
      <p class="subtitle">因子预测力、冗余度与市场状态敏感性分析</p>
    </header>

    <!-- Error State -->
    <el-alert
      v-if="error"
      class="page-alert"
      type="error"
      :title="error"
      show-icon
      :closable="false"
    />

    <!-- Section A: Reports List -->
    <section class="section">
      <div class="section-header">
        <h2>评估报告</h2>
        <el-button size="small" class="btn-refresh" :loading="loading" @click="fetchReports">
          刷新
        </el-button>
      </div>

      <div v-if="loading" class="loading-state">
        <el-skeleton :rows="5" animated />
      </div>

      <div v-else-if="!reports.length" class="empty-state">
        <el-empty description="暂无因子评估报告" :image-size="60" />
      </div>

      <el-table
        v-else
        :data="reports"
        highlight-current-row
        class="reports-table"
        @row-click="selectReport"
      >
        <el-table-column label="因子名称" min-width="160">
          <template #default="{ row }">
            <span class="factor-name">{{ row.factor_name }}</span>
          </template>
        </el-table-column>
        <el-table-column label="观测数" width="110" align="right">
          <template #default="{ row }">
            {{ row.observation_count?.toLocaleString() || '--' }}
          </template>
        </el-table-column>
        <el-table-column label="IC Mean (S20)" width="130" align="right">
          <template #default="{ row }">
            <span :class="icClass(row.ic_mean_score20)">
              {{ formatIc(row.ic_mean_score20) }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="ICIR (S20)" width="120" align="right">
          <template #default="{ row }">
            {{ formatNumber(row.icir_score20) }}
          </template>
        </el-table-column>
        <el-table-column label="创建时间" width="170">
          <template #default="{ row }">
            {{ formatDateTime(row.created_at) }}
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="statusType(row.status)" size="small">
              {{ statusLabel(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <!-- Section B: Report Detail -->
    <section v-if="selectedReport" class="section report-detail">
      <div class="section-header">
        <h2>报告详情 · {{ selectedReport.factor_name }}</h2>
        <span class="report-id">ID: {{ selectedReport.id }}</span>
      </div>

      <div v-if="detailLoading" class="loading-state">
        <el-skeleton :rows="8" animated />
      </div>

      <template v-else-if="reportDetail">
        <!-- IC Summary -->
        <div class="detail-card">
          <h3>IC 汇总（各周期）</h3>
          <el-table :data="icSummaryRows" size="small" empty-text="暂无数据">
            <el-table-column prop="horizon" label="周期" width="100" />
            <el-table-column label="IC Mean" align="right">
              <template #default="{ row }">
                <span :class="icClass(row.ic_mean)">{{ formatIc(row.ic_mean) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="IC Std" align="right">
              <template #default="{ row }">{{ formatNumber(row.ic_std) }}</template>
            </el-table-column>
            <el-table-column label="ICIR" align="right">
              <template #default="{ row }">{{ formatNumber(row.icir) }}</template>
            </el-table-column>
          </el-table>
        </div>

        <!-- Quintile Analysis -->
        <div class="detail-card" v-if="quintileRows.length">
          <h3>五分位分析</h3>
          <el-tabs v-model="quintileHorizon" class="detail-tabs">
            <el-tab-pane
              v-for="h in quintileHorizons"
              :key="h"
              :label="`Score${h}`"
              :name="h"
            >
              <el-table :data="quintileRowsForHorizon(h)" size="small" empty-text="暂无数据">
                <el-table-column prop="quintile" label="分位" width="80" />
                <el-table-column label="平均前向收益" align="right">
                  <template #default="{ row }">
                    <span :class="row.avg_return >= 0 ? 'positive' : 'negative'">
                      {{ formatPct(row.avg_return) }}
                    </span>
                  </template>
                </el-table-column>
              </el-table>
            </el-tab-pane>
          </el-tabs>
        </div>

        <!-- Correlation Matrix -->
        <div class="detail-card" v-if="reportDetail.correlation_matrix?.length">
          <h3>组件相关性矩阵</h3>
          <el-table :data="reportDetail.correlation_matrix" size="small" empty-text="暂无数据">
            <el-table-column prop="component" label="组件" min-width="180" />
            <el-table-column label="相关系数" align="right">
              <template #default="{ row }">
                <span :class="corrClass(row.corr)">{{ formatNumber(row.corr, 4) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </div>

        <!-- Decay Curve -->
        <div class="detail-card" v-if="reportDetail.decay_curve?.length">
          <h3>IC 衰减曲线</h3>
          <el-table :data="reportDetail.decay_curve" size="small" empty-text="暂无数据">
            <el-table-column prop="horizon" label="前向天数" width="120" />
            <el-table-column label="IC Mean" align="right">
              <template #default="{ row }">
                <span :class="icClass(row.ic_mean)">{{ formatIc(row.ic_mean) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </div>

        <!-- Regime IC -->
        <div class="detail-card" v-if="reportDetail.regime_ic">
          <h3>市场状态 IC 分解</h3>
          <el-table :data="regimeRows" size="small" empty-text="暂无数据">
            <el-table-column prop="regime" label="市场状态" width="120" />
            <el-table-column label="IC Mean" align="right">
              <template #default="{ row }">
                <span :class="icClass(row.ic_mean)">{{ formatIc(row.ic_mean) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="IC Std" align="right">
              <template #default="{ row }">{{ formatNumber(row.ic_std) }}</template>
            </el-table-column>
          </el-table>
        </div>
      </template>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import {
  factorEvalApi,
  type FactorEvalReportSummary,
  type FactorEvalReportDetail,
  type QuintileItem
} from '@/api/factorEval'

const loading = ref(false)
const error = ref('')
const reports = ref<FactorEvalReportSummary[]>([])
const selectedReport = ref<FactorEvalReportSummary | null>(null)
const reportDetail = ref<FactorEvalReportDetail | null>(null)
const detailLoading = ref(false)
const quintileHorizon = ref('5')

const quintileHorizons = computed(() => {
  if (!reportDetail.value?.quintile_analysis) return []
  return Object.keys(reportDetail.value.quintile_analysis).sort((a, b) => Number(a) - Number(b))
})

const icSummaryRows = computed(() => {
  if (!reportDetail.value?.ic_summary) return []
  const ic = reportDetail.value.ic_summary
  const icir = reportDetail.value.icir_summary || {}
  return Object.keys(ic).map(h => ({
    horizon: `Score${h}`,
    ic_mean: ic[h]!.ic_mean,
    ic_std: ic[h]!.ic_std,
    icir: icir[h] ?? null
  }))
})

const quintileRows = computed(() => {
  if (!reportDetail.value?.quintile_analysis) return []
  return Object.values(reportDetail.value.quintile_analysis).flat()
})

function quintileRowsForHorizon(h: string): QuintileItem[] {
  if (!reportDetail.value?.quintile_analysis) return []
  return reportDetail.value.quintile_analysis[h] || []
}

const regimeRows = computed(() => {
  if (!reportDetail.value?.regime_ic) return []
  const r = reportDetail.value.regime_ic
  // Flatten: regime_ic is {bull: {"5": {ic_mean, ic_std}, "20": {...}}, ...}
  // Pick Score20 as default display horizon
  const DEFAULT_H = '20'
  const extract = (regimeData: any) => {
    if (!regimeData) return { ic_mean: null, ic_std: null }
    const h = regimeData[DEFAULT_H]
    return {
      ic_mean: h?.ic_mean ?? null,
      ic_std: h?.ic_std ?? null,
    }
  }
  return [
    { regime: '牛市', ...extract(r.bull) },
    { regime: '熊市', ...extract(r.bear) },
    { regime: '震荡市', ...extract(r.sideways) },
  ].filter(row => row.ic_mean != null)
})

async function fetchReports() {
  loading.value = true
  error.value = ''
  try {
    const res = await factorEvalApi.getReports({ per_page: 50 })
    reports.value = res.data?.items || []
  } catch (e: any) {
    error.value = e?.response?.data?.message || '获取评估报告失败'
    console.error('Failed to fetch factor eval reports', e)
  } finally {
    loading.value = false
  }
}

async function selectReport(row: FactorEvalReportSummary) {
  if (!row.id) return
  selectedReport.value = row
  detailLoading.value = true
  try {
    const res = await factorEvalApi.getReport(row.id)
    reportDetail.value = res.data
  } catch (e: any) {
    error.value = e?.response?.data?.message || '获取报告详情失败'
    console.error('Failed to fetch report detail', e)
  } finally {
    detailLoading.value = false
  }
}

function formatIc(val: number | null | undefined): string {
  if (val == null) return '--'
  return val.toFixed(4)
}

function formatNumber(val: number | null | undefined, decimals = 2): string {
  if (val == null) return '--'
  return val.toFixed(decimals)
}

function formatPct(val: number | null | undefined): string {
  if (val == null) return '--'
  return (val * 100).toFixed(2) + '%'
}

function formatDateTime(val: string | null): string {
  if (!val) return '--'
  return val.replace('T', ' ').slice(0, 16)
}

function icClass(val: number | null | undefined): string {
  if (val == null) return ''
  if (val > 0.03) return 'positive'
  if (val < -0.03) return 'negative'
  return ''
}

function corrClass(val: number | null | undefined): string {
  if (val == null) return ''
  if (Math.abs(val) > 0.7) return 'negative'
  if (Math.abs(val) > 0.5) return 'warning'
  return ''
}

function statusLabel(val: string): string {
  return {
    COMPLETED: '已完成',
    RUNNING: '运行中',
    FAILED: '失败',
    PENDING: '待处理'
  }[val] || val
}

function statusType(val: string): string {
  if (val === 'COMPLETED') return 'success'
  if (val === 'RUNNING') return 'warning'
  if (val === 'FAILED') return 'danger'
  return 'info'
}

onMounted(fetchReports)
</script>

<style scoped lang="scss">
.factor-eval-page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 32px 24px;
  font-family: 'Inter Variable', Inter, sans-serif;
  color: #d0d6e0;
}

.page-hero {
  margin-bottom: 28px;
  .eyebrow {
    margin: 0 0 4px;
    font-size: 13px;
    font-weight: 510;
    color: #7170ff;
    text-transform: uppercase;
    letter-spacing: 0.1em;
  }
  .page-title {
    font-size: 40px;
    font-weight: 510;
    color: #f7f8f8;
    margin: 8px 0;
    letter-spacing: -0.88px;
    line-height: 1;
  }
  .subtitle {
    font-size: 15px;
    color: #8a8f98;
    margin: 0;
  }
}

.page-alert {
  margin-bottom: 20px;
}

.section {
  margin-bottom: 32px;

  .section-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;

    h2 {
      font-size: 18px;
      font-weight: 590;
      color: #f7f8f8;
      margin: 0;
    }

    .report-id {
      font-size: 12px;
      color: #62666d;
      font-family: 'Berkeley Mono', monospace;
    }
  }

  .btn-refresh {
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: #d0d6e0;
    &:hover {
      color: #f7f8f8;
      background: rgba(255, 255, 255, 0.06);
    }
  }

  .loading-state,
  .empty-state {
    padding: 24px 0;
    color: #62666d;
    font-size: 14px;
  }
}

.reports-table {
  cursor: pointer;
}

.factor-name {
  font-weight: 510;
  color: #f7f8f8;
}

.report-detail {
  border-top: 1px solid rgba(255, 255, 255, 0.05);
  padding-top: 28px;
}

.detail-card {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.06);
  border-radius: 12px;
  padding: 18px;
  margin-bottom: 16px;

  h3 {
    font-size: 15px;
    font-weight: 590;
    color: #f7f8f8;
    margin: 0 0 14px 0;
  }
}

.detail-tabs {
  margin-top: 0;
}

.positive {
  color: #10b981;
}

.negative {
  color: #ef4444;
}

.warning {
  color: #f59e0b;
}
</style>
