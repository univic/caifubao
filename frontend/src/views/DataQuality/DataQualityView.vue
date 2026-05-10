<template>
  <div class="data-quality-page">
    <section class="quality-hero">
      <div>
        <p class="eyebrow">Data Quality</p>
        <h1>数据质量</h1>
        <p class="hero-desc">
          检查行情、后复权和 MA 因子的 freshness 对齐情况，快速判断演示前数据链路是否健康。
        </p>
      </div>
      <el-button type="primary" :loading="loading" @click="loadData">重新检查</el-button>
    </section>

    <el-skeleton v-if="loading && !summary" :rows="8" animated />

    <template v-else>
      <el-alert
        v-if="error"
        type="error"
        :title="error"
        show-icon
        :closable="false"
      />

      <section v-if="summary" class="summary-grid">
        <el-card class="status-card" shadow="hover">
          <div class="status-main">
            <span class="status-dot" :class="summary.status.toLowerCase()"></span>
            <div>
              <div class="label">整体状态</div>
              <div class="value">{{ statusLabel(summary.status) }}</div>
            </div>
          </div>
          <p>最新行情日期：{{ formatDate(summary.latest_quote_date) }}</p>
          <p>
            检测样本：{{ summary.scope.effective_total }} / {{ summary.scope.total_active }}
            <span v-if="summary.scope.excluded_unsupported">
              ，已排除 {{ summary.scope.excluded_unsupported }} 只暂不支持标的
            </span>
          </p>
          <p>生成时间：{{ formatDateTime(summary.generated_at) }}</p>
        </el-card>

        <el-card
          v-for="card in coverageCards"
          :key="card.key"
          class="metric-card"
          shadow="hover"
        >
          <template v-if="card.coverage">
            <div class="label">{{ card.title }}</div>
            <div class="metric-value">{{ card.coverage.ok_rate.toFixed(2) }}%</div>
            <el-progress
              :percentage="card.coverage.ok_rate"
              :status="progressStatus(card.coverage.ok_rate)"
              :stroke-width="10"
            />
            <div class="metric-footer">
              <span>OK {{ card.coverage.ok }}</span>
              <span>异常 {{ abnormalCount(card.coverage) }}</span>
              <span v-if="card.coverage.blocked">阻塞 {{ card.coverage.blocked }}</span>
            </div>
          </template>
          <template v-if="card.industry">
            <div class="label">{{ card.title }}</div>
            <div class="metric-value">{{ card.industry.total_classified }}</div>
            <el-progress
              :percentage="(card.industry.total_classified / Math.max(summary?.scope.effective_total || 1, 1) * 100)"
              :status="card.industry.total_classified > 0 ? 'success' : 'exception'"
              :stroke-width="10"
            />
            <div class="metric-footer">
              <span>{{ card.industry.industry_count }} 个申万行业</span>
              <span v-if="card.industry.last_sync">最后更新 {{ formatDate(card.industry.last_sync) }}</span>
              <span v-else class="text-dim">未同步</span>
            </div>
          </template>
        </el-card>
      </section>

      <section class="toolbar-card">
        <div class="toolbar-left">
          <el-radio-group v-model="filters.status" size="small" @change="handleFilterChange">
            <el-radio-button label="abnormal">异常</el-radio-button>
            <el-radio-button label="all">全部</el-radio-button>
            <el-radio-button label="ok">正常</el-radio-button>
            <el-radio-button label="stale">过期</el-radio-button>
            <el-radio-button label="missing">缺失</el-radio-button>
            <el-radio-button label="ahead">超前</el-radio-button>
          </el-radio-group>
        </div>
        <div class="toolbar-right">
          <el-input
            v-model="filters.q"
            clearable
            placeholder="搜索代码或名称"
            style="width: 220px"
            @keyup.enter="handleFilterChange"
            @clear="handleFilterChange"
          />
          <el-button :loading="itemsLoading" @click="handleFilterChange">查询</el-button>
        </div>
      </section>

      <el-card class="table-card" shadow="hover">
        <template #header>
          <div class="table-header">
            <div>
              <strong>Freshness 明细</strong>
              <p>默认优先展示异常项，便于 demo 前快速排查。</p>
            </div>
            <span class="table-count">共 {{ total }} 条</span>
          </div>
        </template>

        <el-table
          :data="items"
          v-loading="itemsLoading"
          stripe
          class="data-quality-table"
          style="width: 100%"
        >
          <el-table-column prop="code" label="代码" width="110" fixed />
          <el-table-column prop="name" label="名称" width="130" />
          <el-table-column label="状态" width="110">
            <template #default="{ row }">
              <el-tag :type="itemTagType(row.status)" effect="dark">
                {{ itemStatusLabel(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="Quote" width="120">
            <template #default="{ row }">{{ formatDate(row.quote_date) }}</template>
          </el-table-column>
          <el-table-column label="FQ" width="120">
            <template #default="{ row }">
              <span :class="factorStatusClass(row.fq_factor_status)">
                {{ formatFactorDate(row.fq_factor_date, row.fq_factor_status) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column
            v-for="factor in maFactors"
            :key="factor"
            :label="factor"
            width="120"
          >
            <template #default="{ row }">
              <span :class="maStatusClass(row.ma_statuses[factor])">
                {{ formatFactorDate(row.ma_dates[factor], row.ma_statuses[factor]) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="问题" min-width="240">
            <template #default="{ row }">
              <div class="issue-list">
                <el-tag
                  v-for="issue in row.issues.slice(0, 3)"
                  :key="issue"
                  size="small"
                  type="warning"
                  effect="plain"
                >
                  {{ issue }}
                </el-tag>
                <span v-if="!row.issues.length" class="muted">无</span>
                <span v-else-if="row.issues.length > 3" class="muted">
                  +{{ row.issues.length - 3 }}
                </span>
              </div>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination">
          <el-pagination
            layout="prev, pager, next"
            :total="total"
            :page-size="pageSize"
            :current-page="currentPage"
            @current-change="handlePageChange"
          />
        </div>
      </el-card>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { dataQualityApi, type DataQualityCoverage, type DataQualityItem, type DataQualitySummary } from '@/api/dataQuality'

const maFactors = ['MA_10', 'MA_20', 'MA_30', 'MA_60', 'MA_120']
const pageSize = 50
const displayTimezone = 'Asia/Shanghai'

const loading = ref(false)
const itemsLoading = ref(false)
const error = ref('')
const summary = ref<DataQualitySummary | null>(null)
const items = ref<DataQualityItem[]>([])
const total = ref(0)
const currentPage = ref(1)
const filters = reactive({
  status: 'abnormal' as 'all' | 'abnormal' | 'ok' | 'stale' | 'missing' | 'ahead',
  q: ''
})

const coverageCards = computed(() => {
  if (!summary.value) return []
  return [
    { key: 'overall', title: '整体覆盖率', coverage: summary.value.coverage.overall },
    { key: 'quote', title: '行情覆盖率', coverage: summary.value.coverage.quote },
    { key: 'fq', title: 'FQ 覆盖率', coverage: summary.value.coverage.fq_factor },
    { key: 'ma', title: 'MA 覆盖率', coverage: summary.value.coverage.ma_factor },
    { key: 'industry', title: '行业覆盖', coverage: null, industry: summary.value.coverage.industry }
  ]
})

onMounted(() => {
  void loadData()
})

async function loadData() {
  loading.value = true
  error.value = ''
  try {
    summary.value = await dataQualityApi.getSummary()
    await loadItems(1)
  } catch (err: any) {
    error.value = err?.response?.data?.message || '数据质量加载失败'
  } finally {
    loading.value = false
  }
}

async function loadItems(page = currentPage.value) {
  itemsLoading.value = true
  try {
    const res = await dataQualityApi.getItems({
      status: filters.status,
      q: filters.q.trim() || undefined,
      limit: pageSize,
      offset: (page - 1) * pageSize
    })
    items.value = res.items
    total.value = res.total
    currentPage.value = page
  } finally {
    itemsLoading.value = false
  }
}

function handleFilterChange() {
  void loadItems(1)
}

function handlePageChange(page: number) {
  void loadItems(page)
}

function abnormalCount(coverage: DataQualityCoverage) {
  return coverage.missing + coverage.stale + coverage.ahead
}

function progressStatus(value: number) {
  if (value >= 98) return 'success'
  if (value >= 90) return 'warning'
  return 'exception'
}

function statusLabel(value: string) {
  const map: Record<string, string> = {
    OK: '正常',
    WARN: '部分异常',
    ERROR: '严重异常'
  }
  return map[value] || value
}

function itemStatusLabel(value: string) {
  const map: Record<string, string> = {
    OK: '正常',
    STALE: '过期',
    MISSING: '缺失',
    AHEAD: '超前'
  }
  return map[value] || value
}

function itemTagType(value: string) {
  if (value === 'OK') return 'success'
  if (value === 'STALE') return 'warning'
  if (value === 'AHEAD') return 'danger'
  return 'info'
}

function maStatusClass(value: string) {
  return {
    stale: value !== 'OK' && value !== 'NOT_APPLICABLE' && value !== 'BLOCKED_BY_QUOTE',
    muted: value === 'NOT_APPLICABLE' || value === 'BLOCKED_BY_QUOTE'
  }
}

function formatDate(value: string | null | undefined) {
  if (!value) return '-'
  return value.slice(0, 10)
}

function factorStatusClass(value: string) {
  return maStatusClass(value)
}

function formatFactorDate(value: string | null | undefined, status: string) {
  if (status === 'NOT_APPLICABLE') return '不适用'
  if (status === 'BLOCKED_BY_QUOTE') return '待行情更新'
  return formatDate(value)
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '-'
  return new Date(value).toLocaleString('zh-CN', {
    timeZone: displayTimezone,
    hour12: false
  })
}
</script>

<style scoped lang="scss">
.data-quality-page {
  display: flex;
  flex-direction: column;
  gap: 22px;
}

.quality-hero {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 20px;
  padding: 28px;
  border-radius: 22px;
  color: #f7f8f8;
  background:
    radial-gradient(circle at top right, rgba(113, 112, 255, 0.24), transparent 34%),
    linear-gradient(135deg, #0f1011 0%, #191a1b 52%, #0f1011 100%);
  border: 1px solid rgba(255, 255, 255, 0.08);
  box-shadow: 0 18px 42px rgba(0, 0, 0, 0.28);

  h1 {
    margin: 0 0 10px;
    font-size: 32px;
  }
}

.eyebrow {
  margin: 0 0 10px;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  font-size: 12px;
  color: #828fff;
}

.hero-desc {
  margin: 0;
  max-width: 58ch;
  line-height: 1.7;
  color: #d0d6e0;
}

.summary-grid {
  display: grid;
  grid-template-columns: 1.25fr repeat(4, minmax(0, 1fr));
  gap: 16px;

  @media (max-width: 1400px) {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  @media (max-width: 1000px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  @media (max-width: 720px) {
    grid-template-columns: 1fr;
  }
}

.status-card,
.metric-card,
.table-card {
  border-radius: 20px;
}

.status-main {
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 16px;
}

.status-dot {
  width: 18px;
  height: 18px;
  border-radius: 999px;
  background: #22c55e;
  box-shadow: 0 0 0 8px rgba(34, 197, 94, 0.12);

  &.warn {
    background: #f59e0b;
    box-shadow: 0 0 0 8px rgba(245, 158, 11, 0.14);
  }

  &.error {
    background: #ef4444;
    box-shadow: 0 0 0 8px rgba(239, 68, 68, 0.14);
  }
}

.label {
  color: var(--color-text-secondary);
  font-size: 13px;
}

.value,
.metric-value {
  margin-top: 6px;
  font-size: 28px;
  font-weight: 800;
}

.metric-footer {
  display: flex;
  justify-content: space-between;
  margin-top: 12px;
  color: var(--color-text-secondary);
  font-size: 13px;
}

.toolbar-card {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  padding: 16px;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.08);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.24);

  @media (max-width: 900px) {
    align-items: flex-start;
    flex-direction: column;
  }
}

.toolbar-left,
.toolbar-right {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.table-header {
  display: flex;
  justify-content: space-between;
  gap: 16px;

  p {
    margin: 6px 0 0;
    color: var(--color-text-secondary);
    font-size: 13px;
  }
}

.table-count,
.muted {
  color: var(--color-text-secondary);
  font-size: 13px;
}

.table-card {
  :deep(.data-quality-table) {
    --el-table-bg-color: transparent;
    --el-table-header-bg-color: rgba(255, 255, 255, 0.035);
    --el-table-tr-bg-color: transparent;
    --el-table-row-hover-bg-color: rgba(113, 112, 255, 0.08);
    --el-table-current-row-bg-color: rgba(113, 112, 255, 0.12);
    --el-table-border-color: rgba(255, 255, 255, 0.07);
    --el-table-border: 1px solid rgba(255, 255, 255, 0.07);
    --el-table-text-color: var(--color-text-primary);
    --el-table-header-text-color: var(--color-text-secondary);
    border-radius: 16px;
    overflow: hidden;
    background:
      radial-gradient(circle at 12% 0%, rgba(113, 112, 255, 0.08), transparent 34%),
      rgba(255, 255, 255, 0.018);
    color: var(--color-text-primary);
  }

  :deep(.data-quality-table .el-table__inner-wrapper),
  :deep(.data-quality-table .el-table__header-wrapper),
  :deep(.data-quality-table .el-table__body-wrapper),
  :deep(.data-quality-table .el-table__fixed),
  :deep(.data-quality-table .el-table__fixed-right),
  :deep(.data-quality-table .el-table__fixed-header-wrapper),
  :deep(.data-quality-table .el-table__fixed-body-wrapper) {
    background: transparent;
  }

  :deep(.data-quality-table .el-table__inner-wrapper::before),
  :deep(.data-quality-table .el-table__border-left-patch) {
    background-color: rgba(255, 255, 255, 0.07);
  }

  :deep(.data-quality-table .el-table__header-wrapper th.el-table__cell) {
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.055), rgba(255, 255, 255, 0.025));
    color: var(--color-text-secondary);
    font-weight: 600;
    letter-spacing: 0.02em;
  }

  :deep(.data-quality-table .el-table__body tr > td.el-table__cell) {
    background: rgba(255, 255, 255, 0.014);
    color: var(--color-text-primary);
    transition:
      background-color 0.18s ease,
      color 0.18s ease;
  }

  :deep(.data-quality-table .el-table__body tr.el-table__row--striped > td.el-table__cell) {
    background: rgba(255, 255, 255, 0.032);
  }

  :deep(.data-quality-table .el-table__body tr:hover > td.el-table__cell),
  :deep(.data-quality-table .el-table__body tr.el-table__row--striped:hover > td.el-table__cell) {
    background: rgba(113, 112, 255, 0.1) !important;
  }

  :deep(.data-quality-table .el-table__body tr.current-row > td.el-table__cell) {
    background: rgba(113, 112, 255, 0.14) !important;
  }

  :deep(.data-quality-table .el-table-fixed-column--left),
  :deep(.data-quality-table .el-table-fixed-column--right) {
    background: inherit !important;
  }

  :deep(.data-quality-table .el-table__cell.gutter) {
    background: rgba(255, 255, 255, 0.035);
  }

  :deep(.data-quality-table .el-loading-mask) {
    background: rgba(8, 9, 10, 0.72);
    backdrop-filter: blur(8px);
  }
}

.issue-list {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.stale {
  color: #fbbf24;
  font-weight: 700;
}

.pagination {
  display: flex;
  justify-content: flex-end;
  margin-top: 18px;
}
</style>
