<template>
  <div class="strategy-replay-list-page">
    <!-- Hero Section -->
    <header class="page-hero">
      <div class="hero-left">
        <p class="eyebrow">Research · Replay</p>
        <h1 class="page-title">策略重放</h1>
        <p class="subtitle">
          只读的历史重放研究记录。重放不计入前视验证（0/120），也不构成投资建议。
        </p>
      </div>
      <div class="hero-right">
        <el-button class="btn-ghost" :icon="Refresh" :loading="loading" @click="fetchReplays">
          刷新
        </el-button>
      </div>
    </header>

    <!-- Error State -->
    <el-alert
      v-if="errorMessage"
      class="error-alert"
      type="error"
      :title="errorMessage"
      show-icon
      :closable="false"
    />

    <!-- Content Card -->
    <div class="content-card">
      <div class="card-header">
        <div class="header-left">
          <h3 class="card-title">研究重放</h3>
          <p class="card-desc">
            共 {{ total }} 条记录 · 列表仅为摘要，完整决策、成交与净值曲线见详情
          </p>
        </div>
        <span class="research-note">RESEARCH · REPLAY</span>
      </div>

      <!-- Loading State -->
      <div v-if="loading" class="loading-state">
        <el-skeleton :rows="6" animated />
      </div>

      <!-- Empty State -->
      <div v-else-if="!errorMessage && items.length === 0" class="empty-state">
        <el-empty description="暂无策略重放数据" :image-size="80">
          <p class="empty-hint">
            重放记录由运营研究流程写入，本页面只读，不提供创建或修改入口。
          </p>
        </el-empty>
      </div>

      <!-- Table -->
      <template v-else-if="items.length > 0">
        <div class="table-wrapper">
          <el-table
            :data="items"
            class="linear-table"
            highlight-current-row
            @row-click="goToDetail"
          >
            <el-table-column label="名称" min-width="220">
              <template #default="{ row }">
                <div class="name-cell">
                  <span class="rp-name">{{ row.name || row.replay_id }}</span>
                  <span class="rp-id mono-text small">{{ row.replay_id }}</span>
                </div>
              </template>
            </el-table-column>

            <el-table-column label="策略" min-width="130">
              <template #default="{ row }">
                <span class="strategy-label">{{ strategyLabel(row.strategy) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="重放区间" min-width="200">
              <template #default="{ row }">
                <span class="mono-text">
                  {{ formatDate(row.start_date) }} — {{ formatDate(row.end_date) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column label="AUM" width="120" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatMoney(row.aum) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="总收益" width="112" align="right">
              <template #default="{ row }">
                <span class="return-value" :class="pnlClass(row.metrics?.total_return)">
                  {{ formatFractionPercent(row.metrics?.total_return) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column label="最大回撤" width="112" align="right">
              <template #default="{ row }">
                <span class="mono-text danger-text">
                  {{ formatFractionPercent(row.metrics?.max_drawdown) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column label="Sharpe" width="92" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatNumber(row.metrics?.sharpe) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="证据" width="132">
              <template #default>
                <div class="evidence-cell">
                  <span class="replay-badge">REPLAY</span>
                  <span class="forward-count">0/{{ forwardRequired }}</span>
                </div>
              </template>
            </el-table-column>

            <el-table-column label="数据截至" width="120">
              <template #default="{ row }">
                <span class="mono-text small">{{ formatDate(row.data_as_of) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </div>

        <div class="pagination-wrapper">
          <el-pagination
            :current-page="page"
            :page-size="pageSize"
            :page-sizes="[10, 20, 50, 100]"
            :total="total"
            layout="total, sizes, prev, pager, next, jumper"
            @current-change="handlePageChange"
            @size-change="handleSizeChange"
          />
        </div>
      </template>
    </div>

    <p class="footnote">
      REPLAY 表示历史回放研究记录。前视认证（forward_eligible）只为独立的
      {{ forwardRequired }} 会话前视窗口产生，重放永远为 0/{{ forwardRequired }}。
    </p>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh } from '@element-plus/icons-vue'
import {
  listStrategyReplays,
  type StrategyReplaySummary
} from '@/api/strategyReplays'

const router = useRouter()
const loading = ref(false)
const errorMessage = ref('')
const items = ref<StrategyReplaySummary[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const forwardRequired = 120

function formatDate(value: string | null | undefined) {
  if (!value) return '--'
  return value.slice(0, 10)
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

/** Replay metrics are decimal fractions (0.62 => 62.00%). */
function formatFractionPercent(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `${(value * 100).toFixed(2)}%`
}

function pnlClass(value: number | null | undefined) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return ''
  if (numeric > 0) return 'positive'
  if (numeric < 0) return 'negative'
  return ''
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

function goToDetail(row: StrategyReplaySummary) {
  if (!row?.replay_id) return
  router.push(`/strategy-replays/${encodeURIComponent(row.replay_id)}`)
}

async function fetchReplays() {
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await listStrategyReplays({
      page: page.value,
      page_size: pageSize.value
    })
    items.value = response.items || []
    total.value = response.total || 0
    // The API clamps invalid pagination; mirror the effective values.
    page.value = response.page
    pageSize.value = response.page_size
  } catch (error) {
    console.error(error)
    items.value = []
    total.value = 0
    errorMessage.value = '策略重放列表加载失败，请稍后重试。'
  } finally {
    loading.value = false
  }
}

function handlePageChange(nextPage: number) {
  if (nextPage === page.value) return
  page.value = nextPage
  fetchReplays()
}

function handleSizeChange(nextSize: number) {
  if (nextSize === pageSize.value) return
  pageSize.value = nextSize
  page.value = 1
  fetchReplays()
}

onMounted(fetchReplays)
</script>

<style scoped lang="scss">
.strategy-replay-list-page {
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

/* Hero Section */
.page-hero {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  margin-bottom: 40px;
}

.eyebrow {
  font-size: 13px;
  font-weight: 510;
  color: var(--color-brand-accent);
  margin-bottom: 12px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

.page-title {
  font-size: 48px;
  font-weight: 510;
  line-height: 1;
  letter-spacing: -1.056px;
  margin: 0 0 16px 0;
  color: var(--color-text-primary);
}

.subtitle {
  font-size: 16px;
  font-weight: 400;
  color: var(--color-text-tertiary);
  max-width: 720px;
  margin: 0;
  line-height: 1.6;
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

/* Content Card */
.content-card {
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  overflow: hidden;
}

.card-header {
  padding: 24px 32px;
  border-bottom: 1px solid var(--color-border-subtle);
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
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

.research-note {
  flex: none;
  font-family: var(--font-mono);
  font-size: 11px;
  letter-spacing: 0.08em;
  padding: 4px 10px;
  border-radius: 999px;
  border: 1px solid rgba(113, 112, 255, 0.24);
  background: rgba(113, 112, 255, 0.1);
  color: #a9a8ff;
}

.loading-state {
  padding: 32px;
}

/* Empty State */
.empty-state {
  padding: 56px 24px;
}

.empty-hint {
  max-width: 460px;
  margin: 8px auto 0;
  color: var(--color-text-quaternary);
  font-size: 13px;
  line-height: 1.6;
}

:deep(.el-empty__description) {
  color: var(--color-text-tertiary);
  margin-bottom: 8px;
}

/* Table Overrides */
.table-wrapper {
  padding: 0 12px;
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
    cursor: pointer;
  }

  .el-table__row:hover > td {
    background-color: rgba(255, 255, 255, 0.02) !important;
  }
}

/* Cell Styles */
.name-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.rp-name {
  font-size: 14px;
  font-weight: 510;
  color: var(--color-text-primary);
}

.rp-id {
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--color-text-quaternary);
}

.strategy-label {
  font-size: 14px;
  color: var(--color-text-secondary);
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

.evidence-cell {
  display: flex;
  align-items: center;
  gap: 6px;
}

.replay-badge {
  font-size: 10px;
  font-weight: 590;
  letter-spacing: 0.08em;
  padding: 2px 7px;
  border-radius: 4px;
  color: #a9a8ff;
  background: rgba(113, 112, 255, 0.14);
  border: 1px solid rgba(113, 112, 255, 0.28);
}

.forward-count {
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--color-text-tertiary);
}

.pagination-wrapper {
  display: flex;
  justify-content: flex-end;
  padding: 16px 24px 20px;
}

.footnote {
  margin: 16px 4px 0;
  font-size: 12px;
  line-height: 1.7;
  color: var(--color-text-quaternary);
}

@media (max-width: 1024px) {
  .strategy-replay-list-page {
    padding: 24px;
  }

  .page-title {
    font-size: 32px;
  }

  .page-hero {
    flex-direction: column;
    align-items: flex-start;
    gap: 24px;
  }

  .card-header {
    flex-direction: column;
    align-items: flex-start;
  }
}
</style>
