<template>
  <div class="backtest-list-page">
    <!-- Hero Section -->
    <header class="page-hero">
      <div class="hero-left">
        <p class="eyebrow">Backtest</p>
        <h1 class="page-title">策略回测</h1>
        <p class="subtitle">对历史行情运行交易策略，查看收益率、最大回撤、夏普比率等核心指标。</p>
      </div>
      <div class="hero-right">
        <router-link to="/backtest/new">
          <el-button class="btn-primary" :icon="Plus">新建回测</el-button>
        </router-link>
      </div>
    </header>

    <!-- Error Alert -->
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
          <h3 class="card-title">回测记录</h3>
          <p class="card-desc">共 {{ backtests.length }} 条记录</p>
        </div>
        <el-button class="btn-ghost" :icon="Refresh" :loading="loading" @click="fetchBacktests">刷新</el-button>
      </div>

      <div class="table-wrapper" v-loading="loading">
        <el-table
          :data="backtests"
          class="linear-table"
          empty-text="暂无回测记录，点击上方按钮新建"
          @row-click="goToDetail"
          highlight-current-row
        >
          <el-table-column label="名称" min-width="140">
            <template #default="{ row }">
              <div class="name-cell">
                <span class="bt-name">{{ row.name }}</span>
                <span class="bt-stock">{{ row.stock_name || row.stock_code }}</span>
              </div>
            </template>
          </el-table-column>

          <el-table-column label="策略" min-width="140">
            <template #default="{ row }">
              <span class="strategy-label">{{ strategyLabel(row.strategy) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="回测区间" min-width="200">
            <template #default="{ row }">
              <span class="mono-text">{{ formatDate(row.start_date) }} — {{ formatDate(row.end_date) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="收益率" width="120" align="right">
            <template #default="{ row }">
              <span class="return-value" :class="pnlClass(row.total_return)">{{ formatPercent(row.total_return_pct) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="最大回撤" width="120" align="right">
            <template #default="{ row }">
              <span class="mono-text danger-text">{{ formatPercent(row.max_drawdown) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="夏普" width="90" align="right">
            <template #default="{ row }">
              <span class="mono-text">{{ formatNumber(row.sharpe_ratio) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="胜率" width="90" align="right">
            <template #default="{ row }">
              <span class="mono-text">{{ formatPercent(row.win_rate) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="状态" width="100">
            <template #default="{ row }">
              <span class="status-tag" :class="statusClass(row.status)">
                {{ statusLabel(row.status) }}
              </span>
            </template>
          </el-table-column>

          <el-table-column label="创建时间" width="180">
            <template #default="{ row }">
              <span class="mono-text small">{{ formatDateTime(row.created_at) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="操作" width="100" fixed="right">
            <template #default="{ row }">
              <el-popconfirm
                title="确认删除该回测记录？"
                confirm-button-text="删除"
                cancel-button-text="取消"
                @confirm.stop="handleDelete(row.id)"
              >
                <template #reference>
                  <el-button
                    class="btn-delete"
                    :icon="Delete"
                    size="small"
                    text
                    @click.stop
                  >
                    删除
                  </el-button>
                </template>
              </el-popconfirm>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </div>

    <!-- Empty State -->
    <div v-if="!loading && backtests.length === 0" class="empty-state">
      <el-empty description="暂无回测记录">
        <router-link to="/backtest/new">
          <el-button class="btn-primary" :icon="Plus">创建第一个回测</el-button>
        </router-link>
      </el-empty>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh, Plus, Delete } from '@element-plus/icons-vue'
import { backtestApi, type BacktestResult } from '@/api/backtest'

const router = useRouter()
const loading = ref(false)
const errorMessage = ref('')
const backtests = ref<BacktestResult[]>([])

function formatDate(value: string | null) {
  if (!value) return '--'
  return value.slice(0, 10)
}

function formatDateTime(value: string | null) {
  if (!value) return '--'
  return value.replace('T', ' ').slice(0, 19)
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toFixed(2)
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `${value.toFixed(2)}%`
}

function pnlClass(value: number) {
  if (value > 0) return 'positive'
  if (value < 0) return 'negative'
  return ''
}

function strategyLabel(value: string) {
  const map: Record<string, string> = {
    MA_CROSS: '均线交叉策略',
    BUY_HOLD: '买入持有策略',
    GOLDEN_DEATH_CROSS: '金叉死叉策略'
  }
  return map[value] || value
}

function statusLabel(value: string) {
  const map: Record<string, string> = {
    PENDING: '排队中',
    RUNNING: '运行中',
    COMPLETED: '已完成',
    FAILED: '失败'
  }
  return map[value] || value
}

function statusClass(value: string) {
  return {
    PENDING: 'warning',
    RUNNING: 'info',
    COMPLETED: 'success',
    FAILED: 'danger'
  }[value] || 'info'
}

function goToDetail(row: BacktestResult) {
  router.push(`/backtest/${row.id}`)
}

async function fetchBacktests() {
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await backtestApi.list()
    backtests.value = response.items
  } catch (error) {
    console.error(error)
    errorMessage.value = '回测记录加载失败，请稍后重试。'
  } finally {
    loading.value = false
  }
}

async function handleDelete(id: string) {
  try {
    await backtestApi.delete(id)
    backtests.value = backtests.value.filter(item => item.id !== id)
  } catch (error) {
    console.error(error)
    errorMessage.value = '删除失败，请稍后重试。'
  }
}

onMounted(fetchBacktests)
</script>

<style scoped lang="scss">
.backtest-list-page {
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
  margin-bottom: 48px;
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
  font-size: 18px;
  font-weight: 400;
  color: var(--color-text-tertiary);
  max-width: 600px;
  margin: 0;
  line-height: 1.6;
}

/* Button overrides */
:deep(.el-button) {
  height: 36px;
  padding: 0 16px;
  border-radius: 6px;
  font-weight: 510;
  font-size: 14px;
  transition: all 0.2s;
}

.btn-primary {
  background: var(--color-brand) !important;
  border: none !important;
  color: #fff !important;

  &:hover {
    background: var(--color-brand-accent) !important;
  }
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

.btn-delete {
  color: var(--color-text-quaternary) !important;

  &:hover {
    color: #ef4444 !important;
  }
}

/* Error Alert */
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

/* Table Overrides */
.table-wrapper {
  padding: 0 12px 12px 12px;
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

.bt-name {
  font-size: 14px;
  font-weight: 510;
  color: var(--color-text-primary);
}

.bt-stock {
  font-size: 12px;
  font-family: var(--font-mono);
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

/* Status Tags */
.status-tag {
  font-size: 11px;
  font-weight: 510;
  padding: 2px 8px;
  border-radius: 4px;
  text-transform: uppercase;

  &.success {
    color: #10b981;
    background: rgba(16, 185, 129, 0.1);
  }

  &.info {
    color: #60a5fa;
    background: rgba(96, 165, 250, 0.1);
  }

  &.warning {
    color: #fbbf24;
    background: rgba(251, 191, 36, 0.1);
  }

  &.danger {
    color: #ef4444;
    background: rgba(239, 68, 68, 0.1);
  }
}

/* Empty State */
.empty-state {
  margin-top: 80px;
}

:deep(.el-empty__description) {
  color: var(--color-text-tertiary);
  margin-bottom: 16px;
}

@media (max-width: 1024px) {
  .backtest-list-page {
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
}
</style>
