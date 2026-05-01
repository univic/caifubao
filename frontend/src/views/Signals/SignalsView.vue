<template>
  <div class="signals-page">
    <!-- Hero Section -->
    <header class="signals-hero">
      <div class="hero-content">
        <p class="eyebrow">Signals</p>
        <h1 class="page-title">今日信号</h1>
        <p class="subtitle">基于 DataHub 引擎生成的 MA 信号，优先展示最新交易日触发的标的。</p>
      </div>
      <div class="hero-stats">
        <div class="stat-group">
          <span class="stat-label">信号日期</span>
          <span class="stat-value">{{ displayDate }}</span>
        </div>
        <div class="stat-divider"></div>
        <div class="stat-group">
          <span class="stat-label">触发总数</span>
          <span class="stat-value">{{ response?.total ?? 0 }} <small>条</small></span>
        </div>
      </div>
    </header>

    <!-- Filters Section -->
    <div class="filters-container">
      <div class="filter-group">
        <label>日期</label>
        <el-date-picker
          v-model="filters.date"
          type="date"
          value-format="YYYY-MM-DD"
          placeholder="最新交易日"
          clearable
          class="linear-picker"
        />
      </div>
      <div class="filter-group">
        <label>信号类型</label>
        <el-select v-model="filters.signal_name" placeholder="全部信号" clearable class="linear-select">
          <el-option label="MA10 上穿 MA20" value="MA10_CROSS_MA20" />
        </el-select>
      </div>
      <div class="filter-group">
        <label>信号方向</label>
        <el-select v-model="filters.direction" placeholder="全部方向" clearable class="linear-select">
          <el-option label="看多" value="BULLISH" />
          <el-option label="看空" value="BEARISH" />
        </el-select>
      </div>
      <div class="filter-actions">
        <el-button class="btn-primary" :loading="loading" @click="fetchSignals">刷新</el-button>
        <el-button class="btn-ghost" @click="resetFilters">重置</el-button>
      </div>
    </div>

    <!-- Content Section -->
    <div class="content-card">
      <div class="card-header">
        <div class="header-left">
          <h3 class="card-title">信号明细</h3>
          <p class="card-desc">实时量化因子计算结果</p>
        </div>
        <div class="header-right">
          <div class="data-tag" v-if="response?.date">
            <span class="dot"></span>
            {{ formatDate(response.date) }}
          </div>
        </div>
      </div>

      <el-alert
        v-if="errorMessage"
        class="linear-alert"
        type="error"
        :title="errorMessage"
        show-icon
        :closable="false"
      />

      <div class="table-wrapper" v-loading="loading">
        <el-table
          :data="response?.items ?? []"
          class="linear-table"
          empty-text="当前筛选条件下暂无信号"
        >
          <el-table-column label="股票" min-width="180">
            <template #default="{ row }">
              <div class="stock-cell">
                <span class="stock-name">{{ row.stock_name || '--' }}</span>
                <span class="stock-code">{{ row.stock_code }}</span>
              </div>
            </template>
          </el-table-column>
          
          <el-table-column label="信号名称" min-width="180">
            <template #default="{ row }">
              <span class="signal-name-text">{{ signalLabel(row.signal_name) }}</span>
            </template>
          </el-table-column>
          
          <el-table-column label="方向" width="100">
            <template #default="{ row }">
              <span class="direction-tag" :class="row.direction.toLowerCase()">
                {{ directionLabel(row.direction) }}
              </span>
            </template>
          </el-table-column>
          
          <el-table-column label="强度" width="110" align="right">
            <template #default="{ row }">
              <span class="mono-text">{{ formatPercent(row.strength) }}</span>
            </template>
          </el-table-column>
          
          <el-table-column label="因子快照" min-width="220">
            <template #default="{ row }">
              <div class="factor-grid">
                <div class="factor-item">
                  <span class="f-label">MA10</span>
                  <span class="f-value mono-text">{{ formatNumber(row.factor_snapshot?.ma_10) }}</span>
                </div>
                <div class="factor-item">
                  <span class="f-label">MA20</span>
                  <span class="f-value mono-text">{{ formatNumber(row.factor_snapshot?.ma_20) }}</span>
                </div>
              </div>
            </template>
          </el-table-column>
          
          <el-table-column prop="reason" label="触发原因" min-width="260" />
          
          <el-table-column label="生成时间" width="180">
            <template #default="{ row }">
              <span class="mono-text small">{{ formatDateTime(row.generated_at) }}</span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { signalApi, type SignalListResponse } from '@/api/signals'

const loading = ref(false)
const errorMessage = ref('')
const response = ref<SignalListResponse | null>(null)
const filters = reactive({
  date: '',
  signal_name: '',
  direction: ''
})

const displayDate = computed(() => {
  if (!response.value?.date) return '暂无数据'
  return formatDate(response.value.date)
})

function formatDate(value: string | null) {
  if (!value) return '--'
  return value.slice(0, 10)
}

function formatDateTime(value: string | null) {
  if (!value) return '--'
  return value.replace('T', ' ').slice(0, 19)
}

function formatNumber(value: unknown) {
  const numericValue = Number(value)
  if (!Number.isFinite(numericValue)) return '--'
  return numericValue.toFixed(2)
}

function formatPercent(value: number | null) {
  if (value === null || value === undefined) return '--'
  return `${value.toFixed(2)}%`
}

function signalLabel(value: string) {
  if (value === 'MA10_CROSS_MA20') return 'MA10 上穿 MA20'
  return value
}

function directionLabel(value: string) {
  if (value === 'BULLISH') return '看多'
  if (value === 'BEARISH') return '看空'
  return value
}

async function fetchSignals() {
  loading.value = true
  errorMessage.value = ''
  try {
    response.value = await signalApi.listSignals({
      date: filters.date || undefined,
      signal_name: filters.signal_name || undefined,
      direction: filters.direction || undefined,
      limit: 100
    })
  } catch (error) {
    console.error(error)
    errorMessage.value = '信号数据加载失败，请稍后重试。'
  } finally {
    loading.value = false
  }
}

function resetFilters() {
  filters.date = ''
  filters.signal_name = ''
  filters.direction = ''
  fetchSignals()
}

onMounted(fetchSignals)
</script>

<style scoped lang="scss">
.signals-page {
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
.signals-hero {
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

.hero-stats {
  display: flex;
  align-items: center;
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  padding: 16px 32px;
  border-radius: 12px;
  gap: 32px;
}

.stat-group {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.stat-label {
  font-size: 12px;
  color: var(--color-text-quaternary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.stat-value {
  font-size: 20px;
  font-weight: 510;
  color: var(--color-text-primary);
  
  small {
    font-size: 14px;
    color: var(--color-text-tertiary);
    font-weight: 400;
  }
}

.stat-divider {
  width: 1px;
  height: 32px;
  background: var(--color-border-subtle);
}

/* Filters */
.filters-container {
  display: flex;
  align-items: flex-end;
  gap: 20px;
  margin-bottom: 32px;
  padding: 24px;
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
}

.filter-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
  
  label {
    font-size: 12px;
    font-weight: 510;
    color: var(--color-text-quaternary);
  }
}

.filter-actions {
  display: flex;
  gap: 12px;
  margin-left: auto;
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

/* Select/Picker overrides */
:deep(.el-input__wrapper) {
  background-color: rgba(255, 255, 255, 0.02) !important;
  box-shadow: 0 0 0 1px var(--color-border) inset !important;
  border-radius: 6px !important;
}

:deep(.el-input__inner) {
  color: var(--color-text-primary) !important;
  font-family: var(--font-inter) !important;
  font-size: 13px !important;
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

.data-tag {
  display: flex;
  align-items: center;
  gap: 8px;
  background: rgba(255, 255, 255, 0.04);
  padding: 4px 12px;
  border-radius: 9999px;
  font-size: 12px;
  font-weight: 510;
  color: var(--color-text-secondary);
  border: 1px solid var(--color-border-subtle);

  .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #10b981;
    box-shadow: 0 0 8px #10b981;
  }
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
  }

  .el-table__row:hover > td {
    background-color: rgba(255, 255, 255, 0.02) !important;
  }
}

.stock-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.stock-name {
  font-size: 14px;
  font-weight: 510;
  color: var(--color-text-primary);
}

.stock-code {
  font-size: 12px;
  font-family: var(--font-mono);
  color: var(--color-text-quaternary);
}

.signal-name-text {
  font-size: 14px;
  color: var(--color-text-secondary);
}

.direction-tag {
  font-size: 11px;
  font-weight: 510;
  padding: 2px 8px;
  border-radius: 4px;
  text-transform: uppercase;
  
  &.bullish {
    color: #10b981;
    background: rgba(16, 185, 129, 0.1);
  }
  
  &.bearish {
    color: #ef4444;
    background: rgba(239, 68, 68, 0.1);
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

.factor-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}

.factor-item {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.f-label {
  font-size: 10px;
  color: var(--color-text-quaternary);
  text-transform: uppercase;
}

.f-value {
  color: var(--color-text-secondary);
}

.linear-alert {
  margin: 20px;
  background-color: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.2);
  color: #fb7185;
}

@media (max-width: 1024px) {
  .signals-page {
    padding: 24px;
  }
  
  .page-title {
    font-size: 32px;
  }
  
  .signals-hero {
    flex-direction: column;
    align-items: flex-start;
    gap: 24px;
  }
  
  .filters-container {
    flex-wrap: wrap;
  }
  
  .filter-actions {
    margin-left: 0;
    width: 100%;
    
    .el-button {
      flex: 1;
    }
  }
}
</style>

