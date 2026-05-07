<template>
  <div class="market-page">
    <!-- Hero Section -->
    <header class="market-hero">
      <div class="hero-left">
        <p class="eyebrow">Market Board</p>
        <h1 class="page-title">标的看板</h1>
        <p class="subtitle">全量标的行情与量化评分总览，支持多维筛选与预测闭环验证。</p>
      </div>
      
      <div class="hero-right">
        <div class="filter-controls">
          <div class="filter-item search">
            <el-input
              v-model="searchKeyword"
              placeholder="搜索代码或名称..."
              :prefix-icon="Search"
              clearable
              class="linear-input"
            />
          </div>
          <div class="filter-item date">
            <el-date-picker
              v-model="targetDate"
              type="date"
              value-format="YYYY-MM-DD"
              placeholder="选择日期"
              :clearable="false"
              class="linear-picker"
              @change="resetAndFetch"
            />
          </div>
        </div>
      </div>
    </header>

    <!-- Main Content -->
    <div class="content-container">
      <el-tabs v-model="activeTab" class="linear-tabs" @tab-change="resetAndFetch">
        <el-tab-pane label="股票 (Stocks)" name="stock">
          <!-- Desktop Table View -->
          <div class="desktop-view" v-loading="loading">
            <el-table :data="tableData" class="linear-table" style="width: 100%">
              <el-table-column label="排名" width="70" align="center">
                <template #default="{ row }">
                  <span class="rank-text" :class="{ 'top-rank': row.evaluation.display_rank <= 3 }">
                    #{{ row.evaluation.display_rank }}
                  </span>
                </template>
              </el-table-column>

              <el-table-column label="标的" min-width="180">
                <template #default="{ row }">
                  <div class="asset-cell">
                    <span class="asset-name">{{ row.name || '--' }}</span>
                    <span class="asset-code">{{ row.code }}</span>
                  </div>
                </template>
              </el-table-column>

              <el-table-column label="最新价" width="100">
                <template #default="{ row }">
                  <span :class="getPriceClass(row.ohlcv.change_rate)">
                    {{ formatNumber(row.ohlcv.close) }}
                  </span>
                </template>
              </el-table-column>

              <el-table-column label="涨跌幅" width="100">
                <template #default="{ row }">
                  <span :class="getPriceClass(row.ohlcv.change_rate)">
                    {{ formatPercent(row.ohlcv.change_rate) }}
                  </span>
                </template>
              </el-table-column>

              <el-table-column label="评分" width="90" align="center">
                <template #default="{ row }">
                  <div class="score-badge" :class="getScoreClass(row.evaluation.score)">
                    {{ row.evaluation.score !== null ? row.evaluation.score : '--' }}
                  </div>
                </template>
              </el-table-column>

              <el-table-column label="建议" width="100">
                <template #default="{ row }">
                  <el-tag 
                    v-if="row.evaluation.recommendation !== 'NONE'"
                    :type="row.evaluation.recommendation === 'BUY' ? 'success' : 'warning'"
                    effect="dark"
                    size="small"
                  >
                    {{ row.evaluation.recommendation }}
                  </el-tag>
                  <span v-else class="text-dim">--</span>
                </template>
              </el-table-column>

              <el-table-column label="评分依据" min-width="200">
                <template #default="{ row }">
                  <div class="basis-tags">
                    <span v-for="tag in row.evaluation.basis?.signals || []" :key="tag" class="basis-tag signal">{{ tag }}</span>
                    <span v-for="tag in row.evaluation.basis?.trend || []" :key="tag" class="basis-tag trend">{{ tag }}</span>
                  </div>
                </template>
              </el-table-column>

              <el-table-column label="T+5 验证" min-width="150">
                <template #default="{ row }">
                  <div v-if="row.evaluation.status === 'VERIFIED'" class="verify-cell">
                    <span :class="getPriceClass(row.evaluation.max_profit_percentage * 100)">
                      {{ formatPercent(row.evaluation.max_profit_percentage * 100) }}
                    </span>
                    <el-icon v-if="row.evaluation.is_effective" class="effect-icon"><CircleCheck /></el-icon>
                  </div>
                  <span v-else class="text-dim">待验证</span>
                </template>
              </el-table-column>
            </el-table>
          </div>

          <!-- Mobile Card View -->
          <div class="mobile-view" v-loading="loading">
            <div v-for="item in tableData" :key="item.code" class="asset-card">
              <div class="card-header">
                <div class="header-main">
                  <span class="c-rank">#{{ item.evaluation.display_rank }}</span>
                  <span class="c-name">{{ item.name }}</span>
                  <span class="c-code">{{ item.code }}</span>
                </div>
                <div class="header-score" :class="getScoreClass(item.evaluation.score)">
                  {{ item.evaluation.score }}
                </div>
              </div>
              <div class="card-body">
                <div class="price-row">
                  <span class="c-price" :class="getPriceClass(item.ohlcv.change_rate)">
                    {{ formatNumber(item.ohlcv.close) }}
                  </span>
                  <span class="c-change" :class="getPriceClass(item.ohlcv.change_rate)">
                    {{ formatPercent(item.ohlcv.change_rate) }}
                  </span>
                </div>
                <div class="recommendation-row" v-if="item.evaluation.recommendation !== 'NONE'">
                   <el-tag :type="item.evaluation.recommendation === 'BUY' ? 'success' : 'warning'" effect="dark" size="small">
                     {{ item.evaluation.recommendation }}
                   </el-tag>
                </div>
              </div>
            </div>
          </div>
        </el-tab-pane>

        <el-tab-pane label="指数 (Indices)" name="index">
           <!-- Index Table (similar to stock but simplified) -->
           <div class="desktop-view" v-loading="loading">
              <el-table :data="tableData" class="linear-table" style="width: 100%">
                 <el-table-column label="标的" min-width="180">
                   <template #default="{ row }">
                     <div class="asset-cell">
                       <span class="asset-name">{{ row.name || '--' }}</span>
                       <span class="asset-code">{{ row.code }}</span>
                     </div>
                   </template>
                 </el-table-column>
                 <el-table-column label="收盘" width="120">
                   <template #default="{ row }">
                     <span :class="getPriceClass(row.ohlcv.change_rate)">{{ formatNumber(row.ohlcv.close) }}</span>
                   </template>
                 </el-table-column>
                 <el-table-column label="涨幅" width="120">
                   <template #default="{ row }">
                     <span :class="getPriceClass(row.ohlcv.change_rate)">{{ formatPercent(row.ohlcv.change_rate) }}</span>
                   </template>
                 </el-table-column>
                 <el-table-column label="评分" width="100" align="center">
                    <template #default="{ row }">
                      <div class="score-badge" :class="getScoreClass(row.evaluation.score)">
                        {{ row.evaluation.score }}
                      </div>
                    </template>
                 </el-table-column>
                 <el-table-column label="OHLCV" min-width="200">
                    <template #default="{ row }">
                      <div class="ohlcv-mini">
                        <span>O: {{ formatNumber(row.ohlcv.open) }}</span>
                        <span>H: {{ formatNumber(row.ohlcv.high) }}</span>
                        <span>L: {{ formatNumber(row.ohlcv.low) }}</span>
                      </div>
                    </template>
                 </el-table-column>
              </el-table>
           </div>
           
           <div class="mobile-view" v-loading="loading">
              <div v-for="item in tableData" :key="item.code" class="asset-card">
                 <div class="card-header">
                    <span class="c-name">{{ item.name }}</span>
                    <span class="c-change" :class="getPriceClass(item.ohlcv.change_rate)">{{ formatPercent(item.ohlcv.change_rate) }}</span>
                 </div>
              </div>
           </div>
        </el-tab-pane>
      </el-tabs>
      <div class="pagination-row">
        <el-pagination
          layout="prev, pager, next, sizes, total"
          :total="total"
          :current-page="page"
          :page-size="pageSize"
          :page-sizes="[50, 100, 200]"
          @current-change="handlePageChange"
          @size-change="handlePageSizeChange"
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { marketApi, type MarketComprehensiveItem } from '@/api/market'
import { Search, CircleCheck } from '@element-plus/icons-vue'

const loading = ref(false)
const activeTab = ref<'stock' | 'index'>('stock')
const targetDate = ref('')
const searchKeyword = ref('')
const tableData = ref<MarketComprehensiveItem[]>([])
const page = ref(1)
const pageSize = ref(50)
const total = ref(0)
let searchTimer: number | undefined

async function fetchData() {
  loading.value = true
  try {
    const res = await marketApi.getComprehensiveData({
      type: activeTab.value,
      date: targetDate.value || undefined,
      page: page.value,
      per_page: pageSize.value,
      q: searchKeyword.value.trim() || undefined
    })
    tableData.value = res.items
    total.value = res.total
    if (!targetDate.value) {
      targetDate.value = res.date
    }
  } catch (error) {
    console.error('Failed to fetch market data:', error)
  } finally {
    loading.value = false
  }
}

function resetAndFetch() {
  page.value = 1
  fetchData()
}

function handlePageChange(nextPage: number) {
  page.value = nextPage
  fetchData()
}

function handlePageSizeChange(nextSize: number) {
  pageSize.value = nextSize
  resetAndFetch()
}

function formatNumber(val: number | null) {
  if (val === null) return '--'
  return val.toFixed(2)
}

function formatPercent(val: number | null) {
  if (val === null) return '--'
  return (val > 0 ? '+' : '') + val.toFixed(2) + '%'
}

function getPriceClass(val: number | null) {
  if (val === null || val === 0) return ''
  return val > 0 ? 'text-up' : 'text-down'
}

function getScoreClass(score: number | null) {
  if (score === null) return ''
  if (score >= 80) return 'high'
  if (score >= 60) return 'medium'
  return 'low'
}

onMounted(() => {
  fetchData()
})

watch(searchKeyword, () => {
  window.clearTimeout(searchTimer)
  searchTimer = window.setTimeout(resetAndFetch, 300)
})
</script>

<style scoped lang="scss">
.market-page {
  --color-bg: #08090a;
  --color-panel: #0f1011;
  --color-brand: #5e6ad2;
  --color-text-primary: #f7f8f8;
  --color-text-dim: #8a8f98;
  --color-up: #10b981;
  --color-down: #ef4444;
  --color-border: rgba(255, 255, 255, 0.08);

  min-height: 100vh;
  padding: 40px 60px;
  background-color: var(--color-bg);
  color: var(--color-text-primary);
  font-family: 'Inter Variable', sans-serif;
}

.market-hero {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 48px;
  gap: 24px;
}

.eyebrow {
  font-size: 13px;
  font-weight: 510;
  color: var(--color-brand);
  margin-bottom: 12px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

.page-title {
  font-size: 48px;
  font-weight: 510;
  margin: 0 0 16px 0;
  letter-spacing: -0.02em;
}

.subtitle {
  font-size: 18px;
  color: var(--color-text-dim);
  max-width: 600px;
  margin: 0;
  line-height: 1.6;
}

.filter-controls {
  display: flex;
  gap: 16px;
  background: var(--color-panel);
  padding: 16px;
  border-radius: 12px;
  border: 1px solid var(--color-border);
}

.filter-item {
  display: flex;
  flex-direction: column;
  gap: 8px;
  label {
    font-size: 11px;
    color: var(--color-text-dim);
    text-transform: uppercase;
  }
}

.search { width: 220px; }
.date { width: 160px; }

.content-container {
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  padding: 24px;
}

/* Table Text & Badges */
.rank-text {
  font-size: 13px;
  color: var(--color-text-dim);
  font-weight: 500;
  &.top-rank { color: #f59e0b; font-weight: 700; }
}

.asset-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.asset-name { font-weight: 510; color: var(--color-text-primary); }
.asset-code { font-size: 12px; color: var(--color-text-dim); font-family: ui-monospace, SF Mono, monospace; }

.text-up { color: var(--color-up); }
.text-down { color: var(--color-down); }
.text-dim { color: var(--color-text-dim); }

.score-badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 13px;
  font-weight: 600;
  background: rgba(255, 255, 255, 0.05);
  &.high { color: var(--color-up); background: rgba(16, 185, 129, 0.1); }
  &.medium { color: #f59e0b; background: rgba(245, 158, 11, 0.1); }
  &.low { color: var(--color-text-dim); }
}

.basis-tags { display: flex; flex-wrap: wrap; gap: 6px; }
.basis-tag {
  font-size: 10px;
  padding: 1px 6px;
  border-radius: 3px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  &.signal { color: #818cf8; background: rgba(129, 140, 248, 0.1); }
  &.trend { color: #2dd4bf; background: rgba(45, 212, 191, 0.1); }
}

.verify-cell {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  .effect-icon { font-size: 16px; color: var(--color-up); }
}

.ohlcv-mini { display: flex; gap: 12px; font-size: 11px; color: var(--color-text-dim); }

/* Responsive Views */
.mobile-view { display: none; }

@media (max-width: 768px) {
  .desktop-view { display: none; }
  .mobile-view { display: flex; flex-direction: column; gap: 12px; }
  
  .asset-card {
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid var(--color-border);
    border-radius: 8px;
    padding: 16px;
    
    .card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
      
      .header-main {
        display: flex;
        align-items: baseline;
        gap: 8px;
        .c-rank { font-size: 12px; color: #f59e0b; font-weight: 700; }
        .c-name { font-weight: 600; font-size: 15px; }
        .c-code { font-size: 11px; color: var(--color-text-dim); }
      }
      
      .header-score {
        font-size: 14px;
        font-weight: 700;
        &.high { color: var(--color-up); }
        &.medium { color: #f59e0b; }
      }
    }
    
    .card-body {
      display: flex;
      justify-content: space-between;
      align-items: center;
      
      .price-row {
        display: flex;
        gap: 12px;
        .c-price { font-weight: 600; }
        .c-change { font-size: 13px; }
      }
    }
  }
}

/* Linear overrides */
:deep(.linear-tabs) {
  .el-tabs__nav-wrap::after { display: none; }
  .el-tabs__item {
    color: var(--color-text-dim);
    font-size: 15px;
    font-weight: 510;
    &.is-active { color: var(--color-text-primary); }
  }
  .el-tabs__active-bar { background-color: var(--color-brand); }
}

:deep(.linear-table) {
  background: transparent !important;
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: transparent;
  --el-table-border-color: rgba(255, 255, 255, 0.05);
  
  th.el-table__cell {
    color: var(--color-text-dim);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border-bottom: 1px solid var(--color-border);
  }
  td.el-table__cell { padding: 12px 0; }
  .el-table__row:hover > td { background-color: rgba(255, 255, 255, 0.02) !important; }
}

:deep(.linear-input), :deep(.linear-picker) {
  .el-input__wrapper {
    background-color: rgba(255, 255, 255, 0.03);
    box-shadow: 0 0 0 1px var(--color-border) inset;
    &:hover, &.is-focus { box-shadow: 0 0 0 1px var(--color-brand) inset; }
  }
  .el-input__inner { color: var(--color-text-primary); font-size: 13px; }
}

.pagination-row {
  display: flex;
  justify-content: flex-end;
  margin-top: 18px;
}

@media (max-width: 1024px) {
  .market-page { padding: 20px; }
  .market-hero { flex-direction: column; align-items: stretch; }
  .page-title { font-size: 32px; }
}
</style>
