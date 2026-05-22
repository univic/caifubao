<template>
  <div class="discovery-page">
    <!-- Hero Section -->
    <header class="page-hero">
      <p class="eyebrow">Strategy Discovery</p>
      <h1 class="page-title">策略发现</h1>
      <p class="subtitle">跨策略对比与全市场扫描，识别最优策略-标的组合，内置反过拟合护栏。</p>
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

    <!-- Tabs -->
    <div class="content-card">
      <el-tabs v-model="activeTab" class="linear-tabs" @tab-change="handleTabChange">
        <!-- Panel A: Strategy Comparison -->
        <el-tab-pane label="单股票对比" name="compare">
          <div class="tab-panel">
            <!-- Compare Form -->
            <div class="form-row">
              <el-input
                v-model="compareForm.stock_code"
                placeholder="股票代码 (如 sh600519)"
                clearable
                class="linear-input stock-input"
              />
              <el-date-picker
                v-model="compareForm.start_date"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="开始日期"
                class="linear-picker"
              />
              <el-date-picker
                v-model="compareForm.end_date"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="结束日期"
                class="linear-picker"
              />
              <el-button
                class="btn-primary"
                :loading="compareLoading"
                :disabled="!canCompare"
                @click="runCompare"
              >
                开始对比
              </el-button>
            </div>

            <!-- Compare Results Table -->
            <div v-if="compareResults.length > 0" class="results-section">
              <div class="section-header">
                <h3 class="section-title">对比结果</h3>
                <span class="section-desc">{{ compareForm.stock_code }} · {{ compareResults.length }} 个策略</span>
              </div>
              <div class="table-wrapper" v-loading="compareLoading">
                <el-table
                  :data="sortedCompareResults"
                  class="linear-table"
                  empty-text="无对比结果"
                  :row-class-name="compareRowClass"
                >
                  <el-table-column label="策略" min-width="180">
                    <template #default="{ row }">
                      <div class="strategy-cell">
                        <span class="strategy-name">{{ strategyLabel(row.strategy) }}</span>
                        <span v-if="row.error" class="strategy-error">{{ row.error }}</span>
                      </div>
                    </template>
                  </el-table-column>
                  <el-table-column label="收益率" width="100" align="right">
                    <template #default="{ row }">
                      <span class="mono-value" :class="pnlClass(row.total_return_pct)">
                        {{ formatPercent(row.total_return_pct) }}
                      </span>
                    </template>
                  </el-table-column>
                  <el-table-column label="夏普" width="80" align="right">
                    <template #default="{ row }">
                      <span class="mono-value">{{ formatNumber(row.sharpe_ratio) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="最大回撤" width="100" align="right">
                    <template #default="{ row }">
                      <span class="mono-value danger">{{ formatPercent(row.max_drawdown) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="胜率" width="80" align="right">
                    <template #default="{ row }">
                      <span class="mono-value">{{ formatPercent(row.win_rate) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="交易数" width="80" align="right">
                    <template #default="{ row }">
                      <span class="mono-value">{{ row.total_trades }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="超额收益" width="100" align="right">
                    <template #default="{ row }">
                      <span class="mono-value" :class="pnlClass(row.excess_return_pct)">
                        {{ formatPercent(row.excess_return_pct) }}
                      </span>
                    </template>
                  </el-table-column>
                  <el-table-column label="信息比率" width="90" align="right">
                    <template #default="{ row }">
                      <span class="mono-value">{{ formatNumber(row.information_ratio) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="综合分" width="80" align="right">
                    <template #default="{ row }">
                      <span class="composite-score" :class="{ best: row._isBest }">
                        {{ formatNumber(row.composite_score) }}
                      </span>
                    </template>
                  </el-table-column>
                  <el-table-column label="标记" min-width="200">
                    <template #default="{ row }">
                      <div class="flags-cell">
                        <el-tag
                          v-for="flag in parseFlags(row.flags)"
                          :key="flag.text"
                          :type="flag.type"
                          size="small"
                          class="flag-tag"
                          effect="dark"
                        >
                          {{ flag.text }}
                        </el-tag>
                        <el-popover
                          v-if="row.composite_breakdown"
                          placement="top"
                          :width="240"
                          trigger="hover"
                        >
                          <template #reference>
                            <el-tag size="small" class="flag-tag breakdown-tag" effect="dark">
                              分项
                            </el-tag>
                          </template>
                          <div class="breakdown-popover">
                            <div
                              v-for="(val, key) in row.composite_breakdown"
                              :key="key"
                              class="breakdown-row"
                            >
                              <span class="breakdown-key">{{ breakLabel(key) }}</span>
                              <span class="breakdown-val">{{ formatNumber(val) }}</span>
                            </div>
                          </div>
                        </el-popover>
                      </div>
                    </template>
                  </el-table-column>
                </el-table>
              </div>
            </div>

            <!-- Compare Empty -->
            <div v-else-if="compareSubmitted && !compareLoading" class="empty-section">
              <el-empty description="暂无对比结果，请检查股票代码与日期范围" />
            </div>
          </div>
        </el-tab-pane>

        <!-- Panel B: Market Scan -->
        <el-tab-pane label="全市场扫描" name="scan">
          <div class="tab-panel">
            <!-- Scan Form -->
            <div class="form-row">
              <el-select
                v-model="scanForm.strategy"
                placeholder="选择策略"
                class="linear-select strategy-select"
              >
                <el-option label="均线交叉 (MA_CROSS)" value="MA_CROSS" />
                <el-option label="买入持有 (BUY_HOLD)" value="BUY_HOLD" />
                <el-option label="评分阈值 (SCORE_THRESHOLD)" value="SCORE_THRESHOLD" />
                <el-option label="评分动量 (SCORE_MOMENTUM)" value="SCORE_MOMENTUM" />
                <el-option label="Top-N 轮动 (TOP_N_ROTATION)" value="TOP_N_ROTATION" />
                <el-option label="多周期共识 (MULTI_HORIZON_CONSENSUS)" value="MULTI_HORIZON_CONSENSUS" />
              </el-select>
              <el-select
                v-if="isScoreStrategy(scanForm.strategy)"
                v-model="scanForm.horizon"
                placeholder="评分周期"
                class="linear-select horizon-select"
              >
                <el-option label="Score5 (5天)" :value="5" />
                <el-option label="Score20 (20天)" :value="20" />
                <el-option label="Score60 (60天)" :value="60" />
              </el-select>
              <el-date-picker
                v-model="scanForm.start_date"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="开始日期"
                class="linear-picker"
              />
              <el-date-picker
                v-model="scanForm.end_date"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="结束日期"
                class="linear-picker"
              />
              <el-button
                class="btn-primary"
                :loading="scanLoading"
                :disabled="!canScan"
                @click="runScan"
              >
                开始扫描
              </el-button>
            </div>

            <!-- Scan Polling Progress -->
            <el-alert
              v-if="scanPolling"
              class="polling-alert"
              type="info"
              :closable="false"
              show-icon
            >
              <template #title>
                <el-icon class="is-loading" style="margin-right: 8px"><Loading /></el-icon>
                {{ scanPollMessage }}
              </template>
            </el-alert>

            <!-- Bonferroni Info -->
            <el-alert
              v-if="scanBonferroniThreshold !== null"
              class="bonferroni-alert"
              type="info"
              :closable="false"
              show-icon
            >
              <template #title>
                Bonferroni 校正阈值：p &lt; {{ scanBonferroniThreshold.toExponential(2) }}
                （基于 {{ scanTotalStocks || 'N' }} 个标的）
              </template>
            </el-alert>

            <!-- Scan Results Table -->
            <div v-if="scanItems.length > 0" class="results-section">
              <div class="section-header">
                <h3 class="section-title">扫描结果</h3>
                <span class="section-desc">
                  {{ scanForm.strategy }} · 共 {{ scanTotal }} 条 · 第 {{ scanPage }} / {{ Math.ceil(scanTotal / scanPerPage) || 1 }} 页
                </span>
              </div>
              <div class="table-wrapper" v-loading="scanLoading">
                <el-table
                  :data="scanItems"
                  class="linear-table"
                  empty-text="无扫描结果"
                >
                  <el-table-column label="股票" min-width="150">
                    <template #default="{ row }">
                      <div class="stock-cell">
                        <router-link :to="`/quote/${row.stock_code}`" class="stock-link">
                          {{ row.stock_name || row.stock_code }}
                        </router-link>
                        <span class="stock-code-mono">{{ row.stock_code }}</span>
                      </div>
                    </template>
                  </el-table-column>
                  <el-table-column label="收益率" width="100" align="right">
                    <template #default="{ row }">
                      <span class="mono-value" :class="pnlClass(row.total_return_pct)">
                        {{ formatPercent(row.total_return_pct) }}
                      </span>
                    </template>
                  </el-table-column>
                  <el-table-column label="夏普" width="80" align="right">
                    <template #default="{ row }">
                      <span class="mono-value">{{ formatNumber(row.sharpe_ratio) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="最大回撤" width="100" align="right">
                    <template #default="{ row }">
                      <span class="mono-value danger">{{ formatPercent(row.max_drawdown) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="交易数" width="80" align="right">
                    <template #default="{ row }">
                      <span class="mono-value">{{ row.total_trades }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="胜率" width="80" align="right">
                    <template #default="{ row }">
                      <span class="mono-value">{{ formatPercent(row.win_rate) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="超额收益" width="100" align="right">
                    <template #default="{ row }">
                      <span class="mono-value" :class="pnlClass(row.excess_return_pct)">
                        {{ formatPercent(row.excess_return_pct) }}
                      </span>
                    </template>
                  </el-table-column>
                  <el-table-column label="综合分" width="80" align="right">
                    <template #default="{ row }">
                      <span class="composite-score">{{ formatNumber(row.composite_score) }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="标记" min-width="200">
                    <template #default="{ row }">
                      <div class="flags-cell">
                        <el-tag
                          v-for="flag in parseFlags(row.flags)"
                          :key="flag.text"
                          :type="flag.type"
                          size="small"
                          class="flag-tag"
                          effect="dark"
                        >
                          {{ flag.text }}
                        </el-tag>
                      </div>
                    </template>
                  </el-table-column>
                </el-table>
              </div>

              <!-- Pagination -->
              <div v-if="scanTotal > scanPerPage" class="pagination-wrapper">
                <el-pagination
                  v-model:current-page="scanPage"
                  :page-size="scanPerPage"
                  :total="scanTotal"
                  layout="prev, pager, next"
                  background
                  @current-change="runScan"
                />
              </div>
            </div>

            <!-- Scan Empty -->
            <div v-else-if="scanSubmitted && !scanLoading" class="empty-section">
              <el-empty description="暂无扫描结果，请检查策略与日期范围" />
            </div>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Loading } from '@element-plus/icons-vue'
import { backtestApi, type CompareResult, type ScanItem } from '@/api/backtest'

// --- Tab state ---
const activeTab = ref('compare')
const errorMessage = ref('')

// --- Panel A: Compare ---
const compareLoading = ref(false)
const compareSubmitted = ref(false)
const compareResults = ref<CompareResult[]>([])

const compareForm = reactive({
  stock_code: '',
  start_date: '',
  end_date: '',
  initial_cash: 100000,
  benchmark_code: ''
})

const canCompare = computed(() => {
  return compareForm.stock_code.trim() && compareForm.start_date && compareForm.end_date
})

const sortedCompareResults = computed(() => {
  const items = [...compareResults.value].map(item => ({
    ...item,
    _isBest: false
  }))
  // Sort by composite_score descending (unscored items at bottom)
  items.sort((a, b) => {
    const sa = a.composite_score ?? -Infinity
    const sb = b.composite_score ?? -Infinity
    return sb - sa
  })
  // Mark best (first rankable item)
  const best = items.find(item => (item.composite_score ?? -Infinity) > -Infinity)
  if (best) best._isBest = true
  return items
})

function compareRowClass({ row }: { row: CompareResult & { _isBest?: boolean } }) {
  return row._isBest ? 'best-row' : ''
}

async function runCompare() {
  compareLoading.value = true
  compareSubmitted.value = true
  errorMessage.value = ''
  compareResults.value = []

  try {
    const payload: any = {
      stock_code: compareForm.stock_code.trim(),
      start_date: compareForm.start_date,
      end_date: compareForm.end_date,
      initial_cash: compareForm.initial_cash
    }
    if (compareForm.benchmark_code) {
      payload.benchmark_code = compareForm.benchmark_code
    }
    const data = await backtestApi.compare(payload)
    compareResults.value = data?.results ?? []
    if (compareResults.value.length === 0) {
      ElMessage.warning('没有可用的对比结果')
    } else {
      ElMessage.success(`对比完成，共 ${compareResults.value.length} 个策略`)
    }
  } catch (error: any) {
    console.error(error)
    const msg = error?.response?.data?.message || error?.message || '对比请求失败，请稍后重试。'
    errorMessage.value = msg
    ElMessage.error(msg)
  } finally {
    compareLoading.value = false
  }
}

// --- Panel B: Scan ---
const scanLoading = ref(false)
const scanSubmitted = ref(false)
const scanPolling = ref(false)
const scanPollMessage = ref('')
const scanItems = ref<ScanItem[]>([])
const scanTotal = ref(0)
const scanPage = ref(1)
const scanPerPage = ref(50)
const scanBonferroniThreshold = ref<number | null>(null)
const scanTotalStocks = ref<number | null>(null)

const scanForm = reactive({
  strategy: '',
  start_date: '',
  end_date: '',
  horizon: null as number | null,
  initial_cash: 100000,
  min_trades: 5
})

const canScan = computed(() => {
  return scanForm.strategy && scanForm.start_date && scanForm.end_date
})

function isScoreStrategy(strategy: string): boolean {
  return ['SCORE_THRESHOLD', 'SCORE_MOMENTUM'].includes(strategy)
}

async function runScan() {
  scanLoading.value = true
  scanSubmitted.value = true
  errorMessage.value = ''
  scanItems.value = []

  try {
    const payload: any = {
      strategy: scanForm.strategy,
      start_date: scanForm.start_date,
      end_date: scanForm.end_date,
      initial_cash: scanForm.initial_cash,
      page: scanPage.value,
      per_page: scanPerPage.value,
      min_trades: scanForm.min_trades
    }
    if (isScoreStrategy(scanForm.strategy) && scanForm.horizon) {
      payload.horizon = scanForm.horizon
    }
    const response = await backtestApi.scan(payload)

    // Async dispatch: 202 with task_id — poll for results
    if (response.status === 202) {
      const taskId = response.data?.data?.task_id
      const totalStocks = response.data?.data?.total_stocks
      scanTotalStocks.value = totalStocks ?? null
      if (!taskId) {
        ElMessage.error('异步任务创建失败：未返回 task_id')
        return
      }
      ElMessage.info(`全市场扫描已提交（${totalStocks} 只标的），正在异步执行...`)
      scanPolling.value = true
      scanPollMessage.value = `正在执行全市场扫描 (${totalStocks ?? '?'} 只标的)...`

      // Poll until COMPLETED or FAILED
      let pollCount = 0
      const maxPolls = 120  // 4 minutes at 2s intervals
      while (pollCount < maxPolls) {
        await new Promise(resolve => setTimeout(resolve, 2000))
        pollCount++
        scanPollMessage.value = `扫描中... (${pollCount * 2}s / ${totalStocks ?? '?'} 只标的)`
        const task = await backtestApi.getTask(taskId)
        if (task?.status === 'COMPLETED') {
          scanPolling.value = false
          const result = task?.result ?? {}
          scanItems.value = result?.items ?? []
          scanTotal.value = result?.total ?? 0
          scanBonferroniThreshold.value = result?.bonferroni?.corrected_alpha ?? null
          if (!scanTotalStocks.value) {
            scanTotalStocks.value = result?.total_stocks ?? null
          }
          ElMessage.success(`异步扫描完成，共 ${scanTotal.value} 条结果`)
          return
        }
        if (task?.status === 'FAILED') {
          scanPolling.value = false
          errorMessage.value = task?.error || '异步扫描任务失败'
          ElMessage.error(errorMessage.value)
          return
        }
        // PENDING or RUNNING — continue polling
      }
      scanPolling.value = false
      ElMessage.warning('扫描任务超时，请稍后查看任务结果')
      return
    }

    // Synchronous response (200)
    const data = response.data?.data ?? response.data ?? {}
    scanItems.value = data?.items ?? []
    scanTotal.value = data?.total ?? 0
    scanBonferroniThreshold.value = data?.bonferroni?.corrected_alpha ?? null
    scanTotalStocks.value = data?.total_stocks ?? null
    if (scanItems.value.length === 0) {
      ElMessage.warning('没有可用的扫描结果')
    } else {
      ElMessage.success(`扫描完成，共 ${scanTotal.value} 条结果`)
    }
  } catch (error: any) {
    console.error(error)
    scanPolling.value = false
    const msg = error?.response?.data?.message || error?.message || '扫描请求失败，请稍后重试。'
    errorMessage.value = msg
    ElMessage.error(msg)
  } finally {
    scanLoading.value = false
  }
}

function handleTabChange() {
  errorMessage.value = ''
}

// --- Shared helpers ---

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toFixed(2)
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `${value.toFixed(2)}%`
}

function pnlClass(value: number | null | undefined): string {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return ''
  if (numeric > 0) return 'positive'
  if (numeric < 0) return 'negative'
  return ''
}

function strategyLabel(value: string): string {
  const map: Record<string, string> = {
    MA_CROSS: '均线交叉策略',
    BUY_HOLD: '买入持有策略',
    SCORE_THRESHOLD: '评分阈值策略',
    SCORE_MOMENTUM: '评分动量策略',
    TOP_N_ROTATION: 'Top-N 轮动策略',
    MULTI_HORIZON_CONSENSUS: '多周期共识策略'
  }
  return map[value] || value
}

interface ParsedFlag {
  text: string
  type: 'warning' | 'danger' | 'info'
}

function parseFlags(flags: string[] | undefined): ParsedFlag[] {
  if (!flags || !Array.isArray(flags)) return []
  return flags.map(flag => {
    if (flag.startsWith('concentrated_returns')) {
      const pct = flag.split(':')[1] || '?'
      return { text: `集中收益:${pct}`, type: 'danger' as const }
    }
    if (flag === 'low_sample') return { text: '样本不足', type: 'warning' as const }
    if (flag === 'insufficient_period') return { text: '周期不足', type: 'warning' as const }
    if (flag === 'high_drawdown') return { text: '高回撤', type: 'danger' as const }
    if (flag.startsWith('bonferroni_applied')) {
      const n = flag.split(':')[1] || '?'
      return { text: `Bonferroni 已校正 (n=${n})`, type: 'info' as const }
    }
    if (flag.startsWith('not_significant_after_bonferroni')) {
      return { text: '未通过Bonferroni校正', type: 'warning' as const }
    }
    // Generic fallback
    return { text: flag, type: 'info' as const }
  })
}

function breakLabel(key: string): string {
  const map: Record<string, string> = {
    excess_return: '超额收益',
    max_drawdown: '最大回撤',
    info_ratio: '信息比率',
    sharpe: '夏普比率',
    turnover: '换手率',
    concentration: '集中度'
  }
  return map[key] || key
}
</script>

<style scoped lang="scss">
.discovery-page {
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
  font-size: 18px;
  font-weight: 400;
  color: var(--color-text-tertiary);
  max-width: 600px;
  margin: 0;
  line-height: 1.6;
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

/* Linear Tabs Overrides */
:deep(.linear-tabs) {
  .el-tabs__header {
    margin: 0;
    padding: 0 32px;
    border-bottom: 1px solid var(--color-border-subtle);
  }

  .el-tabs__nav-wrap::after {
    display: none;
  }

  .el-tabs__item {
    color: var(--color-text-tertiary);
    font-size: 15px;
    font-weight: 510;
    height: 52px;
    line-height: 52px;
    padding: 0 24px;

    &.is-active {
      color: var(--color-text-primary);
    }
  }

  .el-tabs__active-bar {
    background-color: var(--color-brand);
  }
}

/* Tab Panel */
.tab-panel {
  padding: 28px 32px 32px;
}

/* Form Row */
.form-row {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 24px;
}

.stock-input {
  width: 200px;
}

.strategy-select {
  width: 240px;
}

.horizon-select {
  width: 160px;
}

/* Input/Picker overrides */
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

:deep(.el-select .el-input__inner) {
  color: var(--color-text-primary) !important;
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
  white-space: nowrap;

  &:hover {
    background: var(--color-brand-accent) !important;
  }

  &:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }
}

/* Bonferroni Alert */
.bonferroni-alert {
  margin-bottom: 24px;
  background-color: rgba(96, 165, 250, 0.1);
  border: 1px solid rgba(96, 165, 250, 0.2);
  color: #93c5fd;
}

/* Results Section */
.results-section {
  margin-top: 4px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 16px;
  padding: 0 4px;
}

.section-title {
  font-size: 16px;
  font-weight: 590;
  color: var(--color-text-primary);
  margin: 0;
}

.section-desc {
  font-size: 13px;
  color: var(--color-text-quaternary);
}

/* Table Overrides */
.table-wrapper {
  margin: 0 -4px;
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
    padding: 14px 8px;
  }

  td.el-table__cell {
    padding: 10px 8px;
    border-bottom: 1px solid var(--color-border-subtle);
  }

  .el-table__row:hover > td {
    background-color: rgba(255, 255, 255, 0.02) !important;
  }

  /* Best strategy row highlight */
  .best-row > td {
    background-color: rgba(16, 185, 129, 0.06) !important;
  }
}

/* Cell Styles */
.strategy-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.strategy-name {
  font-size: 14px;
  font-weight: 510;
  color: var(--color-text-primary);
}

.strategy-error {
  font-size: 12px;
  color: #fb7185;
}

.stock-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.stock-link {
  font-size: 14px;
  font-weight: 510;
  color: var(--color-brand-accent);
  text-decoration: none;

  &:hover {
    text-decoration: underline;
  }
}

.stock-code-mono {
  font-size: 12px;
  font-family: var(--font-mono);
  color: var(--color-text-quaternary);
}

.mono-value {
  font-family: var(--font-mono);
  font-size: 13px;
  color: var(--color-text-secondary);

  &.positive { color: #ef4444; }
  &.negative { color: #22c55e; }
  &.danger { color: #fb7185; }
}

.composite-score {
  font-family: var(--font-mono);
  font-size: 14px;
  font-weight: 590;
  color: var(--color-text-primary);

  &.best {
    color: #10b981;
  }
}

/* Flags */
.flags-cell {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.flag-tag {
  font-size: 11px;
  font-weight: 510;
  border-radius: 4px;
  padding: 2px 8px;
  border: none;
}

.breakdown-tag {
  cursor: pointer;
  background: rgba(113, 112, 255, 0.15) !important;
  color: #828fff !important;
}

.breakdown-popover {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.breakdown-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.breakdown-key {
  font-size: 13px;
  color: #8a8f98;
}

.breakdown-val {
  font-family: var(--font-mono);
  font-size: 13px;
  font-weight: 590;
  color: #f7f8f8;
}

/* Pagination */
.pagination-wrapper {
  display: flex;
  justify-content: center;
  margin-top: 24px;
}

:deep(.el-pagination) {
  --el-pagination-button-bg-color: rgba(255, 255, 255, 0.04);
  --el-pagination-bg-color: transparent;
  --el-pagination-text-color: var(--color-text-secondary);
  --el-pagination-hover-color: var(--color-brand-accent);

  .el-pager li {
    color: var(--color-text-secondary);
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid var(--color-border);
    border-radius: 6px;
    margin: 0 3px;

    &.is-active {
      background: var(--color-brand);
      border-color: var(--color-brand);
      color: #fff;
    }

    &:hover {
      color: #f7f8f8;
    }
  }

  button {
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid var(--color-border);
    border-radius: 6px;
    color: var(--color-text-secondary);

    &:hover {
      color: #f7f8f8;
    }

    &:disabled {
      opacity: 0.3;
      background: transparent;
    }
  }
}

/* Empty */
.empty-section {
  margin-top: 40px;
  margin-bottom: 20px;
}

:deep(.el-empty__description) {
  color: var(--color-text-tertiary);
}

/* Responsive */
@media (max-width: 1024px) {
  .discovery-page {
    padding: 24px;
  }

  .page-title {
    font-size: 32px;
  }

  .form-row {
    flex-direction: column;
    align-items: stretch;
  }

  .stock-input,
  .strategy-select,
  .horizon-select {
    width: 100%;
  }
}
</style>
