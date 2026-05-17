<template>
  <div class="portfolio-page">
    <section class="toolbar">
      <div>
        <p class="eyebrow">Portfolio</p>
        <h1>组合管理</h1>
      </div>
      <div class="toolbar-actions">
        <el-select v-model="selectedPortfolioId" placeholder="选择组合" class="portfolio-select">
          <el-option v-for="item in portfolios" :key="item.id" :label="item.name" :value="item.id" />
        </el-select>
        <el-button :icon="Refresh" :loading="loading" @click="reload">刷新</el-button>
      </div>
    </section>

    <section class="create-bar">
      <el-input v-model="newPortfolio.name" placeholder="新组合名称" />
      <el-input-number v-model="newPortfolio.initial_cash" :min="0" :step="100000" controls-position="right" />
      <el-button type="primary" :loading="creating" @click="createPortfolio">新建组合</el-button>
    </section>

    <template v-if="currentPortfolio">
      <section class="metrics">
        <div v-for="item in metricCards" :key="item.label" class="metric">
          <span>{{ item.label }}</span>
          <strong :class="item.className">{{ item.value }}</strong>
        </div>
      </section>

      <section class="content-grid">
        <div class="panel positions-panel">
          <div class="panel-header">
            <h2>当前持仓</h2>
            <el-button size="small" :loading="savingSnapshot" @click="saveSnapshot">保存快照</el-button>
          </div>
          <el-table :data="positions" empty-text="暂无持仓" class="data-table">
            <el-table-column label="标的" min-width="150">
              <template #default="{ row }">
                <div class="stock-cell">
                  <strong>{{ row.stock_name || row.stock_code }}</strong>
                  <span>{{ row.stock_code }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column prop="quantity" label="数量" align="right" width="100" />
            <el-table-column label="成本" align="right" width="110">
              <template #default="{ row }">{{ formatMoney(row.avg_cost) }}</template>
            </el-table-column>
            <el-table-column label="现价" align="right" width="110">
              <template #default="{ row }">{{ formatMoney(row.market_price) }}</template>
            </el-table-column>
            <el-table-column label="市值" align="right" width="130">
              <template #default="{ row }">{{ formatMoney(row.market_value) }}</template>
            </el-table-column>
            <el-table-column label="浮盈亏" align="right" width="130">
              <template #default="{ row }">
                <span :class="pnlClass(row.unrealized_pnl)">{{ formatMoney(row.unrealized_pnl) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="权重" align="right" width="90">
              <template #default="{ row }">{{ formatPercent(row.weight) }}</template>
            </el-table-column>
          </el-table>
        </div>

        <div class="panel trade-panel">
          <h2>录入交易</h2>
          <el-form label-position="top" class="trade-form">
            <el-form-item label="交易类型">
              <el-segmented v-model="transaction.side" :options="sideOptions" />
            </el-form-item>
            <el-form-item v-if="isTradeSide" label="股票代码">
              <el-input v-model="transaction.stock_code" placeholder="sh600000" />
            </el-form-item>
            <el-form-item v-if="isTradeSide" label="股票名称">
              <el-input v-model="transaction.stock_name" placeholder="可选" />
            </el-form-item>
            <div class="form-row">
              <el-form-item v-if="isTradeSide" label="数量">
                <el-input-number v-model="transaction.quantity" :min="0" :step="100" controls-position="right" />
              </el-form-item>
              <el-form-item :label="isTradeSide ? '价格' : '金额'">
                <el-input-number v-model="transaction.price" :min="0" :step="1" controls-position="right" />
              </el-form-item>
            </div>
            <el-form-item v-if="isTradeSide" label="费用">
              <el-input-number v-model="transaction.fee" :min="0" :step="1" controls-position="right" />
            </el-form-item>
            <el-form-item label="原因">
              <el-input v-model="transaction.reason" type="textarea" :rows="3" placeholder="例如：Score20 Top30 调仓" />
            </el-form-item>
            <el-button type="primary" :loading="submitting" class="submit-button" @click="submitTransaction">
              保存交易
            </el-button>
          </el-form>
        </div>
      </section>

      <section class="panel transactions-panel">
        <h2>交易流水</h2>
        <el-table :data="transactions" empty-text="暂无交易" class="data-table">
          <el-table-column label="日期" width="150">
            <template #default="{ row }">{{ formatDate(row.trade_date) }}</template>
          </el-table-column>
          <el-table-column label="类型" width="100">
            <template #default="{ row }">
              <el-tag :type="sideTag(row.side)" effect="plain">{{ sideLabel(row.side) }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="标的" min-width="150">
            <template #default="{ row }">{{ row.stock_name || row.stock_code || '--' }}</template>
          </el-table-column>
          <el-table-column prop="quantity" label="数量" align="right" width="100" />
          <el-table-column label="价格" align="right" width="110">
            <template #default="{ row }">{{ formatMoney(row.price) }}</template>
          </el-table-column>
          <el-table-column label="金额" align="right" width="130">
            <template #default="{ row }">{{ formatMoney(row.amount) }}</template>
          </el-table-column>
          <el-table-column prop="reason" label="原因" min-width="220" />
        </el-table>
      </section>
    </template>

    <el-empty v-else class="empty-state" description="新建一个组合开始记录交易" />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import { portfolioApi, type Portfolio, type PortfolioPosition, type PortfolioTransaction } from '@/api/portfolios'

const loading = ref(false)
const creating = ref(false)
const submitting = ref(false)
const savingSnapshot = ref(false)
const portfolios = ref<Portfolio[]>([])
const positions = ref<PortfolioPosition[]>([])
const transactions = ref<PortfolioTransaction[]>([])
const selectedPortfolioId = ref('')

const newPortfolio = reactive({
  name: '',
  initial_cash: 1000000
})

const transaction = reactive({
  side: 'BUY',
  stock_code: '',
  stock_name: '',
  quantity: 100,
  price: 10,
  fee: 0,
  reason: ''
})

const sideOptions = [
  { label: '买入', value: 'BUY' },
  { label: '卖出', value: 'SELL' },
  { label: '入金', value: 'CASH_IN' },
  { label: '出金', value: 'CASH_OUT' },
  { label: '分红', value: 'DIVIDEND' }
]

const currentPortfolio = computed(() => portfolios.value.find(item => item.id === selectedPortfolioId.value) || null)
const isTradeSide = computed(() => transaction.side === 'BUY' || transaction.side === 'SELL')

const metricCards = computed(() => {
  const summary = currentPortfolio.value?.summary
  return [
    { label: '总资产', value: formatMoney(summary?.total_value), className: '' },
    { label: '现金', value: formatMoney(summary?.cash), className: '' },
    { label: '持仓市值', value: formatMoney(summary?.positions_value), className: '' },
    { label: '累计收益', value: formatMoney(summary?.total_return), className: pnlClass(summary?.total_return || 0) },
    { label: '收益率', value: formatPercent(summary?.total_return_pct), className: pnlClass(summary?.total_return || 0) },
    { label: '持仓数', value: String(summary?.position_count ?? 0), className: '' }
  ]
})

watch(selectedPortfolioId, (id) => {
  if (id) {
    loadPortfolioDetails(id)
  }
})

async function reload() {
  loading.value = true
  try {
    const response = await portfolioApi.listPortfolios()
    portfolios.value = response.items
    if (!selectedPortfolioId.value && response.items.length) {
      selectedPortfolioId.value = response.items[0]!.id
    }
    if (selectedPortfolioId.value) {
      await loadPortfolioDetails(selectedPortfolioId.value)
    }
  } finally {
    loading.value = false
  }
}

async function loadPortfolioDetails(id: string) {
  const [portfolio, positionResponse, transactionResponse] = await Promise.all([
    portfolioApi.getPortfolio(id),
    portfolioApi.getPositions(id),
    portfolioApi.getTransactions(id)
  ])
  const index = portfolios.value.findIndex(item => item.id === id)
  if (index >= 0) portfolios.value[index] = portfolio
  positions.value = positionResponse.items
  transactions.value = transactionResponse.items
}

async function createPortfolio() {
  if (!newPortfolio.name.trim()) {
    ElMessage.error('请输入组合名称')
    return
  }
  creating.value = true
  try {
    const portfolio = await portfolioApi.createPortfolio({
      name: newPortfolio.name.trim(),
      initial_cash: newPortfolio.initial_cash
    })
    portfolios.value.unshift(portfolio)
    selectedPortfolioId.value = portfolio.id
    newPortfolio.name = ''
    ElMessage.success('组合已创建')
  } finally {
    creating.value = false
  }
}

async function submitTransaction() {
  if (!selectedPortfolioId.value) return
  submitting.value = true
  try {
    await portfolioApi.createTransaction(selectedPortfolioId.value, {
      side: transaction.side,
      stock_code: isTradeSide.value ? transaction.stock_code : undefined,
      stock_name: isTradeSide.value ? transaction.stock_name : undefined,
      quantity: isTradeSide.value ? transaction.quantity : 0,
      price: transaction.price,
      fee: isTradeSide.value ? transaction.fee : 0,
      reason: transaction.reason
    })
    transaction.reason = ''
    await reload()
    ElMessage.success('交易已保存')
  } finally {
    submitting.value = false
  }
}

async function saveSnapshot() {
  if (!selectedPortfolioId.value) return
  savingSnapshot.value = true
  try {
    await portfolioApi.createSnapshot(selectedPortfolioId.value)
    ElMessage.success('组合快照已保存')
  } finally {
    savingSnapshot.value = false
  }
}

function formatMoney(value?: number | null) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '--'
  return numeric.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function formatPercent(value?: number | null) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '--'
  return `${(numeric * 100).toFixed(2)}%`
}

function formatDate(value: string | null) {
  if (!value) return '--'
  return value.replace('T', ' ').slice(0, 16)
}

function pnlClass(value: number) {
  if (value > 0) return 'positive'
  if (value < 0) return 'negative'
  return ''
}

function sideLabel(side: string) {
  return {
    BUY: '买入',
    SELL: '卖出',
    CASH_IN: '入金',
    CASH_OUT: '出金',
    DIVIDEND: '分红'
  }[side] || side
}

function sideTag(side: string) {
  if (side === 'BUY' || side === 'CASH_IN' || side === 'DIVIDEND') return 'success'
  if (side === 'SELL' || side === 'CASH_OUT') return 'warning'
  return 'info'
}

onMounted(reload)
</script>

<style scoped lang="scss">
.portfolio-page {
  min-height: 100vh;
  padding: 0;
  color: var(--color-text-primary);
}

.toolbar,
.panel-header,
.create-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
}

.eyebrow {
  margin: 0 0 4px;
  color: var(--color-primary);
  font-size: 13px;
  font-weight: 510;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

h1,
h2 {
  margin: 0;
  color: var(--color-text-primary);
}

h1 {
  font-size: 28px;
}

h2 {
  font-size: 18px;
  font-weight: 590;
}

.toolbar-actions {
  display: flex;
  gap: 10px;
}

.portfolio-select {
  width: 240px;
}

.create-bar,
.panel,
.metric {
  border: 1px solid var(--color-border);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.03);
  box-shadow: var(--box-shadow-light);
}

.create-bar {
  margin-top: 18px;
  padding: 14px;
}

.metrics {
  display: grid;
  grid-template-columns: repeat(6, minmax(120px, 1fr));
  gap: 12px;
  margin-top: 18px;
}

.metric {
  min-height: 84px;
  padding: 14px;
}

.metric span {
  display: block;
  color: var(--color-text-secondary);
  font-size: 12px;
}

.metric strong {
  display: block;
  margin-top: 8px;
  font-size: 24px;
  font-weight: 590;
  color: var(--color-text-primary);
}

.content-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 360px;
  gap: 18px;
  margin-top: 18px;
}

.panel {
  padding: 18px;
}

.data-table {
  margin-top: 14px;
}

.stock-cell {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.stock-cell span {
  color: var(--color-text-placeholder);
  font-size: 12px;
}

.form-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}

.trade-form {
  margin-top: 16px;
}

.submit-button {
  width: 100%;
}

.transactions-panel {
  margin-top: 18px;
}

.positive {
  color: var(--color-success);
}

.negative {
  color: var(--color-danger);
}

.empty-state {
  margin-top: 80px;
}

@media (max-width: 1180px) {
  .metrics,
  .content-grid {
    grid-template-columns: 1fr;
  }
}
</style>
