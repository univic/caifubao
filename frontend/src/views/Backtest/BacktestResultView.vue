<template>
  <div class="backtest-result-page">
    <!-- Loading -->
    <div v-if="loading" class="loading-state">
      <el-skeleton :rows="8" animated />
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

    <!-- Main Content -->
    <template v-if="bt">
      <!-- Hero Section -->
      <header class="page-hero">
        <div class="hero-top">
          <router-link to="/backtest" class="back-link">
            <el-icon><ArrowLeft /></el-icon> 返回列表
          </router-link>
          <span class="status-tag" :class="statusClass(bt.status)">
            {{ statusLabel(bt.status) }}
          </span>
        </div>
        <div class="hero-main">
          <h1 class="page-title">{{ bt.name }}</h1>
          <div class="hero-meta">
            <span class="meta-stock">{{ bt.stock_name || bt.stock_code }}</span>
            <span class="meta-divider">·</span>
            <span class="meta-strategy">{{ strategyLabel(bt.strategy) }}</span>
            <span class="meta-divider">·</span>
            <span class="meta-date">{{ formatDate(bt.start_date) }} — {{ formatDate(bt.end_date) }}</span>
          </div>
        </div>
        <div class="hero-actions">
          <el-button class="btn-ghost" :icon="Refresh" :loading="loading" @click="fetchResult">刷新</el-button>
        </div>
      </header>

      <!-- Error Message for Failed -->
      <el-alert
        v-if="bt.status === 'FAILED' && bt.error_message"
        class="error-alert"
        type="error"
        :title="bt.error_message"
        show-icon
        :closable="false"
      />

      <!-- Metrics Cards -->
      <div v-if="bt.status === 'COMPLETED'" class="metrics-grid">
        <div class="metric-card">
          <span class="metric-label">总收益率</span>
          <span class="metric-value" :class="pnlClass(bt.total_return)">
            {{ formatPercent(bt.total_return_pct) }}
          </span>
          <span class="metric-sub" :class="pnlClass(bt.total_return)">
            {{ formatMoney(bt.total_return) }}
          </span>
        </div>
        <div class="metric-card">
          <span class="metric-label">年化收益</span>
          <span class="metric-value mono" :class="pnlClass(bt.annualized_return)">
            {{ formatPercent(bt.annualized_return) }}
          </span>
        </div>
        <div class="metric-card">
          <span class="metric-label">最大回撤</span>
          <span class="metric-value mono danger">
            {{ formatPercent(bt.max_drawdown) }}
          </span>
          <span v-if="bt.max_drawdown_duration" class="metric-sub muted">
            持续 {{ bt.max_drawdown_duration }} 天
          </span>
        </div>
        <div class="metric-card">
          <span class="metric-label">夏普比率</span>
          <span class="metric-value mono">
            {{ formatNumber(bt.sharpe_ratio) }}
          </span>
        </div>
        <div class="metric-card">
          <span class="metric-label">胜率</span>
          <span class="metric-value mono">
            {{ formatPercent(bt.win_rate) }}
          </span>
          <span class="metric-sub muted">
            {{ bt.profit_trades }} 盈 / {{ bt.loss_trades }} 亏
          </span>
        </div>
        <div class="metric-card">
          <span class="metric-label">交易次数</span>
          <span class="metric-value mono">
            {{ bt.total_trades }}
          </span>
          <span class="metric-sub muted">
            初始资金 {{ formatMoney(bt.initial_cash) }}
          </span>
        </div>
      </div>

      <!-- Friction & Benchmark row -->
      <div v-if="bt.total_commission || bt.benchmark_return_pct !== undefined" class="metrics-grid" style="margin-top: 16px;">
        <template v-if="bt.total_commission">
          <div class="metric-card">
            <span class="metric-label">总佣金</span>
            <span class="metric-value mono">¥{{ formatNumber(bt.total_commission) }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">总印花税</span>
            <span class="metric-value mono">¥{{ formatNumber(bt.total_stamp_duty) }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">总滑点</span>
            <span class="metric-value mono">¥{{ formatNumber(bt.total_slippage) }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">毛收益（费前）</span>
            <span class="metric-value mono" :class="pnlClass(bt.gross_return_pct)">
              {{ formatPercent(bt.gross_return_pct) }}
            </span>
            <span class="metric-sub" :class="pnlClass(bt.gross_return)">
              {{ formatMoney(bt.gross_return) }}
            </span>
          </div>
        </template>
        <template v-if="bt.benchmark_return_pct !== undefined">
          <div class="metric-card">
            <span class="metric-label">基准收益</span>
            <span class="metric-value mono" :class="pnlClass(bt.benchmark_return_pct)">
              {{ formatPercent(bt.benchmark_return_pct) }}
            </span>
            <span class="metric-sub muted">{{ bt.benchmark_code || '沪深300' }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">超额收益</span>
            <span class="metric-value mono" :class="pnlClass(bt.excess_return_pct)">
              {{ formatPercent(bt.excess_return_pct) }}
            </span>
            <span class="metric-sub" :class="pnlClass(bt.excess_return)">
              {{ formatMoney(bt.excess_return) }}
            </span>
          </div>
          <div class="metric-card">
            <span class="metric-label">信息比率</span>
            <span class="metric-value mono">{{ formatNumber(bt.information_ratio) }}</span>
          </div>
        </template>
      </div>

      <!-- Score Config (when available) -->
      <div v-if="bt.score_config" class="meta-card" style="margin-top: 16px;">
        <div class="meta-item">
          <span class="meta-label">评分周期</span>
          <span class="meta-value mono-text">Score{{ bt.score_config.horizon || bt.horizon || '--' }}</span>
        </div>
        <div v-if="bt.score_config.entry_threshold" class="meta-item">
          <span class="meta-label">买入阈值</span>
          <span class="meta-value mono-text">{{ bt.score_config.entry_threshold }}</span>
        </div>
        <div v-if="bt.score_config.exit_threshold" class="meta-item">
          <span class="meta-label">退出阈值</span>
          <span class="meta-value mono-text">{{ bt.score_config.exit_threshold }}</span>
        </div>
        <div v-if="bt.score_config.stop_loss_pct" class="meta-item">
          <span class="meta-label">止损比例</span>
          <span class="meta-value mono-text">{{ bt.score_config.stop_loss_pct }}%</span>
        </div>
        <div v-if="bt.score_config.score_delta" class="meta-item">
          <span class="meta-label">评分变动阈值</span>
          <span class="meta-value mono-text">{{ bt.score_config.score_delta }}</span>
        </div>
        <div v-if="bt.score_config.model_version" class="meta-item">
          <span class="meta-label">模型版本</span>
          <span class="meta-value mono-text small">v{{ bt.score_config.model_version }}</span>
        </div>
      </div>

      <!-- Trades Section -->
      <div v-if="bt.status === 'COMPLETED'" class="content-card">
        <div class="card-header">
          <div>
            <h3 class="card-title">交易记录</h3>
            <p class="card-desc">共 {{ bt.trades?.length || 0 }} 笔交易</p>
          </div>
        </div>

        <div class="table-wrapper">
          <el-table
            :data="bt.trades || []"
            class="linear-table"
            empty-text="暂无交易记录"
          >
            <el-table-column label="日期" width="140">
              <template #default="{ row }">
                <span class="mono-text">{{ formatDate(row.date) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="方向" width="80">
              <template #default="{ row }">
                <span class="side-tag" :class="sideClass(row.side)">
                  {{ sideLabel(row.side) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column label="价格" width="110" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatMoney(row.price) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="数量" width="100" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ row.quantity }}</span>
              </template>
            </el-table-column>

            <el-table-column label="金额" width="130" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatMoney(row.amount) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="盈亏" width="130" align="right">
              <template #default="{ row }">
                <span class="mono-text" :class="pnlClass(row.pnl)">{{ formatMoney(row.pnl) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="原因" min-width="200">
              <template #default="{ row }">
                <span class="reason-text">{{ row.reason || '--' }}</span>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>

      <!-- Daily Values Section -->
      <div v-if="bt.status === 'COMPLETED' && bt.daily_values?.length" class="content-card">
        <div class="card-header">
          <div>
            <h3 class="card-title">每日权益</h3>
            <p class="card-desc">共 {{ bt.daily_values.length }} 个交易日</p>
          </div>
        </div>

        <div class="table-wrapper">
          <el-table
            :data="bt.daily_values"
            class="linear-table"
            max-height="400"
          >
            <el-table-column label="日期" width="140">
              <template #default="{ row }">
                <span class="mono-text">{{ formatDate(row.date) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="总资产" width="140" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatMoney(row.value) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="现金" width="140" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatMoney(row.cash) }}</span>
              </template>
            </el-table-column>

            <el-table-column label="持仓市值" width="140" align="right">
              <template #default="{ row }">
                <span class="mono-text">{{ formatMoney(row.positions_value) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>

      <!-- Per-stock contributions (multi-stock only) -->
      <div v-if="bt.per_stock_contributions?.length" class="content-card">
        <div class="card-header">
          <div>
            <h3 class="card-title">个股贡献</h3>
            <p class="card-desc">共 {{ bt.per_stock_contributions.length }} 只标的</p>
          </div>
        </div>

        <div class="table-wrapper">
          <el-table
            :data="bt.per_stock_contributions"
            stripe
            size="small"
            class="linear-table"
          >
            <el-table-column prop="stock_code" label="代码" width="120">
              <template #default="{ row }">
                <span class="mono-text">{{ row.stock_code }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="stock_name" label="名称" width="140" />
            <el-table-column label="已实现盈亏" width="140" align="right">
              <template #default="{ row }">
                <span class="mono-text" :class="pnlClass(row.realized_pnl)">
                  ¥{{ formatMoney(row.realized_pnl) }}
                </span>
              </template>
            </el-table-column>
            <el-table-column prop="trades" label="交易次数" width="100" align="right" />
          </el-table>
        </div>
      </div>

      <!-- Meta Info -->
      <div class="meta-card">
        <div class="meta-item">
          <span class="meta-label">回测 ID</span>
          <span class="meta-value mono-text small">{{ bt.id }}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">创建时间</span>
          <span class="meta-value mono-text small">{{ formatDateTime(bt.created_at) }}</span>
        </div>
        <div class="meta-item" v-if="bt.completed_at">
          <span class="meta-label">完成时间</span>
          <span class="meta-value mono-text small">{{ formatDateTime(bt.completed_at) }}</span>
        </div>
      </div>
    </template>

    <!-- Not Found -->
    <el-empty v-if="!loading && !errorMessage && !bt" description="未找到回测记录" />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { Refresh, ArrowLeft } from '@element-plus/icons-vue'
import { backtestApi, type BacktestResult } from '@/api/backtest'

const route = useRoute()
const loading = ref(false)
const errorMessage = ref('')
const bt = ref<BacktestResult | null>(null)

const id = route.params.id as string

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

function formatMoney(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
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
    MA_CROSS: '均线交叉策略',
    BUY_HOLD: '买入持有策略',
    SCORE_THRESHOLD: '评分阈值策略',
    SCORE_MOMENTUM: '评分动量策略',
    TOP_N_ROTATION: 'Top-N 轮动策略',
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

function sideLabel(value: string) {
  return value === 'BUY' ? '买入' : value === 'SELL' ? '卖出' : value
}

function sideClass(value: string) {
  return value === 'BUY' ? 'buy' : value === 'SELL' ? 'sell' : ''
}

async function fetchResult() {
  if (!id) return
  loading.value = true
  errorMessage.value = ''
  try {
    bt.value = await backtestApi.get(id)
  } catch (error: any) {
    console.error(error)
    if (error?.response?.status === 404) {
      errorMessage.value = '该回测记录不存在。'
    } else {
      errorMessage.value = '回测详情加载失败，请稍后重试。'
    }
  } finally {
    loading.value = false
  }
}

onMounted(fetchResult)
</script>

<style scoped lang="scss">
.backtest-result-page {
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

/* Button overrides */
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

/* Hero Section */
.page-hero {
  margin-bottom: 32px;
}

.hero-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
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

.hero-main {
  margin-bottom: 16px;
}

.page-title {
  font-size: 36px;
  font-weight: 510;
  line-height: 1;
  letter-spacing: -0.792px;
  margin: 0 0 12px 0;
  color: var(--color-text-primary);
}

.hero-meta {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 15px;
  color: var(--color-text-tertiary);

  .meta-stock {
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

.hero-actions {
  display: flex;
  gap: 12px;
}

/* Status Tags */
.status-tag {
  font-size: 11px;
  font-weight: 510;
  padding: 4px 12px;
  border-radius: 9999px;
  text-transform: uppercase;

  &.success {
    color: #10b981;
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid rgba(16, 185, 129, 0.2);
  }

  &.info {
    color: #60a5fa;
    background: rgba(96, 165, 250, 0.1);
    border: 1px solid rgba(96, 165, 250, 0.2);
  }

  &.warning {
    color: #fbbf24;
    background: rgba(251, 191, 36, 0.1);
    border: 1px solid rgba(251, 191, 36, 0.2);
  }

  &.danger {
    color: #ef4444;
    background: rgba(239, 68, 68, 0.1);
    border: 1px solid rgba(239, 68, 68, 0.2);
  }
}

/* Metrics Grid */
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
  margin-bottom: 32px;

  @media (max-width: 1024px) {
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
  font-size: 13px;
  color: var(--color-text-tertiary);

  &.positive { color: #ef4444; }
  &.negative { color: #22c55e; }
  &.muted { color: var(--color-text-quaternary); }
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

/* Cell Styles */
.mono-text {
  font-family: var(--font-mono);
  font-size: 13px;

  &.small {
    font-size: 12px;
    color: var(--color-text-quaternary);
  }

  &.positive { color: #ef4444; }
  &.negative { color: #22c55e; }
  &.danger { color: #fb7185; }
  &.muted { color: var(--color-text-quaternary); }
}

.reason-text {
  font-size: 13px;
  color: var(--color-text-tertiary);
  display: inline-block;
  max-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.position-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
  align-items: flex-end;
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
}

@media (max-width: 1024px) {
  .backtest-result-page {
    padding: 24px;
  }

  .page-title {
    font-size: 28px;
  }
}
</style>
