<template>
  <div class="backtest-create-page">
    <!-- Hero Section -->
    <header class="page-hero">
      <p class="eyebrow">Backtest</p>
      <h1 class="page-title">新建回测</h1>
      <p class="subtitle">配置策略参数，对历史行情数据进行量化回测。</p>
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

    <!-- Success Alert -->
    <el-alert
      v-if="successMessage"
      class="success-alert"
      type="success"
      :title="successMessage"
      show-icon
      :closable="false"
    />

    <!-- Form Card -->
    <div class="form-card">
      <div class="card-header">
        <h3 class="card-title">回测参数</h3>
        <p class="card-desc">选择标的、策略与时间区间</p>
      </div>

      <el-form
        ref="formRef"
        :model="form"
        :rules="formRules"
        label-position="top"
        class="backtest-form"
        @submit.prevent="handleSubmit"
      >
        <div class="form-grid">
          <!-- Stock Code -->
          <el-form-item label="股票代码" prop="stock_code">
            <el-input
              v-model="form.stock_code"
              placeholder="例如: sh600519"
              clearable
              class="linear-input"
            />
            <template #extra>
              <span class="form-hint">输入交易所前缀: sh（上海）或 sz（深圳）</span>
            </template>
          </el-form-item>

          <!-- Strategy -->
          <el-form-item label="交易策略" prop="strategy">
            <el-select
              v-model="form.strategy"
              placeholder="选择策略"
              class="linear-select"
            >
              <el-option label="均线交叉策略 (MA_CROSS)" value="MA_CROSS" />
              <el-option label="买入持有策略 (BUY_HOLD)" value="BUY_HOLD" />
              <el-option label="评分阈值策略 (SCORE_THRESHOLD)" value="SCORE_THRESHOLD" />
              <el-option label="评分动量策略 (SCORE_MOMENTUM)" value="SCORE_MOMENTUM" />
            </el-select>
            <template #extra>
              <span class="form-hint">
                {{ strategyDescription(form.strategy) }}
              </span>
            </template>
          </el-form-item>

          <!-- Start Date -->
          <el-form-item label="开始日期" prop="start_date">
            <el-date-picker
              v-model="form.start_date"
              type="date"
              value-format="YYYY-MM-DD"
              placeholder="选择开始日期"
              class="linear-picker"
            />
          </el-form-item>

          <!-- End Date -->
          <el-form-item label="结束日期" prop="end_date">
            <el-date-picker
              v-model="form.end_date"
              type="date"
              value-format="YYYY-MM-DD"
              placeholder="选择结束日期"
              class="linear-picker"
            />
          </el-form-item>

          <!-- Initial Cash -->
          <el-form-item label="初始资金" prop="initial_cash">
            <el-input-number
              v-model="form.initial_cash"
              :min="10000"
              :step="10000"
              :precision="2"
              controls-position="right"
              class="linear-input-number"
            />
            <template #extra>
              <span class="form-hint">默认 100,000 元</span>
            </template>
          </el-form-item>
        </div>

        <!-- Score-driven params (visible when strategy is SCORE_THRESHOLD or SCORE_MOMENTUM) -->
        <div
          v-if="['SCORE_THRESHOLD', 'SCORE_MOMENTUM'].includes(form.strategy)"
          class="form-grid"
          style="margin-top: 0; padding-top: 0; border-top: 1px solid var(--color-border-subtle);"
        >
          <el-form-item label="评分模型版本" prop="model_version">
            <el-input
              v-model="form.model_version"
              placeholder="请输入评分数据中的模型版本"
              clearable
              class="linear-input"
            />
            <template #extra>
              <span class="form-hint">仅使用该版本的评分，不会自动选择其他版本</span>
            </template>
          </el-form-item>

          <el-form-item label="评分周期">
            <el-select v-model="form.horizon" placeholder="选择周期" class="linear-select">
              <el-option label="Score5 (短线 5天)" :value="5" />
              <el-option label="Score20 (波段 20天)" :value="20" />
              <el-option label="Score60 (中线 60天)" :value="60" />
            </el-select>
          </el-form-item>

          <el-form-item v-if="form.strategy === 'SCORE_THRESHOLD'" label="买入阈值">
            <el-input-number
              v-model="form.entry_threshold"
              :min="20"
              :max="95"
              :step="5"
              controls-position="right"
              class="linear-input-number"
            />
            <template #extra>
              <span class="form-hint">评分 ≥ 此值时买入（默认70）</span>
            </template>
          </el-form-item>

          <el-form-item v-if="form.strategy === 'SCORE_THRESHOLD'" label="退出阈值">
            <el-input-number
              v-model="form.exit_threshold"
              :min="10"
              :max="80"
              :step="5"
              controls-position="right"
              class="linear-input-number"
            />
            <template #extra>
              <span class="form-hint">评分 &lt; 此值时卖出（默认50）</span>
            </template>
          </el-form-item>

          <el-form-item v-if="form.strategy === 'SCORE_MOMENTUM'" label="评分变动阈值">
            <el-input-number
              v-model="form.score_delta"
              :min="1"
              :max="50"
              :step="1"
              controls-position="right"
              class="linear-input-number"
            />
            <template #extra>
              <span class="form-hint">评分变动 ≥ 此值时触发交易（默认10）</span>
            </template>
          </el-form-item>

          <el-form-item label="止损比例 (%)">
            <el-input-number
              v-model="form.stop_loss_pct"
              :min="-30"
              :max="0"
              :step="1"
              controls-position="right"
              class="linear-input-number"
            />
            <template #extra>
              <span class="form-hint">负值表示亏损比例（默认-5%）</span>
            </template>
          </el-form-item>
        </div>

        <!-- Action Buttons -->
        <div class="form-actions">
          <el-button
            class="btn-ghost"
            @click="handleCancel"
          >
            取消
          </el-button>
          <el-button
            class="btn-primary"
            type="primary"
            native-type="submit"
            :loading="submitting"
          >
            {{ submitting ? '回测运行中...' : '开始回测' }}
          </el-button>
        </div>
      </el-form>
    </div>

    <!-- Result Preview (shown after successful run) -->
    <div v-if="result" class="result-card fade-in">
      <div class="card-header">
        <h3 class="card-title">回测完成</h3>
        <router-link :to="`/backtest/${result.id}`" class="view-detail-link">
          查看详情 →
        </router-link>
      </div>

      <div class="result-metrics">
        <div class="metric-item">
          <span class="metric-label">总收益率</span>
          <span class="metric-value" :class="pnlClass(result.total_return)">
            {{ formatPercent(result.total_return_pct) }}
          </span>
        </div>
        <div class="metric-item">
          <span class="metric-label">年化收益</span>
          <span class="metric-value mono" :class="pnlClass(result.annualized_return)">
            {{ formatPercent(result.annualized_return) }}
          </span>
        </div>
        <div class="metric-item">
          <span class="metric-label">最大回撤</span>
          <span class="metric-value mono danger">
            {{ formatPercent(result.max_drawdown) }}
          </span>
        </div>
        <div class="metric-item">
          <span class="metric-label">夏普比率</span>
          <span class="metric-value mono">
            {{ formatNumber(result.sharpe_ratio) }}
          </span>
        </div>
        <div class="metric-item">
          <span class="metric-label">胜率</span>
          <span class="metric-value mono">
            {{ formatPercent(result.win_rate) }}
          </span>
        </div>
        <div class="metric-item">
          <span class="metric-label">总交易次数</span>
          <span class="metric-value mono">
            {{ result.total_trades }} 笔
          </span>
        </div>

        <!-- Friction costs (show when available) -->
        <template v-if="result.total_commission">
          <div class="metric-item">
            <span class="metric-label">总佣金</span>
            <span class="metric-value mono">
              ¥{{ formatNumber(result.total_commission) }}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">总印花税</span>
            <span class="metric-value mono">
              ¥{{ formatNumber(result.total_stamp_duty) }}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">总滑点成本</span>
            <span class="metric-value mono">
              ¥{{ formatNumber(result.total_slippage) }}
            </span>
          </div>
        </template>

        <!-- Benchmark comparison (show when available) -->
        <template v-if="result.benchmark_return_pct !== undefined">
          <div class="metric-item">
            <span class="metric-label">基准收益 ({{ result.benchmark_code || '沪深300' }})</span>
            <span class="metric-value mono" :class="pnlClass(result.benchmark_return_pct)">
              {{ formatPercent(result.benchmark_return_pct) }}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">超额收益 (Alpha)</span>
            <span class="metric-value mono" :class="pnlClass(result.excess_return_pct)">
              {{ formatPercent(result.excess_return_pct) }}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">信息比率</span>
            <span class="metric-value mono">
              {{ formatNumber(result.information_ratio) }}
            </span>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import type { FormInstance, FormRules } from 'element-plus'
import {
  backtestApi,
  isScoreDrivenStrategy,
  type BacktestResult,
  type NonScoreStrategy,
  type RunBacktestPayload,
} from '@/api/backtest'

const router = useRouter()
const formRef = ref<FormInstance>()
const submitting = ref(false)
const errorMessage = ref('')
const successMessage = ref('')
const result = ref<BacktestResult | null>(null)

const form = reactive({
  stock_code: '',
  strategy: '',
  start_date: '',
  end_date: '',
  initial_cash: 100000,
  horizon: 20 as number | null,
  entry_threshold: 70,
  exit_threshold: 50,
  score_delta: 10,
  stop_loss_pct: -5,
  model_version: ''
})

const formRules: FormRules = {
  stock_code: [
    { required: true, message: '请输入股票代码', trigger: 'blur' },
    { pattern: /^(sh|sz)\d{6}$/, message: '格式: sh600519 或 sz000001', trigger: 'blur' }
  ],
  strategy: [
    { required: true, message: '请选择交易策略', trigger: 'change' }
  ],
  start_date: [
    { required: true, message: '请选择开始日期', trigger: 'change' }
  ],
  end_date: [
    { required: true, message: '请选择结束日期', trigger: 'change' }
  ],
  model_version: [
    {
      validator: (_rule, value, callback) => {
        if (isScoreDrivenStrategy(form.strategy) && !String(value || '').trim()) {
          callback(new Error('请输入评分模型版本'))
          return
        }
        callback()
      },
      trigger: ['blur', 'change']
    }
  ]
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toFixed(2)
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `${value.toFixed(2)}%`
}

function pnlClass(value: number | null | undefined) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return ''
  if (numeric > 0) return 'positive'
  if (numeric < 0) return 'negative'
  return ''
}

function strategyDescription(value: string) {
  const map: Record<string, string> = {
    MA_CROSS: '基于 MA10 与 MA20 均线交叉生成买卖信号',
    BUY_HOLD: '期初买入并持有至期末，衡量基准收益',
    SCORE_THRESHOLD: '基于评分阈值生成买卖信号（Score≥阈值买入，Score<退出阈值卖出）',
    SCORE_MOMENTUM: '基于评分动量变化生成买卖信号（评分上升买入，下降卖出）'
  }
  return map[value] || ''
}

function handleCancel() {
  router.push('/backtest')
}

async function handleSubmit() {
  if (!formRef.value) return

  const valid = await formRef.value.validate().catch(() => false)
  if (!valid) return

  // Validate date range
  if (form.start_date >= form.end_date) {
    ElMessage.error('结束日期必须晚于开始日期')
    return
  }

  submitting.value = true
  errorMessage.value = ''
  successMessage.value = ''
  result.value = null

  try {
    const basePayload = {
      stock_code: form.stock_code.trim(),
      start_date: form.start_date,
      end_date: form.end_date,
      initial_cash: form.initial_cash
    }
    let payload: RunBacktestPayload

    if (form.strategy === 'SCORE_THRESHOLD') {
      payload = {
        ...basePayload,
        strategy: form.strategy,
        horizon: form.horizon as number,
        entry_threshold: form.entry_threshold,
        exit_threshold: form.exit_threshold,
        stop_loss_pct: form.stop_loss_pct,
        model_version: form.model_version.trim()
      }
    } else if (form.strategy === 'SCORE_MOMENTUM') {
      payload = {
        ...basePayload,
        strategy: form.strategy,
        horizon: form.horizon as number,
        score_delta: form.score_delta,
        stop_loss_pct: form.stop_loss_pct,
        model_version: form.model_version.trim()
      }
    } else {
      payload = {
        ...basePayload,
        strategy: form.strategy as NonScoreStrategy
      }
    }

    const backtestResult = await backtestApi.run(payload)
    result.value = backtestResult
    successMessage.value = `回测完成！${backtestResult.name} 已生成。`
    ElMessage.success('回测运行成功')
  } catch (error: any) {
    console.error(error)
    const msg = error?.response?.data?.message || error?.message || '回测运行失败，请稍后重试。'
    errorMessage.value = msg
    ElMessage.error(msg)
  } finally {
    submitting.value = false
  }
}
</script>

<style scoped lang="scss">
.backtest-create-page {
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
  max-width: 800px;
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

/* Alerts */
.error-alert {
  margin-bottom: 24px;
  background-color: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.2);
  color: #fb7185;
}

.success-alert {
  margin-bottom: 24px;
  background-color: rgba(16, 185, 129, 0.1);
  border: 1px solid rgba(16, 185, 129, 0.2);
  color: #34d399;
}

/* Form Card */
.form-card {
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  overflow: hidden;
}

.card-header {
  padding: 24px 32px;
  border-bottom: 1px solid var(--color-border-subtle);
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

.backtest-form {
  padding: 32px;
}

.form-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 24px 20px;
  margin-bottom: 32px;

  @media (max-width: 640px) {
    grid-template-columns: 1fr;
  }
}

.form-hint {
  font-size: 12px;
  color: var(--color-text-quaternary);
  margin-top: 4px;
  display: block;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  padding-top: 24px;
  border-top: 1px solid var(--color-border-subtle);
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

:deep(.el-form-item__label) {
  color: var(--color-text-quaternary) !important;
  font-size: 12px !important;
  font-weight: 510 !important;
  padding-bottom: 8px !important;
}

:deep(.el-input-number) {
  width: 100%;
}

/* Result Card */
.result-card {
  margin-top: 32px;
  background: var(--color-panel);
  border: 1px solid var(--color-border);
  border-radius: 12px;
  overflow: hidden;

  .card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .view-detail-link {
    font-size: 14px;
    font-weight: 510;
    color: var(--color-brand-accent);
    text-decoration: none;

    &:hover {
      text-decoration: underline;
    }
  }
}

.result-metrics {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 1px;
  background: var(--color-border-subtle);
  padding: 0;

  @media (max-width: 768px) {
    grid-template-columns: repeat(2, 1fr);
  }

  .metric-item {
    background: var(--color-panel);
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .metric-label {
    font-size: 12px;
    color: var(--color-text-quaternary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .metric-value {
    font-size: 22px;
    font-weight: 590;
    color: var(--color-text-primary);

    &.mono {
      font-family: var(--font-mono);
    }

    &.positive { color: #ef4444; }
    &.negative { color: #22c55e; }
    &.danger { color: #fb7185; }
  }
}

/* Animations */
.fade-in {
  animation: fadeIn 0.3s ease-out;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@media (max-width: 1024px) {
  .backtest-create-page {
    padding: 24px;
  }

  .page-title {
    font-size: 32px;
  }
}
</style>
