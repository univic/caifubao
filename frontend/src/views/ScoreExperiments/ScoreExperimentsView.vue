<template>
  <div class="experiments-page">
    <section class="toolbar">
      <div>
        <p class="eyebrow">Score Research</p>
        <h1>评分实验</h1>
      </div>
      <el-button type="primary" :loading="loading" @click="fetchExperiments">刷新</el-button>
    </section>

    <section class="workspace">
      <div class="experiment-form">
        <h2>新建实验</h2>
        <el-form label-position="top">
          <el-form-item label="实验名称">
            <el-input v-model="form.name" placeholder="Score20 trend-heavy v1" />
          </el-form-item>
          <el-form-item label="模型版本">
            <el-input v-model="form.model_version" placeholder="score_v2_202604" />
          </el-form-item>
          <el-form-item label="Baseline 版本">
            <el-input v-model="form.baseline_model_version" placeholder="可选，例如 score_v2_202604_base" />
          </el-form-item>
          <el-form-item label="回放区间">
            <el-date-picker
              v-model="dateRange"
              type="daterange"
              value-format="YYYY-MM-DD"
              start-placeholder="开始日期"
              end-placeholder="结束日期"
              class="date-picker"
            />
          </el-form-item>
          <el-form-item label="周期">
            <el-checkbox-group v-model="form.horizons">
              <el-checkbox-button :value="5">Score5</el-checkbox-button>
              <el-checkbox-button :value="20">Score20</el-checkbox-button>
              <el-checkbox-button :value="60">Score60</el-checkbox-button>
            </el-checkbox-group>
          </el-form-item>
          <el-form-item label="因子权重配置">
            <el-input
              v-model="configText"
              type="textarea"
              :rows="9"
              spellcheck="false"
              class="config-editor"
            />
          </el-form-item>
          <el-button type="primary" class="submit-button" :loading="submitting" @click="createExperiment">
            创建并汇总
          </el-button>
        </el-form>
      </div>

      <div class="experiment-list">
        <div class="panel-header">
          <h2>实验记录</h2>
          <span>{{ experiments.length }} 个</span>
        </div>
        <el-table
          :data="experiments"
          empty-text="暂无实验"
          highlight-current-row
          class="experiments-table"
          @row-click="selectExperiment"
        >
          <el-table-column label="实验" min-width="220">
            <template #default="{ row }">
              <div class="name-cell">
                <strong>{{ row.name }}</strong>
                <span>{{ row.model_version }}</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="周期" width="120">
            <template #default="{ row }">{{ row.horizons.join('/') }}</template>
          </el-table-column>
          <el-table-column label="状态" width="110">
            <template #default="{ row }">
              <el-tag :type="statusType(row.status)" effect="plain">{{ statusLabel(row.status) }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="完成时间" width="170">
            <template #default="{ row }">{{ formatDateTime(row.completed_at || row.updated_at) }}</template>
          </el-table-column>
          <el-table-column label="" width="96" align="right">
            <template #default="{ row }">
              <el-button size="small" :loading="runningId === row.id" @click.stop="runExperiment(row)">重跑</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </section>

    <section v-if="selectedExperiment" class="report-panel">
      <div class="panel-header">
        <div>
          <h2>{{ selectedExperiment.name }}</h2>
          <p>{{ selectedExperiment.description || '基于已验证评分预测的实验汇总' }}</p>
        </div>
        <div class="version-meta">
          <span>{{ selectedExperiment.model_version }}</span>
          <span v-if="selectedExperiment.baseline_model_version">vs {{ selectedExperiment.baseline_model_version }}</span>
        </div>
      </div>

      <el-tabs v-model="activeHorizon">
        <el-tab-pane
          v-for="horizon in selectedExperiment.horizons"
          :key="horizon"
          :label="`Score${horizon}`"
          :name="String(horizon)"
        >
          <template v-if="horizonReport(String(horizon))">
            <div class="metric-grid">
              <div v-for="metric in metricCards(horizonReport(String(horizon))!.overall)" :key="metric.label" class="metric">
                <span>{{ metric.label }}</span>
                <strong>{{ metric.value }}</strong>
                <small v-if="metric.delta !== null" :class="{ positive: metric.delta > 0, negative: metric.delta < 0 }">
                  {{ formatSigned(metric.delta) }}
                </small>
              </div>
            </div>

            <div class="report-grid">
              <div class="report-section">
                <h3>Score 分桶</h3>
                <el-table :data="horizonReport(String(horizon))!.score_buckets" size="small" empty-text="暂无已验证样本">
                  <el-table-column prop="bucket" label="区间" width="90" />
                  <el-table-column label="样本" width="80" prop="count" />
                  <el-table-column label="到期收益" align="right">
                    <template #default="{ row }">{{ formatPercent(row.avg_return_at_target) }}</template>
                  </el-table-column>
                  <el-table-column label="最大收益" align="right">
                    <template #default="{ row }">{{ formatPercent(row.avg_max_return) }}</template>
                  </el-table-column>
                  <el-table-column label="命中率" align="right">
                    <template #default="{ row }">{{ formatPercent(row.hit_rate) }}</template>
                  </el-table-column>
                </el-table>
              </div>

              <div class="report-section">
                <h3>TopN 表现</h3>
                <el-table :data="topRows(horizonReport(String(horizon))!.top_n)" size="small" empty-text="暂无已验证样本">
                  <el-table-column prop="name" label="组合" width="90" />
                  <el-table-column prop="count" label="样本" width="80" />
                  <el-table-column label="到期收益" align="right">
                    <template #default="{ row }">{{ formatPercent(row.avg_return_at_target) }}</template>
                  </el-table-column>
                  <el-table-column label="最大收益" align="right">
                    <template #default="{ row }">{{ formatPercent(row.avg_max_return) }}</template>
                  </el-table-column>
                  <el-table-column label="命中率" align="right">
                    <template #default="{ row }">{{ formatPercent(row.hit_rate) }}</template>
                  </el-table-column>
                </el-table>
              </div>
            </div>

            <div class="report-section full">
              <h3>组件表现</h3>
              <el-table :data="componentRows(horizonReport(String(horizon))!.component_summary)" size="small" empty-text="暂无组件样本">
                <el-table-column prop="name" label="组件" min-width="180" />
                <el-table-column prop="count" label="样本" width="80" />
                <el-table-column label="平均分" align="right">
                  <template #default="{ row }">{{ formatNumber(row.avg_score) }}</template>
                </el-table-column>
                <el-table-column label="到期收益" align="right">
                  <template #default="{ row }">{{ formatPercent(row.avg_return_at_target) }}</template>
                </el-table-column>
                <el-table-column label="最大收益" align="right">
                  <template #default="{ row }">{{ formatPercent(row.avg_max_return) }}</template>
                </el-table-column>
                <el-table-column label="命中率" align="right">
                  <template #default="{ row }">{{ formatPercent(row.hit_rate) }}</template>
                </el-table-column>
              </el-table>
            </div>
          </template>
        </el-tab-pane>
      </el-tabs>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import {
  scoreExperimentApi,
  type ScoreExperiment,
  type ScoreExperimentHorizonReport,
  type ScoreMetricSummary
} from '@/api/scoreExperiments'

const defaultConfig = {
  5: { signal_strength: 30, momentum: 25, trend_alignment: 20, breakout_or_position: 15, risk_penalty: 10 },
  20: { signal_strength: 15, momentum: 15, trend_alignment: 30, relative_strength: 15, risk_penalty: 15 },
  60: { signal_strength: 5, momentum: 10, trend_alignment: 35, relative_strength: 25, risk_penalty: 15 }
}

const loading = ref(false)
const submitting = ref(false)
const runningId = ref('')
const experiments = ref<ScoreExperiment[]>([])
const selectedExperiment = ref<ScoreExperiment | null>(null)
const activeHorizon = ref('5')
const dateRange = ref<[string, string]>(['2025-01-01', '2025-12-31'])
const configText = ref(JSON.stringify(defaultConfig, null, 2))
const form = reactive({
  name: '',
  description: '',
  model_version: 'score_v2_202604',
  baseline_model_version: '',
  horizons: [5, 20, 60]
})

const selectedReport = computed(() => selectedExperiment.value?.report as { horizons?: Record<string, ScoreExperimentHorizonReport> } | undefined)

watch(selectedExperiment, (experiment) => {
  activeHorizon.value = String(experiment?.horizons?.[0] ?? 5)
})

function horizonReport(horizon: string) {
  return selectedReport.value?.horizons?.[horizon]
}

async function fetchExperiments() {
  loading.value = true
  try {
    const response = await scoreExperimentApi.listExperiments()
    experiments.value = response.items
    if (!selectedExperiment.value && response.items.length) {
      selectedExperiment.value = response.items[0]!
    }
  } finally {
    loading.value = false
  }
}

async function createExperiment() {
  let config: Record<string, unknown>
  try {
    config = JSON.parse(configText.value || '{}')
  } catch {
    ElMessage.error('因子权重配置不是合法 JSON')
    return
  }
  if (!dateRange.value?.[0] || !dateRange.value?.[1]) {
    ElMessage.error('请选择回放区间')
    return
  }
  submitting.value = true
  try {
    const experiment = await scoreExperimentApi.createExperiment({
      name: form.name || `${form.model_version} research`,
      description: form.description,
      model_version: form.model_version,
      baseline_model_version: form.baseline_model_version || undefined,
      start_date: dateRange.value[0],
      end_date: dateRange.value[1],
      horizons: form.horizons,
      config,
      run_now: true
    })
    selectedExperiment.value = experiment
    await fetchExperiments()
    ElMessage.success('实验已创建')
  } finally {
    submitting.value = false
  }
}

async function runExperiment(row: ScoreExperiment) {
  runningId.value = row.id
  try {
    const experiment = await scoreExperimentApi.runExperiment(row.id)
    selectedExperiment.value = experiment
    await fetchExperiments()
  } finally {
    runningId.value = ''
  }
}

function selectExperiment(row: ScoreExperiment) {
  selectedExperiment.value = row
}

function metricCards(summary: ScoreMetricSummary) {
  const comparison = horizonReport(activeHorizon.value)?.comparison || {}
  return [
    { label: '样本数', value: String(summary.count), delta: comparison.count_delta ?? null },
    { label: '到期收益', value: formatPercent(summary.avg_return_at_target), delta: comparison.avg_return_at_target_delta ?? null },
    { label: '最大收益', value: formatPercent(summary.avg_max_return), delta: comparison.avg_max_return_delta ?? null },
    { label: '最大回撤', value: formatPercent(summary.avg_max_drawdown), delta: comparison.avg_max_drawdown_delta ?? null },
    { label: '命中率', value: formatPercent(summary.hit_rate), delta: comparison.hit_rate_delta ?? null },
    { label: '止损率', value: formatPercent(summary.stop_loss_hit_rate), delta: comparison.stop_loss_hit_rate_delta ?? null }
  ]
}

function topRows(items: Record<string, ScoreMetricSummary>) {
  return Object.entries(items).map(([name, value]) => ({ name, ...value }))
}

function componentRows(items: Record<string, ScoreMetricSummary>) {
  return Object.entries(items).map(([name, value]) => ({ name, ...value }))
}

function formatDateTime(value: string | null) {
  if (!value) return '--'
  return value.replace('T', ' ').slice(0, 16)
}

function formatNumber(value: number | null) {
  if (value === null || value === undefined) return '--'
  return value.toFixed(2)
}

function formatPercent(value: number | null) {
  if (value === null || value === undefined) return '--'
  return `${(value * 100).toFixed(2)}%`
}

function formatSigned(value: number) {
  const sign = value > 0 ? '+' : ''
  return `${sign}${(value * 100).toFixed(2)}pp`
}

function statusLabel(value: string) {
  return {
    CREATED: '已创建',
    RUNNING: '运行中',
    COMPLETED: '已完成',
    FAILED: '失败'
  }[value] || value
}

function statusType(value: string) {
  if (value === 'COMPLETED') return 'success'
  if (value === 'FAILED') return 'danger'
  if (value === 'RUNNING') return 'warning'
  return 'info'
}

onMounted(fetchExperiments)
</script>

<style scoped lang="scss">
.experiments-page {
  min-height: 100vh;
  padding: 28px;
  background: #f6f8fb;
  color: #172033;
}

.toolbar,
.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.eyebrow {
  margin: 0 0 4px;
  color: #64748b;
  font-size: 12px;
  font-weight: 590;
  letter-spacing: 0;
  text-transform: uppercase;
}

h1,
h2,
h3,
p {
  margin: 0;
}

h1 {
  font-size: 28px;
}

h2 {
  font-size: 18px;
}

h3 {
  margin-bottom: 14px;
  font-size: 15px;
}

.workspace {
  display: grid;
  grid-template-columns: minmax(320px, 400px) minmax(0, 1fr);
  gap: 18px;
  margin-top: 20px;
}

.experiment-form,
.experiment-list,
.report-panel,
.report-section {
  border: 1px solid #dce3ee;
  border-radius: 8px;
  background: #fff;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.04);
}

.experiment-form,
.experiment-list,
.report-panel {
  padding: 18px;
}

.experiment-form h2 {
  margin-bottom: 16px;
}

.date-picker,
.submit-button {
  width: 100%;
}

.config-editor {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}

.experiments-table {
  margin-top: 14px;
}

.name-cell {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.name-cell span,
.panel-header p,
.version-meta {
  color: #64748b;
  font-size: 12px;
}

.report-panel {
  margin-top: 18px;
}

.version-meta {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(120px, 1fr));
  gap: 12px;
  margin: 8px 0 18px;
}

.metric {
  min-height: 86px;
  padding: 14px;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: #f8fafc;
}

.metric span,
.metric small {
  display: block;
  color: #64748b;
  font-size: 12px;
}

.metric strong {
  display: block;
  margin-top: 8px;
  font-size: 24px;
}

.metric small {
  margin-top: 6px;
}

.positive {
  color: #059669 !important;
}

.negative {
  color: #dc2626 !important;
}

.report-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;
}

.report-section {
  padding: 16px;
}

.report-section.full {
  margin-top: 16px;
}

@media (max-width: 1180px) {
  .workspace,
  .report-grid {
    grid-template-columns: 1fr;
  }

  .metric-grid {
    grid-template-columns: repeat(2, minmax(120px, 1fr));
  }
}
</style>
