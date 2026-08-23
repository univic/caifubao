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
          <div style="display: flex; align-items: center; gap: 12px;">
            <el-button
              type="primary"
              :disabled="selectedExperiments.length !== 2"
              :loading="comparing"
              @click="handleCompare"
            >
              对比选中实验 ({{ selectedExperiments.length }}/2)
            </el-button>
            <span>{{ experiments.length }} 个</span>
          </div>
        </div>
        <el-table
          :data="experiments"
          empty-text="暂无实验"
          highlight-current-row
          class="experiments-table"
          @row-click="selectExperiment"
          @selection-change="handleSelectionChange"
        >
          <el-table-column type="selection" width="44" />
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

    <el-dialog v-model="compareVisible" title="实验对比" width="80%" destroy-on-close>
      <div v-if="compareResult" class="compare-report">
        <el-alert
          :title="compareResult.verdict"
          :type="compareResult.verdict?.includes('wins') || compareResult.verdict?.includes('improvement') ? 'success' : 'warning'"
          show-icon
          :closable="false"
          style="margin-bottom: 20px"
        />

        <h3>Overall Metrics</h3>
        <el-table :data="compareTableData" stripe size="small" style="margin-bottom: 20px">
          <el-table-column prop="metric" label="Metric" width="200" />
          <el-table-column prop="candidate" label="Candidate" width="180" />
          <el-table-column prop="baseline" label="Baseline" width="180" />
          <el-table-column prop="delta" label="Delta">
            <template #default="{ row }">
              <span :class="deltaClass(row.delta)">{{ formatDelta(row.delta) }}</span>
            </template>
          </el-table-column>
        </el-table>

        <h3>Top-N Hit Rates</h3>
        <el-table :data="topNCompareData" stripe size="small">
          <el-table-column prop="group" label="Group" width="120" />
          <el-table-column prop="candidate_hit" label="Candidate Hit Rate" width="180" />
          <el-table-column prop="baseline_hit" label="Baseline Hit Rate" width="180" />
          <el-table-column prop="delta" label="Delta">
            <template #default="{ row }">
              <span :class="deltaClass(row.delta)">{{ formatDelta(row.delta) }}</span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-dialog>

    <!-- ─── Top Rankings (Task 14.8) ──────────────────────────────────── -->
    <section class="rankings-panel">
      <div class="panel-header">
        <div>
          <h2>实验排名</h2>
          <p>按综合得分排序的评分实验，含 Bonferroni 多重比较校正</p>
        </div>
        <div class="horizon-bar">
          <button
            v-for="h in [5, 20, 60]"
            :key="h"
            class="hz-btn"
            :class="{ active: rankingsHorizon === h }"
            @click="rankingsHorizon = h; fetchRankings()"
          >Score{{ h }}</button>
        </div>
      </div>

      <el-alert
        v-if="rankingsData?.bonferroni"
        class="bonferroni-alert"
        type="info"
        :closable="false"
        show-icon
      >
        <template #title>
          Bonferroni 校正阈值：p &lt; {{ rankingsData.bonferroni.corrected_alpha.toExponential(2) }}
          （基于 {{ rankingsData.bonferroni.num_comparisons }} 次比对）
        </template>
      </el-alert>

      <div class="table-wrapper" v-loading="rankingsLoading">
        <el-alert
          v-if="rankingsError"
          class="quality-alert"
          type="error"
          :title="rankingsError"
          show-icon
          :closable="false"
        />
        <el-table
          v-else-if="rankingsData?.rankings.length"
          :data="rankingsData.rankings"
          class="rankings-table"
          empty-text="暂无排名数据"
        >
          <el-table-column label="#" width="50" align="right">
            <template #default="{ row }">
              <span class="rank-num" :class="{ top: row.rank <= 3 }">{{ row.rank }}</span>
            </template>
          </el-table-column>
          <el-table-column label="实验名称" min-width="200">
            <template #default="{ row }">
              <div class="name-cell">
                <strong>{{ row.name }}</strong>
                <span>{{ row.model_version }}</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="周期" width="80" align="center" prop="horizon" />
          <el-table-column label="综合得分" width="100" align="right">
            <template #default="{ row }">
              <span class="mono-value" :class="{ positive: row.composite_score > 0, negative: row.composite_score < 0 }">
                {{ formatRankingNum(row.composite_score) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="信息比率" width="90" align="right">
            <template #default="{ row }">{{ formatRankingNum(row.metrics?.information_ratio) }}</template>
          </el-table-column>
          <el-table-column label="最大回撤" width="100" align="right">
            <template #default="{ row }">
              <span class="mono-value danger">{{ formatPctVal(row.metrics?.max_drawdown) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="交易数" width="80" align="right">
            <template #default="{ row }">{{ row.metrics?.total_trades ?? '--' }}</template>
          </el-table-column>
          <el-table-column label="标记" min-width="200">
            <template #default="{ row }">
              <div class="flags-cell">
                <el-tag
                  v-for="flag in parseRankingFlags(row.flags)"
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
          <el-table-column label="权重" min-width="240">
            <template #default="{ row }">
              <div class="weights-cell">
                <el-tag
                  v-for="(weight, comp) in sortedWeights(row.weights)"
                  :key="comp"
                  size="small"
                  class="weight-pill"
                >
                  {{ comp }}:{{ weight }}
                </el-tag>
              </div>
            </template>
          </el-table-column>
        </el-table>
        <el-empty v-else-if="!rankingsLoading" description="暂无排名数据，请先创建并运行评分实验" />
      </div>
    </section>

    <!-- ─── Weight Heatmap (Task 14.8) ────────────────────────────────── -->
    <section class="heatmap-panel">
      <div class="panel-header">
        <div>
          <h2>权重热力图</h2>
          <p>评分组件对的平均综合得分，点击单元格可按组件对筛选实验</p>
        </div>
        <div class="horizon-bar">
          <button
            v-for="h in [5, 20, 60]"
            :key="h"
            class="hz-btn"
            :class="{ active: heatmapHorizon === h }"
            @click="heatmapHorizon = h; fetchHeatmap()"
          >Score{{ h }}</button>
        </div>
      </div>

      <div class="table-wrapper" v-loading="heatmapLoading">
        <el-alert
          v-if="heatmapError"
          class="quality-alert"
          type="error"
          :title="heatmapError"
          show-icon
          :closable="false"
        />
        <div v-else-if="heatmapData?.components.length" class="heatmap-grid" :style="heatmapGridStyle">
          <!-- Top-left empty corner -->
          <div class="heatmap-cell corner" />
          <!-- Column headers -->
          <div
            v-for="c in heatmapData.components"
            :key="c"
            class="heatmap-cell header"
            :title="c"
          >{{ formatHeatmapLabel(c) }}</div>
          <!-- Rows -->
          <template v-for="(rowComp, i) in heatmapData.components" :key="rowComp">
            <div class="heatmap-cell row-label" :title="rowComp">{{ formatHeatmapLabel(rowComp) }}</div>
            <div
              v-for="(colComp, j) in heatmapData.components"
              :key="colComp"
              class="heatmap-cell data-cell"
              :style="heatmapCellStyle(i, j)"
              :title="`${formatHeatmapLabel(rowComp)} × ${formatHeatmapLabel(colComp)}: ${heatmapCellValue(i, j)}`"
              @click="handleHeatmapCellClick(i, j)"
            >
              <span class="cell-value">{{ heatmapCellValue(i, j) }}</span>
            </div>
          </template>
        </div>
        <el-empty v-else-if="!heatmapLoading" description="暂无热力图数据" />
      </div>

      <div v-if="heatmapCellInfo" class="heatmap-info">
        <span class="heatmap-info-label">选中组件对：</span>
        <strong>{{ formatHeatmapLabel(heatmapCellInfo.row) }} × {{ formatHeatmapLabel(heatmapCellInfo.col) }}</strong>
        <span class="heatmap-info-score">
          平均得分 {{ formatRankingNum(heatmapCellInfo.value) }}
        </span>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import {
  scoreExperimentApi,
  type CompareResult,
  type HeatmapResponse,
  type RankingsResponse,
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
const comparing = ref(false)
const experiments = ref<ScoreExperiment[]>([])
const selectedExperiment = ref<ScoreExperiment | null>(null)
const selectedExperiments = ref<ScoreExperiment[]>([])
const activeHorizon = ref('5')
const compareVisible = ref(false)
const compareResult = ref<CompareResult | null>(null)
const dateRange = ref<[string, string]>(['2025-01-01', '2025-12-31'])
const configText = ref(JSON.stringify(defaultConfig, null, 2))
const form = reactive({
  name: '',
  description: '',
  model_version: 'score_v2_202604',
  baseline_model_version: '',
  horizons: [5, 20, 60]
})

// ─── Rankings & Heatmap state (Task 14.8) ─────────────────────────────────
const rankingsHorizon = ref(20)
const rankingsLoading = ref(false)
const rankingsError = ref('')
const rankingsData = ref<RankingsResponse | null>(null)

const heatmapHorizon = ref(20)
const heatmapLoading = ref(false)
const heatmapError = ref('')
const heatmapData = ref<HeatmapResponse | null>(null)
const heatmapCellInfo = ref<{ row: string; col: string; value: number | null } | null>(null)

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

function handleSelectionChange(selection: ScoreExperiment[]) {
  selectedExperiments.value = selection
}

async function handleCompare() {
  if (selectedExperiments.value.length !== 2) {
    ElMessage.warning('请选择恰好 2 个实验进行对比')
    return
  }
  const a = selectedExperiments.value[0]!
  const b = selectedExperiments.value[1]!
  const startDate = a.start_date?.slice(0, 10) || '2025-01-01'
  const endDate = a.end_date?.slice(0, 10) || '2025-12-31'
  comparing.value = true
  try {
    const response = await scoreExperimentApi.compare({
      id_a: a.id || a.model_version,
      id_b: b.id || b.model_version,
      start_date: startDate,
      end_date: endDate,
      horizon: 20
    }) as unknown as { success: boolean; data: CompareResult }
    if (response.success && response.data) {
      compareResult.value = response.data
    } else {
      compareResult.value = response.data
    }
    compareVisible.value = true
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : '对比失败'
    ElMessage.error(message)
  } finally {
    comparing.value = false
  }
}

const compareTableData = computed(() => {
  if (!compareResult.value) return []
  const c = compareResult.value.candidate?.overall
  const b = compareResult.value.baseline?.overall
  const d = compareResult.value.deltas || {} as Record<string, number | null>
  return [
    {
      metric: 'Hit Rate',
      candidate: formatPct(c?.hit_rate),
      baseline: formatPct(b?.hit_rate),
      delta: d.hit_rate as number | null | undefined
    },
    {
      metric: 'Avg Return at Target',
      candidate: formatPct(c?.avg_return_at_target),
      baseline: formatPct(b?.avg_return_at_target),
      delta: d.avg_return_at_target as number | null | undefined
    },
    {
      metric: 'Avg Max Return',
      candidate: formatPct(c?.avg_max_return),
      baseline: formatPct(b?.avg_max_return),
      delta: d.avg_max_return as number | null | undefined
    },
    {
      metric: 'Avg Max Drawdown',
      candidate: formatPct(c?.avg_max_drawdown),
      baseline: formatPct(b?.avg_max_drawdown),
      delta: d.avg_max_drawdown as number | null | undefined
    },
    {
      metric: 'Stop-Loss Hit Rate',
      candidate: formatPct(c?.stop_loss_hit_rate),
      baseline: formatPct(b?.stop_loss_hit_rate),
      delta: d.stop_loss_hit_rate as number | null | undefined
    },
    {
      metric: 'Count',
      candidate: c?.count != null ? String(c.count) : '--',
      baseline: b?.count != null ? String(b.count) : '--',
      delta: d.count as number | null | undefined
    }
  ]
})

const topNCompareData = computed(() => {
  if (!compareResult.value) return []
  const cTop = compareResult.value.candidate?.top_n || {}
  const bTop = compareResult.value.baseline?.top_n || {}
  const deltas = compareResult.value.deltas || {}
  const topNDeltas = (deltas.top_n || {}) as Record<string, Record<string, number | null>>
  return Object.keys(cTop).map(key => ({
    group: key,
    candidate_hit: formatPct((cTop[key] as ScoreMetricSummary)?.hit_rate),
    baseline_hit: formatPct((bTop[key] as ScoreMetricSummary)?.hit_rate),
    delta: topNDeltas[key]?.hit_rate_delta as number | null | undefined
  }))
})

function formatPct(val: number | null | undefined): string {
  if (val === null || val === undefined) return '--'
  return (val * 100).toFixed(2) + '%'
}

function formatDelta(val: number | null | undefined): string {
  if (val === null || val === undefined) return '--'
  const sign = val >= 0 ? '+' : ''
  return `${sign}${(val * 100).toFixed(2)}%`
}

function deltaClass(val: number | null | undefined): string {
  if (val === null || val === undefined) return ''
  return val > 0 ? 'positive' : 'negative'
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

// ─── Rankings & Heatmap (Task 14.8) ───────────────────────────────────────

async function fetchRankings() {
  rankingsLoading.value = true
  rankingsError.value = ''
  try {
    const response = await scoreExperimentApi.getRankings({ horizon: rankingsHorizon.value })
    // API wraps in { success, data: { rankings, ... } }, axios interceptor unwraps to that
    const inner: any = (response as any)?.data ?? response
    rankingsData.value = inner?.rankings ? inner : (response as any)
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : '获取排名数据失败'
    rankingsError.value = msg
    rankingsData.value = null
  } finally {
    rankingsLoading.value = false
  }
}

async function fetchHeatmap() {
  heatmapLoading.value = true
  heatmapError.value = ''
  try {
    const response = await scoreExperimentApi.getHeatmap({ horizon: heatmapHorizon.value })
    const inner: any = (response as any)?.data ?? response
    heatmapData.value = inner?.matrix ? inner : (response as any)
    heatmapCellInfo.value = null
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : '获取热力图数据失败'
    heatmapError.value = msg
    heatmapData.value = null
    heatmapCellInfo.value = null
  } finally {
    heatmapLoading.value = false
  }
}

function formatRankingNum(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return value.toFixed(2)
}

function formatPctVal(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  return `${value.toFixed(1)}%`
}

interface ParsedFlag {
  text: string
  type: 'warning' | 'danger' | 'info'
}

function parseRankingFlags(flags: string[] | undefined): ParsedFlag[] {
  if (!flags || !Array.isArray(flags)) return []
  return flags.map(flag => {
    if (flag.startsWith('concentrated_returns')) {
      const pct = flag.split(':')[1] || '?'
      return { text: `集中收益:${pct}`, type: 'danger' as const }
    }
    if (flag === 'low_sample') return { text: '样本不足', type: 'warning' as const }
    if (flag === 'insufficient_period') return { text: '周期不足', type: 'warning' as const }
    if (flag === 'high_drawdown') return { text: '高回撤', type: 'danger' as const }
    if (flag === 'performance_decay') return { text: '表现衰减', type: 'warning' as const }
    if (flag === 'overfit') return { text: '过拟合', type: 'danger' as const }
    return { text: flag, type: 'info' as const }
  })
}

function sortedWeights(weights: Record<string, number> | undefined): [string, number][] {
  if (!weights) return []
  return Object.entries(weights).sort(([, a], [, b]) => b - a)
}

// ─── Heatmap helpers ──────────────────────────────────────────────────────

const heatmapGridStyle = computed(() => {
  if (!heatmapData.value) return {}
  const nc = heatmapData.value.components.length
  return {
    gridTemplateColumns: `auto repeat(${nc}, 1fr)`
  }
})

function _heatmapEntry(i: number, j: number) {
  const components = heatmapData.value?.components
  const matrix = heatmapData.value?.matrix
  if (!components || !matrix) return null
  const cx = components[i]
  const cy = components[j]
  return matrix.find((e: any) => e.component_x === cx && e.component_y === cy) ?? null
}

function heatmapCellStyle(i: number, j: number) {
  const entry = _heatmapEntry(i, j)
  if (!entry) return { background: 'rgba(255,255,255,0.02)' }
  const value = entry.avg_score
  if (value === null || value === undefined || value === 0) {
    return { background: 'rgba(255,255,255,0.02)' }
  }
  const clamped = Math.max(-1.5, Math.min(1.5, Number(value)))
  const intensity = Math.abs(clamped) / 1.5
  if (clamped >= 0) {
    return { background: `rgba(16, 185, 129, ${(intensity * 0.7).toFixed(2)})` }
  } else {
    return { background: `rgba(239, 68, 68, ${(intensity * 0.7).toFixed(2)})` }
  }
}

function heatmapCellValue(i: number, j: number): string {
  const entry = _heatmapEntry(i, j)
  if (!entry) return '--'
  const value = entry.avg_score
  if (value === null || value === undefined) return '--'
  return Number(value).toFixed(2)
}

function formatHeatmapLabel(component: string): string {
  const map: Record<string, string> = {
    trend_alignment: '趋势对齐',
    momentum: '动量',
    relative_strength: '相对强度',
    signal_strength: '信号强度',
    risk_penalty: '风险惩罚',
    breakout_or_pos: '突破/位',
    volume_ratio: '量比',
    bb_position: '布林位',
    atr_ratio: 'ATR比',
    consecutive_up: '连阳',
    turnover_accel: '换手加速',
    gap_ratio: '缺口比',
    yearly_position: '年位',
    rsi_14: 'RSI14'
  }
  return map[component] || component
}

function handleHeatmapCellClick(i: number, j: number) {
  const entry = _heatmapEntry(i, j)
  const comps = heatmapData.value?.components
  if (!comps || !entry) return
  const rowComp = comps[i] ?? ''
  const colComp = comps[j] ?? ''
  if (!rowComp || !colComp) return
  heatmapCellInfo.value = {
    row: rowComp,
    col: colComp,
    value: entry.avg_score ?? null
  }
}

onMounted(() => {
  fetchExperiments()
  fetchRankings()
  fetchHeatmap()
})
</script>

<style scoped lang="scss">
.experiments-page {
  min-height: 100vh;
  padding: 0;
  color: var(--color-text-primary);
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
  color: var(--color-primary);
  font-size: 13px;
  font-weight: 510;
  letter-spacing: 0.1em;
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
  color: var(--color-text-primary);
}

h2 {
  font-size: 18px;
  font-weight: 590;
  color: var(--color-text-primary);
}

h3 {
  margin-bottom: 14px;
  font-size: 15px;
  color: var(--color-text-primary);
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
  border: 1px solid var(--color-border);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.03);
  box-shadow: var(--box-shadow-light);
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
  font-family: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace;
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
  color: var(--color-text-placeholder);
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
  border: 1px solid var(--color-border-light);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.02);
}

.metric span,
.metric small {
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

.metric small {
  margin-top: 6px;
}

.positive {
  color: var(--color-success) !important;
}

.negative {
  color: var(--color-danger) !important;
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

.compare-report h3 {
  margin: 18px 0 10px;
  font-size: 16px;
  font-weight: 590;
}

/* ─── Rankings & Heatmap (Task 14.8) ─────────────────────────────────────── */

.rankings-panel,
.heatmap-panel {
  margin-top: 18px;
  padding: 18px;
  border: 1px solid var(--color-border);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.03);
  box-shadow: var(--box-shadow-light);
}

.horizon-bar {
  display: flex;
  gap: 4px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.05);
  border-radius: 8px;
  padding: 3px;
  width: fit-content;
}

.hz-btn {
  font-size: 12px;
  font-weight: 590;
  padding: 6px 16px;
  border: none;
  border-radius: 6px;
  background: transparent;
  color: var(--color-text-placeholder);
  cursor: pointer;
  transition: background 0.2s, color 0.2s;

  &.active {
    background: var(--color-primary-dark);
    color: var(--color-text-primary);
  }

  &:hover:not(.active) {
    color: var(--color-text-regular);
  }
}

.bonferroni-alert {
  margin: 16px 0;
  background-color: rgba(96, 165, 250, 0.1);
  border: 1px solid rgba(96, 165, 250, 0.2);
  color: #93c5fd;
}

.quality-alert {
  margin: 16px 0;
}

.table-wrapper {
  margin-top: 12px;
  overflow-x: auto;
}

.rankings-table {
  font-size: 13px;
}

.rank-num {
  font-family: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace;
  font-size: 14px;
  font-weight: 590;
  color: var(--color-text-secondary);

  &.top {
    color: var(--color-primary);
  }
}

.mono-value {
  font-family: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace;
  font-size: 13px;
  color: var(--color-text-regular);

  &.positive { color: var(--color-up); }
  &.negative { color: var(--color-down); }
  &.danger { color: var(--color-danger); }
}

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

.weights-cell {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.weight-pill {
  font-size: 11px;
  font-weight: 510;
  background: rgba(113, 112, 255, 0.1);
  color: #828fff;
  border: 1px solid rgba(113, 112, 255, 0.18);
  border-radius: 999px;
  padding: 2px 10px;
}

/* ─── Heatmap Grid ──────────────────────────────────────────────────── */

.heatmap-grid {
  display: grid;
  gap: 2px;
  margin-top: 12px;
  overflow-x: auto;
}

.heatmap-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  min-width: 64px;
  min-height: 40px;
  padding: 6px 8px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 510;
  transition: background 0.15s;
}

.heatmap-cell.corner {
  background: transparent;
}

.heatmap-cell.header,
.heatmap-cell.row-label {
  color: var(--color-text-secondary);
  font-size: 11px;
  font-weight: 590;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  background: transparent;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.heatmap-cell.row-label {
  justify-content: flex-end;
  padding-right: 10px;
}

.heatmap-cell.row-label {
  justify-content: flex-end;
}

.heatmap-cell.data-cell {
  cursor: pointer;
  border: 1px solid rgba(255, 255, 255, 0.04);
  color: var(--color-text-regular);
  font-family: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace;
  font-size: 11px;

  &:hover {
    border-color: var(--color-primary);
    box-shadow: 0 0 0 1px var(--color-primary);
    z-index: 1;
  }
}

.cell-value {
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.4);
}

.heatmap-info {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-top: 14px;
  padding: 10px 14px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  font-size: 13px;
  color: var(--color-text-regular);

  .heatmap-info-label {
    color: var(--color-text-secondary);
  }

  strong {
    color: var(--color-text-primary);
    font-weight: 590;
  }

  .heatmap-info-score {
    margin-left: auto;
    font-family: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace;
    font-weight: 590;
    color: var(--color-primary);
  }
}
</style>
