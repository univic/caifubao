<template>
  <div class="decisions-page">
    <header class="page-hero">
      <p class="eyebrow">Decisions</p>
      <h1 class="page-title">决策面板</h1>
      <p class="subtitle">今日评分信号、质量监控与预警</p>
    </header>

    <!-- Quality Alert -->
    <el-alert
      v-if="quality?.decay_detected"
      class="quality-alert"
      type="warning"
      :title="quality.decay_detail"
      show-icon
      :closable="false"
    />

    <!-- Horizon Toggle -->
    <div class="horizon-bar">
      <button
        v-for="h in [5, 20, 60]"
        :key="h"
        class="hz-btn"
        :class="{ active: horizon === h }"
        @click="horizon = h; fetchAll()"
      >Score{{ h }}</button>
    </div>

    <!-- Alert Cards -->
    <section class="section">
      <h2>
        评分预警
        <span class="badge">{{ alerts.length }}</span>
      </h2>
      <div v-if="loading" class="loading">加载中...</div>
      <div v-else-if="!alerts.length" class="empty">无预警 — 今日评分稳定</div>
      <div v-else class="alert-grid">
        <div
          v-for="alert in alerts"
          :key="alert.stock_code"
          class="alert-card"
          :class="alert.alert_type"
        >
          <div class="alert-header">
            <span class="stock-name">{{ alert.stock_name || alert.stock_code }}</span>
            <span class="stock-code">{{ alert.stock_code }}</span>
          </div>
          <div class="alert-score">
            <span class="score-value">{{ alert.score?.toFixed(0) }}</span>
            <span v-if="alert.score_delta" class="score-delta" :class="alert.score_delta > 0 ? 'up' : 'down'">
              {{ alert.score_delta > 0 ? '+' : '' }}{{ alert.score_delta }}
            </span>
          </div>
          <div class="alert-detail">{{ alert.alert_detail }}</div>
          <div class="alert-meta">
            <span class="rec-tag" :class="alert.recommendation?.toLowerCase()">{{ alert.recommendation }}</span>
            <span class="rank">#{{ alert.rank }}</span>
          </div>
        </div>
      </div>
    </section>

    <!-- Quality Monitoring -->
    <section class="section">
      <h2>评分质量监控</h2>
      <div v-if="qualityLoading" class="loading">加载中...</div>
      <div v-else-if="quality">
        <div class="quality-metrics">
          <div class="q-metric">
            <span class="q-label">滚动 {{ quality.window_days }} 日命中率</span>
            <span class="q-value" :class="quality.latest_hit_rate > 0.5 ? 'good' : 'bad'">
              {{ formatPct(quality.latest_hit_rate) }}
            </span>
          </div>
          <div class="q-metric">
            <span class="q-label">统计窗口</span>
            <span class="q-value">{{ quality.rolling_hit_rates?.length || 0 }} 天</span>
          </div>
          <div class="q-metric">
            <span class="q-label">衰减检测</span>
            <span class="q-value" :class="quality.decay_detected ? 'bad' : 'good'">
              {{ quality.decay_detected ? '⚠ 已检测' : '✓ 正常' }}
            </span>
          </div>
        </div>
        <!-- Simple hit rate trend bars -->
        <div class="trend-bars" v-if="quality.rolling_hit_rates?.length">
          <div
            v-for="(point, idx) in quality.rolling_hit_rates.slice(-20)"
            :key="idx"
            class="trend-bar"
            :style="{ height: (point.rolling_hit_rate * 100) + '%' }"
            :title="point.date + ': ' + formatPct(point.rolling_hit_rate)"
          />
        </div>
      </div>
    </section>

    <!-- Daily Recommendations -->
    <section class="section">
      <h2>
        每日推荐
        <span class="badge">{{ dashboardItems.length }}</span>
      </h2>

      <div class="dashboard-horizon-bar">
        <button
          v-for="h in [5, 20, 60]"
          :key="h"
          class="hz-btn"
          :class="{ active: dashHorizon === h }"
          @click="dashHorizon = h; fetchDashboard()"
        >Score{{ h }}</button>
      </div>

      <div v-if="dashLoading" class="loading">加载中...</div>
      <el-alert
        v-else-if="dashError"
        class="quality-alert"
        type="error"
        :title="dashError"
        show-icon
        :closable="false"
      />
      <div v-else-if="!dashboardItems.length" class="empty">暂无今日推荐数据</div>

      <div v-else class="dashboard-table-wrap">
        <el-table
          :data="dashboardItems"
          size="small"
          class="dashboard-table"
          empty-text="暂无推荐数据"
        >
          <el-table-column label="股票代码" width="110">
            <template #default="{ row }">
              <router-link
                :to="{ name: 'QuoteDetail', params: { symbol: row.stock_code } }"
                class="stock-link"
              >
                {{ row.stock_code }}
              </router-link>
            </template>
          </el-table-column>
          <el-table-column label="股票名称" min-width="100">
            <template #default="{ row }">
              <span class="stock-name-text">{{ row.stock_name || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="评分" width="70" align="right">
            <template #default="{ row }">
              <span class="score-val">{{ row.score?.toFixed(0) || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="排名" width="60" align="right" prop="rank" />
          <el-table-column label="推荐" width="80" align="center">
            <template #default="{ row }">
              <span class="rec-tag" :class="row.recommendation?.toLowerCase()">
                {{ recLabel(row.recommendation) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="信心度" width="80" align="center">
            <template #default="{ row }">
              <span class="confidence-tag" :class="row.confidence">
                {{ confidenceLabel(row.confidence) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="命中率" width="80" align="right">
            <template #default="{ row }">
              <span :class="{ good: row.hit_rate > 0.5, bad: row.hit_rate != null && row.hit_rate <= 0.5 }">
                {{ formatPct(row.hit_rate) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="趋势" width="70" align="center">
            <template #default="{ row }">
              <span class="trend-arrow">{{ trendArrow(row.trend) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="失效条件" min-width="140">
            <template #default="{ row }">
              <span v-if="row.invalidation" class="invalidation-text">
                <template v-if="row.invalidation.exit_threshold != null">退出 &lt; {{ row.invalidation.exit_threshold }}</template>
                <template v-if="row.invalidation.stop_loss_pct != null">
                  <span v-if="row.invalidation.exit_threshold != null">, </span>止损 {{ row.invalidation.stop_loss_pct }}%
                </template>
                <template v-if="!row.invalidation.exit_threshold && row.invalidation.stop_loss_pct == null">--</template>
              </span>
              <span v-else class="text-dim">--</span>
            </template>
          </el-table-column>
          <el-table-column label="仓位建议" width="110" align="right">
            <template #default="{ row }">
              <span v-if="row.position_sizing?.target_weight_pct != null" class="position-text">
                {{ row.position_sizing.target_weight_pct.toFixed(1) }}% / {{ row.position_sizing.max_shares || '--' }}股
              </span>
              <span v-else class="text-dim">--</span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </section>

    <!-- ======================== Decision Journal ======================== -->
    <section class="section">
      <h2>
        决策日志
        <span class="badge">{{ journalTotal }}</span>
      </h2>

      <!-- Journal tabs -->
      <div class="tab-bar">
        <button
          class="tab-btn"
          :class="{ active: journalTab === 'list' }"
          @click="journalTab = 'list'"
        >日志列表</button>
        <button
          class="tab-btn"
          :class="{ active: journalTab === 'summary' }"
          @click="journalTab = 'summary'; fetchJournalSummary(); fetchJournalAttribution()"
        >日志汇总</button>
        <button
          class="tab-btn"
          :class="{ active: journalTab === 'create' }"
          @click="journalTab = 'create'"
        >记录决策</button>
      </div>

      <!-- Tab A: Journal List -->
      <div v-if="journalTab === 'list'">
        <!-- Filters -->
        <div class="journal-filters">
          <el-select
            v-model="journalFilter.execution_type"
            placeholder="执行类型"
            clearable
            class="linear-select"
            style="width: 140px"
            @change="fetchJournal()"
          >
            <el-option label="全部" value="" />
            <el-option label="已执行" value="followed" />
            <el-option label="偏离" value="deviated" />
            <el-option label="错过" value="missed" />
          </el-select>
          <el-input
            v-model="journalFilter.stock_code"
            placeholder="股票代码"
            clearable
            class="linear-input"
            style="width: 140px"
            @clear="fetchJournal()"
            @change="fetchJournal()"
          />
          <el-date-picker
            v-model="journalDateRange"
            type="daterange"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            value-format="YYYY-MM-DD"
            class="linear-picker"
            style="width: 260px"
            @change="fetchJournal()"
          />
        </div>

        <div v-if="journalLoading" class="loading">加载中...</div>
        <el-alert
          v-else-if="journalError"
          class="quality-alert"
          type="error"
          :title="journalError"
          show-icon
          :closable="false"
        />
        <div v-else-if="!journalItems.length" class="empty">暂无决策日志</div>
        <div v-else class="table-wrap">
          <el-table
            :data="journalItems"
            size="small"
            empty-text="暂无数据"
          >
            <el-table-column label="日期" width="105" prop="date" />
            <el-table-column label="股票代码" width="110">
              <template #default="{ row }">
                <router-link
                  :to="{ name: 'QuoteDetail', params: { symbol: row.stock_code } }"
                  class="stock-link"
                >
                  {{ row.stock_code }}
                </router-link>
              </template>
            </el-table-column>
            <el-table-column label="股票名称" min-width="90">
              <template #default="{ row }">
                <span class="stock-name-text">{{ row.stock_name || '--' }}</span>
              </template>
            </el-table-column>
            <el-table-column label="推荐操作" width="80" align="center">
              <template #default="{ row }">
                <span class="action-tag" :class="row.recommended_action?.toLowerCase()">
                  {{ actionLabel(row.recommended_action) }}
                </span>
              </template>
            </el-table-column>
            <el-table-column label="信心度" width="70" align="center">
              <template #default="{ row }">
                <span class="confidence-tag" :class="row.confidence">
                  {{ row.confidence === 'high' ? '高' : row.confidence === 'medium' ? '中' : '低' }}
                </span>
              </template>
            </el-table-column>
            <el-table-column label="执行" width="55" align="center">
              <template #default="{ row }">
                <span>{{ row.executed ? '✅' : '❌' }}</span>
              </template>
            </el-table-column>
            <el-table-column label="执行类型" width="80" align="center">
              <template #default="{ row }">
                <span class="exec-tag" :class="row.execution_type">
                  {{ execLabel(row.execution_type) }}
                </span>
              </template>
            </el-table-column>
            <el-table-column label="盈亏" width="90" align="right">
              <template #default="{ row }">
                <span :class="pnlClass(row.realized_pnl)">
                  {{ formatMoney(row.realized_pnl) }}
                </span>
              </template>
            </el-table-column>
            <el-table-column label="盈亏%" width="80" align="right">
              <template #default="{ row }">
                <span :class="pnlClass(row.realized_pnl_pct)">
                  {{ formatPct2(row.realized_pnl_pct) }}
                </span>
              </template>
            </el-table-column>
          </el-table>

          <!-- Journal pagination -->
          <div v-if="journalTotal > journalPerPage" class="journal-pagination">
            <button
              class="page-btn"
              :disabled="journalPage <= 1"
              @click="journalPage--; fetchJournal()"
            >上一页</button>
            <span class="page-info">{{ journalPage }} / {{ Math.ceil(journalTotal / journalPerPage) }}</span>
            <button
              class="page-btn"
              :disabled="journalPage >= Math.ceil(journalTotal / journalPerPage)"
              @click="journalPage++; fetchJournal()"
            >下一页</button>
          </div>
        </div>
      </div>

      <!-- Tab B: Journal Summary -->
      <div v-if="journalTab === 'summary'">
        <div v-if="summaryLoading" class="loading">加载中...</div>
        <el-alert
          v-else-if="summaryError"
          class="quality-alert"
          type="error"
          :title="summaryError"
          show-icon
          :closable="false"
        />
        <div v-else-if="journalSummary">
          <!-- Summary Cards -->
          <div class="summary-grid">
            <div class="summary-card">
              <span class="s-label">模型质量命中率</span>
              <span class="s-value" :class="journalSummary.model_quality?.hit_rate > 0.5 ? 'good' : 'bad'">
                {{ formatPct(journalSummary.model_quality?.hit_rate) }}
              </span>
            </div>
            <div class="summary-card">
              <span class="s-label">执行纪律</span>
              <span class="s-value" :class="journalSummary.execution_discipline?.follow_through_rate > 0.5 ? 'good' : 'bad'">
                {{ formatPct(journalSummary.execution_discipline?.follow_through_rate) }}
              </span>
            </div>
            <div class="summary-card">
              <span class="s-label">总盈亏</span>
              <span class="s-value" :class="pnlClass(journalSummary.total_pnl)">
                {{ formatMoney(journalSummary.total_pnl) }}
              </span>
            </div>
            <div class="summary-card">
              <span class="s-label">总交易数</span>
              <span class="s-value">{{ journalSummary.total_trades ?? '--' }}</span>
            </div>
          </div>

          <!-- Attribution Tables -->
          <div v-if="journalAttribution" class="attribution-section">
            <h3>盈亏归因 — 按评分组件</h3>
            <div class="table-wrap" v-if="journalAttribution.by_component?.length">
              <el-table :data="journalAttribution.by_component" size="small">
                <el-table-column label="组件" prop="component_id" />
                <el-table-column label="盈亏" width="100" align="right">
                  <template #default="{ row }">
                    <span :class="pnlClass(row.pnl)">{{ formatMoney(row.pnl) }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="盈亏%" width="90" align="right">
                  <template #default="{ row }">
                    <span :class="pnlClass(row.pnl_pct)">{{ formatPct2(row.pnl_pct) }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="交易数" width="80" align="right" prop="trade_count" />
              </el-table>
            </div>
            <div v-else class="empty">暂无组件归因数据</div>

            <h3>盈亏归因 — 按评分周期</h3>
            <div class="table-wrap" v-if="journalAttribution.by_horizon?.length">
              <el-table :data="journalAttribution.by_horizon" size="small">
                <el-table-column label="周期" width="100">
                  <template #default="{ row }">
                    <span>Score {{ row.horizon }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="盈亏" width="100" align="right">
                  <template #default="{ row }">
                    <span :class="pnlClass(row.pnl)">{{ formatMoney(row.pnl) }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="盈亏%" width="90" align="right">
                  <template #default="{ row }">
                    <span :class="pnlClass(row.pnl_pct)">{{ formatPct2(row.pnl_pct) }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="交易数" width="80" align="right" prop="trade_count" />
              </el-table>
            </div>
            <div v-else class="empty">暂无周期归因数据</div>
          </div>
        </div>
        <div v-else class="empty">暂无汇总数据</div>
      </div>

      <!-- Tab C: Log a Decision -->
      <div v-if="journalTab === 'create'" class="form-card">
        <div class="card-header">
          <h3 class="card-title">记录决策</h3>
        </div>
        <div class="form-body">
          <el-alert
            v-if="journalPostError"
            class="quality-alert"
            type="error"
            :title="journalPostError"
            show-icon
            :closable="false"
          />
          <el-form label-position="top" @submit.prevent="handlePostJournal">
            <div class="form-grid">
              <el-form-item label="股票代码">
                <el-input
                  v-model="journalForm.stock_code"
                  placeholder="例如: sh600519"
                  class="linear-input"
                />
              </el-form-item>

              <el-form-item label="推荐操作">
                <el-select
                  v-model="journalForm.recommended_action"
                  placeholder="选择操作"
                  class="linear-select"
                >
                  <el-option label="买入" value="BUY" />
                  <el-option label="卖出" value="SELL" />
                  <el-option label="持有" value="HOLD" />
                  <el-option label="关注" value="WATCH" />
                </el-select>
              </el-form-item>

              <el-form-item label="信心度">
                <el-select
                  v-model="journalForm.confidence"
                  placeholder="选择信心度"
                  class="linear-select"
                >
                  <el-option label="高" value="high" />
                  <el-option label="中" value="medium" />
                  <el-option label="低" value="low" />
                </el-select>
              </el-form-item>

              <el-form-item label="入场价格">
                <el-input-number
                  v-model="journalForm.entry_price"
                  :min="0"
                  :precision="2"
                  controls-position="right"
                  class="linear-input-number"
                />
              </el-form-item>

              <el-form-item label="目标价格">
                <el-input-number
                  v-model="journalForm.target_price"
                  :min="0"
                  :precision="2"
                  controls-position="right"
                  class="linear-input-number"
                />
              </el-form-item>

              <el-form-item label="止损价格">
                <el-input-number
                  v-model="journalForm.stop_loss"
                  :min="0"
                  :precision="2"
                  controls-position="right"
                  class="linear-input-number"
                />
              </el-form-item>

              <el-form-item label="仓位比例 (%)">
                <el-input-number
                  v-model="journalForm.position_size_pct"
                  :min="0"
                  :max="100"
                  :precision="1"
                  controls-position="right"
                  class="linear-input-number"
                />
              </el-form-item>
            </div>

            <el-form-item label="是否已执行">
              <el-switch v-model="journalForm.executed" />
            </el-form-item>

            <div v-if="journalForm.executed" class="form-grid">
              <el-form-item label="执行价格">
                <el-input-number
                  v-model="journalForm.executed_price"
                  :min="0"
                  :precision="2"
                  controls-position="right"
                  class="linear-input-number"
                />
              </el-form-item>
              <el-form-item label="执行数量">
                <el-input-number
                  v-model="journalForm.executed_quantity"
                  :min="100"
                  :step="100"
                  controls-position="right"
                  class="linear-input-number"
                />
              </el-form-item>
            </div>

            <el-form-item label="备注">
              <el-input
                v-model="journalForm.notes"
                type="textarea"
                :rows="3"
                placeholder="可选：记录决策理由或复盘笔记"
                class="linear-input"
              />
            </el-form-item>
          </el-form>

          <div class="form-actions">
            <button
              class="primary-btn"
              :disabled="journalPosting"
              @click="handlePostJournal"
            >
              {{ journalPosting ? '提交中...' : '提交' }}
            </button>
          </div>
        </div>
      </div>
    </section>

    <!-- ======================== Rebalance Preview ======================== -->
    <section class="section">
      <h2>再平衡预览</h2>
      <div class="form-card">
        <div class="form-body">
          <el-alert
            v-if="rebalanceError"
            class="quality-alert"
            type="error"
            :title="rebalanceError"
            show-icon
            :closable="false"
          />
          <el-form label-position="top" @submit.prevent="handleRebalance">
            <div class="form-grid">
              <el-form-item label="现金余额">
                <el-input-number
                  v-model="rebalanceCash"
                  :min="0"
                  :precision="2"
                  controls-position="right"
                  class="linear-input-number"
                />
              </el-form-item>
            </div>
            <el-form-item label="持仓股票代码（每行一个，逗号或换行分隔）">
              <el-input
                v-model="rebalanceStocks"
                type="textarea"
                :rows="3"
                placeholder="sh600519&#10;sz000001"
                class="linear-input"
              />
            </el-form-item>
          </el-form>
          <div class="form-actions">
            <button
              class="primary-btn"
              :disabled="rebalanceLoading"
              @click="handleRebalance"
            >
              {{ rebalanceLoading ? '计算中...' : '预览' }}
            </button>
          </div>
        </div>
      </div>

      <!-- Rebalance results -->
      <div v-if="rebalanceItems.length" class="table-wrap" style="margin-top: 16px;">
        <el-table
          :data="rebalanceItems"
          size="small"
          empty-text="无结果"
        >
          <el-table-column label="股票代码" width="110">
            <template #default="{ row }">
              <router-link
                :to="{ name: 'QuoteDetail', params: { symbol: row.stock_code } }"
                class="stock-link"
              >
                {{ row.stock_code }}
              </router-link>
            </template>
          </el-table-column>
          <el-table-column label="股票名称" min-width="90">
            <template #default="{ row }">
              <span class="stock-name-text">{{ row.stock_name || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="当前评分" width="80" align="right">
            <template #default="{ row }">
              <span class="score-val">{{ row.current_score?.toFixed(0) || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="推荐" width="80" align="center">
            <template #default="{ row }">
              <span class="rec-tag" :class="row.recommendation?.toLowerCase()">
                {{ recLabel(row.recommendation) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="90" align="center">
            <template #default="{ row }">
              <span class="rebalance-action-tag" :class="actionClass(row.action)">
                {{ rebalanceActionLabel(row.action) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="理由" min-width="160">
            <template #default="{ row }">
              <span class="reason-text">{{ row.reason || '--' }}</span>
            </template>
          </el-table-column>
        </el-table>
      </div>
      <div v-else-if="rebalanceRan && !rebalanceLoading" class="empty" style="margin-top: 12px;">
        暂无再平衡建议
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref, watch } from 'vue'
import axios from 'axios'
import { ElMessage } from 'element-plus'
import {
  decisionsApi,
  type DecisionDashboardItem,
  type JournalEntry,
  type JournalSummary,
  type JournalAttribution,
  type RebalancePreviewItem
} from '@/api/decisions'

const horizon = ref(20)
const loading = ref(false)
const qualityLoading = ref(false)
const alerts = ref<any[]>([])
const quality = ref<any>(null)

// Dashboard state
const dashHorizon = ref(20)
const dashLoading = ref(false)
const dashError = ref('')
const dashboardItems = ref<DecisionDashboardItem[]>([])

async function fetchAlerts() {
  loading.value = true
  try {
    const res = await axios.get('/api/decisions/alerts', {
      params: { horizon: horizon.value, limit: 20 }
    })
    alerts.value = res.data.data?.alerts || []
  } catch (e) {
    console.error('Failed to fetch alerts', e)
  } finally {
    loading.value = false
  }
}

async function fetchQuality() {
  qualityLoading.value = true
  try {
    const res = await axios.get('/api/decisions/quality', {
      params: { horizon: horizon.value }
    })
    quality.value = res.data.data
  } catch (e) {
    console.error('Failed to fetch quality', e)
  } finally {
    qualityLoading.value = false
  }
}

function fetchAll() {
  fetchAlerts()
  fetchQuality()
}

async function fetchDashboard() {
  dashLoading.value = true
  dashError.value = ''
  try {
    const res = await decisionsApi.getDashboard({
      horizon: dashHorizon.value,
      limit: 20
    })
    const data = res.data
    const key = `score${dashHorizon.value}` as 'score5' | 'score20' | 'score60'
    const horizonData = data[key]
    dashboardItems.value = horizonData?.items || []
    if (!dashboardItems.value.length) {
      dashError.value = ''
    }
  } catch (e: any) {
    dashError.value = e?.response?.data?.message || '获取推荐数据失败'
    console.error('Failed to fetch dashboard', e)
    dashboardItems.value = []
  } finally {
    dashLoading.value = false
  }
}

function formatPct(val: number | null | undefined): string {
  if (val == null) return '--'
  return (val * 100).toFixed(1) + '%'
}

function recLabel(val: string): string {
  if (val === 'BUY') return '买入'
  if (val === 'WATCH') return '关注'
  if (val === 'AVOID') return '回避'
  return val || '--'
}

function confidenceLabel(val: string): string {
  if (val === 'high') return '高'
  if (val === 'medium') return '中'
  if (val === 'low') return '低'
  return val || '--'
}

function trendArrow(val: string): string {
  if (val === 'improving') return '↑'
  if (val === 'declining') return '↓'
  if (val === 'stable') return '→'
  return '--'
}

// ---- Journal State ----
const journalTab = ref<'list' | 'summary' | 'create'>('list')
const journalLoading = ref(false)
const journalError = ref('')
const journalItems = ref<JournalEntry[]>([])
const journalPage = ref(1)
const journalPerPage = ref(20)
const journalTotal = ref(0)
const journalDateRange = ref<string[] | null>(null)
const journalFilter = reactive({
  execution_type: '' as string,
  stock_code: '' as string
})

// Journal summary
const summaryLoading = ref(false)
const summaryError = ref('')
const journalSummary = ref<JournalSummary | null>(null)
const journalAttribution = ref<JournalAttribution | null>(null)

// Journal create form
const journalPosting = ref(false)
const journalPostError = ref('')
const journalForm = reactive({
  stock_code: '',
  recommended_action: '' as string,
  confidence: '' as string,
  entry_price: undefined as number | undefined,
  target_price: undefined as number | undefined,
  stop_loss: undefined as number | undefined,
  position_size_pct: undefined as number | undefined,
  executed: false,
  executed_price: undefined as number | undefined,
  executed_quantity: undefined as number | undefined,
  notes: ''
})

// ---- Rebalance State ----
const rebalanceCash = ref(100000)
const rebalanceStocks = ref('')
const rebalanceLoading = ref(false)
const rebalanceError = ref('')
const rebalanceItems = ref<RebalancePreviewItem[]>([])
const rebalanceRan = ref(false)

// ---- Journal API ----
async function fetchJournal() {
  journalLoading.value = true
  journalError.value = ''
  try {
    const params: any = {
      page: journalPage.value,
      per_page: journalPerPage.value
    }
    if (journalFilter.execution_type) params.execution_type = journalFilter.execution_type
    if (journalFilter.stock_code) params.stock_code = journalFilter.stock_code
    if (journalDateRange.value?.length === 2) {
      params.start_date = journalDateRange.value[0]
      params.end_date = journalDateRange.value[1]
    }
    const res = await decisionsApi.getJournal(params)
    journalItems.value = res.data?.items || []
    journalTotal.value = res.data?.total || 0
  } catch (e: any) {
    journalError.value = e?.response?.data?.message || '获取日志失败'
  } finally {
    journalLoading.value = false
  }
}

async function fetchJournalSummary() {
  summaryLoading.value = true
  summaryError.value = ''
  try {
    const res = await decisionsApi.getJournalSummary()
    journalSummary.value = res.data
  } catch (e: any) {
    summaryError.value = e?.response?.data?.message || '获取汇总失败'
  } finally {
    summaryLoading.value = false
  }
}

async function fetchJournalAttribution() {
  try {
    const res = await decisionsApi.getJournalAttribution()
    journalAttribution.value = res.data
  } catch {
    // attribution may not be available — non-blocking
    journalAttribution.value = null
  }
}

async function handlePostJournal() {
  if (!journalForm.stock_code.trim()) {
    ElMessage.warning('请输入股票代码')
    return
  }
  if (!journalForm.recommended_action) {
    ElMessage.warning('请选择推荐操作')
    return
  }
  if (!journalForm.confidence) {
    ElMessage.warning('请选择信心度')
    return
  }

  journalPosting.value = true
  journalPostError.value = ''
  try {
    await decisionsApi.postJournal({
      stock_code: journalForm.stock_code.trim(),
      recommended_action: journalForm.recommended_action,
      confidence: journalForm.confidence,
      entry_price: journalForm.entry_price,
      target_price: journalForm.target_price,
      stop_loss: journalForm.stop_loss,
      position_size_pct: journalForm.position_size_pct,
      executed: journalForm.executed,
      executed_price: journalForm.executed ? journalForm.executed_price : undefined,
      executed_quantity: journalForm.executed ? journalForm.executed_quantity : undefined,
      notes: journalForm.notes || undefined
    })
    ElMessage.success('决策记录已保存')
    // Reset form
    journalForm.stock_code = ''
    journalForm.recommended_action = ''
    journalForm.confidence = ''
    journalForm.entry_price = undefined
    journalForm.target_price = undefined
    journalForm.stop_loss = undefined
    journalForm.position_size_pct = undefined
    journalForm.executed = false
    journalForm.executed_price = undefined
    journalForm.executed_quantity = undefined
    journalForm.notes = ''
    journalTab.value = 'list'
    journalPage.value = 1
    fetchJournal()
  } catch (e: any) {
    journalPostError.value = e?.response?.data?.message || '提交失败'
  } finally {
    journalPosting.value = false
  }
}

// ---- Rebalance API ----
async function handleRebalance() {
  const codesRaw = rebalanceStocks.value.trim()
  if (!codesRaw) {
    ElMessage.warning('请输入持仓股票代码')
    return
  }
  const portfolioStocks = codesRaw
    .split(/[,\n]+/)
    .map((s) => s.trim())
    .filter(Boolean)

  if (!portfolioStocks.length) {
    ElMessage.warning('请至少输入一个有效的股票代码')
    return
  }

  rebalanceLoading.value = true
  rebalanceError.value = ''
  rebalanceRan.value = false
  try {
    const res = await decisionsApi.postRebalancePreview({
      portfolio_stocks: portfolioStocks,
      cash: rebalanceCash.value
    })
    rebalanceItems.value = res.data?.items || []
    rebalanceRan.value = true
  } catch (e: any) {
    rebalanceError.value = e?.response?.data?.message || '获取再平衡预览失败'
  } finally {
    rebalanceLoading.value = false
  }
}

// ---- Utility ----
function formatMoney(val: number | null | undefined): string {
  if (val == null || !Number.isFinite(val)) return '--'
  return '¥' + val.toFixed(2)
}

function formatPct2(val: number | null | undefined): string {
  if (val == null || !Number.isFinite(val)) return '--'
  return (val * 100).toFixed(2) + '%'
}

function pnlClass(val: number | null | undefined): string {
  if (val == null || !Number.isFinite(val)) return ''
  return val > 0 ? 'positive' : val < 0 ? 'negative' : ''
}

function actionLabel(val: string): string {
  const m: Record<string, string> = { BUY: '买入', SELL: '卖出', HOLD: '持有', WATCH: '关注' }
  return m[val] || val || '--'
}

function execLabel(val: string | null): string {
  const m: Record<string, string> = { followed: '已执行', deviated: '偏离', missed: '错过' }
  return m[val || ''] || val || '--'
}

function rebalanceActionLabel(val: string): string {
  const m: Record<string, string> = { BUY_MORE: '加仓', SELL: '卖出', REDUCE: '减仓', HOLD: '持有' }
  return m[val] || val || '--'
}

function actionClass(val: string): string {
  if (val === 'BUY_MORE') return 'buy'
  if (val === 'SELL' || val === 'REDUCE') return 'sell'
  return 'hold'
}

onMounted(() => {
  fetchAll()
  fetchDashboard()
  fetchJournal()
})
watch(horizon, fetchAll)
</script>

<style scoped lang="scss">
.decisions-page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 32px 24px;
  font-family: 'Inter Variable', Inter, sans-serif;
  color: #d0d6e0;
}

.page-hero {
  margin-bottom: 28px;
  .eyebrow { font-size: 13px; font-weight: 510; color: #7170ff; text-transform: uppercase; letter-spacing: 0.1em; }
  .page-title { font-size: 40px; font-weight: 510; color: #f7f8f8; margin: 8px 0; letter-spacing: -0.88px; }
  .subtitle { font-size: 15px; color: #8a8f98; }
}

.quality-alert { margin-bottom: 20px; }

.horizon-bar {
  display: flex; gap: 4px; margin-bottom: 28px;
  background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.05);
  border-radius: 8px; padding: 3px; width: fit-content;
  .hz-btn {
    font-size: 12px; font-weight: 590; padding: 6px 16px; border: none; border-radius: 6px;
    background: transparent; color: #62666d; cursor: pointer;
    &.active { background: #5e6ad2; color: #f7f8f8; }
    &:hover:not(.active) { color: #d0d6e0; }
  }
}

.section {
  margin-bottom: 32px;
  h2 { font-size: 18px; font-weight: 590; color: #f7f8f8; margin: 0 0 16px 0; display: flex; align-items: center; gap: 10px; }
  .badge { font-size: 11px; font-weight: 510; color: #8a8f98; background: rgba(255,255,255,0.06); padding: 2px 10px; border-radius: 999px; }
  .loading, .empty { font-size: 14px; color: #62666d; padding: 20px 0; }
}

.alert-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 12px;
}

.alert-card {
  background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.06);
  border-radius: 12px; padding: 16px;
  &.strong_buy { border-color: rgba(16, 185, 129, 0.2); background: rgba(16, 185, 129, 0.04); }
  &.score_jump { border-color: rgba(113, 112, 255, 0.2); background: rgba(113, 112, 255, 0.04); }
  .alert-header { display: flex; align-items: baseline; gap: 8px; margin-bottom: 8px; }
  .stock-name { font-size: 15px; font-weight: 590; color: #f7f8f8; }
  .stock-code { font-size: 11px; color: #62666d; font-family: 'Berkeley Mono', monospace; }
  .alert-score { display: flex; align-items: baseline; gap: 8px; margin-bottom: 6px; }
  .score-value { font-size: 32px; font-weight: 590; color: #f7f8f8; }
  .score-delta { font-size: 14px; font-weight: 510; &.up { color: #ef4444; } &.down { color: #22c55e; } }
  .alert-detail { font-size: 13px; color: #8a8f98; margin-bottom: 8px; }
  .alert-meta { display: flex; gap: 8px; align-items: center; }
  .rec-tag { font-size: 10px; font-weight: 590; padding: 2px 8px; border-radius: 4px;
    &.buy { background: rgba(16, 185, 129, 0.1); color: #10b981; }
    &.watch { background: rgba(245, 158, 11, 0.1); color: #f59e0b; }
    &.avoid { background: rgba(239, 68, 68, 0.1); color: #ef4444; }
  }
  .rank { font-size: 11px; color: #62666d; }
}

.quality-metrics {
  display: flex; gap: 24px; margin-bottom: 16px;
  .q-metric { display: flex; flex-direction: column; gap: 4px; }
  .q-label { font-size: 12px; color: #62666d; }
  .q-value { font-size: 20px; font-weight: 590; &.good { color: #10b981; } &.bad { color: #ef4444; } }
}

.trend-bars {
  display: flex; align-items: flex-end; gap: 3px; height: 60px;
  padding: 8px 12px; background: rgba(255,255,255,0.02); border-radius: 8px;
  .trend-bar {
    flex: 1; min-width: 6px; background: #5e6ad2; border-radius: 2px 2px 0 0;
    transition: height 0.3s; opacity: 0.8; cursor: pointer;
    &:hover { opacity: 1; }
  }
}

.dashboard-horizon-bar {
  display: flex; gap: 4px; margin-bottom: 16px;
  background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.05);
  border-radius: 8px; padding: 3px; width: fit-content;
  .hz-btn {
    font-size: 12px; font-weight: 590; padding: 6px 16px; border: none; border-radius: 6px;
    background: transparent; color: #62666d; cursor: pointer;
    &.active { background: #5e6ad2; color: #f7f8f8; }
    &:hover:not(.active) { color: #d0d6e0; }
  }
}

.dashboard-table-wrap {
  overflow-x: auto;
}

.dashboard-table {
  font-size: 13px;
}

.stock-link {
  color: #7170ff;
  text-decoration: none;
  font-family: 'Berkeley Mono', monospace;
  font-size: 12px;
  &:hover { text-decoration: underline; }
}

.stock-name-text {
  color: #f7f8f8;
  font-weight: 510;
}

.score-val {
  font-weight: 590;
  color: #f7f8f8;
}

.confidence-tag {
  font-size: 11px;
  font-weight: 510;
  padding: 2px 8px;
  border-radius: 4px;
  &.high {
    background: rgba(16, 185, 129, 0.1);
    color: #10b981;
  }
  &.medium {
    background: rgba(245, 158, 11, 0.1);
    color: #f59e0b;
  }
  &.low {
    background: rgba(239, 68, 68, 0.1);
    color: #ef4444;
  }
}

.trend-arrow {
  font-size: 16px;
  font-weight: 590;
}

.invalidation-text {
  font-size: 11px;
  color: #8a8f98;
  font-family: 'Berkeley Mono', monospace;
}

.position-text {
  font-size: 12px;
  color: #d0d6e0;
  font-family: 'Berkeley Mono', monospace;
}

.text-dim {
  font-size: 12px;
  color: #62666d;
}

.good { color: #10b981; font-weight: 510; }
.bad { color: #ef4444; font-weight: 510; }

// ---- Journal tab bar ----
.tab-bar {
  display: flex; gap: 4px; margin-bottom: 20px;
  background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.05);
  border-radius: 8px; padding: 3px; width: fit-content;
  .tab-btn {
    font-size: 12px; font-weight: 590; padding: 6px 16px; border: none; border-radius: 6px;
    background: transparent; color: #62666d; cursor: pointer;
    &.active { background: #5e6ad2; color: #f7f8f8; }
    &:hover:not(.active) { color: #d0d6e0; }
  }
}

// ---- Journal filters ----
.journal-filters {
  display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; align-items: center;
}

// ---- Pagination ----
.journal-pagination {
  display: flex; align-items: center; justify-content: center; gap: 12px; margin-top: 16px;
  .page-btn {
    font-size: 12px; font-weight: 510; padding: 6px 14px; border: 1px solid rgba(255,255,255,0.08);
    border-radius: 6px; background: rgba(255,255,255,0.02); color: #d0d6e0; cursor: pointer;
    &:hover:not(:disabled) { color: #f7f8f8; background: rgba(255,255,255,0.05); }
    &:disabled { opacity: 0.3; cursor: not-allowed; }
  }
  .page-info { font-size: 12px; color: #8a8f98; }
}

// ---- Summary cards ----
.summary-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; margin-bottom: 24px;
}
.summary-card {
  background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.06);
  border-radius: 12px; padding: 16px; display: flex; flex-direction: column; gap: 4px;
  .s-label { font-size: 12px; color: #62666d; }
  .s-value { font-size: 24px; font-weight: 590; color: #f7f8f8; }
}

// ---- Attribution section ----
.attribution-section {
  h3 { font-size: 14px; font-weight: 590; color: #d0d6e0; margin: 16px 0 8px; }
}

// ---- Forms ----
.form-card {
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 12px; overflow: hidden;
  .card-header { padding: 20px 24px 0; }
  .card-title { font-size: 16px; font-weight: 590; color: #f7f8f8; }
  .form-body { padding: 16px 24px 24px; }
}

.form-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 0 16px;
}

.form-actions {
  display: flex; gap: 8px; justify-content: flex-end; margin-top: 16px;
}

.primary-btn {
  font-size: 13px; font-weight: 510; padding: 8px 20px; border: none; border-radius: 6px;
  background: #5e6ad2; color: #f7f8f8; cursor: pointer;
  &:hover { background: #7170ff; }
  &:disabled { opacity: 0.5; cursor: not-allowed; }
}

// ---- Table-wrap utility ----
.table-wrap { overflow-x: auto; }

// ---- Tag styles ----
.action-tag {
  font-size: 10px; font-weight: 590; padding: 2px 8px; border-radius: 4px;
  &.buy { background: rgba(16, 185, 129, 0.1); color: #10b981; }
  &.sell { background: rgba(239, 68, 68, 0.1); color: #ef4444; }
  &.hold { background: rgba(245, 158, 11, 0.1); color: #f59e0b; }
  &.watch { background: rgba(113, 112, 255, 0.1); color: #7170ff; }
}

.exec-tag {
  font-size: 10px; font-weight: 510; padding: 2px 8px; border-radius: 4px;
  &.followed { background: rgba(16, 185, 129, 0.1); color: #10b981; }
  &.deviated { background: rgba(245, 158, 11, 0.1); color: #f59e0b; }
  &.missed { background: rgba(239, 68, 68, 0.1); color: #ef4444; }
}

.rebalance-action-tag {
  font-size: 10px; font-weight: 590; padding: 2px 8px; border-radius: 4px;
  &.buy { background: rgba(16, 185, 129, 0.1); color: #10b981; }
  &.sell { background: rgba(239, 68, 68, 0.1); color: #ef4444; }
  &.hold { background: rgba(113, 112, 255, 0.1); color: #7170ff; }
}

.positive { color: #10b981; font-weight: 510; }
.negative { color: #ef4444; font-weight: 510; }

.reason-text { font-size: 12px; color: #8a8f98; }
</style>
