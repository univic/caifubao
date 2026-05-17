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
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import axios from 'axios'

const horizon = ref(20)
const loading = ref(false)
const qualityLoading = ref(false)
const alerts = ref<any[]>([])
const quality = ref<any>(null)

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

function formatPct(val: number | null | undefined): string {
  if (val == null) return '--'
  return (val * 100).toFixed(1) + '%'
}

onMounted(fetchAll)
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
</style>
