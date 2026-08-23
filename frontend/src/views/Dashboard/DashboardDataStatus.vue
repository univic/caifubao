<template>
  <div class="data-status card" v-if="marketStore.dataStatus">
    <div class="section-header">
      <div>
        <h3>数据处理引擎状态</h3>
        <p class="section-desc">
          最新完整交易日:
          {{ formatDate(marketStore.dataStatus.reference_dates.latest_complete_trading_day) || '未知' }}
        </p>
      </div>
      <div class="section-meta">
        统计于 {{ formatDateTime(marketStore.dataStatus.generated_at) }}
      </div>
    </div>

    <div v-if="marketStore.statusLoading && !marketStore.dataStatus" class="status-loading">
      加载中...
    </div>

    <div v-else class="status-grid">
      <div class="status-panel">
        <div class="status-panel-header">
          <h4>指数数据</h4>
          <span class="status-badge" :class="{ healthy: marketStore.dataStatus.index.is_up_to_date }">
            {{ marketStore.dataStatus.index.is_up_to_date ? '已同步' : '落后' }}
          </span>
        </div>
        <div class="status-metrics">
          <div class="metric-pill healthy">
            <span class="label">正常</span>
            <span class="value">{{ marketStore.dataStatus.index.up_to_date_count }}</span>
          </div>
          <div class="metric-pill danger">
            <span class="label">过期</span>
            <span class="value">{{ marketStore.dataStatus.index.expired_count }}</span>
          </div>
        </div>
      </div>

      <div class="status-panel">
        <div class="status-panel-header">
          <h4>个股行情</h4>
          <span class="status-badge" :class="{ healthy: marketStore.dataStatus.stock.is_up_to_date }">
            {{ marketStore.dataStatus.stock.is_up_to_date ? '已同步' : '落后' }}
          </span>
        </div>
        <div class="status-metrics">
          <div class="metric-pill healthy">
            <span class="label">正常</span>
            <span class="value">{{ marketStore.dataStatus.stock.up_to_date_count }}</span>
          </div>
          <div class="metric-pill danger">
            <span class="label">过期</span>
            <span class="value">{{ marketStore.dataStatus.stock.expired_count }}</span>
          </div>
        </div>
      </div>

      <div class="status-panel">
        <div class="status-panel-header">
          <h4>评分管线</h4>
          <span
            class="status-badge"
            :class="{ healthy: marketStore.dataStatus?.scoring_run_today }"
          >
            {{ marketStore.dataStatus?.scoring_run_today ? '已运行' : '未运行' }}
          </span>
        </div>
        <div class="status-metrics">
          <div class="metric-pill" :class="marketStore.dataStatus?.signal_run_today ? 'healthy' : 'danger'">
            <span class="label">信号</span>
            <span class="value">{{ marketStore.dataStatus?.signal_run_today ? 'SUCCESS' : '未执行' }}</span>
          </div>
          <div class="metric-pill" :class="marketStore.dataStatus?.scoring_run_today ? 'healthy' : 'danger'">
            <span class="label">评分</span>
            <span class="value">{{ marketStore.dataStatus?.scoring_run_today ? 'SUCCESS' : '未执行' }}</span>
          </div>
        </div>
        <el-button
          v-if="!marketStore.dataStatus?.scoring_run_today"
          size="small"
          class="btn-ghost-sm"
          style="margin-top: 10px; width: 100%"
          :loading="generatingScores"
          @click="$emit('generateScores')"
        >
          {{ generatingScores ? '生成中...' : '生成评分' }}
        </el-button>
        <p v-if="scoreGenMessage" class="status-msg" :class="scoreGenMessage.includes('失败') ? 'error' : 'success'">
          {{ scoreGenMessage }}
        </p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useMarketStore } from '@/stores/market'

defineProps<{
  generatingScores: boolean
  scoreGenMessage: string
}>()

defineEmits<{
  generateScores: []
}>()

const marketStore = useMarketStore()

function formatDate(value: string | null) {
  if (!value) return ''
  return value.slice(0, 10)
}

function formatDateTime(value: string | null) {
  if (!value) return ''
  return new Date(value).toLocaleString('zh-CN')
}
</script>

<style scoped lang="scss">
.data-status {
  .section-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 16px;
    h3 { margin: 0 0 2px; font-size: 16px; font-weight: 510; color: var(--color-text-primary); }
    .section-desc { font-size: 13px; color: var(--color-text-secondary); margin: 4px 0 0; }
    .section-meta { font-size: 12px; color: var(--color-text-placeholder); font-family: 'Berkeley Mono', ui-monospace, SF Mono, Menlo, monospace; }
  }
  .status-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; @media (max-width: 768px) { grid-template-columns: 1fr; } }
  .status-panel {
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid rgba(255, 255, 255, 0.05);
    border-radius: 8px;
    padding: 16px;
    .status-panel-header { display: flex; justify-content: space-between; margin-bottom: 10px; h4 { margin: 0; font-size: 14px; font-weight: 510; color: var(--color-text-primary); } }
    .status-badge {
      font-size: 12px;
      padding: 2px 8px;
      border-radius: 999px;
      background: rgba(239, 68, 68, 0.12);
      color: #f87171;
      &.healthy { background: rgba(39, 166, 68, 0.12); color: #4ade80; }
    }
  }
  .status-metrics { display: flex; gap: 10px; }
  .metric-pill {
    flex: 1;
    text-align: center;
    padding: 6px 8px;
    border-radius: 6px;
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.05);
    .label { display: block; font-size: 10px; color: var(--color-text-placeholder); margin-bottom: 2px; }
    .value { font-size: 14px; font-weight: 590; color: var(--color-text-primary); }
    &.healthy { background: rgba(39, 166, 68, 0.06); }
    &.danger { background: rgba(239, 68, 68, 0.06); color: #f87171; .value { color: #f87171; } }
  }
  .status-loading { text-align: center; padding: 20px; color: var(--color-text-placeholder); }
  .status-msg {
    font-size: 12px; margin-top: 8px;
    &.success { color: #4ade80; }
    &.error { color: #f87171; }
  }
  .btn-ghost-sm {
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--color-text-secondary);
  }
}
</style>
