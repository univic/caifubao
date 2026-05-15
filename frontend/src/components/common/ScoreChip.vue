<template>
  <div class="mini-score-chip" :class="scoreClass">
    <span class="chip-horizon">S{{ horizon }}</span>
    <span class="chip-score">{{ displayScore }}</span>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  horizon: number
  score: number | null | undefined
}>()

const displayScore = computed(() => {
  if (props.score === null || props.score === undefined) return '--'
  return props.score.toFixed(1)
})

const scoreClass = computed(() => {
  if (props.score === null || props.score === undefined) return ''
  if (props.score >= 80) return 'high'
  if (props.score >= 60) return 'medium'
  return 'low'
})
</script>

<style scoped lang="scss">
.mini-score-chip {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 2px 8px;
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid rgba(255, 255, 255, 0.06);
  min-width: 52px;

  &.high {
    background: rgba(16, 185, 129, 0.08);
    border-color: rgba(16, 185, 129, 0.25);
    .chip-score { color: #10b981; }
  }
  &.medium {
    background: rgba(245, 158, 11, 0.08);
    border-color: rgba(245, 158, 11, 0.25);
    .chip-score { color: #f59e0b; }
  }
  &.low {
    .chip-score { color: #8a8f98; }
  }

  .chip-horizon {
    font-size: 9px;
    color: #8a8f98;
    font-weight: 500;
    text-transform: uppercase;
  }
  .chip-score {
    font-size: 13px;
    font-weight: 700;
    color: #d0d6e0;
  }
}
</style>
