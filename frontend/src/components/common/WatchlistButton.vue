<template>
  <button
    class="watchlist-btn"
    :class="{ watched: isWatched }"
    :title="isWatched ? '取消关注' : '添加关注'"
    @click.stop="handleToggle"
  >
    <el-icon :size="iconSize">
      <StarFilled v-if="isWatched" />
      <Star v-else />
    </el-icon>
  </button>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Star, StarFilled } from '@element-plus/icons-vue'
import { useWatchlistStore } from '@/stores/watchlist'

const props = withDefaults(defineProps<{
  code: string
  name: string
  size?: 'small' | 'default'
}>(), {
  size: 'default'
})

const watchlistStore = useWatchlistStore()
const isWatched = computed(() => watchlistStore.isWatched(props.code))
const iconSize = computed(() => props.size === 'small' ? 14 : 16)

function handleToggle() {
  watchlistStore.toggle(props.code, props.name)
}
</script>

<style scoped lang="scss">
.watchlist-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.02);
  color: #8a8f98;
  cursor: pointer;
  transition: all 0.2s ease;
  padding: 0;

  &:hover {
    background: rgba(255, 255, 255, 0.05);
    border-color: rgba(255, 255, 255, 0.12);
    color: #f59e0b;
  }

  &.watched {
    color: #f59e0b;
    background: rgba(245, 158, 11, 0.08);
    border-color: rgba(245, 158, 11, 0.2);

    &:hover {
      background: rgba(245, 158, 11, 0.12);
      border-color: rgba(245, 158, 11, 0.3);
      color: #fbbf24;
    }
  }
}
</style>
