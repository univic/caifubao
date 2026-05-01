<template>
  <div class="indices-view">
    <div class="page-header">
      <h1 class="page-title">指数全览</h1>
      <p class="page-desc">A股主要指数实时行情</p>
    </div>

    <!-- Main Indices Cards -->
    <div class="section-title">
      <span>主要指数</span>
    </div>
    <div v-if="indicesStore.overviewLoading" class="loading-state">
      <el-icon class="is-loading"><Loading /></el-icon>
      <span>加载中...</span>
    </div>
    <div v-else-if="indicesStore.overviewError" class="error-state">
      <el-alert type="error" :title="indicesStore.overviewError" :closable="false" />
    </div>
    <div v-else class="index-cards">
      <div
        v-for="item in indicesStore.overviewIndices"
        :key="item.code"
        class="index-card"
      >
        <div class="index-name">{{ item.name }}</div>
        <div class="index-code">{{ item.code }}</div>
        <div class="index-price">{{ item.price?.toFixed(2) ?? '--' }}</div>
        <div class="index-change" :class="item.change >= 0 ? 'up' : 'down'">
          {{ item.change >= 0 ? '+' : '' }}{{ item.change?.toFixed(2) ?? '--' }}
          ({{ item.changePct >= 0 ? '+' : '' }}{{ item.changePct?.toFixed(2) ?? '--' }}%)
        </div>
        <div class="index-details">
          <div class="detail-row">
            <span class="label">昨收:</span>
            <span>{{ item.previousClose?.toFixed(2) ?? '--' }}</span>
          </div>
          <div class="detail-row">
            <span class="label">开盘:</span>
            <span>{{ item.open?.toFixed(2) ?? '--' }}</span>
          </div>
          <div class="detail-row">
            <span class="label">最高:</span>
            <span>{{ item.high?.toFixed(2) ?? '--' }}</span>
          </div>
          <div class="detail-row">
            <span class="label">最低:</span>
            <span>{{ item.low?.toFixed(2) ?? '--' }}</span>
          </div>
          <div class="detail-row">
            <span class="label">成交量:</span>
            <span>{{ formatVolume(item.volume) }}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- All Indices List -->
    <div class="section-title" style="margin-top: 32px">
      <span>全部指数</span>
      <div class="sort-controls">
        <span class="sort-label">排序:</span>
        <el-select v-model="sortConfig.sortBy" size="small" style="width: 140px" @change="handleSortChange">
          <el-option label="涨跌幅" value="change_rate" />
          <el-option label="最新价" value="close" />
          <el-option label="成交量" value="volume" />
          <el-option label="代码" value="code" />
        </el-select>
        <el-button-group size="small">
          <el-button :type="sortConfig.order === 'desc' ? 'primary' : ''" @click="setSortOrder('desc')">降序</el-button>
          <el-button :type="sortConfig.order === 'asc' ? 'primary' : ''" @click="setSortOrder('asc')">升序</el-button>
        </el-button-group>
      </div>
    </div>

    <div v-if="indicesStore.listLoading && indicesStore.indicesList.length === 0" class="loading-state">
      <el-icon class="is-loading"><Loading /></el-icon>
      <span>加载中...</span>
    </div>
    <div v-else-if="indicesStore.listError && indicesStore.indicesList.length === 0" class="error-state">
      <el-alert type="error" :title="indicesStore.listError" :closable="false" />
    </div>
    <div v-else class="indices-table-wrapper">
      <el-table
        :data="indicesStore.indicesList"
        stripe
        style="width: 100%"
        v-loading="indicesStore.listLoading"
        :default-sort="{ prop: sortConfig.sortBy, order: sortConfig.order === 'desc' ? 'descending' : 'ascending' }"
        @sort-change="handleTableSortChange"
      >
        <el-table-column prop="code" label="代码" width="100" sortable="custom" />
        <el-table-column prop="name" label="名称" width="120" />
        <el-table-column prop="close" label="最新价" width="100" sortable="custom">
          <template #default="{ row }">
            {{ row.close?.toFixed(2) ?? '--' }}
          </template>
        </el-table-column>
        <el-table-column prop="previousClose" label="昨收" width="100">
          <template #default="{ row }">
            {{ row.previousClose?.toFixed(2) ?? '--' }}
          </template>
        </el-table-column>
        <el-table-column prop="changeRate" label="涨跌幅" width="110" sortable="custom">
          <template #default="{ row }">
            <span :class="row.changeRate >= 0 ? 'up' : 'down'">
              {{ row.changeRate >= 0 ? '+' : '' }}{{ row.changeRate?.toFixed(2) ?? '0.00' }}%
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="changeAmount" label="涨跌额" width="100">
          <template #default="{ row }">
            <span :class="row.changeAmount >= 0 ? 'up' : 'down'">
              {{ row.changeAmount >= 0 ? '+' : '' }}{{ row.changeAmount?.toFixed(2) ?? '0.00' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="open" label="开盘" width="100">
          <template #default="{ row }">
            {{ row.open?.toFixed(2) ?? '--' }}
          </template>
        </el-table-column>
        <el-table-column prop="high" label="最高" width="100">
          <template #default="{ row }">
            {{ row.high?.toFixed(2) ?? '--' }}
          </template>
        </el-table-column>
        <el-table-column prop="low" label="最低" width="100">
          <template #default="{ row }">
            {{ row.low?.toFixed(2) ?? '--' }}
          </template>
        </el-table-column>
        <el-table-column prop="volume" label="成交量" width="120" sortable="custom">
          <template #default="{ row }">
            {{ formatVolume(row.volume) }}
          </template>
        </el-table-column>
        <el-table-column prop="tradeDate" label="日期" width="120">
          <template #default="{ row }">
            {{ row.tradeDate ? formatDate(row.tradeDate) : '--' }}
          </template>
        </el-table-column>
      </el-table>

      <!-- Load More / Pagination Info -->
      <div class="load-more-area" v-if="indicesStore.hasMore && !indicesStore.listError">
        <el-button
          v-if="!indicesStore.listLoading"
          @click="loadMore"
          type="primary"
          plain
        >
          加载更多
        </el-button>
        <div v-else class="loading-more">
          <el-icon class="is-loading"><Loading /></el-icon>
          <span>加载中...</span>
        </div>
      </div>
      <div class="pagination-info" v-else-if="indicesStore.indicesList.length > 0">
        <span>已加载 {{ indicesStore.indicesList.length }} / {{ indicesStore.listTotal }} 条</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, reactive } from 'vue'
import { useIndicesStore } from '@/stores/indices'
import { Loading } from '@element-plus/icons-vue'

const indicesStore = useIndicesStore()

const sortConfig = reactive({
  sortBy: 'change_rate',
  order: 'desc' as 'asc' | 'desc',
})

function handleSortChange(val: string) {
  indicesStore.setSort(val, sortConfig.order)
}

function setSortOrder(order: 'asc' | 'desc') {
  sortConfig.order = order
  indicesStore.setSort(sortConfig.sortBy, order)
}

function handleTableSortChange({ prop, order }: { prop: string; order: string }) {
  if (!prop) return
  const orderMap: Record<string, 'asc' | 'desc'> = {
    ascending: 'asc',
    descending: 'desc',
  }
  const fieldMap: Record<string, string> = {
    code: 'code',
    close: 'close',
    change_rate: 'change_rate',
    volume: 'volume',
  }
  sortConfig.sortBy = fieldMap[prop] || prop || 'change_rate'
  sortConfig.order = orderMap[order] || 'desc'
  indicesStore.setSort(sortConfig.sortBy, sortConfig.order)
}

function loadMore() {
  indicesStore.fetchList(false)
}

function formatVolume(vol: number | null | undefined): string {
  if (vol == null) return '--'
  if (vol >= 1e8) return (vol / 1e8).toFixed(2) + '亿'
  if (vol >= 1e4) return (vol / 1e4).toFixed(2) + '万'
  return vol.toString()
}

function formatDate(dateStr: string): string {
  try {
    return dateStr.split('T')[0] || dateStr
  } catch {
    return dateStr
  }
}

onMounted(() => {
  indicesStore.fetchOverview()
  indicesStore.fetchList(true)
})
</script>

<style scoped lang="scss">
.indices-view {
  padding: 24px;
}

.page-header {
  margin-bottom: 24px;
}

.section-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
  font-size: 16px;
  font-weight: 600;
  color: var(--color-text-primary);

  .sort-controls {
    display: flex;
    align-items: center;
    gap: 8px;

    .sort-label {
      font-size: 14px;
      font-weight: 400;
      color: var(--color-text-secondary);
    }
  }
}

.loading-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 48px;
  color: var(--color-text-secondary);
}

.error-state {
  padding: 16px 0;
}

.index-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;

  @media (max-width: 1400px) {
    grid-template-columns: repeat(3, 1fr);
  }

  @media (max-width: 1100px) {
    grid-template-columns: repeat(2, 1fr);
  }

  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
}

.index-card {
  background: rgba(255, 255, 255, 0.03);
  border-radius: 16px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  padding: 20px;
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.24);

  .index-name {
    font-size: 16px;
    font-weight: 600;
    color: var(--color-text-primary);
  }

  .index-code {
    font-size: 12px;
    color: var(--color-text-secondary);
    margin-bottom: 8px;
  }

  .index-price {
    font-size: 28px;
    font-weight: 600;
    color: var(--color-text-primary);
    margin-bottom: 4px;
  }

  .index-change {
    font-size: 14px;
    margin-bottom: 12px;

    &.up {
      color: #ef4444;
    }
    
    &.down {
      color: #22c55e;
    }
  }

  .index-details {
    border-top: 1px solid rgba(255, 255, 255, 0.08);
    padding-top: 12px;

    .detail-row {
      display: flex;
      justify-content: space-between;
      font-size: 13px;
      margin-bottom: 4px;

      .label {
        color: var(--color-text-secondary);
      }

      span:last-child {
        color: var(--color-text-primary);
      }
    }
  }
}

.indices-table-wrapper {
  background: rgba(255, 255, 255, 0.03);
  border-radius: 16px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  padding: 16px;
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.24);

  .up {
    color: #ef4444;
  }

  .down {
    color: #22c55e;
  }
}

.load-more-area {
  display: flex;
  justify-content: center;
  padding: 24px 0;

  .loading-more {
    display: flex;
    align-items: center;
    gap: 8px;
    color: var(--color-text-secondary);
  }
}

.pagination-info {
  text-align: center;
  padding: 16px 0;
  font-size: 14px;
  color: var(--color-text-secondary);
}
</style>
