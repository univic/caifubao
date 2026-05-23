<template>
  <div class="watchlists-page">
    <header class="page-hero">
      <p class="eyebrow">Watchlists</p>
      <h1 class="page-title">自选列表</h1>
      <p class="subtitle">管理自选股票池，追踪评分与推荐信号</p>
    </header>

    <!-- Error alert -->
    <el-alert
      v-if="errorMsg"
      class="error-alert"
      type="error"
      :title="errorMsg"
      show-icon
      :closable="false"
    />

    <!-- Create / Back toggle -->
    <div class="action-bar">
      <button
        v-if="selectedId || showCreate"
        class="ghost-btn"
        @click="backToList"
      >
        返回列表
      </button>
      <button
        v-if="!showCreate && !selectedId"
        class="primary-btn"
        @click="showCreate = true"
      >
        + 新建自选
      </button>
    </div>

    <!-- Detail View -->
    <section v-if="selectedId && detail" class="section">
      <div class="detail-header">
        <h2>{{ detail.name }}</h2>
        <span class="badge">{{ detail.stocks?.length || 0 }} 只</span>
        <button class="danger-btn" @click="handleDelete">删除</button>
      </div>

      <div v-if="detailLoading" class="loading">加载中...</div>
      <div v-else-if="!detail.stocks?.length" class="empty">暂无股票</div>
      <div v-else class="table-wrap">
        <el-table :data="detail.stocks" size="small" empty-text="暂无数据">
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
              <span class="stock-name">{{ row.stock_name || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="Score 5" width="80" align="right">
            <template #default="{ row }">
              <span class="score-val">{{ row.scores?.score5?.value?.toFixed(0) || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="Score 20" width="80" align="right">
            <template #default="{ row }">
              <span class="score-val">{{ row.scores?.score20?.value?.toFixed(0) || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="Score 60" width="80" align="right">
            <template #default="{ row }">
              <span class="score-val">{{ row.scores?.score60?.value?.toFixed(0) || '--' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="推荐" width="80" align="center">
            <template #default="{ row }">
              <span class="rec-tag" :class="row.scores?.score20?.recommendation?.toLowerCase()">
                {{ recLabel(row.scores?.score20?.recommendation) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="现价" width="90" align="right">
            <template #default="{ row }">
              <span class="price">{{ row.current_price?.toFixed(2) || '--' }}</span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </section>

    <!-- Create Form -->
    <section v-if="showCreate" class="section">
      <div class="form-card">
        <div class="card-header">
          <h3 class="card-title">新建自选列表</h3>
        </div>
        <div class="form-body">
          <el-form label-position="top" @submit.prevent="handleCreate">
            <el-form-item label="列表名称">
              <el-input
                v-model="createName"
                placeholder="例如：重点关注、低估值组合"
                class="linear-input"
              />
            </el-form-item>
            <el-form-item label="股票代码（每行一个，英文逗号或换行分隔）">
              <el-input
                v-model="createCodes"
                type="textarea"
                :rows="4"
                placeholder="sh600519&#10;sz000001&#10;sh601318"
                class="linear-input"
              />
            </el-form-item>
          </el-form>
          <div class="form-actions">
            <button class="ghost-btn" @click="showCreate = false">取消</button>
            <button
              class="primary-btn"
              :disabled="createSubmitting"
              @click="handleCreate"
            >
              {{ createSubmitting ? '创建中...' : '创建' }}
            </button>
          </div>
        </div>
      </div>
    </section>

    <!-- List View -->
    <section v-if="!showCreate && !selectedId" class="section">
      <h2>
        我的列表
        <span class="badge">{{ watchlists.length }}</span>
      </h2>
      <div v-if="listLoading" class="loading">加载中...</div>
      <div v-else-if="!watchlists.length" class="empty">暂无自选列表，点击上方按钮创建</div>
      <div v-else class="watchlist-grid">
        <div
          v-for="wl in watchlists"
          :key="wl.id"
          class="wl-card"
          @click="selectWatchlist(wl.id)"
        >
          <div class="wl-name">{{ wl.name }}</div>
          <div class="wl-meta">
            <span class="wl-count">{{ wl.stock_count }} 只股票</span>
            <span class="wl-date">{{ formatDate(wl.created_at) }}</span>
          </div>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { decisionsApi, type WatchlistItem, type WatchlistDetail } from '@/api/decisions'

const watchlists = ref<WatchlistItem[]>([])
const listLoading = ref(false)
const errorMsg = ref('')

// Create state
const showCreate = ref(false)
const createName = ref('')
const createCodes = ref('')
const createSubmitting = ref(false)

// Detail state
const selectedId = ref<string | null>(null)
const detail = ref<WatchlistDetail | null>(null)
const detailLoading = ref(false)

function recLabel(val: string): string {
  if (val === 'BUY') return '买入'
  if (val === 'WATCH') return '关注'
  if (val === 'AVOID') return '回避'
  return val || '--'
}

function formatDate(val: string): string {
  if (!val) return '--'
  return val.slice(0, 10)
}

async function fetchWatchlists() {
  listLoading.value = true
  errorMsg.value = ''
  try {
    const res = await decisionsApi.getWatchlists()
    watchlists.value = res.data?.items || []
  } catch (e: any) {
    errorMsg.value = e?.response?.data?.message || '获取自选列表失败'
  } finally {
    listLoading.value = false
  }
}

async function selectWatchlist(id: string) {
  selectedId.value = id
  detailLoading.value = true
  errorMsg.value = ''
  try {
    const res = await decisionsApi.getWatchlist(id)
    detail.value = res.data
  } catch (e: any) {
    errorMsg.value = e?.response?.data?.message || '获取自选详情失败'
    selectedId.value = null
  } finally {
    detailLoading.value = false
  }
}

function backToList() {
  selectedId.value = null
  detail.value = null
  showCreate.value = false
  fetchWatchlists()
}

async function handleCreate() {
  const name = createName.value.trim()
  const codes = createCodes.value.trim()
  if (!name) {
    ElMessage.warning('请输入列表名称')
    return
  }
  if (!codes) {
    ElMessage.warning('请输入股票代码')
    return
  }

  const stockCodes = codes
    .split(/[,\n]+/)
    .map((s) => s.trim())
    .filter(Boolean)

  if (!stockCodes.length) {
    ElMessage.warning('请至少输入一个有效的股票代码')
    return
  }

  createSubmitting.value = true
  errorMsg.value = ''
  try {
    await decisionsApi.createWatchlist({ name, stock_codes: stockCodes })
    ElMessage.success('自选列表创建成功')
    createName.value = ''
    createCodes.value = ''
    showCreate.value = false
    fetchWatchlists()
  } catch (e: any) {
    errorMsg.value = e?.response?.data?.message || '创建失败'
  } finally {
    createSubmitting.value = false
  }
}

async function handleDelete() {
  if (!selectedId.value) return
  try {
    await ElMessageBox.confirm('确定要删除这个自选列表吗？', '确认删除', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning'
    })
    await decisionsApi.deleteWatchlist(selectedId.value)
    ElMessage.success('已删除')
    selectedId.value = null
    detail.value = null
    fetchWatchlists()
  } catch (e: any) {
    if (e !== 'cancel' && e !== 'close') {
      errorMsg.value = e?.response?.data?.message || '删除失败'
    }
  }
}

onMounted(fetchWatchlists)
</script>

<style scoped lang="scss">
.watchlists-page {
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

.error-alert { margin-bottom: 20px; }

.action-bar {
  display: flex; gap: 8px; margin-bottom: 24px;
}

.primary-btn {
  font-size: 13px; font-weight: 510; padding: 8px 20px; border: none; border-radius: 6px;
  background: #5e6ad2; color: #f7f8f8; cursor: pointer;
  &:hover { background: #7170ff; }
  &:disabled { opacity: 0.5; cursor: not-allowed; }
}

.ghost-btn {
  font-size: 13px; font-weight: 510; padding: 8px 20px; border: 1px solid rgba(255,255,255,0.08);
  border-radius: 6px; background: rgba(255,255,255,0.02); color: #d0d6e0; cursor: pointer;
  &:hover { color: #f7f8f8; background: rgba(255,255,255,0.05); }
}

.danger-btn {
  font-size: 12px; font-weight: 510; padding: 6px 14px; border: 1px solid rgba(239, 68, 68, 0.2);
  border-radius: 6px; background: rgba(239, 68, 68, 0.06); color: #ef4444; cursor: pointer;
  &:hover { background: rgba(239, 68, 68, 0.12); }
}

.section {
  margin-bottom: 32px;
  h2 { font-size: 18px; font-weight: 590; color: #f7f8f8; margin: 0 0 16px 0; display: flex; align-items: center; gap: 10px; }
  .badge { font-size: 11px; font-weight: 510; color: #8a8f98; background: rgba(255,255,255,0.06); padding: 2px 10px; border-radius: 999px; }
  .loading, .empty { font-size: 14px; color: #62666d; padding: 20px 0; }
}

.detail-header {
  display: flex; align-items: center; gap: 12px; margin-bottom: 16px;
  h2 { margin: 0; }
}

.table-wrap {
  overflow-x: auto;
}

.stock-link {
  color: #7170ff;
  text-decoration: none;
  font-family: 'Berkeley Mono', monospace;
  font-size: 12px;
  &:hover { text-decoration: underline; }
}

.stock-name { color: #f7f8f8; font-weight: 510; }
.score-val { font-weight: 590; color: #f7f8f8; }
.price { font-family: 'Berkeley Mono', monospace; font-size: 12px; color: #d0d6e0; }

.rec-tag {
  font-size: 10px; font-weight: 590; padding: 2px 8px; border-radius: 4px;
  &.buy { background: rgba(16, 185, 129, 0.1); color: #10b981; }
  &.watch { background: rgba(245, 158, 11, 0.1); color: #f59e0b; }
  &.avoid { background: rgba(239, 68, 68, 0.1); color: #ef4444; }
}

.form-card {
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 12px; overflow: hidden;
  .card-header { padding: 20px 24px 0; }
  .card-title { font-size: 16px; font-weight: 590; color: #f7f8f8; }
  .form-body { padding: 16px 24px 24px; }
  .form-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 16px; }
}

.watchlist-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 12px;
}

.wl-card {
  background: rgba(255,255,255,0.02);
  border: 1px solid rgba(255,255,255,0.06);
  border-radius: 12px;
  padding: 20px;
  cursor: pointer;
  transition: border-color 0.2s, background 0.2s;
  &:hover {
    border-color: rgba(255,255,255,0.12);
    background: rgba(255,255,255,0.04);
  }
  .wl-name { font-size: 16px; font-weight: 590; color: #f7f8f8; margin-bottom: 8px; }
  .wl-meta { display: flex; gap: 16px; font-size: 12px; color: #62666d; }
}
</style>
