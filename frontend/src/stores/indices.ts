import { defineStore } from 'pinia'
import { ref } from 'vue'
import { getIndicesOverview, getIndicesList, type IndexOverview, type IndexListItem } from '@/api/indices'

export const useIndicesStore = defineStore('indices', () => {
  // Overview state
  const overviewIndices = ref<IndexOverview[]>([])
  const overviewLoading = ref(false)
  const overviewError = ref<string | null>(null)

  // List state
  const indicesList = ref<IndexListItem[]>([])
  const listLoading = ref(false)
  const listError = ref<string | null>(null)
  const listPage = ref(1)
  const listPageSize = ref(100)
  const listTotal = ref(0)
  const listSortBy = ref('change_rate')
  const listOrder = ref<'asc' | 'desc'>('desc')
  const hasMore = ref(true)

  async function fetchOverview() {
    overviewLoading.value = true
    overviewError.value = null
    try {
      const res = await getIndicesOverview()
      overviewIndices.value = res.data
    } catch (err: unknown) {
      overviewError.value = err instanceof Error ? err.message : 'Failed to fetch overview'
    } finally {
      overviewLoading.value = false
    }
  }

  async function fetchList(reset = false) {
    if (reset) {
      listPage.value = 1
      indicesList.value = []
      hasMore.value = true
    }

    if (!hasMore.value && !reset) return

    listLoading.value = true
    listError.value = null

    try {
      const res = await getIndicesList({
        page: listPage.value,
        page_size: listPageSize.value,
        sort_by: listSortBy.value,
        order: listOrder.value,
      })

      if (reset) {
        indicesList.value = res.items
      } else {
        indicesList.value = [...indicesList.value, ...res.items]
      }

      listTotal.value = res.total
      hasMore.value = indicesList.value.length < listTotal.value
      listPage.value += 1
    } catch (err: unknown) {
      listError.value = err instanceof Error ? err.message : 'Failed to fetch indices list'
    } finally {
      listLoading.value = false
    }
  }

  function setSort(sortBy: string, order: 'asc' | 'desc') {
    listSortBy.value = sortBy
    listOrder.value = order
    fetchList(true)
  }

  return {
    // Overview
    overviewIndices,
    overviewLoading,
    overviewError,
    fetchOverview,
    // List
    indicesList,
    listLoading,
    listError,
    listPage,
    listPageSize,
    listTotal,
    listSortBy,
    listOrder,
    hasMore,
    fetchList,
    setSort,
  }
})
