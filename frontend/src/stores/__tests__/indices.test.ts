import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('@/api/indices', () => ({
  getIndicesOverview: vi.fn(),
  getIndicesList: vi.fn(),
}))

import { useIndicesStore } from '../indices'
import { getIndicesList, getIndicesOverview } from '@/api/indices'

describe('Indices Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.resetAllMocks()
  })

  it('reads overview data from the unwrapped JSON payload', async () => {
    vi.mocked(getIndicesOverview).mockResolvedValue({
      data: [
        {
          code: 'sh000001',
          name: '上证指数',
          price: 3100,
          previousClose: 3080,
          change: 20,
          changePct: 0.65,
        },
      ],
    })

    const store = useIndicesStore()
    await store.fetchOverview()

    expect(store.overviewIndices).toEqual([
      expect.objectContaining({
        code: 'sh000001',
        name: '上证指数',
      }),
    ])
  })

  it('reads paginated list data from the unwrapped JSON payload', async () => {
    vi.mocked(getIndicesList).mockResolvedValue({
      items: [
        {
          code: 'sh000001',
          name: '上证指数',
          close: 3100,
          previousClose: 3080,
          open: 3090,
          high: 3110,
          low: 3085,
          changeRate: 0.65,
          changeAmount: 20,
          volume: 123456,
          tradeDate: '2026-04-06',
        },
      ],
      total: 1,
      page: 1,
      page_size: 100,
    })

    const store = useIndicesStore()
    await store.fetchList(true)

    expect(store.indicesList).toHaveLength(1)
    expect(store.indicesList[0]).toEqual(
      expect.objectContaining({
        code: 'sh000001',
        changeRate: 0.65,
      }),
    )
    expect(store.listTotal).toBe(1)
    expect(store.hasMore).toBe(false)
  })
})
