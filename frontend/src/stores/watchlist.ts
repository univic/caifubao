import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export interface WatchlistItem {
  code: string
  name: string
  addedAt: string
}

const STORAGE_KEY = 'caifubao_watchlist'

function loadFromStorage(): WatchlistItem[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) {
      return JSON.parse(raw) as WatchlistItem[]
    }
  } catch {
    // Corrupted data, reset
  }
  return []
}

function saveToStorage(items: WatchlistItem[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(items))
}

export const useWatchlistStore = defineStore('watchlist', () => {
  const items = ref<WatchlistItem[]>(loadFromStorage())

  const codes = computed(() => new Set(items.value.map(item => item.code)))
  const count = computed(() => items.value.length)

  function isWatched(code: string): boolean {
    return codes.value.has(code)
  }

  function add(code: string, name: string) {
    if (isWatched(code)) return
    items.value.push({
      code,
      name,
      addedAt: new Date().toISOString()
    })
    saveToStorage(items.value)
  }

  function remove(code: string) {
    items.value = items.value.filter(item => item.code !== code)
    saveToStorage(items.value)
  }

  function toggle(code: string, name: string) {
    if (isWatched(code)) {
      remove(code)
    } else {
      add(code, name)
    }
  }

  return {
    items,
    codes,
    count,
    isWatched,
    add,
    remove,
    toggle
  }
})
