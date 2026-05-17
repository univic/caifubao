/**
 * Shared pagination composable.
 * Eliminates duplicated page/per-page/total logic across views.
 */
import { ref, computed } from 'vue'

export interface PaginationOptions {
  /** Items per page (default 20) */
  perPage?: number
  /** Initial page (default 1) */
  page?: number
}

export function usePagination(options: PaginationOptions = {}) {
  const page = ref(options.page ?? 1)
  const perPage = ref(options.perPage ?? 20)
  const total = ref(0)

  const totalPages = computed(() => Math.max(1, Math.ceil(total.value / perPage.value)))

  /** Reset to page 1 and return query params. Call before each fetch. */
  function reset(): { page: number; per_page: number } {
    page.value = 1
    return toParams()
  }

  /** Return current page + per_page as query params. */
  function toParams(): { page: number; per_page: number } {
    return { page: page.value, per_page: perPage.value }
  }

  /** Handle Element Plus pagination @current-change. */
  function onPageChange(p: number) {
    page.value = p
  }

  /** Handle Element Plus pagination @size-change. */
  function onSizeChange(size: number) {
    perPage.value = size
    page.value = 1
  }

  return {
    page,
    perPage,
    total,
    totalPages,
    reset,
    toParams,
    onPageChange,
    onSizeChange
  }
}
