<template>
  <div class="app-layout">
    <Header />
    <main class="main-content">
      <router-view />
    </main>
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import Header from './Header.vue'
import { useUserStore } from '@/stores/user'

const userStore = useUserStore()

onMounted(() => {
  userStore.initUser()
})
</script>

<style scoped lang="scss">
.app-layout {
  min-height: 100vh;
  background:
    radial-gradient(circle at 18% 0%, rgba(113, 112, 255, 0.12), transparent 28%),
    radial-gradient(circle at 82% 0%, rgba(94, 106, 210, 0.12), transparent 24%),
    #08090a;
}

.main-content {
  --color-primary: #7170ff;
  --color-primary-light: #828fff;
  --color-primary-dark: #5e6ad2;
  --color-success: #27a644;
  --color-success-light: #4ade80;
  --color-warning: #d97706;
  --color-info: #8a8f98;
  --color-bg-page: #08090a;
  --color-bg-card: #191a1b;
  --color-bg-header: rgba(15, 16, 17, 0.82);
  --color-text-primary: #f7f8f8;
  --color-text-regular: #d0d6e0;
  --color-text-secondary: #8a8f98;
  --color-text-placeholder: #62666d;
  --color-border: rgba(255, 255, 255, 0.08);
  --color-border-light: rgba(255, 255, 255, 0.06);
  --color-border-lighter: rgba(255, 255, 255, 0.05);
  --box-shadow-light: 0 1px 0 rgba(255, 255, 255, 0.03), 0 18px 40px rgba(0, 0, 0, 0.28);
  --box-shadow-elevated: 0 24px 60px rgba(0, 0, 0, 0.38);
  min-height: calc(100vh - var(--topbar-height, 72px));
  margin-top: var(--topbar-height, 72px);
  padding: 28px 24px 44px;
  background: transparent;
  max-width: 1600px;
  margin-left: auto;
  margin-right: auto;
  color: var(--color-text-primary);
}

.main-content :deep(.card),
.main-content :deep(.el-card) {
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 20px;
  box-shadow: var(--box-shadow-light);
  color: var(--color-text-primary);
}

.main-content :deep(.el-card__header) {
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
}

.main-content :deep(.el-empty) {
  --el-text-color-primary: var(--color-text-primary);
  --el-text-color-regular: var(--color-text-secondary);
  color: var(--color-text-primary);
}

.main-content :deep(.el-empty__description) {
  color: var(--color-text-secondary);
}

.main-content :deep(.el-skeleton__item) {
  background: rgba(255, 255, 255, 0.05);
}

.main-content :deep(.el-alert) {
  border: 1px solid rgba(255, 255, 255, 0.08);
  background: rgba(255, 255, 255, 0.03);
  color: var(--color-text-primary);
}

.main-content :deep(.el-table),
.main-content :deep(.el-table__inner-wrapper),
.main-content :deep(.el-table__header-wrapper),
.main-content :deep(.el-table__body-wrapper),
.main-content :deep(.el-table__empty-block) {
  background: transparent;
}

.main-content :deep(.el-table) {
  --el-table-border-color: rgba(255, 255, 255, 0.08);
  --el-table-border: 1px solid rgba(255, 255, 255, 0.08);
  --el-table-header-bg-color: rgba(255, 255, 255, 0.03);
  --el-table-tr-bg-color: transparent;
  --el-table-row-hover-bg-color: rgba(255, 255, 255, 0.04);
  --el-table-current-row-bg-color: rgba(113, 112, 255, 0.08);
  --el-table-text-color: var(--color-text-primary);
  --el-table-header-text-color: var(--color-text-secondary);
}

.main-content :deep(.el-table th.el-table__cell),
.main-content :deep(.el-table td.el-table__cell) {
  background: transparent;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  color: var(--color-text-primary);
}

.main-content :deep(.el-table__empty-text) {
  color: var(--color-text-secondary);
}

.main-content :deep(.el-input__wrapper),
.main-content :deep(.el-select__wrapper),
.main-content :deep(.el-textarea__inner) {
  background: rgba(255, 255, 255, 0.03);
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.08);
  color: var(--color-text-primary);
}

.main-content :deep(.el-input__wrapper.is-focus),
.main-content :deep(.el-select__wrapper.is-focused) {
  box-shadow: inset 0 0 0 1px rgba(113, 112, 255, 0.34), 0 0 0 4px rgba(113, 112, 255, 0.12);
}

.main-content :deep(.el-input__inner),
.main-content :deep(.el-textarea__inner) {
  color: var(--color-text-primary);
}

.main-content :deep(.el-input__inner::placeholder),
.main-content :deep(.el-textarea__inner::placeholder) {
  color: var(--color-text-placeholder);
}

.main-content :deep(.el-radio-button__inner),
.main-content :deep(.el-button.is-plain),
.main-content :deep(.el-button--default) {
  border-color: rgba(255, 255, 255, 0.08);
  background: rgba(255, 255, 255, 0.03);
  color: var(--color-text-primary);
}

.main-content :deep(.el-button.is-link) {
  color: var(--color-primary);
}

.main-content :deep(.el-button.is-plain:hover),
.main-content :deep(.el-button--default:hover) {
  background: rgba(255, 255, 255, 0.05);
  color: var(--color-text-primary);
}

.main-content :deep(.el-radio-button__inner) {
  background: rgba(255, 255, 255, 0.02);
  color: var(--color-text-secondary);
}

.main-content :deep(.el-radio-button__original-radio:checked + .el-radio-button__inner),
.main-content :deep(.el-radio-button__inner.is-active) {
  background: rgba(113, 112, 255, 0.16);
  color: #f7f8f8;
  box-shadow: none;
}

.main-content :deep(.el-pagination) {
  --el-text-color-regular: var(--color-text-secondary);
  --el-fill-color-light: rgba(255, 255, 255, 0.04);
}

.main-content :deep(.el-pagination .btn-prev),
.main-content :deep(.el-pagination .btn-next),
.main-content :deep(.el-pagination .el-pager li) {
  background: rgba(255, 255, 255, 0.03);
  color: var(--color-text-secondary);
  border: 1px solid rgba(255, 255, 255, 0.08);
}

.main-content :deep(.el-pagination .el-pager li.is-active) {
  background: rgba(113, 112, 255, 0.16);
  color: #f7f8f8;
}

.main-content :deep(.el-tag) {
  border-color: rgba(255, 255, 255, 0.08);
  background: rgba(255, 255, 255, 0.04);
  color: var(--color-text-primary);
}

.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.2s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>
