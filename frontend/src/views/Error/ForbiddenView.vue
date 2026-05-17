<template>
  <div class="forbidden-container">
    <div class="forbidden-content">
      <el-icon class="forbidden-icon"><Lock /></el-icon>
      <h1>403</h1>
      <p>抱歉，您没有权限访问此页面</p>
      <el-button type="primary" @click="goHome">返回首页</el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRouter } from 'vue-router'
import { Lock } from '@element-plus/icons-vue'

const router = useRouter()

function goHome() {
  router.push('/')
}
</script>

<style scoped lang="scss">
.forbidden-container {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-bg-page);
}

.forbidden-content {
  text-align: center;
  
  .forbidden-icon {
    font-size: 80px;
    color: var(--color-text-placeholder);
    margin-bottom: 16px;
  }
  
  h1 {
    font-size: 72px;
    font-weight: 590;
    color: var(--color-text-primary);
    margin: 0;
  }
  
  p {
    font-size: 18px;
    color: var(--color-text-secondary);
    margin: 16px 0 32px;
  }
}
</style>
