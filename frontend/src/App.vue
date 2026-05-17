<template>
  <el-config-provider :locale="zhCn">
    <router-view />
  </el-config-provider>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { useUserStore } from '@/stores/user'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'

const userStore = useUserStore()

onMounted(() => {
  userStore.initUser()
})
</script>

<style>
#app {
  font-family: 'Inter', 'SF Pro Display', -apple-system, system-ui, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
}
.placeholder {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 400px;
  color: var(--color-text-secondary, #8a8f98);
}
</style>
