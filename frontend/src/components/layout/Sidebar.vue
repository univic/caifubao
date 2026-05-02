<template>
  <el-aside :width="collapsed ? '64px' : '210px'" class="sidebar">
    <div class="logo">
      <el-icon v-if="!collapsed" class="logo-icon-text"><Coin /></el-icon>
      <span v-if="!collapsed" class="logo-text">财富宝</span>
      <span v-else class="logo-icon">财</span>
    </div>
    
    <el-menu
      :default-active="activeMenu"
      :collapse="collapsed"
      :collapse-transition="false"
      class="sidebar-menu"
      background-color="#304156"
      text-color="#bfcbd9"
      active-text-color="#409eff"
      router
    >
      <el-menu-item index="/">
        <el-icon><DataAnalysis /></el-icon>
        <template #title>市场总览</template>
      </el-menu-item>

      <el-menu-item index="/app/indices">
        <el-icon><DataBoard /></el-icon>
        <template #title>指数全览</template>
      </el-menu-item>

      <el-menu-item index="/app/data-quality">
        <el-icon><Monitor /></el-icon>
        <template #title>数据质量</template>
      </el-menu-item>

      <el-menu-item index="/history">
        <el-icon><TrendCharts /></el-icon>
        <template #title>历史行情</template>
      </el-menu-item>
      
      <el-menu-item index="/backtest">
        <el-icon><Cpu /></el-icon>
        <template #title>回测系统</template>
      </el-menu-item>
      
      <el-menu-item index="/market">
        <el-icon><PieChart /></el-icon>
        <template #title>标的看板</template>
      </el-menu-item>
      
      <el-menu-item index="/signals">
        <el-icon><Bell /></el-icon>
        <template #title>信号与机会</template>
      </el-menu-item>

      <el-menu-item index="/score-experiments">
        <el-icon><DataLine /></el-icon>
        <template #title>评分实验</template>
      </el-menu-item>
      
      <el-sub-menu v-if="userStore.isAdmin" index="/admin">
        <template #title>
          <el-icon><Setting /></el-icon>
          <span>系统管理</span>
        </template>
        <el-menu-item index="/admin/users">用户管理</el-menu-item>
      </el-sub-menu>
    </el-menu>
  </el-aside>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { DataAnalysis, DataBoard, TrendCharts, Cpu, Bell, Setting, Coin, Monitor, PieChart, DataLine } from '@element-plus/icons-vue'

defineProps<{
  collapsed: boolean
}>()

const route = useRoute()
const userStore = useUserStore()

const activeMenu = computed(() => route.path)
</script>

<style scoped lang="scss">
.sidebar {
  background-color: #304156;
  height: 100vh;
  position: fixed;
  left: 0;
  top: 0;
  overflow-x: hidden;
  transition: width 0.3s;
  z-index: 100;
}

.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #2b3a4a;
  
  img {
    height: 36px;
    margin-right: 8px;
  }
  
  .logo-text {
    color: #fff;
    font-size: 18px;
    font-weight: 600;
  }
  
  .logo-icon {
    color: #409eff;
    font-size: 24px;
    font-weight: 700;
  }
}

.sidebar-menu {
  border-right: none;
  
  &:not(.el-menu--collapse) {
    width: 210px;
  }
}
</style>
