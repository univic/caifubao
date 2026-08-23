<template>
  <header class="topbar">
    <div class="topbar__inner">
      <div class="topbar__brand">
        <button class="topbar__menu-button" type="button" @click="drawerVisible = true">
          <el-icon><Menu /></el-icon>
        </button>

        <router-link class="brand" to="/" aria-label="财富宝">
          <span class="brand__mark">C</span>
          <span class="brand__text">
            <strong>财富宝</strong>
            <small>量化工作台</small>
          </span>
        </router-link>
      </div>

      <nav class="topbar__nav" aria-label="主导航">
        <el-menu :default-active="activeMenu" mode="horizontal" class="topbar-menu" router>
          <el-menu-item index="/">
            <el-icon><Grid /></el-icon>
            <span>总览</span>
          </el-menu-item>

          <el-menu-item index="/history">
            <el-icon><TrendCharts /></el-icon>
            <span>行情</span>
          </el-menu-item>

          <el-menu-item index="/market">
            <el-icon><PieChart /></el-icon>
            <span>机会筛选</span>
          </el-menu-item>

          <el-menu-item index="/signals">
            <el-icon><Bell /></el-icon>
            <span>今日信号</span>
          </el-menu-item>

          <el-menu-item index="/indices">
            <el-icon><DataBoard /></el-icon>
            <span>指数</span>
          </el-menu-item>

          <el-menu-item index="/backtest">
            <el-icon><Cpu /></el-icon>
            <span>回测</span>
          </el-menu-item>

          <el-menu-item index="/discovery">
            <el-icon><Search /></el-icon>
            <span>策略发现</span>
          </el-menu-item>

          <el-menu-item index="/decisions">
            <el-icon><Notebook /></el-icon>
            <span>决策面板</span>
          </el-menu-item>

          <el-menu-item index="/portfolio">
            <el-icon><Wallet /></el-icon>
            <span>自选与组合</span>
          </el-menu-item>

          <el-menu-item index="/watchlists">
            <el-icon><Star /></el-icon>
            <span>自选列表</span>
          </el-menu-item>

          <el-sub-menu index="market-data">
            <template #title>
              <el-icon><DataAnalysis /></el-icon>
              <span>市场数据</span>
            </template>
            <el-menu-item index="/history">历史行情</el-menu-item>
            <el-menu-item index="/indices">指数全览</el-menu-item>
            <el-menu-item index="/data-quality">数据质量</el-menu-item>
          </el-sub-menu>

          <el-menu-item index="/score-experiments">
            <el-icon><DataLine /></el-icon>
            <span>评分实验</span>
          </el-menu-item>

          <el-menu-item index="/factor-eval">
            <el-icon><Histogram /></el-icon>
            <span>因子评估</span>
          </el-menu-item>

          <el-sub-menu v-if="userStore.isAdmin" index="/admin/users">
            <template #title>
              <el-icon><Setting /></el-icon>
              <span>管理</span>
            </template>
            <el-menu-item index="/admin/users">用户管理</el-menu-item>
          </el-sub-menu>
        </el-menu>
      </nav>

      <div class="topbar__actions">
        <EnvBadge />

        <el-dropdown @command="handleCommand" trigger="click" popper-class="topbar-dropdown">
          <button class="user-pill" type="button">
            <el-avatar :size="28" :icon="UserFilled" />
            <span class="user-pill__name">{{ userStore.userName || '未命名用户' }}</span>
            <el-icon class="user-pill__chevron"><ArrowDown /></el-icon>
          </button>
          <template #dropdown>
            <el-dropdown-menu class="topbar-dropdown">
              <el-dropdown-item command="profile">
                <el-icon><User /></el-icon>
                个人资料
              </el-dropdown-item>
              <el-dropdown-item divided command="logout">
                <el-icon><SwitchButton /></el-icon>
                退出登录
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </div>

    <el-drawer
      v-model="drawerVisible"
      title="菜单"
      direction="ltr"
      size="280px"
      class="topbar-drawer"
      :append-to-body="true"
      :z-index="2000"
    >
      <el-menu
        :default-active="activeMenu"
        class="topbar-drawer__menu"
        router
        @select="drawerVisible = false"
      >
        <el-menu-item index="/">
          <el-icon><Grid /></el-icon>
          <span>总览</span>
        </el-menu-item>

        <el-menu-item index="/history">
          <el-icon><TrendCharts /></el-icon>
          <span>行情</span>
        </el-menu-item>

        <el-menu-item index="/market">
          <el-icon><PieChart /></el-icon>
          <span>机会筛选</span>
        </el-menu-item>

        <el-menu-item index="/signals">
          <el-icon><Bell /></el-icon>
          <span>今日信号</span>
        </el-menu-item>

        <el-menu-item index="/indices">
          <el-icon><DataBoard /></el-icon>
          <span>指数</span>
        </el-menu-item>

        <el-menu-item index="/backtest">
          <el-icon><Cpu /></el-icon>
          <span>回测</span>
        </el-menu-item>

        <el-menu-item index="/discovery">
          <el-icon><Search /></el-icon>
          <span>策略发现</span>
        </el-menu-item>

        <el-menu-item index="/decisions">
          <el-icon><Notebook /></el-icon>
          <span>决策面板</span>
        </el-menu-item>

        <el-menu-item index="/portfolio">
          <el-icon><Wallet /></el-icon>
          <span>自选与组合</span>
        </el-menu-item>

        <el-menu-item index="/watchlists">
          <el-icon><Star /></el-icon>
          <span>自选列表</span>
        </el-menu-item>

        <el-sub-menu index="market-data">
          <template #title>
            <el-icon><DataAnalysis /></el-icon>
            <span>市场数据</span>
          </template>
          <el-menu-item index="/history">历史行情</el-menu-item>
          <el-menu-item index="/indices">指数全览</el-menu-item>
          <el-menu-item index="/data-quality">数据质量</el-menu-item>
        </el-sub-menu>

        <el-menu-item index="/score-experiments">
          <el-icon><DataLine /></el-icon>
          <span>评分实验</span>
        </el-menu-item>

        <el-menu-item index="/factor-eval">
          <el-icon><Histogram /></el-icon>
          <span>因子评估</span>
        </el-menu-item>

        <el-sub-menu v-if="userStore.isAdmin" index="/admin/users">
          <template #title>
            <el-icon><Setting /></el-icon>
            <span>管理</span>
          </template>
          <el-menu-item index="/admin/users">用户管理</el-menu-item>
        </el-sub-menu>
      </el-menu>
    </el-drawer>
  </header>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user'
import EnvBadge from '@/components/layout/EnvBadge.vue'
import {
  ArrowDown,
  Bell,
  Cpu,
  DataAnalysis,
  DataBoard,
  DataLine,
  Grid,
  Histogram,
  Menu,
  PieChart,
  Search,
  Setting,
  SwitchButton,
  TrendCharts,
  User,
  UserFilled,
  Wallet
} from '@element-plus/icons-vue'
import { ElMessageBox } from 'element-plus'

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()
const drawerVisible = ref(false)

const activeMenu = computed(() => {
  if (route.path.startsWith('/market')) return '/market'
  if (route.path.startsWith('/history')) return '/history'
  if (route.path.startsWith('/quote')) return '/history'
  if (route.path.startsWith('/data-quality')) return '/data-quality'
  if (route.path.startsWith('/indices')) return '/indices'
  if (route.path.startsWith('/signals')) return '/signals'
  if (route.path.startsWith('/score-experiments')) return '/score-experiments'
  if (route.path.startsWith('/factor-eval')) return '/factor-eval'
  if (route.path.startsWith('/decisions')) return '/decisions'
  if (route.path.startsWith('/portfolio')) return '/portfolio'
  if (route.path.startsWith('/backtest')) return '/backtest'
  if (route.path.startsWith('/discovery')) return '/discovery'
  if (route.path.startsWith('/watchlists')) return '/watchlists'
  if (route.path.startsWith('/admin')) return '/admin/users'
  if (route.path === '/profile') return 'profile'
  return '/'
})

async function handleCommand(command: string) {
  if (command === 'logout') {
    try {
      await ElMessageBox.confirm('确定要退出登录吗？', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      })
      userStore.logout()
      router.push('/login')
    } catch {
      // User cancelled
    }
  } else if (command === 'profile') {
    router.push('/profile')
  }
}
</script>

<style scoped lang="scss">
.topbar {
  position: fixed;
  inset: 0 0 auto 0;
  z-index: 120;
  height: var(--topbar-height, 72px);
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  background: rgba(8, 9, 10, 0.82);
  backdrop-filter: blur(18px);
  -webkit-backdrop-filter: blur(18px);
}

.topbar__inner {
  height: 100%;
  width: 100%;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 0 24px;
}

.topbar__brand {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}

.topbar__menu-button {
  display: none;
  width: 38px;
  height: 38px;
  align-items: center;
  justify-content: center;
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  background: rgba(255, 255, 255, 0.03);
  color: #d0d6e0;
  cursor: pointer;

  &:hover {
    color: #f7f8f8;
    background: rgba(255, 255, 255, 0.05);
  }
}

.brand {
  display: inline-flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
  text-decoration: none;
  color: inherit;
}

.brand__mark {
  width: 40px;
  height: 40px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 14px;
  background:
    linear-gradient(135deg, rgba(113, 112, 255, 0.2), rgba(94, 106, 210, 0.16)),
    #191a1b;
  border: 1px solid rgba(255, 255, 255, 0.08);
  color: #f7f8f8;
  font-size: 16px;
  font-weight: 590;
  letter-spacing: 0.04em;
}

.brand__text {
  display: flex;
  flex-direction: column;
  min-width: 0;

  strong {
    font-size: 15px;
    font-weight: 590;
    color: #f7f8f8;
    line-height: 1.1;
    letter-spacing: -0.02em;
  }

  small {
    font-size: 12px;
    color: #8a8f98;
    margin-top: 2px;
  }
}

.topbar__nav {
  min-width: 0;
}

.topbar-menu {
  display: flex;
  justify-content: center;
  align-items: center;
  border-bottom: none;
  background: transparent;

  :deep(.el-menu--horizontal) {
    border-bottom: none;
  }

  :deep(.el-menu-item),
  :deep(.el-sub-menu__title) {
    height: 48px;
    line-height: 48px;
    border-bottom: none !important;
    border-radius: 12px;
    margin: 0 4px;
    color: #d0d6e0;
    background: transparent;
    font-size: 13px;
    font-weight: 510;
  }

  :deep(.el-menu-item.is-active) {
    color: #f7f8f8;
    background: rgba(113, 112, 255, 0.14);
  }

  :deep(.el-menu-item:hover),
  :deep(.el-sub-menu__title:hover) {
    background: rgba(255, 255, 255, 0.04);
    color: #f7f8f8;
  }
}

.topbar__actions {
  display: flex;
  align-items: center;
  gap: 12px;
}



.user-pill {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  padding: 6px 10px 6px 6px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.03);
  color: #f7f8f8;
  cursor: pointer;
}

.user-pill__name {
  font-size: 13px;
  font-weight: 510;
  max-width: 160px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.user-pill__chevron {
  color: #8a8f98;
}

.topbar-drawer {
  :deep(.el-drawer__body) {
    padding-top: 4px;
    background: #0f1011;
  }

  :deep(.el-drawer__header) {
    color: #f7f8f8;
    margin-bottom: 8px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
  }
}

.topbar-drawer__menu {
  border-right: none;
  background: transparent;

  :deep(.el-menu-item),
  :deep(.el-sub-menu__title) {
    color: #d0d6e0;
    border-radius: 12px;
    margin: 2px 0;
  }

  :deep(.el-menu-item.is-active) {
    color: #f7f8f8;
    background: rgba(113, 112, 255, 0.14);
  }
}

:global(.el-popper.is-light.topbar-dropdown) {
  background: #191a1b !important;
  border: 1px solid rgba(255, 255, 255, 0.08) !important;
  box-shadow: 0 24px 60px rgba(0, 0, 0, 0.35) !important;
}

:global(.topbar-dropdown .el-dropdown-menu) {
  background: transparent !important;
  padding: 6px !important;
}

:global(.topbar-dropdown .el-dropdown-menu__item) {
  color: #d0d6e0 !important;
  border-radius: 8px !important;
  margin: 2px 0 !important;
  font-size: 13px !important;
  padding: 8px 16px !important;
}

:global(.topbar-dropdown .el-dropdown-menu__item:hover),
:global(.topbar-dropdown .el-dropdown-menu__item:focus) {
  background: rgba(255, 255, 255, 0.05) !important;
  color: #f7f8f8 !important;
}

:global(.topbar-dropdown .el-popper__arrow::before) {
  background: #191a1b !important;
  border: 1px solid rgba(255, 255, 255, 0.08) !important;
}

@media (max-width: 1024px) {
  .topbar__inner {
    grid-template-columns: auto 1fr auto;
  }

  .topbar__nav {
    display: none;
  }

  .topbar__menu-button {
    display: inline-flex;
  }

  .env-badge {
    display: none;
  }
}

@media (max-width: 640px) {
  .topbar__inner {
    padding: 0 16px;
    gap: 10px;
  }

  .brand__text small {
    display: none;
  }

  .user-pill__name {
    max-width: 92px;
  }
}
</style>
