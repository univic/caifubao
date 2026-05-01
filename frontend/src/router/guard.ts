import type { Router } from 'vue-router'
import { useUserStore } from '@/stores/user'

export function setupRouteGuard(router: Router) {
  // 白名单：不需要登录的页面
  const whiteList = ['/welcome', '/login', '/register', '/forgot-password', '/reset-password', '/403']
  
  router.beforeEach(async (to, _from, next) => {
    const userStore = useUserStore()
    
    // 统一从 localStorage 读取 token
    const token = localStorage.getItem('token')
    const hasToken = !!token
    
    if (hasToken) {
      if (to.path === '/login' || to.path === '/welcome') {
        // 已登录用户跳转到后台总览
        next({ path: '/' })
      } else {
        // 检查用户信息是否已加载
        if (!userStore.userInfo) {
          try {
            if (!userStore.token) {
              userStore.initUser()
            }
            await userStore.fetchUserInfo()
          } catch {
            if (!to.meta.requiresAuth) {
              next()
              return
            }
            userStore.logout()
            next(`/login?redirect=${to.path}`)
            return
          }
        }
        
        // 检查管理员权限
        if (to.meta.requiresAdmin && !userStore.isAdmin) {
          next('/403')
          return
        }
        
        next()
      }
    } else {
      // 检查路径是否在白名单中，或者 meta 明确标记为不需要权限
      if (whiteList.includes(to.path) || to.meta.requiresAuth === false) {
        next()
      } else {
        // 默认所有未匹配公开路径的请求重定向到登录
        next(`/login?redirect=${to.path}`)
      }
    }
  })
}
