import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { UserInfo, LoginResponse } from '@/api/auth'
import { authApi } from '@/api/auth'

export const useUserStore = defineStore('user', () => {
  const token = ref<string | null>(localStorage.getItem('token'))
  const userInfo = ref<UserInfo | null>(null)
  const loading = ref(false)

  const isLoggedIn = computed(() => !!token.value)
  const isAdmin = computed(() => userInfo.value?.role?.includes('ADM') ?? false)
  const userName = computed(() => userInfo.value?.username ?? '')

  async function doLogin(user: string, pwd: string) {
    loading.value = true
    try {
      const res = await authApi.login({ username: user, password: pwd }) as unknown as LoginResponse
      token.value = res.token
      localStorage.setItem('token', res.token)
      // 如果后端返回 refresh_token，也存储起来
      if (res.refresh_token) {
        localStorage.setItem('refresh_token', res.refresh_token)
      }
      userInfo.value = res.user
      localStorage.setItem('user', JSON.stringify(res.user))
      return res
    } finally {
      loading.value = false
    }
  }

  async function doRegister(user: string, email: string, pwd: string) {
    loading.value = true
    try {
      const res = await authApi.register({ username: user, email, password: pwd })
      return res
    } finally {
      loading.value = false
    }
  }

  async function fetchUserInfo() {
    if (!token.value) return
    try {
      const res: any = await authApi.getUserInfo()
      userInfo.value = res as UserInfo
      localStorage.setItem('user', JSON.stringify(res))
    } catch {
      logout()
    }
  }

  function logout() {
    token.value = null
    userInfo.value = null
    localStorage.removeItem('token')
    localStorage.removeItem('refresh_token')
    localStorage.removeItem('user')
  }

  function initUser() {
    const savedToken = localStorage.getItem('token')
    const savedUser = localStorage.getItem('user')
    if (savedToken) {
      token.value = savedToken
      if (savedUser) {
        try {
          userInfo.value = JSON.parse(savedUser)
        } catch {
          // Invalid JSON, clear it
          localStorage.removeItem('user')
        }
      }
    }
  }

  return {
    token,
    userInfo,
    loading,
    isLoggedIn,
    isAdmin,
    userName,
    doLogin,
    doRegister,
    logout,
    fetchUserInfo,
    initUser
  }
})
