import axios, { type AxiosInstance, type AxiosError, type InternalAxiosRequestConfig, type AxiosResponse } from 'axios'
import { ElMessage } from 'element-plus'

const api: AxiosInstance = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// Token 刷新状态
let isRefreshing = false
type RefreshSubscriber = {
  resolve: (token: string) => void
  reject: (error: unknown) => void
}
let refreshSubscribers: RefreshSubscriber[] = []

// 订阅 token 刷新（成功与失败都必须结束等待，否则并发请求会永久挂起）
function subscribeTokenRefresh(
  resolve: (token: string) => void,
  reject: (error: unknown) => void
) {
  refreshSubscribers.push({ resolve, reject })
}

// 通知所有订阅者
function notifyTokenRefresh(token: string) {
  const subscribers = refreshSubscribers
  refreshSubscribers = []
  subscribers.forEach(subscriber => subscriber.resolve(token))
}

// 刷新失败：让所有排队请求立即失败，而不是一直 pending
function rejectTokenRefresh(error: unknown) {
  const subscribers = refreshSubscribers
  refreshSubscribers = []
  subscribers.forEach(subscriber => subscriber.reject(error))
}

// 请求拦截器 - 添加 Token
api.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const token = localStorage.getItem('token')
    if (token && config.headers) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  (error: AxiosError) => {
    return Promise.reject(error)
  }
)

// 刷新 Token 的函数 - 使用独立的 axios 实例避免拦截器循环
async function refreshToken(): Promise<string> {
  const refreshToken = localStorage.getItem('refresh_token')
  if (!refreshToken) {
    throw new Error('No refresh token')
  }
  
  const response = await axios.post('/api/auth/refresh', {
    refresh_token: refreshToken
  })
  
  const newToken = response.data.token
  localStorage.setItem('token', newToken)
  return newToken
}

// 响应拦截器 - 处理错误
api.interceptors.response.use(
  (response: AxiosResponse) => {
    // 检查响应类型，只对 JSON 应用 data 转换
    const contentType = response.headers['content-type']
    if (contentType && contentType.includes('application/json')) {
      return response.data
    }
    // 非 JSON 响应（如文件下载）直接返回完整 response
    return response
  },
  async (error: AxiosError<{ message?: string }>) => {
    const originalRequest = error.config as InternalAxiosRequestConfig & { _retry?: boolean }
    const url = error.config?.url || ''
    const isAuthEndpoint = [
      '/auth/login',
      '/auth/register',
      '/auth/refresh',
      '/auth/forgot-password',
      '/auth/reset-password'
    ].some(endpoint => url.includes(endpoint))
    
    if (error.response?.status === 401 && originalRequest && !originalRequest._retry && !isAuthEndpoint) {
      // 如果正在刷新 token，将请求加入队列
      if (isRefreshing) {
        return new Promise((resolve, reject) => {
          subscribeTokenRefresh(
            (token: string) => {
              if (originalRequest.headers) {
                originalRequest.headers.Authorization = `Bearer ${token}`
              }
              resolve(api(originalRequest))
            },
            reject
          )
        })
      }
      
      originalRequest._retry = true
      isRefreshing = true
      
      try {
        const newToken = await refreshToken()
        isRefreshing = false
        notifyTokenRefresh(newToken)
        
        // 重试原始请求
        if (originalRequest.headers) {
          originalRequest.headers.Authorization = `Bearer ${newToken}`
        }
        return api(originalRequest)
      } catch (refreshError) {
        isRefreshing = false
        // 刷新失败：先释放所有排队请求，再走下方统一 401 处理
        rejectTokenRefresh(refreshError)
        error.response = { status: 401 } as AxiosResponse
      }
    }
    
    if (error.response) {
      const status = error.response.status
      
      switch (status) {
        case 401:
          if (!isAuthEndpoint) {
            ElMessage.error('登录已过期，请重新登录')
            localStorage.removeItem('token')
            localStorage.removeItem('refresh_token')
            localStorage.removeItem('user')
            window.location.href = '/login'
          }
          break
        case 403:
          ElMessage.error('没有权限执行此操作')
          break
        case 404:
          ElMessage.error('请求的资源不存在')
          break
        case 500:
          ElMessage.error('服务器内部错误')
          break
        default:
          ElMessage.error(error.response.data?.message || '请求失败')
      }
    } else if (error.request) {
      ElMessage.error('网络连接失败，请检查网络')
    } else {
      ElMessage.error('请求配置错误')
    }
    
    return Promise.reject(error)
  }
)

export default api
