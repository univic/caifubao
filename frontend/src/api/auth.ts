import api from './index'

export interface LoginParams {
  username: string
  password: string
}

export interface LoginResponse {
  token: string
  refresh_token?: string
  user: UserInfo
}

export interface RegisterParams {
  username: string
  email: string
  password: string
}

export interface UserInfo {
  id: string
  username: string
  email: string
  role: string[]
  user_status: string[]
}

export const authApi = {
  login(data: LoginParams) {
    return api.post<LoginResponse>('/auth/login', data)
  },
  
  register(data: RegisterParams) {
    return api.post('/auth/register', data)
  },
  
  logout() {
    return api.post('/auth/logout')
  },
  
  refreshToken(refreshToken: string) {
    return api.post<{ token: string }>('/auth/refresh', { refresh_token: refreshToken })
  },
  
  getUserInfo() {
    return api.get<UserInfo>('/auth/user')
  },
  
  updateProfile(data: Partial<UserInfo>) {
    return api.put('/auth/profile', data)
  },
  
  changePassword(oldPassword: string, newPassword: string) {
    return api.post('/auth/change-password', { old_password: oldPassword, new_password: newPassword })
  },
  
  forgotPassword(email: string) {
    return api.post('/auth/forgot-password', { email })
  },
  
  resetPassword(token: string, password: string) {
    return api.post('/auth/reset-password', { token, password })
  }
}

export const userApi = {
  getUserList(params?: { page?: number; page_size?: number; keyword?: string }) {
    return api.get('/admin/users', { params })
  },
  
  getUser(id: string) {
    return api.get(`/admin/users/${id}`)
  },
  
  createUser(data: { username: string; email: string; password: string; role?: string[] }) {
    return api.post('/admin/users', data)
  },
  
  updateUser(id: string, data: Partial<UserInfo>) {
    return api.put(`/admin/users/${id}`, data)
  },
  
  deleteUser(id: string) {
    return api.delete(`/admin/users/${id}`)
  },
  
  disableUser(id: string) {
    return api.post(`/admin/users/${id}/disable`)
  },
  
  enableUser(id: string) {
    return api.post(`/admin/users/${id}/enable`)
  }
}
