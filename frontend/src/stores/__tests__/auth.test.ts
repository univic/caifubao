// frontend/src/stores/__tests__/auth.test.ts
import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useUserStore } from '../user'

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: (key: string) => store[key] || null,
    setItem: (key: string, value: string) => { store[key] = value },
    removeItem: (key: string) => { delete store[key] },
    clear: () => { store = {} }
  }
})()

Object.defineProperty(global, 'localStorage', { value: localStorageMock })

describe('Authentication Flow', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    localStorage.clear()
  })

  describe('Login Flow', () => {
    it('should have null token initially', () => {
      const store = useUserStore()
      expect(store.token).toBeNull()
    })

    it('should not be logged in initially', () => {
      const store = useUserStore()
      expect(store.isLoggedIn).toBe(false)
    })

    it('should have null userInfo initially', () => {
      const store = useUserStore()
      expect(store.userInfo).toBeNull()
    })
  })

  describe('Logout Flow', () => {
    it('should clear all auth data on logout', () => {
      const mockUser = { 
        id: '1', 
        username: 'test', 
        email: 'test@test.com', 
        role: ['USER'], 
        user_status: ['20'] 
      }
      localStorage.setItem('token', 'test-token')
      localStorage.setItem('refresh_token', 'refresh-token')
      localStorage.setItem('user', JSON.stringify(mockUser))
      
      const store = useUserStore()
      store.initUser()
      store.logout()
      
      expect(store.token).toBeNull()
      expect(store.userInfo).toBeNull()
      expect(localStorage.getItem('token')).toBeNull()
      expect(localStorage.getItem('refresh_token')).toBeNull()
      expect(localStorage.getItem('user')).toBeNull()
    })
  })

  describe('initUser', () => {
    it('should initialize user from localStorage', () => {
      const mockUser = { 
        id: '1', 
        username: 'test', 
        email: 'test@test.com', 
        role: ['USER'], 
        user_status: ['20'] 
      }
      localStorage.setItem('token', 'mock-token')
      localStorage.setItem('user', JSON.stringify(mockUser))
      
      const store = useUserStore()
      store.initUser()
      
      expect(store.token).toBe('mock-token')
      expect(store.userInfo).toEqual(mockUser)
      expect(store.isLoggedIn).toBe(true)
    })

    it('should handle invalid JSON in localStorage', () => {
      localStorage.setItem('token', 'mock-token')
      localStorage.setItem('user', 'invalid-json')
      
      const store = useUserStore()
      store.initUser()
      
      expect(store.token).toBe('mock-token')
      expect(store.userInfo).toBeNull()
    })

    it('should handle empty localStorage', () => {
      const store = useUserStore()
      store.initUser()
      
      expect(store.token).toBeNull()
      expect(store.userInfo).toBeNull()
    })
  })

  describe('Permission Checks', () => {
    it('should return true when user has ADM role', () => {
      const mockUser = { 
        id: '1', 
        username: 'admin', 
        email: 'admin@test.com', 
        role: ['USER', 'ADM'], 
        user_status: ['20'] 
      }
      localStorage.setItem('token', 'mock-token')
      localStorage.setItem('user', JSON.stringify(mockUser))
      
      const store = useUserStore()
      store.initUser()
      
      expect(store.isAdmin).toBe(true)
    })

    it('should return false when user does not have ADM role', () => {
      const mockUser = { 
        id: '1', 
        username: 'user', 
        email: 'user@test.com', 
        role: ['USER'], 
        user_status: ['20'] 
      }
      localStorage.setItem('token', 'mock-token')
      localStorage.setItem('user', JSON.stringify(mockUser))
      
      const store = useUserStore()
      store.initUser()
      
      expect(store.isAdmin).toBe(false)
    })
  })
})

describe('Password Validation', () => {
  const validatePassword = (password: string): { valid: boolean; message: string } => {
    if (!password) {
      return { valid: false, message: '请输入密码' }
    }
    if (password.length < 8) {
      return { valid: false, message: '密码至少8位' }
    }
    if (!/\d/.test(password)) {
      return { valid: false, message: '密码需包含数字' }
    }
    if (!/[a-zA-Z]/.test(password)) {
      return { valid: false, message: '密码需包含字母' }
    }
    return { valid: true, message: '' }
  }

  it('should reject empty password', () => {
    const result = validatePassword('')
    expect(result.valid).toBe(false)
  })

  it('should reject password less than 8 characters', () => {
    const result = validatePassword('12345')
    expect(result.valid).toBe(false)
    expect(result.message).toBe('密码至少8位')
  })

  it('should reject password without numbers', () => {
    const result = validatePassword('abcdefgh')
    expect(result.valid).toBe(false)
    expect(result.message).toBe('密码需包含数字')
  })

  it('should reject password without letters', () => {
    const result = validatePassword('12345678')
    expect(result.valid).toBe(false)
    expect(result.message).toBe('密码需包含字母')
  })

  it('should accept valid password', () => {
    const result = validatePassword('abc12345')
    expect(result.valid).toBe(true)
  })

  it('should accept password with mixed case', () => {
    const result = validatePassword('Test1234')
    expect(result.valid).toBe(true)
  })
})

describe('Email Validation', () => {
  const validateEmail = (email: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
    return emailRegex.test(email)
  }

  it('should reject invalid email', () => {
    expect(validateEmail('invalid')).toBe(false)
    expect(validateEmail('invalid@')).toBe(false)
    expect(validateEmail('@domain.com')).toBe(false)
    expect(validateEmail('invalid@domain')).toBe(false)
    expect(validateEmail('')).toBe(false)
  })

  it('should accept valid email', () => {
    expect(validateEmail('test@test.com')).toBe(true)
    expect(validateEmail('user.name@domain.co.uk')).toBe(true)
    expect(validateEmail('admin@example.org')).toBe(true)
  })
})
