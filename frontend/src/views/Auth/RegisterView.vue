<template>
  <div class="register-container">
    <div class="register-card">
      <div class="register-header">
        <h1>注册账号</h1>
        <p>加入财富宝量化投资平台</p>
      </div>
      
      <el-form
        ref="formRef"
        :model="form"
        :rules="rules"
        class="register-form"
        @submit.prevent="handleRegister"
      >
        <el-form-item prop="username">
          <el-input
            v-model="form.username"
            placeholder="用户名"
            size="large"
            :prefix-icon="User"
          />
        </el-form-item>
        
        <el-form-item prop="email">
          <el-input
            v-model="form.email"
            placeholder="邮箱"
            size="large"
            :prefix-icon="Message"
          />
        </el-form-item>
        
        <el-form-item prop="password">
          <el-input
            v-model="form.password"
            type="password"
            placeholder="密码 (至少8位，包含数字和字母)"
            size="large"
            :prefix-icon="Lock"
            show-password
          />
        </el-form-item>
        
        <el-form-item prop="confirmPassword">
          <el-input
            v-model="form.confirmPassword"
            type="password"
            placeholder="确认密码"
            size="large"
            :prefix-icon="Lock"
            show-password
            @keyup.enter="handleRegister"
          />
        </el-form-item>
        
        <el-form-item>
          <el-button
            type="primary"
            size="large"
            :loading="loading"
            class="register-btn"
            @click="handleRegister"
          >
            注册
          </el-button>
        </el-form-item>
      </el-form>
      
      <div class="register-footer">
        <span>已有账号？</span>
        <router-link to="/login">立即登录</router-link>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'
import { User, Lock, Message } from '@element-plus/icons-vue'

const router = useRouter()
const userStore = useUserStore()

const formRef = ref<FormInstance>()
const loading = ref(false)

const form = reactive({
  username: '',
  email: '',
  password: '',
  confirmPassword: ''
})

// 密码验证规则
const validatePassword = (rule: any, value: string, callback: any) => {
  if (!value) {
    callback(new Error('请输入密码'))
  } else if (value.length < 8) {
    callback(new Error('密码至少8位'))
  } else if (!/\d/.test(value)) {
    callback(new Error('密码需包含数字'))
  } else if (!/[a-zA-Z]/.test(value)) {
    callback(new Error('密码需包含字母'))
  } else {
    callback()
  }
}

// 确认密码验证
const validateConfirmPassword = (rule: any, value: string, callback: any) => {
  if (!value) {
    callback(new Error('请确认密码'))
  } else if (value !== form.password) {
    callback(new Error('两次输入密码不一致'))
  } else {
    callback()
  }
}

const rules: FormRules = {
  username: [
    { required: true, message: '请输入用户名', trigger: 'blur' },
    { min: 3, max: 20, message: '用户名3-20个字符', trigger: 'blur' }
  ],
  email: [
    { required: true, message: '请输入邮箱', trigger: 'blur' },
    { type: 'email', message: '请输入正确的邮箱格式', trigger: 'blur' }
  ],
  password: [
    { required: true, validator: validatePassword, trigger: 'blur' }
  ],
  confirmPassword: [
    { required: true, validator: validateConfirmPassword, trigger: 'blur' }
  ]
}

async function handleRegister() {
  if (!formRef.value) return
  
  await formRef.value.validate(async (valid) => {
    if (!valid) return
    
    loading.value = true
    try {
      await userStore.doRegister(form.username, form.email, form.password)
      ElMessage.success('注册成功，请登录')
      router.push('/login')
    } catch (error: any) {
      // Enhanced error handling with specific messages
      let errorMessage = '注册失败'
      if (error.response) {
        const status = error.response.status
        const message = error.response.data?.message
        
        switch (status) {
          case 400:
            // Check for specific error messages from backend
            if (message?.includes('uppercase')) {
              errorMessage = '密码必须包含至少一个大写字母'
            } else if (message?.includes('lowercase')) {
              errorMessage = '密码必须包含至少一个小写字母'
            } else if (message?.includes('digit')) {
              errorMessage = '密码必须包含至少一个数字'
            } else if (message?.includes('special')) {
              errorMessage = '密码必须包含至少一个特殊字符'
            } else if (message?.includes('8 characters')) {
              errorMessage = '密码长度至少为8位'
            } else {
              errorMessage = message || '请求参数错误'
            }
            break
          case 409:
            errorMessage = '用户名或邮箱已被注册'
            break
          case 422:
            errorMessage = message || '数据验证失败'
            break
          case 500:
            errorMessage = '服务器内部错误，请稍后重试'
            break
          default:
            errorMessage = message || `注册失败 (${status})`
        }
      } else if (error.request) {
        errorMessage = '网络连接失败，请检查网络设置'
      } else {
        errorMessage = error.message || '注册失败'
      }
      
      ElMessage.error(errorMessage)
    } finally {
      loading.value = false
    }
  })
}
</script>

<style scoped lang="scss">
.register-container {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background:
    radial-gradient(circle at 18% 0%, rgba(113, 112, 255, 0.12), transparent 28%),
    radial-gradient(circle at 82% 0%, rgba(94, 106, 210, 0.12), transparent 24%),
    #08090a;
}

.register-card {
  width: 400px;
  padding: 40px;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  box-shadow: var(--box-shadow-light);
}

.register-header {
  text-align: center;
  margin-bottom: 32px;

  h1 {
    font-size: 24px;
    font-weight: 590;
    letter-spacing: -0.288px;
    color: var(--color-text-primary);
    margin-bottom: 8px;
  }

  p {
    font-size: 15px;
    color: var(--color-text-secondary);
    letter-spacing: -0.165px;
  }
}

.register-form {
  .register-btn {
    width: 100%;
  }
}

.register-footer {
  text-align: center;
  margin-top: 16px;

  span {
    color: var(--color-text-secondary);
    font-size: 14px;
  }

  a {
    color: var(--color-primary);
    text-decoration: none;
    font-size: 14px;

    &:hover {
      color: var(--color-primary-hover);
    }
  }
}
</style>
