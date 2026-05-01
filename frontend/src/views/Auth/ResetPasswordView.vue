<template>
  <div class="reset-container">
    <div class="reset-card">
      <div class="reset-header">
        <h1>重置密码</h1>
        <p>请输入您的新密码</p>
      </div>
      
      <el-form
        ref="formRef"
        :model="form"
        :rules="rules"
        class="reset-form"
        @submit.prevent="handleReset"
      >
        <el-form-item prop="password">
          <el-input
            v-model="form.password"
            type="password"
            placeholder="新密码 (至少8位，包含数字和字母)"
            size="large"
            :prefix-icon="Lock"
            show-password
          />
        </el-form-item>
        
        <el-form-item prop="confirmPassword">
          <el-input
            v-model="form.confirmPassword"
            type="password"
            placeholder="确认新密码"
            size="large"
            :prefix-icon="Lock"
            show-password
            @keyup.enter="handleReset"
          />
        </el-form-item>
        
        <el-form-item>
          <el-button
            type="primary"
            size="large"
            :loading="loading"
            class="reset-btn"
            @click="handleReset"
          >
            重置密码
          </el-button>
        </el-form-item>
      </el-form>
      
      <div class="reset-footer">
        <router-link to="/login">返回登录</router-link>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive, ref, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { authApi } from '@/api/auth'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'
import { Lock } from '@element-plus/icons-vue'

const router = useRouter()
const route = useRoute()
const formRef = ref<FormInstance>()
const loading = ref(false)

const form = reactive({
  password: '',
  confirmPassword: ''
})

// 密码验证规则
const validatePassword = (rule: any, value: string, callback: any) => {
  if (!value) {
    callback(new Error('请输入新密码'))
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
  password: [
    { required: true, validator: validatePassword, trigger: 'blur' }
  ],
  confirmPassword: [
    { required: true, validator: validateConfirmPassword, trigger: 'blur' }
  ]
}

onMounted(() => {
  // 检查是否有 token 参数
  const token = route.query.token
  if (!token) {
    ElMessage.error('无效的重置链接')
    router.push('/forgot-password')
  }
})

async function handleReset() {
  if (!formRef.value) return
  
  await formRef.value.validate(async (valid) => {
    if (!valid) return
    
    const token = route.query.token as string
    if (!token) {
      ElMessage.error('无效的 token')
      return
    }
    
    loading.value = true
    try {
      await authApi.resetPassword(token, form.password)
      ElMessage.success('密码重置成功')
      router.push('/login')
    } catch (error: any) {
      ElMessage.error(error.response?.data?.message || '重置失败，请重新尝试')
    } finally {
      loading.value = false
    }
  })
}
</script>

<style scoped lang="scss">
.reset-container {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.reset-card {
  width: 400px;
  padding: 40px;
  background: #fff;
  border-radius: 12px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
}

.reset-header {
  text-align: center;
  margin-bottom: 32px;
  
  h1 {
    font-size: 24px;
    font-weight: 600;
    color: var(--color-primary);
    margin-bottom: 8px;
  }
  
  p {
    font-size: 14px;
    color: var(--color-text-secondary);
  }
}

.reset-form {
  .reset-btn {
    width: 100%;
  }
}

.reset-footer {
  text-align: center;
  margin-top: 16px;
  
  a {
    color: var(--color-primary);
    text-decoration: none;
    font-size: 14px;
    
    &:hover {
      text-decoration: underline;
    }
  }
}
</style>
