<template>
  <div class="forgot-container">
    <div class="forgot-card">
      <div class="forgot-header">
        <h1>找回密码</h1>
        <p>输入您的注册邮箱，我们将发送密码重置链接</p>
      </div>
      
      <!-- 步骤1: 输入邮箱 -->
      <el-form
        v-if="step === 1"
        ref="formRef"
        :model="form"
        :rules="rules"
        class="forgot-form"
        @submit.prevent="handleSubmitEmail"
      >
        <el-form-item prop="email">
          <el-input
            v-model="form.email"
            placeholder="注册邮箱"
            size="large"
            :prefix-icon="Message"
          />
        </el-form-item>
        
        <el-form-item>
          <el-button
            type="primary"
            size="large"
            :loading="loading"
            class="submit-btn"
            @click="handleSubmitEmail"
          >
            发送重置链接
          </el-button>
        </el-form-item>
      </el-form>
      
      <!-- 步骤2: 邮件已发送 -->
      <div v-else class="success-step">
        <el-icon class="success-icon" :size="64"><CircleCheckFilled /></el-icon>
        <h2>邮件已发送</h2>
        <p>我们已向 <strong>{{ form.email }}</strong> 发送了密码重置链接</p>
        <p class="hint">请查收邮件并点击链接重置密码，链接有效期为24小时</p>
        <el-button type="primary" size="large" @click="step = 1">
          重新发送
        </el-button>
        <div class="back-link">
          <router-link to="/login">返回登录</router-link>
        </div>
      </div>
      
      <div v-if="step === 1" class="forgot-footer">
        <router-link to="/login">返回登录</router-link>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive, ref } from 'vue'
import { authApi } from '@/api/auth'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'
import { Message, CircleCheckFilled } from '@element-plus/icons-vue'

const formRef = ref<FormInstance>()
const loading = ref(false)
const step = ref(1)

const form = reactive({
  email: ''
})

const rules: FormRules = {
  email: [
    { required: true, message: '请输入邮箱', trigger: 'blur' },
    { type: 'email', message: '请输入正确的邮箱格式', trigger: 'blur' }
  ]
}

async function handleSubmitEmail() {
  if (!formRef.value) return
  
  await formRef.value.validate(async (valid) => {
    if (!valid) return
    
    loading.value = true
    try {
      await authApi.forgotPassword(form.email)
      step.value = 2
    } catch (error: any) {
      ElMessage.error(error.response?.data?.message || '发送失败，请稍后重试')
    } finally {
      loading.value = false
    }
  })
}
</script>

<style scoped lang="scss">
.forgot-container {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.forgot-card {
  width: 400px;
  padding: 40px;
  background: #fff;
  border-radius: 12px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
}

.forgot-header {
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

.forgot-form {
  .submit-btn {
    width: 100%;
  }
}

.success-step {
  text-align: center;
  padding: 20px 0;
  
  .success-icon {
    color: #67c23a;
    margin-bottom: 16px;
  }
  
  h2 {
    font-size: 20px;
    color: #333;
    margin-bottom: 12px;
  }
  
  p {
    font-size: 14px;
    color: #666;
    margin-bottom: 8px;
    
    strong {
      color: var(--color-primary);
    }
  }
  
  .hint {
    font-size: 12px;
    color: #999;
    margin-bottom: 24px;
  }
  
  .back-link {
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
}

.forgot-footer {
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
