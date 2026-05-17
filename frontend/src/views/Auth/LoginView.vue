<template>
  <div class="login-page">
    <div class="login-page__grain" />
    <div class="login-page__glow login-page__glow--one" />
    <div class="login-page__glow login-page__glow--two" />

    <div class="login-shell">
      <section class="login-story">
        <div class="eyebrow">Linear-inspired workspace</div>
        <h1>财富宝</h1>
        <p class="lead">
          用更安静的界面看懂行情、信号和回测，把复杂交易工作流收进一条清晰的时间线里。
        </p>

        <div class="story-metrics">
          <div class="story-metric">
            <strong>日线行情</strong>
            <span>稳定更新主要市场数据</span>
          </div>
          <div class="story-metric">
            <strong>信号与回测</strong>
            <span>从发现到验证更顺滑</span>
          </div>
          <div class="story-metric">
            <strong>数据质量</strong>
            <span>一眼看见 freshness 状态</span>
          </div>
        </div>

        <ul class="story-points">
          <li>低噪声布局，减少“看起来很忙”的视觉负担</li>
          <li>更快进入市场总览、历史行情和策略验证</li>
          <li>登录后直接进入应用工作区，不再回到公开首页</li>
        </ul>
      </section>

      <section class="login-panel">
        <div class="login-card">
          <div class="login-card__header">
            <div class="login-card__kicker">
              <span>欢迎回来</span>
              <EnvBadge size="small" />
            </div>
            <h2>登录你的量化工作台</h2>
            <p>输入账号后，继续查看行情、信号和回测结果。</p>
          </div>

          <el-form
            ref="formRef"
            :model="form"
            :rules="rules"
            class="login-form"
            @submit.prevent="handleLogin"
          >
            <el-form-item prop="username">
              <el-input
                v-model="form.username"
                placeholder="用户名"
                size="large"
                :prefix-icon="User"
                class="login-input"
              />
            </el-form-item>

            <el-form-item prop="password">
              <el-input
                v-model="form.password"
                type="password"
                placeholder="密码"
                size="large"
                :prefix-icon="Lock"
                show-password
                class="login-input"
                @keyup.enter="handleLogin"
              />
            </el-form-item>

            <el-form-item>
              <el-button
                type="primary"
                size="large"
                :loading="loading"
                class="login-btn"
                @click="handleLogin"
              >
                进入工作区
              </el-button>
            </el-form-item>
          </el-form>

          <div class="login-card__footer">
            <router-link to="/register">注册账号</router-link>
            <span class="divider">·</span>
            <router-link to="/forgot-password">忘记密码</router-link>
          </div>
        </div>
      </section>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive, ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { ElMessage, type FormInstance } from 'element-plus'
import { User, Lock } from '@element-plus/icons-vue'
import EnvBadge from '@/components/layout/EnvBadge.vue'

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()

const formRef = ref<FormInstance>()
const loading = ref(false)

const form = reactive({
  username: '',
  password: ''
})

const rules = {
  username: [{ required: true, message: '请输入用户名', trigger: 'blur' }],
  password: [{ required: true, message: '请输入密码', trigger: 'blur' }]
}

async function handleLogin() {
  if (loading.value) return
  if (!formRef.value) return

  await formRef.value.validate(async (valid) => {
    if (!valid) return

    loading.value = true
    try {
      await userStore.doLogin(form.username, form.password)
      ElMessage.success('登录成功')
      const redirect =
        typeof route.query.redirect === 'string' && route.query.redirect !== '/login'
          ? route.query.redirect
          : '/'
      router.replace(redirect)
    } catch (error: any) {
      let errorMessage = '登录失败'
      if (error.response) {
        const status = error.response.status
        const message = error.response.data?.message

        switch (status) {
          case 400:
            errorMessage = message || '请求参数错误'
            break
          case 401:
            errorMessage = message || '用户名或密码错误'
            break
          case 403:
            errorMessage = message || '账号已被锁定'
            break
          case 404:
            errorMessage = '用户不存在'
            break
          case 429:
            errorMessage = '登录尝试次数过多，请稍后重试'
            break
          case 500:
            errorMessage = '服务器内部错误，请稍后重试'
            break
          case 503:
            errorMessage = message || '数据库暂时不可用，请稍后重试'
            break
          default:
            errorMessage = message || `登录失败 (${status})`
        }
      } else if (error.request) {
        errorMessage = '网络连接失败，请检查网络设置'
      } else {
        errorMessage = error.message || '登录失败'
      }

      ElMessage.error(errorMessage)
    } finally {
      loading.value = false
    }
  })
}
</script>

<style scoped lang="scss">
.login-page {
  position: relative;
  min-height: 100vh;
  overflow: hidden;
  background:
    radial-gradient(circle at top left, rgba(113, 112, 255, 0.18), transparent 26%),
    radial-gradient(circle at 82% 18%, rgba(94, 106, 210, 0.16), transparent 22%),
    linear-gradient(180deg, #08090a 0%, #0f1011 100%);
}

.login-page__grain {
  position: absolute;
  inset: 0;
  opacity: 0.08;
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.02) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
  background-size: 28px 28px;
  pointer-events: none;
}

.login-page__glow {
  position: absolute;
  border-radius: 999px;
  filter: blur(26px);
  pointer-events: none;
}

.login-page__glow--one {
  top: 8%;
  right: 8%;
  width: 280px;
  height: 280px;
  background: rgba(113, 112, 255, 0.22);
}

.login-page__glow--two {
  left: -80px;
  bottom: -120px;
  width: 360px;
  height: 360px;
  background: rgba(255, 255, 255, 0.04);
}

.login-shell {
  position: relative;
  z-index: 1;
  min-height: 100vh;
  display: grid;
  grid-template-columns: minmax(0, 1.08fr) minmax(420px, 0.92fr);
  gap: 28px;
  padding: 32px;
}

.login-story,
.login-card {
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.03);
  box-shadow:
    0 0 0 1px rgba(0, 0, 0, 0.12),
    0 24px 60px rgba(0, 0, 0, 0.32);
  backdrop-filter: blur(18px);
  -webkit-backdrop-filter: blur(18px);
}

.login-story {
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding: clamp(28px, 4vw, 56px);
}

.eyebrow {
  display: inline-flex;
  align-self: flex-start;
  padding: 8px 12px;
  border-radius: 999px;
  border: 1px solid rgba(113, 112, 255, 0.18);
  background: rgba(113, 112, 255, 0.12);
  color: #828fff;
  font-size: 12px;
  font-weight: 590;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.login-story h1 {
  margin: 20px 0 16px;
  font-size: clamp(42px, 7vw, 72px);
  line-height: 0.95;
  letter-spacing: -1.584px;
  color: #f7f8f8;
  font-weight: 510;
}

.lead {
  max-width: 560px;
  margin: 0;
  font-size: clamp(16px, 1.4vw, 18px);
  line-height: 1.7;
  color: #d0d6e0;
}

.story-metrics {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 14px;
  margin-top: 28px;
}

.story-metric {
  padding: 18px;
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.06);
  background: rgba(255, 255, 255, 0.02);

  strong {
    display: block;
    font-size: 14px;
    color: #f7f8f8;
    margin-bottom: 6px;
    font-weight: 590;
  }

  span {
    display: block;
    color: #8a8f98;
    font-size: 13px;
    line-height: 1.6;
  }
}

.story-points {
  margin-top: 28px;
  padding-left: 18px;
  color: #d0d6e0;
  display: grid;
  gap: 10px;

  li {
    line-height: 1.7;
  }
}

.login-panel {
  display: flex;
  align-items: center;
}

.login-card {
  width: 100%;
  padding: clamp(24px, 3.6vw, 40px);
}

.login-card__header {
  margin-bottom: 24px;

  .login-card__kicker {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 7px 11px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.04);
    color: #d0d6e0;
    font-size: 12px;
    font-weight: 590;
  }

  h2 {
    margin: 16px 0 10px;
    font-size: 28px;
    line-height: 1.15;
    letter-spacing: -0.04em;
    color: #f7f8f8;
    font-weight: 510;
  }

  p {
    margin: 0;
    color: #8a8f98;
    line-height: 1.6;
  }
}

.login-form {
  :deep(.el-form-item) {
    margin-bottom: 14px;
  }

  :deep(.el-input__wrapper) {
    height: 50px;
    border-radius: 6px;
    background: rgba(255, 255, 255, 0.03);
    box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.08);
  }

  :deep(.el-input__wrapper.is-focus) {
    box-shadow: inset 0 0 0 1px rgba(113, 112, 255, 0.36), 0 0 0 4px rgba(113, 112, 255, 0.12);
  }

  :deep(.el-input__inner) {
    color: #f7f8f8;
  }

  :deep(.el-input__inner::placeholder) {
    color: #62666d;
  }
}

.login-btn {
  width: 100%;
  height: 52px;
  border: 1px solid rgba(113, 112, 255, 0.28);
  border-radius: 6px;
  background: linear-gradient(135deg, #5e6ad2 0%, #7170ff 100%);
  box-shadow: 0 4px 12px rgba(94, 106, 210, 0.2);
  font-weight: 590;
  letter-spacing: 0.01em;
}

.login-card__footer {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 12px;
  margin-top: 18px;

  a {
    color: #828fff;
    text-decoration: none;
    font-size: 14px;
    font-weight: 510;

    &:hover {
      text-decoration: underline;
    }
  }

  .divider {
    color: #62666d;
  }
}

@media (max-width: 1100px) {
  .login-shell {
    grid-template-columns: 1fr;
  }

  .login-story {
    min-height: auto;
  }
}

@media (max-width: 768px) {
  .login-shell {
    padding: 16px;
    gap: 16px;
  }

  .login-story {
    padding: 24px;
    border-radius: 24px;
  }

  .story-metrics {
    grid-template-columns: 1fr;
  }

  .login-card {
    border-radius: 24px;
  }
}
</style>
