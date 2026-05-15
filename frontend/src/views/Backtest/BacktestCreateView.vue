<template>
  <div class="backtest-create-page">
    <section class="topbar">
      <div class="title-block">
        <p class="eyebrow">Score Strategy Backtest</p>
        <h1>创建评分策略回测</h1>
        <p class="subtitle">基于评分策略模拟历史持仓表现</p>
      </div>
      <div class="topbar-actions">
        <el-button plain @click="goBack">返回</el-button>
      </div>
    </section>

    <el-card class="form-card" shadow="hover">
      <el-form
        ref="formRef"
        :model="form"
        :rules="rules"
        label-position="top"
        class="backtest-form"
        @submit.prevent="handleSubmit"
      >
        <div class="form-grid">
          <el-form-item label="评分周期" prop="horizon">
            <el-select v-model="form.horizon" placeholder="选择评分周期" class="full-width">
              <el-option :value="5" label="Score5 · 短期 (5日)" />
              <el-option :value="20" label="Score20 · 中期 (20日)" />
              <el-option :value="60" label="Score60 · 长期 (60日)" />
            </el-select>
          </el-form-item>

          <el-form-item label="持仓数量" prop="top_n">
            <el-input-number
              v-model="form.top_n"
              :min="1"
              :max="50"
              :step="1"
              class="full-width"
              placeholder="选择 Top N 只股票"
            />
            <template #extra>
              <span class="form-hint">每日持有评分最高的前 N 只股票</span>
            </template>
          </el-form-item>

          <el-form-item label="回测日期区间" prop="dateRange">
            <el-date-picker
              v-model="form.dateRange"
              type="daterange"
              range-separator="至"
              start-placeholder="开始日期"
              end-placeholder="结束日期"
              format="YYYY-MM-DD"
              value-format="YYYY-MM-DD"
              class="full-width"
            />
            <template #extra>
              <span class="form-hint">选择回测的时间范围</span>
            </template>
          </el-form-item>

          <el-form-item label="模型版本" prop="model_version">
            <el-input
              v-model="form.model_version"
              placeholder="可选，留空使用最新版本"
              clearable
            />
            <template #extra>
              <span class="form-hint">指定评分模型的版本号</span>
            </template>
          </el-form-item>
        </div>

        <div class="form-actions">
          <el-button @click="goBack">取消</el-button>
          <el-button type="primary" native-type="submit" :loading="submitting">
            开始回测
          </el-button>
        </div>
      </el-form>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'
import { scoreStrategiesApi, type BacktestResponse } from '@/api/scoreStrategies'

const router = useRouter()
const formRef = ref<FormInstance>()
const submitting = ref(false)

const form = reactive({
  horizon: 5 as number,
  top_n: 10,
  dateRange: null as [string, string] | null,
  model_version: ''
})

const rules: FormRules = {
  horizon: [{ required: true, message: '请选择评分周期', trigger: 'change' }],
  top_n: [
    { required: true, message: '请输入持仓数量', trigger: 'blur' },
    { type: 'number', min: 1, max: 50, message: '持仓数量在 1-50 之间', trigger: 'blur' }
  ],
  dateRange: [{ required: true, message: '请选择回测日期区间', trigger: 'change' }]
}

async function handleSubmit() {
  if (!formRef.value) return
  const valid = await formRef.value.validate().catch(() => false)
  if (!valid) return

  submitting.value = true
  try {
    const [start_date, end_date] = form.dateRange!
    const resp: BacktestResponse = await scoreStrategiesApi.runBacktest({
      horizon: form.horizon,
      top_n: form.top_n,
      start_date,
      end_date,
      model_version: form.model_version || undefined
    })

    if (!resp.success) {
      ElMessage.error(resp.message || '回测请求失败')
      return
    }

    sessionStorage.setItem('backtest_result', JSON.stringify(resp))
    ElMessage.success('回测完成')
    router.push({ name: 'BacktestResultView' })
  } catch (error: any) {
    ElMessage.error(error?.response?.data?.message || '回测请求失败，请稍后重试')
  } finally {
    submitting.value = false
  }
}

function goBack() {
  router.push({ name: 'BacktestList' })
}
</script>

<style scoped lang="scss">
.backtest-create-page {
  display: flex;
  flex-direction: column;
  gap: 20px;
  max-width: 740px;
  margin: 0 auto;
}

.topbar {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  padding: 26px 28px;
  border-radius: 22px;
  background:
    radial-gradient(circle at top right, rgba(113, 112, 255, 0.18), transparent 34%),
    linear-gradient(135deg, #0f1011 0%, #191a1b 55%, #0f1011 100%);
  border: 1px solid rgba(255, 255, 255, 0.08);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.28);
}

.topbar-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  padding-top: 4px;
}

.title-block {
  .eyebrow {
    margin: 0 0 8px;
    color: #828fff;
    font-size: 12px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
  }

  h1 {
    margin: 0 0 8px;
    font-size: 32px;
  }

  .subtitle {
    margin: 0;
    color: var(--color-text-secondary);
    line-height: 1.7;
  }
}

.form-card {
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px 24px;

  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
}

.full-width {
  width: 100%;
}

.form-hint {
  font-size: 12px;
  color: #8a8f98;
  line-height: 1.5;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  margin-top: 24px;
  padding-top: 20px;
  border-top: 1px solid rgba(255, 255, 255, 0.05);
}

:deep(.el-form-item__label) {
  color: #d0d6e0;
  font-weight: 510;
  font-size: 14px;
}

:deep(.el-input__wrapper) {
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 6px;
  box-shadow: none;
}

:deep(.el-input__inner) {
  color: #f7f8f8;
}

:deep(.el-select .el-input__wrapper) {
  background: rgba(255, 255, 255, 0.04);
}

:deep(.el-input-number .el-input__wrapper) {
  background: rgba(255, 255, 255, 0.04);
}

:deep(.el-date-editor .el-input__wrapper) {
  background: rgba(255, 255, 255, 0.04);
}
</style>
