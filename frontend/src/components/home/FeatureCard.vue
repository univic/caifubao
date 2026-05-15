<script setup lang="ts">
interface Props {
  icon?: string
  title: string
  description: string
}

defineProps<Props>()
</script>

<template>
  <div class="feature-card">
    <div v-if="icon" class="feature-card__icon">
      <span v-html="icon" />
    </div>
    <h3 class="feature-card__title">{{ title }}</h3>
    <p class="feature-card__description">{{ description }}</p>
  </div>
</template>

<style scoped>
.feature-card {
  position: relative;
  padding: 32px 28px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
  transition: all 0.2s ease;
  overflow: hidden;
}

.feature-card:hover {
  transform: translateY(-4px);
  background: rgba(255, 255, 255, 0.04);
  border-color: rgba(113, 112, 255, 0.2);
  box-shadow: 0 12px 32px rgba(0, 0, 0, 0.24);
}

.feature-card__icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 48px;
  height: 48px;
  margin-bottom: 20px;
  border-radius: 10px;
  background: rgba(113, 112, 255, 0.12);
  font-size: 24px;
}

.feature-card__icon :deep(svg) {
  width: 24px;
  height: 24px;
  stroke: #7170ff;
}

.feature-card__title {
  margin: 0 0 8px;
  font-size: 18px;
  font-weight: 590;
  letter-spacing: -0.24px;
  color: #f7f8f8;
}

.feature-card__description {
  margin: 0;
  font-size: 15px;
  line-height: 1.6;
  color: #8a8f98;
  letter-spacing: -0.165px;
}
</style>
