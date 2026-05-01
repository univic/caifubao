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
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.03);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.06);
  transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  overflow: hidden;
}

.feature-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent, rgba(255, 107, 53, 0.3), transparent);
  opacity: 0;
  transition: opacity 0.4s ease;
}

.feature-card:hover {
  transform: translateY(-8px);
  background: rgba(255, 255, 255, 0.06);
  border-color: rgba(255, 107, 53, 0.2);
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3),
              0 0 60px rgba(255, 107, 53, 0.1);
}

.feature-card:hover::before {
  opacity: 1;
}

.feature-card__icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 56px;
  height: 56px;
  margin-bottom: 20px;
  border-radius: 14px;
  background: linear-gradient(135deg, rgba(255, 107, 53, 0.15), rgba(247, 201, 72, 0.1));
  font-size: 28px;
}

.feature-card__icon :deep(svg) {
  width: 28px;
  height: 28px;
  fill: #ff6b35;
}

.feature-card__title {
  margin: 0 0 12px;
  font-size: 20px;
  font-weight: 600;
  color: #ffffff;
  font-family: 'Orbitron', 'PingFang SC', sans-serif;
}

.feature-card__description {
  margin: 0;
  font-size: 14px;
  line-height: 1.6;
  color: #a0a0b0;
}

/* Mobile responsive */
@media (max-width: 768px) {
  .feature-card {
    padding: 24px 20px;
  }
  
  .feature-card__icon {
    width: 48px;
    height: 48px;
    margin-bottom: 16px;
    font-size: 24px;
  }
  
  .feature-card__title {
    font-size: 18px;
  }
  
  .feature-card__description {
    font-size: 13px;
  }
}
</style>
