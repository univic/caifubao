<script setup lang="ts">
import GlassButton from '@/components/home/GlassButton.vue'
import FeatureCard from '@/components/home/FeatureCard.vue'

const features = [
  {
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M18.7 8l-5.1 5.2-2.8-2.7L7 14.3"/></svg>',
    title: '市场总览',
    description: '实时行情数据，全方位把握市场动态，了解最新价格走势和交易机会。'
  },
  {
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>',
    title: '智能回测',
    description: '量化策略回测分析，科学验证交易策略有效性，优化投资决策。'
  },
  {
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>',
    title: '信号发现',
    description: '智能交易信号推送，及时发现潜在投资机会，抢占市场先机。'
  }
]
</script>

<template>
  <div class="home-view">
    <div class="home-view__glow home-view__glow--top" />
    <div class="home-view__glow home-view__glow--bottom" />

    <section class="hero">
      <div class="hero__content">
        <p class="hero__eyebrow">Caifubao Quant Workspace</p>
        <h1 class="hero__title">
          <span class="hero__title-main">财富宝</span>
          <span class="hero__title-sub">大 A 量化分析系统</span>
        </h1>
        <p class="hero__description">
          智能量化，洞悉市场 — 用更安静的界面看懂行情、信号和回测。
        </p>
        <div class="hero__actions">
          <GlassButton type="primary" to="/login">
            登录
          </GlassButton>
          <GlassButton type="secondary" to="/register">
            注册
          </GlassButton>
        </div>
      </div>
    </section>

    <section class="features">
      <div class="features__container">
        <h2 class="features__title">核心功能</h2>
        <p class="features__subtitle">
          强大的量化分析工具，助您做出更明智的投资决策
        </p>
        <div class="features__grid">
          <FeatureCard
            v-for="feature in features"
            :key="feature.title"
            :icon="feature.icon"
            :title="feature.title"
            :description="feature.description"
          />
        </div>
      </div>
    </section>

    <footer class="footer">
      <div class="footer__container">
        <p class="footer__copyright">
          &copy; 2026 Caifubao. All rights reserved.
        </p>
      </div>
    </footer>
  </div>
</template>

<style scoped>
.home-view {
  position: relative;
  min-height: 100vh;
  background: #08090a;
  overflow-x: hidden;
  font-family: 'Inter Variable', 'SF Pro Display', -apple-system, system-ui, 'Segoe UI', Roboto, sans-serif;
  font-feature-settings: "cv01", "ss03";
}

.home-view__glow {
  position: absolute;
  border-radius: 999px;
  filter: blur(80px);
  pointer-events: none;
  opacity: 0.3;
}

.home-view__glow--top {
  top: -10%;
  left: 20%;
  width: 600px;
  height: 400px;
  background: rgba(113, 112, 255, 0.14);
}

.home-view__glow--bottom {
  right: -5%;
  bottom: 10%;
  width: 500px;
  height: 500px;
  background: rgba(94, 106, 210, 0.1);
}

/* Hero Section */
.hero {
  position: relative;
  z-index: 1;
  min-height: 68vh;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 80px 24px 60px;
}

.hero__content {
  text-align: center;
  max-width: 800px;
  animation: fadeInUp 0.8s ease-out;
}

.hero__eyebrow {
  display: inline-block;
  margin-bottom: 16px;
  padding: 6px 14px;
  border-radius: 999px;
  border: 1px solid rgba(113, 112, 255, 0.2);
  background: rgba(113, 112, 255, 0.08);
  color: #828fff;
  font-size: 12px;
  font-weight: 590;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.hero__title {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin: 0 0 24px;
}

.hero__title-main {
  font-size: 72px;
  font-weight: 510;
  line-height: 1;
  letter-spacing: -1.584px;
  color: #f7f8f8;
}

.hero__title-sub {
  font-size: 20px;
  font-weight: 400;
  color: #8a8f98;
  letter-spacing: -0.24px;
}

.hero__description {
  font-size: 18px;
  color: #8a8f98;
  margin: 0 0 48px;
  line-height: 1.6;
  letter-spacing: -0.165px;
  max-width: 560px;
  margin-left: auto;
  margin-right: auto;
}

.hero__actions {
  display: flex;
  gap: 16px;
  justify-content: center;
  flex-wrap: wrap;
}

/* Features Section */
.features {
  position: relative;
  z-index: 1;
  padding: 80px 24px;
  border-top: 1px solid rgba(255, 255, 255, 0.05);
}

.features__container {
  max-width: 1200px;
  margin: 0 auto;
  width: 100%;
}

.features__title {
  font-size: 32px;
  font-weight: 510;
  color: #f7f8f8;
  text-align: center;
  margin: 0 0 12px;
  letter-spacing: -0.704px;
}

.features__subtitle {
  font-size: 16px;
  color: #8a8f98;
  text-align: center;
  margin: 0 0 48px;
}

.features__grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 32px;
}

/* Footer */
.footer {
  position: relative;
  z-index: 1;
  padding: 32px 24px;
  border-top: 1px solid rgba(255, 255, 255, 0.05);
}

.footer__container {
  max-width: 1200px;
  margin: 0 auto;
  text-align: center;
}

.footer__copyright {
  margin: 0;
  font-size: 13px;
  color: #62666d;
}

/* Animations */
@keyframes fadeInUp {
  from {
    opacity: 0;
    transform: translateY(24px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* Responsive */
@media (max-width: 1024px) {
  .features__grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .hero {
    min-height: 60vh;
  }

  .hero__title-main {
    font-size: 48px;
    letter-spacing: -1.056px;
  }
}

@media (max-width: 768px) {
  .hero {
    min-height: 65vh;
    padding: 64px 16px 48px;
  }

  .hero__title-main {
    font-size: 36px;
    letter-spacing: -0.704px;
  }

  .hero__title-sub {
    font-size: 16px;
  }

  .hero__description {
    font-size: 16px;
    margin-bottom: 36px;
  }

  .hero__actions {
    flex-direction: column;
    align-items: center;
  }

  .features {
    padding: 48px 16px;
  }

  .features__grid {
    grid-template-columns: 1fr;
    gap: 24px;
  }

  .features__title {
    font-size: 24px;
    letter-spacing: -0.288px;
  }
}

@media (max-width: 480px) {
  .hero__title-main {
    font-size: 28px;
    letter-spacing: -0.24px;
  }

  .hero__description {
    font-size: 15px;
  }
}
</style>
