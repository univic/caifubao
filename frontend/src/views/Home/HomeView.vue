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
    <!-- Hero Section with Black Hole Background (65-70% of viewport) -->
    <section class="hero">
      <img src="/blackhole_bg.jpg" alt="Gargantua Black Hole" class="hero__bg" />
      
      <div class="hero__content">
        <h1 class="hero__title">
          <span class="hero__title-main">Caifubao</span>
          <span class="hero__title-sub">大A量化分析系统</span>
        </h1>
        
        <p class="hero__description">
          智能量化，洞悉市场
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
    
    <!-- Features Section (30-35% of viewport) -->
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
    
    <!-- Footer -->
    <footer class="footer">
      <div class="footer__container">
        <p class="footer__copyright">
          © 2026 Caifubao. All rights reserved.
        </p>
      </div>
    </footer>
  </div>
</template>

<style scoped>
.home-view {
  min-height: 100vh;
  background: #0a0a0f;
  overflow-x: hidden;
}

/* Hero Section - 65-70% of viewport */
.hero {
  position: relative;
  height: 68vh;
  min-height: 500px;
  max-height: 800px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 40px 20px;
}

.hero__bg {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  object-fit: cover;
  object-position: center;
  z-index: 0;
}

.hero__content {
  position: relative;
  z-index: 1;
  text-align: center;
  max-width: 800px;
  animation: fadeInUp 1s ease-out;
}

.hero__title {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin: 0 0 24px;
}

.hero__title-main {
  font-family: 'Orbitron', 'PingFang SC', sans-serif;
  font-size: 72px;
  font-weight: 700;
  letter-spacing: 8px;
  background: linear-gradient(135deg, #ffffff 0%, #f7c948 50%, #ff6b35 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  text-shadow: 0 0 60px rgba(255, 107, 53, 0.5);
}

.hero__title-sub {
  font-family: 'Inter', 'PingFang SC', sans-serif;
  font-size: 20px;
  font-weight: 400;
  color: #a0a0b0;
  letter-spacing: 4px;
}

.hero__description {
  font-size: 24px;
  color: #a0a0b0;
  margin: 0 0 48px;
  letter-spacing: 2px;
}

.hero__actions {
  display: flex;
  gap: 20px;
  justify-content: center;
  flex-wrap: wrap;
}

/* Features Section - 30-35% of viewport */
.features {
  position: relative;
  height: 32vh;
  min-height: 300px;
  max-height: 500px;
  padding: 60px 20px;
  background: linear-gradient(180deg, #0a0a0f 0%, #12121a 100%);
  display: flex;
  flex-direction: column;
  justify-content: center;
}

.features__container {
  max-width: 1200px;
  margin: 0 auto;
  width: 100%;
}

.features__title {
  font-family: 'Orbitron', 'PingFang SC', sans-serif;
  font-size: 36px;
  font-weight: 600;
  color: #ffffff;
  text-align: center;
  margin: 0 0 16px;
}

.features__subtitle {
  font-size: 16px;
  color: #a0a0b0;
  text-align: center;
  margin: 0 0 40px;
}

.features__grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 32px;
}

/* Footer */
.footer {
  padding: 40px 20px;
  background: #12121a;
  border-top: 1px solid rgba(255, 255, 255, 0.06);
}

.footer__container {
  max-width: 1200px;
  margin: 0 auto;
  text-align: center;
}

.footer__copyright {
  margin: 0;
  font-size: 14px;
  color: #666680;
}

/* Animations */
@keyframes fadeInUp {
  from {
    opacity: 0;
    transform: translateY(30px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* Responsive Design */
@media (max-width: 1024px) {
  .features__grid {
    grid-template-columns: repeat(2, 1fr);
  }
  
  .hero {
    height: 65vh;
  }
  
  .features {
    height: 35vh;
  }
}

@media (max-width: 768px) {
  .hero {
    height: 70vh;
    min-height: 450px;
    padding: 30px 16px;
  }
  
  .hero__title-main {
    font-size: 42px;
    letter-spacing: 4px;
  }
  
  .hero__title-sub {
    font-size: 14px;
    letter-spacing: 2px;
  }
  
  .hero__description {
    font-size: 18px;
    margin-bottom: 36px;
  }
  
  .hero__actions {
    flex-direction: column;
    align-items: center;
  }
  
  .features {
    height: auto;
    min-height: auto;
    padding: 50px 16px;
  }
  
  .features__grid {
    grid-template-columns: 1fr;
    gap: 24px;
  }
  
  .features__title {
    font-size: 28px;
  }
  
  .features__subtitle {
    margin-bottom: 30px;
  }
}

@media (max-width: 480px) {
  .hero__title-main {
    font-size: 32px;
    letter-spacing: 2px;
  }
  
  .hero__description {
    font-size: 16px;
  }
}
</style>
