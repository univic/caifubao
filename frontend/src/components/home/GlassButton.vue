<script setup lang="ts">
import { ref } from 'vue'

interface Props {
  type?: 'primary' | 'secondary'
  to?: string
  href?: string
  disabled?: boolean
  loading?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  type: 'primary',
  disabled: false,
  loading: false
})

const emit = defineEmits<{
  click: [event: MouseEvent]
}>()

const buttonRef = ref<HTMLButtonElement | null>(null)
const ripples = ref<Array<{ x: number; y: number; id: number }>>([])
let rippleId = 0

const handleClick = (event: MouseEvent) => {
  if (!props.disabled && !props.loading) {
    // Create ripple effect
    const button = buttonRef.value
    if (button) {
      const rect = button.getBoundingClientRect()
      const x = event.clientX - rect.left
      const y = event.clientY - rect.top
      const id = ++rippleId
      
      ripples.value.push({ x, y, id })
      
      // Clean up ripple after animation
      setTimeout(() => {
        ripples.value = ripples.value.filter(r => r.id !== id)
      }, 800)
    }
    
    emit('click', event)
  }
}
</script>

<template>
  <component
    :is="to ? 'router-link' : href ? 'a' : 'button'"
    :to="to"
    :href="href"
    ref="buttonRef"
    class="glass-button"
    :class="[
      `glass-button--${type}`,
      { 'glass-button--disabled': disabled, 'glass-button--loading': loading }
    ]"
    :disabled="disabled"
    @click="handleClick"
  >
    <!-- Ripple effects -->
    <span
      v-for="ripple in ripples"
      :key="ripple.id"
      class="glass-button__ripple"
      :style="{ left: ripple.x + 'px', top: ripple.y + 'px' }"
    />
    
    <!-- Shimmer effect -->
    <span class="glass-button__shimmer" />
    
    <!-- Content -->
    <span v-if="loading" class="glass-button__loader" />
    <span v-else class="glass-button__content">
      <slot />
    </span>
    
    <!-- Edge glow -->
    <span class="glass-button__edge-glow" />
  </component>
</template>

<style scoped>
.glass-button {
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 16px 40px;
  font-size: 16px;
  font-weight: 600;
  font-family: 'Inter', 'PingFang SC', sans-serif;
  text-decoration: none;
  border: none;
  border-radius: 20px;
  cursor: pointer;
  overflow: hidden;
  transition: all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
  
  /* Ultra-strong glass effect */
  background: rgba(255, 255, 255, 0.06);
  backdrop-filter: blur(28px) saturate(180%);
  -webkit-backdrop-filter: blur(28px) saturate(180%);
  
  /* 3D depth with layered shadows */
  box-shadow: 
    0 4px 6px rgba(0, 0, 0, 0.1),
    0 8px 16px rgba(0, 0, 0, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.1),
    inset 0 -1px 0 rgba(0, 0, 0, 0.1);
  
  /* Border for depth */
  background-clip: padding-box;
}

.glass-button::before {
  content: '';
  position: absolute;
  inset: 0;
  border-radius: 20px;
  padding: 1.5px;
  background: linear-gradient(
    135deg,
    rgba(255, 255, 255, 0.4) 0%,
    rgba(255, 200, 150, 0.3) 25%,
    rgba(255, 107, 53, 0.4) 50%,
    rgba(247, 201, 72, 0.3) 75%,
    rgba(255, 255, 255, 0.4) 100%
  );
  -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  -webkit-mask-composite: xor;
  mask-composite: exclude;
  opacity: 0.7;
  transition: opacity 0.4s ease;
}

/* Inner glow for glass depth */
.glass-button::after {
  content: '';
  position: absolute;
  inset: 1px;
  border-radius: 19px;
  background: linear-gradient(
    180deg,
    rgba(255, 255, 255, 0.08) 0%,
    transparent 50%,
    rgba(0, 0, 0, 0.05) 100%
  );
  pointer-events: none;
}

/* Edge glow effect */
.glass-button__edge-glow {
  position: absolute;
  inset: -2px;
  border-radius: 22px;
  background: linear-gradient(
    135deg,
    rgba(255, 107, 53, 0.6) 0%,
    rgba(247, 201, 72, 0.4) 50%,
    rgba(255, 107, 53, 0.6) 100%
  );
  opacity: 0;
  filter: blur(20px);
  z-index: -1;
  transition: opacity 0.4s ease;
  animation: edgePulse 3s ease-in-out infinite;
}

@keyframes edgePulse {
  0%, 100% {
    transform: scale(1);
    opacity: 0.3;
  }
  50% {
    transform: scale(1.05);
    opacity: 0.5;
  }
}

/* Primary variant */
.glass-button--primary {
  color: #ffffff;
  background: linear-gradient(
    135deg,
    rgba(255, 107, 53, 0.25) 0%,
    rgba(247, 201, 72, 0.18) 100%
  );
}

.glass-button--primary:hover:not(:disabled) {
  transform: scale(1.05) translateY(-2px);
  box-shadow: 
    0 12px 28px rgba(255, 107, 53, 0.35),
    0 8px 16px rgba(0, 0, 0, 0.2),
    0 0 40px rgba(255, 107, 53, 0.2),
    inset 0 1px 0 rgba(255, 255, 255, 0.15),
    inset 0 -1px 0 rgba(0, 0, 0, 0.1);
}

.glass-button--primary:hover:not(:disabled)::before {
  opacity: 1;
  background: linear-gradient(
    135deg,
    rgba(255, 255, 255, 0.6) 0%,
    rgba(255, 200, 150, 0.5) 25%,
    rgba(255, 107, 53, 0.6) 50%,
    rgba(247, 201, 72, 0.5) 75%,
    rgba(255, 255, 255, 0.6) 100%
  );
}

.glass-button--primary:hover:not(:disabled) .glass-button__edge-glow {
  opacity: 0.6;
  animation: none;
}

.glass-button--primary:active:not(:disabled) {
  transform: scale(0.98) translateY(0);
  box-shadow: 
    0 6px 14px rgba(255, 107, 53, 0.3),
    0 4px 8px rgba(0, 0, 0, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.1),
    inset 0 -1px 0 rgba(0, 0, 0, 0.1);
}

/* Secondary variant */
.glass-button--secondary {
  color: #a0a0b0;
  background: rgba(255, 255, 255, 0.04);
}

.glass-button--secondary::before {
  background: linear-gradient(
    135deg,
    rgba(160, 160, 176, 0.4) 0%,
    rgba(136, 136, 160, 0.3) 50%,
    rgba(160, 160, 176, 0.4) 100%
  );
}

.glass-button--secondary:hover:not(:disabled) {
  color: #ffffff;
  background: rgba(255, 255, 255, 0.08);
  transform: scale(1.05) translateY(-2px);
  box-shadow: 
    0 12px 28px rgba(100, 100, 120, 0.25),
    0 8px 16px rgba(0, 0, 0, 0.2),
    0 0 30px rgba(136, 136, 160, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.12),
    inset 0 -1px 0 rgba(0, 0, 0, 0.1);
}

.glass-button--secondary:hover:not(:disabled)::before {
  opacity: 1;
}

.glass-button--secondary .glass-button__edge-glow {
  background: linear-gradient(
    135deg,
    rgba(136, 136, 160, 0.5) 0%,
    rgba(100, 100, 120, 0.3) 100%
  );
}

.glass-button--secondary:hover:not(:disabled) .glass-button__edge-glow {
  opacity: 0.4;
  animation: none;
}

/* Shimmer effect */
.glass-button__shimmer {
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
  background: linear-gradient(
    90deg,
    transparent 0%,
    rgba(255, 255, 255, 0.1) 50%,
    transparent 100%
  );
  transform: skewX(-20deg);
  transition: left 0.6s ease;
  pointer-events: none;
}

.glass-button:hover:not(:disabled) .glass-button__shimmer {
  left: 100%;
  transition: left 0.8s ease;
}

/* Ripple effect */
.glass-button__ripple {
  position: absolute;
  border-radius: 50%;
  background: radial-gradient(
    circle,
    rgba(255, 255, 255, 0.4) 0%,
    rgba(255, 255, 255, 0.2) 40%,
    transparent 70%
  );
  transform: translate(-50%, -50%) scale(0);
  animation: rippleAnim 0.8s ease-out forwards;
  pointer-events: none;
}

@keyframes rippleAnim {
  0% {
    transform: translate(-50%, -50%) scale(0);
    opacity: 1;
  }
  100% {
    transform: translate(-50%, -50%) scale(4);
    opacity: 0;
  }
}

/* Disabled state */
.glass-button--disabled,
.glass-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
  transform: none !important;
}

/* Loading state */
.glass-button--loading {
  cursor: wait;
}

.glass-button__loader {
  width: 20px;
  height: 20px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-top-color: #ffffff;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.glass-button__content {
  display: flex;
  align-items: center;
  gap: 8px;
  position: relative;
  z-index: 1;
}

/* Refraction highlight */
.glass-button::part(content) {
  position: relative;
}

.glass-button::before {
  /* Rainbow edge dispersion on hover */
  background: linear-gradient(
    135deg,
    rgba(255, 0, 0, 0.3) 0%,
    rgba(255, 165, 0, 0.3) 17%,
    rgba(255, 255, 0, 0.3) 33%,
    rgba(0, 255, 0, 0.3) 50%,
    rgba(0, 0, 255, 0.3) 67%,
    rgba(128, 0, 128, 0.3) 83%,
    rgba(255, 0, 0, 0.3) 100%
  );
}

/* Mobile responsive */
@media (max-width: 768px) {
  .glass-button {
    padding: 14px 28px;
    font-size: 14px;
    border-radius: 16px;
  }
  
  .glass-button__edge-glow {
    animation-duration: 2s;
  }
}
</style>
