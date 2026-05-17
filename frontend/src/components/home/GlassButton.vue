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
    const button = buttonRef.value
    if (button) {
      const rect = button.getBoundingClientRect()
      const x = event.clientX - rect.left
      const y = event.clientY - rect.top
      const id = ++rippleId
      ripples.value.push({ x, y, id })
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
    <span
      v-for="ripple in ripples"
      :key="ripple.id"
      class="glass-button__ripple"
      :style="{ left: ripple.x + 'px', top: ripple.y + 'px' }"
    />
    <span class="glass-button__shimmer" />
    <span v-if="loading" class="glass-button__loader" />
    <span v-else class="glass-button__content">
      <slot />
    </span>
  </component>
</template>

<style scoped>
.glass-button {
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 12px 32px;
  font-size: 15px;
  font-weight: 510;
  font-family: 'Inter Variable', 'SF Pro Display', -apple-system, system-ui, sans-serif;
  font-feature-settings: "cv01", "ss03";
  text-decoration: none;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  overflow: hidden;
  transition: all 0.2s ease;
  letter-spacing: -0.165px;
}

/* Primary variant - brand indigo */
.glass-button--primary {
  color: #f7f8f8;
  background: #5e6ad2;
  border: 1px solid rgba(113, 112, 255, 0.28);
  box-shadow: 0 4px 12px rgba(94, 106, 210, 0.2);
}

.glass-button--primary:hover:not(:disabled) {
  background: #7170ff;
  box-shadow: 0 6px 20px rgba(94, 106, 210, 0.32);
}

.glass-button--primary:active:not(:disabled) {
  background: #5e6ad2;
  box-shadow: 0 2px 8px rgba(94, 106, 210, 0.15);
}

/* Secondary variant */
.glass-button--secondary {
  color: #d0d6e0;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
}

.glass-button--secondary:hover:not(:disabled) {
  color: #f7f8f8;
  background: rgba(255, 255, 255, 0.05);
  border-color: rgba(255, 255, 255, 0.12);
}

/* Disabled state */
.glass-button--disabled,
.glass-button:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

/* Loading state */
.glass-button--loading {
  cursor: wait;
}

.glass-button__loader {
  width: 20px;
  height: 20px;
  border: 2px solid rgba(255, 255, 255, 0.2);
  border-top-color: #f7f8f8;
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
    rgba(255, 255, 255, 0.06) 50%,
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
  background: rgba(255, 255, 255, 0.3);
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

@media (max-width: 768px) {
  .glass-button {
    padding: 10px 24px;
    font-size: 14px;
  }
}
</style>
