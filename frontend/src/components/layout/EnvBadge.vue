<template>
  <div v-if="env !== 'prod'" class="env-badge env-badge--dev" :class="[sizeClass]">
    <span class="env-badge__dot" />
    <span>DEV</span>
  </div>
  <div v-else class="env-badge env-badge--prod" :class="[sizeClass]">
    <span class="env-badge__dot" />
    <span>PROD</span>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    size?: 'default' | 'small'
  }>(),
  { size: 'default' }
)

/* Environment is set at build time via VITE_APP_ENV.
 * Default when unset is 'production' — the production dashboard
 * always shows the PROD badge so operators know which instance
 * they're interacting with.
 *
 * Accepted values:
 *   production / prod  → gray PROD badge
 *   development / dev  → amber DEV badge
 *   anything else       → amber DEV badge (dev default)
 */
const env = computed(() => {
    const raw = import.meta.env.VITE_APP_ENV || 'production'
    // Normalize: 'production', 'prod' both map to 'prod'
    if (raw === 'production' || raw === 'prod') {
        return 'prod'
    }
    return 'dev'
})

const sizeClass = computed(() => `env-badge--${props.size}`)
</script>

<style scoped lang="scss">
.env-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  white-space: nowrap;
  letter-spacing: 0.03em;
  line-height: 1;
  border-width: 1px;
  border-style: solid;
}

.env-badge__dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  flex-shrink: 0;
}

/* Dev — amber / caution */
.env-badge--dev {
  color: #fcd34d;
  border-color: rgba(252, 211, 77, 0.3);
  background: rgba(252, 211, 77, 0.1);
}

.env-badge--dev .env-badge__dot {
  background: #fbbf24;
  box-shadow: 0 0 0 3px rgba(251, 191, 36, 0.2);
}

/* Prod — subtle / low noise */
.env-badge--prod {
  color: #8a8f98;
  border-color: rgba(255, 255, 255, 0.08);
  background: rgba(255, 255, 255, 0.03);
}

.env-badge--prod .env-badge__dot {
  background: #6b7280;
  box-shadow: 0 0 0 3px rgba(107, 114, 128, 0.12);
}

/* Small variant (used in header action bar) */
.env-badge--small {
  padding: 3px 8px;
  font-size: 11px;
  gap: 5px;
}

.env-badge--small .env-badge__dot {
  width: 6px;
  height: 6px;
}
</style>
