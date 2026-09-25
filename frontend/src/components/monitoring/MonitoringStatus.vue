<script setup lang="ts">
import { computed } from 'vue'
import type { HealthState } from '../../composables/useMonitoring'

const props = defineProps<{ state: HealthState }>()
const label = computed(() => ({ ok: '正常', warning: '需关注', critical: '严重', unavailable: '采集失败', not_connected: '未接入' })[props.state] ?? '未接入')
const tone = computed(() => ['ok', 'warning', 'critical'].includes(props.state) ? props.state : 'unavailable')
</script>

<template>
  <span class="health-badge" :class="tone">{{ label }}</span>
</template>

<style scoped>
.health-badge { display: inline-flex; align-items: center; padding: .3rem .65rem; border-radius: 999px; font-size: .75rem; font-weight: 700; white-space: nowrap; background: #263541; color: #c1d0dc; }
.health-badge.ok { background: #153b34; color: #8ce6c9; }
.health-badge.warning { background: #48391f; color: #f1cb82; }
.health-badge.critical { background: #51272c; color: #ffa2a7; }
</style>
