<script setup lang="ts">
import type { ConnectionState } from '../../composables/useRealtimeChat'

defineProps<{ state: ConnectionState; label: string; status: string }>()
defineEmits<{ connect: [] }>()
</script>

<template>
  <div class="panel status-panel">
    <span class="status-dot" :class="{ active: state === 'connected' }" />
    <div><strong>{{ label }}</strong><p>{{ status }}</p></div>
    <button type="button" :disabled="state === 'connecting'" @click="$emit('connect')">建立实时连接</button>
  </div>
</template>
