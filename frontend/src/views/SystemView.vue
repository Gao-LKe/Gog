<script setup lang="ts">
import { computed, shallowRef } from 'vue'

const protectionEnabled = shallowRef(false)
const protectionLabel = computed(() => protectionEnabled.value ? '防护策略已启用' : '等待后端策略接入')
</script>

<template>
  <section class="feature-page">
    <div class="eyebrow">04 / RESILIENCE</div>
    <h1>系统防护基础架构</h1>
    <p class="lead">以可替换边界承载限流、并发隔离和熔断，业务降级策略留待后续接入。</p>
    <div class="panel protection-grid">
      <div v-for="item in ['流量控制', '并发隔离', '故障降级']" :key="item" class="protection-item">
        <span class="status-dot" :class="{ active: protectionEnabled }" />
        <span>{{ item }}</span>
      </div>
      <button type="button" @click="protectionEnabled = !protectionEnabled">
        {{ protectionLabel }}
      </button>
    </div>
  </section>
</template>
