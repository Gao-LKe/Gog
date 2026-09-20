<script setup lang="ts">
import { shallowRef } from 'vue'

const paymentState = shallowRef('待接入支付服务')

function initialize() {
  paymentState.value = '支付流程边界已初始化'
}
</script>

<template>
  <section class="feature-page">
    <div class="eyebrow">03 / PAYMENT</div>
    <h1>支付基础架构</h1>
    <p class="lead">围绕支付意图、幂等入口与状态事件保留最小前端承载层。</p>
    <div class="panel status-panel">
      <div class="metric"><span class="metric-label">模块状态</span><strong>{{ paymentState }}</strong></div>
      <div class="metric"><span class="metric-label">核心依赖</span><strong>MySQL · Redis · Kafka</strong></div>
      <button type="button" @click="initialize">初始化支付边界</button>
    </div>
  </section>
</template>
