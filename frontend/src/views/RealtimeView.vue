<script setup lang="ts">
import { shallowRef } from 'vue'

const connectionState = shallowRef<'idle' | 'ready'>('idle')

function prepareConnection() {
  connectionState.value = 'ready'
}
</script>

<template>
  <section class="feature-page">
    <div class="eyebrow">02 / REALTIME</div>
    <h1>实时通信基础架构</h1>
    <p class="lead">预留 WebSocket 连接管理、心跳、鉴权和 Kafka 事件分发的接入点。</p>
    <div class="panel status-panel">
      <span class="status-dot" :class="{ active: connectionState === 'ready' }" />
      <div>
        <strong>{{ connectionState === 'ready' ? '连接层已准备' : '等待连接层' }}</strong>
        <p>当前仅展示基础状态，不建立真实长连接。</p>
      </div>
      <button type="button" @click="prepareConnection">准备连接</button>
    </div>
  </section>
</template>
