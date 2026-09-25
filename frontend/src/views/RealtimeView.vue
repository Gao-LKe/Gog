<script setup lang="ts">
import RealtimeConnectionPanel from '../components/realtime/RealtimeConnectionPanel.vue'
import RealtimeMessageComposer from '../components/realtime/RealtimeMessageComposer.vue'
import RealtimeMessageList from '../components/realtime/RealtimeMessageList.vue'
import { useRealtimeChat } from '../composables/useRealtimeChat'

const { connectionState, connectionLabel, status, messages, connect, send } = useRealtimeChat()
</script>

<template>
  <section class="feature-page">
    <div class="eyebrow">02 / REALTIME</div>
    <h1>实时通信</h1>
    <p class="lead">建立连接后同步最近 15 条收发消息；实时席位不足或连接关闭时，自动使用 HTTP 继续同步和发送。</p>
    <RealtimeConnectionPanel :state="connectionState" :label="connectionLabel" :status="status" @connect="connect" />
    <RealtimeMessageComposer @send="send" />
    <RealtimeMessageList :messages="messages" />
  </section>
</template>
