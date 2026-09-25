<script setup lang="ts">
import type { ChatMessage } from '../../composables/useRealtimeChat'

defineProps<{ messages: ChatMessage[] }>()
</script>

<template>
  <div class="panel">
    <strong>最近消息</strong>
    <p v-if="!messages.length" class="provider-note">尚未收到消息。</p>
    <div v-for="message in messages" :key="message.id" class="chat-message">
      <small>#{{ message.id }} · 用户 {{ message.sender_user_id }}</small>
      <p>{{ message.content }}</p>
    </div>
  </div>
</template>
