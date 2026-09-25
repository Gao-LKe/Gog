<script setup lang="ts">
import { shallowRef } from 'vue'

const emit = defineEmits<{ send: [conversationID: number, content: string] }>()
const conversationID = shallowRef(1)
const content = shallowRef('')

function submit() {
  const trimmed = content.value.trim()
  if (!trimmed || conversationID.value < 1) return
  emit('send', conversationID.value, trimmed)
  content.value = ''
}
</script>

<template>
  <form class="panel form-panel" @submit.prevent="submit">
    <label for="conversation-id">会话 ID</label>
    <input id="conversation-id" v-model.number="conversationID" type="number" min="1" />
    <label for="message-content">消息</label>
    <input id="message-content" v-model="content" maxlength="4096" placeholder="输入一条文字消息" />
    <button type="submit" :disabled="!content.trim()">发送消息</button>
  </form>
</template>
