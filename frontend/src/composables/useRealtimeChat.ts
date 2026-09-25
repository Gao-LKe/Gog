import { computed, onBeforeUnmount, readonly, ref, shallowRef } from 'vue'
import { getAccessToken, request } from '../api/client'

export type ConnectionState = 'idle' | 'connecting' | 'connected' | 'http' | 'error'

export type ChatMessage = {
  id: number
  conversation_id: number
  sender_user_id: number
  recipient_user_id: number
  client_message_id: string
  content: string
  received_at: string | null
  created_at: string
}

type MessageEnvelope = ChatMessage & { type: string }
type SnapshotEnvelope = { type: 'message.snapshot'; messages: ChatMessage[] }
type MeResponse = { id: number }
type RecentMessagesResponse = { messages: ChatMessage[] }

const pollingIntervalMs = 15_000

function websocketURL() {
  const url = new URL(`${import.meta.env.VITE_API_BASE_URL ?? '/api'}/v1/realtime/ws`, window.location.origin)
  url.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return url.toString()
}

export function useRealtimeChat() {
  const connectionState = shallowRef<ConnectionState>('idle')
  const status = shallowRef('登录后可连接测试会话 1。')
  const currentUserID = shallowRef<number>()
  const messages = ref<ChatMessage[]>([])
  const orderedMessages = computed(() => [...messages.value].sort((left, right) => {
    const timeDifference = new Date(left.created_at).getTime() - new Date(right.created_at).getTime()
    return timeDifference || left.id - right.id
  }))
  const connectionLabel = computed(() => ({ idle: '未连接', connecting: '正在连接', connected: '实时连接', http: 'HTTP 通信', error: '连接失败' })[connectionState.value])

  let socket: WebSocket | undefined
  let pollingTimer: ReturnType<typeof setInterval> | undefined

  function mergeMessages(incoming: ChatMessage[]) {
    const all = new Map(messages.value.map(message => [message.id, message]))
    for (const message of incoming) all.set(message.id, message)
    messages.value = [...all.values()]
  }

  async function acknowledge(message: ChatMessage) {
    if (message.recipient_user_id !== currentUserID.value || message.received_at) return
    if (connectionState.value === 'connected' && socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'message.ack', message_id: message.id }))
      return
    }
    try {
      const updated = await request<ChatMessage>(`/v1/realtime/messages/${message.id}/ack`, { method: 'POST' })
      mergeMessages([updated])
    } catch {
      // The next poll retries this idempotent acknowledgement.
    }
  }

  async function acceptMessages(incoming: ChatMessage[]) {
    mergeMessages(incoming)
    await Promise.all(incoming.map(acknowledge))
  }

  async function loadRecentMessages() {
    try {
      const response = await request<RecentMessagesResponse>('/v1/realtime/messages/recent')
      await acceptMessages(response.messages)
    } catch {
      status.value = '无法同步最近消息，请确认登录状态。'
    }
  }

  function stopPolling() {
    if (pollingTimer) clearInterval(pollingTimer)
    pollingTimer = undefined
  }

  function startHTTPFallback() {
    if (connectionState.value === 'http') return
    connectionState.value = 'http'
    status.value = '实时席位不可用，已切换为 HTTP 消息同步。'
    void loadRecentMessages()
    pollingTimer = setInterval(() => { void loadRecentMessages() }, pollingIntervalMs)
  }

  async function connect() {
    const token = getAccessToken()
    if (!token) { status.value = '请先在“鉴权”页面登录。'; return }
    stopPolling()
    socket?.close()
    connectionState.value = 'connecting'
    try {
      currentUserID.value = (await request<MeResponse>('/v1/auth/me')).id
    } catch {
      connectionState.value = 'error'
      status.value = '无法确认登录状态。'
      return
    }
    socket = new WebSocket(websocketURL(), ['bearer', token])
    socket.onopen = () => { connectionState.value = 'connected'; status.value = '已建立实时连接，正在同步最近 15 条消息。' }
    socket.onmessage = (event) => {
      let payload: MessageEnvelope | SnapshotEnvelope
      try { payload = JSON.parse(event.data) as MessageEnvelope | SnapshotEnvelope } catch { return }
      if (payload.type === 'message.snapshot' && 'messages' in payload) { void acceptMessages(payload.messages); return }
      if (payload.type === 'message.created') void acceptMessages([payload])
    }
    socket.onerror = () => { status.value = '实时连接失败。' }
    socket.onclose = () => { if (connectionState.value !== 'idle') startHTTPFallback() }
  }

  async function send(conversationID: number, content: string) {
    const payload = { type: 'message.send', conversation_id: conversationID, client_message_id: crypto.randomUUID(), content }
    if (connectionState.value === 'connected' && socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(payload))
      return true
    }
    try {
      const message = await request<ChatMessage>('/v1/realtime/messages', { method: 'POST', body: payload })
      mergeMessages([message])
      return true
    } catch {
      status.value = '消息发送失败，请稍后重试。'
      return false
    }
  }

  function close() {
    connectionState.value = 'idle'
    stopPolling()
    socket?.close()
    socket = undefined
  }

  onBeforeUnmount(close)
  return { connectionState: readonly(connectionState), connectionLabel, status: readonly(status), messages: orderedMessages, connect, send }
}
