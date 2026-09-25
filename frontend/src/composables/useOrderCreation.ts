import { onUnmounted, shallowRef } from 'vue'
import { request } from '../api/client'

export type OrderDraft = {
  listingId: number
  quantity: number
}

export type OrderRequestState = {
  requestId?: string
  status: 'idle' | 'processing' | 'created' | 'rejected' | 'paying' | 'paid' | 'error'
  orderNo?: string
  expiresAt?: string
  message?: string
}

type OrderRequestResponse = {
  request_id: string
  status: 'processing' | 'created' | 'rejected'
  order_no?: string
  expires_at?: string
  reject_reason?: string
}

type PaymentResponse = {
  payment_no: string
  status: 'succeeded'
}

const pollIntervalMs = 1_000

function newIdempotencyKey() {
  return crypto.randomUUID()
}

export function useOrderCreation() {
  const state = shallowRef<OrderRequestState>({ status: 'idle' })
  let pollTimer: ReturnType<typeof setTimeout> | undefined

  function stopPolling() {
    if (pollTimer) clearTimeout(pollTimer)
    pollTimer = undefined
  }

  async function poll(requestId: string): Promise<void> {
    try {
      const result = await request<OrderRequestResponse>(`/v1/order-requests/${encodeURIComponent(requestId)}`)
      state.value = {
        requestId: result.request_id,
        status: result.status,
        orderNo: result.order_no,
        expiresAt: result.expires_at,
        message: result.reject_reason,
      }
      if (result.status === 'processing') {
        pollTimer = setTimeout(() => { void poll(requestId) }, pollIntervalMs)
      }
    } catch {
      state.value = { requestId, status: 'error', message: '订单状态暂时无法读取，请稍后重试查询。' }
    }
  }

  async function submit(draft: OrderDraft) {
    stopPolling()
    const requestId = newIdempotencyKey()
    state.value = { requestId, status: 'processing' }
    try {
      await request<OrderRequestResponse>('/v1/order-requests', {
        method: 'POST',
        headers: { 'Idempotency-Key': requestId },
        body: { items: [{ listing_id: draft.listingId, quantity: draft.quantity }] },
      })
      await poll(requestId)
    } catch {
      state.value = { requestId, status: 'error', message: '下单请求未被接收，请使用同一请求标识重试。' }
    }
  }

  async function pay() {
    if (!state.value.orderNo) return
    const orderNo = state.value.orderNo
    state.value = { ...state.value, status: 'paying', message: undefined }
    try {
      const result = await request<PaymentResponse>('/v1/payments', {
        method: 'POST',
        headers: { 'Idempotency-Key': newIdempotencyKey() },
        body: { order_no: orderNo },
      })
      state.value = { ...state.value, status: result.status === 'succeeded' ? 'paid' : 'error' }
    } catch {
      state.value = { ...state.value, status: 'created', message: '支付未完成：请确认余额充足且订单尚未超时。' }
    }
  }

  onUnmounted(stopPolling)
  return { state, submit, pay }
}
