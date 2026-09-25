<script setup lang="ts">
import { computed } from 'vue'
import type { OrderRequestState } from '../../composables/useOrderCreation'

const props = defineProps<{
  state: OrderRequestState
}>()

const emit = defineEmits<{
  pay: []
}>()

const title = computed(() => {
  const labels: Record<OrderRequestState['status'], string> = {
    idle: '等待提交',
    processing: '正在创建订单',
    created: '订单已创建',
    rejected: '订单未创建',
    paying: '正在余额支付',
    paid: '支付成功',
    error: '操作未完成',
  }
  return labels[props.state.status]
})

const canPay = computed(() => props.state.status === 'created' && Boolean(props.state.orderNo))
</script>

<template>
  <section class="panel status-panel" aria-live="polite">
    <div class="metric">
      <span class="metric-label">订单状态</span>
      <strong>{{ title }}</strong>
      <small v-if="state.status === 'processing'">订单尚未入库，暂不显示订单详情或付款入口。</small>
    </div>
    <div v-if="state.orderNo" class="metric">
      <span class="metric-label">订单号</span>
      <strong>{{ state.orderNo }}</strong>
      <small v-if="state.expiresAt">请在 {{ new Date(state.expiresAt).toLocaleString() }} 前完成支付。</small>
    </div>
    <p v-if="state.message" class="status-message">{{ state.message }}</p>
    <button v-if="canPay" type="button" @click="emit('pay')">使用余额支付</button>
  </section>
</template>

<style scoped>
.status-message {
  flex-basis: 100%;
  margin: 0;
  color: #ffcca3;
}
</style>
