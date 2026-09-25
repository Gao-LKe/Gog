<script setup lang="ts">
import OrderRequestForm from '../components/payment/OrderRequestForm.vue'
import OrderRequestStatus from '../components/payment/OrderRequestStatus.vue'
import { useOrderCreation, type OrderDraft } from '../composables/useOrderCreation'

const { state, submit, pay } = useOrderCreation()

function submitOrder(draft: OrderDraft) {
  void submit(draft)
}
</script>

<template>
  <section class="feature-page">
    <div class="eyebrow">03 / PAYMENT</div>
    <h1>下单与余额支付</h1>
    <p class="lead">请求先进入 Kafka；仅在订单、库存预占已经提交到 MySQL 后，才展示真实订单和付款按钮。</p>
    <OrderRequestForm :disabled="state.status === 'processing' || state.status === 'paying'" @submit="submitOrder" />
    <OrderRequestStatus :state="state" @pay="pay" />
  </section>
</template>
