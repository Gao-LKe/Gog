<script setup lang="ts">
import { reactive } from 'vue'
import type { OrderDraft } from '../../composables/useOrderCreation'

const emit = defineEmits<{
  submit: [draft: OrderDraft]
}>()

defineProps<{
  disabled: boolean
}>()

const draft = reactive<OrderDraft>({ listingId: 0, quantity: 1 })

function submit() {
  if (!Number.isSafeInteger(draft.listingId) || draft.listingId <= 0 || !Number.isSafeInteger(draft.quantity) || draft.quantity <= 0) return
  emit('submit', { ...draft })
}
</script>

<template>
  <form class="panel form-panel" @submit.prevent="submit">
    <label for="listing-id">商品 ID</label>
    <input id="listing-id" v-model.number="draft.listingId" type="number" min="1" :disabled="disabled" required>
    <label for="quantity">数量</label>
    <input id="quantity" v-model.number="draft.quantity" type="number" min="1" :disabled="disabled" required>
    <button type="submit" :disabled="disabled">提交订单请求</button>
  </form>
</template>

<style scoped>
.form-panel {
  margin-top: 2rem;
}
</style>
