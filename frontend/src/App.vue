<script setup lang="ts">
import { RouterLink, RouterView } from 'vue-router'

const navigation = [
  { label: '鉴权', path: '/login' },
  { label: '通信', path: '/realtime' },
  { label: '支付', path: '/payment' },
  { label: '系统防护', path: '/system' },
]
</script>

<template>
  <div class="app-shell">
    <header class="app-header">
      <RouterLink class="brand" to="/login">
        <span class="brand-mark">G</span>
        <span>商城技术底座</span>
      </RouterLink>
      <nav class="nav-list" aria-label="主导航">
        <RouterLink v-for="item in navigation" :key="item.path" :to="item.path" class="nav-link">
          {{ item.label }}
        </RouterLink>
      </nav>
    </header>
    <main class="app-main">
      <RouterView />
    </main>
  </div>
</template>
