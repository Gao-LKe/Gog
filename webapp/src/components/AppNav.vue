<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()

const path = computed(() => route.path)

function isActive(prefix: string) {
  if (prefix === '/') return path.value === '/'
  return path.value === prefix || path.value.startsWith(prefix + '/')
}

function logout() {
  auth.clearAuth()
  router.push('/login')
}
</script>

<template>
  <header class="topbar">
    <div class="container row">
      <RouterLink to="/" class="brand">GameBog</RouterLink>
      <nav class="nav">
        <RouterLink to="/" :class="{ 'nav-active': isActive('/') }">首页</RouterLink>
        <RouterLink to="/topics" :class="{ 'nav-active': isActive('/topics') }">话题</RouterLink>
        <RouterLink to="/game-library" :class="{ 'nav-active': isActive('/game-library') }">游戏库</RouterLink>
        <RouterLink v-if="auth.isLoggedIn" to="/me" :class="{ 'nav-active': isActive('/me') }">个人中心</RouterLink>
        <RouterLink v-if="!auth.isLoggedIn" to="/login" :class="{ 'nav-active': isActive('/login') }">登录</RouterLink>
        <button v-else type="button" class="nav-logout" @click="logout">退出</button>
      </nav>
    </div>
  </header>
</template>

<style scoped>
.brand {
  font-weight: 700;
  text-decoration: none;
  color: inherit;
}
.nav a.router-link-active.nav-active,
.nav .router-link-active {
  font-weight: 600;
}
.nav-logout {
  border: none;
  background: transparent;
  color: inherit;
  font: inherit;
  cursor: pointer;
  padding: 0;
}
.nav-logout:hover {
  opacity: 0.75;
}
</style>
