<script setup lang="ts">
import { shallowRef } from 'vue'

const identifier = shallowRef('')
const status = shallowRef('基础鉴权入口已就绪')

function submit() {
  status.value = identifier.value ? '请求将由后端鉴权模块处理' : '请输入账号后继续'
}
</script>

<template>
  <section class="feature-page">
    <div class="eyebrow">01 / AUTHENTICATION</div>
    <h1>统一鉴权基础架构</h1>
    <p class="lead">当前提供 JWT 校验与身份上下文，并为会话和权限策略保留接入边界。</p>
    <form class="panel form-panel" @submit.prevent="submit">
      <label for="identifier">账号</label>
      <input id="identifier" v-model="identifier" placeholder="username or email" />
      <button type="submit">进入鉴权流程</button>
      <small>{{ status }}</small>
    </form>
  </section>
</template>
