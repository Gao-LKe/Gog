<script setup lang="ts">
import { computed, ref } from 'vue'
import { request, setSessionTokens } from '../api/client'

type LoginMode = 'email' | 'password' | 'register'

type TokenPair = {
  access_token: string
  refresh_token: string
  access_expires_at: string
}

const mode = ref<LoginMode>('email')
const email = ref('')
const code = ref('')
const displayName = ref('')
const password = ref('')
const status = ref('使用邮箱验证码登录')
const sending = ref(false)
const submitting = ref(false)
const actionLabel = computed(() => {
  if (mode.value === 'register') return '注册并登录'
  return '登录'
})
const needsCode = computed(() => mode.value !== 'password')
const deviceType = /Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent) ? 'mobile' : 'pc'

async function sendCode() {
  if (!email.value) {
    status.value = '请先填写邮箱地址'
    return
  }
  sending.value = true
  try {
    await request<void>('/v1/auth/email/send', {
      method: 'POST',
      body: { email: email.value, purpose: mode.value === 'register' ? 'register' : 'login' },
    })
    status.value = '验证码已提交发送；当前本地开发环境请查看后端服务日志'
  } catch {
    status.value = '验证码发送失败，请稍后重试'
  } finally {
    sending.value = false
  }
}

async function submit() {
  if (!email.value || (needsCode.value && !code.value) || ((mode.value === 'password' || mode.value === 'register') && !password.value)) {
    status.value = needsCode.value ? '请填写邮箱地址、验证码和密码' : '请填写邮箱地址和密码'
    return
  }
  submitting.value = true
  try {
    const path = mode.value === 'email' ? '/v1/auth/login/email' : mode.value === 'password' ? '/v1/auth/login/password' : '/v1/auth/register'
    const body = mode.value === 'email'
      ? { email: email.value, code: code.value, device_type: deviceType }
      : mode.value === 'password'
        ? { email: email.value, password: password.value, device_type: deviceType }
        : { email: email.value, code: code.value, display_name: displayName.value, password: password.value, device_type: deviceType }
    const pair = await request<TokenPair>(path, { method: 'POST', body })
    setSessionTokens(pair)
    status.value = '登录成功，访问令牌仅保留在当前页面内存中'
    code.value = ''
  } catch {
    status.value = '验证失败，请确认邮箱、验证码或密码后重试'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <section class="feature-page">
    <div class="eyebrow">01 / AUTHENTICATION</div>
    <h1>统一鉴权基础架构</h1>
    <p class="lead">邮箱验证码可用于注册与登录；注册时设置密码，之后也可使用邮箱和密码登录。</p>
    <form class="panel form-panel" @submit.prevent="submit">
      <div class="mode-switch" role="group" aria-label="登录方式">
        <button type="button" :class="{ secondary: mode !== 'email' }" @click="mode = 'email'">验证码登录</button>
        <button type="button" :class="{ secondary: mode !== 'password' }" @click="mode = 'password'">密码登录</button>
        <button type="button" :class="{ secondary: mode !== 'register' }" @click="mode = 'register'">注册</button>
      </div>
      <label v-if="mode === 'register'" for="display-name">昵称（可选）</label>
      <input v-if="mode === 'register'" id="display-name" v-model="displayName" maxlength="80" placeholder="默认使用邮箱前缀" />
      <label for="email">邮箱地址</label>
      <input id="email" v-model="email" type="email" autocomplete="email" placeholder="name@example.com" />
      <label v-if="needsCode" for="code">验证码</label>
      <div v-if="needsCode" class="code-row">
        <input id="code" v-model="code" inputmode="numeric" autocomplete="one-time-code" maxlength="6" placeholder="6 位验证码" />
        <button type="button" class="secondary" :disabled="sending" @click="sendCode">{{ sending ? '发送中' : '获取验证码' }}</button>
      </div>
      <label v-if="mode === 'password' || mode === 'register'" for="password">密码</label>
      <input v-if="mode === 'password' || mode === 'register'" id="password" v-model="password" type="password" autocomplete="current-password" minlength="8" maxlength="72" placeholder="8 至 72 个字符" />
      <button type="submit" :disabled="submitting">{{ submitting ? '处理中' : actionLabel }}</button>
      <small>{{ status }}</small>
    </form>
    <p class="provider-note">后续可在账号设置中绑定手机号，不影响邮箱登录。</p>
  </section>
</template>
