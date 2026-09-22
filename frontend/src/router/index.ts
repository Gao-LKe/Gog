import { createRouter, createWebHistory } from 'vue-router'
import LoginView from '@/views/LoginView.vue'
import RealtimeView from '@/views/RealtimeView.vue'
import PaymentView from '@/views/PaymentView.vue'
import SystemView from '@/views/SystemView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/login' },
    { path: '/login', component: LoginView, meta: { title: '鉴权' } },
    { path: '/realtime', component: RealtimeView, meta: { title: '通信' } },
    { path: '/payment', component: PaymentView, meta: { title: '支付' } },
    { path: '/system', component: SystemView, meta: { title: '系统防护' } },
  ],
})

router.afterEach((to) => {
  document.title = `${String(to.meta.title ?? '首页')} · 软件激活码售卖与交易平台`
})

export default router
