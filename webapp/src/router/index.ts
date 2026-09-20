import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'home', component: () => import('@/views/HomeView.vue') },
    { path: '/login', name: 'login', component: () => import('@/views/LoginView.vue'), meta: { guest: true } },
    { path: '/me', name: 'me', component: () => import('@/views/MeView.vue'), meta: { requiresAuth: true } },
    {
      path: '/points-mall',
      name: 'points-mall',
      component: () => import('@/views/PointsMallView.vue'),
      meta: { requiresAuth: true },
    },
    { path: '/editor', name: 'editor', component: () => import('@/views/EditorView.vue'), meta: { requiresAuth: true } },
    { path: '/dm', name: 'dm', component: () => import('@/views/DMView.vue'), meta: { requiresAuth: true } },
    { path: '/topics', name: 'topics', component: () => import('@/views/TopicsView.vue') },
    { path: '/topic/:id/discuss', name: 'topic-discuss', component: () => import('@/views/TopicDiscussView.vue') },
    { path: '/topic/:id', name: 'topic', component: () => import('@/views/TopicView.vue') },
    { path: '/article/:id', name: 'article', component: () => import('@/views/ArticleView.vue') },
    { path: '/u/:id', name: 'user', component: () => import('@/views/UserView.vue') },
    { path: '/game-library', name: 'game-library', component: () => import('@/views/GameLibraryView.vue') },
    { path: '/game/:id', name: 'game', component: () => import('@/views/GameDetailView.vue') },
    { path: '/:pathMatch(.*)*', redirect: '/' },
  ],
})

router.beforeEach((to) => {
  const auth = useAuthStore()
  if (to.meta.requiresAuth && !auth.isLoggedIn) {
    return { path: '/login', query: { redirect: to.fullPath } }
  }
  if (to.meta.guest && auth.isLoggedIn && to.path === '/login') {
    return { path: '/' }
  }
  return true
})

export default router
