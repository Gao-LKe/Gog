<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { RouterLink } from 'vue-router'
import AppLayout from '@/layouts/AppLayout.vue'
import { api } from '@/api/http'
import { useAuthStore } from '@/stores/auth'
import { articleCoverUrl, fmtTime, pick, renderStatsHtml } from '@/utils/blog'
import '@/assets/home.css'

const auth = useAuthStore()

const leaderboard = ref<Array<Record<string, unknown>>>([])
const lbEmpty = ref('')
const latest = ref<Array<Record<string, unknown>>>([])
const latestEmpty = ref('加载中...')
const latestScope = ref<'all' | 'follow'>('all')
const latestPage = ref(1)
const latestTotal = ref(0)
const latestSize = 6

function maxLatestPage() {
  const raw = Math.max(1, Math.ceil((latestTotal.value || 0) / latestSize))
  return latestScope.value === 'all' ? Math.min(3, raw) : raw
}

async function loadLeaderboard() {
  lbEmpty.value = ''
  try {
    const resp = await api<{ article_list?: Array<Record<string, unknown>> }>(
      '/api/v1/articles/leaderboard?type=view',
    )
    leaderboard.value = resp.data?.article_list || []
    if (!leaderboard.value.length) lbEmpty.value = '暂无榜单数据'
  } catch (e) {
    leaderboard.value = []
    lbEmpty.value = '加载失败：' + (e instanceof Error ? e.message : String(e))
  }
}

async function loadLatest() {
  latestEmpty.value = '加载中...'
  if (latestScope.value === 'follow' && !auth.isLoggedIn) {
    latestScope.value = 'all'
    latestPage.value = 1
    latestEmpty.value = '登录后可查看「仅关注」'
    return
  }
  try {
    const endpoint =
      latestScope.value === 'follow'
        ? `/api/v1/articles/following_latest?page=${latestPage.value}&size=${latestSize}`
        : `/api/v1/articles/latest?page=${latestPage.value}&size=${latestSize}`
    const resp = await api<{ article_list?: Array<Record<string, unknown>>; total?: number }>(endpoint)
    latest.value = resp.data?.article_list || []
    let total = Number(resp.data?.total || 0)
    if (latestScope.value === 'all' && total > 18) total = 18
    latestTotal.value = total
    latestEmpty.value = latest.value.length ? '' : '暂无文章'
    if (latestPage.value > maxLatestPage()) latestPage.value = maxLatestPage()
  } catch (e) {
    latest.value = []
    latestEmpty.value = '加载失败：' + (e instanceof Error ? e.message : String(e))
  }
}

onMounted(() => {
  loadLeaderboard()
  loadLatest()
})
</script>

<template>
  <AppLayout>
    <div class="container page-home">
      <section class="home-hero">
        <div class="home-hero-inner">
          <p class="home-hero-kicker">GameBog</p>
          <h1 class="home-hero-title">游戏社区 · 文章 · 话题 · 游戏库</h1>
          <p class="home-hero-desc">发现热门内容，关注作者动态，探索社区。</p>
          <div class="home-hero-actions">
            <RouterLink to="/topics" class="home-btn home-btn-primary">浏览话题</RouterLink>
            <RouterLink to="/editor" class="home-btn">写文章</RouterLink>
          </div>
        </div>
      </section>

      <div class="home-grid">
        <section class="card home-card">
          <h2 class="home-card-title">阅读榜</h2>
          <p v-if="lbEmpty" class="muted">{{ lbEmpty }}</p>
          <ol v-else class="home-lb-list">
            <li v-for="(a, idx) in leaderboard" :key="String(a.id)" class="home-lb-row">
              <span class="home-lb-rank">{{ idx + 1 }}</span>
              <div class="home-lb-body">
                <RouterLink :to="'/article/' + a.id">{{ a.title || '文章 ' + a.id }}</RouterLink>
                <div class="home-lb-meta" v-html="renderStatsHtml(a.viewCount, a.likeCount, a.commentCount)" />
              </div>
            </li>
          </ol>
        </section>

        <section class="card home-card">
          <div class="home-card-hd row">
            <h2 class="home-card-title">最新文章</h2>
            <div class="home-toggle">
              <button type="button" :class="{ 'toggle-active': latestScope === 'all' }" @click="latestScope = 'all'; latestPage = 1; loadLatest()">全部</button>
              <button type="button" :disabled="!auth.isLoggedIn" :class="{ 'toggle-active': latestScope === 'follow' }" @click="latestScope = 'follow'; latestPage = 1; loadLatest()">仅关注</button>
            </div>
          </div>
          <p v-if="latestEmpty" class="muted">{{ latestEmpty }}</p>
          <div v-else id="latest">
            <div v-for="a in latest" :key="String(a.id)" class="home-feed-item">
              <img class="home-feed-thumb" :src="articleCoverUrl(a)" alt="" loading="lazy" />
              <div class="home-feed-main">
                <div class="home-feed-title">
                  <RouterLink :to="'/article/' + a.id">{{ a.title }}</RouterLink>
                </div>
                <div v-if="a.summary" class="home-feed-summary">{{ String(a.summary).slice(0, 200) }}</div>
                <div class="home-feed-meta" v-html="(fmtTime(a.createdAt) ? fmtTime(a.createdAt) + ' · ' : '') + renderStatsHtml(a.viewCount, a.likeCount, a.commentCount)" />
              </div>
            </div>
          </div>
          <div v-if="latestTotal > latestSize" class="me-pager" style="margin-top: 16px">
            <button type="button" :disabled="latestPage <= 1" @click="latestPage--; loadLatest()">← 上一页</button>
            <span class="muted">第 {{ latestPage }} / {{ maxLatestPage() }} 页</span>
            <button type="button" :disabled="latestPage >= maxLatestPage()" @click="latestPage++; loadLatest()">下一页 →</button>
          </div>
        </section>
      </div>
    </div>
  </AppLayout>
</template>
