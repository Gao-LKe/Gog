<script setup lang="ts">
import { computed } from 'vue'
import type { MonitoringOverview, MonitoringWindow } from '../../composables/useMonitoring'

const props = defineProps<{ data: MonitoringOverview['http']; window: MonitoringWindow }>()
const count = (value: number | null) => value === null ? '暂无数据' : value.toLocaleString('zh-CN')
const milliseconds = (value: number | null) => value === null ? '暂无数据' : `${value.toFixed(1)} ms`

const chartLines = computed(() => {
  const history = props.data.history
  if (history.length < 2) return null
  const max = Math.max(1, ...history.map(point => Math.max(point.rps, point.error_rps)))
  const line = (select: (point: typeof history[number]) => number) => history.map((point, index) => {
    const x = 4 + (index / (history.length - 1)) * 592
    const y = 112 - (Math.max(0, select(point)) / max) * 104
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return { requests: line(point => point.rps), errors: line(point => point.error_rps) }
})
</script>

<template>
  <section class="monitor-panel" aria-labelledby="http-heading">
    <div class="panel-heading"><div><p class="section-kicker">HTTP</p><h2 id="http-heading">服务接口</h2></div><span class="heading-note">所选时间窗口内统计</span></div>
    <div class="metric-grid">
      <div class="metric-item"><span>收到请求</span><strong>{{ count(data.received) }}</strong></div>
      <div class="metric-item"><span>已完成</span><strong>{{ count(data.completed) }}</strong></div>
      <div class="metric-item"><span>成功 / 重定向</span><strong>{{ count(data.success) }} / {{ count(data.redirects) }}</strong></div>
      <div class="metric-item"><span>4xx / 5xx</span><strong>{{ count(data.client_errors) }} / {{ count(data.server_errors) }}</strong></div>
      <div class="metric-item"><span>其中 429 限流</span><strong>{{ count(data.rate_limited) }}</strong></div>
      <div class="metric-item"><span>请求速率</span><strong>{{ data.rps.toFixed(2) }} 次/秒</strong></div>
      <div class="metric-item"><span>平均响应</span><strong>{{ milliseconds(data.avg_ms) }}</strong></div>
      <div class="metric-item"><span>P95 / P99 响应</span><strong>{{ milliseconds(data.p95_ms) }} / {{ milliseconds(data.p99_ms) }}</strong></div>
    </div>

    <div class="chart-header"><h3>请求速率趋势</h3><span>绿色：全部 · 红色：5xx</span></div>
    <div v-if="chartLines" class="trend-chart">
      <svg viewBox="0 0 600 120" preserveAspectRatio="none" role="img" :aria-label="`近 ${window} 的请求速率和失败速率趋势`">
        <line x1="4" y1="112" x2="596" y2="112" stroke="#344c5d" />
        <polyline :points="chartLines.requests" fill="none" stroke="#7fe1c1" stroke-width="2.5" vector-effect="non-scaling-stroke" />
        <polyline :points="chartLines.errors" fill="none" stroke="#ff8f97" stroke-width="2" vector-effect="non-scaling-stroke" />
      </svg>
    </div>
    <p v-else class="empty">趋势数据不足</p>

    <h3 class="table-title">逐接口统计</h3>
    <div v-if="data.routes.length" class="table-scroll">
      <table><thead><tr><th scope="col">接口</th><th scope="col">完成</th><th scope="col">成功</th><th scope="col">4xx</th><th scope="col">5xx</th><th scope="col">平均</th><th scope="col">P95</th></tr></thead>
        <tbody><tr v-for="route in data.routes" :key="`${route.method} ${route.route}`"><th scope="row"><code>{{ route.method }} {{ route.route }}</code></th><td>{{ count(route.completed) }}</td><td>{{ count(route.success) }}</td><td>{{ count(route.client_errors) }}</td><td>{{ count(route.server_errors) }}</td><td>{{ milliseconds(route.avg_ms) }}</td><td>{{ milliseconds(route.p95_ms) }}</td></tr></tbody>
      </table>
    </div>
    <p v-else class="empty">该时间窗口内没有接口统计数据。</p>
  </section>
</template>

<style scoped>
.monitor-panel { margin-top: 1rem; padding: 1.5rem; border: 1px solid #263a49; border-radius: 1rem; background: rgba(18, 30, 41, .82); }
.panel-heading, .chart-header { display: flex; justify-content: space-between; align-items: flex-end; gap: 1rem; flex-wrap: wrap; }
.section-kicker { margin: 0 0 .3rem; color: #7fe1c1; font-size: .7rem; font-weight: 700; letter-spacing: .12em; }
h2 { margin: 0; font-size: 1.3rem; } h3 { margin: 0; font-size: .95rem; }
.heading-note, .chart-header span { color: #91a3b7; font-size: .78rem; }
.metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: .7rem; margin-top: 1.4rem; }
.metric-item { display: grid; gap: .5rem; min-width: 0; padding: 1rem; border: 1px solid #2a4050; border-radius: .7rem; background: #101e29; }
.metric-item span { color: #91a3b7; font-size: .76rem; }
.metric-item strong { font-size: 1.08rem; overflow-wrap: anywhere; font-variant-numeric: tabular-nums; }
.chart-header { margin: 1.8rem 0 .7rem; }
.trend-chart { height: 150px; padding: .8rem; border: 1px solid #263a49; border-radius: .65rem; background: #0e1a24; }
.trend-chart svg { display: block; width: 100%; height: 100%; }
.table-title { margin: 1.8rem 0 .7rem; }
.table-scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; min-width: 720px; text-align: left; }
th, td { padding: .7rem .65rem; border-bottom: 1px solid #263a49; font-size: .78rem; white-space: nowrap; font-variant-numeric: tabular-nums; }
thead th { color: #91a3b7; font-weight: 600; }
tbody th { color: #d9e5ee; font-weight: 500; }
code { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
.empty { color: #91a3b7; font-size: .85rem; }
</style>
