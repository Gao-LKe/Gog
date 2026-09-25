<script setup lang="ts">
import type { MonitoringOverview } from '../../composables/useMonitoring'
import MonitoringStatus from './MonitoringStatus.vue'

defineProps<{ data: MonitoringOverview['mysql'] }>()
const count = (value: number | null) => value === null ? '暂无数据' : value.toLocaleString('zh-CN')
const seconds = (value: number | null) => value === null ? '暂无数据' : `${value.toFixed(3)} 秒`
</script>

<template>
  <section class="monitor-panel" aria-labelledby="mysql-heading">
    <div class="panel-heading"><div><p class="section-kicker">MYSQL</p><h2 id="mysql-heading">数据库连接池</h2></div><MonitoringStatus :state="data.state" /></div>
    <div v-if="['ok', 'warning', 'critical'].includes(data.state)" class="metric-grid">
      <div class="metric-item"><span>已打开连接</span><strong>{{ count(data.open_connections) }}</strong></div>
      <div class="metric-item"><span>使用中连接</span><strong>{{ count(data.in_use) }}</strong></div>
      <div class="metric-item"><span>累计等待次数</span><strong>{{ count(data.wait_count) }}</strong></div>
      <div class="metric-item"><span>累计连接等待</span><strong>{{ seconds(data.wait_seconds) }}</strong></div>
    </div>
    <p v-else class="note">暂无可靠的数据库指标。</p>
    <p v-if="['ok', 'warning', 'critical'].includes(data.state)" class="note">连接池等待为进程累计值；判断是否恶化时需比较连续快照。</p>
  </section>
</template>

<style scoped>
.monitor-panel { margin-top: 1rem; padding: 1.5rem; border: 1px solid #263a49; border-radius: 1rem; background: rgba(18, 30, 41, .82); }
.panel-heading { display: flex; align-items: center; justify-content: space-between; gap: 1rem; flex-wrap: wrap; }
.section-kicker { margin: 0 0 .3rem; color: #7fe1c1; font-size: .7rem; font-weight: 700; letter-spacing: .12em; }
h2 { margin: 0; font-size: 1.3rem; }
.metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: .7rem; margin-top: 1.4rem; }
.metric-item { display: grid; gap: .5rem; padding: 1rem; border: 1px solid #2a4050; border-radius: .7rem; background: #101e29; }
.metric-item span, .note { color: #91a3b7; font-size: .78rem; }
.metric-item strong { font-size: 1.08rem; font-variant-numeric: tabular-nums; }
.note { margin: 1rem 0 0; }
</style>
