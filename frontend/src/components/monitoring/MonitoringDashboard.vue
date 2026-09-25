<script setup lang="ts">
import { computed } from 'vue'
import { useMonitoring, type MonitoringWindow } from '../../composables/useMonitoring'
import MonitoringStatus from './MonitoringStatus.vue'
import HttpMonitoringPanel from './HttpMonitoringPanel.vue'
import OrderMonitoringPanel from './OrderMonitoringPanel.vue'
import MysqlMonitoringPanel from './MysqlMonitoringPanel.vue'

const { window, overview, loading, error, refresh } = useMonitoring()
const observedAt = computed(() => {
  if (!overview.value?.observed_at) return '暂无采集时间'
  const date = new Date(overview.value.observed_at)
  return Number.isNaN(date.getTime()) ? '采集时间未知' : `采集于 ${date.toLocaleString('zh-CN')}`
})
const windows: Array<{ value: MonitoringWindow; label: string }> = [
  { value: '1m', label: '近 1 分钟' },
  { value: '5m', label: '近 5 分钟' },
  { value: '1h', label: '近 1 小时' },
]
</script>

<template>
  <div class="monitoring-dashboard">
    <div class="toolbar">
      <div class="window-options" role="group" aria-label="统计时间窗口">
        <button v-for="item in windows" :key="item.value" type="button" class="window-button" :class="{ selected: window === item.value }" :aria-pressed="window === item.value" @click="window = item.value">
          {{ item.label }}
        </button>
      </div>
      <button type="button" class="secondary refresh-button" :disabled="loading" @click="refresh">{{ loading ? '刷新中…' : '立即刷新' }}</button>
    </div>

    <p v-if="error" class="notice error" role="alert">{{ error }}</p>
    <p v-else-if="loading && !overview" class="notice" role="status">正在读取监测数据…</p>
    <template v-if="overview">
      <div class="snapshot-line">
        <div class="snapshot-state"><span>系统状态</span><MonitoringStatus :state="overview.state" /></div>
        <span class="snapshot-time">{{ observedAt }} · 每 15 秒刷新</span>
      </div>
      <HttpMonitoringPanel :data="overview.http" :window="window" />
      <OrderMonitoringPanel :data="overview.order" :window="window" />
      <MysqlMonitoringPanel :data="overview.mysql" />
    </template>
  </div>
</template>

<style scoped>
.monitoring-dashboard { margin-top: 2rem; }
.toolbar, .snapshot-line, .snapshot-state { display: flex; align-items: center; gap: 1rem; }
.toolbar, .snapshot-line { justify-content: space-between; flex-wrap: wrap; }
.window-options { display: flex; gap: .4rem; flex-wrap: wrap; }
.window-button { background: #1b2c3a; color: #afc1d0; font-size: .82rem; padding: .55rem .8rem; }
.window-button.selected { background: #7fe1c1; color: #071017; }
.refresh-button { font-size: .82rem; padding: .55rem .8rem; }
.snapshot-line { margin-top: 1.35rem; padding: .9rem 1rem; border: 1px solid #263a49; border-radius: .75rem; background: #101d28; }
.snapshot-state { color: #c6d2dc; font-size: .85rem; }
.snapshot-time { color: #91a3b7; font-size: .8rem; }
.notice { padding: 1rem; margin: 1.4rem 0; border: 1px solid #344c5d; border-radius: .75rem; color: #bfd0dc; }
.notice.error { border-color: #704047; color: #ffb9bd; background: #2d1b23; }
</style>
