<script setup lang="ts">
import MonitoringDashboard from '../components/monitoring/MonitoringDashboard.vue'
</script>

<template>
  <section class="feature-page monitoring-page">
    <div class="eyebrow">04 / MONITORING</div>
    <h1>系统运行监测</h1>
    <p class="lead">查看服务接口、订单队列与数据库的实时状态。统计仅用于观察，不会自动调整流量或处理速度。</p>
    <MonitoringDashboard />
  </section>
</template>

<style scoped>
.monitoring-page { max-width: 100%; }
.monitoring-page h1 { max-width: none; }
.monitoring-page .lead { max-width: 760px; }
</style>
