<script setup lang="ts">
import type { MonitoringOverview, MonitoringWindow } from '../../composables/useMonitoring'
import MonitoringStatus from './MonitoringStatus.vue'

defineProps<{ data: MonitoringOverview['order']; window: MonitoringWindow }>()
const count = (value: number | null) => value === null ? '暂无数据' : value.toLocaleString('zh-CN')
const milliseconds = (value: number | null) => value === null ? '暂无数据' : `${value.toFixed(1)} ms`
const duration = (value: number | null) => value === null ? '暂无数据' : `${Math.round(value).toLocaleString('zh-CN')} 秒`
const available = (state: string) => ['ok', 'warning', 'critical'].includes(state)
</script>

<template>
  <section class="monitor-panel" aria-labelledby="order-heading">
    <div class="panel-heading"><div><p class="section-kicker">ORDER PIPELINE</p><h2 id="order-heading">订单链路</h2></div><span class="heading-note">数量统计：近 {{ window }} · 积压：当前快照</span></div>
    <div class="stage-grid">
      <section class="stage"><div class="stage-heading"><h3>输入端 · 下单到 Kafka</h3><MonitoringStatus :state="data.input.state" /></div><dl v-if="available(data.input.state)"><div><dt>提交请求</dt><dd>{{ count(data.input.submitted) }}</dd></div><div><dt>确认接收</dt><dd>{{ count(data.input.accepted) }}</dd></div><div><dt>参数拒绝</dt><dd>{{ count(data.input.invalid) }}</dd></div><div><dt>投递失败</dt><dd>{{ count(data.input.failed) }}</dd></div><div><dt>平均投递耗时</dt><dd>{{ milliseconds(data.input.avg_publish_ms) }}</dd></div></dl><p v-else class="empty">暂无可靠的输入端指标。</p></section>
      <section class="stage"><div class="stage-heading"><h3>积压端 · Kafka</h3><MonitoringStatus :state="data.queue.state" /></div><dl v-if="available(data.queue.state)"><div><dt>消费组积压</dt><dd>{{ count(data.queue.lag) }}</dd></div><div><dt>最老等待</dt><dd>{{ duration(data.queue.oldest_seconds) }}</dd></div><div><dt>超过 3 小时的请求数</dt><dd>{{ count(data.queue.overdue) }}</dd></div></dl><p v-else class="empty">暂无可靠的队列指标。</p></section>
      <section class="stage"><div class="stage-heading"><h3>输出端 · 消费者到 MySQL</h3><MonitoringStatus :state="data.output.state" /></div><dl v-if="available(data.output.state)"><div><dt>处理消息</dt><dd>{{ count(data.output.processed) }}</dd></div><div><dt>创建订单</dt><dd>{{ count(data.output.created) }}</dd></div><div><dt>业务拒绝</dt><dd>{{ count(data.output.rejected) }}</dd></div><div><dt>处理失败</dt><dd>{{ count(data.output.failed) }}</dd></div><div><dt>平均处理耗时</dt><dd>{{ milliseconds(data.output.avg_ms) }}</dd></div></dl><p v-else class="empty">暂无可靠的输出端指标。</p></section>
    </div>
    <h3 class="table-title">分区积压</h3>
    <div v-if="available(data.queue.state) && data.queue.partitions.length" class="table-scroll"><table><thead><tr><th scope="col">分区</th><th scope="col">积压消息</th><th scope="col">最老等待</th></tr></thead><tbody><tr v-for="partition in data.queue.partitions" :key="partition.partition"><th scope="row">{{ partition.partition }}</th><td>{{ count(partition.lag) }}</td><td>{{ duration(partition.oldest_seconds) }}</td></tr></tbody></table></div>
    <p v-else class="empty">暂无分区监测数据。</p>
  </section>
</template>

<style scoped>
.monitor-panel { margin-top: 1rem; padding: 1.5rem; border: 1px solid #263a49; border-radius: 1rem; background: rgba(18, 30, 41, .82); }
.panel-heading, .stage-heading { display: flex; align-items: center; justify-content: space-between; gap: .75rem; flex-wrap: wrap; }
.section-kicker { margin: 0 0 .3rem; color: #7fe1c1; font-size: .7rem; font-weight: 700; letter-spacing: .12em; }
h2 { margin: 0; font-size: 1.3rem; } h3 { margin: 0; font-size: .94rem; }
.heading-note { color: #91a3b7; font-size: .78rem; }
.stage-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(245px, 1fr)); gap: .8rem; margin-top: 1.4rem; }
.stage { min-width: 0; padding: 1rem; border: 1px solid #2a4050; border-radius: .7rem; background: #101e29; }
dl { display: grid; gap: .75rem; margin: 1.2rem 0 0; }
dl div { display: flex; justify-content: space-between; gap: 1rem; align-items: baseline; }
dt { color: #91a3b7; font-size: .78rem; } dd { margin: 0; font-size: .9rem; font-weight: 600; text-align: right; font-variant-numeric: tabular-nums; }
.table-title { margin: 1.8rem 0 .7rem; }
.table-scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; min-width: 380px; text-align: left; }
th, td { padding: .7rem .65rem; border-bottom: 1px solid #263a49; font-size: .8rem; font-variant-numeric: tabular-nums; }
thead th { color: #91a3b7; font-weight: 600; }
.empty { color: #91a3b7; font-size: .85rem; }
</style>
