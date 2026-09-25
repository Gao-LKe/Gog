import { onUnmounted, shallowRef, watch } from 'vue'
import { request } from '../api/client'

export type MonitoringWindow = '1m' | '5m' | '1h'
export type HealthState = 'ok' | 'warning' | 'critical' | 'unavailable' | string

export interface HttpRouteMetric {
  method: string
  route: string
  completed: number
  success: number
  client_errors: number
  server_errors: number
  avg_ms: number | null
  p95_ms: number | null
}

export interface MonitoringOverview {
  observed_at: string
  window: string
  state: HealthState
  http: {
    received: number
    completed: number
    success: number
    redirects: number
    client_errors: number
    server_errors: number
    rate_limited: number
    rps: number
    avg_ms: number | null
    p95_ms: number | null
    p99_ms: number | null
    routes: HttpRouteMetric[]
    history: Array<{ at: string; rps: number; error_rps: number }>
  }
  order: {
    input: { state: HealthState; submitted: number; accepted: number; invalid: number; failed: number; avg_publish_ms: number | null }
    queue: { state: HealthState; lag: number | null; oldest_seconds: number | null; overdue: number | null; partitions: Array<{ partition: number; lag: number; oldest_seconds: number | null }> }
    output: { state: HealthState; processed: number; created: number; rejected: number; failed: number; avg_ms: number | null }
  }
  mysql: { state: HealthState; open_connections: number | null; in_use: number | null; wait_count: number | null; wait_seconds: number | null }
}

const refreshIntervalMs = 15_000

export function useMonitoring() {
  const window = shallowRef<MonitoringWindow>('5m')
  const overview = shallowRef<MonitoringOverview | null>(null)
  const loading = shallowRef(false)
  const error = shallowRef('')
  let timer: ReturnType<typeof setTimeout> | undefined
  let controller: AbortController | undefined

  function stopPending() {
    if (timer) clearTimeout(timer)
    controller?.abort()
    timer = undefined
  }

  async function refresh() {
    stopPending()
    const current = new AbortController()
    controller = current
    loading.value = true
    try {
      const result = await request<MonitoringOverview>(`/v1/monitoring/overview?window=${window.value}`, { signal: current.signal })
      if (current.signal.aborted) return
      overview.value = result
      error.value = ''
    } catch (cause) {
      if (current.signal.aborted) return
      overview.value = null
      const detail = cause instanceof Error ? cause.message : ''
      error.value = /\b(401|403)\b/.test(detail)
        ? '需要管理员权限才能查看系统监测。请先使用管理员账号登录。'
        : '监测数据暂时无法读取，请检查服务连接后重试。'
    } finally {
      if (controller === current && !current.signal.aborted) {
        loading.value = false
        timer = setTimeout(() => { void refresh() }, refreshIntervalMs)
      }
    }
  }

  watch(window, () => {
    overview.value = null
    void refresh()
  }, { immediate: true })
  onUnmounted(stopPending)

  return { window, overview, loading, error, refresh }
}
