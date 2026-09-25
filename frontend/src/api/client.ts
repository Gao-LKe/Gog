export type ApiRequestOptions = Omit<RequestInit, 'body'> & {
  body?: BodyInit | Record<string, unknown> | null
}

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? '/api'
let accessToken = ''
let refreshToken = ''
let refreshTimer: ReturnType<typeof setTimeout> | undefined

export function setAccessToken(token: string) {
  accessToken = token
}

export function getAccessToken() {
  return accessToken
}

export type SessionTokens = {
  access_token: string
  refresh_token: string
  access_expires_at: string
}

export function setSessionTokens(tokens: SessionTokens) {
  accessToken = tokens.access_token
  refreshToken = tokens.refresh_token
  if (refreshTimer) clearTimeout(refreshTimer)
  const delay = Math.max(0, new Date(tokens.access_expires_at).getTime() - Date.now() - 30_000)
  refreshTimer = setTimeout(() => { void renewAccessToken() }, delay)
}

export async function renewAccessToken(): Promise<boolean> {
  if (!refreshToken) return false
  try {
    const response = await fetch(`${apiBaseUrl}/v1/auth/refresh`, {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refreshToken }),
    })
    if (!response.ok) throw new Error('refresh rejected')
    setSessionTokens(await response.json() as SessionTokens)
    return true
  } catch {
    accessToken = ''
    refreshToken = ''
    if (refreshTimer) clearTimeout(refreshTimer)
    return false
  }
}

export async function request<T>(path: string, options: ApiRequestOptions = {}): Promise<T> {
  const headers = new Headers(options.headers)
  headers.set('Accept', 'application/json')
  if (accessToken) {
    headers.set('Authorization', `Bearer ${accessToken}`)
  }

  let body = options.body
  if (body && typeof body === 'object' && !(body instanceof FormData) && !(body instanceof Blob)) {
    headers.set('Content-Type', 'application/json')
    body = JSON.stringify(body)
  }

  const response = await fetch(`${apiBaseUrl}${path}`, { ...options, headers, body })
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status}`)
  }
  return response.status === 204 ? (undefined as T) : response.json() as Promise<T>
}
