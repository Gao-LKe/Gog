export type ApiRequestOptions = Omit<RequestInit, 'body'> & {
  body?: BodyInit | Record<string, unknown> | null
}

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? '/api'

export async function request<T>(path: string, options: ApiRequestOptions = {}): Promise<T> {
  const headers = new Headers(options.headers)
  headers.set('Accept', 'application/json')

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
