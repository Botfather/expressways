export type TransportKind = 'tcp' | 'unix'

export interface ConsoleSettings {
  transport: TransportKind
  address: string
  socketPath: string
  token: string
}

export interface HealthView {
  node_name: string
  status: string
}

export interface AuthStateView {
  audience: string
  issuers: Array<{ key_id: string; status: string }>
  principals: Array<{
    id: string
    kind: string
    display_name: string
    status: string
    allowed_key_ids: string[]
    quota_profile: string
  }>
  revocations: {
    revoked_tokens: string[]
    revoked_principals: string[]
    revoked_key_ids: string[]
  }
}

export interface AdopterStatusView {
  id: string
  package: string
  description: string
  enabled: boolean
  status: string
  detail: string
  capabilities: string[]
  last_run_at: string | null
}

export interface BrokerMetricsView {
  uptime_seconds: number
  total_requests: number
  health_requests: number
  admin_requests: number
  auth_failures: number
  policy_denials: number
  quota_denials: number
  storage_failures: number
  audit_failures: number
  publish: {
    requests: number
    successes: number
    failures: number
    average_latency_ms: number
    max_latency_ms: number
  }
  consume: {
    requests: number
    successes: number
    failures: number
    average_latency_ms: number
    max_latency_ms: number
  }
  storage: {
    topic_count: number
    segment_count: number
    total_bytes: number
    reclaimed_segments: number
    reclaimed_bytes: number
    recovered_segments: number
    truncated_bytes: number
  }
  audit: {
    event_count: number
    last_hash: string | null
  }
  streams: {
    open_streams: number
    opened_streams: number
    closed_streams: number
    keepalives_sent: number
    event_frames_sent: number
    events_delivered: number
    delivery_failures: number
    slow_consumer_drops: number
    idle_timeouts: number
    watch_stream: {
      requests: number
      successes: number
      failures: number
      average_latency_ms: number
      max_latency_ms: number
    }
  }
  resilience: {
    service_mode: string
    degraded_components: string[]
  }
}

export interface AgentCardView {
  agent_id: string
  principal: string
  display_name: string
  version: string
  summary: string
  skills: string[]
  subscriptions: string[]
  publications: string[]
  schemas: Array<{ name: string; version: string }>
  endpoint: { transport: string; address: string }
  classification: string
  retention_class: string
  ttl_seconds: number
  updated_at: string
  last_seen_at: string
  expires_at: string
}

export interface MonitorSnapshot {
  health: HealthView
  metrics: BrokerMetricsView
  adopters: AdopterStatusView[]
  auth: AuthStateView
  agents: AgentCardView[]
  cursor: number
}

export interface MetricHistoryPoint {
  timestamp: number
  totalRequests: number
  authFailures: number
  publishLatencyMs: number
  consumeLatencyMs: number
}

export interface RegistryEventView {
  sequence: number
  timestamp: string
  kind: 'registered' | 'heartbeated' | 'removed' | 'cleaned_up'
  card: AgentCardView
}

export interface RegistryStreamEventPayload {
  kind: 'opened' | 'events' | 'keepalive' | 'closed' | 'error'
  cursor: number | null
  events: RegistryEventView[]
  message: string | null
}

export interface StoredMessageView {
  message_id: string
  topic: string
  offset: number
  timestamp: string
  producer: string
  classification: string
  payload: string
}

export interface TopicConsumeResult {
  topic: string
  messages: StoredMessageView[]
  next_offset: number
}
