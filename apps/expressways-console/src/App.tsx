import { useEffect, useMemo, useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import { onRegistryStreamEvent } from './api'
import { useMonitorStore } from './store/monitorStore'
import type { MetricHistoryPoint, StoredMessageView } from './types'

type TabKey = 'overview' | 'registry' | 'topics'

type ToastState = {
  tone: 'success' | 'error'
  message: string
}

function App() {
  const {
    snapshot,
    draftSettings,
    settings,
    loading,
    error,
    setDraftSettings,
    saveSettings,
    refresh,
    autoRefresh,
    setAutoRefresh,
    pollIntervalMs,
    setPollIntervalMs,
    metricHistory,
    streamStatus,
    streamCursor,
    streamMessage,
    registryEvents,
    startStream,
    stopStream,
    applyStreamEvent,
    clearRegistryEvents,
    consumeTopic,
    topicMessages,
    topicNextOffset,
    topicLoading,
  } = useMonitorStore()

  const [activeTab, setActiveTab] = useState<TabKey>('overview')
  const [topicName, setTopicName] = useState('tasks')
  const [topicOffset, setTopicOffset] = useState(0)
  const [topicLimit, setTopicLimit] = useState(50)
  const [producerFilter, setProducerFilter] = useState('')
  const [classificationFilter, setClassificationFilter] = useState('all')
  const [payloadFilter, setPayloadFilter] = useState('')
  const [toast, setToast] = useState<ToastState | null>(null)
  const hasUnsavedChanges =
    draftSettings.transport !== settings.transport ||
    draftSettings.address !== settings.address ||
    draftSettings.socketPath !== settings.socketPath ||
    draftSettings.token !== settings.token

  const tokenLooksValid = draftSettings.token.trim().length > 0 && draftSettings.token.trim().split('.').length === 3

  useEffect(() => {
    void refresh()
  }, [refresh])

  useEffect(() => {
    let unlisten: (() => void) | undefined
    void onRegistryStreamEvent((payload) => {
      applyStreamEvent(payload)
    }).then((dispose) => {
      unlisten = dispose
    })

    return () => {
      if (unlisten) {
        unlisten()
      }
    }
  }, [applyStreamEvent])

  useEffect(() => {
    if (!autoRefresh) {
      return
    }

    const timer = window.setInterval(() => {
      void refresh()
    }, pollIntervalMs)

    return () => window.clearInterval(timer)
  }, [autoRefresh, pollIntervalMs, refresh])

  useEffect(() => {
    if (!toast) {
      return
    }
    const timer = window.setTimeout(() => {
      setToast(null)
    }, 2200)
    return () => window.clearTimeout(timer)
  }, [toast])

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    if (!tokenLooksValid) {
      setToast({ tone: 'error', message: 'Token format looks invalid. Expected 3 JWT sections.' })
      return
    }

    saveSettings()
    const ok = await refresh()
    if (ok) {
      setToast({ tone: 'success', message: 'Settings saved. New token is active.' })
      return
    }
    setToast({ tone: 'error', message: 'Saved, but refresh failed. Check broker and token scopes.' })
  }

  const filteredTopicMessages = useMemo(() => {
    return topicMessages.filter((message) => {
      if (producerFilter && !message.producer.toLowerCase().includes(producerFilter.toLowerCase())) {
        return false
      }
      if (classificationFilter !== 'all' && message.classification !== classificationFilter) {
        return false
      }
      if (payloadFilter && !message.payload.toLowerCase().includes(payloadFilter.toLowerCase())) {
        return false
      }
      return true
    })
  }, [classificationFilter, payloadFilter, producerFilter, topicMessages])

  return (
    <main className="mx-auto min-h-screen max-w-7xl px-4 py-6 md:px-8">
      <header className="mb-6 animate-fadeup rounded-3xl border border-white/70 bg-white/70 p-6 shadow-xl backdrop-blur">
        <p className="font-mono text-xs uppercase tracking-[0.22em] text-ink/70">Expressways Monitoring Suite</p>
        <div className="mt-3 flex flex-wrap items-end justify-between gap-4">
          <div>
            <h1 className="font-heading text-3xl font-bold text-ink md:text-4xl">Control Plane Console</h1>
            <p className="mt-2 max-w-2xl text-sm text-ink/80">
              Observe health, metrics, adopters, auth state, and discovery registry in a single local dashboard.
            </p>
          </div>
          <button
            type="button"
            onClick={() => void refresh()}
            className="rounded-xl bg-signal px-4 py-2 font-mono text-xs font-semibold uppercase tracking-[0.18em] text-white transition hover:brightness-95"
            disabled={loading}
          >
            {loading ? 'Refreshing...' : 'Refresh Now'}
          </button>
        </div>
        <div className="mt-2">
          {hasUnsavedChanges ? (
            <span className="rounded-full border border-signal/40 bg-signal/10 px-3 py-1 font-mono text-[11px] uppercase tracking-[0.12em] text-signal">
              Unsaved changes
            </span>
          ) : (
            <span className="rounded-full border border-leaf/40 bg-leaf/10 px-3 py-1 font-mono text-[11px] uppercase tracking-[0.12em] text-leaf">
              Saved
            </span>
          )}
        </div>

        {toast ? (
          <p
            className={`mt-3 inline-flex rounded-lg px-3 py-2 font-mono text-[11px] uppercase tracking-[0.12em] ${
              toast.tone === 'success'
                ? 'border border-leaf/40 bg-leaf/10 text-leaf'
                : 'border border-signal/40 bg-signal/10 text-signal'
            }`}
          >
            {toast.message}
          </p>
        ) : null}
      </header>

      <section className="mb-6 animate-fadeup rounded-3xl border border-ink/10 bg-white p-5 shadow-lg" style={{ animationDelay: '120ms' }}>
        <form onSubmit={onSubmit} className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-6">
          <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70">
            Transport
            <select
              value={draftSettings.transport}
              onChange={(event) => setDraftSettings({ transport: event.target.value as 'tcp' | 'unix' })}
              className="rounded-lg border border-ink/20 px-3 py-2 text-sm normal-case tracking-normal"
            >
              <option value="tcp">tcp</option>
              <option value="unix">unix</option>
            </select>
          </label>

          <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70 md:col-span-1 lg:col-span-2">
            TCP Address
            <input
              value={draftSettings.address}
              onChange={(event) => setDraftSettings({ address: event.target.value })}
              className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              placeholder="127.0.0.1:7766"
            />
          </label>

          <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70 md:col-span-1 lg:col-span-2">
            Unix Socket
            <input
              value={draftSettings.socketPath}
              onChange={(event) => setDraftSettings({ socketPath: event.target.value })}
              className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              placeholder="./tmp/expressways.sock"
            />
          </label>

          <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70">
            Poll Interval (ms)
            <input
              type="number"
              min={500}
              step={250}
              value={pollIntervalMs}
              onChange={(event) => setPollIntervalMs(Number(event.target.value))}
              className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
            />
          </label>

          <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70 md:col-span-2 lg:col-span-5">
            Capability Token
            <textarea
              value={draftSettings.token}
              onChange={(event) => setDraftSettings({ token: event.target.value })}
              className="h-20 rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs"
              placeholder="Paste signed capability token"
            />
          </label>

          <div className="flex items-end">
            <label className="inline-flex items-center gap-2 rounded-lg border border-ink/20 bg-paper px-3 py-2 text-xs font-medium uppercase tracking-[0.12em] text-ink/80">
              <input type="checkbox" checked={autoRefresh} onChange={(event) => setAutoRefresh(event.target.checked)} />
              Auto Refresh
            </label>
          </div>

          <div className="flex items-end md:col-span-2 lg:col-span-1">
            <button
              type="submit"
              className="w-full rounded-lg bg-ink px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
              disabled={loading || !hasUnsavedChanges}
            >
              Save
            </button>
          </div>
        </form>

        <p className="mt-2 font-mono text-[11px] uppercase tracking-[0.12em] text-ink/55">
          Using saved {settings.transport === 'tcp' ? settings.address : settings.socketPath}
        </p>
        {hasUnsavedChanges ? (
          <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.12em] text-signal/85">
            Save settings to apply updated token and connection details
          </p>
        ) : null}

        <div className="mt-4 flex flex-wrap gap-2">
          <TabButton title="Overview" active={activeTab === 'overview'} onClick={() => setActiveTab('overview')} />
          <TabButton title="Registry Stream" active={activeTab === 'registry'} onClick={() => setActiveTab('registry')} />
          <TabButton title="Topic Monitor" active={activeTab === 'topics'} onClick={() => setActiveTab('topics')} />
        </div>

        {error ? (
          <p className="mt-3 rounded-lg border border-signal/40 bg-signal/10 p-3 font-mono text-xs text-signal">{error}</p>
        ) : null}
      </section>

      {activeTab === 'overview' ? (
        <>
          <section className="mb-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
            <Panel title="Request Trend (rolling)">
              <TrendChart
                history={metricHistory}
                lines={[
                  { label: 'total requests', color: '#112A46', selector: (point) => point.totalRequests },
                  { label: 'auth failures', color: '#F05A28', selector: (point) => point.authFailures },
                ]}
              />
            </Panel>
            <Panel title="Latency Trend (ms)">
              <TrendChart
                history={metricHistory}
                lines={[
                  { label: 'publish avg', color: '#1F7A53', selector: (point) => point.publishLatencyMs },
                  { label: 'consume avg', color: '#2656B8', selector: (point) => point.consumeLatencyMs },
                ]}
              />
            </Panel>
          </section>

          <section className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
            <MetricCard label="Service Mode" value={snapshot?.metrics?.resilience.service_mode ?? 'unknown'} accent="leaf" />
            <MetricCard label="Total Requests" value={String(snapshot?.metrics?.total_requests ?? 0)} accent="ink" />
            <MetricCard label="Auth Failures" value={String(snapshot?.metrics?.auth_failures ?? 0)} accent="signal" />
            <MetricCard label="Audit Events" value={String(snapshot?.metrics?.audit.event_count ?? 0)} accent="leaf" />
          </section>

          <section className="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
            <Panel title="Health">
              <KeyValue label="Node" value={snapshot?.health?.node_name ?? '-'} />
              <KeyValue label="Status" value={snapshot?.health?.status ?? '-'} />
              <KeyValue label="Uptime (s)" value={String(snapshot?.metrics?.uptime_seconds ?? 0)} />
            </Panel>

            <Panel title="Quota and Policy Signals">
              <KeyValue label="Policy Denials" value={String(snapshot?.metrics?.policy_denials ?? 0)} />
              <KeyValue label="Quota Denials" value={String(snapshot?.metrics?.quota_denials ?? 0)} />
              <KeyValue label="Storage Failures" value={String(snapshot?.metrics?.storage_failures ?? 0)} />
              <KeyValue label="Audit Failures" value={String(snapshot?.metrics?.audit_failures ?? 0)} />
            </Panel>
          </section>

          <section className="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
            <Panel title="Adopters">
              <ul className="space-y-2">
                {(snapshot?.adopters ?? []).map((adopter) => (
                  <li key={adopter.id} className="rounded-xl border border-ink/10 bg-paper p-3 text-sm">
                    <p className="font-heading text-base text-ink">{adopter.id}</p>
                    <p className="font-mono text-xs text-ink/70">{adopter.package}</p>
                    <p className="mt-1 text-xs text-ink/80">{adopter.status}: {adopter.detail}</p>
                  </li>
                ))}
              </ul>
            </Panel>

            <Panel title="Auth Snapshot">
              <KeyValue label="Audience" value={snapshot?.auth?.audience ?? '-'} />
              <KeyValue label="Issuers" value={String(snapshot?.auth?.issuers.length ?? 0)} />
              <KeyValue label="Principals" value={String(snapshot?.auth?.principals.length ?? 0)} />
              <KeyValue label="Revoked Tokens" value={String(snapshot?.auth?.revocations.revoked_tokens.length ?? 0)} />
            </Panel>
          </section>

          <section className="mt-6 animate-fadeup rounded-3xl border border-ink/10 bg-white p-5 shadow-lg" style={{ animationDelay: '160ms' }}>
            <h2 className="mb-3 font-heading text-xl font-semibold text-ink">Discovery Registry</h2>
            <div className="overflow-x-auto">
              <table className="min-w-full border-collapse text-left text-sm">
                <thead>
                  <tr className="border-b border-ink/15 font-mono text-xs uppercase tracking-[0.14em] text-ink/60">
                    <th className="px-2 py-2">Agent</th>
                    <th className="px-2 py-2">Principal</th>
                    <th className="px-2 py-2">Version</th>
                    <th className="px-2 py-2">Expires</th>
                  </tr>
                </thead>
                <tbody>
                  {(snapshot?.agents ?? []).map((agent) => (
                    <tr key={agent.agent_id} className="border-b border-ink/10 last:border-b-0">
                      <td className="px-2 py-2 font-medium text-ink">{agent.display_name}</td>
                      <td className="px-2 py-2 font-mono text-xs text-ink/75">{agent.principal}</td>
                      <td className="px-2 py-2 text-ink/90">{agent.version}</td>
                      <td className="px-2 py-2 font-mono text-xs text-ink/75">{agent.expires_at}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}

      {activeTab === 'registry' ? (
        <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <Panel title="Live Registry Stream">
            <div className="mb-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => void startStream()}
                className="rounded-lg bg-leaf px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                disabled={hasUnsavedChanges}
              >
                Start Stream
              </button>
              <button
                type="button"
                onClick={() => void stopStream()}
                className="rounded-lg border border-ink/25 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                disabled={hasUnsavedChanges}
              >
                Stop Stream
              </button>
              <button
                type="button"
                onClick={clearRegistryEvents}
                className="rounded-lg border border-ink/25 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
              >
                Clear Feed
              </button>
            </div>
            <KeyValue label="Status" value={streamStatus} />
            <KeyValue label="Cursor" value={String(streamCursor ?? '-')} />
            <KeyValue label="Message" value={streamMessage ?? '-'} />
          </Panel>

          <Panel title="Event Feed">
            <div className="max-h-[520px] space-y-2 overflow-y-auto pr-1">
              {registryEvents.map((event) => (
                <div key={`${event.sequence}-${event.kind}-${event.card.agent_id}`} className="rounded-xl border border-ink/10 bg-paper p-3">
                  <div className="mb-1 flex items-center justify-between">
                    <p className="font-mono text-xs uppercase tracking-[0.12em] text-ink/70">{event.kind}</p>
                    <p className="font-mono text-xs text-ink/65">#{event.sequence}</p>
                  </div>
                  <p className="font-heading text-base text-ink">{event.card.display_name}</p>
                  <p className="font-mono text-xs text-ink/70">{event.card.agent_id} • {event.card.principal}</p>
                  <p className="mt-1 text-xs text-ink/70">{event.timestamp}</p>
                </div>
              ))}
            </div>
          </Panel>
        </section>
      ) : null}

      {activeTab === 'topics' ? (
        <section className="grid grid-cols-1 gap-4">
          <Panel title="Topic Monitor">
            <form
              className="grid grid-cols-1 gap-3 md:grid-cols-5"
              onSubmit={(event) => {
                event.preventDefault()
                void consumeTopic(topicName, topicOffset, topicLimit)
              }}
            >
              <input
                value={topicName}
                onChange={(event) => setTopicName(event.target.value)}
                placeholder="topic"
                className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              />
              <input
                type="number"
                min={0}
                value={topicOffset}
                onChange={(event) => setTopicOffset(Number(event.target.value))}
                className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              />
              <input
                type="number"
                min={1}
                max={500}
                value={topicLimit}
                onChange={(event) => setTopicLimit(Number(event.target.value))}
                className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              />
              <button
                type="submit"
                className="rounded-lg bg-ink px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                disabled={topicLoading || hasUnsavedChanges}
              >
                {topicLoading ? 'Loading...' : 'Consume'}
              </button>
              <button
                type="button"
                onClick={() => {
                  setTopicOffset(topicNextOffset)
                  void consumeTopic(topicName, topicNextOffset, topicLimit)
                }}
                className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                disabled={topicLoading || hasUnsavedChanges}
              >
                Next Batch
              </button>
            </form>

            {hasUnsavedChanges ? (
              <p className="font-mono text-[11px] uppercase tracking-[0.12em] text-signal/85">
                Save settings before consuming topics
              </p>
            ) : null}

            <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3">
              <input
                value={producerFilter}
                onChange={(event) => setProducerFilter(event.target.value)}
                placeholder="filter producer"
                className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              />
              <select
                value={classificationFilter}
                onChange={(event) => setClassificationFilter(event.target.value)}
                className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              >
                <option value="all">all classifications</option>
                <option value="public">public</option>
                <option value="internal">internal</option>
                <option value="confidential">confidential</option>
                <option value="restricted">restricted</option>
              </select>
              <input
                value={payloadFilter}
                onChange={(event) => setPayloadFilter(event.target.value)}
                placeholder="search payload"
                className="rounded-lg border border-ink/20 px-3 py-2 text-sm"
              />
            </div>
          </Panel>

          <Panel title={`Messages (${filteredTopicMessages.length})`}>
            <div className="space-y-3">
              {filteredTopicMessages.map((message) => (
                <MessageCard key={message.message_id} message={message} />
              ))}
            </div>
          </Panel>
        </section>
      ) : null}
    </main>
  )
}

function TabButton({ title, active, onClick }: { title: string; active: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full px-4 py-2 font-mono text-xs uppercase tracking-[0.13em] transition ${
        active ? 'bg-ink text-white' : 'border border-ink/20 text-ink/75 hover:bg-mist'
      }`}
    >
      {title}
    </button>
  )
}

function MetricCard({ label, value, accent }: { label: string; value: string; accent: 'ink' | 'leaf' | 'signal' }) {
  const accents: Record<string, string> = {
    ink: 'from-ink/20 to-ink/5',
    leaf: 'from-leaf/25 to-leaf/5',
    signal: 'from-signal/25 to-signal/5',
  }

  return (
    <article className={`animate-fadeup rounded-2xl border border-white/70 bg-gradient-to-br ${accents[accent]} p-4 shadow-md`}>
      <p className="font-mono text-xs uppercase tracking-[0.14em] text-ink/70">{label}</p>
      <p className="mt-2 font-heading text-2xl font-semibold text-ink">{value}</p>
    </article>
  )
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <article className="rounded-3xl border border-ink/10 bg-white p-5 shadow-lg">
      <h2 className="mb-4 font-heading text-xl font-semibold text-ink">{title}</h2>
      <div className="space-y-2">{children}</div>
    </article>
  )
}

function TrendChart({
  history,
  lines,
}: {
  history: MetricHistoryPoint[]
  lines: Array<{ label: string; color: string; selector: (point: MetricHistoryPoint) => number }>
}) {
  const width = 640
  const height = 180
  const points = history.slice(-40)
  const maxY = Math.max(1, ...points.flatMap((point) => lines.map((line) => line.selector(point))))

  return (
    <div>
      <svg viewBox={`0 0 ${width} ${height}`} className="h-44 w-full rounded-xl bg-paper">
        <rect x={0} y={0} width={width} height={height} fill="transparent" />
        {lines.map((line) => {
          const path = points
            .map((point, index) => {
              const x = (index / Math.max(1, points.length - 1)) * (width - 24) + 12
              const y = height - 16 - (line.selector(point) / maxY) * (height - 32)
              return `${index === 0 ? 'M' : 'L'} ${x} ${y}`
            })
            .join(' ')
          return <path key={line.label} d={path} fill="none" stroke={line.color} strokeWidth={2.5} />
        })}
      </svg>
      <div className="mt-2 flex flex-wrap gap-3 text-xs">
        {lines.map((line) => (
          <span key={line.label} className="inline-flex items-center gap-2 text-ink/80">
            <span className="h-2.5 w-2.5 rounded-full" style={{ background: line.color }} />
            {line.label}
          </span>
        ))}
      </div>
    </div>
  )
}

function KeyValue({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between rounded-xl bg-paper px-3 py-2">
      <span className="font-mono text-xs uppercase tracking-[0.12em] text-ink/70">{label}</span>
      <span className="text-sm font-medium text-ink">{value}</span>
    </div>
  )
}

function MessageCard({ message }: { message: StoredMessageView }) {
  return (
    <article className="rounded-xl border border-ink/10 bg-paper p-3">
      <div className="mb-1 flex flex-wrap items-center justify-between gap-2">
        <p className="font-mono text-xs uppercase tracking-[0.12em] text-ink/70">offset {message.offset}</p>
        <p className="rounded bg-white px-2 py-0.5 font-mono text-xs text-ink/70">{message.classification}</p>
      </div>
      <p className="font-mono text-xs text-ink/75">{message.message_id}</p>
      <p className="mt-1 text-sm text-ink">producer: {message.producer}</p>
      <p className="mt-2 whitespace-pre-wrap rounded-lg bg-white p-2 font-mono text-xs text-ink/85">{message.payload}</p>
      <p className="mt-2 text-xs text-ink/70">{message.timestamp}</p>
    </article>
  )
}

export default App
