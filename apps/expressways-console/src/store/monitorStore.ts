import { create } from 'zustand'
import {
  consumeTopic,
  fetchSnapshot,
  startRegistryStream,
  stopRegistryStream,
} from '../api'
import type {
  ConsoleSettings,
  MetricHistoryPoint,
  MonitorSnapshot,
  RegistryStreamEventPayload,
  StoredMessageView,
} from '../types'

type StreamStatus = 'stopped' | 'starting' | 'running' | 'error'

interface MonitorState {
  draftSettings: ConsoleSettings
  settings: ConsoleSettings
  snapshot: MonitorSnapshot | null
  metricHistory: MetricHistoryPoint[]
  loading: boolean
  error: string | null
  autoRefresh: boolean
  pollIntervalMs: number
  streamStatus: StreamStatus
  streamCursor: number | null
  streamMessage: string | null
  registryEvents: RegistryStreamEventPayload['events']
  topicMessages: StoredMessageView[]
  topicNextOffset: number
  topicLoading: boolean
  setDraftSettings: (patch: Partial<ConsoleSettings>) => void
  saveSettings: () => void
  setAutoRefresh: (enabled: boolean) => void
  setPollIntervalMs: (value: number) => void
  startStream: () => Promise<void>
  stopStream: () => Promise<void>
  applyStreamEvent: (payload: RegistryStreamEventPayload) => void
  clearRegistryEvents: () => void
  consumeTopic: (topic: string, offset: number, limit: number) => Promise<void>
  refresh: () => Promise<boolean>
}

const defaultSettings: ConsoleSettings = {
  transport: 'tcp',
  address: '127.0.0.1:7766',
  socketPath: './tmp/expressways.sock',
  token: '',
}

const SETTINGS_STORAGE_KEY = 'expressways-console-settings-v1'

function loadSavedSettings(): ConsoleSettings {
  if (typeof window === 'undefined') {
    return defaultSettings
  }

  const raw = window.localStorage.getItem(SETTINGS_STORAGE_KEY)
  if (!raw) {
    return defaultSettings
  }

  try {
    const parsed = JSON.parse(raw) as Partial<ConsoleSettings>
    return {
      transport: parsed.transport === 'unix' ? 'unix' : 'tcp',
      address: typeof parsed.address === 'string' ? parsed.address : defaultSettings.address,
      socketPath: typeof parsed.socketPath === 'string' ? parsed.socketPath : defaultSettings.socketPath,
      token: typeof parsed.token === 'string' ? parsed.token : '',
    }
  } catch {
    return defaultSettings
  }
}

function persistSettings(settings: ConsoleSettings): void {
  if (typeof window === 'undefined') {
    return
  }
  window.localStorage.setItem(SETTINGS_STORAGE_KEY, JSON.stringify(settings))
}

const initialSettings = loadSavedSettings()

export const useMonitorStore = create<MonitorState>((set, get) => ({
  draftSettings: initialSettings,
  settings: initialSettings,
  snapshot: null,
  metricHistory: [],
  loading: false,
  error: null,
  autoRefresh: true,
  pollIntervalMs: 3000,
  streamStatus: 'stopped',
  streamCursor: null,
  streamMessage: null,
  registryEvents: [],
  topicMessages: [],
  topicNextOffset: 0,
  topicLoading: false,

  setDraftSettings: (patch) => {
    set((state) => ({
      draftSettings: {
        ...state.draftSettings,
        ...patch,
      },
    }))
  },

  saveSettings: () => {
    const { draftSettings } = get()
    persistSettings(draftSettings)
    set({ settings: draftSettings })
  },

  setAutoRefresh: (enabled) => {
    set({ autoRefresh: enabled })
  },

  setPollIntervalMs: (value) => {
    const normalized = Number.isFinite(value) ? Math.max(500, Math.floor(value)) : 3000
    set({ pollIntervalMs: normalized })
  },

  startStream: async () => {
    const { settings, streamCursor } = get()
    set({ streamStatus: 'starting', streamMessage: null })
    try {
      await startRegistryStream(settings, streamCursor)
      set({ streamStatus: 'running' })
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ streamStatus: 'error', streamMessage: detail })
    }
  },

  stopStream: async () => {
    try {
      await stopRegistryStream()
      set({ streamStatus: 'stopped', streamMessage: null })
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ streamStatus: 'error', streamMessage: detail })
    }
  },

  applyStreamEvent: (payload) => {
    set((state) => {
      const nextEvents =
        payload.kind === 'events'
          ? [...payload.events, ...state.registryEvents].slice(0, 200)
          : state.registryEvents
      return {
        streamCursor: payload.cursor ?? state.streamCursor,
        streamMessage: payload.message,
        streamStatus:
          payload.kind === 'error'
            ? 'error'
            : payload.kind === 'closed'
              ? 'stopped'
              : 'running',
        registryEvents: nextEvents,
      }
    })
  },

  clearRegistryEvents: () => {
    set({ registryEvents: [] })
  },

  consumeTopic: async (topic, offset, limit) => {
    const { settings } = get()
    set({ topicLoading: true, error: null })
    try {
      const result = await consumeTopic(settings, topic, offset, limit)
      set({
        topicMessages: result.messages,
        topicNextOffset: result.next_offset,
        topicLoading: false,
      })
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ loading: false, topicLoading: false, error: detail })
    }
  },

  refresh: async () => {
    const { settings } = get()
    set({ loading: true, error: null })
    try {
      const snapshot = await fetchSnapshot(settings)
      set((state) => {
        const point: MetricHistoryPoint = {
          timestamp: Date.now(),
          totalRequests: snapshot.metrics.total_requests,
          authFailures: snapshot.metrics.auth_failures,
          publishLatencyMs: snapshot.metrics.publish.average_latency_ms,
          consumeLatencyMs: snapshot.metrics.consume.average_latency_ms,
        }
        return {
          snapshot,
          loading: false,
          streamCursor: snapshot.cursor,
          metricHistory: [...state.metricHistory, point].slice(-90),
        }
      })
      return true
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ loading: false, error: detail })
      return false
    }
  },
}))
