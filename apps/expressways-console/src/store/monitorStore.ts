import { create } from 'zustand'
import {
  consumeTopic,
  fetchConfigSnapshot,
  fetchSnapshot,
  listConfigBackups as listConfigBackupsApi,
  restartConfigServices as restartConfigServicesApi,
  rollbackConfigComponent as rollbackConfigComponentApi,
  startRegistryStream,
  stopRegistryStream,
  updateConfigComponent as updateConfigComponentApi,
} from '../api'
import type {
  ConfigBackupEntry,
  ConfigBackupsResult,
  ConfigComponentRollbackResult,
  ConfigComponentUpdateResult,
  ConfigConsoleSnapshot,
  ConfigRestartServicesResult,
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
  configSnapshot: ConfigConsoleSnapshot | null
  configLoading: boolean
  configError: string | null
  configSavingComponentId: string | null
  configBackupLoadingComponentId: string | null
  configRollbackComponentId: string | null
  configRestartingServiceIds: string[]
  configBackupsByComponent: Record<string, ConfigBackupEntry[]>
  setDraftSettings: (patch: Partial<ConsoleSettings>) => void
  saveSettings: () => void
  setAutoRefresh: (enabled: boolean) => void
  setPollIntervalMs: (value: number) => void
  startStream: () => Promise<void>
  stopStream: () => Promise<void>
  applyStreamEvent: (payload: RegistryStreamEventPayload) => void
  clearRegistryEvents: () => void
  consumeTopic: (topic: string, offset: number, limit: number) => Promise<void>
  refreshConfig: () => Promise<void>
  saveConfigComponent: (
    componentId: string,
    content: string,
  ) => Promise<ConfigComponentUpdateResult | null>
  loadConfigBackups: (componentId: string) => Promise<ConfigBackupsResult | null>
  rollbackConfigComponent: (
    componentId: string,
    backupPath: string,
  ) => Promise<ConfigComponentRollbackResult | null>
  restartConfigServices: (serviceIds: string[]) => Promise<ConfigRestartServicesResult | null>
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
  configSnapshot: null,
  configLoading: false,
  configError: null,
  configSavingComponentId: null,
  configBackupLoadingComponentId: null,
  configRollbackComponentId: null,
  configRestartingServiceIds: [],
  configBackupsByComponent: {},

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

  refreshConfig: async () => {
    set({ configLoading: true, configError: null })
    try {
      const configSnapshot = await fetchConfigSnapshot()
      set({ configSnapshot, configLoading: false })
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configLoading: false, configError: detail })
    }
  },

  saveConfigComponent: async (componentId, content) => {
    set({ configSavingComponentId: componentId, configError: null })
    try {
      const result = await updateConfigComponentApi(componentId, content)
      set((state) => {
        if (!state.configSnapshot) {
          return {
            configSavingComponentId: null,
            configSnapshot: {
              rootPath: '',
              components: [result.component],
            },
          }
        }

        const nextComponents = state.configSnapshot.components.map((component) =>
          component.id === result.component.id ? result.component : component,
        )
        const hasComponent = nextComponents.some((component) => component.id === result.component.id)
        const components = hasComponent ? nextComponents : [...nextComponents, result.component]
        return {
          configSavingComponentId: null,
          configSnapshot: {
            ...state.configSnapshot,
            components,
          },
        }
      })
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configSavingComponentId: null, configError: detail })
      return null
    }
  },

  loadConfigBackups: async (componentId) => {
    set({ configBackupLoadingComponentId: componentId, configError: null })
    try {
      const result = await listConfigBackupsApi(componentId, 100)
      set((state) => ({
        configBackupLoadingComponentId: null,
        configBackupsByComponent: {
          ...state.configBackupsByComponent,
          [componentId]: result.backups,
        },
      }))
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configBackupLoadingComponentId: null, configError: detail })
      return null
    }
  },

  rollbackConfigComponent: async (componentId, backupPath) => {
    set({ configRollbackComponentId: componentId, configError: null })
    try {
      const result = await rollbackConfigComponentApi(componentId, backupPath)
      set((state) => {
        const snapshot = state.configSnapshot
        if (!snapshot) {
          return {
            configRollbackComponentId: null,
            configSnapshot: {
              rootPath: '',
              components: [result.component],
            },
          }
        }

        const updatedComponents = snapshot.components.map((component) =>
          component.id === result.component.id ? result.component : component,
        )
        return {
          configRollbackComponentId: null,
          configSnapshot: {
            ...snapshot,
            components: updatedComponents,
          },
        }
      })
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configRollbackComponentId: null, configError: detail })
      return null
    }
  },

  restartConfigServices: async (serviceIds) => {
    const normalized = Array.from(new Set(serviceIds.map((serviceId) => serviceId.trim()).filter(Boolean)))
    if (normalized.length === 0) {
      return {
        restartedAtMs: Date.now(),
        outcomes: [],
      }
    }

    set({ configRestartingServiceIds: normalized, configError: null })
    try {
      const result = await restartConfigServicesApi(normalized)
      set({ configRestartingServiceIds: [] })
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configRestartingServiceIds: [], configError: detail })
      return null
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
