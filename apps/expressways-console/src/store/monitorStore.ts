import { create } from 'zustand'
import {
  consumeTopic,
  executeAdvancedControl as executeAdvancedControlApi,
  listConfigAuditEntries as listConfigAuditEntriesApi,
  fetchConfigSnapshot,
  fetchSnapshot,
  listConfigBackups as listConfigBackupsApi,
  runConfigServiceAction as runConfigServiceActionApi,
  runOperatorAction as runOperatorActionApi,
  restartConfigServices as restartConfigServicesApi,
  rollbackConfigComponent as rollbackConfigComponentApi,
  startRegistryStream,
  stopRegistryStream,
  updateConfigSection as updateConfigSectionApi,
  updateConfigComponent as updateConfigComponentApi,
} from '../api'
import type {
  AdvancedControlExecuteResult,
  ConfigAuditEntriesResult,
  ConfigAuditEntryView,
  ConfigBackupEntry,
  ConfigBackupsResult,
  ConfigComponentRollbackResult,
  ConfigComponentUpdateResult,
  ConfigConsoleSnapshot,
  ConfigRestartServicesResult,
  ConsoleSettings,
  MetricHistoryPoint,
  MonitorSnapshot,
  OperatorAction,
  OperatorActionResult,
  RegistryStreamEventPayload,
  ServiceControlAction,
  ServiceControlResult,
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
  advancedControlRunning: boolean
  advancedControlError: string | null
  advancedControlHistory: AdvancedControlExecuteResult[]
  configSnapshot: ConfigConsoleSnapshot | null
  configLoading: boolean
  configError: string | null
  configAuditEntries: ConfigAuditEntryView[]
  configAuditLoading: boolean
  configSavingComponentId: string | null
  configBackupLoadingComponentId: string | null
  configRollbackComponentId: string | null
  configRestartingServiceIds: string[]
  serviceActionRunningKey: string | null
  serviceControlHistory: ServiceControlResult[]
  operatorActionRunning: OperatorAction | null
  operatorActionHistory: OperatorActionResult[]
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
  runAdvancedControl: (
    command: unknown,
    attachmentBase64: string | null,
    guardAcknowledged: boolean,
    guardReason: string | null,
  ) => Promise<AdvancedControlExecuteResult | null>
  clearAdvancedControlHistory: () => void
  refreshConfig: () => Promise<void>
  loadConfigAuditEntries: (limit?: number) => Promise<ConfigAuditEntriesResult | null>
  saveConfigComponent: (
    componentId: string,
    content: string,
  ) => Promise<ConfigComponentUpdateResult | null>
  saveConfigSection: (
    componentId: string,
    sectionKey: string,
    fieldValues: Record<string, unknown>,
  ) => Promise<ConfigComponentUpdateResult | null>
  loadConfigBackups: (componentId: string) => Promise<ConfigBackupsResult | null>
  rollbackConfigComponent: (
    componentId: string,
    backupPath: string,
  ) => Promise<ConfigComponentRollbackResult | null>
  restartConfigServices: (serviceIds: string[]) => Promise<ConfigRestartServicesResult | null>
  runServiceAction: (
    serviceId: string,
    action: ServiceControlAction,
  ) => Promise<ServiceControlResult | null>
  runOperatorAction: (action: OperatorAction) => Promise<OperatorActionResult | null>
  clearServiceControlHistory: () => void
  clearOperatorActionHistory: () => void
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
  advancedControlRunning: false,
  advancedControlError: null,
  advancedControlHistory: [],
  configSnapshot: null,
  configLoading: false,
  configError: null,
  configAuditEntries: [],
  configAuditLoading: false,
  configSavingComponentId: null,
  configBackupLoadingComponentId: null,
  configRollbackComponentId: null,
  configRestartingServiceIds: [],
  serviceActionRunningKey: null,
  serviceControlHistory: [],
  operatorActionRunning: null,
  operatorActionHistory: [],
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

  runAdvancedControl: async (command, attachmentBase64, guardAcknowledged, guardReason) => {
    const { settings } = get()
    set({ advancedControlRunning: true, advancedControlError: null })
    try {
      const result = await executeAdvancedControlApi(
        settings,
        command,
        attachmentBase64,
        guardAcknowledged,
        guardReason,
      )
      set((state) => ({
        advancedControlRunning: false,
        advancedControlHistory: [result, ...state.advancedControlHistory].slice(0, 25),
      }))
      void get().loadConfigAuditEntries()
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ advancedControlRunning: false, advancedControlError: detail })
      void get().loadConfigAuditEntries()
      return null
    }
  },

  clearAdvancedControlHistory: () => {
    set({ advancedControlHistory: [], advancedControlError: null })
  },

  refreshConfig: async () => {
    set({ configLoading: true, configError: null })
    try {
      const [configSnapshot, auditEntries] = await Promise.all([
        fetchConfigSnapshot(),
        listConfigAuditEntriesApi(200),
      ])
      set({
        configSnapshot,
        configAuditEntries: auditEntries.entries,
        configLoading: false,
        configAuditLoading: false,
      })
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configLoading: false, configError: detail })
    }
  },

  loadConfigAuditEntries: async (limit = 200) => {
    set({ configAuditLoading: true, configError: null })
    try {
      const result = await listConfigAuditEntriesApi(limit)
      set({
        configAuditEntries: result.entries,
        configAuditLoading: false,
      })
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configAuditLoading: false, configError: detail })
      return null
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
      void get().loadConfigAuditEntries()
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configSavingComponentId: null, configError: detail })
      return null
    }
  },

  saveConfigSection: async (componentId, sectionKey, fieldValues) => {
    set({ configSavingComponentId: componentId, configError: null })
    try {
      const result = await updateConfigSectionApi(componentId, sectionKey, fieldValues)
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
      void get().loadConfigAuditEntries()
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
      void get().loadConfigAuditEntries()
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
      void get().loadConfigAuditEntries()
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ configRestartingServiceIds: [], configError: detail })
      return null
    }
  },

  runServiceAction: async (serviceId, action) => {
    const normalizedServiceId = serviceId.trim()
    if (!normalizedServiceId) {
      return null
    }

    const runningKey = `${normalizedServiceId}:${action}`
    set({ serviceActionRunningKey: runningKey, configError: null })
    try {
      const result = await runConfigServiceActionApi(normalizedServiceId, action)
      set((state) => ({
        serviceActionRunningKey: null,
        serviceControlHistory: [result, ...state.serviceControlHistory].slice(0, 50),
      }))
      void get().loadConfigAuditEntries()
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ serviceActionRunningKey: null, configError: detail })
      return null
    }
  },

  runOperatorAction: async (action) => {
    set({ operatorActionRunning: action, configError: null })
    try {
      const result = await runOperatorActionApi(action)
      set((state) => ({
        operatorActionRunning: null,
        operatorActionHistory: [result, ...state.operatorActionHistory].slice(0, 50),
      }))
      void get().loadConfigAuditEntries()
      return result
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      set({ operatorActionRunning: null, configError: detail })
      return null
    }
  },

  clearServiceControlHistory: () => {
    set({ serviceControlHistory: [] })
  },

  clearOperatorActionHistory: () => {
    set({ operatorActionHistory: [] })
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
