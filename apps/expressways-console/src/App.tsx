import { useEffect, useMemo, useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import { onRegistryStreamEvent, provisionLocalCredentials } from './api'
import { useMonitorStore } from './store/monitorStore'
import type {
  ConfigAuditEntryView,
  CredentialProvisionResult,
  ConfigComponentView,
  ConfigConsoleSnapshot,
  ConfigFormFieldKind,
  ConfigFormFieldView,
  ConfigTableArrayView,
  ConfigRestartHint,
  MetricHistoryPoint,
  MonitorSnapshot,
  OperatorAction,
  OperatorActionResult,
  ServiceControlAction,
  ServiceControlResult,
  StoredMessageView,
} from './types'

type TabKey = 'overview' | 'registry' | 'topics' | 'config' | 'advanced'
type ConfigEditorMode = 'form' | 'raw'

type AdvancedCommandTemplate = {
  id: string
  label: string
  description: string
  command: Record<string, unknown>
  defaultAttachmentBase64?: string
}

type DiagnosticStatus = 'pass' | 'warn' | 'fail'

type DiagnosticCheck = {
  id: string
  label: string
  status: DiagnosticStatus
  detail: string
}

type DecodedCapabilityScope = {
  resource: string
  actions: string[]
}

type DecodedCapabilityClaims = {
  token_id?: string
  principal?: string
  audience?: string
  issued_at?: string
  expires_at?: string
  scopes?: DecodedCapabilityScope[]
}

type DecodedCapabilityToken = {
  key_id?: string
  claims?: DecodedCapabilityClaims
}

type PolicyRule = {
  principal: string
  resource: string
  actions: string[]
}

type TokenPrincipalPolicyDiagnostics = {
  principal: string | null
  keyId: string | null
  audience: string | null
  tokenId: string | null
  expiresAt: string | null
  checks: DiagnosticCheck[]
}

type ServiceDefinition = {
  id: string
  label: string
  description: string
}

type OperatorActionDefinition = {
  action: OperatorAction
  label: string
  description: string
}

const ADVANCED_COMMAND_TEMPLATES: AdvancedCommandTemplate[] = [
  {
    id: 'health',
    label: 'Health',
    description: 'Broker liveness and service mode.',
    command: {
      type: 'health',
    },
  },
  {
    id: 'get_metrics',
    label: 'Get Metrics',
    description: 'Full broker metrics snapshot.',
    command: {
      type: 'get_metrics',
    },
  },
  {
    id: 'list_agents',
    label: 'List Agents',
    description: 'List discovery registry entries.',
    command: {
      type: 'list_agents',
      query: {
        include_stale: true,
      },
    },
  },
  {
    id: 'watch_agents',
    label: 'Watch Agents (poll)',
    description: 'Long-poll registry events without opening a stream.',
    command: {
      type: 'watch_agents',
      query: {
        include_stale: true,
      },
      cursor: null,
      max_events: 100,
      wait_timeout_ms: 1000,
    },
  },
  {
    id: 'create_topic',
    label: 'Create Topic',
    description: 'Create a topic with retention and classification defaults.',
    command: {
      type: 'create_topic',
      topic: {
        name: 'interop.chat.requests',
        retention_class: 'operational',
        default_classification: 'internal',
      },
    },
  },
  {
    id: 'publish',
    label: 'Publish Message',
    description: 'Publish a plain text payload to any topic.',
    command: {
      type: 'publish',
      topic: 'interop.chat.requests',
      classification: 'internal',
      payload: '{"hello":"world"}',
    },
  },
  {
    id: 'consume',
    label: 'Consume Messages',
    description: 'Consume a message batch from a topic.',
    command: {
      type: 'consume',
      topic: 'interop.chat.requests',
      offset: 0,
      limit: 50,
    },
  },
  {
    id: 'put_artifact',
    label: 'Put Artifact',
    description: 'Upload attachment bytes (set attachmentBase64 below).',
    command: {
      type: 'put_artifact',
      artifact_id: null,
      content_type: 'text/plain',
      byte_length: 12,
      sha256: null,
      classification: 'internal',
      retention_class: 'operational',
    },
    defaultAttachmentBase64: 'aGVsbG8gd29ybGQK',
  },
  {
    id: 'get_artifact',
    label: 'Get Artifact',
    description: 'Fetch artifact metadata and binary attachment bytes.',
    command: {
      type: 'get_artifact',
      artifact_id: 'replace-with-artifact-id',
    },
  },
]

const SERVICE_ACTIONS: ServiceControlAction[] = ['start', 'stop', 'restart', 'status']

const GUARDED_ADVANCED_COMMAND_TYPES = new Set<string>([
  'register_agent',
  'heartbeat_agent',
  'cleanup_stale_agents',
  'remove_agent',
  'create_topic',
  'revoke_token',
  'revoke_principal',
  'revoke_key',
  'put_artifact',
  'publish',
])

const SERVICE_DEFINITIONS: ServiceDefinition[] = [
  {
    id: 'expressways-server',
    label: 'Expressways Broker',
    description: 'Primary broker daemon used by the desktop single-node workflow.',
  },
  {
    id: 'nanobot-runtime',
    label: 'Nanobot Runtime',
    description: 'Optional runtime service used for Nanobot parity and wiring validation.',
  },
]

const OPERATOR_ACTION_DEFINITIONS: OperatorActionDefinition[] = [
  {
    action: 'bootstrap_local',
    label: 'Bootstrap Local',
    description: 'Generate issuer keys and guarded admin token in local var paths.',
  },
  {
    action: 'generate_admin_token',
    label: 'Generate Admin Token',
    description: 'Re-issue admin token after principal/policy changes.',
  },
  {
    action: 'verify_first_run',
    label: 'Verify First Run',
    description: 'Run health, metrics, and baseline publish/consume checks.',
  },
  {
    action: 'export_support_bundle',
    label: 'Export Support Bundle',
    description: 'Capture config/audit/log diagnostics for rehearsal evidence.',
  },
]

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
    advancedControlRunning,
    advancedControlError,
    advancedControlHistory,
    runAdvancedControl,
    clearAdvancedControlHistory,
    configSnapshot,
    configLoading,
    configError,
    configAuditEntries,
    configAuditLoading,
    configSavingComponentId,
    configBackupLoadingComponentId,
    configRollbackComponentId,
    configRestartingServiceIds,
    serviceActionRunningKey,
    serviceControlHistory,
    operatorActionRunning,
    operatorActionHistory,
    configBackupsByComponent,
    refreshConfig,
    loadConfigAuditEntries,
    saveConfigComponent,
    saveConfigSection,
    loadConfigBackups,
    rollbackConfigComponent,
    restartConfigServices,
    runServiceAction,
    runOperatorAction,
    clearServiceControlHistory,
    clearOperatorActionHistory,
  } = useMonitorStore()

  const [activeTab, setActiveTab] = useState<TabKey>('overview')
  const [topicName, setTopicName] = useState('tasks')
  const [topicOffset, setTopicOffset] = useState(0)
  const [topicLimit, setTopicLimit] = useState(50)
  const [producerFilter, setProducerFilter] = useState('')
  const [classificationFilter, setClassificationFilter] = useState('all')
  const [payloadFilter, setPayloadFilter] = useState('')
  const [advancedTemplateId, setAdvancedTemplateId] = useState(ADVANCED_COMMAND_TEMPLATES[0]?.id ?? 'health')
  const [advancedCommandInput, setAdvancedCommandInput] = useState(() =>
    JSON.stringify(ADVANCED_COMMAND_TEMPLATES[0]?.command ?? { type: 'health' }, null, 2),
  )
  const [advancedAttachmentInput, setAdvancedAttachmentInput] = useState('')
  const [advancedGuardAcknowledged, setAdvancedGuardAcknowledged] = useState(false)
  const [advancedGuardReason, setAdvancedGuardReason] = useState('')
  const [configDrafts, setConfigDrafts] = useState<Record<string, string>>({})
  const [configEditorModes, setConfigEditorModes] = useState<Record<string, ConfigEditorMode>>({})
  const [configFormDrafts, setConfigFormDrafts] = useState<Record<string, Record<string, unknown>>>({})
  const [configTableArrayDrafts, setConfigTableArrayDrafts] = useState<
    Record<string, Record<string, Array<Record<string, unknown>>>>
  >({})
  const [visibleDiffs, setVisibleDiffs] = useState<Record<string, boolean>>({})
  const [visibleBackups, setVisibleBackups] = useState<Record<string, boolean>>({})
  const [guidedFlowRunning, setGuidedFlowRunning] = useState(false)
  const [credentialBundleRoot, setCredentialBundleRoot] = useState('')
  const [credentialProvisioning, setCredentialProvisioning] = useState(false)
  const [credentialRefreshToken, setCredentialRefreshToken] = useState(false)
  const [credentialResult, setCredentialResult] = useState<CredentialProvisionResult | null>(null)
  const [toast, setToast] = useState<ToastState | null>(null)
  const hasUnsavedChanges =
    draftSettings.transport !== settings.transport ||
    draftSettings.address !== settings.address ||
    draftSettings.socketPath !== settings.socketPath ||
    draftSettings.token !== settings.token

  const tokenTrimmed = draftSettings.token.trim()
  const tokenLooksPresent = tokenTrimmed.length > 0
  const tokenLooksCanonical = tokenTrimmed.split('.').filter(Boolean).length === 2
  const advancedCommandType = useMemo(() => {
    try {
      const parsed = JSON.parse(advancedCommandInput) as unknown
      return parseAdvancedCommandType(parsed)
    } catch {
      return null
    }
  }, [advancedCommandInput])
  const advancedCommandRequiresGuard = useMemo(
    () =>
      advancedCommandType ? isGuardedAdvancedCommandType(advancedCommandType) : false,
    [advancedCommandType],
  )

  useEffect(() => {
    void refresh()
    void refreshConfig()
  }, [refresh, refreshConfig])

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

  useEffect(() => {
    if (advancedCommandRequiresGuard) {
      return
    }
    setAdvancedGuardAcknowledged(false)
    setAdvancedGuardReason('')
  }, [advancedCommandRequiresGuard])

  useEffect(() => {
    if (!configSnapshot) {
      return
    }
    setConfigDrafts((previous) => {
      const next: Record<string, string> = {}
      for (const component of configSnapshot.components) {
        next[component.id] = previous[component.id] ?? component.content
      }
      return next
    })
    setConfigEditorModes((previous) => {
      const next: Record<string, ConfigEditorMode> = {}
      for (const component of configSnapshot.components) {
        const hasFormSections = component.sections.some(
          (section) => section.formFields.length > 0 || section.tableArrays.length > 0,
        )
        const fallback: ConfigEditorMode = hasFormSections ? 'form' : 'raw'
        next[component.id] = previous[component.id] ?? fallback
      }
      return next
    })
    setConfigFormDrafts((previous) => {
      const next: Record<string, Record<string, unknown>> = {}
      for (const component of configSnapshot.components) {
        for (const section of component.sections) {
          const draftKey = configSectionDraftKey(component.id, section.key)
          next[draftKey] = previous[draftKey] ?? createSectionFormDraft(section.formFields)
        }
      }
      return next
    })
    setConfigTableArrayDrafts((previous) => {
      const next: Record<string, Record<string, Array<Record<string, unknown>>>> = {}
      for (const component of configSnapshot.components) {
        for (const section of component.sections) {
          const draftKey = configSectionDraftKey(component.id, section.key)
          next[draftKey] =
            previous[draftKey] ?? createSectionTableArrayDraft(section.tableArrays)
        }
      }
      return next
    })
  }, [configSnapshot])

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    if (!tokenLooksPresent) {
      setToast({ tone: 'error', message: 'Capability token is required.' })
      return
    }

    saveSettings()
    const ok = await refresh()
    if (ok) {
      const suffix = tokenLooksCanonical
        ? ''
        : ' Token format is non-canonical; expected payload.signature.'
      setToast({ tone: 'success', message: `Settings saved. New token is active.${suffix}` })
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

  const configGroups = useMemo(() => {
    if (!configSnapshot) {
      return [] as Array<[string, ConfigComponentView[]]>
    }

    const grouped = new Map<string, ConfigComponentView[]>()
    for (const component of configSnapshot.components) {
      const current = grouped.get(component.group) ?? []
      current.push(component)
      grouped.set(component.group, current)
    }

    return Array.from(grouped.entries()).map(
      ([group, components]): [string, ConfigComponentView[]] => [
        group,
        [...components].sort((left, right) => left.name.localeCompare(right.name)),
      ],
    )
  }, [configSnapshot])

  const tokenDiagnostics = useMemo(
    () => buildTokenPrincipalPolicyDiagnostics(settings.token, snapshot, configSnapshot),
    [settings.token, snapshot, configSnapshot],
  )

  const latestServiceResultByService = useMemo(() => {
    const next = new Map<string, ServiceControlResult>()
    for (const result of serviceControlHistory) {
      if (!next.has(result.serviceId)) {
        next.set(result.serviceId, result)
      }
    }
    return next
  }, [serviceControlHistory])

  const latestOperatorResultByAction = useMemo(() => {
    const next = new Map<OperatorAction, OperatorActionResult>()
    for (const result of operatorActionHistory) {
      if (!next.has(result.action)) {
        next.set(result.action, result)
      }
    }
    return next
  }, [operatorActionHistory])

  const runServiceLifecycleAction = async (
    serviceId: string,
    action: ServiceControlAction,
    options?: { suppressToast?: boolean },
  ): Promise<ServiceControlResult | null> => {
    const result = await runServiceAction(serviceId, action)
    if (!result) {
      if (!options?.suppressToast) {
        setToast({
          tone: 'error',
          message: `${serviceId} ${action} failed before execution.`,
        })
      }
      return null
    }

    if (!options?.suppressToast) {
      setToast({
        tone: result.ok ? 'success' : 'error',
        message: result.ok
          ? `${serviceId} ${action} completed.`
          : `${serviceId} ${action} failed (code ${result.statusCode ?? 'n/a'}).`,
      })
    }
    return result
  }

  const runOperatorWorkflowAction = async (
    action: OperatorAction,
    options?: { suppressToast?: boolean },
  ): Promise<OperatorActionResult | null> => {
    const result = await runOperatorAction(action)
    if (!result) {
      if (!options?.suppressToast) {
        setToast({
          tone: 'error',
          message: `${formatOperatorActionLabel(action)} failed before execution.`,
        })
      }
      return null
    }

    if (!options?.suppressToast) {
      setToast({
        tone: result.ok ? 'success' : 'error',
        message: result.ok
          ? `${formatOperatorActionLabel(action)} completed.`
          : `${formatOperatorActionLabel(action)} failed (code ${result.statusCode ?? 'n/a'}).`,
      })
    }
    return result
  }

  const runGuidedFirstRunFlow = async () => {
    if (guidedFlowRunning) {
      return
    }

    setGuidedFlowRunning(true)
    try {
      const bootstrap = await runOperatorWorkflowAction('bootstrap_local', { suppressToast: true })
      if (!bootstrap?.ok) {
        setToast({
          tone: 'error',
          message: 'Guided flow stopped at bootstrap-local.',
        })
        return
      }

      const startBroker = await runServiceLifecycleAction('expressways-server', 'start', {
        suppressToast: true,
      })
      if (!startBroker?.ok) {
        setToast({
          tone: 'error',
          message: 'Guided flow stopped at broker start.',
        })
        return
      }

      const verify = await runOperatorWorkflowAction('verify_first_run', { suppressToast: true })
      if (!verify?.ok) {
        setToast({
          tone: 'error',
          message: 'Guided flow stopped at verify-first-run.',
        })
        return
      }

      const supportBundle = await runOperatorWorkflowAction('export_support_bundle', {
        suppressToast: true,
      })
      if (!supportBundle?.ok) {
        setToast({
          tone: 'error',
          message: 'Guided flow stopped at export-support-bundle.',
        })
        return
      }

      setToast({
        tone: 'success',
        message: 'Guided first-run flow completed successfully.',
      })
    } finally {
      setGuidedFlowRunning(false)
    }
  }

  const provisionPackagedCredentials = async () => {
    if (!credentialBundleRoot.trim() || credentialProvisioning) {
      return
    }
    setCredentialProvisioning(true)
    try {
      const result = await provisionLocalCredentials(
        credentialBundleRoot.trim(),
        credentialRefreshToken,
      )
      setCredentialResult(result)
      setToast({ tone: 'success', message: result.message })
    } catch (provisionError) {
      setToast({
        tone: 'error',
        message: provisionError instanceof Error ? provisionError.message : String(provisionError),
      })
    } finally {
      setCredentialProvisioning(false)
    }
  }

  const updateConfigDraft = (componentId: string, value: string) => {
    setConfigDrafts((previous) => ({
      ...previous,
      [componentId]: value,
    }))
  }

  const resetConfigDraft = (component: ConfigComponentView) => {
    setConfigDrafts((previous) => ({
      ...previous,
      [component.id]: component.content,
    }))
  }

  const saveConfigDraft = async (component: ConfigComponentView) => {
    const draft = configDrafts[component.id] ?? component.content
    const result = await saveConfigComponent(component.id, draft)
    if (!result) {
      setToast({ tone: 'error', message: `Failed to save ${component.name}. Check TOML syntax.` })
      return
    }
    const saved = result.component
    setConfigDrafts((previous) => ({
      ...previous,
      [saved.id]: saved.content,
    }))
    syncComponentFormDrafts(saved)

    const restartLabel = result.restartHints.map((hint) => hint.service).join(', ')
    const backupLabel = result.backupPath ? ` Backup: ${result.backupPath}` : ''
    const restartSuffix = restartLabel ? ` Restart recommended: ${restartLabel}.` : ''
    setToast({
      tone: 'success',
      message: `${component.name} applied.${restartSuffix}${backupLabel}`,
    })

    if (visibleBackups[component.id]) {
      void loadConfigBackups(component.id)
    }
  }

  const syncComponentFormDrafts = (component: ConfigComponentView) => {
    setConfigFormDrafts((previous) => {
      const next = { ...previous }
      for (const section of component.sections) {
        if (section.formFields.length > 0) {
          next[configSectionDraftKey(component.id, section.key)] = createSectionFormDraft(section.formFields)
        }
      }
      return next
    })
    setConfigTableArrayDrafts((previous) => {
      const next = { ...previous }
      for (const section of component.sections) {
        next[configSectionDraftKey(component.id, section.key)] = createSectionTableArrayDraft(
          section.tableArrays,
        )
      }
      return next
    })
  }

  const setConfigEditorMode = (componentId: string, mode: ConfigEditorMode) => {
    setConfigEditorModes((previous) => ({
      ...previous,
      [componentId]: mode,
    }))
  }

  const updateSectionFormField = (
    componentId: string,
    sectionKey: string,
    fieldKey: string,
    value: unknown,
  ) => {
    const draftKey = configSectionDraftKey(componentId, sectionKey)
    setConfigFormDrafts((previous) => ({
      ...previous,
      [draftKey]: {
        ...(previous[draftKey] ?? {}),
        [fieldKey]: value,
      },
    }))
  }

  const updateTableArrayEntryField = (
    componentId: string,
    sectionKey: string,
    arrayKey: string,
    entryIndex: number,
    fieldKey: string,
    value: unknown,
  ) => {
    const draftKey = configSectionDraftKey(componentId, sectionKey)
    setConfigTableArrayDrafts((previous) => {
      const sectionDraft = previous[draftKey] ?? {}
      const arrayDraft = sectionDraft[arrayKey] ?? []
      const nextArray = arrayDraft.map((entry, index) =>
        index === entryIndex ? { ...entry, [fieldKey]: value } : entry,
      )
      return {
        ...previous,
        [draftKey]: {
          ...sectionDraft,
          [arrayKey]: nextArray,
        },
      }
    })
  }

  const addTableArrayEntry = (
    componentId: string,
    sectionKey: string,
    tableArray: ConfigTableArrayView,
  ) => {
    const draftKey = configSectionDraftKey(componentId, sectionKey)
    setConfigTableArrayDrafts((previous) => {
      const sectionDraft = previous[draftKey] ?? {}
      const arrayDraft = sectionDraft[tableArray.key] ?? []
      const nextEntry = createTableArrayEntryDraft(tableArray.entryFields)
      return {
        ...previous,
        [draftKey]: {
          ...sectionDraft,
          [tableArray.key]: [...arrayDraft, nextEntry],
        },
      }
    })
  }

  const removeTableArrayEntry = (
    componentId: string,
    sectionKey: string,
    arrayKey: string,
    entryIndex: number,
  ) => {
    const draftKey = configSectionDraftKey(componentId, sectionKey)
    setConfigTableArrayDrafts((previous) => {
      const sectionDraft = previous[draftKey] ?? {}
      const arrayDraft = sectionDraft[arrayKey] ?? []
      return {
        ...previous,
        [draftKey]: {
          ...sectionDraft,
          [arrayKey]: arrayDraft.filter((_, index) => index !== entryIndex),
        },
      }
    })
  }

  const resetSectionFormDraft = (
    componentId: string,
    section: ConfigComponentView['sections'][number],
  ) => {
    const draftKey = configSectionDraftKey(componentId, section.key)
    setConfigFormDrafts((previous) => ({
      ...previous,
      [draftKey]: createSectionFormDraft(section.formFields),
    }))
    setConfigTableArrayDrafts((previous) => ({
      ...previous,
      [draftKey]: createSectionTableArrayDraft(section.tableArrays),
    }))
  }

  const saveSectionFormDraft = async (
    component: ConfigComponentView,
    section: ConfigComponentView['sections'][number],
    rawDirty: boolean,
  ) => {
    if (rawDirty) {
      setToast({
        tone: 'error',
        message: 'Raw TOML has unsaved changes. Apply or reset raw draft before saving form sections.',
      })
      return
    }

    const draftKey = configSectionDraftKey(component.id, section.key)
    const sectionDraft = configFormDrafts[draftKey] ?? createSectionFormDraft(section.formFields)
    const sectionTableArrayDraft =
      configTableArrayDrafts[draftKey] ?? createSectionTableArrayDraft(section.tableArrays)
    const normalizedValues: Record<string, unknown> = {}
    for (const field of section.formFields) {
      const rawValue = sectionDraft[field.key] ?? toSectionDraftValue(field)
      try {
        const normalizedValue = normalizeSectionFormValue(field.kind, rawValue)
        const validationError = validateSectionFieldValue(field, normalizedValue)
        if (validationError) {
          setToast({
            tone: 'error',
            message: `Invalid ${section.key}.${field.key}: ${validationError}`,
          })
          return
        }
        normalizedValues[field.key] = normalizedValue
      } catch (error) {
        setToast({
          tone: 'error',
          message: `Invalid ${section.key}.${field.key}: ${error instanceof Error ? error.message : String(error)}`,
        })
        return
      }
    }

    for (const tableArray of section.tableArrays) {
      const entryDrafts = sectionTableArrayDraft[tableArray.key] ?? []
      const schemaKeys = new Set(tableArray.entryFields.map((field) => field.key))
      const normalizedEntries: Array<Record<string, unknown>> = []
      for (let entryIndex = 0; entryIndex < entryDrafts.length; entryIndex += 1) {
        const entryDraft = entryDrafts[entryIndex] ?? {}
        const normalizedEntry: Record<string, unknown> = {}
        for (const field of tableArray.entryFields) {
          const rawValue = entryDraft[field.key] ?? toDraftFieldValue(field.kind, field.value)
          try {
            const normalizedValue = normalizeSectionFormValue(field.kind, rawValue)
            const validationError = validateSectionFieldValue(field, normalizedValue)
            if (validationError) {
              setToast({
                tone: 'error',
                message: `Invalid ${section.key}.${tableArray.key}[${entryIndex + 1}].${field.key}: ${validationError}`,
              })
              return
            }
            normalizedEntry[field.key] = normalizedValue
          } catch (error) {
            setToast({
              tone: 'error',
              message: `Invalid ${section.key}.${tableArray.key}[${entryIndex + 1}].${field.key}: ${
                error instanceof Error ? error.message : String(error)
              }`,
            })
            return
          }
        }
        for (const [entryKey, entryValue] of Object.entries(entryDraft)) {
          if (!schemaKeys.has(entryKey)) {
            normalizedEntry[entryKey] = entryValue
          }
        }
        normalizedEntries.push(normalizedEntry)
      }
      normalizedValues[tableArray.key] = normalizedEntries
    }

    const result = await saveConfigSection(component.id, section.key, normalizedValues)
    if (!result) {
      setToast({ tone: 'error', message: `Failed to save section ${section.key} for ${component.name}.` })
      return
    }

    const saved = result.component
    setConfigDrafts((previous) => ({
      ...previous,
      [saved.id]: saved.content,
    }))
    syncComponentFormDrafts(saved)

    const restartLabel = result.restartHints.map((hint) => hint.service).join(', ')
    const backupLabel = result.backupPath ? ` Backup: ${result.backupPath}` : ''
    const restartSuffix = restartLabel ? ` Restart recommended: ${restartLabel}.` : ''
    setToast({
      tone: 'success',
      message: `${component.name} section ${section.key} applied.${restartSuffix}${backupLabel}`,
    })

    if (visibleBackups[component.id]) {
      void loadConfigBackups(component.id)
    }
  }

  const toggleDiffVisibility = (componentId: string) => {
    setVisibleDiffs((previous) => ({
      ...previous,
      [componentId]: !previous[componentId],
    }))
  }

  const toggleBackupsVisibility = (componentId: string) => {
    setVisibleBackups((previous) => {
      const nextVisible = !previous[componentId]
      if (nextVisible) {
        void loadConfigBackups(componentId)
      }
      return {
        ...previous,
        [componentId]: nextVisible,
      }
    })
  }

  const rollbackToBackup = async (component: ConfigComponentView, backupPath: string) => {
    const result = await rollbackConfigComponent(component.id, backupPath)
    if (!result) {
      setToast({
        tone: 'error',
        message: `Rollback failed for ${component.name}.`,
      })
      return
    }

    const restored = result.component
    setConfigDrafts((previous) => ({
      ...previous,
      [restored.id]: restored.content,
    }))
    syncComponentFormDrafts(restored)
    setToast({
      tone: 'success',
      message: `${component.name} rolled back from backup.`,
    })
    void loadConfigBackups(component.id)
  }

  const runSuggestedRestarts = async (hints: ConfigRestartHint[]) => {
    const serviceIds = Array.from(
      new Set(
        hints
          .map((hint) => hint.serviceId)
          .filter((value): value is string => typeof value === 'string' && value.length > 0),
      ),
    )
    if (serviceIds.length === 0) {
      setToast({
        tone: 'error',
        message: 'No actionable restart service was found for this component.',
      })
      return
    }

    const result = await restartConfigServices(serviceIds)
    if (!result) {
      setToast({
        tone: 'error',
        message: 'Restart orchestration failed.',
      })
      return
    }

    const failed = result.outcomes.filter((outcome) => !outcome.ok)
    if (failed.length > 0) {
      setToast({
        tone: 'error',
        message: `Restart completed with errors (${failed.length}/${result.outcomes.length} failed).`,
      })
      return
    }

    setToast({
      tone: 'success',
      message: `Restarted ${result.outcomes.length} service(s).`,
    })
  }

  const applyAdvancedTemplate = (templateId: string) => {
    const template = ADVANCED_COMMAND_TEMPLATES.find((candidate) => candidate.id === templateId)
    if (!template) {
      return
    }

    setAdvancedTemplateId(template.id)
    setAdvancedCommandInput(JSON.stringify(template.command, null, 2))
    setAdvancedAttachmentInput(template.defaultAttachmentBase64 ?? '')
  }

  const runAdvancedControlCommand = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    if (hasUnsavedChanges) {
      setToast({
        tone: 'error',
        message: 'Save settings before running advanced control commands.',
      })
      return
    }

    let parsedCommand: unknown
    try {
      parsedCommand = JSON.parse(advancedCommandInput)
    } catch (error) {
      setToast({
        tone: 'error',
        message: `Command JSON is invalid: ${error instanceof Error ? error.message : String(error)}`,
      })
      return
    }

    if (advancedCommandRequiresGuard && !advancedGuardAcknowledged) {
      setToast({
        tone: 'error',
        message: `Command ${advancedCommandType ?? 'unknown'} requires explicit guard acknowledgment.`,
      })
      return
    }
    if (advancedCommandRequiresGuard && advancedGuardReason.trim().length < 8) {
      setToast({
        tone: 'error',
        message: 'Guard reason must be at least 8 characters for mutating commands.',
      })
      return
    }

    const result = await runAdvancedControl(
      parsedCommand,
      advancedAttachmentInput.trim().length > 0 ? advancedAttachmentInput.trim() : null,
      advancedGuardAcknowledged,
      advancedGuardReason,
    )
    if (!result) {
      setToast({
        tone: 'error',
        message: 'Advanced control command failed.',
      })
      return
    }

    if (result.guarded) {
      setAdvancedGuardAcknowledged(false)
      setAdvancedGuardReason('')
    }

    setToast({
      tone: 'success',
      message: `Executed ${result.commandType}${result.guarded ? ' (guarded)' : ''} → ${result.responseType}.`,
    })
  }

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
        {tokenLooksPresent && !tokenLooksCanonical ? (
          <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.12em] text-amber-700">
            Token format hint: expected payload.signature (2 sections)
          </p>
        ) : null}

        <div className="mt-4 flex flex-wrap gap-2">
          <TabButton title="Overview" active={activeTab === 'overview'} onClick={() => setActiveTab('overview')} />
          <TabButton title="Registry Stream" active={activeTab === 'registry'} onClick={() => setActiveTab('registry')} />
          <TabButton title="Topic Monitor" active={activeTab === 'topics'} onClick={() => setActiveTab('topics')} />
          <TabButton title="Config Console" active={activeTab === 'config'} onClick={() => setActiveTab('config')} />
          <TabButton title="Advanced Control" active={activeTab === 'advanced'} onClick={() => setActiveTab('advanced')} />
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

          <section className="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2 xl:grid-cols-3">
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

            <Panel title="Token-Principal-Policy Diagnostics">
              <KeyValue label="Principal" value={tokenDiagnostics.principal ?? '-'} />
              <KeyValue label="Token ID" value={tokenDiagnostics.tokenId ?? '-'} />
              <KeyValue label="Key ID" value={tokenDiagnostics.keyId ?? '-'} />
              <KeyValue label="Audience" value={tokenDiagnostics.audience ?? '-'} />
              <KeyValue label="Expires" value={tokenDiagnostics.expiresAt ?? '-'} />

              <div className="mt-3 space-y-2">
                {tokenDiagnostics.checks.map((check) => (
                  <article
                    key={check.id}
                    className={`rounded-lg border px-3 py-2 text-xs ${
                      check.status === 'pass'
                        ? 'border-leaf/35 bg-leaf/10 text-leaf'
                        : check.status === 'warn'
                          ? 'border-amber-400/50 bg-amber-50 text-amber-900'
                          : 'border-signal/35 bg-signal/10 text-signal'
                    }`}
                  >
                    <p className="font-mono uppercase tracking-[0.1em]">{check.label}</p>
                    <p className="mt-1">{check.detail}</p>
                  </article>
                ))}
              </div>
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

      {activeTab === 'advanced' ? (
        <section className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <Panel title="Advanced Broker Control">
            <form className="space-y-3" onSubmit={runAdvancedControlCommand}>
              <div className="grid grid-cols-1 gap-3 md:grid-cols-4">
                <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70 md:col-span-3">
                  Template
                  <select
                    value={advancedTemplateId}
                    onChange={(event) => setAdvancedTemplateId(event.target.value)}
                    className="rounded-lg border border-ink/20 px-3 py-2 text-sm normal-case tracking-normal"
                  >
                    {ADVANCED_COMMAND_TEMPLATES.map((template) => (
                      <option key={template.id} value={template.id}>
                        {template.label}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  type="button"
                  onClick={() => applyAdvancedTemplate(advancedTemplateId)}
                  className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                >
                  Load Template
                </button>
              </div>

              <p className="text-xs text-ink/75">
                {
                  ADVANCED_COMMAND_TEMPLATES.find((template) => template.id === advancedTemplateId)
                    ?.description
                }
              </p>

              <div className="rounded-lg border border-ink/15 bg-paper p-3 text-xs text-ink/80">
                <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                  Command Type: {advancedCommandType ?? 'unknown'}
                </p>
                <p className="mt-1">
                  {advancedCommandRequiresGuard
                    ? 'This command mutates broker state and requires guard acknowledgment and a reason.'
                    : 'This command is read-only and does not require guard acknowledgment.'}
                </p>
              </div>

              {advancedCommandRequiresGuard ? (
                <div className="space-y-2 rounded-lg border border-amber-400/50 bg-amber-50 p-3 text-xs text-amber-900">
                  <label className="inline-flex items-center gap-2 font-mono uppercase tracking-[0.1em]">
                    <input
                      type="checkbox"
                      checked={advancedGuardAcknowledged}
                      onChange={(event) => setAdvancedGuardAcknowledged(event.target.checked)}
                    />
                    I acknowledge this command changes broker state
                  </label>
                  <label className="flex flex-col gap-1 font-medium uppercase tracking-[0.1em]">
                    Guard Reason
                    <textarea
                      value={advancedGuardReason}
                      onChange={(event) => setAdvancedGuardReason(event.target.value)}
                      className="h-16 rounded-lg border border-amber-300/70 bg-white px-3 py-2 font-mono text-xs normal-case tracking-normal text-ink"
                      placeholder="Short reason for execution (required, 8+ chars)"
                      spellCheck={false}
                    />
                  </label>
                </div>
              ) : null}

              <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70">
                Control Command JSON
                <textarea
                  value={advancedCommandInput}
                  onChange={(event) => setAdvancedCommandInput(event.target.value)}
                  className="h-80 rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs text-ink"
                  spellCheck={false}
                />
              </label>

              <label className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.12em] text-ink/70">
                Optional attachmentBase64
                <textarea
                  value={advancedAttachmentInput}
                  onChange={(event) => setAdvancedAttachmentInput(event.target.value)}
                  className="h-20 rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs text-ink"
                  spellCheck={false}
                  placeholder="Only needed for put_artifact or other attachment-aware workflows."
                />
              </label>

              <div className="flex flex-wrap gap-2">
                <button
                  type="submit"
                  className="rounded-lg bg-ink px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                  disabled={advancedControlRunning || hasUnsavedChanges}
                >
                  {advancedControlRunning ? 'Executing...' : 'Execute Command'}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    try {
                      const parsed = JSON.parse(advancedCommandInput) as unknown
                      setAdvancedCommandInput(JSON.stringify(parsed, null, 2))
                    } catch {
                      setToast({
                        tone: 'error',
                        message: 'Cannot format invalid JSON.',
                      })
                    }
                  }}
                  className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                >
                  Format JSON
                </button>
              </div>
            </form>

            {hasUnsavedChanges ? (
              <p className="mt-2 font-mono text-[11px] uppercase tracking-[0.12em] text-signal/85">
                Save settings before executing advanced commands.
              </p>
            ) : null}

            {advancedControlError ? (
              <p className="mt-3 rounded-lg border border-signal/40 bg-signal/10 p-3 font-mono text-xs text-signal">
                {advancedControlError}
              </p>
            ) : null}

            <div className="mt-3 rounded-lg border border-amber-400/40 bg-amber-50 p-3 text-xs text-amber-900">
              <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-amber-800">Safety Notes</p>
              <p className="mt-1">
                `open_agent_watch_stream` is stream-only and should be used via the Registry Stream tab. Artifact
                upload/download can carry binary bytes through `attachmentBase64`.
              </p>
            </div>
          </Panel>

          <Panel title="Execution History">
            <div className="mb-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={clearAdvancedControlHistory}
                className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                disabled={advancedControlHistory.length === 0}
              >
                Clear History
              </button>
              <p className="rounded-lg border border-ink/15 bg-paper px-3 py-2 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                Entries: {advancedControlHistory.length}
              </p>
            </div>

            {advancedControlHistory.length === 0 ? (
              <p className="text-sm text-ink/80">No advanced control executions yet.</p>
            ) : (
              <div className="max-h-[900px] space-y-3 overflow-y-auto pr-1">
                {advancedControlHistory.map((entry, index) => (
                  <article key={`${entry.executedAtMs}-${entry.commandType}-${index}`} className="rounded-xl border border-ink/10 bg-paper p-3">
                    <div className="mb-2 flex flex-wrap items-start justify-between gap-2">
                      <div>
                        <p className="font-heading text-base text-ink">
                          {entry.commandType} → {entry.responseType}
                        </p>
                        <p className="font-mono text-[11px] text-ink/70">{formatTimestamp(entry.executedAtMs)}</p>
                      </div>
                      <div className="flex flex-wrap items-center gap-2">
                        <span
                          className={`rounded-full border px-3 py-1 font-mono text-[10px] uppercase tracking-[0.1em] ${
                            entry.guarded
                              ? 'border-amber-500/40 bg-amber-50 text-amber-900'
                              : 'border-leaf/40 bg-leaf/10 text-leaf'
                          }`}
                        >
                          {entry.guarded ? 'guarded command' : 'read command'}
                        </span>
                        <span className="rounded-full border border-ink/20 bg-white px-3 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-ink/70">
                          attachment {formatBytes(entry.attachmentBytes)}
                        </span>
                      </div>
                    </div>

                    <pre className="max-h-80 overflow-auto rounded-lg border border-ink/10 bg-white p-2 font-mono text-[11px] text-ink/85">
                      {truncatePreview(formatJsonValue(entry.response), 16000)}
                    </pre>

                    {entry.attachmentBase64 ? (
                      <details className="mt-2 rounded-lg border border-ink/10 bg-white p-2">
                        <summary className="cursor-pointer font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                          Response attachmentBase64
                        </summary>
                        <pre className="mt-2 max-h-40 overflow-auto rounded bg-paper p-2 font-mono text-[11px] text-ink/85">
                          {truncatePreview(entry.attachmentBase64, 8000)}
                        </pre>
                      </details>
                    ) : null}
                  </article>
                ))}
              </div>
            )}
          </Panel>
        </section>
      ) : null}

      {activeTab === 'config' ? (
        <section className="grid grid-cols-1 gap-4">
          <Panel title="Configuration Console">
            <div className="mb-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => void refreshConfig()}
                className="rounded-lg bg-ink px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                disabled={configLoading}
              >
                {configLoading ? 'Loading...' : 'Reload Config'}
              </button>
              <p className="rounded-lg border border-ink/15 bg-paper px-3 py-2 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                Root: {configSnapshot?.rootPath ?? '-'}
              </p>
            </div>
            <p className="text-sm text-ink/80">
              Edit configuration in mixed mode: form controls for core broker sections plus raw TOML mode for full-file edits.
            </p>
            {configError ? (
              <p className="mt-3 rounded-lg border border-signal/40 bg-signal/10 p-3 font-mono text-xs text-signal">
                {configError}
              </p>
            ) : null}
          </Panel>

          <Panel title="Service Lifecycle">
            <p className="text-sm text-ink/80">
              Run local service controls directly from the console. This executes
              <span className="mx-1 rounded bg-paper px-1 py-0.5 font-mono text-[11px]">scripts/expressways-service.sh</span>
              from the workspace root.
            </p>
            <div className="mt-3 space-y-3">
              {SERVICE_DEFINITIONS.map((service) => {
                const latest = latestServiceResultByService.get(service.id)
                return (
                  <article key={service.id} className="rounded-xl border border-ink/10 bg-paper p-3">
                    <div className="flex flex-wrap items-start justify-between gap-2">
                      <div>
                        <p className="font-heading text-base text-ink">{service.label}</p>
                        <p className="text-xs text-ink/75">{service.description}</p>
                        <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/65">
                          {service.id}
                        </p>
                      </div>
                      {latest ? (
                        <span
                          className={`rounded-full border px-3 py-1 font-mono text-[10px] uppercase tracking-[0.1em] ${
                            latest.ok
                              ? 'border-leaf/40 bg-leaf/10 text-leaf'
                              : 'border-signal/40 bg-signal/10 text-signal'
                          }`}
                        >
                          {latest.action} {latest.ok ? 'ok' : 'failed'}
                        </span>
                      ) : (
                        <span className="rounded-full border border-ink/20 bg-white px-3 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-ink/65">
                          no runs yet
                        </span>
                      )}
                    </div>

                    <div className="mt-3 flex flex-wrap gap-2">
                      {SERVICE_ACTIONS.map((action) => {
                        const running = serviceActionRunningKey === `${service.id}:${action}`
                        return (
                          <button
                            key={`${service.id}-${action}`}
                            type="button"
                            onClick={() => void runServiceLifecycleAction(service.id, action)}
                            className="rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                            disabled={running || guidedFlowRunning}
                          >
                            {running ? `${action}...` : action}
                          </button>
                        )
                      })}
                    </div>

                    {latest ? (
                      <div className="mt-2 rounded-lg border border-ink/10 bg-white p-2">
                        <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                          Last run: {formatTimestamp(latest.executedAtMs)} • code {latest.statusCode ?? 'n/a'}
                        </p>
                        <p className="mt-1 text-xs text-ink/80">{latest.message}</p>
                        {latest.stdout || latest.stderr ? (
                          <details className="mt-2 rounded border border-ink/10 bg-paper p-2">
                            <summary className="cursor-pointer font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                              Output
                            </summary>
                            {latest.stdout ? (
                              <pre className="mt-2 max-h-32 overflow-auto rounded bg-white p-2 font-mono text-[11px] text-ink/80">
                                {truncatePreview(latest.stdout, 4000)}
                              </pre>
                            ) : null}
                            {latest.stderr ? (
                              <pre className="mt-2 max-h-32 overflow-auto rounded bg-white p-2 font-mono text-[11px] text-signal">
                                {truncatePreview(latest.stderr, 4000)}
                              </pre>
                            ) : null}
                          </details>
                        ) : null}
                      </div>
                    ) : null}
                  </article>
                )
              })}
            </div>
            <div className="mt-3">
              <button
                type="button"
                onClick={clearServiceControlHistory}
                className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                disabled={serviceControlHistory.length === 0}
              >
                Clear Service History
              </button>
            </div>
          </Panel>

          <Panel title="Operator Workflow">
            <div className="mb-4 rounded-xl border border-leaf/25 bg-leaf/5 p-3">
              <p className="font-heading text-base text-ink">Packaged Credential Provisioning</p>
              <p className="mt-1 text-xs text-ink/75">
                Select the extracted Expressways bundle root. Provisioning validates its broker config,
                refuses partial or symlinked credential paths, and never returns private keys or token contents to the UI.
              </p>
              <div className="mt-3 flex flex-col gap-2 sm:flex-row">
                <input
                  value={credentialBundleRoot}
                  onChange={(event) => setCredentialBundleRoot(event.target.value)}
                  placeholder="/path/to/extracted/expressways-bundle"
                  aria-label="Expressways bundle root"
                  className="min-w-0 flex-1 rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs text-ink"
                />
                <button
                  type="button"
                  onClick={() => void provisionPackagedCredentials()}
                  className="rounded-lg bg-leaf px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                  disabled={credentialProvisioning || !credentialBundleRoot.trim()}
                >
                  {credentialProvisioning ? 'Provisioning...' : 'Provision Credentials'}
                </button>
              </div>
              <label className="mt-2 inline-flex items-center gap-2 text-xs text-ink/75">
                <input
                  type="checkbox"
                  checked={credentialRefreshToken}
                  onChange={(event) => setCredentialRefreshToken(event.target.checked)}
                />
                Reissue the 30-day token when a complete credential set already exists
              </label>
              {credentialResult ? (
                <div className="mt-3 rounded-lg border border-leaf/20 bg-white p-2 text-xs text-ink/80">
                  <p>{credentialResult.message}</p>
                  <p className="mt-1 font-mono text-[11px]">Token: {credentialResult.tokenPath}</p>
                  {credentialResult.expiresAt ? (
                    <p className="font-mono text-[11px]">Expires: {credentialResult.expiresAt}</p>
                  ) : null}
                </div>
              ) : null}
            </div>
            <div className="mb-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => void runGuidedFirstRunFlow()}
                className="rounded-lg bg-leaf px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                disabled={guidedFlowRunning || Boolean(serviceActionRunningKey) || Boolean(operatorActionRunning)}
              >
                {guidedFlowRunning ? 'Running Guided Flow...' : 'Run Guided First-Run Flow'}
              </button>
              <button
                type="button"
                onClick={clearOperatorActionHistory}
                className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                disabled={operatorActionHistory.length === 0}
              >
                Clear Operator History
              </button>
            </div>

            <p className="text-sm text-ink/80">
              Recommended order: `bootstrap_local` → start broker service → `verify_first_run` → `export_support_bundle`.
              Use `generate_admin_token` when principal or policy changed and token needs to be reissued.
            </p>

            <div className="mt-3 space-y-3">
              {OPERATOR_ACTION_DEFINITIONS.map((definition) => {
                const running = operatorActionRunning === definition.action
                const latest = latestOperatorResultByAction.get(definition.action)
                return (
                  <article key={definition.action} className="rounded-xl border border-ink/10 bg-paper p-3">
                    <div className="flex flex-wrap items-start justify-between gap-2">
                      <div>
                        <p className="font-heading text-base text-ink">{definition.label}</p>
                        <p className="text-xs text-ink/75">{definition.description}</p>
                        <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/65">
                          make {latest?.target ?? normalizeOperatorActionTarget(definition.action)}
                        </p>
                      </div>
                      {latest ? (
                        <span
                          className={`rounded-full border px-3 py-1 font-mono text-[10px] uppercase tracking-[0.1em] ${
                            latest.ok
                              ? 'border-leaf/40 bg-leaf/10 text-leaf'
                              : 'border-signal/40 bg-signal/10 text-signal'
                          }`}
                        >
                          {latest.ok ? 'ok' : 'failed'}
                        </span>
                      ) : (
                        <span className="rounded-full border border-ink/20 bg-white px-3 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-ink/65">
                          no runs yet
                        </span>
                      )}
                    </div>

                    <div className="mt-3 flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => void runOperatorWorkflowAction(definition.action)}
                        className="rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                        disabled={running || guidedFlowRunning}
                      >
                        {running ? 'Running...' : definition.label}
                      </button>
                    </div>

                    {latest ? (
                      <div className="mt-2 rounded-lg border border-ink/10 bg-white p-2">
                        <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                          Last run: {formatTimestamp(latest.executedAtMs)} • code {latest.statusCode ?? 'n/a'}
                        </p>
                        <p className="mt-1 text-xs text-ink/80">{latest.message}</p>
                        {latest.stdout || latest.stderr ? (
                          <details className="mt-2 rounded border border-ink/10 bg-paper p-2">
                            <summary className="cursor-pointer font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                              Output
                            </summary>
                            {latest.stdout ? (
                              <pre className="mt-2 max-h-32 overflow-auto rounded bg-white p-2 font-mono text-[11px] text-ink/80">
                                {truncatePreview(latest.stdout, 4000)}
                              </pre>
                            ) : null}
                            {latest.stderr ? (
                              <pre className="mt-2 max-h-32 overflow-auto rounded bg-white p-2 font-mono text-[11px] text-signal">
                                {truncatePreview(latest.stderr, 4000)}
                              </pre>
                            ) : null}
                          </details>
                        ) : null}
                      </div>
                    ) : null}
                  </article>
                )
              })}
            </div>
          </Panel>

          <Panel title="Config Audit Trail">
            <div className="mb-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => void loadConfigAuditEntries(200)}
                className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                disabled={configAuditLoading}
              >
                {configAuditLoading ? 'Loading...' : 'Reload Audit Trail'}
              </button>
              <p className="rounded-lg border border-ink/15 bg-paper px-3 py-2 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                Entries: {configAuditEntries.length}
              </p>
            </div>
            {configAuditEntries.length === 0 ? (
              <p className="text-sm text-ink/75">No config audit entries recorded yet.</p>
            ) : (
              <div className="max-h-[520px] space-y-2 overflow-y-auto pr-1">
                {configAuditEntries.map((entry) => (
                  <ConfigAuditEntryCard key={entry.entryId} entry={entry} />
                ))}
              </div>
            )}
          </Panel>

          {configGroups.map(([group, components]) => (
            <Panel key={group} title={`Component Group: ${group}`}>
              <div className="space-y-4">
                {components.map((component) => {
                  const draft = configDrafts[component.id] ?? component.content
                  const dirty = draft !== component.content
                  const saving = configSavingComponentId === component.id
                  const loadingBackups = configBackupLoadingComponentId === component.id
                  const rollingBack = configRollbackComponentId === component.id
                  const backupsVisible = visibleBackups[component.id] ?? false
                  const diffVisible = visibleDiffs[component.id] ?? false
                  const backups = configBackupsByComponent[component.id] ?? []
                  const diff = dirty ? buildLineDiff(component.content, draft) : []
                  const formSections = component.sections.filter(
                    (section) => section.formFields.length > 0 || section.tableArrays.length > 0,
                  )
                  const hasFormMode = formSections.length > 0
                  const editorMode: ConfigEditorMode = hasFormMode
                    ? (configEditorModes[component.id] ?? 'form')
                    : 'raw'
                  const restartableServiceIds = Array.from(
                    new Set(
                      component.restartHints
                        .map((hint) => hint.serviceId)
                        .filter((value): value is string => typeof value === 'string' && value.length > 0),
                    ),
                  )
                  const restartBusy = restartableServiceIds.some((serviceId) =>
                    configRestartingServiceIds.includes(serviceId),
                  )
                  return (
                    <article key={component.id} className="rounded-2xl border border-ink/15 bg-paper/70 p-4">
                      <div className="mb-2 flex flex-wrap items-start justify-between gap-2">
                        <div>
                          <p className="font-heading text-lg text-ink">{component.name}</p>
                          <p className="text-xs text-ink/75">{component.description}</p>
                        </div>
                        <span
                          className={`rounded-full border px-3 py-1 font-mono text-[11px] uppercase tracking-[0.11em] ${
                            dirty
                              ? 'border-signal/40 bg-signal/10 text-signal'
                              : 'border-leaf/40 bg-leaf/10 text-leaf'
                          }`}
                        >
                          {dirty ? 'Unsaved' : 'Saved'}
                        </span>
                      </div>

                      <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/60">
                        {component.filePath}
                      </p>
                      <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/55">
                        {component.exists ? 'existing file' : 'new file'} • last modified{' '}
                        {formatTimestamp(component.updatedAtMs)}
                      </p>

                      {component.sections.length > 0 ? (
                        <div className="mt-2 flex flex-wrap gap-2">
                          {component.sections.map((section) => (
                            <span
                              key={`${component.id}-${section.key}`}
                              className="rounded border border-ink/15 bg-white px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-ink/70"
                              title={section.summary}
                            >
                              {section.key} ({section.kind})
                            </span>
                          ))}
                        </div>
                      ) : null}

                      {component.parseError ? (
                        <p className="mt-2 rounded-lg border border-signal/40 bg-signal/10 p-2 font-mono text-xs text-signal">
                          Parse warning: {component.parseError}
                        </p>
                      ) : null}

                      {hasFormMode ? (
                        <div className="mt-2 flex flex-wrap items-center gap-2 rounded-lg border border-ink/15 bg-white p-2">
                          <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/65">Editor Mode</p>
                          <button
                            type="button"
                            onClick={() => setConfigEditorMode(component.id, 'form')}
                            className={`rounded border px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] ${
                              editorMode === 'form'
                                ? 'border-leaf/50 bg-leaf/10 text-leaf'
                                : 'border-ink/20 bg-white text-ink/75'
                            }`}
                          >
                            Form
                          </button>
                          <button
                            type="button"
                            onClick={() => setConfigEditorMode(component.id, 'raw')}
                            className={`rounded border px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] ${
                              editorMode === 'raw'
                                ? 'border-leaf/50 bg-leaf/10 text-leaf'
                                : 'border-ink/20 bg-white text-ink/75'
                            }`}
                          >
                            Raw TOML
                          </button>
                          <p className="text-xs text-ink/65">
                            Form mode covers core broker sections, including nested table arrays. Use Raw TOML for unsupported sections and advanced structures.
                          </p>
                        </div>
                      ) : null}

                      {component.restartHints.length > 0 ? (
                        <div className="mt-2 space-y-1 rounded-lg border border-amber-400/40 bg-amber-50 p-2">
                          <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-amber-700">
                            Restart Recommendations
                          </p>
                          {component.restartHints.map((hint) => (
                            <div key={`${component.id}-${hint.service}`} className="text-xs text-amber-800">
                              <p>{hint.service}: {hint.reason}</p>
                              {hint.command ? (
                                <p className="mt-1 rounded bg-white/80 px-2 py-1 font-mono text-[11px] text-amber-900">
                                  {hint.command}
                                </p>
                              ) : null}
                              {hint.serviceId ? (
                                <button
                                  type="button"
                                  onClick={() => void runSuggestedRestarts([hint])}
                                  className="mt-1 rounded border border-amber-500/40 bg-white px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-amber-800"
                                  disabled={configRestartingServiceIds.includes(hint.serviceId)}
                                >
                                  {configRestartingServiceIds.includes(hint.serviceId) ? 'Restarting...' : 'Restart Now'}
                                </button>
                              ) : null}
                            </div>
                          ))}
                        </div>
                      ) : null}

                      {editorMode === 'form' && hasFormMode ? (
                        <div className="mt-3 space-y-3">
                          {formSections.map((section) => {
                            const sectionDraftKey = configSectionDraftKey(component.id, section.key)
                            const sectionDraft =
                              configFormDrafts[sectionDraftKey] ?? createSectionFormDraft(section.formFields)
                            const sectionTableArrayDraft =
                              configTableArrayDrafts[sectionDraftKey] ??
                              createSectionTableArrayDraft(section.tableArrays)
                            return (
                              <article
                                key={`${component.id}-${section.key}-form`}
                                className="rounded-xl border border-ink/15 bg-white p-3"
                              >
                                <div className="mb-2 flex flex-wrap items-start justify-between gap-2">
                                  <div>
                                    <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                                      Section {section.key}
                                    </p>
                                    <p className="text-xs text-ink/70">{section.summary}</p>
                                  </div>
                                  <span className="rounded border border-ink/20 bg-paper px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-ink/70">
                                    {section.formFields.length} fields • {section.tableArrays.length} tables
                                  </span>
                                </div>

                                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                                  {section.formFields.map((field) => {
                                    const rawValue = sectionDraft[field.key] ?? toSectionDraftValue(field)
                                    const validationHint = formatFieldValidationHint(field)
                                    const validationError = previewSectionFieldValidation(field, rawValue)
                                    if (field.kind === 'boolean') {
                                      return (
                                        <div
                                          key={`${component.id}-${section.key}-${field.key}`}
                                          className="rounded-lg border border-ink/15 bg-paper px-3 py-2 text-xs text-ink/80 md:col-span-2"
                                        >
                                          <label className="flex items-center justify-between">
                                            <span className="font-mono uppercase tracking-[0.1em]">{field.label}</span>
                                            <input
                                              type="checkbox"
                                              checked={Boolean(rawValue)}
                                              onChange={(event) =>
                                                updateSectionFormField(
                                                  component.id,
                                                  section.key,
                                                  field.key,
                                                  event.target.checked,
                                                )
                                              }
                                            />
                                          </label>
                                          {field.description ? (
                                            <p className="mt-1 normal-case tracking-normal text-ink/70">
                                              {field.description}
                                            </p>
                                          ) : null}
                                          {validationHint ? (
                                            <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/60">
                                              {validationHint}
                                            </p>
                                          ) : null}
                                          {validationError ? (
                                            <p className="mt-1 text-[11px] normal-case tracking-normal text-signal">
                                              {validationError}
                                            </p>
                                          ) : null}
                                        </div>
                                      )
                                    }

                                    if (field.kind === 'string_array') {
                                      const displayValue =
                                        typeof rawValue === 'string'
                                          ? rawValue
                                          : Array.isArray(rawValue)
                                            ? rawValue.map(String).join(', ')
                                            : ''
                                      return (
                                        <label
                                          key={`${component.id}-${section.key}-${field.key}`}
                                          className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.1em] text-ink/70 md:col-span-2"
                                        >
                                          {field.label}
                                          <textarea
                                            value={displayValue}
                                            onChange={(event) =>
                                              updateSectionFormField(
                                                component.id,
                                                section.key,
                                                field.key,
                                                event.target.value,
                                              )
                                            }
                                            className="h-20 rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs normal-case tracking-normal text-ink"
                                            spellCheck={false}
                                            placeholder="comma-separated values"
                                          />
                                          {field.description ? (
                                            <p className="normal-case tracking-normal text-ink/70">
                                              {field.description}
                                            </p>
                                          ) : null}
                                          {validationHint ? (
                                            <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/60">
                                              {validationHint}
                                            </p>
                                          ) : null}
                                          {validationError ? (
                                            <p className="text-[11px] normal-case tracking-normal text-signal">
                                              {validationError}
                                            </p>
                                          ) : null}
                                        </label>
                                      )
                                    }

                                    const displayValue =
                                      typeof rawValue === 'string' || typeof rawValue === 'number'
                                        ? String(rawValue)
                                        : ''
                                    return (
                                      <label
                                        key={`${component.id}-${section.key}-${field.key}`}
                                        className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.1em] text-ink/70"
                                      >
                                        {field.label}
                                        <input
                                          type={field.kind === 'integer' || field.kind === 'float' ? 'number' : 'text'}
                                          step={field.kind === 'float' ? 'any' : undefined}
                                          min={
                                            (field.kind === 'integer' || field.kind === 'float')
                                              ? (field.validation?.min ?? undefined)
                                              : undefined
                                          }
                                          max={
                                            (field.kind === 'integer' || field.kind === 'float')
                                              ? (field.validation?.max ?? undefined)
                                              : undefined
                                          }
                                          value={displayValue}
                                          onChange={(event) =>
                                            updateSectionFormField(
                                              component.id,
                                              section.key,
                                              field.key,
                                              event.target.value,
                                            )
                                          }
                                          className="rounded-lg border border-ink/20 px-3 py-2 text-sm normal-case tracking-normal"
                                        />
                                        {field.description ? (
                                          <p className="normal-case tracking-normal text-ink/70">
                                            {field.description}
                                          </p>
                                        ) : null}
                                        {validationHint ? (
                                          <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/60">
                                            {validationHint}
                                          </p>
                                        ) : null}
                                        {validationError ? (
                                          <p className="text-[11px] normal-case tracking-normal text-signal">
                                            {validationError}
                                          </p>
                                        ) : null}
                                      </label>
                                    )
                                  })}
                                </div>

                                {section.tableArrays.length > 0 ? (
                                  <div className="mt-3 space-y-3">
                                    {section.tableArrays.map((tableArray) => {
                                      const entryDrafts = sectionTableArrayDraft[tableArray.key] ?? []
                                      return (
                                        <article
                                          key={`${component.id}-${section.key}-${tableArray.key}-table-array`}
                                          className="rounded-lg border border-ink/15 bg-paper p-3"
                                        >
                                          <div className="mb-2 flex flex-wrap items-start justify-between gap-2">
                                            <div>
                                              <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                                                {tableArray.label} ({tableArray.key})
                                              </p>
                                              {tableArray.description ? (
                                                <p className="text-xs normal-case tracking-normal text-ink/70">
                                                  {tableArray.description}
                                                </p>
                                              ) : null}
                                            </div>
                                            <div className="flex items-center gap-2">
                                              <span className="rounded border border-ink/20 bg-white px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-ink/70">
                                                {entryDrafts.length} entries
                                              </span>
                                              <button
                                                type="button"
                                                onClick={() =>
                                                  addTableArrayEntry(component.id, section.key, tableArray)
                                                }
                                                className="rounded border border-leaf/40 bg-white px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-leaf"
                                                disabled={saving || rollingBack || !component.editable}
                                              >
                                                Add Entry
                                              </button>
                                            </div>
                                          </div>

                                          {entryDrafts.length === 0 ? (
                                            <p className="rounded border border-ink/15 bg-white px-3 py-2 text-xs text-ink/70">
                                              No entries yet. Add an entry to configure this table.
                                            </p>
                                          ) : (
                                            <div className="space-y-3">
                                              {entryDrafts.map((entryDraft, entryIndex) => (
                                                <div
                                                  key={`${component.id}-${section.key}-${tableArray.key}-entry-${entryIndex}`}
                                                  className="rounded border border-ink/15 bg-white p-3"
                                                >
                                                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                                                    <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">
                                                      Entry {entryIndex + 1}
                                                    </p>
                                                    <button
                                                      type="button"
                                                      onClick={() =>
                                                        removeTableArrayEntry(
                                                          component.id,
                                                          section.key,
                                                          tableArray.key,
                                                          entryIndex,
                                                        )
                                                      }
                                                      className="rounded border border-signal/40 bg-white px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-signal"
                                                      disabled={saving || rollingBack || !component.editable}
                                                    >
                                                      Remove
                                                    </button>
                                                  </div>

                                                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                                                    {tableArray.entryFields.map((field) => {
                                                      const rawValue =
                                                        entryDraft[field.key] ??
                                                        toDraftFieldValue(field.kind, field.value)
                                                      const validationHint = formatFieldValidationHint(field)
                                                      const validationError = previewSectionFieldValidation(
                                                        field,
                                                        rawValue,
                                                      )

                                                      if (field.kind === 'boolean') {
                                                        return (
                                                          <div
                                                            key={`${component.id}-${section.key}-${tableArray.key}-${entryIndex}-${field.key}`}
                                                            className="rounded-lg border border-ink/15 bg-paper px-3 py-2 text-xs text-ink/80 md:col-span-2"
                                                          >
                                                            <label className="flex items-center justify-between">
                                                              <span className="font-mono uppercase tracking-[0.1em]">
                                                                {field.label}
                                                              </span>
                                                              <input
                                                                type="checkbox"
                                                                checked={Boolean(rawValue)}
                                                                onChange={(event) =>
                                                                  updateTableArrayEntryField(
                                                                    component.id,
                                                                    section.key,
                                                                    tableArray.key,
                                                                    entryIndex,
                                                                    field.key,
                                                                    event.target.checked,
                                                                  )
                                                                }
                                                              />
                                                            </label>
                                                            {field.description ? (
                                                              <p className="mt-1 normal-case tracking-normal text-ink/70">
                                                                {field.description}
                                                              </p>
                                                            ) : null}
                                                            {validationHint ? (
                                                              <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/60">
                                                                {validationHint}
                                                              </p>
                                                            ) : null}
                                                            {validationError ? (
                                                              <p className="mt-1 text-[11px] normal-case tracking-normal text-signal">
                                                                {validationError}
                                                              </p>
                                                            ) : null}
                                                          </div>
                                                        )
                                                      }

                                                      if (field.kind === 'string_array') {
                                                        const displayValue =
                                                          typeof rawValue === 'string'
                                                            ? rawValue
                                                            : Array.isArray(rawValue)
                                                              ? rawValue.map(String).join(', ')
                                                              : ''
                                                        return (
                                                          <label
                                                            key={`${component.id}-${section.key}-${tableArray.key}-${entryIndex}-${field.key}`}
                                                            className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.1em] text-ink/70 md:col-span-2"
                                                          >
                                                            {field.label}
                                                            <textarea
                                                              value={displayValue}
                                                              onChange={(event) =>
                                                                updateTableArrayEntryField(
                                                                  component.id,
                                                                  section.key,
                                                                  tableArray.key,
                                                                  entryIndex,
                                                                  field.key,
                                                                  event.target.value,
                                                                )
                                                              }
                                                              className="h-20 rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs normal-case tracking-normal text-ink"
                                                              spellCheck={false}
                                                              placeholder="comma-separated values"
                                                            />
                                                            {field.description ? (
                                                              <p className="normal-case tracking-normal text-ink/70">
                                                                {field.description}
                                                              </p>
                                                            ) : null}
                                                            {validationHint ? (
                                                              <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/60">
                                                                {validationHint}
                                                              </p>
                                                            ) : null}
                                                            {validationError ? (
                                                              <p className="text-[11px] normal-case tracking-normal text-signal">
                                                                {validationError}
                                                              </p>
                                                            ) : null}
                                                          </label>
                                                        )
                                                      }

                                                      const displayValue =
                                                        typeof rawValue === 'string' ||
                                                        typeof rawValue === 'number'
                                                          ? String(rawValue)
                                                          : ''
                                                      return (
                                                        <label
                                                          key={`${component.id}-${section.key}-${tableArray.key}-${entryIndex}-${field.key}`}
                                                          className="flex flex-col gap-1 text-xs font-medium uppercase tracking-[0.1em] text-ink/70"
                                                        >
                                                          {field.label}
                                                          <input
                                                            type={
                                                              field.kind === 'integer' || field.kind === 'float'
                                                                ? 'number'
                                                                : 'text'
                                                            }
                                                            step={field.kind === 'float' ? 'any' : undefined}
                                                            min={
                                                              field.kind === 'integer' || field.kind === 'float'
                                                                ? (field.validation?.min ?? undefined)
                                                                : undefined
                                                            }
                                                            max={
                                                              field.kind === 'integer' || field.kind === 'float'
                                                                ? (field.validation?.max ?? undefined)
                                                                : undefined
                                                            }
                                                            value={displayValue}
                                                            onChange={(event) =>
                                                              updateTableArrayEntryField(
                                                                component.id,
                                                                section.key,
                                                                tableArray.key,
                                                                entryIndex,
                                                                field.key,
                                                                event.target.value,
                                                              )
                                                            }
                                                            className="rounded-lg border border-ink/20 px-3 py-2 text-sm normal-case tracking-normal"
                                                          />
                                                          {field.description ? (
                                                            <p className="normal-case tracking-normal text-ink/70">
                                                              {field.description}
                                                            </p>
                                                          ) : null}
                                                          {validationHint ? (
                                                            <p className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink/60">
                                                              {validationHint}
                                                            </p>
                                                          ) : null}
                                                          {validationError ? (
                                                            <p className="text-[11px] normal-case tracking-normal text-signal">
                                                              {validationError}
                                                            </p>
                                                          ) : null}
                                                        </label>
                                                      )
                                                    })}
                                                  </div>
                                                </div>
                                              ))}
                                            </div>
                                          )}
                                        </article>
                                      )
                                    })}
                                  </div>
                                ) : null}

                                <div className="mt-3 flex flex-wrap gap-2">
                                  <button
                                    type="button"
                                    onClick={() => resetSectionFormDraft(component.id, section)}
                                    className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                                    disabled={saving || rollingBack}
                                  >
                                    Reset Section
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => void saveSectionFormDraft(component, section, dirty)}
                                    className="rounded-lg bg-leaf px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                                    disabled={saving || rollingBack || !component.editable}
                                  >
                                    {saving ? 'Applying...' : `Apply ${section.key}`}
                                  </button>
                                </div>
                              </article>
                            )
                          })}

                          {dirty ? (
                            <p className="rounded-lg border border-amber-400/40 bg-amber-50 p-2 font-mono text-[11px] uppercase tracking-[0.1em] text-amber-800">
                              Raw draft has unsaved changes. Apply or reset raw mode before saving form sections.
                            </p>
                          ) : null}
                        </div>
                      ) : (
                        <textarea
                          value={draft}
                          onChange={(event) => updateConfigDraft(component.id, event.target.value)}
                          className="mt-3 h-80 w-full rounded-lg border border-ink/20 bg-white px-3 py-2 font-mono text-xs text-ink"
                          placeholder={'[component]\nkey = "value"'}
                          spellCheck={false}
                        />
                      )}

                      <div className="mt-3 flex flex-wrap gap-2">
                        {editorMode === 'raw' ? (
                          <button
                            type="button"
                            onClick={() => toggleDiffVisibility(component.id)}
                            className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                            disabled={!dirty}
                          >
                            {diffVisible ? 'Hide Diff' : 'View Diff'}
                          </button>
                        ) : null}
                        <button
                          type="button"
                          onClick={() => toggleBackupsVisibility(component.id)}
                          className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                          disabled={loadingBackups}
                        >
                          {loadingBackups ? 'Loading...' : backupsVisible ? 'Hide Backups' : 'Backups'}
                        </button>
                        {editorMode === 'raw' ? (
                          <button
                            type="button"
                            onClick={() => resetConfigDraft(component)}
                            className="rounded-lg border border-ink/20 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-ink"
                            disabled={saving || rollingBack || !dirty}
                          >
                            Reset
                          </button>
                        ) : null}
                        {editorMode === 'raw' ? (
                          <button
                            type="button"
                            onClick={() => void saveConfigDraft(component)}
                            className="rounded-lg bg-leaf px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                            disabled={saving || rollingBack || !dirty || !component.editable}
                          >
                            {saving ? 'Applying...' : 'Apply Changes'}
                          </button>
                        ) : null}
                        <button
                          type="button"
                          onClick={() => void runSuggestedRestarts(component.restartHints)}
                          className="rounded-lg bg-amber-500 px-3 py-2 font-mono text-xs uppercase tracking-[0.12em] text-white"
                          disabled={restartBusy || restartableServiceIds.length === 0}
                        >
                          {restartBusy ? 'Restarting...' : 'Restart Suggested'}
                        </button>
                      </div>

                      {editorMode === 'raw' && diffVisible && dirty ? (
                        <div className="mt-3 rounded-lg border border-ink/15 bg-white p-2">
                          <p className="mb-2 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">Diff Preview</p>
                          <div className="max-h-56 overflow-auto rounded border border-ink/10 bg-ink/5 p-2 font-mono text-[11px]">
                            {diff.map((line, index) => (
                              <p
                                key={`${component.id}-diff-${index}`}
                                className={
                                  line.kind === 'add'
                                    ? 'bg-leaf/15 text-leaf'
                                    : line.kind === 'remove'
                                      ? 'bg-signal/15 text-signal'
                                      : 'text-ink/75'
                                }
                              >
                                {line.kind === 'add' ? '+' : line.kind === 'remove' ? '-' : ' '} {line.text}
                              </p>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {backupsVisible ? (
                        <div className="mt-3 rounded-lg border border-ink/15 bg-white p-2">
                          <p className="mb-2 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/70">Backups</p>
                          {backups.length === 0 ? (
                            <p className="text-xs text-ink/70">No backups found for this component yet.</p>
                          ) : (
                            <div className="space-y-2">
                              {backups.map((backup) => (
                                <div key={backup.backupPath} className="rounded border border-ink/10 bg-paper p-2">
                                  <p className="font-mono text-[11px] text-ink/75">{backup.backupPath}</p>
                                  <p className="mt-1 text-xs text-ink/70">
                                    {formatTimestamp(backup.createdAtMs)} • {formatBytes(backup.sizeBytes)}
                                  </p>
                                  <button
                                    type="button"
                                    onClick={() => void rollbackToBackup(component, backup.backupPath)}
                                    className="mt-2 rounded border border-signal/40 bg-white px-2 py-1 font-mono text-[10px] uppercase tracking-[0.1em] text-signal"
                                    disabled={rollingBack || saving}
                                  >
                                    {rollingBack ? 'Rolling Back...' : 'Rollback to This Backup'}
                                  </button>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ) : null}
                    </article>
                  )
                })}
              </div>
            </Panel>
          ))}
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

function ConfigAuditEntryCard({ entry }: { entry: ConfigAuditEntryView }) {
  const statusTone =
    entry.success === null
      ? 'border-ink/20 bg-white text-ink/75'
      : entry.success
        ? 'border-leaf/40 bg-leaf/10 text-leaf'
        : 'border-signal/40 bg-signal/10 text-signal'
  return (
    <article className="rounded-xl border border-ink/10 bg-paper p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="font-heading text-sm text-ink">
            {entry.category} / {entry.action}
          </p>
          <p className="font-mono text-[11px] text-ink/70">{formatTimestamp(entry.recordedAtMs)}</p>
        </div>
        <span className={`rounded-full border px-3 py-1 font-mono text-[10px] uppercase tracking-[0.1em] ${statusTone}`}>
          {entry.success === null ? 'recorded' : entry.success ? 'success' : 'failed'}
        </span>
      </div>
      <p className="mt-2 text-xs text-ink/85">{entry.summary}</p>
      <p className="mt-2 font-mono text-[11px] text-ink/65">
        actor {entry.actor}
        {entry.componentId ? ` • component ${entry.componentId}` : ''}
        {entry.sectionKey ? ` • section ${entry.sectionKey}` : ''}
        {entry.serviceId ? ` • service ${entry.serviceId}` : ''}
        {entry.commandType ? ` • command ${entry.commandType}` : ''}
        {typeof entry.statusCode === 'number' ? ` • code ${entry.statusCode}` : ''}
      </p>
      {entry.diff ? (
        <p className="mt-1 font-mono text-[11px] uppercase tracking-[0.1em] text-ink/65">
          diff +{entry.diff.addedLines} -{entry.diff.removedLines} ~{entry.diff.changedLines}
        </p>
      ) : null}
    </article>
  )
}

function formatTimestamp(value: number | null): string {
  if (!value) {
    return '-'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return '-'
  }
  return date.toLocaleString()
}

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return '0 B'
  }
  if (value < 1024) {
    return `${value} B`
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KiB`
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`
}

function formatJsonValue(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

function truncatePreview(value: string, maxChars: number): string {
  if (value.length <= maxChars) {
    return value
  }
  return `${value.slice(0, maxChars)}...`
}

function configSectionDraftKey(componentId: string, sectionKey: string): string {
  return `${componentId}::${sectionKey}`
}

function createSectionFormDraft(fields: ConfigFormFieldView[]): Record<string, unknown> {
  const next: Record<string, unknown> = {}
  for (const field of fields) {
    next[field.key] = toSectionDraftValue(field)
  }
  return next
}

function createSectionTableArrayDraft(
  tableArrays: ConfigTableArrayView[],
): Record<string, Array<Record<string, unknown>>> {
  const next: Record<string, Array<Record<string, unknown>>> = {}
  for (const tableArray of tableArrays) {
    next[tableArray.key] = tableArray.entries.map((entry) =>
      createTableArrayEntryDraft(tableArray.entryFields, entry),
    )
  }
  return next
}

function createTableArrayEntryDraft(
  fields: ConfigFormFieldView[],
  source?: Record<string, unknown>,
): Record<string, unknown> {
  const next: Record<string, unknown> = {}
  const knownKeys = new Set<string>()
  for (const field of fields) {
    knownKeys.add(field.key)
    const rawValue =
      source && Object.prototype.hasOwnProperty.call(source, field.key) ? source[field.key] : field.value
    next[field.key] = toDraftFieldValue(field.kind, rawValue)
  }
  if (source) {
    for (const [key, value] of Object.entries(source)) {
      if (!knownKeys.has(key)) {
        next[key] = value
      }
    }
  }
  return next
}

function toSectionDraftValue(field: ConfigFormFieldView): unknown {
  return toDraftFieldValue(field.kind, field.value)
}

function toDraftFieldValue(kind: ConfigFormFieldKind, value: unknown): unknown {
  if (kind === 'string_array') {
    if (typeof value === 'string') {
      return value
    }
    if (Array.isArray(value)) {
      return value.map(String).join(', ')
    }
    return ''
  }
  return value
}

function normalizeSectionFormValue(kind: ConfigFormFieldKind, value: unknown): unknown {
  if (kind === 'boolean') {
    if (typeof value === 'boolean') {
      return value
    }
    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase()
      if (normalized === 'true') {
        return true
      }
      if (normalized === 'false') {
        return false
      }
    }
    throw new Error('expected boolean value')
  }

  if (kind === 'integer') {
    const raw = typeof value === 'number' ? String(value) : String(value ?? '').trim()
    if (!raw) {
      throw new Error('value is required')
    }
    if (!/^-?\d+$/.test(raw)) {
      throw new Error('expected integer')
    }
    return Number.parseInt(raw, 10)
  }

  if (kind === 'float') {
    const raw = typeof value === 'number' ? String(value) : String(value ?? '').trim()
    if (!raw) {
      throw new Error('value is required')
    }
    const parsed = Number.parseFloat(raw)
    if (!Number.isFinite(parsed)) {
      throw new Error('expected number')
    }
    return parsed
  }

  if (kind === 'string_array') {
    if (Array.isArray(value)) {
      return value
        .map((item) => String(item).trim())
        .filter((item) => item.length > 0)
    }
    return String(value ?? '')
      .split(/[,\n]/)
      .map((item) => item.trim())
      .filter((item) => item.length > 0)
  }

  return String(value ?? '')
}

function parseAdvancedCommandType(command: unknown): string | null {
  if (!command || typeof command !== 'object' || Array.isArray(command)) {
    return null
  }
  const value = (command as Record<string, unknown>).type
  if (typeof value !== 'string') {
    return null
  }
  const normalized = value.trim()
  return normalized.length > 0 ? normalized : null
}

function isGuardedAdvancedCommandType(commandType: string): boolean {
  return GUARDED_ADVANCED_COMMAND_TYPES.has(commandType)
}

function validateSectionFieldValue(field: ConfigFormFieldView, value: unknown): string | null {
  const validation = field.validation
  if (!validation) {
    return null
  }

  if (validation.required) {
    if (typeof value === 'string' && value.trim().length === 0) {
      return 'value is required'
    }
    if (Array.isArray(value) && value.length === 0) {
      return 'at least one value is required'
    }
  }

  const hasRange = typeof validation.min === 'number' || typeof validation.max === 'number'
  if (hasRange && (field.kind === 'integer' || field.kind === 'float')) {
    const numericValue = typeof value === 'number' ? value : Number.parseFloat(String(value))
    if (!Number.isFinite(numericValue)) {
      return 'expected numeric value'
    }
    if (typeof validation.min === 'number' && numericValue < validation.min) {
      return `must be >= ${validation.min}`
    }
    if (typeof validation.max === 'number' && numericValue > validation.max) {
      return `must be <= ${validation.max}`
    }
  }

  if (validation.allowedValues && validation.allowedValues.length > 0) {
    if (field.kind === 'string') {
      const normalized = String(value ?? '').trim()
      if (!validation.allowedValues.includes(normalized)) {
        return `must be one of: ${validation.allowedValues.join(', ')}`
      }
    }
    if (field.kind === 'string_array') {
      const values = Array.isArray(value)
        ? value.map((item) => String(item).trim()).filter(Boolean)
        : String(value ?? '')
            .split(/[,\n]/)
            .map((item) => item.trim())
            .filter(Boolean)
      const invalid = values.find((item) => !validation.allowedValues?.includes(item))
      if (invalid) {
        return `entry ${invalid} must be one of: ${validation.allowedValues.join(', ')}`
      }
    }
  }

  return null
}

function previewSectionFieldValidation(field: ConfigFormFieldView, rawValue: unknown): string | null {
  try {
    const normalized = normalizeSectionFormValue(field.kind, rawValue)
    return validateSectionFieldValue(field, normalized)
  } catch (error) {
    return error instanceof Error ? error.message : String(error)
  }
}

function formatFieldValidationHint(field: ConfigFormFieldView): string | null {
  const validation = field.validation
  if (!validation) {
    return null
  }

  const hints: string[] = []
  if (validation.required) {
    hints.push('required')
  }
  if (typeof validation.min === 'number') {
    hints.push(`min ${validation.min}`)
  }
  if (typeof validation.max === 'number') {
    hints.push(`max ${validation.max}`)
  }
  if (validation.allowedValues && validation.allowedValues.length > 0) {
    hints.push(`allowed: ${validation.allowedValues.join(', ')}`)
  }
  return hints.length > 0 ? hints.join(' | ') : null
}

function formatOperatorActionLabel(action: OperatorAction): string {
  switch (action) {
    case 'bootstrap_local':
      return 'Bootstrap Local'
    case 'generate_admin_token':
      return 'Generate Admin Token'
    case 'verify_first_run':
      return 'Verify First Run'
    case 'export_support_bundle':
      return 'Export Support Bundle'
  }
}

function normalizeOperatorActionTarget(action: OperatorAction): string {
  switch (action) {
    case 'bootstrap_local':
      return 'bootstrap-local'
    case 'generate_admin_token':
      return 'generate-admin-token'
    case 'verify_first_run':
      return 'verify-first-run'
    case 'export_support_bundle':
      return 'export-support-bundle'
  }
}

const REQUIRED_BOOTSTRAP_OPERATIONS: Array<{ label: string; resource: string; action: string }> = [
  { label: 'Broker health', resource: 'system:broker', action: 'health' },
  { label: 'Broker admin', resource: 'system:broker', action: 'admin' },
  { label: 'Topic publish', resource: 'topic:*', action: 'publish' },
  { label: 'Topic consume', resource: 'topic:*', action: 'consume' },
  { label: 'Registry admin', resource: 'registry:agents*', action: 'admin' },
]

function buildTokenPrincipalPolicyDiagnostics(
  token: string,
  snapshot: MonitorSnapshot | null,
  configSnapshot: ConfigConsoleSnapshot | null,
): TokenPrincipalPolicyDiagnostics {
  const checks: DiagnosticCheck[] = []
  const trimmed = token.trim()

  if (trimmed.length === 0) {
    checks.push({
      id: 'token-present',
      label: 'Token Presence',
      status: 'fail',
      detail: 'No token saved. Set a capability token and refresh.',
    })
    return {
      principal: null,
      keyId: null,
      audience: null,
      tokenId: null,
      expiresAt: null,
      checks,
    }
  }

  const tokenParts = trimmed.split('.')
  const canonicalFormat = tokenParts.length === 2 && tokenParts.every((part) => part.length > 0)
  checks.push({
    id: 'token-format',
    label: 'Token Format',
    status: canonicalFormat ? 'pass' : 'fail',
    detail: canonicalFormat
      ? 'Format matches payload.signature.'
      : 'Expected payload.signature (2 sections).',
  })

  const decoded = decodeCapabilityToken(trimmed)
  if (!decoded) {
    checks.push({
      id: 'token-decode',
      label: 'Token Decode',
      status: 'fail',
      detail: 'Unable to decode token payload.',
    })
    return {
      principal: null,
      keyId: null,
      audience: null,
      tokenId: null,
      expiresAt: null,
      checks,
    }
  }

  const keyId = normalizeString(decoded.key_id)
  const claims = decoded.claims ?? {}
  const principal = normalizeString(claims.principal)
  const audience = normalizeString(claims.audience)
  const tokenId = normalizeString(claims.token_id)
  const expiresAt = normalizeString(claims.expires_at)
  const scopes = Array.isArray(claims.scopes)
    ? claims.scopes.filter(
        (scope): scope is DecodedCapabilityScope =>
          Boolean(
            scope &&
              typeof scope.resource === 'string' &&
              Array.isArray(scope.actions) &&
              scope.actions.every((action) => typeof action === 'string'),
          ),
      )
    : []

  checks.push({
    id: 'token-claims',
    label: 'Token Claims',
    status: principal ? 'pass' : 'fail',
    detail: principal
      ? `Decoded token for principal ${principal}.`
      : 'Token payload is missing principal.',
  })

  if (expiresAt) {
    const expiresMs = Date.parse(expiresAt)
    const isExpired = Number.isFinite(expiresMs) && expiresMs <= Date.now()
    checks.push({
      id: 'token-expiry',
      label: 'Token Expiry',
      status: isExpired ? 'fail' : 'pass',
      detail: isExpired ? `Token expired at ${expiresAt}.` : `Token expires at ${expiresAt}.`,
    })
  } else {
    checks.push({
      id: 'token-expiry-missing',
      label: 'Token Expiry',
      status: 'warn',
      detail: 'Token payload does not include expires_at.',
    })
  }

  if (!snapshot) {
    checks.push({
      id: 'snapshot-available',
      label: 'Auth Snapshot',
      status: 'warn',
      detail: 'Broker snapshot unavailable. Save settings and refresh to validate principal and policy.',
    })
    return {
      principal,
      keyId,
      audience,
      tokenId,
      expiresAt,
      checks,
    }
  }

  const audienceMatches = Boolean(audience && audience === snapshot.auth.audience)
  checks.push({
    id: 'audience-match',
    label: 'Audience Match',
    status: audienceMatches ? 'pass' : 'fail',
    detail: audienceMatches
      ? `Token audience matches broker audience ${snapshot.auth.audience}.`
      : `Token audience ${audience ?? '-'} does not match broker audience ${snapshot.auth.audience}.`,
  })

  const principalView = principal
    ? snapshot.auth.principals.find((candidate) => candidate.id === principal)
    : undefined
  checks.push({
    id: 'principal-registered',
    label: 'Principal Registration',
    status: principalView ? 'pass' : 'fail',
    detail: principalView
      ? `Principal ${principalView.id} is registered.`
      : `Principal ${principal ?? '-'} is not in auth.principals.`,
  })

  if (principalView) {
    checks.push({
      id: 'principal-status',
      label: 'Principal Status',
      status: principalView.status === 'active' ? 'pass' : 'fail',
      detail:
        principalView.status === 'active'
          ? `Principal status is active (${principalView.quota_profile} quota profile).`
          : `Principal status is ${principalView.status}.`,
    })

    const revokedPrincipal = snapshot.auth.revocations.revoked_principals.includes(principalView.id)
    checks.push({
      id: 'principal-revoked',
      label: 'Principal Revocation',
      status: revokedPrincipal ? 'fail' : 'pass',
      detail: revokedPrincipal ? 'Principal is revoked.' : 'Principal is not revoked.',
    })
  }

  if (keyId) {
    const issuer = snapshot.auth.issuers.find((candidate) => candidate.key_id === keyId)
    if (!issuer) {
      checks.push({
        id: 'issuer-registered',
        label: 'Issuer Registration',
        status: 'fail',
        detail: `Key ${keyId} is not in broker issuer registry.`,
      })
    } else {
      checks.push({
        id: 'issuer-status',
        label: 'Issuer Status',
        status: issuer.status === 'active' || issuer.status === 'rotating' ? 'pass' : 'fail',
        detail: `Issuer ${issuer.key_id} status is ${issuer.status}.`,
      })
    }

    const keyRevoked = snapshot.auth.revocations.revoked_key_ids.includes(keyId)
    checks.push({
      id: 'key-revoked',
      label: 'Issuer Key Revocation',
      status: keyRevoked ? 'fail' : 'pass',
      detail: keyRevoked ? `Issuer key ${keyId} is revoked.` : `Issuer key ${keyId} is not revoked.`,
    })

    if (principalView) {
      const keyAllowed =
        principalView.allowed_key_ids.length === 0 || principalView.allowed_key_ids.includes(keyId)
      checks.push({
        id: 'principal-key-allowlist',
        label: 'Principal Key Allowlist',
        status: keyAllowed ? 'pass' : 'fail',
        detail: keyAllowed
          ? `Principal allows key ${keyId}.`
          : `Principal does not allow key ${keyId}.`,
      })
    }
  } else {
    checks.push({
      id: 'issuer-key-missing',
      label: 'Issuer Key Claim',
      status: 'warn',
      detail: 'Token payload is missing key_id.',
    })
  }

  if (scopes.length === 0) {
    checks.push({
      id: 'scope-presence',
      label: 'Token Scope Coverage',
      status: 'fail',
      detail: 'Token does not include scopes.',
    })
  } else {
    const missingScopes = REQUIRED_BOOTSTRAP_OPERATIONS.filter(
      (operation) =>
        !scopes.some(
          (scope) =>
            scope.actions.includes(operation.action) &&
            resourcesOverlap(scope.resource, operation.resource),
        ),
    )
    checks.push({
      id: 'scope-coverage',
      label: 'Token Scope Coverage',
      status: missingScopes.length === 0 ? 'pass' : 'fail',
      detail:
        missingScopes.length === 0
          ? `Token covers ${REQUIRED_BOOTSTRAP_OPERATIONS.length} baseline operations.`
          : `Missing scopes for: ${missingScopes.map((item) => item.label).join(', ')}.`,
    })
  }

  const policyRules = extractPolicyRulesFromConfig(configSnapshot)
  if (policyRules.length === 0) {
    checks.push({
      id: 'policy-rules-available',
      label: 'Policy Rules',
      status: 'warn',
      detail: 'No local broker policy rules found in configuration snapshot.',
    })
  } else {
    const principalPolicyRules = principal
      ? policyRules.filter((rule) => rule.principal === principal)
      : []
    checks.push({
      id: 'principal-policy-rules',
      label: 'Principal Policy Rules',
      status: principalPolicyRules.length > 0 ? 'pass' : 'fail',
      detail:
        principalPolicyRules.length > 0
          ? `Found ${principalPolicyRules.length} policy rule(s) for principal.`
          : `No policy rules found for ${principal ?? '-'}.`,
    })

    const missingPolicyOps = REQUIRED_BOOTSTRAP_OPERATIONS.filter(
      (operation) =>
        !principalPolicyRules.some(
          (rule) =>
            rule.actions.includes(operation.action) &&
            resourcesOverlap(rule.resource, operation.resource),
        ),
    )
    checks.push({
      id: 'policy-operation-coverage',
      label: 'Policy Operation Coverage',
      status: missingPolicyOps.length === 0 ? 'pass' : 'fail',
      detail:
        missingPolicyOps.length === 0
          ? `Policy covers ${REQUIRED_BOOTSTRAP_OPERATIONS.length} baseline operations.`
          : `Missing policy coverage for: ${missingPolicyOps.map((item) => item.label).join(', ')}.`,
    })
  }

  checks.push({
    id: 'policy-denial-signal',
    label: 'Policy Denial Signal',
    status: snapshot.metrics.policy_denials > 0 ? 'warn' : 'pass',
    detail:
      snapshot.metrics.policy_denials > 0
        ? `Observed ${snapshot.metrics.policy_denials} policy denials in current metrics snapshot.`
        : 'No policy denials reported in current metrics snapshot.',
  })

  return {
    principal,
    keyId,
    audience,
    tokenId,
    expiresAt,
    checks,
  }
}

function decodeCapabilityToken(token: string): DecodedCapabilityToken | null {
  const parts = token.split('.')
  if (parts.length !== 2 || !parts[0]) {
    return null
  }
  try {
    const raw = decodeBase64Url(parts[0])
    const parsed = JSON.parse(raw) as DecodedCapabilityToken
    return typeof parsed === 'object' && parsed ? parsed : null
  } catch {
    return null
  }
}

function decodeBase64Url(value: string): string {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/')
  const padLength = normalized.length % 4 === 0 ? 0 : 4 - (normalized.length % 4)
  const padded = `${normalized}${'='.repeat(padLength)}`
  return atob(padded)
}

function normalizeString(value: unknown): string | null {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null
}

function extractPolicyRulesFromConfig(configSnapshot: ConfigConsoleSnapshot | null): PolicyRule[] {
  const brokerComponent = configSnapshot?.components.find(
    (component) =>
      component.group === 'broker' &&
      (component.id === 'configs/expressways.example.toml' ||
        component.content.includes('[[policy.rules]]')),
  )
  const content = brokerComponent?.content ?? ''
  if (!content.includes('[[policy.rules]]')) {
    return []
  }

  const rules: PolicyRule[] = []
  const lines = content.split('\n')
  let current: PolicyRule | null = null
  let inPolicyRule = false

  for (const rawLine of lines) {
    const line = rawLine.split('#')[0]?.trim() ?? ''
    if (!line) {
      continue
    }

    if (line === '[[policy.rules]]') {
      if (current && current.principal && current.resource) {
        rules.push(current)
      }
      current = { principal: '', resource: '', actions: [] }
      inPolicyRule = true
      continue
    }

    if (line.startsWith('[[') || line.startsWith('[')) {
      if (inPolicyRule && current && current.principal && current.resource) {
        rules.push(current)
      }
      inPolicyRule = false
      current = null
      continue
    }

    if (!inPolicyRule || !current) {
      continue
    }

    const principalMatch = line.match(/^principal\s*=\s*"([^"]+)"$/)
    if (principalMatch) {
      current.principal = principalMatch[1]
      continue
    }

    const resourceMatch = line.match(/^resource\s*=\s*"([^"]+)"$/)
    if (resourceMatch) {
      current.resource = resourceMatch[1]
      continue
    }

    const actionsMatch = line.match(/^actions\s*=\s*\[(.*)\]$/)
    if (actionsMatch) {
      current.actions = actionsMatch[1]
        .split(',')
        .map((item) => item.trim().replace(/^"/, '').replace(/"$/, ''))
        .filter((item) => item.length > 0)
    }
  }

  if (inPolicyRule && current && current.principal && current.resource) {
    rules.push(current)
  }

  return rules
}

function resourcesOverlap(left: string, right: string): boolean {
  return resourcePatternMatches(left, right) || resourcePatternMatches(right, left)
}

function resourcePatternMatches(pattern: string, value: string): boolean {
  if (pattern === value) {
    return true
  }
  if (pattern.endsWith('*')) {
    const prefix = pattern.slice(0, -1)
    return value.startsWith(prefix)
  }
  return false
}

type DiffLine = {
  kind: 'context' | 'add' | 'remove'
  text: string
}

function buildLineDiff(previousContent: string, nextContent: string): DiffLine[] {
  const before = previousContent.split('\n')
  const after = nextContent.split('\n')

  const rows = before.length + 1
  const cols = after.length + 1
  const dp = Array.from({ length: rows }, () => Array<number>(cols).fill(0))

  for (let i = before.length - 1; i >= 0; i -= 1) {
    for (let j = after.length - 1; j >= 0; j -= 1) {
      if (before[i] === after[j]) {
        dp[i][j] = dp[i + 1][j + 1] + 1
      } else {
        dp[i][j] = Math.max(dp[i + 1][j], dp[i][j + 1])
      }
    }
  }

  const output: DiffLine[] = []
  let i = 0
  let j = 0
  while (i < before.length && j < after.length) {
    if (before[i] === after[j]) {
      output.push({ kind: 'context', text: before[i] })
      i += 1
      j += 1
      continue
    }
    if (dp[i + 1][j] >= dp[i][j + 1]) {
      output.push({ kind: 'remove', text: before[i] })
      i += 1
    } else {
      output.push({ kind: 'add', text: after[j] })
      j += 1
    }
  }
  while (i < before.length) {
    output.push({ kind: 'remove', text: before[i] })
    i += 1
  }
  while (j < after.length) {
    output.push({ kind: 'add', text: after[j] })
    j += 1
  }

  return output
}

export default App
