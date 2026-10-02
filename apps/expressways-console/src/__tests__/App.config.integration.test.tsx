import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import App from '../App'
import { useMonitorStore } from '../store/monitorStore'
import type { ConfigComponentView, ConfigConsoleSnapshot, MonitorSnapshot } from '../types'

const apiMocks = vi.hoisted(() => ({
  fetchSnapshot: vi.fn(),
  consumeTopic: vi.fn(),
  executeAdvancedControl: vi.fn(),
  startRegistryStream: vi.fn(),
  stopRegistryStream: vi.fn(),
  onRegistryStreamEvent: vi.fn(),
  fetchConfigSnapshot: vi.fn(),
  updateConfigComponent: vi.fn(),
  updateConfigSection: vi.fn(),
  listConfigBackups: vi.fn(),
  listConfigAuditEntries: vi.fn(),
  rollbackConfigComponent: vi.fn(),
  restartConfigServices: vi.fn(),
  runConfigServiceAction: vi.fn(),
  runOperatorAction: vi.fn(),
  provisionLocalCredentials: vi.fn(),
}))

vi.mock('../api', () => apiMocks)

function createMonitorSnapshot(): MonitorSnapshot {
  return {
    health: {
      node_name: 'dev-node',
      status: 'ok',
    },
    metrics: {
      uptime_seconds: 1,
      total_requests: 0,
      health_requests: 0,
      admin_requests: 0,
      auth_failures: 0,
      policy_denials: 0,
      quota_denials: 0,
      storage_failures: 0,
      audit_failures: 0,
      publish: {
        requests: 0,
        successes: 0,
        failures: 0,
        average_latency_ms: 0,
        max_latency_ms: 0,
      },
      consume: {
        requests: 0,
        successes: 0,
        failures: 0,
        average_latency_ms: 0,
        max_latency_ms: 0,
      },
      storage: {
        topic_count: 0,
        segment_count: 0,
        total_bytes: 0,
        reclaimed_segments: 0,
        reclaimed_bytes: 0,
        recovered_segments: 0,
        truncated_bytes: 0,
      },
      audit: {
        event_count: 0,
        last_hash: null,
      },
      streams: {
        open_streams: 0,
        opened_streams: 0,
        closed_streams: 0,
        keepalives_sent: 0,
        event_frames_sent: 0,
        events_delivered: 0,
        delivery_failures: 0,
        slow_consumer_drops: 0,
        idle_timeouts: 0,
        watch_stream: {
          requests: 0,
          successes: 0,
          failures: 0,
          average_latency_ms: 0,
          max_latency_ms: 0,
        },
      },
      resilience: {
        service_mode: 'healthy',
        degraded_components: [],
      },
    },
    adopters: [],
    auth: {
      audience: 'expressways',
      issuers: [],
      principals: [],
      revocations: {
        revoked_tokens: [],
        revoked_principals: [],
        revoked_key_ids: [],
      },
    },
    agents: [],
    cursor: 0,
  }
}

function createConfigComponent(): ConfigComponentView {
  return {
    id: 'configs/expressways.example.toml',
    name: 'Expressways Broker',
    group: 'broker',
    description: 'Primary broker config',
    filePath: 'configs/expressways.example.toml',
    exists: true,
    editable: true,
    updatedAtMs: 1710000000000,
    parseError: null,
    sections: [
      {
        key: 'policy',
        kind: 'table',
        summary: 'Policy section',
        formFields: [
          {
            key: 'default_decision',
            label: 'Default Decision',
            kind: 'string',
            value: 'deny',
            description: null,
            validation: {
              required: true,
              min: null,
              max: null,
              allowedValues: ['deny', 'allow'],
            },
          },
        ],
        tableArrays: [
          {
            key: 'rules',
            label: 'Rules',
            description: 'Server-side policy rules',
            entryFields: [
              {
                key: 'principal',
                label: 'Principal',
                kind: 'string',
                value: '',
                description: 'Principal identifier',
                validation: {
                  required: true,
                  min: null,
                  max: null,
                  allowedValues: null,
                },
              },
              {
                key: 'resource',
                label: 'Resource',
                kind: 'string',
                value: '',
                description: 'Resource selector',
                validation: {
                  required: true,
                  min: null,
                  max: null,
                  allowedValues: null,
                },
              },
              {
                key: 'actions',
                label: 'Actions',
                kind: 'string_array',
                value: [],
                description: 'Allowed actions',
                validation: {
                  required: true,
                  min: null,
                  max: null,
                  allowedValues: ['health', 'publish', 'consume', 'admin'],
                },
              },
            ],
            entries: [
              {
                principal: 'local:developer',
                resource: 'topic:*',
                actions: ['publish', 'consume'],
              },
            ],
          },
        ],
      },
    ],
    restartHints: [],
    content: `[policy]
default_decision = "deny"

[[policy.rules]]
principal = "local:developer"
resource = "topic:*"
actions = ["publish", "consume"]
`,
  }
}

function createConfigSnapshot(): ConfigConsoleSnapshot {
  return {
    rootPath: '/tmp/expressways',
    components: [createConfigComponent()],
  }
}

function seedMonitorState(configSnapshot: ConfigConsoleSnapshot): void {
  useMonitorStore.setState({
    draftSettings: {
      transport: 'tcp',
      address: '127.0.0.1:7766',
      socketPath: './tmp/expressways.sock',
      token: 'payload.signature',
    },
    settings: {
      transport: 'tcp',
      address: '127.0.0.1:7766',
      socketPath: './tmp/expressways.sock',
      token: 'payload.signature',
    },
    snapshot: createMonitorSnapshot(),
    metricHistory: [],
    loading: false,
    error: null,
    autoRefresh: false,
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
    configSnapshot,
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
  })
}

describe('Config Console Integration', () => {
  beforeEach(() => {
    const configSnapshot = createConfigSnapshot()
    const component = configSnapshot.components[0]

    apiMocks.fetchSnapshot.mockResolvedValue(createMonitorSnapshot())
    apiMocks.consumeTopic.mockResolvedValue({
      topic: 'tasks',
      messages: [],
      next_offset: 0,
    })
    apiMocks.executeAdvancedControl.mockResolvedValue({
      commandType: 'health',
      guarded: false,
      responseType: 'health',
      response: {},
      attachmentBase64: null,
      attachmentBytes: 0,
      executedAtMs: Date.now(),
    })
    apiMocks.startRegistryStream.mockResolvedValue(undefined)
    apiMocks.stopRegistryStream.mockResolvedValue(undefined)
    apiMocks.onRegistryStreamEvent.mockResolvedValue(() => undefined)
    apiMocks.fetchConfigSnapshot.mockResolvedValue(configSnapshot)
    apiMocks.updateConfigComponent.mockResolvedValue({
      component,
      backupPath: null,
      appliedAtMs: Date.now(),
      restartHints: [],
    })
    apiMocks.updateConfigSection.mockResolvedValue({
      component,
      backupPath: null,
      appliedAtMs: Date.now(),
      restartHints: [],
    })
    apiMocks.listConfigBackups.mockResolvedValue({
      componentId: component.id,
      backups: [],
    })
    apiMocks.listConfigAuditEntries.mockResolvedValue({ entries: [] })
    apiMocks.rollbackConfigComponent.mockResolvedValue({
      component,
      rollbackSource: 'test',
      backupPath: null,
      appliedAtMs: Date.now(),
      restartHints: [],
    })
    apiMocks.restartConfigServices.mockResolvedValue({
      restartedAtMs: Date.now(),
      outcomes: [],
    })
    apiMocks.runConfigServiceAction.mockResolvedValue({
      serviceId: 'expressways-server',
      action: 'status',
      ok: true,
      statusCode: 0,
      message: 'ok',
      stdout: '',
      stderr: '',
      executedAtMs: Date.now(),
    })
    apiMocks.runOperatorAction.mockResolvedValue({
      action: 'verify_first_run',
      target: 'verify-first-run',
      ok: true,
      statusCode: 0,
      message: 'ok',
      stdout: '',
      stderr: '',
      executedAtMs: Date.now(),
    })
    apiMocks.provisionLocalCredentials.mockResolvedValue({
      bundleRoot: '/opt/expressways',
      created: true,
      privateKeyPath: '/opt/expressways/var/auth/issuer.private',
      publicKeyPath: '/opt/expressways/var/auth/issuer.public',
      tokenPath: '/opt/expressways/var/auth/developer.token',
      tokenId: 'token-1',
      expiresAt: '2026-11-02T00:00:00Z',
      message: 'Created owner-protected credentials.',
    })

    seedMonitorState(configSnapshot)
  })

  afterEach(() => {
    cleanup()
  })

  it('saves nested policy.rules entries from form mode with normalized arrays', async () => {
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: 'Config Console' }))
    const applyPolicyButton = await screen.findByRole('button', { name: 'Apply policy' })
    const componentCard = applyPolicyButton.closest('article')
    expect(componentCard).not.toBeNull()
    const scopedComponent = within(componentCard as HTMLElement)

    const rulesLabel = scopedComponent.getByText('Rules (rules)')
    expect(rulesLabel).toBeInTheDocument()
    const rulesCard = rulesLabel.closest('article')
    expect(rulesCard).not.toBeNull()
    const scopedRules = within(rulesCard as HTMLElement)

    const initialTextboxes = scopedRules.getAllByRole('textbox')
    const initialActionsInput = initialTextboxes[2]
    await user.clear(initialActionsInput)
    await user.type(initialActionsInput, 'health, publish')

    await user.click(scopedRules.getByRole('button', { name: 'Add Entry' }))

    const textboxesAfterAdd = scopedRules.getAllByRole('textbox')
    await user.type(textboxesAfterAdd[3], 'local:agent')
    await user.type(textboxesAfterAdd[4], 'topic:tasks')
    await user.type(textboxesAfterAdd[5], 'consume, admin')

    await user.click(applyPolicyButton)

    await waitFor(() => expect(apiMocks.updateConfigSection).toHaveBeenCalledTimes(1))
    const [componentId, sectionKey, values] = apiMocks.updateConfigSection.mock.calls[0] as [
      string,
      string,
      Record<string, unknown>,
    ]
    expect(componentId).toBe('configs/expressways.example.toml')
    expect(sectionKey).toBe('policy')
    expect(values.default_decision).toBe('deny')
    expect(values.rules).toEqual([
      {
        principal: 'local:developer',
        resource: 'topic:*',
        actions: ['health', 'publish'],
      },
      {
        principal: 'local:agent',
        resource: 'topic:tasks',
        actions: ['consume', 'admin'],
      },
    ])
  })

  it('blocks form apply when raw mode has unsaved edits after mode transition', async () => {
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: 'Config Console' }))
    const applyPolicyButton = await screen.findByRole('button', { name: 'Apply policy' })
    const sectionCard = applyPolicyButton.closest('article')
    expect(sectionCard).not.toBeNull()
    const componentCard = sectionCard?.parentElement?.closest('article')
    expect(componentCard).not.toBeNull()
    const scopedComponent = within(componentCard as HTMLElement)

    await user.click(scopedComponent.getByRole('button', { name: 'Raw TOML' }))
    const rawEditor = await scopedComponent.findByDisplayValue(/\[policy\]/)
    await user.type(rawEditor, '\n# dirty change')

    await user.click(scopedComponent.getByRole('button', { name: 'Form' }))
    await user.click(scopedComponent.getByRole('button', { name: 'Apply policy' }))

    expect(apiMocks.updateConfigSection).not.toHaveBeenCalled()
    expect(
      await screen.findByText(
        'Raw TOML has unsaved changes. Apply or reset raw draft before saving form sections.',
      ),
    ).toBeInTheDocument()
  })

  it('provisions packaged credentials for an explicitly selected bundle without displaying secrets', async () => {
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: 'Config Console' }))
    const rootInput = screen.getByRole('textbox', { name: 'Expressways bundle root' })
    await user.type(rootInput, '/opt/expressways')
    await user.click(screen.getByRole('button', { name: 'Provision Credentials' }))

    await waitFor(() =>
      expect(apiMocks.provisionLocalCredentials).toHaveBeenCalledWith('/opt/expressways', false),
    )
    expect(
      (await screen.findAllByText('Created owner-protected credentials.')).length,
    ).toBeGreaterThan(0)
    expect(screen.getByText('Token: /opt/expressways/var/auth/developer.token')).toBeInTheDocument()
  })
})
