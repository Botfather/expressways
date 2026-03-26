import { invoke } from '@tauri-apps/api/core'
import { listen, type UnlistenFn } from '@tauri-apps/api/event'
import type {
  AdvancedControlExecuteResult,
  ConfigBackupsResult,
  ConfigComponentRollbackResult,
  ConfigComponentUpdateResult,
  ConfigConsoleSnapshot,
  ConsoleSettings,
  ConfigRestartServicesResult,
  MonitorSnapshot,
} from './types'
import type {
  RegistryStreamEventPayload,
  TopicConsumeResult,
} from './types'

export async function fetchSnapshot(settings: ConsoleSettings): Promise<MonitorSnapshot> {
  return invoke<MonitorSnapshot>('monitor_snapshot', { settings })
}

export async function consumeTopic(
  settings: ConsoleSettings,
  topic: string,
  offset: number,
  limit: number,
): Promise<TopicConsumeResult> {
  return invoke<TopicConsumeResult>('monitor_consume_topic', {
    settings,
    topic,
    offset,
    limit,
  })
}

export async function executeAdvancedControl(
  settings: ConsoleSettings,
  command: unknown,
  attachmentBase64: string | null,
): Promise<AdvancedControlExecuteResult> {
  const normalizedAttachment = attachmentBase64 && attachmentBase64.trim().length > 0 ? attachmentBase64.trim() : null
  return invoke<AdvancedControlExecuteResult>('monitor_execute_control', {
    settings,
    input: {
      command,
      attachmentBase64: normalizedAttachment,
    },
  })
}

export async function startRegistryStream(
  settings: ConsoleSettings,
  cursor: number | null,
): Promise<void> {
  await invoke('monitor_start_registry_stream', {
    settings,
    cursor,
  })
}

export async function stopRegistryStream(): Promise<void> {
  await invoke('monitor_stop_registry_stream')
}

export async function onRegistryStreamEvent(
  handler: (payload: RegistryStreamEventPayload) => void,
): Promise<UnlistenFn> {
  return listen<RegistryStreamEventPayload>('registry-stream-event', (event) => {
    handler(event.payload)
  })
}

export async function fetchConfigSnapshot(): Promise<ConfigConsoleSnapshot> {
  return invoke<ConfigConsoleSnapshot>('config_console_snapshot')
}

export async function updateConfigComponent(
  componentId: string,
  content: string,
): Promise<ConfigComponentUpdateResult> {
  return invoke<ConfigComponentUpdateResult>('config_console_update_component', {
    input: {
      componentId,
      content,
    },
  })
}

export async function listConfigBackups(
  componentId: string,
  limit = 50,
): Promise<ConfigBackupsResult> {
  return invoke<ConfigBackupsResult>('config_console_list_backups', {
    input: {
      componentId,
      limit,
    },
  })
}

export async function rollbackConfigComponent(
  componentId: string,
  backupPath: string,
): Promise<ConfigComponentRollbackResult> {
  return invoke<ConfigComponentRollbackResult>('config_console_rollback_component', {
    input: {
      componentId,
      backupPath,
    },
  })
}

export async function restartConfigServices(
  serviceIds: string[],
): Promise<ConfigRestartServicesResult> {
  return invoke<ConfigRestartServicesResult>('config_console_restart_services', {
    input: {
      serviceIds,
    },
  })
}
