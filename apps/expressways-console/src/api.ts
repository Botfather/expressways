import { invoke } from '@tauri-apps/api/core'
import { listen, type UnlistenFn } from '@tauri-apps/api/event'
import type { ConsoleSettings, MonitorSnapshot } from './types'
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
