use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{Duration, Utc};
use expressways_auth::{CapabilityIssuer, write_secret_file};
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    Action, AdopterStatusView, AgentCard, AgentQuery, AuthStateView, BrokerMetricsView,
    CapabilityClaims, CapabilityScope, ControlCommand, ControlRequest, ControlResponse,
    RegistryEvent, StoredMessage, StreamFrame,
};
use serde::{Deserialize, Serialize};
use tauri::{Emitter, State};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsoleSettings {
    transport: String,
    address: String,
    socket_path: String,
    token: String,
}

#[derive(Debug, Clone, Serialize)]
struct HealthView {
    node_name: String,
    status: String,
}

#[derive(Debug, Clone, Serialize)]
struct MonitorSnapshot {
    health: HealthView,
    metrics: BrokerMetricsView,
    adopters: Vec<AdopterStatusView>,
    auth: AuthStateView,
    agents: Vec<AgentCard>,
    cursor: u64,
}

#[derive(Debug, Clone, Serialize)]
struct TopicConsumeResult {
    topic: String,
    messages: Vec<StoredMessage>,
    next_offset: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdvancedControlInput {
    command: serde_json::Value,
    #[serde(default)]
    attachment_base64: Option<String>,
    #[serde(default)]
    guard_acknowledged: bool,
    #[serde(default)]
    guard_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdvancedControlResult {
    command_type: String,
    guarded: bool,
    response_type: String,
    response: serde_json::Value,
    attachment_base64: Option<String>,
    attachment_bytes: u64,
    executed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum RegistryStreamEventKind {
    Opened,
    Events,
    Keepalive,
    Closed,
    Error,
}

#[derive(Debug, Clone, Serialize)]
struct RegistryStreamEventPayload {
    kind: RegistryStreamEventKind,
    cursor: Option<u64>,
    events: Vec<RegistryEvent>,
    message: Option<String>,
}

#[derive(Default)]
struct RegistryStreamState {
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigConsoleSnapshot {
    root_path: String,
    components: Vec<ConfigComponentView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigComponentView {
    id: String,
    name: String,
    group: String,
    description: String,
    file_path: String,
    exists: bool,
    editable: bool,
    updated_at_ms: Option<u64>,
    parse_error: Option<String>,
    sections: Vec<ConfigSectionView>,
    restart_hints: Vec<ConfigRestartHint>,
    content: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigSectionView {
    key: String,
    kind: String,
    summary: String,
    form_fields: Vec<ConfigFormFieldView>,
    table_arrays: Vec<ConfigTableArrayView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigFormFieldView {
    key: String,
    label: String,
    kind: String,
    value: serde_json::Value,
    description: Option<String>,
    validation: Option<ConfigFormValidationView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigFormValidationView {
    required: bool,
    min: Option<f64>,
    max: Option<f64>,
    allowed_values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigTableArrayView {
    key: String,
    label: String,
    description: Option<String>,
    entry_fields: Vec<ConfigFormFieldView>,
    entries: Vec<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRestartHint {
    service_id: Option<String>,
    service: String,
    reason: String,
    command: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigComponentUpdateInput {
    component_id: String,
    content: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigSectionUpdateInput {
    component_id: String,
    section_key: String,
    #[serde(default)]
    field_values: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigComponentUpdateResult {
    component: ConfigComponentView,
    backup_path: Option<String>,
    applied_at_ms: u64,
    restart_hints: Vec<ConfigRestartHint>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigBackupEntry {
    backup_path: String,
    created_at_ms: Option<u64>,
    size_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigBackupsResult {
    component_id: String,
    backups: Vec<ConfigBackupEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigBackupsInput {
    component_id: String,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRollbackInput {
    component_id: String,
    backup_path: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRollbackResult {
    component: ConfigComponentView,
    rollback_source: String,
    backup_path: Option<String>,
    applied_at_ms: u64,
    restart_hints: Vec<ConfigRestartHint>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRestartServicesInput {
    service_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRestartServiceOutcome {
    service_id: String,
    ok: bool,
    status_code: Option<i32>,
    message: String,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRestartServicesResult {
    restarted_at_ms: u64,
    outcomes: Vec<ConfigRestartServiceOutcome>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigAuditEntriesInput {
    limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigDiffSummaryView {
    added_lines: u64,
    removed_lines: u64,
    changed_lines: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigAuditEntryView {
    entry_id: String,
    recorded_at_ms: u64,
    actor: String,
    category: String,
    action: String,
    component_id: Option<String>,
    section_key: Option<String>,
    service_id: Option<String>,
    command_type: Option<String>,
    success: Option<bool>,
    status_code: Option<i32>,
    summary: String,
    diff: Option<ConfigDiffSummaryView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigAuditEntriesResult {
    entries: Vec<ConfigAuditEntryView>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceControlInput {
    service_id: String,
    action: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServiceControlResult {
    service_id: String,
    action: String,
    ok: bool,
    status_code: Option<i32>,
    message: String,
    stdout: String,
    stderr: String,
    executed_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperatorActionInput {
    action: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct OperatorActionResult {
    action: String,
    target: String,
    ok: bool,
    status_code: Option<i32>,
    message: String,
    stdout: String,
    stderr: String,
    executed_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialProvisionInput {
    bundle_root: String,
    #[serde(default)]
    refresh_token: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialProvisionResult {
    bundle_root: String,
    created: bool,
    private_key_path: String,
    public_key_path: String,
    token_path: String,
    token_id: Option<String>,
    expires_at: Option<String>,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ProvisionBrokerConfig {
    auth: ProvisionAuthConfig,
}

#[derive(Debug, Deserialize)]
struct ProvisionAuthConfig {
    issuers: Vec<ProvisionIssuerConfig>,
    principals: Vec<ProvisionPrincipalConfig>,
}

#[derive(Debug, Deserialize)]
struct ProvisionIssuerConfig {
    key_id: String,
    public_key_path: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ProvisionPrincipalConfig {
    id: String,
    status: String,
    allowed_key_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct ConfigComponentSpec {
    id: String,
    name: String,
    group: String,
    description: String,
    relative_path: String,
    editable: bool,
}

#[derive(Debug, Clone)]
struct ConfigFormFieldSchema {
    description: &'static str,
    required: bool,
    min: Option<f64>,
    max: Option<f64>,
    allowed_values: &'static [&'static str],
}

#[derive(Debug, Clone)]
struct ConfigTableArrayFieldSchema {
    key: &'static str,
    kind: &'static str,
    description: &'static str,
    required: bool,
    min: Option<f64>,
    max: Option<f64>,
    allowed_values: &'static [&'static str],
}

#[derive(Debug, Clone)]
struct ConfigApplyContext {
    action: &'static str,
    section_key: Option<String>,
    summary: String,
}

static CONFIG_AUDIT_COUNTER: AtomicU64 = AtomicU64::new(1);
static CONFIG_FILE_COUNTER: AtomicU64 = AtomicU64::new(1);
static CONFIG_AUDIT_LOCK: StdMutex<()> = StdMutex::new(());
const MAX_CONFIG_COMPONENT_BYTES: u64 = 1024 * 1024;
const MAX_CONFIG_AUDIT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CONFIG_AUDIT_RECORD_BYTES: u64 = 64 * 1024;

#[tauri::command]
async fn monitor_snapshot(settings: ConsoleSettings) -> Result<MonitorSnapshot, String> {
    if settings.token.trim().is_empty() {
        return Err("capability token is required".to_owned());
    }

    let endpoint = build_endpoint(&settings)?;
    let mut client = Client::connect(endpoint)
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;

    let health = match send_command(&mut client, &settings.token, ControlCommand::Health).await? {
        ControlResponse::Health { node_name, status } => HealthView { node_name, status },
        response => {
            return Err(format!(
                "unexpected response for health command: {}",
                response_name(&response)
            ));
        }
    };

    let metrics =
        match send_command(&mut client, &settings.token, ControlCommand::GetMetrics).await? {
            ControlResponse::Metrics { metrics } => metrics,
            response => {
                return Err(format!(
                    "unexpected response for metrics command: {}",
                    response_name(&response)
                ));
            }
        };

    let adopters =
        match send_command(&mut client, &settings.token, ControlCommand::GetAdopters).await? {
            ControlResponse::Adopters { adopters } => adopters,
            response => {
                return Err(format!(
                    "unexpected response for adopters command: {}",
                    response_name(&response)
                ));
            }
        };

    let auth =
        match send_command(&mut client, &settings.token, ControlCommand::GetAuthState).await? {
            ControlResponse::AuthState { state } => state,
            response => {
                return Err(format!(
                    "unexpected response for auth command: {}",
                    response_name(&response)
                ));
            }
        };

    let (agents, cursor) = match send_command(
        &mut client,
        &settings.token,
        ControlCommand::ListAgents {
            query: AgentQuery {
                include_stale: true,
                ..AgentQuery::default()
            },
        },
    )
    .await?
    {
        ControlResponse::Agents { agents, cursor } => (agents, cursor),
        response => {
            return Err(format!(
                "unexpected response for list_agents command: {}",
                response_name(&response)
            ));
        }
    };

    Ok(MonitorSnapshot {
        health,
        metrics,
        adopters,
        auth,
        agents,
        cursor,
    })
}

#[tauri::command]
async fn monitor_consume_topic(
    settings: ConsoleSettings,
    topic: String,
    offset: u64,
    limit: usize,
) -> Result<TopicConsumeResult, String> {
    if settings.token.trim().is_empty() {
        return Err("capability token is required".to_owned());
    }

    if topic.trim().is_empty() {
        return Err("topic is required".to_owned());
    }

    let endpoint = build_endpoint(&settings)?;
    let mut client = Client::connect(endpoint)
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;

    match send_command(
        &mut client,
        &settings.token,
        ControlCommand::Consume {
            topic: topic.clone(),
            offset,
            limit: limit.max(1),
        },
    )
    .await?
    {
        ControlResponse::Messages {
            topic,
            messages,
            next_offset,
        } => Ok(TopicConsumeResult {
            topic,
            messages,
            next_offset,
        }),
        response => Err(format!(
            "unexpected response for consume command: {}",
            response_name(&response)
        )),
    }
}

#[tauri::command]
async fn monitor_execute_control(
    settings: ConsoleSettings,
    input: AdvancedControlInput,
) -> Result<AdvancedControlResult, String> {
    if settings.token.trim().is_empty() {
        return Err("capability token is required".to_owned());
    }

    let command: ControlCommand = serde_json::from_value(input.command)
        .map_err(|error| format!("invalid command payload: {error}"))?;
    if matches!(command, ControlCommand::OpenAgentWatchStream { .. }) {
        return Err(
            "open_agent_watch_stream is stream-only. Use the Registry Stream tab instead."
                .to_owned(),
        );
    }
    let command_type = command_name(&command).to_owned();
    let guarded = command_requires_guard(&command);
    let guard_reason = normalize_guard_reason(input.guard_reason.as_deref());
    if guarded {
        if !input.guard_acknowledged {
            return Err(format!(
                "command `{command_type}` requires guard acknowledgment before execution"
            ));
        }
        if guard_reason
            .as_ref()
            .map(|value| value.chars().count() < 8)
            .unwrap_or(true)
        {
            return Err(format!(
                "command `{command_type}` requires a guard reason (at least 8 characters)"
            ));
        }
    }

    let request_attachment = decode_optional_base64(input.attachment_base64.as_deref())?;
    let root = config_root_path();
    let actor = config_audit_actor();

    let endpoint = build_endpoint(&settings)?;
    let mut client = Client::connect(endpoint)
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;
    let (response, response_attachment) = match send_command_with_attachment(
        &mut client,
        &settings.token,
        command,
        request_attachment,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            let now = system_time_to_millis(SystemTime::now()).unwrap_or(0);
            let audit_result = append_config_audit_entry(
                &root,
                ConfigAuditEntryView {
                    entry_id: next_config_audit_entry_id(now),
                    recorded_at_ms: now,
                    actor,
                    category: "advanced_control".to_owned(),
                    action: "execute".to_owned(),
                    component_id: None,
                    section_key: None,
                    service_id: None,
                    command_type: Some(command_type.clone()),
                    success: Some(false),
                    status_code: None,
                    summary: format!(
                        "advanced command `{command_type}` failed before response: {error}"
                    ),
                    diff: None,
                },
            );
            return match audit_result {
                Ok(()) => Err(error),
                Err(audit_error) => Err(format!(
                    "{error}; additionally failed to record the operator audit entry: {audit_error}"
                )),
            };
        }
    };
    let response_type = response_name(&response).to_owned();
    let response_json = serde_json::to_value(&response)
        .map_err(|error| format!("failed to serialize response payload: {error}"))?;

    let attachment_bytes = response_attachment
        .as_ref()
        .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
        .unwrap_or(0);
    let attachment_base64 = response_attachment
        .as_ref()
        .map(|bytes| BASE64_STANDARD.encode(bytes));
    let executed_at_ms = system_time_to_millis(SystemTime::now()).unwrap_or(0);
    let guard_suffix = guard_reason
        .as_ref()
        .map(|reason| format!(" Guard reason: {reason}"))
        .unwrap_or_default();
    append_config_audit_entry(
        &root,
        ConfigAuditEntryView {
            entry_id: next_config_audit_entry_id(executed_at_ms),
            recorded_at_ms: executed_at_ms,
            actor,
            category: "advanced_control".to_owned(),
            action: "execute".to_owned(),
            component_id: None,
            section_key: None,
            service_id: None,
            command_type: Some(command_type.clone()),
            success: Some(true),
            status_code: None,
            summary: format!(
                "advanced command `{command_type}` executed (response `{response_type}`).{}",
                guard_suffix
            ),
            diff: None,
        },
    )?;

    Ok(AdvancedControlResult {
        command_type,
        guarded,
        response_type,
        response: response_json,
        attachment_base64,
        attachment_bytes,
        executed_at_ms,
    })
}

#[tauri::command]
async fn monitor_start_registry_stream(
    app: tauri::AppHandle,
    state: State<'_, RegistryStreamState>,
    settings: ConsoleSettings,
    cursor: Option<u64>,
) -> Result<(), String> {
    if settings.token.trim().is_empty() {
        return Err("capability token is required".to_owned());
    }

    let mut task_slot = state.task.lock().await;
    if let Some(task) = task_slot.take() {
        task.abort();
    }

    let task = tokio::spawn(async move {
        let endpoint = match build_endpoint(&settings) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                let _ = emit_registry_stream(
                    &app,
                    RegistryStreamEventPayload {
                        kind: RegistryStreamEventKind::Error,
                        cursor: None,
                        events: Vec::new(),
                        message: Some(error),
                    },
                );
                return;
            }
        };

        let client = match Client::connect(endpoint).await {
            Ok(client) => client,
            Err(error) => {
                let _ = emit_registry_stream(
                    &app,
                    RegistryStreamEventPayload {
                        kind: RegistryStreamEventKind::Error,
                        cursor: None,
                        events: Vec::new(),
                        message: Some(format!("failed to connect: {error}")),
                    },
                );
                return;
            }
        };

        let mut stream = client.into_stream();
        let open = stream
            .open(ControlRequest {
                capability_token: settings.token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query: AgentQuery {
                        include_stale: true,
                        ..AgentQuery::default()
                    },
                    cursor,
                    max_events: 100,
                    wait_timeout_ms: 30_000,
                },
            })
            .await;

        let open_frame = match open {
            Ok(frame) => frame,
            Err(error) => {
                let _ = emit_registry_stream(
                    &app,
                    RegistryStreamEventPayload {
                        kind: RegistryStreamEventKind::Error,
                        cursor: None,
                        events: Vec::new(),
                        message: Some(format!("failed to open stream: {error}")),
                    },
                );
                return;
            }
        };

        if !handle_stream_frame(&app, open_frame) {
            return;
        }

        loop {
            match stream.next_frame().await {
                Ok(Some(frame)) => {
                    if !handle_stream_frame(&app, frame) {
                        break;
                    }
                }
                Ok(None) => {
                    let _ = emit_registry_stream(
                        &app,
                        RegistryStreamEventPayload {
                            kind: RegistryStreamEventKind::Closed,
                            cursor: None,
                            events: Vec::new(),
                            message: Some("stream ended by server".to_owned()),
                        },
                    );
                    break;
                }
                Err(error) => {
                    let _ = emit_registry_stream(
                        &app,
                        RegistryStreamEventPayload {
                            kind: RegistryStreamEventKind::Error,
                            cursor: None,
                            events: Vec::new(),
                            message: Some(format!("stream read failed: {error}")),
                        },
                    );
                    break;
                }
            }
        }
    });

    *task_slot = Some(task);
    Ok(())
}

#[tauri::command]
async fn monitor_stop_registry_stream(state: State<'_, RegistryStreamState>) -> Result<(), String> {
    let mut task_slot = state.task.lock().await;
    if let Some(task) = task_slot.take() {
        task.abort();
    }
    Ok(())
}

#[tauri::command]
async fn config_console_snapshot() -> Result<ConfigConsoleSnapshot, String> {
    let root = config_root_path();
    let components = load_config_components(&root)?;
    Ok(ConfigConsoleSnapshot {
        root_path: root.display().to_string(),
        components,
    })
}

#[tauri::command]
async fn config_console_update_component(
    input: ConfigComponentUpdateInput,
) -> Result<ConfigComponentUpdateResult, String> {
    let root = config_root_path();
    let Some(spec) = resolve_component_spec(&root, &input.component_id) else {
        return Err(format!(
            "unknown component `{}`; refresh configuration snapshot and retry",
            input.component_id
        ));
    };

    apply_component_content(
        &root,
        &spec,
        input.content,
        ConfigApplyContext {
            action: "apply_component",
            section_key: None,
            summary: "applied component content from raw TOML editor".to_owned(),
        },
    )
}

#[tauri::command]
async fn config_console_update_section(
    input: ConfigSectionUpdateInput,
) -> Result<ConfigComponentUpdateResult, String> {
    let root = config_root_path();
    let Some(spec) = resolve_component_spec(&root, &input.component_id) else {
        return Err(format!(
            "unknown component `{}`; refresh configuration snapshot and retry",
            input.component_id
        ));
    };
    if !spec.editable {
        return Err(format!("component `{}` is not editable", spec.id));
    }
    if input.section_key.trim().is_empty() {
        return Err("section key is required".to_owned());
    }

    let path = root.join(Path::new(&spec.relative_path));
    ensure_existing_path_within_root(&root, &path)?;
    let content =
        read_bounded_utf8_regular_file(&path, MAX_CONFIG_COMPONENT_BYTES).map_err(|error| {
            format!(
                "failed to read {} before section update: {error}",
                path.display()
            )
        })?;
    let next = apply_section_form_update(
        &spec,
        content,
        input.section_key.trim(),
        &input.field_values,
    )?;
    apply_component_content(
        &root,
        &spec,
        next,
        ConfigApplyContext {
            action: "apply_section",
            section_key: Some(input.section_key.trim().to_owned()),
            summary: format!(
                "applied section form update for `{}` with {} field(s)",
                input.section_key.trim(),
                input.field_values.len()
            ),
        },
    )
}

#[tauri::command]
async fn config_console_list_backups(
    input: ConfigBackupsInput,
) -> Result<ConfigBackupsResult, String> {
    let root = config_root_path();
    let Some(spec) = resolve_component_spec(&root, &input.component_id) else {
        return Err(format!(
            "unknown component `{}`; refresh configuration snapshot and retry",
            input.component_id
        ));
    };

    let limit = input.limit.unwrap_or(50).clamp(1, 500);
    let backups = list_component_backups(&root, &spec, limit)?;
    Ok(ConfigBackupsResult {
        component_id: input.component_id,
        backups,
    })
}

#[tauri::command]
async fn config_console_list_audit_entries(
    input: ConfigAuditEntriesInput,
) -> Result<ConfigAuditEntriesResult, String> {
    let root = config_root_path();
    let limit = input.limit.unwrap_or(100).clamp(1, 1000);
    let entries = list_config_audit_entries(&root, limit)?;
    Ok(ConfigAuditEntriesResult { entries })
}

#[tauri::command]
async fn config_console_rollback_component(
    input: ConfigRollbackInput,
) -> Result<ConfigRollbackResult, String> {
    let root = config_root_path();
    let Some(spec) = resolve_component_spec(&root, &input.component_id) else {
        return Err(format!(
            "unknown component `{}`; refresh configuration snapshot and retry",
            input.component_id
        ));
    };

    let backup_path = PathBuf::from(input.backup_path.trim());
    if backup_path.as_os_str().is_empty() {
        return Err("backup path is required".to_owned());
    }

    let backups = list_component_backups(&root, &spec, 500)?;
    let Some(selected) = backups
        .iter()
        .find(|entry| Path::new(&entry.backup_path) == backup_path)
    else {
        return Err("selected backup is not available for this component".to_owned());
    };

    ensure_existing_path_within_root(&root, &backup_path)?;
    let content = read_bounded_utf8_regular_file(&backup_path, MAX_CONFIG_COMPONENT_BYTES)
        .map_err(|error| format!("failed to read backup {}: {error}", backup_path.display()))?;

    let applied = apply_component_content(
        &root,
        &spec,
        content,
        ConfigApplyContext {
            action: "rollback_component",
            section_key: None,
            summary: format!(
                "rolled back component from backup `{}`",
                selected.backup_path
            ),
        },
    )?;
    Ok(ConfigRollbackResult {
        component: applied.component,
        rollback_source: selected.backup_path.clone(),
        backup_path: applied.backup_path,
        applied_at_ms: applied.applied_at_ms,
        restart_hints: applied.restart_hints,
    })
}

#[tauri::command]
async fn config_console_restart_services(
    input: ConfigRestartServicesInput,
) -> Result<ConfigRestartServicesResult, String> {
    let root = config_root_path();
    let actor = config_audit_actor();
    let mut outcomes = Vec::new();
    let mut seen = HashSet::new();

    for raw_service_id in input.service_ids {
        let service_id = raw_service_id.trim().to_owned();
        if service_id.is_empty() || !seen.insert(service_id.clone()) {
            continue;
        }

        if !is_supported_restart_service(&service_id) {
            let outcome = ConfigRestartServiceOutcome {
                service_id,
                ok: false,
                status_code: None,
                message: "unsupported service id".to_owned(),
                stdout: String::new(),
                stderr: String::new(),
            };
            let now = system_time_to_millis(SystemTime::now()).unwrap_or(0);
            append_config_audit_entry(
                &root,
                ConfigAuditEntryView {
                    entry_id: next_config_audit_entry_id(now),
                    recorded_at_ms: now,
                    actor: actor.clone(),
                    category: "service".to_owned(),
                    action: "restart".to_owned(),
                    component_id: None,
                    section_key: None,
                    service_id: Some(outcome.service_id.clone()),
                    command_type: None,
                    success: Some(false),
                    status_code: None,
                    summary: "restart rejected: unsupported service id".to_owned(),
                    diff: None,
                },
            )?;
            outcomes.push(outcome);
            continue;
        }

        let outcome = run_restart_service(&root, &service_id).await;
        let now = system_time_to_millis(SystemTime::now()).unwrap_or(0);
        append_config_audit_entry(
            &root,
            ConfigAuditEntryView {
                entry_id: next_config_audit_entry_id(now),
                recorded_at_ms: now,
                actor: actor.clone(),
                category: "service".to_owned(),
                action: "restart".to_owned(),
                component_id: None,
                section_key: None,
                service_id: Some(outcome.service_id.clone()),
                command_type: None,
                success: Some(outcome.ok),
                status_code: outcome.status_code,
                summary: format!("service restart result: {}", outcome.message),
                diff: None,
            },
        )?;
        outcomes.push(outcome);
    }

    Ok(ConfigRestartServicesResult {
        restarted_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
        outcomes,
    })
}

#[tauri::command]
async fn config_console_service_action(
    input: ServiceControlInput,
) -> Result<ServiceControlResult, String> {
    let root = config_root_path();
    let service_id = input.service_id.trim().to_owned();
    if service_id.is_empty() {
        return Err("service id is required".to_owned());
    }
    if !is_supported_restart_service(&service_id) {
        return Err(format!("unsupported service id `{service_id}`"));
    }

    let action = normalize_service_action(&input.action)
        .ok_or_else(|| format!("unsupported service action `{}`", input.action.trim()))?;
    let result = run_service_action(&root, &service_id, action).await;
    let now = system_time_to_millis(SystemTime::now()).unwrap_or(0);
    append_config_audit_entry(
        &root,
        ConfigAuditEntryView {
            entry_id: next_config_audit_entry_id(now),
            recorded_at_ms: now,
            actor: config_audit_actor(),
            category: "service".to_owned(),
            action: action.to_owned(),
            component_id: None,
            section_key: None,
            service_id: Some(service_id),
            command_type: None,
            success: Some(result.ok),
            status_code: result.status_code,
            summary: format!("service action result: {}", result.message),
            diff: None,
        },
    )?;
    Ok(result)
}

#[tauri::command]
async fn operator_run_action(input: OperatorActionInput) -> Result<OperatorActionResult, String> {
    let root = config_root_path();
    let action = input.action.trim().to_owned();
    if action.is_empty() {
        return Err("operator action is required".to_owned());
    }

    let Some(target) = resolve_operator_make_target(&action) else {
        return Err(format!("unsupported operator action `{action}`"));
    };
    let result = run_operator_make_target(&root, target, &action).await;
    let now = system_time_to_millis(SystemTime::now()).unwrap_or(0);
    append_config_audit_entry(
        &root,
        ConfigAuditEntryView {
            entry_id: next_config_audit_entry_id(now),
            recorded_at_ms: now,
            actor: config_audit_actor(),
            category: "operator".to_owned(),
            action: action.clone(),
            component_id: None,
            section_key: None,
            service_id: None,
            command_type: None,
            success: Some(result.ok),
            status_code: result.status_code,
            summary: format!(
                "operator action target `{target}` result: {}",
                result.message
            ),
            diff: None,
        },
    )?;
    Ok(result)
}

#[tauri::command]
async fn operator_provision_credentials(
    input: CredentialProvisionInput,
) -> Result<CredentialProvisionResult, String> {
    let result = provision_local_credentials(&input.bundle_root, input.refresh_token)?;
    let now = system_time_to_millis(SystemTime::now()).unwrap_or(0);
    append_config_audit_entry(
        Path::new(&result.bundle_root),
        ConfigAuditEntryView {
            entry_id: next_config_audit_entry_id(now),
            recorded_at_ms: now,
            actor: config_audit_actor(),
            category: "operator".to_owned(),
            action: "provision_credentials".to_owned(),
            component_id: None,
            section_key: None,
            service_id: None,
            command_type: None,
            success: Some(true),
            status_code: Some(0),
            summary: result.message.clone(),
            diff: None,
        },
    )?;
    Ok(result)
}

fn provision_local_credentials(
    bundle_root: &str,
    refresh_token: bool,
) -> Result<CredentialProvisionResult, String> {
    let requested_root = bundle_root.trim();
    if requested_root.is_empty() {
        return Err("bundle root is required".to_owned());
    }
    let root = fs::canonicalize(requested_root)
        .map_err(|error| format!("failed to resolve bundle root `{requested_root}`: {error}"))?;
    let root_metadata = fs::symlink_metadata(&root)
        .map_err(|error| format!("failed to inspect bundle root {}: {error}", root.display()))?;
    if !root_metadata.is_dir() {
        return Err(format!(
            "bundle root is not a directory: {}",
            root.display()
        ));
    }

    let config_path = root.join("configs/expressways.example.toml");
    let config_text = read_bounded_utf8_regular_file(&config_path, MAX_CONFIG_COMPONENT_BYTES)
        .map_err(|error| format!("failed to read {}: {error}", config_path.display()))?;
    let config: ProvisionBrokerConfig = toml::from_str(&config_text)
        .map_err(|error| format!("failed to parse {}: {error}", config_path.display()))?;
    let issuer = config
        .auth
        .issuers
        .iter()
        .find(|issuer| issuer.key_id == "dev")
        .ok_or_else(|| "broker config does not define issuer `dev`".to_owned())?;
    if issuer.status != "active" {
        return Err(format!(
            "issuer `dev` must be active before provisioning (found `{}`)",
            issuer.status
        ));
    }
    if issuer.public_key_path != "./var/auth/issuer.public" {
        return Err(format!(
            "issuer `dev` public_key_path must be ./var/auth/issuer.public for packaged provisioning (found `{}`)",
            issuer.public_key_path
        ));
    }
    let principal = config
        .auth
        .principals
        .iter()
        .find(|principal| principal.id == "local:developer")
        .ok_or_else(|| "broker config does not define principal `local:developer`".to_owned())?;
    if principal.status != "active" || !principal.allowed_key_ids.iter().any(|key| key == "dev") {
        return Err("principal `local:developer` must be active and allow issuer `dev`".to_owned());
    }

    for relative in ["var", "var/auth"] {
        let path = root.join(relative);
        if let Ok(metadata) = fs::symlink_metadata(&path)
            && metadata.file_type().is_symlink()
        {
            return Err(format!(
                "refusing to provision through symlinked directory {}",
                path.display()
            ));
        }
    }
    let auth_dir = root.join("var/auth");
    fs::create_dir_all(&auth_dir)
        .map_err(|error| format!("failed to create {}: {error}", auth_dir.display()))?;
    let canonical_auth = fs::canonicalize(&auth_dir)
        .map_err(|error| format!("failed to resolve {}: {error}", auth_dir.display()))?;
    if !canonical_auth.starts_with(&root) {
        return Err("credential directory escapes the selected bundle root".to_owned());
    }

    let private_key_path = auth_dir.join("issuer.private");
    let public_key_path = auth_dir.join("issuer.public");
    let token_path = auth_dir.join("developer.token");
    let paths = [&private_key_path, &public_key_path, &token_path];
    let existing = paths.iter().filter(|path| path.exists()).count();
    if existing == paths.len() {
        for path in paths {
            let metadata = fs::symlink_metadata(path)
                .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(format!(
                    "credential path is not a regular file: {}",
                    path.display()
                ));
            }
        }
        let existing_issuer = CapabilityIssuer::from_private_key_file("dev", &private_key_path)
            .map_err(|error| format!("existing private key is invalid: {error}"))?;
        if refresh_token {
            let token_id = Uuid::now_v7();
            let issued_at = Utc::now();
            let expires_at = issued_at + Duration::days(30);
            let token =
                issue_local_operator_token(&existing_issuer, token_id, issued_at, expires_at)?;
            atomic_replace_private_file(&token_path, token.as_bytes()).map_err(|error| {
                format!(
                    "failed to replace developer token {}: {error}",
                    token_path.display()
                )
            })?;
            return Ok(CredentialProvisionResult {
                bundle_root: root.display().to_string(),
                created: false,
                private_key_path: private_key_path.display().to_string(),
                public_key_path: public_key_path.display().to_string(),
                token_path: token_path.display().to_string(),
                token_id: Some(token_id.to_string()),
                expires_at: Some(expires_at.to_rfc3339()),
                message: "Reissued the 30-day developer capability without rotating issuer keys."
                    .to_owned(),
            });
        }
        return Ok(CredentialProvisionResult {
            bundle_root: root.display().to_string(),
            created: false,
            private_key_path: private_key_path.display().to_string(),
            public_key_path: public_key_path.display().to_string(),
            token_path: token_path.display().to_string(),
            token_id: None,
            expires_at: None,
            message: "Credentials already exist; no secret files were changed.".to_owned(),
        });
    }
    if existing != 0 {
        return Err(
            "credential set is partial; preserve or remove it manually before provisioning"
                .to_owned(),
        );
    }

    let token_id = Uuid::now_v7();
    let issued_at = Utc::now();
    let expires_at = issued_at + Duration::days(30);
    let capability_issuer = CapabilityIssuer::generate("dev");
    let provision_result = (|| -> Result<(), String> {
        capability_issuer
            .write_private_key(&private_key_path)
            .map_err(|error| format!("failed to write private key: {error}"))?;
        capability_issuer
            .write_public_key(&public_key_path)
            .map_err(|error| format!("failed to write public key: {error}"))?;
        let token =
            issue_local_operator_token(&capability_issuer, token_id, issued_at, expires_at)?;
        write_secret_file(&token_path, token.as_bytes())
            .map_err(|error| format!("failed to write developer token: {error}"))?;
        Ok(())
    })();
    if let Err(error) = provision_result {
        for path in paths {
            let _ = fs::remove_file(path);
        }
        return Err(error);
    }

    Ok(CredentialProvisionResult {
        bundle_root: root.display().to_string(),
        created: true,
        private_key_path: private_key_path.display().to_string(),
        public_key_path: public_key_path.display().to_string(),
        token_path: token_path.display().to_string(),
        token_id: Some(token_id.to_string()),
        expires_at: Some(expires_at.to_rfc3339()),
        message: "Created owner-protected local issuer keys and a 30-day developer capability."
            .to_owned(),
    })
}

fn issue_local_operator_token(
    issuer: &CapabilityIssuer,
    token_id: Uuid,
    issued_at: chrono::DateTime<Utc>,
    expires_at: chrono::DateTime<Utc>,
) -> Result<String, String> {
    issuer
        .issue(CapabilityClaims {
            token_id,
            principal: "local:developer".to_owned(),
            audience: "expressways".to_owned(),
            issued_at,
            expires_at,
            scopes: local_operator_scopes(),
        })
        .map_err(|error| format!("failed to issue developer capability: {error}"))
}

fn local_operator_scopes() -> Vec<CapabilityScope> {
    vec![
        CapabilityScope {
            resource: "system:broker".to_owned(),
            actions: vec![Action::Health, Action::Admin],
        },
        CapabilityScope {
            resource: "topic:*".to_owned(),
            actions: vec![Action::Admin, Action::Publish, Action::Consume],
        },
        CapabilityScope {
            resource: "artifact:*".to_owned(),
            actions: vec![Action::Admin, Action::Publish, Action::Consume],
        },
        CapabilityScope {
            resource: "registry:agents*".to_owned(),
            actions: vec![Action::Admin],
        },
    ]
}

fn config_root_path() -> PathBuf {
    if let Ok(root_override) = std::env::var("EXPRESSWAYS_CONFIG_ROOT") {
        let trimmed = root_override.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .ancestors()
        .nth(3)
        .map(Path::to_path_buf)
        .unwrap_or(manifest)
}

fn read_bounded_utf8_regular_file(path: &Path, max_bytes: u64) -> std::io::Result<String> {
    let initial_metadata = fs::symlink_metadata(path)?;
    if !initial_metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path is not a regular file",
        ));
    }

    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path is not a regular file",
        ));
    }
    if metadata.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("file is {} bytes; maximum is {max_bytes}", metadata.len()),
        ));
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "file is too large to read")
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file grew beyond its size limit",
        ));
    }
    String::from_utf8(bytes).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "file is not valid UTF-8")
    })
}

fn write_new_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn atomic_replace_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no file name")
    })?;
    let counter = CONFIG_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".{}.{}.{counter}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| {
        write_new_private_file(&temporary, bytes)?;
        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(path)?;
        }
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn rollback_config_after_audit_failure(path: &Path, previous: Option<&str>) -> std::io::Result<()> {
    if let Some(previous) = previous {
        return atomic_replace_private_file(path, previous.as_bytes());
    }
    fs::remove_file(path)?;
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn ensure_existing_path_within_root(root: &Path, path: &Path) -> Result<(), String> {
    let canonical_root = fs::canonicalize(root)
        .map_err(|error| format!("failed to resolve config root {}: {error}", root.display()))?;
    let canonical_path = fs::canonicalize(path)
        .map_err(|error| format!("failed to resolve {}: {error}", path.display()))?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(format!(
            "path {} escapes config root {}",
            path.display(),
            root.display()
        ));
    }
    Ok(())
}

fn ensure_destination_within_root(root: &Path, path: &Path) -> Result<(), String> {
    let canonical_root = fs::canonicalize(root)
        .map_err(|error| format!("failed to resolve config root {}: {error}", root.display()))?;
    let parent = path
        .parent()
        .ok_or_else(|| format!("path {} has no parent", path.display()))?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|error| format!("failed to resolve {}: {error}", parent.display()))?;
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(format!(
            "destination {} escapes config root {}",
            path.display(),
            root.display()
        ));
    }
    Ok(())
}

fn ensure_directory_tree_within_root(root: &Path, directory: &Path) -> Result<(), String> {
    let relative = directory.strip_prefix(root).map_err(|_| {
        format!(
            "directory {} is outside config root {}",
            directory.display(),
            root.display()
        )
    })?;
    let canonical_root = fs::canonicalize(root)
        .map_err(|error| format!("failed to resolve config root {}: {error}", root.display()))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(_) => {
                return Err(format!(
                    "configuration directory {} is not a real directory",
                    current.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .map_err(|error| format!("failed to create {}: {error}", current.display()))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&current, fs::Permissions::from_mode(0o700)).map_err(
                        |error| {
                            format!(
                                "failed to secure configuration directory {}: {error}",
                                current.display()
                            )
                        },
                    )?;
                }
            }
            Err(error) => {
                return Err(format!("failed to inspect {}: {error}", current.display()));
            }
        }
        let canonical_current = fs::canonicalize(&current)
            .map_err(|error| format!("failed to resolve {}: {error}", current.display()))?;
        if !canonical_current.starts_with(&canonical_root) {
            return Err(format!(
                "configuration directory {} escapes config root {}",
                current.display(),
                root.display()
            ));
        }
    }
    Ok(())
}

fn apply_component_content(
    root: &Path,
    spec: &ConfigComponentSpec,
    content: String,
    context: ConfigApplyContext,
) -> Result<ConfigComponentUpdateResult, String> {
    if content.trim().is_empty() {
        return Err("configuration payload cannot be empty".to_owned());
    }
    if content.len() as u64 > MAX_CONFIG_COMPONENT_BYTES {
        return Err(format!(
            "configuration payload is {} bytes; maximum is {MAX_CONFIG_COMPONENT_BYTES}",
            content.len()
        ));
    }

    let parsed = toml::from_str::<toml::Value>(&content)
        .map_err(|error| format!("invalid TOML: {error}"))?;
    validate_component_content(spec, &parsed)?;

    let path = root.join(Path::new(&spec.relative_path));
    if let Some(parent) = path.parent() {
        ensure_directory_tree_within_root(root, parent)?;
    }
    ensure_destination_within_root(root, &path)?;

    let existing_content = match read_bounded_utf8_regular_file(&path, MAX_CONFIG_COMPONENT_BYTES) {
        Ok(existing) => Some(existing),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(format!(
                "failed to read existing {} before apply: {error}",
                path.display()
            ));
        }
    };
    let backup_path = match existing_content.as_ref() {
        Some(existing) => Some(write_component_backup(root, &spec.relative_path, existing)?),
        None => None,
    };

    let mut output = content;
    if !output.ends_with('\n') && (output.len() as u64) < MAX_CONFIG_COMPONENT_BYTES {
        output.push('\n');
    }
    atomic_replace_private_file(&path, output.as_bytes())
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;

    let restart_hints = restart_hints_for_component(spec);
    let component = load_component(root, spec.clone())?;
    let applied_at_ms = system_time_to_millis(SystemTime::now()).unwrap_or(0);
    let diff_summary =
        config_diff_summary(existing_content.as_deref().unwrap_or_default(), &output);
    let mut summary = context.summary;
    if let Some(backup) = backup_path.as_ref() {
        summary.push_str(&format!(" Backup: {backup}."));
    }
    let audit_result = append_config_audit_entry(
        root,
        ConfigAuditEntryView {
            entry_id: next_config_audit_entry_id(applied_at_ms),
            recorded_at_ms: applied_at_ms,
            actor: config_audit_actor(),
            category: "config".to_owned(),
            action: context.action.to_owned(),
            component_id: Some(spec.id.clone()),
            section_key: context.section_key,
            service_id: None,
            command_type: None,
            success: Some(true),
            status_code: None,
            summary,
            diff: Some(diff_summary),
        },
    );
    if let Err(audit_error) = audit_result {
        return match rollback_config_after_audit_failure(&path, existing_content.as_deref()) {
            Ok(()) => Err(format!(
                "configuration update was rolled back because its audit entry failed: {audit_error}"
            )),
            Err(rollback_error) => Err(format!(
                "configuration audit failed ({audit_error}) and rollback also failed ({rollback_error})"
            )),
        };
    }

    Ok(ConfigComponentUpdateResult {
        component,
        backup_path,
        applied_at_ms,
        restart_hints,
    })
}

fn load_config_components(root: &Path) -> Result<Vec<ConfigComponentView>, String> {
    let mut components = Vec::new();
    for spec in component_specs(root) {
        components.push(load_component(root, spec)?);
    }
    Ok(components)
}

fn resolve_component_spec(root: &Path, component_id: &str) -> Option<ConfigComponentSpec> {
    component_specs(root)
        .into_iter()
        .find(|spec| spec.id == component_id)
}

fn component_specs(root: &Path) -> Vec<ConfigComponentSpec> {
    let mut specs = known_component_specs();
    discover_component_specs(
        root,
        "configs",
        "broker",
        "Discovered broker configuration file",
        &mut specs,
    );
    discover_component_specs(
        root,
        "var/agent/nanobot-system",
        "nanobot",
        "Discovered Nanobot system configuration file",
        &mut specs,
    );

    specs.sort_by(|left, right| {
        left.group
            .cmp(&right.group)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });
    specs
}

fn known_component_specs() -> Vec<ConfigComponentSpec> {
    vec![
        ConfigComponentSpec {
            id: "configs/expressways.example.toml".to_owned(),
            name: "Expressways Broker".to_owned(),
            group: "broker".to_owned(),
            description:
                "Primary broker configuration: server, auth, quotas, policy, storage, and adopters"
                    .to_owned(),
            relative_path: "configs/expressways.example.toml".to_owned(),
            editable: true,
        },
        ConfigComponentSpec {
            id: "var/agent/nanobot-system/nanobot-system.toml".to_owned(),
            name: "Nanobot System Wiring".to_owned(),
            group: "nanobot".to_owned(),
            description: "Generated topic wiring for Nanobot parity runtime and bridge integration"
                .to_owned(),
            relative_path: "var/agent/nanobot-system/nanobot-system.toml".to_owned(),
            editable: true,
        },
        ConfigComponentSpec {
            id: "var/agent/nanobot-system/nanobot-auth-policy-snippets.toml".to_owned(),
            name: "Nanobot Auth and Policy Snippets".to_owned(),
            group: "nanobot".to_owned(),
            description: "Generated principal, quota, and policy snippets for Nanobot components"
                .to_owned(),
            relative_path: "var/agent/nanobot-system/nanobot-auth-policy-snippets.toml".to_owned(),
            editable: true,
        },
    ]
}

fn discover_component_specs(
    root: &Path,
    relative_dir: &str,
    group: &str,
    description: &str,
    specs: &mut Vec<ConfigComponentSpec>,
) {
    let path = root.join(relative_dir);
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };

    for entry in entries.flatten() {
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }

        let file_path = entry.path();
        let is_toml = file_path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| extension.eq_ignore_ascii_case("toml"))
            .unwrap_or(false);
        if !is_toml {
            continue;
        }

        let Some(relative_path) = normalize_relative_path(root, &file_path) else {
            continue;
        };
        if specs.iter().any(|spec| spec.id == relative_path) {
            continue;
        }

        let stem = file_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("config");
        specs.push(ConfigComponentSpec {
            id: relative_path.clone(),
            name: title_case_identifier(stem),
            group: group.to_owned(),
            description: description.to_owned(),
            relative_path,
            editable: true,
        });
    }
}

fn load_component(root: &Path, spec: ConfigComponentSpec) -> Result<ConfigComponentView, String> {
    let file_path = root.join(Path::new(&spec.relative_path));
    let restart_hints = restart_hints_for_component(&spec);
    let metadata = match fs::symlink_metadata(&file_path) {
        Ok(metadata) if metadata.file_type().is_file() => Some(metadata),
        Ok(_) => {
            return Err(format!(
                "configuration path {} is not a regular file",
                file_path.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(format!(
                "failed to inspect {}: {error}",
                file_path.display()
            ));
        }
    };
    let exists = metadata.is_some();
    let updated_at_ms = metadata
        .and_then(|meta| meta.modified().ok())
        .and_then(system_time_to_millis);

    let mut content = String::new();
    let mut parse_error = None;
    let mut sections = Vec::new();

    if exists {
        ensure_existing_path_within_root(root, &file_path)?;
        content = read_bounded_utf8_regular_file(&file_path, MAX_CONFIG_COMPONENT_BYTES)
            .map_err(|error| format!("failed to read {}: {error}", file_path.display()))?;
        match component_sections(&spec, &content) {
            Ok(parsed_sections) => sections = parsed_sections,
            Err(error) => parse_error = Some(error),
        }
    }

    Ok(ConfigComponentView {
        id: spec.id,
        name: spec.name,
        group: spec.group,
        description: spec.description,
        file_path: file_path.display().to_string(),
        exists,
        editable: spec.editable,
        updated_at_ms,
        parse_error,
        sections,
        restart_hints,
        content,
    })
}

fn write_component_backup(
    root: &Path,
    relative_path: &str,
    content: &str,
) -> Result<String, String> {
    let backup_root = backup_root_path(root);
    ensure_directory_tree_within_root(root, &backup_root)?;
    ensure_destination_within_root(root, &backup_root.join("containment-check"))?;

    let timestamp = system_time_to_millis(SystemTime::now()).unwrap_or(0);
    let counter = CONFIG_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let backup_name = format!(
        "{}.{}.{counter}.bak.toml",
        relative_path.replace('/', "__"),
        timestamp
    );
    let backup_path = backup_root.join(backup_name);
    write_new_private_file(&backup_path, content.as_bytes())
        .map_err(|error| format!("failed to write backup {}: {error}", backup_path.display()))?;
    Ok(backup_path.display().to_string())
}

fn list_component_backups(
    root: &Path,
    spec: &ConfigComponentSpec,
    limit: usize,
) -> Result<Vec<ConfigBackupEntry>, String> {
    let backup_root = backup_root_path(root);
    match fs::symlink_metadata(&backup_root) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err("configuration backup path is not a real directory".to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "failed to inspect backup directory {}: {error}",
                backup_root.display()
            ));
        }
    };
    ensure_destination_within_root(root, &backup_root.join("containment-check"))?;
    let entries = fs::read_dir(&backup_root)
        .map_err(|error| format!("failed to read {}: {error}", backup_root.display()))?;

    let prefix = format!("{}.", backup_filename_prefix(&spec.relative_path));
    let mut backups = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if !file_name.starts_with(&prefix) || !file_name.ends_with(".bak.toml") {
            continue;
        }

        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if !metadata.is_file() {
            continue;
        }

        backups.push(ConfigBackupEntry {
            backup_path: path.display().to_string(),
            created_at_ms: metadata.modified().ok().and_then(system_time_to_millis),
            size_bytes: metadata.len(),
        });
    }

    backups.sort_by(|left, right| {
        right
            .created_at_ms
            .cmp(&left.created_at_ms)
            .then_with(|| right.backup_path.cmp(&left.backup_path))
    });
    backups.truncate(limit.max(1));
    Ok(backups)
}

fn backup_root_path(root: &Path) -> PathBuf {
    root.join("var/agent/config-backups")
}

fn backup_filename_prefix(relative_path: &str) -> String {
    relative_path.replace('/', "__")
}

fn restart_hints_for_component(spec: &ConfigComponentSpec) -> Vec<ConfigRestartHint> {
    let mut hints = Vec::new();

    if spec.relative_path == "configs/expressways.example.toml" {
        hints.push(ConfigRestartHint {
            service_id: Some("expressways-server".to_owned()),
            service: "expressways-server".to_owned(),
            reason: "Broker config changed; restart broker to apply auth/policy/quota and runtime settings.".to_owned(),
            command: Some(
                "bash scripts/expressways-service.sh restart expressways-server".to_owned(),
            ),
        });
    }

    if spec.relative_path.ends_with("/nanobot-system.toml") {
        hints.push(ConfigRestartHint {
            service_id: Some("nanobot-runtime".to_owned()),
            service: "expressways-nanobot-system runtime".to_owned(),
            reason:
                "Nanobot wiring changed; restart runtime so topic and behavior settings reload."
                    .to_owned(),
            command: Some("bash scripts/expressways-service.sh restart nanobot-runtime".to_owned()),
        });
    }

    if spec
        .relative_path
        .ends_with("/nanobot-auth-policy-snippets.toml")
    {
        hints.push(ConfigRestartHint {
            service_id: None,
            service: "expressways-server".to_owned(),
            reason:
                "Snippet updates are inert until merged into broker config and broker restarted."
                    .to_owned(),
            command: None,
        });
    }

    hints
}

fn is_supported_restart_service(service_id: &str) -> bool {
    matches!(service_id, "expressways-server" | "nanobot-runtime")
}

async fn run_restart_service(root: &Path, service_id: &str) -> ConfigRestartServiceOutcome {
    let result = run_service_action(root, service_id, "restart").await;
    ConfigRestartServiceOutcome {
        service_id: result.service_id,
        ok: result.ok,
        status_code: result.status_code,
        message: result.message,
        stdout: result.stdout,
        stderr: result.stderr,
    }
}

fn normalize_service_action(action: &str) -> Option<&'static str> {
    match action.trim().to_ascii_lowercase().as_str() {
        "start" => Some("start"),
        "stop" => Some("stop"),
        "restart" => Some("restart"),
        "status" => Some("status"),
        _ => None,
    }
}

async fn run_service_action(root: &Path, service_id: &str, action: &str) -> ServiceControlResult {
    let root = root.to_path_buf();
    let service_id_owned = service_id.to_owned();
    let action_owned = action.to_owned();
    let result = tokio::task::spawn_blocking(move || {
        std::process::Command::new("bash")
            .arg("scripts/expressways-service.sh")
            .arg(action_owned)
            .arg(service_id_owned)
            .current_dir(root)
            .output()
    })
    .await;

    match result {
        Ok(Ok(output)) => {
            let status_code = output.status.code();
            let ok = output.status.success();
            let stdout = truncate_output(String::from_utf8_lossy(&output.stdout).to_string());
            let stderr = truncate_output(String::from_utf8_lossy(&output.stderr).to_string());
            let message = if ok {
                format!("{action} completed")
            } else {
                format!("{action} failed")
            };
            ServiceControlResult {
                service_id: service_id.to_owned(),
                action: action.to_owned(),
                ok,
                status_code,
                message,
                stdout,
                stderr,
                executed_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
            }
        }
        Ok(Err(error)) => ServiceControlResult {
            service_id: service_id.to_owned(),
            action: action.to_owned(),
            ok: false,
            status_code: None,
            message: format!("failed to execute {action} command: {error}"),
            stdout: String::new(),
            stderr: String::new(),
            executed_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
        },
        Err(error) => ServiceControlResult {
            service_id: service_id.to_owned(),
            action: action.to_owned(),
            ok: false,
            status_code: None,
            message: format!("{action} worker failed: {error}"),
            stdout: String::new(),
            stderr: String::new(),
            executed_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
        },
    }
}

fn resolve_operator_make_target(action: &str) -> Option<&'static str> {
    match action.trim() {
        "bootstrap_local" => Some("bootstrap-local"),
        "generate_admin_token" => Some("generate-admin-token"),
        "verify_first_run" => Some("verify-first-run"),
        "export_support_bundle" => Some("export-support-bundle"),
        _ => None,
    }
}

async fn run_operator_make_target(root: &Path, target: &str, action: &str) -> OperatorActionResult {
    let root = root.to_path_buf();
    let target_owned = target.to_owned();
    let result = tokio::task::spawn_blocking(move || {
        std::process::Command::new("make")
            .arg(&target_owned)
            .current_dir(root)
            .output()
    })
    .await;

    match result {
        Ok(Ok(output)) => {
            let status_code = output.status.code();
            let ok = output.status.success();
            let stdout = truncate_output(String::from_utf8_lossy(&output.stdout).to_string());
            let stderr = truncate_output(String::from_utf8_lossy(&output.stderr).to_string());
            let message = if ok {
                format!("{target} completed")
            } else {
                format!("{target} failed")
            };
            OperatorActionResult {
                action: action.to_owned(),
                target: target.to_owned(),
                ok,
                status_code,
                message,
                stdout,
                stderr,
                executed_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
            }
        }
        Ok(Err(error)) => OperatorActionResult {
            action: action.to_owned(),
            target: target.to_owned(),
            ok: false,
            status_code: None,
            message: format!("failed to execute make target `{target}`: {error}"),
            stdout: String::new(),
            stderr: String::new(),
            executed_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
        },
        Err(error) => OperatorActionResult {
            action: action.to_owned(),
            target: target.to_owned(),
            ok: false,
            status_code: None,
            message: format!("make worker failed for `{target}`: {error}"),
            stdout: String::new(),
            stderr: String::new(),
            executed_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
        },
    }
}

fn truncate_output(text: String) -> String {
    let max_chars = 4000usize;
    if text.chars().count() <= max_chars {
        return text.trim().to_owned();
    }

    let mut out = String::with_capacity(max_chars + 3);
    for (index, ch) in text.chars().enumerate() {
        if index >= max_chars {
            break;
        }
        out.push(ch);
    }
    out.push_str("...");
    out
}

fn normalize_relative_path(root: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(root).ok().map(|relative| {
        relative
            .iter()
            .map(|segment| segment.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    })
}

fn component_sections(
    spec: &ConfigComponentSpec,
    content: &str,
) -> Result<Vec<ConfigSectionView>, String> {
    let value = toml::from_str::<toml::Value>(content)
        .map_err(|error| format!("TOML parse failed: {error}"))?;
    let Some(table) = value.as_table() else {
        return Ok(Vec::new());
    };

    let mut sections = Vec::with_capacity(table.len());
    for (key, value) in table {
        sections.push(ConfigSectionView {
            key: key.to_owned(),
            kind: toml_kind(value).to_owned(),
            summary: toml_summary(value),
            form_fields: section_form_fields(spec, key, value),
            table_arrays: section_table_arrays(spec, key, value),
        });
    }
    Ok(sections)
}

fn apply_section_form_update(
    spec: &ConfigComponentSpec,
    content: String,
    section_key: &str,
    field_values: &BTreeMap<String, serde_json::Value>,
) -> Result<String, String> {
    if field_values.is_empty() {
        return Err("at least one form field value is required".to_owned());
    }

    let mut root_value = toml::from_str::<toml::Value>(&content)
        .map_err(|error| format!("invalid TOML: {error}"))?;
    let Some(root_table) = root_value.as_table_mut() else {
        return Err("component root must be a TOML table".to_owned());
    };

    let Some(current_section) = root_table.get(section_key).cloned() else {
        return Err(format!("unknown section `{section_key}`"));
    };
    let supports_scalar_fields =
        !section_form_fields(spec, section_key, &current_section).is_empty();
    let supports_table_arrays =
        !section_table_arrays(spec, section_key, &current_section).is_empty();
    if !supports_scalar_fields && !supports_table_arrays {
        return Err(format!(
            "section `{section_key}` does not support form mode; use raw TOML mode"
        ));
    }

    let Some(section_value) = root_table.get_mut(section_key) else {
        return Err(format!("unknown section `{section_key}`"));
    };
    let Some(section_table) = section_value.as_table_mut() else {
        return Err(format!("section `{section_key}` is not a TOML table"));
    };

    for (field_key, field_value) in field_values {
        let Some(current_value) = section_table.get(field_key).cloned() else {
            return Err(format!(
                "field `{section_key}.{field_key}` is not present in current section"
            ));
        };
        let parsed_value =
            parse_section_form_value(section_key, field_key, field_value, &current_value)?;
        validate_section_form_value(section_key, field_key, &parsed_value)?;
        validate_section_nested_field_value(section_key, field_key, &parsed_value)?;
        section_table.insert(field_key.clone(), parsed_value);
    }
    validate_section_table_consistency(section_key, section_table)?;

    toml::to_string_pretty(&root_value)
        .map_err(|error| format!("failed to serialize updated config: {error}"))
}

fn section_form_fields(
    spec: &ConfigComponentSpec,
    section_key: &str,
    value: &toml::Value,
) -> Vec<ConfigFormFieldView> {
    if !is_core_form_section(spec, section_key) {
        return Vec::new();
    }

    let Some(table) = value.as_table() else {
        return Vec::new();
    };

    let mut fields = Vec::new();
    for (field_key, field_value) in table {
        let Some((kind, serialized_value)) = toml_form_field_value(field_value) else {
            continue;
        };
        let schema = config_form_field_schema(section_key, field_key);
        fields.push(ConfigFormFieldView {
            key: field_key.to_owned(),
            label: title_case_identifier(field_key),
            kind: kind.to_owned(),
            value: serialized_value,
            description: schema.as_ref().map(|item| item.description.to_owned()),
            validation: schema.as_ref().map(config_form_validation_view),
        });
    }
    fields.sort_by(|left, right| left.key.cmp(&right.key));
    fields
}

fn section_table_arrays(
    spec: &ConfigComponentSpec,
    section_key: &str,
    value: &toml::Value,
) -> Vec<ConfigTableArrayView> {
    if !is_core_form_section(spec, section_key) {
        return Vec::new();
    }

    let Some(table) = value.as_table() else {
        return Vec::new();
    };

    let mut arrays = Vec::new();
    for (field_key, field_value) in table {
        let Some(items) = field_value.as_array() else {
            continue;
        };
        if !items.iter().all(toml::Value::is_table) {
            continue;
        }

        let Some(base_schemas) = config_table_array_field_schemas(section_key, field_key) else {
            continue;
        };
        let mut entry_fields = base_schemas
            .iter()
            .map(|schema| ConfigFormFieldView {
                key: schema.key.to_owned(),
                label: title_case_identifier(schema.key),
                kind: schema.kind.to_owned(),
                value: default_form_field_value(schema.kind),
                description: Some(schema.description.to_owned()),
                validation: Some(ConfigFormValidationView {
                    required: schema.required,
                    min: schema.min,
                    max: schema.max,
                    allowed_values: (!schema.allowed_values.is_empty()).then(|| {
                        schema
                            .allowed_values
                            .iter()
                            .map(|value| (*value).to_owned())
                            .collect()
                    }),
                }),
            })
            .collect::<Vec<_>>();

        let mut discovered_fields: BTreeMap<String, String> = BTreeMap::new();
        let known_keys = base_schemas
            .iter()
            .map(|schema| schema.key)
            .collect::<HashSet<_>>();
        let mut entries = Vec::new();
        for item in items {
            let Some(item_table) = item.as_table() else {
                continue;
            };
            let mut entry = BTreeMap::new();

            for schema in &base_schemas {
                let value = item_table
                    .get(schema.key)
                    .and_then(toml_entry_to_json)
                    .unwrap_or_else(|| default_form_field_value(schema.kind));
                entry.insert(schema.key.to_owned(), value);
            }

            for (extra_key, extra_value) in item_table {
                if known_keys.contains(extra_key.as_str()) {
                    continue;
                }
                let Some((kind, serialized)) = toml_form_field_value(extra_value) else {
                    continue;
                };
                entry.insert(extra_key.clone(), serialized);
                discovered_fields
                    .entry(extra_key.clone())
                    .or_insert_with(|| kind.to_owned());
            }
            entries.push(entry);
        }

        for (key, kind) in discovered_fields {
            entry_fields.push(ConfigFormFieldView {
                key: key.clone(),
                label: title_case_identifier(&key),
                kind,
                value: serde_json::Value::Null,
                description: Some(
                    "Field discovered from existing config entry (no strict validation schema)."
                        .to_owned(),
                ),
                validation: None,
            });
        }
        entry_fields.sort_by(|left, right| left.key.cmp(&right.key));

        arrays.push(ConfigTableArrayView {
            key: field_key.to_owned(),
            label: title_case_identifier(field_key),
            description: config_table_array_description(section_key, field_key).map(str::to_owned),
            entry_fields,
            entries,
        });
    }

    arrays.sort_by(|left, right| left.key.cmp(&right.key));
    arrays
}

fn is_core_form_section(spec: &ConfigComponentSpec, section_key: &str) -> bool {
    if spec.group != "broker" {
        return false;
    }

    matches!(
        section_key,
        "server"
            | "storage"
            | "audit"
            | "resilience"
            | "registry"
            | "auth"
            | "adopters"
            | "policy"
            | "quotas"
    )
}

fn command_requires_guard(command: &ControlCommand) -> bool {
    matches!(
        command,
        ControlCommand::RegisterAgent { .. }
            | ControlCommand::HeartbeatAgent { .. }
            | ControlCommand::CleanupStaleAgents
            | ControlCommand::RemoveAgent { .. }
            | ControlCommand::CreateTopic { .. }
            | ControlCommand::RevokeToken { .. }
            | ControlCommand::RevokePrincipal { .. }
            | ControlCommand::RevokeKey { .. }
            | ControlCommand::PutArtifact { .. }
            | ControlCommand::Publish { .. }
    )
}

fn normalize_guard_reason(input: Option<&str>) -> Option<String> {
    let raw = input?;
    let normalized = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn config_audit_root_path(root: &Path) -> PathBuf {
    root.join("var/agent/config-audit")
}

fn config_audit_log_path(root: &Path) -> PathBuf {
    config_audit_root_path(root).join("entries.jsonl")
}

fn next_config_audit_entry_id(recorded_at_ms: u64) -> String {
    let counter = CONFIG_AUDIT_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{recorded_at_ms}-{counter:06}")
}

fn config_audit_actor() -> String {
    if let Ok(value) = std::env::var("EXPRESSWAYS_CONSOLE_ACTOR") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }

    let user = std::env::var("USER")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("USERNAME")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    format!("console:{user}")
}

fn append_config_audit_entry(root: &Path, entry: ConfigAuditEntryView) -> Result<(), String> {
    let audit_root = config_audit_root_path(root);
    ensure_directory_tree_within_root(root, &audit_root)?;

    let path = config_audit_log_path(root);
    let serialized = serde_json::to_string(&entry)
        .map_err(|error| format!("failed to serialize config audit entry: {error}"))?;
    if serialized.len() as u64 > MAX_CONFIG_AUDIT_RECORD_BYTES {
        return Err(format!(
            "config audit entry is {} bytes; maximum is {MAX_CONFIG_AUDIT_RECORD_BYTES}",
            serialized.len()
        ));
    }
    let mut record = serialized.into_bytes();
    record.push(b'\n');
    let _guard = CONFIG_AUDIT_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut options = fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(&path).map_err(|error| {
        format!(
            "failed to open config audit log {}: {error}",
            path.display()
        )
    })?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "config audit path {} is not a regular file",
            path.display()
        ));
    }
    let projected = metadata
        .len()
        .checked_add(record.len() as u64)
        .ok_or_else(|| "config audit size overflowed while preparing an append".to_owned())?;
    if projected > MAX_CONFIG_AUDIT_BYTES {
        return Err(format!(
            "config audit log would exceed {MAX_CONFIG_AUDIT_BYTES} bytes; export and rotate it before continuing"
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("failed to secure {}: {error}", path.display()))?;
    }
    file.write_all(&record).map_err(|error| {
        format!(
            "failed to append config audit log {}: {error}",
            path.display()
        )
    })?;
    file.sync_data().map_err(|error| {
        format!(
            "failed to sync config audit log {}: {error}",
            path.display()
        )
    })?;
    Ok(())
}

fn list_config_audit_entries(
    root: &Path,
    limit: usize,
) -> Result<Vec<ConfigAuditEntryView>, String> {
    let path = config_audit_log_path(root);
    let initial_metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => return Err("config audit path is not a regular file".to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "failed to inspect config audit log {}: {error}",
                path.display()
            ));
        }
    };
    if initial_metadata.len() > MAX_CONFIG_AUDIT_BYTES {
        return Err(format!(
            "config audit log is {} bytes; maximum readable size is {MAX_CONFIG_AUDIT_BYTES}",
            initial_metadata.len()
        ));
    }
    ensure_existing_path_within_root(root, &path)?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(&path) {
        Ok(file) => file,
        Err(error) => {
            return Err(format!(
                "failed to read config audit log {}: {error}",
                path.display()
            ));
        }
    };

    let metadata = file
        .metadata()
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_AUDIT_BYTES {
        return Err("config audit log changed to an invalid or oversized file".to_owned());
    }

    let mut entries = VecDeque::with_capacity(limit.max(1));
    let mut reader = BufReader::new(file.take(MAX_CONFIG_AUDIT_BYTES + 1));
    let mut total_bytes = 0_u64;
    loop {
        let mut line = Vec::new();
        let bytes_read = reader
            .by_ref()
            .take(MAX_CONFIG_AUDIT_RECORD_BYTES + 2)
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("failed to read config audit log: {error}"))?;
        if bytes_read == 0 {
            break;
        }
        total_bytes = total_bytes.saturating_add(bytes_read as u64);
        if total_bytes > MAX_CONFIG_AUDIT_BYTES {
            return Err("config audit log grew beyond its readable size limit".to_owned());
        }
        if line.len() as u64 > MAX_CONFIG_AUDIT_RECORD_BYTES + 1
            || (line.len() as u64 == MAX_CONFIG_AUDIT_RECORD_BYTES + 1
                && line.last() != Some(&b'\n'))
        {
            return Err("config audit log contains an oversized record".to_owned());
        }
        let line = std::str::from_utf8(&line)
            .map_err(|_| "config audit log contains invalid UTF-8".to_owned())?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<ConfigAuditEntryView>(trimmed) {
            if entries.len() == limit.max(1) {
                entries.pop_front();
            }
            entries.push_back(entry);
        }
    }
    let mut entries = entries.into_iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        right
            .recorded_at_ms
            .cmp(&left.recorded_at_ms)
            .then_with(|| right.entry_id.cmp(&left.entry_id))
    });
    entries.truncate(limit.max(1));
    Ok(entries)
}

fn config_diff_summary(previous: &str, next: &str) -> ConfigDiffSummaryView {
    let before_lines = previous.lines().collect::<Vec<_>>();
    let after_lines = next.lines().collect::<Vec<_>>();
    let rows = before_lines.len() + 1;
    let cols = after_lines.len() + 1;
    let mut longest_common_subsequence = vec![vec![0usize; cols]; rows];

    for row in (0..before_lines.len()).rev() {
        for col in (0..after_lines.len()).rev() {
            longest_common_subsequence[row][col] = if before_lines[row] == after_lines[col] {
                longest_common_subsequence[row + 1][col + 1] + 1
            } else {
                longest_common_subsequence[row + 1][col]
                    .max(longest_common_subsequence[row][col + 1])
            };
        }
    }

    let mut row = 0usize;
    let mut col = 0usize;
    let mut added_lines = 0u64;
    let mut removed_lines = 0u64;
    while row < before_lines.len() && col < after_lines.len() {
        if before_lines[row] == after_lines[col] {
            row += 1;
            col += 1;
            continue;
        }

        if longest_common_subsequence[row + 1][col] >= longest_common_subsequence[row][col + 1] {
            removed_lines = removed_lines.saturating_add(1);
            row += 1;
        } else {
            added_lines = added_lines.saturating_add(1);
            col += 1;
        }
    }
    removed_lines = removed_lines.saturating_add((before_lines.len() - row) as u64);
    added_lines = added_lines.saturating_add((after_lines.len() - col) as u64);

    ConfigDiffSummaryView {
        added_lines,
        removed_lines,
        changed_lines: added_lines.min(removed_lines),
    }
}

fn config_form_field_schema(section_key: &str, field_key: &str) -> Option<ConfigFormFieldSchema> {
    match (section_key, field_key) {
        ("server", "node_name") => Some(ConfigFormFieldSchema {
            description: "Broker node identifier used in health and metrics views.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("server", "transport") => Some(ConfigFormFieldSchema {
            description: "Listener transport mode for broker command traffic.",
            required: true,
            min: None,
            max: None,
            allowed_values: &["tcp", "unix"],
        }),
        ("server", "listen_addr") => Some(ConfigFormFieldSchema {
            description: "TCP listen address for broker control-plane access.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("server", "socket_path") => Some(ConfigFormFieldSchema {
            description: "Unix domain socket path used when transport is unix.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("server", "data_dir") => Some(ConfigFormFieldSchema {
            description: "Root directory for local broker data.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("server", "log_level") => Some(ConfigFormFieldSchema {
            description: "Structured log level for broker runtime output.",
            required: true,
            min: None,
            max: None,
            allowed_values: &["trace", "debug", "info", "warn", "error"],
        }),
        ("server", "max_connections") => Some(ConfigFormFieldSchema {
            description: "Maximum number of concurrent broker client connections.",
            required: true,
            min: Some(1.0),
            max: None,
            allowed_values: &[],
        }),
        ("server", "max_frame_bytes") => Some(ConfigFormFieldSchema {
            description: "Maximum request frame size accepted before authentication.",
            required: true,
            min: Some(256.0),
            max: Some(67_108_864.0),
            allowed_values: &[],
        }),
        ("server", "connection_idle_timeout_ms") => Some(ConfigFormFieldSchema {
            description: "Maximum time to wait for a complete request frame from an idle client.",
            required: true,
            min: Some(100.0),
            max: Some(3_600_000.0),
            allowed_values: &[],
        }),
        ("storage", "segment_max_bytes") => Some(ConfigFormFieldSchema {
            description: "Maximum segment file size before rolling to a new segment.",
            required: true,
            min: Some(1024.0),
            max: None,
            allowed_values: &[],
        }),
        ("storage", "retention_class") => Some(ConfigFormFieldSchema {
            description: "Default retention class for newly created topics.",
            required: true,
            min: None,
            max: None,
            allowed_values: &["ephemeral", "operational", "regulated"],
        }),
        ("storage", "default_classification") => Some(ConfigFormFieldSchema {
            description: "Default sensitivity label applied to new messages.",
            required: true,
            min: None,
            max: None,
            allowed_values: &["public", "internal", "confidential", "restricted"],
        }),
        ("storage", "ephemeral_retention_bytes") => Some(ConfigFormFieldSchema {
            description: "Retention budget for ephemeral topics.",
            required: true,
            min: Some(1024.0),
            max: None,
            allowed_values: &[],
        }),
        ("storage", "operational_retention_bytes") => Some(ConfigFormFieldSchema {
            description: "Retention budget for operational topics.",
            required: true,
            min: Some(1024.0),
            max: None,
            allowed_values: &[],
        }),
        ("storage", "regulated_retention_bytes") => Some(ConfigFormFieldSchema {
            description: "Retention budget for regulated topics.",
            required: true,
            min: Some(1024.0),
            max: None,
            allowed_values: &[],
        }),
        ("storage", "max_total_bytes") => Some(ConfigFormFieldSchema {
            description: "Hard cap across all retained topic segment bytes.",
            required: true,
            min: Some(4096.0),
            max: None,
            allowed_values: &[],
        }),
        ("storage", "reclaim_target_bytes") => Some(ConfigFormFieldSchema {
            description: "Compaction reclaim target when storage pressure is detected.",
            required: true,
            min: Some(0.0),
            max: None,
            allowed_values: &[],
        }),
        ("audit", "path") => Some(ConfigFormFieldSchema {
            description: "JSONL path for tamper-evident audit events.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("resilience", "allow_degraded_startup") => Some(ConfigFormFieldSchema {
            description: "Permit broker start when some subsystems cannot fully initialize.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("resilience", "allow_degraded_runtime") => Some(ConfigFormFieldSchema {
            description: "Allow runtime requests to proceed in degraded service mode.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("resilience", "audit_retry_attempts") => Some(ConfigFormFieldSchema {
            description: "Number of retry attempts before audit write failure is surfaced.",
            required: true,
            min: Some(0.0),
            max: Some(20.0),
            allowed_values: &[],
        }),
        ("resilience", "audit_retry_backoff_ms") => Some(ConfigFormFieldSchema {
            description: "Backoff delay between audit retry attempts.",
            required: true,
            min: Some(0.0),
            max: Some(60_000.0),
            allowed_values: &[],
        }),
        ("resilience", "listener_retry_delay_ms") => Some(ConfigFormFieldSchema {
            description: "Delay before listener bind retries when startup fails.",
            required: true,
            min: Some(10.0),
            max: Some(120_000.0),
            allowed_values: &[],
        }),
        ("registry", "backend") => Some(ConfigFormFieldSchema {
            description: "Registry storage backend implementation.",
            required: true,
            min: None,
            max: None,
            allowed_values: &["file"],
        }),
        ("registry", "path") => Some(ConfigFormFieldSchema {
            description: "Path for persisted registry agent cards.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("registry", "default_ttl_seconds") => Some(ConfigFormFieldSchema {
            description: "Default registry TTL for registered agent cards.",
            required: true,
            min: Some(1.0),
            max: Some(86_400.0),
            allowed_values: &[],
        }),
        ("registry", "event_history_limit") => Some(ConfigFormFieldSchema {
            description: "Maximum events retained for watch replay history.",
            required: true,
            min: Some(1.0),
            max: Some(1_000_000.0),
            allowed_values: &[],
        }),
        ("registry", "stream_send_timeout_ms") => Some(ConfigFormFieldSchema {
            description: "Send timeout for registry stream frames.",
            required: true,
            min: Some(1.0),
            max: Some(120_000.0),
            allowed_values: &[],
        }),
        ("registry", "stream_idle_keepalive_limit") => Some(ConfigFormFieldSchema {
            description: "Keepalive frame limit before idle stream closure.",
            required: true,
            min: Some(1.0),
            max: Some(10_000.0),
            allowed_values: &[],
        }),
        ("auth", "audience") => Some(ConfigFormFieldSchema {
            description: "Audience required for accepted capability tokens.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("auth", "revocation_path") => Some(ConfigFormFieldSchema {
            description: "Path to revocation registry persisted by broker auth subsystem.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("adopters", "enabled") => Some(ConfigFormFieldSchema {
            description: "Enabled adopter package ids compiled into server binary.",
            required: false,
            min: None,
            max: None,
            allowed_values: &["audit_integrity", "storage_guard", "registry_guard"],
        }),
        ("adopters", "probe_interval_seconds") => Some(ConfigFormFieldSchema {
            description: "Probe interval for periodic adopter health checks.",
            required: true,
            min: Some(1.0),
            max: Some(3600.0),
            allowed_values: &[],
        }),
        ("adopters", "require_installed") => Some(ConfigFormFieldSchema {
            description: "Fail startup when enabled adopter package is not compiled in.",
            required: true,
            min: None,
            max: None,
            allowed_values: &[],
        }),
        ("policy", "default_decision") => Some(ConfigFormFieldSchema {
            description: "Server-side fallback decision for unmatched policy checks.",
            required: true,
            min: None,
            max: None,
            allowed_values: &["deny", "allow"],
        }),
        _ => None,
    }
}

fn config_table_array_description(section_key: &str, field_key: &str) -> Option<&'static str> {
    match (section_key, field_key) {
        ("auth", "issuers") => Some("Configured issuer key references accepted by broker auth."),
        ("auth", "principals") => {
            Some("Registered principals with status, key allowlists, and quota profile mapping.")
        }
        ("policy", "rules") => Some("Server-side policy rules evaluated after capability checks."),
        ("quotas", "profiles") => {
            Some("Quota profiles used by principals for publish/consume paths.")
        }
        _ => None,
    }
}

fn config_table_array_field_schemas(
    section_key: &str,
    field_key: &str,
) -> Option<Vec<ConfigTableArrayFieldSchema>> {
    match (section_key, field_key) {
        ("auth", "issuers") => Some(vec![
            ConfigTableArrayFieldSchema {
                key: "key_id",
                kind: "string",
                description: "Issuer key identifier referenced by capability tokens.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "public_key_path",
                kind: "string",
                description: "Path to issuer public key file.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "status",
                kind: "string",
                description: "Issuer key lifecycle status.",
                required: true,
                min: None,
                max: None,
                allowed_values: &["active", "rotating", "inactive"],
            },
        ]),
        ("auth", "principals") => Some(vec![
            ConfigTableArrayFieldSchema {
                key: "id",
                kind: "string",
                description: "Principal identifier embedded in signed capability claims.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "kind",
                kind: "string",
                description: "Principal category for operator/agent/service grouping.",
                required: true,
                min: None,
                max: None,
                allowed_values: &["developer", "agent", "service"],
            },
            ConfigTableArrayFieldSchema {
                key: "display_name",
                kind: "string",
                description: "Operator-visible display name for principal diagnostics.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "status",
                kind: "string",
                description: "Principal status.",
                required: true,
                min: None,
                max: None,
                allowed_values: &["active", "inactive", "disabled"],
            },
            ConfigTableArrayFieldSchema {
                key: "allowed_key_ids",
                kind: "string_array",
                description: "Issuer key ids this principal accepts.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "quota_profile",
                kind: "string",
                description: "Quota profile linked to principal request budgets.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
        ]),
        ("policy", "rules") => Some(vec![
            ConfigTableArrayFieldSchema {
                key: "principal",
                kind: "string",
                description: "Principal id matched for policy rule.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "resource",
                kind: "string",
                description: "Resource selector pattern for rule.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "actions",
                kind: "string_array",
                description: "Allowed actions when principal/resource rule matches.",
                required: true,
                min: None,
                max: None,
                allowed_values: &["health", "publish", "consume", "admin"],
            },
        ]),
        ("quotas", "profiles") => Some(vec![
            ConfigTableArrayFieldSchema {
                key: "name",
                kind: "string",
                description: "Quota profile name referenced by principal records.",
                required: true,
                min: None,
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "publish_payload_max_bytes",
                kind: "integer",
                description: "Maximum publish payload bytes.",
                required: true,
                min: Some(1.0),
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "publish_requests_per_window",
                kind: "integer",
                description: "Publish request budget per window.",
                required: true,
                min: Some(1.0),
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "publish_window_seconds",
                kind: "integer",
                description: "Publish budget window size in seconds.",
                required: true,
                min: Some(1.0),
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "consume_max_limit",
                kind: "integer",
                description: "Maximum consume batch size.",
                required: true,
                min: Some(1.0),
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "consume_requests_per_window",
                kind: "integer",
                description: "Consume request budget per window.",
                required: true,
                min: Some(1.0),
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "consume_window_seconds",
                kind: "integer",
                description: "Consume budget window size in seconds.",
                required: true,
                min: Some(1.0),
                max: None,
                allowed_values: &[],
            },
            ConfigTableArrayFieldSchema {
                key: "backpressure_mode",
                kind: "string",
                description: "Backpressure behavior when quota is exhausted.",
                required: true,
                min: None,
                max: None,
                allowed_values: &["reject", "delay"],
            },
            ConfigTableArrayFieldSchema {
                key: "backpressure_delay_ms",
                kind: "integer",
                description: "Delay applied when backpressure mode is delay.",
                required: true,
                min: Some(0.0),
                max: None,
                allowed_values: &[],
            },
        ]),
        _ => None,
    }
}

fn default_form_field_value(kind: &str) -> serde_json::Value {
    match kind {
        "string" => serde_json::Value::String(String::new()),
        "integer" => serde_json::Value::from(0),
        "float" => serde_json::Value::from(0.0),
        "boolean" => serde_json::Value::from(false),
        "string_array" => serde_json::Value::Array(Vec::new()),
        _ => serde_json::Value::Null,
    }
}

fn toml_entry_to_json(value: &toml::Value) -> Option<serde_json::Value> {
    toml_form_field_value(value).map(|(_, serialized)| serialized)
}

fn config_form_validation_view(schema: &ConfigFormFieldSchema) -> ConfigFormValidationView {
    ConfigFormValidationView {
        required: schema.required,
        min: schema.min,
        max: schema.max,
        allowed_values: (!schema.allowed_values.is_empty()).then(|| {
            schema
                .allowed_values
                .iter()
                .map(|value| (*value).to_owned())
                .collect()
        }),
    }
}

fn validate_section_form_value(
    section_key: &str,
    field_key: &str,
    parsed_value: &toml::Value,
) -> Result<(), String> {
    let Some(schema) = config_form_field_schema(section_key, field_key) else {
        return Ok(());
    };

    if schema.required {
        match parsed_value {
            toml::Value::String(value) if value.trim().is_empty() => {
                return Err(format!(
                    "field `{section_key}.{field_key}` is required and cannot be empty"
                ));
            }
            toml::Value::Array(values) if values.is_empty() => {
                return Err(format!(
                    "field `{section_key}.{field_key}` is required and cannot be empty"
                ));
            }
            _ => {}
        }
    }

    if let Some(min) = schema.min {
        let Some(number) = parsed_value
            .as_float()
            .or_else(|| parsed_value.as_integer().map(|value| value as f64))
        else {
            return Err(format!("field `{section_key}.{field_key}` must be numeric"));
        };
        if number < min {
            return Err(format!(
                "field `{section_key}.{field_key}` must be >= {min}"
            ));
        }
    }

    if let Some(max) = schema.max {
        let Some(number) = parsed_value
            .as_float()
            .or_else(|| parsed_value.as_integer().map(|value| value as f64))
        else {
            return Err(format!("field `{section_key}.{field_key}` must be numeric"));
        };
        if number > max {
            return Err(format!(
                "field `{section_key}.{field_key}` must be <= {max}"
            ));
        }
    }

    if !schema.allowed_values.is_empty() {
        match parsed_value {
            toml::Value::String(value) => {
                if !schema
                    .allowed_values
                    .iter()
                    .any(|allowed| value.trim() == *allowed)
                {
                    return Err(format!(
                        "field `{section_key}.{field_key}` must be one of [{}]",
                        schema.allowed_values.join(", ")
                    ));
                }
            }
            toml::Value::Array(values) => {
                for value in values {
                    let Some(value) = value.as_str() else {
                        return Err(format!(
                            "field `{section_key}.{field_key}` must contain string entries"
                        ));
                    };
                    if !schema
                        .allowed_values
                        .iter()
                        .any(|allowed| value.trim() == *allowed)
                    {
                        return Err(format!(
                            "field `{section_key}.{field_key}` entry `{value}` must be one of [{}]",
                            schema.allowed_values.join(", ")
                        ));
                    }
                }
            }
            _ => {
                return Err(format!(
                    "field `{section_key}.{field_key}` has unsupported type for allowlist validation"
                ));
            }
        }
    }

    Ok(())
}

fn validate_section_nested_field_value(
    section_key: &str,
    field_key: &str,
    parsed_value: &toml::Value,
) -> Result<(), String> {
    let Some(schemas) = config_table_array_field_schemas(section_key, field_key) else {
        return Ok(());
    };
    let Some(items) = parsed_value.as_array() else {
        return Err(format!(
            "field `{section_key}.{field_key}` must be an array of tables"
        ));
    };
    for (index, item) in items.iter().enumerate() {
        let Some(table) = item.as_table() else {
            return Err(format!(
                "field `{section_key}.{field_key}` entry #{index} must be a table"
            ));
        };
        validate_table_array_entry(section_key, field_key, index, table, &schemas)?;
    }
    Ok(())
}

fn validate_table_array_entry(
    section_key: &str,
    field_key: &str,
    index: usize,
    table: &toml::map::Map<String, toml::Value>,
    schemas: &[ConfigTableArrayFieldSchema],
) -> Result<(), String> {
    for schema in schemas {
        let Some(value) = table.get(schema.key) else {
            if schema.required {
                return Err(format!(
                    "field `{section_key}.{field_key}` entry #{index} is missing required key `{}`",
                    schema.key
                ));
            }
            continue;
        };

        if schema.required {
            match value {
                toml::Value::String(item) if item.trim().is_empty() => {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} cannot be empty",
                        schema.key
                    ));
                }
                toml::Value::Array(items) if items.is_empty() => {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} cannot be empty",
                        schema.key
                    ));
                }
                _ => {}
            }
        }

        match schema.kind {
            "string" => {
                if !value.is_str() {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be string",
                        schema.key
                    ));
                }
            }
            "integer" => {
                let Some(number) = value.as_integer().map(|value| value as f64) else {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be integer",
                        schema.key
                    ));
                };
                if let Some(min) = schema.min
                    && number < min
                {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be >= {min}",
                        schema.key
                    ));
                }
                if let Some(max) = schema.max
                    && number > max
                {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be <= {max}",
                        schema.key
                    ));
                }
            }
            "float" => {
                let Some(number) = value
                    .as_float()
                    .or_else(|| value.as_integer().map(|value| value as f64))
                else {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be numeric",
                        schema.key
                    ));
                };
                if let Some(min) = schema.min
                    && number < min
                {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be >= {min}",
                        schema.key
                    ));
                }
                if let Some(max) = schema.max
                    && number > max
                {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be <= {max}",
                        schema.key
                    ));
                }
            }
            "boolean" => {
                if !value.is_bool() {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be boolean",
                        schema.key
                    ));
                }
            }
            "string_array" => {
                let Some(items) = value.as_array() else {
                    return Err(format!(
                        "field `{section_key}.{field_key}.{}` entry #{index} must be string array",
                        schema.key
                    ));
                };
                for item in items {
                    if !item.is_str() {
                        return Err(format!(
                            "field `{section_key}.{field_key}.{}` entry #{index} must contain string values",
                            schema.key
                        ));
                    }
                }
            }
            _ => {}
        }

        if !schema.allowed_values.is_empty() {
            match value {
                toml::Value::String(item) => {
                    if !schema.allowed_values.iter().any(|allowed| *allowed == item) {
                        return Err(format!(
                            "field `{section_key}.{field_key}.{}` entry #{index} must be one of [{}]",
                            schema.key,
                            schema.allowed_values.join(", ")
                        ));
                    }
                }
                toml::Value::Array(items) => {
                    for item in items {
                        let Some(item) = item.as_str() else {
                            return Err(format!(
                                "field `{section_key}.{field_key}.{}` entry #{index} must contain string values",
                                schema.key
                            ));
                        };
                        if !schema.allowed_values.contains(&item) {
                            return Err(format!(
                                "field `{section_key}.{field_key}.{}` entry #{index} value `{item}` must be one of [{}]",
                                schema.key,
                                schema.allowed_values.join(", ")
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn validate_section_table_consistency(
    section_key: &str,
    section_table: &toml::map::Map<String, toml::Value>,
) -> Result<(), String> {
    if section_key != "storage" {
        return Ok(());
    }

    let max_total_bytes = section_table
        .get("max_total_bytes")
        .and_then(toml::Value::as_integer);
    let reclaim_target_bytes = section_table
        .get("reclaim_target_bytes")
        .and_then(toml::Value::as_integer);
    if let (Some(max_total_bytes), Some(reclaim_target_bytes)) =
        (max_total_bytes, reclaim_target_bytes)
        && reclaim_target_bytes > max_total_bytes
    {
        return Err(
            "field `storage.reclaim_target_bytes` must be <= storage.max_total_bytes".to_owned(),
        );
    }

    for key in [
        "ephemeral_retention_bytes",
        "operational_retention_bytes",
        "regulated_retention_bytes",
    ] {
        let retention_bytes = section_table.get(key).and_then(toml::Value::as_integer);
        if let (Some(max_total_bytes), Some(retention_bytes)) = (max_total_bytes, retention_bytes)
            && retention_bytes > max_total_bytes
        {
            return Err(format!(
                "field `storage.{key}` must be <= storage.max_total_bytes"
            ));
        }
    }

    Ok(())
}

fn validate_component_content(
    spec: &ConfigComponentSpec,
    parsed: &toml::Value,
) -> Result<(), String> {
    if spec.group != "broker" {
        return Ok(());
    }

    let Some(root_table) = parsed.as_table() else {
        return Err("component root must be a TOML table".to_owned());
    };

    for (section_key, section_value) in root_table {
        if !is_core_form_section(spec, section_key) {
            continue;
        }
        let Some(section_table) = section_value.as_table() else {
            continue;
        };
        for (field_key, field_value) in section_table {
            validate_section_form_value(section_key, field_key, field_value)?;
            validate_section_nested_field_value(section_key, field_key, field_value)?;
        }
        validate_section_table_consistency(section_key, section_table)?;
    }

    Ok(())
}

fn toml_form_field_value(value: &toml::Value) -> Option<(&'static str, serde_json::Value)> {
    match value {
        toml::Value::String(item) => Some(("string", serde_json::Value::String(item.clone()))),
        toml::Value::Integer(item) => Some(("integer", serde_json::Value::from(*item))),
        toml::Value::Float(item) => Some(("float", serde_json::Value::from(*item))),
        toml::Value::Boolean(item) => Some(("boolean", serde_json::Value::from(*item))),
        toml::Value::Array(items) => {
            if !items.iter().all(toml::Value::is_str) {
                return None;
            }
            let values = items
                .iter()
                .filter_map(|entry| entry.as_str())
                .map(|entry| serde_json::Value::String(entry.to_owned()))
                .collect::<Vec<_>>();
            Some(("string_array", serde_json::Value::Array(values)))
        }
        _ => None,
    }
}

fn parse_section_form_value(
    section_key: &str,
    field_key: &str,
    input: &serde_json::Value,
    current: &toml::Value,
) -> Result<toml::Value, String> {
    match current {
        toml::Value::String(_) => match input {
            serde_json::Value::String(value) => Ok(toml::Value::String(value.clone())),
            serde_json::Value::Number(value) => Ok(toml::Value::String(value.to_string())),
            serde_json::Value::Bool(value) => Ok(toml::Value::String(value.to_string())),
            _ => Err(format!(
                "field `{section_key}.{field_key}` expects a string value"
            )),
        },
        toml::Value::Integer(_) => {
            if let Some(value) = input.as_i64() {
                return Ok(toml::Value::Integer(value));
            }
            if let Some(value) = input.as_u64() {
                let value = i64::try_from(value).map_err(|_| {
                    format!("field `{section_key}.{field_key}` integer is too large")
                })?;
                return Ok(toml::Value::Integer(value));
            }
            if let Some(value) = input.as_str() {
                let parsed = value
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| format!("field `{section_key}.{field_key}` expects an integer"))?;
                return Ok(toml::Value::Integer(parsed));
            }
            Err(format!(
                "field `{section_key}.{field_key}` expects an integer value"
            ))
        }
        toml::Value::Float(_) => {
            if let Some(value) = input.as_f64() {
                return Ok(toml::Value::Float(value));
            }
            if let Some(value) = input.as_i64() {
                return Ok(toml::Value::Float(value as f64));
            }
            if let Some(value) = input.as_str() {
                let parsed = value.trim().parse::<f64>().map_err(|_| {
                    format!("field `{section_key}.{field_key}` expects a numeric value")
                })?;
                return Ok(toml::Value::Float(parsed));
            }
            Err(format!(
                "field `{section_key}.{field_key}` expects a numeric value"
            ))
        }
        toml::Value::Boolean(_) => {
            if let Some(value) = input.as_bool() {
                return Ok(toml::Value::Boolean(value));
            }
            if let Some(value) = input.as_str() {
                let normalized = value.trim().to_ascii_lowercase();
                let parsed = match normalized.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return Err(format!(
                            "field `{section_key}.{field_key}` expects true or false"
                        ));
                    }
                };
                return Ok(toml::Value::Boolean(parsed));
            }
            Err(format!(
                "field `{section_key}.{field_key}` expects a boolean value"
            ))
        }
        toml::Value::Array(items) => {
            let is_string_array = items.iter().all(toml::Value::is_str);
            let is_table_array = items.iter().all(toml::Value::is_table);
            if is_string_array {
                let values = if let Some(values) = input.as_array() {
                    values
                        .iter()
                        .map(|item| {
                            item.as_str().map(str::trim).filter(|item| !item.is_empty()).map(str::to_owned).ok_or_else(|| {
                                format!(
                                    "field `{section_key}.{field_key}` array entries must be strings"
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else if let Some(value) = input.as_str() {
                    value
                        .split([',', '\n'])
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                } else {
                    return Err(format!(
                        "field `{section_key}.{field_key}` expects a comma-separated string or string array"
                    ));
                };

                return Ok(toml::Value::Array(
                    values
                        .into_iter()
                        .map(toml::Value::String)
                        .collect::<Vec<_>>(),
                ));
            }
            if is_table_array {
                let items = if let Some(values) = input.as_array() {
                    values
                        .iter()
                        .map(|value| {
                            let Some(item_object) = value.as_object() else {
                                return Err(format!(
                                    "field `{section_key}.{field_key}` table-array entries must be objects"
                                ));
                            };
                            let mut table = toml::map::Map::new();
                            for (entry_key, entry_value) in item_object {
                                table.insert(
                                    entry_key.clone(),
                                    json_value_to_toml(
                                        entry_value,
                                        section_key,
                                        field_key,
                                        entry_key,
                                    )?,
                                );
                            }
                            Ok(toml::Value::Table(table))
                        })
                        .collect::<Result<Vec<_>, String>>()?
                } else if let Some(value) = input.as_str() {
                    let parsed = serde_json::from_str::<serde_json::Value>(value).map_err(|_| {
                        format!(
                            "field `{section_key}.{field_key}` expects JSON array text for table entries"
                        )
                    })?;
                    let Some(values) = parsed.as_array() else {
                        return Err(format!(
                            "field `{section_key}.{field_key}` expects JSON array text for table entries"
                        ));
                    };
                    values
                        .iter()
                        .map(|value| {
                            let Some(item_object) = value.as_object() else {
                                return Err(format!(
                                    "field `{section_key}.{field_key}` table-array entries must be objects"
                                ));
                            };
                            let mut table = toml::map::Map::new();
                            for (entry_key, entry_value) in item_object {
                                table.insert(
                                    entry_key.clone(),
                                    json_value_to_toml(
                                        entry_value,
                                        section_key,
                                        field_key,
                                        entry_key,
                                    )?,
                                );
                            }
                            Ok(toml::Value::Table(table))
                        })
                        .collect::<Result<Vec<_>, String>>()?
                } else {
                    return Err(format!(
                        "field `{section_key}.{field_key}` expects table-array JSON entries"
                    ));
                };
                return Ok(toml::Value::Array(items));
            }
            Err(format!(
                "field `{section_key}.{field_key}` is not a supported form-editable array"
            ))
        }
        _ => Err(format!(
            "field `{section_key}.{field_key}` cannot be edited in form mode"
        )),
    }
}

fn json_value_to_toml(
    value: &serde_json::Value,
    section_key: &str,
    field_key: &str,
    entry_key: &str,
) -> Result<toml::Value, String> {
    match value {
        serde_json::Value::Null => Ok(toml::Value::String(String::new())),
        serde_json::Value::Bool(item) => Ok(toml::Value::Boolean(*item)),
        serde_json::Value::Number(item) => {
            if let Some(integer) = item.as_i64() {
                return Ok(toml::Value::Integer(integer));
            }
            if let Some(float) = item.as_f64() {
                return Ok(toml::Value::Float(float));
            }
            Err(format!(
                "field `{section_key}.{field_key}.{entry_key}` has unsupported numeric value"
            ))
        }
        serde_json::Value::String(item) => Ok(toml::Value::String(item.clone())),
        serde_json::Value::Array(items) => {
            let mut output = Vec::new();
            for item in items {
                match item {
                    serde_json::Value::String(value) => {
                        output.push(toml::Value::String(value.clone()))
                    }
                    serde_json::Value::Bool(value) => output.push(toml::Value::Boolean(*value)),
                    serde_json::Value::Number(value) => {
                        if let Some(integer) = value.as_i64() {
                            output.push(toml::Value::Integer(integer));
                        } else if let Some(float) = value.as_f64() {
                            output.push(toml::Value::Float(float));
                        } else {
                            return Err(format!(
                                "field `{section_key}.{field_key}.{entry_key}` array includes unsupported number value"
                            ));
                        }
                    }
                    _ => {
                        return Err(format!(
                            "field `{section_key}.{field_key}.{entry_key}` array entries must be primitive values"
                        ));
                    }
                }
            }
            Ok(toml::Value::Array(output))
        }
        serde_json::Value::Object(object) => {
            let mut table = toml::map::Map::new();
            for (object_key, object_value) in object {
                table.insert(
                    object_key.clone(),
                    json_value_to_toml(object_value, section_key, field_key, object_key)?,
                );
            }
            Ok(toml::Value::Table(table))
        }
    }
}

fn toml_kind(value: &toml::Value) -> &'static str {
    match value {
        toml::Value::String(_) => "string",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    }
}

fn toml_summary(value: &toml::Value) -> String {
    match value {
        toml::Value::Table(table) => format!("table with {} keys", table.len()),
        toml::Value::Array(values) => {
            let all_tables = values.iter().all(toml::Value::is_table);
            if all_tables {
                format!("array of tables ({} entries)", values.len())
            } else {
                format!("array ({} entries)", values.len())
            }
        }
        toml::Value::String(value) => format!("\"{}\"", truncate_summary(value)),
        toml::Value::Integer(value) => value.to_string(),
        toml::Value::Float(value) => value.to_string(),
        toml::Value::Boolean(value) => value.to_string(),
        toml::Value::Datetime(value) => value.to_string(),
    }
}

fn truncate_summary(value: &str) -> String {
    let max_len = 48usize;
    if value.chars().count() <= max_len {
        return value.to_owned();
    }
    let mut output = String::with_capacity(max_len + 3);
    for (index, ch) in value.chars().enumerate() {
        if index >= max_len {
            break;
        }
        output.push(ch);
    }
    output.push_str("...");
    output
}

fn title_case_identifier(value: &str) -> String {
    let mut parts = Vec::new();
    for part in value.split(['-', '_', '.']) {
        if part.is_empty() {
            continue;
        }
        let mut chars = part.chars();
        let Some(first) = chars.next() else {
            continue;
        };
        let mut title = first.to_ascii_uppercase().to_string();
        title.push_str(chars.as_str());
        parts.push(title);
    }
    if parts.is_empty() {
        "Config".to_owned()
    } else {
        parts.join(" ")
    }
}

fn system_time_to_millis(time: SystemTime) -> Option<u64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

fn build_endpoint(settings: &ConsoleSettings) -> Result<Endpoint, String> {
    match settings.transport.as_str() {
        "tcp" => Ok(Endpoint::Tcp(settings.address.clone())),
        "unix" => {
            #[cfg(unix)]
            {
                Ok(Endpoint::Unix(settings.socket_path.clone().into()))
            }
            #[cfg(not(unix))]
            {
                Err("unix transport is not supported on this platform".to_owned())
            }
        }
        other => Err(format!("unsupported transport: {other}")),
    }
}

fn handle_stream_frame(app: &tauri::AppHandle, frame: StreamFrame) -> bool {
    match frame {
        StreamFrame::AgentWatchOpened { cursor } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Opened,
                    cursor: Some(cursor),
                    events: Vec::new(),
                    message: None,
                },
            );
            true
        }
        StreamFrame::RegistryEvents { events, cursor } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Events,
                    cursor: Some(cursor),
                    events,
                    message: None,
                },
            );
            true
        }
        StreamFrame::KeepAlive { cursor } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Keepalive,
                    cursor: Some(cursor),
                    events: Vec::new(),
                    message: None,
                },
            );
            true
        }
        StreamFrame::StreamError { code, message } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Error,
                    cursor: None,
                    events: Vec::new(),
                    message: Some(format!("{code}: {message}")),
                },
            );
            false
        }
        StreamFrame::StreamClosed { cursor, reason } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Closed,
                    cursor: Some(cursor),
                    events: Vec::new(),
                    message: Some(reason),
                },
            );
            false
        }
    }
}

fn emit_registry_stream(
    app: &tauri::AppHandle,
    payload: RegistryStreamEventPayload,
) -> Result<(), String> {
    app.emit("registry-stream-event", payload)
        .map_err(|error| format!("emit failed: {error}"))
}

async fn send_command(
    client: &mut Client,
    token: &str,
    command: ControlCommand,
) -> Result<ControlResponse, String> {
    let (response, attachment) = send_command_with_attachment(client, token, command, None).await?;
    if attachment.is_some() {
        return Err("unexpected attachment in response".to_owned());
    }
    Ok(response)
}

async fn send_command_with_attachment(
    client: &mut Client,
    token: &str,
    command: ControlCommand,
    attachment: Option<Vec<u8>>,
) -> Result<(ControlResponse, Option<Vec<u8>>), String> {
    let (response, response_attachment) = client
        .send_with_attachment(
            ControlRequest {
                capability_token: token.to_owned(),
                command,
            },
            attachment,
        )
        .await
        .map_err(|error| error.to_string())?;

    if let ControlResponse::Error { code, message } = &response {
        return Err(format!("{code}: {message}"));
    }

    Ok((response, response_attachment))
}

fn decode_optional_base64(input: Option<&str>) -> Result<Option<Vec<u8>>, String> {
    let Some(input) = input else {
        return Ok(None);
    };

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    BASE64_STANDARD
        .decode(trimmed)
        .map(Some)
        .map_err(|error| format!("failed to decode attachmentBase64: {error}"))
}

fn response_name(response: &ControlResponse) -> &'static str {
    match response {
        ControlResponse::Health { .. } => "health",
        ControlResponse::Metrics { .. } => "metrics",
        ControlResponse::Adopters { .. } => "adopters",
        ControlResponse::AuthState { .. } => "auth_state",
        ControlResponse::AgentRegistered { .. } => "agent_registered",
        ControlResponse::AgentHeartbeat { .. } => "agent_heartbeat",
        ControlResponse::Agents { .. } => "agents",
        ControlResponse::RegistryEvents { .. } => "registry_events",
        ControlResponse::AgentsCleanedUp { .. } => "agents_cleaned_up",
        ControlResponse::AgentRemoved { .. } => "agent_removed",
        ControlResponse::TopicCreated { .. } => "topic_created",
        ControlResponse::RevocationUpdated { .. } => "revocation_updated",
        ControlResponse::ArtifactStored { .. } => "artifact_stored",
        ControlResponse::ArtifactMetadata { .. } => "artifact_metadata",
        ControlResponse::Artifact { .. } => "artifact",
        ControlResponse::PublishAccepted { .. } => "publish_accepted",
        ControlResponse::Messages { .. } => "messages",
        ControlResponse::Error { .. } => "error",
    }
}

fn command_name(command: &ControlCommand) -> &'static str {
    match command {
        ControlCommand::Health => "health",
        ControlCommand::GetAuthState => "get_auth_state",
        ControlCommand::GetMetrics => "get_metrics",
        ControlCommand::GetAdopters => "get_adopters",
        ControlCommand::RegisterAgent { .. } => "register_agent",
        ControlCommand::HeartbeatAgent { .. } => "heartbeat_agent",
        ControlCommand::ListAgents { .. } => "list_agents",
        ControlCommand::WatchAgents { .. } => "watch_agents",
        ControlCommand::OpenAgentWatchStream { .. } => "open_agent_watch_stream",
        ControlCommand::CleanupStaleAgents => "cleanup_stale_agents",
        ControlCommand::RemoveAgent { .. } => "remove_agent",
        ControlCommand::CreateTopic { .. } => "create_topic",
        ControlCommand::RevokeToken { .. } => "revoke_token",
        ControlCommand::RevokePrincipal { .. } => "revoke_principal",
        ControlCommand::RevokeKey { .. } => "revoke_key",
        ControlCommand::PutArtifact { .. } => "put_artifact",
        ControlCommand::GetArtifact { .. } => "get_artifact",
        ControlCommand::StatArtifact { .. } => "stat_artifact",
        ControlCommand::Publish { .. } => "publish",
        ControlCommand::Consume { .. } => "consume",
        ControlCommand::WatchTopic { .. } => "watch_topic",
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(RegistryStreamState::default())
        .invoke_handler(tauri::generate_handler![
            monitor_snapshot,
            monitor_consume_topic,
            monitor_execute_control,
            monitor_start_registry_stream,
            monitor_stop_registry_stream,
            config_console_snapshot,
            config_console_update_component,
            config_console_update_section,
            config_console_list_backups,
            config_console_list_audit_entries,
            config_console_rollback_component,
            config_console_restart_services,
            config_console_service_action,
            operator_run_action,
            operator_provision_credentials
        ])
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn broker_spec() -> ConfigComponentSpec {
        ConfigComponentSpec {
            id: "configs/expressways.example.toml".to_owned(),
            name: "Expressways Broker".to_owned(),
            group: "broker".to_owned(),
            description: "test".to_owned(),
            relative_path: "configs/expressways.example.toml".to_owned(),
            editable: true,
        }
    }

    fn temporary_root(label: &str) -> PathBuf {
        let counter = CONFIG_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "expressways-console-{label}-{}-{counter}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("create temporary root");
        root
    }

    #[test]
    fn bounded_config_reader_rejects_oversized_and_symlinked_files() {
        let root = temporary_root("bounded-reader");
        let path = root.join("config.toml");
        fs::File::create(&path)
            .expect("create config")
            .set_len(5)
            .expect("size config");
        assert!(read_bounded_utf8_regular_file(&path, 4).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            fs::write(&path, b"a = 1\n").expect("write config");
            let link = root.join("linked.toml");
            symlink(&path, &link).expect("create config symlink");
            assert!(read_bounded_utf8_regular_file(&link, 1024).is_err());
        }
        fs::remove_dir_all(root).expect("remove temporary root");
    }

    #[test]
    fn private_atomic_replace_is_durable_and_owner_only() {
        let root = temporary_root("atomic-config");
        let path = root.join("config.toml");
        fs::write(&path, b"old").expect("write old config");

        atomic_replace_private_file(&path, b"new").expect("replace config");

        assert_eq!(fs::read_to_string(&path).expect("read config"), "new");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path)
                    .expect("inspect config")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        fs::remove_dir_all(root).expect("remove temporary root");
    }

    #[cfg(unix)]
    #[test]
    fn config_directory_creation_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let root = temporary_root("contained-config");
        let outside = temporary_root("outside-config");
        symlink(&outside, root.join("configs")).expect("create directory symlink");

        assert!(ensure_directory_tree_within_root(&root, &root.join("configs/nested")).is_err());
        assert!(!outside.join("nested").exists());
        fs::remove_dir_all(root).expect("remove temporary root");
        fs::remove_dir_all(outside).expect("remove outside root");
    }

    #[test]
    fn config_payload_limit_is_enforced_before_parsing() {
        let root = temporary_root("payload-limit");
        let error = apply_component_content(
            &root,
            &broker_spec(),
            "x".repeat(MAX_CONFIG_COMPONENT_BYTES as usize + 1),
            ConfigApplyContext {
                action: "test",
                section_key: None,
                summary: "test".to_owned(),
            },
        )
        .expect_err("oversized config must fail");
        assert!(error.contains("maximum"));
        fs::remove_dir_all(root).expect("remove temporary root");
    }

    #[test]
    fn section_form_fields_extracts_core_scalar_fields() {
        let spec = broker_spec();
        let value = toml::Value::Table(toml::map::Map::from_iter([
            (
                "node_name".to_owned(),
                toml::Value::String("dev".to_owned()),
            ),
            ("enabled".to_owned(), toml::Value::Boolean(true)),
            (
                "tags".to_owned(),
                toml::Value::Array(vec![toml::Value::String("a".to_owned())]),
            ),
            (
                "nested".to_owned(),
                toml::Value::Table(toml::map::Map::from_iter([(
                    "x".to_owned(),
                    toml::Value::Integer(1),
                )])),
            ),
        ]));

        let fields = section_form_fields(&spec, "server", &value);
        assert!(fields.iter().any(|field| field.key == "node_name"));
        assert!(fields.iter().any(|field| field.key == "enabled"));
        assert!(fields.iter().any(|field| field.key == "tags"));
        assert!(!fields.iter().any(|field| field.key == "nested"));
    }

    #[test]
    fn apply_section_form_update_updates_section_values() {
        let spec = broker_spec();
        let content = r#"[server]
node_name = "dev-node"
listen_addr = "127.0.0.1:7766"

[storage]
segment_max_bytes = 1024
"#
        .to_owned();

        let values = BTreeMap::from_iter([(
            "node_name".to_owned(),
            serde_json::Value::String("prod-node".to_owned()),
        )]);
        let updated = apply_section_form_update(&spec, content, "server", &values).expect("update");
        let parsed = toml::from_str::<toml::Value>(&updated).expect("parse");
        let node_name = parsed
            .get("server")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("node_name"))
            .and_then(toml::Value::as_str);
        assert_eq!(node_name, Some("prod-node"));
    }

    #[test]
    fn section_table_arrays_extract_policy_rules() {
        let spec = broker_spec();
        let value = toml::Value::Table(toml::map::Map::from_iter([
            (
                "default_decision".to_owned(),
                toml::Value::String("deny".to_owned()),
            ),
            (
                "rules".to_owned(),
                toml::Value::Array(vec![toml::Value::Table(toml::map::Map::from_iter([
                    (
                        "principal".to_owned(),
                        toml::Value::String("local:developer".to_owned()),
                    ),
                    (
                        "resource".to_owned(),
                        toml::Value::String("system:broker".to_owned()),
                    ),
                    (
                        "actions".to_owned(),
                        toml::Value::Array(vec![
                            toml::Value::String("health".to_owned()),
                            toml::Value::String("admin".to_owned()),
                        ]),
                    ),
                ]))]),
            ),
        ]));

        let arrays = section_table_arrays(&spec, "policy", &value);
        assert_eq!(arrays.len(), 1);
        assert_eq!(arrays[0].key, "rules");
        assert_eq!(arrays[0].entries.len(), 1);
        assert_eq!(
            arrays[0].entries[0].get("principal"),
            Some(&serde_json::Value::String("local:developer".to_owned()))
        );
    }

    #[test]
    fn apply_section_form_update_updates_table_array_entries() {
        let spec = broker_spec();
        let content = r#"[policy]
default_decision = "deny"

[[policy.rules]]
principal = "local:developer"
resource = "system:broker"
actions = ["health", "admin"]
"#
        .to_owned();

        let values = BTreeMap::from_iter([(
            "rules".to_owned(),
            serde_json::json!([
                {
                    "principal": "local:developer",
                    "resource": "system:broker",
                    "actions": ["health", "admin"]
                },
                {
                    "principal": "local:agent-orchestrator",
                    "resource": "topic:task*",
                    "actions": ["publish", "consume"]
                }
            ]),
        )]);

        let updated = apply_section_form_update(&spec, content, "policy", &values).expect("update");
        let parsed = toml::from_str::<toml::Value>(&updated).expect("parse");
        let rules = parsed
            .get("policy")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("rules"))
            .and_then(toml::Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn apply_section_form_update_rejects_invalid_policy_action() {
        let spec = broker_spec();
        let content = r#"[policy]
default_decision = "deny"

[[policy.rules]]
principal = "local:developer"
resource = "system:broker"
actions = ["health", "admin"]
"#
        .to_owned();

        let values = BTreeMap::from_iter([(
            "rules".to_owned(),
            serde_json::json!([
                {
                    "principal": "local:developer",
                    "resource": "system:broker",
                    "actions": ["health", "delete"]
                }
            ]),
        )]);

        let error = apply_section_form_update(&spec, content, "policy", &values)
            .expect_err("invalid action should fail");
        assert!(error.contains("must be one of"));
    }

    #[test]
    fn normalize_service_action_allows_supported_actions() {
        assert_eq!(normalize_service_action("start"), Some("start"));
        assert_eq!(normalize_service_action("stop"), Some("stop"));
        assert_eq!(normalize_service_action("restart"), Some("restart"));
        assert_eq!(normalize_service_action("status"), Some("status"));
        assert_eq!(normalize_service_action(" invalid "), None);
    }

    #[test]
    fn resolve_operator_make_target_maps_known_actions() {
        assert_eq!(
            resolve_operator_make_target("bootstrap_local"),
            Some("bootstrap-local")
        );
        assert_eq!(
            resolve_operator_make_target("generate_admin_token"),
            Some("generate-admin-token")
        );
        assert_eq!(
            resolve_operator_make_target("verify_first_run"),
            Some("verify-first-run")
        );
        assert_eq!(
            resolve_operator_make_target("export_support_bundle"),
            Some("export-support-bundle")
        );
        assert_eq!(resolve_operator_make_target("unknown"), None);
    }

    #[test]
    fn command_guarding_covers_mutating_commands() {
        assert!(!command_requires_guard(&ControlCommand::Health));
        assert!(!command_requires_guard(&ControlCommand::GetMetrics));
        assert!(command_requires_guard(&ControlCommand::CreateTopic {
            topic: expressways_protocol::TopicSpec {
                name: "tasks".to_owned(),
                retention_class: expressways_protocol::RetentionClass::Operational,
                default_classification: expressways_protocol::Classification::Internal,
            },
        }));
        assert!(command_requires_guard(&ControlCommand::Publish {
            topic: "tasks".to_owned(),
            classification: Some(expressways_protocol::Classification::Internal),
            payload: "hello".to_owned(),
        }));
    }

    #[test]
    fn validate_section_form_value_enforces_allowlist_and_bounds() {
        let invalid_transport = toml::Value::String("udp".to_owned());
        let error = validate_section_form_value("server", "transport", &invalid_transport)
            .expect_err("invalid transport should fail");
        assert!(error.contains("must be one of"));

        let invalid_probe = toml::Value::Integer(0);
        let error =
            validate_section_form_value("adopters", "probe_interval_seconds", &invalid_probe)
                .expect_err("probe interval below minimum should fail");
        assert!(error.contains(">="));

        let valid_probe = toml::Value::Integer(30);
        validate_section_form_value("adopters", "probe_interval_seconds", &valid_probe)
            .expect("valid probe interval");
    }

    #[test]
    fn validate_storage_consistency_catches_reclaim_above_max() {
        let table = toml::map::Map::from_iter([
            ("max_total_bytes".to_owned(), toml::Value::Integer(100)),
            ("reclaim_target_bytes".to_owned(), toml::Value::Integer(120)),
        ]);
        let error = validate_section_table_consistency("storage", &table)
            .expect_err("reclaim target must be <= max total bytes");
        assert!(error.contains("reclaim_target_bytes"));
    }

    #[test]
    fn config_diff_summary_reports_line_deltas() {
        let previous = "a\nb\nc\n";
        let next = "a\nb2\nc\nd\n";
        let summary = config_diff_summary(previous, next);
        assert_eq!(summary.added_lines, 2);
        assert_eq!(summary.removed_lines, 1);
        assert_eq!(summary.changed_lines, 1);
    }

    #[test]
    fn append_and_list_config_audit_entries_round_trip() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_millis(0))
            .as_millis();
        let root = std::env::temp_dir().join(format!("expressways-console-audit-test-{unique}"));
        fs::create_dir_all(&root).expect("temp root");

        let first = ConfigAuditEntryView {
            entry_id: "1".to_owned(),
            recorded_at_ms: 1,
            actor: "console:test".to_owned(),
            category: "config".to_owned(),
            action: "apply_component".to_owned(),
            component_id: Some("configs/expressways.example.toml".to_owned()),
            section_key: None,
            service_id: None,
            command_type: None,
            success: Some(true),
            status_code: None,
            summary: "first".to_owned(),
            diff: Some(ConfigDiffSummaryView {
                added_lines: 1,
                removed_lines: 0,
                changed_lines: 0,
            }),
        };
        let second = ConfigAuditEntryView {
            entry_id: "2".to_owned(),
            recorded_at_ms: 2,
            actor: "console:test".to_owned(),
            category: "service".to_owned(),
            action: "restart".to_owned(),
            component_id: None,
            section_key: None,
            service_id: Some("expressways-server".to_owned()),
            command_type: None,
            success: Some(true),
            status_code: Some(0),
            summary: "second".to_owned(),
            diff: None,
        };

        append_config_audit_entry(&root, first).expect("append first");
        append_config_audit_entry(&root, second).expect("append second");

        let entries = list_config_audit_entries(&root, 10).expect("list entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].recorded_at_ms, 2);
        assert_eq!(entries[0].summary, "second");
        assert_eq!(entries[1].recorded_at_ms, 1);
        assert_eq!(entries[1].summary, "first");
        let latest = list_config_audit_entries(&root, 1).expect("list latest entry");
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].summary, "second");

        fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[test]
    fn config_audit_rejects_symlinked_log() {
        use std::os::unix::fs::symlink;

        let root = temporary_root("audit-symlink");
        let audit_root = config_audit_root_path(&root);
        ensure_directory_tree_within_root(&root, &audit_root).expect("create audit root");
        let outside = root.join("outside.jsonl");
        fs::write(&outside, b"").expect("write outside log");
        symlink(&outside, config_audit_log_path(&root)).expect("create audit symlink");

        let entry = ConfigAuditEntryView {
            entry_id: "1".to_owned(),
            recorded_at_ms: 1,
            actor: "console:test".to_owned(),
            category: "config".to_owned(),
            action: "test".to_owned(),
            component_id: None,
            section_key: None,
            service_id: None,
            command_type: None,
            success: Some(true),
            status_code: None,
            summary: "test".to_owned(),
            diff: None,
        };
        assert!(append_config_audit_entry(&root, entry).is_err());
        assert!(list_config_audit_entries(&root, 10).is_err());
        assert!(fs::read(&outside).expect("read outside log").is_empty());
        fs::remove_dir_all(root).expect("remove temporary root");
    }

    #[test]
    fn config_update_rolls_back_when_audit_is_full() {
        let root = temporary_root("audit-rollback");
        let config_dir = root.join("configs");
        ensure_directory_tree_within_root(&root, &config_dir).expect("create config directory");
        let path = config_dir.join("custom.toml");
        fs::write(&path, b"value = \"old\"\n").expect("write original config");
        let audit_root = config_audit_root_path(&root);
        ensure_directory_tree_within_root(&root, &audit_root).expect("create audit directory");
        fs::File::create(config_audit_log_path(&root))
            .expect("create audit log")
            .set_len(MAX_CONFIG_AUDIT_BYTES)
            .expect("fill audit log");
        let spec = ConfigComponentSpec {
            id: "configs/custom.toml".to_owned(),
            name: "Custom".to_owned(),
            group: "custom".to_owned(),
            description: "test".to_owned(),
            relative_path: "configs/custom.toml".to_owned(),
            editable: true,
        };

        let error = apply_component_content(
            &root,
            &spec,
            "value = \"new\"\n".to_owned(),
            ConfigApplyContext {
                action: "test",
                section_key: None,
                summary: "test".to_owned(),
            },
        )
        .expect_err("full audit log must fail the update");

        assert!(error.contains("rolled back"));
        assert_eq!(
            fs::read_to_string(path).expect("read rolled back config"),
            "value = \"old\"\n"
        );
        fs::remove_dir_all(root).expect("remove temporary root");
    }

    #[test]
    fn rollback_reliability_meets_m2_target() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_millis(0))
            .as_millis();
        let root = std::env::temp_dir().join(format!("expressways-console-rollback-test-{unique}"));
        let spec = broker_spec();
        let path = root.join(&spec.relative_path);
        fs::create_dir_all(path.parent().expect("config parent")).expect("create config parent");

        let baseline = r#"[server]
node_name = "dev-node"
transport = "tcp"
listen_addr = "127.0.0.1:7766"
socket_path = "./tmp/expressways.sock"
data_dir = "./var/data"
log_level = "info"

[storage]
segment_max_bytes = 1048576
retention_class = "operational"
default_classification = "internal"
ephemeral_retention_bytes = 4194304
operational_retention_bytes = 16777216
regulated_retention_bytes = 67108864
max_total_bytes = 134217728
        reclaim_target_bytes = 117440512
"#;
        fs::write(&path, baseline).expect("write baseline");
        let baseline_expected = if baseline.ends_with('\n') {
            baseline.to_owned()
        } else {
            format!("{baseline}\n")
        };

        let attempts = 100usize;
        let mut rollback_successes = 0usize;

        for index in 0..attempts {
            let updated = format!(
                r#"[server]
node_name = "dev-node-{index}"
transport = "tcp"
listen_addr = "127.0.0.1:7766"
socket_path = "./tmp/expressways.sock"
data_dir = "./var/data"
log_level = "info"

[storage]
segment_max_bytes = 1048576
retention_class = "operational"
default_classification = "internal"
ephemeral_retention_bytes = 4194304
operational_retention_bytes = 16777216
regulated_retention_bytes = 67108864
max_total_bytes = 134217728
reclaim_target_bytes = 117440512
"#
            );

            let applied = apply_component_content(
                &root,
                &spec,
                updated,
                ConfigApplyContext {
                    action: "apply_component",
                    section_key: None,
                    summary: "apply for rollback reliability test".to_owned(),
                },
            )
            .expect("apply updated config");
            let backup_path = applied.backup_path.expect("backup path");
            let backup_content = fs::read_to_string(&backup_path).expect("read backup");

            let _rolled_back = apply_component_content(
                &root,
                &spec,
                backup_content,
                ConfigApplyContext {
                    action: "rollback_component",
                    section_key: None,
                    summary: "rollback for rollback reliability test".to_owned(),
                },
            )
            .expect("rollback apply");

            let current = fs::read_to_string(&path).expect("read restored file");
            if current == baseline_expected {
                rollback_successes += 1;
            }
        }

        let success_rate = (rollback_successes as f64 / attempts as f64) * 100.0;
        eprintln!(
            "ROLLBACK_RELIABILITY success_rate={success_rate:.2} attempts={attempts} successes={rollback_successes}"
        );
        assert!(
            success_rate >= 99.0,
            "rollback reliability below M2 target: {success_rate:.2}%"
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn packaged_credential_provisioning_is_secure_idempotent_and_never_returns_secrets() {
        let unique = Uuid::now_v7();
        let root = std::env::temp_dir().join(format!("expressways-provision-{unique}"));
        let config_dir = root.join("configs");
        fs::create_dir_all(&config_dir).expect("create config dir");
        let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repository_root = manifest_root
            .ancestors()
            .nth(3)
            .expect("repository root")
            .to_path_buf();
        fs::copy(
            repository_root.join("configs/expressways.example.toml"),
            config_dir.join("expressways.example.toml"),
        )
        .expect("copy config");

        let first = provision_local_credentials(root.to_str().expect("root utf8"), false)
            .expect("provision credentials");
        assert!(first.created);
        assert!(first.token_id.is_some());
        assert!(first.expires_at.is_some());
        let private = root.join("var/auth/issuer.private");
        let public = root.join("var/auth/issuer.public");
        let token = root.join("var/auth/developer.token");
        assert!(private.is_file() && public.is_file() && token.is_file());
        assert!(
            !first
                .message
                .contains(&fs::read_to_string(&token).expect("read token"))
        );
        CapabilityIssuer::from_private_key_file("dev", &private).expect("valid private key");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&private)
                    .expect("private metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(&token)
                    .expect("token metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        let second = provision_local_credentials(root.to_str().expect("root utf8"), false)
            .expect("idempotent provision");
        assert!(!second.created);
        assert!(second.token_id.is_none());
        let original_token = fs::read_to_string(&token).expect("original token");
        let refreshed = provision_local_credentials(root.to_str().expect("root utf8"), true)
            .expect("refresh token");
        assert!(!refreshed.created);
        assert!(refreshed.token_id.is_some());
        assert_ne!(
            fs::read_to_string(&token).expect("refreshed token"),
            original_token
        );
        fs::remove_dir_all(root).ok();
    }
}
