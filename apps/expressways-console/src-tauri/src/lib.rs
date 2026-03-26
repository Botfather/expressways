use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    AdopterStatusView, AgentCard, AgentQuery, AuthStateView, BrokerMetricsView, ControlCommand,
    ControlRequest, ControlResponse, RegistryEvent, StoredMessage, StreamFrame,
};
use serde::{Deserialize, Serialize};
use tauri::{Emitter, State};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

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
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdvancedControlResult {
    command_type: String,
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

#[derive(Debug, Clone)]
struct ConfigComponentSpec {
    id: String,
    name: String,
    group: String,
    description: String,
    relative_path: String,
    editable: bool,
}

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

    let request_attachment = decode_optional_base64(input.attachment_base64.as_deref())?;

    let endpoint = build_endpoint(&settings)?;
    let mut client = Client::connect(endpoint)
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;
    let command_type = command_name(&command).to_owned();
    let (response, response_attachment) =
        send_command_with_attachment(&mut client, &settings.token, command, request_attachment)
            .await?;
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

    Ok(AdvancedControlResult {
        command_type,
        response_type,
        response: response_json,
        attachment_base64,
        attachment_bytes,
        executed_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
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

    apply_component_content(&root, &spec, input.content)
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

    let limit = input.limit.unwrap_or(50).max(1).min(500);
    let backups = list_component_backups(&root, &spec, limit)?;
    Ok(ConfigBackupsResult {
        component_id: input.component_id,
        backups,
    })
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

    let content = fs::read_to_string(&backup_path)
        .map_err(|error| format!("failed to read backup {}: {error}", backup_path.display()))?;

    let applied = apply_component_content(&root, &spec, content)?;
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
    let mut outcomes = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for raw_service_id in input.service_ids {
        let service_id = raw_service_id.trim().to_owned();
        if service_id.is_empty() || !seen.insert(service_id.clone()) {
            continue;
        }

        if !is_supported_restart_service(&service_id) {
            outcomes.push(ConfigRestartServiceOutcome {
                service_id,
                ok: false,
                status_code: None,
                message: "unsupported service id".to_owned(),
                stdout: String::new(),
                stderr: String::new(),
            });
            continue;
        }

        outcomes.push(run_restart_service(&root, &service_id).await);
    }

    Ok(ConfigRestartServicesResult {
        restarted_at_ms: system_time_to_millis(SystemTime::now()).unwrap_or(0),
        outcomes,
    })
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

fn apply_component_content(
    root: &Path,
    spec: &ConfigComponentSpec,
    content: String,
) -> Result<ConfigComponentUpdateResult, String> {
    if content.trim().is_empty() {
        return Err("configuration payload cannot be empty".to_owned());
    }

    content
        .parse::<toml::Value>()
        .map_err(|error| format!("invalid TOML: {error}"))?;

    let path = root.join(Path::new(&spec.relative_path));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }

    let backup_path = match fs::read_to_string(&path) {
        Ok(existing) => Some(write_component_backup(
            root,
            &spec.relative_path,
            &existing,
        )?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(format!(
                "failed to read existing {} before apply: {error}",
                path.display()
            ));
        }
    };

    let mut output = content;
    if !output.ends_with('\n') {
        output.push('\n');
    }
    fs::write(&path, output)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;

    let restart_hints = restart_hints_for_component(spec);
    let component = load_component(root, spec.clone())?;
    let applied_at_ms = system_time_to_millis(SystemTime::now()).unwrap_or(0);

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
    let metadata = fs::metadata(&file_path).ok();
    let exists = metadata.is_some();
    let updated_at_ms = metadata
        .and_then(|meta| meta.modified().ok())
        .and_then(system_time_to_millis);

    let mut content = String::new();
    let mut parse_error = None;
    let mut sections = Vec::new();

    if exists {
        content = fs::read_to_string(&file_path)
            .map_err(|error| format!("failed to read {}: {error}", file_path.display()))?;
        match component_sections(&content) {
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
    fs::create_dir_all(&backup_root)
        .map_err(|error| format!("failed to create {}: {error}", backup_root.display()))?;

    let timestamp = system_time_to_millis(SystemTime::now()).unwrap_or(0);
    let backup_name = format!(
        "{}.{}.bak.toml",
        relative_path.replace('/', "__"),
        timestamp
    );
    let backup_path = backup_root.join(backup_name);
    fs::write(&backup_path, content)
        .map_err(|error| format!("failed to write backup {}: {error}", backup_path.display()))?;
    Ok(backup_path.display().to_string())
}

fn list_component_backups(
    root: &Path,
    spec: &ConfigComponentSpec,
    limit: usize,
) -> Result<Vec<ConfigBackupEntry>, String> {
    let backup_root = backup_root_path(root);
    let Ok(entries) = fs::read_dir(&backup_root) else {
        return Ok(Vec::new());
    };

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

        let metadata = match fs::metadata(&path) {
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
    let root = root.to_path_buf();
    let service_id_owned = service_id.to_owned();
    let result = tokio::task::spawn_blocking(move || {
        std::process::Command::new("bash")
            .arg("scripts/expressways-service.sh")
            .arg("restart")
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
                "restart completed".to_owned()
            } else {
                "restart failed".to_owned()
            };
            ConfigRestartServiceOutcome {
                service_id: service_id.to_owned(),
                ok,
                status_code,
                message,
                stdout,
                stderr,
            }
        }
        Ok(Err(error)) => ConfigRestartServiceOutcome {
            service_id: service_id.to_owned(),
            ok: false,
            status_code: None,
            message: format!("failed to execute restart command: {error}"),
            stdout: String::new(),
            stderr: String::new(),
        },
        Err(error) => ConfigRestartServiceOutcome {
            service_id: service_id.to_owned(),
            ok: false,
            status_code: None,
            message: format!("restart worker failed: {error}"),
            stdout: String::new(),
            stderr: String::new(),
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

fn component_sections(content: &str) -> Result<Vec<ConfigSectionView>, String> {
    let value = content
        .parse::<toml::Value>()
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
        });
    }
    Ok(sections)
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
            config_console_list_backups,
            config_console_rollback_component,
            config_console_restart_services
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
