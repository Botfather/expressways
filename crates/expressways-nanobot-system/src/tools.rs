use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use chrono::Utc;
use expressways_client::{Client, Endpoint};
use expressways_protocol::{Classification, ControlCommand, ControlRequest, ControlResponse};
use uuid::Uuid;

use crate::model::{
    NanobotInboundEnvelope, NanobotMessageRef, NanobotSessionRef, SYSTEM_SCHEMA_VERSION,
};

#[derive(Debug, Clone)]
pub struct ToolContext {
    pub endpoint: Endpoint,
    pub capability_token: String,
    pub inbound_topic: String,
    pub workspace_roots: Vec<PathBuf>,
    pub allowed_exec_programs: Vec<String>,
    pub max_exec_output_bytes: usize,
    pub instance_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct ToolRegistry;

impl ToolRegistry {
    pub fn available(&self) -> &'static [&'static str] {
        &["echo", "read_file", "exec", "spawn_subagent"]
    }

    pub async fn invoke(
        &self,
        name: &str,
        args: serde_json::Value,
        context: &ToolContext,
        session: &NanobotSessionRef,
    ) -> Result<serde_json::Value, String> {
        match name {
            "echo" => Ok(serde_json::json!({ "ok": true, "echo": args })),
            "read_file" => self.read_file(args, context).await,
            "exec" => self.exec(args, context).await,
            "spawn_subagent" => self.spawn_subagent(args, context, session).await,
            other => Err(format!("unsupported tool `{other}`")),
        }
    }

    async fn read_file(
        &self,
        args: serde_json::Value,
        context: &ToolContext,
    ) -> Result<serde_json::Value, String> {
        let path = args
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "read_file requires string argument `path`".to_owned())?;
        let max_bytes = args
            .get("max_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(16_384)
            .min(1024 * 1024) as usize;

        let resolved = resolve_workspace_path(path, &context.workspace_roots)?;
        let bytes = tokio::fs::read(&resolved)
            .await
            .map_err(|error| format!("failed to read {}: {error}", resolved.display()))?;
        let truncated = bytes.len() > max_bytes;
        let keep = bytes.len().min(max_bytes);
        let preview = String::from_utf8_lossy(&bytes[..keep]).to_string();

        Ok(serde_json::json!({
            "ok": true,
            "path": resolved.display().to_string(),
            "byte_length": bytes.len(),
            "truncated": truncated,
            "content_preview": preview,
        }))
    }

    async fn exec(
        &self,
        args: serde_json::Value,
        context: &ToolContext,
    ) -> Result<serde_json::Value, String> {
        let program = args
            .get("program")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "exec requires string argument `program`".to_owned())?
            .trim()
            .to_owned();
        if program.is_empty() {
            return Err("exec requires a non-empty `program`".to_owned());
        }

        if !context.allowed_exec_programs.is_empty()
            && !context
                .allowed_exec_programs
                .iter()
                .any(|allowed| allowed == &program)
        {
            return Err(format!(
                "exec denied for program `{program}`; allowed programs: {}",
                context.allowed_exec_programs.join(", ")
            ));
        }

        let arg_list = args
            .get("args")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let timeout_ms = args
            .get("timeout_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(5_000)
            .clamp(100, 30_000);
        let limit = context.max_exec_output_bytes.max(256);

        let execution = tokio::task::spawn_blocking({
            let program = program.clone();
            let arg_list = arg_list.clone();
            move || {
                Command::new(program)
                    .args(arg_list)
                    .output()
                    .map_err(|error| error.to_string())
            }
        });

        let output = tokio::time::timeout(Duration::from_millis(timeout_ms), execution)
            .await
            .map_err(|_| format!("exec timed out after {timeout_ms}ms"))?
            .map_err(|error| format!("exec join error: {error}"))??;

        let stdout = truncate_text(&output.stdout, limit);
        let stderr = truncate_text(&output.stderr, limit);

        Ok(serde_json::json!({
            "ok": output.status.success(),
            "status_code": output.status.code(),
            "stdout": stdout,
            "stderr": stderr,
            "truncated_limit_bytes": limit,
        }))
    }

    async fn spawn_subagent(
        &self,
        args: serde_json::Value,
        context: &ToolContext,
        parent_session: &NanobotSessionRef,
    ) -> Result<serde_json::Value, String> {
        let prompt = args
            .get("prompt")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "spawn_subagent requires string argument `prompt`".to_owned())?
            .trim()
            .to_owned();
        if prompt.is_empty() {
            return Err("spawn_subagent requires a non-empty prompt".to_owned());
        }

        let session_id = args
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| {
                format!("{}:subagent:{}", parent_session.session_id, Uuid::now_v7())
            });
        let message_id = Uuid::now_v7().to_string();
        let envelope = NanobotInboundEnvelope {
            schema_version: SYSTEM_SCHEMA_VERSION.to_owned(),
            source_runtime: "expressways-nanobot-subagent".to_owned(),
            instance_id: context.instance_id.clone(),
            session: NanobotSessionRef {
                session_id: session_id.clone(),
                channel: parent_session.channel.clone(),
                account_id: parent_session.account_id.clone(),
                sender_id: "subagent".to_owned(),
                sender_display_name: Some("Subagent".to_owned()),
            },
            message: NanobotMessageRef {
                message_id: Some(message_id.clone()),
                role: Some("user".to_owned()),
                text: Some(prompt),
                attachments: Vec::new(),
            },
            metadata: serde_json::json!({
                "spawned_from_session_id": parent_session.session_id,
                "spawned_at": Utc::now(),
            }),
            received_at: Utc::now(),
        };

        publish_json(
            &context.endpoint,
            &context.capability_token,
            &context.inbound_topic,
            Classification::Internal,
            &envelope,
        )
        .await?;

        Ok(serde_json::json!({
            "ok": true,
            "queued": true,
            "session_id": session_id,
            "message_id": message_id,
            "topic": context.inbound_topic,
        }))
    }
}

fn truncate_text(bytes: &[u8], max_bytes: usize) -> String {
    let keep = bytes.len().min(max_bytes);
    String::from_utf8_lossy(&bytes[..keep]).to_string()
}

fn resolve_workspace_path(path: &str, roots: &[PathBuf]) -> Result<PathBuf, String> {
    let candidate = Path::new(path);
    let absolute = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("failed to resolve current directory: {error}"))?
            .join(candidate)
    };
    let canonical = absolute
        .canonicalize()
        .map_err(|error| format!("failed to canonicalize {}: {error}", absolute.display()))?;

    if roots.is_empty() {
        return Ok(canonical);
    }

    let mut allowed = false;
    for root in roots {
        let canonical_root = root.canonicalize().unwrap_or_else(|_| root.clone());
        if canonical.starts_with(&canonical_root) {
            allowed = true;
            break;
        }
    }

    if !allowed {
        return Err(format!(
            "access to {} is outside allowed workspaces",
            canonical.display()
        ));
    }

    Ok(canonical)
}

pub async fn publish_json<T: serde::Serialize>(
    endpoint: &Endpoint,
    capability_token: &str,
    topic: &str,
    classification: Classification,
    payload: &T,
) -> Result<(), String> {
    let mut client = Client::connect(endpoint.clone())
        .await
        .map_err(|error| format!("failed to connect client: {error}"))?;
    let payload = serde_json::to_string(payload)
        .map_err(|error| format!("failed to serialize payload: {error}"))?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::Publish {
                topic: topic.to_owned(),
                classification: Some(classification),
                payload,
            },
        })
        .await
        .map_err(|error| format!("failed to publish to {topic}: {error}"))?;

    match response {
        ControlResponse::PublishAccepted { .. } => Ok(()),
        ControlResponse::Error { code, message } => {
            Err(format!("publish rejected for {topic}: {code}: {message}"))
        }
        other => Err(format!(
            "unexpected response while publishing to {topic}: {other:?}"
        )),
    }
}
