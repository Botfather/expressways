use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use chrono::Utc;
use expressways_client::{Client, Endpoint};
use expressways_protocol::{Classification, ControlCommand, ControlRequest, ControlResponse};
use tokio::io::{AsyncRead, AsyncReadExt};
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
        let mut open_options = tokio::fs::OpenOptions::new();
        open_options.read(true);
        #[cfg(unix)]
        open_options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        let file = open_options
            .open(&resolved)
            .await
            .map_err(|error| format!("failed to read {}: {error}", resolved.display()))?;
        let metadata = file
            .metadata()
            .await
            .map_err(|error| format!("failed to inspect {}: {error}", resolved.display()))?;
        if !metadata.is_file() {
            return Err(format!(
                "read_file requires a regular file: {}",
                resolved.display()
            ));
        }

        let mut bytes = Vec::with_capacity(max_bytes.saturating_add(1));
        file.take(max_bytes.saturating_add(1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| format!("failed to read {}: {error}", resolved.display()))?;
        let truncated = metadata.len() > max_bytes as u64 || bytes.len() > max_bytes;
        let keep = bytes.len().min(max_bytes);
        let preview = String::from_utf8_lossy(&bytes[..keep]).to_string();

        Ok(serde_json::json!({
            "ok": true,
            "path": resolved.display().to_string(),
            "byte_length": metadata.len(),
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

        ensure_program_allowed(&program, &context.allowed_exec_programs)?;

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
        let limit = context.max_exec_output_bytes.clamp(256, 1024 * 1024);
        let output = execute_bounded(&program, &arg_list, timeout_ms, limit).await?;

        let stdout = truncate_text(&output.stdout, limit);
        let stderr = truncate_text(&output.stderr, limit);

        Ok(serde_json::json!({
            "ok": output.status.success(),
            "status_code": output.status.code(),
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": output.stdout_truncated,
            "stderr_truncated": output.stderr_truncated,
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

#[derive(Debug)]
struct BoundedProcessOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

async fn execute_bounded(
    program: &str,
    args: &[String],
    timeout_ms: u64,
    limit: usize,
) -> Result<BoundedProcessOutput, String> {
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("LANG", "C.UTF-8")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    {
        for name in ["PATHEXT", "SYSTEMROOT", "WINDIR"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
    }

    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to execute `{program}`: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("failed to capture stdout for `{program}`"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("failed to capture stderr for `{program}`"))?;
    let stdout_reader = tokio::spawn(read_bounded(stdout, limit));
    let stderr_reader = tokio::spawn(read_bounded(stderr, limit));

    let status = match tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait()).await {
        Ok(result) => result.map_err(|error| format!("failed to wait for `{program}`: {error}"))?,
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            stdout_reader.abort();
            stderr_reader.abort();
            return Err(format!("exec timed out after {timeout_ms}ms"));
        }
    };
    let (stdout, stdout_truncated) = finish_reader(stdout_reader, "stdout").await?;
    let (stderr, stderr_truncated) = finish_reader(stderr_reader, "stderr").await?;

    Ok(BoundedProcessOutput {
        status,
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
    })
}

async fn read_bounded<R>(mut reader: R, limit: usize) -> std::io::Result<(Vec<u8>, bool)>
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(captured.len());
        let keep = remaining.min(count);
        captured.extend_from_slice(&buffer[..keep]);
        truncated |= keep < count;
    }
    Ok((captured, truncated))
}

async fn finish_reader(
    mut reader: tokio::task::JoinHandle<std::io::Result<(Vec<u8>, bool)>>,
    stream: &str,
) -> Result<(Vec<u8>, bool), String> {
    match tokio::time::timeout(Duration::from_secs(1), &mut reader).await {
        Ok(Ok(result)) => result.map_err(|error| format!("failed to read {stream}: {error}")),
        Ok(Err(error)) => Err(format!("failed to join {stream} reader: {error}")),
        Err(_) => {
            reader.abort();
            let _ = reader.await;
            Ok((Vec::new(), true))
        }
    }
}

fn ensure_program_allowed(program: &str, allowed_programs: &[String]) -> Result<(), String> {
    if allowed_programs.iter().any(|allowed| allowed == program) {
        return Ok(());
    }
    let configured = if allowed_programs.is_empty() {
        "none (execution is disabled)".to_owned()
    } else {
        allowed_programs.join(", ")
    };
    Err(format!(
        "exec denied for program `{program}`; allowed programs: {configured}"
    ))
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
        return Err("read_file denied because no workspace roots are configured".to_owned());
    }

    let mut allowed = false;
    for root in roots {
        let canonical_root = root.canonicalize().map_err(|error| {
            format!(
                "failed to canonicalize workspace root {}: {error}",
                root.display()
            )
        })?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tool_context(workspace_root: PathBuf) -> ToolContext {
        ToolContext {
            endpoint: Endpoint::Tcp("127.0.0.1:1".to_owned()),
            capability_token: "test-token".to_owned(),
            inbound_topic: "test-inbound".to_owned(),
            workspace_roots: vec![workspace_root],
            allowed_exec_programs: Vec::new(),
            max_exec_output_bytes: 1024,
            instance_id: "test-instance".to_owned(),
        }
    }

    fn temporary_workspace() -> PathBuf {
        let path = std::env::temp_dir().join(format!("expressways-tools-{}", Uuid::now_v7()));
        std::fs::create_dir(&path).expect("create temporary workspace");
        path
    }

    #[test]
    fn empty_exec_allowlist_denies_every_program() {
        let error = ensure_program_allowed("sh", &[]).expect_err("empty allowlist must deny");
        assert!(error.contains("execution is disabled"));
    }

    #[test]
    fn exec_allowlist_requires_exact_program_match() {
        let allowed = vec!["git".to_owned()];
        assert!(ensure_program_allowed("git", &allowed).is_ok());
        assert!(ensure_program_allowed("/usr/bin/git", &allowed).is_err());
        assert!(ensure_program_allowed("sh", &allowed).is_err());
    }

    #[test]
    fn empty_workspace_roots_deny_file_reads() {
        let error = resolve_workspace_path("Cargo.toml", &[])
            .expect_err("empty workspace roots must deny reads");
        assert!(error.contains("no workspace roots"));
    }

    #[tokio::test]
    async fn read_file_reads_only_the_requested_preview() {
        let workspace = temporary_workspace();
        let path = workspace.join("large.txt");
        std::fs::write(&path, b"abcdefghij").expect("write test file");
        let context = test_tool_context(workspace.clone());

        let result = ToolRegistry
            .read_file(
                serde_json::json!({ "path": path, "max_bytes": 4 }),
                &context,
            )
            .await
            .expect("read file preview");

        assert_eq!(result["byte_length"], 10);
        assert_eq!(result["content_preview"], "abcd");
        assert_eq!(result["truncated"], true);
        std::fs::remove_dir_all(workspace).expect("remove temporary workspace");
    }

    #[tokio::test]
    async fn read_file_rejects_non_regular_files() {
        let workspace = temporary_workspace();
        let context = test_tool_context(workspace.clone());

        let error = ToolRegistry
            .read_file(
                serde_json::json!({ "path": &workspace, "max_bytes": 4 }),
                &context,
            )
            .await
            .expect_err("directory reads must be rejected");

        assert!(error.contains("regular file"));
        std::fs::remove_dir_all(workspace).expect("remove temporary workspace");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounded_execution_caps_output_and_reports_truncation() {
        let output = execute_bounded(
            "/bin/sh",
            &[
                "-c".to_owned(),
                "printf 123456789; printf abcdef >&2".to_owned(),
            ],
            1_000,
            4,
        )
        .await
        .expect("execute command");

        assert_eq!(output.stdout, b"1234");
        assert_eq!(output.stderr, b"abcd");
        assert!(output.stdout_truncated);
        assert!(output.stderr_truncated);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounded_execution_clears_unrelated_environment() {
        let output = execute_bounded("/usr/bin/env", &[], 1_000, 16_384)
            .await
            .expect("execute env");
        let environment = String::from_utf8(output.stdout).expect("utf8 environment");

        assert!(
            environment
                .lines()
                .all(|line| { line.starts_with("PATH=") || line == "LANG=C.UTF-8" })
        );
        assert!(!environment.contains("TOKEN="));
        assert!(!environment.contains("KEY="));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounded_execution_enforces_timeout() {
        let started = std::time::Instant::now();
        let error = execute_bounded(
            "/bin/sh",
            &["-c".to_owned(), "sleep 5".to_owned()],
            100,
            1024,
        )
        .await
        .expect_err("command should time out");

        assert!(error.contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
