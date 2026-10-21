use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use base64::Engine as _;
use chrono::{Duration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_audit::{repair_incomplete_tail, verify_file, visit_verified_events};
use expressways_auth::{CapabilityIssuer, verify_detached_signature, write_secret_file};
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    Action, AgentEndpoint, AgentQuery, AgentRegistration, AgentSchemaRef, AuthStateView,
    BrokerMetricsView, CapabilityClaims, CapabilityScope, Classification, ControlCommand,
    ControlRequest, ControlResponse, RetentionClass, StreamFrame, TASK_EVENTS_TOPIC, TASKS_TOPIC,
    TaskEvent, TaskPayload, TaskRequirements, TaskRetryPolicy, TaskStatus, TaskWorkItem, TopicSpec,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_CLI_CONFIG_BYTES: u64 = 1024 * 1024;
const MAX_SUPPORT_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RUNTIME_BACKUP_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RUNTIME_BACKUP_SIGNATURE_BYTES: u64 = 64 * 1024;
const MAX_RUNTIME_BACKUP_PUBLISH_JOURNAL_BYTES: u64 = 64 * 1024;
const MAX_RUNTIME_RESTORE_JOURNAL_BYTES: u64 = 1024 * 1024;
const MAX_CLI_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SUPPORT_BUNDLE_LOG_LINE_BYTES: u64 = 64 * 1024;

fn read_bounded_regular_file(path: &Path, max_bytes: u64) -> anyhow::Result<Vec<u8>> {
    let initial_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if !initial_metadata.file_type().is_file() {
        bail!("{} is not a regular file", path.display());
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect opened file {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{} is not a regular file", path.display());
    }
    if metadata.len() > max_bytes {
        bail!(
            "{} is {} bytes; maximum is {max_bytes}",
            path.display(),
            metadata.len()
        );
    }
    let capacity = usize::try_from(metadata.len())
        .with_context(|| format!("{} is too large to read", path.display()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.len() as u64 > max_bytes {
        bail!("{} grew beyond the {max_bytes}-byte limit", path.display());
    }
    Ok(bytes)
}

fn read_bounded_utf8_file(path: &Path, max_bytes: u64) -> anyhow::Result<String> {
    let bytes = read_bounded_regular_file(path, max_bytes)?;
    String::from_utf8(bytes).with_context(|| format!("{} is not valid UTF-8", path.display()))
}

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    transport: TransportKind,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransportKind {
    Tcp,
    Unix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SupportBundleRedactionProfile {
    Standard,
    Strict,
}

impl SupportBundleRedactionProfile {
    fn policy_name(self) -> &'static str {
        match self {
            Self::Standard => "expressways.support_bundle.redaction.standard.v1",
            Self::Strict => "expressways.support_bundle.redaction.strict.v1",
        }
    }
}

#[derive(Debug, Args, Clone)]
struct TokenArgs {
    #[arg(long, conflicts_with = "token_file")]
    token: Option<String>,
    #[arg(long)]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Command {
    GenerateKeypair {
        #[arg(long, default_value = "dev")]
        key_id: String,
        #[arg(long)]
        private_key: PathBuf,
        #[arg(long)]
        public_key: PathBuf,
    },
    IssueToken {
        #[arg(long, default_value = "dev")]
        key_id: String,
        #[arg(long)]
        private_key: PathBuf,
        #[arg(long)]
        principal: String,
        #[arg(long, default_value = "expressways")]
        audience: String,
        #[arg(long, default_value_t = 3600)]
        expires_in_seconds: i64,
        #[arg(long = "scope", value_parser = parse_scope)]
        scopes: Vec<CapabilityScope>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    ValidatePrincipal {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        principal: String,
        #[arg(long)]
        key_id: Option<String>,
        #[arg(long, default_value_t = false)]
        allow_disabled: bool,
        #[arg(long, default_value_t = false)]
        allow_policy_gap: bool,
    },
    AuthState {
        #[command(flatten)]
        token: TokenArgs,
    },
    Adopters {
        #[command(flatten)]
        token: TokenArgs,
    },
    Metrics {
        #[command(flatten)]
        token: TokenArgs,
    },
    RegisterAgent {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        agent_id: String,
        #[arg(long)]
        display_name: String,
        #[arg(long)]
        version: String,
        #[arg(long, default_value = "")]
        summary: String,
        #[arg(long = "skill")]
        skills: Vec<String>,
        #[arg(long = "subscribe")]
        subscriptions: Vec<String>,
        #[arg(long = "publish-topic")]
        publications: Vec<String>,
        #[arg(long = "schema", value_parser = parse_schema)]
        schemas: Vec<AgentSchemaRef>,
        #[arg(long, default_value = "control_tcp")]
        endpoint_transport: String,
        #[arg(long)]
        endpoint_address: String,
        #[arg(long, default_value = "internal")]
        classification: Classification,
        #[arg(long, default_value = "operational")]
        retention_class: RetentionClass,
        #[arg(long)]
        ttl_seconds: Option<u64>,
    },
    HeartbeatAgent {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        agent_id: String,
    },
    ListAgents {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value_t = false)]
        include_stale: bool,
    },
    WatchAgents {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value_t = false)]
        include_stale: bool,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        max_events: usize,
        #[arg(long, default_value_t = 30000)]
        wait_timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        follow: bool,
    },
    WatchAgentsStream {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value_t = false)]
        include_stale: bool,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        max_events: usize,
        #[arg(long, default_value_t = 30000)]
        wait_timeout_ms: u64,
        #[arg(long, default_value_t = true)]
        resume: bool,
        #[arg(long, default_value_t = 250)]
        retry_delay_ms: u64,
    },
    CleanupStaleAgents {
        #[command(flatten)]
        token: TokenArgs,
    },
    RemoveAgent {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        agent_id: String,
    },
    RevokeToken {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        token_id: Uuid,
    },
    RevokePrincipal {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        principal: String,
    },
    RevokeKey {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        key_id: String,
    },
    Health {
        #[command(flatten)]
        token: TokenArgs,
    },
    CreateTopic {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value = "operational")]
        retention_class: RetentionClass,
        #[arg(long, default_value = "internal")]
        classification: Classification,
    },
    Publish {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        payload: String,
        #[arg(long)]
        classification: Option<Classification>,
    },
    PutArtifact {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        artifact_id: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        text: Option<String>,
        #[arg(long)]
        base64: Option<String>,
        #[arg(long)]
        content_type: Option<String>,
        #[arg(long)]
        sha256: Option<String>,
        #[arg(long)]
        classification: Option<Classification>,
        #[arg(long, default_value = "operational")]
        retention_class: RetentionClass,
    },
    GetArtifact {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        artifact_id: String,
        #[arg(long)]
        output_file: Option<PathBuf>,
    },
    StatArtifact {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        artifact_id: String,
    },
    SubmitTask {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = TASKS_TOPIC)]
        topic: String,
        #[arg(long)]
        task_id: Option<String>,
        #[arg(long)]
        task_type: String,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        requires_topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long = "preferred-agent")]
        preferred_agents: Vec<String>,
        #[arg(long = "avoid-agent")]
        avoid_agents: Vec<String>,
        #[arg(long)]
        required_agent: Option<String>,
        #[arg(long)]
        affinity_key: Option<String>,
        #[arg(long, default_value_t = 0)]
        priority: i32,
        #[arg(long)]
        payload_json: Option<String>,
        #[arg(long)]
        payload_text: Option<String>,
        #[arg(long)]
        payload_file: Option<PathBuf>,
        #[arg(long)]
        payload_base64: Option<String>,
        #[arg(long)]
        payload_inline: bool,
        #[arg(long)]
        payload_content_type: Option<String>,
        #[arg(long)]
        payload_sha256: Option<String>,
        #[arg(long, default_value_t = 3)]
        max_attempts: u32,
        #[arg(long, default_value_t = 300)]
        timeout_seconds: u64,
        #[arg(long, default_value_t = 5)]
        retry_delay_seconds: u64,
        #[arg(long)]
        classification: Option<Classification>,
    },
    ReportTask {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        topic: String,
        #[arg(long)]
        task_id: String,
        #[arg(long)]
        task_offset: Option<u64>,
        #[arg(long)]
        assignment_id: Uuid,
        #[arg(long)]
        agent_id: String,
        #[arg(long)]
        status: TaskStatus,
        #[arg(long, default_value_t = 1)]
        attempt: u32,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        classification: Option<Classification>,
    },
    Consume {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    VerifyAudit {
        #[arg(long, default_value = "./var/audit/audit.jsonl")]
        path: PathBuf,
    },
    RepairAuditTail {
        #[arg(long, default_value = "./var/audit/audit.jsonl")]
        path: PathBuf,
        #[arg(long)]
        quarantine_output: PathBuf,
        #[arg(long, default_value_t = false)]
        confirm: bool,
    },
    ExportAudit {
        #[arg(long, default_value = "./var/audit/audit.jsonl")]
        path: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    ExportSupportBundle {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "configs/expressways.example.toml")]
        config: PathBuf,
        #[arg(long, default_value = "./var/audit/audit.jsonl")]
        audit_log: PathBuf,
        #[arg(long, default_value = "./var/agent/config-audit/entries.jsonl")]
        config_audit_log: PathBuf,
        #[arg(long, default_value = "./var/agent/service-control/logs")]
        logs_dir: PathBuf,
        #[arg(long, default_value_t = 5)]
        head_lines: usize,
        #[arg(long, default_value_t = 5)]
        tail_lines: usize,
        #[arg(long, default_value_t = 20)]
        log_tail_lines: usize,
        #[arg(long, default_value_t = true)]
        redact_sensitive: bool,
        #[arg(long, value_enum, default_value_t = SupportBundleRedactionProfile::Standard)]
        redaction_profile: SupportBundleRedactionProfile,
        #[arg(long, default_value = "[REDACTED]")]
        redact_placeholder: String,
        #[arg(long, default_value = "./var/agent/support-bundle.json")]
        output: PathBuf,
    },
    ValidateSupportBundle {
        #[arg(long, default_value = "./var/agent/support-bundle.json")]
        bundle: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BackupRuntime {
        #[arg(long, default_value = "configs/expressways.example.toml")]
        config: PathBuf,
        #[arg(long, default_value = "./var/agent/backups")]
        output_dir: PathBuf,
        #[arg(long)]
        backup_id: Option<String>,
        #[arg(long, default_value = "./var/agent/config-audit/entries.jsonl")]
        config_audit_log: PathBuf,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        orchestrator_state: PathBuf,
        #[arg(
            long,
            value_name = "NAME=PATH",
            default_values = [
                "interop_bridge=./var/agent/interop-bridge-state.json",
                "nanobot_runtime=./var/agent/nanobot-runtime"
            ]
        )]
        additional_state: Vec<String>,
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        /// Confirm that the broker, orchestrator, adapters, and other state writers are stopped.
        #[arg(long, default_value_t = false)]
        confirm_writers_stopped: bool,
        #[arg(long)]
        signing_private_key: PathBuf,
        #[arg(long, default_value = "runtime-backup")]
        signing_key_id: String,
    },
    RestoreRuntime {
        #[arg(long)]
        backup_dir: PathBuf,
        #[arg(long, default_value = "configs/expressways.example.toml")]
        config: PathBuf,
        #[arg(long, default_value = "./var/agent/config-audit/entries.jsonl")]
        config_audit_log: PathBuf,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        orchestrator_state: PathBuf,
        #[arg(
            long,
            value_name = "NAME=PATH",
            default_values = [
                "interop_bridge=./var/agent/interop-bridge-state.json",
                "nanobot_runtime=./var/agent/nanobot-runtime"
            ]
        )]
        additional_state: Vec<String>,
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Confirm that the broker, orchestrator, adapters, and other state writers are stopped.
        #[arg(long, default_value_t = false)]
        confirm_writers_stopped: bool,
        #[arg(long)]
        verification_public_key: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::GenerateKeypair {
            key_id,
            private_key,
            public_key,
        } => {
            let issuer = CapabilityIssuer::generate(key_id);
            issuer.write_private_key(&private_key)?;
            issuer.write_public_key(&public_key)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "private_key": private_key,
                    "public_key": public_key
                }))?
            );
            Ok(())
        }
        Command::IssueToken {
            key_id,
            private_key,
            principal,
            audience,
            expires_in_seconds,
            scopes,
            output,
        } => {
            if scopes.is_empty() {
                bail!("at least one --scope must be provided");
            }

            let issuer = CapabilityIssuer::from_private_key_file(key_id, private_key)?;
            let token_id = Uuid::now_v7();
            let expires_at = Utc::now() + Duration::seconds(expires_in_seconds);
            let claims = CapabilityClaims {
                token_id,
                principal: principal.clone(),
                audience: audience.clone(),
                issued_at: Utc::now(),
                expires_at,
                scopes,
            };
            let token = issuer.issue(claims)?;
            if let Some(path) = output {
                write_secret_file(&path, token.as_bytes())
                    .with_context(|| format!("failed to write {}", path.display()))?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "token_file": path,
                        "token_id": token_id,
                        "principal": principal,
                        "audience": audience,
                        "expires_at": expires_at,
                    }))?
                );
            } else {
                println!("{token}");
            }
            Ok(())
        }
        Command::ValidatePrincipal {
            config,
            principal,
            key_id,
            allow_disabled,
            allow_policy_gap,
        } => {
            let report = validate_principal_in_config(
                &config,
                &principal,
                key_id.as_deref(),
                allow_disabled,
                allow_policy_gap,
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::VerifyAudit { path } => {
            let report = verify_file(&path)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::RepairAuditTail {
            path,
            quarantine_output,
            confirm,
        } => {
            if !confirm {
                bail!(
                    "audit tail repair mutates the live log; inspect and preserve the source, then rerun with --confirm"
                );
            }
            let report = repair_incomplete_tail(&path, &quarantine_output)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::ExportAudit { path, output } => {
            let event_count = export_verified_audit(&path, &output)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "output": output,
                    "event_count": event_count,
                }))?
            );
            Ok(())
        }
        Command::ExportSupportBundle {
            token,
            config,
            audit_log,
            config_audit_log,
            logs_dir,
            head_lines,
            tail_lines,
            log_tail_lines,
            redact_sensitive,
            redaction_profile,
            redact_placeholder,
            output,
        } => {
            let mut warnings = Vec::new();
            let redaction_settings = SupportBundleRedactionSettings {
                enabled: redact_sensitive,
                profile: redaction_profile,
                placeholder: redact_placeholder.trim().to_owned(),
            };
            let mut redacted_lines = 0usize;
            let config_summary = summarize_support_bundle_config(&config, &mut warnings)?;
            let audit_summary = summarize_support_bundle_audit(
                &audit_log,
                head_lines,
                tail_lines,
                "audit log",
                &mut warnings,
                &redaction_settings,
                &mut redacted_lines,
            )?;
            let config_audit_summary = summarize_support_bundle_audit(
                &config_audit_log,
                head_lines,
                tail_lines,
                "config audit log",
                &mut warnings,
                &redaction_settings,
                &mut redacted_lines,
            )?;
            let log_summaries = summarize_support_bundle_logs(
                &logs_dir,
                log_tail_lines,
                &mut warnings,
                &redaction_settings,
                &mut redacted_lines,
            )?;

            let endpoint =
                endpoint_from_cli(cli.transport, cli.address.clone(), cli.socket.clone())?;
            let capability_token = resolve_optional_token(token)?;
            let broker_snapshot = if let Some(capability_token) = capability_token {
                let (snapshot, broker_warnings) =
                    collect_support_bundle_broker_snapshot(endpoint, capability_token).await;
                warnings.extend(broker_warnings);
                snapshot
            } else {
                warnings.push(
                    "broker snapshot omitted: no --token or --token-file provided".to_owned(),
                );
                None
            };

            let bundle = SupportBundle {
                schema_version: "expressways.support_bundle.v1".to_owned(),
                generated_at: Utc::now(),
                broker_transport: match cli.transport {
                    TransportKind::Tcp => "tcp".to_owned(),
                    TransportKind::Unix => "unix".to_owned(),
                },
                broker_address: cli.address,
                config: config_summary,
                audit: audit_summary,
                config_audit: config_audit_summary,
                logs: log_summaries,
                broker_snapshot,
                redaction: SupportBundleRedactionSummary {
                    enabled: redaction_settings.enabled,
                    profile: redaction_settings.profile,
                    policy: redaction_settings.profile.policy_name().to_owned(),
                    placeholder: redaction_settings.placeholder.clone(),
                    redacted_lines,
                },
                warnings: warnings.clone(),
            };

            atomic_private_replace_file(&output, &serde_json::to_vec_pretty(&bundle)?)
                .with_context(|| format!("failed to write {}", output.display()))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "output": output,
                    "warning_count": warnings.len(),
                    "log_files": bundle.logs.len(),
                    "audit_lines": bundle.audit.line_count,
                    "config_audit_lines": bundle.config_audit.line_count,
                    "redaction_profile": bundle.redaction.profile,
                    "redaction_policy": bundle.redaction.policy,
                    "redacted_lines": bundle.redaction.redacted_lines,
                    "broker_snapshot_included": bundle.broker_snapshot.is_some(),
                }))?
            );
            Ok(())
        }
        Command::ValidateSupportBundle { bundle, output } => {
            let report = validate_support_bundle_coverage(&bundle)?;
            if let Some(path) = output {
                atomic_private_replace_file(&path, &serde_json::to_vec_pretty(&report)?)
                    .with_context(|| format!("failed to write {}", path.display()))?;
            }
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.pass {
                bail!(
                    "support bundle coverage validation failed with {} undiagnosable incidents",
                    report.undiagnosable_incidents
                );
            }
            Ok(())
        }
        Command::BackupRuntime {
            config,
            output_dir,
            backup_id,
            config_audit_log,
            orchestrator_state,
            additional_state,
            overwrite,
            confirm_writers_stopped,
            signing_private_key,
            signing_key_id,
        } => {
            let backup = create_runtime_backup(BackupRuntimeOptions {
                config,
                output_dir,
                backup_id,
                config_audit_log,
                orchestrator_state,
                additional_state,
                overwrite,
                confirm_writers_stopped,
                signing_private_key,
                signing_key_id,
            })?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "backup_dir": backup.backup_dir,
                    "manifest": backup.manifest_path,
                    "signature": backup.signature_path,
                    "copied_entries": backup.copied_entries,
                    "missing_entries": backup.missing_entries,
                    "warning_count": backup.warnings.len(),
                    "warnings": backup.warnings,
                }))?
            );
            Ok(())
        }
        Command::RestoreRuntime {
            backup_dir,
            config,
            config_audit_log,
            orchestrator_state,
            additional_state,
            overwrite,
            dry_run,
            confirm_writers_stopped,
            verification_public_key,
        } => {
            let restore = restore_runtime_backup(RestoreRuntimeOptions {
                backup_dir,
                config,
                config_audit_log,
                orchestrator_state,
                additional_state,
                overwrite,
                dry_run,
                confirm_writers_stopped,
                verification_public_key,
            })?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "manifest": restore.manifest_path,
                    "restored_entries": restore.restored_entries,
                    "skipped_missing_optional_entries": restore.skipped_missing_optional_entries,
                    "dry_run": restore.dry_run,
                }))?
            );
            Ok(())
        }
        Command::WatchAgents {
            token,
            skill,
            topic,
            principal,
            include_stale,
            cursor,
            max_events,
            wait_timeout_ms,
            follow,
        } => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let mut client = Client::connect(endpoint).await?;
            let capability_token = resolve_token(token)?;
            let query = AgentQuery {
                skill,
                topic,
                principal,
                include_stale,
            };
            let mut next_cursor = cursor;

            loop {
                let response = client
                    .send(ControlRequest {
                        capability_token: capability_token.clone(),
                        command: ControlCommand::WatchAgents {
                            query: query.clone(),
                            cursor: next_cursor,
                            max_events,
                            wait_timeout_ms,
                        },
                    })
                    .await?;
                println!("{}", serde_json::to_string_pretty(&response)?);

                match response {
                    ControlResponse::RegistryEvents { cursor, .. } => {
                        if !follow {
                            break;
                        }
                        next_cursor = Some(cursor);
                    }
                    ControlResponse::Error { .. } => break,
                    other => {
                        bail!("unexpected response for watch-agents: {other:?}");
                    }
                }
            }

            Ok(())
        }
        Command::WatchAgentsStream {
            token,
            skill,
            topic,
            principal,
            include_stale,
            cursor,
            max_events,
            wait_timeout_ms,
            resume,
            retry_delay_ms,
        } => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let capability_token = resolve_token(token)?;
            let query = AgentQuery {
                skill,
                topic,
                principal,
                include_stale,
            };

            let mut next_cursor = cursor;
            loop {
                let client = Client::connect(endpoint.clone()).await?;
                let mut stream = client.into_stream();

                let opened = stream
                    .open(ControlRequest {
                        capability_token: capability_token.clone(),
                        command: ControlCommand::OpenAgentWatchStream {
                            query: query.clone(),
                            cursor: next_cursor,
                            max_events,
                            wait_timeout_ms,
                        },
                    })
                    .await?;
                println!("{}", serde_json::to_string_pretty(&opened)?);

                match opened {
                    StreamFrame::AgentWatchOpened { cursor } => {
                        next_cursor = Some(cursor);
                    }
                    StreamFrame::StreamError { .. } | StreamFrame::StreamClosed { .. } => {
                        return Ok(());
                    }
                    other => {
                        bail!("unexpected opening frame for watch-agents-stream: {other:?}");
                    }
                }

                let mut should_resume = false;
                let mut saw_terminal_frame = false;
                while let Some(frame) = stream.next_frame().await? {
                    println!("{}", serde_json::to_string_pretty(&frame)?);
                    match frame {
                        StreamFrame::RegistryEvents { cursor, .. }
                        | StreamFrame::KeepAlive { cursor } => {
                            next_cursor = Some(cursor);
                        }
                        StreamFrame::StreamClosed { cursor, .. } => {
                            next_cursor = Some(cursor);
                            should_resume = resume;
                            saw_terminal_frame = true;
                            break;
                        }
                        StreamFrame::StreamError { code, .. } => {
                            if resume && code == "connection_closed" {
                                should_resume = true;
                                saw_terminal_frame = true;
                                break;
                            }
                            return Ok(());
                        }
                        StreamFrame::AgentWatchOpened { .. } => {
                            bail!("unexpected additional opening frame from watch stream");
                        }
                    }
                }

                if !saw_terminal_frame && resume {
                    should_resume = true;
                }

                if !should_resume {
                    return Ok(());
                }

                tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms)).await;
            }
        }
        Command::PutArtifact {
            token,
            artifact_id,
            file,
            text,
            base64,
            content_type,
            sha256,
            classification,
            retention_class,
        } => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let mut client = Client::connect(endpoint).await?;
            let capability_token = resolve_token(token)?;
            let (command, attachment) = build_put_artifact_request(
                artifact_id,
                file,
                text,
                base64,
                content_type,
                sha256,
                classification,
                retention_class,
            )?;
            let (response, returned_attachment) = client
                .send_with_attachment(
                    ControlRequest {
                        capability_token,
                        command,
                    },
                    Some(attachment),
                )
                .await?;
            if returned_attachment.is_some() {
                bail!("broker returned unexpected binary attachment for put-artifact");
            }
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        Command::GetArtifact {
            token,
            artifact_id,
            output_file,
        } => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let mut client = Client::connect(endpoint).await?;
            let (response, attachment) = client
                .send_with_attachment(
                    ControlRequest {
                        capability_token: resolve_token(token)?,
                        command: ControlCommand::GetArtifact { artifact_id },
                    },
                    None,
                )
                .await?;
            if let Some(path) = output_file {
                match &response {
                    ControlResponse::Artifact { artifact } => {
                        let bytes = attachment
                            .as_deref()
                            .context("broker response is missing artifact bytes")?;
                        atomic_private_replace_file(&path, bytes)
                            .with_context(|| format!("failed to write {}", path.display()))?;
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "artifact": artifact,
                                "output_file": path,
                            }))?
                        );
                    }
                    _ => {
                        println!("{}", serde_json::to_string_pretty(&response)?);
                    }
                }
            } else if attachment.is_some() {
                match &response {
                    ControlResponse::Artifact { artifact } => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "artifact": artifact,
                                "byte_stream_available": true,
                            }))?
                        );
                    }
                    _ => {
                        println!("{}", serde_json::to_string_pretty(&response)?);
                    }
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            Ok(())
        }
        Command::SubmitTask {
            token,
            topic,
            task_id,
            task_type,
            skill,
            requires_topic,
            principal,
            preferred_agents,
            avoid_agents,
            required_agent,
            affinity_key,
            priority,
            payload_json,
            payload_text,
            payload_file,
            payload_base64,
            payload_inline,
            payload_content_type,
            payload_sha256,
            max_attempts,
            timeout_seconds,
            retry_delay_seconds,
            classification,
        } => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let mut client = Client::connect(endpoint).await?;
            let capability_token = resolve_token(token)?;
            let payload = build_submit_task_payload_with_client(
                &mut client,
                &capability_token,
                payload_json,
                payload_text,
                payload_file,
                payload_base64,
                payload_inline,
                payload_content_type,
                payload_sha256,
                classification.clone(),
            )
            .await?;
            let response = client
                .send(ControlRequest {
                    capability_token,
                    command: ControlCommand::Publish {
                        topic,
                        classification,
                        payload: serde_json::to_string(&TaskWorkItem {
                            task_id: task_id.unwrap_or_else(|| Uuid::now_v7().to_string()),
                            task_type,
                            priority,
                            requirements: TaskRequirements {
                                skill,
                                topic: requires_topic,
                                principal,
                                preferred_agents,
                                avoid_agents,
                                required_agent,
                                affinity_key,
                            },
                            payload,
                            retry_policy: TaskRetryPolicy {
                                max_attempts,
                                timeout_seconds,
                                retry_delay_seconds,
                            },
                            submitted_at: Utc::now(),
                        })?,
                    },
                })
                .await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        command => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let mut client = Client::connect(endpoint).await?;
            let request = request_from_command(command)?;
            let response = client.send(request).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
    }
}

fn endpoint_from_cli(
    transport: TransportKind,
    address: String,
    socket: PathBuf,
) -> anyhow::Result<Endpoint> {
    match transport {
        TransportKind::Tcp => Ok(Endpoint::Tcp(address)),
        TransportKind::Unix => {
            #[cfg(unix)]
            {
                Ok(Endpoint::Unix(socket))
            }
            #[cfg(not(unix))]
            {
                let _ = socket;
                bail!("unix transport is not supported on this platform")
            }
        }
    }
}

fn request_from_command(command: Command) -> anyhow::Result<ControlRequest> {
    match command {
        Command::AuthState { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::GetAuthState,
        }),
        Command::Adopters { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::GetAdopters,
        }),
        Command::Metrics { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::GetMetrics,
        }),
        Command::RegisterAgent {
            token,
            agent_id,
            display_name,
            version,
            summary,
            skills,
            subscriptions,
            publications,
            schemas,
            endpoint_transport,
            endpoint_address,
            classification,
            retention_class,
            ttl_seconds,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RegisterAgent {
                registration: AgentRegistration {
                    agent_id,
                    display_name,
                    version,
                    summary,
                    skills,
                    subscriptions,
                    publications,
                    schemas,
                    endpoint: AgentEndpoint {
                        transport: endpoint_transport,
                        address: endpoint_address,
                    },
                    classification,
                    retention_class,
                    ttl_seconds,
                },
            },
        }),
        Command::HeartbeatAgent { token, agent_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::HeartbeatAgent { agent_id },
        }),
        Command::ListAgents {
            token,
            skill,
            topic,
            principal,
            include_stale,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::ListAgents {
                query: AgentQuery {
                    skill,
                    topic,
                    principal,
                    include_stale,
                },
            },
        }),
        Command::WatchAgents { .. } => {
            bail!("watch-agents is handled directly by the CLI runtime")
        }
        Command::WatchAgentsStream { .. } => {
            bail!("watch-agents-stream is handled directly by the CLI runtime")
        }
        Command::CleanupStaleAgents { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::CleanupStaleAgents,
        }),
        Command::RemoveAgent { token, agent_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RemoveAgent { agent_id },
        }),
        Command::RevokeToken { token, token_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RevokeToken { token_id },
        }),
        Command::RevokePrincipal { token, principal } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RevokePrincipal { principal },
        }),
        Command::RevokeKey { token, key_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RevokeKey { key_id },
        }),
        Command::Health { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::Health,
        }),
        Command::CreateTopic {
            token,
            topic,
            retention_class,
            classification,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: topic,
                    retention_class,
                    default_classification: classification,
                },
            },
        }),
        Command::Publish {
            token,
            topic,
            payload,
            classification,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::Publish {
                topic,
                classification,
                payload,
            },
        }),
        Command::PutArtifact { .. } => {
            bail!("put-artifact is handled directly by the CLI runtime")
        }
        Command::GetArtifact { .. } => {
            bail!("get-artifact is handled directly by the CLI runtime")
        }
        Command::StatArtifact { token, artifact_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::StatArtifact { artifact_id },
        }),
        Command::SubmitTask {
            token,
            topic,
            task_id,
            task_type,
            skill,
            requires_topic,
            principal,
            preferred_agents,
            avoid_agents,
            required_agent,
            affinity_key,
            priority,
            payload_json,
            payload_text,
            payload_file,
            payload_base64,
            payload_inline,
            payload_content_type,
            payload_sha256,
            max_attempts,
            timeout_seconds,
            retry_delay_seconds,
            classification,
        } => {
            if payload_file.is_some() && !payload_inline {
                bail!(
                    "submit-task with broker-managed file uploads is handled directly by the CLI runtime"
                );
            }

            Ok(ControlRequest {
                capability_token: resolve_token(token)?,
                command: ControlCommand::Publish {
                    topic,
                    classification,
                    payload: serde_json::to_string(&TaskWorkItem {
                        task_id: task_id.unwrap_or_else(|| Uuid::now_v7().to_string()),
                        task_type,
                        priority,
                        requirements: TaskRequirements {
                            skill,
                            topic: requires_topic,
                            principal,
                            preferred_agents,
                            avoid_agents,
                            required_agent,
                            affinity_key,
                        },
                        payload: build_submit_task_payload(
                            payload_json,
                            payload_text,
                            payload_file,
                            payload_base64,
                            payload_inline,
                            payload_content_type,
                            payload_sha256,
                        )?,
                        retry_policy: TaskRetryPolicy {
                            max_attempts,
                            timeout_seconds,
                            retry_delay_seconds,
                        },
                        submitted_at: Utc::now(),
                    })?,
                },
            })
        }
        Command::ReportTask {
            token,
            topic,
            task_id,
            task_offset,
            assignment_id,
            agent_id,
            status,
            attempt,
            reason,
            classification,
        } => {
            if !matches!(status, TaskStatus::Completed | TaskStatus::Failed) {
                bail!("report-task status must be either completed or failed");
            }

            Ok(ControlRequest {
                capability_token: resolve_token(token)?,
                command: ControlCommand::Publish {
                    topic,
                    classification,
                    payload: serde_json::to_string(&TaskEvent {
                        event_id: Uuid::now_v7(),
                        task_id,
                        task_offset,
                        assignment_id: Some(assignment_id),
                        agent_id: Some(agent_id),
                        status,
                        attempt,
                        reason,
                        emitted_at: Utc::now(),
                    })?,
                },
            })
        }
        Command::Consume {
            token,
            topic,
            offset,
            limit,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::Consume {
                topic,
                offset,
                limit,
            },
        }),
        Command::GenerateKeypair { .. }
        | Command::IssueToken { .. }
        | Command::ValidatePrincipal { .. }
        | Command::VerifyAudit { .. }
        | Command::RepairAuditTail { .. }
        | Command::ExportAudit { .. }
        | Command::ExportSupportBundle { .. }
        | Command::ValidateSupportBundle { .. }
        | Command::BackupRuntime { .. }
        | Command::RestoreRuntime { .. } => {
            bail!("this command does not produce a control request")
        }
    }
}

fn export_verified_audit(path: &Path, output: &Path) -> anyhow::Result<u64> {
    if path == output
        || (output.exists() && fs::canonicalize(path).ok() == fs::canonicalize(output).ok())
    {
        bail!("audit export output must not replace the source audit log");
    }

    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let file_name = output
        .file_name()
        .context("audit export output must name a file")?
        .to_string_lossy();
    let staged_path = parent.join(format!(".{file_name}.{}.tmp", Uuid::now_v7()));

    let result = (|| -> anyhow::Result<u64> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&staged_path)
            .with_context(|| format!("failed to create {}", staged_path.display()))?;
        let mut writer = BufWriter::new(file);
        writer.write_all(b"{\"events\":[")?;
        let mut first = true;
        let verification = visit_verified_events(path, |event| {
            if !first {
                writer.write_all(b",")?;
            }
            first = false;
            serde_json::to_writer(&mut writer, event)?;
            Ok(())
        })?;
        writer.write_all(b"],\"verification\":")?;
        serde_json::to_writer(&mut writer, &verification)?;
        writer.write_all(b"}\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);

        fs::rename(&staged_path, output)
            .with_context(|| format!("failed to replace {}", output.display()))?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(verification.event_count)
    })();

    if result.is_err() {
        let _ = fs::remove_file(&staged_path);
    }
    result
}

#[derive(Debug, Clone, Deserialize)]
struct BootstrapConfigFile {
    auth: Option<BootstrapAuthConfig>,
    policy: Option<BootstrapPolicyConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct BootstrapAuthConfig {
    #[serde(default)]
    principals: Vec<BootstrapPrincipalConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct BootstrapPrincipalConfig {
    id: String,
    #[serde(default = "default_principal_status")]
    status: String,
    #[serde(default)]
    allowed_key_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BootstrapPolicyConfig {
    #[serde(default)]
    rules: Vec<BootstrapPolicyRuleConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct BootstrapPolicyRuleConfig {
    principal: String,
    resource: String,
    #[serde(default)]
    actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PrincipalValidationReport {
    config: String,
    principal: String,
    status: String,
    allowed_key_ids: Vec<String>,
    key_id: Option<String>,
    key_allowed: bool,
    policy_rule_count: usize,
    policy_resources: Vec<String>,
}

fn validate_principal_in_config(
    path: &Path,
    principal_id: &str,
    key_id: Option<&str>,
    allow_disabled: bool,
    allow_policy_gap: bool,
) -> anyhow::Result<PrincipalValidationReport> {
    let raw = read_bounded_utf8_file(path, MAX_CLI_CONFIG_BYTES)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let report =
        validate_principal_from_toml(&raw, path.display().to_string(), principal_id, key_id)?;

    if !allow_disabled && !report.status.eq_ignore_ascii_case("active") {
        bail!(
            "principal `{}` exists but is not active (status `{}`) in {}",
            principal_id,
            report.status,
            path.display()
        );
    }

    if !allow_policy_gap && report.policy_rule_count == 0 {
        bail!(
            "principal `{}` has no policy rules in {}; broker policy is default-deny",
            principal_id,
            path.display()
        );
    }

    if let Some(key_id) = key_id
        && !report.key_allowed
    {
        bail!(
            "principal `{}` does not allow issuer key `{}` in {}",
            principal_id,
            key_id,
            path.display()
        );
    }

    Ok(report)
}

fn validate_principal_from_toml(
    raw: &str,
    config_label: String,
    principal_id: &str,
    key_id: Option<&str>,
) -> anyhow::Result<PrincipalValidationReport> {
    let config: BootstrapConfigFile = toml::from_str(raw).context("failed to parse TOML config")?;
    let principals = config
        .auth
        .as_ref()
        .map(|auth| auth.principals.as_slice())
        .unwrap_or(&[]);

    let Some(principal) = principals
        .iter()
        .find(|candidate| candidate.id == principal_id)
    else {
        let known = principals
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        if known.is_empty() {
            bail!(
                "principal `{}` is not registered in {} (no auth.principals entries found)",
                principal_id,
                config_label
            );
        }
        bail!(
            "principal `{}` is not registered in {} (available: {})",
            principal_id,
            config_label,
            known
        );
    };

    let policy_rules = config
        .policy
        .as_ref()
        .map(|policy| policy.rules.as_slice())
        .unwrap_or(&[]);
    let mut policy_resources = Vec::new();
    for rule in policy_rules {
        if rule.principal == principal_id {
            let action_suffix = if rule.actions.is_empty() {
                "none".to_owned()
            } else {
                rule.actions.join(",")
            };
            policy_resources.push(format!("{}:{}", rule.resource, action_suffix));
        }
    }
    policy_resources.sort();
    policy_resources.dedup();

    let key_allowed = match key_id {
        Some(target_key_id) => {
            principal.allowed_key_ids.is_empty()
                || principal
                    .allowed_key_ids
                    .iter()
                    .any(|candidate| candidate == target_key_id)
        }
        None => true,
    };

    Ok(PrincipalValidationReport {
        config: config_label,
        principal: principal.id.clone(),
        status: principal.status.clone(),
        allowed_key_ids: principal.allowed_key_ids.clone(),
        key_id: key_id.map(str::to_owned),
        key_allowed,
        policy_rule_count: policy_resources.len(),
        policy_resources,
    })
}

fn default_principal_status() -> String {
    "active".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupportBundle {
    schema_version: String,
    generated_at: chrono::DateTime<Utc>,
    broker_transport: String,
    broker_address: String,
    config: SupportBundleConfigSummary,
    audit: SupportBundleAuditSummary,
    #[serde(default)]
    config_audit: SupportBundleAuditSummary,
    #[serde(default)]
    logs: Vec<SupportBundleLogSummary>,
    broker_snapshot: Option<SupportBundleBrokerSnapshot>,
    #[serde(default)]
    redaction: SupportBundleRedactionSummary,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct SupportBundleRedactionSettings {
    enabled: bool,
    profile: SupportBundleRedactionProfile,
    placeholder: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupportBundleRedactionSummary {
    enabled: bool,
    profile: SupportBundleRedactionProfile,
    policy: String,
    placeholder: String,
    redacted_lines: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupportBundleConfigSummary {
    path: String,
    exists: bool,
    size_bytes: Option<u64>,
    modified_at: Option<chrono::DateTime<Utc>>,
    section_keys: Vec<String>,
    parse_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SupportBundleAuditSummary {
    path: String,
    exists: bool,
    size_bytes: Option<u64>,
    modified_at: Option<chrono::DateTime<Utc>>,
    line_count: usize,
    head: Vec<String>,
    tail: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SupportBundleLogSummary {
    path: String,
    size_bytes: u64,
    modified_at: Option<chrono::DateTime<Utc>>,
    tail: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupportBundleBrokerHealth {
    node_name: String,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupportBundleBrokerSnapshot {
    health: Option<SupportBundleBrokerHealth>,
    metrics: Option<BrokerMetricsView>,
    auth: Option<AuthStateView>,
}

impl Default for SupportBundleRedactionSummary {
    fn default() -> Self {
        Self {
            enabled: false,
            profile: SupportBundleRedactionProfile::Standard,
            policy: String::new(),
            placeholder: "[REDACTED]".to_owned(),
            redacted_lines: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SupportBundleCoverageReport {
    bundle_path: String,
    bundle_schema_version: String,
    generated_at: chrono::DateTime<Utc>,
    incident_count: usize,
    diagnosable_incidents: usize,
    undiagnosable_incidents: usize,
    pass: bool,
    incidents: Vec<SupportBundleIncidentCoverage>,
    warning_count: usize,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SupportBundleIncidentCoverage {
    id: String,
    summary: String,
    diagnosable: bool,
    required_evidence: Vec<String>,
    missing_evidence: Vec<String>,
}

fn validate_support_bundle_coverage(path: &Path) -> anyhow::Result<SupportBundleCoverageReport> {
    let raw = read_bounded_utf8_file(path, MAX_SUPPORT_BUNDLE_BYTES)
        .with_context(|| format!("failed to read support bundle {}", path.display()))?;
    let bundle: SupportBundle = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse support bundle {}", path.display()))?;
    if bundle.schema_version != "expressways.support_bundle.v1" {
        bail!(
            "unsupported support bundle schema version `{}`",
            bundle.schema_version
        );
    }

    let incidents = build_support_bundle_incident_coverage(&bundle);
    let undiagnosable_incidents = incidents.iter().filter(|item| !item.diagnosable).count();
    let diagnosable_incidents = incidents.len().saturating_sub(undiagnosable_incidents);
    Ok(SupportBundleCoverageReport {
        bundle_path: path.display().to_string(),
        bundle_schema_version: bundle.schema_version.clone(),
        generated_at: bundle.generated_at,
        incident_count: incidents.len(),
        diagnosable_incidents,
        undiagnosable_incidents,
        pass: undiagnosable_incidents == 0,
        incidents,
        warning_count: bundle.warnings.len(),
        warnings: bundle.warnings,
    })
}

fn build_support_bundle_incident_coverage(
    bundle: &SupportBundle,
) -> Vec<SupportBundleIncidentCoverage> {
    let has_metrics = bundle
        .broker_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.metrics.as_ref())
        .is_some();
    let has_health = bundle
        .broker_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.health.as_ref())
        .is_some();
    let has_auth_snapshot = bundle
        .broker_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.auth.as_ref())
        .is_some();
    let has_adopter_metrics = bundle
        .broker_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.metrics.as_ref())
        .map(|metrics| !metrics.adopters.is_empty())
        .unwrap_or(false);
    let has_config = bundle.config.exists;
    let has_audit = bundle.audit.exists;
    let has_config_audit = bundle.config_audit.exists;
    let has_redaction_metadata = !bundle.redaction.policy.trim().is_empty();
    let has_storage_config = support_bundle_has_config_section(bundle, "storage");
    let has_auth_config = support_bundle_has_config_section(bundle, "auth");
    let has_policy_config = support_bundle_has_config_section(bundle, "policy");
    let has_quotas_config = support_bundle_has_config_section(bundle, "quotas");
    let has_registry_config = support_bundle_has_config_section(bundle, "registry");

    vec![
        support_bundle_incident(
            "degraded_mode_audit_path",
            "Diagnose degraded-mode serving caused by unavailable audit sink.",
            vec![
                ("broker health snapshot", has_health),
                ("broker metrics snapshot", has_metrics),
                ("audit log summary metadata", has_audit),
                ("config metadata for resilience controls", has_config),
            ],
        ),
        support_bundle_incident(
            "degraded_mode_storage_path",
            "Diagnose degraded-mode serving caused by unavailable storage subsystem.",
            vec![
                ("broker metrics snapshot", has_metrics),
                ("storage section metadata from config", has_storage_config),
                ("audit log summary metadata", has_audit),
            ],
        ),
        support_bundle_incident(
            "storage_pressure_and_retention",
            "Diagnose storage pressure and retention reclamation behavior.",
            vec![
                ("broker metrics snapshot", has_metrics),
                ("storage section metadata from config", has_storage_config),
                ("audit log summary metadata", has_audit),
            ],
        ),
        support_bundle_incident(
            "auth_revocation_denial",
            "Diagnose request denials caused by token/principal/issuer revocation state.",
            vec![
                ("auth snapshot from broker", has_auth_snapshot),
                ("auth section metadata from config", has_auth_config),
                ("audit log summary metadata", has_audit),
            ],
        ),
        support_bundle_incident(
            "policy_denial",
            "Diagnose server-side policy denials (`access_denied`) for authenticated callers.",
            vec![
                ("broker metrics snapshot", has_metrics),
                ("policy section metadata from config", has_policy_config),
                ("audit log summary metadata", has_audit),
            ],
        ),
        support_bundle_incident(
            "quota_denial",
            "Diagnose quota denials (`quota_exceeded`) and profile-level pressure.",
            vec![
                ("broker metrics snapshot", has_metrics),
                ("quotas section metadata from config", has_quotas_config),
                ("audit log summary metadata", has_audit),
            ],
        ),
        support_bundle_incident(
            "issuer_principal_mismatch",
            "Diagnose issuer/principal linkage failures and invalid capability checks.",
            vec![
                ("auth snapshot from broker", has_auth_snapshot),
                ("auth section metadata from config", has_auth_config),
                ("audit log summary metadata", has_audit),
            ],
        ),
        support_bundle_incident(
            "registry_discovery_staleness",
            "Diagnose registry discovery/watch path issues and stale agent visibility.",
            vec![
                ("broker metrics snapshot", has_metrics),
                ("registry section metadata from config", has_registry_config),
                ("audit log summary metadata", has_audit),
            ],
        ),
        support_bundle_incident(
            "adopter_probe_health",
            "Diagnose adopter probe failures and capability guard degradation.",
            vec![
                ("broker metrics snapshot", has_metrics),
                ("adopter status metrics", has_adopter_metrics),
                ("config metadata for adopter controls", has_config),
            ],
        ),
        support_bundle_incident(
            "config_change_regression",
            "Diagnose regressions after operator config changes and rollback attempts.",
            vec![
                ("config file metadata", has_config),
                ("config-audit summary metadata", has_config_audit),
                ("redaction metadata policy", has_redaction_metadata),
                ("audit log summary metadata", has_audit),
            ],
        ),
    ]
}

fn support_bundle_incident(
    id: &str,
    summary: &str,
    requirements: Vec<(&str, bool)>,
) -> SupportBundleIncidentCoverage {
    let required_evidence = requirements
        .iter()
        .map(|(label, _)| (*label).to_owned())
        .collect::<Vec<_>>();
    let missing_evidence = requirements
        .iter()
        .filter_map(|(label, satisfied)| (!*satisfied).then_some((*label).to_owned()))
        .collect::<Vec<_>>();

    SupportBundleIncidentCoverage {
        id: id.to_owned(),
        summary: summary.to_owned(),
        diagnosable: missing_evidence.is_empty(),
        required_evidence,
        missing_evidence,
    }
}

fn support_bundle_has_config_section(bundle: &SupportBundle, section: &str) -> bool {
    bundle
        .config
        .section_keys
        .iter()
        .any(|item| item == section)
}

fn summarize_support_bundle_config(
    path: &Path,
    warnings: &mut Vec<String>,
) -> anyhow::Result<SupportBundleConfigSummary> {
    let (exists, size_bytes, modified_at) = summarize_file_metadata(path);
    if !exists {
        warnings.push(format!("config file not found: {}", path.display()));
        return Ok(SupportBundleConfigSummary {
            path: path.display().to_string(),
            exists,
            size_bytes,
            modified_at,
            section_keys: Vec::new(),
            parse_error: None,
        });
    }

    let raw = read_bounded_utf8_file(path, MAX_CLI_CONFIG_BYTES)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let mut section_keys = Vec::new();
    let mut parse_error = None;
    match toml::from_str::<toml::Value>(&raw) {
        Ok(toml::Value::Table(table)) => {
            section_keys = table.keys().cloned().collect();
            section_keys.sort();
        }
        Ok(_) => {}
        Err(error) => {
            parse_error = Some(error.to_string());
            warnings.push(format!(
                "failed to parse config file {} as TOML: {error}",
                path.display()
            ));
        }
    }

    Ok(SupportBundleConfigSummary {
        path: path.display().to_string(),
        exists,
        size_bytes,
        modified_at,
        section_keys,
        parse_error,
    })
}

fn summarize_support_bundle_audit(
    path: &Path,
    head_lines: usize,
    tail_lines: usize,
    label: &str,
    warnings: &mut Vec<String>,
    redaction_settings: &SupportBundleRedactionSettings,
    redacted_lines: &mut usize,
) -> anyhow::Result<SupportBundleAuditSummary> {
    let (exists, size_bytes, modified_at) = summarize_file_metadata(path);
    if !exists {
        warnings.push(format!("{label} not found: {}", path.display()));
        return Ok(SupportBundleAuditSummary {
            path: path.display().to_string(),
            exists,
            size_bytes,
            modified_at,
            line_count: 0,
            head: Vec::new(),
            tail: Vec::new(),
        });
    }

    let (line_count, head, tail) = read_head_and_tail_lines(
        path,
        head_lines,
        tail_lines,
        redaction_settings,
        redacted_lines,
    )?;
    Ok(SupportBundleAuditSummary {
        path: path.display().to_string(),
        exists,
        size_bytes,
        modified_at,
        line_count,
        head,
        tail,
    })
}

fn summarize_support_bundle_logs(
    logs_dir: &PathBuf,
    tail_lines: usize,
    warnings: &mut Vec<String>,
    redaction_settings: &SupportBundleRedactionSettings,
    redacted_lines: &mut usize,
) -> anyhow::Result<Vec<SupportBundleLogSummary>> {
    let Ok(entries) = fs::read_dir(logs_dir) else {
        warnings.push(format!("logs directory not found: {}", logs_dir.display()));
        return Ok(Vec::new());
    };

    let mut logs = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }

        let (_, _, modified_at) = summarize_file_metadata(&path);
        let tail = match read_head_and_tail_lines(
            &path,
            0,
            tail_lines,
            redaction_settings,
            redacted_lines,
        ) {
            Ok((_, _, tail)) => tail,
            Err(error) => {
                warnings.push(format!(
                    "failed to read log tail from {}: {error}",
                    path.display()
                ));
                Vec::new()
            }
        };
        logs.push(SupportBundleLogSummary {
            path: path.display().to_string(),
            size_bytes: metadata.len(),
            modified_at,
            tail,
        });
    }

    logs.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(logs)
}

fn summarize_file_metadata(path: &Path) -> (bool, Option<u64>, Option<chrono::DateTime<Utc>>) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return (false, None, None);
    };
    if !metadata.file_type().is_file() {
        return (false, None, None);
    }

    let modified_at = metadata.modified().ok().map(chrono::DateTime::<Utc>::from);
    (true, Some(metadata.len()), modified_at)
}

fn read_head_and_tail_lines(
    path: &Path,
    head_lines: usize,
    tail_lines: usize,
    redaction_settings: &SupportBundleRedactionSettings,
    redacted_lines: &mut usize,
) -> anyhow::Result<(usize, Vec<String>, Vec<String>)> {
    let initial_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if !initial_metadata.file_type().is_file() {
        bail!("{} is not a regular file", path.display());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    if !file
        .metadata()
        .with_context(|| format!("failed to inspect {}", path.display()))?
        .is_file()
    {
        bail!("{} is not a regular file", path.display());
    }
    let mut reader = BufReader::new(file);
    let mut head = Vec::new();
    let mut tail = VecDeque::new();
    let mut line_count = 0usize;

    loop {
        let mut line = Vec::new();
        let bytes_read = reader
            .by_ref()
            .take(MAX_SUPPORT_BUNDLE_LOG_LINE_BYTES + 2)
            .read_until(b'\n', &mut line)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        if line.len() as u64 > MAX_SUPPORT_BUNDLE_LOG_LINE_BYTES + 1
            || (line.len() as u64 == MAX_SUPPORT_BUNDLE_LOG_LINE_BYTES + 1
                && line.last() != Some(&b'\n'))
        {
            bail!(
                "{} contains a log line larger than {MAX_SUPPORT_BUNDLE_LOG_LINE_BYTES} bytes",
                path.display()
            );
        }
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        let line = std::str::from_utf8(&line)
            .with_context(|| format!("{} contains invalid UTF-8", path.display()))?;
        line_count = line_count.saturating_add(1);
        let (normalized, redacted) = redact_support_bundle_line(line, redaction_settings);
        if redacted {
            *redacted_lines = redacted_lines.saturating_add(1);
        }
        let normalized = truncate_support_bundle_line(normalized);
        if head_lines > 0 && head.len() < head_lines {
            head.push(normalized.clone());
        }
        if tail_lines > 0 {
            if tail.len() == tail_lines {
                tail.pop_front();
            }
            tail.push_back(normalized);
        }
    }

    Ok((line_count, head, tail.into_iter().collect()))
}

fn truncate_support_bundle_line(value: String) -> String {
    const MAX_CHARS: usize = 400;
    if value.chars().count() <= MAX_CHARS {
        return value;
    }

    let mut truncated = String::with_capacity(MAX_CHARS + 3);
    for (index, ch) in value.chars().enumerate() {
        if index >= MAX_CHARS {
            break;
        }
        truncated.push(ch);
    }
    truncated.push_str("...");
    truncated
}

fn redact_support_bundle_line(
    value: &str,
    settings: &SupportBundleRedactionSettings,
) -> (String, bool) {
    if !settings.enabled {
        return (value.to_owned(), false);
    }

    let placeholder = if settings.placeholder.trim().is_empty() {
        "[REDACTED]"
    } else {
        settings.placeholder.as_str()
    };

    if should_redact_support_bundle_line(value, settings.profile) {
        return (placeholder.to_owned(), true);
    }

    (value.to_owned(), false)
}

fn should_redact_support_bundle_line(value: &str, profile: SupportBundleRedactionProfile) -> bool {
    let lower = value.to_ascii_lowercase();
    let standard_markers = [
        "capability_token",
        "authorization",
        "token_file",
        "admin.token",
        "developer.token",
        "private_key",
        "issuer.private",
        "password",
        "secret",
        "bearer ",
    ];
    if standard_markers.iter().any(|marker| lower.contains(marker)) {
        return true;
    }

    for candidate in value.split_whitespace() {
        let normalized = normalize_support_bundle_token_candidate(candidate);
        if looks_like_capability_token(normalized) {
            return true;
        }
    }

    if profile != SupportBundleRedactionProfile::Strict {
        return false;
    }

    let strict_markers = [
        "api_key",
        "apikey",
        "x-api-key",
        "access_token",
        "refresh_token",
        "client_secret",
        "session_token",
        "set-cookie",
        "cookie:",
        "authorization:",
        "begin private key",
        "ssh-rsa",
        "aws_secret_access_key",
        "aws_access_key_id",
    ];
    if strict_markers.iter().any(|marker| lower.contains(marker)) {
        return true;
    }

    if looks_like_strict_sensitive_assignment(value) {
        return true;
    }

    for candidate in value.split_whitespace() {
        let normalized = normalize_support_bundle_strict_token_candidate(candidate);
        if looks_like_strict_secret_token(normalized) {
            return true;
        }
    }

    false
}

fn normalize_support_bundle_token_candidate(value: &str) -> &str {
    value
        .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '.' && ch != '-' && ch != '_')
}

fn normalize_support_bundle_strict_token_candidate(value: &str) -> &str {
    value.trim_matches(|ch: char| {
        !ch.is_ascii_alphanumeric()
            && ch != '.'
            && ch != '-'
            && ch != '_'
            && ch != '+'
            && ch != '/'
            && ch != '='
    })
}

fn looks_like_strict_sensitive_assignment(value: &str) -> bool {
    const ASSIGNMENT_KEY_MARKERS: [&str; 11] = [
        "token",
        "secret",
        "password",
        "credential",
        "cookie",
        "session",
        "authorization",
        "bearer",
        "private_key",
        "api_key",
        "apikey",
    ];

    let Some((raw_key, raw_value)) = value.split_once('=').or_else(|| value.split_once(':')) else {
        return false;
    };

    let key = raw_key.trim().to_ascii_lowercase();
    if key.is_empty() {
        return false;
    }
    if !ASSIGNMENT_KEY_MARKERS
        .iter()
        .any(|marker| key.contains(marker))
    {
        return false;
    }

    let assigned_value = normalize_support_bundle_strict_token_candidate(raw_value.trim());
    !assigned_value.is_empty()
}

fn looks_like_strict_secret_token(value: &str) -> bool {
    if value.len() < 28 {
        return false;
    }
    if looks_like_uuid(value) {
        return false;
    }

    let valid_charset = value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | '+' | '/' | '='));
    if !valid_charset {
        return false;
    }

    let has_alpha = value.chars().any(|ch| ch.is_ascii_alphabetic());
    let has_digit = value.chars().any(|ch| ch.is_ascii_digit());
    if !(has_alpha && has_digit) {
        return false;
    }

    let has_token_punctuation = value
        .chars()
        .any(|ch| matches!(ch, '.' | '-' | '_' | '+' | '/' | '='));
    has_token_punctuation || value.len() >= 40
}

fn looks_like_uuid(value: &str) -> bool {
    let parts = value.split('-').collect::<Vec<_>>();
    if parts.len() != 5 {
        return false;
    }
    let expected_lengths = [8usize, 4, 4, 4, 12];
    parts
        .iter()
        .zip(expected_lengths.iter())
        .all(|(part, expected)| {
            part.len() == *expected && part.chars().all(|ch| ch.is_ascii_hexdigit())
        })
}

fn looks_like_capability_token(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() != 2 {
        return false;
    }
    if parts[0].len() < 16 || parts[1].len() < 16 {
        return false;
    }
    parts.iter().all(|part| {
        part.chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    })
}

#[derive(Debug, Clone)]
struct BackupRuntimeOptions {
    config: PathBuf,
    output_dir: PathBuf,
    backup_id: Option<String>,
    config_audit_log: PathBuf,
    orchestrator_state: PathBuf,
    additional_state: Vec<String>,
    overwrite: bool,
    confirm_writers_stopped: bool,
    signing_private_key: PathBuf,
    signing_key_id: String,
}

#[derive(Debug, Clone)]
struct RestoreRuntimeOptions {
    backup_dir: PathBuf,
    config: PathBuf,
    config_audit_log: PathBuf,
    orchestrator_state: PathBuf,
    additional_state: Vec<String>,
    overwrite: bool,
    dry_run: bool,
    confirm_writers_stopped: bool,
    verification_public_key: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeBackupResult {
    backup_dir: String,
    manifest_path: String,
    signature_path: String,
    copied_entries: usize,
    missing_entries: usize,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeRestoreResult {
    manifest_path: String,
    restored_entries: usize,
    skipped_missing_optional_entries: usize,
    dry_run: bool,
}

#[derive(Debug, Clone)]
struct RuntimeBackupTarget {
    key: String,
    source_path: PathBuf,
    required: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeBackupConfigFile {
    server: RuntimeBackupServerSection,
    audit: RuntimeBackupAuditSection,
    auth: RuntimeBackupAuthSection,
    #[serde(default)]
    registry: RuntimeBackupRegistrySection,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeBackupServerSection {
    data_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeBackupAuditSection {
    path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeBackupAuthSection {
    revocation_path: PathBuf,
    #[serde(default)]
    issuers: Vec<RuntimeBackupIssuerSection>,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeBackupIssuerSection {
    key_id: String,
    public_key_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RuntimeBackupRegistrySection {
    path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeBackupManifest {
    schema_version: String,
    created_at: chrono::DateTime<Utc>,
    backup_id: String,
    config_path: String,
    entries: Vec<RuntimeBackupManifestEntry>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeBackupManifestEntry {
    key: String,
    source_path: String,
    backup_path: Option<String>,
    kind: RuntimeBackupEntryKind,
    required: bool,
    exists: bool,
    size_bytes: Option<u64>,
    sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeBackupSignature {
    algorithm: String,
    key_id: String,
    signature: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RuntimeBackupEntryKind {
    File,
    Directory,
    Unknown,
}

#[derive(Debug)]
struct RuntimeRestorePlanEntry {
    key: String,
    source_path: PathBuf,
    destination_path: PathBuf,
    kind: RuntimeBackupEntryKind,
}

#[derive(Debug)]
struct PreparedRuntimeRestoreEntry {
    key: String,
    destination_path: PathBuf,
    staged_path: PathBuf,
    previous_path: PathBuf,
    had_previous: bool,
    committed: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RuntimeRestoreJournalPhase {
    Prepared,
    Committed,
}

#[derive(Debug, Serialize, Deserialize)]
struct RuntimeRestoreJournal {
    schema_version: u32,
    operation_id: Uuid,
    phase: RuntimeRestoreJournalPhase,
    entries: Vec<RuntimeRestoreJournalEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RuntimeRestoreJournalEntry {
    key: String,
    destination_path: PathBuf,
    staged_path: PathBuf,
    previous_path: PathBuf,
    had_previous: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct RuntimeBackupPublishJournal {
    schema_version: u32,
    operation_id: Uuid,
    backup_id: String,
    final_path: PathBuf,
    staged_path: PathBuf,
    previous_path: PathBuf,
    had_previous: bool,
}

struct RuntimeBackupStagingGuard {
    path: PathBuf,
    armed: bool,
}

struct RuntimeTransactionLock {
    file: File,
}

impl Drop for RuntimeTransactionLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn acquire_runtime_transaction_lock(path: &Path) -> anyhow::Result<RuntimeTransactionLock> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create transaction lock parent {}",
            parent.display()
        )
    })?;
    let existed = path_exists_without_following(path)?;
    if existed && !fs::symlink_metadata(path)?.file_type().is_file() {
        bail!(
            "transaction lock path is not a regular file: {}",
            path.display()
        );
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("failed to open transaction lock {}", path.display()))?;
    if !file.metadata()?.is_file() {
        bail!(
            "transaction lock path is not a regular file: {}",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    file.lock()
        .with_context(|| format!("failed to acquire transaction lock {}", path.display()))?;
    if !existed {
        file.sync_all()?;
        sync_restore_directory(parent)?;
    }
    Ok(RuntimeTransactionLock { file })
}

impl RuntimeBackupStagingGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RuntimeBackupStagingGuard {
    fn drop(&mut self) {
        if self.armed
            && self.path.exists()
            && remove_path_for_restore(&self.path).is_ok()
            && let Some(parent) = self.path.parent()
        {
            let _ = sync_restore_directory(parent);
        }
    }
}

fn create_runtime_backup(options: BackupRuntimeOptions) -> anyhow::Result<RuntimeBackupResult> {
    if !options.confirm_writers_stopped {
        bail!(
            "backup-runtime requires --confirm-writers-stopped after stopping the broker, orchestrator, adapters, and every other process that writes runtime state"
        );
    }
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let config_path = resolve_runtime_backup_path(&cwd, options.config);
    let output_dir = resolve_runtime_backup_path(&cwd, options.output_dir);
    let backup_id = normalize_runtime_backup_id(
        options
            .backup_id
            .unwrap_or_else(default_runtime_backup_id)
            .as_str(),
    )?;

    let config_audit_log = resolve_runtime_backup_path(&cwd, options.config_audit_log);
    let orchestrator_state = resolve_runtime_backup_path(&cwd, options.orchestrator_state);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create backup output {}", output_dir.display()))?;
    let backup_dir = output_dir.join(&backup_id);
    let publish_journal_path = runtime_backup_publish_journal_path(&output_dir, &backup_id);
    let _transaction_lock =
        acquire_runtime_transaction_lock(&publish_journal_path.with_extension("lock"))?;
    recover_runtime_backup_publish(&publish_journal_path, &output_dir, &backup_id)?;

    if backup_dir.exists() && !options.overwrite {
        bail!(
            "backup directory already exists: {} (use --overwrite to replace it)",
            backup_dir.display()
        );
    }

    let operation_id = Uuid::now_v7();
    let staged_backup_dir = runtime_backup_staged_path(&output_dir, &backup_id, operation_id);
    let previous_backup_dir = runtime_backup_previous_path(&output_dir, &backup_id, operation_id);
    let mut staging_guard = RuntimeBackupStagingGuard::new(staged_backup_dir.clone());

    fs::create_dir_all(staged_backup_dir.join("payload")).with_context(|| {
        format!(
            "failed to create staged backup directory {}",
            staged_backup_dir.display()
        )
    })?;

    let targets = load_runtime_backup_targets(
        &config_path,
        &config_audit_log,
        &orchestrator_state,
        &options.additional_state,
    )?;
    validate_runtime_backup_output_disjoint(&output_dir, &targets)?;
    let mut warnings = Vec::new();
    let mut manifest = RuntimeBackupManifest {
        schema_version: "expressways.runtime_backup.v1".to_owned(),
        created_at: Utc::now(),
        backup_id: backup_id.clone(),
        config_path: config_path.display().to_string(),
        entries: Vec::new(),
        warnings: Vec::new(),
    };

    let mut seen_paths = HashSet::new();
    for target in targets {
        let source_key = target.source_path.display().to_string();
        if !seen_paths.insert(source_key.clone()) {
            warnings.push(format!(
                "skipping duplicate backup target `{}` ({})",
                target.key, source_key
            ));
            continue;
        }

        let metadata = match fs::symlink_metadata(&target.source_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if target.required {
                    bail!(
                        "required backup target `{}` not found: {}",
                        target.key,
                        target.source_path.display()
                    );
                }
                warnings.push(format!(
                    "backup target `{}` not found: {}",
                    target.key,
                    target.source_path.display()
                ));
                manifest.entries.push(RuntimeBackupManifestEntry {
                    key: target.key,
                    source_path: source_key,
                    backup_path: None,
                    kind: RuntimeBackupEntryKind::Unknown,
                    required: target.required,
                    exists: false,
                    size_bytes: None,
                    sha256: None,
                });
                continue;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to inspect backup target `{}` at {}",
                        target.key,
                        target.source_path.display()
                    )
                });
            }
        };

        if metadata.file_type().is_symlink() {
            if target.required {
                bail!(
                    "required backup target `{}` must not be a symlink: {}",
                    target.key,
                    target.source_path.display()
                );
            }
            warnings.push(format!(
                "backup target `{}` is a symlink and was skipped: {}",
                target.key,
                target.source_path.display()
            ));
            manifest.entries.push(RuntimeBackupManifestEntry {
                key: target.key,
                source_path: source_key,
                backup_path: None,
                kind: RuntimeBackupEntryKind::Unknown,
                required: target.required,
                exists: false,
                size_bytes: None,
                sha256: None,
            });
            continue;
        }

        let kind = if metadata.is_file() {
            RuntimeBackupEntryKind::File
        } else if metadata.is_dir() {
            RuntimeBackupEntryKind::Directory
        } else {
            RuntimeBackupEntryKind::Unknown
        };
        if kind == RuntimeBackupEntryKind::Unknown {
            if target.required {
                bail!(
                    "required backup target `{}` has unsupported type: {}",
                    target.key,
                    target.source_path.display()
                );
            }
            warnings.push(format!(
                "backup target `{}` has unsupported type and was skipped: {}",
                target.key,
                target.source_path.display()
            ));
            manifest.entries.push(RuntimeBackupManifestEntry {
                key: target.key,
                source_path: source_key,
                backup_path: None,
                kind,
                required: target.required,
                exists: false,
                size_bytes: None,
                sha256: None,
            });
            continue;
        }

        let relative_path = PathBuf::from("payload").join(format!(
            "{:02}-{}",
            manifest.entries.len() + 1,
            sanitize_runtime_backup_key(&target.key)
        ));
        let backup_path = staged_backup_dir.join(&relative_path);
        copy_runtime_backup_path(&target.source_path, &backup_path, kind)?;

        let (size_bytes, sha256) = runtime_backup_payload_digest(&backup_path, kind)?;

        manifest.entries.push(RuntimeBackupManifestEntry {
            key: target.key,
            source_path: source_key,
            backup_path: Some(relative_path.display().to_string()),
            kind,
            required: target.required,
            exists: true,
            size_bytes: Some(size_bytes),
            sha256: Some(sha256),
        });
    }

    manifest.warnings = warnings;
    let staged_manifest_path = staged_backup_dir.join("manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    write_private_synced_file(&staged_manifest_path, &manifest_bytes)
        .with_context(|| format!("failed to write {}", staged_manifest_path.display()))?;
    let signer = CapabilityIssuer::from_private_key_file(
        &options.signing_key_id,
        &options.signing_private_key,
    )
    .context("failed to load runtime backup signing key")?;
    let signature = RuntimeBackupSignature {
        algorithm: "ed25519".to_owned(),
        key_id: options.signing_key_id,
        signature: signer.sign_bytes(&manifest_bytes),
    };
    let staged_signature_path = staged_backup_dir.join("manifest.sig.json");
    write_private_synced_file(
        &staged_signature_path,
        &serde_json::to_vec_pretty(&signature)?,
    )
    .with_context(|| format!("failed to write {}", staged_signature_path.display()))?;
    sync_runtime_restore_directory_tree(&staged_backup_dir).with_context(|| {
        format!(
            "failed to make staged backup durable at {}",
            staged_backup_dir.display()
        )
    })?;

    let publish_journal = RuntimeBackupPublishJournal {
        schema_version: 1,
        operation_id,
        backup_id: backup_id.clone(),
        final_path: backup_dir.clone(),
        staged_path: staged_backup_dir,
        previous_path: previous_backup_dir,
        had_previous: backup_dir.exists(),
    };
    write_runtime_backup_publish_journal(&publish_journal_path, &publish_journal)?;
    staging_guard.disarm();
    commit_runtime_backup_publish(&publish_journal_path, &output_dir, &publish_journal)?;

    let copied_entries = manifest.entries.iter().filter(|entry| entry.exists).count();
    let missing_entries = manifest.entries.len().saturating_sub(copied_entries);
    let manifest_path = backup_dir.join("manifest.json");
    let signature_path = backup_dir.join("manifest.sig.json");
    Ok(RuntimeBackupResult {
        backup_dir: backup_dir.display().to_string(),
        manifest_path: manifest_path.display().to_string(),
        signature_path: signature_path.display().to_string(),
        copied_entries,
        missing_entries,
        warnings: manifest.warnings,
    })
}

fn restore_runtime_backup(options: RestoreRuntimeOptions) -> anyhow::Result<RuntimeRestoreResult> {
    if !options.dry_run && !options.confirm_writers_stopped {
        bail!(
            "restore-runtime requires --confirm-writers-stopped after stopping the broker, orchestrator, adapters, and every other process that writes runtime state"
        );
    }
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let backup_dir = resolve_runtime_backup_path(&cwd, options.backup_dir);
    let config_path = resolve_runtime_backup_path(&cwd, options.config);
    let verification_public_key =
        resolve_runtime_backup_path(&cwd, options.verification_public_key);
    let config_audit_log = resolve_runtime_backup_path(&cwd, options.config_audit_log);
    let orchestrator_state = resolve_runtime_backup_path(&cwd, options.orchestrator_state);
    let manifest_path = backup_dir.join("manifest.json");
    let raw = read_bounded_regular_file(&manifest_path, MAX_RUNTIME_BACKUP_MANIFEST_BYTES)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let signature_path = backup_dir.join("manifest.sig.json");
    let signature: RuntimeBackupSignature = serde_json::from_slice(
        &read_bounded_regular_file(&signature_path, MAX_RUNTIME_BACKUP_SIGNATURE_BYTES)
            .with_context(|| format!("failed to read {}", signature_path.display()))?,
    )
    .with_context(|| format!("failed to parse {}", signature_path.display()))?;
    if signature.algorithm != "ed25519" {
        bail!(
            "unsupported runtime backup signature algorithm `{}`",
            signature.algorithm
        );
    }
    verify_detached_signature(&verification_public_key, &raw, &signature.signature).with_context(
        || {
            format!(
                "runtime backup manifest signature verification failed for key `{}`",
                signature.key_id
            )
        },
    )?;
    let manifest: RuntimeBackupManifest = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
    if manifest.schema_version != "expressways.runtime_backup.v1" {
        bail!(
            "unsupported runtime backup schema `{}` in {}",
            manifest.schema_version,
            manifest_path.display()
        );
    }

    let config_source_path = if config_path.exists() {
        config_path.clone()
    } else {
        let mut config_entries = manifest
            .entries
            .iter()
            .filter(|entry| entry.key == "config_file");
        let config_entry = config_entries
            .next()
            .context("backup manifest has no config_file entry for disaster recovery")?;
        if config_entries.next().is_some() {
            bail!("backup manifest contains duplicate config_file entries");
        }
        verified_runtime_backup_entry_source(&backup_dir, config_entry)?
    };
    let trusted_targets = load_runtime_backup_targets_from_config(
        &config_source_path,
        &config_path,
        &config_audit_log,
        &orchestrator_state,
        &options.additional_state,
    )?;
    let mut trusted_destinations = trusted_targets
        .into_iter()
        .map(|target| (target.key, target.source_path))
        .collect::<HashMap<_, _>>();

    let mut restored_entries = 0usize;
    let mut skipped_missing_optional_entries = 0usize;
    let mut seen_entry_keys = HashSet::new();
    let mut seen_destinations = HashSet::new();
    let mut restore_plan = Vec::new();
    for entry in &manifest.entries {
        if !seen_entry_keys.insert(entry.key.as_str()) {
            bail!(
                "backup manifest contains duplicate entry key `{}`",
                entry.key
            );
        }
        let destination_path = trusted_destinations.remove(&entry.key).ok_or_else(|| {
            anyhow::anyhow!(
                "backup entry `{}` is not a target in the trusted runtime configuration",
                entry.key
            )
        })?;
        if !seen_destinations.insert(destination_path.clone()) {
            bail!(
                "backup manifest maps multiple entries to restore destination {}",
                destination_path.display()
            );
        }
        if !entry.exists {
            if entry.required {
                bail!(
                    "backup is incomplete: required target `{}` was missing at backup time ({})",
                    entry.key,
                    entry.source_path
                );
            }
            skipped_missing_optional_entries = skipped_missing_optional_entries.saturating_add(1);
            continue;
        }

        let source_path = verified_runtime_backup_entry_source(&backup_dir, entry)?;

        if destination_path.exists() && !options.overwrite && !options.dry_run {
            bail!(
                "restore destination exists for `{}`: {} (use --overwrite to replace it)",
                entry.key,
                destination_path.display()
            );
        }
        restore_plan.push(RuntimeRestorePlanEntry {
            key: entry.key.clone(),
            source_path,
            destination_path,
            kind: entry.kind,
        });
        restored_entries = restored_entries.saturating_add(1);
    }

    let journal_parent = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let journal_path = journal_parent.join(".expressways-restore-transaction.json");
    validate_runtime_restore_topology(&restore_plan, &backup_dir, &journal_path)?;
    if !options.dry_run {
        restore_runtime_backup_plan(&restore_plan, &journal_path)?;
    }

    Ok(RuntimeRestoreResult {
        manifest_path: manifest_path.display().to_string(),
        restored_entries,
        skipped_missing_optional_entries,
        dry_run: options.dry_run,
    })
}

fn resolve_runtime_backup_payload(
    backup_dir: &Path,
    backup_path: &str,
    entry_key: &str,
) -> anyhow::Result<PathBuf> {
    let relative = Path::new(backup_path);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
        || relative
            .components()
            .next()
            .and_then(|component| match component {
                std::path::Component::Normal(value) => value.to_str(),
                _ => None,
            })
            != Some("payload")
    {
        bail!(
            "backup entry `{entry_key}` has an unsafe backup_path `{backup_path}`; paths must stay under payload/"
        );
    }

    let payload_root = backup_dir.join("payload");
    let source_path = backup_dir.join(relative);
    let metadata = fs::symlink_metadata(&source_path).with_context(|| {
        format!(
            "backup payload missing for `{entry_key}`: {}",
            source_path.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        bail!(
            "backup payload for `{entry_key}` must not be a symlink: {}",
            source_path.display()
        );
    }
    let canonical_root = fs::canonicalize(&payload_root)
        .with_context(|| format!("failed to resolve {}", payload_root.display()))?;
    let canonical_source = fs::canonicalize(&source_path)
        .with_context(|| format!("failed to resolve {}", source_path.display()))?;
    if !canonical_source.starts_with(&canonical_root) {
        bail!(
            "backup payload for `{entry_key}` escapes the payload directory: {}",
            source_path.display()
        );
    }
    Ok(source_path)
}

fn verified_runtime_backup_entry_source(
    backup_dir: &Path,
    entry: &RuntimeBackupManifestEntry,
) -> anyhow::Result<PathBuf> {
    if !entry.exists {
        bail!("backup entry `{}` is not present", entry.key);
    }
    let backup_path = entry.backup_path.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "backup entry `{}` is marked as present but has no backup_path",
            entry.key
        )
    })?;
    let source_path = resolve_runtime_backup_payload(backup_dir, backup_path, &entry.key)?;
    let expected_size = entry.size_bytes.ok_or_else(|| {
        anyhow::anyhow!(
            "backup entry `{}` has no recorded payload size; refusing an unverifiable restore",
            entry.key
        )
    })?;
    let expected_sha256 = entry.sha256.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "backup entry `{}` has no SHA-256 digest; refusing an unverifiable restore",
            entry.key
        )
    })?;
    let (actual_size, actual_sha256) = runtime_backup_payload_digest(&source_path, entry.kind)?;
    if actual_size != expected_size || actual_sha256 != expected_sha256 {
        bail!(
            "backup payload integrity check failed for `{}`: expected {} bytes / {}, found {} bytes / {}",
            entry.key,
            expected_size,
            expected_sha256,
            actual_size,
            actual_sha256
        );
    }
    Ok(source_path)
}

fn load_runtime_backup_targets(
    config_path: &Path,
    config_audit_log: &Path,
    orchestrator_state: &Path,
    additional_state: &[String],
) -> anyhow::Result<Vec<RuntimeBackupTarget>> {
    load_runtime_backup_targets_from_config(
        config_path,
        config_path,
        config_audit_log,
        orchestrator_state,
        additional_state,
    )
}

fn load_runtime_backup_targets_from_config(
    config_source_path: &Path,
    config_destination_path: &Path,
    config_audit_log: &Path,
    orchestrator_state: &Path,
    additional_state: &[String],
) -> anyhow::Result<Vec<RuntimeBackupTarget>> {
    let raw = read_bounded_utf8_file(config_source_path, MAX_CLI_CONFIG_BYTES)
        .with_context(|| format!("failed to read config {}", config_source_path.display()))?;
    let config: RuntimeBackupConfigFile = toml::from_str(&raw)
        .context("failed to parse TOML config for runtime backup or restore")?;

    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let data_dir = resolve_runtime_backup_path(&cwd, config.server.data_dir);
    let registry_path = config
        .registry
        .path
        .map(|path| resolve_runtime_backup_path(&cwd, path))
        .unwrap_or_else(|| data_dir.join("registry").join("agents.json"));

    let mut targets = vec![
        RuntimeBackupTarget {
            key: "config_file".to_owned(),
            source_path: resolve_runtime_backup_path(&cwd, config_destination_path.to_path_buf()),
            required: true,
        },
        RuntimeBackupTarget {
            key: "server_data".to_owned(),
            source_path: data_dir.clone(),
            required: true,
        },
        RuntimeBackupTarget {
            key: "audit_log".to_owned(),
            source_path: resolve_runtime_backup_path(&cwd, config.audit.path),
            required: false,
        },
        RuntimeBackupTarget {
            key: "auth_revocations".to_owned(),
            source_path: resolve_runtime_backup_path(&cwd, config.auth.revocation_path),
            required: true,
        },
        RuntimeBackupTarget {
            key: "config_audit_log".to_owned(),
            source_path: resolve_runtime_backup_path(&cwd, config_audit_log.to_path_buf()),
            required: false,
        },
        RuntimeBackupTarget {
            key: "orchestrator_state".to_owned(),
            source_path: resolve_runtime_backup_path(&cwd, orchestrator_state.to_path_buf()),
            required: false,
        },
    ];
    if !registry_path.starts_with(&data_dir) {
        targets.push(RuntimeBackupTarget {
            key: "registry_state".to_owned(),
            source_path: registry_path,
            required: false,
        });
    }
    targets.extend(
        config
            .auth
            .issuers
            .into_iter()
            .map(|issuer| RuntimeBackupTarget {
                key: format!("issuer_public_key:{}", issuer.key_id),
                source_path: resolve_runtime_backup_path(&cwd, issuer.public_key_path),
                required: true,
            }),
    );
    targets.extend(parse_additional_runtime_targets(additional_state, &cwd)?);

    Ok(targets)
}

fn parse_additional_runtime_targets(
    values: &[String],
    cwd: &Path,
) -> anyhow::Result<Vec<RuntimeBackupTarget>> {
    let mut names = HashSet::new();
    let mut targets = Vec::with_capacity(values.len());
    for value in values {
        let (name, path) = value.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("additional runtime state `{value}` must use NAME=PATH syntax")
        })?;
        if name.is_empty()
            || name.len() > 64
            || !name.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            bail!(
                "additional runtime state name `{name}` must contain 1..=64 ASCII letters, digits, `-`, or `_`"
            );
        }
        if path.is_empty() {
            bail!("additional runtime state `{name}` has an empty path");
        }
        if !names.insert(name.to_owned()) {
            bail!("duplicate additional runtime state name `{name}`");
        }
        targets.push(RuntimeBackupTarget {
            key: format!("additional_state:{name}"),
            source_path: resolve_runtime_backup_path(cwd, PathBuf::from(path)),
            required: false,
        });
    }
    Ok(targets)
}

fn validate_runtime_backup_output_disjoint(
    output_dir: &Path,
    targets: &[RuntimeBackupTarget],
) -> anyhow::Result<()> {
    let canonical_output = fs::canonicalize(output_dir)
        .with_context(|| format!("failed to resolve backup output {}", output_dir.display()))?;
    for target in targets {
        let metadata = match fs::symlink_metadata(&target.source_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_dir() {
            continue;
        }
        let canonical_target = fs::canonicalize(&target.source_path).with_context(|| {
            format!(
                "failed to resolve backup target `{}` at {}",
                target.key,
                target.source_path.display()
            )
        })?;
        if canonical_output.starts_with(&canonical_target)
            || canonical_target.starts_with(&canonical_output)
        {
            bail!(
                "backup output {} overlaps directory target `{}` at {}; choose a disjoint output directory",
                output_dir.display(),
                target.key,
                target.source_path.display()
            );
        }
    }
    Ok(())
}

fn validate_runtime_restore_topology(
    plan: &[RuntimeRestorePlanEntry],
    backup_dir: &Path,
    journal_path: &Path,
) -> anyhow::Result<()> {
    let backup_dir = normalize_runtime_path(backup_dir)?;
    let journal_path = normalize_runtime_path(journal_path)?;
    let destinations = plan
        .iter()
        .map(|entry| {
            Ok((
                entry,
                normalize_runtime_path(&entry.destination_path)?,
                entry.kind == RuntimeBackupEntryKind::Directory,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    for (entry, destination, is_directory) in &destinations {
        if destination == &backup_dir
            || destination.starts_with(&backup_dir)
            || (*is_directory && backup_dir.starts_with(destination))
        {
            bail!(
                "restore destination `{}` at {} overlaps backup bundle {}; move the bundle to a disjoint location",
                entry.key,
                entry.destination_path.display(),
                backup_dir.display()
            );
        }
        if destination == &journal_path || (*is_directory && journal_path.starts_with(destination))
        {
            bail!(
                "restore destination `{}` at {} overlaps restore journal {}; choose a config path outside restored directories",
                entry.key,
                entry.destination_path.display(),
                journal_path.display()
            );
        }
    }

    for left_index in 0..destinations.len() {
        let (left_entry, left, left_is_directory) = &destinations[left_index];
        for (right_entry, right, right_is_directory) in &destinations[left_index + 1..] {
            if left == right
                || (*left_is_directory && right.starts_with(left))
                || (*right_is_directory && left.starts_with(right))
            {
                bail!(
                    "restore destinations `{}` ({}) and `{}` ({}) overlap; runtime state targets must be disjoint",
                    left_entry.key,
                    left_entry.destination_path.display(),
                    right_entry.key,
                    right_entry.destination_path.display()
                );
            }
        }
    }
    Ok(())
}

fn normalize_runtime_path(path: &Path) -> anyhow::Result<PathBuf> {
    use std::path::Component;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    bail!("path escapes its filesystem root: {}", path.display());
                }
            }
        }
    }
    Ok(normalized)
}

fn resolve_runtime_backup_path(cwd: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

fn default_runtime_backup_id() -> String {
    format!("expressways-backup-{}", Utc::now().format("%Y%m%dT%H%M%SZ"))
}

fn normalize_runtime_backup_id(value: &str) -> anyhow::Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("backup id must not be empty");
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        bail!(
            "backup id `{trimmed}` contains unsupported characters; use letters, digits, `-`, `_`, or `.`"
        );
    }
    Ok(trimmed.to_owned())
}

fn sanitize_runtime_backup_key(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "entry".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn copy_runtime_backup_path(
    source: &Path,
    destination: &Path,
    kind: RuntimeBackupEntryKind,
) -> anyhow::Result<()> {
    match kind {
        RuntimeBackupEntryKind::File => {
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            fs::copy(source, destination).with_context(|| {
                format!(
                    "failed to copy file {} -> {}",
                    source.display(),
                    destination.display()
                )
            })?;
            Ok(())
        }
        RuntimeBackupEntryKind::Directory => copy_runtime_backup_directory(source, destination),
        RuntimeBackupEntryKind::Unknown => {
            bail!("cannot copy runtime backup entry with unknown type")
        }
    }
}

fn write_private_synced_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))?;
    Ok(())
}

fn atomic_private_replace_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let file_name = path
        .file_name()
        .context("output path must contain a file name")?;
    let staged_path = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        Uuid::now_v7()
    ));
    let result = (|| -> anyhow::Result<()> {
        write_private_synced_file(&staged_path, bytes)?;
        fs::rename(&staged_path, path)
            .with_context(|| format!("failed to replace {}", path.display()))?;
        sync_restore_directory(parent)?;
        Ok(())
    })();
    if result.is_err() && path_exists_without_following(&staged_path).unwrap_or(false) {
        let _ = fs::remove_file(&staged_path);
    }
    result
}

fn copy_runtime_backup_directory(source: &Path, destination: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("failed to read directory {}", source.display()))?
    {
        let entry = entry?;
        let child_source = entry.path();
        let child_destination = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&child_source)?;
        if metadata.file_type().is_symlink() {
            bail!(
                "backup source directory contains a symlink, which cannot be captured safely: {}",
                child_source.display()
            );
        }
        if metadata.is_dir() {
            copy_runtime_backup_directory(&child_source, &child_destination)?;
        } else if metadata.is_file() {
            if let Some(parent) = child_destination.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&child_source, &child_destination).with_context(|| {
                format!(
                    "failed to copy file {} -> {}",
                    child_source.display(),
                    child_destination.display()
                )
            })?;
        } else {
            bail!(
                "backup source directory contains an unsupported entry type: {}",
                child_source.display()
            );
        }
    }
    Ok(())
}

fn runtime_backup_payload_digest(
    path: &Path,
    kind: RuntimeBackupEntryKind,
) -> anyhow::Result<(u64, String)> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect backup payload {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!(
            "backup payload must not contain symlinks: {}",
            path.display()
        );
    }

    let mut hasher = Sha256::new();
    let size = match kind {
        RuntimeBackupEntryKind::File if metadata.is_file() => {
            hash_runtime_backup_file(path, "", &mut hasher)?
        }
        RuntimeBackupEntryKind::Directory if metadata.is_dir() => {
            hash_runtime_backup_directory(path, path, &mut hasher)?
        }
        RuntimeBackupEntryKind::File => {
            bail!(
                "backup payload is not the recorded file type: {}",
                path.display()
            )
        }
        RuntimeBackupEntryKind::Directory => {
            bail!(
                "backup payload is not the recorded directory type: {}",
                path.display()
            )
        }
        RuntimeBackupEntryKind::Unknown => {
            bail!("cannot verify runtime backup entry with unknown type")
        }
    };
    Ok((size, hex::encode(hasher.finalize())))
}

fn hash_runtime_backup_directory(
    root: &Path,
    directory: &Path,
    hasher: &mut Sha256,
) -> anyhow::Result<u64> {
    let mut entries = fs::read_dir(directory)
        .with_context(|| format!("failed to read directory {}", directory.display()))?
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    let mut total = 0u64;
    for entry in entries {
        let path = entry.path();
        let relative = path.strip_prefix(root).with_context(|| {
            format!(
                "failed to derive backup-relative path for {}",
                path.display()
            )
        })?;
        let relative = relative.to_str().ok_or_else(|| {
            anyhow::anyhow!(
                "backup payload contains a non-UTF-8 path that cannot be verified: {}",
                path.display()
            )
        })?;
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            bail!(
                "backup payload must not contain symlinks: {}",
                path.display()
            );
        }
        if metadata.is_dir() {
            hasher.update(b"directory\0");
            hash_runtime_backup_path(relative, hasher);
            total = total.saturating_add(hash_runtime_backup_directory(root, &path, hasher)?);
        } else if metadata.is_file() {
            total = total.saturating_add(hash_runtime_backup_file(&path, relative, hasher)?);
        } else {
            bail!(
                "backup payload has unsupported entry type: {}",
                path.display()
            );
        }
    }
    Ok(total)
}

fn hash_runtime_backup_file(
    path: &Path,
    relative: &str,
    hasher: &mut Sha256,
) -> anyhow::Result<u64> {
    hasher.update(b"file\0");
    hash_runtime_backup_path(relative, hasher);
    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to open backup payload {}", path.display()))?;
    let size = file.metadata()?.len();
    hasher.update(size.to_le_bytes());
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(size)
}

fn hash_runtime_backup_path(relative: &str, hasher: &mut Sha256) {
    hasher.update((relative.len() as u64).to_le_bytes());
    hasher.update(relative.as_bytes());
}

fn remove_path_for_restore(path: &Path) -> anyhow::Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if metadata.file_type().is_dir() {
        fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove directory {}", path.display()))?;
    } else {
        fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

fn runtime_backup_publish_journal_path(output_dir: &Path, backup_id: &str) -> PathBuf {
    output_dir.join(format!(".{backup_id}.publish.json"))
}

fn runtime_backup_publish_marker_path(journal_path: &Path) -> PathBuf {
    journal_path.with_extension("committed")
}

fn runtime_backup_staged_path(output_dir: &Path, backup_id: &str, operation_id: Uuid) -> PathBuf {
    output_dir.join(format!(".{backup_id}.staged-{operation_id}"))
}

fn runtime_backup_previous_path(output_dir: &Path, backup_id: &str, operation_id: Uuid) -> PathBuf {
    output_dir.join(format!(".{backup_id}.previous-{operation_id}"))
}

fn validate_runtime_backup_publish_journal(
    journal: &RuntimeBackupPublishJournal,
    output_dir: &Path,
    backup_id: &str,
) -> anyhow::Result<()> {
    if journal.schema_version != 1 || journal.backup_id != backup_id {
        bail!("runtime backup publish journal has an unsupported identity or schema");
    }
    let expected_final = output_dir.join(backup_id);
    let expected_staged = runtime_backup_staged_path(output_dir, backup_id, journal.operation_id);
    let expected_previous =
        runtime_backup_previous_path(output_dir, backup_id, journal.operation_id);
    if journal.final_path != expected_final
        || journal.staged_path != expected_staged
        || journal.previous_path != expected_previous
    {
        bail!("runtime backup publish journal contains unsafe transaction paths");
    }
    Ok(())
}

fn write_runtime_backup_publish_journal(
    journal_path: &Path,
    journal: &RuntimeBackupPublishJournal,
) -> anyhow::Result<()> {
    if path_exists_without_following(journal_path)? {
        bail!(
            "runtime backup publish transaction already exists at {}",
            journal_path.display()
        );
    }
    write_private_synced_file(journal_path, &serde_json::to_vec_pretty(journal)?)?;
    sync_restore_directory(journal_path.parent().unwrap_or_else(|| Path::new(".")))
}

fn write_runtime_backup_publish_marker(journal_path: &Path) -> anyhow::Result<()> {
    let marker_path = runtime_backup_publish_marker_path(journal_path);
    write_private_synced_file(&marker_path, b"committed\n")?;
    sync_restore_directory(marker_path.parent().unwrap_or_else(|| Path::new(".")))
}

fn remove_runtime_backup_publish_metadata(journal_path: &Path) -> anyhow::Result<()> {
    let marker_path = runtime_backup_publish_marker_path(journal_path);
    if path_exists_without_following(journal_path)? {
        fs::remove_file(journal_path)
            .with_context(|| format!("failed to remove {}", journal_path.display()))?;
        sync_restore_directory(journal_path.parent().unwrap_or_else(|| Path::new(".")))?;
    }
    if path_exists_without_following(&marker_path)? {
        fs::remove_file(&marker_path)
            .with_context(|| format!("failed to remove {}", marker_path.display()))?;
        sync_restore_directory(marker_path.parent().unwrap_or_else(|| Path::new(".")))?;
    }
    Ok(())
}

fn recover_runtime_backup_publish(
    journal_path: &Path,
    output_dir: &Path,
    backup_id: &str,
) -> anyhow::Result<()> {
    let marker_path = runtime_backup_publish_marker_path(journal_path);
    if !path_exists_without_following(journal_path)? {
        if path_exists_without_following(&marker_path)? {
            fs::remove_file(&marker_path)
                .with_context(|| format!("failed to remove {}", marker_path.display()))?;
            sync_restore_directory(output_dir)?;
        }
        return Ok(());
    }

    let journal: RuntimeBackupPublishJournal = serde_json::from_slice(
        &read_bounded_regular_file(journal_path, MAX_RUNTIME_BACKUP_PUBLISH_JOURNAL_BYTES)
            .with_context(|| {
                format!(
                    "failed to read runtime backup publish journal {}",
                    journal_path.display()
                )
            })?,
    )
    .with_context(|| {
        format!(
            "failed to parse runtime backup publish journal {}",
            journal_path.display()
        )
    })?;
    validate_runtime_backup_publish_journal(&journal, output_dir, backup_id)?;

    if path_exists_without_following(&marker_path)? {
        if !path_exists_without_following(&journal.final_path)? {
            if path_exists_without_following(&journal.staged_path)? {
                fs::rename(&journal.staged_path, &journal.final_path).with_context(|| {
                    format!(
                        "failed to finish committed backup publication {}",
                        journal.final_path.display()
                    )
                })?;
            } else {
                bail!(
                    "committed backup publication is missing both final and staged bundle for {}",
                    journal.backup_id
                );
            }
        }
        if path_exists_without_following(&journal.previous_path)? {
            remove_path_for_restore(&journal.previous_path)?;
        }
        if path_exists_without_following(&journal.staged_path)? {
            remove_path_for_restore(&journal.staged_path)?;
        }
    } else {
        if journal.had_previous && path_exists_without_following(&journal.previous_path)? {
            if path_exists_without_following(&journal.final_path)? {
                remove_path_for_restore(&journal.final_path)?;
            }
            fs::rename(&journal.previous_path, &journal.final_path).with_context(|| {
                format!(
                    "failed to roll back backup publication {}",
                    journal.final_path.display()
                )
            })?;
        } else if !journal.had_previous && path_exists_without_following(&journal.final_path)? {
            remove_path_for_restore(&journal.final_path)?;
        } else if journal.had_previous && !path_exists_without_following(&journal.final_path)? {
            bail!(
                "interrupted backup publication lost the previous bundle for {}",
                journal.backup_id
            );
        }
        if path_exists_without_following(&journal.staged_path)? {
            remove_path_for_restore(&journal.staged_path)?;
        }
        if path_exists_without_following(&journal.previous_path)? {
            remove_path_for_restore(&journal.previous_path)?;
        }
    }

    sync_restore_directory(output_dir)?;
    remove_runtime_backup_publish_metadata(journal_path)
}

fn commit_runtime_backup_publish(
    journal_path: &Path,
    output_dir: &Path,
    journal: &RuntimeBackupPublishJournal,
) -> anyhow::Result<()> {
    validate_runtime_backup_publish_journal(journal, output_dir, &journal.backup_id)?;
    if journal.had_previous {
        fs::rename(&journal.final_path, &journal.previous_path).with_context(|| {
            format!(
                "failed to preserve previous backup {}",
                journal.final_path.display()
            )
        })?;
        sync_restore_directory(output_dir)?;
    }

    if let Err(error) = fs::rename(&journal.staged_path, &journal.final_path) {
        if journal.had_previous && path_exists_without_following(&journal.previous_path)? {
            fs::rename(&journal.previous_path, &journal.final_path).with_context(|| {
                format!("failed to roll back previous backup after publish error: {error}")
            })?;
            sync_restore_directory(output_dir)?;
        }
        return Err(error).with_context(|| {
            format!(
                "failed to publish runtime backup {}",
                journal.final_path.display()
            )
        });
    }
    sync_restore_directory(output_dir)?;
    write_runtime_backup_publish_marker(journal_path)?;

    if path_exists_without_following(&journal.previous_path)? {
        remove_path_for_restore(&journal.previous_path)?;
        sync_restore_directory(output_dir)?;
    }
    remove_runtime_backup_publish_metadata(journal_path)
}

fn restore_runtime_backup_plan(
    plan: &[RuntimeRestorePlanEntry],
    journal_path: &Path,
) -> anyhow::Result<()> {
    let _transaction_lock = acquire_runtime_transaction_lock(&journal_path.with_extension("lock"))?;
    recover_runtime_restore_transaction(plan, journal_path)?;
    let operation_id = Uuid::now_v7();
    let mut prepared = Vec::with_capacity(plan.len());

    for (index, entry) in plan.iter().enumerate() {
        match prepare_runtime_restore_entry(entry, operation_id, index) {
            Ok(prepared_entry) => prepared.push(prepared_entry),
            Err(error) => {
                cleanup_prepared_restore_paths(&prepared);
                return Err(error);
            }
        }
    }

    for entry in &mut prepared {
        entry.had_previous = path_exists_without_following(&entry.destination_path)?;
    }
    let journal = runtime_restore_journal(operation_id, &prepared);
    write_runtime_restore_journal(journal_path, &journal)?;

    for index in 0..prepared.len() {
        if prepared[index].had_previous
            && let Err(error) = fs::rename(
                &prepared[index].destination_path,
                &prepared[index].previous_path,
            )
        {
            let rollback = rollback_runtime_restore(&mut prepared[..index]);
            cleanup_staged_restore_paths(&prepared);
            if rollback.is_ok() {
                remove_runtime_restore_journal(journal_path)?;
            }
            return Err(restore_commit_error(
                &prepared[index].key,
                error.into(),
                rollback,
            ));
        }

        if let Err(error) = fs::rename(
            &prepared[index].staged_path,
            &prepared[index].destination_path,
        ) {
            let current_rollback = if prepared[index].had_previous {
                fs::rename(
                    &prepared[index].previous_path,
                    &prepared[index].destination_path,
                )
                .map_err(anyhow::Error::from)
            } else {
                Ok(())
            };
            let prior_rollback = rollback_runtime_restore(&mut prepared[..index]);
            cleanup_staged_restore_paths(&prepared);
            let rollback = current_rollback.and(prior_rollback);
            if rollback.is_ok() {
                remove_runtime_restore_journal(journal_path)?;
            }
            return Err(restore_commit_error(
                &prepared[index].key,
                error.into(),
                rollback,
            ));
        }
        prepared[index].committed = true;
    }

    sync_restore_parents(&prepared)?;
    write_runtime_restore_commit_marker(journal_path)?;
    for entry in &prepared {
        if entry.had_previous {
            remove_path_for_restore(&entry.previous_path).with_context(|| {
                format!(
                    "restore committed, but failed to remove previous target {}",
                    entry.previous_path.display()
                )
            })?;
        }
    }
    remove_runtime_restore_journal(journal_path)?;
    Ok(())
}

fn runtime_restore_journal(
    operation_id: Uuid,
    entries: &[PreparedRuntimeRestoreEntry],
) -> RuntimeRestoreJournal {
    RuntimeRestoreJournal {
        schema_version: 1,
        operation_id,
        phase: RuntimeRestoreJournalPhase::Prepared,
        entries: entries
            .iter()
            .map(|entry| RuntimeRestoreJournalEntry {
                key: entry.key.clone(),
                destination_path: entry.destination_path.clone(),
                staged_path: entry.staged_path.clone(),
                previous_path: entry.previous_path.clone(),
                had_previous: entry.had_previous,
            })
            .collect(),
    }
}

fn write_runtime_restore_journal(
    journal_path: &Path,
    journal: &RuntimeRestoreJournal,
) -> anyhow::Result<()> {
    if path_exists_without_following(journal_path)? {
        bail!(
            "runtime restore transaction already exists at {}; rerun restore to recover it",
            journal_path.display()
        );
    }
    let parent = journal_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create restore journal parent {}",
            parent.display()
        )
    })?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(journal_path).with_context(|| {
        format!(
            "failed to create restore journal {}",
            journal_path.display()
        )
    })?;
    serde_json::to_writer_pretty(&mut file, journal)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    sync_restore_directory(parent)?;
    Ok(())
}

fn restore_commit_marker_path(journal_path: &Path) -> PathBuf {
    journal_path.with_extension("committed")
}

fn write_runtime_restore_commit_marker(journal_path: &Path) -> anyhow::Result<()> {
    let marker_path = restore_commit_marker_path(journal_path);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&marker_path).with_context(|| {
        format!(
            "failed to create restore commit marker {}",
            marker_path.display()
        )
    })?;
    file.write_all(b"committed\n")?;
    file.sync_all()?;
    sync_restore_directory(marker_path.parent().unwrap_or_else(|| Path::new(".")))?;
    Ok(())
}

fn remove_runtime_restore_journal(journal_path: &Path) -> anyhow::Result<()> {
    let marker_path = restore_commit_marker_path(journal_path);
    if path_exists_without_following(journal_path)? {
        fs::remove_file(journal_path)
            .with_context(|| format!("failed to remove {}", journal_path.display()))?;
        sync_restore_directory(journal_path.parent().unwrap_or_else(|| Path::new(".")))?;
    }
    if path_exists_without_following(&marker_path)? {
        fs::remove_file(&marker_path)
            .with_context(|| format!("failed to remove {}", marker_path.display()))?;
    }
    sync_restore_directory(journal_path.parent().unwrap_or_else(|| Path::new(".")))?;
    Ok(())
}

fn recover_runtime_restore_transaction(
    plan: &[RuntimeRestorePlanEntry],
    journal_path: &Path,
) -> anyhow::Result<()> {
    if !path_exists_without_following(journal_path)? {
        let marker_path = restore_commit_marker_path(journal_path);
        if path_exists_without_following(&marker_path)? {
            fs::remove_file(&marker_path).with_context(|| {
                format!(
                    "failed to finish committed restore cleanup at {}",
                    marker_path.display()
                )
            })?;
            sync_restore_directory(marker_path.parent().unwrap_or_else(|| Path::new(".")))?;
        }
        return Ok(());
    }
    let raw = read_bounded_regular_file(journal_path, MAX_RUNTIME_RESTORE_JOURNAL_BYTES)
        .with_context(|| format!("failed to read restore journal {}", journal_path.display()))?;
    let journal: RuntimeRestoreJournal = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse restore journal {}", journal_path.display()))?;
    validate_runtime_restore_journal(plan, &journal)?;
    let committed = path_exists_without_following(&restore_commit_marker_path(journal_path))?;

    if committed {
        for entry in &journal.entries {
            if !path_exists_without_following(&entry.destination_path)? {
                bail!(
                    "committed restore target is missing during recovery: {}",
                    entry.destination_path.display()
                );
            }
            for path in [&entry.staged_path, &entry.previous_path] {
                if path_exists_without_following(path)? {
                    remove_path_for_restore(path)?;
                }
            }
        }
    } else {
        for entry in journal.entries.iter().rev() {
            let staged_exists = path_exists_without_following(&entry.staged_path)?;
            let previous_exists = path_exists_without_following(&entry.previous_path)?;
            if entry.had_previous && previous_exists {
                if path_exists_without_following(&entry.destination_path)? {
                    remove_path_for_restore(&entry.destination_path)?;
                }
                fs::rename(&entry.previous_path, &entry.destination_path).with_context(|| {
                    format!(
                        "failed to roll back interrupted restore target {}",
                        entry.destination_path.display()
                    )
                })?;
            } else if !entry.had_previous
                && !staged_exists
                && path_exists_without_following(&entry.destination_path)?
            {
                remove_path_for_restore(&entry.destination_path)?;
            }
            if staged_exists {
                remove_path_for_restore(&entry.staged_path)?;
            }
        }
    }
    remove_runtime_restore_journal(journal_path)
}

fn validate_runtime_restore_journal(
    plan: &[RuntimeRestorePlanEntry],
    journal: &RuntimeRestoreJournal,
) -> anyhow::Result<()> {
    if journal.schema_version != 1 || journal.phase != RuntimeRestoreJournalPhase::Prepared {
        bail!("unsupported runtime restore journal schema or phase");
    }
    if journal.entries.len() != plan.len() {
        bail!("runtime restore journal does not match the current restore plan");
    }
    for (index, (journal_entry, plan_entry)) in journal.entries.iter().zip(plan.iter()).enumerate()
    {
        if journal_entry.key != plan_entry.key
            || journal_entry.destination_path != plan_entry.destination_path
        {
            bail!("runtime restore journal target does not match the current restore plan");
        }
        let parent = plan_entry.destination_path.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "restore destination has no parent directory: {}",
                plan_entry.destination_path.display()
            )
        })?;
        let leaf = plan_entry
            .destination_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("target");
        let expected_staged =
            parent.join(format!(".{leaf}.restore-{}-{index}", journal.operation_id));
        let expected_previous =
            parent.join(format!(".{leaf}.previous-{}-{index}", journal.operation_id));
        if journal_entry.staged_path != expected_staged
            || journal_entry.previous_path != expected_previous
        {
            bail!("runtime restore journal contains unsafe transaction paths");
        }
    }
    Ok(())
}

fn sync_restore_parents(entries: &[PreparedRuntimeRestoreEntry]) -> anyhow::Result<()> {
    let mut parents = HashSet::new();
    for entry in entries {
        if let Some(parent) = entry.destination_path.parent() {
            parents.insert(parent.to_path_buf());
        }
    }
    for parent in parents {
        sync_restore_directory(&parent)?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_restore_directory(path: &Path) -> anyhow::Result<()> {
    File::open(path)
        .with_context(|| format!("failed to open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to sync directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_restore_directory(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

fn prepare_runtime_restore_entry(
    entry: &RuntimeRestorePlanEntry,
    operation_id: Uuid,
    index: usize,
) -> anyhow::Result<PreparedRuntimeRestoreEntry> {
    let parent = entry.destination_path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "restore destination has no parent directory: {}",
            entry.destination_path.display()
        )
    })?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create restore parent {}", parent.display()))?;
    let leaf = entry
        .destination_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("target");
    let staged_path = parent.join(format!(".{leaf}.restore-{operation_id}-{index}"));
    let previous_path = parent.join(format!(".{leaf}.previous-{operation_id}-{index}"));

    if let Err(error) = copy_runtime_backup_path(&entry.source_path, &staged_path, entry.kind) {
        if path_exists_without_following(&staged_path).unwrap_or(false) {
            let _ = remove_path_for_restore(&staged_path);
        }
        return Err(error)
            .with_context(|| format!("failed to stage runtime target `{}`", entry.key));
    }
    if let Err(error) = sync_runtime_restore_path(&staged_path, entry.kind) {
        let _ = remove_path_for_restore(&staged_path);
        return Err(error)
            .with_context(|| format!("failed to sync staged runtime target `{}`", entry.key));
    }

    Ok(PreparedRuntimeRestoreEntry {
        key: entry.key.clone(),
        destination_path: entry.destination_path.clone(),
        staged_path,
        previous_path,
        had_previous: false,
        committed: false,
    })
}

fn sync_runtime_restore_path(path: &Path, kind: RuntimeBackupEntryKind) -> anyhow::Result<()> {
    match kind {
        RuntimeBackupEntryKind::File => File::open(path)
            .with_context(|| format!("failed to open staged file {}", path.display()))?
            .sync_all()
            .with_context(|| format!("failed to sync staged file {}", path.display())),
        RuntimeBackupEntryKind::Directory => sync_runtime_restore_directory_tree(path),
        RuntimeBackupEntryKind::Unknown => {
            bail!("cannot sync runtime backup entry with unknown type")
        }
    }
}

fn sync_runtime_restore_directory_tree(path: &Path) -> anyhow::Result<()> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read staged directory {}", path.display()))?
    {
        let entry = entry?;
        let child = entry.path();
        let metadata = fs::symlink_metadata(&child)?;
        if metadata.is_dir() {
            sync_runtime_restore_directory_tree(&child)?;
        } else if metadata.is_file() {
            File::open(&child)?.sync_all()?;
        } else {
            bail!(
                "staged restore contains unsupported entry: {}",
                child.display()
            );
        }
    }
    sync_restore_directory(path)
}

fn path_exists_without_following(path: &Path) -> anyhow::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("failed to inspect restore target {}", path.display())),
    }
}

fn rollback_runtime_restore(entries: &mut [PreparedRuntimeRestoreEntry]) -> anyhow::Result<()> {
    let mut failures = Vec::new();
    for entry in entries.iter_mut().rev().filter(|entry| entry.committed) {
        if path_exists_without_following(&entry.destination_path).unwrap_or(false)
            && let Err(error) = remove_path_for_restore(&entry.destination_path)
        {
            failures.push(format!(
                "failed to remove restored target {}: {error}",
                entry.destination_path.display()
            ));
            continue;
        }
        if entry.had_previous
            && let Err(error) = fs::rename(&entry.previous_path, &entry.destination_path)
        {
            failures.push(format!(
                "failed to restore previous target {}: {error}",
                entry.destination_path.display()
            ));
            continue;
        }
        entry.committed = false;
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn cleanup_prepared_restore_paths(entries: &[PreparedRuntimeRestoreEntry]) {
    for entry in entries {
        for path in [&entry.staged_path, &entry.previous_path] {
            if path_exists_without_following(path).unwrap_or(false) {
                let _ = remove_path_for_restore(path);
            }
        }
    }
}

fn cleanup_staged_restore_paths(entries: &[PreparedRuntimeRestoreEntry]) {
    for entry in entries {
        if path_exists_without_following(&entry.staged_path).unwrap_or(false) {
            let _ = remove_path_for_restore(&entry.staged_path);
        }
    }
}

fn restore_commit_error(
    key: &str,
    commit_error: anyhow::Error,
    rollback: anyhow::Result<()>,
) -> anyhow::Error {
    match rollback {
        Ok(()) => anyhow::anyhow!(
            "failed to commit runtime target `{key}`; prior targets were rolled back: {commit_error}"
        ),
        Err(rollback_error) => anyhow::anyhow!(
            "failed to commit runtime target `{key}` ({commit_error}); rollback also failed: {rollback_error}"
        ),
    }
}

async fn collect_support_bundle_broker_snapshot(
    endpoint: Endpoint,
    capability_token: String,
) -> (Option<SupportBundleBrokerSnapshot>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut health = None;
    let mut metrics = None;
    let mut auth = None;

    let mut client = match Client::connect(endpoint).await {
        Ok(client) => client,
        Err(error) => {
            warnings.push(format!(
                "broker snapshot omitted: failed to connect ({error})"
            ));
            return (None, warnings);
        }
    };

    match client
        .send(ControlRequest {
            capability_token: capability_token.clone(),
            command: ControlCommand::Health,
        })
        .await
    {
        Ok(ControlResponse::Health { node_name, status }) => {
            health = Some(SupportBundleBrokerHealth { node_name, status });
        }
        Ok(ControlResponse::Error { code, message }) => {
            warnings.push(format!("health command denied: {code}: {message}"));
        }
        Ok(other) => {
            warnings.push(format!(
                "health command returned unexpected response: {other:?}"
            ));
        }
        Err(error) => warnings.push(format!("health command failed: {error}")),
    }

    match client
        .send(ControlRequest {
            capability_token: capability_token.clone(),
            command: ControlCommand::GetMetrics,
        })
        .await
    {
        Ok(ControlResponse::Metrics { metrics: view }) => {
            metrics = Some(view);
        }
        Ok(ControlResponse::Error { code, message }) => {
            warnings.push(format!("metrics command denied: {code}: {message}"));
        }
        Ok(other) => {
            warnings.push(format!(
                "metrics command returned unexpected response: {other:?}"
            ));
        }
        Err(error) => warnings.push(format!("metrics command failed: {error}")),
    }

    match client
        .send(ControlRequest {
            capability_token,
            command: ControlCommand::GetAuthState,
        })
        .await
    {
        Ok(ControlResponse::AuthState { state }) => {
            auth = Some(state);
        }
        Ok(ControlResponse::Error { code, message }) => {
            warnings.push(format!("auth-state command denied: {code}: {message}"));
        }
        Ok(other) => {
            warnings.push(format!(
                "auth-state command returned unexpected response: {other:?}"
            ));
        }
        Err(error) => warnings.push(format!("auth-state command failed: {error}")),
    }

    if health.is_none() && metrics.is_none() && auth.is_none() {
        (None, warnings)
    } else {
        (
            Some(SupportBundleBrokerSnapshot {
                health,
                metrics,
                auth,
            }),
            warnings,
        )
    }
}

fn resolve_optional_token(args: TokenArgs) -> anyhow::Result<Option<String>> {
    if args.token.is_none() && args.token_file.is_none() {
        return Ok(None);
    }
    Ok(Some(resolve_token(args)?))
}

fn resolve_token(args: TokenArgs) -> anyhow::Result<String> {
    if let Some(token) = args.token {
        return expressways_client::normalize_capability_token(&token);
    }

    if let Some(path) = args.token_file {
        return expressways_client::read_capability_token_file(&path);
    }

    bail!("a capability token is required via --token or --token-file")
}

fn parse_scope(value: &str) -> Result<CapabilityScope, String> {
    let (resource, actions_raw) = value
        .rsplit_once(':')
        .ok_or_else(|| "scope must look like resource:action[,action]".to_owned())?;
    let actions = actions_raw
        .split(',')
        .map(|item| item.parse::<Action>())
        .collect::<Result<Vec<_>, _>>()?;

    if actions.is_empty() {
        return Err("scope must contain at least one action".to_owned());
    }

    Ok(CapabilityScope {
        resource: resource.to_owned(),
        actions,
    })
}

fn parse_schema(value: &str) -> Result<AgentSchemaRef, String> {
    let (name, version) = value
        .split_once(':')
        .ok_or_else(|| "schema must look like name:version".to_owned())?;
    if name.trim().is_empty() || version.trim().is_empty() {
        return Err("schema name and version must both be present".to_owned());
    }

    Ok(AgentSchemaRef {
        name: name.trim().to_owned(),
        version: version.trim().to_owned(),
    })
}

fn build_submit_task_payload(
    payload_json: Option<String>,
    payload_text: Option<String>,
    payload_file: Option<PathBuf>,
    payload_base64: Option<String>,
    payload_inline: bool,
    payload_content_type: Option<String>,
    payload_sha256: Option<String>,
) -> anyhow::Result<TaskPayload> {
    validate_submit_task_payload_selection(
        payload_json.is_some(),
        payload_text.is_some(),
        payload_file.is_some(),
        payload_base64.is_some(),
        payload_inline,
        payload_sha256.is_some(),
    )?;

    if let Some(payload_json) = payload_json {
        return Ok(TaskPayload::json(
            serde_json::from_str(&payload_json)
                .context("failed to parse --payload-json as JSON")?,
        ));
    }
    if let Some(payload_text) = payload_text {
        return Ok(TaskPayload::text(
            payload_text,
            payload_content_type.unwrap_or_else(|| "text/plain; charset=utf-8".to_owned()),
        ));
    }
    if let Some(payload_base64) = payload_base64 {
        return Ok(TaskPayload::bytes_base64(
            payload_base64,
            payload_content_type.unwrap_or_else(|| "application/octet-stream".to_owned()),
            None,
        ));
    }
    if let Some(payload_file) = payload_file {
        let metadata = fs::symlink_metadata(&payload_file).with_context(|| {
            format!("failed to inspect payload file {}", payload_file.display())
        })?;
        if !metadata.file_type().is_file() {
            bail!(
                "payload path {} is not a regular file",
                payload_file.display()
            );
        }

        if payload_inline {
            let bytes = read_bounded_regular_file(&payload_file, MAX_CLI_ATTACHMENT_BYTES)
                .with_context(|| {
                    format!("failed to read payload file {}", payload_file.display())
                })?;
            return Ok(TaskPayload::bytes(
                &bytes,
                payload_content_type.unwrap_or_else(|| "application/octet-stream".to_owned()),
            ));
        }

        return Ok(TaskPayload::file_ref(
            payload_file.display().to_string(),
            payload_content_type,
            Some(metadata.len()),
            payload_sha256,
        ));
    }

    Ok(TaskPayload::default())
}

#[allow(clippy::too_many_arguments)] // CLI options map directly to mutually exclusive payload flags.
async fn build_submit_task_payload_with_client(
    client: &mut Client,
    capability_token: &str,
    payload_json: Option<String>,
    payload_text: Option<String>,
    payload_file: Option<PathBuf>,
    payload_base64: Option<String>,
    payload_inline: bool,
    payload_content_type: Option<String>,
    payload_sha256: Option<String>,
    classification: Option<Classification>,
) -> anyhow::Result<TaskPayload> {
    validate_submit_task_payload_selection(
        payload_json.is_some(),
        payload_text.is_some(),
        payload_file.is_some(),
        payload_base64.is_some(),
        payload_inline,
        payload_sha256.is_some(),
    )?;

    if let Some(payload_file) = payload_file {
        if payload_inline {
            return build_submit_task_payload(
                payload_json,
                payload_text,
                Some(payload_file),
                payload_base64,
                true,
                payload_content_type,
                payload_sha256,
            );
        }

        let (command, attachment) = build_put_artifact_request(
            None,
            Some(payload_file),
            None,
            None,
            payload_content_type,
            payload_sha256,
            classification,
            RetentionClass::Operational,
        )?;
        let (response, returned_attachment) = client
            .send_with_attachment(
                ControlRequest {
                    capability_token: capability_token.to_owned(),
                    command,
                },
                Some(attachment),
            )
            .await?;
        if returned_attachment.is_some() {
            bail!("broker returned unexpected binary attachment while uploading task artifact");
        }
        match response {
            ControlResponse::ArtifactStored { artifact } => Ok(TaskPayload::artifact_ref(
                artifact.artifact_id,
                Some(artifact.content_type),
                Some(artifact.byte_length),
                Some(artifact.sha256),
                None,
            )),
            ControlResponse::Error { code, message } => {
                bail!("broker rejected artifact upload: {code}: {message}")
            }
            other => bail!("unexpected response while uploading task artifact: {other:?}"),
        }
    } else {
        build_submit_task_payload(
            payload_json,
            payload_text,
            None,
            payload_base64,
            payload_inline,
            payload_content_type,
            payload_sha256,
        )
    }
}

fn validate_submit_task_payload_selection(
    payload_json: bool,
    payload_text: bool,
    payload_file: bool,
    payload_base64: bool,
    payload_inline: bool,
    payload_sha256: bool,
) -> anyhow::Result<()> {
    let supplied_sources = [payload_json, payload_text, payload_file, payload_base64]
        .into_iter()
        .filter(|present| *present)
        .count();

    if supplied_sources > 1 {
        bail!(
            "submit-task accepts only one payload source: choose one of --payload-json, --payload-text, --payload-file, or --payload-base64"
        );
    }
    if payload_inline && !payload_file {
        bail!("--payload-inline can only be used together with --payload-file");
    }
    if payload_sha256 && !payload_file {
        bail!("--payload-sha256 can only be used together with --payload-file");
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)] // CLI options map directly to artifact request fields.
fn build_put_artifact_request(
    artifact_id: Option<String>,
    file: Option<PathBuf>,
    text: Option<String>,
    base64: Option<String>,
    content_type: Option<String>,
    sha256: Option<String>,
    classification: Option<Classification>,
    retention_class: RetentionClass,
) -> anyhow::Result<(ControlCommand, Vec<u8>)> {
    let supplied_sources = [file.is_some(), text.is_some(), base64.is_some()]
        .into_iter()
        .filter(|present| *present)
        .count();
    if supplied_sources != 1 {
        bail!("put-artifact accepts exactly one source: choose one of --file, --text, or --base64");
    }
    if sha256.is_some() && file.is_none() && base64.is_none() {
        bail!("--sha256 can only be used together with --file or --base64");
    }

    let (bytes, content_type): (Vec<u8>, String) = if let Some(path) = file {
        let bytes = read_bounded_regular_file(&path, MAX_CLI_ATTACHMENT_BYTES)
            .with_context(|| format!("failed to read payload file {}", path.display()))?;
        (
            bytes,
            content_type.unwrap_or_else(|| "application/octet-stream".to_owned()),
        )
    } else if let Some(text) = text {
        (
            text.into_bytes(),
            content_type.unwrap_or_else(|| "text/plain; charset=utf-8".to_owned()),
        )
    } else if let Some(data_base64) = base64 {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data_base64)
            .context("failed to decode --base64 artifact payload")?;
        (
            bytes,
            content_type.unwrap_or_else(|| "application/octet-stream".to_owned()),
        )
    } else {
        unreachable!("validated exactly one artifact source");
    };
    if bytes.len() as u64 > MAX_CLI_ATTACHMENT_BYTES {
        bail!(
            "artifact payload is {} bytes; maximum is {MAX_CLI_ATTACHMENT_BYTES}",
            bytes.len()
        );
    }

    Ok((
        ControlCommand::PutArtifact {
            artifact_id,
            content_type,
            byte_length: bytes.len() as u64,
            sha256,
            classification,
            retention_class: Some(retention_class),
        },
        bytes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_cli_reader_rejects_oversized_and_symlinked_files() {
        let path = std::env::temp_dir().join(format!("expressways-cli-{}.bin", Uuid::now_v7()));
        File::create(&path)
            .expect("create input")
            .set_len(5)
            .expect("size input");
        assert!(read_bounded_regular_file(&path, 4).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let link = std::env::temp_dir().join(format!("expressways-cli-{}.bin", Uuid::now_v7()));
            symlink(&path, &link).expect("create input symlink");
            assert!(read_bounded_regular_file(&link, 16).is_err());
            fs::remove_file(link).expect("remove input symlink");
        }
        fs::remove_file(path).expect("remove input");
    }

    #[test]
    fn artifact_file_is_rejected_before_an_oversized_allocation() {
        let path = std::env::temp_dir().join(format!("expressways-cli-{}.bin", Uuid::now_v7()));
        File::create(&path)
            .expect("create artifact")
            .set_len(MAX_CLI_ATTACHMENT_BYTES + 1)
            .expect("size artifact");

        let result = build_put_artifact_request(
            None,
            Some(path.clone()),
            None,
            None,
            None,
            None,
            None,
            RetentionClass::Operational,
        );

        assert!(result.is_err());
        fs::remove_file(path).expect("remove artifact");
    }

    #[test]
    fn support_bundle_log_reader_rejects_oversized_lines() {
        let path = std::env::temp_dir().join(format!("expressways-cli-{}.log", Uuid::now_v7()));
        File::create(&path)
            .expect("create log")
            .set_len(MAX_SUPPORT_BUNDLE_LOG_LINE_BYTES + 2)
            .expect("size log");
        let mut redacted = 0;

        let result = read_head_and_tail_lines(
            &path,
            5,
            5,
            &SupportBundleRedactionSettings {
                enabled: false,
                profile: SupportBundleRedactionProfile::Standard,
                placeholder: "[REDACTED]".to_owned(),
            },
            &mut redacted,
        );

        assert!(result.is_err());
        fs::remove_file(path).expect("remove log");
    }

    #[test]
    fn submit_task_builds_a_publish_request() {
        let request = request_from_command(Command::SubmitTask {
            token: TokenArgs {
                token: Some("signed-token".to_owned()),
                token_file: None,
            },
            topic: TASKS_TOPIC.to_owned(),
            task_id: Some("task-1".to_owned()),
            task_type: "summarize_document".to_owned(),
            skill: Some("summarize".to_owned()),
            requires_topic: Some("topic:results".to_owned()),
            principal: Some("local:agent-alpha".to_owned()),
            preferred_agents: vec!["alpha".to_owned(), "beta".to_owned()],
            avoid_agents: vec!["gamma".to_owned()],
            required_agent: Some("alpha".to_owned()),
            affinity_key: Some("conversation-1".to_owned()),
            priority: 25,
            payload_json: Some("{\"path\":\"notes.md\"}".to_owned()),
            payload_text: None,
            payload_file: None,
            payload_base64: None,
            payload_inline: false,
            payload_content_type: None,
            payload_sha256: None,
            max_attempts: 4,
            timeout_seconds: 90,
            retry_delay_seconds: 12,
            classification: Some(Classification::Internal),
        })
        .expect("request");

        match request.command {
            ControlCommand::Publish {
                topic,
                classification,
                payload,
            } => {
                assert_eq!(topic, TASKS_TOPIC);
                assert_eq!(classification, Some(Classification::Internal));
                let task: TaskWorkItem = serde_json::from_str(&payload).expect("parse task");
                assert_eq!(task.task_id, "task-1");
                assert_eq!(task.task_type, "summarize_document");
                assert_eq!(task.priority, 25);
                assert_eq!(task.requirements.skill.as_deref(), Some("summarize"));
                assert_eq!(task.requirements.topic.as_deref(), Some("topic:results"));
                assert_eq!(
                    task.requirements.principal.as_deref(),
                    Some("local:agent-alpha")
                );
                assert_eq!(task.requirements.preferred_agents, vec!["alpha", "beta"]);
                assert_eq!(task.requirements.avoid_agents, vec!["gamma"]);
                assert_eq!(task.retry_policy.max_attempts, 4);
                assert_eq!(task.retry_policy.timeout_seconds, 90);
                assert_eq!(task.retry_policy.retry_delay_seconds, 12);
                assert_eq!(
                    task.payload,
                    TaskPayload::json(serde_json::json!({ "path": "notes.md" }))
                );
            }
            other => panic!("expected publish request, got {other:?}"),
        }
    }

    #[test]
    fn submit_task_builds_inline_binary_payloads() {
        let path = std::env::temp_dir().join(format!("expressways-task-{}.bin", Uuid::now_v7()));
        fs::write(&path, b"PNG").expect("write payload file");

        let request = request_from_command(Command::SubmitTask {
            token: TokenArgs {
                token: Some("signed-token".to_owned()),
                token_file: None,
            },
            topic: TASKS_TOPIC.to_owned(),
            task_id: Some("task-image".to_owned()),
            task_type: "classify_image".to_owned(),
            skill: Some("vision".to_owned()),
            requires_topic: None,
            principal: None,
            preferred_agents: Vec::new(),
            avoid_agents: Vec::new(),
            required_agent: None,
            affinity_key: None,
            priority: 0,
            payload_json: None,
            payload_text: None,
            payload_file: Some(path.clone()),
            payload_base64: None,
            payload_inline: true,
            payload_content_type: Some("image/png".to_owned()),
            payload_sha256: None,
            max_attempts: 3,
            timeout_seconds: 300,
            retry_delay_seconds: 5,
            classification: None,
        })
        .expect("request");

        match request.command {
            ControlCommand::Publish { payload, .. } => {
                let task: TaskWorkItem = serde_json::from_str(&payload).expect("parse task");
                assert_eq!(
                    task.payload.decode_inline_bytes().expect("decode bytes"),
                    Some(b"PNG".to_vec())
                );
                assert_eq!(task.payload.content_type(), Some("image/png"));
            }
            other => panic!("expected publish request, got {other:?}"),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn put_artifact_builds_binary_upload_command() {
        let path =
            std::env::temp_dir().join(format!("expressways-artifact-{}.bin", Uuid::now_v7()));
        fs::write(&path, b"PDF").expect("write artifact file");

        let (command, attachment) = build_put_artifact_request(
            Some("blob-1".to_owned()),
            Some(path.clone()),
            None,
            None,
            Some("application/pdf".to_owned()),
            Some("abc123".to_owned()),
            Some(Classification::Restricted),
            RetentionClass::Regulated,
        )
        .expect("request");

        match command {
            ControlCommand::PutArtifact {
                artifact_id,
                content_type,
                byte_length,
                sha256,
                classification,
                retention_class,
            } => {
                assert_eq!(artifact_id.as_deref(), Some("blob-1"));
                assert_eq!(content_type, "application/pdf");
                assert_eq!(byte_length, 3);
                assert_eq!(attachment, b"PDF".to_vec());
                assert_eq!(sha256.as_deref(), Some("abc123"));
                assert_eq!(classification, Some(Classification::Restricted));
                assert_eq!(retention_class, Some(RetentionClass::Regulated));
            }
            other => panic!("expected artifact upload request, got {other:?}"),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn submit_task_rejects_non_inline_file_requests_from_request_builder() {
        let error = request_from_command(Command::SubmitTask {
            token: TokenArgs {
                token: Some("signed-token".to_owned()),
                token_file: None,
            },
            topic: TASKS_TOPIC.to_owned(),
            task_id: Some("task-file".to_owned()),
            task_type: "inspect_blob".to_owned(),
            skill: Some("binary".to_owned()),
            requires_topic: None,
            principal: None,
            preferred_agents: Vec::new(),
            avoid_agents: Vec::new(),
            required_agent: None,
            affinity_key: None,
            priority: 0,
            payload_json: None,
            payload_text: None,
            payload_file: Some(PathBuf::from("./var/agent/incoming/report.pdf")),
            payload_base64: None,
            payload_inline: false,
            payload_content_type: Some("application/pdf".to_owned()),
            payload_sha256: None,
            max_attempts: 3,
            timeout_seconds: 300,
            retry_delay_seconds: 5,
            classification: None,
        })
        .expect_err("runtime-managed upload should bail");

        assert!(
            error
                .to_string()
                .contains("handled directly by the CLI runtime")
        );
    }

    #[test]
    fn report_task_rejects_non_terminal_statuses() {
        let error = request_from_command(Command::ReportTask {
            token: TokenArgs {
                token: Some("signed-token".to_owned()),
                token_file: None,
            },
            topic: TASK_EVENTS_TOPIC.to_owned(),
            task_id: "task-1".to_owned(),
            task_offset: Some(0),
            assignment_id: Uuid::nil(),
            agent_id: "alpha".to_owned(),
            status: TaskStatus::Assigned,
            attempt: 1,
            reason: None,
            classification: None,
        })
        .expect_err("invalid status");

        assert!(error.to_string().contains("completed or failed"));
    }

    #[test]
    fn validate_principal_from_toml_reports_policy_and_key_status() {
        let raw = r#"
[auth]
audience = "expressways"

[[auth.principals]]
id = "local:developer"
status = "active"
allowed_key_ids = ["dev"]

[policy]

[[policy.rules]]
principal = "local:developer"
resource = "system:broker"
actions = ["health", "admin"]
"#;

        let report = validate_principal_from_toml(
            raw,
            "test-config".to_owned(),
            "local:developer",
            Some("dev"),
        )
        .expect("principal diagnostics");
        assert_eq!(report.principal, "local:developer");
        assert_eq!(report.status, "active");
        assert!(report.key_allowed);
        assert_eq!(report.policy_rule_count, 1);
        assert_eq!(report.policy_resources, vec!["system:broker:health,admin"]);
    }

    #[test]
    fn validate_principal_from_toml_rejects_missing_principal() {
        let raw = r#"
[auth]
audience = "expressways"
"#;

        let error = validate_principal_from_toml(
            raw,
            "test-config".to_owned(),
            "local:missing",
            Some("dev"),
        )
        .expect_err("missing principal should fail");
        assert!(error.to_string().contains("not registered"));
    }

    #[test]
    fn validate_principal_from_toml_flags_disallowed_key() {
        let raw = r#"
[auth]
audience = "expressways"

[[auth.principals]]
id = "local:developer"
status = "active"
allowed_key_ids = ["other"]
"#;

        let report = validate_principal_from_toml(
            raw,
            "test-config".to_owned(),
            "local:developer",
            Some("dev"),
        )
        .expect("principal diagnostics");
        assert!(!report.key_allowed);
    }

    #[test]
    fn summarize_support_bundle_config_reads_top_level_sections() {
        let path = std::env::temp_dir().join(format!("expressways-config-{}.toml", Uuid::now_v7()));
        fs::write(
            &path,
            r#"
[schema]
version = 1

[server]
node_name = "dev-node"

[storage]
segment_max_bytes = 1024
"#,
        )
        .expect("write config");
        let mut warnings = Vec::new();
        let summary = summarize_support_bundle_config(&path, &mut warnings).expect("summary");
        assert!(summary.exists);
        assert!(summary.parse_error.is_none());
        assert!(summary.section_keys.contains(&"schema".to_owned()));
        assert!(summary.section_keys.contains(&"server".to_owned()));
        assert!(summary.section_keys.contains(&"storage".to_owned()));
        assert!(warnings.is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_head_and_tail_lines_returns_expected_windows() {
        let path = std::env::temp_dir().join(format!("expressways-support-{}.log", Uuid::now_v7()));
        fs::write(&path, "a\nb\nc\nd\ne\n").expect("write log");
        let settings = SupportBundleRedactionSettings {
            enabled: false,
            profile: SupportBundleRedactionProfile::Standard,
            placeholder: "[REDACTED]".to_owned(),
        };
        let mut redacted_lines = 0usize;
        let (line_count, head, tail) =
            read_head_and_tail_lines(&path, 2, 2, &settings, &mut redacted_lines)
                .expect("read windows");
        assert_eq!(line_count, 5);
        assert_eq!(head, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(tail, vec!["d".to_owned(), "e".to_owned()]);
        assert_eq!(redacted_lines, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn summarize_support_bundle_audit_includes_head_and_tail() {
        let path = std::env::temp_dir().join(format!("expressways-audit-{}.jsonl", Uuid::now_v7()));
        fs::write(&path, "1\n2\n3\n4\n5\n").expect("write audit log");
        let mut warnings = Vec::new();
        let settings = SupportBundleRedactionSettings {
            enabled: false,
            profile: SupportBundleRedactionProfile::Standard,
            placeholder: "[REDACTED]".to_owned(),
        };
        let mut redacted_lines = 0usize;
        let summary = summarize_support_bundle_audit(
            &path,
            2,
            2,
            "audit log",
            &mut warnings,
            &settings,
            &mut redacted_lines,
        )
        .expect("summary");
        assert!(summary.exists);
        assert_eq!(summary.line_count, 5);
        assert_eq!(summary.head, vec!["1".to_owned(), "2".to_owned()]);
        assert_eq!(summary.tail, vec!["4".to_owned(), "5".to_owned()]);
        assert!(warnings.is_empty());
        assert_eq!(redacted_lines, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn summarize_support_bundle_audit_reports_missing_label() {
        let path = std::env::temp_dir().join(format!(
            "expressways-missing-audit-{}.jsonl",
            Uuid::now_v7()
        ));
        let mut warnings = Vec::new();
        let settings = SupportBundleRedactionSettings {
            enabled: true,
            profile: SupportBundleRedactionProfile::Standard,
            placeholder: "[REDACTED]".to_owned(),
        };
        let mut redacted_lines = 0usize;
        let summary = summarize_support_bundle_audit(
            &path,
            2,
            2,
            "config audit log",
            &mut warnings,
            &settings,
            &mut redacted_lines,
        )
        .expect("summary");
        assert!(!summary.exists);
        assert_eq!(summary.line_count, 0);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("config audit log not found"))
        );
        assert_eq!(redacted_lines, 0);
    }

    #[test]
    fn read_head_and_tail_lines_redacts_sensitive_entries() {
        let path = std::env::temp_dir().join(format!("expressways-redact-{}.log", Uuid::now_v7()));
        fs::write(
            &path,
            "safe line\ncapability_token = eyJraWQiOiJkZXYifQ.GQnAy4UUeYzxjYt5LQmVnYV8x\n",
        )
        .expect("write log");
        let settings = SupportBundleRedactionSettings {
            enabled: true,
            profile: SupportBundleRedactionProfile::Standard,
            placeholder: "[MASKED]".to_owned(),
        };
        let mut redacted_lines = 0usize;
        let (_, head, tail) = read_head_and_tail_lines(&path, 5, 5, &settings, &mut redacted_lines)
            .expect("read windows");
        assert_eq!(redacted_lines, 1);
        assert!(head.iter().any(|line| line == "[MASKED]"));
        assert!(tail.iter().any(|line| line == "[MASKED]"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn strict_profile_redacts_additional_sensitive_lines() {
        let candidate = "set-cookie: session_token=AbCdEfGhIjKlMnOpQrStUvWxYz1234567890";
        let standard_settings = SupportBundleRedactionSettings {
            enabled: true,
            profile: SupportBundleRedactionProfile::Standard,
            placeholder: "[MASKED]".to_owned(),
        };
        let strict_settings = SupportBundleRedactionSettings {
            enabled: true,
            profile: SupportBundleRedactionProfile::Strict,
            placeholder: "[MASKED]".to_owned(),
        };

        let (standard_value, standard_redacted) =
            redact_support_bundle_line(candidate, &standard_settings);
        assert_eq!(standard_value, candidate.to_owned());
        assert!(!standard_redacted);

        let (strict_value, strict_redacted) =
            redact_support_bundle_line(candidate, &strict_settings);
        assert_eq!(strict_value, "[MASKED]".to_owned());
        assert!(strict_redacted);
    }

    #[test]
    fn redaction_profile_policy_names_are_stable() {
        assert_eq!(
            SupportBundleRedactionProfile::Standard.policy_name(),
            "expressways.support_bundle.redaction.standard.v1"
        );
        assert_eq!(
            SupportBundleRedactionProfile::Strict.policy_name(),
            "expressways.support_bundle.redaction.strict.v1"
        );
    }

    #[test]
    fn redaction_disable_flag_overrides_profile_rules() {
        let candidate = "authorization: Bearer eyJraWQiOiJkZXYifQ.GQnAy4UUeYzxjYt5LQmVnYV8x";
        let settings = SupportBundleRedactionSettings {
            enabled: false,
            profile: SupportBundleRedactionProfile::Strict,
            placeholder: "[MASKED]".to_owned(),
        };
        let (value, redacted) = redact_support_bundle_line(candidate, &settings);
        assert_eq!(value, candidate.to_owned());
        assert!(!redacted);
    }

    fn sample_support_bundle(path: &Path, complete: bool) -> SupportBundle {
        let mut section_keys = vec![
            "auth".to_owned(),
            "storage".to_owned(),
            "policy".to_owned(),
            "quotas".to_owned(),
            "registry".to_owned(),
            "resilience".to_owned(),
        ];
        if complete {
            section_keys.push("adopters".to_owned());
        }

        SupportBundle {
            schema_version: "expressways.support_bundle.v1".to_owned(),
            generated_at: Utc::now(),
            broker_transport: "tcp".to_owned(),
            broker_address: "127.0.0.1:7766".to_owned(),
            config: SupportBundleConfigSummary {
                path: path.display().to_string(),
                exists: true,
                size_bytes: Some(1024),
                modified_at: Some(Utc::now()),
                section_keys,
                parse_error: None,
            },
            audit: SupportBundleAuditSummary {
                path: "./var/audit/audit.jsonl".to_owned(),
                exists: true,
                size_bytes: Some(128),
                modified_at: Some(Utc::now()),
                line_count: 3,
                head: vec!["{\"decision\":\"allow\"}".to_owned()],
                tail: vec!["{\"decision\":\"deny\"}".to_owned()],
            },
            config_audit: SupportBundleAuditSummary {
                path: "./var/agent/config-audit/entries.jsonl".to_owned(),
                exists: complete,
                size_bytes: complete.then_some(64),
                modified_at: Some(Utc::now()),
                line_count: if complete { 1 } else { 0 },
                head: if complete {
                    vec!["{\"action\":\"apply\"}".to_owned()]
                } else {
                    Vec::new()
                },
                tail: if complete {
                    vec!["{\"action\":\"apply\"}".to_owned()]
                } else {
                    Vec::new()
                },
            },
            logs: Vec::new(),
            broker_snapshot: Some(SupportBundleBrokerSnapshot {
                health: Some(SupportBundleBrokerHealth {
                    node_name: "dev-node".to_owned(),
                    status: "ok".to_owned(),
                }),
                metrics: Some(BrokerMetricsView {
                    uptime_seconds: 10,
                    total_requests: 50,
                    health_requests: 10,
                    admin_requests: 5,
                    auth_failures: 1,
                    policy_denials: 1,
                    quota_denials: 1,
                    storage_failures: 0,
                    audit_failures: 0,
                    publish: expressways_protocol::OperationMetricsView {
                        requests: 10,
                        successes: 9,
                        failures: 1,
                        average_latency_ms: 2,
                        max_latency_ms: 8,
                    },
                    consume: expressways_protocol::OperationMetricsView {
                        requests: 10,
                        successes: 10,
                        failures: 0,
                        average_latency_ms: 2,
                        max_latency_ms: 8,
                    },
                    storage: expressways_protocol::StorageMetricsView {
                        topic_count: 2,
                        segment_count: 4,
                        total_bytes: 1024,
                        reclaimed_segments: 1,
                        reclaimed_bytes: 256,
                        recovered_segments: 0,
                        truncated_bytes: 0,
                    },
                    audit: expressways_protocol::AuditMetricsView {
                        event_count: 10,
                        last_hash: Some("abc123".to_owned()),
                    },
                    streams: expressways_protocol::StreamMetricsView {
                        open_streams: 0,
                        opened_streams: 0,
                        closed_streams: 0,
                        keepalives_sent: 0,
                        event_frames_sent: 0,
                        events_delivered: 0,
                        delivery_failures: 0,
                        slow_consumer_drops: 0,
                        idle_timeouts: 0,
                        watch_stream: expressways_protocol::OperationMetricsView {
                            requests: 0,
                            successes: 0,
                            failures: 0,
                            average_latency_ms: 0,
                            max_latency_ms: 0,
                        },
                    },
                    resilience: expressways_protocol::ResilienceMetricsView {
                        service_mode: "healthy".to_owned(),
                        degraded_components: Vec::new(),
                    },
                    adopters: if complete {
                        vec![expressways_protocol::AdopterStatusView {
                            id: "audit_integrity".to_owned(),
                            package: "expressways-adopter-audit-integrity".to_owned(),
                            description: "Audit integrity checks".to_owned(),
                            enabled: true,
                            status: "healthy".to_owned(),
                            detail: "ok".to_owned(),
                            capabilities: vec!["health_probe".to_owned()],
                            last_run_at: Some(Utc::now()),
                        }]
                    } else {
                        Vec::new()
                    },
                }),
                auth: complete.then_some(AuthStateView {
                    audience: "expressways".to_owned(),
                    issuers: Vec::new(),
                    principals: Vec::new(),
                    revocations: expressways_protocol::AuthRevocationView {
                        revoked_tokens: Vec::new(),
                        revoked_principals: Vec::new(),
                        revoked_key_ids: Vec::new(),
                    },
                }),
            }),
            redaction: SupportBundleRedactionSummary {
                enabled: true,
                profile: SupportBundleRedactionProfile::Standard,
                policy: if complete {
                    SupportBundleRedactionProfile::Standard
                        .policy_name()
                        .to_owned()
                } else {
                    String::new()
                },
                placeholder: "[REDACTED]".to_owned(),
                redacted_lines: 0,
            },
            warnings: Vec::new(),
        }
    }

    #[test]
    fn validate_support_bundle_coverage_passes_for_complete_bundle() {
        let path = std::env::temp_dir().join(format!(
            "expressways-support-coverage-{}.json",
            Uuid::now_v7()
        ));
        let bundle = sample_support_bundle(&path, true);
        fs::write(
            &path,
            serde_json::to_vec_pretty(&bundle).expect("serialize bundle"),
        )
        .expect("write bundle");

        let report = validate_support_bundle_coverage(&path).expect("coverage report");
        assert!(report.pass);
        assert_eq!(report.undiagnosable_incidents, 0);
        assert_eq!(report.incident_count, 10);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn validate_support_bundle_coverage_flags_missing_evidence() {
        let path = std::env::temp_dir().join(format!(
            "expressways-support-coverage-missing-{}.json",
            Uuid::now_v7()
        ));
        let bundle = sample_support_bundle(&path, false);
        fs::write(
            &path,
            serde_json::to_vec_pretty(&bundle).expect("serialize bundle"),
        )
        .expect("write bundle");

        let report = validate_support_bundle_coverage(&path).expect("coverage report");
        assert!(!report.pass);
        assert!(report.undiagnosable_incidents >= 1);
        assert!(
            report
                .incidents
                .iter()
                .any(|incident| incident.id == "auth_revocation_denial" && !incident.diagnosable)
        );
        let _ = fs::remove_file(path);
    }

    fn toml_path(path: &Path) -> String {
        path.display()
            .to_string()
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
    }

    fn write_signed_backup_manifest(
        manifest_path: &Path,
        manifest: &RuntimeBackupManifest,
        signer: &CapabilityIssuer,
    ) {
        let bytes = serde_json::to_vec_pretty(manifest).expect("serialize backup manifest");
        fs::write(manifest_path, &bytes).expect("write backup manifest");
        let signature = RuntimeBackupSignature {
            algorithm: "ed25519".to_owned(),
            key_id: "runtime-backup".to_owned(),
            signature: signer.sign_bytes(&bytes),
        };
        fs::write(
            manifest_path
                .parent()
                .expect("manifest parent")
                .join("manifest.sig.json"),
            serde_json::to_vec_pretty(&signature).expect("serialize backup signature"),
        )
        .expect("write backup signature");
    }

    #[test]
    fn runtime_backup_and_restore_round_trip_restores_state_files() {
        let root =
            std::env::temp_dir().join(format!("expressways-runtime-backup-{}", Uuid::now_v7()));
        let config_path = root.join("configs/expressways.backup.toml");
        let data_file = root.join("var/data/tasks/state.json");
        let artifact_blob = root.join("var/data/artifacts/blobs/blob-1.blob");
        let audit_path = root.join("var/audit/audit.jsonl");
        let revocation_path = root.join("var/auth/revocations.json");
        let issuer_private_path = root.join("var/auth/issuer.private");
        let issuer_public_path = root.join("var/auth/issuer.public");
        let registry_path = root.join("var/registry/agents.json");
        let config_audit_log = root.join("var/agent/config-audit/entries.jsonl");
        let orchestrator_state = root.join("var/orchestrator/state.json");
        let interop_state = root.join("var/agent/interop-bridge-state.json");
        let nanobot_state = root.join("var/agent/nanobot-runtime/runtime-state.json");
        let additional_state = vec![
            format!("interop_bridge={}", interop_state.display()),
            format!(
                "nanobot_runtime={}",
                nanobot_state
                    .parent()
                    .expect("nanobot state parent")
                    .display()
            ),
        ];

        fs::create_dir_all(data_file.parent().expect("state parent")).expect("create state dir");
        fs::create_dir_all(artifact_blob.parent().expect("artifact parent"))
            .expect("create artifact dir");
        fs::create_dir_all(audit_path.parent().expect("audit parent")).expect("create audit dir");
        fs::create_dir_all(revocation_path.parent().expect("auth parent"))
            .expect("create auth dir");
        fs::create_dir_all(registry_path.parent().expect("registry parent"))
            .expect("create registry dir");
        fs::create_dir_all(config_audit_log.parent().expect("config-audit parent"))
            .expect("create config audit dir");
        fs::create_dir_all(orchestrator_state.parent().expect("orchestrator parent"))
            .expect("create orchestrator dir");
        fs::create_dir_all(nanobot_state.parent().expect("nanobot state parent"))
            .expect("create nanobot state dir");
        fs::create_dir_all(config_path.parent().expect("config parent"))
            .expect("create config dir");

        fs::write(&data_file, "{\"next_offset\":12}").expect("write state file");
        fs::write(&artifact_blob, b"BLOB").expect("write artifact blob");
        fs::write(&audit_path, "{\"event\":\"allow\"}\n").expect("write audit");
        fs::write(&revocation_path, "{\"revoked_token_ids\":[]}\n").expect("write revocations");
        let backup_signer = CapabilityIssuer::generate("runtime-backup");
        backup_signer
            .write_private_key(&issuer_private_path)
            .expect("write issuer private key");
        backup_signer
            .write_public_key(&issuer_public_path)
            .expect("write issuer public key");
        fs::write(&registry_path, "{\"schema_version\":1,\"agents\":[]}").expect("write registry");
        fs::write(&config_audit_log, "{\"entry\":1}\n").expect("write config audit");
        fs::write(&orchestrator_state, "{\"tasks\":[]}").expect("write orchestrator state");
        fs::write(&interop_state, "{\"schema_version\":1,\"cursors\":{}}")
            .expect("write interop state");
        fs::write(&nanobot_state, "{\"schema_version\":1}").expect("write nanobot state");

        let config = format!(
            r#"
[server]
data_dir = "{data_dir}"

[audit]
path = "{audit_path}"

[auth]
revocation_path = "{revocation_path}"

[[auth.issuers]]
key_id = "dev"
public_key_path = "{issuer_public_path}"

[registry]
path = "{registry_path}"
"#,
            data_dir = toml_path(&root.join("var/data")),
            audit_path = toml_path(&audit_path),
            revocation_path = toml_path(&revocation_path),
            issuer_public_path = toml_path(&issuer_public_path),
            registry_path = toml_path(&registry_path),
        );
        fs::write(&config_path, &config).expect("write config");

        let backup = create_runtime_backup(BackupRuntimeOptions {
            config: config_path.clone(),
            output_dir: root.join("var/agent/backups"),
            backup_id: Some("unit-backup".to_owned()),
            config_audit_log: config_audit_log.clone(),
            orchestrator_state: orchestrator_state.clone(),
            additional_state: additional_state.clone(),
            overwrite: false,
            confirm_writers_stopped: true,
            signing_private_key: issuer_private_path.clone(),
            signing_key_id: "runtime-backup".to_owned(),
        })
        .expect("create backup");

        assert!(PathBuf::from(&backup.manifest_path).exists());
        assert!(backup.copied_entries >= 6);

        let backup = create_runtime_backup(BackupRuntimeOptions {
            config: config_path.clone(),
            output_dir: root.join("var/agent/backups"),
            backup_id: Some("unit-backup".to_owned()),
            config_audit_log: config_audit_log.clone(),
            orchestrator_state: orchestrator_state.clone(),
            additional_state: additional_state.clone(),
            overwrite: true,
            confirm_writers_stopped: true,
            signing_private_key: issuer_private_path.clone(),
            signing_key_id: "runtime-backup".to_owned(),
        })
        .expect("transactionally overwrite named backup");
        assert!(PathBuf::from(&backup.manifest_path).exists());
        assert!(
            !runtime_backup_publish_journal_path(&root.join("var/agent/backups"), "unit-backup")
                .exists()
        );

        fs::write(&data_file, "{\"next_offset\":999}").expect("mutate state file");
        fs::remove_file(&config_path).expect("remove live config");
        fs::remove_file(&audit_path).expect("remove audit file");
        fs::remove_file(&registry_path).expect("remove registry file");
        fs::write(&orchestrator_state, "{\"tasks\":[\"changed\"]}").expect("mutate orchestrator");
        fs::remove_file(&interop_state).expect("remove interop state");
        fs::remove_dir_all(nanobot_state.parent().expect("nanobot state parent"))
            .expect("remove nanobot state");

        let restore = restore_runtime_backup(RestoreRuntimeOptions {
            backup_dir: PathBuf::from(&backup.backup_dir),
            config: config_path.clone(),
            config_audit_log: config_audit_log.clone(),
            orchestrator_state: orchestrator_state.clone(),
            additional_state: additional_state.clone(),
            overwrite: true,
            dry_run: false,
            confirm_writers_stopped: true,
            verification_public_key: issuer_public_path.clone(),
        })
        .expect("restore backup");

        assert!(restore.restored_entries >= 6);
        assert_eq!(
            fs::read_to_string(&config_path).expect("read restored config"),
            config
        );
        assert_eq!(
            fs::read_to_string(&data_file).expect("read restored state"),
            "{\"next_offset\":12}"
        );
        assert_eq!(
            fs::read_to_string(&audit_path).expect("read restored audit"),
            "{\"event\":\"allow\"}\n"
        );
        assert_eq!(
            fs::read_to_string(&registry_path).expect("read restored registry"),
            "{\"schema_version\":1,\"agents\":[]}"
        );
        assert_eq!(
            fs::read_to_string(&orchestrator_state).expect("read restored orchestrator"),
            "{\"tasks\":[]}"
        );
        assert_eq!(
            fs::read_to_string(&interop_state).expect("read restored interop state"),
            "{\"schema_version\":1,\"cursors\":{}}"
        );
        assert_eq!(
            fs::read_to_string(&nanobot_state).expect("read restored nanobot state"),
            "{\"schema_version\":1}"
        );

        let manifest_path = PathBuf::from(&backup.manifest_path);
        let original_manifest = fs::read(&manifest_path).expect("read manifest");
        let mut manifest: RuntimeBackupManifest =
            serde_json::from_slice(&original_manifest).expect("parse manifest");
        let mut unsigned_tamper = manifest.clone();
        unsigned_tamper
            .warnings
            .push("attacker-modified manifest".to_owned());
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&unsigned_tamper).expect("serialize tampered manifest"),
        )
        .expect("write tampered manifest");
        let error = restore_runtime_backup(RestoreRuntimeOptions {
            backup_dir: PathBuf::from(&backup.backup_dir),
            config: config_path.clone(),
            config_audit_log: config_audit_log.clone(),
            orchestrator_state: orchestrator_state.clone(),
            additional_state: additional_state.clone(),
            overwrite: true,
            dry_run: true,
            confirm_writers_stopped: false,
            verification_public_key: issuer_public_path.clone(),
        })
        .expect_err("unsigned manifest tampering must fail");
        assert!(error.to_string().contains("signature verification failed"));
        fs::write(&manifest_path, &original_manifest).expect("restore signed manifest bytes");

        let audit_backup_path = manifest
            .entries
            .iter()
            .find(|entry| entry.key == "audit_log")
            .and_then(|entry| entry.backup_path.as_ref())
            .map(|path| PathBuf::from(&backup.backup_dir).join(path))
            .expect("audit backup payload");
        let original_audit_backup = fs::read(&audit_backup_path).expect("read audit backup");
        fs::write(&audit_backup_path, b"{\"event\":\"deny!\"}\n")
            .expect("tamper audit backup with same-size content");
        fs::write(&data_file, "{\"next_offset\":777}").expect("mutate live data before failure");
        let error = restore_runtime_backup(RestoreRuntimeOptions {
            backup_dir: PathBuf::from(&backup.backup_dir),
            config: config_path.clone(),
            config_audit_log: config_audit_log.clone(),
            orchestrator_state: orchestrator_state.clone(),
            additional_state: additional_state.clone(),
            overwrite: true,
            dry_run: false,
            confirm_writers_stopped: true,
            verification_public_key: issuer_public_path.clone(),
        })
        .expect_err("same-size payload tampering must fail");
        assert!(error.to_string().contains("integrity check failed"));
        assert_eq!(
            fs::read_to_string(&data_file).expect("read live data after failed restore"),
            "{\"next_offset\":777}",
            "no target may be modified until every backup payload passes validation"
        );
        fs::write(&audit_backup_path, original_audit_backup).expect("restore audit backup");

        let present_entry = manifest
            .entries
            .iter_mut()
            .find(|entry| entry.exists)
            .expect("present backup entry");
        present_entry.backup_path = Some("../../outside".to_owned());
        write_signed_backup_manifest(&manifest_path, &manifest, &backup_signer);
        let error = restore_runtime_backup(RestoreRuntimeOptions {
            backup_dir: PathBuf::from(&backup.backup_dir),
            config: config_path.clone(),
            config_audit_log: config_audit_log.clone(),
            orchestrator_state: orchestrator_state.clone(),
            additional_state: additional_state.clone(),
            overwrite: true,
            dry_run: true,
            confirm_writers_stopped: false,
            verification_public_key: issuer_public_path.clone(),
        })
        .expect_err("path traversal must fail");
        assert!(error.to_string().contains("unsafe backup_path"));

        let mut manifest: RuntimeBackupManifest =
            serde_json::from_slice(&original_manifest).expect("parse restored manifest");
        write_signed_backup_manifest(&manifest_path, &manifest, &backup_signer);
        manifest
            .entries
            .iter_mut()
            .find(|entry| entry.exists)
            .expect("present backup entry")
            .source_path = root.join("outside-target").display().to_string();
        write_signed_backup_manifest(&manifest_path, &manifest, &backup_signer);
        let relocated = restore_runtime_backup(RestoreRuntimeOptions {
            backup_dir: PathBuf::from(&backup.backup_dir),
            config: config_path,
            config_audit_log,
            orchestrator_state,
            additional_state,
            overwrite: true,
            dry_run: true,
            confirm_writers_stopped: false,
            verification_public_key: issuer_public_path,
        })
        .expect("signed source metadata must not override config-derived destinations");
        assert!(relocated.dry_run);
    }

    #[test]
    fn runtime_backup_and_restore_require_quiesced_writer_confirmation() {
        let backup_error = create_runtime_backup(BackupRuntimeOptions {
            config: PathBuf::from("unused.toml"),
            output_dir: PathBuf::from("unused-backups"),
            backup_id: Some("unused".to_owned()),
            config_audit_log: PathBuf::from("unused-config-audit.jsonl"),
            orchestrator_state: PathBuf::from("unused-orchestrator.json"),
            additional_state: Vec::new(),
            overwrite: false,
            confirm_writers_stopped: false,
            signing_private_key: PathBuf::from("unused-private-key"),
            signing_key_id: "unused".to_owned(),
        })
        .expect_err("live-writer backup must be refused");
        assert!(
            backup_error
                .to_string()
                .contains("--confirm-writers-stopped")
        );

        let restore_error = restore_runtime_backup(RestoreRuntimeOptions {
            backup_dir: PathBuf::from("unused-backup"),
            config: PathBuf::from("unused.toml"),
            config_audit_log: PathBuf::from("unused-config-audit.jsonl"),
            orchestrator_state: PathBuf::from("unused-orchestrator.json"),
            additional_state: Vec::new(),
            overwrite: true,
            dry_run: false,
            confirm_writers_stopped: false,
            verification_public_key: PathBuf::from("unused-public-key"),
        })
        .expect_err("live-writer restore must be refused");
        assert!(
            restore_error
                .to_string()
                .contains("--confirm-writers-stopped")
        );
    }

    #[test]
    fn runtime_backup_output_must_be_disjoint_from_directory_targets() {
        let root =
            std::env::temp_dir().join(format!("expressways-backup-overlap-{}", Uuid::now_v7()));
        let state_dir = root.join("state");
        let nested_output = state_dir.join("backups");
        fs::create_dir_all(&nested_output).expect("create nested output");
        let state_target = RuntimeBackupTarget {
            key: "server_data".to_owned(),
            source_path: state_dir,
            required: true,
        };
        let error = validate_runtime_backup_output_disjoint(
            &nested_output,
            std::slice::from_ref(&state_target),
        )
        .expect_err("output inside state must fail");
        assert!(error.to_string().contains("overlaps directory target"));

        let outer_output = root.join("outer-backups");
        let nested_state = outer_output.join("state");
        fs::create_dir_all(&nested_state).expect("create nested state");
        let nested_target = RuntimeBackupTarget {
            key: "server_data".to_owned(),
            source_path: nested_state,
            required: true,
        };
        let error = validate_runtime_backup_output_disjoint(
            &outer_output,
            std::slice::from_ref(&nested_target),
        )
        .expect_err("state inside output must fail");
        assert!(error.to_string().contains("overlaps directory target"));

        fs::remove_dir_all(root).expect("remove overlap fixture");
    }

    #[test]
    fn additional_runtime_state_names_are_stable_and_unique() {
        let cwd = Path::new("/tmp/expressways-additional-state");
        let targets = parse_additional_runtime_targets(
            &[
                "bridge=bridge.json".to_owned(),
                "nanobot_runtime=nanobot".to_owned(),
            ],
            cwd,
        )
        .expect("parse named runtime state");
        assert_eq!(targets[0].key, "additional_state:bridge");
        assert_eq!(targets[0].source_path, cwd.join("bridge.json"));
        assert_eq!(targets[1].key, "additional_state:nanobot_runtime");

        let duplicate = parse_additional_runtime_targets(
            &["bridge=one".to_owned(), "bridge=two".to_owned()],
            cwd,
        )
        .expect_err("duplicate names must fail");
        assert!(duplicate.to_string().contains("duplicate"));
        assert!(parse_additional_runtime_targets(&["bad name=state".to_owned()], cwd).is_err());
        assert!(parse_additional_runtime_targets(&["missing-separator".to_owned()], cwd).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn runtime_backup_rejects_nested_symlinks_in_directory_state() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "expressways-backup-nested-symlink-{}",
            Uuid::now_v7()
        ));
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(&source).expect("create source");
        fs::write(source.join("state.json"), b"{}").expect("write state");
        symlink(source.join("state.json"), source.join("state-link.json"))
            .expect("create nested symlink");

        let error = copy_runtime_backup_directory(&source, &destination)
            .expect_err("nested symlink must fail backup");
        assert!(error.to_string().contains("contains a symlink"));
        fs::remove_dir_all(root).expect("remove symlink fixture");
    }

    #[test]
    fn runtime_restore_rejects_overlapping_transaction_paths() {
        let root =
            std::env::temp_dir().join(format!("expressways-restore-overlap-{}", Uuid::now_v7()));
        let backup_dir = root.join("backup");
        let journal_path = root.join("config/.expressways-restore-transaction.json");
        let parent = RuntimeRestorePlanEntry {
            key: "server_data".to_owned(),
            source_path: backup_dir.join("payload/server-data"),
            destination_path: root.join("live"),
            kind: RuntimeBackupEntryKind::Directory,
        };
        let child = RuntimeRestorePlanEntry {
            key: "registry_state".to_owned(),
            source_path: backup_dir.join("payload/registry"),
            destination_path: root.join("live/registry/agents.json"),
            kind: RuntimeBackupEntryKind::File,
        };
        let error = validate_runtime_restore_topology(&[parent, child], &backup_dir, &journal_path)
            .expect_err("nested restore targets must fail");
        assert!(
            error
                .to_string()
                .contains("runtime state targets must be disjoint")
        );

        let bundle_target = RuntimeRestorePlanEntry {
            key: "server_data".to_owned(),
            source_path: backup_dir.join("payload/server-data"),
            destination_path: backup_dir.join("restored-data"),
            kind: RuntimeBackupEntryKind::Directory,
        };
        let error = validate_runtime_restore_topology(&[bundle_target], &backup_dir, &journal_path)
            .expect_err("restore into bundle must fail");
        assert!(error.to_string().contains("overlaps backup bundle"));

        let journal_parent_target = RuntimeRestorePlanEntry {
            key: "server_data".to_owned(),
            source_path: backup_dir.join("payload/server-data"),
            destination_path: root.join("config"),
            kind: RuntimeBackupEntryKind::Directory,
        };
        let error =
            validate_runtime_restore_topology(&[journal_parent_target], &backup_dir, &journal_path)
                .expect_err("journal inside restored directory must fail");
        assert!(error.to_string().contains("overlaps restore journal"));
    }

    #[test]
    fn runtime_restore_stages_every_target_before_replacing_live_state() {
        let root = std::env::temp_dir().join(format!(
            "expressways-runtime-transactional-restore-{}",
            Uuid::now_v7()
        ));
        let source_one = root.join("backup/one");
        let source_two = root.join("backup/two");
        let destination_one = root.join("live/one");
        let destination_two = root.join("live/two");
        fs::create_dir_all(source_one.parent().expect("backup parent"))
            .expect("create backup parent");
        fs::create_dir_all(destination_one.parent().expect("live parent"))
            .expect("create live parent");
        fs::write(&source_one, b"new-one").expect("write first backup payload");
        fs::write(&destination_one, b"old-one").expect("write first live target");
        fs::write(&destination_two, b"old-two").expect("write second live target");

        let plan = vec![
            RuntimeRestorePlanEntry {
                key: "one".to_owned(),
                source_path: source_one,
                destination_path: destination_one.clone(),
                kind: RuntimeBackupEntryKind::File,
            },
            RuntimeRestorePlanEntry {
                key: "two".to_owned(),
                source_path: source_two.clone(),
                destination_path: destination_two.clone(),
                kind: RuntimeBackupEntryKind::File,
            },
        ];
        let journal_path = root.join("live/.expressways-restore-transaction.json");

        let error = restore_runtime_backup_plan(&plan, &journal_path)
            .expect_err("a staging failure must abort the restore transaction");
        assert!(
            error
                .to_string()
                .contains("failed to stage runtime target `two`")
        );
        assert_eq!(
            fs::read(&destination_one).expect("read first live target"),
            b"old-one"
        );
        assert_eq!(
            fs::read(&destination_two).expect("read second live target"),
            b"old-two"
        );
        assert!(
            fs::read_dir(root.join("live"))
                .expect("read live directory")
                .all(|entry| !entry
                    .expect("live entry")
                    .file_name()
                    .to_string_lossy()
                    .contains(".restore-"))
        );

        fs::write(&source_two, b"new-two").expect("write second backup payload");
        restore_runtime_backup_plan(&plan, &journal_path)
            .expect("commit staged restore transaction");
        assert_eq!(
            fs::read(&destination_one).expect("read restored first target"),
            b"new-one"
        );
        assert_eq!(
            fs::read(&destination_two).expect("read restored second target"),
            b"new-two"
        );
        fs::remove_dir_all(root).expect("remove transactional restore fixture");
    }

    #[test]
    fn runtime_restore_journal_recovers_interrupted_and_committed_transactions() {
        let root = std::env::temp_dir().join(format!(
            "expressways-runtime-journal-recovery-{}",
            Uuid::now_v7()
        ));
        let source = root.join("backup/state");
        let destination = root.join("live/state");
        let journal_path = root.join("live/.expressways-restore-transaction.json");
        fs::create_dir_all(source.parent().expect("backup parent")).expect("create backup parent");
        fs::create_dir_all(destination.parent().expect("live parent")).expect("create live parent");
        fs::write(&source, b"new-state").expect("write backup state");
        fs::write(&destination, b"old-state").expect("write live state");
        let plan = vec![RuntimeRestorePlanEntry {
            key: "state".to_owned(),
            source_path: source,
            destination_path: destination.clone(),
            kind: RuntimeBackupEntryKind::File,
        }];

        let operation_id = Uuid::now_v7();
        let mut prepared = vec![
            prepare_runtime_restore_entry(&plan[0], operation_id, 0).expect("prepare restore"),
        ];
        prepared[0].had_previous = true;
        write_runtime_restore_journal(
            &journal_path,
            &runtime_restore_journal(operation_id, &prepared),
        )
        .expect("write prepared journal");
        fs::rename(&destination, &prepared[0].previous_path).expect("stage previous state");
        fs::rename(&prepared[0].staged_path, &destination).expect("install new state");

        recover_runtime_restore_transaction(&plan, &journal_path)
            .expect("roll back interrupted transaction");
        assert_eq!(
            fs::read(&destination).expect("read rolled-back state"),
            b"old-state"
        );
        assert!(!journal_path.exists());

        let operation_id = Uuid::now_v7();
        let mut prepared = vec![
            prepare_runtime_restore_entry(&plan[0], operation_id, 0).expect("prepare restore"),
        ];
        prepared[0].had_previous = true;
        write_runtime_restore_journal(
            &journal_path,
            &runtime_restore_journal(operation_id, &prepared),
        )
        .expect("write second prepared journal");
        fs::rename(&destination, &prepared[0].previous_path).expect("stage prior state");
        fs::rename(&prepared[0].staged_path, &destination).expect("install committed state");
        write_runtime_restore_commit_marker(&journal_path).expect("mark transaction committed");

        recover_runtime_restore_transaction(&plan, &journal_path)
            .expect("finish committed transaction cleanup");
        assert_eq!(
            fs::read(&destination).expect("read committed state"),
            b"new-state"
        );
        assert!(!journal_path.exists());
        assert!(!restore_commit_marker_path(&journal_path).exists());

        write_runtime_restore_commit_marker(&journal_path).expect("write orphaned commit marker");
        recover_runtime_restore_transaction(&plan, &journal_path)
            .expect("finish cleanup after journal removal");
        assert!(!restore_commit_marker_path(&journal_path).exists());
        fs::remove_dir_all(root).expect("remove journal recovery fixture");
    }

    #[test]
    fn runtime_backup_publish_journal_recovers_interrupted_overwrites() {
        let output_dir = std::env::temp_dir().join(format!(
            "expressways-backup-publish-journal-{}",
            Uuid::now_v7()
        ));
        fs::create_dir_all(&output_dir).expect("create backup output");
        let backup_id = "stable-backup";
        let final_path = output_dir.join(backup_id);
        fs::create_dir_all(&final_path).expect("create old backup");
        fs::write(final_path.join("version"), b"old").expect("write old backup");
        let journal_path = runtime_backup_publish_journal_path(&output_dir, backup_id);

        let operation_id = Uuid::now_v7();
        let staged_path = runtime_backup_staged_path(&output_dir, backup_id, operation_id);
        let previous_path = runtime_backup_previous_path(&output_dir, backup_id, operation_id);
        fs::create_dir_all(&staged_path).expect("create staged backup");
        fs::write(staged_path.join("version"), b"new").expect("write staged backup");
        let journal = RuntimeBackupPublishJournal {
            schema_version: 1,
            operation_id,
            backup_id: backup_id.to_owned(),
            final_path: final_path.clone(),
            staged_path: staged_path.clone(),
            previous_path: previous_path.clone(),
            had_previous: true,
        };
        write_runtime_backup_publish_journal(&journal_path, &journal)
            .expect("write publish journal");
        fs::rename(&final_path, &previous_path).expect("preserve old backup");
        fs::rename(&staged_path, &final_path).expect("install uncommitted backup");

        recover_runtime_backup_publish(&journal_path, &output_dir, backup_id)
            .expect("roll back interrupted backup overwrite");
        assert_eq!(
            fs::read(final_path.join("version")).expect("read rolled-back backup"),
            b"old"
        );
        assert!(!journal_path.exists());

        let operation_id = Uuid::now_v7();
        let staged_path = runtime_backup_staged_path(&output_dir, backup_id, operation_id);
        let previous_path = runtime_backup_previous_path(&output_dir, backup_id, operation_id);
        fs::create_dir_all(&staged_path).expect("create second staged backup");
        fs::write(staged_path.join("version"), b"new").expect("write committed backup");
        let journal = RuntimeBackupPublishJournal {
            schema_version: 1,
            operation_id,
            backup_id: backup_id.to_owned(),
            final_path: final_path.clone(),
            staged_path: staged_path.clone(),
            previous_path: previous_path.clone(),
            had_previous: true,
        };
        write_runtime_backup_publish_journal(&journal_path, &journal)
            .expect("write second publish journal");
        fs::rename(&final_path, &previous_path).expect("preserve rolled-back backup");
        fs::rename(&staged_path, &final_path).expect("install committed backup");
        write_runtime_backup_publish_marker(&journal_path).expect("mark backup committed");

        recover_runtime_backup_publish(&journal_path, &output_dir, backup_id)
            .expect("finish committed backup cleanup");
        assert_eq!(
            fs::read(final_path.join("version")).expect("read committed backup"),
            b"new"
        );
        assert!(!previous_path.exists());
        assert!(!journal_path.exists());
        assert!(!runtime_backup_publish_marker_path(&journal_path).exists());

        fs::remove_dir_all(output_dir).expect("remove publish recovery fixture");
    }

    #[test]
    fn runtime_backup_fails_when_required_target_is_missing() {
        let root =
            std::env::temp_dir().join(format!("expressways-runtime-missing-{}", Uuid::now_v7()));
        let config_path = root.join("configs/expressways.backup.toml");
        let audit_path = root.join("var/audit/audit.jsonl");
        let revocation_path = root.join("var/auth/revocations.json");
        let issuer_private_path = root.join("var/auth/issuer.private");
        let issuer_public_path = root.join("var/auth/issuer.public");

        fs::create_dir_all(config_path.parent().expect("config parent"))
            .expect("create config dir");
        fs::create_dir_all(audit_path.parent().expect("audit parent")).expect("create audit dir");
        fs::create_dir_all(revocation_path.parent().expect("auth parent"))
            .expect("create auth dir");

        fs::write(&audit_path, "{\"event\":\"allow\"}\n").expect("write audit");
        fs::write(&revocation_path, "{\"revoked_token_ids\":[]}\n").expect("write revocations");
        let backup_signer = CapabilityIssuer::generate("runtime-backup");
        backup_signer
            .write_private_key(&issuer_private_path)
            .expect("write issuer private key");
        backup_signer
            .write_public_key(&issuer_public_path)
            .expect("write issuer public key");

        let config = format!(
            r#"
[server]
data_dir = "{data_dir}"

[audit]
path = "{audit_path}"

[auth]
revocation_path = "{revocation_path}"

[[auth.issuers]]
key_id = "dev"
public_key_path = "{issuer_public_path}"
"#,
            data_dir = toml_path(&root.join("var/data-missing")),
            audit_path = toml_path(&audit_path),
            revocation_path = toml_path(&revocation_path),
            issuer_public_path = toml_path(&issuer_public_path),
        );
        fs::write(&config_path, config).expect("write config");

        let error = create_runtime_backup(BackupRuntimeOptions {
            config: config_path.clone(),
            output_dir: root.join("var/agent/backups"),
            backup_id: Some("missing-required".to_owned()),
            config_audit_log: root.join("var/agent/config-audit/entries.jsonl"),
            orchestrator_state: root.join("var/orchestrator/state.json"),
            additional_state: Vec::new(),
            overwrite: false,
            confirm_writers_stopped: true,
            signing_private_key: issuer_private_path,
            signing_key_id: "runtime-backup".to_owned(),
        })
        .expect_err("incomplete required state must fail backup creation");
        assert!(error.to_string().contains("required backup target"));
        assert!(error.to_string().contains("server_data"));
    }

    #[test]
    fn normalize_runtime_backup_id_rejects_unsupported_characters() {
        let error = normalize_runtime_backup_id("bad/id").expect_err("invalid backup id");
        assert!(error.to_string().contains("unsupported characters"));
    }

    #[test]
    fn resolve_optional_token_without_inputs_returns_none() {
        let result = resolve_optional_token(TokenArgs {
            token: None,
            token_file: None,
        })
        .expect("optional token");
        assert!(result.is_none());
    }

    #[test]
    fn audit_export_is_verified_and_atomically_replaced() {
        use expressways_audit::{AuditDecision, AuditOutcome, AuditSink, DraftAuditEvent};

        let root = std::env::temp_dir().join(format!("expressways-export-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create root");
        let audit_path = root.join("audit.jsonl");
        let output_path = root.join("export.json");
        let mut sink = AuditSink::new(&audit_path).expect("create audit sink");
        sink.append(DraftAuditEvent {
            principal: "local:developer".to_owned(),
            action: Action::Admin,
            resource: "system:broker".to_owned(),
            decision: AuditDecision::Allow,
            outcome: AuditOutcome::Succeeded,
            detail: None,
        })
        .expect("append audit event");

        assert_eq!(
            export_verified_audit(&audit_path, &output_path).expect("export audit"),
            1
        );
        let exported: serde_json::Value =
            serde_json::from_slice(&fs::read(&output_path).expect("read exported audit"))
                .expect("parse exported audit");
        assert_eq!(exported["events"].as_array().map(Vec::len), Some(1));
        assert_eq!(exported["verification"]["event_count"], 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&output_path)
                    .expect("export metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        fs::write(&output_path, b"preserve-me").expect("write sentinel");
        fs::write(&audit_path, b"not-json\n").expect("tamper audit");
        assert!(export_verified_audit(&audit_path, &output_path).is_err());
        assert_eq!(
            fs::read(&output_path).expect("read sentinel"),
            b"preserve-me"
        );
        assert_eq!(
            fs::read_dir(&root)
                .expect("read root")
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
                .count(),
            0
        );
    }

    #[test]
    fn audit_export_rejects_overwriting_source() {
        let root = std::env::temp_dir().join(format!("expressways-export-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create root");
        let audit_path = root.join("audit.jsonl");
        fs::write(&audit_path, b"").expect("write audit");
        let error = export_verified_audit(&audit_path, &audit_path)
            .expect_err("source replacement should fail");
        assert!(error.to_string().contains("must not replace"));
    }
}
