use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_client::{Client, Endpoint};
use expressways_nanobot_system::bootstrap::{CreateSystemConfig, create_system};
use expressways_nanobot_system::model::{
    DEFAULT_INBOUND_TOPIC, DEFAULT_OUTBOUND_STREAM_TOPIC, DEFAULT_OUTBOUND_TOPIC,
    DEFAULT_RUNTIME_AGENT_ID, DEFAULT_RUNTIME_EVENTS_TOPIC, DEFAULT_RUNTIME_INSTANCE_ID,
    DEFAULT_RUNTIME_SOURCE, NanobotInboundEnvelope, NanobotMessageRef, NanobotRuntimeEvent,
    NanobotSessionRef,
};
use expressways_nanobot_system::provider::{
    AnthropicConfig, GeminiConfig, OpenAiConfig, ProviderBackend, ProviderRequest,
    ProviderRetryConfig, ProviderStep, anthropic_next_step, gemini_next_step,
    openai_compatible_next_step, openai_next_step,
};
use expressways_nanobot_system::runtime::{
    InteropWorkerConfig, RuntimeConfig, run_interop_worker, run_runtime,
};
use expressways_nanobot_system::tools::publish_json;
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse, INTEROP_CHAT_REPLIES_TOPIC,
    INTEROP_CHAT_REQUESTS_TOPIC, INTEROP_CHAT_RESULTS_TOPIC, RetentionClass,
};
use reqwest::Client as HttpClient;
use serde::Serialize;
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(about = "Nanobot-style runtime and bootstrap tooling on Expressways")]
struct Cli {
    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    transport: TransportKind,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransportKind {
    Tcp,
    Unix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProviderKind {
    RuleBased,
    #[value(name = "openai")]
    OpenAi,
    /// Local or self-hosted OpenAI-compatible server (Ollama, LM Studio, vLLM).
    #[value(name = "openai-compatible")]
    OpenAiCompatible,
    #[value(name = "anthropic")]
    Anthropic,
    #[value(name = "gemini")]
    Gemini,
}

const OPENAI_API_KEY_ENV: &str = "OPENAI_API_KEY";
const OPENAI_COMPATIBLE_API_KEY_ENV: &str = "OPENAI_COMPATIBLE_API_KEY";
const ANTHROPIC_API_KEY_ENV: &str = "ANTHROPIC_API_KEY";
const GEMINI_API_KEY_ENV: &str = "GEMINI_API_KEY";
const DEFAULT_PROBE_PROMPT: &str = "Reply with the single word OK.";

/// Model provider selection shared by `run-runtime` and `probe-provider`.
#[derive(Debug, Args, Clone)]
struct ProviderArgs {
    #[arg(long, value_enum, default_value_t = ProviderKind::RuleBased)]
    provider: ProviderKind,
    /// Provider API base URL. Required for `openai-compatible`
    /// (for example http://127.0.0.1:11434 for Ollama).
    #[arg(long = "provider-base-url")]
    provider_base_url: Option<String>,
    #[arg(long = "provider-model")]
    provider_model: Option<String>,
    /// API key. Defaults to OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY
    /// or OPENAI_COMPATIBLE_API_KEY depending on --provider.
    #[arg(long = "provider-api-key")]
    provider_api_key: Option<String>,
    #[arg(long = "provider-timeout-seconds", default_value_t = 60)]
    provider_timeout_seconds: u64,
    #[arg(long = "provider-max-attempts", default_value_t = 3)]
    provider_max_attempts: usize,
    #[arg(long = "provider-base-backoff-ms", default_value_t = 200)]
    provider_base_backoff_ms: u64,
    #[arg(long = "provider-max-backoff-ms", default_value_t = 2_000)]
    provider_max_backoff_ms: u64,
    #[arg(long = "provider-jitter-ms", default_value_t = 75)]
    provider_jitter_ms: u64,
    #[arg(long = "provider-system-prompt")]
    provider_system_prompt: Option<String>,
    #[arg(long = "anthropic-api-version", default_value = "2023-06-01")]
    anthropic_api_version: String,
}

#[derive(Debug, Args, Clone)]
struct TokenArgs {
    #[arg(long, conflicts_with = "token_file")]
    token: Option<String>,
    #[arg(long)]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)] // Clap constructs one short-lived command; boxing harms parsing ergonomics.
enum Command {
    CreateSystem {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "nanobot")]
        topic_prefix: String,
        #[arg(long, default_value = "./var/agent/nanobot-system")]
        output_dir: PathBuf,
        #[arg(long, default_value = "local:nanobot-runtime")]
        runtime_principal: String,
        #[arg(long, default_value = "local:nanobot-bridge")]
        bridge_principal: String,
    },
    RunRuntime {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = DEFAULT_RUNTIME_AGENT_ID)]
        agent_id: String,
        #[arg(long, default_value = "Nanobot Runtime (Expressways)")]
        display_name: String,
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        version: String,
        #[arg(
            long,
            default_value = "Nanobot-style agent loop with tools, memory, cron, and events"
        )]
        summary: String,
        #[arg(long, default_value = "local_nanobot_worker")]
        endpoint_transport: String,
        #[arg(long, default_value = "127.0.0.1:9910")]
        endpoint_address: String,
        #[arg(long, default_value = "internal")]
        classification: Classification,
        #[arg(long, default_value = "operational")]
        retention_class: RetentionClass,
        #[arg(long, default_value_t = 120)]
        ttl_seconds: u64,
        #[arg(long, default_value = "./var/agent/nanobot-runtime")]
        state_dir: PathBuf,
        #[arg(long, default_value = DEFAULT_INBOUND_TOPIC)]
        inbound_topic: String,
        #[arg(long, default_value = DEFAULT_OUTBOUND_TOPIC)]
        outbound_topic: String,
        #[arg(long, default_value = DEFAULT_OUTBOUND_STREAM_TOPIC)]
        outbound_stream_topic: String,
        #[arg(long, default_value = DEFAULT_RUNTIME_EVENTS_TOPIC)]
        runtime_events_topic: String,
        /// Consume orchestrated interop.chat.handoff tasks and publish
        /// correlated interop.chat.reply.v1 responses.
        #[arg(long, default_value_t = false)]
        interop_worker: bool,
        #[arg(long, default_value = INTEROP_CHAT_REQUESTS_TOPIC)]
        tasks_topic: String,
        #[arg(long, default_value = INTEROP_CHAT_RESULTS_TOPIC)]
        task_events_topic: String,
        #[arg(long, default_value = INTEROP_CHAT_REPLIES_TOPIC)]
        replies_topic: String,
        #[arg(long, default_value_t = 50)]
        batch_limit: usize,
        #[arg(long, default_value_t = 500)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 30)]
        heartbeat_interval_seconds: u64,
        #[arg(long, default_value_t = 48)]
        max_session_turns: usize,
        #[arg(long, default_value_t = 4)]
        max_tool_steps: usize,
        #[arg(long, default_value_t = 12)]
        memory_entries: usize,
        #[arg(long = "workspace-root", default_value = ".")]
        workspace_roots: Vec<PathBuf>,
        #[arg(long = "allow-exec-program")]
        allowed_exec_programs: Vec<String>,
        #[arg(long, default_value_t = 8192)]
        max_exec_output_bytes: usize,
        #[arg(long, default_value_t = true)]
        ensure_topics: bool,
        #[arg(long, default_value = DEFAULT_RUNTIME_INSTANCE_ID)]
        instance_id: String,
        #[arg(long, default_value = DEFAULT_RUNTIME_SOURCE)]
        source_runtime: String,
        #[command(flatten)]
        provider_args: ProviderArgs,
        #[arg(long = "provider-circuit-failure-threshold", default_value_t = 3)]
        provider_circuit_failure_threshold: usize,
        #[arg(long = "provider-circuit-cooldown-seconds", default_value_t = 30)]
        provider_circuit_cooldown_seconds: u64,
        #[arg(long = "provider-failover", default_value_t = false)]
        provider_failover: bool,
        #[arg(long = "provider-streaming", default_value_t = false)]
        provider_streaming: bool,
        #[arg(long = "fallback-provider-base-url")]
        fallback_provider_base_url: Option<String>,
        #[arg(long = "fallback-provider-model")]
        fallback_provider_model: Option<String>,
        #[arg(long = "fallback-provider-api-key")]
        fallback_provider_api_key: Option<String>,
        #[arg(long, default_value_t = false)]
        once: bool,
    },
    /// Send one test prompt to the configured model provider and exit.
    ///
    /// Prints a JSON result. Exits 0 on success, 1 on a configuration error,
    /// and a distinct code per failure kind: 10 authentication_failed,
    /// 11 host_unreachable, 12 model_not_found, 13 rate_limited, 14 timeout,
    /// 15 endpoint_not_found, 16 server_error, 17 request_rejected,
    /// 18 invalid_response.
    ProbeProvider {
        #[command(flatten)]
        provider_args: ProviderArgs,
        #[arg(long, default_value = DEFAULT_PROBE_PROMPT)]
        prompt: String,
    },
    Ingest {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = DEFAULT_INBOUND_TOPIC)]
        inbound_topic: String,
        #[arg(long, default_value = DEFAULT_RUNTIME_INSTANCE_ID)]
        instance_id: String,
        #[arg(long, default_value = "channel-bridge")]
        source_runtime: String,
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        channel: String,
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        sender_id: String,
        #[arg(long)]
        sender_display_name: Option<String>,
        #[arg(long)]
        text: String,
        #[arg(long)]
        message_id: Option<String>,
        #[arg(long, default_value = "{}")]
        metadata_json: String,
    },
    TailOutbound {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = DEFAULT_OUTBOUND_TOPIC)]
        outbound_topic: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = 1_000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = false)]
        raw: bool,
    },
    SummarizeProviderEvents {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = DEFAULT_RUNTIME_EVENTS_TOPIC)]
        runtime_events_topic: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 200)]
        limit: usize,
        #[arg(long)]
        session_id: Option<String>,
    },
    RunCron {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = DEFAULT_INBOUND_TOPIC)]
        inbound_topic: String,
        #[arg(long, default_value = DEFAULT_RUNTIME_INSTANCE_ID)]
        instance_id: String,
        #[arg(long, default_value = "nanobot-cron")]
        source_runtime: String,
        #[arg(long, default_value = "nanobot-cron")]
        session_id: String,
        #[arg(long, default_value = "system")]
        channel: String,
        #[arg(long, default_value = "local")]
        account_id: String,
        #[arg(long, default_value = "cron")]
        sender_id: String,
        #[arg(long, default_value = "Cron Trigger")]
        sender_display_name: String,
        #[arg(long)]
        text: String,
        #[arg(long, default_value_t = 60)]
        interval_seconds: u64,
        #[arg(long)]
        iterations: Option<usize>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;
    let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;

    match cli.command {
        Command::CreateSystem {
            token,
            topic_prefix,
            output_dir,
            runtime_principal,
            bridge_principal,
        } => {
            let capability_token = resolve_token(token)?;
            let output = create_system(CreateSystemConfig {
                endpoint,
                capability_token,
                topic_prefix,
                output_dir,
                runtime_principal,
                bridge_principal,
            })
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "created": {
                        "inbound_topic": output.inbound_topic,
                        "outbound_topic": output.outbound_topic,
                        "outbound_stream_topic": output.outbound_stream_topic,
                        "runtime_events_topic": output.runtime_events_topic,
                        "summary_path": output.summary_path,
                        "snippets_path": output.snippets_path,
                    }
                }))?
            );
            Ok(())
        }
        Command::RunRuntime {
            token,
            agent_id,
            display_name,
            version,
            summary,
            endpoint_transport,
            endpoint_address,
            classification,
            retention_class,
            ttl_seconds,
            state_dir,
            inbound_topic,
            outbound_topic,
            outbound_stream_topic,
            runtime_events_topic,
            interop_worker,
            tasks_topic,
            task_events_topic,
            replies_topic,
            batch_limit,
            poll_interval_ms,
            heartbeat_interval_seconds,
            max_session_turns,
            max_tool_steps,
            memory_entries,
            workspace_roots,
            allowed_exec_programs,
            max_exec_output_bytes,
            ensure_topics,
            instance_id,
            source_runtime,
            provider_args,
            provider_circuit_failure_threshold,
            provider_circuit_cooldown_seconds,
            provider_failover,
            provider_streaming,
            fallback_provider_base_url,
            fallback_provider_model,
            fallback_provider_api_key,
            once,
        } => {
            let capability_token = resolve_token(token)?;
            let env = |name: &str| std::env::var(name).ok();
            let provider = build_provider(&provider_args, &env)?;
            let fallback_provider = if provider_failover {
                Some(build_fallback_provider(
                    &provider_args,
                    FallbackProviderArgs {
                        base_url: fallback_provider_base_url,
                        model: fallback_provider_model,
                        api_key: fallback_provider_api_key,
                    },
                    &env,
                )?)
            } else {
                None
            };
            let worker_state_path = state_dir.join("interop-worker-state.json");
            let runtime = RuntimeConfig {
                endpoint,
                capability_token,
                agent_id,
                display_name,
                version,
                summary,
                endpoint_transport,
                endpoint_address,
                classification,
                retention_class,
                ttl_seconds,
                state_path: state_dir.join("runtime-state.json"),
                session_dir: state_dir.join("sessions"),
                memory_dir: state_dir.join("memory"),
                inbound_topic,
                outbound_topic,
                outbound_stream_topic,
                runtime_events_topic,
                poll_interval: Duration::from_millis(poll_interval_ms.max(1)),
                heartbeat_interval: Duration::from_secs(heartbeat_interval_seconds.max(1)),
                batch_limit: batch_limit.max(1),
                once,
                max_session_turns: max_session_turns.max(1),
                max_tool_steps: max_tool_steps.max(1),
                memory_entries: memory_entries.max(1),
                workspace_roots,
                allowed_exec_programs,
                max_exec_output_bytes,
                ensure_topics,
                instance_id,
                source_runtime,
                provider,
                fallback_provider,
                provider_streaming_enabled: provider_streaming,
                provider_circuit_failure_threshold: provider_circuit_failure_threshold.max(1),
                provider_circuit_cooldown: Duration::from_secs(
                    provider_circuit_cooldown_seconds.max(1),
                ),
            };
            if interop_worker {
                run_interop_worker(InteropWorkerConfig {
                    runtime,
                    worker_state_path,
                    tasks_topic,
                    task_events_topic,
                    replies_topic,
                })
                .await
            } else {
                run_runtime(runtime).await
            }
        }
        Command::ProbeProvider {
            provider_args,
            prompt,
        } => {
            let env = |name: &str| std::env::var(name).ok();
            let provider = build_provider(&provider_args, &env)?;
            let (result, exit_code) = probe_provider(&provider, &prompt).await;
            println!("{}", serde_json::to_string_pretty(&result)?);
            if exit_code != 0 {
                std::process::exit(i32::from(exit_code));
            }
            Ok(())
        }
        Command::Ingest {
            token,
            inbound_topic,
            instance_id,
            source_runtime,
            session_id,
            channel,
            account_id,
            sender_id,
            sender_display_name,
            text,
            message_id,
            metadata_json,
        } => {
            let capability_token = resolve_token(token)?;
            let metadata = serde_json::from_str::<serde_json::Value>(&metadata_json)
                .context("failed to parse --metadata-json")?;
            if !metadata.is_object() {
                bail!("--metadata-json must be a JSON object");
            }
            let payload = NanobotInboundEnvelope {
                schema_version: expressways_nanobot_system::model::SYSTEM_SCHEMA_VERSION.to_owned(),
                source_runtime,
                instance_id,
                session: NanobotSessionRef {
                    session_id,
                    channel,
                    account_id,
                    sender_id,
                    sender_display_name,
                },
                message: NanobotMessageRef {
                    message_id: Some(message_id.unwrap_or_else(|| Uuid::now_v7().to_string())),
                    role: Some("user".to_owned()),
                    text: Some(text),
                    attachments: Vec::new(),
                    content: Vec::new(),
                },
                metadata,
                received_at: Utc::now(),
            };
            publish_json(
                &endpoint,
                &capability_token,
                &inbound_topic,
                Classification::Internal,
                &payload,
            )
            .await
            .map_err(anyhow::Error::msg)?;
            info!(topic = %inbound_topic, "ingress message published");
            Ok(())
        }
        Command::TailOutbound {
            token,
            outbound_topic,
            mut offset,
            limit,
            follow,
            poll_interval_ms,
            raw,
        } => {
            let capability_token = resolve_token(token)?;
            loop {
                let messages = consume_messages(
                    &endpoint,
                    &capability_token,
                    &outbound_topic,
                    offset,
                    limit.max(1),
                )
                .await?;
                if messages.is_empty() && !follow {
                    break;
                }

                for message in messages {
                    offset = offset.max(message.offset.saturating_add(1));
                    if raw {
                        println!("{}", message.payload);
                        continue;
                    }
                    match serde_json::from_str::<serde_json::Value>(&message.payload) {
                        Ok(value) => println!("{}", serde_json::to_string_pretty(&value)?),
                        Err(_) => println!("{}", message.payload),
                    }
                }

                if !follow {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(poll_interval_ms.max(1))).await;
            }
            Ok(())
        }
        Command::SummarizeProviderEvents {
            token,
            runtime_events_topic,
            offset,
            limit,
            session_id,
        } => {
            let capability_token = resolve_token(token)?;
            let messages = consume_messages(
                &endpoint,
                &capability_token,
                &runtime_events_topic,
                offset,
                limit.max(1),
            )
            .await?;
            let summary = summarize_provider_events(
                &runtime_events_topic,
                offset,
                session_id.as_deref(),
                &messages,
            );
            println!("{}", serde_json::to_string_pretty(&summary)?);
            Ok(())
        }
        Command::RunCron {
            token,
            inbound_topic,
            instance_id,
            source_runtime,
            session_id,
            channel,
            account_id,
            sender_id,
            sender_display_name,
            text,
            interval_seconds,
            iterations,
        } => {
            let capability_token = resolve_token(token)?;
            let mut remaining = iterations.unwrap_or(usize::MAX);
            let mut tick = 0usize;
            while remaining > 0 {
                tick += 1;
                remaining = remaining.saturating_sub(1);
                let payload = NanobotInboundEnvelope {
                    schema_version: expressways_nanobot_system::model::SYSTEM_SCHEMA_VERSION
                        .to_owned(),
                    source_runtime: source_runtime.clone(),
                    instance_id: instance_id.clone(),
                    session: NanobotSessionRef {
                        session_id: session_id.clone(),
                        channel: channel.clone(),
                        account_id: account_id.clone(),
                        sender_id: sender_id.clone(),
                        sender_display_name: Some(sender_display_name.clone()),
                    },
                    message: NanobotMessageRef {
                        message_id: Some(Uuid::now_v7().to_string()),
                        role: Some("user".to_owned()),
                        text: Some(text.clone()),
                        attachments: Vec::new(),
                        content: Vec::new(),
                    },
                    metadata: serde_json::json!({
                        "scheduled": true,
                        "tick": tick,
                        "interval_seconds": interval_seconds,
                    }),
                    received_at: Utc::now(),
                };

                publish_json(
                    &endpoint,
                    &capability_token,
                    &inbound_topic,
                    Classification::Internal,
                    &payload,
                )
                .await
                .map_err(anyhow::Error::msg)?;

                info!(
                    tick,
                    topic = %inbound_topic,
                    session_id = %session_id,
                    "cron message published"
                );

                if remaining == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(interval_seconds.max(1))).await;
            }
            Ok(())
        }
    }
}

const PROVIDER_RUNTIME_EVENT_NAMES: [&str; 8] = [
    "provider_error",
    "provider_retry_recovered",
    "provider_circuit_open",
    "provider_circuit_blocked",
    "provider_circuit_closed",
    "provider_failover_attempt",
    "provider_failover_succeeded",
    "provider_failover_failed",
];

#[derive(Debug, Clone, Serialize)]
struct ProviderErrorSnapshot {
    offset: u64,
    timestamp: DateTime<Utc>,
    event: String,
    session_id: Option<String>,
    message_id: Option<String>,
    error: String,
    error_kind: Option<String>,
    model: Option<String>,
    api_base: Option<String>,
    attempts: Option<u64>,
    elapsed_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct ProviderEventsSummary {
    topic: String,
    offset_start: u64,
    offset_next: u64,
    messages_examined: usize,
    provider_events_matched: usize,
    parse_failures: usize,
    session_filter: Option<String>,
    counts_by_event: BTreeMap<String, u64>,
    counts_by_provider: BTreeMap<String, u64>,
    latest_error_by_provider: BTreeMap<String, ProviderErrorSnapshot>,
}

fn summarize_provider_events(
    topic: &str,
    offset_start: u64,
    session_filter: Option<&str>,
    messages: &[expressways_protocol::StoredMessage],
) -> ProviderEventsSummary {
    let mut counts_by_event: BTreeMap<String, u64> = BTreeMap::new();
    let mut counts_by_provider: BTreeMap<String, u64> = BTreeMap::new();
    let mut latest_error_by_provider: BTreeMap<String, ProviderErrorSnapshot> = BTreeMap::new();
    let mut provider_events_matched = 0usize;
    let mut parse_failures = 0usize;
    let mut offset_next = offset_start;

    for message in messages {
        offset_next = offset_next.max(message.offset.saturating_add(1));
        let event = match serde_json::from_str::<NanobotRuntimeEvent>(&message.payload) {
            Ok(event) => event,
            Err(_) => {
                parse_failures = parse_failures.saturating_add(1);
                continue;
            }
        };

        if let Some(session_filter) = session_filter
            && event.session_id.as_deref() != Some(session_filter)
        {
            continue;
        }
        if !is_provider_runtime_event(&event.event) {
            continue;
        }

        provider_events_matched = provider_events_matched.saturating_add(1);
        *counts_by_event.entry(event.event.clone()).or_insert(0) += 1;

        let providers = provider_names_for_event_detail(&event.detail);
        for provider in providers {
            *counts_by_provider.entry(provider).or_insert(0) += 1;
        }

        if event.event == "provider_error" {
            let Some(provider) = event
                .detail
                .get("provider")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
            else {
                continue;
            };
            let snapshot = ProviderErrorSnapshot {
                offset: message.offset,
                timestamp: event.timestamp,
                event: event.event.clone(),
                session_id: event.session_id.clone(),
                message_id: event.message_id.clone(),
                error: event
                    .detail
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                error_kind: event
                    .detail
                    .get("error_kind")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                model: event
                    .detail
                    .get("model")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                api_base: event
                    .detail
                    .get("api_base")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                attempts: event
                    .detail
                    .get("attempts")
                    .and_then(serde_json::Value::as_u64),
                elapsed_ms: event
                    .detail
                    .get("elapsed_ms")
                    .and_then(serde_json::Value::as_u64),
            };

            match latest_error_by_provider.get(&provider) {
                Some(existing) if existing.offset >= snapshot.offset => {}
                _ => {
                    latest_error_by_provider.insert(provider, snapshot);
                }
            }
        }
    }

    ProviderEventsSummary {
        topic: topic.to_owned(),
        offset_start,
        offset_next,
        messages_examined: messages.len(),
        provider_events_matched,
        parse_failures,
        session_filter: session_filter.map(str::to_owned),
        counts_by_event,
        counts_by_provider,
        latest_error_by_provider,
    }
}

fn is_provider_runtime_event(event: &str) -> bool {
    PROVIDER_RUNTIME_EVENT_NAMES
        .iter()
        .any(|known| known == &event)
}

fn provider_names_for_event_detail(detail: &serde_json::Value) -> BTreeSet<String> {
    let mut providers = BTreeSet::new();
    if let Some(provider) = detail.get("provider").and_then(serde_json::Value::as_str) {
        providers.insert(provider.to_owned());
    }
    if let Some(provider) = detail
        .get("from_provider")
        .and_then(serde_json::Value::as_str)
    {
        providers.insert(provider.to_owned());
    }
    if let Some(provider) = detail
        .get("to_provider")
        .and_then(serde_json::Value::as_str)
    {
        providers.insert(provider.to_owned());
    }
    providers
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
                bail!("unix transport is unsupported on this platform")
            }
        }
    }
}

fn resolve_token(args: TokenArgs) -> anyhow::Result<String> {
    if let Some(token) = args.token {
        return expressways_client::normalize_capability_token(&token);
    }
    let token_file = args
        .token_file
        .context("provide --token or --token-file for authenticated commands")?;
    expressways_client::read_capability_token_file(&token_file)
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level)
        .or_else(|_| EnvFilter::try_new(format!("expressways_nanobot_system={log_level}")))
        .context("invalid log level")?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .init();
    Ok(())
}

fn build_http_client(timeout: Duration) -> anyhow::Result<HttpClient> {
    HttpClient::builder()
        .timeout(timeout.max(Duration::from_secs(1)))
        .build()
        .context("failed to build HTTP client")
}

fn non_empty_or_default(
    value: Option<String>,
    default: impl FnOnce() -> String,
    field_name: &str,
) -> anyhow::Result<String> {
    let resolved = value.unwrap_or_else(default);
    let trimmed = resolved.trim();
    if trimmed.is_empty() {
        bail!("{field_name} must not be empty");
    }
    Ok(trimmed.to_owned())
}

struct ProviderSettings {
    timeout: Duration,
    retry: ProviderRetryConfig,
    system_prompt: Option<String>,
    anthropic_api_version: String,
}

impl ProviderSettings {
    fn from_args(args: &ProviderArgs) -> Self {
        Self {
            timeout: Duration::from_secs(args.provider_timeout_seconds.max(1)),
            retry: ProviderRetryConfig {
                max_attempts: args.provider_max_attempts,
                base_backoff_ms: args.provider_base_backoff_ms,
                max_backoff_ms: args.provider_max_backoff_ms,
                jitter_ms: args.provider_jitter_ms,
            }
            .normalized(),
            system_prompt: args
                .provider_system_prompt
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
            anthropic_api_version: if args.anthropic_api_version.trim().is_empty() {
                AnthropicConfig::default_api_version()
            } else {
                args.anthropic_api_version.clone()
            },
        }
    }
}

/// Values and flag names for one provider slot (primary or fallback), so
/// error messages name the flag the operator actually has to set.
struct ProviderFlags {
    base_url: Option<String>,
    model: Option<String>,
    api_key: Option<String>,
    flag_prefix: &'static str,
    role: &'static str,
}

impl ProviderFlags {
    fn primary(args: &ProviderArgs) -> Self {
        Self {
            base_url: args.provider_base_url.clone(),
            model: args.provider_model.clone(),
            api_key: args.provider_api_key.clone(),
            flag_prefix: "provider",
            role: "provider",
        }
    }

    fn flag(&self, name: &str) -> String {
        format!("--{}-{name}", self.flag_prefix)
    }
}

struct FallbackProviderArgs {
    base_url: Option<String>,
    model: Option<String>,
    api_key: Option<String>,
}

fn build_provider(
    args: &ProviderArgs,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<ProviderBackend> {
    let settings = ProviderSettings::from_args(args);
    let flags = ProviderFlags::primary(args);
    match args.provider {
        ProviderKind::RuleBased => Ok(ProviderBackend::RuleBased),
        ProviderKind::OpenAi => Ok(ProviderBackend::OpenAi(build_openai_config(
            &settings, flags, env,
        )?)),
        ProviderKind::OpenAiCompatible => Ok(ProviderBackend::OpenAiCompatible(
            build_openai_compatible_config(&settings, flags, env)?,
        )),
        ProviderKind::Anthropic => Ok(ProviderBackend::Anthropic(build_anthropic_config(
            &settings, flags, env,
        )?)),
        ProviderKind::Gemini => Ok(ProviderBackend::Gemini(build_gemini_config(
            &settings, flags, env,
        )?)),
    }
}

fn build_fallback_provider(
    args: &ProviderArgs,
    fallback: FallbackProviderArgs,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<ProviderBackend> {
    let settings = ProviderSettings::from_args(args);
    let flags = ProviderFlags {
        base_url: fallback.base_url,
        model: fallback.model,
        api_key: fallback.api_key,
        flag_prefix: "fallback-provider",
        role: "fallback",
    };
    match args.provider {
        ProviderKind::OpenAi => Ok(ProviderBackend::Anthropic(build_anthropic_config(
            &settings, flags, env,
        )?)),
        ProviderKind::Anthropic => Ok(ProviderBackend::OpenAi(build_openai_config(
            &settings, flags, env,
        )?)),
        ProviderKind::RuleBased | ProviderKind::OpenAiCompatible | ProviderKind::Gemini => {
            bail!("--provider-failover requires --provider openai or --provider anthropic")
        }
    }
}

fn resolve_api_key(
    flags: &ProviderFlags,
    env: &dyn Fn(&str) -> Option<String>,
    env_name: &str,
) -> Option<String> {
    flags
        .api_key
        .clone()
        .or_else(|| env(env_name))
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn require_api_key(
    flags: &ProviderFlags,
    env: &dyn Fn(&str) -> Option<String>,
    env_name: &str,
    label: &str,
) -> anyhow::Result<String> {
    match resolve_api_key(flags, env, env_name) {
        Some(key) => Ok(key),
        None => bail!(
            "{label} {} requires {} or {env_name}",
            flags.role,
            flags.flag("api-key")
        ),
    }
}

fn resolve_base_url(
    flags: &ProviderFlags,
    default: impl FnOnce() -> String,
) -> anyhow::Result<String> {
    let flag = flags.flag("base-url");
    let base_url = non_empty_or_default(flags.base_url.clone(), default, &flag)?;
    validate_base_url(&base_url, &flag)?;
    Ok(base_url)
}

fn validate_base_url(base_url: &str, flag: &str) -> anyhow::Result<()> {
    let parsed = reqwest::Url::parse(base_url).ok();
    let valid = parsed
        .as_ref()
        .is_some_and(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some());
    if !valid {
        bail!(
            "{flag} must be an http:// or https:// URL with a host (for example http://127.0.0.1:11434), got `{base_url}`"
        );
    }
    Ok(())
}

fn build_openai_config(
    settings: &ProviderSettings,
    flags: ProviderFlags,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<OpenAiConfig> {
    let api_base = resolve_base_url(&flags, OpenAiConfig::default_api_base)?;
    let model = non_empty_or_default(
        flags.model.clone(),
        || "gpt-4o-mini".to_owned(),
        &flags.flag("model"),
    )?;
    let api_key = require_api_key(&flags, env, OPENAI_API_KEY_ENV, "OpenAI")?;
    Ok(OpenAiConfig {
        api_base,
        model,
        api_key: Some(api_key),
        timeout: settings.timeout,
        retry: settings.retry,
        system_prompt: settings
            .system_prompt
            .clone()
            .unwrap_or_else(OpenAiConfig::default_system_prompt),
        http_client: build_http_client(settings.timeout)
            .context("failed to build OpenAI provider HTTP client")?,
    })
}

fn build_openai_compatible_config(
    settings: &ProviderSettings,
    flags: ProviderFlags,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<OpenAiConfig> {
    let base_url_flag = flags.flag("base-url");
    let model_flag = flags.flag("model");
    if flags
        .base_url
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
    {
        bail!(
            "OpenAI-compatible provider requires {base_url_flag} with the server's host and port (for example http://127.0.0.1:11434)"
        );
    }
    if flags
        .model
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
    {
        bail!("OpenAI-compatible provider requires {model_flag} with the server's model name");
    }
    let api_base = resolve_base_url(&flags, String::new)?;
    let model = non_empty_or_default(flags.model.clone(), String::new, &model_flag)?;
    Ok(OpenAiConfig {
        api_base,
        model,
        api_key: resolve_api_key(&flags, env, OPENAI_COMPATIBLE_API_KEY_ENV),
        timeout: settings.timeout,
        retry: settings.retry,
        system_prompt: settings
            .system_prompt
            .clone()
            .unwrap_or_else(OpenAiConfig::default_system_prompt),
        http_client: build_http_client(settings.timeout)
            .context("failed to build OpenAI-compatible provider HTTP client")?,
    })
}

fn build_anthropic_config(
    settings: &ProviderSettings,
    flags: ProviderFlags,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<AnthropicConfig> {
    let api_base = resolve_base_url(&flags, AnthropicConfig::default_api_base)?;
    let model = non_empty_or_default(
        flags.model.clone(),
        || "claude-3-5-sonnet-latest".to_owned(),
        &flags.flag("model"),
    )?;
    let api_key = require_api_key(&flags, env, ANTHROPIC_API_KEY_ENV, "Anthropic")?;
    Ok(AnthropicConfig {
        api_base,
        model,
        api_key: Some(api_key),
        timeout: settings.timeout,
        retry: settings.retry,
        system_prompt: settings
            .system_prompt
            .clone()
            .unwrap_or_else(AnthropicConfig::default_system_prompt),
        api_version: settings.anthropic_api_version.clone(),
        http_client: build_http_client(settings.timeout)
            .context("failed to build Anthropic provider HTTP client")?,
    })
}

fn build_gemini_config(
    settings: &ProviderSettings,
    flags: ProviderFlags,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<GeminiConfig> {
    let api_base = resolve_base_url(&flags, GeminiConfig::default_api_base)?;
    let model = non_empty_or_default(
        flags.model.clone(),
        GeminiConfig::default_model,
        &flags.flag("model"),
    )?;
    let api_key = require_api_key(&flags, env, GEMINI_API_KEY_ENV, "Gemini")?;
    Ok(GeminiConfig {
        api_base,
        model,
        api_key: Some(api_key),
        timeout: settings.timeout,
        retry: settings.retry,
        system_prompt: settings
            .system_prompt
            .clone()
            .unwrap_or_else(GeminiConfig::default_system_prompt),
        http_client: build_http_client(settings.timeout)
            .context("failed to build Gemini provider HTTP client")?,
    })
}

/// Run one provider call for `probe-provider`. Returns the JSON report and
/// the process exit code (0 on success, the error kind's code otherwise).
async fn probe_provider(provider: &ProviderBackend, prompt: &str) -> (serde_json::Value, u8) {
    let request = ProviderRequest {
        latest_user_text: prompt.to_owned(),
        history: Vec::new(),
        memory_summary: None,
        pending_tool_result: None,
    };
    let (name, model, api_base, result) = match provider {
        ProviderBackend::RuleBased => {
            return (
                serde_json::json!({
                    "ok": true,
                    "provider": "rule-based",
                    "detail": "the rule-based provider has no model endpoint to test",
                }),
                0,
            );
        }
        ProviderBackend::OpenAi(config) => (
            "openai",
            &config.model,
            &config.api_base,
            openai_next_step(&request, config).await,
        ),
        ProviderBackend::OpenAiCompatible(config) => (
            "openai-compatible",
            &config.model,
            &config.api_base,
            openai_compatible_next_step(&request, config).await,
        ),
        ProviderBackend::Anthropic(config) => (
            "anthropic",
            &config.model,
            &config.api_base,
            anthropic_next_step(&request, config).await,
        ),
        ProviderBackend::Gemini(config) => (
            "gemini",
            &config.model,
            &config.api_base,
            gemini_next_step(&request, config).await,
        ),
    };
    match result {
        Ok(invocation) => {
            let reply = match invocation.step {
                ProviderStep::Respond { text } => serde_json::json!({ "text": text }),
                ProviderStep::ToolCalls { calls } => serde_json::json!({
                    "tool_calls": calls.into_iter().map(|call| call.name).collect::<Vec<_>>()
                }),
            };
            (
                serde_json::json!({
                    "ok": true,
                    "provider": name,
                    "model": model,
                    "api_base": api_base,
                    "attempts": invocation.stats.attempts,
                    "elapsed_ms": invocation.stats.elapsed_ms,
                    "reply": reply,
                }),
                0,
            )
        }
        Err(error) => (
            serde_json::json!({
                "ok": false,
                "provider": name,
                "model": model,
                "api_base": api_base,
                "error_kind": error.kind.as_str(),
                "exit_code": error.kind.exit_code(),
                "hint": error.kind.hint(),
                "error": error.message,
                "attempts": error.stats.attempts,
                "elapsed_ms": error.stats.elapsed_ms,
            }),
            error.kind.exit_code(),
        ),
    }
}

async fn consume_messages(
    endpoint: &Endpoint,
    capability_token: &str,
    topic: &str,
    offset: u64,
    limit: usize,
) -> anyhow::Result<Vec<expressways_protocol::StoredMessage>> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::Consume {
                topic: topic.to_owned(),
                offset,
                limit,
            },
        })
        .await?;
    match response {
        ControlResponse::Messages { messages, .. } => Ok(messages),
        ControlResponse::Error { code, message } => {
            bail!("consume failed for topic `{topic}`: {code}: {message}")
        }
        other => bail!("unexpected consume response for topic `{topic}`: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expressways_protocol::{Classification, StoredMessage};
    use uuid::Uuid;

    fn stored_message(offset: u64, payload: serde_json::Value) -> StoredMessage {
        StoredMessage {
            message_id: Uuid::now_v7(),
            topic: DEFAULT_RUNTIME_EVENTS_TOPIC.to_owned(),
            offset,
            timestamp: Utc::now(),
            producer: "test".to_owned(),
            classification: Classification::Internal,
            payload: payload.to_string(),
        }
    }

    #[test]
    fn provider_event_summary_counts_events_and_latest_errors() {
        let provider_error_old = serde_json::json!({
            "schema_version": "nanobot.expressways.v1",
            "source_runtime": "expressways-nanobot-system",
            "instance_id": "i-1",
            "event": "provider_error",
            "session_id": "s-1",
            "message_id": "m-1",
            "detail": {
                "provider": "openai",
                "model": "gpt-4o-mini",
                "api_base": "https://api.openai.com/v1",
                "attempts": 3,
                "elapsed_ms": 801,
                "error": "HTTP 429"
            },
            "timestamp": Utc::now(),
        });
        let provider_error_new = serde_json::json!({
            "schema_version": "nanobot.expressways.v1",
            "source_runtime": "expressways-nanobot-system",
            "instance_id": "i-1",
            "event": "provider_error",
            "session_id": "s-1",
            "message_id": "m-2",
            "detail": {
                "provider": "openai",
                "model": "gpt-4o-mini",
                "api_base": "https://api.openai.com/v1",
                "attempts": 1,
                "elapsed_ms": 204,
                "error": "timeout",
                "error_kind": "timeout"
            },
            "timestamp": Utc::now(),
        });
        let failover_attempt = serde_json::json!({
            "schema_version": "nanobot.expressways.v1",
            "source_runtime": "expressways-nanobot-system",
            "instance_id": "i-1",
            "event": "provider_failover_attempt",
            "session_id": "s-1",
            "message_id": "m-3",
            "detail": {
                "from_provider": "openai",
                "to_provider": "anthropic",
                "from_error": "HTTP 500"
            },
            "timestamp": Utc::now(),
        });
        let non_provider_event = serde_json::json!({
            "schema_version": "nanobot.expressways.v1",
            "source_runtime": "expressways-nanobot-system",
            "instance_id": "i-1",
            "event": "message_processed",
            "session_id": "s-1",
            "message_id": "m-4",
            "detail": {},
            "timestamp": Utc::now(),
        });

        let messages = vec![
            stored_message(5, provider_error_old),
            stored_message(6, failover_attempt),
            stored_message(7, non_provider_event),
            stored_message(8, provider_error_new),
        ];
        let summary = summarize_provider_events(DEFAULT_RUNTIME_EVENTS_TOPIC, 5, None, &messages);
        assert_eq!(summary.provider_events_matched, 3);
        assert_eq!(summary.counts_by_event["provider_error"], 2);
        assert_eq!(summary.counts_by_event["provider_failover_attempt"], 1);
        assert_eq!(summary.counts_by_provider["openai"], 3);
        assert_eq!(summary.counts_by_provider["anthropic"], 1);
        assert_eq!(
            summary.latest_error_by_provider["openai"].error,
            "timeout".to_owned()
        );
        assert_eq!(summary.latest_error_by_provider["openai"].offset, 8);
        assert_eq!(
            summary.latest_error_by_provider["openai"]
                .error_kind
                .as_deref(),
            Some("timeout")
        );
        assert_eq!(summary.offset_next, 9);
    }

    #[test]
    fn provider_event_summary_respects_session_filter() {
        let event_for_session_a = serde_json::json!({
            "schema_version": "nanobot.expressways.v1",
            "source_runtime": "expressways-nanobot-system",
            "instance_id": "i-1",
            "event": "provider_error",
            "session_id": "a",
            "message_id": "m-1",
            "detail": {
                "provider": "openai",
                "error": "HTTP 500"
            },
            "timestamp": Utc::now(),
        });
        let event_for_session_b = serde_json::json!({
            "schema_version": "nanobot.expressways.v1",
            "source_runtime": "expressways-nanobot-system",
            "instance_id": "i-1",
            "event": "provider_error",
            "session_id": "b",
            "message_id": "m-2",
            "detail": {
                "provider": "anthropic",
                "error": "HTTP 503"
            },
            "timestamp": Utc::now(),
        });
        let messages = vec![
            stored_message(1, event_for_session_a),
            stored_message(2, event_for_session_b),
        ];
        let summary =
            summarize_provider_events(DEFAULT_RUNTIME_EVENTS_TOPIC, 1, Some("b"), &messages);
        assert_eq!(summary.provider_events_matched, 1);
        assert_eq!(summary.counts_by_provider["anthropic"], 1);
        assert!(!summary.counts_by_provider.contains_key("openai"));
        assert!(summary.latest_error_by_provider.contains_key("anthropic"));
        assert_eq!(summary.session_filter.as_deref(), Some("b"));
    }

    fn parse_provider_args(extra: &[&str]) -> ProviderArgs {
        let mut argv = vec!["expressways-nanobot-system", "probe-provider"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).expect("parse cli").command {
            Command::ProbeProvider { provider_args, .. } => provider_args,
            other => panic!("expected probe-provider, got {other:?}"),
        }
    }

    fn env_from(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    #[test]
    fn run_runtime_accepts_new_provider_names() {
        for provider in [
            "gemini",
            "openai-compatible",
            "openai",
            "anthropic",
            "rule-based",
        ] {
            let cli = Cli::try_parse_from([
                "expressways-nanobot-system",
                "run-runtime",
                "--token",
                "t",
                "--provider",
                provider,
            ])
            .unwrap_or_else(|error| panic!("parse {provider}: {error}"));
            assert!(matches!(cli.command, Command::RunRuntime { .. }));
        }
    }

    #[test]
    fn selects_gemini_with_env_key_and_defaults() {
        let args = parse_provider_args(&["--provider", "gemini"]);
        let provider = build_provider(&args, &env_from(&[("GEMINI_API_KEY", "g-key")]))
            .expect("gemini provider");
        let ProviderBackend::Gemini(config) = provider else {
            panic!("expected gemini backend");
        };
        assert_eq!(config.api_key.as_deref(), Some("g-key"));
        assert_eq!(config.model, GeminiConfig::default_model());
        assert_eq!(config.api_base, GeminiConfig::default_api_base());

        let args = parse_provider_args(&[
            "--provider",
            "gemini",
            "--provider-model",
            "gemini-2.5-pro",
            "--provider-api-key",
            "flag-key",
        ]);
        let ProviderBackend::Gemini(config) =
            build_provider(&args, &env_from(&[("GEMINI_API_KEY", "g-key")])).expect("gemini")
        else {
            panic!("expected gemini backend");
        };
        assert_eq!(config.api_key.as_deref(), Some("flag-key"));
        assert_eq!(config.model, "gemini-2.5-pro");
    }

    #[test]
    fn gemini_without_key_names_the_env_var() {
        let args = parse_provider_args(&["--provider", "gemini"]);
        let error = build_provider(&args, &env_from(&[("OPENAI_API_KEY", "o-key")]))
            .expect_err("missing gemini key");
        assert_eq!(
            error.to_string(),
            "Gemini provider requires --provider-api-key or GEMINI_API_KEY"
        );
    }

    #[test]
    fn selects_openai_compatible_with_optional_key() {
        let args = parse_provider_args(&[
            "--provider",
            "openai-compatible",
            "--provider-base-url",
            "http://127.0.0.1:11434",
            "--provider-model",
            "llama3.2",
        ]);
        let ProviderBackend::OpenAiCompatible(config) =
            build_provider(&args, &env_from(&[("OPENAI_API_KEY", "must-not-leak")]))
                .expect("openai-compatible provider")
        else {
            panic!("expected openai-compatible backend");
        };
        assert_eq!(config.api_base, "http://127.0.0.1:11434");
        assert_eq!(config.model, "llama3.2");
        assert_eq!(
            config.api_key, None,
            "OPENAI_API_KEY must not be sent to local servers"
        );

        let ProviderBackend::OpenAiCompatible(config) = build_provider(
            &args,
            &env_from(&[("OPENAI_COMPATIBLE_API_KEY", "local-key")]),
        )
        .expect("openai-compatible provider") else {
            panic!("expected openai-compatible backend");
        };
        assert_eq!(config.api_key.as_deref(), Some("local-key"));
    }

    #[test]
    fn openai_compatible_requires_base_url_and_model() {
        let no_env = env_from(&[]);
        let args = parse_provider_args(&[
            "--provider",
            "openai-compatible",
            "--provider-model",
            "llama3.2",
        ]);
        let error = build_provider(&args, &no_env).expect_err("missing base url");
        assert!(error.to_string().contains("--provider-base-url"), "{error}");

        let args = parse_provider_args(&[
            "--provider",
            "openai-compatible",
            "--provider-base-url",
            "http://127.0.0.1:1234",
        ]);
        let error = build_provider(&args, &no_env).expect_err("missing model");
        assert!(error.to_string().contains("--provider-model"), "{error}");

        let args = parse_provider_args(&[
            "--provider",
            "openai-compatible",
            "--provider-base-url",
            "localhost:11434",
            "--provider-model",
            "llama3.2",
        ]);
        let error = build_provider(&args, &no_env).expect_err("scheme-less url");
        assert!(error.to_string().contains("http:// or https://"), "{error}");
    }

    #[test]
    fn existing_providers_keep_their_key_env_vars() {
        let args = parse_provider_args(&["--provider", "openai"]);
        assert!(matches!(
            build_provider(&args, &env_from(&[("OPENAI_API_KEY", "o")])),
            Ok(ProviderBackend::OpenAi(_))
        ));
        let args = parse_provider_args(&["--provider", "anthropic"]);
        assert!(matches!(
            build_provider(&args, &env_from(&[("ANTHROPIC_API_KEY", "a")])),
            Ok(ProviderBackend::Anthropic(_))
        ));
        let error = build_provider(&args, &env_from(&[])).expect_err("missing anthropic key");
        assert_eq!(
            error.to_string(),
            "Anthropic provider requires --provider-api-key or ANTHROPIC_API_KEY"
        );
    }

    #[test]
    fn failover_is_limited_to_openai_and_anthropic() {
        let env = env_from(&[("OPENAI_API_KEY", "o"), ("ANTHROPIC_API_KEY", "a")]);
        let fallback = || FallbackProviderArgs {
            base_url: None,
            model: None,
            api_key: None,
        };
        let args = parse_provider_args(&["--provider", "openai"]);
        assert!(matches!(
            build_fallback_provider(&args, fallback(), &env),
            Ok(ProviderBackend::Anthropic(_))
        ));
        for provider in ["gemini", "openai-compatible", "rule-based"] {
            let args = parse_provider_args(&["--provider", provider]);
            let error =
                build_fallback_provider(&args, fallback(), &env).expect_err("unsupported failover");
            assert!(error.to_string().contains("--provider-failover"), "{error}");
        }
    }

    #[tokio::test]
    async fn probe_reports_unreachable_host_with_exit_code() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind port");
        let address = listener.local_addr().expect("local addr");
        drop(listener);
        let base_url = format!("http://{address}");
        let args = parse_provider_args(&[
            "--provider",
            "openai-compatible",
            "--provider-base-url",
            &base_url,
            "--provider-model",
            "llama3.2",
            "--provider-max-attempts",
            "1",
        ]);
        let provider = build_provider(&args, &env_from(&[])).expect("provider");
        let (report, exit_code) = probe_provider(&provider, DEFAULT_PROBE_PROMPT).await;
        assert_eq!(exit_code, 11);
        assert_eq!(report["ok"], false);
        assert_eq!(report["provider"], "openai-compatible");
        assert_eq!(report["error_kind"], "host_unreachable");
        assert_eq!(report["exit_code"], 11);
    }

    #[tokio::test]
    async fn probe_reports_success_and_auth_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let address = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let replies = [
                (
                    "200 OK",
                    r#"{"candidates":[{"content":{"parts":[{"text":"OK"}]}}]}"#,
                ),
                (
                    "401 Unauthorized",
                    r#"{"error":{"code":401,"status":"UNAUTHENTICATED"}}"#,
                ),
            ];
            for (status, body) in replies {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut buffer = vec![0u8; 65536];
                let mut read = 0;
                loop {
                    let count = socket.read(&mut buffer[read..]).await.expect("read");
                    read += count;
                    let text = String::from_utf8_lossy(&buffer[..read]);
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text[..end]
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|value| value.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        if read >= end + 4 + length {
                            break;
                        }
                    }
                    if count == 0 {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.expect("write");
                let _ = socket.shutdown().await;
            }
        });

        let base_url = format!("http://{address}");
        let args = parse_provider_args(&[
            "--provider",
            "gemini",
            "--provider-base-url",
            &base_url,
            "--provider-max-attempts",
            "1",
        ]);
        let provider =
            build_provider(&args, &env_from(&[("GEMINI_API_KEY", "g")])).expect("provider");

        let (report, exit_code) = probe_provider(&provider, DEFAULT_PROBE_PROMPT).await;
        assert_eq!(exit_code, 0, "{report}");
        assert_eq!(report["ok"], true);
        assert_eq!(report["reply"]["text"], "OK");

        let (report, exit_code) = probe_provider(&provider, DEFAULT_PROBE_PROMPT).await;
        assert_eq!(exit_code, 10, "{report}");
        assert_eq!(report["error_kind"], "authentication_failed");
        server.await.expect("mock server");
    }
}
