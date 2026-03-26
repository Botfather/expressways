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
    AnthropicConfig, OpenAiConfig, ProviderBackend, ProviderRetryConfig,
};
use expressways_nanobot_system::runtime::{RuntimeConfig, run_runtime};
use expressways_nanobot_system::tools::publish_json;
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse, RetentionClass,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProviderKind {
    RuleBased,
    #[value(name = "openai")]
    OpenAi,
    #[value(name = "anthropic")]
    Anthropic,
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
        #[arg(long, value_enum, default_value_t = ProviderKind::RuleBased)]
        provider: ProviderKind,
        #[arg(long = "provider-base-url")]
        provider_base_url: Option<String>,
        #[arg(long = "provider-model")]
        provider_model: Option<String>,
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
        #[arg(long = "provider-system-prompt")]
        provider_system_prompt: Option<String>,
        #[arg(long = "anthropic-api-version", default_value = "2023-06-01")]
        anthropic_api_version: String,
        #[arg(long, default_value_t = false)]
        once: bool,
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
            provider,
            provider_base_url,
            provider_model,
            provider_api_key,
            provider_timeout_seconds,
            provider_max_attempts,
            provider_base_backoff_ms,
            provider_max_backoff_ms,
            provider_jitter_ms,
            provider_circuit_failure_threshold,
            provider_circuit_cooldown_seconds,
            provider_failover,
            provider_streaming,
            fallback_provider_base_url,
            fallback_provider_model,
            fallback_provider_api_key,
            provider_system_prompt,
            anthropic_api_version,
            once,
        } => {
            let capability_token = resolve_token(token)?;
            let timeout = Duration::from_secs(provider_timeout_seconds.max(1));
            let retry = ProviderRetryConfig {
                max_attempts: provider_max_attempts,
                base_backoff_ms: provider_base_backoff_ms,
                max_backoff_ms: provider_max_backoff_ms,
                jitter_ms: provider_jitter_ms,
            }
            .normalized();
            let system_prompt_override = provider_system_prompt
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            let anthropic_api_version = if anthropic_api_version.trim().is_empty() {
                AnthropicConfig::default_api_version()
            } else {
                anthropic_api_version
            };
            let provider_kind = provider;

            let provider = match provider_kind {
                ProviderKind::RuleBased => ProviderBackend::RuleBased,
                ProviderKind::OpenAi => {
                    let api_base = non_empty_or_default(
                        provider_base_url,
                        OpenAiConfig::default_api_base,
                        "--provider-base-url",
                    )?;
                    let model = non_empty_or_default(
                        provider_model,
                        || "gpt-4o-mini".to_owned(),
                        "--provider-model",
                    )?;
                    let api_key = non_empty_or_default(
                        provider_api_key.or_else(|| std::env::var("OPENAI_API_KEY").ok()),
                        || String::new(),
                        "--provider-api-key or OPENAI_API_KEY",
                    )?;
                    if api_key.trim().is_empty() {
                        bail!("OpenAI provider requires --provider-api-key or OPENAI_API_KEY");
                    }
                    let http_client = build_http_client(timeout)
                        .context("failed to build OpenAI provider HTTP client")?;

                    ProviderBackend::OpenAi(OpenAiConfig {
                        api_base,
                        model,
                        api_key: Some(api_key),
                        timeout,
                        retry,
                        system_prompt: system_prompt_override
                            .clone()
                            .unwrap_or_else(OpenAiConfig::default_system_prompt),
                        http_client,
                    })
                }
                ProviderKind::Anthropic => {
                    let api_base = non_empty_or_default(
                        provider_base_url,
                        AnthropicConfig::default_api_base,
                        "--provider-base-url",
                    )?;
                    let model = non_empty_or_default(
                        provider_model,
                        || "claude-3-5-sonnet-latest".to_owned(),
                        "--provider-model",
                    )?;
                    let api_key = non_empty_or_default(
                        provider_api_key.or_else(|| std::env::var("ANTHROPIC_API_KEY").ok()),
                        || String::new(),
                        "--provider-api-key or ANTHROPIC_API_KEY",
                    )?;
                    if api_key.trim().is_empty() {
                        bail!(
                            "Anthropic provider requires --provider-api-key or ANTHROPIC_API_KEY"
                        );
                    }
                    let http_client = build_http_client(timeout)
                        .context("failed to build Anthropic provider HTTP client")?;

                    ProviderBackend::Anthropic(AnthropicConfig {
                        api_base,
                        model,
                        api_key: Some(api_key),
                        timeout,
                        retry,
                        system_prompt: system_prompt_override
                            .clone()
                            .unwrap_or_else(AnthropicConfig::default_system_prompt),
                        api_version: anthropic_api_version.clone(),
                        http_client,
                    })
                }
            };
            let fallback_provider = if provider_failover {
                match provider_kind {
                    ProviderKind::RuleBased => {
                        bail!(
                            "--provider-failover requires --provider openai or --provider anthropic"
                        )
                    }
                    ProviderKind::OpenAi => {
                        let api_base = non_empty_or_default(
                            fallback_provider_base_url,
                            AnthropicConfig::default_api_base,
                            "--fallback-provider-base-url",
                        )?;
                        let model = non_empty_or_default(
                            fallback_provider_model,
                            || "claude-3-5-sonnet-latest".to_owned(),
                            "--fallback-provider-model",
                        )?;
                        let api_key = non_empty_or_default(
                            fallback_provider_api_key
                                .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok()),
                            || String::new(),
                            "--fallback-provider-api-key or ANTHROPIC_API_KEY",
                        )?;
                        if api_key.trim().is_empty() {
                            bail!(
                                "Anthropic fallback requires --fallback-provider-api-key or ANTHROPIC_API_KEY"
                            );
                        }
                        let http_client = build_http_client(timeout)
                            .context("failed to build Anthropic fallback HTTP client")?;
                        Some(ProviderBackend::Anthropic(AnthropicConfig {
                            api_base,
                            model,
                            api_key: Some(api_key),
                            timeout,
                            retry,
                            system_prompt: system_prompt_override
                                .clone()
                                .unwrap_or_else(AnthropicConfig::default_system_prompt),
                            api_version: anthropic_api_version.clone(),
                            http_client,
                        }))
                    }
                    ProviderKind::Anthropic => {
                        let api_base = non_empty_or_default(
                            fallback_provider_base_url,
                            OpenAiConfig::default_api_base,
                            "--fallback-provider-base-url",
                        )?;
                        let model = non_empty_or_default(
                            fallback_provider_model,
                            || "gpt-4o-mini".to_owned(),
                            "--fallback-provider-model",
                        )?;
                        let api_key = non_empty_or_default(
                            fallback_provider_api_key
                                .or_else(|| std::env::var("OPENAI_API_KEY").ok()),
                            || String::new(),
                            "--fallback-provider-api-key or OPENAI_API_KEY",
                        )?;
                        if api_key.trim().is_empty() {
                            bail!(
                                "OpenAI fallback requires --fallback-provider-api-key or OPENAI_API_KEY"
                            );
                        }
                        let http_client = build_http_client(timeout)
                            .context("failed to build OpenAI fallback HTTP client")?;
                        Some(ProviderBackend::OpenAi(OpenAiConfig {
                            api_base,
                            model,
                            api_key: Some(api_key),
                            timeout,
                            retry,
                            system_prompt: system_prompt_override
                                .clone()
                                .unwrap_or_else(OpenAiConfig::default_system_prompt),
                            http_client,
                        }))
                    }
                }
            } else {
                None
            };
            run_runtime(RuntimeConfig {
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
            })
            .await
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

        if let Some(session_filter) = session_filter {
            if event.session_id.as_deref() != Some(session_filter) {
                continue;
            }
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
        if token.trim().is_empty() {
            bail!("--token cannot be empty");
        }
        return Ok(token);
    }
    let token_file = args
        .token_file
        .context("provide --token or --token-file for authenticated commands")?;
    let token = std::fs::read_to_string(&token_file)
        .with_context(|| format!("failed to read {}", token_file.display()))?;
    let token = token.trim().to_owned();
    if token.is_empty() {
        bail!("token file {} was empty", token_file.display());
    }
    Ok(token)
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
                "error": "timeout"
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
}
