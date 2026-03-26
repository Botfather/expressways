use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    AgentEndpoint, AgentRegistration, Classification, ControlCommand, ControlRequest,
    ControlResponse, RetentionClass, TopicSpec,
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::model::{
    NanobotInboundEnvelope, NanobotMessageRef, NanobotOutboundEnvelope, NanobotRuntimeEvent,
    NanobotStreamChunkEnvelope, SessionRole, SessionTurn,
};
use crate::provider::{
    Provider, ProviderBackend, ProviderInvocationStats, ProviderRequest, ProviderStep,
    RuleBasedProvider, ToolResultContext, anthropic_next_step, anthropic_next_step_streaming,
    openai_next_step, openai_next_step_streaming,
};
use crate::state::{MemoryStore, SessionStore, load_runtime_state, save_runtime_state};
use crate::tools::{ToolContext, ToolRegistry, publish_json};

const PROVIDER_UNAVAILABLE_RESPONSE: &str =
    "I'm having trouble reaching the model provider right now. Please try again.";

fn provider_unavailable_response() -> String {
    PROVIDER_UNAVAILABLE_RESPONSE.to_owned()
}

fn provider_event_detail(
    provider: &str,
    model: &str,
    api_base: &str,
    stats: ProviderInvocationStats,
    error: Option<&str>,
) -> serde_json::Value {
    let mut detail = serde_json::Map::new();
    detail.insert(
        "provider".to_owned(),
        serde_json::Value::String(provider.to_owned()),
    );
    detail.insert(
        "model".to_owned(),
        serde_json::Value::String(model.to_owned()),
    );
    detail.insert(
        "api_base".to_owned(),
        serde_json::Value::String(api_base.to_owned()),
    );
    detail.insert("attempts".to_owned(), serde_json::json!(stats.attempts));
    detail.insert("elapsed_ms".to_owned(), serde_json::json!(stats.elapsed_ms));
    if let Some(error) = error {
        detail.insert(
            "error".to_owned(),
            serde_json::Value::String(error.to_owned()),
        );
    }
    serde_json::Value::Object(detail)
}

#[derive(Debug, Clone, Default)]
struct ProviderCircuit {
    consecutive_failures: usize,
    open_until: Option<Instant>,
}

#[derive(Debug, Clone, Default)]
struct ProviderRuntimeState {
    openai: ProviderCircuit,
    anthropic: ProviderCircuit,
}

impl ProviderRuntimeState {
    fn circuit_mut(&mut self, provider: &str) -> Option<&mut ProviderCircuit> {
        match provider {
            "openai" => Some(&mut self.openai),
            "anthropic" => Some(&mut self.anthropic),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct ProviderFailure {
    provider: &'static str,
    model: String,
    api_base: String,
    message: String,
    stats: ProviderInvocationStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitGateDecision {
    Allow { recovering_probe: bool },
    Blocked { remaining_cooldown_ms: u64 },
}

fn evaluate_circuit_gate(circuit: &mut ProviderCircuit, now: Instant) -> CircuitGateDecision {
    match circuit.open_until {
        Some(open_until) if open_until > now => CircuitGateDecision::Blocked {
            remaining_cooldown_ms: open_until
                .saturating_duration_since(now)
                .as_millis()
                .min(u64::MAX as u128) as u64,
        },
        Some(_) => {
            circuit.open_until = None;
            CircuitGateDecision::Allow {
                recovering_probe: true,
            }
        }
        None => CircuitGateDecision::Allow {
            recovering_probe: false,
        },
    }
}

fn record_circuit_failure(
    circuit: &mut ProviderCircuit,
    threshold: usize,
    cooldown: Duration,
    now: Instant,
) -> bool {
    let threshold = threshold.max(1);
    circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
    if circuit.consecutive_failures >= threshold {
        circuit.open_until = Some(now + cooldown.max(Duration::from_secs(1)));
        circuit.consecutive_failures = 0;
        true
    } else {
        false
    }
}

fn record_circuit_success(circuit: &mut ProviderCircuit) {
    circuit.consecutive_failures = 0;
    circuit.open_until = None;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailoverDecision {
    Skip,
    Attempt { to_provider: &'static str },
    Succeeded { to_provider: &'static str },
    Failed { to_provider: &'static str },
}

fn decide_failover_transition(
    fallback_provider: Option<&ProviderBackend>,
    fallback_result: Option<&Result<ProviderStep, ProviderFailure>>,
) -> FailoverDecision {
    let Some(to_provider) = fallback_provider
        .and_then(provider_identity)
        .map(|(name, _, _)| name)
    else {
        return FailoverDecision::Skip;
    };

    match fallback_result {
        None => FailoverDecision::Attempt { to_provider },
        Some(Ok(_)) => FailoverDecision::Succeeded { to_provider },
        Some(Err(_)) => FailoverDecision::Failed { to_provider },
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub endpoint: Endpoint,
    pub capability_token: String,
    pub agent_id: String,
    pub display_name: String,
    pub version: String,
    pub summary: String,
    pub endpoint_transport: String,
    pub endpoint_address: String,
    pub classification: Classification,
    pub retention_class: RetentionClass,
    pub ttl_seconds: u64,
    pub state_path: PathBuf,
    pub session_dir: PathBuf,
    pub memory_dir: PathBuf,
    pub inbound_topic: String,
    pub outbound_topic: String,
    pub outbound_stream_topic: String,
    pub runtime_events_topic: String,
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub batch_limit: usize,
    pub once: bool,
    pub max_session_turns: usize,
    pub max_tool_steps: usize,
    pub memory_entries: usize,
    pub workspace_roots: Vec<PathBuf>,
    pub allowed_exec_programs: Vec<String>,
    pub max_exec_output_bytes: usize,
    pub ensure_topics: bool,
    pub instance_id: String,
    pub source_runtime: String,
    pub provider: ProviderBackend,
    pub fallback_provider: Option<ProviderBackend>,
    pub provider_streaming_enabled: bool,
    pub provider_circuit_failure_threshold: usize,
    pub provider_circuit_cooldown: Duration,
}

pub async fn run_runtime(config: RuntimeConfig) -> anyhow::Result<()> {
    if config.ensure_topics {
        ensure_topic(
            &config.endpoint,
            &config.capability_token,
            &config.inbound_topic,
            RetentionClass::Operational,
            Classification::Internal,
        )
        .await?;
        ensure_topic(
            &config.endpoint,
            &config.capability_token,
            &config.outbound_topic,
            RetentionClass::Operational,
            Classification::Internal,
        )
        .await?;
        ensure_topic(
            &config.endpoint,
            &config.capability_token,
            &config.outbound_stream_topic,
            RetentionClass::Operational,
            Classification::Internal,
        )
        .await?;
        ensure_topic(
            &config.endpoint,
            &config.capability_token,
            &config.runtime_events_topic,
            RetentionClass::Operational,
            Classification::Internal,
        )
        .await?;
    }

    let session_store = SessionStore::new(&config.session_dir)?;
    let memory_store = MemoryStore::new(&config.memory_dir)?;
    let mut state = load_runtime_state(&config.state_path).unwrap_or_default();
    let tools = ToolRegistry;
    let mut provider_runtime_state = ProviderRuntimeState::default();

    register_agent(&config).await?;
    info!(
        agent_id = %config.agent_id,
        inbound_topic = %config.inbound_topic,
        outbound_topic = %config.outbound_topic,
        outbound_stream_topic = %config.outbound_stream_topic,
        runtime_events_topic = %config.runtime_events_topic,
        "runtime agent registered"
    );

    let shutdown = CancellationToken::new();
    let signal_handle = if config.once {
        None
    } else {
        Some(tokio::spawn(wait_for_shutdown(shutdown.clone())))
    };

    let heartbeat_handle = tokio::spawn(run_heartbeat_loop(
        config.endpoint.clone(),
        config.capability_token.clone(),
        config.agent_id.clone(),
        shutdown.clone(),
        config.heartbeat_interval,
    ));

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let messages = consume_messages(
            &config.endpoint,
            &config.capability_token,
            &config.inbound_topic,
            state.inbound_offset,
            config.batch_limit.max(1),
        )
        .await?;

        if messages.is_empty() {
            if config.once {
                break;
            }
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(config.poll_interval.max(Duration::from_millis(1))) => {}
            }
            continue;
        }

        for message in messages {
            state.inbound_offset = state.inbound_offset.max(message.offset.saturating_add(1));

            if let Err(error) = process_inbound_message(
                &config,
                &session_store,
                &memory_store,
                &tools,
                &mut provider_runtime_state,
                message.payload,
            )
            .await
            {
                warn!(error = %error, "failed to process inbound message");
                let detail = serde_json::json!({ "error": error.to_string() });
                let _ = emit_runtime_event(&config, "message_failed", None, None, detail).await;
            }

            save_runtime_state(&config.state_path, &state)?;
            if config.once {
                shutdown.cancel();
                break;
            }
        }
    }

    shutdown.cancel();
    await_task("heartbeat", heartbeat_handle).await;
    if let Some(signal_handle) = signal_handle {
        signal_handle.abort();
        await_task("signal", signal_handle).await;
    }

    if let Err(error) =
        remove_agent(&config.endpoint, &config.capability_token, &config.agent_id).await
    {
        warn!(error = %error, "failed to remove runtime agent registration");
    }

    Ok(())
}

async fn process_inbound_message(
    config: &RuntimeConfig,
    sessions: &SessionStore,
    memory: &MemoryStore,
    tools: &ToolRegistry,
    provider_runtime_state: &mut ProviderRuntimeState,
    payload: String,
) -> anyhow::Result<()> {
    let envelope = serde_json::from_str::<NanobotInboundEnvelope>(&payload)
        .context("failed to parse nanobot inbound payload")?
        .normalized();

    let session_id = envelope.session.session_id.clone();
    let inbound_message_id = envelope
        .message
        .message_id
        .clone()
        .unwrap_or_else(|| Uuid::now_v7().to_string());
    let user_text = envelope.message.text.clone().unwrap_or_default();

    sessions.append_turn(
        &session_id,
        &SessionTurn {
            timestamp: Utc::now(),
            role: SessionRole::User,
            text: Some(user_text.clone()),
            message_id: Some(inbound_message_id.clone()),
            tool_name: None,
            tool_args: None,
            tool_result: None,
        },
    )?;

    let mut history = sessions.load_recent_turns(&session_id, config.max_session_turns.max(1))?;
    let memory_summary = memory
        .summarize_recent(&session_id, config.memory_entries.max(1))
        .unwrap_or(None);
    let mut provider = RuleBasedProvider;
    let mut pending_tool_result: Option<ToolResultContext> = None;
    let mut assistant_text = None;
    let stream_id = Uuid::now_v7().to_string();

    let tool_context = ToolContext {
        endpoint: config.endpoint.clone(),
        capability_token: config.capability_token.clone(),
        inbound_topic: config.inbound_topic.clone(),
        workspace_roots: config.workspace_roots.clone(),
        allowed_exec_programs: config.allowed_exec_programs.clone(),
        max_exec_output_bytes: config.max_exec_output_bytes,
        instance_id: config.instance_id.clone(),
    };

    for _ in 0..config.max_tool_steps.max(1) {
        let provider_request = ProviderRequest {
            latest_user_text: user_text.clone(),
            history: history.clone(),
            memory_summary: memory_summary.clone(),
            pending_tool_result: pending_tool_result.clone(),
        };

        let (stream_chunk_tx, stream_join) = if config.provider_streaming_enabled {
            let (tx, mut rx) = unbounded_channel::<String>();
            let endpoint = config.endpoint.clone();
            let capability_token = config.capability_token.clone();
            let outbound_stream_topic = config.outbound_stream_topic.clone();
            let source_runtime = config.source_runtime.clone();
            let instance_id = config.instance_id.clone();
            let session = envelope.session.clone();
            let in_reply_to_message_id = inbound_message_id.clone();
            let stream_id = stream_id.clone();
            let join = tokio::spawn(async move {
                let mut chunk_index = 0u64;
                while let Some(text_delta) = rx.recv().await {
                    if text_delta.is_empty() {
                        continue;
                    }
                    chunk_index = chunk_index.saturating_add(1);
                    if let Err(error) = publish_stream_chunk(
                        &endpoint,
                        &capability_token,
                        &outbound_stream_topic,
                        &source_runtime,
                        &instance_id,
                        &session,
                        &stream_id,
                        &in_reply_to_message_id,
                        chunk_index,
                        &text_delta,
                        false,
                    )
                    .await
                    {
                        warn!(
                            error = %error,
                            stream_id = %stream_id,
                            chunk_index,
                            "failed to publish stream chunk"
                        );
                    }
                }
                chunk_index
            });
            (Some(tx), Some(join))
        } else {
            (None, None)
        };

        let step = resolve_provider_step(
            config,
            provider_runtime_state,
            &provider_request,
            &session_id,
            &inbound_message_id,
            &mut provider,
            stream_chunk_tx.clone(),
        )
        .await;
        drop(stream_chunk_tx);
        let streamed_chunk_count = match stream_join {
            Some(join) => match join.await {
                Ok(count) => count,
                Err(error) => {
                    warn!(error = %error, "stream chunk publisher task failed");
                    0
                }
            },
            None => 0,
        };
        match step {
            ProviderStep::Respond { text } => {
                if streamed_chunk_count > 0 {
                    if let Err(error) = publish_stream_chunk(
                        &config.endpoint,
                        &config.capability_token,
                        &config.outbound_stream_topic,
                        &config.source_runtime,
                        &config.instance_id,
                        &envelope.session,
                        &stream_id,
                        &inbound_message_id,
                        streamed_chunk_count.saturating_add(1),
                        "",
                        true,
                    )
                    .await
                    {
                        warn!(error = %error, stream_id = %stream_id, "failed to publish final stream marker");
                    }
                }
                assistant_text = Some(text);
                break;
            }
            ProviderStep::ToolCalls { calls } => {
                if calls.is_empty() {
                    assistant_text =
                        Some("Provider returned no tool calls and no assistant text.".to_owned());
                    break;
                }

                for call in calls {
                    let name = call.name;
                    let args = call.args;
                    sessions.append_turn(
                        &session_id,
                        &SessionTurn {
                            timestamp: Utc::now(),
                            role: SessionRole::ToolCall,
                            text: None,
                            message_id: None,
                            tool_name: Some(name.clone()),
                            tool_args: Some(args.clone()),
                            tool_result: None,
                        },
                    )?;

                    let result = match tools
                        .invoke(&name, args.clone(), &tool_context, &envelope.session)
                        .await
                    {
                        Ok(result) => result,
                        Err(error) => serde_json::json!({ "ok": false, "error": error }),
                    };
                    sessions.append_turn(
                        &session_id,
                        &SessionTurn {
                            timestamp: Utc::now(),
                            role: SessionRole::ToolResult,
                            text: None,
                            message_id: None,
                            tool_name: Some(name.clone()),
                            tool_args: Some(args),
                            tool_result: Some(result.clone()),
                        },
                    )?;

                    pending_tool_result = Some(ToolResultContext {
                        tool_name: name,
                        result,
                    });
                }

                history =
                    sessions.load_recent_turns(&session_id, config.max_session_turns.max(1))?;
            }
        }
    }

    let assistant_text = assistant_text.unwrap_or_else(|| {
        "I could not produce a response after the configured tool loop budget.".to_owned()
    });
    let outbound_message_id = Uuid::now_v7().to_string();
    sessions.append_turn(
        &session_id,
        &SessionTurn {
            timestamp: Utc::now(),
            role: SessionRole::Assistant,
            text: Some(assistant_text.clone()),
            message_id: Some(outbound_message_id.clone()),
            tool_name: None,
            tool_args: None,
            tool_result: None,
        },
    )?;
    memory.append_note(
        &session_id,
        format!(
            "user: {} | assistant: {}",
            truncate_for_memory(&user_text),
            truncate_for_memory(&assistant_text)
        ),
    )?;

    let outbound = NanobotOutboundEnvelope {
        schema_version: envelope.schema_version.clone(),
        source_runtime: config.source_runtime.clone(),
        instance_id: config.instance_id.clone(),
        session: envelope.session.clone(),
        message: NanobotMessageRef {
            message_id: Some(outbound_message_id),
            role: Some("assistant".to_owned()),
            text: Some(assistant_text),
            attachments: Vec::new(),
        },
        in_reply_to_message_id: Some(inbound_message_id.clone()),
        metadata: serde_json::json!({
            "inbound_source_runtime": envelope.source_runtime,
            "inbound_instance_id": envelope.instance_id,
            "processed_at": Utc::now(),
            "available_tools": tools.available(),
        }),
        generated_at: Utc::now(),
    };

    publish_json(
        &config.endpoint,
        &config.capability_token,
        &config.outbound_topic,
        Classification::Internal,
        &outbound,
    )
    .await
    .map_err(anyhow::Error::msg)?;

    emit_runtime_event(
        config,
        "message_processed",
        Some(session_id),
        Some(inbound_message_id),
        serde_json::json!({
            "outbound_topic": config.outbound_topic,
            "runtime": config.source_runtime,
        }),
    )
    .await?;

    Ok(())
}

fn provider_identity(provider: &ProviderBackend) -> Option<(&'static str, &str, &str)> {
    match provider {
        ProviderBackend::OpenAi(config) => Some(("openai", &config.model, &config.api_base)),
        ProviderBackend::Anthropic(config) => Some(("anthropic", &config.model, &config.api_base)),
        ProviderBackend::RuleBased => None,
    }
}

async fn resolve_provider_step(
    config: &RuntimeConfig,
    provider_runtime_state: &mut ProviderRuntimeState,
    provider_request: &ProviderRequest,
    session_id: &str,
    inbound_message_id: &str,
    rule_based_provider: &mut RuleBasedProvider,
    stream_chunk_tx: Option<UnboundedSender<String>>,
) -> ProviderStep {
    if matches!(config.provider, ProviderBackend::RuleBased) {
        return rule_based_provider.next_step(provider_request);
    }

    match attempt_external_provider(
        config,
        provider_runtime_state,
        &config.provider,
        provider_request,
        session_id,
        inbound_message_id,
        stream_chunk_tx.clone(),
    )
    .await
    {
        Ok(step) => step,
        Err(primary_failure) => {
            let fallback_provider = config.fallback_provider.as_ref();
            let FailoverDecision::Attempt { to_provider } =
                decide_failover_transition(fallback_provider, None)
            else {
                return ProviderStep::Respond {
                    text: provider_unavailable_response(),
                };
            };
            let Some(fallback_provider) = fallback_provider else {
                return ProviderStep::Respond {
                    text: provider_unavailable_response(),
                };
            };
            let _ = emit_runtime_event(
                config,
                "provider_failover_attempt",
                Some(session_id.to_owned()),
                Some(inbound_message_id.to_owned()),
                serde_json::json!({
                    "from_provider": primary_failure.provider,
                    "to_provider": to_provider,
                    "from_error": primary_failure.message,
                }),
            )
            .await;

            let fallback_result = attempt_external_provider(
                config,
                provider_runtime_state,
                fallback_provider,
                provider_request,
                session_id,
                inbound_message_id,
                stream_chunk_tx.clone(),
            )
            .await;
            match decide_failover_transition(Some(fallback_provider), Some(&fallback_result)) {
                FailoverDecision::Succeeded { to_provider } => {
                    let Ok(step) = fallback_result else {
                        return ProviderStep::Respond {
                            text: provider_unavailable_response(),
                        };
                    };
                    let _ = emit_runtime_event(
                        config,
                        "provider_failover_succeeded",
                        Some(session_id.to_owned()),
                        Some(inbound_message_id.to_owned()),
                        serde_json::json!({
                            "from_provider": primary_failure.provider,
                            "to_provider": to_provider,
                        }),
                    )
                    .await;
                    step
                }
                FailoverDecision::Failed { to_provider } => {
                    let Err(fallback_failure) = fallback_result else {
                        return ProviderStep::Respond {
                            text: provider_unavailable_response(),
                        };
                    };
                    let _ = emit_runtime_event(
                        config,
                        "provider_failover_failed",
                        Some(session_id.to_owned()),
                        Some(inbound_message_id.to_owned()),
                        serde_json::json!({
                            "from_provider": primary_failure.provider,
                            "to_provider": to_provider,
                            "from_error": primary_failure.message,
                            "to_error": fallback_failure.message,
                        }),
                    )
                    .await;
                    ProviderStep::Respond {
                        text: provider_unavailable_response(),
                    }
                }
                FailoverDecision::Skip | FailoverDecision::Attempt { .. } => {
                    ProviderStep::Respond {
                        text: provider_unavailable_response(),
                    }
                }
            }
        }
    }
}

async fn attempt_external_provider(
    config: &RuntimeConfig,
    provider_runtime_state: &mut ProviderRuntimeState,
    provider: &ProviderBackend,
    provider_request: &ProviderRequest,
    session_id: &str,
    inbound_message_id: &str,
    stream_chunk_tx: Option<UnboundedSender<String>>,
) -> Result<ProviderStep, ProviderFailure> {
    let Some((provider_name, model, api_base)) = provider_identity(provider) else {
        return Ok(ProviderStep::Respond {
            text: provider_unavailable_response(),
        });
    };

    let threshold = config.provider_circuit_failure_threshold.max(1);
    let cooldown = config.provider_circuit_cooldown.max(Duration::from_secs(1));
    let now = Instant::now();
    let gate = {
        let Some(circuit) = provider_runtime_state.circuit_mut(provider_name) else {
            return Ok(ProviderStep::Respond {
                text: provider_unavailable_response(),
            });
        };
        evaluate_circuit_gate(circuit, now)
    };
    let recovering_probe = matches!(
        gate,
        CircuitGateDecision::Allow {
            recovering_probe: true
        }
    );
    if let CircuitGateDecision::Blocked {
        remaining_cooldown_ms,
    } = gate
    {
        let _ = emit_runtime_event(
            config,
            "provider_circuit_blocked",
            Some(session_id.to_owned()),
            Some(inbound_message_id.to_owned()),
            serde_json::json!({
                "provider": provider_name,
                "model": model,
                "api_base": api_base,
                "remaining_cooldown_ms": remaining_cooldown_ms,
            }),
        )
        .await;
        return Err(ProviderFailure {
            provider: provider_name,
            model: model.to_owned(),
            api_base: api_base.to_owned(),
            message: "provider circuit is open".to_owned(),
            stats: ProviderInvocationStats {
                attempts: 0,
                elapsed_ms: 0,
            },
        });
    }

    let outcome = match provider {
        ProviderBackend::OpenAi(provider_config) => {
            let invocation_result = if config.provider_streaming_enabled {
                let tx = stream_chunk_tx.clone();
                openai_next_step_streaming(provider_request, provider_config, move |chunk| {
                    if let Some(sender) = tx.as_ref() {
                        let _ = sender.send(chunk.to_owned());
                    }
                })
                .await
            } else {
                openai_next_step(provider_request, provider_config).await
            };
            match invocation_result {
                Ok(outcome) => outcome,
                Err(error) => {
                    let failure = ProviderFailure {
                        provider: provider_name,
                        model: model.to_owned(),
                        api_base: api_base.to_owned(),
                        message: error.message,
                        stats: error.stats,
                    };
                    warn!(
                        error = %failure.message,
                        provider = %failure.provider,
                        model = %failure.model,
                        api_base = %failure.api_base,
                        attempts = failure.stats.attempts,
                        elapsed_ms = failure.stats.elapsed_ms,
                        "provider failed; returning fallback assistant response"
                    );
                    let _ = emit_runtime_event(
                        config,
                        "provider_error",
                        Some(session_id.to_owned()),
                        Some(inbound_message_id.to_owned()),
                        provider_event_detail(
                            failure.provider,
                            &failure.model,
                            &failure.api_base,
                            failure.stats,
                            Some(&failure.message),
                        ),
                    )
                    .await;

                    let opened_circuit =
                        if let Some(circuit) = provider_runtime_state.circuit_mut(provider_name) {
                            record_circuit_failure(circuit, threshold, cooldown, Instant::now())
                        } else {
                            false
                        };
                    if opened_circuit {
                        let _ = emit_runtime_event(
                            config,
                            "provider_circuit_open",
                            Some(session_id.to_owned()),
                            Some(inbound_message_id.to_owned()),
                            serde_json::json!({
                                "provider": failure.provider,
                                "model": failure.model,
                                "api_base": failure.api_base,
                                "cooldown_ms": cooldown.as_millis().min(u64::MAX as u128) as u64,
                                "failure_threshold": threshold,
                            }),
                        )
                        .await;
                    }
                    return Err(failure);
                }
            }
        }
        ProviderBackend::Anthropic(provider_config) => {
            let invocation_result = if config.provider_streaming_enabled {
                let tx = stream_chunk_tx.clone();
                anthropic_next_step_streaming(provider_request, provider_config, move |chunk| {
                    if let Some(sender) = tx.as_ref() {
                        let _ = sender.send(chunk.to_owned());
                    }
                })
                .await
            } else {
                anthropic_next_step(provider_request, provider_config).await
            };
            match invocation_result {
                Ok(outcome) => outcome,
                Err(error) => {
                    let failure = ProviderFailure {
                        provider: provider_name,
                        model: model.to_owned(),
                        api_base: api_base.to_owned(),
                        message: error.message,
                        stats: error.stats,
                    };
                    warn!(
                        error = %failure.message,
                        provider = %failure.provider,
                        model = %failure.model,
                        api_base = %failure.api_base,
                        attempts = failure.stats.attempts,
                        elapsed_ms = failure.stats.elapsed_ms,
                        "provider failed; returning fallback assistant response"
                    );
                    let _ = emit_runtime_event(
                        config,
                        "provider_error",
                        Some(session_id.to_owned()),
                        Some(inbound_message_id.to_owned()),
                        provider_event_detail(
                            failure.provider,
                            &failure.model,
                            &failure.api_base,
                            failure.stats,
                            Some(&failure.message),
                        ),
                    )
                    .await;

                    let opened_circuit =
                        if let Some(circuit) = provider_runtime_state.circuit_mut(provider_name) {
                            record_circuit_failure(circuit, threshold, cooldown, Instant::now())
                        } else {
                            false
                        };
                    if opened_circuit {
                        let _ = emit_runtime_event(
                            config,
                            "provider_circuit_open",
                            Some(session_id.to_owned()),
                            Some(inbound_message_id.to_owned()),
                            serde_json::json!({
                                "provider": failure.provider,
                                "model": failure.model,
                                "api_base": failure.api_base,
                                "cooldown_ms": cooldown.as_millis().min(u64::MAX as u128) as u64,
                                "failure_threshold": threshold,
                            }),
                        )
                        .await;
                    }
                    return Err(failure);
                }
            }
        }
        ProviderBackend::RuleBased => {
            return Ok(ProviderStep::Respond {
                text: provider_unavailable_response(),
            });
        }
    };

    if let Some(circuit) = provider_runtime_state.circuit_mut(provider_name) {
        record_circuit_success(circuit);
    }

    if outcome.stats.attempts > 1 {
        let _ = emit_runtime_event(
            config,
            "provider_retry_recovered",
            Some(session_id.to_owned()),
            Some(inbound_message_id.to_owned()),
            provider_event_detail(provider_name, model, api_base, outcome.stats, None),
        )
        .await;
    }

    if recovering_probe {
        let _ = emit_runtime_event(
            config,
            "provider_circuit_closed",
            Some(session_id.to_owned()),
            Some(inbound_message_id.to_owned()),
            provider_event_detail(provider_name, model, api_base, outcome.stats, None),
        )
        .await;
    }

    Ok(outcome.step)
}

fn truncate_for_memory(text: &str) -> String {
    let trimmed = text.trim();
    let max_chars = 240usize;
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_owned();
    }
    let mut out = String::with_capacity(max_chars + 1);
    for (idx, ch) in trimmed.chars().enumerate() {
        if idx >= max_chars {
            break;
        }
        out.push(ch);
    }
    out.push_str("...");
    out
}

#[allow(clippy::too_many_arguments)]
async fn publish_stream_chunk(
    endpoint: &Endpoint,
    capability_token: &str,
    outbound_stream_topic: &str,
    source_runtime: &str,
    instance_id: &str,
    session: &crate::model::NanobotSessionRef,
    stream_id: &str,
    in_reply_to_message_id: &str,
    chunk_index: u64,
    text_delta: &str,
    done: bool,
) -> Result<(), String> {
    let payload = NanobotStreamChunkEnvelope {
        schema_version: crate::model::SYSTEM_SCHEMA_VERSION.to_owned(),
        source_runtime: source_runtime.to_owned(),
        instance_id: instance_id.to_owned(),
        session: session.clone(),
        stream_id: stream_id.to_owned(),
        in_reply_to_message_id: Some(in_reply_to_message_id.to_owned()),
        chunk_index,
        text_delta: text_delta.to_owned(),
        done,
        metadata: serde_json::json!({
            "kind": "assistant_stream_chunk",
        }),
        generated_at: Utc::now(),
    };
    publish_json(
        endpoint,
        capability_token,
        outbound_stream_topic,
        Classification::Internal,
        &payload,
    )
    .await
    .map(|_| ())
}

async fn emit_runtime_event(
    config: &RuntimeConfig,
    event: &str,
    session_id: Option<String>,
    message_id: Option<String>,
    detail: serde_json::Value,
) -> anyhow::Result<()> {
    let payload = NanobotRuntimeEvent {
        schema_version: crate::model::SYSTEM_SCHEMA_VERSION.to_owned(),
        source_runtime: config.source_runtime.clone(),
        instance_id: config.instance_id.clone(),
        event: event.to_owned(),
        session_id,
        message_id,
        detail,
        timestamp: Utc::now(),
    };
    publish_json(
        &config.endpoint,
        &config.capability_token,
        &config.runtime_events_topic,
        Classification::Internal,
        &payload,
    )
    .await
    .map_err(anyhow::Error::msg)
}

async fn ensure_topic(
    endpoint: &Endpoint,
    capability_token: &str,
    topic: &str,
    retention_class: RetentionClass,
    classification: Classification,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: topic.to_owned(),
                    retention_class,
                    default_classification: classification,
                },
            },
        })
        .await?;
    match response {
        ControlResponse::TopicCreated { .. } => Ok(()),
        ControlResponse::Error { code, message } => {
            anyhow::bail!("failed to create or validate topic `{topic}`: {code}: {message}")
        }
        other => anyhow::bail!("unexpected response while ensuring topic `{topic}`: {other:?}"),
    }
}

async fn register_agent(config: &RuntimeConfig) -> anyhow::Result<()> {
    let mut client = Client::connect(config.endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: config.capability_token.clone(),
            command: ControlCommand::RegisterAgent {
                registration: AgentRegistration {
                    agent_id: config.agent_id.clone(),
                    display_name: config.display_name.clone(),
                    version: config.version.clone(),
                    summary: config.summary.clone(),
                    skills: vec![
                        "nanobot-chat".to_owned(),
                        "nanobot-tools".to_owned(),
                        "nanobot-memory".to_owned(),
                        "nanobot-cron".to_owned(),
                        "nanobot-subagent".to_owned(),
                    ],
                    subscriptions: vec![config.inbound_topic.clone()],
                    publications: vec![
                        config.outbound_topic.clone(),
                        config.outbound_stream_topic.clone(),
                        config.runtime_events_topic.clone(),
                    ],
                    schemas: Vec::new(),
                    endpoint: AgentEndpoint {
                        transport: config.endpoint_transport.clone(),
                        address: config.endpoint_address.clone(),
                    },
                    classification: config.classification.clone(),
                    retention_class: config.retention_class.clone(),
                    ttl_seconds: Some(config.ttl_seconds.max(30)),
                },
            },
        })
        .await?;

    match response {
        ControlResponse::AgentRegistered { .. } => Ok(()),
        ControlResponse::Error { code, message } => {
            anyhow::bail!("agent registration failed: {code}: {message}")
        }
        other => anyhow::bail!("unexpected response during registration: {other:?}"),
    }
}

async fn remove_agent(
    endpoint: &Endpoint,
    capability_token: &str,
    agent_id: &str,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::RemoveAgent {
                agent_id: agent_id.to_owned(),
            },
        })
        .await?;
    match response {
        ControlResponse::AgentRemoved { .. } => Ok(()),
        ControlResponse::Error { .. } => Ok(()),
        other => anyhow::bail!("unexpected response while removing agent: {other:?}"),
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
            anyhow::bail!("consume failed for topic `{topic}`: {code}: {message}")
        }
        other => anyhow::bail!("unexpected consume response for topic `{topic}`: {other:?}"),
    }
}

async fn run_heartbeat_loop(
    endpoint: Endpoint,
    capability_token: String,
    agent_id: String,
    shutdown: CancellationToken,
    interval: Duration,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval.max(Duration::from_secs(1))) => {}
        }
        if let Err(error) = heartbeat_once(&endpoint, &capability_token, &agent_id).await {
            warn!(error = %error, agent_id = %agent_id, "agent heartbeat failed");
        }
    }
}

async fn heartbeat_once(
    endpoint: &Endpoint,
    capability_token: &str,
    agent_id: &str,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::HeartbeatAgent {
                agent_id: agent_id.to_owned(),
            },
        })
        .await?;
    match response {
        ControlResponse::AgentHeartbeat { .. } => Ok(()),
        ControlResponse::Error { code, message } => {
            anyhow::bail!("heartbeat rejected: {code}: {message}")
        }
        other => anyhow::bail!("unexpected heartbeat response: {other:?}"),
    }
}

async fn wait_for_shutdown(shutdown: CancellationToken) {
    if tokio::signal::ctrl_c().await.is_ok() {
        shutdown.cancel();
    }
}

async fn await_task(name: &str, handle: tokio::task::JoinHandle<()>) {
    match handle.await {
        Ok(()) => {}
        Err(error) if error.is_cancelled() => {}
        Err(error) => warn!(task = name, error = %error, "background task exited unexpectedly"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{AnthropicConfig, OpenAiConfig};
    use std::time::Instant;

    #[test]
    fn provider_fallback_response_is_sanitized() {
        let text = provider_unavailable_response();
        assert_eq!(text, PROVIDER_UNAVAILABLE_RESPONSE);
        let lower = text.to_lowercase();
        assert!(!lower.contains("openai"));
        assert!(!lower.contains("anthropic"));
        assert!(!lower.contains("http"));
        assert!(!lower.contains("api key"));
        assert!(!lower.contains("token"));
    }

    #[test]
    fn provider_event_detail_includes_retry_stats_and_optional_error() {
        let stats = ProviderInvocationStats {
            attempts: 3,
            elapsed_ms: 912,
        };
        let with_error = provider_event_detail(
            "openai",
            "gpt-5-mini",
            "https://api.openai.com/v1",
            stats,
            Some("HTTP 429"),
        );
        assert_eq!(with_error["provider"], "openai");
        assert_eq!(with_error["model"], "gpt-5-mini");
        assert_eq!(with_error["attempts"], 3);
        assert_eq!(with_error["elapsed_ms"], 912);
        assert_eq!(with_error["error"], "HTTP 429");

        let without_error = provider_event_detail(
            "anthropic",
            "claude-sonnet-4-5",
            "https://api.anthropic.com",
            stats,
            None,
        );
        assert_eq!(without_error["provider"], "anthropic");
        assert_eq!(without_error["error"], serde_json::Value::Null);
    }

    #[test]
    fn circuit_opens_after_failure_threshold_and_blocks_until_cooldown() {
        let mut circuit = ProviderCircuit::default();
        let now = Instant::now();
        let cooldown = Duration::from_secs(10);

        assert!(!record_circuit_failure(&mut circuit, 2, cooldown, now));
        assert_eq!(circuit.consecutive_failures, 1);
        assert!(record_circuit_failure(&mut circuit, 2, cooldown, now));
        assert_eq!(circuit.consecutive_failures, 0);
        assert!(circuit.open_until.is_some());

        let gate = evaluate_circuit_gate(&mut circuit, now);
        match gate {
            CircuitGateDecision::Blocked {
                remaining_cooldown_ms,
            } => {
                assert!(remaining_cooldown_ms > 0);
            }
            other => panic!("expected blocked gate, got {other:?}"),
        }
    }

    #[test]
    fn circuit_closes_on_recovery_probe_after_cooldown() {
        let mut circuit = ProviderCircuit {
            consecutive_failures: 0,
            open_until: Some(Instant::now() - Duration::from_millis(1)),
        };
        let gate = evaluate_circuit_gate(&mut circuit, Instant::now());
        assert_eq!(
            gate,
            CircuitGateDecision::Allow {
                recovering_probe: true
            }
        );
        assert!(circuit.open_until.is_none());
    }

    #[test]
    fn circuit_success_resets_failures_and_open_state() {
        let mut circuit = ProviderCircuit {
            consecutive_failures: 7,
            open_until: Some(Instant::now() + Duration::from_secs(30)),
        };
        record_circuit_success(&mut circuit);
        assert_eq!(circuit.consecutive_failures, 0);
        assert!(circuit.open_until.is_none());
    }

    #[test]
    fn failover_transition_covers_attempt_success_and_failure() {
        let fallback = ProviderBackend::Anthropic(AnthropicConfig {
            api_base: AnthropicConfig::default_api_base(),
            model: "claude-3-5-sonnet-latest".to_owned(),
            api_key: Some("test".to_owned()),
            timeout: Duration::from_secs(1),
            retry: crate::provider::ProviderRetryConfig::default(),
            system_prompt: AnthropicConfig::default_system_prompt(),
            api_version: AnthropicConfig::default_api_version(),
            http_client: reqwest::Client::builder()
                .build()
                .expect("build http client"),
        });
        assert_eq!(
            decide_failover_transition(Some(&fallback), None),
            FailoverDecision::Attempt {
                to_provider: "anthropic"
            }
        );

        let ok_result = Ok(ProviderStep::Respond {
            text: "fallback ok".to_owned(),
        });
        assert_eq!(
            decide_failover_transition(Some(&fallback), Some(&ok_result)),
            FailoverDecision::Succeeded {
                to_provider: "anthropic"
            }
        );

        let failure = ProviderFailure {
            provider: "anthropic",
            model: "claude-3-5-sonnet-latest".to_owned(),
            api_base: AnthropicConfig::default_api_base(),
            message: "boom".to_owned(),
            stats: ProviderInvocationStats {
                attempts: 1,
                elapsed_ms: 10,
            },
        };
        let err_result = Err(failure);
        assert_eq!(
            decide_failover_transition(Some(&fallback), Some(&err_result)),
            FailoverDecision::Failed {
                to_provider: "anthropic"
            }
        );
        assert_eq!(
            decide_failover_transition(None, None),
            FailoverDecision::Skip
        );
    }

    #[test]
    fn failover_transition_ignores_rule_based_fallback() {
        let fallback = ProviderBackend::RuleBased;
        assert_eq!(
            decide_failover_transition(Some(&fallback), None),
            FailoverDecision::Skip
        );

        let openai_fallback = ProviderBackend::OpenAi(OpenAiConfig {
            api_base: OpenAiConfig::default_api_base(),
            model: "gpt-4o-mini".to_owned(),
            api_key: Some("test".to_owned()),
            timeout: Duration::from_secs(1),
            retry: crate::provider::ProviderRetryConfig::default(),
            system_prompt: OpenAiConfig::default_system_prompt(),
            http_client: reqwest::Client::builder()
                .build()
                .expect("build http client"),
        });
        assert_eq!(
            decide_failover_transition(Some(&openai_fallback), None),
            FailoverDecision::Attempt {
                to_provider: "openai"
            }
        );
    }
}
