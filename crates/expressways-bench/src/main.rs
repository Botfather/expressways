use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use chrono::{Duration as ChronoDuration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_auth::CapabilityIssuer;
use expressways_client::{Client, Endpoint, StreamClient};
use expressways_protocol::{
    Action, AgentQuery, CapabilityClaims, CapabilityScope, Classification, ControlCommand,
    ControlRequest, ControlResponse, RetentionClass, StreamFrame, TopicSpec,
};
use expressways_storage::{DiskPressurePolicy, RetentionPolicy, Storage, StorageConfig};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    Broker(BrokerBenchArgs),
    RegistryWatch(RegistryWatchBenchArgs),
    Storage(StorageBenchArgs),
    Suite(SuiteArgs),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransportKind {
    Tcp,
    Unix,
}

#[derive(Debug, Args)]
struct BrokerBenchArgs {
    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    transport: TransportKind,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "configs/expressways.example.toml")]
    config: PathBuf,
    #[arg(long, default_value = "target/debug/expressways-server")]
    server_bin: PathBuf,
    #[arg(long, default_value = "./var/auth/issuer.private")]
    private_key: PathBuf,
    #[arg(long, default_value = "dev")]
    key_id: String,
    #[arg(long, default_value = "local:developer")]
    principal: String,
    #[arg(long, default_value = "expressways")]
    audience: String,
    #[arg(long, default_value_t = 25)]
    warmup_iterations: usize,
    #[arg(long, default_value_t = 200)]
    iterations: usize,
    #[arg(long, default_value_t = 512)]
    payload_bytes: usize,
    #[arg(long, default_value = "benchmarks")]
    topic: String,
    #[arg(long)]
    spawn_server: bool,
    #[arg(long = "broker-output")]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct StorageBenchArgs {
    #[arg(long, default_value = "./var/benchmarks/storage")]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 2_000)]
    message_count: usize,
    #[arg(long, default_value_t = 512)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 250)]
    read_batch: usize,
    #[arg(long, default_value_t = 1_048_576)]
    segment_max_bytes: u64,
    #[arg(long = "storage-output")]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WatchMode {
    LongPoll,
    Stream,
}

#[derive(Debug, Args, Clone)]
struct RegistryWatchBenchArgs {
    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    transport: TransportKind,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "configs/expressways.example.toml")]
    config: PathBuf,
    #[arg(long, default_value = "target/debug/expressways-server")]
    server_bin: PathBuf,
    #[arg(long, default_value = "./var/auth/issuer.private")]
    private_key: PathBuf,
    #[arg(long, default_value = "dev")]
    key_id: String,
    #[arg(long, default_value = "local:developer")]
    principal: String,
    #[arg(long, default_value = "expressways")]
    audience: String,
    #[arg(long, value_enum, default_value_t = WatchMode::LongPoll)]
    mode: WatchMode,
    #[arg(long, default_value_t = 10)]
    warmup_iterations: usize,
    #[arg(long, default_value_t = 100)]
    iterations: usize,
    #[arg(long, default_value_t = 30000)]
    wait_timeout_ms: u64,
    #[arg(long, default_value = "watch-bench-agent")]
    agent_id: String,
    #[arg(long, default_value = "watch-bench")]
    skill: String,
    #[arg(long)]
    spawn_server: bool,
    #[arg(long = "watch-output")]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct SuiteArgs {
    #[arg(long, default_value = "configs/expressways.example.toml")]
    config: PathBuf,
    #[arg(long, default_value = "target/debug/expressways-server")]
    server_bin: PathBuf,
    #[arg(long, default_value = "./var/auth/issuer.private")]
    private_key: PathBuf,
    #[arg(long, default_value = "dev")]
    key_id: String,
    #[arg(long, default_value = "local:developer")]
    principal: String,
    #[arg(long, default_value = "expressways")]
    audience: String,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[arg(long, default_value_t = 25)]
    warmup_iterations: usize,
    #[arg(long, default_value_t = 200)]
    broker_iterations: usize,
    #[arg(long, default_value_t = 512)]
    payload_bytes: usize,
    #[arg(long, default_value = "benchmarks")]
    topic: String,
    #[arg(long)]
    spawn_server: bool,
    #[arg(long, default_value = "./var/benchmarks/storage")]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 2_000)]
    message_count: usize,
    #[arg(long, default_value_t = 250)]
    read_batch: usize,
    #[arg(long, default_value_t = 1_048_576)]
    segment_max_bytes: u64,
    #[arg(long, default_value_t = 10)]
    watch_warmup_iterations: usize,
    #[arg(long, default_value_t = 100)]
    watch_iterations: usize,
    #[arg(long, default_value_t = 30000)]
    watch_timeout_ms: u64,
    #[arg(long, default_value = "./var/benchmarks/latest.json")]
    output: PathBuf,
}

#[derive(Debug, Serialize)]
struct TransportBenchmarkReport {
    transport: String,
    iterations: usize,
    payload_bytes: usize,
    total_duration_ms: u128,
    throughput_ops_per_sec: f64,
    average_latency_ms: f64,
    p95_latency_ms: u128,
    max_latency_ms: u128,
}

#[derive(Debug, Serialize)]
struct StorageBenchmarkReport {
    message_count: usize,
    payload_bytes: usize,
    append_duration_ms: u128,
    append_ops_per_sec: f64,
    read_duration_ms: u128,
    read_ops_per_sec: f64,
    segment_count: u64,
    total_bytes: u64,
}

#[derive(Debug, Serialize)]
struct RegistryWatchBenchmarkReport {
    transport: String,
    mode: String,
    iterations: usize,
    wait_timeout_ms: u64,
    total_duration_ms: u128,
    throughput_ops_per_sec: f64,
    average_latency_ms: f64,
    p95_latency_ms: u128,
    max_latency_ms: u128,
}

#[derive(Debug, Serialize)]
struct BenchmarkSuiteReport {
    generated_at: chrono::DateTime<Utc>,
    tcp: Option<TransportBenchmarkReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unix: Option<TransportBenchmarkReport>,
    registry_watch_long_poll: RegistryWatchBenchmarkReport,
    registry_watch_stream: RegistryWatchBenchmarkReport,
    storage: StorageBenchmarkReport,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        CommandKind::Broker(args) => {
            let runtime = tokio::runtime::Runtime::new()?;
            let report = runtime.block_on(run_broker_benchmark(&args))?;
            print_or_write(&report, args.output.as_deref())?;
        }
        CommandKind::RegistryWatch(args) => {
            let runtime = tokio::runtime::Runtime::new()?;
            let report = runtime.block_on(run_registry_watch_benchmark(&args))?;
            print_or_write(&report, args.output.as_deref())?;
        }
        CommandKind::Storage(args) => {
            let report = run_storage_benchmark(&args)?;
            print_or_write(&report, args.output.as_deref())?;
        }
        CommandKind::Suite(args) => {
            let runtime = tokio::runtime::Runtime::new()?;
            let broker_args = BrokerBenchArgs {
                transport: TransportKind::Tcp,
                address: args.address.clone(),
                socket: args.socket.clone(),
                config: args.config.clone(),
                server_bin: args.server_bin.clone(),
                private_key: args.private_key.clone(),
                key_id: args.key_id.clone(),
                principal: args.principal.clone(),
                audience: args.audience.clone(),
                warmup_iterations: args.warmup_iterations,
                iterations: args.broker_iterations,
                payload_bytes: args.payload_bytes,
                topic: args.topic.clone(),
                spawn_server: args.spawn_server,
                output: None,
            };
            let watch_long_poll_args = RegistryWatchBenchArgs {
                transport: TransportKind::Tcp,
                address: args.address.clone(),
                socket: args.socket.clone(),
                config: args.config.clone(),
                server_bin: args.server_bin.clone(),
                private_key: args.private_key.clone(),
                key_id: args.key_id.clone(),
                principal: args.principal.clone(),
                audience: args.audience.clone(),
                mode: WatchMode::LongPoll,
                warmup_iterations: args.watch_warmup_iterations,
                iterations: args.watch_iterations,
                wait_timeout_ms: args.watch_timeout_ms,
                agent_id: "watch-bench-agent".to_owned(),
                skill: "watch-bench".to_owned(),
                spawn_server: args.spawn_server,
                output: None,
            };
            let watch_stream_args = RegistryWatchBenchArgs {
                mode: WatchMode::Stream,
                ..watch_long_poll_args.clone()
            };
            let storage_args = StorageBenchArgs {
                data_dir: args.data_dir.clone(),
                message_count: args.message_count,
                payload_bytes: args.payload_bytes,
                read_batch: args.read_batch,
                segment_max_bytes: args.segment_max_bytes,
                output: None,
            };
            let tcp = runtime.block_on(run_broker_benchmark(&broker_args))?;

            #[cfg(unix)]
            let unix = Some(runtime.block_on(run_broker_benchmark(&BrokerBenchArgs {
                transport: TransportKind::Unix,
                address: args.address.clone(),
                socket: args.socket.clone(),
                config: args.config.clone(),
                server_bin: args.server_bin.clone(),
                private_key: args.private_key.clone(),
                key_id: args.key_id.clone(),
                principal: args.principal.clone(),
                audience: args.audience.clone(),
                warmup_iterations: args.warmup_iterations,
                iterations: args.broker_iterations,
                payload_bytes: args.payload_bytes,
                topic: args.topic.clone(),
                spawn_server: args.spawn_server,
                output: None,
            }))?);
            #[cfg(not(unix))]
            let unix = None;

            let registry_watch_long_poll =
                runtime.block_on(run_registry_watch_benchmark(&watch_long_poll_args))?;
            let registry_watch_stream =
                runtime.block_on(run_registry_watch_benchmark(&watch_stream_args))?;
            let storage = run_storage_benchmark(&storage_args)?;
            let report = BenchmarkSuiteReport {
                generated_at: Utc::now(),
                tcp: Some(tcp),
                unix,
                registry_watch_long_poll,
                registry_watch_stream,
                storage,
            };
            print_or_write(&report, Some(&args.output))?;
        }
    }

    Ok(())
}

async fn run_broker_benchmark(args: &BrokerBenchArgs) -> anyhow::Result<TransportBenchmarkReport> {
    let token = issue_benchmark_token(
        &args.key_id,
        &args.private_key,
        &args.principal,
        &args.audience,
        &args.topic,
    )?;

    let mut launch_artifact = if args.spawn_server {
        let launch = spawn_server(args)?;
        wait_for_server(args, &token).await?;
        Some(launch)
    } else {
        None
    };

    let setup_result = ensure_topic(args, &token).await;
    if let (Err(error), Some(launch)) = (&setup_result, launch_artifact.as_mut()) {
        let _ = stop_server(&mut launch.child);
        if let Some(path) = &launch.temp_config {
            let _ = fs::remove_file(path);
        }
        return Err(anyhow::anyhow!(error.to_string()));
    }
    setup_result?;

    let endpoint = endpoint_for(args);
    let mut client = Client::connect(endpoint.clone())
        .await
        .with_context(|| "failed to connect benchmark client")?;

    let warmup_payload = bench_payload(args.payload_bytes, 0);
    for index in 0..args.warmup_iterations {
        let response = client
            .send(ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::Publish {
                    topic: args.topic.clone(),
                    classification: Some(Classification::Internal),
                    payload: bench_payload(warmup_payload.len(), index),
                },
            })
            .await?;
        ensure_publish_response(response)?;
    }

    let mut latencies = Vec::with_capacity(args.iterations);
    let started_at = Instant::now();
    for index in 0..args.iterations {
        let iteration_started = Instant::now();
        let response = client
            .send(ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::Publish {
                    topic: args.topic.clone(),
                    classification: Some(Classification::Internal),
                    payload: bench_payload(args.payload_bytes, index),
                },
            })
            .await?;
        ensure_publish_response(response)?;
        latencies.push(iteration_started.elapsed());
    }
    let total_duration = started_at.elapsed();

    if let Some(mut launch) = launch_artifact {
        stop_server(&mut launch.child)?;
        if let Some(path) = launch.temp_config.take() {
            let _ = fs::remove_file(path);
        }
    }

    Ok(TransportBenchmarkReport {
        transport: match args.transport {
            TransportKind::Tcp => "tcp".to_owned(),
            TransportKind::Unix => "unix".to_owned(),
        },
        iterations: args.iterations,
        payload_bytes: args.payload_bytes,
        total_duration_ms: total_duration.as_millis(),
        throughput_ops_per_sec: ops_per_sec(args.iterations, total_duration),
        average_latency_ms: average_latency_ms(&latencies),
        p95_latency_ms: percentile_latency_ms(&latencies, 0.95),
        max_latency_ms: latencies.iter().map(Duration::as_millis).max().unwrap_or(0),
    })
}

async fn run_registry_watch_benchmark(
    args: &RegistryWatchBenchArgs,
) -> anyhow::Result<RegistryWatchBenchmarkReport> {
    let token = issue_watch_benchmark_token(
        &args.key_id,
        &args.private_key,
        &args.principal,
        &args.audience,
    )?;

    let mut launch_artifact = if args.spawn_server {
        let launch = spawn_watch_server(args)?;
        wait_for_watch_server(args, &token).await?;
        Some(launch)
    } else {
        None
    };

    let benchmark = match args.mode {
        WatchMode::LongPoll => run_long_poll_watch_benchmark(args, &token).await,
        WatchMode::Stream => run_stream_watch_benchmark(args, &token).await,
    };

    if let Some(mut launch) = launch_artifact.take() {
        let _ = stop_server(&mut launch.child);
        if let Some(path) = launch.temp_config.take() {
            let _ = fs::remove_file(path);
        }
    }

    benchmark
}

async fn run_long_poll_watch_benchmark(
    args: &RegistryWatchBenchArgs,
    token: &str,
) -> anyhow::Result<RegistryWatchBenchmarkReport> {
    let endpoint = watch_endpoint_for(args);
    let mut list_client = Client::connect(endpoint.clone()).await?;
    let mut watch_client = Client::connect(endpoint.clone()).await?;
    let mut mutate_client = Client::connect(endpoint).await?;
    let query = AgentQuery {
        skill: Some(args.skill.clone()),
        topic: None,
        principal: Some(args.principal.clone()),
        include_stale: true,
    };
    let mut cursor = current_watch_cursor(&mut list_client, token, &query).await?;

    for index in 0..args.warmup_iterations {
        let watch_request = ControlRequest {
            capability_token: token.to_owned(),
            command: ControlCommand::WatchAgents {
                query: query.clone(),
                cursor: Some(cursor),
                max_events: 1,
                wait_timeout_ms: args.wait_timeout_ms,
            },
        };
        let register_request =
            register_watch_agent_request(token, args, index, args.warmup_iterations);
        let (watch_response, register_response) = tokio::join!(
            watch_client.send(watch_request),
            mutate_client.send(register_request)
        );
        ensure_agent_registered(register_response?)?;
        cursor = ensure_watch_response(watch_response?, &args.agent_id)?;
    }

    let mut latencies = Vec::with_capacity(args.iterations);
    let started_at = Instant::now();
    for index in 0..args.iterations {
        let watch_request = ControlRequest {
            capability_token: token.to_owned(),
            command: ControlCommand::WatchAgents {
                query: query.clone(),
                cursor: Some(cursor),
                max_events: 1,
                wait_timeout_ms: args.wait_timeout_ms,
            },
        };
        let register_request = register_watch_agent_request(token, args, index, 0);
        let iteration_started = Instant::now();
        let (watch_response, register_response) = tokio::join!(
            watch_client.send(watch_request),
            mutate_client.send(register_request)
        );
        ensure_agent_registered(register_response?)?;
        cursor = ensure_watch_response(watch_response?, &args.agent_id)?;
        latencies.push(iteration_started.elapsed());
    }
    let total_duration = started_at.elapsed();

    Ok(RegistryWatchBenchmarkReport {
        transport: transport_label(args.transport).to_owned(),
        mode: "long_poll".to_owned(),
        iterations: args.iterations,
        wait_timeout_ms: args.wait_timeout_ms,
        total_duration_ms: total_duration.as_millis(),
        throughput_ops_per_sec: ops_per_sec(args.iterations, total_duration),
        average_latency_ms: average_latency_ms(&latencies),
        p95_latency_ms: percentile_latency_ms(&latencies, 0.95),
        max_latency_ms: latencies.iter().map(Duration::as_millis).max().unwrap_or(0),
    })
}

async fn run_stream_watch_benchmark(
    args: &RegistryWatchBenchArgs,
    token: &str,
) -> anyhow::Result<RegistryWatchBenchmarkReport> {
    let endpoint = watch_endpoint_for(args);
    let mut list_client = Client::connect(endpoint.clone()).await?;
    let stream_client = Client::connect(endpoint.clone()).await?;
    let mut stream = stream_client.into_stream();
    let mut mutate_client = Client::connect(endpoint).await?;
    let query = AgentQuery {
        skill: Some(args.skill.clone()),
        topic: None,
        principal: Some(args.principal.clone()),
        include_stale: true,
    };
    let initial_cursor = current_watch_cursor(&mut list_client, token, &query).await?;
    let opened = stream
        .open(ControlRequest {
            capability_token: token.to_owned(),
            command: ControlCommand::OpenAgentWatchStream {
                query: query.clone(),
                cursor: Some(initial_cursor),
                max_events: 1,
                wait_timeout_ms: args.wait_timeout_ms,
            },
        })
        .await?;
    let mut cursor = ensure_stream_opened(opened)?;

    for index in 0..args.warmup_iterations {
        let register_request =
            register_watch_agent_request(token, args, index, args.warmup_iterations);
        let (frame, register_response) = tokio::join!(
            next_registry_event_frame(&mut stream),
            mutate_client.send(register_request)
        );
        ensure_agent_registered(register_response?)?;
        cursor = ensure_stream_event_frame(frame?, &args.agent_id)?;
    }

    let mut latencies = Vec::with_capacity(args.iterations);
    let started_at = Instant::now();
    for index in 0..args.iterations {
        let register_request = register_watch_agent_request(token, args, index, 0);
        let iteration_started = Instant::now();
        let (frame, register_response) = tokio::join!(
            next_registry_event_frame(&mut stream),
            mutate_client.send(register_request)
        );
        ensure_agent_registered(register_response?)?;
        cursor = ensure_stream_event_frame(frame?, &args.agent_id)?;
        latencies.push(iteration_started.elapsed());
    }
    let total_duration = started_at.elapsed();

    let _ = cursor;
    Ok(RegistryWatchBenchmarkReport {
        transport: transport_label(args.transport).to_owned(),
        mode: "stream".to_owned(),
        iterations: args.iterations,
        wait_timeout_ms: args.wait_timeout_ms,
        total_duration_ms: total_duration.as_millis(),
        throughput_ops_per_sec: ops_per_sec(args.iterations, total_duration),
        average_latency_ms: average_latency_ms(&latencies),
        p95_latency_ms: percentile_latency_ms(&latencies, 0.95),
        max_latency_ms: latencies.iter().map(Duration::as_millis).max().unwrap_or(0),
    })
}

fn run_storage_benchmark(args: &StorageBenchArgs) -> anyhow::Result<StorageBenchmarkReport> {
    if args.data_dir.exists() {
        fs::remove_dir_all(&args.data_dir)
            .with_context(|| format!("failed to clear {}", args.data_dir.display()))?;
    }

    let storage = Storage::new(StorageConfig {
        data_dir: args.data_dir.clone(),
        segment_max_bytes: args.segment_max_bytes,
        default_retention_class: RetentionClass::Operational,
        default_classification: Classification::Internal,
        retention_policy: RetentionPolicy {
            ephemeral_max_bytes: 32 * 1024 * 1024,
            operational_max_bytes: 64 * 1024 * 1024,
            regulated_max_bytes: 128 * 1024 * 1024,
        },
        disk_pressure: DiskPressurePolicy {
            max_total_bytes: 256 * 1024 * 1024,
            reclaim_target_bytes: 224 * 1024 * 1024,
        },
    })?;

    storage.ensure_topic(TopicSpec {
        name: "storage-bench".to_owned(),
        retention_class: RetentionClass::Operational,
        default_classification: Classification::Internal,
    })?;

    let append_started = Instant::now();
    for index in 0..args.message_count {
        storage.append(
            "storage-bench",
            "bench",
            Some(Classification::Internal),
            bench_payload(args.payload_bytes, index),
        )?;
    }
    let append_duration = append_started.elapsed();

    let read_started = Instant::now();
    let mut offset = 0u64;
    let mut read_messages = 0usize;
    while read_messages < args.message_count {
        let batch = storage.read_from("storage-bench", offset, args.read_batch)?;
        if batch.is_empty() {
            break;
        }
        read_messages += batch.len();
        offset = batch
            .last()
            .map(|message| message.offset + 1)
            .unwrap_or(offset);
    }
    let read_duration = read_started.elapsed();
    let stats = storage.stats()?;

    Ok(StorageBenchmarkReport {
        message_count: args.message_count,
        payload_bytes: args.payload_bytes,
        append_duration_ms: append_duration.as_millis(),
        append_ops_per_sec: ops_per_sec(args.message_count, append_duration),
        read_duration_ms: read_duration.as_millis(),
        read_ops_per_sec: ops_per_sec(read_messages, read_duration),
        segment_count: stats.segment_count,
        total_bytes: stats.total_bytes,
    })
}

fn issue_benchmark_token(
    key_id: &str,
    private_key: &Path,
    principal: &str,
    audience: &str,
    topic: &str,
) -> anyhow::Result<String> {
    let issuer = CapabilityIssuer::from_private_key_file(key_id.to_owned(), private_key)?;
    let claims = CapabilityClaims {
        token_id: Uuid::now_v7(),
        principal: principal.to_owned(),
        audience: audience.to_owned(),
        issued_at: Utc::now(),
        expires_at: Utc::now() + ChronoDuration::hours(1),
        scopes: vec![
            CapabilityScope {
                resource: "system:broker".to_owned(),
                actions: vec![Action::Health, Action::Admin],
            },
            CapabilityScope {
                resource: format!("topic:{topic}"),
                actions: vec![Action::Admin, Action::Publish, Action::Consume],
            },
        ],
    };

    issuer
        .issue(claims)
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}

fn issue_watch_benchmark_token(
    key_id: &str,
    private_key: &Path,
    principal: &str,
    audience: &str,
) -> anyhow::Result<String> {
    let issuer = CapabilityIssuer::from_private_key_file(key_id.to_owned(), private_key)?;
    let claims = CapabilityClaims {
        token_id: Uuid::now_v7(),
        principal: principal.to_owned(),
        audience: audience.to_owned(),
        issued_at: Utc::now(),
        expires_at: Utc::now() + ChronoDuration::hours(1),
        scopes: vec![
            CapabilityScope {
                resource: "system:broker".to_owned(),
                actions: vec![Action::Health, Action::Admin],
            },
            CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            },
        ],
    };

    issuer
        .issue(claims)
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}

struct ServerLaunch {
    child: Child,
    temp_config: Option<PathBuf>,
}

fn spawn_server(args: &BrokerBenchArgs) -> anyhow::Result<ServerLaunch> {
    let temp_config = render_benchmark_config(args)?;
    let config_path = temp_config.as_deref().unwrap_or(&args.config);
    let mut command = Command::new(&args.server_bin);
    command
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = command
        .spawn()
        .with_context(|| format!("failed to spawn {}", args.server_bin.display()))?;

    Ok(ServerLaunch { child, temp_config })
}

fn spawn_watch_server(args: &RegistryWatchBenchArgs) -> anyhow::Result<ServerLaunch> {
    let temp_config = render_watch_benchmark_config(args)?;
    let config_path = temp_config.as_deref().unwrap_or(&args.config);
    let mut command = Command::new(&args.server_bin);
    command
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = command
        .spawn()
        .with_context(|| format!("failed to spawn {}", args.server_bin.display()))?;

    Ok(ServerLaunch { child, temp_config })
}

async fn wait_for_server(args: &BrokerBenchArgs, token: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Client::connect(endpoint_for(args)).await {
            Ok(mut client) => {
                let response = client
                    .send(ControlRequest {
                        capability_token: token.to_owned(),
                        command: ControlCommand::Health,
                    })
                    .await;
                if let Ok(ControlResponse::Health { .. }) = response {
                    return Ok(());
                }
            }
            Err(_) => {}
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for benchmark server to become ready");
        }

        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn wait_for_watch_server(args: &RegistryWatchBenchArgs, token: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Client::connect(watch_endpoint_for(args)).await {
            Ok(mut client) => {
                let response = client
                    .send(ControlRequest {
                        capability_token: token.to_owned(),
                        command: ControlCommand::Health,
                    })
                    .await;
                if let Ok(ControlResponse::Health { .. }) = response {
                    return Ok(());
                }
            }
            Err(_) => {}
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for benchmark server to become ready");
        }

        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn ensure_topic(args: &BrokerBenchArgs, token: &str) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint_for(args))
        .await
        .with_context(|| "failed to connect benchmark setup client")?;
    let response = client
        .send(ControlRequest {
            capability_token: token.to_owned(),
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: args.topic.clone(),
                    retention_class: RetentionClass::Operational,
                    default_classification: Classification::Internal,
                },
            },
        })
        .await?;

    match response {
        ControlResponse::TopicCreated { .. } => Ok(()),
        ControlResponse::Error { code, message } if code == "storage_error" => {
            if message.contains("does not exist") || message.contains("invalid") {
                bail!(message)
            }
            Ok(())
        }
        ControlResponse::Error { message: _, .. } => Ok(()),
        other => bail!("unexpected setup response: {other:?}"),
    }
}

fn endpoint_for(args: &BrokerBenchArgs) -> Endpoint {
    match args.transport {
        TransportKind::Tcp => Endpoint::Tcp(args.address.clone()),
        TransportKind::Unix => {
            #[cfg(unix)]
            {
                Endpoint::Unix(args.socket.clone())
            }
            #[cfg(not(unix))]
            {
                let _ = args;
                unreachable!("unix transport is only compiled on unix");
            }
        }
    }
}

fn watch_endpoint_for(args: &RegistryWatchBenchArgs) -> Endpoint {
    match args.transport {
        TransportKind::Tcp => Endpoint::Tcp(args.address.clone()),
        TransportKind::Unix => {
            #[cfg(unix)]
            {
                Endpoint::Unix(args.socket.clone())
            }
            #[cfg(not(unix))]
            {
                let _ = args;
                unreachable!("unix transport is only compiled on unix");
            }
        }
    }
}

fn render_benchmark_config(args: &BrokerBenchArgs) -> anyhow::Result<Option<PathBuf>> {
    let raw = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let mut value: toml::Value =
        toml::from_str(&raw).context("failed to parse benchmark config")?;
    if let Some(profiles) = value
        .get_mut("quotas")
        .and_then(|quotas| quotas.get_mut("profiles"))
        .and_then(toml::Value::as_array_mut)
    {
        for profile in profiles {
            if let Some(table) = profile.as_table_mut() {
                table.insert(
                    "publish_payload_max_bytes".to_owned(),
                    toml::Value::Integer((args.payload_bytes.max(16_384)) as i64),
                );
                table.insert(
                    "publish_requests_per_window".to_owned(),
                    toml::Value::Integer((args.iterations.max(5000)) as i64),
                );
                table.insert(
                    "consume_requests_per_window".to_owned(),
                    toml::Value::Integer((args.iterations.max(5000)) as i64),
                );
                table.insert(
                    "consume_max_limit".to_owned(),
                    toml::Value::Integer((args.iterations.max(5000)) as i64),
                );
            }
        }
    }
    let server = value
        .get_mut("server")
        .and_then(toml::Value::as_table_mut)
        .context("benchmark config is missing a [server] table")?;

    match args.transport {
        TransportKind::Tcp => {
            server.insert(
                "transport".to_owned(),
                toml::Value::String("tcp".to_owned()),
            );
            server.insert(
                "listen_addr".to_owned(),
                toml::Value::String(args.address.clone()),
            );
        }
        TransportKind::Unix => {
            server.insert(
                "transport".to_owned(),
                toml::Value::String("unix".to_owned()),
            );
            server.insert(
                "socket_path".to_owned(),
                toml::Value::String(args.socket.display().to_string()),
            );
        }
    }

    let rendered = toml::to_string_pretty(&value).context("failed to render benchmark config")?;
    let path = std::env::temp_dir().join(format!(
        "expressways-bench-{}-{}.toml",
        match args.transport {
            TransportKind::Tcp => "tcp",
            TransportKind::Unix => "unix",
        },
        Uuid::now_v7()
    ));
    fs::write(&path, rendered).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(Some(path))
}

fn render_watch_benchmark_config(args: &RegistryWatchBenchArgs) -> anyhow::Result<Option<PathBuf>> {
    let raw = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let mut value: toml::Value =
        toml::from_str(&raw).context("failed to parse benchmark config")?;
    let server = value
        .get_mut("server")
        .and_then(toml::Value::as_table_mut)
        .context("benchmark config is missing a [server] table")?;

    match args.transport {
        TransportKind::Tcp => {
            server.insert(
                "transport".to_owned(),
                toml::Value::String("tcp".to_owned()),
            );
            server.insert(
                "listen_addr".to_owned(),
                toml::Value::String(args.address.clone()),
            );
        }
        TransportKind::Unix => {
            server.insert(
                "transport".to_owned(),
                toml::Value::String("unix".to_owned()),
            );
            server.insert(
                "socket_path".to_owned(),
                toml::Value::String(args.socket.display().to_string()),
            );
        }
    }

    let rendered = toml::to_string_pretty(&value).context("failed to render benchmark config")?;
    let path = std::env::temp_dir().join(format!(
        "expressways-watch-bench-{}-{}.toml",
        transport_label(args.transport),
        Uuid::now_v7()
    ));
    fs::write(&path, rendered).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(Some(path))
}

fn ensure_publish_response(response: ControlResponse) -> anyhow::Result<()> {
    match response {
        ControlResponse::PublishAccepted { .. } => Ok(()),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected benchmark response: {other:?}"),
    }
}

fn ensure_agent_registered(response: ControlResponse) -> anyhow::Result<()> {
    match response {
        ControlResponse::AgentRegistered { .. } => Ok(()),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected registry mutation response: {other:?}"),
    }
}

fn ensure_watch_response(response: ControlResponse, agent_id: &str) -> anyhow::Result<u64> {
    match response {
        ControlResponse::RegistryEvents { events, cursor, .. } => {
            let event = events
                .into_iter()
                .find(|event| event.card.agent_id == agent_id)
                .context("watch response did not contain the benchmark agent event")?;
            Ok(cursor.max(event.sequence))
        }
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected watch response: {other:?}"),
    }
}

fn ensure_stream_opened(frame: StreamFrame) -> anyhow::Result<u64> {
    match frame {
        StreamFrame::AgentWatchOpened { cursor } => Ok(cursor),
        StreamFrame::StreamError { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected stream opening frame: {other:?}"),
    }
}

fn ensure_stream_event_frame(frame: StreamFrame, agent_id: &str) -> anyhow::Result<u64> {
    match frame {
        StreamFrame::RegistryEvents { events, cursor } => {
            let event = events
                .into_iter()
                .find(|event| event.card.agent_id == agent_id)
                .context("stream frame did not contain the benchmark agent event")?;
            Ok(cursor.max(event.sequence))
        }
        StreamFrame::StreamError { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected stream event frame: {other:?}"),
    }
}

async fn current_watch_cursor(
    client: &mut Client,
    token: &str,
    query: &AgentQuery,
) -> anyhow::Result<u64> {
    let response = client
        .send(ControlRequest {
            capability_token: token.to_owned(),
            command: ControlCommand::ListAgents {
                query: query.clone(),
            },
        })
        .await?;

    match response {
        ControlResponse::Agents { cursor, .. } => Ok(cursor),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected list-agents response: {other:?}"),
    }
}

fn register_watch_agent_request(
    token: &str,
    args: &RegistryWatchBenchArgs,
    index: usize,
    offset: usize,
) -> ControlRequest {
    let version_index = index + offset;
    ControlRequest {
        capability_token: token.to_owned(),
        command: ControlCommand::RegisterAgent {
            registration: expressways_protocol::AgentRegistration {
                agent_id: args.agent_id.clone(),
                display_name: "Watch Benchmark Agent".to_owned(),
                version: format!("1.0.{version_index}"),
                summary: format!("watch benchmark iteration {version_index}"),
                skills: vec![args.skill.clone()],
                subscriptions: vec!["topic:bench-input".to_owned()],
                publications: vec!["topic:bench-output".to_owned()],
                schemas: Vec::new(),
                endpoint: expressways_protocol::AgentEndpoint {
                    transport: "control_tcp".to_owned(),
                    address: "127.0.0.1:9901".to_owned(),
                },
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                ttl_seconds: Some(300),
            },
        },
    }
}

async fn next_registry_event_frame(stream: &mut StreamClient) -> anyhow::Result<StreamFrame> {
    loop {
        match stream.next_frame().await? {
            Some(StreamFrame::KeepAlive { .. }) => continue,
            Some(frame) => return Ok(frame),
            None => bail!("watch stream closed before delivering a registry event"),
        }
    }
}

fn transport_label(transport: TransportKind) -> &'static str {
    match transport {
        TransportKind::Tcp => "tcp",
        TransportKind::Unix => "unix",
    }
}

fn bench_payload(payload_bytes: usize, index: usize) -> String {
    let seed = format!("bench-{index:08x}-");
    if payload_bytes <= seed.len() {
        return seed[..payload_bytes].to_owned();
    }

    let remaining = payload_bytes - seed.len();
    format!("{seed}{}", "x".repeat(remaining))
}

fn ops_per_sec(count: usize, duration: Duration) -> f64 {
    if duration.is_zero() {
        return count as f64;
    }
    count as f64 / duration.as_secs_f64()
}

fn average_latency_ms(latencies: &[Duration]) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let total_ms = latencies.iter().map(Duration::as_secs_f64).sum::<f64>() * 1000.0;
    total_ms / latencies.len() as f64
}

fn percentile_latency_ms(latencies: &[Duration], percentile: f64) -> u128 {
    if latencies.is_empty() {
        return 0;
    }

    let mut sorted = latencies
        .iter()
        .map(Duration::as_millis)
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    let index = ((sorted.len() as f64 - 1.0) * percentile).round() as usize;
    sorted[index]
}

fn print_or_write<T: Serialize>(value: &T, output: Option<&Path>) -> anyhow::Result<()> {
    let rendered = serde_json::to_vec_pretty(value)?;
    if let Some(output) = output {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(output, rendered)
            .with_context(|| format!("failed to write {}", output.display()))?;
    } else {
        println!("{}", String::from_utf8(rendered)?);
    }
    Ok(())
}

fn stop_server(child: &mut Child) -> anyhow::Result<()> {
    child.kill().context("failed to stop benchmark server")?;
    let _ = child.wait();
    thread::sleep(Duration::from_millis(150));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_payload_has_expected_size() {
        let payload = bench_payload(64, 7);
        assert_eq!(payload.len(), 64);
    }

    #[test]
    fn percentile_latency_picks_high_percentile_value() {
        let samples = vec![
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(10),
        ];
        assert_eq!(percentile_latency_ms(&samples, 0.95), 10);
    }
}
