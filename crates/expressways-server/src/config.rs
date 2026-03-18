use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use expressways_auth::AuthConfig;
use expressways_policy::PolicyConfig;
use expressways_protocol::{Classification, RetentionClass};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageSection,
    pub audit: AuditSection,
    pub auth: AuthConfig,
    pub quotas: crate::quota::QuotaConfig,
    #[serde(default)]
    pub registry: RegistrySection,
    #[serde(default)]
    pub resilience: ResilienceSection,
    #[serde(default)]
    pub adopters: AdoptersSection,
    pub policy: PolicyConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub node_name: String,
    pub transport: TransportKind,
    pub listen_addr: Option<String>,
    pub socket_path: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub log_level: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    Tcp,
    Unix,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageSection {
    pub segment_max_bytes: u64,
    pub retention_class: RetentionClass,
    pub default_classification: Classification,
    pub ephemeral_retention_bytes: u64,
    pub operational_retention_bytes: u64,
    pub regulated_retention_bytes: u64,
    pub max_total_bytes: u64,
    pub reclaim_target_bytes: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuditSection {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResilienceSection {
    #[serde(default = "default_allow_degraded_startup")]
    pub allow_degraded_startup: bool,
    #[serde(default = "default_allow_degraded_runtime")]
    pub allow_degraded_runtime: bool,
    #[serde(default = "default_audit_retry_attempts")]
    pub audit_retry_attempts: u32,
    #[serde(default = "default_audit_retry_backoff_ms")]
    pub audit_retry_backoff_ms: u64,
    #[serde(default = "default_listener_retry_delay_ms")]
    pub listener_retry_delay_ms: u64,
}

impl Default for ResilienceSection {
    fn default() -> Self {
        Self {
            allow_degraded_startup: default_allow_degraded_startup(),
            allow_degraded_runtime: default_allow_degraded_runtime(),
            audit_retry_attempts: default_audit_retry_attempts(),
            audit_retry_backoff_ms: default_audit_retry_backoff_ms(),
            listener_retry_delay_ms: default_listener_retry_delay_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdoptersSection {
    #[serde(default)]
    pub enabled: Vec<String>,
    #[serde(default = "default_adopter_probe_interval_seconds")]
    pub probe_interval_seconds: u64,
    #[serde(default = "default_adopter_require_installed")]
    pub require_installed: bool,
    #[serde(default)]
    pub packages: BTreeMap<String, toml::Table>,
}

impl Default for AdoptersSection {
    fn default() -> Self {
        Self {
            enabled: Vec::new(),
            probe_interval_seconds: default_adopter_probe_interval_seconds(),
            require_installed: default_adopter_require_installed(),
            packages: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegistrySection {
    #[serde(default)]
    pub backend: RegistryBackend,
    pub path: Option<PathBuf>,
    #[serde(default = "default_registry_ttl_seconds")]
    pub default_ttl_seconds: u64,
    #[serde(default = "default_registry_event_history_limit")]
    pub event_history_limit: usize,
    #[serde(default = "default_registry_stream_send_timeout_ms")]
    pub stream_send_timeout_ms: u64,
    #[serde(default = "default_registry_stream_idle_keepalive_limit")]
    pub stream_idle_keepalive_limit: u64,
}

impl Default for RegistrySection {
    fn default() -> Self {
        Self {
            backend: RegistryBackend::File,
            path: None,
            default_ttl_seconds: default_registry_ttl_seconds(),
            event_history_limit: default_registry_event_history_limit(),
            stream_send_timeout_ms: default_registry_stream_send_timeout_ms(),
            stream_idle_keepalive_limit: default_registry_stream_idle_keepalive_limit(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RegistryBackend {
    #[default]
    File,
}

fn default_registry_ttl_seconds() -> u64 {
    300
}

fn default_registry_event_history_limit() -> usize {
    1024
}

fn default_registry_stream_send_timeout_ms() -> u64 {
    1_000
}

fn default_registry_stream_idle_keepalive_limit() -> u64 {
    12
}

fn default_allow_degraded_startup() -> bool {
    true
}

fn default_allow_degraded_runtime() -> bool {
    true
}

fn default_audit_retry_attempts() -> u32 {
    3
}

fn default_audit_retry_backoff_ms() -> u64 {
    50
}

fn default_listener_retry_delay_ms() -> u64 {
    250
}

fn default_adopter_probe_interval_seconds() -> u64 {
    30
}

fn default_adopter_require_installed() -> bool {
    true
}

impl AppConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config at {}", path.display()))?;
        toml::from_str(&raw).context("failed to parse TOML config")
    }
}
