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

impl AppConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config at {}", path.display()))?;
        toml::from_str(&raw).context("failed to parse TOML config")
    }
}
