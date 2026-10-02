use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
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

pub const CURRENT_CONFIG_SCHEMA_VERSION: u32 = 1;
const LEGACY_CONFIG_SCHEMA_VERSION: u32 = 0;
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ConfigSchemaSection {
    #[serde(default)]
    version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfigLoadReport {
    pub source_schema_version: u32,
    pub target_schema_version: u32,
    pub migrated: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct AppConfigEnvelope {
    #[serde(default)]
    schema: ConfigSchemaSection,
    #[serde(flatten)]
    app: AppConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub node_name: String,
    pub transport: TransportKind,
    pub listen_addr: Option<String>,
    pub socket_path: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub log_level: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: usize,
    #[serde(default = "default_connection_idle_timeout_ms")]
    pub connection_idle_timeout_ms: u64,
}

fn default_max_connections() -> usize {
    256
}

fn default_max_frame_bytes() -> usize {
    8 * 1024 * 1024
}

fn default_connection_idle_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    Tcp,
    Unix,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct AuditSection {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
    pub fn load_with_report(path: &Path) -> anyhow::Result<(Self, AppConfigLoadReport)> {
        let raw = read_bounded_config(path)
            .with_context(|| format!("failed to read config at {}", path.display()))?;
        let raw = std::str::from_utf8(&raw).context("config is not valid UTF-8")?;
        let envelope: AppConfigEnvelope =
            toml::from_str(raw).context("failed to parse TOML config")?;
        let source_schema_version = envelope.schema.version;
        if source_schema_version > CURRENT_CONFIG_SCHEMA_VERSION {
            bail!(
                "config schema version {} is newer than supported version {}",
                source_schema_version,
                CURRENT_CONFIG_SCHEMA_VERSION
            );
        }
        let migrated = source_schema_version == LEGACY_CONFIG_SCHEMA_VERSION;
        let report = AppConfigLoadReport {
            source_schema_version,
            target_schema_version: CURRENT_CONFIG_SCHEMA_VERSION,
            migrated,
        };
        Ok((envelope.app, report))
    }
}

fn read_bounded_config(path: &Path) -> std::io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "config path is not a regular file",
        ));
    }
    if metadata.len() > MAX_CONFIG_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "config is {} bytes, exceeding the {MAX_CONFIG_BYTES} byte limit",
                metadata.len()
            ),
        ));
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "config is too large to read",
        )
    })?;
    let mut raw = Vec::with_capacity(capacity);
    File::open(path)?
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut raw)?;
    if raw.len() as u64 > MAX_CONFIG_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "config grew beyond its size limit while being read",
        ));
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use uuid::Uuid;

    fn temp_config_path() -> PathBuf {
        std::env::temp_dir().join(format!("expressways-config-{}.toml", Uuid::now_v7()))
    }

    fn write_config(contents: &str) -> PathBuf {
        let path = temp_config_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        let mut file = fs::File::create(&path).expect("create config");
        file.write_all(contents.as_bytes()).expect("write config");
        path
    }

    fn minimal_config(schema_section: &str) -> String {
        format!(
            r#"{schema_section}
[server]
node_name = "dev-node"
transport = "tcp"
listen_addr = "127.0.0.1:7766"
data_dir = "./var/data"
log_level = "info"

[storage]
segment_max_bytes = 1048576
retention_class = "operational"
default_classification = "internal"
ephemeral_retention_bytes = 4194304
operational_retention_bytes = 16777216
regulated_retention_bytes = 67108864
max_total_bytes = 134217728
reclaim_target_bytes = 117440512

[audit]
path = "./var/audit/audit.jsonl"

[auth]
audience = "expressways"
revocation_path = "./var/auth/revocations.json"

[[auth.issuers]]
key_id = "dev"
public_key_path = "./var/auth/issuer.public"
status = "active"

[[auth.principals]]
id = "local:developer"
kind = "developer"
display_name = "Local Developer"
status = "active"
allowed_key_ids = ["dev"]
quota_profile = "operator"

[quotas]

[[quotas.profiles]]
name = "operator"
publish_payload_max_bytes = 16384
publish_requests_per_window = 20
publish_window_seconds = 1
consume_max_limit = 100
consume_requests_per_window = 20
consume_window_seconds = 1
backpressure_mode = "reject"
backpressure_delay_ms = 0

[policy]
default_decision = "deny"
"#
        )
    }

    #[test]
    fn load_marks_legacy_configs_as_migrated() {
        let path = write_config(&minimal_config(""));
        let (_, report) = AppConfig::load_with_report(&path).expect("load config");
        assert_eq!(report.source_schema_version, LEGACY_CONFIG_SCHEMA_VERSION);
        assert_eq!(report.target_schema_version, CURRENT_CONFIG_SCHEMA_VERSION);
        assert!(report.migrated);
    }

    #[test]
    fn load_accepts_current_schema_version_without_migration() {
        let path = write_config(&minimal_config(
            r#"[schema]
version = 1
"#,
        ));
        let (config, report) = AppConfig::load_with_report(&path).expect("load config");
        assert_eq!(report.source_schema_version, CURRENT_CONFIG_SCHEMA_VERSION);
        assert_eq!(report.target_schema_version, CURRENT_CONFIG_SCHEMA_VERSION);
        assert!(!report.migrated);
        assert_eq!(config.server.max_connections, 256);
        assert_eq!(config.server.max_frame_bytes, 8 * 1024 * 1024);
        assert_eq!(config.server.connection_idle_timeout_ms, 30_000);
    }

    #[test]
    fn load_rejects_newer_schema_versions() {
        let path = write_config(&minimal_config(
            r#"[schema]
version = 99
"#,
        ));
        let error = AppConfig::load_with_report(&path).expect_err("expected schema error");
        assert!(
            error
                .to_string()
                .contains("config schema version 99 is newer than supported version 1")
        );
    }

    #[test]
    fn load_rejects_oversized_and_non_utf8_configs_before_parsing() {
        let oversized = temp_config_path();
        fs::File::create(&oversized)
            .expect("create oversized config")
            .set_len(MAX_CONFIG_BYTES + 1)
            .expect("make sparse oversized config");
        let error = AppConfig::load_with_report(&oversized)
            .expect_err("oversized config should fail before parsing");
        assert!(format!("{error:#}").contains("exceeding the 1048576 byte limit"));

        let non_utf8 = temp_config_path();
        fs::write(&non_utf8, [0xff, 0xfe]).expect("write non-UTF-8 config");
        let error =
            AppConfig::load_with_report(&non_utf8).expect_err("non-UTF-8 config should fail");
        assert!(error.to_string().contains("config is not valid UTF-8"));
    }

    #[test]
    fn load_rejects_unknown_leaf_fields() {
        for (needle, replacement, typo) in [
            (
                "log_level = \"info\"",
                "log_level = \"info\"\nmax_conections = 999",
                "max_conections",
            ),
            (
                "audience = \"expressways\"",
                "audience = \"expressways\"\naudence = \"wrong\"",
                "audence",
            ),
            (
                "name = \"operator\"",
                "name = \"operator\"\nconsume_max_limt = 999",
                "consume_max_limt",
            ),
            (
                "default_decision = \"deny\"",
                "default_decision = \"deny\"\ndefault_allow = true",
                "default_allow",
            ),
        ] {
            let path = write_config(&minimal_config("").replace(needle, replacement));
            let error = AppConfig::load_with_report(&path).expect_err("typoed field must fail");
            assert!(format!("{error:#}").contains(&format!("unknown field `{typo}`")));
        }
    }

    #[cfg(unix)]
    #[test]
    fn load_rejects_symlinked_config_files() {
        use std::os::unix::fs::symlink;

        let target = write_config(&minimal_config(""));
        let link = temp_config_path();
        symlink(target, &link).expect("create config symlink");
        let error = AppConfig::load_with_report(&link).expect_err("symlinked config must fail");
        assert!(format!("{error:#}").contains("config path is not a regular file"));
    }
}
