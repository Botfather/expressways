use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;

use chrono::{Duration, Utc};
use expressways_protocol::{AgentCard, AgentQuery, AgentRegistration};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const REGISTRY_SCHEMA_VERSION: u32 = 1;
const LEGACY_REGISTRY_SCHEMA_VERSION: u32 = 0;
const MAX_REGISTRATION_BYTES: usize = 64 * 1024;
const MAX_REGISTRY_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_REGISTRY_AGENTS: usize = 10_000;
const MAX_AGENT_ID_BYTES: usize = 128;
const MAX_PRINCIPAL_BYTES: usize = 256;
const MAX_DISPLAY_NAME_BYTES: usize = 256;
const MAX_VERSION_BYTES: usize = 128;
const MAX_SUMMARY_BYTES: usize = 4 * 1024;
const MAX_ENDPOINT_TRANSPORT_BYTES: usize = 64;
const MAX_ENDPOINT_ADDRESS_BYTES: usize = 2 * 1024;
const MAX_DISCOVERY_VALUES: usize = 128;
const MAX_DISCOVERY_VALUE_BYTES: usize = 256;
const MAX_SCHEMAS: usize = 64;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("agent_id must not be empty")]
    MissingAgentId,
    #[error("display_name must not be empty")]
    MissingDisplayName,
    #[error("version must not be empty")]
    MissingVersion,
    #[error("endpoint transport must not be empty")]
    MissingEndpointTransport,
    #[error("endpoint address must not be empty")]
    MissingEndpointAddress,
    #[error("agent `{agent_id}` is owned by `{owner}`")]
    OwnershipConflict { agent_id: String, owner: String },
    #[error("agent `{0}` is not registered")]
    NotFound(String),
    #[error("ttl_seconds must be greater than zero and fit in the supported timestamp range")]
    InvalidTtl,
    #[error("agent registration is {bytes} bytes; maximum is {max_bytes} bytes")]
    RegistrationTooLarge { bytes: usize, max_bytes: usize },
    #[error("registry file is {bytes} bytes; maximum is {max_bytes} bytes")]
    RegistryFileTooLarge { bytes: u64, max_bytes: u64 },
    #[error("registry contains {count} agents; maximum is {max_agents}")]
    TooManyAgents { count: usize, max_agents: usize },
    #[error("{field} is {bytes} bytes; maximum is {max_bytes} bytes")]
    FieldTooLong {
        field: &'static str,
        bytes: usize,
        max_bytes: usize,
    },
    #[error("{field} contains {count} entries; maximum is {max_entries}")]
    TooManyEntries {
        field: &'static str,
        count: usize,
        max_entries: usize,
    },
    #[error("registry contains duplicate agent_id `{0}`")]
    DuplicateAgentId(String),
    #[error("stored agent `{agent_id}` is invalid: {reason}")]
    InvalidStoredAgent { agent_id: String, reason: String },
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("registry schema version {found} is newer than supported version {supported}")]
    UnsupportedSchemaVersion { found: u32, supported: u32 },
}

pub trait RegistryStore: std::fmt::Debug + Send + Sync {
    fn load_agents(&self) -> Result<Vec<AgentCard>, RegistryError>;
    fn save_agents(&self, agents: &[AgentCard]) -> Result<(), RegistryError>;
}

#[derive(Debug)]
pub struct FileRegistryStore {
    path: PathBuf,
    cache: Mutex<Option<CachedRegistry>>,
}

#[derive(Debug, Clone)]
struct CachedRegistry {
    fingerprint: FileFingerprint,
    agents: Vec<AgentCard>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl FileRegistryStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            cache: Mutex::new(None),
        }
    }
}

impl RegistryStore for FileRegistryStore {
    fn load_agents(&self) -> Result<Vec<AgentCard>, RegistryError> {
        let initial_metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                *self
                    .cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                return Ok(Vec::new());
            }
            Err(error) => return Err(error.into()),
        };
        if !initial_metadata.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "registry path is not a regular file",
            )
            .into());
        }

        let path_fingerprint = file_fingerprint(&initial_metadata);
        if let Some(agents) = self.cached_agents(&path_fingerprint) {
            return Ok(agents);
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(&self.path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "registry path is not a regular file",
            )
            .into());
        }
        let fingerprint = file_fingerprint(&metadata);
        if let Some(agents) = self.cached_agents(&fingerprint) {
            return Ok(agents);
        }
        let bytes = metadata.len();
        if bytes > MAX_REGISTRY_FILE_BYTES {
            return Err(RegistryError::RegistryFileTooLarge {
                bytes,
                max_bytes: MAX_REGISTRY_FILE_BYTES,
            });
        }
        let mut raw = String::with_capacity(usize::try_from(bytes).unwrap_or(0));
        file.take(MAX_REGISTRY_FILE_BYTES + 1)
            .read_to_string(&mut raw)?;
        if u64::try_from(raw.len()).unwrap_or(u64::MAX) > MAX_REGISTRY_FILE_BYTES {
            return Err(RegistryError::RegistryFileTooLarge {
                bytes: u64::try_from(raw.len()).unwrap_or(u64::MAX),
                max_bytes: MAX_REGISTRY_FILE_BYTES,
            });
        }
        let mut document: RegistryDocument = serde_json::from_str(&raw)?;
        if document.agents.len() > MAX_REGISTRY_AGENTS {
            return Err(RegistryError::TooManyAgents {
                count: document.agents.len(),
                max_agents: MAX_REGISTRY_AGENTS,
            });
        }
        validate_stored_agents(&document.agents)?;
        let migrated = migrate_registry_document(&mut document)?;
        if migrated {
            self.save_document(&document)?;
        }
        let fingerprint = if migrated {
            file_fingerprint(&fs::metadata(&self.path)?)
        } else {
            fingerprint
        };
        self.cache_agents(fingerprint, document.agents.clone());
        Ok(document.agents)
    }

    fn save_agents(&self, agents: &[AgentCard]) -> Result<(), RegistryError> {
        if agents.len() > MAX_REGISTRY_AGENTS {
            return Err(RegistryError::TooManyAgents {
                count: agents.len(),
                max_agents: MAX_REGISTRY_AGENTS,
            });
        }
        validate_stored_agents(agents)?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let document = RegistryDocument {
            schema_version: REGISTRY_SCHEMA_VERSION,
            agents: agents.to_vec(),
        };
        self.save_document(&document)?;
        self.cache_agents(
            file_fingerprint(&fs::metadata(&self.path)?),
            agents.to_vec(),
        );
        Ok(())
    }
}

impl FileRegistryStore {
    fn cached_agents(&self, fingerprint: &FileFingerprint) -> Option<Vec<AgentCard>> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .filter(|cached| &cached.fingerprint == fingerprint)
            .map(|cached| cached.agents.clone())
    }

    fn cache_agents(&self, fingerprint: FileFingerprint, agents: Vec<AgentCard>) {
        *self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CachedRegistry {
            fingerprint,
            agents,
        });
    }

    fn save_document(&self, document: &RegistryDocument) -> Result<(), RegistryError> {
        let raw = serde_json::to_vec_pretty(&document)?;
        if u64::try_from(raw.len()).unwrap_or(u64::MAX) > MAX_REGISTRY_FILE_BYTES {
            return Err(RegistryError::RegistryFileTooLarge {
                bytes: u64::try_from(raw.len()).unwrap_or(u64::MAX),
                max_bytes: MAX_REGISTRY_FILE_BYTES,
            });
        }
        let temp_path = self.path.with_extension(format!("tmp-{}", Uuid::now_v7()));
        let result = (|| -> Result<(), std::io::Error> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp_path)?;
            file.write_all(&raw)?;
            file.sync_all()?;
            fs::rename(&temp_path, &self.path)?;
            #[cfg(unix)]
            if let Some(parent) = self.path.parent() {
                File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result.map_err(RegistryError::Io)
    }
}

fn file_fingerprint(metadata: &fs::Metadata) -> FileFingerprint {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    FileFingerprint {
        len: metadata.len(),
        modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    }
}

#[derive(Debug)]
pub struct AgentRegistry {
    store: Box<dyn RegistryStore>,
    default_ttl_seconds: u64,
    mutation_lock: Mutex<()>,
}

impl AgentRegistry {
    pub fn file(path: PathBuf, default_ttl_seconds: u64) -> Self {
        Self {
            store: Box::new(FileRegistryStore::new(path)),
            default_ttl_seconds,
            mutation_lock: Mutex::new(()),
        }
    }

    pub fn register(
        &self,
        principal: &str,
        registration: AgentRegistration,
    ) -> Result<AgentCard, RegistryError> {
        validate_registration(&registration, self.default_ttl_seconds)?;
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut agents = self.store.load_agents()?;
        let card = build_card(principal, registration, self.default_ttl_seconds)?;

        if let Some(existing) = agents
            .iter_mut()
            .find(|candidate| candidate.agent_id == card.agent_id)
        {
            if existing.principal != principal {
                return Err(RegistryError::OwnershipConflict {
                    agent_id: existing.agent_id.clone(),
                    owner: existing.principal.clone(),
                });
            }
            *existing = card.clone();
        } else if agents.len() < MAX_REGISTRY_AGENTS {
            agents.push(card.clone());
        } else {
            return Err(RegistryError::TooManyAgents {
                count: agents.len(),
                max_agents: MAX_REGISTRY_AGENTS,
            });
        }

        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        self.store.save_agents(&agents)?;
        Ok(card)
    }

    pub fn list(&self, query: &AgentQuery) -> Result<Vec<AgentCard>, RegistryError> {
        let mut agents = self.store.load_agents()?;
        agents.retain(|agent| matches_query(agent, query, Utc::now()));
        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        Ok(agents)
    }

    pub fn heartbeat(
        &self,
        principal: &str,
        principal_kind: &str,
        agent_id: &str,
    ) -> Result<AgentCard, RegistryError> {
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut agents = self.store.load_agents()?;
        let card = agents
            .iter_mut()
            .find(|candidate| candidate.agent_id == agent_id)
            .ok_or_else(|| RegistryError::NotFound(agent_id.to_owned()))?;

        if card.principal != principal && principal_kind != "developer" {
            return Err(RegistryError::OwnershipConflict {
                agent_id: agent_id.to_owned(),
                owner: card.principal.clone(),
            });
        }

        let now = Utc::now();
        card.last_seen_at = now;
        card.updated_at = now;
        card.expires_at = expires_at(now, card.ttl_seconds)?;
        let updated = card.clone();

        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        self.store.save_agents(&agents)?;
        Ok(updated)
    }

    pub fn cleanup_stale(&self) -> Result<Vec<AgentCard>, RegistryError> {
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut agents = self.store.load_agents()?;
        let now = Utc::now();
        let mut removed_cards = Vec::new();
        agents.retain(|agent| {
            let expired = is_stale(agent, now);
            if expired {
                removed_cards.push(agent.clone());
            }
            !expired
        });
        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        self.store.save_agents(&agents)?;
        Ok(removed_cards)
    }

    pub fn remove(
        &self,
        principal: &str,
        principal_kind: &str,
        agent_id: &str,
    ) -> Result<AgentCard, RegistryError> {
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut agents = self.store.load_agents()?;
        let index = agents
            .iter()
            .position(|candidate| candidate.agent_id == agent_id)
            .ok_or_else(|| RegistryError::NotFound(agent_id.to_owned()))?;

        let owner = agents[index].principal.clone();
        if owner != principal && principal_kind != "developer" {
            return Err(RegistryError::OwnershipConflict {
                agent_id: agent_id.to_owned(),
                owner,
            });
        }

        let removed = agents.remove(index);
        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        self.store.save_agents(&agents)?;
        Ok(removed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryDocument {
    #[serde(default = "default_schema_version")]
    schema_version: u32,
    #[serde(default)]
    agents: Vec<AgentCard>,
}

fn default_schema_version() -> u32 {
    LEGACY_REGISTRY_SCHEMA_VERSION
}

fn migrate_registry_document(document: &mut RegistryDocument) -> Result<bool, RegistryError> {
    match document.schema_version {
        LEGACY_REGISTRY_SCHEMA_VERSION => {
            document.schema_version = REGISTRY_SCHEMA_VERSION;
            Ok(true)
        }
        REGISTRY_SCHEMA_VERSION => Ok(false),
        found => Err(RegistryError::UnsupportedSchemaVersion {
            found,
            supported: REGISTRY_SCHEMA_VERSION,
        }),
    }
}

fn validate_registration(
    registration: &AgentRegistration,
    default_ttl_seconds: u64,
) -> Result<(), RegistryError> {
    if registration.agent_id.trim().is_empty() {
        return Err(RegistryError::MissingAgentId);
    }
    if registration.display_name.trim().is_empty() {
        return Err(RegistryError::MissingDisplayName);
    }
    if registration.version.trim().is_empty() {
        return Err(RegistryError::MissingVersion);
    }
    if registration.endpoint.transport.trim().is_empty() {
        return Err(RegistryError::MissingEndpointTransport);
    }
    if registration.endpoint.address.trim().is_empty() {
        return Err(RegistryError::MissingEndpointAddress);
    }
    validate_field_length("agent_id", &registration.agent_id, MAX_AGENT_ID_BYTES)?;
    validate_field_length(
        "display_name",
        &registration.display_name,
        MAX_DISPLAY_NAME_BYTES,
    )?;
    validate_field_length("version", &registration.version, MAX_VERSION_BYTES)?;
    validate_field_length("summary", &registration.summary, MAX_SUMMARY_BYTES)?;
    validate_field_length(
        "endpoint.transport",
        &registration.endpoint.transport,
        MAX_ENDPOINT_TRANSPORT_BYTES,
    )?;
    validate_field_length(
        "endpoint.address",
        &registration.endpoint.address,
        MAX_ENDPOINT_ADDRESS_BYTES,
    )?;
    validate_values("skills", &registration.skills, MAX_DISCOVERY_VALUES)?;
    validate_values(
        "subscriptions",
        &registration.subscriptions,
        MAX_DISCOVERY_VALUES,
    )?;
    validate_values(
        "publications",
        &registration.publications,
        MAX_DISCOVERY_VALUES,
    )?;
    if registration.schemas.len() > MAX_SCHEMAS {
        return Err(RegistryError::TooManyEntries {
            field: "schemas",
            count: registration.schemas.len(),
            max_entries: MAX_SCHEMAS,
        });
    }
    for schema in &registration.schemas {
        validate_field_length("schemas.name", &schema.name, MAX_DISCOVERY_VALUE_BYTES)?;
        validate_field_length("schemas.version", &schema.version, MAX_VERSION_BYTES)?;
    }
    let bytes = serde_json::to_vec(registration)?.len();
    if bytes > MAX_REGISTRATION_BYTES {
        return Err(RegistryError::RegistrationTooLarge {
            bytes,
            max_bytes: MAX_REGISTRATION_BYTES,
        });
    }
    let ttl_seconds = registration.ttl_seconds.unwrap_or(default_ttl_seconds);
    if ttl_seconds == 0 || expires_at(Utc::now(), ttl_seconds).is_err() {
        return Err(RegistryError::InvalidTtl);
    }

    Ok(())
}

fn validate_field_length(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), RegistryError> {
    if value.len() > max_bytes {
        return Err(RegistryError::FieldTooLong {
            field,
            bytes: value.len(),
            max_bytes,
        });
    }
    Ok(())
}

fn validate_values(
    field: &'static str,
    values: &[String],
    max_entries: usize,
) -> Result<(), RegistryError> {
    if values.len() > max_entries {
        return Err(RegistryError::TooManyEntries {
            field,
            count: values.len(),
            max_entries,
        });
    }
    for value in values {
        validate_field_length(field, value, MAX_DISCOVERY_VALUE_BYTES)?;
    }
    Ok(())
}

fn validate_stored_agents(agents: &[AgentCard]) -> Result<(), RegistryError> {
    let mut agent_ids = HashSet::with_capacity(agents.len());
    for card in agents {
        if !agent_ids.insert(card.agent_id.as_str()) {
            return Err(RegistryError::DuplicateAgentId(card.agent_id.clone()));
        }
        let registration = AgentRegistration {
            agent_id: card.agent_id.clone(),
            display_name: card.display_name.clone(),
            version: card.version.clone(),
            summary: card.summary.clone(),
            skills: card.skills.clone(),
            subscriptions: card.subscriptions.clone(),
            publications: card.publications.clone(),
            schemas: card.schemas.clone(),
            endpoint: card.endpoint.clone(),
            classification: card.classification.clone(),
            retention_class: card.retention_class.clone(),
            ttl_seconds: Some(card.ttl_seconds),
        };
        validate_registration(&registration, card.ttl_seconds).map_err(|error| {
            RegistryError::InvalidStoredAgent {
                agent_id: card.agent_id.clone(),
                reason: error.to_string(),
            }
        })?;
        if card.principal.trim().is_empty() {
            return Err(RegistryError::InvalidStoredAgent {
                agent_id: card.agent_id.clone(),
                reason: "principal must not be empty".to_owned(),
            });
        }
        validate_field_length("principal", &card.principal, MAX_PRINCIPAL_BYTES).map_err(
            |error| RegistryError::InvalidStoredAgent {
                agent_id: card.agent_id.clone(),
                reason: error.to_string(),
            },
        )?;
        let expected_expiry = expires_at(card.last_seen_at, card.ttl_seconds).map_err(|error| {
            RegistryError::InvalidStoredAgent {
                agent_id: card.agent_id.clone(),
                reason: error.to_string(),
            }
        })?;
        if expected_expiry != card.expires_at {
            return Err(RegistryError::InvalidStoredAgent {
                agent_id: card.agent_id.clone(),
                reason: "expires_at does not match last_seen_at plus ttl_seconds".to_owned(),
            });
        }
    }
    Ok(())
}

fn build_card(
    principal: &str,
    registration: AgentRegistration,
    default_ttl_seconds: u64,
) -> Result<AgentCard, RegistryError> {
    let ttl_seconds = registration.ttl_seconds.unwrap_or(default_ttl_seconds);
    let now = Utc::now();
    Ok(AgentCard {
        agent_id: registration.agent_id.trim().to_owned(),
        principal: principal.to_owned(),
        display_name: registration.display_name.trim().to_owned(),
        version: registration.version.trim().to_owned(),
        summary: registration.summary.trim().to_owned(),
        skills: normalize_values(registration.skills),
        subscriptions: normalize_values(registration.subscriptions),
        publications: normalize_values(registration.publications),
        schemas: registration.schemas,
        endpoint: registration.endpoint,
        classification: registration.classification,
        retention_class: registration.retention_class,
        ttl_seconds,
        updated_at: now,
        last_seen_at: now,
        expires_at: expires_at(now, ttl_seconds)?,
    })
}

fn expires_at(
    now: chrono::DateTime<Utc>,
    ttl_seconds: u64,
) -> Result<chrono::DateTime<Utc>, RegistryError> {
    let ttl_seconds = i64::try_from(ttl_seconds).map_err(|_| RegistryError::InvalidTtl)?;
    now.checked_add_signed(Duration::seconds(ttl_seconds))
        .ok_or(RegistryError::InvalidTtl)
}

fn normalize_values(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_ascii_lowercase()) {
            normalized.push(trimmed.to_owned());
        }
    }
    normalized.sort();
    normalized
}

fn matches_query(agent: &AgentCard, query: &AgentQuery, now: chrono::DateTime<Utc>) -> bool {
    if !query.include_stale && is_stale(agent, now) {
        return false;
    }

    matches_optional(&query.principal, &agent.principal)
        && matches_skill(query.skill.as_deref(), &agent.skills)
        && matches_topic(
            query.topic.as_deref(),
            &agent.subscriptions,
            &agent.publications,
        )
}

fn is_stale(agent: &AgentCard, now: chrono::DateTime<Utc>) -> bool {
    agent.expires_at <= now
}

fn matches_optional(expected: &Option<String>, actual: &str) -> bool {
    match expected {
        Some(expected) => expected == actual,
        None => true,
    }
}

fn matches_skill(expected: Option<&str>, skills: &[String]) -> bool {
    match expected {
        Some(expected) => skills
            .iter()
            .any(|skill| skill.eq_ignore_ascii_case(expected)),
        None => true,
    }
}

fn matches_topic(
    expected: Option<&str>,
    subscriptions: &[String],
    publications: &[String],
) -> bool {
    match expected {
        Some(expected) => {
            subscriptions.iter().any(|topic| topic == expected)
                || publications.iter().any(|topic| topic == expected)
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expressways_protocol::{
        AgentEndpoint, AgentQuery, AgentRegistration, Classification, RetentionClass,
    };
    use std::fs;
    use std::sync::Arc;
    use uuid::Uuid;

    fn registry_path() -> PathBuf {
        std::env::temp_dir().join(format!("expressways-registry-{}.json", Uuid::now_v7()))
    }

    fn registration(agent_id: &str) -> AgentRegistration {
        AgentRegistration {
            agent_id: agent_id.to_owned(),
            display_name: "Summarizer".to_owned(),
            version: "1.0.0".to_owned(),
            summary: "Summarizes long-form documents".to_owned(),
            skills: vec![
                "summarize".to_owned(),
                "pdf".to_owned(),
                "summarize".to_owned(),
            ],
            subscriptions: vec!["topic:tasks".to_owned()],
            publications: vec!["topic:results".to_owned()],
            schemas: Vec::new(),
            endpoint: AgentEndpoint {
                transport: "control_tcp".to_owned(),
                address: "127.0.0.1:8800".to_owned(),
            },
            classification: Classification::Internal,
            retention_class: RetentionClass::Operational,
            ttl_seconds: None,
        }
    }

    #[test]
    fn register_list_and_remove_round_trip_through_file_store() {
        let path = registry_path();
        let registry = AgentRegistry::file(path.clone(), 300);

        let card = registry
            .register("local:agent-alpha", registration("agent-alpha"))
            .expect("register card");
        assert_eq!(card.principal, "local:agent-alpha");
        assert_eq!(card.skills, vec!["pdf".to_owned(), "summarize".to_owned()]);

        let by_skill = registry
            .list(&AgentQuery {
                skill: Some("PDF".to_owned()),
                topic: None,
                principal: None,
                include_stale: false,
            })
            .expect("list by skill");
        assert_eq!(by_skill.len(), 1);

        let persisted = FileRegistryStore::new(path.clone())
            .load_agents()
            .expect("load persisted agents");
        assert_eq!(persisted.len(), 1);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path)
                .expect("registry metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        registry
            .remove("local:agent-alpha", "agent", "agent-alpha")
            .expect("remove card");
        let remaining = registry.list(&AgentQuery::default()).expect("list");
        assert!(remaining.is_empty());
    }

    #[test]
    fn concurrent_registrations_do_not_lose_updates() {
        let registry = Arc::new(AgentRegistry::file(registry_path(), 300));
        let handles = (0..8)
            .map(|index| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    registry
                        .register(
                            &format!("local:agent-{index}"),
                            registration(&format!("agent-{index}")),
                        )
                        .expect("register concurrently");
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().expect("join registry writer");
        }

        let agents = registry
            .list(&AgentQuery::default())
            .expect("list registered agents");
        assert_eq!(agents.len(), 8);
        assert_eq!(agents[0].agent_id, "agent-0");
        assert_eq!(agents[7].agent_id, "agent-7");
    }

    #[test]
    fn cross_principal_update_is_rejected() {
        let registry = AgentRegistry::file(registry_path(), 300);
        registry
            .register("local:agent-alpha", registration("agent-alpha"))
            .expect("initial register");

        let error = registry
            .register("local:agent-beta", registration("agent-alpha"))
            .expect_err("ownership conflict");

        assert!(matches!(error, RegistryError::OwnershipConflict { .. }));
    }

    #[test]
    fn rejects_ttl_values_that_overflow_timestamps() {
        let path = registry_path();
        let registry = AgentRegistry::file(path.clone(), 300);
        let mut oversized = registration("agent-oversized");
        oversized.ttl_seconds = Some(u64::MAX);
        assert!(matches!(
            registry.register("local:agent-alpha", oversized),
            Err(RegistryError::InvalidTtl)
        ));

        registry
            .register("local:agent-alpha", registration("agent-tampered"))
            .expect("register valid card");
        let store = FileRegistryStore::new(path);
        let mut agents = store.load_agents().expect("load cards");
        agents[0].ttl_seconds = u64::MAX;
        assert!(matches!(
            store.save_agents(&agents),
            Err(RegistryError::InvalidStoredAgent { .. })
        ));
    }

    #[test]
    fn developer_can_remove_other_principal_entry() {
        let registry = AgentRegistry::file(registry_path(), 300);
        registry
            .register("local:agent-alpha", registration("agent-alpha"))
            .expect("initial register");

        registry
            .remove("local:developer", "developer", "agent-alpha")
            .expect("developer removal");

        let remaining = registry.list(&AgentQuery::default()).expect("list");
        assert!(remaining.is_empty());
    }

    #[test]
    fn stale_entries_are_hidden_until_heartbeat_or_cleanup() {
        let path = registry_path();
        let registry = AgentRegistry::file(path.clone(), 60);
        registry
            .register(
                "local:agent-alpha",
                AgentRegistration {
                    ttl_seconds: Some(1),
                    ..registration("agent-alpha")
                },
            )
            .expect("register");

        let store = FileRegistryStore::new(path.clone());
        let mut agents = store.load_agents().expect("load");
        let expired_at = Utc::now() - Duration::seconds(1);
        agents[0].expires_at = expired_at;
        agents[0].last_seen_at = expired_at - Duration::seconds(1);
        agents[0].updated_at = agents[0].last_seen_at;
        store.save_agents(&agents).expect("save stale");

        let hidden = registry.list(&AgentQuery::default()).expect("list active");
        assert!(hidden.is_empty());

        let visible = registry
            .list(&AgentQuery {
                include_stale: true,
                ..AgentQuery::default()
            })
            .expect("list stale");
        assert_eq!(visible.len(), 1);

        let heartbeated = registry
            .heartbeat("local:agent-alpha", "agent", "agent-alpha")
            .expect("heartbeat");
        assert!(heartbeated.expires_at > Utc::now());

        let refreshed = registry
            .list(&AgentQuery::default())
            .expect("list refreshed");
        assert_eq!(refreshed.len(), 1);

        let mut agents = store.load_agents().expect("load for cleanup");
        let expired_at = Utc::now() - Duration::seconds(1);
        agents[0].expires_at = expired_at;
        agents[0].last_seen_at = expired_at - Duration::seconds(1);
        agents[0].updated_at = agents[0].last_seen_at;
        store.save_agents(&agents).expect("save stale again");

        let removed = registry.cleanup_stale().expect("cleanup");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].agent_id, "agent-alpha");
        let final_list = registry
            .list(&AgentQuery {
                include_stale: true,
                ..AgentQuery::default()
            })
            .expect("list after cleanup");
        assert!(final_list.is_empty());
    }

    #[test]
    fn load_migrates_legacy_schema_version_and_rewrites_document() {
        let path = registry_path();
        fs::write(&path, "{\"agents\":[]}").expect("write legacy document");

        let store = FileRegistryStore::new(path.clone());
        let agents = store.load_agents().expect("load legacy document");
        assert!(agents.is_empty());

        let rewritten = fs::read_to_string(path).expect("read rewritten document");
        let document: serde_json::Value =
            serde_json::from_str(&rewritten).expect("parse rewritten document");
        assert_eq!(document["schema_version"], REGISTRY_SCHEMA_VERSION);
    }

    #[test]
    fn load_rejects_newer_schema_version() {
        let path = registry_path();
        fs::write(&path, "{\"schema_version\":99,\"agents\":[]}")
            .expect("write unsupported document");
        let store = FileRegistryStore::new(path);
        let error = store
            .load_agents()
            .expect_err("unsupported version should fail");
        assert!(matches!(
            error,
            RegistryError::UnsupportedSchemaVersion { found: 99, .. }
        ));
    }

    #[test]
    fn oversized_registrations_are_rejected_before_persistence() {
        let path = registry_path();
        let registry = AgentRegistry::file(path.clone(), 300);
        let mut oversized_field = registration("agent-alpha");
        oversized_field.summary = "x".repeat(MAX_SUMMARY_BYTES + 1);

        let error = registry
            .register("local:agent-alpha", oversized_field)
            .expect_err("oversized field should fail");
        assert!(matches!(error, RegistryError::FieldTooLong { .. }));

        let mut too_many_values = registration("agent-alpha");
        too_many_values.skills = vec!["skill".to_owned(); MAX_DISCOVERY_VALUES + 1];
        let error = registry
            .register("local:agent-alpha", too_many_values)
            .expect_err("excessive value count should fail");
        assert!(matches!(error, RegistryError::TooManyEntries { .. }));

        let mut oversized_registration = registration("agent-alpha");
        oversized_registration.skills = (0..MAX_DISCOVERY_VALUES)
            .map(|index| format!("{index:03}-{}", "x".repeat(MAX_DISCOVERY_VALUE_BYTES - 4)))
            .collect();
        oversized_registration.subscriptions = oversized_registration.skills.clone();
        oversized_registration.publications = oversized_registration.skills.clone();
        let error = registry
            .register("local:agent-alpha", oversized_registration)
            .expect_err("oversized registration should fail");
        assert!(matches!(error, RegistryError::RegistrationTooLarge { .. }));
        assert!(!path.exists());
    }

    #[test]
    fn oversized_registry_files_are_rejected_before_reading() {
        let path = registry_path();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create sparse registry");
        file.set_len(MAX_REGISTRY_FILE_BYTES + 1)
            .expect("extend sparse registry");

        let error = FileRegistryStore::new(path)
            .load_agents()
            .expect_err("oversized registry should fail");
        assert!(matches!(error, RegistryError::RegistryFileTooLarge { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn registry_loader_rejects_symlinked_files() {
        use std::os::unix::fs::symlink;

        let target = registry_path();
        let link = registry_path();
        fs::write(&target, b"{\"schema_version\":1,\"agents\":[]}").expect("write registry target");
        symlink(&target, &link).expect("create registry symlink");

        let error = FileRegistryStore::new(link)
            .load_agents()
            .expect_err("registry symlink must fail");
        assert!(matches!(error, RegistryError::Io(_)));
    }

    #[test]
    fn persisted_duplicate_agent_ids_are_rejected() {
        let path = registry_path();
        let registry = AgentRegistry::file(path.clone(), 300);
        registry
            .register("local:agent-alpha", registration("agent-alpha"))
            .expect("register agent");

        let mut document: RegistryDocument =
            serde_json::from_slice(&fs::read(&path).expect("read registry"))
                .expect("parse registry");
        document.agents.push(document.agents[0].clone());
        fs::write(
            &path,
            serde_json::to_vec(&document).expect("serialize duplicate registry"),
        )
        .expect("write duplicate registry");

        let error = FileRegistryStore::new(path)
            .load_agents()
            .expect_err("duplicate IDs should fail");
        assert!(matches!(error, RegistryError::DuplicateAgentId(_)));
    }
}
