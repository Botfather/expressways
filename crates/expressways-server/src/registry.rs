use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use chrono::{Duration, Utc};
use expressways_protocol::{AgentCard, AgentQuery, AgentRegistration};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const REGISTRY_SCHEMA_VERSION: u32 = 1;

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
    #[error("ttl_seconds must be greater than zero")]
    InvalidTtl,
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub trait RegistryStore: std::fmt::Debug + Send + Sync {
    fn load_agents(&self) -> Result<Vec<AgentCard>, RegistryError>;
    fn save_agents(&self, agents: &[AgentCard]) -> Result<(), RegistryError>;
}

#[derive(Debug)]
pub struct FileRegistryStore {
    path: PathBuf,
}

impl FileRegistryStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl RegistryStore for FileRegistryStore {
    fn load_agents(&self) -> Result<Vec<AgentCard>, RegistryError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let raw = fs::read_to_string(&self.path)?;
        let document: RegistryDocument = serde_json::from_str(&raw)?;
        Ok(document.agents)
    }

    fn save_agents(&self, agents: &[AgentCard]) -> Result<(), RegistryError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let document = RegistryDocument {
            schema_version: REGISTRY_SCHEMA_VERSION,
            agents: agents.to_vec(),
        };
        let raw = serde_json::to_vec_pretty(&document)?;
        let temp_path = self.path.with_extension("tmp");
        fs::write(&temp_path, raw)?;
        fs::rename(&temp_path, &self.path)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct AgentRegistry {
    store: Box<dyn RegistryStore>,
    default_ttl_seconds: u64,
}

impl AgentRegistry {
    pub fn file(path: PathBuf, default_ttl_seconds: u64) -> Self {
        Self {
            store: Box::new(FileRegistryStore::new(path)),
            default_ttl_seconds,
        }
    }

    pub fn register(
        &self,
        principal: &str,
        registration: AgentRegistration,
    ) -> Result<AgentCard, RegistryError> {
        validate_registration(&registration, self.default_ttl_seconds)?;

        let mut agents = self.store.load_agents()?;
        let card = build_card(principal, registration, self.default_ttl_seconds);

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
        } else {
            agents.push(card.clone());
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
        card.expires_at = now + Duration::seconds(card.ttl_seconds as i64);
        let updated = card.clone();

        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        self.store.save_agents(&agents)?;
        Ok(updated)
    }

    pub fn cleanup_stale(&self) -> Result<Vec<AgentCard>, RegistryError> {
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
    REGISTRY_SCHEMA_VERSION
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
    if registration.ttl_seconds.unwrap_or(default_ttl_seconds) == 0 {
        return Err(RegistryError::InvalidTtl);
    }

    Ok(())
}

fn build_card(
    principal: &str,
    registration: AgentRegistration,
    default_ttl_seconds: u64,
) -> AgentCard {
    let ttl_seconds = registration.ttl_seconds.unwrap_or(default_ttl_seconds);
    let now = Utc::now();
    AgentCard {
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
        expires_at: now + Duration::seconds(ttl_seconds as i64),
    }
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

        let persisted = FileRegistryStore::new(path)
            .load_agents()
            .expect("load persisted agents");
        assert_eq!(persisted.len(), 1);

        registry
            .remove("local:agent-alpha", "agent", "agent-alpha")
            .expect("remove card");
        let remaining = registry.list(&AgentQuery::default()).expect("list");
        assert!(remaining.is_empty());
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
        agents[0].expires_at = Utc::now() - Duration::seconds(1);
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
        agents[0].expires_at = Utc::now() - Duration::seconds(1);
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
}
