use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use chrono::{DateTime, Utc};
use expressways_protocol::{AgentCard, AgentQuery, RegistryEvent, RegistryEventKind};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorState {
    pub observed_at: DateTime<Utc>,
    pub source_cursor: u64,
    pub agents: BTreeMap<String, AgentCard>,
    pub assignments: BTreeMap<String, AgentAssignmentStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentAssignmentStats {
    pub assignment_count: u64,
    pub last_assigned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentRequirements {
    pub skill: Option<String>,
    pub topic: Option<String>,
    pub principal: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentDecision {
    pub assignment_id: Uuid,
    pub selected_agent_id: String,
    pub selected_principal: String,
    pub selected_endpoint: expressways_protocol::AgentEndpoint,
    pub strategy: String,
    pub requirements: AssignmentRequirements,
    pub source_cursor: u64,
    pub decided_at: DateTime<Utc>,
}

impl Default for OrchestratorState {
    fn default() -> Self {
        Self {
            observed_at: Utc::now(),
            source_cursor: 0,
            agents: BTreeMap::new(),
            assignments: BTreeMap::new(),
        }
    }
}

pub fn apply_snapshot(state: &mut OrchestratorState, agents: Vec<AgentCard>, cursor: u64) {
    state.source_cursor = cursor;
    state.observed_at = Utc::now();
    state.agents = agents
        .into_iter()
        .map(|agent| (agent.agent_id.clone(), agent))
        .collect();
    state
        .assignments
        .retain(|agent_id, _| state.agents.contains_key(agent_id));
}

pub fn apply_event(state: &mut OrchestratorState, event: RegistryEvent) {
    state.source_cursor = state.source_cursor.max(event.sequence);
    state.observed_at = event.timestamp;
    match event.kind {
        RegistryEventKind::Registered | RegistryEventKind::Heartbeated => {
            state.agents.insert(event.card.agent_id.clone(), event.card);
        }
        RegistryEventKind::Removed | RegistryEventKind::CleanedUp => {
            state.agents.remove(&event.card.agent_id);
        }
    }
    state
        .assignments
        .retain(|agent_id, _| state.agents.contains_key(agent_id));
}

pub fn select_agent<'a>(
    state: &'a OrchestratorState,
    requirements: &AssignmentRequirements,
    now: DateTime<Utc>,
) -> Option<&'a AgentCard> {
    state
        .agents
        .values()
        .filter(|agent| agent.expires_at > now)
        .filter(|agent| matches_requirements(agent, requirements))
        .min_by(|left, right| {
            let left_stats = state
                .assignments
                .get(&left.agent_id)
                .cloned()
                .unwrap_or_default();
            let right_stats = state
                .assignments
                .get(&right.agent_id)
                .cloned()
                .unwrap_or_default();

            (
                left_stats.assignment_count,
                left_stats.last_assigned_at,
                left.agent_id.as_str(),
            )
                .cmp(&(
                    right_stats.assignment_count,
                    right_stats.last_assigned_at,
                    right.agent_id.as_str(),
                ))
        })
}

pub fn record_assignment(
    state: &mut OrchestratorState,
    requirements: AssignmentRequirements,
    agent: &AgentCard,
    decided_at: DateTime<Utc>,
) -> AssignmentDecision {
    let stats = state.assignments.entry(agent.agent_id.clone()).or_default();
    stats.assignment_count += 1;
    stats.last_assigned_at = Some(decided_at);
    state.observed_at = decided_at;

    AssignmentDecision {
        assignment_id: Uuid::now_v7(),
        selected_agent_id: agent.agent_id.clone(),
        selected_principal: agent.principal.clone(),
        selected_endpoint: agent.endpoint.clone(),
        strategy: "least_recently_assigned".to_owned(),
        requirements,
        source_cursor: state.source_cursor,
        decided_at,
    }
}

pub fn query_from_requirements(
    requirements: &AssignmentRequirements,
    include_stale: bool,
) -> AgentQuery {
    AgentQuery {
        skill: requirements.skill.clone(),
        topic: requirements.topic.clone(),
        principal: requirements.principal.clone(),
        include_stale,
    }
}

pub fn load_state(path: &Path) -> anyhow::Result<OrchestratorState> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read orchestrator state {}", path.display()))?;
    serde_json::from_str(&raw).context("failed to parse orchestrator state")
}

pub fn save_state(path: &Path, state: &OrchestratorState) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = serde_json::to_vec_pretty(state)?;
    std::fs::write(path, rendered)
        .with_context(|| format!("failed to write orchestrator state {}", path.display()))?;
    Ok(())
}

fn matches_requirements(agent: &AgentCard, requirements: &AssignmentRequirements) -> bool {
    matches_optional(&requirements.principal, &agent.principal)
        && matches_skill(requirements.skill.as_deref(), &agent.skills)
        && matches_topic(
            requirements.topic.as_deref(),
            &agent.subscriptions,
            &agent.publications,
        )
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
    use chrono::Duration;
    use expressways_protocol::{AgentEndpoint, Classification, RetentionClass};

    use super::*;

    fn card(agent_id: &str, skill: &str) -> AgentCard {
        let now = Utc::now();
        AgentCard {
            agent_id: agent_id.to_owned(),
            principal: "local:agent-orchestrator".to_owned(),
            display_name: agent_id.to_owned(),
            version: "1.0.0".to_owned(),
            summary: "test".to_owned(),
            skills: vec![skill.to_owned()],
            subscriptions: vec!["topic:tasks".to_owned()],
            publications: vec!["topic:results".to_owned()],
            schemas: Vec::new(),
            endpoint: AgentEndpoint {
                transport: "control_tcp".to_owned(),
                address: format!("127.0.0.1:{}", 8800 + agent_id.len()),
            },
            classification: Classification::Internal,
            retention_class: RetentionClass::Operational,
            ttl_seconds: 300,
            updated_at: now,
            last_seen_at: now,
            expires_at: now + Duration::seconds(300),
        }
    }

    #[test]
    fn selection_prefers_never_assigned_then_least_recently_assigned() {
        let mut state = OrchestratorState::default();
        apply_snapshot(
            &mut state,
            vec![card("alpha", "summarize"), card("beta", "summarize")],
            4,
        );
        let requirements = AssignmentRequirements {
            skill: Some("summarize".to_owned()),
            topic: None,
            principal: None,
        };

        let first = select_agent(&state, &requirements, Utc::now())
            .expect("first agent")
            .clone();
        assert_eq!(first.agent_id, "alpha");
        let first_decision =
            record_assignment(&mut state, requirements.clone(), &first, Utc::now());
        assert_eq!(first_decision.selected_agent_id, "alpha");

        let second = select_agent(&state, &requirements, Utc::now()).expect("second agent");
        assert_eq!(second.agent_id, "beta");
    }

    #[test]
    fn removed_events_prune_agents_and_assignment_stats() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone()], 2);
        let decision = record_assignment(
            &mut state,
            AssignmentRequirements {
                skill: Some("summarize".to_owned()),
                topic: None,
                principal: None,
            },
            &alpha,
            Utc::now(),
        );
        assert_eq!(decision.selected_agent_id, "alpha");
        assert!(state.assignments.contains_key("alpha"));

        apply_event(
            &mut state,
            RegistryEvent {
                sequence: 3,
                timestamp: Utc::now(),
                kind: RegistryEventKind::Removed,
                card: alpha,
            },
        );

        assert!(!state.agents.contains_key("alpha"));
        assert!(!state.assignments.contains_key("alpha"));
        assert_eq!(state.source_cursor, 3);
    }
}
