use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use aetherbus_protocol::Action;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Attempted,
    Succeeded,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DraftAuditEvent {
    pub principal: String,
    pub action: Action,
    pub resource: String,
    pub decision: AuditDecision,
    pub outcome: AuditOutcome,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    pub event_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub principal: String,
    pub action: Action,
    pub resource: String,
    pub decision: AuditDecision,
    pub outcome: AuditOutcome,
    pub detail: Option<String>,
    pub prev_hash: Option<String>,
    pub hash: String,
}

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug)]
pub struct AuditSink {
    path: PathBuf,
    previous_hash: Option<String>,
}

impl AuditSink {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let previous_hash = if path.exists() {
            last_hash_from_file(&path)?
        } else {
            None
        };

        Ok(Self {
            path,
            previous_hash,
        })
    }

    pub fn append(&mut self, draft: DraftAuditEvent) -> Result<AuditEvent, AuditError> {
        let timestamp = Utc::now();
        let prev_hash = self.previous_hash.clone();
        let hash = compute_hash(&draft, timestamp, prev_hash.as_deref())?;

        let event = AuditEvent {
            event_id: Uuid::now_v7(),
            timestamp,
            principal: draft.principal,
            action: draft.action,
            resource: draft.resource,
            decision: draft.decision,
            outcome: draft.outcome,
            detail: draft.detail,
            prev_hash,
            hash: hash.clone(),
        };

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        serde_json::to_writer(&mut file, &event)?;
        file.write_all(b"\n")?;
        file.flush()?;

        self.previous_hash = Some(hash);
        Ok(event)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn last_hash_from_file(path: &Path) -> Result<Option<String>, AuditError> {
    let file = File::open(path)?;
    let mut last = None;

    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        last = Some(line);
    }

    if let Some(line) = last {
        let event: AuditEvent = serde_json::from_str(&line)?;
        return Ok(Some(event.hash));
    }

    Ok(None)
}

fn compute_hash(
    draft: &DraftAuditEvent,
    timestamp: DateTime<Utc>,
    prev_hash: Option<&str>,
) -> Result<String, AuditError> {
    let payload = serde_json::json!({
        "principal": draft.principal,
        "action": draft.action,
        "resource": draft.resource,
        "decision": draft.decision,
        "outcome": draft.outcome,
        "detail": draft.detail,
        "timestamp": timestamp,
        "prev_hash": prev_hash,
    });

    let serialized = serde_json::to_vec(&payload)?;
    let mut hasher = Sha256::new();
    if let Some(previous) = prev_hash {
        hasher.update(previous.as_bytes());
    }
    hasher.update(serialized);

    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_events_chain_together() {
        let root = std::env::temp_dir().join(format!("aetherbus-audit-{}", Uuid::now_v7()));
        let path = root.join("audit.jsonl");

        let mut sink = AuditSink::new(&path).expect("create sink");
        let first = sink
            .append(DraftAuditEvent {
                principal: "local:developer".to_owned(),
                action: Action::Admin,
                resource: "system:broker".to_owned(),
                decision: AuditDecision::Allow,
                outcome: AuditOutcome::Attempted,
                detail: Some("startup".to_owned()),
            })
            .expect("append first event");
        let second = sink
            .append(DraftAuditEvent {
                principal: "local:developer".to_owned(),
                action: Action::Publish,
                resource: "topic:tasks".to_owned(),
                decision: AuditDecision::Allow,
                outcome: AuditOutcome::Succeeded,
                detail: Some("published message".to_owned()),
            })
            .expect("append second event");

        assert_eq!(second.prev_hash, Some(first.hash));
    }
}
