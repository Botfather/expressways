use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use expressways_protocol::Action;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const AUDIT_EVENT_SCHEMA_VERSION: u32 = 1;
const MAX_AUDIT_EVENT_BYTES: usize = 8 * 1024 * 1024;

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
    #[serde(default)]
    pub schema_version: u32,
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
    #[error("audit integrity failure: {0}")]
    Integrity(String),
    #[error("audit event exceeds the {MAX_AUDIT_EVENT_BYTES}-byte limit")]
    EventTooLarge,
    #[error("audit event count exhausted")]
    EventCountExhausted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditLogSummary {
    pub event_count: u64,
    pub last_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditVerificationReport {
    pub path: PathBuf,
    pub event_count: u64,
    pub first_event_id: Option<Uuid>,
    pub last_event_id: Option<Uuid>,
    pub first_timestamp: Option<DateTime<Utc>>,
    pub last_timestamp: Option<DateTime<Utc>>,
    pub last_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditTailRepairReport {
    pub path: PathBuf,
    pub quarantine_path: PathBuf,
    pub original_bytes: u64,
    pub repaired_bytes: u64,
    pub removed_bytes: u64,
    pub retained_events: u64,
    pub last_hash: Option<String>,
}

#[derive(Debug)]
pub struct AuditSink {
    path: PathBuf,
    event_count: u64,
    previous_hash: Option<String>,
}

impl AuditSink {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            create_private_dir(parent)?;
        }
        let exists = match open_existing_audit_file(&path)? {
            Some(file) => {
                set_private_file_permissions(&file)?;
                true
            }
            None => false,
        };

        let summary = if exists {
            let report = verify_file(&path)?;
            AuditLogSummary {
                event_count: report.event_count,
                last_hash: report.last_hash,
            }
        } else {
            AuditLogSummary {
                event_count: 0,
                last_hash: None,
            }
        };

        Ok(Self {
            path,
            event_count: summary.event_count,
            previous_hash: summary.last_hash,
        })
    }

    pub fn append(&mut self, draft: DraftAuditEvent) -> Result<AuditEvent, AuditError> {
        let timestamp = Utc::now();
        let event_id = Uuid::now_v7();
        let prev_hash = self.previous_hash.clone();
        let hash = compute_hash(
            &draft,
            timestamp,
            prev_hash.as_deref(),
            AUDIT_EVENT_SCHEMA_VERSION,
            Some(event_id),
        )?;

        let event = AuditEvent {
            schema_version: AUDIT_EVENT_SCHEMA_VERSION,
            event_id,
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

        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let mut file = options.open(&self.path)?;
        if !file.metadata()?.is_file() {
            return Err(AuditError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "audit path is not a regular file",
            )));
        }
        set_private_file_permissions(&file)?;
        let original_length = file.metadata()?.len();

        let encoded = serde_json::to_vec(&event)?;
        if encoded.len() > MAX_AUDIT_EVENT_BYTES {
            return Err(AuditError::EventTooLarge);
        }
        let next_event_count = self
            .event_count
            .checked_add(1)
            .ok_or(AuditError::EventCountExhausted)?;

        let append_result = (|| -> std::io::Result<()> {
            file.write_all(&encoded)?;
            file.write_all(b"\n")?;
            file.sync_data()?;
            sync_parent_dir(&self.path)?;
            Ok(())
        })();
        if let Err(append_error) = append_result {
            let rollback_result = file
                .set_len(original_length)
                .and_then(|()| file.sync_data());
            return Err(AuditError::Io(match rollback_result {
                Ok(()) => append_error,
                Err(rollback_error) => std::io::Error::other(format!(
                    "audit append failed ({append_error}); rollback to {original_length} bytes also failed ({rollback_error})"
                )),
            }));
        }

        self.event_count = next_event_count;
        self.previous_hash = Some(hash);
        Ok(event)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn summary(&self) -> AuditLogSummary {
        AuditLogSummary {
            event_count: self.event_count,
            last_hash: self.previous_hash.clone(),
        }
    }
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
        })?;
        File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn create_private_dir(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_private_file_permissions(file: &File) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = file;
    Ok(())
}

fn open_existing_audit_file(path: &Path) -> Result<Option<File>, AuditError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_file() {
        return Err(AuditError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "audit path is not a regular file",
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(AuditError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "audit path is not a regular file",
        )));
    }
    Ok(Some(file))
}

pub fn load_events(path: impl AsRef<Path>) -> Result<Vec<AuditEvent>, AuditError> {
    let path = path.as_ref();
    let Some(file) = open_existing_audit_file(path)? else {
        return Ok(Vec::new());
    };

    let mut reader = BufReader::new(file);
    let mut events = Vec::new();
    let mut encoded = Vec::new();
    while read_limited_line(&mut reader, &mut encoded)? {
        if encoded.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        events.push(serde_json::from_slice(&encoded)?);
    }

    Ok(events)
}

pub fn verify_file(path: impl AsRef<Path>) -> Result<AuditVerificationReport, AuditError> {
    visit_verified_events(path, |_| Ok(()))
}

pub fn visit_verified_events(
    path: impl AsRef<Path>,
    mut visitor: impl FnMut(&AuditEvent) -> Result<(), AuditError>,
) -> Result<AuditVerificationReport, AuditError> {
    let path = path.as_ref().to_path_buf();
    let Some(file) = open_existing_audit_file(&path)? else {
        return Ok(AuditVerificationReport {
            path,
            event_count: 0,
            first_event_id: None,
            last_event_id: None,
            first_timestamp: None,
            last_timestamp: None,
            last_hash: None,
        });
    };

    let mut reader = BufReader::new(file);
    visit_verified_reader(path, &mut reader, &mut visitor)
}

fn visit_verified_reader(
    path: PathBuf,
    reader: &mut impl BufRead,
    visitor: &mut impl FnMut(&AuditEvent) -> Result<(), AuditError>,
) -> Result<AuditVerificationReport, AuditError> {
    let mut previous_hash: Option<String> = None;
    let mut event_count = 0_u64;
    let mut first_event_id = None;
    let mut last_event_id = None;
    let mut first_timestamp = None;
    let mut last_timestamp = None;
    let mut encoded = Vec::new();

    while read_limited_line(reader, &mut encoded)? {
        if encoded.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let event: AuditEvent = serde_json::from_slice(&encoded)?;
        if event.schema_version > AUDIT_EVENT_SCHEMA_VERSION {
            return Err(AuditError::Integrity(format!(
                "event {} uses unsupported schema version {} (maximum supported is {})",
                event.event_id, event.schema_version, AUDIT_EVENT_SCHEMA_VERSION
            )));
        }
        if event.prev_hash != previous_hash {
            return Err(AuditError::Integrity(format!(
                "event {} has an unexpected prev_hash",
                event.event_id
            )));
        }

        let expected_hash = compute_hash(
            &DraftAuditEvent {
                principal: event.principal.clone(),
                action: event.action.clone(),
                resource: event.resource.clone(),
                decision: event.decision.clone(),
                outcome: event.outcome.clone(),
                detail: event.detail.clone(),
            },
            event.timestamp,
            event.prev_hash.as_deref(),
            event.schema_version,
            (event.schema_version >= AUDIT_EVENT_SCHEMA_VERSION).then_some(event.event_id),
        )?;

        if expected_hash != event.hash {
            return Err(AuditError::Integrity(format!(
                "event {} hash does not match payload",
                event.event_id
            )));
        }

        visitor(&event)?;
        event_count = event_count
            .checked_add(1)
            .ok_or(AuditError::EventCountExhausted)?;
        first_event_id.get_or_insert(event.event_id);
        first_timestamp.get_or_insert(event.timestamp);
        last_event_id = Some(event.event_id);
        last_timestamp = Some(event.timestamp);
        previous_hash = Some(event.hash);
    }

    Ok(AuditVerificationReport {
        path,
        event_count,
        first_event_id,
        last_event_id,
        first_timestamp,
        last_timestamp,
        last_hash: previous_hash,
    })
}

pub fn repair_incomplete_tail(
    path: impl AsRef<Path>,
    quarantine_path: impl AsRef<Path>,
) -> Result<AuditTailRepairReport, AuditError> {
    let path = path.as_ref().to_path_buf();
    let quarantine_path = quarantine_path.as_ref().to_path_buf();
    if path == quarantine_path {
        return Err(AuditError::Integrity(
            "audit repair quarantine must differ from the audit log".to_owned(),
        ));
    }

    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.file_type().is_file() {
        return Err(AuditError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "audit path is not a regular file",
        )));
    }
    let original_bytes = metadata.len();
    if original_bytes == 0 {
        return Err(AuditError::Integrity(
            "audit log is empty and has no incomplete tail".to_owned(),
        ));
    }

    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(&path)?;
    if !file.metadata()?.is_file() {
        return Err(AuditError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "audit path is not a regular file",
        )));
    }

    file.seek(SeekFrom::End(-1))?;
    let mut final_byte = [0_u8; 1];
    file.read_exact(&mut final_byte)?;
    if final_byte[0] == b'\n' {
        return Err(AuditError::Integrity(
            "audit log ends at a complete record boundary; refusing repair".to_owned(),
        ));
    }

    let scan_limit = (MAX_AUDIT_EVENT_BYTES as u64).saturating_add(1);
    let scan_floor = original_bytes.saturating_sub(scan_limit);
    let mut end = original_bytes;
    let mut repaired_bytes = None;
    let mut buffer = vec![0_u8; 8 * 1024];
    while end > scan_floor {
        let start = end.saturating_sub(buffer.len() as u64).max(scan_floor);
        let length = usize::try_from(end - start).map_err(|_| {
            AuditError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "audit tail length overflow",
            ))
        })?;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buffer[..length])?;
        if let Some(position) = buffer[..length].iter().rposition(|byte| *byte == b'\n') {
            repaired_bytes = Some(start + position as u64 + 1);
            break;
        }
        end = start;
    }
    let repaired_bytes = match repaired_bytes {
        Some(bytes) => bytes,
        None if scan_floor == 0 => 0,
        None => return Err(AuditError::EventTooLarge),
    };
    let removed_bytes = original_bytes.saturating_sub(repaired_bytes);

    let mut verification_file = file.try_clone()?;
    verification_file.seek(SeekFrom::Start(0))?;
    let mut verification_reader = BufReader::new(verification_file.take(repaired_bytes));
    let verification =
        visit_verified_reader(path.clone(), &mut verification_reader, &mut |_| Ok(()))?;

    file.seek(SeekFrom::Start(repaired_bytes))?;
    let mut tail = Vec::with_capacity(usize::try_from(removed_bytes).unwrap_or_default());
    (&mut file).take(scan_limit).read_to_end(&mut tail)?;
    if tail.len() as u64 != removed_bytes {
        return Err(AuditError::Integrity(
            "audit tail changed while preparing repair".to_owned(),
        ));
    }

    if let Some(parent) = quarantine_path.parent() {
        create_private_dir(parent)?;
    }
    let mut quarantine_options = OpenOptions::new();
    quarantine_options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        quarantine_options.mode(0o600);
    }
    let mut quarantine = quarantine_options.open(&quarantine_path)?;
    quarantine.write_all(&tail)?;
    quarantine.sync_all()?;
    sync_parent_dir(&quarantine_path)?;

    file.set_len(repaired_bytes)?;
    file.sync_data()?;

    Ok(AuditTailRepairReport {
        path,
        quarantine_path,
        original_bytes,
        repaired_bytes,
        removed_bytes,
        retained_events: verification.event_count,
        last_hash: verification.last_hash,
    })
}

fn read_limited_line(reader: &mut impl BufRead, output: &mut Vec<u8>) -> Result<bool, AuditError> {
    output.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(!output.is_empty());
        }

        let chunk_len = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if output.len().saturating_add(chunk_len) > MAX_AUDIT_EVENT_BYTES + 1 {
            return Err(AuditError::EventTooLarge);
        }
        output.extend_from_slice(&available[..chunk_len]);
        reader.consume(chunk_len);
        if output.last() == Some(&b'\n') {
            return Ok(true);
        }
    }
}

fn compute_hash(
    draft: &DraftAuditEvent,
    timestamp: DateTime<Utc>,
    prev_hash: Option<&str>,
    schema_version: u32,
    event_id: Option<Uuid>,
) -> Result<String, AuditError> {
    let mut payload = serde_json::json!({
        "principal": draft.principal,
        "action": draft.action,
        "resource": draft.resource,
        "decision": draft.decision,
        "outcome": draft.outcome,
        "detail": draft.detail,
        "timestamp": timestamp,
        "prev_hash": prev_hash,
    });
    if schema_version >= AUDIT_EVENT_SCHEMA_VERSION {
        payload["schema_version"] = serde_json::json!(schema_version);
        payload["event_id"] = serde_json::json!(event_id.ok_or_else(|| {
            AuditError::Integrity("versioned audit event is missing its event ID".to_owned())
        })?);
    }

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
        let root = std::env::temp_dir().join(format!("expressways-audit-{}", Uuid::now_v7()));
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

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path)
                    .expect("audit metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(&root)
                    .expect("audit directory metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }

        assert_eq!(second.prev_hash, Some(first.hash));
        assert_eq!(sink.summary().event_count, 2);
    }

    #[test]
    fn incomplete_tail_repair_preserves_verified_prefix_and_quarantines_bytes() {
        let root =
            std::env::temp_dir().join(format!("expressways-audit-repair-{}", Uuid::now_v7()));
        let path = root.join("audit.jsonl");
        let quarantine = root.join("audit.incomplete-tail");
        let mut sink = AuditSink::new(&path).expect("create sink");
        for detail in ["first", "second"] {
            sink.append(DraftAuditEvent {
                principal: "local:developer".to_owned(),
                action: Action::Admin,
                resource: "system:broker".to_owned(),
                decision: AuditDecision::Allow,
                outcome: AuditOutcome::Succeeded,
                detail: Some(detail.to_owned()),
            })
            .expect("append event");
        }
        drop(sink);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open audit")
            .write_all(b"{\"interrupted\":")
            .expect("append incomplete tail");
        assert!(verify_file(&path).is_err());

        let report =
            repair_incomplete_tail(&path, &quarantine).expect("repair incomplete audit tail");
        assert_eq!(report.retained_events, 2);
        assert_eq!(report.removed_bytes, b"{\"interrupted\":".len() as u64);
        assert_eq!(
            fs::read(&quarantine).expect("read quarantine"),
            b"{\"interrupted\":"
        );
        assert_eq!(
            verify_file(&path)
                .expect("verify repaired audit")
                .event_count,
            2
        );
        assert!(repair_incomplete_tail(&path, root.join("unused")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn audit_operations_reject_symlinked_logs() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("expressways-audit-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create root");
        let target = root.join("target.jsonl");
        let link = root.join("audit.jsonl");
        fs::write(&target, b"sentinel").expect("write target");
        symlink(&target, &link).expect("create audit symlink");

        assert!(AuditSink::new(&link).is_err());
        assert!(verify_file(&link).is_err());
        assert!(load_events(&link).is_err());
        assert_eq!(fs::read(&target).expect("read target"), b"sentinel");
    }

    #[test]
    fn verification_detects_tampering() {
        let root = std::env::temp_dir().join(format!("expressways-audit-{}", Uuid::now_v7()));
        let path = root.join("audit.jsonl");

        let mut sink = AuditSink::new(&path).expect("create sink");
        sink.append(DraftAuditEvent {
            principal: "local:developer".to_owned(),
            action: Action::Admin,
            resource: "system:broker".to_owned(),
            decision: AuditDecision::Allow,
            outcome: AuditOutcome::Succeeded,
            detail: Some("startup".to_owned()),
        })
        .expect("append event");

        let lines = fs::read_to_string(&path).expect("read audit");
        let mut event: AuditEvent = serde_json::from_str(lines.trim()).expect("parse event");
        event.resource = "topic:tampered".to_owned();
        fs::write(
            &path,
            format!(
                "{}\n",
                serde_json::to_string(&event).expect("serialize tampered event")
            ),
        )
        .expect("rewrite audit");

        let error = verify_file(&path).expect_err("tampered audit should fail");
        assert!(matches!(error, AuditError::Integrity(_)));
        assert!(AuditSink::new(&path).is_err());
    }

    #[test]
    fn verification_detects_event_id_tampering_for_versioned_events() {
        let root = std::env::temp_dir().join(format!("expressways-audit-{}", Uuid::now_v7()));
        let path = root.join("audit.jsonl");
        let mut sink = AuditSink::new(&path).expect("create sink");
        sink.append(DraftAuditEvent {
            principal: "local:developer".to_owned(),
            action: Action::Admin,
            resource: "system:broker".to_owned(),
            decision: AuditDecision::Allow,
            outcome: AuditOutcome::Succeeded,
            detail: None,
        })
        .expect("append event");

        let line = fs::read_to_string(&path).expect("read audit");
        let mut event: AuditEvent = serde_json::from_str(line.trim()).expect("parse event");
        event.event_id = Uuid::now_v7();
        fs::write(
            &path,
            format!(
                "{}\n",
                serde_json::to_string(&event).expect("serialize event")
            ),
        )
        .expect("rewrite audit");

        let error = verify_file(&path).expect_err("event ID tampering should fail");
        assert!(matches!(error, AuditError::Integrity(_)));
    }

    #[test]
    fn oversized_events_are_rejected_on_append_and_verification() {
        let root = std::env::temp_dir().join(format!("expressways-audit-{}", Uuid::now_v7()));
        let path = root.join("audit.jsonl");
        let mut sink = AuditSink::new(&path).expect("create sink");
        let error = sink
            .append(DraftAuditEvent {
                principal: "local:developer".to_owned(),
                action: Action::Admin,
                resource: "system:broker".to_owned(),
                decision: AuditDecision::Allow,
                outcome: AuditOutcome::Succeeded,
                detail: Some("x".repeat(MAX_AUDIT_EVENT_BYTES)),
            })
            .expect_err("oversized append should fail");
        assert!(matches!(error, AuditError::EventTooLarge));
        assert_eq!(sink.summary().event_count, 0);

        fs::write(&path, vec![b'x'; MAX_AUDIT_EVENT_BYTES + 2]).expect("write oversized event");
        let error = verify_file(&path).expect_err("oversized stored event should fail");
        assert!(matches!(error, AuditError::EventTooLarge));
    }
}
