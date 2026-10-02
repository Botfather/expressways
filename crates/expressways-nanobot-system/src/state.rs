use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::model::{MemoryEntry, RuntimeState, SessionRole, SessionTurn};

const MAX_SESSION_ID_BYTES: usize = 256;
const MAX_JSONL_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_JSONL_RECORD_BYTES: usize = 1024 * 1024;
const MAX_RUNTIME_STATE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct SessionStore {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct MemoryStore {
    root: PathBuf,
}

impl SessionStore {
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create session store at {}", root.display()))?;
        set_private_directory_permissions(&root)?;
        Ok(Self { root })
    }

    pub fn append_turn(&self, session_id: &str, turn: &SessionTurn) -> anyhow::Result<()> {
        let path = self.session_path(session_id)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut rendered = serde_json::to_vec(turn).context("failed to serialize session turn")?;
        rendered.push(b'\n');
        let mut file = open_bounded_private_append(&path, rendered.len())
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.write_all(&rendered)
            .with_context(|| format!("failed to append {}", path.display()))?;
        file.flush()?;
        Ok(())
    }

    pub fn load_recent_turns(
        &self,
        session_id: &str,
        max_turns: usize,
    ) -> anyhow::Result<Vec<SessionTurn>> {
        let path = self.session_path(session_id)?;
        if !path.exists() {
            return Ok(Vec::new());
        }

        let turns = load_recent_jsonl::<SessionTurn>(&path, max_turns.max(1))?;
        Ok(trim_turns_for_context(turns, max_turns.max(1)))
    }

    fn session_path(&self, session_id: &str) -> anyhow::Result<PathBuf> {
        Ok(self
            .root
            .join(format!("{}.jsonl", session_component(session_id)?)))
    }
}

impl MemoryStore {
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create memory store at {}", root.display()))?;
        set_private_directory_permissions(&root)?;
        Ok(Self { root })
    }

    pub fn append_note(&self, session_id: &str, note: impl Into<String>) -> anyhow::Result<()> {
        let note = note.into();
        if note.trim().is_empty() {
            return Ok(());
        }

        let path = self.memory_path(session_id)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let entry = MemoryEntry {
            timestamp: chrono::Utc::now(),
            note,
        };
        let mut rendered =
            serde_json::to_vec(&entry).context("failed to serialize memory entry")?;
        rendered.push(b'\n');
        let mut file = open_bounded_private_append(&path, rendered.len())
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.write_all(&rendered)
            .with_context(|| format!("failed to append {}", path.display()))?;
        file.flush()?;
        Ok(())
    }

    pub fn summarize_recent(
        &self,
        session_id: &str,
        max_entries: usize,
    ) -> anyhow::Result<Option<String>> {
        let path = self.memory_path(session_id)?;
        if !path.exists() {
            return Ok(None);
        }

        let entries = load_recent_jsonl::<MemoryEntry>(&path, max_entries.max(1))?;

        if entries.is_empty() {
            return Ok(None);
        }

        let keep = max_entries.max(1).min(entries.len());
        let start = entries.len().saturating_sub(keep);
        let mut notes = entries[start..]
            .iter()
            .map(|entry| entry.note.trim())
            .filter(|note| !note.is_empty())
            .collect::<Vec<_>>();
        if notes.is_empty() {
            return Ok(None);
        }

        if notes.len() > 6 {
            notes = notes.split_off(notes.len() - 6);
        }

        Ok(Some(notes.join(" | ")))
    }

    fn memory_path(&self, session_id: &str) -> anyhow::Result<PathBuf> {
        Ok(self
            .root
            .join(format!("{}.memory.jsonl", session_component(session_id)?)))
    }
}

pub fn load_runtime_state(path: &Path) -> anyhow::Result<RuntimeState> {
    if !path.exists() {
        return Ok(RuntimeState::default());
    }
    let raw = read_bounded_file(path, MAX_RUNTIME_STATE_BYTES)
        .with_context(|| format!("failed to read runtime state {}", path.display()))?;
    serde_json::from_slice(&raw).context("failed to parse runtime state")
}

pub fn save_runtime_state(path: &Path, state: &RuntimeState) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = serde_json::to_vec_pretty(state).context("failed to serialize runtime state")?;
    if rendered.len() as u64 > MAX_RUNTIME_STATE_BYTES {
        bail!("runtime state exceeds the {MAX_RUNTIME_STATE_BYTES} byte limit");
    }
    atomic_replace(path, &rendered)
        .with_context(|| format!("failed to write runtime state {}", path.display()))?;
    Ok(())
}

fn load_recent_jsonl<T: DeserializeOwned>(path: &Path, keep: usize) -> anyhow::Result<Vec<T>> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("state path {} is not a regular file", path.display());
    }
    if metadata.len() > MAX_JSONL_FILE_BYTES {
        bail!(
            "state file {} is {} bytes, exceeding the {} byte limit",
            path.display(),
            metadata.len(),
            MAX_JSONL_FILE_BYTES
        );
    }

    let file = File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(file.take(MAX_JSONL_FILE_BYTES + 1));
    let mut recent = VecDeque::with_capacity(keep.min(1024));
    let mut line_number = 0u64;
    let mut total_bytes = 0u64;
    loop {
        let mut line = Vec::new();
        let bytes_read = (&mut reader)
            .take(MAX_JSONL_RECORD_BYTES as u64 + 1)
            .read_until(b'\n', &mut line)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        total_bytes = total_bytes.saturating_add(bytes_read as u64);
        if total_bytes > MAX_JSONL_FILE_BYTES {
            bail!(
                "state file {} grew beyond the {} byte limit",
                path.display(),
                MAX_JSONL_FILE_BYTES
            );
        }
        line_number = line_number.saturating_add(1);
        if bytes_read > MAX_JSONL_RECORD_BYTES {
            bail!(
                "record {line_number} in {} exceeds the {} byte limit",
                path.display(),
                MAX_JSONL_RECORD_BYTES
            );
        }
        let complete = line.last() == Some(&b'\n');
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let item = match serde_json::from_slice(&line) {
            Ok(item) => item,
            Err(_) if !complete => break,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("invalid JSON record {line_number} in {}", path.display())
                });
            }
        };
        if recent.len() == keep {
            recent.pop_front();
        }
        recent.push_back(item);
    }
    Ok(recent.into_iter().collect())
}

fn open_bounded_private_append(path: &Path, record_bytes: usize) -> anyhow::Result<File> {
    if record_bytes > MAX_JSONL_RECORD_BYTES {
        bail!("JSONL record exceeds the {MAX_JSONL_RECORD_BYTES} byte limit");
    }
    if let Ok(metadata) = fs::symlink_metadata(path)
        && !metadata.file_type().is_file()
    {
        bail!("state path {} is not a regular file", path.display());
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    let current_bytes = file.metadata()?.len();
    let record_bytes = u64::try_from(record_bytes).context("record length overflow")?;
    if current_bytes.saturating_add(record_bytes) > MAX_JSONL_FILE_BYTES {
        bail!(
            "state file {} would exceed the {} byte limit",
            path.display(),
            MAX_JSONL_FILE_BYTES
        );
    }
    Ok(file)
}

fn read_bounded_file(path: &Path, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "state path is not a bounded regular file",
        ));
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "state is too large to read",
        )
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)?
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "state grew beyond its size limit while being read",
        ));
    }
    Ok(bytes)
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temp_path = path.with_extension(format!("tmp-{}", Uuid::now_v7()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(path)?;
        }
        fs::rename(&temp_path, path)?;
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp_path);
    }
    result
}

fn set_private_directory_permissions(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn trim_turns_for_context(turns: Vec<SessionTurn>, max_turns: usize) -> Vec<SessionTurn> {
    if turns.len() <= max_turns {
        return turns;
    }

    let mut trimmed = turns[turns.len().saturating_sub(max_turns)..].to_vec();

    while let Some(first) = trimmed.first() {
        let drop = matches!(first.role, SessionRole::ToolResult);
        if !drop {
            break;
        }
        trimmed.remove(0);
    }

    trimmed
}

fn session_component(value: &str) -> anyhow::Result<String> {
    if value.is_empty() || value.len() > MAX_SESSION_ID_BYTES {
        bail!("session id must contain 1..={MAX_SESSION_ID_BYTES} bytes");
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Ok(value.to_owned());
    }
    let mut encoded = String::with_capacity(1 + value.len() * 2);
    encoded.push('~');
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn trim_drops_leading_orphan_tool_result() {
        let turns = vec![
            SessionTurn {
                timestamp: Utc::now(),
                role: SessionRole::User,
                text: Some("one".to_owned()),
                message_id: None,
                tool_name: None,
                tool_args: None,
                tool_result: None,
            },
            SessionTurn {
                timestamp: Utc::now(),
                role: SessionRole::ToolResult,
                text: None,
                message_id: None,
                tool_name: Some("read_file".to_owned()),
                tool_args: None,
                tool_result: Some(serde_json::json!({"ok": true})),
            },
            SessionTurn {
                timestamp: Utc::now(),
                role: SessionRole::Assistant,
                text: Some("done".to_owned()),
                message_id: None,
                tool_name: None,
                tool_args: None,
                tool_result: None,
            },
        ];

        let trimmed = trim_turns_for_context(turns, 2);
        assert_eq!(trimmed.len(), 1);
        assert!(matches!(trimmed[0].role, SessionRole::Assistant));
    }

    #[test]
    fn session_component_is_collision_free_and_rejects_invalid_lengths() {
        assert_eq!(session_component("chat-1").expect("safe id"), "chat-1");
        assert_ne!(
            session_component("a/b").expect("encoded id"),
            session_component("a?b").expect("different encoded id")
        );
        assert!(session_component("").is_err());
        assert!(session_component(&"x".repeat(MAX_SESSION_ID_BYTES + 1)).is_err());
    }

    fn test_turn(text: &str) -> SessionTurn {
        SessionTurn {
            timestamp: Utc::now(),
            role: SessionRole::User,
            text: Some(text.to_owned()),
            message_id: None,
            tool_name: None,
            tool_args: None,
            tool_result: None,
        }
    }

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("expressways-nanobot-state-{}", Uuid::now_v7()))
    }

    #[test]
    fn session_store_keeps_recent_records_and_ignores_partial_tail() {
        let root = temp_root();
        let store = SessionStore::new(&root).expect("create store");
        store
            .append_turn("chat", &test_turn("one"))
            .expect("append one");
        store
            .append_turn("chat", &test_turn("two"))
            .expect("append two");
        store
            .append_turn("chat", &test_turn("three"))
            .expect("append three");
        let path = store.session_path("chat").expect("session path");
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open session")
            .write_all(b"{\"partial\":")
            .expect("append partial tail");

        let turns = store.load_recent_turns("chat", 2).expect("load recent");
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].text.as_deref(), Some("two"));
        assert_eq!(turns[1].text.as_deref(), Some("three"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(root)
                    .expect("root metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(path)
                    .expect("file metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn session_store_rejects_complete_malformed_and_oversized_records() {
        let store = SessionStore::new(temp_root()).expect("create store");
        let path = store.session_path("chat").expect("session path");
        fs::write(&path, b"not-json\n").expect("write malformed record");
        assert!(store.load_recent_turns("chat", 10).is_err());

        let oversized = test_turn(&"x".repeat(MAX_JSONL_RECORD_BYTES));
        assert!(store.append_turn("other", &oversized).is_err());
    }

    #[test]
    fn runtime_state_is_bounded_atomic_and_owner_only() {
        let path = temp_root().join("runtime-state.json");
        save_runtime_state(&path, &RuntimeState::default()).expect("save runtime state");
        load_runtime_state(&path).expect("load runtime state");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path)
                    .expect("state metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        File::create(&path)
            .expect("replace state")
            .set_len(MAX_RUNTIME_STATE_BYTES + 1)
            .expect("make sparse oversized state");
        assert!(load_runtime_state(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn session_store_rejects_symlinked_state_files() {
        use std::os::unix::fs::symlink;

        let root = temp_root();
        let store = SessionStore::new(&root).expect("create store");
        let outside = temp_root().join("outside.jsonl");
        fs::create_dir_all(outside.parent().expect("outside parent")).expect("create outside dir");
        fs::write(&outside, b"sentinel").expect("write outside file");
        let path = store.session_path("chat").expect("session path");
        symlink(&outside, &path).expect("create symlink");

        assert!(store.append_turn("chat", &test_turn("secret")).is_err());
        assert!(store.load_recent_turns("chat", 10).is_err());
        assert_eq!(fs::read(outside).expect("read outside"), b"sentinel");
    }
}
