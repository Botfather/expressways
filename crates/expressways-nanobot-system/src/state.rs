use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::model::{MemoryEntry, RuntimeState, SessionRole, SessionTurn};

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
        std::fs::create_dir_all(&root)
            .with_context(|| format!("failed to create session store at {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn append_turn(&self, session_id: &str, turn: &SessionTurn) -> anyhow::Result<()> {
        let path = self.session_path(session_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut rendered = serde_json::to_vec(turn).context("failed to serialize session turn")?;
        rendered.push(b'\n');
        file.write_all(&rendered)
            .with_context(|| format!("failed to append {}", path.display()))?;
        Ok(())
    }

    pub fn load_recent_turns(
        &self,
        session_id: &str,
        max_turns: usize,
    ) -> anyhow::Result<Vec<SessionTurn>> {
        let path = self.session_path(session_id);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = std::fs::File::open(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut turns = Vec::new();
        for line in reader.lines() {
            let line = line.with_context(|| format!("failed to read {}", path.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(turn) = serde_json::from_str::<SessionTurn>(&line) {
                turns.push(turn);
            }
        }

        Ok(trim_turns_for_context(turns, max_turns.max(1)))
    }

    fn session_path(&self, session_id: &str) -> PathBuf {
        self.root
            .join(format!("{}.jsonl", sanitize_component(session_id)))
    }
}

impl MemoryStore {
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("failed to create memory store at {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn append_note(&self, session_id: &str, note: impl Into<String>) -> anyhow::Result<()> {
        let note = note.into();
        if note.trim().is_empty() {
            return Ok(());
        }

        let path = self.memory_path(session_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let entry = MemoryEntry {
            timestamp: chrono::Utc::now(),
            note,
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut rendered =
            serde_json::to_vec(&entry).context("failed to serialize memory entry")?;
        rendered.push(b'\n');
        file.write_all(&rendered)
            .with_context(|| format!("failed to append {}", path.display()))?;
        Ok(())
    }

    pub fn summarize_recent(
        &self,
        session_id: &str,
        max_entries: usize,
    ) -> anyhow::Result<Option<String>> {
        let path = self.memory_path(session_id);
        if !path.exists() {
            return Ok(None);
        }

        let file = std::fs::File::open(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line.with_context(|| format!("failed to read {}", path.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<MemoryEntry>(&line) {
                entries.push(entry);
            }
        }

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

    fn memory_path(&self, session_id: &str) -> PathBuf {
        self.root
            .join(format!("{}.memory.jsonl", sanitize_component(session_id)))
    }
}

pub fn load_runtime_state(path: &Path) -> anyhow::Result<RuntimeState> {
    if !path.exists() {
        return Ok(RuntimeState::default());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read runtime state {}", path.display()))?;
    serde_json::from_str(&raw).context("failed to parse runtime state")
}

pub fn save_runtime_state(path: &Path, state: &RuntimeState) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = serde_json::to_vec_pretty(state).context("failed to serialize runtime state")?;
    std::fs::write(path, rendered)
        .with_context(|| format!("failed to write runtime state {}", path.display()))?;
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

fn sanitize_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "session".to_owned()
    } else {
        out
    }
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
    fn sanitize_component_replaces_path_characters() {
        assert_eq!(sanitize_component("../abc/def"), ".._abc_def");
        assert_eq!(sanitize_component(""), "session");
    }
}
