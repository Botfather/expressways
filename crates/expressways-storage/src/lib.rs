use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use expressways_protocol::{Classification, RetentionClass, StoredMessage, TopicSpec};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const FRAME_LEN_BYTES: u64 = 4;
const INDEX_ENTRY_BYTES: u64 = 8;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_max_bytes: u64,
    pub default_retention_class: RetentionClass,
    pub default_classification: Classification,
}

#[derive(Debug)]
pub struct Storage {
    config: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TopicState {
    spec: TopicSpec,
    next_offset: u64,
    active_segment_base: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentInfo {
    base_offset: u64,
    message_count: u64,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("binary serialization error: {0}")]
    BinarySerialization(#[from] Box<bincode::ErrorKind>),
    #[error("invalid topic name `{0}`")]
    InvalidTopic(String),
    #[error("topic `{0}` does not exist")]
    MissingTopic(String),
    #[error("index for segment `{segment}` is corrupt")]
    CorruptIndex { segment: String },
}

impl Storage {
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        fs::create_dir_all(&config.data_dir)?;
        Ok(Self { config })
    }

    pub fn ensure_topic(&self, spec: TopicSpec) -> Result<TopicSpec, StorageError> {
        validate_topic_name(&spec.name)?;
        let topic_dir = self.topic_dir(&spec.name);
        let state_path = topic_dir.join("state.json");
        let segments_dir = topic_dir.join("segments");

        fs::create_dir_all(&segments_dir)?;

        if state_path.exists() {
            self.ensure_binary_layout(&spec.name)?;
            let state = self.read_state(&spec.name)?;
            return Ok(state.spec);
        }

        let state = TopicState {
            spec: spec.clone(),
            next_offset: 0,
            active_segment_base: 0,
        };

        self.persist_state(&state)?;
        Ok(spec)
    }

    pub fn append(
        &self,
        topic: &str,
        producer: &str,
        classification: Option<Classification>,
        payload: String,
    ) -> Result<StoredMessage, StorageError> {
        let spec = self.ensure_topic(self.default_topic_spec(topic))?;
        let mut state = self.read_state(topic)?;

        let message = StoredMessage {
            message_id: Uuid::now_v7(),
            topic: topic.to_owned(),
            offset: state.next_offset,
            timestamp: Utc::now(),
            producer: producer.to_owned(),
            classification: classification.unwrap_or(spec.default_classification),
            payload,
        };

        let encoded = bincode::serialize(&message)?;
        let entry_len = FRAME_LEN_BYTES + encoded.len() as u64;
        let mut segment_base = state.active_segment_base;
        let mut segment_path = self.segment_path(topic, segment_base);
        let mut index_path = self.index_path(topic, segment_base);
        let current_len = file_len(&segment_path)?;

        if current_len > 0 && current_len + entry_len > self.config.segment_max_bytes {
            segment_base = state.next_offset;
            state.active_segment_base = segment_base;
            segment_path = self.segment_path(topic, segment_base);
            index_path = self.index_path(topic, segment_base);
        }

        if let Some(parent) = segment_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let write_position = file_len(&segment_path)?;
        let mut segment = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;
        segment.write_all(&(encoded.len() as u32).to_le_bytes())?;
        segment.write_all(&encoded)?;
        segment.flush()?;

        let mut index = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)?;
        index.write_all(&write_position.to_le_bytes())?;
        index.flush()?;

        state.next_offset += 1;
        self.persist_state(&state)?;

        Ok(message)
    }

    pub fn read_from(
        &self,
        topic: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        validate_topic_name(topic)?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let topic_dir = self.topic_dir(topic);
        if !topic_dir.exists() {
            return Err(StorageError::MissingTopic(topic.to_owned()));
        }
        self.ensure_binary_layout(topic)?;

        let mut messages = Vec::new();
        for segment in self.segment_infos(topic)? {
            if segment.base_offset + segment.message_count <= offset {
                continue;
            }

            let start_index = offset.saturating_sub(segment.base_offset);
            let mut index_file = File::open(self.index_path(topic, segment.base_offset))?;
            let mut segment_file = File::open(self.segment_path(topic, segment.base_offset))?;

            index_file.seek(SeekFrom::Start(start_index * INDEX_ENTRY_BYTES))?;

            for _ in start_index..segment.message_count {
                let mut position_bytes = [0u8; 8];
                index_file.read_exact(&mut position_bytes)?;
                let position = u64::from_le_bytes(position_bytes);

                segment_file.seek(SeekFrom::Start(position))?;

                let mut len_bytes = [0u8; 4];
                segment_file.read_exact(&mut len_bytes)?;
                let frame_len = u32::from_le_bytes(len_bytes) as usize;
                let mut payload = vec![0u8; frame_len];
                segment_file.read_exact(&mut payload)?;

                let message: StoredMessage = bincode::deserialize(&payload)?;
                if message.offset < offset {
                    continue;
                }

                messages.push(message);
                if messages.len() == limit {
                    return Ok(messages);
                }
            }
        }

        Ok(messages)
    }

    pub fn next_offset(&self, topic: &str) -> Result<u64, StorageError> {
        self.ensure_binary_layout(topic)?;
        Ok(self.read_state(topic)?.next_offset)
    }

    fn default_topic_spec(&self, topic: &str) -> TopicSpec {
        TopicSpec {
            name: topic.to_owned(),
            retention_class: self.config.default_retention_class.clone(),
            default_classification: self.config.default_classification.clone(),
        }
    }

    fn read_state(&self, topic: &str) -> Result<TopicState, StorageError> {
        validate_topic_name(topic)?;
        let path = self.topic_dir(topic).join("state.json");
        if !path.exists() {
            return Err(StorageError::MissingTopic(topic.to_owned()));
        }

        let data = fs::read(path)?;
        Ok(serde_json::from_slice(&data)?)
    }

    fn persist_state(&self, state: &TopicState) -> Result<(), StorageError> {
        let path = self.topic_dir(&state.spec.name).join("state.json");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_vec_pretty(state)?;
        fs::write(path, data)?;
        Ok(())
    }

    fn ensure_binary_layout(&self, topic: &str) -> Result<(), StorageError> {
        let legacy_segments = self.legacy_segment_paths(topic)?;
        if legacy_segments.is_empty() || self.migration_marker_path(topic).exists() {
            return Ok(());
        }

        let mut messages = self.read_legacy_messages(&legacy_segments)?;
        messages.extend(self.read_binary_messages(topic)?);
        messages.sort_by_key(|message| message.offset);
        messages.dedup_by_key(|message| message.offset);

        self.rewrite_binary_segments(topic, &messages)?;
        fs::write(
            self.migration_marker_path(topic),
            b"legacy segments migrated",
        )?;
        Ok(())
    }

    fn segment_infos(&self, topic: &str) -> Result<Vec<SegmentInfo>, StorageError> {
        let segments_dir = self.topic_dir(topic).join("segments");
        let mut infos = Vec::new();

        for entry in fs::read_dir(segments_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("idx") {
                continue;
            }

            let stem = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| StorageError::CorruptIndex {
                    segment: path.display().to_string(),
                })?;
            let base_offset = stem
                .parse::<u64>()
                .map_err(|_| StorageError::CorruptIndex {
                    segment: path.display().to_string(),
                })?;
            let len = fs::metadata(&path)?.len();
            if len % INDEX_ENTRY_BYTES != 0 {
                return Err(StorageError::CorruptIndex {
                    segment: path.display().to_string(),
                });
            }

            infos.push(SegmentInfo {
                base_offset,
                message_count: len / INDEX_ENTRY_BYTES,
            });
        }

        infos.sort_by_key(|info| info.base_offset);
        Ok(infos)
    }

    fn read_legacy_messages(&self, paths: &[PathBuf]) -> Result<Vec<StoredMessage>, StorageError> {
        let mut messages = Vec::new();

        for path in paths {
            let file = File::open(path)?;
            for line in BufReader::new(file).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                messages.push(serde_json::from_str(&line)?);
            }
        }

        Ok(messages)
    }

    fn read_binary_messages(&self, topic: &str) -> Result<Vec<StoredMessage>, StorageError> {
        let mut messages = Vec::new();

        for segment in self.segment_infos(topic)? {
            let mut index_file = File::open(self.index_path(topic, segment.base_offset))?;
            let mut segment_file = File::open(self.segment_path(topic, segment.base_offset))?;

            for _ in 0..segment.message_count {
                let mut position_bytes = [0u8; 8];
                index_file.read_exact(&mut position_bytes)?;
                let position = u64::from_le_bytes(position_bytes);

                segment_file.seek(SeekFrom::Start(position))?;
                let mut len_bytes = [0u8; 4];
                segment_file.read_exact(&mut len_bytes)?;
                let frame_len = u32::from_le_bytes(len_bytes) as usize;
                let mut payload = vec![0u8; frame_len];
                segment_file.read_exact(&mut payload)?;
                messages.push(bincode::deserialize(&payload)?);
            }
        }

        Ok(messages)
    }

    fn rewrite_binary_segments(
        &self,
        topic: &str,
        messages: &[StoredMessage],
    ) -> Result<(), StorageError> {
        let segments_dir = self.topic_dir(topic).join("segments");
        fs::create_dir_all(&segments_dir)?;

        for entry in fs::read_dir(&segments_dir)? {
            let path = entry?.path();
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("seg") | Some("idx") => fs::remove_file(path)?,
                _ => {}
            }
        }

        let mut state = self.read_state(topic)?;
        let mut current_base = 0u64;
        let mut current_segment_len = 0u64;
        let mut last_base = 0u64;

        for message in messages {
            let encoded = bincode::serialize(message)?;
            let entry_len = FRAME_LEN_BYTES + encoded.len() as u64;

            if current_segment_len == 0 {
                current_base = message.offset;
                last_base = current_base;
            } else if current_segment_len + entry_len > self.config.segment_max_bytes {
                current_base = message.offset;
                current_segment_len = 0;
                last_base = current_base;
            }

            let segment_path = self.segment_path(topic, current_base);
            let index_path = self.index_path(topic, current_base);
            let write_position = current_segment_len;

            let mut segment = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&segment_path)?;
            segment.write_all(&(encoded.len() as u32).to_le_bytes())?;
            segment.write_all(&encoded)?;
            segment.flush()?;

            let mut index = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&index_path)?;
            index.write_all(&write_position.to_le_bytes())?;
            index.flush()?;

            current_segment_len += entry_len;
        }

        state.active_segment_base = if messages.is_empty() { 0 } else { last_base };
        state.next_offset = messages
            .iter()
            .map(|message| message.offset)
            .max()
            .map(|offset| offset + 1)
            .unwrap_or(0);
        self.persist_state(&state)?;

        Ok(())
    }

    fn legacy_segment_paths(&self, topic: &str) -> Result<Vec<PathBuf>, StorageError> {
        let segments_dir = self.topic_dir(topic).join("segments");
        if !segments_dir.exists() {
            return Ok(Vec::new());
        }

        let mut legacy = fs::read_dir(segments_dir)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
            .collect::<Vec<_>>();
        legacy.sort();
        Ok(legacy)
    }

    fn migration_marker_path(&self, topic: &str) -> PathBuf {
        self.topic_dir(topic).join("legacy.migrated")
    }

    fn topic_dir(&self, topic: &str) -> PathBuf {
        self.config.data_dir.join(topic)
    }

    fn segment_path(&self, topic: &str, base_offset: u64) -> PathBuf {
        self.topic_dir(topic)
            .join("segments")
            .join(format!("{base_offset:020}.seg"))
    }

    fn index_path(&self, topic: &str, base_offset: u64) -> PathBuf {
        self.topic_dir(topic)
            .join("segments")
            .join(format!("{base_offset:020}.idx"))
    }
}

fn validate_topic_name(name: &str) -> Result<(), StorageError> {
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.');

    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidTopic(name.to_owned()))
    }
}

fn file_len(path: &Path) -> Result<u64, StorageError> {
    if path.exists() {
        Ok(fs::metadata(path)?.len())
    } else {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_storage(segment_max_bytes: u64) -> Storage {
        let data_dir = std::env::temp_dir().join(format!("expressways-storage-{}", Uuid::now_v7()));
        Storage::new(StorageConfig {
            data_dir,
            segment_max_bytes,
            default_retention_class: RetentionClass::Operational,
            default_classification: Classification::Internal,
        })
        .expect("create storage")
    }

    #[test]
    fn append_and_read_messages() {
        let storage = test_storage(1024);

        storage
            .append("tasks", "local:developer", None, "hello".to_owned())
            .expect("append first");
        storage
            .append(
                "tasks",
                "local:developer",
                Some(Classification::Confidential),
                "world".to_owned(),
            )
            .expect("append second");

        let messages = storage.read_from("tasks", 0, 10).expect("read messages");

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].classification, Classification::Internal);
        assert_eq!(messages[1].classification, Classification::Confidential);
        assert_eq!(storage.next_offset("tasks").expect("offset"), 2);
    }

    #[test]
    fn storage_rolls_segments_and_reads_from_offset() {
        let storage = test_storage(220);

        storage
            .append("tasks", "local:developer", None, "first".repeat(8))
            .expect("append first");
        storage
            .append("tasks", "local:developer", None, "second".repeat(8))
            .expect("append second");
        storage
            .append("tasks", "local:developer", None, "third".repeat(8))
            .expect("append third");

        let messages = storage.read_from("tasks", 1, 10).expect("read from offset");

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].offset, 1);
        assert_eq!(messages[1].offset, 2);
    }

    #[test]
    fn migrates_legacy_json_segments() {
        let storage = test_storage(4096);
        storage
            .ensure_topic(TopicSpec {
                name: "tasks".to_owned(),
                retention_class: RetentionClass::Operational,
                default_classification: Classification::Internal,
            })
            .expect("create topic");

        let topic_dir = storage.topic_dir("tasks");
        let segments_dir = topic_dir.join("segments");
        fs::remove_file(topic_dir.join("state.json")).expect("remove state");
        fs::write(
            topic_dir.join("state.json"),
            serde_json::to_vec(&TopicState {
                spec: TopicSpec {
                    name: "tasks".to_owned(),
                    retention_class: RetentionClass::Operational,
                    default_classification: Classification::Internal,
                },
                next_offset: 1,
                active_segment_base: 0,
            })
            .expect("serialize state"),
        )
        .expect("write state");
        fs::write(
            segments_dir.join("00000000000000000000.jsonl"),
            format!(
                "{}\n",
                serde_json::to_string(&StoredMessage {
                    message_id: Uuid::now_v7(),
                    topic: "tasks".to_owned(),
                    offset: 0,
                    timestamp: Utc::now(),
                    producer: "legacy".to_owned(),
                    classification: Classification::Internal,
                    payload: "from legacy".to_owned(),
                })
                .expect("serialize message")
            ),
        )
        .expect("write legacy segment");

        let messages = storage
            .read_from("tasks", 0, 10)
            .expect("read migrated data");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, "from legacy");
        assert!(segments_dir.join("00000000000000000000.seg").exists());
        assert!(segments_dir.join("00000000000000000000.idx").exists());
    }
}
