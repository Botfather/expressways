use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use chrono::Utc;
use expressways_protocol::{Classification, RetentionClass, StoredMessage, TopicSpec};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const FRAME_LEN_BYTES: u64 = 4;
const INDEX_ENTRY_BYTES: u64 = 8;
const MAX_STORED_FRAME_BYTES: u64 = 64 * 1024 * 1024;
const MAX_TOPIC_STATE_BYTES: u64 = 64 * 1024;
const MAX_LEGACY_MIGRATION_BYTES: u64 = 256 * 1024 * 1024;
const TOPIC_STATE_SCHEMA_VERSION: u32 = 1;
const LEGACY_TOPIC_STATE_SCHEMA_VERSION: u32 = 0;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_max_bytes: u64,
    pub default_retention_class: RetentionClass,
    pub default_classification: Classification,
    pub retention_policy: RetentionPolicy,
    pub disk_pressure: DiskPressurePolicy,
}

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub ephemeral_max_bytes: u64,
    pub operational_max_bytes: u64,
    pub regulated_max_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct DiskPressurePolicy {
    pub max_total_bytes: u64,
    pub reclaim_target_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintenanceStats {
    pub reclaimed_segments: u64,
    pub reclaimed_bytes: u64,
    pub recovered_segments: u64,
    pub truncated_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStats {
    pub topic_count: u64,
    pub segment_count: u64,
    pub total_bytes: u64,
    pub maintenance: MaintenanceStats,
}

#[derive(Debug)]
pub struct Storage {
    config: StorageConfig,
    maintenance: Mutex<MaintenanceStats>,
    total_bytes: Mutex<u64>,
    topic_states: Mutex<HashMap<String, TopicState>>,
    topic_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TopicState {
    #[serde(default)]
    schema_version: u32,
    spec: TopicSpec,
    next_offset: u64,
    active_segment_base: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentInfo {
    base_offset: u64,
    message_count: u64,
    segment_bytes: u64,
    index_bytes: u64,
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
    #[error(
        "disk pressure cannot be reduced below {max_total_bytes} bytes; current usage is {current_bytes} bytes"
    )]
    DiskPressure {
        current_bytes: u64,
        max_total_bytes: u64,
    },
    #[error("index for segment `{segment}` is corrupt")]
    CorruptIndex { segment: String },
    #[error("serialized message frame is too large: {bytes} bytes")]
    FrameTooLarge { bytes: usize },
    #[error("topic state file is too large: {bytes} bytes")]
    StateTooLarge { bytes: u64 },
    #[error("legacy segment `{path}` is not a regular file")]
    InvalidLegacySegment { path: String },
    #[error("legacy migration input is too large: {bytes} bytes; maximum is {max_bytes} bytes")]
    LegacyMigrationTooLarge { bytes: u64, max_bytes: u64 },
    #[error("legacy record in `{path}` is too large: more than {max_bytes} bytes")]
    LegacyRecordTooLarge { path: String, max_bytes: u64 },
    #[error("topic `{topic}` offset space is exhausted")]
    OffsetExhausted { topic: String },
    #[error(
        "message at offset {offset} requires {encoded_bytes} JSON bytes, exceeding response budget {max_bytes}"
    )]
    MessageExceedsReadBudget {
        offset: u64,
        encoded_bytes: usize,
        max_bytes: usize,
    },
    #[error(
        "topic `{topic}` state schema version {found} is newer than supported version {supported}"
    )]
    UnsupportedStateVersion {
        topic: String,
        found: u32,
        supported: u32,
    },
}

impl Storage {
    fn lock_maintenance(&self) -> MutexGuard<'_, MaintenanceStats> {
        self.maintenance
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn topic_lock(&self, topic: &str) -> Arc<Mutex<()>> {
        self.topic_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(topic.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        if config.disk_pressure.reclaim_target_bytes > config.disk_pressure.max_total_bytes {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reclaim_target_bytes must be less than or equal to max_total_bytes",
            )));
        }
        fs::create_dir_all(&config.data_dir)?;
        set_private_directory_permissions(&config.data_dir)?;
        let total_bytes = storage_bytes_in_dir(&config.data_dir)?;
        Ok(Self {
            config,
            maintenance: Mutex::new(MaintenanceStats::default()),
            total_bytes: Mutex::new(total_bytes),
            topic_states: Mutex::new(HashMap::new()),
            topic_locks: Mutex::new(HashMap::new()),
        })
    }

    pub fn ensure_topic(&self, spec: TopicSpec) -> Result<TopicSpec, StorageError> {
        validate_topic_name(&spec.name)?;
        let topic_dir = self.topic_dir(&spec.name);
        let state_path = topic_dir.join("state.json");
        let segments_dir = topic_dir.join("segments");

        fs::create_dir_all(&segments_dir)?;

        if state_path.exists() {
            if let Some(state) = self.cached_topic_state(&spec.name) {
                return Ok(state.spec);
            }
            self.ensure_binary_layout(&spec.name)?;
            self.recover_topic(&spec.name)?;
            let state = self
                .cached_topic_state(&spec.name)
                .ok_or_else(|| StorageError::MissingTopic(spec.name.clone()))?;
            return Ok(state.spec);
        }

        let state = TopicState {
            schema_version: TOPIC_STATE_SCHEMA_VERSION,
            spec: spec.clone(),
            next_offset: 0,
            active_segment_base: 0,
        };

        self.persist_state(&state)?;
        self.cache_topic_state(state);
        Ok(spec)
    }

    pub fn append(
        &self,
        topic: &str,
        producer: &str,
        classification: Option<Classification>,
        payload: String,
    ) -> Result<StoredMessage, StorageError> {
        validate_topic_name(topic)?;
        let topic_lock = self.topic_lock(topic);
        let _topic_guard = topic_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut state = self.cached_topic_state(topic);
        if state.is_none() {
            self.ensure_topic(self.default_topic_spec(topic))?;
            state = self.cached_topic_state(topic);
        }
        let mut state = state.ok_or_else(|| StorageError::MissingTopic(topic.to_owned()))?;
        let spec = state.spec.clone();
        let next_offset =
            state
                .next_offset
                .checked_add(1)
                .ok_or_else(|| StorageError::OffsetExhausted {
                    topic: topic.to_owned(),
                })?;

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
        if encoded.len() as u64 > MAX_STORED_FRAME_BYTES {
            return Err(StorageError::FrameTooLarge {
                bytes: encoded.len(),
            });
        }
        let frame_len = u32::try_from(encoded.len()).map_err(|_| StorageError::FrameTooLarge {
            bytes: encoded.len(),
        })?;
        let entry_len = FRAME_LEN_BYTES + u64::from(frame_len) + INDEX_ENTRY_BYTES;
        let mut segment_base = state.active_segment_base;
        let mut segment_path = self.segment_path(topic, segment_base);
        let mut index_path = self.index_path(topic, segment_base);
        let current_len = file_len(&segment_path)?;

        if current_len > 0 && current_len.saturating_add(entry_len) > self.config.segment_max_bytes
        {
            segment_base = state.next_offset;
            state.active_segment_base = segment_base;
            segment_path = self.segment_path(topic, segment_base);
            index_path = self.index_path(topic, segment_base);
        }

        if let Some(parent) = segment_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let write_position = file_len(&segment_path)?;
        let original_index_len = file_len(&index_path)?;
        self.reserve_global_budget(entry_len)?;
        let write_result = (|| -> Result<(), StorageError> {
            let mut segment_options = OpenOptions::new();
            segment_options.create(true).append(true);
            set_private_open_mode(&mut segment_options);
            let mut segment = segment_options.open(&segment_path)?;
            segment.write_all(&frame_len.to_le_bytes())?;
            segment.write_all(&encoded)?;
            segment.flush()?;

            let mut index_options = OpenOptions::new();
            index_options.create(true).append(true);
            set_private_open_mode(&mut index_options);
            let mut index = index_options.open(&index_path)?;
            index.write_all(&write_position.to_le_bytes())?;
            index.flush()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            let segment_rollback = truncate_file(&segment_path, write_position);
            let index_rollback = truncate_file(&index_path, original_index_len);
            if segment_rollback.is_ok() && index_rollback.is_ok() {
                self.decrease_total_bytes(entry_len);
            } else {
                self.refresh_total_bytes()?;
                segment_rollback?;
                index_rollback?;
            }
            return Err(error);
        }

        state.next_offset = next_offset;
        self.cache_topic_state(state);
        self.enforce_topic_retention(topic, &spec.retention_class)?;

        Ok(message)
    }

    pub fn read_from(
        &self,
        topic: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        self.read_from_bounded(topic, offset, limit, usize::MAX)
    }

    pub fn read_from_bounded(
        &self,
        topic: &str,
        offset: u64,
        limit: usize,
        max_json_bytes: usize,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        validate_topic_name(topic)?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let topic_dir = self.topic_dir(topic);
        if !topic_dir.exists() {
            return Err(StorageError::MissingTopic(topic.to_owned()));
        }
        let topic_lock = self.topic_lock(topic);
        let _topic_guard = topic_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cached_topic_state(topic).is_none() {
            self.ensure_binary_layout(topic)?;
            self.recover_topic(topic)?;
        }

        let mut messages = Vec::new();
        let mut json_bytes = 0usize;
        for segment in self.segment_infos(topic)? {
            if segment.base_offset.saturating_add(segment.message_count) <= offset {
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
                if frame_len as u64 > MAX_STORED_FRAME_BYTES {
                    return Err(StorageError::FrameTooLarge { bytes: frame_len });
                }
                let mut payload = vec![0u8; frame_len];
                segment_file.read_exact(&mut payload)?;

                let message: StoredMessage = bincode::deserialize(&payload)?;
                if message.offset < offset {
                    continue;
                }

                let encoded_bytes = serde_json::to_vec(&message)?.len();
                let projected = json_bytes.saturating_add(encoded_bytes).saturating_add(1);
                if projected > max_json_bytes {
                    if messages.is_empty() {
                        return Err(StorageError::MessageExceedsReadBudget {
                            offset: message.offset,
                            encoded_bytes,
                            max_bytes: max_json_bytes,
                        });
                    }
                    return Ok(messages);
                }
                json_bytes = projected;
                messages.push(message);
                if messages.len() == limit {
                    return Ok(messages);
                }
            }
        }

        Ok(messages)
    }

    pub fn next_offset(&self, topic: &str) -> Result<u64, StorageError> {
        validate_topic_name(topic)?;
        let topic_lock = self.topic_lock(topic);
        let _topic_guard = topic_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cached_topic_state(topic).is_none() {
            self.ensure_binary_layout(topic)?;
            self.recover_topic(topic)?;
        }
        if let Some(state) = self.cached_topic_state(topic) {
            return Ok(state.next_offset);
        }
        let state = self.reconcile_topic_state(topic)?;
        self.cache_topic_state(state.clone());
        Ok(state.next_offset)
    }

    pub fn stats(&self) -> Result<StorageStats, StorageError> {
        let mut topic_count = 0_u64;
        let mut segment_count = 0_u64;
        let mut total_bytes = 0_u64;

        if self.config.data_dir.exists() {
            for entry in fs::read_dir(&self.config.data_dir)? {
                let path = entry?.path();
                if !path.is_dir() {
                    continue;
                }

                topic_count = topic_count.saturating_add(1);
                let topic_name = path
                    .file_name()
                    .and_then(|item| item.to_str())
                    .ok_or_else(|| StorageError::InvalidTopic(path.display().to_string()))?;
                let topic_lock = self.topic_lock(topic_name);
                let _topic_guard = topic_lock
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if self.cached_topic_state(topic_name).is_none() {
                    self.ensure_binary_layout(topic_name)?;
                    self.recover_topic(topic_name)?;
                }

                for segment in self.segment_infos(topic_name)? {
                    segment_count = segment_count.saturating_add(1);
                    total_bytes = total_bytes
                        .saturating_add(segment.segment_bytes.saturating_add(segment.index_bytes));
                }
            }
        }

        Ok(StorageStats {
            topic_count,
            segment_count,
            total_bytes,
            maintenance: self.lock_maintenance().clone(),
        })
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

        let data = read_bounded_file(&path, MAX_TOPIC_STATE_BYTES).map_err(|error| {
            if error.kind() == std::io::ErrorKind::InvalidData {
                StorageError::StateTooLarge {
                    bytes: fs::metadata(&path).map_or(MAX_TOPIC_STATE_BYTES + 1, |item| item.len()),
                }
            } else {
                StorageError::Io(error)
            }
        })?;
        let mut state: TopicState = serde_json::from_slice(&data)?;
        let migrated = match state.schema_version {
            LEGACY_TOPIC_STATE_SCHEMA_VERSION => {
                state.schema_version = TOPIC_STATE_SCHEMA_VERSION;
                true
            }
            TOPIC_STATE_SCHEMA_VERSION => false,
            found => {
                return Err(StorageError::UnsupportedStateVersion {
                    topic: topic.to_owned(),
                    found,
                    supported: TOPIC_STATE_SCHEMA_VERSION,
                });
            }
        };
        if migrated {
            self.persist_state(&state)?;
        }
        Ok(state)
    }

    fn cached_topic_state(&self, topic: &str) -> Option<TopicState> {
        self.topic_states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(topic)
            .cloned()
    }

    fn cache_topic_state(&self, state: TopicState) {
        self.topic_states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(state.spec.name.clone(), state);
    }

    fn reconcile_topic_state(&self, topic: &str) -> Result<TopicState, StorageError> {
        let mut state = self.read_state(topic)?;
        let segments = self.segment_infos(topic)?;
        let (next_offset, active_segment_base) = if let Some(segment) = segments.last() {
            (
                segment
                    .base_offset
                    .checked_add(segment.message_count)
                    .ok_or_else(|| StorageError::OffsetExhausted {
                        topic: topic.to_owned(),
                    })?,
                segment.base_offset,
            )
        } else {
            (0, 0)
        };
        if state.next_offset != next_offset || state.active_segment_base != active_segment_base {
            state.next_offset = next_offset;
            state.active_segment_base = active_segment_base;
            self.persist_state(&state)?;
        }
        Ok(state)
    }

    fn persist_state(&self, state: &TopicState) -> Result<(), StorageError> {
        let path = self.topic_dir(&state.spec.name).join("state.json");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut persisted = state.clone();
        persisted.schema_version = TOPIC_STATE_SCHEMA_VERSION;
        let data = serde_json::to_vec_pretty(&persisted)?;
        atomic_replace(&path, &data)?;
        Ok(())
    }

    fn ensure_binary_layout(&self, topic: &str) -> Result<(), StorageError> {
        let legacy_segments = self.legacy_segment_paths(topic)?;
        if legacy_segments.is_empty() || self.migration_marker_path(topic).exists() {
            return Ok(());
        }

        self.recover_topic(topic)?;
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

    fn recover_topic(&self, topic: &str) -> Result<(), StorageError> {
        validate_topic_name(topic)?;
        let segments_dir = self.topic_dir(topic).join("segments");
        if !segments_dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&segments_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("idx")
                && !self.segment_path_exists_for_index(&path)
            {
                let bytes = fs::metadata(&path)?.len();
                fs::remove_file(path)?;
                self.decrease_total_bytes(bytes);
            }
        }

        for base_offset in self.segment_bases(topic)? {
            self.recover_segment(topic, base_offset)?;
        }

        let state = self.reconcile_topic_state(topic)?;
        self.cache_topic_state(state);

        Ok(())
    }

    fn segment_infos(&self, topic: &str) -> Result<Vec<SegmentInfo>, StorageError> {
        let mut infos = Vec::new();
        for base_offset in self.segment_bases(topic)? {
            let segment_path = self.segment_path(topic, base_offset);
            let index_path = self.index_path(topic, base_offset);
            let index_len = fs::metadata(&index_path)?.len();
            if index_len % INDEX_ENTRY_BYTES != 0 {
                return Err(StorageError::CorruptIndex {
                    segment: index_path.display().to_string(),
                });
            }
            base_offset
                .checked_add(index_len / INDEX_ENTRY_BYTES)
                .ok_or_else(|| StorageError::OffsetExhausted {
                    topic: topic.to_owned(),
                })?;
            infos.push(SegmentInfo {
                base_offset,
                message_count: index_len / INDEX_ENTRY_BYTES,
                segment_bytes: fs::metadata(&segment_path)?.len(),
                index_bytes: index_len,
            });
        }

        infos.sort_by_key(|info| info.base_offset);
        Ok(infos)
    }

    fn read_legacy_messages(&self, paths: &[PathBuf]) -> Result<Vec<StoredMessage>, StorageError> {
        let mut messages = Vec::new();
        let mut migration_bytes = 0_u64;

        for path in paths {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.file_type().is_file() {
                return Err(StorageError::InvalidLegacySegment {
                    path: path.display().to_string(),
                });
            }
            migration_bytes = migration_bytes.checked_add(metadata.len()).ok_or(
                StorageError::LegacyMigrationTooLarge {
                    bytes: u64::MAX,
                    max_bytes: MAX_LEGACY_MIGRATION_BYTES,
                },
            )?;
            if migration_bytes > MAX_LEGACY_MIGRATION_BYTES {
                return Err(StorageError::LegacyMigrationTooLarge {
                    bytes: migration_bytes,
                    max_bytes: MAX_LEGACY_MIGRATION_BYTES,
                });
            }

            let file = File::open(path)?;
            let mut reader = BufReader::new(file);
            loop {
                let mut line = Vec::new();
                let bytes_read = reader
                    .by_ref()
                    .take(MAX_STORED_FRAME_BYTES + 2)
                    .read_until(b'\n', &mut line)?;
                if bytes_read == 0 {
                    break;
                }
                if line.len() as u64 > MAX_STORED_FRAME_BYTES + 1
                    || (line.len() as u64 == MAX_STORED_FRAME_BYTES + 1
                        && line.last() != Some(&b'\n'))
                {
                    return Err(StorageError::LegacyRecordTooLarge {
                        path: path.display().to_string(),
                        max_bytes: MAX_STORED_FRAME_BYTES,
                    });
                }
                let line = std::str::from_utf8(&line).map_err(|error| {
                    StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
                })?;
                if line.trim().is_empty() {
                    continue;
                }
                messages.push(serde_json::from_str(line)?);
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
                if frame_len as u64 > MAX_STORED_FRAME_BYTES {
                    return Err(StorageError::FrameTooLarge { bytes: frame_len });
                }
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
            if encoded.len() as u64 > MAX_STORED_FRAME_BYTES {
                return Err(StorageError::FrameTooLarge {
                    bytes: encoded.len(),
                });
            }
            let frame_len =
                u32::try_from(encoded.len()).map_err(|_| StorageError::FrameTooLarge {
                    bytes: encoded.len(),
                })?;
            let entry_len = FRAME_LEN_BYTES + u64::from(frame_len);

            if current_segment_len == 0 {
                current_base = message.offset;
                last_base = current_base;
            } else if current_segment_len.saturating_add(entry_len) > self.config.segment_max_bytes
            {
                current_base = message.offset;
                current_segment_len = 0;
                last_base = current_base;
            }

            let segment_path = self.segment_path(topic, current_base);
            let index_path = self.index_path(topic, current_base);
            let write_position = current_segment_len;

            let mut segment_options = OpenOptions::new();
            segment_options.create(true).append(true);
            set_private_open_mode(&mut segment_options);
            let mut segment = segment_options.open(&segment_path)?;
            segment.write_all(&frame_len.to_le_bytes())?;
            segment.write_all(&encoded)?;
            segment.flush()?;

            let mut index_options = OpenOptions::new();
            index_options.create(true).append(true);
            set_private_open_mode(&mut index_options);
            let mut index = index_options.open(&index_path)?;
            index.write_all(&write_position.to_le_bytes())?;
            index.flush()?;

            current_segment_len = current_segment_len.checked_add(entry_len).ok_or_else(|| {
                StorageError::OffsetExhausted {
                    topic: topic.to_owned(),
                }
            })?;
        }

        state.active_segment_base = if messages.is_empty() { 0 } else { last_base };
        state.next_offset =
            messages
                .iter()
                .map(|message| message.offset)
                .max()
                .map_or(Ok(0), |offset| {
                    offset
                        .checked_add(1)
                        .ok_or_else(|| StorageError::OffsetExhausted {
                            topic: topic.to_owned(),
                        })
                })?;
        self.persist_state(&state)?;
        self.refresh_total_bytes()?;

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

    fn retention_limit(&self, class: &RetentionClass) -> u64 {
        match class {
            RetentionClass::Ephemeral => self.config.retention_policy.ephemeral_max_bytes,
            RetentionClass::Operational => self.config.retention_policy.operational_max_bytes,
            RetentionClass::Regulated => self.config.retention_policy.regulated_max_bytes,
        }
    }

    fn segment_bases(&self, topic: &str) -> Result<Vec<u64>, StorageError> {
        let segments_dir = self.topic_dir(topic).join("segments");
        if !segments_dir.exists() {
            return Ok(Vec::new());
        }

        let mut bases = Vec::new();
        for entry in fs::read_dir(segments_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("seg") {
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
            bases.push(base_offset);
        }

        bases.sort_unstable();
        Ok(bases)
    }

    fn recover_segment(&self, topic: &str, base_offset: u64) -> Result<(), StorageError> {
        let segment_path = self.segment_path(topic, base_offset);
        let index_path = self.index_path(topic, base_offset);
        let mut segment = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment_path)?;
        let end = segment.seek(SeekFrom::End(0))?;
        segment.seek(SeekFrom::Start(0))?;

        let mut position = 0u64;
        let mut truncated_bytes = 0u64;
        let temp_index_path = index_path.with_extension(format!("idx.tmp-{}", Uuid::now_v7()));
        let mut index_options = OpenOptions::new();
        index_options.write(true).create_new(true);
        set_private_open_mode(&mut index_options);
        let mut rebuilt_index = index_options.open(&temp_index_path)?;

        let rebuild_result = (|| -> Result<(), StorageError> {
            loop {
                if position == end {
                    break;
                }
                let mut len_bytes = [0u8; 4];
                match segment.read_exact(&mut len_bytes) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                        if end > position {
                            segment.set_len(position)?;
                            truncated_bytes = end - position;
                        }
                        break;
                    }
                    Err(error) => return Err(StorageError::Io(error)),
                }

                let frame_len = u32::from_le_bytes(len_bytes) as u64;
                if frame_len > MAX_STORED_FRAME_BYTES {
                    segment.set_len(position)?;
                    truncated_bytes = end.saturating_sub(position);
                    break;
                }
                let Some(frame_end) = position
                    .checked_add(FRAME_LEN_BYTES)
                    .and_then(|position| position.checked_add(frame_len))
                else {
                    segment.set_len(position)?;
                    truncated_bytes = end.saturating_sub(position);
                    break;
                };
                if frame_end > end {
                    segment.set_len(position)?;
                    truncated_bytes = end - position;
                    break;
                }

                rebuilt_index.write_all(&position.to_le_bytes())?;
                segment.seek(SeekFrom::Start(frame_end))?;
                position = frame_end;
            }
            rebuilt_index.flush()?;
            rebuilt_index.sync_all()?;
            Ok(())
        })();
        if let Err(error) = rebuild_result {
            drop(rebuilt_index);
            let _ = fs::remove_file(&temp_index_path);
            return Err(error);
        }
        drop(rebuilt_index);

        if let Some(parent) = index_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let existing_index_len = file_len(&index_path)?;
        let rebuilt_index_len = file_len(&temp_index_path)?;
        let install_result = (|| -> Result<bool, StorageError> {
            let index_matches = index_path.exists() && files_equal(&index_path, &temp_index_path)?;
            if index_matches {
                fs::remove_file(&temp_index_path)?;
                return Ok(false);
            }
            #[cfg(windows)]
            if index_path.exists() {
                fs::remove_file(&index_path)?;
            }
            fs::rename(&temp_index_path, &index_path)?;
            Ok(true)
        })();
        let recovered = match install_result {
            Ok(recovered) => recovered,
            Err(error) => {
                let _ = fs::remove_file(&temp_index_path);
                return Err(error);
            }
        };
        if recovered {
            self.bump_recovered_segments(1);
        }
        let bytes_before = end.saturating_add(existing_index_len);
        let bytes_after = end
            .saturating_sub(truncated_bytes)
            .saturating_add(rebuilt_index_len);
        self.adjust_total_bytes(bytes_before, bytes_after);
        if truncated_bytes > 0 {
            self.bump_truncated_bytes(truncated_bytes);
        }

        Ok(())
    }

    fn segment_path_exists_for_index(&self, index_path: &Path) -> bool {
        let Some(stem) = index_path.file_stem().and_then(|stem| stem.to_str()) else {
            return false;
        };
        let Some(parent) = index_path.parent() else {
            return false;
        };
        parent.join(format!("{stem}.seg")).exists()
    }

    fn enforce_topic_retention(
        &self,
        topic: &str,
        retention_class: &RetentionClass,
    ) -> Result<(), StorageError> {
        let mut global_total = self
            .total_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let limit = self.retention_limit(retention_class);
        if *global_total <= limit {
            return Ok(());
        }
        let mut segments = self.segment_infos(topic)?;
        if segments.len() <= 1 {
            return Ok(());
        }

        let mut total_bytes = segments.iter().fold(0_u64, |total, segment| {
            total.saturating_add(segment.segment_bytes.saturating_add(segment.index_bytes))
        });
        while total_bytes > limit && segments.len() > 1 {
            let oldest = segments.remove(0);
            let removed = self.remove_segment(topic, &oldest)?;
            total_bytes = total_bytes.saturating_sub(removed);
            *global_total = global_total.saturating_sub(removed);
        }

        Ok(())
    }

    fn reserve_global_budget(&self, reserved_bytes: u64) -> Result<(), StorageError> {
        let mut total_bytes = self
            .total_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if total_bytes.saturating_add(reserved_bytes) <= self.config.disk_pressure.max_total_bytes {
            *total_bytes = total_bytes.saturating_add(reserved_bytes);
            return Ok(());
        }

        let mut reclaimable = self.reclaimable_segments()?;
        reclaimable.sort_by_key(|candidate| candidate.modified);

        for candidate in reclaimable {
            if total_bytes.saturating_add(reserved_bytes)
                <= self.config.disk_pressure.reclaim_target_bytes
            {
                break;
            }
            let removed = self.remove_segment(&candidate.topic, &candidate.segment)?;
            *total_bytes = total_bytes.saturating_sub(removed);
        }

        let projected = total_bytes.saturating_add(reserved_bytes);
        if projected > self.config.disk_pressure.max_total_bytes {
            return Err(StorageError::DiskPressure {
                current_bytes: projected,
                max_total_bytes: self.config.disk_pressure.max_total_bytes,
            });
        }

        *total_bytes = projected;
        Ok(())
    }

    fn decrease_total_bytes(&self, bytes: u64) {
        let mut total = self
            .total_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *total = total.saturating_sub(bytes);
    }

    fn adjust_total_bytes(&self, before: u64, after: u64) {
        let mut total = self
            .total_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if after >= before {
            *total = total.saturating_add(after - before);
        } else {
            *total = total.saturating_sub(before - after);
        }
    }

    fn refresh_total_bytes(&self) -> Result<(), StorageError> {
        let actual = storage_bytes_in_dir(&self.config.data_dir)?;
        let mut total = self
            .total_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *total = actual;
        Ok(())
    }

    fn reclaimable_segments(&self) -> Result<Vec<ReclaimCandidate>, StorageError> {
        let mut reclaimable = Vec::new();
        if !self.config.data_dir.exists() {
            return Ok(reclaimable);
        }

        for entry in fs::read_dir(&self.config.data_dir)? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let topic = path
                .file_name()
                .and_then(|item| item.to_str())
                .ok_or_else(|| StorageError::InvalidTopic(path.display().to_string()))?
                .to_owned();
            let segments = self.segment_infos(&topic)?;
            if segments.len() <= 1 {
                continue;
            }
            let active_base = segments
                .last()
                .map(|segment| segment.base_offset)
                .unwrap_or_default();

            for segment in segments
                .into_iter()
                .filter(|segment| segment.base_offset != active_base)
            {
                let modified = fs::metadata(self.segment_path(&topic, segment.base_offset))?
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                reclaimable.push(ReclaimCandidate {
                    topic: topic.clone(),
                    modified,
                    segment,
                });
            }
        }

        Ok(reclaimable)
    }

    fn remove_segment(&self, topic: &str, segment: &SegmentInfo) -> Result<u64, StorageError> {
        let segment_path = self.segment_path(topic, segment.base_offset);
        let index_path = self.index_path(topic, segment.base_offset);
        let bytes = segment.segment_bytes.saturating_add(segment.index_bytes);

        if segment_path.exists() {
            fs::remove_file(segment_path)?;
        }
        if index_path.exists() {
            fs::remove_file(index_path)?;
        }

        self.bump_reclaimed(1, bytes);
        Ok(bytes)
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

    fn bump_reclaimed(&self, segments: u64, bytes: u64) {
        let mut maintenance = self.lock_maintenance();
        maintenance.reclaimed_segments += segments;
        maintenance.reclaimed_bytes += bytes;
    }

    fn bump_recovered_segments(&self, segments: u64) {
        let mut maintenance = self.lock_maintenance();
        maintenance.recovered_segments += segments;
    }

    fn bump_truncated_bytes(&self, bytes: u64) {
        let mut maintenance = self.lock_maintenance();
        maintenance.truncated_bytes += bytes;
    }
}

#[derive(Debug)]
struct ReclaimCandidate {
    topic: String,
    modified: SystemTime,
    segment: SegmentInfo,
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

fn truncate_file(path: &Path, length: u64) -> Result<(), StorageError> {
    if path.exists() {
        OpenOptions::new().write(true).open(path)?.set_len(length)?;
    }
    Ok(())
}

fn read_bounded_file(path: &Path, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    let metadata_len = fs::metadata(path)?.len();
    if metadata_len > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file exceeds configured size limit",
        ));
    }
    let capacity = usize::try_from(metadata_len).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "file is too large to read")
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)?
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file grew beyond configured size limit",
        ));
    }
    Ok(bytes)
}

fn files_equal(left: &Path, right: &Path) -> std::io::Result<bool> {
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = BufReader::new(File::open(left)?);
    let mut right = BufReader::new(File::open(right)?);
    let mut left_buffer = [0u8; 8192];
    let mut right_buffer = [0u8; 8192];
    loop {
        let left_read = left.read(&mut left_buffer)?;
        let right_read = right.read(&mut right_buffer)?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

fn storage_bytes_in_dir(data_dir: &Path) -> Result<u64, StorageError> {
    let mut total_bytes = 0u64;
    if !data_dir.exists() {
        return Ok(total_bytes);
    }

    for topic_entry in fs::read_dir(data_dir)? {
        let topic_path = topic_entry?.path();
        if !topic_path.is_dir() {
            continue;
        }

        let segments_path = topic_path.join("segments");
        if !segments_path.is_dir() {
            continue;
        }
        for segment_entry in fs::read_dir(segments_path)? {
            let path = segment_entry?.path();
            if matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("seg" | "idx")
            ) {
                total_bytes = total_bytes.saturating_add(fs::metadata(path)?.len());
            }
        }
    }

    Ok(total_bytes)
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temp_path = path.with_extension(format!("tmp-{}", Uuid::now_v7()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        set_private_open_mode(&mut options);
        let mut file = options.open(&temp_path)?;
        file.write_all(bytes)?;
        file.flush()?;
        fs::rename(&temp_path, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn set_private_open_mode(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
}

fn set_private_directory_permissions(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
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
            retention_policy: RetentionPolicy {
                ephemeral_max_bytes: 256,
                operational_max_bytes: 1024,
                regulated_max_bytes: 2048,
            },
            disk_pressure: DiskPressurePolicy {
                max_total_bytes: 4096,
                reclaim_target_bytes: 3072,
            },
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

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&storage.config.data_dir)
                    .expect("data directory metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(storage.segment_path("tasks", 0))
                    .expect("segment metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(storage.index_path("tasks", 0))
                    .expect("index metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn bounded_reads_stop_before_exceeding_response_budget() {
        let storage = test_storage(4096);
        let first = storage
            .append("tasks", "local:developer", None, "first".repeat(16))
            .expect("append first");
        storage
            .append("tasks", "local:developer", None, "second".repeat(16))
            .expect("append second");
        let first_budget = serde_json::to_vec(&first).expect("serialize first").len() + 1;

        let messages = storage
            .read_from_bounded("tasks", 0, 10, first_budget)
            .expect("bounded read");
        assert_eq!(messages, vec![first.clone()]);

        let error = storage
            .read_from_bounded("tasks", 0, 10, first_budget - 1)
            .expect_err("single oversized message must fail explicitly");
        assert!(matches!(
            error,
            StorageError::MessageExceedsReadBudget { offset: 0, .. }
        ));
    }

    #[test]
    fn restart_recovers_offsets_from_segments_when_cached_state_was_not_persisted() {
        let storage = test_storage(4096);
        let config = storage.config.clone();
        storage
            .append("tasks", "local:developer", None, "first".to_owned())
            .expect("append first");
        storage
            .append("tasks", "local:developer", None, "second".to_owned())
            .expect("append second");
        drop(storage);

        let reopened = Storage::new(config).expect("reopen storage");
        let message = reopened
            .append("tasks", "local:developer", None, "third".to_owned())
            .expect("append after restart");

        assert_eq!(message.offset, 2);
        assert_eq!(reopened.next_offset("tasks").expect("next offset"), 3);
    }

    #[test]
    fn concurrent_appends_keep_offsets_unique_and_ordered() {
        let data_dir = std::env::temp_dir().join(format!("expressways-storage-{}", Uuid::now_v7()));
        let storage = Arc::new(
            Storage::new(StorageConfig {
                data_dir,
                segment_max_bytes: 1_048_576,
                default_retention_class: RetentionClass::Operational,
                default_classification: Classification::Internal,
                retention_policy: RetentionPolicy {
                    ephemeral_max_bytes: 64 * 1_048_576,
                    operational_max_bytes: 64 * 1_048_576,
                    regulated_max_bytes: 64 * 1_048_576,
                },
                disk_pressure: DiskPressurePolicy {
                    max_total_bytes: 64 * 1_048_576,
                    reclaim_target_bytes: 48 * 1_048_576,
                },
            })
            .expect("create storage"),
        );

        let workers = 8;
        let appends_per_worker = 25;
        let handles = (0..workers)
            .map(|worker| {
                let storage = Arc::clone(&storage);
                std::thread::spawn(move || {
                    for index in 0..appends_per_worker {
                        storage
                            .append(
                                "tasks",
                                &format!("worker-{worker}"),
                                None,
                                format!("message-{worker}-{index}"),
                            )
                            .expect("append concurrently");
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().expect("join append worker");
        }

        let expected = workers * appends_per_worker;
        let messages = storage
            .read_from("tasks", 0, expected)
            .expect("read concurrent messages");
        assert_eq!(messages.len(), expected);
        assert_eq!(
            messages
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            (0..expected as u64).collect::<Vec<_>>()
        );
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
        let config = storage.config.clone();
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
                schema_version: LEGACY_TOPIC_STATE_SCHEMA_VERSION,
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
        drop(storage);
        let storage = Storage::new(config).expect("reopen storage");

        let messages = storage
            .read_from("tasks", 0, 10)
            .expect("read migrated data");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, "from legacy");
        assert!(segments_dir.join("00000000000000000000.seg").exists());
        assert!(segments_dir.join("00000000000000000000.idx").exists());
        let migrated_state: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(topic_dir.join("state.json")).expect("read migrated state"),
        )
        .expect("parse migrated state");
        assert_eq!(migrated_state["schema_version"], TOPIC_STATE_SCHEMA_VERSION);
    }

    #[cfg(unix)]
    #[test]
    fn legacy_migration_rejects_symlinked_segments() {
        use std::os::unix::fs::symlink;

        let storage = test_storage(4096);
        let target = storage.config.data_dir.join("legacy-target.jsonl");
        let link = storage.config.data_dir.join("legacy-link.jsonl");
        fs::write(&target, b"{}\n").expect("write legacy target");
        symlink(&target, &link).expect("create legacy symlink");

        let error = storage
            .read_legacy_messages(&[link])
            .expect_err("legacy symlink must be rejected");

        assert!(matches!(error, StorageError::InvalidLegacySegment { .. }));
    }

    #[test]
    fn legacy_migration_rejects_oversized_input_before_reading() {
        let storage = test_storage(4096);
        let path = storage.config.data_dir.join("oversized.jsonl");
        let file = File::create(&path).expect("create sparse legacy segment");
        file.set_len(MAX_LEGACY_MIGRATION_BYTES + 1)
            .expect("size sparse legacy segment");

        let error = storage
            .read_legacy_messages(&[path])
            .expect_err("oversized migration must be rejected");

        assert!(matches!(
            error,
            StorageError::LegacyMigrationTooLarge { .. }
        ));
    }

    #[test]
    fn retention_reclaims_old_segments() {
        let storage = test_storage(180);
        storage
            .ensure_topic(TopicSpec {
                name: "ephemeral".to_owned(),
                retention_class: RetentionClass::Ephemeral,
                default_classification: Classification::Internal,
            })
            .expect("create topic");

        for index in 0..4 {
            storage
                .append(
                    "ephemeral",
                    "local:developer",
                    None,
                    format!("message-{index}-{}", "x".repeat(48)),
                )
                .expect("append message");
        }

        let stats = storage.stats().expect("storage stats");
        assert!(stats.maintenance.reclaimed_segments >= 1);
        assert!(stats.segment_count >= 1);
    }

    #[test]
    fn disk_pressure_rejects_when_no_more_segments_can_be_reclaimed() {
        let data_dir = std::env::temp_dir().join(format!("expressways-storage-{}", Uuid::now_v7()));
        let storage = Storage::new(StorageConfig {
            data_dir,
            segment_max_bytes: 4096,
            default_retention_class: RetentionClass::Operational,
            default_classification: Classification::Internal,
            retention_policy: RetentionPolicy {
                ephemeral_max_bytes: 4096,
                operational_max_bytes: 4096,
                regulated_max_bytes: 4096,
            },
            disk_pressure: DiskPressurePolicy {
                max_total_bytes: 220,
                reclaim_target_bytes: 180,
            },
        })
        .expect("create storage");

        let error = storage
            .append("tasks", "local:developer", None, "x".repeat(512))
            .expect_err("append should fail under disk pressure");

        assert!(matches!(error, StorageError::DiskPressure { .. }));
        assert_eq!(
            storage.next_offset("tasks").expect("next offset"),
            0,
            "failed appends must not advance cached topic state"
        );
    }

    #[test]
    fn concurrent_topics_cannot_overcommit_global_disk_budget() {
        let data_dir = std::env::temp_dir().join(format!("expressways-storage-{}", Uuid::now_v7()));
        let max_total_bytes = 2_000;
        let storage = Arc::new(
            Storage::new(StorageConfig {
                data_dir,
                segment_max_bytes: 4096,
                default_retention_class: RetentionClass::Operational,
                default_classification: Classification::Internal,
                retention_policy: RetentionPolicy {
                    ephemeral_max_bytes: 64 * 1_048_576,
                    operational_max_bytes: 64 * 1_048_576,
                    regulated_max_bytes: 64 * 1_048_576,
                },
                disk_pressure: DiskPressurePolicy {
                    max_total_bytes,
                    reclaim_target_bytes: 1_500,
                },
            })
            .expect("create storage"),
        );
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let handles = (0..16)
            .map(|index| {
                let storage = Arc::clone(&storage);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    storage.append(
                        &format!("topic-{index}"),
                        "local:developer",
                        None,
                        "x".repeat(256),
                    )
                })
            })
            .collect::<Vec<_>>();

        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("join append worker"))
            .collect::<Vec<_>>();
        assert!(results.iter().any(Result::is_ok));
        assert!(
            results
                .iter()
                .any(|result| matches!(result, Err(StorageError::DiskPressure { .. })))
        );

        let stats = storage.stats().expect("storage stats");
        assert!(stats.total_bytes <= max_total_bytes);
        assert_eq!(
            *storage
                .total_bytes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            stats.total_bytes
        );
    }

    #[test]
    fn recovers_truncated_segment_by_rebuilding_index() {
        let storage = test_storage(4096);
        let config = storage.config.clone();
        storage
            .append("tasks", "local:developer", None, "hello".to_owned())
            .expect("append first");
        storage
            .append("tasks", "local:developer", None, "world".to_owned())
            .expect("append second");

        let segment_path = storage.segment_path("tasks", 0);
        let segment_len = fs::metadata(&segment_path).expect("segment metadata").len();
        fs::write(storage.index_path("tasks", 0), b"corrupt").expect("corrupt index");
        OpenOptions::new()
            .write(true)
            .open(&segment_path)
            .expect("open segment")
            .set_len(segment_len - 3)
            .expect("truncate segment");
        drop(storage);
        let storage = Storage::new(config).expect("reopen storage");

        let messages = storage.read_from("tasks", 0, 10).expect("recover and read");
        let stats = storage.stats().expect("storage stats");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, "hello");
        assert!(stats.maintenance.recovered_segments >= 1);
        assert!(stats.maintenance.truncated_bytes >= 1);
    }

    #[test]
    fn recovery_streams_large_corrupt_index_instead_of_loading_it() {
        let storage = test_storage(4096);
        let config = storage.config.clone();
        storage
            .append("tasks", "local:developer", None, "hello".to_owned())
            .expect("append message");

        let index_path = storage.index_path("tasks", 0);
        OpenOptions::new()
            .write(true)
            .open(&index_path)
            .expect("open index")
            .set_len(512 * 1024 * 1024)
            .expect("make sparse corrupt index");
        drop(storage);
        let storage = Storage::new(config).expect("reopen storage");

        assert_eq!(storage.next_offset("tasks").expect("recover topic"), 1);
        assert_eq!(fs::metadata(index_path).expect("index metadata").len(), 8);
    }

    #[test]
    fn recovery_truncates_oversized_frame_without_allocating_payload() {
        let storage = test_storage(4096);
        let config = storage.config.clone();
        storage
            .ensure_topic(TopicSpec {
                name: "tasks".to_owned(),
                retention_class: RetentionClass::Operational,
                default_classification: Classification::Internal,
            })
            .expect("create topic");
        let segment_path = storage.segment_path("tasks", 0);
        let mut segment = File::create(&segment_path).expect("create segment");
        segment
            .write_all(&((MAX_STORED_FRAME_BYTES + 1) as u32).to_le_bytes())
            .expect("write oversized frame header");
        segment
            .set_len(FRAME_LEN_BYTES + MAX_STORED_FRAME_BYTES + 1)
            .expect("make sparse oversized frame");
        drop(segment);
        drop(storage);
        let storage = Storage::new(config).expect("reopen storage");

        assert_eq!(storage.next_offset("tasks").expect("recover topic"), 0);
        assert_eq!(
            fs::metadata(segment_path).expect("segment metadata").len(),
            0
        );
    }

    #[test]
    fn oversized_topic_state_is_rejected_before_reading() {
        let storage = test_storage(4096);
        let config = storage.config.clone();
        storage
            .ensure_topic(TopicSpec {
                name: "tasks".to_owned(),
                retention_class: RetentionClass::Operational,
                default_classification: Classification::Internal,
            })
            .expect("create topic");
        let state_path = storage.topic_dir("tasks").join("state.json");
        OpenOptions::new()
            .write(true)
            .open(&state_path)
            .expect("open state")
            .set_len(MAX_TOPIC_STATE_BYTES + 1)
            .expect("make sparse oversized state");
        drop(storage);
        let storage = Storage::new(config).expect("reopen storage");

        let error = storage
            .next_offset("tasks")
            .expect_err("oversized topic state must fail");
        assert!(matches!(error, StorageError::StateTooLarge { .. }));
    }

    #[test]
    fn corrupt_segment_offsets_cannot_wrap_topic_offset_space() {
        let storage = test_storage(4096);
        storage
            .append("tasks", "local:developer", None, "hello".to_owned())
            .expect("append message");

        fs::rename(
            storage.segment_path("tasks", 0),
            storage.segment_path("tasks", u64::MAX),
        )
        .expect("rename segment");
        fs::rename(
            storage.index_path("tasks", 0),
            storage.index_path("tasks", u64::MAX),
        )
        .expect("rename index");

        let error = storage
            .read_from("tasks", 0, 10)
            .expect_err("corrupt offset range must fail");
        assert!(matches!(
            error,
            StorageError::OffsetExhausted { ref topic } if topic == "tasks"
        ));
    }

    #[test]
    fn topic_state_rejects_newer_schema_versions() {
        let storage = test_storage(4096);
        let config = storage.config.clone();
        let topic = TopicSpec {
            name: "tasks".to_owned(),
            retention_class: RetentionClass::Operational,
            default_classification: Classification::Internal,
        };
        storage.ensure_topic(topic).expect("create topic");

        let state_path = storage.topic_dir("tasks").join("state.json");
        fs::write(
            &state_path,
            serde_json::json!({
                "schema_version": 99,
                "spec": {
                    "name": "tasks",
                    "retention_class": "operational",
                    "default_classification": "internal"
                },
                "next_offset": 0,
                "active_segment_base": 0
            })
            .to_string(),
        )
        .expect("write unsupported state");
        drop(storage);
        let storage = Storage::new(config).expect("reopen storage");

        let error = storage
            .next_offset("tasks")
            .expect_err("unsupported schema should fail");
        assert!(matches!(
            error,
            StorageError::UnsupportedStateVersion { found: 99, .. }
        ));
    }
}
