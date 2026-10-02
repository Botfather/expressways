use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use expressways_protocol::{ArtifactMetadata, Classification, RetentionClass};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ARTIFACT_METADATA_BYTES: u64 = 64 * 1024;
const MAX_ARTIFACT_ID_BYTES: usize = 128;
const MAX_CONTENT_TYPE_BYTES: usize = 256;
const MAX_PRINCIPAL_BYTES: usize = 256;

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
    put_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone)]
pub struct PutArtifactRequest {
    pub artifact_id: Option<String>,
    pub content_type: String,
    pub data: Vec<u8>,
    pub sha256: Option<String>,
    pub classification: Classification,
    pub retention_class: RetentionClass,
    pub principal: String,
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact id `{0}` is invalid")]
    InvalidArtifactId(String),
    #[error("artifact content type must not be empty")]
    EmptyContentType,
    #[error("artifact {kind} is {bytes} bytes; maximum is {max_bytes} bytes")]
    TooLarge {
        kind: &'static str,
        bytes: u64,
        max_bytes: u64,
    },
    #[error("artifact `{artifact_id}` length mismatch: expected {expected}, got {actual}")]
    LengthMismatch {
        artifact_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("artifact store path is not a contained regular file: {0}")]
    UnsafeFile(PathBuf),
    #[error("artifact metadata is invalid: {0}")]
    InvalidMetadata(String),
    #[error("artifact `{0}` already exists")]
    AlreadyExists(String),
    #[error("artifact `{0}` was not found")]
    Missing(String),
    #[error("artifact metadata id mismatch: requested `{requested}`, found `{found}`")]
    MetadataIdMismatch { requested: String, found: String },
    #[error("artifact `{artifact_id}` sha256 mismatch: expected {expected}, got {actual}")]
    Sha256Mismatch {
        artifact_id: String,
        expected: String,
        actual: String,
    },
    #[error("failed to persist artifact store data: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize artifact metadata: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl ArtifactStore {
    pub fn new(root: PathBuf) -> Result<Self, ArtifactError> {
        create_private_dir(&root)?;
        create_private_dir(&root.join("blobs"))?;
        create_private_dir(&root.join("metadata"))?;
        Ok(Self {
            root,
            put_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn put(&self, request: PutArtifactRequest) -> Result<ArtifactMetadata, ArtifactError> {
        if request.content_type.trim().is_empty() {
            return Err(ArtifactError::EmptyContentType);
        }
        validate_text_length(
            "content_type",
            &request.content_type,
            MAX_CONTENT_TYPE_BYTES,
        )?;
        validate_text_length("principal", &request.principal, MAX_PRINCIPAL_BYTES)?;
        let byte_length = u64::try_from(request.data.len()).unwrap_or(u64::MAX);
        if byte_length > MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::TooLarge {
                kind: "blob",
                bytes: byte_length,
                max_bytes: MAX_ARTIFACT_BYTES,
            });
        }

        let artifact_id = request
            .artifact_id
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        validate_artifact_id(&artifact_id)?;

        let blob_path = self.blob_path(&artifact_id);
        let metadata_path = self.metadata_path(&artifact_id);
        let actual_sha256 = sha256_hex(&request.data);
        if let Some(expected_sha256) = request.sha256.as_deref()
            && !expected_sha256.eq_ignore_ascii_case(&actual_sha256)
        {
            return Err(ArtifactError::Sha256Mismatch {
                artifact_id,
                expected: expected_sha256.to_owned(),
                actual: actual_sha256,
            });
        }

        // Serialize the existence check and two-file commit within this store.
        // A retried named upload is therefore deterministic even when requests
        // arrive concurrently through cloned handles.
        let _put_guard = self.put_lock.lock().map_err(|_| {
            ArtifactError::Io(std::io::Error::other("artifact put lock is poisoned"))
        })?;
        if blob_path.exists() || metadata_path.exists() {
            if blob_path.exists() && metadata_path.exists() {
                let (existing, _) = self.get(&artifact_id)?;
                if existing.byte_length == byte_length
                    && existing.sha256.eq_ignore_ascii_case(&actual_sha256)
                {
                    return Ok(existing);
                }
                return Err(ArtifactError::AlreadyExists(artifact_id));
            }
            return Err(ArtifactError::InvalidMetadata(format!(
                "artifact `{artifact_id}` has an incomplete blob/metadata pair"
            )));
        }

        if let Some(parent) = blob_path.parent() {
            create_private_dir(parent)?;
        }
        if let Some(parent) = metadata_path.parent() {
            create_private_dir(parent)?;
        }

        let metadata = ArtifactMetadata {
            artifact_id: artifact_id.clone(),
            content_type: request.content_type,
            byte_length,
            sha256: actual_sha256,
            classification: request.classification,
            retention_class: request.retention_class,
            created_at: Utc::now(),
            principal: request.principal,
            // Broker filesystem paths are never part of the remote artifact contract.
            local_path: None,
        };
        let serialized_metadata = serde_json::to_vec_pretty(&metadata)?;

        write_new(&blob_path, &request.data).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                ArtifactError::AlreadyExists(artifact_id.clone())
            } else {
                ArtifactError::Io(error)
            }
        })?;
        if let Err(error) = write_new(&metadata_path, &serialized_metadata) {
            let _ = fs::remove_file(&blob_path);
            return Err(if error.kind() == std::io::ErrorKind::AlreadyExists {
                ArtifactError::AlreadyExists(artifact_id)
            } else {
                ArtifactError::Io(error)
            });
        }
        Ok(metadata)
    }

    pub fn stat(&self, artifact_id: &str) -> Result<ArtifactMetadata, ArtifactError> {
        validate_artifact_id(artifact_id)?;
        let metadata_path = self.metadata_path(artifact_id);
        if !metadata_path.exists() {
            return Err(ArtifactError::Missing(artifact_id.to_owned()));
        }

        let metadata_root = self.root.join("metadata");
        let mut metadata: ArtifactMetadata = serde_json::from_slice(&read_bounded_contained(
            &metadata_path,
            &metadata_root,
            MAX_ARTIFACT_METADATA_BYTES,
            "metadata",
        )?)?;
        if metadata.artifact_id != artifact_id {
            return Err(ArtifactError::MetadataIdMismatch {
                requested: artifact_id.to_owned(),
                found: metadata.artifact_id,
            });
        }
        validate_metadata(&metadata)?;
        // Sanitize legacy metadata that persisted a broker-local path.
        metadata.local_path = None;
        Ok(metadata)
    }

    pub fn get(&self, artifact_id: &str) -> Result<(ArtifactMetadata, Vec<u8>), ArtifactError> {
        let metadata = self.stat(artifact_id)?;
        // The persisted local_path is informational only. Never trust mutable
        // metadata to select an arbitrary filesystem path for broker reads.
        let blob_path = self.blob_path(artifact_id);
        if !blob_path.exists() {
            return Err(ArtifactError::Missing(metadata.artifact_id));
        }

        let blob_root = self.root.join("blobs");
        let bytes = read_bounded_contained(&blob_path, &blob_root, metadata.byte_length, "blob")?;
        let actual_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if actual_length != metadata.byte_length {
            return Err(ArtifactError::LengthMismatch {
                artifact_id: metadata.artifact_id.clone(),
                expected: metadata.byte_length,
                actual: actual_length,
            });
        }
        let actual_sha256 = sha256_hex(&bytes);
        if metadata.sha256 != actual_sha256 {
            return Err(ArtifactError::Sha256Mismatch {
                artifact_id: metadata.artifact_id.clone(),
                expected: metadata.sha256.clone(),
                actual: actual_sha256,
            });
        }

        Ok((metadata, bytes))
    }

    fn blob_path(&self, artifact_id: &str) -> PathBuf {
        self.root.join("blobs").join(format!("{artifact_id}.blob"))
    }

    fn metadata_path(&self, artifact_id: &str) -> PathBuf {
        self.root
            .join("metadata")
            .join(format!("{artifact_id}.json"))
    }
}

fn write_new(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn create_private_dir(path: &std::path::Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn validate_artifact_id(artifact_id: &str) -> Result<(), ArtifactError> {
    if artifact_id.is_empty()
        || artifact_id.len() > MAX_ARTIFACT_ID_BYTES
        || !artifact_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(ArtifactError::InvalidArtifactId(artifact_id.to_owned()));
    }

    Ok(())
}

fn validate_text_length(field: &str, value: &str, max_bytes: usize) -> Result<(), ArtifactError> {
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(ArtifactError::InvalidMetadata(format!(
            "{field} must contain at most {max_bytes} bytes and no control characters; got {} bytes",
            value.len()
        )));
    }
    Ok(())
}

fn validate_metadata(metadata: &ArtifactMetadata) -> Result<(), ArtifactError> {
    if metadata.byte_length > MAX_ARTIFACT_BYTES {
        return Err(ArtifactError::TooLarge {
            kind: "blob",
            bytes: metadata.byte_length,
            max_bytes: MAX_ARTIFACT_BYTES,
        });
    }
    validate_text_length(
        "content_type",
        &metadata.content_type,
        MAX_CONTENT_TYPE_BYTES,
    )?;
    validate_text_length("principal", &metadata.principal, MAX_PRINCIPAL_BYTES)?;
    if metadata.content_type.trim().is_empty() || metadata.principal.trim().is_empty() {
        return Err(ArtifactError::InvalidMetadata(
            "content_type and principal must not be empty".to_owned(),
        ));
    }
    if metadata.sha256.len() != 64 || !metadata.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ArtifactError::InvalidMetadata(
            "sha256 must contain exactly 64 hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn read_bounded_contained(
    path: &Path,
    root: &Path,
    max_bytes: u64,
    kind: &'static str,
) -> Result<Vec<u8>, ArtifactError> {
    let canonical_root = fs::canonicalize(root)?;
    let canonical_path = fs::canonicalize(path)?;
    if !canonical_path.starts_with(&canonical_root) || !canonical_path.is_file() {
        return Err(ArtifactError::UnsafeFile(path.to_path_buf()));
    }

    let file = File::open(&canonical_path)?;
    let declared_size = file.metadata()?.len();
    if declared_size > max_bytes {
        return Err(ArtifactError::TooLarge {
            kind,
            bytes: declared_size,
            max_bytes,
        });
    }
    let capacity = usize::try_from(declared_size).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_size > max_bytes {
        return Err(ArtifactError::TooLarge {
            kind,
            bytes: actual_size,
            max_bytes,
        });
    }
    Ok(bytes)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("expressways-artifacts-{name}-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create temp root");
        root
    }

    #[test]
    fn stores_and_reads_artifacts_with_metadata() {
        let root = temp_root("roundtrip");
        let store = ArtifactStore::new(root.clone()).expect("create store");

        let metadata = store
            .put(PutArtifactRequest {
                artifact_id: Some("blob-1".to_owned()),
                content_type: "application/pdf".to_owned(),
                data: b"PDF".to_vec(),
                sha256: None,
                classification: Classification::Restricted,
                retention_class: RetentionClass::Regulated,
                principal: "local:developer".to_owned(),
            })
            .expect("store artifact");

        assert_eq!(metadata.artifact_id, "blob-1");
        assert_eq!(metadata.byte_length, 3);
        assert_eq!(metadata.classification, Classification::Restricted);
        assert!(metadata.local_path.is_none());

        let (stored, bytes) = store.get("blob-1").expect("read artifact");
        assert!(stored.local_path.is_none());
        assert_eq!(stored.sha256, metadata.sha256);
        assert_eq!(bytes, b"PDF");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&root)
                    .expect("root metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(store.blob_path("blob-1"))
                    .expect("blob metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(store.metadata_path("blob-1"))
                    .expect("artifact metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn identical_named_uploads_are_idempotent_but_conflicts_fail() {
        let root = temp_root("idempotent-put");
        let store = ArtifactStore::new(root).expect("create store");
        let request = PutArtifactRequest {
            artifact_id: Some("content-addressed-blob".to_owned()),
            content_type: "application/octet-stream".to_owned(),
            data: b"same bytes".to_vec(),
            sha256: None,
            classification: Classification::Internal,
            retention_class: RetentionClass::Operational,
            principal: "local:adapter".to_owned(),
        };

        let first = store.put(request.clone()).expect("initial upload");
        let retried = store.put(request.clone()).expect("idempotent retry");
        assert_eq!(retried, first);

        let mut conflict = request;
        conflict.data = b"different bytes".to_vec();
        assert!(matches!(
            store.put(conflict),
            Err(ArtifactError::AlreadyExists(id)) if id == "content-addressed-blob"
        ));
    }

    #[test]
    fn rejects_sha256_mismatches() {
        let root = temp_root("sha");
        let store = ArtifactStore::new(root).expect("create store");

        let error = store
            .put(PutArtifactRequest {
                artifact_id: Some("blob-2".to_owned()),
                content_type: "application/octet-stream".to_owned(),
                data: b"abc".to_vec(),
                sha256: Some("deadbeef".to_owned()),
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                principal: "local:developer".to_owned(),
            })
            .expect_err("mismatch should fail");

        assert!(matches!(error, ArtifactError::Sha256Mismatch { .. }));
    }

    #[test]
    fn get_ignores_tampered_local_path() {
        let root = temp_root("tampered-path");
        let store = ArtifactStore::new(root.clone()).expect("create store");
        store
            .put(PutArtifactRequest {
                artifact_id: Some("blob-safe".to_owned()),
                content_type: "application/octet-stream".to_owned(),
                data: b"safe".to_vec(),
                sha256: None,
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                principal: "local:developer".to_owned(),
            })
            .expect("store artifact");

        let outside = root.join("outside-secret");
        fs::write(&outside, b"secret").expect("write outside file");
        let metadata_path = store.metadata_path("blob-safe");
        let mut metadata: ArtifactMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read metadata"))
                .expect("parse metadata");
        metadata.local_path = Some(outside.display().to_string());
        fs::write(
            metadata_path,
            serde_json::to_vec_pretty(&metadata).expect("serialize metadata"),
        )
        .expect("tamper metadata");

        let (_, bytes) = store.get("blob-safe").expect("read stored artifact");
        assert_eq!(bytes, b"safe");
    }

    #[test]
    fn stat_rejects_tampered_metadata_id() {
        let root = temp_root("tampered-id");
        let store = ArtifactStore::new(root).expect("create store");
        let metadata = store
            .put(PutArtifactRequest {
                artifact_id: Some("blob-original".to_owned()),
                content_type: "application/octet-stream".to_owned(),
                data: b"data".to_vec(),
                sha256: None,
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                principal: "local:developer".to_owned(),
            })
            .expect("store artifact");
        let mut tampered = metadata;
        tampered.artifact_id = "blob-other".to_owned();
        fs::write(
            store.metadata_path("blob-original"),
            serde_json::to_vec_pretty(&tampered).expect("serialize metadata"),
        )
        .expect("tamper metadata");

        assert!(matches!(
            store.stat("blob-original"),
            Err(ArtifactError::MetadataIdMismatch { .. })
        ));
    }

    #[test]
    fn oversized_or_truncated_artifact_files_fail_before_unbounded_reads() {
        let root = temp_root("bounded-read");
        let store = ArtifactStore::new(root).expect("create store");
        store
            .put(PutArtifactRequest {
                artifact_id: Some("blob-bounded".to_owned()),
                content_type: "application/octet-stream".to_owned(),
                data: b"data".to_vec(),
                sha256: None,
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                principal: "local:developer".to_owned(),
            })
            .expect("store artifact");

        OpenOptions::new()
            .write(true)
            .open(store.blob_path("blob-bounded"))
            .expect("open blob")
            .set_len(MAX_ARTIFACT_BYTES + 1)
            .expect("extend sparse blob");
        assert!(matches!(
            store.get("blob-bounded"),
            Err(ArtifactError::TooLarge { kind: "blob", .. })
        ));

        OpenOptions::new()
            .write(true)
            .open(store.metadata_path("blob-bounded"))
            .expect("open metadata")
            .set_len(MAX_ARTIFACT_METADATA_BYTES + 1)
            .expect("extend sparse metadata");
        assert!(matches!(
            store.stat("blob-bounded"),
            Err(ArtifactError::TooLarge {
                kind: "metadata",
                ..
            })
        ));
    }

    #[test]
    fn blob_length_mismatches_fail_before_hash_validation() {
        let root = temp_root("length-mismatch");
        let store = ArtifactStore::new(root).expect("create store");
        store
            .put(PutArtifactRequest {
                artifact_id: Some("blob-short".to_owned()),
                content_type: "application/octet-stream".to_owned(),
                data: b"data".to_vec(),
                sha256: None,
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                principal: "local:developer".to_owned(),
            })
            .expect("store artifact");
        OpenOptions::new()
            .write(true)
            .open(store.blob_path("blob-short"))
            .expect("open blob")
            .set_len(2)
            .expect("truncate blob");

        assert!(matches!(
            store.get("blob-short"),
            Err(ArtifactError::LengthMismatch {
                expected: 4,
                actual: 2,
                ..
            })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn blob_symlinks_cannot_escape_the_artifact_blob_directory() {
        use std::os::unix::fs::symlink;

        let root = temp_root("symlink");
        let store = ArtifactStore::new(root.clone()).expect("create store");
        store
            .put(PutArtifactRequest {
                artifact_id: Some("blob-link".to_owned()),
                content_type: "application/octet-stream".to_owned(),
                data: b"safe".to_vec(),
                sha256: None,
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                principal: "local:developer".to_owned(),
            })
            .expect("store artifact");
        let outside = root.join("outside-secret");
        fs::write(&outside, b"safe").expect("write outside file");
        fs::remove_file(store.blob_path("blob-link")).expect("remove blob");
        symlink(&outside, store.blob_path("blob-link")).expect("link blob");

        assert!(matches!(
            store.get("blob-link"),
            Err(ArtifactError::UnsafeFile(_))
        ));
    }
}
