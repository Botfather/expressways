use std::fs;
use std::path::PathBuf;

use chrono::Utc;
use expressways_protocol::{ArtifactMetadata, Classification, RetentionClass};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
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
    #[error("artifact `{0}` already exists")]
    AlreadyExists(String),
    #[error("artifact `{0}` was not found")]
    Missing(String),
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
        fs::create_dir_all(root.join("blobs"))?;
        fs::create_dir_all(root.join("metadata"))?;
        Ok(Self { root })
    }

    pub fn put(&self, request: PutArtifactRequest) -> Result<ArtifactMetadata, ArtifactError> {
        if request.content_type.trim().is_empty() {
            return Err(ArtifactError::EmptyContentType);
        }

        let artifact_id = request
            .artifact_id
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        validate_artifact_id(&artifact_id)?;

        let blob_path = self.blob_path(&artifact_id);
        let metadata_path = self.metadata_path(&artifact_id);
        if blob_path.exists() || metadata_path.exists() {
            return Err(ArtifactError::AlreadyExists(artifact_id));
        }

        let actual_sha256 = sha256_hex(&request.data);
        if let Some(expected_sha256) = request.sha256.as_deref() {
            if !expected_sha256.eq_ignore_ascii_case(&actual_sha256) {
                return Err(ArtifactError::Sha256Mismatch {
                    artifact_id,
                    expected: expected_sha256.to_owned(),
                    actual: actual_sha256,
                });
            }
        }

        if let Some(parent) = blob_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = metadata_path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(&blob_path, &request.data)?;
        let metadata = ArtifactMetadata {
            artifact_id: artifact_id.clone(),
            content_type: request.content_type,
            byte_length: request.data.len() as u64,
            sha256: actual_sha256,
            classification: request.classification,
            retention_class: request.retention_class,
            created_at: Utc::now(),
            principal: request.principal,
            local_path: Some(blob_path.display().to_string()),
        };
        fs::write(&metadata_path, serde_json::to_vec_pretty(&metadata)?)?;
        Ok(metadata)
    }

    pub fn stat(&self, artifact_id: &str) -> Result<ArtifactMetadata, ArtifactError> {
        validate_artifact_id(artifact_id)?;
        let metadata_path = self.metadata_path(artifact_id);
        if !metadata_path.exists() {
            return Err(ArtifactError::Missing(artifact_id.to_owned()));
        }

        Ok(serde_json::from_slice(&fs::read(metadata_path)?)?)
    }

    pub fn get(&self, artifact_id: &str) -> Result<(ArtifactMetadata, Vec<u8>), ArtifactError> {
        let metadata = self.stat(artifact_id)?;
        let blob_path = metadata
            .local_path
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.blob_path(&metadata.artifact_id));
        if !blob_path.exists() {
            return Err(ArtifactError::Missing(metadata.artifact_id));
        }

        let bytes = fs::read(&blob_path)?;
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

fn validate_artifact_id(artifact_id: &str) -> Result<(), ArtifactError> {
    if artifact_id.is_empty()
        || !artifact_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(ArtifactError::InvalidArtifactId(artifact_id.to_owned()));
    }

    Ok(())
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
        let store = ArtifactStore::new(root).expect("create store");

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

        let (stored, bytes) = store.get("blob-1").expect("read artifact");
        assert_eq!(stored.sha256, metadata.sha256);
        assert_eq!(bytes, b"PDF");
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
}
