use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail, ensure};
use expressways_client::Client;
use expressways_protocol::{
    ArtifactMetadata, Classification, ControlCommand, ControlRequest, ControlResponse,
    RetentionClass,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const ADAPTER_CURSOR_SCHEMA_VERSION: u32 = 1;
pub const MAX_CURSOR_FILE_BYTES: u64 = 64 * 1024;
pub const MAX_CURSOR_STREAMS: usize = 256;
pub const MAX_CURSOR_NAME_BYTES: usize = 256;
pub const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CursorDocument {
    schema_version: u32,
    #[serde(default)]
    cursors: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub struct CursorStore {
    path: PathBuf,
    document: CursorDocument,
}

impl CursorStore {
    pub fn open(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        Self::open_with_legacy_field(path, None)
    }

    /// Loads the current cursor document. When `legacy` is supplied, a v1 JSON
    /// object containing the named numeric field is migrated into `stream`.
    pub fn open_with_legacy_field(
        path: impl Into<PathBuf>,
        legacy: Option<(&str, &str)>,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        if !path.exists() {
            return Ok(Self {
                path,
                document: CursorDocument {
                    schema_version: ADAPTER_CURSOR_SCHEMA_VERSION,
                    cursors: BTreeMap::new(),
                },
            });
        }

        let bytes = read_bounded_private_file(&path)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse cursor state {}", path.display()))?;
        let mut migrated = false;
        let document = if value.get("cursors").is_some() {
            serde_json::from_value::<CursorDocument>(value.clone())
                .with_context(|| format!("invalid cursor state document {}", path.display()))?
        } else {
            let Some((legacy_field, stream)) = legacy else {
                bail!("cursor state {} does not contain cursors", path.display());
            };
            let schema_version = value
                .get("schema_version")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            let offset = value
                    .get(legacy_field)
                    .and_then(serde_json::Value::as_u64)
                    .with_context(|| {
                        format!(
                            "cursor state {} has neither current cursors nor legacy field {legacy_field}",
                            path.display()
                        )
                    })?;
            ensure!(
                schema_version == u64::from(ADAPTER_CURSOR_SCHEMA_VERSION),
                "unsupported cursor state schema version {schema_version}"
            );
            validate_cursor_name(stream)?;
            migrated = true;
            CursorDocument {
                schema_version: ADAPTER_CURSOR_SCHEMA_VERSION,
                cursors: BTreeMap::from([(stream.to_owned(), offset)]),
            }
        };
        validate_document(&document)?;
        let store = Self { path, document };
        if migrated {
            store.persist()?;
        }
        Ok(store)
    }

    pub fn next_offset(&self, stream: &str) -> anyhow::Result<u64> {
        validate_cursor_name(stream)?;
        Ok(self.document.cursors.get(stream).copied().unwrap_or(0))
    }

    /// Persists the next offset only after external delivery succeeds.
    /// Regressions are rejected so replays cannot silently move a durable
    /// adapter cursor backwards.
    pub fn checkpoint(&mut self, stream: &str, next_offset: u64) -> anyhow::Result<()> {
        validate_cursor_name(stream)?;
        let current = self.next_offset(stream)?;
        ensure!(
            next_offset >= current,
            "cursor regression for {stream}: {next_offset} is behind {current}"
        );
        if !self.document.cursors.contains_key(stream) {
            ensure!(
                self.document.cursors.len() < MAX_CURSOR_STREAMS,
                "cursor document exceeds {MAX_CURSOR_STREAMS} streams"
            );
        }
        let previous = self.document.cursors.insert(stream.to_owned(), next_offset);
        if let Err(error) = self.persist() {
            match previous {
                Some(previous) => {
                    self.document.cursors.insert(stream.to_owned(), previous);
                }
                None => {
                    self.document.cursors.remove(stream);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    fn persist(&self) -> anyhow::Result<()> {
        validate_document(&self.document)?;
        let rendered = serde_json::to_vec_pretty(&self.document)?;
        ensure!(
            rendered.len() as u64 <= MAX_CURSOR_FILE_BYTES,
            "serialized cursor state exceeds {MAX_CURSOR_FILE_BYTES} bytes"
        );
        atomic_private_replace(&self.path, &rendered)
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactUpload {
    pub artifact_id: Option<String>,
    pub content_type: String,
    pub bytes: Vec<u8>,
    pub classification: Classification,
    pub retention_class: RetentionClass,
}

pub fn stable_id(prefix: &str, key: &str) -> anyhow::Result<String> {
    ensure!(!prefix.is_empty(), "stable id prefix must not be empty");
    ensure!(!key.is_empty(), "stable id key must not be empty");
    ensure!(prefix.len() <= 64, "stable id prefix exceeds 64 bytes");
    ensure!(
        prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "stable id prefix contains unsupported characters"
    );
    ensure!(key.len() <= 1024 * 1024, "stable id key exceeds 1 MiB");
    let digest = sha256_hex(key.as_bytes());
    Ok(format!("{prefix}-{}", &digest[..32]))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn verify_artifact_claims(
    bytes: &[u8],
    expected_length: Option<u64>,
    expected_sha256: Option<&str>,
) -> anyhow::Result<String> {
    ensure!(
        bytes.len() <= MAX_ARTIFACT_BYTES,
        "artifact exceeds {MAX_ARTIFACT_BYTES} bytes"
    );
    if let Some(expected_length) = expected_length {
        ensure!(
            expected_length == bytes.len() as u64,
            "artifact length mismatch: expected {expected_length}, received {}",
            bytes.len()
        );
    }
    let actual = sha256_hex(bytes);
    if let Some(expected) = expected_sha256 {
        ensure!(
            expected.len() == 64 && expected.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "artifact SHA-256 claim must contain 64 hexadecimal characters"
        );
        ensure!(
            expected.eq_ignore_ascii_case(&actual),
            "artifact SHA-256 mismatch"
        );
    }
    Ok(actual)
}

pub async fn put_artifact(
    client: &mut Client,
    capability_token: &str,
    upload: ArtifactUpload,
) -> anyhow::Result<ArtifactMetadata> {
    ensure!(
        !capability_token.is_empty(),
        "capability token must not be empty"
    );
    ensure!(!upload.bytes.is_empty(), "artifact must not be empty");
    ensure!(
        !upload.content_type.is_empty() && upload.content_type.len() <= 256,
        "artifact content type must contain 1..=256 bytes"
    );
    let expected_artifact_id = upload.artifact_id.clone();
    let expected_content_type = upload.content_type.clone();
    let expected_classification = upload.classification.clone();
    let expected_retention_class = upload.retention_class.clone();
    let byte_length = upload.bytes.len() as u64;
    let sha256 = verify_artifact_claims(&upload.bytes, None, None)?;
    let (response, attachment) = client
        .send_with_attachment(
            ControlRequest {
                capability_token: capability_token.to_owned(),
                command: ControlCommand::PutArtifact {
                    artifact_id: upload.artifact_id,
                    content_type: upload.content_type,
                    byte_length,
                    sha256: Some(sha256.clone()),
                    classification: Some(upload.classification),
                    retention_class: Some(upload.retention_class),
                },
            },
            Some(upload.bytes),
        )
        .await
        .context("send artifact to broker")?;
    ensure!(
        attachment.is_none(),
        "broker returned bytes for artifact upload"
    );
    match response {
        ControlResponse::ArtifactStored { artifact } => {
            if let Some(expected_artifact_id) = expected_artifact_id {
                ensure!(
                    artifact.artifact_id == expected_artifact_id,
                    "broker returned the wrong artifact id"
                );
            }
            ensure!(
                artifact.content_type == expected_content_type,
                "broker returned the wrong artifact content type"
            );
            ensure!(
                artifact.byte_length == byte_length,
                "broker returned the wrong artifact length"
            );
            ensure!(
                artifact.sha256.eq_ignore_ascii_case(&sha256),
                "broker returned the wrong artifact digest"
            );
            ensure!(
                artifact.classification == expected_classification,
                "broker returned the wrong artifact classification"
            );
            ensure!(
                artifact.retention_class == expected_retention_class,
                "broker returned the wrong artifact retention class"
            );
            Ok(artifact)
        }
        ControlResponse::Error { code, message } => {
            bail!("broker rejected artifact: {code}: {message}")
        }
        other => bail!("unexpected artifact upload response: {other:?}"),
    }
}

pub async fn get_artifact(
    client: &mut Client,
    capability_token: &str,
    artifact_id: &str,
) -> anyhow::Result<(ArtifactMetadata, Vec<u8>)> {
    ensure!(
        !capability_token.is_empty(),
        "capability token must not be empty"
    );
    ensure!(!artifact_id.is_empty(), "artifact id must not be empty");
    let (response, attachment) = client
        .send_with_attachment(
            ControlRequest {
                capability_token: capability_token.to_owned(),
                command: ControlCommand::GetArtifact {
                    artifact_id: artifact_id.to_owned(),
                },
            },
            None,
        )
        .await
        .context("get artifact from broker")?;
    let artifact = match response {
        ControlResponse::Artifact { artifact } => artifact,
        ControlResponse::Error { code, message } => {
            bail!("broker rejected artifact read: {code}: {message}")
        }
        other => bail!("unexpected artifact read response: {other:?}"),
    };
    ensure!(
        artifact.artifact_id == artifact_id,
        "broker returned the wrong artifact id"
    );
    let bytes = attachment.context("broker omitted artifact bytes")?;
    verify_artifact_claims(&bytes, Some(artifact.byte_length), Some(&artifact.sha256))?;
    Ok((artifact, bytes))
}

fn validate_cursor_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= MAX_CURSOR_NAME_BYTES,
        "cursor name must contain 1..={MAX_CURSOR_NAME_BYTES} bytes"
    );
    ensure!(
        !name.chars().any(char::is_control),
        "cursor name must not contain control characters"
    );
    Ok(())
}

fn validate_document(document: &CursorDocument) -> anyhow::Result<()> {
    ensure!(
        document.schema_version == ADAPTER_CURSOR_SCHEMA_VERSION,
        "unsupported cursor state schema version {}",
        document.schema_version
    );
    ensure!(
        document.cursors.len() <= MAX_CURSOR_STREAMS,
        "cursor document exceeds {MAX_CURSOR_STREAMS} streams"
    );
    for stream in document.cursors.keys() {
        validate_cursor_name(stream)?;
    }
    Ok(())
}

fn read_bounded_private_file(path: &Path) -> anyhow::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect cursor state {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "cursor state must be a regular non-symlink file"
    );
    ensure!(
        metadata.len() <= MAX_CURSOR_FILE_BYTES,
        "cursor state exceeds {MAX_CURSOR_FILE_BYTES} bytes"
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let opened = file.metadata()?;
    ensure!(opened.is_file(), "cursor state must remain a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = opened.permissions().mode() & 0o777;
        ensure!(
            mode & 0o022 == 0,
            "cursor state has unsafe writable permissions {mode:o}"
        );
    }
    let mut bytes = Vec::with_capacity(usize::try_from(opened.len()).unwrap_or_default());
    file.take(MAX_CURSOR_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_CURSOR_FILE_BYTES,
        "cursor state grew beyond {MAX_CURSOR_FILE_BYTES} bytes"
    );
    Ok(bytes)
}

fn atomic_private_replace(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .context("cursor state path has no file name")?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        Uuid::now_v7()
    ));
    let result = (|| -> std::io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(path)?;
        }
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.with_context(|| format!("save cursor state {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use chrono::Utc;
    use expressways_client::{CustomEndpoint, Endpoint};
    use expressways_protocol::ControlWireEnvelope;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    type ObservedRequest = Arc<Mutex<Option<(ControlRequest, Option<Vec<u8>>)>>>;

    fn fake_broker(
        response: ControlResponse,
        response_attachment: Option<Vec<u8>>,
    ) -> (Endpoint, ObservedRequest) {
        let observed = Arc::new(Mutex::new(None));
        let observed_for_connector = Arc::clone(&observed);
        let endpoint = Endpoint::Custom(CustomEndpoint::new("adapter-sdk-test", move || {
            let response = response.clone();
            let response_attachment = response_attachment.clone();
            let observed = Arc::clone(&observed_for_connector);
            async move {
                let (client, mut server) = tokio::io::duplex(1024 * 1024);
                tokio::spawn(async move {
                    let frame_length = server.read_u32().await.unwrap() as usize;
                    let mut frame = vec![0_u8; frame_length];
                    server.read_exact(&mut frame).await.unwrap();
                    let (envelope, attachment) =
                        ControlWireEnvelope::decode_packet(&frame).unwrap();
                    let request = match envelope {
                        ControlWireEnvelope::Request { request, .. } => request,
                        other => panic!("unexpected request: {other:?}"),
                    };
                    *observed.lock().unwrap() =
                        Some((request, (!attachment.is_empty()).then_some(attachment)));
                    let envelope = ControlWireEnvelope::Response {
                        response,
                        attachment_length: response_attachment
                            .as_ref()
                            .map_or(0, |bytes| bytes.len() as u64),
                    };
                    let packet = envelope
                        .encode_with_attachment(response_attachment.as_deref())
                        .unwrap();
                    server.write_u32(packet.len() as u32).await.unwrap();
                    server.write_all(&packet).await.unwrap();
                });
                Ok(Box::new(client) as expressways_client::BoxedClientIo)
            }
        }));
        (endpoint, observed)
    }

    fn artifact_metadata(bytes: &[u8]) -> ArtifactMetadata {
        ArtifactMetadata {
            artifact_id: "artifact-1".to_owned(),
            content_type: "image/png".to_owned(),
            byte_length: bytes.len() as u64,
            sha256: sha256_hex(bytes),
            classification: Classification::Internal,
            retention_class: RetentionClass::Operational,
            created_at: Utc::now(),
            principal: "adapter:test".to_owned(),
            local_path: None,
        }
    }

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "expressways-adapter-{label}-{}.json",
            Uuid::now_v7()
        ))
    }

    #[test]
    fn cursor_round_trip_is_monotonic_and_migrates_legacy_state() {
        let path = temp_path("cursor");
        fs::write(&path, br#"{"schema_version":1,"replies_offset":12}"#).unwrap();
        let mut store = CursorStore::open_with_legacy_field(
            &path,
            Some(("replies_offset", "interop.chat.replies")),
        )
        .unwrap();
        assert_eq!(store.next_offset("interop.chat.replies").unwrap(), 12);
        store.checkpoint("interop.chat.replies", 13).unwrap();
        assert!(store.checkpoint("interop.chat.replies", 11).is_err());
        let reopened = CursorStore::open(&path).unwrap();
        assert_eq!(reopened.next_offset("interop.chat.replies").unwrap(), 13);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn failed_checkpoint_does_not_advance_memory() {
        let parent_file = temp_path("not-a-directory");
        fs::write(&parent_file, b"occupied").unwrap();
        let path = parent_file.join("cursor.json");
        let mut store = CursorStore::open(&path).unwrap();
        assert!(store.checkpoint("replies", 9).is_err());
        assert_eq!(store.next_offset("replies").unwrap(), 0);
        fs::remove_file(parent_file).unwrap();
    }

    #[test]
    fn stable_ids_and_artifact_claims_are_deterministic() {
        assert_eq!(
            stable_id("task", "message-1").unwrap(),
            stable_id("task", "message-1").unwrap()
        );
        assert_ne!(
            stable_id("task", "message-1").unwrap(),
            stable_id("task", "message-2").unwrap()
        );
        let digest = verify_artifact_claims(b"hello", Some(5), None).unwrap();
        assert_eq!(
            digest,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert!(verify_artifact_claims(b"hello", Some(4), None).is_err());
        assert!(verify_artifact_claims(b"hello", Some(5), Some(&"0".repeat(64))).is_err());
    }

    #[tokio::test]
    async fn artifact_helpers_use_raw_bytes_and_verify_downloads() {
        let bytes = b"not-really-a-png";
        let metadata = artifact_metadata(bytes);
        let (endpoint, observed) = fake_broker(
            ControlResponse::ArtifactStored {
                artifact: metadata.clone(),
            },
            None,
        );
        let mut client = Client::connect(endpoint).await.unwrap();
        let stored = put_artifact(
            &mut client,
            "capability",
            ArtifactUpload {
                artifact_id: Some("artifact-1".to_owned()),
                content_type: "image/png".to_owned(),
                bytes: bytes.to_vec(),
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
            },
        )
        .await
        .unwrap();
        assert_eq!(stored, metadata);
        let observed = observed.lock().unwrap().clone().unwrap();
        assert_eq!(observed.1.as_deref(), Some(bytes.as_slice()));
        assert!(matches!(
            observed.0.command,
            ControlCommand::PutArtifact { byte_length, .. } if byte_length == bytes.len() as u64
        ));

        let (endpoint, observed) = fake_broker(
            ControlResponse::Artifact {
                artifact: metadata.clone(),
            },
            Some(bytes.to_vec()),
        );
        let mut client = Client::connect(endpoint).await.unwrap();
        let (downloaded_metadata, downloaded) =
            get_artifact(&mut client, "capability", "artifact-1")
                .await
                .unwrap();
        assert_eq!(downloaded_metadata, metadata);
        assert_eq!(downloaded, bytes);
        assert!(observed.lock().unwrap().as_ref().is_some_and(|(request, attachment)| {
            attachment.is_none()
                && matches!(
                    &request.command,
                    ControlCommand::GetArtifact { artifact_id } if artifact_id == "artifact-1"
                )
        }));

        let mut wrong_identity = metadata;
        wrong_identity.artifact_id = "artifact-other".to_owned();
        let (endpoint, _) = fake_broker(
            ControlResponse::Artifact {
                artifact: wrong_identity,
            },
            Some(bytes.to_vec()),
        );
        let mut client = Client::connect(endpoint).await.unwrap();
        assert!(
            get_artifact(&mut client, "capability", "artifact-1")
                .await
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn cursor_store_rejects_symlinks() {
        use std::os::unix::fs::symlink;
        let target = temp_path("target");
        let link = temp_path("link");
        fs::write(&target, br#"{"schema_version":1,"cursors":{}}"#).unwrap();
        symlink(&target, &link).unwrap();
        assert!(CursorStore::open(&link).is_err());
        fs::remove_file(link).unwrap();
        fs::remove_file(target).unwrap();
    }
}
