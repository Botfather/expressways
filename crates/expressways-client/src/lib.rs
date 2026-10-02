#[cfg(test)]
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use expressways_protocol::{
    ArtifactMetadata, Classification, ControlCommand, ControlRequest, ControlResponse,
    ControlWireEnvelope, StoredMessage, StreamFrame, TASK_EVENTS_TOPIC, TASKS_TOPIC, TaskEvent,
    TaskPayload, TaskStatus, TaskWorkItem,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const MAX_CAPABILITY_TOKEN_FILE_BYTES: u64 = 64 * 1024;
pub const MAX_SECRET_FILE_BYTES: u64 = 64 * 1024;
pub const MAX_AGENT_WORKER_STATE_BYTES: u64 = 1024 * 1024;
pub const MAX_CLIENT_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub fn normalize_capability_token(token: &str) -> anyhow::Result<String> {
    let token = token.trim();
    anyhow::ensure!(!token.is_empty(), "capability token is empty");
    anyhow::ensure!(
        token.len() as u64 <= MAX_CAPABILITY_TOKEN_FILE_BYTES,
        "capability token is {} bytes; maximum is {MAX_CAPABILITY_TOKEN_FILE_BYTES}",
        token.len()
    );
    Ok(token.to_owned())
}

pub fn read_capability_token_file(path: &Path) -> anyhow::Result<String> {
    let token = read_private_text_file(path, MAX_CAPABILITY_TOKEN_FILE_BYTES, "token")?;
    normalize_capability_token(&token)
        .map_err(|error| anyhow::anyhow!("invalid token file {}: {error}", path.display()))
}

pub fn read_secret_file(path: &Path) -> anyhow::Result<String> {
    let secret = read_private_text_file(path, MAX_SECRET_FILE_BYTES, "secret")?;
    let secret = secret.trim();
    anyhow::ensure!(
        !secret.is_empty(),
        "secret file {} is empty",
        path.display()
    );
    Ok(secret.to_owned())
}

pub async fn read_bounded_utf8_file(path: &Path, max_bytes: u64) -> anyhow::Result<String> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .await
        .map_err(|error| anyhow::anyhow!("failed to open {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|error| anyhow::anyhow!("failed to inspect {}: {error}", path.display()))?;
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() <= max_bytes,
        "{} is {} bytes; maximum is {max_bytes}",
        path.display(),
        metadata.len()
    );
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| anyhow::anyhow!("{} is too large to read", path.display()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
    anyhow::ensure!(
        bytes.len() as u64 <= max_bytes,
        "{} grew beyond the {max_bytes}-byte limit",
        path.display()
    );
    String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{} is not valid UTF-8", path.display()))
}

fn read_private_text_file(path: &Path, max_bytes: u64, kind: &str) -> anyhow::Result<String> {
    let initial_metadata = fs::symlink_metadata(path).map_err(|error| {
        anyhow::anyhow!("failed to inspect {kind} file {}: {error}", path.display())
    })?;
    anyhow::ensure!(
        initial_metadata.file_type().is_file(),
        "{kind} path {} is not a regular file",
        path.display()
    );

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file(),
        "{kind} path {} is not a regular file",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() <= max_bytes,
        "{kind} file {} is {} bytes; maximum is {max_bytes}",
        path.display(),
        metadata.len()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        anyhow::ensure!(
            mode & 0o077 == 0,
            "{kind} file {} has insecure permissions {mode:o}; expected no group or world access",
            path.display()
        );
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= max_bytes,
        "{kind} file {} grew beyond its size limit",
        path.display()
    );
    let value = std::str::from_utf8(&bytes)
        .map_err(|_| anyhow::anyhow!("{kind} file {} is not valid UTF-8", path.display()))?;
    Ok(value.to_owned())
}

pub fn load_agent_worker_state(path: &Path) -> anyhow::Result<AgentWorkerState> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AgentWorkerState::default());
        }
        Err(error) => {
            return Err(anyhow::anyhow!(
                "failed to inspect worker state {}: {error}",
                path.display()
            ));
        }
    };
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "worker state path {} is not a regular file",
        path.display()
    );

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file(),
        "worker state path {} is not a regular file",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() <= MAX_AGENT_WORKER_STATE_BYTES,
        "worker state {} is {} bytes; maximum is {MAX_AGENT_WORKER_STATE_BYTES}",
        path.display(),
        metadata.len()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        anyhow::ensure!(
            mode & 0o022 == 0,
            "worker state {} has unsafe writable permissions {mode:o}",
            path.display()
        );
    }

    let capacity = usize::try_from(metadata.len())
        .map_err(|_| anyhow::anyhow!("worker state {} is too large", path.display()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(MAX_AGENT_WORKER_STATE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_AGENT_WORKER_STATE_BYTES,
        "worker state {} grew beyond its size limit",
        path.display()
    );
    serde_json::from_slice(&bytes).map_err(|error| {
        anyhow::anyhow!("failed to parse worker state {}: {error}", path.display())
    })
}

pub fn save_agent_worker_state(path: &Path, state: &AgentWorkerState) -> anyhow::Result<()> {
    let rendered = serde_json::to_vec_pretty(state)?;
    anyhow::ensure!(
        rendered.len() as u64 <= MAX_AGENT_WORKER_STATE_BYTES,
        "serialized worker state is {} bytes; maximum is {MAX_AGENT_WORKER_STATE_BYTES}",
        rendered.len()
    );
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| anyhow::anyhow!("failed to create {}: {error}", parent.display()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("worker state path {} has no file name", path.display()))?;
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
        file.write_all(&rendered)?;
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
    result
        .map_err(|error| anyhow::anyhow!("failed to save worker state {}: {error}", path.display()))
}

pub trait ClientIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ClientIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub type BoxedClientIo = Box<dyn ClientIo>;
type BoxedConnectFuture = Pin<Box<dyn Future<Output = Result<BoxedClientIo, ClientError>> + Send>>;

#[derive(Clone)]
pub struct CustomEndpoint {
    label: String,
    connector: Arc<dyn Fn() -> BoxedConnectFuture + Send + Sync>,
}

impl std::fmt::Debug for CustomEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CustomEndpoint")
            .field("label", &self.label)
            .finish()
    }
}

impl CustomEndpoint {
    pub fn new<F, Fut>(label: impl Into<String>, connector: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BoxedClientIo, ClientError>> + Send + 'static,
    {
        Self {
            label: label.into(),
            connector: Arc::new(move || Box::pin(connector())),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Endpoint {
    Tcp(String),
    #[cfg(unix)]
    Unix(PathBuf),
    Custom(CustomEndpoint),
}

#[derive(Debug)]
pub struct Client {
    transport: Transport,
}

#[derive(Debug)]
pub struct StreamClient {
    transport: Transport,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentWorkerState {
    #[serde(default)]
    pub task_event_offset: u64,
    #[serde(default)]
    pub pending_report: Option<TaskEvent>,
}

#[derive(Clone)]
pub struct AgentWorker {
    endpoint: Endpoint,
    capability_token: String,
    agent_id: String,
    tasks_topic: String,
    task_events_topic: String,
    batch_limit: usize,
    state: AgentWorkerState,
    #[cfg(test)]
    mock_exchanges: Option<Arc<Mutex<VecDeque<MockExchange>>>>,
}

#[derive(Debug, Clone)]
pub struct AssignedTask {
    pub assignment: TaskEvent,
    pub task_message: StoredMessage,
    pub task: TaskWorkItem,
    #[doc(hidden)]
    pub hydrated_payload_bytes: Option<Vec<u8>>,
}

impl AssignedTask {
    pub fn payload_kind(&self) -> &'static str {
        self.task.payload.kind()
    }

    pub fn payload_content_type(&self) -> Option<&str> {
        self.task.payload.content_type()
    }

    pub fn payload_json_value(&self) -> Option<&serde_json::Value> {
        self.task.payload.json_value()
    }

    pub fn decode_payload_json<T>(&self) -> Result<T, PayloadAccessError>
    where
        T: DeserializeOwned,
    {
        let value = self
            .task
            .payload
            .json_value()
            .cloned()
            .ok_or_else(|| self.unexpected_payload_kind("json"))?;
        serde_json::from_value(value).map_err(|source| PayloadAccessError::InvalidJson {
            task_id: self.task.task_id.clone(),
            source,
        })
    }

    pub fn payload_text(&self) -> Option<&str> {
        match &self.task.payload {
            TaskPayload::Text { text, .. } => Some(text.as_str()),
            _ => None,
        }
    }

    pub fn payload_file_ref(&self) -> Option<TaskFileRef> {
        match &self.task.payload {
            TaskPayload::FileRef {
                path,
                content_type,
                byte_length,
                sha256,
            } => Some(TaskFileRef {
                path: PathBuf::from(path),
                content_type: content_type.clone(),
                byte_length: *byte_length,
                sha256: sha256.clone(),
            }),
            _ => None,
        }
    }

    pub fn payload_artifact_ref(&self) -> Option<TaskArtifactRef> {
        match &self.task.payload {
            TaskPayload::ArtifactRef {
                artifact_id,
                content_type,
                byte_length,
                sha256,
                local_path,
            } => Some(TaskArtifactRef {
                artifact_id: artifact_id.clone(),
                content_type: content_type.clone(),
                byte_length: *byte_length,
                sha256: sha256.clone(),
                local_path: local_path.as_ref().map(PathBuf::from),
            }),
            _ => None,
        }
    }

    pub fn decode_inline_bytes(&self) -> Result<Option<Vec<u8>>, PayloadAccessError> {
        self.task.payload.decode_inline_bytes().map_err(|detail| {
            PayloadAccessError::InvalidInlineBytes {
                task_id: self.task.task_id.clone(),
                detail,
            }
        })
    }

    pub async fn read_payload_bytes(&self) -> Result<Vec<u8>, PayloadAccessError> {
        if let Some(bytes) = self.decode_inline_bytes()? {
            return Ok(bytes);
        }
        if let Some(file_ref) = self.payload_file_ref() {
            return Err(PayloadAccessError::UntrustedLocalPath {
                task_id: self.task.task_id.clone(),
                kind: "file_ref",
                path: file_ref.path,
            });
        }
        if let Some(artifact_ref) = self.payload_artifact_ref() {
            return self.hydrated_payload_bytes.clone().ok_or_else(|| {
                PayloadAccessError::MissingArtifactBytes {
                    task_id: self.task.task_id.clone(),
                    artifact_id: artifact_ref.artifact_id,
                }
            });
        }
        if let Some(text) = self.payload_text() {
            return Ok(text.as_bytes().to_vec());
        }

        Err(self.unexpected_payload_kind("text, bytes, file_ref, or artifact_ref"))
    }

    fn unexpected_payload_kind(&self, expected: &'static str) -> PayloadAccessError {
        PayloadAccessError::UnexpectedKind {
            task_id: self.task.task_id.clone(),
            expected,
            actual: self.task.payload.kind(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFileRef {
    pub path: PathBuf,
    pub content_type: Option<String>,
    pub byte_length: Option<u64>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskArtifactRef {
    pub artifact_id: String,
    pub content_type: Option<String>,
    pub byte_length: Option<u64>,
    pub sha256: Option<String>,
    pub local_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct TaskExecutionContext {
    cancellation: CancellationToken,
    invalidation: Arc<Mutex<Option<TaskInvalidation>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInvalidation {
    pub status: TaskStatus,
    pub reason: Option<String>,
    pub emitted_at: DateTime<Utc>,
    pub assignment_id: Option<Uuid>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerRunOutcome {
    Idle,
    Completed {
        task_id: String,
        assignment_id: Uuid,
    },
    Failed {
        task_id: String,
        assignment_id: Option<Uuid>,
        reason: String,
    },
    Canceled {
        task_id: String,
        assignment_id: Option<Uuid>,
        status: TaskStatus,
        reason: Option<String>,
    },
}

#[derive(Debug, Error)]
pub enum PayloadAccessError {
    #[error("task `{task_id}` expected a {expected} payload, got `{actual}`")]
    UnexpectedKind {
        task_id: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("task `{task_id}` has invalid inline bytes payload: {detail}")]
    InvalidInlineBytes { task_id: String, detail: String },
    #[error("task `{task_id}` references artifact `{artifact_id}` without broker-hydrated bytes")]
    MissingArtifactBytes {
        task_id: String,
        artifact_id: String,
    },
    #[error(
        "task `{task_id}` contains an untrusted {kind} local path `{}`; local path reads are disabled",
        path.display()
    )]
    UntrustedLocalPath {
        task_id: String,
        kind: &'static str,
        path: PathBuf,
    },
    #[error("failed to decode JSON payload for task `{task_id}`: {source}")]
    InvalidJson {
        task_id: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to read file-backed payload for task `{task_id}` from {path}: {source}")]
    Io {
        task_id: String,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Atomically writes a handler result below an operator-configured directory.
///
/// The destination parent is canonicalized after creation to reject symlink
/// escapes. Writing through a same-directory temporary file prevents an
/// existing destination symlink from redirecting the write.
pub async fn write_contained_file(
    root: &Path,
    destination: &Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    tokio::fs::create_dir_all(root).await?;
    let canonical_root = tokio::fs::canonicalize(root).await?;
    let parent = destination.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "destination has no parent directory",
        )
    })?;
    tokio::fs::create_dir_all(parent).await?;
    let canonical_parent = tokio::fs::canonicalize(parent).await?;
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "destination parent {} escapes configured root {}",
                canonical_parent.display(),
                canonical_root.display()
            ),
        ));
    }

    let file_name = destination.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "destination has no file name",
        )
    })?;
    let temporary = canonical_parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        uuid::Uuid::now_v7()
    ));
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temporary, canonical_parent.join(file_name)).await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

enum Transport {
    Tcp(Framed<TcpStream, LengthDelimitedCodec>),
    #[cfg(unix)]
    Unix(Framed<UnixStream, LengthDelimitedCodec>),
    Custom(Framed<BoxedClientIo, LengthDelimitedCodec>),
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::Tcp(_) => "tcp",
            #[cfg(unix)]
            Self::Unix(_) => "unix",
            Self::Custom(_) => "custom",
        };
        formatter.debug_tuple("Transport").field(&kind).finish()
    }
}

enum WorkerClient {
    Live(Client),
    #[cfg(test)]
    Mock(MockClient),
}

#[cfg(test)]
struct MockClient {
    exchanges: Arc<Mutex<VecDeque<MockExchange>>>,
}

#[cfg(test)]
struct MockExchange {
    check: Box<dyn Fn(&ControlRequest) + Send + Sync>,
    response: Result<ControlResponse, ClientError>,
    attachment: Option<Vec<u8>>,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol framing error: {0}")]
    Codec(#[from] LengthDelimitedCodecError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("wire protocol error: {0}")]
    Wire(String),
    #[error("unexpected binary attachment in response")]
    UnexpectedAttachment,
    #[error("server closed the connection")]
    ConnectionClosed,
    #[error("unix sockets are not supported on this platform")]
    UnixUnsupported,
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("broker returned an error while {operation}: {code}: {message}")]
    Broker {
        operation: &'static str,
        code: String,
        message: String,
    },
    #[error("unexpected response while {operation}: {response}")]
    UnexpectedResponse {
        operation: &'static str,
        response: String,
    },
    #[error("malformed assignment event for task `{task_id}`: {detail}")]
    MalformedAssignment { task_id: String, detail: String },
    #[error("failed to hydrate artifact `{artifact_id}` for task `{task_id}`: {detail}")]
    ArtifactHydration {
        task_id: String,
        artifact_id: String,
        detail: String,
    },
}

#[derive(Debug, Error)]
enum TaskResolutionError {
    #[error("assignment for task `{task_id}` is missing task_offset")]
    MissingTaskOffset { task_id: String },
    #[error("task `{task_id}` was not found at offset {task_offset}")]
    TaskNotFound { task_id: String, task_offset: u64 },
    #[error(
        "task assignment expected `{task_id}` at offset {expected_offset}, but broker returned offset {actual_offset}"
    )]
    UnexpectedTaskOffset {
        task_id: String,
        expected_offset: u64,
        actual_offset: u64,
    },
    #[error(
        "task assignment references `{expected_task_id}` but task payload declared `{actual_task_id}`"
    )]
    TaskIdMismatch {
        expected_task_id: String,
        actual_task_id: String,
    },
    #[error("failed to parse task payload at offset {task_offset}: {source}")]
    InvalidTaskPayload {
        task_offset: u64,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
enum AssignmentFetchError {
    #[error(transparent)]
    Worker(#[from] WorkerError),
    #[error(transparent)]
    Task(#[from] TaskResolutionError),
}

type LengthDelimitedCodecError = tokio_util::codec::LengthDelimitedCodecError;

impl Client {
    pub async fn connect(endpoint: Endpoint) -> Result<Self, ClientError> {
        match endpoint {
            Endpoint::Tcp(address) => {
                let stream = TcpStream::connect(address).await?;
                stream.set_nodelay(true)?;
                Ok(Self {
                    transport: Transport::Tcp(Framed::new(stream, client_codec())),
                })
            }
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let stream = UnixStream::connect(path).await?;
                Ok(Self {
                    transport: Transport::Unix(Framed::new(stream, client_codec())),
                })
            }
            Endpoint::Custom(custom) => {
                let stream = (custom.connector)().await?;
                Ok(Self {
                    transport: Transport::Custom(Framed::new(stream, client_codec())),
                })
            }
        }
    }

    pub async fn send(&mut self, request: ControlRequest) -> Result<ControlResponse, ClientError> {
        let (response, attachment) = self.send_with_attachment(request, None).await?;
        if attachment.is_some() {
            return Err(ClientError::UnexpectedAttachment);
        }
        Ok(response)
    }

    pub async fn send_with_attachment(
        &mut self,
        request: ControlRequest,
        attachment: Option<Vec<u8>>,
    ) -> Result<(ControlResponse, Option<Vec<u8>>), ClientError> {
        match &mut self.transport {
            Transport::Tcp(transport) => send_request(transport, request, attachment).await,
            #[cfg(unix)]
            Transport::Unix(transport) => send_request(transport, request, attachment).await,
            Transport::Custom(transport) => send_request(transport, request, attachment).await,
        }
    }

    pub fn into_stream(self) -> StreamClient {
        StreamClient {
            transport: self.transport,
        }
    }
}

fn client_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_CLIENT_FRAME_BYTES)
        .new_codec()
}

impl StreamClient {
    pub async fn open(&mut self, request: ControlRequest) -> Result<StreamFrame, ClientError> {
        match &mut self.transport {
            Transport::Tcp(transport) => send_stream_open(transport, request).await,
            #[cfg(unix)]
            Transport::Unix(transport) => send_stream_open(transport, request).await,
            Transport::Custom(transport) => send_stream_open(transport, request).await,
        }
    }

    pub async fn next_frame(&mut self) -> Result<Option<StreamFrame>, ClientError> {
        match &mut self.transport {
            Transport::Tcp(transport) => read_stream_frame(transport).await,
            #[cfg(unix)]
            Transport::Unix(transport) => read_stream_frame(transport).await,
            Transport::Custom(transport) => read_stream_frame(transport).await,
        }
    }
}

impl WorkerClient {
    async fn send(&mut self, request: ControlRequest) -> Result<ControlResponse, ClientError> {
        match self {
            Self::Live(client) => client.send(request).await,
            #[cfg(test)]
            Self::Mock(client) => client.send(request).await,
        }
    }

    async fn send_with_attachment(
        &mut self,
        request: ControlRequest,
    ) -> Result<(ControlResponse, Option<Vec<u8>>), ClientError> {
        match self {
            Self::Live(client) => client.send_with_attachment(request, None).await,
            #[cfg(test)]
            Self::Mock(client) => client.send_with_attachment(request).await,
        }
    }
}

#[cfg(test)]
impl MockClient {
    async fn send(&mut self, request: ControlRequest) -> Result<ControlResponse, ClientError> {
        let exchange = self
            .exchanges
            .lock()
            .expect("queue lock")
            .pop_front()
            .expect("unexpected request");
        (exchange.check)(&request);
        exchange.response
    }

    async fn send_with_attachment(
        &mut self,
        request: ControlRequest,
    ) -> Result<(ControlResponse, Option<Vec<u8>>), ClientError> {
        let exchange = self
            .exchanges
            .lock()
            .expect("queue lock")
            .pop_front()
            .expect("unexpected request");
        (exchange.check)(&request);
        exchange
            .response
            .map(|response| (response, exchange.attachment))
    }
}

impl AgentWorker {
    pub fn new(
        endpoint: Endpoint,
        capability_token: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self {
        Self {
            endpoint,
            capability_token: capability_token.into(),
            agent_id: agent_id.into(),
            tasks_topic: TASKS_TOPIC.to_owned(),
            task_events_topic: TASK_EVENTS_TOPIC.to_owned(),
            batch_limit: 50,
            state: AgentWorkerState::default(),
            #[cfg(test)]
            mock_exchanges: None,
        }
    }

    pub fn with_topics(
        mut self,
        tasks_topic: impl Into<String>,
        task_events_topic: impl Into<String>,
    ) -> Self {
        self.tasks_topic = tasks_topic.into();
        self.task_events_topic = task_events_topic.into();
        self
    }

    pub fn with_batch_limit(mut self, batch_limit: usize) -> Self {
        self.batch_limit = batch_limit.max(1);
        self
    }

    pub fn with_state(mut self, state: AgentWorkerState) -> Self {
        self.state = state;
        self
    }

    #[cfg(test)]
    fn with_mock_exchanges(mut self, exchanges: Vec<MockExchange>) -> Self {
        self.mock_exchanges = Some(Arc::new(Mutex::new(VecDeque::from(exchanges))));
        self
    }

    pub fn state(&self) -> &AgentWorkerState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut AgentWorkerState {
        &mut self.state
    }

    pub fn into_state(self) -> AgentWorkerState {
        self.state
    }

    pub async fn flush_pending_report(&mut self) -> Result<bool, WorkerError> {
        let mut client = self.connect_worker_client().await?;
        self.flush_pending_report_with_client(&mut client).await
    }

    pub async fn run_once<H, Fut>(&mut self, handler: H) -> Result<WorkerRunOutcome, WorkerError>
    where
        H: FnOnce(AssignedTask) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        self.run_once_with_context(|assignment, _context| handler(assignment))
            .await
    }

    pub async fn run_once_with_context<H, Fut>(
        &mut self,
        handler: H,
    ) -> Result<WorkerRunOutcome, WorkerError>
    where
        H: FnOnce(AssignedTask, TaskExecutionContext) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        let mut client = self.connect_worker_client().await?;
        self.flush_pending_report_with_client(&mut client).await?;

        let Some(assignment_event) = self.poll_next_assignment_event(&mut client).await? else {
            return Ok(WorkerRunOutcome::Idle);
        };

        let report = match self
            .resolve_assignment(&mut client, &assignment_event)
            .await
        {
            Ok(assignment) => {
                let context = TaskExecutionContext::default();
                let watch_client = self.connect_worker_client().await?;
                let capability_token = self.capability_token.clone();
                let task_events_topic = self.task_events_topic.clone();
                let watch_offset = self.state.task_event_offset;
                let batch_limit = self.batch_limit;
                let watched_assignment = assignment_event.clone();
                let watched_context = context.clone();
                let watch_handle = tokio::spawn(async move {
                    watch_assignment_invalidation_loop(
                        watch_client,
                        capability_token,
                        task_events_topic,
                        watch_offset,
                        batch_limit,
                        watched_assignment,
                        watched_context,
                    )
                    .await
                });
                let handler_result = handler(assignment, context.clone()).await;

                let final_scan = if context.is_cancelled() {
                    Ok(())
                } else {
                    let mut scan_client = self.connect_worker_client().await?;
                    scan_assignment_invalidation_once(
                        &mut scan_client,
                        &self.capability_token,
                        &self.task_events_topic,
                        self.state.task_event_offset,
                        self.batch_limit,
                        &assignment_event,
                        &context,
                    )
                    .await
                };

                if !watch_handle.is_finished() {
                    watch_handle.abort();
                }
                let watcher_result = match watch_handle.await {
                    Ok(result) => result,
                    Err(error) if error.is_cancelled() => Ok(()),
                    Err(error) => Err(WorkerError::UnexpectedResponse {
                        operation: "watching assignment invalidation",
                        response: error.to_string(),
                    }),
                };

                if let Some(invalidation) = context.invalidation() {
                    return Ok(WorkerRunOutcome::Canceled {
                        task_id: assignment_event.task_id.clone(),
                        assignment_id: assignment_event.assignment_id,
                        status: invalidation.status,
                        reason: invalidation.reason,
                    });
                }

                watcher_result?;
                final_scan?;

                match handler_result {
                    Ok(()) => {
                        self.build_task_report(&assignment_event, TaskStatus::Completed, None)
                    }
                    Err(reason) => {
                        self.build_task_report(&assignment_event, TaskStatus::Failed, Some(reason))
                    }
                }
            }
            Err(AssignmentFetchError::Task(error)) => self.build_task_report(
                &assignment_event,
                TaskStatus::Failed,
                Some(error.to_string()),
            ),
            Err(AssignmentFetchError::Worker(error)) => return Err(error),
        };

        let outcome = outcome_from_report(&report);
        self.state.pending_report = Some(report);
        self.flush_pending_report_with_client(&mut client).await?;
        Ok(outcome)
    }

    async fn connect_worker_client(&self) -> Result<WorkerClient, ClientError> {
        #[cfg(test)]
        if let Some(exchanges) = &self.mock_exchanges {
            return Ok(WorkerClient::Mock(MockClient {
                exchanges: Arc::clone(exchanges),
            }));
        }

        Ok(WorkerClient::Live(
            Client::connect(self.endpoint.clone()).await?,
        ))
    }

    async fn flush_pending_report_with_client(
        &mut self,
        client: &mut WorkerClient,
    ) -> Result<bool, WorkerError> {
        let Some(event) = self.state.pending_report.clone() else {
            return Ok(false);
        };

        publish_task_event(
            client,
            &self.capability_token,
            &self.task_events_topic,
            &event,
        )
        .await?;
        self.state.pending_report = None;
        Ok(true)
    }

    async fn poll_next_assignment_event(
        &mut self,
        client: &mut WorkerClient,
    ) -> Result<Option<TaskEvent>, WorkerError> {
        loop {
            let messages = consume_messages(
                client,
                &self.capability_token,
                &self.task_events_topic,
                self.state.task_event_offset,
                self.batch_limit,
            )
            .await?;
            let message_count = messages.len();

            if messages.is_empty() {
                return Ok(None);
            }

            for message in messages {
                self.state.task_event_offset = self
                    .state
                    .task_event_offset
                    .max(message.offset.saturating_add(1));
                let event = match serde_json::from_str::<TaskEvent>(&message.payload) {
                    Ok(event) => event,
                    Err(_) => continue,
                };

                if event.status != TaskStatus::Assigned {
                    continue;
                }

                if event.agent_id.as_deref() != Some(self.agent_id.as_str()) {
                    continue;
                }

                if event.assignment_id.is_none() {
                    return Err(WorkerError::MalformedAssignment {
                        task_id: event.task_id.clone(),
                        detail: "assignment event is missing assignment_id".to_owned(),
                    });
                }

                return Ok(Some(event));
            }

            if message_count < self.batch_limit {
                return Ok(None);
            }
        }
    }

    async fn resolve_assignment(
        &self,
        client: &mut WorkerClient,
        assignment: &TaskEvent,
    ) -> Result<AssignedTask, AssignmentFetchError> {
        let task_offset =
            assignment
                .task_offset
                .ok_or_else(|| TaskResolutionError::MissingTaskOffset {
                    task_id: assignment.task_id.clone(),
                })?;
        let mut messages = consume_messages(
            client,
            &self.capability_token,
            &self.tasks_topic,
            task_offset,
            1,
        )
        .await?;
        let task_message = messages
            .pop()
            .ok_or_else(|| TaskResolutionError::TaskNotFound {
                task_id: assignment.task_id.clone(),
                task_offset,
            })?;
        if task_message.offset != task_offset {
            return Err(TaskResolutionError::UnexpectedTaskOffset {
                task_id: assignment.task_id.clone(),
                expected_offset: task_offset,
                actual_offset: task_message.offset,
            }
            .into());
        }

        let task =
            serde_json::from_str::<TaskWorkItem>(&task_message.payload).map_err(|source| {
                TaskResolutionError::InvalidTaskPayload {
                    task_offset,
                    source,
                }
            })?;
        if task.task_id != assignment.task_id {
            return Err(TaskResolutionError::TaskIdMismatch {
                expected_task_id: assignment.task_id.clone(),
                actual_task_id: task.task_id.clone(),
            }
            .into());
        }

        let hydrated_payload_bytes = if let TaskPayload::ArtifactRef {
            artifact_id,
            byte_length,
            sha256,
            ..
        } = &task.payload
        {
            let (response, attachment) = client
                .send_with_attachment(ControlRequest {
                    capability_token: self.capability_token.clone(),
                    command: ControlCommand::GetArtifact {
                        artifact_id: artifact_id.clone(),
                    },
                })
                .await
                .map_err(WorkerError::from)?;
            match response {
                ControlResponse::Artifact { artifact } => {
                    let bytes = attachment.ok_or_else(|| WorkerError::ArtifactHydration {
                        task_id: task.task_id.clone(),
                        artifact_id: artifact_id.clone(),
                        detail: "broker response omitted artifact bytes".to_owned(),
                    })?;
                    verify_hydrated_artifact(
                        &task.task_id,
                        artifact_id,
                        *byte_length,
                        sha256.as_deref(),
                        &artifact,
                        &bytes,
                    )?;
                    Some(bytes)
                }
                ControlResponse::Error { code, message } => {
                    return Err(WorkerError::Broker {
                        operation: "hydrating task artifact",
                        code,
                        message,
                    }
                    .into());
                }
                other => {
                    return Err(WorkerError::UnexpectedResponse {
                        operation: "hydrating task artifact",
                        response: format!("{other:?}"),
                    }
                    .into());
                }
            }
        } else {
            None
        };

        Ok(AssignedTask {
            assignment: assignment.clone(),
            task_message,
            task,
            hydrated_payload_bytes,
        })
    }

    fn build_task_report(
        &self,
        assignment: &TaskEvent,
        status: TaskStatus,
        reason: Option<String>,
    ) -> TaskEvent {
        TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: assignment.task_id.clone(),
            task_offset: assignment.task_offset,
            assignment_id: assignment.assignment_id,
            agent_id: Some(self.agent_id.clone()),
            status,
            attempt: assignment.attempt,
            reason,
            emitted_at: Utc::now(),
        }
    }
}

fn verify_hydrated_artifact(
    task_id: &str,
    expected_artifact_id: &str,
    declared_byte_length: Option<u64>,
    declared_sha256: Option<&str>,
    artifact: &ArtifactMetadata,
    bytes: &[u8],
) -> Result<(), WorkerError> {
    let actual_length = bytes.len() as u64;
    let actual_sha256 = hex::encode(Sha256::digest(bytes));
    let invalid = artifact.artifact_id != expected_artifact_id
        || artifact.byte_length != actual_length
        || artifact.sha256 != actual_sha256
        || declared_byte_length.is_some_and(|length| length != actual_length)
        || declared_sha256.is_some_and(|sha256| sha256 != actual_sha256);
    if invalid {
        return Err(WorkerError::ArtifactHydration {
            task_id: task_id.to_owned(),
            artifact_id: expected_artifact_id.to_owned(),
            detail: format!(
                "integrity mismatch (response id={}, declared bytes={declared_byte_length:?}, response bytes={}, actual bytes={}, declared sha256={declared_sha256:?}, response sha256={}, actual sha256={})",
                artifact.artifact_id,
                artifact.byte_length,
                actual_length,
                artifact.sha256,
                actual_sha256
            ),
        });
    }
    Ok(())
}

impl Default for TaskExecutionContext {
    fn default() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            invalidation: Arc::new(Mutex::new(None)),
        }
    }
}

impl TaskExecutionContext {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub fn invalidation(&self) -> Option<TaskInvalidation> {
        self.invalidation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn invalidate(&self, invalidation: TaskInvalidation) {
        let mut slot = self
            .invalidation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_none() {
            *slot = Some(invalidation);
            self.cancellation.cancel();
        }
    }
}

async fn watch_assignment_invalidation_loop(
    mut client: WorkerClient,
    capability_token: String,
    task_events_topic: String,
    start_offset: u64,
    batch_limit: usize,
    assignment: TaskEvent,
    context: TaskExecutionContext,
) -> Result<(), WorkerError> {
    let mut next_offset = start_offset;

    loop {
        if scan_assignment_invalidation_batch(
            &mut client,
            &capability_token,
            &task_events_topic,
            &mut next_offset,
            batch_limit,
            &assignment,
            &context,
        )
        .await?
        {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn scan_assignment_invalidation_once(
    client: &mut WorkerClient,
    capability_token: &str,
    task_events_topic: &str,
    start_offset: u64,
    batch_limit: usize,
    assignment: &TaskEvent,
    context: &TaskExecutionContext,
) -> Result<(), WorkerError> {
    let mut next_offset = start_offset;

    loop {
        let messages = consume_messages(
            client,
            capability_token,
            task_events_topic,
            next_offset,
            batch_limit,
        )
        .await?;
        if messages.is_empty() {
            return Ok(());
        }

        for message in messages {
            next_offset = message.offset.saturating_add(1);
            let Ok(event) = serde_json::from_str::<TaskEvent>(&message.payload) else {
                continue;
            };
            if let Some(invalidation) = task_invalidation_for_event(assignment, &event) {
                context.invalidate(invalidation);
                return Ok(());
            }
        }
    }
}

async fn scan_assignment_invalidation_batch(
    client: &mut WorkerClient,
    capability_token: &str,
    task_events_topic: &str,
    next_offset: &mut u64,
    batch_limit: usize,
    assignment: &TaskEvent,
    context: &TaskExecutionContext,
) -> Result<bool, WorkerError> {
    let messages = consume_messages(
        client,
        capability_token,
        task_events_topic,
        *next_offset,
        batch_limit,
    )
    .await?;
    if messages.is_empty() {
        return Ok(false);
    }

    for message in messages {
        *next_offset = message.offset.saturating_add(1);
        let Ok(event) = serde_json::from_str::<TaskEvent>(&message.payload) else {
            continue;
        };
        if let Some(invalidation) = task_invalidation_for_event(assignment, &event) {
            context.invalidate(invalidation);
            return Ok(true);
        }
    }

    Ok(false)
}

fn task_invalidation_for_event(
    assignment: &TaskEvent,
    event: &TaskEvent,
) -> Option<TaskInvalidation> {
    if event.task_id != assignment.task_id {
        return None;
    }

    let same_assignment = event.assignment_id == assignment.assignment_id
        && event.agent_id.as_deref() == assignment.agent_id.as_deref();

    let invalidates = match event.status {
        TaskStatus::Assigned => !same_assignment,
        TaskStatus::Pending => same_assignment || event.assignment_id.is_none(),
        TaskStatus::RetryScheduled
        | TaskStatus::TimedOut
        | TaskStatus::Exhausted
        | TaskStatus::Canceled => same_assignment || event.assignment_id.is_none(),
        TaskStatus::Completed | TaskStatus::Failed => same_assignment,
    };

    invalidates.then(|| TaskInvalidation {
        status: event.status,
        reason: event.reason.clone(),
        emitted_at: event.emitted_at,
        assignment_id: event.assignment_id,
        agent_id: event.agent_id.clone(),
    })
}

async fn send_request<T>(
    transport: &mut Framed<T, LengthDelimitedCodec>,
    request: ControlRequest,
    attachment: Option<Vec<u8>>,
) -> Result<(ControlResponse, Option<Vec<u8>>), ClientError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let payload = ControlWireEnvelope::Request {
        request,
        attachment_length: 0,
    }
    .encode_with_attachment(attachment.as_deref())?;
    transport.send(payload.into()).await?;

    let frame = transport
        .next()
        .await
        .ok_or(ClientError::ConnectionClosed)??;
    let (envelope, attachment) =
        ControlWireEnvelope::decode_packet(&frame).map_err(ClientError::Wire)?;
    match envelope {
        ControlWireEnvelope::Response { response, .. } => {
            Ok((response, (!attachment.is_empty()).then_some(attachment)))
        }
        other => Err(ClientError::Wire(format!(
            "expected response packet, got {other:?}"
        ))),
    }
}

async fn send_stream_open<T>(
    transport: &mut Framed<T, LengthDelimitedCodec>,
    request: ControlRequest,
) -> Result<StreamFrame, ClientError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let payload = ControlWireEnvelope::Request {
        request,
        attachment_length: 0,
    }
    .encode_with_attachment(None)?;
    transport.send(payload.into()).await?;

    let frame = transport
        .next()
        .await
        .ok_or(ClientError::ConnectionClosed)??;
    let (envelope, attachment) =
        ControlWireEnvelope::decode_packet(&frame).map_err(ClientError::Wire)?;
    if !attachment.is_empty() {
        return Err(ClientError::UnexpectedAttachment);
    }
    match envelope {
        ControlWireEnvelope::Stream { frame } => Ok(frame),
        other => Err(ClientError::Wire(format!(
            "expected stream packet, got {other:?}"
        ))),
    }
}

async fn read_stream_frame<T>(
    transport: &mut Framed<T, LengthDelimitedCodec>,
) -> Result<Option<StreamFrame>, ClientError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match transport.next().await {
        Some(frame) => {
            let frame = frame?;
            let (envelope, attachment) =
                ControlWireEnvelope::decode_packet(&frame).map_err(ClientError::Wire)?;
            if !attachment.is_empty() {
                return Err(ClientError::UnexpectedAttachment);
            }
            match envelope {
                ControlWireEnvelope::Stream { frame } => Ok(Some(frame)),
                other => Err(ClientError::Wire(format!(
                    "expected stream frame packet, got {other:?}"
                ))),
            }
        }
        None => Ok(None),
    }
}

async fn publish_task_event(
    client: &mut WorkerClient,
    capability_token: &str,
    topic: &str,
    event: &TaskEvent,
) -> Result<(), WorkerError> {
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::Publish {
                topic: topic.to_owned(),
                classification: Some(Classification::Internal),
                payload: serde_json::to_string(event)?,
            },
        })
        .await?;

    match response {
        ControlResponse::PublishAccepted { .. } => Ok(()),
        ControlResponse::Error { code, message } => Err(WorkerError::Broker {
            operation: "publishing task event",
            code,
            message,
        }),
        other => Err(WorkerError::UnexpectedResponse {
            operation: "publishing task event",
            response: format!("{other:?}"),
        }),
    }
}

async fn consume_messages(
    client: &mut WorkerClient,
    capability_token: &str,
    topic: &str,
    offset: u64,
    limit: usize,
) -> Result<Vec<StoredMessage>, WorkerError> {
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::Consume {
                topic: topic.to_owned(),
                offset,
                limit,
            },
        })
        .await?;

    match response {
        ControlResponse::Messages { messages, .. } => Ok(messages),
        ControlResponse::Error { code, message } => Err(WorkerError::Broker {
            operation: "consuming messages",
            code,
            message,
        }),
        other => Err(WorkerError::UnexpectedResponse {
            operation: "consuming messages",
            response: format!("{other:?}"),
        }),
    }
}

fn outcome_from_report(report: &TaskEvent) -> WorkerRunOutcome {
    match report.status {
        TaskStatus::Completed => WorkerRunOutcome::Completed {
            task_id: report.task_id.clone(),
            assignment_id: report.assignment_id.unwrap_or_else(Uuid::nil),
        },
        TaskStatus::Failed => WorkerRunOutcome::Failed {
            task_id: report.task_id.clone(),
            assignment_id: report.assignment_id,
            reason: report
                .reason
                .clone()
                .unwrap_or_else(|| "task failed".to_owned()),
        },
        _ => WorkerRunOutcome::Idle,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use expressways_protocol::{TaskPayload, TaskRequirements};

    fn temp_token_path() -> PathBuf {
        std::env::temp_dir().join(format!("expressways-token-{}", Uuid::now_v7()))
    }

    #[test]
    fn capability_token_files_are_bounded_private_regular_files() {
        let path = temp_token_path();
        fs::write(&path, b"  signed-token\n").expect("write token");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("set private permissions");
        }
        assert_eq!(
            read_capability_token_file(&path).expect("read token"),
            "signed-token"
        );

        File::create(&path)
            .expect("replace token")
            .set_len(MAX_CAPABILITY_TOKEN_FILE_BYTES + 1)
            .expect("make sparse oversized token");
        assert!(read_capability_token_file(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn capability_token_files_reject_insecure_modes_and_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let path = temp_token_path();
        fs::write(&path, b"signed-token").expect("write token");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("set insecure permissions");
        assert!(read_capability_token_file(&path).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("set secure permissions");
        let link = temp_token_path();
        symlink(&path, &link).expect("create symlink");
        assert!(read_capability_token_file(&link).is_err());
    }

    #[test]
    fn inline_capability_tokens_are_trimmed_and_bounded() {
        assert_eq!(
            normalize_capability_token("  signed-token \n").expect("normalize"),
            "signed-token"
        );
        assert!(normalize_capability_token("  ").is_err());
        assert!(
            normalize_capability_token(&"x".repeat(MAX_CAPABILITY_TOKEN_FILE_BYTES as usize + 1))
                .is_err()
        );
    }

    #[test]
    fn client_codec_uses_the_explicit_protocol_frame_ceiling() {
        assert_eq!(client_codec().max_frame_length(), MAX_CLIENT_FRAME_BYTES);
    }

    #[test]
    fn worker_state_round_trips_through_private_atomic_file() {
        let path = std::env::temp_dir().join(format!("expressways-worker-{}.json", Uuid::now_v7()));
        let state = AgentWorkerState {
            task_event_offset: 42,
            pending_report: None,
        };

        save_agent_worker_state(&path, &state).expect("save worker state");
        assert_eq!(
            load_agent_worker_state(&path)
                .expect("load worker state")
                .task_event_offset,
            42
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path)
                    .expect("inspect worker state")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        fs::remove_file(path).expect("remove worker state");
    }

    #[test]
    fn worker_state_rejects_oversized_files_before_parsing() {
        let path = std::env::temp_dir().join(format!("expressways-worker-{}.json", Uuid::now_v7()));
        File::create(&path)
            .expect("create worker state")
            .set_len(MAX_AGENT_WORKER_STATE_BYTES + 1)
            .expect("size worker state");

        assert!(load_agent_worker_state(&path).is_err());
        fs::remove_file(path).expect("remove worker state");
    }

    #[cfg(unix)]
    #[test]
    fn worker_state_rejects_symlinks_and_group_writable_files() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let path = std::env::temp_dir().join(format!("expressways-worker-{}.json", Uuid::now_v7()));
        fs::write(&path, b"{}").expect("write worker state");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o620))
            .expect("make worker state group writable");
        assert!(load_agent_worker_state(&path).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("make worker state private");
        let link = std::env::temp_dir().join(format!("expressways-worker-{}.json", Uuid::now_v7()));
        symlink(&path, &link).expect("create worker state symlink");
        assert!(load_agent_worker_state(&link).is_err());

        fs::remove_file(link).expect("remove worker state symlink");
        fs::remove_file(path).expect("remove worker state");
    }

    #[tokio::test]
    async fn agent_worker_runs_assignment_and_publishes_completion() {
        let assignment_id =
            Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa11").expect("assignment id");
        let assignment = TaskEvent {
            event_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa12").expect("event id"),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 1,
            reason: None,
            emitted_at: Utc::now(),
        };
        let artifact_bytes = b"managed artifact".to_vec();
        let artifact_sha256 = hex::encode(Sha256::digest(&artifact_bytes));
        let mut task = task_work_item("task-1");
        task.payload = TaskPayload::artifact_ref(
            "artifact-1",
            Some("application/octet-stream".to_owned()),
            Some(artifact_bytes.len() as u64),
            Some(artifact_sha256.clone()),
            Some("/untrusted/server/path.blob".to_owned()),
        );
        let mut worker =
            AgentWorker::new(Endpoint::Tcp("unused".to_owned()), "signed-token", "alpha")
                .with_mock_exchanges(vec![
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        0,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASK_EVENTS_TOPIC,
                                0,
                                serde_json::to_string(&assignment).expect("serialize assignment"),
                            )],
                            next_offset: 1,
                        },
                    ),
                    expect_consume(
                        TASKS_TOPIC,
                        4,
                        1,
                        ControlResponse::Messages {
                            topic: TASKS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASKS_TOPIC,
                                4,
                                serde_json::to_string(&task).expect("serialize task"),
                            )],
                            next_offset: 5,
                        },
                    ),
                    expect_get_artifact("artifact-1", artifact_bytes.clone(), artifact_sha256),
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        1,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: Vec::new(),
                            next_offset: 1,
                        },
                    ),
                    expect_publish_task_event(
                        TASK_EVENTS_TOPIC,
                        TaskStatus::Completed,
                        "task-1",
                        Some(assignment_id),
                        "alpha",
                        None,
                        ControlResponse::PublishAccepted {
                            message_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa13")
                                .expect("message id"),
                            offset: 1,
                            classification: Classification::Internal,
                        },
                    ),
                ]);
        let outcome = worker
            .run_once(|assignment| async move {
                assert_eq!(assignment.task.task_id, "task-1");
                assert_eq!(
                    assignment
                        .read_payload_bytes()
                        .await
                        .expect("read hydrated artifact"),
                    b"managed artifact".to_vec()
                );
                Ok(())
            })
            .await
            .expect("run worker");

        assert_eq!(
            outcome,
            WorkerRunOutcome::Completed {
                task_id: "task-1".to_owned(),
                assignment_id,
            }
        );
        assert_eq!(worker.state().task_event_offset, 1);
        assert!(worker.state().pending_report.is_none());
    }

    #[tokio::test]
    async fn agent_worker_retries_pending_report_before_consuming_new_work() {
        let assignment_id =
            Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa21").expect("assignment id");
        let assignment = TaskEvent {
            event_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa22").expect("event id"),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 1,
            reason: None,
            emitted_at: Utc::now(),
        };
        let task = task_work_item("task-1");
        let mut worker =
            AgentWorker::new(Endpoint::Tcp("unused".to_owned()), "signed-token", "alpha")
                .with_mock_exchanges(vec![
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        0,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASK_EVENTS_TOPIC,
                                0,
                                serde_json::to_string(&assignment).expect("serialize assignment"),
                            )],
                            next_offset: 1,
                        },
                    ),
                    expect_consume(
                        TASKS_TOPIC,
                        4,
                        1,
                        ControlResponse::Messages {
                            topic: TASKS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASKS_TOPIC,
                                4,
                                serde_json::to_string(&task).expect("serialize task"),
                            )],
                            next_offset: 5,
                        },
                    ),
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        1,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: Vec::new(),
                            next_offset: 1,
                        },
                    ),
                    expect_publish_task_event(
                        TASK_EVENTS_TOPIC,
                        TaskStatus::Completed,
                        "task-1",
                        Some(assignment_id),
                        "alpha",
                        None,
                        ControlResponse::Error {
                            code: "service_degraded".to_owned(),
                            message: "task events unavailable".to_owned(),
                        },
                    ),
                    expect_publish_task_event(
                        TASK_EVENTS_TOPIC,
                        TaskStatus::Completed,
                        "task-1",
                        Some(assignment_id),
                        "alpha",
                        None,
                        ControlResponse::PublishAccepted {
                            message_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa23")
                                .expect("message id"),
                            offset: 1,
                            classification: Classification::Internal,
                        },
                    ),
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        1,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: Vec::new(),
                            next_offset: 1,
                        },
                    ),
                ]);
        let error = worker
            .run_once(|_| async move { Ok(()) })
            .await
            .expect_err("initial publish should fail");
        match error {
            WorkerError::Broker {
                operation, code, ..
            } => {
                assert_eq!(operation, "publishing task event");
                assert_eq!(code, "service_degraded");
            }
            other => panic!("expected broker error, got {other:?}"),
        }
        assert_eq!(worker.state().task_event_offset, 1);
        assert!(worker.state().pending_report.is_some());

        let outcome = worker
            .run_once(|_| async move { panic!("handler should not run") })
            .await
            .expect("retry pending report");
        assert_eq!(outcome, WorkerRunOutcome::Idle);
        assert!(worker.state().pending_report.is_none());
    }

    #[tokio::test]
    async fn agent_worker_marks_missing_task_payload_as_failed() {
        let assignment_id =
            Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa31").expect("assignment id");
        let assignment = TaskEvent {
            event_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa32").expect("event id"),
            task_id: "task-1".to_owned(),
            task_offset: Some(9),
            assignment_id: Some(assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 2,
            reason: None,
            emitted_at: Utc::now(),
        };
        let mut worker =
            AgentWorker::new(Endpoint::Tcp("unused".to_owned()), "signed-token", "alpha")
                .with_mock_exchanges(vec![
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        0,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASK_EVENTS_TOPIC,
                                0,
                                serde_json::to_string(&assignment).expect("serialize assignment"),
                            )],
                            next_offset: 1,
                        },
                    ),
                    expect_consume(
                        TASKS_TOPIC,
                        9,
                        1,
                        ControlResponse::Messages {
                            topic: TASKS_TOPIC.to_owned(),
                            messages: Vec::new(),
                            next_offset: 9,
                        },
                    ),
                    expect_publish_task_event(
                        TASK_EVENTS_TOPIC,
                        TaskStatus::Failed,
                        "task-1",
                        Some(assignment_id),
                        "alpha",
                        Some("not found"),
                        ControlResponse::PublishAccepted {
                            message_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa33")
                                .expect("message id"),
                            offset: 1,
                            classification: Classification::Internal,
                        },
                    ),
                ]);
        let outcome = worker
            .run_once(|_| async move { panic!("handler should not run") })
            .await
            .expect("run worker");

        match outcome {
            WorkerRunOutcome::Failed {
                task_id,
                assignment_id: Some(id),
                reason,
            } => {
                assert_eq!(task_id, "task-1");
                assert_eq!(id, assignment_id);
                assert!(reason.contains("not found"));
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn agent_worker_stops_when_assignment_is_canceled() {
        let assignment_id =
            Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa41").expect("assignment id");
        let assignment = TaskEvent {
            event_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa42").expect("event id"),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 1,
            reason: None,
            emitted_at: Utc::now(),
        };
        let canceled = TaskEvent {
            event_id: Uuid::parse_str("018f7f2f-8d84-7b11-9f4e-9b5531e3aa43")
                .expect("cancel event id"),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Canceled,
            attempt: 1,
            reason: Some("operator canceled work".to_owned()),
            emitted_at: Utc::now(),
        };
        let task = task_work_item("task-1");
        let mut worker =
            AgentWorker::new(Endpoint::Tcp("unused".to_owned()), "signed-token", "alpha")
                .with_mock_exchanges(vec![
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        0,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASK_EVENTS_TOPIC,
                                0,
                                serde_json::to_string(&assignment).expect("serialize assignment"),
                            )],
                            next_offset: 1,
                        },
                    ),
                    expect_consume(
                        TASKS_TOPIC,
                        4,
                        1,
                        ControlResponse::Messages {
                            topic: TASKS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASKS_TOPIC,
                                4,
                                serde_json::to_string(&task).expect("serialize task"),
                            )],
                            next_offset: 5,
                        },
                    ),
                    expect_consume(
                        TASK_EVENTS_TOPIC,
                        1,
                        50,
                        ControlResponse::Messages {
                            topic: TASK_EVENTS_TOPIC.to_owned(),
                            messages: vec![stored_message(
                                TASK_EVENTS_TOPIC,
                                1,
                                serde_json::to_string(&canceled).expect("serialize cancel event"),
                            )],
                            next_offset: 2,
                        },
                    ),
                ]);

        let outcome = worker
            .run_once_with_context(|_, context| async move {
                context.cancelled().await;
                Err("handler should stop after cancellation".to_owned())
            })
            .await
            .expect("run worker");

        assert_eq!(
            outcome,
            WorkerRunOutcome::Canceled {
                task_id: "task-1".to_owned(),
                assignment_id: Some(assignment_id),
                status: TaskStatus::Canceled,
                reason: Some("operator canceled work".to_owned()),
            }
        );
        assert!(worker.state().pending_report.is_none());
    }

    #[test]
    fn assigned_task_helpers_decode_json_and_metadata() {
        #[derive(Debug, Deserialize, PartialEq, Eq)]
        struct DemoPayload {
            path: String,
        }

        let mut task = task_work_item("task-1");
        task.payload = TaskPayload::json(serde_json::json!({ "path": "notes.md" }));
        let assigned = assigned_task(task);

        assert_eq!(assigned.payload_kind(), "json");
        assert_eq!(assigned.payload_content_type(), Some("application/json"));
        assert_eq!(
            assigned
                .decode_payload_json::<DemoPayload>()
                .expect("json payload"),
            DemoPayload {
                path: "notes.md".to_owned()
            }
        );
        assert!(assigned.payload_text().is_none());
        assert!(assigned.payload_file_ref().is_none());
        assert!(assigned.payload_artifact_ref().is_none());
    }

    #[tokio::test]
    async fn assigned_task_helpers_read_inline_and_reject_untrusted_file_payloads() {
        let mut inline_task = task_work_item("task-inline");
        inline_task.payload = TaskPayload::bytes(b"PNG", "image/png");
        let inline_assigned = assigned_task(inline_task);
        assert_eq!(
            inline_assigned.decode_inline_bytes().expect("inline bytes"),
            Some(b"PNG".to_vec())
        );
        assert_eq!(
            inline_assigned
                .read_payload_bytes()
                .await
                .expect("read inline"),
            b"PNG".to_vec()
        );

        let path = PathBuf::from("/untrusted/host/path.pdf");
        let mut file_task = task_work_item("task-file");
        file_task.payload = TaskPayload::file_ref(
            path.display().to_string(),
            Some("application/pdf".to_owned()),
            Some(3),
            Some("abc123".to_owned()),
        );
        let file_assigned = assigned_task(file_task);

        let file_ref = file_assigned.payload_file_ref().expect("file ref");
        assert_eq!(file_ref.path, path);
        assert_eq!(file_ref.content_type.as_deref(), Some("application/pdf"));
        let error = file_assigned
            .read_payload_bytes()
            .await
            .expect_err("untrusted file references must not be read");
        assert!(matches!(
            error,
            PayloadAccessError::UntrustedLocalPath { .. }
        ));
    }

    #[tokio::test]
    async fn assigned_task_helpers_read_artifact_ref_payload_bytes() {
        let mut task = task_work_item("task-artifact");
        task.payload = TaskPayload::artifact_ref(
            "artifact-1",
            Some("application/x-protobuf".to_owned()),
            Some(5),
            Some("abc123".to_owned()),
            Some("/untrusted/server/path.blob".to_owned()),
        );
        let mut assigned = assigned_task(task);
        assigned.hydrated_payload_bytes = Some(b"PROTO".to_vec());

        let artifact_ref = assigned.payload_artifact_ref().expect("artifact ref");
        assert_eq!(artifact_ref.artifact_id, "artifact-1");
        assert_eq!(
            artifact_ref.content_type.as_deref(),
            Some("application/x-protobuf")
        );
        assert_eq!(
            assigned
                .read_payload_bytes()
                .await
                .expect("read artifact payload"),
            b"PROTO".to_vec()
        );
    }

    #[test]
    fn hydrated_artifact_verification_rejects_tampered_bytes() {
        let expected = b"expected artifact";
        let expected_sha256 = hex::encode(Sha256::digest(expected));
        let metadata = ArtifactMetadata {
            artifact_id: "artifact-1".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            byte_length: expected.len() as u64,
            sha256: expected_sha256.clone(),
            classification: Classification::Internal,
            retention_class: expressways_protocol::RetentionClass::Operational,
            created_at: Utc::now(),
            principal: "local:developer".to_owned(),
            local_path: Some("/untrusted/server/path.blob".to_owned()),
        };

        let error = verify_hydrated_artifact(
            "task-1",
            "artifact-1",
            Some(expected.len() as u64),
            Some(&expected_sha256),
            &metadata,
            b"tampered artifact",
        )
        .expect_err("tampered bytes must fail integrity verification");

        assert!(matches!(error, WorkerError::ArtifactHydration { .. }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn contained_file_write_rejects_symlinked_parent_escape() {
        use std::os::unix::fs::symlink;

        let base = std::env::temp_dir().join(format!("expressways-output-{}", Uuid::now_v7()));
        let root = base.join("root");
        let outside = base.join("outside");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        tokio::fs::create_dir_all(&outside)
            .await
            .expect("create outside");
        symlink(&outside, root.join("escape")).expect("create symlink");

        let error = write_contained_file(&root, &root.join("escape/result.json"), b"secret")
            .await
            .expect_err("symlink escape must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(!outside.join("result.json").exists());

        let _ = tokio::fs::remove_dir_all(base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn contained_file_write_replaces_destination_symlink_not_target() {
        use std::os::unix::fs::symlink;

        let base = std::env::temp_dir().join(format!("expressways-output-{}", Uuid::now_v7()));
        let root = base.join("root");
        let outside = base.join("outside.txt");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        tokio::fs::write(&outside, b"original")
            .await
            .expect("write outside");
        let destination = root.join("result.json");
        symlink(&outside, &destination).expect("create destination symlink");

        write_contained_file(&root, &destination, b"safe")
            .await
            .expect("write contained file");
        assert_eq!(
            tokio::fs::read(&outside).await.expect("read outside"),
            b"original"
        );
        assert_eq!(
            tokio::fs::read(&destination).await.expect("read result"),
            b"safe"
        );

        let _ = tokio::fs::remove_dir_all(base).await;
    }

    fn expect_consume(
        topic: &str,
        offset: u64,
        limit: usize,
        response: ControlResponse,
    ) -> MockExchange {
        let topic = topic.to_owned();
        MockExchange {
            check: Box::new(move |request| {
                assert_eq!(request.capability_token, "signed-token");
                match &request.command {
                    ControlCommand::Consume {
                        topic: actual_topic,
                        offset: actual_offset,
                        limit: actual_limit,
                    } => {
                        assert_eq!(actual_topic, &topic);
                        assert_eq!(*actual_offset, offset);
                        assert_eq!(*actual_limit, limit);
                    }
                    other => panic!("expected consume request, got {other:?}"),
                }
            }),
            response: Ok(response),
            attachment: None,
        }
    }

    fn expect_publish_task_event(
        topic: &str,
        expected_status: TaskStatus,
        expected_task_id: &str,
        expected_assignment_id: Option<Uuid>,
        expected_agent_id: &str,
        reason_contains: Option<&str>,
        response: ControlResponse,
    ) -> MockExchange {
        let topic = topic.to_owned();
        let expected_task_id = expected_task_id.to_owned();
        let expected_agent_id = expected_agent_id.to_owned();
        let reason_contains = reason_contains.map(str::to_owned);

        MockExchange {
            check: Box::new(move |request| {
                assert_eq!(request.capability_token, "signed-token");
                match &request.command {
                    ControlCommand::Publish {
                        topic: actual_topic,
                        classification,
                        payload,
                    } => {
                        assert_eq!(actual_topic, &topic);
                        assert_eq!(classification, &Some(Classification::Internal));
                        let event: TaskEvent =
                            serde_json::from_str(payload).expect("parse task event payload");
                        assert_eq!(event.status, expected_status);
                        assert_eq!(event.task_id, expected_task_id);
                        assert_eq!(event.assignment_id, expected_assignment_id);
                        assert_eq!(event.agent_id.as_deref(), Some(expected_agent_id.as_str()));
                        if let Some(expected_reason) = &reason_contains {
                            assert!(
                                event
                                    .reason
                                    .as_deref()
                                    .is_some_and(|reason| reason.contains(expected_reason)),
                                "expected reason containing `{expected_reason}`, got {:?}",
                                event.reason
                            );
                        }
                    }
                    other => panic!("expected publish request, got {other:?}"),
                }
            }),
            response: Ok(response),
            attachment: None,
        }
    }

    fn expect_get_artifact(artifact_id: &str, bytes: Vec<u8>, sha256: String) -> MockExchange {
        let expected_id = artifact_id.to_owned();
        let response_id = expected_id.clone();
        let byte_length = bytes.len() as u64;
        MockExchange {
            check: Box::new(move |request| match &request.command {
                ControlCommand::GetArtifact { artifact_id } => {
                    assert_eq!(artifact_id, &expected_id);
                }
                other => panic!("expected get artifact request, got {other:?}"),
            }),
            response: Ok(ControlResponse::Artifact {
                artifact: ArtifactMetadata {
                    artifact_id: response_id,
                    content_type: "application/octet-stream".to_owned(),
                    byte_length,
                    sha256,
                    classification: Classification::Internal,
                    retention_class: expressways_protocol::RetentionClass::Operational,
                    created_at: Utc::now(),
                    principal: "local:developer".to_owned(),
                    local_path: Some("/untrusted/server/path.blob".to_owned()),
                },
            }),
            attachment: Some(bytes),
        }
    }

    fn task_work_item(task_id: &str) -> TaskWorkItem {
        TaskWorkItem {
            task_id: task_id.to_owned(),
            task_type: "summarize_document".to_owned(),
            priority: 0,
            requirements: TaskRequirements {
                skill: Some("summarize".to_owned()),
                topic: Some("topic:results".to_owned()),
                principal: None,
                preferred_agents: Vec::new(),
                avoid_agents: Vec::new(),
                required_agent: None,
                affinity_key: None,
            },
            payload: TaskPayload::json(serde_json::json!({ "path": "notes.md" })),
            retry_policy: Default::default(),
            submitted_at: Utc::now(),
        }
    }

    fn assigned_task(task: TaskWorkItem) -> AssignedTask {
        AssignedTask {
            assignment: TaskEvent {
                event_id: Uuid::now_v7(),
                task_id: task.task_id.clone(),
                task_offset: Some(4),
                assignment_id: Some(Uuid::now_v7()),
                agent_id: Some("alpha".to_owned()),
                status: TaskStatus::Assigned,
                attempt: 1,
                reason: None,
                emitted_at: Utc::now(),
            },
            task_message: stored_message(
                TASKS_TOPIC,
                4,
                serde_json::to_string(&task).expect("serialize task"),
            ),
            task,
            hydrated_payload_bytes: None,
        }
    }

    fn stored_message(topic: &str, offset: u64, payload: String) -> StoredMessage {
        StoredMessage {
            message_id: Uuid::now_v7(),
            topic: topic.to_owned(),
            offset,
            timestamp: Utc::now(),
            producer: "local:agent-orchestrator".to_owned(),
            classification: Classification::Internal,
            payload,
        }
    }
}
