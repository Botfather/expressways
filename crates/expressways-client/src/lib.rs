#[cfg(test)]
use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse, ControlWireEnvelope,
    StoredMessage, StreamFrame, TASK_EVENTS_TOPIC, TASKS_TOPIC, TaskEvent, TaskPayload, TaskStatus,
    TaskWorkItem,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
            return tokio::fs::read(&file_ref.path).await.map_err(|source| {
                PayloadAccessError::Io {
                    task_id: self.task.task_id.clone(),
                    path: file_ref.path,
                    source,
                }
            });
        }
        if let Some(artifact_ref) = self.payload_artifact_ref() {
            let path =
                artifact_ref
                    .local_path
                    .ok_or_else(|| PayloadAccessError::MissingArtifactPath {
                        task_id: self.task.task_id.clone(),
                        artifact_id: artifact_ref.artifact_id,
                    })?;
            return tokio::fs::read(&path)
                .await
                .map_err(|source| PayloadAccessError::Io {
                    task_id: self.task.task_id.clone(),
                    path,
                    source,
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
    #[error("task `{task_id}` references artifact `{artifact_id}` without a local path")]
    MissingArtifactPath {
        task_id: String,
        artifact_id: String,
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
                    transport: Transport::Tcp(Framed::new(stream, LengthDelimitedCodec::new())),
                })
            }
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let stream = UnixStream::connect(path).await?;
                Ok(Self {
                    transport: Transport::Unix(Framed::new(stream, LengthDelimitedCodec::new())),
                })
            }
            Endpoint::Custom(custom) => {
                let stream = (custom.connector)().await?;
                Ok(Self {
                    transport: Transport::Custom(Framed::new(stream, LengthDelimitedCodec::new())),
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

        Ok(AssignedTask {
            assignment: assignment.clone(),
            task_message,
            task,
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
            .expect("execution context lock")
            .clone()
    }

    fn invalidate(&self, invalidation: TaskInvalidation) {
        let mut slot = self.invalidation.lock().expect("execution context lock");
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
    async fn assigned_task_helpers_read_inline_and_file_payload_bytes() {
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

        let path = std::env::temp_dir().join(format!("expressways-payload-{}.bin", Uuid::now_v7()));
        std::fs::write(&path, b"PDF").expect("write payload file");
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
        assert_eq!(
            file_assigned.read_payload_bytes().await.expect("read file"),
            b"PDF".to_vec()
        );

        let _ = std::fs::remove_file(file_ref.path);
    }

    #[tokio::test]
    async fn assigned_task_helpers_read_artifact_ref_payload_bytes() {
        let path =
            std::env::temp_dir().join(format!("expressways-artifact-{}.bin", Uuid::now_v7()));
        std::fs::write(&path, b"PROTO").expect("write artifact file");
        let mut task = task_work_item("task-artifact");
        task.payload = TaskPayload::artifact_ref(
            "artifact-1",
            Some("application/x-protobuf".to_owned()),
            Some(5),
            Some("abc123".to_owned()),
            Some(path.display().to_string()),
        );
        let assigned = assigned_task(task);

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

        let _ = std::fs::remove_file(path);
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
