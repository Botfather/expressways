use std::fmt::{Display, Formatter};
use std::str::FromStr;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BROKER_RESOURCE: &str = "system:broker";
pub const REGISTRY_RESOURCE: &str = "registry:agents";
pub const ARTIFACT_COLLECTION_RESOURCE: &str = "artifact:blobs";
pub const TASKS_TOPIC: &str = "tasks";
pub const TASK_EVENTS_TOPIC: &str = "task_events";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Health,
    Publish,
    Consume,
    Admin,
}

impl Action {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Publish => "publish",
            Self::Consume => "consume",
            Self::Admin => "admin",
        }
    }
}

impl Display for Action {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Action {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "health" => Ok(Self::Health),
            "publish" => Ok(Self::Publish),
            "consume" => Ok(Self::Consume),
            "admin" => Ok(Self::Admin),
            other => Err(format!("unsupported action `{other}`")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

impl Default for Classification {
    fn default() -> Self {
        Self::Internal
    }
}

impl Display for Classification {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Restricted => "restricted",
        })
    }
}

impl FromStr for Classification {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "public" => Ok(Self::Public),
            "internal" => Ok(Self::Internal),
            "confidential" => Ok(Self::Confidential),
            "restricted" => Ok(Self::Restricted),
            other => Err(format!("unsupported classification `{other}`")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetentionClass {
    Ephemeral,
    Operational,
    Regulated,
}

impl Default for RetentionClass {
    fn default() -> Self {
        Self::Operational
    }
}

impl Display for RetentionClass {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Ephemeral => "ephemeral",
            Self::Operational => "operational",
            Self::Regulated => "regulated",
        })
    }
}

impl FromStr for RetentionClass {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ephemeral" => Ok(Self::Ephemeral),
            "operational" => Ok(Self::Operational),
            "regulated" => Ok(Self::Regulated),
            other => Err(format!("unsupported retention class `{other}`")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityScope {
    pub resource: String,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityClaims {
    pub token_id: Uuid,
    pub principal: String,
    #[serde(default = "default_audience")]
    pub audience: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub scopes: Vec<CapabilityScope>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicSpec {
    pub name: String,
    pub retention_class: RetentionClass,
    pub default_classification: Classification,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSchemaRef {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentEndpoint {
    pub transport: String,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRegistration {
    pub agent_id: String,
    pub display_name: String,
    pub version: String,
    pub summary: String,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub subscriptions: Vec<String>,
    #[serde(default)]
    pub publications: Vec<String>,
    #[serde(default)]
    pub schemas: Vec<AgentSchemaRef>,
    pub endpoint: AgentEndpoint,
    #[serde(default)]
    pub classification: Classification,
    #[serde(default)]
    pub retention_class: RetentionClass,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCard {
    pub agent_id: String,
    pub principal: String,
    pub display_name: String,
    pub version: String,
    pub summary: String,
    pub skills: Vec<String>,
    pub subscriptions: Vec<String>,
    pub publications: Vec<String>,
    pub schemas: Vec<AgentSchemaRef>,
    pub endpoint: AgentEndpoint,
    pub classification: Classification,
    pub retention_class: RetentionClass,
    pub ttl_seconds: u64,
    pub updated_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AgentQuery {
    pub skill: Option<String>,
    pub topic: Option<String>,
    pub principal: Option<String>,
    #[serde(default)]
    pub include_stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TaskRequirements {
    pub skill: Option<String>,
    pub topic: Option<String>,
    pub principal: Option<String>,
    #[serde(default)]
    pub preferred_agents: Vec<String>,
    #[serde(default)]
    pub avoid_agents: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRetryPolicy {
    #[serde(default = "default_task_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_task_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_task_retry_delay_seconds")]
    pub retry_delay_seconds: u64,
}

impl Default for TaskRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_task_max_attempts(),
            timeout_seconds: default_task_timeout_seconds(),
            retry_delay_seconds: default_task_retry_delay_seconds(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactMetadata {
    pub artifact_id: String,
    pub content_type: String,
    pub byte_length: u64,
    pub sha256: String,
    #[serde(default)]
    pub classification: Classification,
    #[serde(default)]
    pub retention_class: RetentionClass,
    #[serde(default = "default_timestamp")]
    pub created_at: DateTime<Utc>,
    pub principal: String,
    pub local_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskPayload {
    Json(serde_json::Value),
    Text {
        text: String,
        content_type: String,
    },
    Bytes {
        data_base64: String,
        content_type: String,
        byte_length: Option<usize>,
    },
    FileRef {
        path: String,
        content_type: Option<String>,
        byte_length: Option<u64>,
        sha256: Option<String>,
    },
    ArtifactRef {
        artifact_id: String,
        content_type: Option<String>,
        byte_length: Option<u64>,
        sha256: Option<String>,
        local_path: Option<String>,
    },
}

impl Default for TaskPayload {
    fn default() -> Self {
        Self::Json(serde_json::Value::Null)
    }
}

impl TaskPayload {
    pub fn json(value: serde_json::Value) -> Self {
        Self::Json(value)
    }

    pub fn text(text: impl Into<String>, content_type: impl Into<String>) -> Self {
        Self::Text {
            text: text.into(),
            content_type: content_type.into(),
        }
    }

    pub fn bytes_base64(
        data_base64: impl Into<String>,
        content_type: impl Into<String>,
        byte_length: Option<usize>,
    ) -> Self {
        Self::Bytes {
            data_base64: data_base64.into(),
            content_type: content_type.into(),
            byte_length,
        }
    }

    pub fn bytes(bytes: &[u8], content_type: impl Into<String>) -> Self {
        Self::Bytes {
            data_base64: BASE64_STANDARD.encode(bytes),
            content_type: content_type.into(),
            byte_length: Some(bytes.len()),
        }
    }

    pub fn file_ref(
        path: impl Into<String>,
        content_type: Option<String>,
        byte_length: Option<u64>,
        sha256: Option<String>,
    ) -> Self {
        Self::FileRef {
            path: path.into(),
            content_type,
            byte_length,
            sha256,
        }
    }

    pub fn artifact_ref(
        artifact_id: impl Into<String>,
        content_type: Option<String>,
        byte_length: Option<u64>,
        sha256: Option<String>,
        local_path: Option<String>,
    ) -> Self {
        Self::ArtifactRef {
            artifact_id: artifact_id.into(),
            content_type,
            byte_length,
            sha256,
            local_path,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Json(_) => "json",
            Self::Text { .. } => "text",
            Self::Bytes { .. } => "bytes",
            Self::FileRef { .. } => "file_ref",
            Self::ArtifactRef { .. } => "artifact_ref",
        }
    }

    pub fn content_type(&self) -> Option<&str> {
        match self {
            Self::Json(_) => Some("application/json"),
            Self::Text { content_type, .. } | Self::Bytes { content_type, .. } => {
                Some(content_type.as_str())
            }
            Self::FileRef { content_type, .. } | Self::ArtifactRef { content_type, .. } => {
                content_type.as_deref()
            }
        }
    }

    pub fn json_value(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Json(value) => Some(value),
            _ => None,
        }
    }

    pub fn into_json_value(self) -> Option<serde_json::Value> {
        match self {
            Self::Json(value) => Some(value),
            _ => None,
        }
    }

    pub fn decode_inline_bytes(&self) -> Result<Option<Vec<u8>>, String> {
        match self {
            Self::Bytes { data_base64, .. } => BASE64_STANDARD
                .decode(data_base64)
                .map(Some)
                .map_err(|error| format!("invalid base64 task payload: {error}")),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "_expressways_payload_kind", rename_all = "snake_case")]
enum TaskPayloadEnvelope {
    Text {
        text: String,
        #[serde(default = "default_text_content_type")]
        content_type: String,
    },
    Bytes {
        data_base64: String,
        #[serde(default = "default_binary_content_type")]
        content_type: String,
        byte_length: Option<usize>,
    },
    FileRef {
        path: String,
        content_type: Option<String>,
        byte_length: Option<u64>,
        sha256: Option<String>,
    },
    ArtifactRef {
        artifact_id: String,
        content_type: Option<String>,
        byte_length: Option<u64>,
        sha256: Option<String>,
        local_path: Option<String>,
    },
}

impl Serialize for TaskPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Json(value) => value.serialize(serializer),
            Self::Text { text, content_type } => TaskPayloadEnvelope::Text {
                text: text.clone(),
                content_type: content_type.clone(),
            }
            .serialize(serializer),
            Self::Bytes {
                data_base64,
                content_type,
                byte_length,
            } => TaskPayloadEnvelope::Bytes {
                data_base64: data_base64.clone(),
                content_type: content_type.clone(),
                byte_length: *byte_length,
            }
            .serialize(serializer),
            Self::FileRef {
                path,
                content_type,
                byte_length,
                sha256,
            } => TaskPayloadEnvelope::FileRef {
                path: path.clone(),
                content_type: content_type.clone(),
                byte_length: *byte_length,
                sha256: sha256.clone(),
            }
            .serialize(serializer),
            Self::ArtifactRef {
                artifact_id,
                content_type,
                byte_length,
                sha256,
                local_path,
            } => TaskPayloadEnvelope::ArtifactRef {
                artifact_id: artifact_id.clone(),
                content_type: content_type.clone(),
                byte_length: *byte_length,
                sha256: sha256.clone(),
                local_path: local_path.clone(),
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for TaskPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value
            .as_object()
            .and_then(|object| object.get("_expressways_payload_kind"))
            .and_then(serde_json::Value::as_str)
            .is_some()
        {
            let envelope = serde_json::from_value::<TaskPayloadEnvelope>(value)
                .map_err(serde::de::Error::custom)?;
            return Ok(match envelope {
                TaskPayloadEnvelope::Text { text, content_type } => {
                    Self::Text { text, content_type }
                }
                TaskPayloadEnvelope::Bytes {
                    data_base64,
                    content_type,
                    byte_length,
                } => Self::Bytes {
                    data_base64,
                    content_type,
                    byte_length,
                },
                TaskPayloadEnvelope::FileRef {
                    path,
                    content_type,
                    byte_length,
                    sha256,
                } => Self::FileRef {
                    path,
                    content_type,
                    byte_length,
                    sha256,
                },
                TaskPayloadEnvelope::ArtifactRef {
                    artifact_id,
                    content_type,
                    byte_length,
                    sha256,
                    local_path,
                } => Self::ArtifactRef {
                    artifact_id,
                    content_type,
                    byte_length,
                    sha256,
                    local_path,
                },
            });
        }

        Ok(Self::Json(value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskWorkItem {
    pub task_id: String,
    pub task_type: String,
    #[serde(default = "default_task_priority")]
    pub priority: i32,
    #[serde(default)]
    pub requirements: TaskRequirements,
    #[serde(default)]
    pub payload: TaskPayload,
    #[serde(default)]
    pub retry_policy: TaskRetryPolicy,
    #[serde(default = "default_timestamp")]
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Assigned,
    Completed,
    Failed,
    RetryScheduled,
    TimedOut,
    Exhausted,
    Canceled,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Exhausted | Self::Canceled)
    }
}

impl Display for TaskStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Pending => "pending",
            Self::Assigned => "assigned",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::RetryScheduled => "retry_scheduled",
            Self::TimedOut => "timed_out",
            Self::Exhausted => "exhausted",
            Self::Canceled => "canceled",
        })
    }
}

impl FromStr for TaskStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "assigned" => Ok(Self::Assigned),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "retry_scheduled" => Ok(Self::RetryScheduled),
            "timed_out" => Ok(Self::TimedOut),
            "exhausted" => Ok(Self::Exhausted),
            "canceled" => Ok(Self::Canceled),
            other => Err(format!("unsupported task status `{other}`")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskEvent {
    pub event_id: Uuid,
    pub task_id: String,
    pub task_offset: Option<u64>,
    pub assignment_id: Option<Uuid>,
    pub agent_id: Option<String>,
    pub status: TaskStatus,
    pub attempt: u32,
    pub reason: Option<String>,
    #[serde(default = "default_timestamp")]
    pub emitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RegistryEventKind {
    Registered,
    Heartbeated,
    Removed,
    CleanedUp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryEvent {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub kind: RegistryEventKind,
    pub card: AgentCard,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMessage {
    pub message_id: Uuid,
    pub topic: String,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
    pub producer: String,
    pub classification: Classification,
    pub payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthIssuerView {
    pub key_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthPrincipalView {
    pub id: String,
    pub kind: String,
    pub display_name: String,
    pub status: String,
    pub allowed_key_ids: Vec<String>,
    pub quota_profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthRevocationView {
    pub revoked_tokens: Vec<Uuid>,
    pub revoked_principals: Vec<String>,
    pub revoked_key_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthStateView {
    pub audience: String,
    pub issuers: Vec<AuthIssuerView>,
    pub principals: Vec<AuthPrincipalView>,
    pub revocations: AuthRevocationView,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationMetricsView {
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub average_latency_ms: u64,
    pub max_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageMetricsView {
    pub topic_count: u64,
    pub segment_count: u64,
    pub total_bytes: u64,
    pub reclaimed_segments: u64,
    pub reclaimed_bytes: u64,
    pub recovered_segments: u64,
    pub truncated_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditMetricsView {
    pub event_count: u64,
    pub last_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamMetricsView {
    pub open_streams: u64,
    pub opened_streams: u64,
    pub closed_streams: u64,
    pub keepalives_sent: u64,
    pub event_frames_sent: u64,
    pub events_delivered: u64,
    pub delivery_failures: u64,
    pub slow_consumer_drops: u64,
    pub idle_timeouts: u64,
    pub watch_stream: OperationMetricsView,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdopterStatusView {
    pub id: String,
    pub package: String,
    pub description: String,
    pub enabled: bool,
    pub status: String,
    pub detail: String,
    pub capabilities: Vec<String>,
    pub last_run_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResilienceMetricsView {
    pub service_mode: String,
    pub degraded_components: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerMetricsView {
    pub uptime_seconds: u64,
    pub total_requests: u64,
    pub health_requests: u64,
    pub admin_requests: u64,
    pub auth_failures: u64,
    pub policy_denials: u64,
    pub quota_denials: u64,
    pub storage_failures: u64,
    pub audit_failures: u64,
    pub publish: OperationMetricsView,
    pub consume: OperationMetricsView,
    pub storage: StorageMetricsView,
    pub audit: AuditMetricsView,
    pub streams: StreamMetricsView,
    pub resilience: ResilienceMetricsView,
    pub adopters: Vec<AdopterStatusView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlCommand {
    Health,
    GetAuthState,
    GetMetrics,
    GetAdopters,
    RegisterAgent {
        registration: AgentRegistration,
    },
    HeartbeatAgent {
        agent_id: String,
    },
    ListAgents {
        query: AgentQuery,
    },
    WatchAgents {
        query: AgentQuery,
        cursor: Option<u64>,
        max_events: usize,
        wait_timeout_ms: u64,
    },
    OpenAgentWatchStream {
        query: AgentQuery,
        cursor: Option<u64>,
        max_events: usize,
        wait_timeout_ms: u64,
    },
    CleanupStaleAgents,
    RemoveAgent {
        agent_id: String,
    },
    CreateTopic {
        topic: TopicSpec,
    },
    RevokeToken {
        token_id: Uuid,
    },
    RevokePrincipal {
        principal: String,
    },
    RevokeKey {
        key_id: String,
    },
    PutArtifact {
        artifact_id: Option<String>,
        content_type: String,
        byte_length: u64,
        sha256: Option<String>,
        classification: Option<Classification>,
        retention_class: Option<RetentionClass>,
    },
    GetArtifact {
        artifact_id: String,
    },
    StatArtifact {
        artifact_id: String,
    },
    Publish {
        topic: String,
        classification: Option<Classification>,
        payload: String,
    },
    Consume {
        topic: String,
        offset: u64,
        limit: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControlRequest {
    pub capability_token: String,
    pub command: ControlCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamFrame {
    AgentWatchOpened {
        cursor: u64,
    },
    RegistryEvents {
        events: Vec<RegistryEvent>,
        cursor: u64,
    },
    KeepAlive {
        cursor: u64,
    },
    StreamError {
        code: String,
        message: String,
    },
    StreamClosed {
        cursor: u64,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlResponse {
    Health {
        node_name: String,
        status: String,
    },
    Metrics {
        metrics: BrokerMetricsView,
    },
    Adopters {
        adopters: Vec<AdopterStatusView>,
    },
    AuthState {
        state: AuthStateView,
    },
    AgentRegistered {
        card: AgentCard,
    },
    AgentHeartbeat {
        card: AgentCard,
    },
    Agents {
        agents: Vec<AgentCard>,
        cursor: u64,
    },
    RegistryEvents {
        events: Vec<RegistryEvent>,
        cursor: u64,
        timed_out: bool,
    },
    AgentsCleanedUp {
        removed_agent_ids: Vec<String>,
    },
    AgentRemoved {
        agent_id: String,
    },
    TopicCreated {
        topic: TopicSpec,
    },
    RevocationUpdated {
        revocations: AuthRevocationView,
    },
    ArtifactStored {
        artifact: ArtifactMetadata,
    },
    ArtifactMetadata {
        artifact: ArtifactMetadata,
    },
    Artifact {
        artifact: ArtifactMetadata,
    },
    PublishAccepted {
        message_id: Uuid,
        offset: u64,
        classification: Classification,
    },
    Messages {
        topic: String,
        messages: Vec<StoredMessage>,
        next_offset: u64,
    },
    Error {
        code: String,
        message: String,
    },
}

impl ControlResponse {
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlWireEnvelope {
    Request {
        request: ControlRequest,
        #[serde(default)]
        attachment_length: u64,
    },
    Response {
        response: ControlResponse,
        #[serde(default)]
        attachment_length: u64,
    },
    Stream {
        frame: StreamFrame,
    },
}

impl ControlWireEnvelope {
    pub fn encode_with_attachment(
        &self,
        attachment: Option<&[u8]>,
    ) -> Result<Vec<u8>, serde_json::Error> {
        let attachment_length = attachment.map_or(0usize, |bytes| bytes.len());
        let envelope = match self {
            Self::Request { request, .. } => Self::Request {
                request: request.clone(),
                attachment_length: attachment_length as u64,
            },
            Self::Response { response, .. } => Self::Response {
                response: response.clone(),
                attachment_length: attachment_length as u64,
            },
            Self::Stream { frame } => Self::Stream {
                frame: frame.clone(),
            },
        };

        let header = serde_json::to_vec(&envelope)?;
        let mut packet = Vec::with_capacity(4 + header.len() + attachment_length);
        packet.extend_from_slice(&(header.len() as u32).to_le_bytes());
        packet.extend_from_slice(&header);
        if let Some(attachment) = attachment {
            packet.extend_from_slice(attachment);
        }
        Ok(packet)
    }

    pub fn decode_packet(bytes: &[u8]) -> Result<(Self, Vec<u8>), String> {
        if bytes.len() < 4 {
            return Err("control packet is missing header length prefix".to_owned());
        }

        let header_len = u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .map_err(|_| "failed to decode header length".to_owned())?,
        ) as usize;
        if bytes.len() < 4 + header_len {
            return Err("control packet is truncated".to_owned());
        }

        let header = serde_json::from_slice::<Self>(&bytes[4..4 + header_len])
            .map_err(|error| format!("failed to decode control header: {error}"))?;
        let attachment = bytes[4 + header_len..].to_vec();
        let expected_length = match &header {
            Self::Request {
                attachment_length, ..
            }
            | Self::Response {
                attachment_length, ..
            } => *attachment_length as usize,
            Self::Stream { .. } => 0,
        };
        if attachment.len() != expected_length {
            return Err(format!(
                "control attachment length mismatch: expected {expected_length} bytes, got {}",
                attachment.len()
            ));
        }

        Ok((header, attachment))
    }
}

pub fn topic_resource(name: &str) -> String {
    format!("topic:{name}")
}

pub fn registry_entry_resource(agent_id: &str) -> String {
    format!("registry:agents:{agent_id}")
}

pub fn artifact_resource(artifact_id: &str) -> String {
    format!("artifact:{artifact_id}")
}

fn default_audience() -> String {
    "expressways".to_owned()
}

fn default_task_max_attempts() -> u32 {
    3
}

fn default_task_timeout_seconds() -> u64 {
    300
}

fn default_task_retry_delay_seconds() -> u64 {
    5
}

fn default_task_priority() -> i32 {
    0
}

fn default_text_content_type() -> String {
    "text/plain; charset=utf-8".to_owned()
}

fn default_binary_content_type() -> String {
    "application/octet-stream".to_owned()
}

fn default_timestamp() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_round_trips_as_snake_case() {
        let json = serde_json::to_string(&Action::Publish).expect("serialize action");
        assert_eq!(json, "\"publish\"");

        let action: Action = serde_json::from_str(&json).expect("deserialize action");
        assert_eq!(action, Action::Publish);
    }

    #[test]
    fn task_status_round_trips_as_snake_case() {
        let json = serde_json::to_string(&TaskStatus::Canceled).expect("serialize task status");
        assert_eq!(json, "\"canceled\"");

        let status: TaskStatus = serde_json::from_str(&json).expect("deserialize task status");
        assert_eq!(status, TaskStatus::Canceled);
    }

    #[test]
    fn task_work_item_defaults_scheduler_fields() {
        let task: TaskWorkItem = serde_json::from_value(serde_json::json!({
            "task_id": "task-1",
            "task_type": "summarize_document"
        }))
        .expect("deserialize task work item");

        assert_eq!(task.priority, 0);
        assert!(task.requirements.preferred_agents.is_empty());
        assert!(task.requirements.avoid_agents.is_empty());
        assert_eq!(task.payload, TaskPayload::Json(serde_json::Value::Null));
    }

    #[test]
    fn task_payload_keeps_legacy_json_shape() {
        let task: TaskWorkItem = serde_json::from_value(serde_json::json!({
            "task_id": "task-1",
            "task_type": "summarize_document",
            "payload": { "path": "notes.md" }
        }))
        .expect("deserialize task work item");

        assert_eq!(
            task.payload,
            TaskPayload::Json(serde_json::json!({ "path": "notes.md" }))
        );

        let rendered = serde_json::to_value(&task).expect("serialize task work item");
        assert_eq!(
            rendered["payload"],
            serde_json::json!({ "path": "notes.md" })
        );
    }

    #[test]
    fn task_payload_supports_inline_bytes_and_file_refs() {
        let bytes = TaskPayload::bytes(b"PDF", "application/pdf");
        let bytes_json = serde_json::to_value(&bytes).expect("serialize bytes payload");
        assert_eq!(bytes_json["_expressways_payload_kind"], "bytes");
        assert_eq!(
            bytes.decode_inline_bytes().expect("decode bytes"),
            Some(b"PDF".to_vec())
        );

        let file_ref = TaskPayload::file_ref(
            "./var/agent/incoming/doc.pdf",
            Some("application/pdf".to_owned()),
            Some(1024),
            Some("abc123".to_owned()),
        );
        let file_ref_json = serde_json::to_value(&file_ref).expect("serialize file ref payload");
        assert_eq!(file_ref_json["_expressways_payload_kind"], "file_ref");
        assert_eq!(file_ref_json["path"], "./var/agent/incoming/doc.pdf");

        let artifact_ref = TaskPayload::artifact_ref(
            "artifact-1",
            Some("application/pdf".to_owned()),
            Some(2048),
            Some("def456".to_owned()),
            Some("./var/data/artifacts/blobs/artifact-1.blob".to_owned()),
        );
        let artifact_ref_json =
            serde_json::to_value(&artifact_ref).expect("serialize artifact ref payload");
        assert_eq!(
            artifact_ref_json["_expressways_payload_kind"],
            "artifact_ref"
        );
        assert_eq!(artifact_ref_json["artifact_id"], "artifact-1");
        assert_eq!(artifact_ref.content_type(), Some("application/pdf"));
    }

    #[test]
    fn control_wire_envelope_round_trips_attachment() {
        let packet = ControlWireEnvelope::Request {
            request: ControlRequest {
                capability_token: "signed-token".to_owned(),
                command: ControlCommand::PutArtifact {
                    artifact_id: Some("artifact-1".to_owned()),
                    content_type: "application/pdf".to_owned(),
                    byte_length: 3,
                    sha256: None,
                    classification: Some(Classification::Internal),
                    retention_class: Some(RetentionClass::Operational),
                },
            },
            attachment_length: 0,
        }
        .encode_with_attachment(Some(b"PDF"))
        .expect("encode wire packet");

        let (decoded, attachment) =
            ControlWireEnvelope::decode_packet(&packet).expect("decode wire packet");
        assert_eq!(attachment, b"PDF");

        match decoded {
            ControlWireEnvelope::Request {
                request,
                attachment_length,
            } => {
                assert_eq!(attachment_length, 3);
                match request.command {
                    ControlCommand::PutArtifact { byte_length, .. } => {
                        assert_eq!(byte_length, 3);
                    }
                    other => panic!("expected put-artifact command, got {other:?}"),
                }
            }
            other => panic!("expected request packet, got {other:?}"),
        }
    }
}
