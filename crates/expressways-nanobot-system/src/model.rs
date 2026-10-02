use chrono::{DateTime, Utc};
use expressways_protocol::InteropChatContent;
use serde::{Deserialize, Serialize};

pub const SYSTEM_SCHEMA_VERSION: &str = "nanobot.expressways.v1";
pub const DEFAULT_TOPIC_PREFIX: &str = "nanobot";
pub const DEFAULT_INBOUND_TOPIC: &str = "nanobot.inbound";
pub const DEFAULT_OUTBOUND_TOPIC: &str = "nanobot.outbound";
pub const DEFAULT_OUTBOUND_STREAM_TOPIC: &str = "nanobot.outbound.stream";
pub const DEFAULT_RUNTIME_EVENTS_TOPIC: &str = "nanobot.runtime.events";
pub const DEFAULT_RUNTIME_AGENT_ID: &str = "nanobot-runtime";
pub const DEFAULT_RUNTIME_INSTANCE_ID: &str = "nanobot-instance-1";
pub const DEFAULT_RUNTIME_SOURCE: &str = "expressways-nanobot-system";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NanobotSessionRef {
    pub session_id: String,
    pub channel: String,
    pub account_id: String,
    pub sender_id: String,
    #[serde(default)]
    pub sender_display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct NanobotAttachmentRef {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub byte_length: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct NanobotMessageRef {
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub attachments: Vec<NanobotAttachmentRef>,
    #[serde(default)]
    pub content: Vec<InteropChatContent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NanobotInboundEnvelope {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default = "default_runtime_source")]
    pub source_runtime: String,
    #[serde(default = "default_instance_id")]
    pub instance_id: String,
    pub session: NanobotSessionRef,
    pub message: NanobotMessageRef,
    #[serde(default = "default_json_object")]
    pub metadata: serde_json::Value,
    #[serde(default = "default_timestamp")]
    pub received_at: DateTime<Utc>,
}

impl NanobotInboundEnvelope {
    pub fn normalized(mut self) -> Self {
        if self.schema_version.trim().is_empty() {
            self.schema_version = default_schema_version();
        }
        if self.source_runtime.trim().is_empty() {
            self.source_runtime = default_runtime_source();
        }
        if self.instance_id.trim().is_empty() {
            self.instance_id = default_instance_id();
        }
        if self.message.role.is_none() {
            self.message.role = Some("user".to_owned());
        }
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NanobotOutboundEnvelope {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default = "default_runtime_source")]
    pub source_runtime: String,
    pub instance_id: String,
    pub session: NanobotSessionRef,
    pub message: NanobotMessageRef,
    #[serde(default)]
    pub in_reply_to_message_id: Option<String>,
    #[serde(default = "default_json_object")]
    pub metadata: serde_json::Value,
    #[serde(default = "default_timestamp")]
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NanobotStreamChunkEnvelope {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default = "default_runtime_source")]
    pub source_runtime: String,
    pub instance_id: String,
    pub session: NanobotSessionRef,
    pub stream_id: String,
    #[serde(default)]
    pub in_reply_to_message_id: Option<String>,
    pub chunk_index: u64,
    pub text_delta: String,
    pub done: bool,
    #[serde(default = "default_json_object")]
    pub metadata: serde_json::Value,
    #[serde(default = "default_timestamp")]
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NanobotRuntimeEvent {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default = "default_runtime_source")]
    pub source_runtime: String,
    pub instance_id: String,
    pub event: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default = "default_json_object")]
    pub detail: serde_json::Value,
    #[serde(default = "default_timestamp")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionRole {
    User,
    Assistant,
    ToolCall,
    ToolResult,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionTurn {
    #[serde(default = "default_timestamp")]
    pub timestamp: DateTime<Utc>,
    pub role: SessionRole,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_args: Option<serde_json::Value>,
    #[serde(default)]
    pub tool_result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryEntry {
    #[serde(default = "default_timestamp")]
    pub timestamp: DateTime<Utc>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RuntimeState {
    #[serde(default)]
    pub inbound_offset: u64,
    #[serde(default)]
    pub outbound_offset: u64,
}

pub fn default_schema_version() -> String {
    SYSTEM_SCHEMA_VERSION.to_owned()
}

pub fn default_runtime_source() -> String {
    DEFAULT_RUNTIME_SOURCE.to_owned()
}

pub fn default_instance_id() -> String {
    DEFAULT_RUNTIME_INSTANCE_ID.to_owned()
}

pub fn default_timestamp() -> DateTime<Utc> {
    Utc::now()
}

pub fn default_json_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}
