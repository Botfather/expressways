use std::path::PathBuf;

use anyhow::Context;
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse, RetentionClass, TopicSpec,
};

use crate::model::{
    DEFAULT_INBOUND_TOPIC, DEFAULT_OUTBOUND_STREAM_TOPIC, DEFAULT_OUTBOUND_TOPIC,
    DEFAULT_RUNTIME_EVENTS_TOPIC, DEFAULT_TOPIC_PREFIX,
};

#[derive(Debug, Clone)]
pub struct CreateSystemConfig {
    pub endpoint: Endpoint,
    pub capability_token: String,
    pub topic_prefix: String,
    pub output_dir: PathBuf,
    pub runtime_principal: String,
    pub bridge_principal: String,
}

#[derive(Debug, Clone)]
pub struct CreateSystemOutput {
    pub inbound_topic: String,
    pub outbound_topic: String,
    pub outbound_stream_topic: String,
    pub runtime_events_topic: String,
    pub summary_path: PathBuf,
    pub snippets_path: PathBuf,
}

pub async fn create_system(config: CreateSystemConfig) -> anyhow::Result<CreateSystemOutput> {
    let prefix = normalize_prefix(&config.topic_prefix);
    let inbound_topic = format!("{prefix}.inbound");
    let outbound_topic = format!("{prefix}.outbound");
    let outbound_stream_topic = format!("{prefix}.outbound.stream");
    let runtime_events_topic = format!("{prefix}.runtime.events");

    ensure_topic(
        &config.endpoint,
        &config.capability_token,
        &inbound_topic,
        RetentionClass::Operational,
        Classification::Internal,
    )
    .await?;
    ensure_topic(
        &config.endpoint,
        &config.capability_token,
        &outbound_topic,
        RetentionClass::Operational,
        Classification::Internal,
    )
    .await?;
    ensure_topic(
        &config.endpoint,
        &config.capability_token,
        &outbound_stream_topic,
        RetentionClass::Operational,
        Classification::Internal,
    )
    .await?;
    ensure_topic(
        &config.endpoint,
        &config.capability_token,
        &runtime_events_topic,
        RetentionClass::Operational,
        Classification::Internal,
    )
    .await?;

    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;

    let summary_path = config.output_dir.join("nanobot-system.toml");
    let snippets_path = config.output_dir.join("nanobot-auth-policy-snippets.toml");
    std::fs::write(
        &summary_path,
        render_summary_file(
            &prefix,
            &inbound_topic,
            &outbound_topic,
            &outbound_stream_topic,
            &runtime_events_topic,
        ),
    )
    .with_context(|| format!("failed to write {}", summary_path.display()))?;
    std::fs::write(
        &snippets_path,
        render_snippets_file(
            &config.runtime_principal,
            &config.bridge_principal,
            &inbound_topic,
            &outbound_topic,
            &outbound_stream_topic,
            &runtime_events_topic,
        ),
    )
    .with_context(|| format!("failed to write {}", snippets_path.display()))?;

    Ok(CreateSystemOutput {
        inbound_topic,
        outbound_topic,
        outbound_stream_topic,
        runtime_events_topic,
        summary_path,
        snippets_path,
    })
}

pub fn default_topics_for_prefix(prefix: &str) -> (String, String, String, String) {
    let prefix = normalize_prefix(prefix);
    (
        format!("{prefix}.inbound"),
        format!("{prefix}.outbound"),
        format!("{prefix}.outbound.stream"),
        format!("{prefix}.runtime.events"),
    )
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim();
    if trimmed.is_empty() {
        return DEFAULT_TOPIC_PREFIX.to_owned();
    }
    trimmed.trim_end_matches('.').to_owned()
}

fn render_summary_file(
    prefix: &str,
    inbound_topic: &str,
    outbound_topic: &str,
    outbound_stream_topic: &str,
    runtime_events_topic: &str,
) -> String {
    format!(
        concat!(
            "schema_version = \"expressways.nanobot-system.v1\"\n",
            "topic_prefix = \"{prefix}\"\n",
            "inbound_topic = \"{inbound}\"\n",
            "outbound_topic = \"{outbound}\"\n",
            "outbound_stream_topic = \"{outbound_stream}\"\n",
            "runtime_events_topic = \"{events}\"\n",
            "\n",
            "# defaults\n",
            "# canonical inbound topic in this repo: {default_inbound}\n",
            "# canonical outbound topic in this repo: {default_outbound}\n",
            "# canonical outbound stream topic in this repo: {default_outbound_stream}\n",
            "# canonical runtime events topic in this repo: {default_events}\n"
        ),
        prefix = prefix,
        inbound = inbound_topic,
        outbound = outbound_topic,
        outbound_stream = outbound_stream_topic,
        events = runtime_events_topic,
        default_inbound = DEFAULT_INBOUND_TOPIC,
        default_outbound = DEFAULT_OUTBOUND_TOPIC,
        default_outbound_stream = DEFAULT_OUTBOUND_STREAM_TOPIC,
        default_events = DEFAULT_RUNTIME_EVENTS_TOPIC,
    )
}

fn render_snippets_file(
    runtime_principal: &str,
    bridge_principal: &str,
    inbound_topic: &str,
    outbound_topic: &str,
    outbound_stream_topic: &str,
    runtime_events_topic: &str,
) -> String {
    format!(
        concat!(
            "# Add these snippets to your server config.\n",
            "# Principals\n",
            "[[auth.principals]]\n",
            "id = \"{runtime_principal}\"\n",
            "kind = \"agent\"\n",
            "display_name = \"Nanobot Runtime\"\n",
            "status = \"active\"\n",
            "allowed_key_ids = [\"dev\"]\n",
            "quota_profile = \"nanobot_runtime\"\n",
            "\n",
            "[[auth.principals]]\n",
            "id = \"{bridge_principal}\"\n",
            "kind = \"service\"\n",
            "display_name = \"Nanobot Channel Bridge\"\n",
            "status = \"active\"\n",
            "allowed_key_ids = [\"dev\"]\n",
            "quota_profile = \"nanobot_bridge\"\n",
            "\n",
            "# Quota profiles\n",
            "[[quotas.profiles]]\n",
            "name = \"nanobot_runtime\"\n",
            "publish_payload_max_bytes = 65536\n",
            "publish_requests_per_window = 30\n",
            "publish_window_seconds = 1\n",
            "consume_max_limit = 100\n",
            "consume_requests_per_window = 30\n",
            "consume_window_seconds = 1\n",
            "backpressure_mode = \"delay\"\n",
            "backpressure_delay_ms = 50\n",
            "\n",
            "[[quotas.profiles]]\n",
            "name = \"nanobot_bridge\"\n",
            "publish_payload_max_bytes = 65536\n",
            "publish_requests_per_window = 60\n",
            "publish_window_seconds = 1\n",
            "consume_max_limit = 200\n",
            "consume_requests_per_window = 60\n",
            "consume_window_seconds = 1\n",
            "backpressure_mode = \"delay\"\n",
            "backpressure_delay_ms = 50\n",
            "\n",
            "# Policy rules\n",
            "[[policy.rules]]\n",
            "principal = \"{runtime_principal}\"\n",
            "resource = \"topic:{inbound_topic}\"\n",
            "actions = [\"consume\", \"admin\"]\n",
            "\n",
            "[[policy.rules]]\n",
            "principal = \"{runtime_principal}\"\n",
            "resource = \"topic:{outbound_topic}\"\n",
            "actions = [\"publish\", \"admin\"]\n",
            "\n",
            "[[policy.rules]]\n",
            "principal = \"{runtime_principal}\"\n",
            "resource = \"topic:{outbound_stream_topic}\"\n",
            "actions = [\"publish\", \"admin\"]\n",
            "\n",
            "[[policy.rules]]\n",
            "principal = \"{runtime_principal}\"\n",
            "resource = \"topic:{runtime_events_topic}\"\n",
            "actions = [\"publish\", \"admin\"]\n",
            "\n",
            "[[policy.rules]]\n",
            "principal = \"{bridge_principal}\"\n",
            "resource = \"topic:{inbound_topic}\"\n",
            "actions = [\"publish\", \"admin\"]\n",
            "\n",
            "[[policy.rules]]\n",
            "principal = \"{bridge_principal}\"\n",
            "resource = \"topic:{outbound_topic}\"\n",
            "actions = [\"consume\", \"admin\"]\n",
            "\n",
            "[[policy.rules]]\n",
            "principal = \"{bridge_principal}\"\n",
            "resource = \"topic:{outbound_stream_topic}\"\n",
            "actions = [\"consume\", \"admin\"]\n",
            "\n",
            "[[policy.rules]]\n",
            "principal = \"{bridge_principal}\"\n",
            "resource = \"topic:{runtime_events_topic}\"\n",
            "actions = [\"consume\", \"admin\"]\n"
        ),
        runtime_principal = runtime_principal,
        bridge_principal = bridge_principal,
        inbound_topic = inbound_topic,
        outbound_topic = outbound_topic,
        outbound_stream_topic = outbound_stream_topic,
        runtime_events_topic = runtime_events_topic,
    )
}

async fn ensure_topic(
    endpoint: &Endpoint,
    capability_token: &str,
    topic: &str,
    retention_class: RetentionClass,
    classification: Classification,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: topic.to_owned(),
                    retention_class,
                    default_classification: classification,
                },
            },
        })
        .await?;
    match response {
        ControlResponse::TopicCreated { .. } => Ok(()),
        ControlResponse::Error { code, message } => {
            anyhow::bail!("failed to create or validate topic `{topic}`: {code}: {message}")
        }
        other => anyhow::bail!("unexpected response while ensuring topic `{topic}`: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_defaults_and_trims() {
        assert_eq!(normalize_prefix(""), DEFAULT_TOPIC_PREFIX);
        assert_eq!(normalize_prefix("nanobot."), "nanobot");
    }

    #[test]
    fn default_topic_derivation() {
        let (inbound, outbound, stream, events) = default_topics_for_prefix("team.nano");
        assert_eq!(inbound, "team.nano.inbound");
        assert_eq!(outbound, "team.nano.outbound");
        assert_eq!(stream, "team.nano.outbound.stream");
        assert_eq!(events, "team.nano.runtime.events");
    }
}
