use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct QuotaConfig {
    #[serde(default)]
    pub profiles: Vec<QuotaProfile>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuotaProfile {
    pub name: String,
    pub publish_payload_max_bytes: usize,
    pub publish_requests_per_window: u32,
    pub publish_window_seconds: u64,
    pub consume_max_limit: usize,
    pub consume_requests_per_window: u32,
    pub consume_window_seconds: u64,
    #[serde(default)]
    pub backpressure_mode: BackpressureMode,
    #[serde(default)]
    pub backpressure_delay_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureMode {
    Reject,
    Delay,
}

impl Default for BackpressureMode {
    fn default() -> Self {
        Self::Reject
    }
}

#[derive(Debug)]
pub struct QuotaManager {
    profiles: HashMap<String, QuotaProfile>,
    usage: Mutex<HashMap<QuotaBucket, RateWindow>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QuotaBucket {
    principal: String,
    operation: QuotaOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum QuotaOperation {
    Publish,
    Consume,
}

impl QuotaOperation {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Consume => "consume",
        }
    }
}

#[derive(Debug, Clone)]
struct RateWindow {
    started_at: Instant,
    count: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum QuotaError {
    #[error("quota profile `{0}` is not configured")]
    UnknownProfile(String),
    #[error(
        "principal `{principal}` attempted to publish {requested_bytes} bytes above the {max_bytes}-byte limit for quota profile `{profile}`"
    )]
    PublishPayloadTooLarge {
        principal: String,
        profile: String,
        requested_bytes: usize,
        max_bytes: usize,
    },
    #[error(
        "principal `{principal}` requested consume limit {requested_limit} above the max {max_limit} for quota profile `{profile}`"
    )]
    ConsumeLimitTooLarge {
        principal: String,
        profile: String,
        requested_limit: usize,
        max_limit: usize,
    },
    #[error(
        "principal `{principal}` exceeded the {operation} rate limit for quota profile `{profile}`"
    )]
    RateExceeded {
        principal: String,
        profile: String,
        operation: String,
    },
}

enum RateDecision {
    Allow,
    Delay(Duration),
}

impl QuotaManager {
    pub fn new(config: QuotaConfig) -> anyhow::Result<Self> {
        if config.profiles.is_empty() {
            anyhow::bail!("at least one quota profile is required");
        }

        let mut profiles = HashMap::new();
        for profile in config.profiles {
            validate_profile(&profile)?;
            let name = profile.name.clone();
            if profiles.insert(name.clone(), profile).is_some() {
                anyhow::bail!("duplicate quota profile `{name}`");
            }
        }

        Ok(Self {
            profiles,
            usage: Mutex::new(HashMap::new()),
        })
    }

    pub fn validate_principals<'a>(
        &self,
        principal_profiles: impl IntoIterator<Item = &'a str>,
    ) -> anyhow::Result<()> {
        for profile in principal_profiles {
            if !self.profiles.contains_key(profile) {
                anyhow::bail!("principal references unknown quota profile `{profile}`");
            }
        }

        Ok(())
    }

    pub async fn enforce_publish(
        &self,
        principal: &str,
        profile_name: &str,
        payload_bytes: usize,
    ) -> Result<(), QuotaError> {
        let profile = self.profile(profile_name)?;
        if payload_bytes > profile.publish_payload_max_bytes {
            return Err(QuotaError::PublishPayloadTooLarge {
                principal: principal.to_owned(),
                profile: profile.name.clone(),
                requested_bytes: payload_bytes,
                max_bytes: profile.publish_payload_max_bytes,
            });
        }

        self.enforce_rate(principal, &profile, QuotaOperation::Publish)
            .await
    }

    pub async fn enforce_consume(
        &self,
        principal: &str,
        profile_name: &str,
        limit: usize,
    ) -> Result<(), QuotaError> {
        let profile = self.profile(profile_name)?;
        if limit > profile.consume_max_limit {
            return Err(QuotaError::ConsumeLimitTooLarge {
                principal: principal.to_owned(),
                profile: profile.name.clone(),
                requested_limit: limit,
                max_limit: profile.consume_max_limit,
            });
        }

        self.enforce_rate(principal, &profile, QuotaOperation::Consume)
            .await
    }

    fn profile(&self, profile_name: &str) -> Result<QuotaProfile, QuotaError> {
        self.profiles
            .get(profile_name)
            .cloned()
            .ok_or_else(|| QuotaError::UnknownProfile(profile_name.to_owned()))
    }

    async fn enforce_rate(
        &self,
        principal: &str,
        profile: &QuotaProfile,
        operation: QuotaOperation,
    ) -> Result<(), QuotaError> {
        match self.check_rate(principal, profile, operation)? {
            RateDecision::Allow => Ok(()),
            RateDecision::Delay(delay) => {
                tokio::time::sleep(delay).await;
                match self.check_rate(principal, profile, operation)? {
                    RateDecision::Allow => Ok(()),
                    RateDecision::Delay(_) => Err(rate_error(principal, profile, operation)),
                }
            }
        }
    }

    fn check_rate(
        &self,
        principal: &str,
        profile: &QuotaProfile,
        operation: QuotaOperation,
    ) -> Result<RateDecision, QuotaError> {
        let mut usage = self.usage.lock().expect("quota usage lock");
        let key = QuotaBucket {
            principal: principal.to_owned(),
            operation,
        };
        let now = Instant::now();
        let window = rate_window(profile, operation);
        let max_requests = rate_limit(profile, operation);

        let entry = usage.entry(key).or_insert_with(|| RateWindow {
            started_at: now,
            count: 0,
        });

        if now.duration_since(entry.started_at) >= window {
            entry.started_at = now;
            entry.count = 0;
        }

        if entry.count < max_requests {
            entry.count += 1;
            return Ok(RateDecision::Allow);
        }

        match profile.backpressure_mode {
            BackpressureMode::Reject => Err(rate_error(principal, profile, operation)),
            BackpressureMode::Delay => {
                let elapsed = now.duration_since(entry.started_at);
                let remaining_window = window.saturating_sub(elapsed);
                let configured_delay = Duration::from_millis(profile.backpressure_delay_ms.max(1));
                Ok(RateDecision::Delay(remaining_window.max(configured_delay)))
            }
        }
    }
}

fn validate_profile(profile: &QuotaProfile) -> anyhow::Result<()> {
    if profile.name.trim().is_empty() {
        anyhow::bail!("quota profile names must not be empty");
    }
    if profile.publish_payload_max_bytes == 0 {
        anyhow::bail!(
            "quota profile `{}` must allow at least one publish byte",
            profile.name
        );
    }
    if profile.publish_requests_per_window == 0 {
        anyhow::bail!(
            "quota profile `{}` must allow at least one publish request per window",
            profile.name
        );
    }
    if profile.publish_window_seconds == 0 {
        anyhow::bail!(
            "quota profile `{}` publish window must be at least one second",
            profile.name
        );
    }
    if profile.consume_max_limit == 0 {
        anyhow::bail!(
            "quota profile `{}` must allow at least one consumed message",
            profile.name
        );
    }
    if profile.consume_requests_per_window == 0 {
        anyhow::bail!(
            "quota profile `{}` must allow at least one consume request per window",
            profile.name
        );
    }
    if profile.consume_window_seconds == 0 {
        anyhow::bail!(
            "quota profile `{}` consume window must be at least one second",
            profile.name
        );
    }

    Ok(())
}

fn rate_limit(profile: &QuotaProfile, operation: QuotaOperation) -> u32 {
    match operation {
        QuotaOperation::Publish => profile.publish_requests_per_window,
        QuotaOperation::Consume => profile.consume_requests_per_window,
    }
}

fn rate_window(profile: &QuotaProfile, operation: QuotaOperation) -> Duration {
    match operation {
        QuotaOperation::Publish => Duration::from_secs(profile.publish_window_seconds),
        QuotaOperation::Consume => Duration::from_secs(profile.consume_window_seconds),
    }
}

fn rate_error(principal: &str, profile: &QuotaProfile, operation: QuotaOperation) -> QuotaError {
    QuotaError::RateExceeded {
        principal: principal.to_owned(),
        profile: profile.name.clone(),
        operation: operation.as_str().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(mode: BackpressureMode) -> QuotaProfile {
        QuotaProfile {
            name: "default".to_owned(),
            publish_payload_max_bytes: 8,
            publish_requests_per_window: 1,
            publish_window_seconds: 1,
            consume_max_limit: 5,
            consume_requests_per_window: 1,
            consume_window_seconds: 1,
            backpressure_mode: mode,
            backpressure_delay_ms: 5,
        }
    }

    fn manager(mode: BackpressureMode) -> QuotaManager {
        QuotaManager::new(QuotaConfig {
            profiles: vec![profile(mode)],
        })
        .expect("create quota manager")
    }

    #[tokio::test]
    async fn publish_payload_limit_is_enforced() {
        let manager = manager(BackpressureMode::Reject);
        let error = manager
            .enforce_publish("local:agent-alpha", "default", 12)
            .await
            .expect_err("payload should be rejected");

        assert!(matches!(error, QuotaError::PublishPayloadTooLarge { .. }));
    }

    #[tokio::test]
    async fn consume_limit_is_enforced() {
        let manager = manager(BackpressureMode::Reject);
        let error = manager
            .enforce_consume("local:agent-alpha", "default", 12)
            .await
            .expect_err("consume batch should be rejected");

        assert!(matches!(error, QuotaError::ConsumeLimitTooLarge { .. }));
    }

    #[tokio::test]
    async fn reject_mode_denies_second_request_in_same_window() {
        let manager = manager(BackpressureMode::Reject);
        manager
            .enforce_publish("local:agent-alpha", "default", 4)
            .await
            .expect("first publish should pass");

        let error = manager
            .enforce_publish("local:agent-alpha", "default", 4)
            .await
            .expect_err("second publish should be rate limited");

        assert!(matches!(error, QuotaError::RateExceeded { .. }));
    }

    #[tokio::test]
    async fn delay_mode_waits_for_window_reset() {
        let mut delayed_profile = profile(BackpressureMode::Delay);
        delayed_profile.publish_window_seconds = 1;
        delayed_profile.backpressure_delay_ms = 1_100;
        let manager = QuotaManager::new(QuotaConfig {
            profiles: vec![delayed_profile],
        })
        .expect("create quota manager");

        manager
            .enforce_publish("local:agent-alpha", "default", 4)
            .await
            .expect("first publish should pass");
        manager
            .enforce_publish("local:agent-alpha", "default", 4)
            .await
            .expect("delayed publish should pass after window reset");
    }

    #[test]
    fn validate_principals_rejects_unknown_profile() {
        let manager = manager(BackpressureMode::Reject);
        let error = manager
            .validate_principals(["missing"])
            .expect_err("missing profile should fail");

        assert!(
            error
                .to_string()
                .contains("principal references unknown quota profile")
        );
    }
}
