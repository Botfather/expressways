use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdopterCapability {
    HealthProbe,
    SelfHeal,
    IntegrityCheck,
}

impl AdopterCapability {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::HealthProbe => "health_probe",
            Self::SelfHeal => "self_heal",
            Self::IntegrityCheck => "integrity_check",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdopterManifest {
    pub id: String,
    pub package: String,
    pub description: String,
    pub capabilities: Vec<AdopterCapability>,
    pub fail_closed: bool,
}

#[derive(Debug, Clone)]
pub struct AdopterContext {
    pub data_dir: PathBuf,
    pub audit_path: PathBuf,
    pub registry_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdopterHealth {
    Healthy,
    Degraded,
    Failed,
}

impl AdopterHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdopterOutcome {
    pub status: AdopterHealth,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdopterSnapshot {
    pub id: String,
    pub package: String,
    pub description: String,
    pub enabled: bool,
    pub status: String,
    pub detail: String,
    pub capabilities: Vec<String>,
    pub last_run_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error)]
pub enum AdopterError {
    #[error("invalid adopter settings: {0}")]
    InvalidSettings(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("adopter failure: {0}")]
    Message(String),
}

pub trait Adopter: Send + Sync {
    fn manifest(&self) -> &AdopterManifest;
    fn inspect(&self, context: &AdopterContext) -> Result<AdopterOutcome, AdopterError>;

    fn remediate(
        &self,
        _context: &AdopterContext,
        _outcome: &AdopterOutcome,
    ) -> Result<Option<String>, AdopterError> {
        Ok(None)
    }
}

pub fn load_settings<T>(settings: Option<&toml::Table>) -> Result<T, AdopterError>
where
    T: serde::de::DeserializeOwned + Default,
{
    let Some(settings) = settings else {
        return Ok(T::default());
    };

    let value = toml::Value::Table(settings.clone());
    value
        .try_into()
        .map_err(|error: toml::de::Error| AdopterError::InvalidSettings(error.to_string()))
}
