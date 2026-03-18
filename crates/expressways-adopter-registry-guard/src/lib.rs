use std::fs;

use expressways_adopter_api::{
    Adopter, AdopterCapability, AdopterContext, AdopterError, AdopterHealth, AdopterManifest,
    AdopterOutcome, load_settings,
};
use serde::Deserialize;

const REGISTRY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize)]
struct RegistryGuardSettings {
    #[serde(default = "default_bootstrap_missing")]
    bootstrap_missing: bool,
}

impl Default for RegistryGuardSettings {
    fn default() -> Self {
        Self {
            bootstrap_missing: default_bootstrap_missing(),
        }
    }
}

fn default_bootstrap_missing() -> bool {
    true
}

#[derive(Debug)]
pub struct RegistryGuardAdopter {
    manifest: AdopterManifest,
    settings: RegistryGuardSettings,
}

impl RegistryGuardAdopter {
    fn new(settings: RegistryGuardSettings) -> Self {
        Self {
            manifest: manifest(),
            settings,
        }
    }
}

pub fn manifest() -> AdopterManifest {
    AdopterManifest {
        id: "registry_guard".to_owned(),
        package: "expressways-adopter-registry-guard".to_owned(),
        description:
            "Checks that the registry document is readable, structurally valid, and bootstrappable."
                .to_owned(),
        capabilities: vec![AdopterCapability::HealthProbe, AdopterCapability::SelfHeal],
        fail_closed: false,
    }
}

pub fn build(settings: Option<&toml::Table>) -> Result<Box<dyn Adopter>, AdopterError> {
    Ok(Box::new(RegistryGuardAdopter::new(load_settings(
        settings,
    )?)))
}

impl Adopter for RegistryGuardAdopter {
    fn manifest(&self) -> &AdopterManifest {
        &self.manifest
    }

    fn inspect(&self, context: &AdopterContext) -> Result<AdopterOutcome, AdopterError> {
        let Some(parent) = context.registry_path.parent() else {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Failed,
                detail: format!(
                    "registry path {} does not have a parent directory",
                    context.registry_path.display()
                ),
            });
        };

        if !parent.exists() {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Degraded,
                detail: format!("registry parent {} is missing", parent.display()),
            });
        }

        if !context.registry_path.exists() {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Degraded,
                detail: format!(
                    "registry document {} is missing",
                    context.registry_path.display()
                ),
            });
        }

        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&context.registry_path)?) //
                .map_err(|error| AdopterError::Message(error.to_string()))?;
        let schema_version = value
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                AdopterError::Message("registry schema_version is missing".to_owned())
            })?;
        if schema_version == 0 {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Failed,
                detail: "registry schema_version must be greater than zero".to_owned(),
            });
        }
        if !value.get("agents").is_some_and(serde_json::Value::is_array) {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Failed,
                detail: "registry agents field must be an array".to_owned(),
            });
        }

        Ok(AdopterOutcome {
            status: AdopterHealth::Healthy,
            detail: format!(
                "registry document {} is structurally valid",
                context.registry_path.display()
            ),
        })
    }

    fn remediate(
        &self,
        context: &AdopterContext,
        outcome: &AdopterOutcome,
    ) -> Result<Option<String>, AdopterError> {
        if outcome.status == AdopterHealth::Failed {
            return Ok(None);
        }

        let Some(parent) = context.registry_path.parent() else {
            return Ok(None);
        };
        fs::create_dir_all(parent)?;

        if self.settings.bootstrap_missing && !context.registry_path.exists() {
            fs::write(
                &context.registry_path,
                serde_json::to_vec_pretty(&serde_json::json!({
                    "schema_version": REGISTRY_SCHEMA_VERSION,
                    "agents": [],
                }))
                .map_err(|error| AdopterError::Message(error.to_string()))?,
            )?;
        }

        Ok(Some(format!(
            "ensured registry parent {} exists and registry bootstrap is available",
            parent.display()
        )))
    }
}
