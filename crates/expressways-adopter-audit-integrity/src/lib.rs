use std::fs::{self, OpenOptions};

use expressways_adopter_api::{
    Adopter, AdopterCapability, AdopterContext, AdopterError, AdopterHealth, AdopterManifest,
    AdopterOutcome, load_settings,
};
use expressways_audit::{AuditError, verify_file};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct AuditIntegritySettings {
    #[serde(default = "default_verify_chain")]
    verify_chain: bool,
    #[serde(default = "default_precreate_file")]
    precreate_file: bool,
}

impl Default for AuditIntegritySettings {
    fn default() -> Self {
        Self {
            verify_chain: default_verify_chain(),
            precreate_file: default_precreate_file(),
        }
    }
}

fn default_verify_chain() -> bool {
    true
}

fn default_precreate_file() -> bool {
    true
}

#[derive(Debug)]
pub struct AuditIntegrityAdopter {
    manifest: AdopterManifest,
    settings: AuditIntegritySettings,
}

impl AuditIntegrityAdopter {
    fn new(settings: AuditIntegritySettings) -> Self {
        Self {
            manifest: manifest(),
            settings,
        }
    }
}

pub fn manifest() -> AdopterManifest {
    AdopterManifest {
        id: "audit_integrity".to_owned(),
        package: "expressways-adopter-audit-integrity".to_owned(),
        description:
            "Verifies the audit chain and preflights appendability for the broker audit trail."
                .to_owned(),
        capabilities: vec![
            AdopterCapability::HealthProbe,
            AdopterCapability::IntegrityCheck,
            AdopterCapability::SelfHeal,
        ],
        fail_closed: true,
    }
}

pub fn build(settings: Option<&toml::Table>) -> Result<Box<dyn Adopter>, AdopterError> {
    Ok(Box::new(AuditIntegrityAdopter::new(load_settings(
        settings,
    )?)))
}

impl Adopter for AuditIntegrityAdopter {
    fn manifest(&self) -> &AdopterManifest {
        &self.manifest
    }

    fn inspect(&self, context: &AdopterContext) -> Result<AdopterOutcome, AdopterError> {
        let Some(parent) = context.audit_path.parent() else {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Failed,
                detail: format!(
                    "audit path {} does not have a parent directory",
                    context.audit_path.display()
                ),
            });
        };

        if !parent.exists() {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Degraded,
                detail: format!("audit parent {} is missing", parent.display()),
            });
        }

        let mut options = OpenOptions::new();
        options.create(self.settings.precreate_file).append(true);
        options.open(&context.audit_path)?;

        if self.settings.verify_chain && context.audit_path.exists() {
            match verify_file(&context.audit_path) {
                Ok(_) => {}
                Err(AuditError::Integrity(error)) => {
                    return Ok(AdopterOutcome {
                        status: AdopterHealth::Failed,
                        detail: format!("audit chain verification failed: {error}"),
                    });
                }
                Err(error) => return Err(AdopterError::Message(error.to_string())),
            }
        }

        Ok(AdopterOutcome {
            status: AdopterHealth::Healthy,
            detail: format!("audit path {} is writable", context.audit_path.display()),
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

        let Some(parent) = context.audit_path.parent() else {
            return Ok(None);
        };

        fs::create_dir_all(parent)?;
        if self.settings.precreate_file {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&context.audit_path)?;
        }

        Ok(Some(format!(
            "ensured audit parent {} and audit file path {} exist",
            parent.display(),
            context.audit_path.display()
        )))
    }
}
