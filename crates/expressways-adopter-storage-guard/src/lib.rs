use std::fs::{self, OpenOptions};
use std::io::Write;

use expressways_adopter_api::{
    Adopter, AdopterCapability, AdopterContext, AdopterError, AdopterHealth, AdopterManifest,
    AdopterOutcome, load_settings,
};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct StorageGuardSettings {
    #[serde(default = "default_probe_filename")]
    probe_filename: String,
}

impl Default for StorageGuardSettings {
    fn default() -> Self {
        Self {
            probe_filename: default_probe_filename(),
        }
    }
}

fn default_probe_filename() -> String {
    ".expressways-storage-guard".to_owned()
}

#[derive(Debug)]
pub struct StorageGuardAdopter {
    manifest: AdopterManifest,
    settings: StorageGuardSettings,
}

impl StorageGuardAdopter {
    fn new(settings: StorageGuardSettings) -> Self {
        Self {
            manifest: manifest(),
            settings,
        }
    }
}

pub fn manifest() -> AdopterManifest {
    AdopterManifest {
        id: "storage_guard".to_owned(),
        package: "expressways-adopter-storage-guard".to_owned(),
        description: "Validates that the broker data directory exists, is a directory, and accepts write probes."
            .to_owned(),
        capabilities: vec![AdopterCapability::HealthProbe, AdopterCapability::SelfHeal],
        fail_closed: true,
    }
}

pub fn build(settings: Option<&toml::Table>) -> Result<Box<dyn Adopter>, AdopterError> {
    Ok(Box::new(StorageGuardAdopter::new(load_settings(settings)?)))
}

impl Adopter for StorageGuardAdopter {
    fn manifest(&self) -> &AdopterManifest {
        &self.manifest
    }

    fn inspect(&self, context: &AdopterContext) -> Result<AdopterOutcome, AdopterError> {
        if context.data_dir.exists() && !context.data_dir.is_dir() {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Failed,
                detail: format!(
                    "storage path {} exists but is not a directory",
                    context.data_dir.display()
                ),
            });
        }

        if !context.data_dir.exists() {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Degraded,
                detail: format!(
                    "storage directory {} is missing",
                    context.data_dir.display()
                ),
            });
        }

        let probe_path = context.data_dir.join(&self.settings.probe_filename);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&probe_path)?;
        file.write_all(b"expressways-storage-guard")?;
        file.sync_all()?;
        drop(file);
        fs::remove_file(&probe_path)?;

        Ok(AdopterOutcome {
            status: AdopterHealth::Healthy,
            detail: format!(
                "storage directory {} passed write probe",
                context.data_dir.display()
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

        fs::create_dir_all(&context.data_dir)?;
        Ok(Some(format!(
            "ensured storage directory {} exists",
            context.data_dir.display()
        )))
    }
}
