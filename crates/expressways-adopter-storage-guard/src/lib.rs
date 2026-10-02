use std::fs::{self, OpenOptions};
use std::io::Write;

use expressways_adopter_api::{
    Adopter, AdopterCapability, AdopterContext, AdopterError, AdopterHealth, AdopterManifest,
    AdopterOutcome, load_settings,
};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
    let settings: StorageGuardSettings = load_settings(settings)?;
    validate_probe_filename(&settings.probe_filename)?;
    Ok(Box::new(StorageGuardAdopter::new(settings)))
}

impl Adopter for StorageGuardAdopter {
    fn manifest(&self) -> &AdopterManifest {
        &self.manifest
    }

    fn inspect(&self, context: &AdopterContext) -> Result<AdopterOutcome, AdopterError> {
        if context.data_dir.exists()
            && !fs::symlink_metadata(&context.data_dir)?
                .file_type()
                .is_dir()
        {
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

        let probe_path = context.data_dir.join(format!(
            "{}-{}",
            self.settings.probe_filename,
            Uuid::now_v7()
        ));
        let probe_result = (|| -> std::io::Result<()> {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&probe_path)?;
            file.write_all(b"expressways-storage-guard")?;
            file.sync_all()?;
            drop(file);
            fs::remove_file(&probe_path)
        })();
        if probe_result.is_err() {
            let _ = fs::remove_file(&probe_path);
        }
        probe_result?;

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

fn validate_probe_filename(value: &str) -> Result<(), AdopterError> {
    let path = std::path::Path::new(value);
    if value.is_empty()
        || value.len() > 128
        || path.components().count() != 1
        || !matches!(
            path.components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(AdopterError::InvalidSettings(
            "probe_filename must be a single filename of at most 128 bytes".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(data_dir: std::path::PathBuf) -> AdopterContext {
        AdopterContext {
            audit_path: data_dir.join("audit.jsonl"),
            registry_path: data_dir.join("agents.json"),
            data_dir,
        }
    }

    #[test]
    fn rejects_probe_path_traversal() {
        let mut settings = toml::Table::new();
        settings.insert(
            "probe_filename".to_owned(),
            toml::Value::String("../victim".to_owned()),
        );
        assert!(build(Some(&settings)).is_err());
    }

    #[test]
    fn probe_does_not_overwrite_existing_files() {
        let root = std::env::temp_dir().join(format!("storage-guard-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create root");
        let sentinel = root.join(".expressways-storage-guard");
        fs::write(&sentinel, b"sentinel").expect("write sentinel");
        let adopter = build(None).expect("build adopter");
        assert_eq!(
            adopter.inspect(&context(root)).expect("inspect").status,
            AdopterHealth::Healthy
        );
        assert_eq!(fs::read(sentinel).expect("read sentinel"), b"sentinel");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_storage_directory() {
        use std::os::unix::fs::symlink;
        let outside =
            std::env::temp_dir().join(format!("storage-guard-outside-{}", Uuid::now_v7()));
        fs::create_dir_all(&outside).expect("create outside");
        let link = std::env::temp_dir().join(format!("storage-guard-link-{}", Uuid::now_v7()));
        symlink(outside, &link).expect("create symlink");
        let outcome = build(None)
            .expect("build adopter")
            .inspect(&context(link))
            .expect("inspect");
        assert_eq!(outcome.status, AdopterHealth::Failed);
    }
}
