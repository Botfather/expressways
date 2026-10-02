use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};

use expressways_adopter_api::{
    Adopter, AdopterCapability, AdopterContext, AdopterError, AdopterHealth, AdopterManifest,
    AdopterOutcome, load_settings,
};
use serde::Deserialize;
use uuid::Uuid;

const REGISTRY_SCHEMA_VERSION: u32 = 1;
const MAX_REGISTRY_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_REGISTRY_AGENTS: usize = 10_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

        let raw = read_bounded_registry(&context.registry_path)?;
        let document: RegistryDocument = serde_json::from_slice(&raw)
            .map_err(|error| AdopterError::Message(error.to_string()))?;
        if document.schema_version != REGISTRY_SCHEMA_VERSION {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Failed,
                detail: format!(
                    "registry schema_version {} is not supported; expected {REGISTRY_SCHEMA_VERSION}",
                    document.schema_version
                ),
            });
        }
        if document.agents.len() > MAX_REGISTRY_AGENTS {
            return Ok(AdopterOutcome {
                status: AdopterHealth::Failed,
                detail: format!(
                    "registry contains {} agents; maximum is {MAX_REGISTRY_AGENTS}",
                    document.agents.len()
                ),
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
            let rendered = serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": REGISTRY_SCHEMA_VERSION,
                "agents": [],
            }))
            .map_err(|error| AdopterError::Message(error.to_string()))?;
            install_registry_bootstrap(&context.registry_path, &rendered)?;
        }

        Ok(Some(format!(
            "ensured registry parent {} exists and registry bootstrap is available",
            parent.display()
        )))
    }
}

fn install_registry_bootstrap(path: &std::path::Path, rendered: &[u8]) -> std::io::Result<()> {
    let temp_path = path.with_extension(format!("bootstrap-{}", Uuid::now_v7()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp_path)?;
        file.write_all(rendered)?;
        file.sync_all()?;
        fs::hard_link(&temp_path, path)?;
        fs::remove_file(&temp_path)?;
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp_path);
    }
    result
}

#[derive(Deserialize)]
struct RegistryDocument {
    schema_version: u32,
    agents: Vec<serde_json::Value>,
}

fn read_bounded_registry(path: &std::path::Path) -> Result<Vec<u8>, AdopterError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(AdopterError::Message(format!(
            "registry path {} is not a regular file",
            path.display()
        )));
    }
    if metadata.len() > MAX_REGISTRY_FILE_BYTES {
        return Err(AdopterError::Message(format!(
            "registry document is {} bytes; maximum is {MAX_REGISTRY_FILE_BYTES}",
            metadata.len()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.is_file() || opened_metadata.len() > MAX_REGISTRY_FILE_BYTES {
        return Err(AdopterError::Message(
            "registry document changed to an invalid or oversized file".to_owned(),
        ));
    }
    let mut raw = Vec::with_capacity(opened_metadata.len() as usize);
    file.take(MAX_REGISTRY_FILE_BYTES + 1)
        .read_to_end(&mut raw)?;
    if raw.len() as u64 > MAX_REGISTRY_FILE_BYTES {
        return Err(AdopterError::Message(
            "registry document grew beyond its size limit".to_owned(),
        ));
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn context(path: std::path::PathBuf) -> AdopterContext {
        let parent = path.parent().expect("registry parent").to_path_buf();
        AdopterContext {
            data_dir: parent.clone(),
            audit_path: parent.join("audit.jsonl"),
            registry_path: path,
        }
    }

    fn temp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("registry-guard-{}.json", Uuid::now_v7()))
    }

    #[test]
    fn rejects_oversized_and_future_registry_documents() {
        let path = temp_path();
        File::create(&path)
            .expect("create registry")
            .set_len(MAX_REGISTRY_FILE_BYTES + 1)
            .expect("make sparse registry");
        assert!(build(None).expect("build").inspect(&context(path)).is_err());

        let path = temp_path();
        fs::write(&path, br#"{"schema_version":99,"agents":[]}"#).expect("write registry");
        let outcome = build(None)
            .expect("build")
            .inspect(&context(path))
            .expect("inspect");
        assert_eq!(outcome.status, AdopterHealth::Failed);
    }

    #[test]
    fn bootstrap_creates_private_valid_registry() {
        let path = temp_path();
        let adopter = build(None).expect("build");
        let context = context(path.clone());
        let outcome = adopter.inspect(&context).expect("inspect missing");
        adopter.remediate(&context, &outcome).expect("remediate");
        assert_eq!(
            adopter.inspect(&context).expect("inspect created").status,
            AdopterHealth::Healthy
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).expect("metadata").permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
