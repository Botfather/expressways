use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::Utc;
use expressways_adopter_api::{
    Adopter, AdopterContext, AdopterHealth, AdopterManifest, AdopterOutcome, AdopterSnapshot,
};
use expressways_protocol::AdopterStatusView;
use tracing::{info, warn};

use crate::ServiceModeTracker;
use crate::config::AppConfig;

type AdopterFactoryFn =
    fn(Option<&toml::Table>) -> Result<Box<dyn Adopter>, expressways_adopter_api::AdopterError>;

#[derive(Clone)]
struct AdopterFactory {
    manifest: AdopterManifest,
    build: AdopterFactoryFn,
}

struct InstalledAdopter {
    manifest: AdopterManifest,
    adopter: Box<dyn Adopter>,
}

pub struct AdopterManager {
    context: AdopterContext,
    probe_interval: Duration,
    statuses: Mutex<BTreeMap<String, AdopterStatusView>>,
    installed: Vec<InstalledAdopter>,
}

impl std::fmt::Debug for AdopterManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdopterManager")
            .field("probe_interval", &self.probe_interval)
            .field("statuses", &self.snapshot())
            .finish()
    }
}

impl AdopterManager {
    pub fn build(config: &AppConfig) -> anyhow::Result<Self> {
        let context = AdopterContext {
            data_dir: config.server.data_dir.clone(),
            audit_path: config.audit.path.clone(),
            registry_path: config
                .registry
                .path
                .clone()
                .unwrap_or_else(|| config.server.data_dir.join("registry").join("agents.json")),
        };
        let probe_interval = Duration::from_secs(config.adopters.probe_interval_seconds.max(1));
        let enabled = config
            .adopters
            .enabled
            .iter()
            .map(|item| item.trim().to_owned())
            .filter(|item| !item.is_empty())
            .collect::<BTreeSet<_>>();

        let catalog = built_in_catalog();
        let mut statuses = BTreeMap::new();
        let mut installed = Vec::new();

        for factory in catalog.values() {
            let manifest = factory.manifest.clone();
            let enabled_here = enabled.contains(&manifest.id);
            let detail = if enabled_here {
                "configured and awaiting probe".to_owned()
            } else {
                "available but not enabled".to_owned()
            };
            statuses.insert(
                manifest.id.clone(),
                snapshot_from_manifest(&manifest, enabled_here, "inactive", detail, None),
            );

            if !enabled_here {
                continue;
            }

            let adopter = (factory.build)(config.adopters.packages.get(&manifest.id))
                .with_context(|| format!("failed to build adopter `{}`", manifest.id))?;
            installed.push(InstalledAdopter { manifest, adopter });
        }

        for missing in enabled.iter().filter(|item| !catalog.contains_key(*item)) {
            if config.adopters.require_installed {
                bail!(
                    "enabled adopter `{missing}` is not installed; compile expressways-server with the matching adopter feature or disable it in config"
                );
            }
            statuses.insert(
                missing.clone(),
                AdopterStatusView {
                    id: missing.clone(),
                    package: "not_installed".to_owned(),
                    description: "configured adopter package is not installed".to_owned(),
                    enabled: true,
                    status: "failed".to_owned(),
                    detail: "configured adopter package is not installed in this server build"
                        .to_owned(),
                    capabilities: Vec::new(),
                    last_run_at: Some(Utc::now()),
                },
            );
        }

        Ok(Self {
            context,
            probe_interval,
            statuses: Mutex::new(statuses),
            installed,
        })
    }

    pub fn probe_interval(&self) -> Duration {
        self.probe_interval
    }

    pub fn has_enabled(&self) -> bool {
        self.statuses
            .lock()
            .expect("adopter statuses lock")
            .values()
            .any(|status| status.enabled)
    }

    pub fn snapshot(&self) -> Vec<AdopterStatusView> {
        self.statuses
            .lock()
            .expect("adopter statuses lock")
            .values()
            .cloned()
            .collect()
    }

    pub fn probe_now(&self, service_mode: &ServiceModeTracker) {
        {
            let statuses = self.statuses.lock().expect("adopter statuses lock");
            for status in statuses.values().filter(|status| status.enabled) {
                let key = format!("adopter:{}", status.id);
                if status.status == "healthy" || status.status == "inactive" {
                    service_mode.clear_component(&key);
                } else {
                    service_mode.mark_degraded(key, status.detail.clone());
                }
            }
        }

        for installed in &self.installed {
            let manifest = installed.manifest.clone();
            let mut outcome = match installed.adopter.inspect(&self.context) {
                Ok(outcome) => outcome,
                Err(error) => AdopterOutcome {
                    status: AdopterHealth::Failed,
                    detail: error.to_string(),
                },
            };

            if outcome.status != AdopterHealth::Healthy
                && manifest
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == "self_heal")
            {
                match installed.adopter.remediate(&self.context, &outcome) {
                    Ok(Some(detail)) => {
                        info!(adopter_id = %manifest.id, remediation = %detail, "adopter remediation executed");
                        outcome = match installed.adopter.inspect(&self.context) {
                            Ok(outcome) => outcome,
                            Err(error) => AdopterOutcome {
                                status: AdopterHealth::Failed,
                                detail: error.to_string(),
                            },
                        };
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(adopter_id = %manifest.id, error = %error, "adopter remediation failed");
                        outcome = AdopterOutcome {
                            status: AdopterHealth::Failed,
                            detail: format!("remediation failed: {error}"),
                        };
                    }
                }
            }

            let key = format!("adopter:{}", manifest.id);
            let snapshot = snapshot_from_manifest(
                &manifest,
                true,
                outcome.status.as_str(),
                outcome.detail.clone(),
                Some(Utc::now()),
            );

            self.statuses
                .lock()
                .expect("adopter statuses lock")
                .insert(manifest.id.clone(), snapshot);

            if outcome.status == AdopterHealth::Healthy {
                service_mode.clear_component(&key);
            } else {
                service_mode.mark_degraded(key, outcome.detail);
            }
        }
    }
}

fn snapshot_from_manifest(
    manifest: &AdopterManifest,
    enabled: bool,
    status: &str,
    detail: String,
    last_run_at: Option<chrono::DateTime<Utc>>,
) -> AdopterStatusView {
    let snapshot = AdopterSnapshot {
        id: manifest.id.clone(),
        package: manifest.package.clone(),
        description: manifest.description.clone(),
        enabled,
        status: status.to_owned(),
        detail,
        capabilities: manifest
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
        last_run_at,
    };

    AdopterStatusView {
        id: snapshot.id,
        package: snapshot.package,
        description: snapshot.description,
        enabled: snapshot.enabled,
        status: snapshot.status,
        detail: snapshot.detail,
        capabilities: snapshot.capabilities,
        last_run_at: snapshot.last_run_at,
    }
}

fn built_in_catalog() -> BTreeMap<String, AdopterFactory> {
    let mut catalog = BTreeMap::new();

    #[cfg(feature = "adopter-audit-integrity")]
    {
        register(
            &mut catalog,
            expressways_adopter_audit_integrity::manifest(),
            expressways_adopter_audit_integrity::build,
        );
    }

    #[cfg(feature = "adopter-storage-guard")]
    {
        register(
            &mut catalog,
            expressways_adopter_storage_guard::manifest(),
            expressways_adopter_storage_guard::build,
        );
    }

    #[cfg(feature = "adopter-registry-guard")]
    {
        register(
            &mut catalog,
            expressways_adopter_registry_guard::manifest(),
            expressways_adopter_registry_guard::build,
        );
    }

    catalog
}

fn register(
    catalog: &mut BTreeMap<String, AdopterFactory>,
    manifest: AdopterManifest,
    build: AdopterFactoryFn,
) {
    catalog.insert(manifest.id.clone(), AdopterFactory { manifest, build });
}
