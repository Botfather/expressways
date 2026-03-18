use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use aetherbus_protocol::{Action, CapabilityClaims, CapabilityScope};
use anyhow::Context;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TokenPayload {
    key_id: String,
    claims: CapabilityClaims,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Developer,
    Agent,
    Service,
}

impl PrincipalKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Developer => "developer",
            Self::Agent => "agent",
            Self::Service => "service",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalStatus {
    Active,
    Disabled,
}

impl Default for PrincipalStatus {
    fn default() -> Self {
        Self::Active
    }
}

impl PrincipalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrincipalRecord {
    pub id: String,
    pub kind: PrincipalKind,
    pub display_name: String,
    #[serde(default)]
    pub status: PrincipalStatus,
    #[serde(default)]
    pub allowed_key_ids: Vec<String>,
    pub quota_profile: String,
}

impl PrincipalRecord {
    fn allows_key(&self, key_id: &str) -> bool {
        self.allowed_key_ids.is_empty() || self.allowed_key_ids.iter().any(|item| item == key_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IssuerStatus {
    Active,
    Rotating,
    Disabled,
}

impl Default for IssuerStatus {
    fn default() -> Self {
        Self::Active
    }
}

impl IssuerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Rotating => "rotating",
            Self::Disabled => "disabled",
        }
    }

    fn allows_verification(&self) -> bool {
        matches!(self, Self::Active | Self::Rotating)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrustedIssuerConfig {
    pub key_id: String,
    pub public_key_path: PathBuf,
    #[serde(default)]
    pub status: IssuerStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthConfig {
    #[serde(default = "default_audience")]
    pub audience: String,
    pub revocation_path: PathBuf,
    #[serde(default)]
    pub issuers: Vec<TrustedIssuerConfig>,
    #[serde(default)]
    pub principals: Vec<PrincipalRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RevocationList {
    #[serde(default)]
    pub revoked_tokens: Vec<Uuid>,
    #[serde(default)]
    pub revoked_principals: Vec<String>,
    #[serde(default)]
    pub revoked_key_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IssuerSummary {
    pub key_id: String,
    pub status: IssuerStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthSnapshot {
    pub audience: String,
    pub issuers: Vec<IssuerSummary>,
    pub principals: Vec<PrincipalRecord>,
    pub revocations: RevocationList,
}

impl RevocationList {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::default());
        }

        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_str(&raw).context("failed to parse revocation list")
    }

    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let raw = serde_json::to_vec_pretty(self).context("failed to serialize revocation list")?;
        fs::write(path, raw).with_context(|| format!("failed to write {}", path.display()))
    }

    pub fn revoke_token(&mut self, token_id: Uuid) {
        if !self.revoked_tokens.contains(&token_id) {
            self.revoked_tokens.push(token_id);
        }
    }

    pub fn revoke_principal(&mut self, principal: impl Into<String>) {
        let principal = principal.into();
        if !self
            .revoked_principals
            .iter()
            .any(|item| item == &principal)
        {
            self.revoked_principals.push(principal);
        }
    }

    pub fn revoke_key(&mut self, key_id: impl Into<String>) {
        let key_id = key_id.into();
        if !self.revoked_key_ids.iter().any(|item| item == &key_id) {
            self.revoked_key_ids.push(key_id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCapability {
    pub key_id: String,
    pub claims: CapabilityClaims,
    pub principal: PrincipalRecord,
}

impl VerifiedCapability {
    pub fn principal(&self) -> &str {
        &self.principal.id
    }

    pub fn token_id(&self) -> String {
        self.claims.token_id.to_string()
    }

    pub fn quota_profile(&self) -> &str {
        &self.principal.quota_profile
    }

    pub fn principal_kind(&self) -> &PrincipalKind {
        &self.principal.kind
    }

    pub fn authorize(&self, resource: &str, action: &Action) -> Result<(), AuthError> {
        if self.claims.expires_at < Utc::now() {
            return Err(AuthError::Expired(self.claims.token_id.to_string()));
        }

        if self
            .claims
            .scopes
            .iter()
            .any(|scope| scope_matches(scope, resource, action))
        {
            return Ok(());
        }

        Err(AuthError::ScopeDenied {
            token_id: self.claims.token_id.to_string(),
            action: action.to_string(),
            resource: resource.to_owned(),
        })
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid key id `{0}`")]
    InvalidKeyId(String),
    #[error("token format is invalid")]
    InvalidTokenFormat,
    #[error("token signature is invalid")]
    InvalidSignature,
    #[error("token `{0}` has expired")]
    Expired(String),
    #[error("token audience `{actual}` does not match expected audience `{expected}`")]
    InvalidAudience { expected: String, actual: String },
    #[error("principal `{0}` is not registered")]
    UnknownPrincipal(String),
    #[error("principal `{0}` is disabled")]
    DisabledPrincipal(String),
    #[error("issuer key `{0}` is disabled")]
    DisabledIssuer(String),
    #[error("issuer key `{0}` is revoked")]
    RevokedKey(String),
    #[error("principal `{0}` is revoked")]
    RevokedPrincipal(String),
    #[error("token `{0}` is revoked")]
    RevokedToken(String),
    #[error("principal `{principal}` does not accept issuer key `{key_id}`")]
    KeyNotAllowed { principal: String, key_id: String },
    #[error("token `{token_id}` does not allow action `{action}` on `{resource}`")]
    ScopeDenied {
        token_id: String,
        action: String,
        resource: String,
    },
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("hex decoding error: {0}")]
    Hex(#[from] hex::FromHexError),
}

#[derive(Debug, Clone)]
pub struct CapabilityIssuer {
    key_id: String,
    signing_key: SigningKey,
}

impl CapabilityIssuer {
    pub fn generate(key_id: impl Into<String>) -> Self {
        let mut seed = [0u8; 32];
        OsRng.fill_bytes(&mut seed);
        Self {
            key_id: key_id.into(),
            signing_key: SigningKey::from_bytes(&seed),
        }
    }

    pub fn from_private_key_file(
        key_id: impl Into<String>,
        private_key_path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(private_key_path.as_ref()).with_context(|| {
            format!(
                "failed to read private key from {}",
                private_key_path.as_ref().display()
            )
        })?;
        let bytes = hex::decode(raw.trim()).context("private key is not valid hex")?;
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("private key must be 32 bytes"))?;

        Ok(Self {
            key_id: key_id.into(),
            signing_key: SigningKey::from_bytes(&key_bytes),
        })
    }

    pub fn write_private_key(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(path.as_ref(), hex::encode(self.signing_key.to_bytes()))
            .with_context(|| format!("failed to write {}", path.as_ref().display()))
    }

    pub fn write_public_key(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(
            path.as_ref(),
            hex::encode(self.signing_key.verifying_key().to_bytes()),
        )
        .with_context(|| format!("failed to write {}", path.as_ref().display()))
    }

    pub fn issue(&self, claims: CapabilityClaims) -> Result<String, AuthError> {
        let payload = TokenPayload {
            key_id: self.key_id.clone(),
            claims,
        };
        let bytes = serde_json::to_vec(&payload)?;
        let signature = self.signing_key.sign(&bytes);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(bytes),
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        ))
    }
}

#[derive(Debug)]
struct LoadedIssuer {
    status: IssuerStatus,
    verifying_key: VerifyingKey,
}

#[derive(Debug, Default)]
struct CachedRevocations {
    modified: Option<SystemTime>,
    list: RevocationList,
}

#[derive(Debug)]
pub struct CapabilityVerifier {
    audience: String,
    issuers: HashMap<String, LoadedIssuer>,
    principals: HashMap<String, PrincipalRecord>,
    revocation_path: PathBuf,
    revocations: Mutex<CachedRevocations>,
}

impl CapabilityVerifier {
    pub fn from_config(config: &AuthConfig) -> anyhow::Result<Self> {
        if config.issuers.is_empty() {
            anyhow::bail!("at least one trusted issuer is required");
        }
        if config.principals.is_empty() {
            anyhow::bail!("at least one principal is required");
        }

        let mut issuers = HashMap::new();
        for issuer in &config.issuers {
            if issuers.contains_key(&issuer.key_id) {
                anyhow::bail!("duplicate trusted issuer key id `{}`", issuer.key_id);
            }
            let verifying_key = load_verifying_key(&issuer.public_key_path)?;
            issuers.insert(
                issuer.key_id.clone(),
                LoadedIssuer {
                    status: issuer.status.clone(),
                    verifying_key,
                },
            );
        }

        let mut principals = HashMap::new();
        for principal in &config.principals {
            if principals.contains_key(&principal.id) {
                anyhow::bail!("duplicate principal id `{}`", principal.id);
            }

            for key_id in &principal.allowed_key_ids {
                if !issuers.contains_key(key_id) {
                    anyhow::bail!(
                        "principal `{}` references unknown issuer key `{}`",
                        principal.id,
                        key_id
                    );
                }
            }

            principals.insert(principal.id.clone(), principal.clone());
        }

        Ok(Self {
            audience: config.audience.clone(),
            issuers,
            principals,
            revocation_path: config.revocation_path.clone(),
            revocations: Mutex::new(CachedRevocations::default()),
        })
    }

    pub fn snapshot(&self) -> anyhow::Result<AuthSnapshot> {
        let mut issuers = self
            .issuers
            .iter()
            .map(|(key_id, issuer)| IssuerSummary {
                key_id: key_id.clone(),
                status: issuer.status.clone(),
            })
            .collect::<Vec<_>>();
        issuers.sort_by(|left, right| left.key_id.cmp(&right.key_id));

        let mut principals = self.principals.values().cloned().collect::<Vec<_>>();
        principals.sort_by(|left, right| left.id.cmp(&right.id));

        Ok(AuthSnapshot {
            audience: self.audience.clone(),
            issuers,
            principals,
            revocations: self.current_revocation_list()?,
        })
    }

    pub fn revoke_token(&self, token_id: Uuid) -> anyhow::Result<RevocationList> {
        self.update_revocations(|list| {
            list.revoke_token(token_id);
            Ok(())
        })
    }

    pub fn revoke_principal(&self, principal: &str) -> anyhow::Result<RevocationList> {
        if !self.principals.contains_key(principal) {
            anyhow::bail!("principal `{principal}` is not registered");
        }

        self.update_revocations(|list| {
            list.revoke_principal(principal);
            Ok(())
        })
    }

    pub fn revoke_key(&self, key_id: &str) -> anyhow::Result<RevocationList> {
        if !self.issuers.contains_key(key_id) {
            anyhow::bail!("issuer key `{key_id}` is not registered");
        }

        self.update_revocations(|list| {
            list.revoke_key(key_id);
            Ok(())
        })
    }

    pub fn verify(&self, token: &str) -> Result<VerifiedCapability, AuthError> {
        let (payload_b64, signature_b64) =
            token.split_once('.').ok_or(AuthError::InvalidTokenFormat)?;
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_b64)
            .map_err(|_| AuthError::InvalidTokenFormat)?;
        let signature_bytes = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|_| AuthError::InvalidTokenFormat)?;
        let signature =
            Signature::from_slice(&signature_bytes).map_err(|_| AuthError::InvalidTokenFormat)?;

        let payload: TokenPayload = serde_json::from_slice(&payload_bytes)?;
        let issuer = self
            .issuers
            .get(&payload.key_id)
            .ok_or_else(|| AuthError::InvalidKeyId(payload.key_id.clone()))?;

        if !issuer.status.allows_verification() {
            return Err(AuthError::DisabledIssuer(payload.key_id));
        }

        issuer
            .verifying_key
            .verify(&payload_bytes, &signature)
            .map_err(|_| AuthError::InvalidSignature)?;

        if payload.claims.expires_at < Utc::now() {
            return Err(AuthError::Expired(payload.claims.token_id.to_string()));
        }

        if payload.claims.audience != self.audience {
            return Err(AuthError::InvalidAudience {
                expected: self.audience.clone(),
                actual: payload.claims.audience,
            });
        }

        let revocations = self.current_revocations()?;
        if revocations.revoked_key_ids.contains(&payload.key_id) {
            return Err(AuthError::RevokedKey(payload.key_id));
        }
        if revocations
            .revoked_tokens
            .contains(&payload.claims.token_id.to_string())
        {
            return Err(AuthError::RevokedToken(payload.claims.token_id.to_string()));
        }
        if revocations
            .revoked_principals
            .contains(&payload.claims.principal)
        {
            return Err(AuthError::RevokedPrincipal(payload.claims.principal));
        }

        let principal = self
            .principals
            .get(&payload.claims.principal)
            .cloned()
            .ok_or_else(|| AuthError::UnknownPrincipal(payload.claims.principal.clone()))?;

        if principal.status != PrincipalStatus::Active {
            return Err(AuthError::DisabledPrincipal(principal.id));
        }
        if !principal.allows_key(&payload.key_id) {
            return Err(AuthError::KeyNotAllowed {
                principal: principal.id,
                key_id: payload.key_id,
            });
        }

        Ok(VerifiedCapability {
            key_id: payload.key_id,
            claims: payload.claims,
            principal,
        })
    }

    fn current_revocations(&self) -> Result<ResolvedRevocations, AuthError> {
        Ok(ResolvedRevocations::from(&self.current_revocation_list()?))
    }

    fn current_revocation_list(&self) -> Result<RevocationList, AuthError> {
        let mut guard = self.revocations.lock().expect("revocation cache lock");
        refresh_revocations(&self.revocation_path, &mut guard)
            .map_err(|error| AuthError::Io(std::io::Error::other(error.to_string())))?;
        Ok(guard.list.clone())
    }

    fn update_revocations<F>(&self, update: F) -> anyhow::Result<RevocationList>
    where
        F: FnOnce(&mut RevocationList) -> anyhow::Result<()>,
    {
        let mut guard = self.revocations.lock().expect("revocation cache lock");
        refresh_revocations(&self.revocation_path, &mut guard)?;
        update(&mut guard.list)?;
        guard.list.save(&self.revocation_path)?;
        guard.modified = fs::metadata(&self.revocation_path)
            .ok()
            .and_then(|item| item.modified().ok());
        Ok(guard.list.clone())
    }
}

#[derive(Debug)]
struct ResolvedRevocations {
    revoked_tokens: HashSet<String>,
    revoked_principals: HashSet<String>,
    revoked_key_ids: HashSet<String>,
}

impl From<&RevocationList> for ResolvedRevocations {
    fn from(list: &RevocationList) -> Self {
        Self {
            revoked_tokens: list
                .revoked_tokens
                .iter()
                .map(Uuid::to_string)
                .collect::<HashSet<_>>(),
            revoked_principals: list
                .revoked_principals
                .iter()
                .cloned()
                .collect::<HashSet<_>>(),
            revoked_key_ids: list.revoked_key_ids.iter().cloned().collect::<HashSet<_>>(),
        }
    }
}

fn refresh_revocations(path: &Path, guard: &mut CachedRevocations) -> anyhow::Result<()> {
    let metadata = fs::metadata(path).ok();
    let modified = metadata.and_then(|item| item.modified().ok());

    if guard.modified != modified {
        guard.list = if path.exists() {
            RevocationList::load(path)?
        } else {
            RevocationList::default()
        };
        guard.modified = modified;
    }

    Ok(())
}

fn load_verifying_key(path: &Path) -> anyhow::Result<VerifyingKey> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let bytes = hex::decode(raw.trim()).context("public key is not valid hex")?;
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("public key must be 32 bytes"))?;
    VerifyingKey::from_bytes(&key_bytes).context("public key is invalid ed25519 bytes")
}

fn scope_matches(scope: &CapabilityScope, resource: &str, action: &Action) -> bool {
    pattern_matches(&scope.resource, resource)
        && scope.actions.iter().any(|allowed| allowed == action)
}

fn pattern_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }

    pattern == value
}

fn default_audience() -> String {
    "aetherbus".to_owned()
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;

    fn principal(id: &str) -> PrincipalRecord {
        PrincipalRecord {
            id: id.to_owned(),
            kind: PrincipalKind::Agent,
            display_name: "Test Principal".to_owned(),
            status: PrincipalStatus::Active,
            allowed_key_ids: vec!["dev".to_owned()],
            quota_profile: "default".to_owned(),
        }
    }

    fn auth_fixture() -> (CapabilityIssuer, AuthConfig, PathBuf, PathBuf, PathBuf) {
        let issuer = CapabilityIssuer::generate("dev");
        let root = std::env::temp_dir().join(format!("aetherbus-auth-{}", Uuid::now_v7()));
        let private_path = root.join("issuer.private");
        let public_path = root.join("issuer.public");
        let revocation_path = root.join("revocations.json");
        issuer
            .write_private_key(&private_path)
            .expect("write private key");
        issuer
            .write_public_key(&public_path)
            .expect("write public key");

        (
            issuer,
            AuthConfig {
                audience: "aetherbus".to_owned(),
                revocation_path: revocation_path.clone(),
                issuers: vec![TrustedIssuerConfig {
                    key_id: "dev".to_owned(),
                    public_key_path: public_path.clone(),
                    status: IssuerStatus::Active,
                }],
                principals: vec![principal("local:agent-alpha")],
            },
            private_path,
            public_path,
            revocation_path,
        )
    }

    #[test]
    fn issued_tokens_verify_and_authorize() {
        let (issuer, config, _, _, _) = auth_fixture();
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:agent-alpha".to_owned(),
                audience: "aetherbus".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish, Action::Consume],
                }],
            })
            .expect("issue token");

        let verified = verifier.verify(&token).expect("verify token");
        verified
            .authorize("topic:tasks", &Action::Publish)
            .expect("authorize publish");
        assert_eq!(verified.principal(), "local:agent-alpha");
        assert_eq!(verified.quota_profile(), "default");
    }

    #[test]
    fn wrong_scope_is_denied() {
        let (issuer, config, _, _, _) = auth_fixture();
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:agent-alpha".to_owned(),
                audience: "aetherbus".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:tasks".to_owned(),
                    actions: vec![Action::Publish],
                }],
            })
            .expect("issue token");

        let verified = verifier.verify(&token).expect("verify token");
        let error = verified
            .authorize("system:broker", &Action::Admin)
            .expect_err("scope should deny admin access");

        assert!(matches!(error, AuthError::ScopeDenied { .. }));
    }

    #[test]
    fn expired_tokens_are_rejected() {
        let (issuer, config, _, _, _) = auth_fixture();
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:agent-alpha".to_owned(),
                audience: "aetherbus".to_owned(),
                issued_at: Utc::now() - Duration::minutes(20),
                expires_at: Utc::now() - Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish],
                }],
            })
            .expect("issue token");

        let error = verifier
            .verify(&token)
            .expect_err("expired token should be rejected");

        assert!(matches!(error, AuthError::Expired(_)));
    }

    #[test]
    fn wrong_audience_is_rejected() {
        let (issuer, config, _, _, _) = auth_fixture();
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:agent-alpha".to_owned(),
                audience: "other-bus".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish],
                }],
            })
            .expect("issue token");

        let error = verifier
            .verify(&token)
            .expect_err("wrong audience should be rejected");

        assert!(matches!(error, AuthError::InvalidAudience { .. }));
    }

    #[test]
    fn revocations_are_applied() {
        let (issuer, config, _, _, revocation_path) = auth_fixture();
        let token_id = Uuid::now_v7();
        let token = issuer
            .issue(CapabilityClaims {
                token_id,
                principal: "local:agent-alpha".to_owned(),
                audience: "aetherbus".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish],
                }],
            })
            .expect("issue token");

        let mut revocations = RevocationList::default();
        revocations.revoke_token(token_id);
        revocations
            .save(&revocation_path)
            .expect("write revocation file");

        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");
        let error = verifier
            .verify(&token)
            .expect_err("token should be revoked");

        assert!(matches!(error, AuthError::RevokedToken(_)));
    }

    #[test]
    fn unknown_principal_is_rejected() {
        let (issuer, config, _, _, _) = auth_fixture();
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:missing".to_owned(),
                audience: "aetherbus".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish],
                }],
            })
            .expect("issue token");

        let error = verifier
            .verify(&token)
            .expect_err("unknown principal should be rejected");

        assert!(matches!(error, AuthError::UnknownPrincipal(_)));
    }

    #[test]
    fn disabled_issuer_is_rejected() {
        let (issuer, mut config, _, _, _) = auth_fixture();
        config.issuers[0].status = IssuerStatus::Disabled;
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:agent-alpha".to_owned(),
                audience: "aetherbus".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish],
                }],
            })
            .expect("issue token");

        let error = verifier
            .verify(&token)
            .expect_err("disabled issuer should be rejected");

        assert!(matches!(error, AuthError::DisabledIssuer(_)));
    }

    #[test]
    fn key_not_allowed_is_rejected() {
        let (issuer, mut config, _, public_path, _) = auth_fixture();
        let other_issuer = CapabilityIssuer::generate("other");
        let other_public_path = public_path
            .parent()
            .expect("public key parent")
            .join("other.public");
        other_issuer
            .write_public_key(&other_public_path)
            .expect("write other public key");
        config.issuers.push(TrustedIssuerConfig {
            key_id: "other".to_owned(),
            public_key_path: other_public_path,
            status: IssuerStatus::Active,
        });
        config.principals[0].allowed_key_ids = vec!["other".to_owned()];
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:agent-alpha".to_owned(),
                audience: "aetherbus".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish],
                }],
            })
            .expect("issue token");

        let error = verifier
            .verify(&token)
            .expect_err("key should not be allowed");

        assert!(matches!(error, AuthError::KeyNotAllowed { .. }));
    }
}
