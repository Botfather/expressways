use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use anyhow::{Context, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use expressways_protocol::{Action, CapabilityClaims, CapabilityScope};
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalStatus {
    #[default]
    Active,
    Disabled,
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
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IssuerStatus {
    #[default]
    Active,
    Rotating,
    Disabled,
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
#[serde(deny_unknown_fields)]
pub struct TrustedIssuerConfig {
    pub key_id: String,
    pub public_key_path: PathBuf,
    #[serde(default)]
    pub status: IssuerStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default = "default_audience")]
    pub audience: String,
    pub revocation_path: PathBuf,
    #[serde(default)]
    pub issuers: Vec<TrustedIssuerConfig>,
    #[serde(default)]
    pub principals: Vec<PrincipalRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RevocationList {
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default)]
    pub revoked_tokens: Vec<Uuid>,
    #[serde(default)]
    pub revoked_principals: Vec<String>,
    #[serde(default)]
    pub revoked_key_ids: Vec<String>,
}

const REVOCATION_SCHEMA_VERSION: u32 = 1;
const LEGACY_REVOCATION_SCHEMA_VERSION: u32 = 0;
const MAX_REVOCATION_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_REVOKED_TOKENS: usize = 100_000;
const MAX_REVOKED_PRINCIPALS: usize = 10_000;
const MAX_REVOKED_KEY_IDS: usize = 1_000;
const MAX_AUTH_IDENTIFIER_BYTES: usize = 256;
const MAX_AUTH_DISPLAY_NAME_BYTES: usize = 1024;
const MAX_TRUSTED_ISSUERS: usize = 1_000;
const MAX_PRINCIPALS: usize = 10_000;
const MAX_ALLOWED_KEYS_PER_PRINCIPAL: usize = 1_000;
const MAX_KEY_FILE_BYTES: u64 = 4 * 1024;
const MAX_CAPABILITY_TOKEN_BYTES: usize = 64 * 1024;

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

        let raw = read_bounded_text(path, MAX_REVOCATION_FILE_BYTES, "revocation list")?;
        let mut list: Self =
            serde_json::from_str(&raw).context("failed to parse revocation list")?;
        list.validate()?;
        match list.schema_version {
            LEGACY_REVOCATION_SCHEMA_VERSION => {
                list.schema_version = REVOCATION_SCHEMA_VERSION;
                list.save(path)?;
                Ok(list)
            }
            REVOCATION_SCHEMA_VERSION => Ok(list),
            found => bail!(
                "revocation list schema version {} is newer than supported version {}",
                found,
                REVOCATION_SCHEMA_VERSION
            ),
        }
    }

    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut persisted = self.clone();
        persisted.schema_version = REVOCATION_SCHEMA_VERSION;
        persisted.validate()?;
        let raw =
            serde_json::to_vec_pretty(&persisted).context("failed to serialize revocation list")?;
        if u64::try_from(raw.len()).unwrap_or(u64::MAX) > MAX_REVOCATION_FILE_BYTES {
            bail!(
                "revocation list exceeds the {}-byte limit",
                MAX_REVOCATION_FILE_BYTES
            );
        }
        let temp_path = path.with_extension(format!("tmp-{}", Uuid::now_v7()));
        let result = (|| -> std::io::Result<()> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(&temp_path)?;
            file.write_all(&raw)?;
            file.sync_all()?;
            fs::rename(&temp_path, path)?;
            #[cfg(unix)]
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result.with_context(|| format!("failed to write {}", path.display()))
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

    fn validate(&self) -> anyhow::Result<()> {
        validate_unique_count("revoked_tokens", &self.revoked_tokens, MAX_REVOKED_TOKENS)?;
        validate_unique_count(
            "revoked_principals",
            &self.revoked_principals,
            MAX_REVOKED_PRINCIPALS,
        )?;
        validate_unique_count(
            "revoked_key_ids",
            &self.revoked_key_ids,
            MAX_REVOKED_KEY_IDS,
        )?;
        for value in &self.revoked_principals {
            validate_auth_identifier("revoked principal", value)?;
        }
        for value in &self.revoked_key_ids {
            validate_auth_identifier("revoked key id", value)?;
        }
        Ok(())
    }
}

impl Default for RevocationList {
    fn default() -> Self {
        Self {
            schema_version: REVOCATION_SCHEMA_VERSION,
            revoked_tokens: Vec::new(),
            revoked_principals: Vec::new(),
            revoked_key_ids: Vec::new(),
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
    #[error("capability token is {bytes} bytes; maximum is {max_bytes} bytes")]
    TokenTooLarge { bytes: usize, max_bytes: usize },
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
        #[cfg(unix)]
        {
            let mode = fs::metadata(private_key_path.as_ref())
                .with_context(|| {
                    format!(
                        "failed to inspect private key {}",
                        private_key_path.as_ref().display()
                    )
                })?
                .permissions()
                .mode()
                & 0o777;
            if mode & 0o077 != 0 {
                bail!(
                    "private key {} has insecure permissions {:03o}; expected owner-only access (0600)",
                    private_key_path.as_ref().display(),
                    mode
                );
            }
        }
        let raw = read_bounded_text(private_key_path.as_ref(), MAX_KEY_FILE_BYTES, "private key")?;
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
        atomic_write_key(
            path.as_ref(),
            hex::encode(self.signing_key.to_bytes()).as_bytes(),
            true,
        )
    }

    pub fn write_public_key(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        atomic_write_key(
            path.as_ref(),
            hex::encode(self.signing_key.verifying_key().to_bytes()).as_bytes(),
            false,
        )
    }

    pub fn issue(&self, claims: CapabilityClaims) -> Result<String, AuthError> {
        let payload = TokenPayload {
            key_id: self.key_id.clone(),
            claims,
        };
        let bytes = serde_json::to_vec(&payload)?;
        let signature = self.signing_key.sign(&bytes);
        let token = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(bytes),
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        );
        if token.len() > MAX_CAPABILITY_TOKEN_BYTES {
            return Err(AuthError::TokenTooLarge {
                bytes: token.len(),
                max_bytes: MAX_CAPABILITY_TOKEN_BYTES,
            });
        }
        Ok(token)
    }

    pub fn sign_bytes(&self, message: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(self.signing_key.sign(message).to_bytes())
    }
}

fn atomic_write_key(path: &Path, bytes: &[u8], private: bool) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let temp_path = path.with_extension(format!("tmp-{}", Uuid::now_v7()));
    let result = (|| -> std::io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(if private { 0o600 } else { 0o644 });
        #[cfg(not(unix))]
        let _ = private;
        let mut file = options.open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp_path, path)?;
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result.with_context(|| format!("failed to write {}", path.display()))
}

pub fn verify_detached_signature(
    public_key_path: impl AsRef<Path>,
    message: &[u8],
    signature: &str,
) -> anyhow::Result<()> {
    let verifying_key = load_verifying_key(public_key_path.as_ref())?;
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(signature.trim())
        .context("detached signature is not valid base64url")?;
    let signature =
        Signature::from_slice(&signature_bytes).context("detached signature is invalid")?;
    verifying_key
        .verify(message, &signature)
        .context("detached signature verification failed")
}

pub fn write_secret_file(path: impl AsRef<Path>, bytes: &[u8]) -> anyhow::Result<()> {
    atomic_write_key(path.as_ref(), bytes, true)
}

#[derive(Debug)]
struct LoadedIssuer {
    status: IssuerStatus,
    verifying_key: VerifyingKey,
}

#[derive(Debug, Default)]
struct CachedRevocations {
    fingerprint: Option<FileFingerprint>,
    list: RevocationList,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
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
        validate_auth_config(config)?;
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
        if token.len() > MAX_CAPABILITY_TOKEN_BYTES {
            return Err(AuthError::TokenTooLarge {
                bytes: token.len(),
                max_bytes: MAX_CAPABILITY_TOKEN_BYTES,
            });
        }
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
        let mut guard = self
            .revocations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        refresh_revocations(&self.revocation_path, &mut guard)
            .map_err(|error| AuthError::Io(std::io::Error::other(error.to_string())))?;
        Ok(guard.list.clone())
    }

    fn update_revocations<F>(&self, update: F) -> anyhow::Result<RevocationList>
    where
        F: FnOnce(&mut RevocationList) -> anyhow::Result<()>,
    {
        let mut guard = self
            .revocations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        refresh_revocations(&self.revocation_path, &mut guard)?;
        update(&mut guard.list)?;
        guard.list.save(&self.revocation_path)?;
        guard.fingerprint = fs::metadata(&self.revocation_path)
            .ok()
            .map(|item| file_fingerprint(&item));
        Ok(guard.list.clone())
    }
}

fn validate_auth_config(config: &AuthConfig) -> anyhow::Result<()> {
    validate_auth_identifier("audience", &config.audience)?;
    if config.issuers.len() > MAX_TRUSTED_ISSUERS {
        bail!(
            "auth config has {} issuers, exceeding the limit of {MAX_TRUSTED_ISSUERS}",
            config.issuers.len()
        );
    }
    if config.principals.len() > MAX_PRINCIPALS {
        bail!(
            "auth config has {} principals, exceeding the limit of {MAX_PRINCIPALS}",
            config.principals.len()
        );
    }
    for issuer in &config.issuers {
        validate_auth_identifier("issuer key id", &issuer.key_id)?;
        if issuer.public_key_path.as_os_str().is_empty() {
            bail!("issuer `{}` has an empty public key path", issuer.key_id);
        }
    }
    for principal in &config.principals {
        validate_auth_identifier("principal id", &principal.id)?;
        validate_auth_identifier("quota profile", &principal.quota_profile)?;
        if principal.display_name.is_empty()
            || principal.display_name.len() > MAX_AUTH_DISPLAY_NAME_BYTES
        {
            bail!(
                "principal `{}` display name must contain 1..={MAX_AUTH_DISPLAY_NAME_BYTES} bytes",
                principal.id
            );
        }
        if principal.allowed_key_ids.len() > MAX_ALLOWED_KEYS_PER_PRINCIPAL {
            bail!(
                "principal `{}` has too many allowed issuer keys",
                principal.id
            );
        }
        let mut keys = HashSet::new();
        for key_id in &principal.allowed_key_ids {
            validate_auth_identifier("allowed issuer key id", key_id)?;
            if !keys.insert(key_id) {
                bail!(
                    "principal `{}` contains duplicate allowed issuer key `{key_id}`",
                    principal.id
                );
            }
        }
    }
    Ok(())
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
    let fingerprint = fs::metadata(path).ok().map(|item| file_fingerprint(&item));

    if guard.fingerprint != fingerprint {
        guard.list = if path.exists() {
            RevocationList::load(path)?
        } else {
            RevocationList::default()
        };
        guard.fingerprint = fingerprint;
    }

    Ok(())
}

fn validate_unique_count<T>(field: &str, values: &[T], max: usize) -> anyhow::Result<()>
where
    T: Eq + std::hash::Hash,
{
    if values.len() > max {
        bail!(
            "{field} contains {} entries; maximum is {max}",
            values.len()
        );
    }
    let unique = values.iter().collect::<HashSet<_>>();
    if unique.len() != values.len() {
        bail!("{field} contains duplicate entries");
    }
    Ok(())
}

fn validate_auth_identifier(field: &str, value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if value.len() > MAX_AUTH_IDENTIFIER_BYTES {
        bail!(
            "{field} is {} bytes; maximum is {MAX_AUTH_IDENTIFIER_BYTES} bytes",
            value.len()
        );
    }
    Ok(())
}

fn read_bounded_text(path: &Path, max_bytes: u64, kind: &str) -> anyhow::Result<String> {
    let initial_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {kind} {}", path.display()))?;
    if !initial_metadata.file_type().is_file() {
        bail!("{kind} {} is not a regular file", path.display());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .with_context(|| format!("failed to read {kind} {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {kind} {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{kind} {} is not a regular file", path.display());
    }
    let declared_size = metadata.len();
    if declared_size > max_bytes {
        bail!(
            "{kind} {} is {declared_size} bytes; maximum is {max_bytes} bytes",
            path.display()
        );
    }
    let mut raw = String::with_capacity(usize::try_from(declared_size).unwrap_or(0));
    file.take(max_bytes.saturating_add(1))
        .read_to_string(&mut raw)
        .with_context(|| format!("failed to read {kind} {}", path.display()))?;
    if u64::try_from(raw.len()).unwrap_or(u64::MAX) > max_bytes {
        bail!(
            "{kind} {} grew beyond the {max_bytes}-byte limit while being read",
            path.display()
        );
    }
    Ok(raw)
}

fn file_fingerprint(metadata: &fs::Metadata) -> FileFingerprint {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    FileFingerprint {
        len: metadata.len(),
        modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    }
}

fn load_verifying_key(path: &Path) -> anyhow::Result<VerifyingKey> {
    let raw = read_bounded_text(path, MAX_KEY_FILE_BYTES, "public key")?;
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
    "expressways".to_owned()
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

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

    #[cfg(unix)]
    #[test]
    fn private_keys_are_written_atomically_with_owner_only_permissions() {
        let root = std::env::temp_dir().join(format!("expressways-key-mode-{}", Uuid::now_v7()));
        let private_path = root.join("issuer.private");
        let issuer = CapabilityIssuer::generate("dev");

        issuer
            .write_private_key(&private_path)
            .expect("write private key");

        let mode = fs::metadata(&private_path)
            .expect("private key metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        assert!(
            fs::read_dir(&root)
                .expect("read key directory")
                .all(|entry| !entry
                    .expect("key directory entry")
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp-"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn private_key_loading_rejects_group_or_world_access() {
        let root = std::env::temp_dir().join(format!("expressways-key-mode-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create key directory");
        let private_path = root.join("issuer.private");
        fs::write(&private_path, "00".repeat(32)).expect("write private key");
        fs::set_permissions(&private_path, fs::Permissions::from_mode(0o644))
            .expect("set insecure permissions");

        let error = CapabilityIssuer::from_private_key_file("dev", &private_path)
            .expect_err("insecure key permissions must fail");
        assert!(error.to_string().contains("insecure permissions 644"));
    }

    fn auth_fixture() -> (CapabilityIssuer, AuthConfig, PathBuf, PathBuf, PathBuf) {
        let issuer = CapabilityIssuer::generate("dev");
        let root = std::env::temp_dir().join(format!("expressways-auth-{}", Uuid::now_v7()));
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
                audience: "expressways".to_owned(),
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
                audience: "expressways".to_owned(),
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
                audience: "expressways".to_owned(),
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
                audience: "expressways".to_owned(),
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
                audience: "expressways".to_owned(),
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
    fn revocation_load_migrates_legacy_schema_and_rewrites_file() {
        let (_, _, _, _, revocation_path) = auth_fixture();
        fs::write(
            &revocation_path,
            "{\"revoked_tokens\":[],\"revoked_principals\":[],\"revoked_key_ids\":[]}",
        )
        .expect("write legacy revocations");

        let loaded = RevocationList::load(&revocation_path).expect("load revocations");
        assert_eq!(loaded.schema_version, REVOCATION_SCHEMA_VERSION);

        let rewritten = fs::read_to_string(&revocation_path).expect("read migrated revocations");
        let value: serde_json::Value = serde_json::from_str(&rewritten).expect("parse migrated");
        assert_eq!(value["schema_version"], REVOCATION_SCHEMA_VERSION);
    }

    #[test]
    fn revocation_load_rejects_newer_schema_versions() {
        let (_, _, _, _, revocation_path) = auth_fixture();
        fs::write(
            &revocation_path,
            "{\"schema_version\":99,\"revoked_tokens\":[],\"revoked_principals\":[],\"revoked_key_ids\":[]}",
        )
        .expect("write unsupported revocations");
        let error = RevocationList::load(&revocation_path).expect_err("unsupported schema fails");
        assert!(
            error
                .to_string()
                .contains("revocation list schema version 99 is newer than supported version 1")
        );
    }

    #[test]
    fn unknown_principal_is_rejected() {
        let (issuer, config, _, _, _) = auth_fixture();
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");

        let token = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:missing".to_owned(),
                audience: "expressways".to_owned(),
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
                audience: "expressways".to_owned(),
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
                audience: "expressways".to_owned(),
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

    #[test]
    fn oversized_tokens_are_rejected_before_decoding() {
        let (issuer, config, _, _, _) = auth_fixture();
        let verifier = CapabilityVerifier::from_config(&config).expect("build verifier");
        let error = verifier
            .verify(&"x".repeat(MAX_CAPABILITY_TOKEN_BYTES + 1))
            .expect_err("oversized token must fail");
        assert!(matches!(error, AuthError::TokenTooLarge { .. }));

        let error = issuer
            .issue(CapabilityClaims {
                token_id: Uuid::now_v7(),
                principal: "local:agent-alpha".to_owned(),
                audience: "expressways".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes: vec![CapabilityScope {
                    resource: "x".repeat(MAX_CAPABILITY_TOKEN_BYTES),
                    actions: vec![Action::Publish],
                }],
            })
            .expect_err("issuer must reject oversized token");
        assert!(matches!(error, AuthError::TokenTooLarge { .. }));
    }

    #[test]
    fn oversized_auth_files_fail_before_unbounded_reads() {
        let root = std::env::temp_dir().join(format!("expressways-auth-bounds-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create root");
        let revocations = root.join("revocations.json");
        File::create(&revocations)
            .expect("create revocations")
            .set_len(MAX_REVOCATION_FILE_BYTES + 1)
            .expect("extend revocations");
        let error =
            RevocationList::load(&revocations).expect_err("oversized revocations must fail");
        assert!(error.to_string().contains("maximum"));

        let private_key = root.join("issuer.private");
        File::create(&private_key)
            .expect("create private key")
            .set_len(MAX_KEY_FILE_BYTES + 1)
            .expect("extend private key");
        #[cfg(unix)]
        fs::set_permissions(&private_key, fs::Permissions::from_mode(0o600))
            .expect("set private key permissions");
        let error = CapabilityIssuer::from_private_key_file("dev", &private_key)
            .expect_err("oversized private key must fail");
        assert!(error.to_string().contains("maximum"));
    }

    #[cfg(unix)]
    #[test]
    fn bounded_auth_reader_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("expressways-auth-link-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("create root");
        let target = root.join("target.json");
        let link = root.join("revocations.json");
        fs::write(&target, b"{}").expect("write target");
        symlink(&target, &link).expect("create symlink");

        assert!(read_bounded_text(&link, MAX_REVOCATION_FILE_BYTES, "revocation file").is_err());
        fs::remove_dir_all(root).expect("remove root");
    }

    #[test]
    fn duplicate_and_excessive_revocations_are_rejected() {
        let token_id = Uuid::now_v7();
        let duplicates = RevocationList {
            schema_version: REVOCATION_SCHEMA_VERSION,
            revoked_tokens: vec![token_id, token_id],
            revoked_principals: Vec::new(),
            revoked_key_ids: Vec::new(),
        };
        assert!(duplicates.validate().is_err());

        let excessive = RevocationList {
            schema_version: REVOCATION_SCHEMA_VERSION,
            revoked_tokens: Vec::new(),
            revoked_principals: vec!["p".to_owned(); MAX_REVOKED_PRINCIPALS + 1],
            revoked_key_ids: Vec::new(),
        };
        assert!(excessive.validate().is_err());
    }

    #[test]
    fn auth_config_rejects_invalid_identifiers_and_duplicate_allowed_keys() {
        let (_, mut config, _, _, _) = auth_fixture();
        config.audience.clear();
        assert!(validate_auth_config(&config).is_err());

        config.audience = "expressways".to_owned();
        config.principals[0].allowed_key_ids = vec!["dev".to_owned(), "dev".to_owned()];
        let error = validate_auth_config(&config).expect_err("duplicate allowed key must fail");
        assert!(error.to_string().contains("duplicate allowed issuer key"));

        config.principals[0].allowed_key_ids.clear();
        config.principals[0].display_name = "x".repeat(MAX_AUTH_DISPLAY_NAME_BYTES + 1);
        assert!(validate_auth_config(&config).is_err());
    }
}
