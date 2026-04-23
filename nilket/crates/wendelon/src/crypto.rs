use crate::audit::{AuditAuthority, AuditEvent, AuditOperation, AuditOutcome, AuditSink};
use crate::metrics::global as metrics;
use crate::schema::{IdentityPrefix, NodeId, SlabId, TenantId, TenantKekId};
use async_trait::async_trait;
use base64::Engine as _;
use chrono::Utc;
#[cfg(all(feature = "fips", feature = "non-fips"))]
compile_error!("features `fips` and `non-fips` are mutually exclusive");
#[cfg(not(any(feature = "fips", feature = "non-fips")))]
compile_error!("enable one of the `fips` or `non-fips` crypto feature profiles");
#[cfg(not(feature = "fips"))]
use aws_lc_rs::aead::AES_256_GCM_SIV;
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::hmac;
use aws_lc_rs::key_wrap::{AES_256, AesKek, KeyWrapPadded};
use reqwest::Url;
use scrypt::{Params as ScryptParams, scrypt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::{Error, ErrorKind};
use std::io::{Read, Write};
use std::ops::Range;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

pub const SLAB_MAX_BYTES_HARD_CEILING: u64 = 1 << 35;
pub const SLAB_ENCRYPTION_CHUNK_BYTES: usize = 64 * 1024;
pub const SLAB_AEAD_TAG_BYTES: usize = 16;
pub const SLAB_AAD_BYTES: usize = 48;
pub const SLAB_NONCE_BYTES: usize = 12;
pub const SLAB_NONCE_VERSION: u32 = 0;
pub const SLAB_ENVELOPE_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES: usize = 2048;
pub const DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES: usize = 10_000;

const _: [(); 8] = [(); std::mem::size_of::<u64>()];
const ADR_TIERED_SLAB_MAGIC_HEAD: [u8; 8] = *b"WENDSLAB";
const ADR_TIERED_SLAB_MAGIC_TAIL: [u8; 8] = *b"BALSDNEW";
const ADR_TIERED_SLAB_HEADER_BYTES: usize = 64;
const ADR_TIERED_SLAB_FOOTER_BYTES: usize = 256;
const ADR_TIERED_SLAB_TAIL_BYTES: usize = 16;
const ADR_TIERED_SLAB_INDEX_ENTRY_BYTES: usize = 32;
const ADR_TIERED_SLAB_WIDE_ID_BYTES: usize = 16;
const ADR_TIERED_SLAB_WRAPPED_DEK_BYTES: usize = 96;
const ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES: usize = 40;
const ADR_TIERED_SLAB_HMAC_OFFSET: usize = 192;
const ADR_TIERED_SLAB_HMAC_BYTES: usize = 32;
const ADR_TIERED_SLAB_RESERVED_SUFFIX_BYTES: usize = 32;
const ADR_TIERED_SLAB_CHUNK_SIZE_BYTES: u32 = SLAB_ENCRYPTION_CHUNK_BYTES as u32;
const SLAB_AAD_WIDE_FIELD_BYTES: usize = 16;
const DEFAULT_TENANT_KEK_IDLE_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_TENANT_KEK_ABSOLUTE_TTL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_SLAB_DEK_IDLE_TTL: Duration = Duration::from_secs(15 * 60);
const PARALLEL_CHUNK_CRYPTO_MIN_CHUNKS: usize = 128;
const PARALLEL_CHUNK_CRYPTO_MAX_WORKERS: usize = 8;
#[cfg(any(test, not(feature = "fips")))]
const DEV_INSECURE_ROOT_KEK_DIR: &str = ".wendelon";
#[cfg(any(test, not(feature = "fips")))]
const DEV_INSECURE_ROOT_KEK_FILE: &str = "dev-root-kek.bin";
const LOCAL_SEALED_ROOT_KEK_DIR: &str = ".wendelon";
const LOCAL_SEALED_ROOT_KEK_FILE: &str = "local-sealed-root-kek.bin";
const LOCAL_SEALED_ROOT_KEK_MAGIC: [u8; 8] = *b"WENDLKEK";
const LOCAL_SEALED_ROOT_KEK_VERSION: u32 = 1;
const LOCAL_SEALED_ROOT_KEK_SALT_BYTES: usize = 32;
const LOCAL_SEALED_ROOT_KEK_NONCE_BYTES: usize = 12;
const LOCAL_SEALED_ROOT_KEK_CIPHERTEXT_BYTES: usize = 32 + 16;
const LOCAL_SEALED_ROOT_KEK_TOTAL_BYTES: usize = LOCAL_SEALED_ROOT_KEK_MAGIC.len()
	+ 4 + LOCAL_SEALED_ROOT_KEK_SALT_BYTES
	+ LOCAL_SEALED_ROOT_KEK_NONCE_BYTES
	+ LOCAL_SEALED_ROOT_KEK_CIPHERTEXT_BYTES;
const LOCAL_SEALED_ROOT_KEK_SCRYPT_LOG_N: u8 = 15;
const LOCAL_SEALED_ROOT_KEK_SCRYPT_R: u32 = 8;
const LOCAL_SEALED_ROOT_KEK_SCRYPT_P: u32 = 1;
const LOCAL_SEALED_ROOT_KEK_AAD: &[u8] = b"wendelon.local-sealed.root-kek.v1";
const AWS_KMS_ROOT_KEK_DIR: &str = ".wendelon";
const AWS_KMS_ROOT_KEK_FILE: &str = "aws-kms-root-kek.bin";
const AWS_KMS_ROOT_KEK_MAGIC: [u8; 8] = *b"WENDAKMS";
const AWS_KMS_ROOT_KEK_VERSION: u32 = 1;
const AWS_KMS_SERVICE: &str = "kms";
const AWS_KMS_CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const AWS_KMS_TIMEOUT_SECS: u64 = 5;
pub const DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV: &str = "WENDELON_ROOT_KEK_SECRET";
pub const DEFAULT_AWS_KMS_ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
pub const DEFAULT_AWS_KMS_SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
pub const DEFAULT_AWS_KMS_SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CipherId {
	Aes256GcmSiv = 0,
	Aes256GcmInjective = 1,
}

impl CipherId {
	pub fn label(self) -> &'static str {
		match self {
			Self::Aes256GcmSiv => "aes-256-gcm-siv",
			Self::Aes256GcmInjective => "aes-256-gcm",
		}
	}

	pub fn to_wire(self) -> u8 {
		match self {
			Self::Aes256GcmSiv => 0,
			Self::Aes256GcmInjective => 1,
		}
	}

	pub fn from_wire(value: u8) -> std::io::Result<Self> {
		match value {
			0 => Ok(Self::Aes256GcmSiv),
			1 => Ok(Self::Aes256GcmInjective),
			_ => Err(Error::new(
				ErrorKind::InvalidData,
				format!("unknown cipher_id {value} in slab envelope"),
			)),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootKekProviderConfig {
	DevInsecure,
	LocalSealed(LocalSealedRootKekConfig),
	AwsKms(AwsKmsRootKekConfig),
	Pkcs11(Pkcs11RootKekConfig),
}

impl Default for RootKekProviderConfig {
	fn default() -> Self {
		Self::LocalSealed(LocalSealedRootKekConfig::default())
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalSealedRootKekConfig {
	#[serde(default)]
	pub unlock_secret_env: Option<String>,
	#[serde(default)]
	pub unlock_secret_file: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsKmsRootKekConfig {
	pub region: String,
	pub key_id: String,
	#[serde(default)]
	pub endpoint: Option<String>,
	#[serde(default = "default_aws_kms_access_key_id_env")]
	pub access_key_id_env: String,
	#[serde(default = "default_aws_kms_secret_access_key_env")]
	pub secret_access_key_env: String,
	#[serde(default = "default_aws_kms_session_token_env")]
	pub session_token_env: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pkcs11RootKekConfig {
	pub library_path: PathBuf,
	pub token_label: String,
	pub key_label: String,
	pub pin_env: String,
	#[serde(default)]
	pub slot: Option<u64>,
}

impl Default for LocalSealedRootKekConfig {
	fn default() -> Self {
		Self {
			unlock_secret_env: Some(DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV.into()),
			unlock_secret_file: None,
		}
	}
}

fn default_aws_kms_access_key_id_env() -> String {
	DEFAULT_AWS_KMS_ACCESS_KEY_ID_ENV.into()
}

fn default_aws_kms_secret_access_key_env() -> String {
	DEFAULT_AWS_KMS_SECRET_ACCESS_KEY_ENV.into()
}

fn default_aws_kms_session_token_env() -> String {
	DEFAULT_AWS_KMS_SESSION_TOKEN_ENV.into()
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum RootKekProviderConfigWire {
	Label(String),
	Structured {
		kind: String,
		#[serde(default)]
		unlock_secret_env: Option<String>,
		#[serde(default)]
		unlock_secret_file: Option<PathBuf>,
		#[serde(default)]
		region: Option<String>,
		#[serde(default)]
		key_id: Option<String>,
		#[serde(default)]
		endpoint: Option<String>,
		#[serde(default)]
		access_key_id_env: Option<String>,
		#[serde(default)]
		secret_access_key_env: Option<String>,
		#[serde(default)]
		session_token_env: Option<String>,
		#[serde(default)]
		library_path: Option<PathBuf>,
		#[serde(default)]
		token_label: Option<String>,
		#[serde(default)]
		key_label: Option<String>,
		#[serde(default)]
		pin_env: Option<String>,
		#[serde(default)]
		slot: Option<u64>,
	},
}

impl Serialize for RootKekProviderConfig {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match self {
			Self::DevInsecure => RootKekProviderConfigWire::Label("dev-insecure".into()),
			Self::LocalSealed(cfg) => RootKekProviderConfigWire::Structured {
				kind: "local-sealed".into(),
				unlock_secret_env: cfg.unlock_secret_env.clone(),
				unlock_secret_file: cfg.unlock_secret_file.clone(),
				region: None,
				key_id: None,
				endpoint: None,
				access_key_id_env: None,
				secret_access_key_env: None,
				session_token_env: None,
				library_path: None,
				token_label: None,
				key_label: None,
				pin_env: None,
				slot: None,
			},
			Self::AwsKms(cfg) => RootKekProviderConfigWire::Structured {
				kind: "aws-kms".into(),
				unlock_secret_env: None,
				unlock_secret_file: None,
				region: Some(cfg.region.clone()),
				key_id: Some(cfg.key_id.clone()),
				endpoint: cfg.endpoint.clone(),
				access_key_id_env: Some(cfg.access_key_id_env.clone()),
				secret_access_key_env: Some(cfg.secret_access_key_env.clone()),
				session_token_env: Some(cfg.session_token_env.clone()),
				library_path: None,
				token_label: None,
				key_label: None,
				pin_env: None,
				slot: None,
			},
			Self::Pkcs11(cfg) => RootKekProviderConfigWire::Structured {
				kind: "pkcs11".into(),
				unlock_secret_env: None,
				unlock_secret_file: None,
				region: None,
				key_id: None,
				endpoint: None,
				access_key_id_env: None,
				secret_access_key_env: None,
				session_token_env: None,
				library_path: Some(cfg.library_path.clone()),
				token_label: Some(cfg.token_label.clone()),
				key_label: Some(cfg.key_label.clone()),
				pin_env: Some(cfg.pin_env.clone()),
				slot: cfg.slot,
			},
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for RootKekProviderConfig {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		match RootKekProviderConfigWire::deserialize(deserializer)? {
			RootKekProviderConfigWire::Label(label) => match label.as_str() {
				"dev-insecure" => Ok(Self::DevInsecure),
				"local-sealed" => Err(serde::de::Error::custom(
					"crypto.root_kek_provider=local-sealed requires object form with unlock_secret_env or unlock_secret_file",
				)),
				other => Err(serde::de::Error::custom(format!(
					"unknown crypto.root_kek_provider '{other}'"
				))),
			},
			RootKekProviderConfigWire::Structured {
				kind,
				unlock_secret_env,
				unlock_secret_file,
				region,
				key_id,
				endpoint,
				access_key_id_env,
				secret_access_key_env,
				session_token_env,
				library_path,
				token_label,
				key_label,
				pin_env,
				slot,
			} => {
				match kind.as_str() {
					"dev-insecure" => {
						if unlock_secret_env.is_some()
							|| unlock_secret_file.is_some()
							|| region.is_some() || key_id.is_some()
							|| endpoint.is_some() || access_key_id_env.is_some()
							|| secret_access_key_env.is_some()
							|| session_token_env.is_some()
							|| library_path.is_some()
							|| token_label.is_some()
							|| key_label.is_some()
							|| pin_env.is_some() || slot.is_some()
						{
							return Err(serde::de::Error::custom(
								"crypto.root_kek_provider kind=dev-insecure does not accept provider-specific fields",
							));
						}
						Ok(Self::DevInsecure)
					}
					"local-sealed" => {
						if region.is_some()
							|| key_id.is_some() || endpoint.is_some()
							|| access_key_id_env.is_some()
							|| secret_access_key_env.is_some()
							|| session_token_env.is_some()
							|| library_path.is_some()
							|| token_label.is_some()
							|| key_label.is_some()
							|| pin_env.is_some() || slot.is_some()
						{
							return Err(serde::de::Error::custom(
								"crypto.root_kek_provider kind=local-sealed does not accept aws-kms or pkcs11 fields",
							));
						}
						Ok(Self::LocalSealed(LocalSealedRootKekConfig {
							unlock_secret_env,
							unlock_secret_file,
						}))
					}
					"aws-kms" => {
						if unlock_secret_env.is_some()
							|| unlock_secret_file.is_some()
							|| library_path.is_some()
							|| token_label.is_some()
							|| key_label.is_some()
							|| pin_env.is_some() || slot.is_some()
						{
							return Err(serde::de::Error::custom(
								"crypto.root_kek_provider kind=aws-kms does not accept local-sealed or pkcs11 fields",
							));
						}
						let region = region.ok_or_else(|| {
							serde::de::Error::custom(
								"crypto.root_kek_provider kind=aws-kms requires region",
							)
						})?;
						let key_id = key_id.ok_or_else(|| {
							serde::de::Error::custom(
								"crypto.root_kek_provider kind=aws-kms requires key_id",
							)
						})?;
						Ok(Self::AwsKms(AwsKmsRootKekConfig {
							region,
							key_id,
							endpoint,
							access_key_id_env: access_key_id_env
								.unwrap_or_else(default_aws_kms_access_key_id_env),
							secret_access_key_env: secret_access_key_env
								.unwrap_or_else(default_aws_kms_secret_access_key_env),
							session_token_env: session_token_env
								.unwrap_or_else(default_aws_kms_session_token_env),
						}))
					}
					"pkcs11" => {
						if unlock_secret_env.is_some()
							|| unlock_secret_file.is_some()
							|| region.is_some() || key_id.is_some()
							|| endpoint.is_some() || access_key_id_env.is_some()
							|| secret_access_key_env.is_some()
							|| session_token_env.is_some()
						{
							return Err(serde::de::Error::custom(
								"crypto.root_kek_provider kind=pkcs11 does not accept local-sealed or aws-kms fields",
							));
						}
						let library_path = library_path.ok_or_else(|| {
							serde::de::Error::custom(
								"crypto.root_kek_provider kind=pkcs11 requires library_path",
							)
						})?;
						let token_label = token_label.ok_or_else(|| {
							serde::de::Error::custom(
								"crypto.root_kek_provider kind=pkcs11 requires token_label",
							)
						})?;
						let key_label = key_label.ok_or_else(|| {
							serde::de::Error::custom(
								"crypto.root_kek_provider kind=pkcs11 requires key_label",
							)
						})?;
						let pin_env = pin_env.ok_or_else(|| {
							serde::de::Error::custom(
								"crypto.root_kek_provider kind=pkcs11 requires pin_env",
							)
						})?;
						Ok(Self::Pkcs11(Pkcs11RootKekConfig {
							library_path,
							token_label,
							key_label,
							pin_env,
							slot,
						}))
					}
					other => Err(serde::de::Error::custom(format!(
						"unknown crypto.root_kek_provider kind '{other}'"
					))),
				}
			}
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootKekBootstrapMode {
	GenerateOrCreate,
	EnrollExisting,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootKekProviderHealth {
	pub ready: bool,
	pub detail: Option<String>,
}

impl RootKekProviderHealth {
	pub fn ready() -> Self {
		Self {
			ready: true,
			detail: None,
		}
	}

	pub fn degraded(detail: impl Into<String>) -> Self {
		Self {
			ready: false,
			detail: Some(detail.into()),
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RootKekProviderInit<'a> {
	data_dir: &'a Path,
	bootstrap_mode: RootKekBootstrapMode,
}

impl<'a> RootKekProviderInit<'a> {
	fn generate_or_create(data_dir: &'a Path) -> Self {
		Self {
			data_dir,
			bootstrap_mode: RootKekBootstrapMode::GenerateOrCreate,
		}
	}
}

impl LocalSealedRootKekConfig {
	pub(crate) fn absolutize_from_config_dir(&mut self, config_path: &Path) {
		if let Some(path) = self.unlock_secret_file.as_mut() {
			if !path.is_absolute() {
				*path = config_path
					.parent()
					.unwrap_or_else(|| Path::new("."))
					.join(&*path);
			}
		}
	}

	fn validate(&self) -> std::io::Result<()> {
		match (
			self.unlock_secret_env.as_deref(),
			self.unlock_secret_file.as_ref(),
		) {
			(Some(env), None) => {
				if env.trim().is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"crypto.root_kek_provider.unlock_secret_env must not be empty",
					));
				}
				if std::env::var_os(env).is_none() {
					return Err(Error::new(
						ErrorKind::NotFound,
						format!(
							"crypto.root_kek_provider.unlock_secret_env '{}' is not set",
							env
						),
					));
				}
				Ok(())
			}
			(None, Some(path)) => {
				if !path.is_file() {
					return Err(Error::new(
						ErrorKind::NotFound,
						format!(
							"crypto.root_kek_provider.unlock_secret_file is not an existing file: {}",
							path.display()
						),
					));
				}
				Ok(())
			}
			(Some(_), Some(_)) => Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.root_kek_provider local-sealed requires exactly one of unlock_secret_env or unlock_secret_file",
			)),
			(None, None) => Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.root_kek_provider local-sealed requires unlock_secret_env or unlock_secret_file",
			)),
		}
	}

	fn load_unlock_secret(&self) -> std::io::Result<Zeroizing<Vec<u8>>> {
		match (
			self.unlock_secret_env.as_deref(),
			self.unlock_secret_file.as_ref(),
		) {
			(Some(env), None) => {
				let secret = std::env::var(env).map_err(|error| {
					Error::new(
						ErrorKind::NotFound,
						format!(
							"failed to load crypto.root_kek_provider.unlock_secret_env '{}': {error}",
							env
						),
					)
				})?;
				let secret = Zeroizing::new(secret.into_bytes());
				if secret.is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						format!(
							"crypto.root_kek_provider.unlock_secret_env '{}' must not be empty",
							env
						),
					));
				}
				Ok(secret)
			}
			(None, Some(path)) => {
				let secret = Zeroizing::new(std::fs::read(path).map_err(|error| {
					Error::new(
						error.kind(),
						format!(
							"failed to read crypto.root_kek_provider.unlock_secret_file {}: {error}",
							path.display()
						),
					)
				})?);
				if secret.is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						format!(
							"crypto.root_kek_provider.unlock_secret_file {} must not be empty",
							path.display()
						),
					));
				}
				Ok(secret)
			}
			(Some(_), Some(_)) => Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.root_kek_provider local-sealed requires exactly one of unlock_secret_env or unlock_secret_file",
			)),
			(None, None) => Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.root_kek_provider local-sealed requires unlock_secret_env or unlock_secret_file",
			)),
		}
	}
}

impl AwsKmsRootKekConfig {
	fn validate(&self) -> std::io::Result<()> {
		if self.region.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.root_kek_provider.region must not be empty for aws-kms",
			));
		}
		if self.key_id.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.root_kek_provider.key_id must not be empty for aws-kms",
			));
		}
		for (field, value) in [
			("access_key_id_env", self.access_key_id_env.as_str()),
			("secret_access_key_env", self.secret_access_key_env.as_str()),
			("session_token_env", self.session_token_env.as_str()),
		] {
			if value.trim().is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!("crypto.root_kek_provider.{field} must not be empty for aws-kms"),
				));
			}
		}
		if let Some(endpoint) = self.endpoint.as_deref() {
			let url = Url::parse(endpoint).map_err(|error| {
				Error::new(
					ErrorKind::InvalidInput,
					format!(
						"crypto.root_kek_provider.endpoint is not a valid URL for aws-kms: {error}"
					),
				)
			})?;
			match url.scheme() {
				"http" | "https" => {}
				other => {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						format!(
							"crypto.root_kek_provider.endpoint for aws-kms must use http or https, got '{other}'"
						),
					));
				}
			}
			if url.host_str().is_none() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"crypto.root_kek_provider.endpoint for aws-kms must include a host",
				));
			}
			if url.path() != "/" && !url.path().is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"crypto.root_kek_provider.endpoint for aws-kms must not include a path",
				));
			}
		}
		Ok(())
	}

	fn load_credentials(&self) -> std::io::Result<AwsKmsCredentials> {
		let access_key_id = std::env::var(&self.access_key_id_env).map_err(|error| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"failed to load crypto.root_kek_provider.access_key_id_env '{}': {error}",
					self.access_key_id_env
				),
			)
		})?;
		if access_key_id.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"crypto.root_kek_provider.access_key_id_env '{}' must not be empty",
					self.access_key_id_env
				),
			));
		}
		let secret_access_key = std::env::var(&self.secret_access_key_env).map_err(|error| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"failed to load crypto.root_kek_provider.secret_access_key_env '{}': {error}",
					self.secret_access_key_env
				),
			)
		})?;
		if secret_access_key.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"crypto.root_kek_provider.secret_access_key_env '{}' must not be empty",
					self.secret_access_key_env
				),
			));
		}
		let session_token = match std::env::var(&self.session_token_env) {
			Ok(value) if !value.trim().is_empty() => Some(value),
			Ok(_) => None,
			Err(std::env::VarError::NotPresent) => None,
			Err(error) => {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"failed to load crypto.root_kek_provider.session_token_env '{}': {error}",
						self.session_token_env
					),
				));
			}
		};
		Ok(AwsKmsCredentials {
			access_key_id,
			secret_access_key,
			session_token,
		})
	}
}

impl Pkcs11RootKekConfig {
	pub(crate) fn absolutize_from_config_dir(&mut self, config_path: &Path) {
		if !self.library_path.is_absolute() {
			self.library_path = config_path
				.parent()
				.unwrap_or_else(|| Path::new("."))
				.join(&self.library_path);
		}
	}

	fn validate(&self) -> std::io::Result<()> {
		if !self.library_path.is_file() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"crypto.root_kek_provider.library_path is not an existing file for pkcs11: {}",
					self.library_path.display()
				),
			));
		}
		for (field, value) in [
			("token_label", self.token_label.as_str()),
			("key_label", self.key_label.as_str()),
			("pin_env", self.pin_env.as_str()),
		] {
			if value.trim().is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!("crypto.root_kek_provider.{field} must not be empty for pkcs11"),
				));
			}
		}
		if std::env::var_os(&self.pin_env).is_none() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"crypto.root_kek_provider.pin_env '{}' is not set for pkcs11",
					self.pin_env
				),
			));
		}
		Ok(())
	}
}

impl RootKekProviderConfig {
	pub fn label(&self) -> &'static str {
		match self {
			Self::DevInsecure => "dev-insecure",
			Self::LocalSealed(_) => "local-sealed",
			Self::AwsKms(_) => "aws-kms",
			Self::Pkcs11(_) => "pkcs11",
		}
	}

	pub(crate) fn absolutize_from_config_dir(&mut self, config_path: &Path) {
		match self {
			Self::LocalSealed(cfg) => cfg.absolutize_from_config_dir(config_path),
			Self::Pkcs11(cfg) => cfg.absolutize_from_config_dir(config_path),
			Self::DevInsecure | Self::AwsKms(_) => {}
		}
	}

	pub(crate) fn validate(&self) -> std::io::Result<()> {
		match self {
			Self::DevInsecure => Ok(()),
			Self::LocalSealed(cfg) => cfg.validate(),
			Self::AwsKms(cfg) => cfg.validate(),
			Self::Pkcs11(cfg) => cfg.validate(),
		}
	}

	pub(crate) fn build(&self, data_dir: &Path) -> std::io::Result<Arc<dyn RootKekProvider>> {
		self.initialize(RootKekProviderInit::generate_or_create(data_dir))
	}

	pub(crate) fn initialize(
		&self,
		init: RootKekProviderInit<'_>,
	) -> std::io::Result<Arc<dyn RootKekProvider>> {
		match self {
			Self::DevInsecure => {
				#[cfg(feature = "fips")]
				{
					return Err(Error::new(
						ErrorKind::Unsupported,
						"crypto.root_kek_provider=dev-insecure is disabled in fips builds",
					));
				}
				#[cfg(not(feature = "fips"))]
				{
					Ok(Arc::new(DevRootKekProvider::initialize(init)?))
				}
			}
			Self::LocalSealed(cfg) => {
				Ok(Arc::new(LocalSealedRootKekProvider::initialize(init, cfg)?))
			}
			Self::AwsKms(cfg) => Ok(Arc::new(AwsKmsRootKekProvider::initialize(init, cfg)?)),
			Self::Pkcs11(cfg) => Err(Error::new(
				ErrorKind::Unsupported,
				format!(
					"crypto.root_kek_provider=pkcs11 is not implemented yet; provider seam only (library_path='{}', token_label='{}', key_label='{}')",
					cfg.library_path.display(),
					cfg.token_label,
					cfg.key_label,
				),
			)),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CryptoCacheConfig {
	pub tenant_kek_cache_max_entries: usize,
	pub slab_dek_cache_max_entries: usize,
}

impl Default for CryptoCacheConfig {
	fn default() -> Self {
		Self {
			tenant_kek_cache_max_entries: DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES,
			slab_dek_cache_max_entries: DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES,
		}
	}
}

impl CryptoCacheConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.tenant_kek_cache_max_entries == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.tenant_kek_cache_max_entries must be greater than zero",
			));
		}
		if self.slab_dek_cache_max_entries == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"crypto.slab_dek_cache_max_entries must be greater than zero",
			));
		}
		Ok(())
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrappedTenantKek {
	#[serde(with = "serde_bytes")]
	pub wrapped_key: Vec<u8>,
}

mod active_tenant_kek_id_wire {
	use crate::schema::TenantKekId;
	use serde::{Deserialize, Deserializer, Serializer};

	pub fn serialize<S>(value: &Option<TenantKekId>, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(value.map(TenantKekId::get).unwrap_or(0))
	}

	pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<TenantKekId>, D::Error>
	where
		D: Deserializer<'de>,
	{
		let raw = u64::deserialize(deserializer)?;
		if raw == 0 {
			Ok(None)
		} else {
			TenantKekId::try_from(raw)
				.map(Some)
				.map_err(serde::de::Error::custom)
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantKeyset {
	#[serde(with = "active_tenant_kek_id_wire")]
	pub active_tenant_kek_id: Option<TenantKekId>,
	pub keys: BTreeMap<TenantKekId, WrappedTenantKek>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallTenantKekVersion {
	pub tenant_id: TenantId,
	pub tenant_kek_id: TenantKekId,
	#[serde(with = "serde_bytes")]
	pub wrapped_tenant_kek: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireTenantKekVersion {
	pub tenant_id: TenantId,
	pub tenant_kek_id: TenantKekId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedTieredSlab {
	pub bytes: Vec<u8>,
	pub tenant_id: TenantId,
	pub tenant_kek_id: TenantKekId,
	pub cipher_id: CipherId,
	pub chunk_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecryptedTieredSlab {
	pub plaintext: Vec<u8>,
	pub tenant_id: TenantId,
	pub slab_id: SlabId,
	pub tenant_kek_id: TenantKekId,
	pub cipher_id: CipherId,
	pub chunk_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FooterSummary {
	pub tenant_id: TenantId,
	pub slab_id: SlabId,
	pub tenant_kek_id: TenantKekId,
	pub cipher_id: CipherId,
	pub chunk_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredOffsetIndexEntry {
	pub key_locator: IdentityPrefix,
	pub byte_offset: u64,
	pub byte_length: u32,
	pub flags: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredSlabObjectMetadata {
	pub tenant_id: TenantId,
	/// Transitional until the slab footer carries the full in-memory HLC.
	pub created_hlc: u64,
	pub offset_index: Vec<TieredOffsetIndexEntry>,
}

pub struct TieredRangeReadPlan {
	footer: FooterSummary,
	tenant_id: TenantId,
	slab_id: SlabId,
	cipher_id: CipherId,
	plaintext_len: usize,
	chunk_range: Range<usize>,
	base_plaintext_offset: usize,
	requested_plaintext_range: Range<usize>,
	ciphertext_range_in_object: Range<u64>,
	prepared_slab_dek: Arc<PreparedSlabDek>,
}

impl TieredRangeReadPlan {
	pub fn footer(&self) -> FooterSummary {
		self.footer
	}

	pub fn ciphertext_range_in_object(&self) -> Range<u64> {
		self.ciphertext_range_in_object.clone()
	}

	pub fn decrypt(&self, ciphertext: &[u8]) -> std::io::Result<Vec<u8>> {
		let expected_ciphertext_len =
			(self.ciphertext_range_in_object.end - self.ciphertext_range_in_object.start) as usize;
		if ciphertext.len() != expected_ciphertext_len {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				format!(
					"tiered range read expected {expected_ciphertext_len} ciphertext bytes, got {}",
					ciphertext.len()
				),
			));
		}
		let decrypted = decrypt_partial_chunks(
			self.cipher_id,
			self.tenant_id,
			self.slab_id,
			self.prepared_slab_dek.clone(),
			self.plaintext_len,
			ciphertext,
			self.chunk_range.clone(),
		)?;
		let relative_start = self
			.requested_plaintext_range
			.start
			.checked_sub(self.base_plaintext_offset)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"requested plaintext range underflow",
				)
			})?;
		let relative_end = self
			.requested_plaintext_range
			.end
			.checked_sub(self.base_plaintext_offset)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"requested plaintext range underflow",
				)
			})?;
		Ok(decrypted[relative_start..relative_end].to_vec())
	}
}

#[derive(Clone)]
pub struct CryptoManager {
	root_kek: Arc<dyn RootKekProvider>,
	keysets: Arc<Mutex<HashMap<TenantId, TenantKeyset>>>,
	tenant_kek_cache: Arc<Mutex<KeyCache<(TenantId, TenantKekId), LockedKeyMaterial>>>,
	slab_dek_cache: Arc<Mutex<KeyCache<(TenantId, SlabId, CipherId), PreparedSlabDek>>>,
	audit_context: Arc<Mutex<Option<CryptoAuditContext>>>,
}

#[derive(Clone)]
struct CryptoAuditContext {
	node_id: NodeId,
	sink: Arc<dyn AuditSink>,
}

impl std::fmt::Debug for CryptoManager {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CryptoManager")
			.field("selected_cipher", &selected_cipher_id().label())
			.field("root_kek_provider", &self.root_kek_provider_label())
			.finish()
	}
}

impl CryptoManager {
	pub(crate) fn new(root_kek: Arc<dyn RootKekProvider>) -> Self {
		Self::new_with_cache_config(root_kek, CryptoCacheConfig::default())
	}

	pub(crate) fn new_with_cache_config(
		root_kek: Arc<dyn RootKekProvider>,
		cache_config: CryptoCacheConfig,
	) -> Self {
		Self {
			root_kek,
			keysets: Arc::new(Mutex::new(HashMap::new())),
			tenant_kek_cache: Arc::new(Mutex::new(KeyCache::new(
				cache_config.tenant_kek_cache_max_entries,
				DEFAULT_TENANT_KEK_IDLE_TTL,
				Some(DEFAULT_TENANT_KEK_ABSOLUTE_TTL),
			))),
			slab_dek_cache: Arc::new(Mutex::new(KeyCache::new(
				cache_config.slab_dek_cache_max_entries,
				DEFAULT_SLAB_DEK_IDLE_TTL,
				None,
			))),
			audit_context: Arc::new(Mutex::new(None)),
		}
	}

	/// Dev/test-only helper. Production bootstrap must select a root
	/// KEK provider explicitly through `NodeBootstrapConfig`.
	#[cfg(test)]
	pub(crate) fn new_dev() -> Self {
		Self::new(Arc::new(
			DevRootKekProvider::new_ephemeral_for_tests()
				.expect("dev root KEK provider must build"),
		))
	}

	pub fn selected_cipher_label(&self) -> &'static str {
		selected_cipher_id().label()
	}

	pub fn selected_cipher_detection_method(&self) -> &'static str {
		selected_cipher_detection_method()
	}

	pub fn root_kek_provider_label(&self) -> &'static str {
		self.root_kek.label()
	}

	pub fn root_kek_provider_health(&self) -> std::io::Result<RootKekProviderHealth> {
		self.root_kek.health()
	}

	pub fn mlock_status_label(&self) -> &'static str {
		mlock_status_label()
	}

	pub(crate) fn attach_audit_sink(
		&self,
		node_id: NodeId,
		sink: Arc<dyn AuditSink>,
	) -> std::io::Result<()> {
		*self.lock_audit_context()? = Some(CryptoAuditContext { node_id, sink });
		Ok(())
	}

	/// Destructively replace the local in-memory keysets from an
	/// authoritative Raft-backed snapshot. Callers must only use this
	/// before Raft can emit tenant-KEK observer callbacks, or from an
	/// observer callback that already owns the committed snapshot being
	/// applied.
	pub(crate) fn replace_keysets_from_authoritative_snapshot(
		&self,
		snapshot: &HashMap<TenantId, TenantKeyset>,
	) -> std::io::Result<()> {
		let mut guard = self.lock_keysets()?;
		*guard = snapshot.clone();
		drop(guard);
		self.invalidate_all_caches()
	}

	pub fn tenant_keysets_snapshot(&self) -> std::io::Result<HashMap<TenantId, TenantKeyset>> {
		Ok(self.lock_keysets()?.clone())
	}

	/// Test-only helper that seeds local in-memory keysets without a
	/// Raft commit. Production code must never call this path.
	pub(crate) fn install_local_only_for_tests(&self, tenant_id: TenantId) -> std::io::Result<()> {
		let exists = self
			.lock_keysets()?
			.get(&tenant_id)
			.map(|keyset| !keyset.keys.is_empty())
			.unwrap_or(false);
		if exists {
			return Ok(());
		}
		let rotation = self.prepare_rotation(tenant_id)?;
		self.apply_rotation(&rotation)
	}

	pub fn prepare_rotation(
		&self,
		tenant_id: TenantId,
	) -> std::io::Result<InstallTenantKekVersion> {
		let tenant_kek = random_256_bit_key()?;
		let tenant_kek_id = self.mint_unused_tenant_kek_id(tenant_id)?;
		let wrapped_tenant_kek = self.root_kek.wrap_tenant_kek(&tenant_kek)?;
		let unwrapped_tenant_kek = self.root_kek.unwrap_tenant_kek(&wrapped_tenant_kek)?;
		if unwrapped_tenant_kek.as_ref() != tenant_kek.as_ref() {
			return Err(Error::other(
				"root KEK provider returned mismatched tenant KEK bytes after wrap/unwrap self-check",
			));
		}
		Ok(InstallTenantKekVersion {
			tenant_id,
			tenant_kek_id,
			wrapped_tenant_kek,
		})
	}

	fn mint_unused_tenant_kek_id(&self, tenant_id: TenantId) -> std::io::Result<TenantKekId> {
		let guard = self.lock_keysets()?;
		let existing = guard.get(&tenant_id).map(|keyset| &keyset.keys);
		for _ in 0..32 {
			let candidate = random_tenant_kek_id("tenant_kek_id")?;
			if existing
				.map(|keys| !keys.contains_key(&candidate))
				.unwrap_or(true)
			{
				return Ok(candidate);
			}
		}
		Err(Error::other(format!(
			"failed to mint an unused tenant KEK id for tenant {} after 32 attempts",
			tenant_id.get()
		)))
	}

	pub(crate) fn apply_rotation(&self, rotation: &InstallTenantKekVersion) -> std::io::Result<()> {
		let mut guard = self.lock_keysets()?;
		let entry = guard.entry(rotation.tenant_id).or_default();
		match entry.keys.entry(rotation.tenant_kek_id) {
			std::collections::btree_map::Entry::Occupied(existing) => {
				if existing.get().wrapped_key != rotation.wrapped_tenant_kek {
					return Err(Error::new(
						ErrorKind::AlreadyExists,
						format!(
							"tenant {} tenant_kek_id {} already exists with different wrapped key bytes",
							rotation.tenant_id.get(),
							rotation.tenant_kek_id
						),
					));
				}
			}
			std::collections::btree_map::Entry::Vacant(slot) => {
				slot.insert(WrappedTenantKek {
					wrapped_key: rotation.wrapped_tenant_kek.clone(),
				});
			}
		}
		entry.active_tenant_kek_id = Some(rotation.tenant_kek_id);
		drop(guard);
		self.invalidate_tenant(rotation.tenant_id)
	}

	pub fn active_tenant_kek_id(&self, tenant_id: TenantId) -> std::io::Result<TenantKekId> {
		let guard = self.lock_keysets()?;
		let keyset = guard.get(&tenant_id).ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"no tenant KEK has been provisioned for tenant {}",
					tenant_id.get()
				),
			)
		})?;
		keyset.active_tenant_kek_id.ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!("tenant {} does not have an active KEK", tenant_id.get()),
			)
		})
	}

	pub fn encrypt_tiered_slab_object(
		&self,
		slab_id: SlabId,
		metadata: &TieredSlabObjectMetadata,
		plaintext: &[u8],
	) -> std::io::Result<EncryptedTieredSlab> {
		let (tenant_kek_id, tenant_kek) = self.get_active_tenant_kek(metadata.tenant_id)?;
		let wrapped_slab_dek = SlabDek::fresh_for(slab_id)?.wrap(&tenant_kek)?;
		let cipher_id = selected_cipher_id();
		let chunk_count = chunk_count_for_len(plaintext.len())?;
		let encrypted_chunks = encrypt_chunks(
			cipher_id,
			metadata.tenant_id,
			wrapped_slab_dek,
			chunk_count,
			plaintext,
		)?;
		let header_bytes = encode_adr022_header(
			slab_id,
			metadata.tenant_id,
			metadata.created_hlc,
			cipher_id,
			chunk_count,
		)?;
		let index_bytes = encode_adr022_offset_index(&metadata.offset_index, plaintext.len())?;
		let record_count = u64::try_from(metadata.offset_index.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered offset index record count exceeds u64::MAX",
			)
		})?;
		let mut footer = encode_adr022_footer(
			slab_id,
			metadata.tenant_id,
			metadata.created_hlc,
			cipher_id,
			chunk_count,
			record_count,
			tenant_kek_id,
			&encrypted_chunks.wrapped_slab_dek,
		)?;
		let footer_len_u32 = ADR_TIERED_SLAB_FOOTER_BYTES as u32;
		let index_len_u32 = u32::try_from(index_bytes.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered offset index exceeds u32::MAX bytes",
			)
		})?;
		let surface_hmac = compute_adr022_surface_hmac(
			&tenant_kek,
			&header_bytes,
			&index_bytes,
			&footer[..ADR_TIERED_SLAB_HMAC_OFFSET],
			&footer[ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES..],
			footer_len_u32,
			index_len_u32,
		);
		footer
			[ADR_TIERED_SLAB_HMAC_OFFSET..ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES]
			.copy_from_slice(surface_hmac.as_ref());

		let total_len = ADR_TIERED_SLAB_MAGIC_HEAD
			.len()
			.checked_add(header_bytes.len())
			.and_then(|n| n.checked_add(encrypted_chunks.ciphertext.len()))
			.and_then(|n| n.checked_add(index_bytes.len()))
			.and_then(|n| n.checked_add(footer.len()))
			.and_then(|n| n.checked_add(ADR_TIERED_SLAB_TAIL_BYTES))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tiered slab length overflow"))?;
		let mut out = Vec::with_capacity(total_len);
		out.extend_from_slice(&ADR_TIERED_SLAB_MAGIC_HEAD);
		out.extend_from_slice(&header_bytes);
		out.extend_from_slice(&encrypted_chunks.ciphertext);
		out.extend_from_slice(&index_bytes);
		out.extend_from_slice(&footer);
		push_u32_be(&mut out, footer_len_u32);
		push_u32_be(&mut out, index_len_u32);
		out.extend_from_slice(&ADR_TIERED_SLAB_MAGIC_TAIL);

		{
			let mut slab_cache = self.lock_slab_dek_cache()?;
			slab_cache.insert(
				(metadata.tenant_id, slab_id, cipher_id),
				encrypted_chunks.cache_key,
			);
		}

		Ok(EncryptedTieredSlab {
			bytes: out,
			tenant_id: metadata.tenant_id,
			tenant_kek_id,
			cipher_id,
			chunk_count,
		})
	}

	pub fn decrypt_tiered_slab(
		&self,
		expected_slab_id: SlabId,
		blob: &[u8],
	) -> std::io::Result<DecryptedTieredSlab> {
		let parsed = match parse_encrypted_footer(blob) {
			Ok(parsed) => parsed,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, None, "footer_parse", &error);
				return Err(error);
			}
		};
		let footer = parsed.footer_summary();
		if parsed.slab_id != expected_slab_id {
			let error = Error::new(
				ErrorKind::InvalidData,
				format!(
					"tiered slab footer slab_id {} does not match expected {}",
					parsed.slab_id, expected_slab_id
				),
			);
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "footer_parse", &error);
			return Err(error);
		}
		if parsed.nonce_version != SLAB_NONCE_VERSION {
			let error = Error::new(
				ErrorKind::InvalidData,
				format!(
					"unsupported nonce version {} in tiered slab {}",
					parsed.nonce_version, expected_slab_id
				),
			);
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "footer_parse", &error);
			return Err(error);
		}
		let tenant_kek = match self.get_tenant_kek(parsed.tenant_id, parsed.tenant_kek_id) {
			Ok(tenant_kek) => tenant_kek,
			Err(error) => {
				self.emit_decrypt_rejected(
					expected_slab_id,
					Some(footer),
					"tenant_kek_lookup",
					&error,
				);
				return Err(error);
			}
		};
		if let Err(error) = verify_parsed_surface_hmac(&tenant_kek, &parsed) {
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
			return Err(error);
		}
		let slab_dek = match self.resolve_prepared_slab_dek(
			parsed.tenant_id,
			parsed.tenant_kek_id,
			parsed.slab_id,
			parsed.cipher_id,
			&parsed.wrapped_slab_dek,
		) {
			Ok(prepared) => prepared,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
				return Err(error);
			}
		};
		let plaintext = match decrypt_chunks(
			parsed.cipher_id,
			parsed.tenant_id,
			parsed.slab_id,
			slab_dek,
			parsed.chunk_count,
			parsed.plaintext_len,
			parsed.ciphertext,
		) {
			Ok(plaintext) => plaintext,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, Some(footer), "aead_chunk", &error);
				return Err(error);
			}
		};
		Ok(DecryptedTieredSlab {
			plaintext,
			tenant_id: parsed.tenant_id,
			slab_id: parsed.slab_id,
			tenant_kek_id: parsed.tenant_kek_id,
			cipher_id: parsed.cipher_id,
			chunk_count: parsed.chunk_count,
		})
	}

	pub fn additional_tiered_metadata_range(
		&self,
		expected_slab_id: SlabId,
		object_size_bytes: u64,
		tail_window: &[u8],
	) -> std::io::Result<Option<Range<u64>>> {
		let parsed = match parse_encrypted_footer_from_tail_window(object_size_bytes, tail_window) {
			Ok(parsed) => parsed,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, None, "footer_parse", &error);
				return Err(error);
			}
		};
		let footer = parsed.footer_summary();
		if parsed.slab_id != expected_slab_id {
			let error = Error::new(
				ErrorKind::InvalidData,
				format!(
					"tiered slab footer slab_id {} does not match expected {}",
					parsed.slab_id, expected_slab_id
				),
			);
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "footer_parse", &error);
			return Err(error);
		}
		if parsed.nonce_version != SLAB_NONCE_VERSION {
			let error = Error::new(
				ErrorKind::InvalidData,
				format!(
					"unsupported nonce version {} in tiered slab {}",
					parsed.nonce_version, expected_slab_id
				),
			);
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "footer_parse", &error);
			return Err(error);
		}
		Ok(parsed.missing_index_range())
	}

	pub fn plan_tiered_range_read(
		&self,
		expected_slab_id: SlabId,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		plaintext_range: Range<usize>,
	) -> std::io::Result<TieredRangeReadPlan> {
		let parsed = match parse_encrypted_footer_from_tail_window(object_size_bytes, tail_window) {
			Ok(parsed) => parsed,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, None, "footer_parse", &error);
				return Err(error);
			}
		};
		let footer = parsed.footer_summary();
		if parsed.slab_id != expected_slab_id {
			let error = Error::new(
				ErrorKind::InvalidData,
				format!(
					"tiered slab footer slab_id {} does not match expected {}",
					parsed.slab_id, expected_slab_id
				),
			);
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "footer_parse", &error);
			return Err(error);
		}
		if parsed.nonce_version != SLAB_NONCE_VERSION {
			let error = Error::new(
				ErrorKind::InvalidData,
				format!(
					"unsupported nonce version {} in tiered slab {}",
					parsed.nonce_version, expected_slab_id
				),
			);
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "footer_parse", &error);
			return Err(error);
		}
		if plaintext_range.start > plaintext_range.end || plaintext_range.end > parsed.plaintext_len
		{
			let error = Error::new(
				ErrorKind::InvalidInput,
				format!(
					"requested plaintext range {}..{} is outside slab plaintext length {}",
					plaintext_range.start, plaintext_range.end, parsed.plaintext_len
				),
			);
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "range_plan", &error);
			return Err(error);
		}
		let tenant_kek = match self.get_tenant_kek(parsed.tenant_id, parsed.tenant_kek_id) {
			Ok(tenant_kek) => tenant_kek,
			Err(error) => {
				self.emit_decrypt_rejected(
					expected_slab_id,
					Some(footer),
					"tenant_kek_lookup",
					&error,
				);
				return Err(error);
			}
		};
		if let Err(error) = verify_tail_surface_hmac(&tenant_kek, &parsed, additional_metadata) {
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
			return Err(error);
		}
		if let Err(error) = verify_index_contains_range(
			parsed.index_bytes(additional_metadata)?,
			plaintext_range.clone(),
		) {
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "range_plan", &error);
			return Err(error);
		}
		let prepared_slab_dek = match self.resolve_prepared_slab_dek(
			parsed.tenant_id,
			parsed.tenant_kek_id,
			parsed.slab_id,
			parsed.cipher_id,
			&parsed.wrapped_slab_dek,
		) {
			Ok(prepared) => prepared,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
				return Err(error);
			}
		};

		let start_chunk = plaintext_range.start / SLAB_ENCRYPTION_CHUNK_BYTES;
		let end_chunk = if plaintext_range.start == plaintext_range.end {
			start_chunk.saturating_add(1)
		} else {
			((plaintext_range.end - 1) / SLAB_ENCRYPTION_CHUNK_BYTES).saturating_add(1)
		};
		let chunk_range = start_chunk..end_chunk;
		let base_plaintext_offset =
			plaintext_bounds_for_chunk(parsed.plaintext_len, chunk_range.start).start;
		let ciphertext_start = parsed.ciphertext_offset_in_object
			+ ciphertext_offset_for_chunk(parsed.plaintext_len, chunk_range.start) as u64;
		let ciphertext_end = parsed.ciphertext_offset_in_object
			+ ciphertext_offset_for_chunk(parsed.plaintext_len, chunk_range.end) as u64;
		Ok(TieredRangeReadPlan {
			footer,
			tenant_id: parsed.tenant_id,
			slab_id: parsed.slab_id,
			cipher_id: parsed.cipher_id,
			plaintext_len: parsed.plaintext_len,
			chunk_range,
			base_plaintext_offset,
			requested_plaintext_range: plaintext_range,
			ciphertext_range_in_object: ciphertext_start..ciphertext_end,
			prepared_slab_dek,
		})
	}

	pub fn encrypted_footer_summary(&self, blob: &[u8]) -> std::io::Result<Option<FooterSummary>> {
		if !is_encrypted_tiered_blob(blob) {
			return Ok(None);
		}
		let parsed = parse_encrypted_footer(blob)?;
		Ok(Some(parsed.footer_summary()))
	}

	fn resolve_prepared_slab_dek(
		&self,
		tenant_id: TenantId,
		tenant_kek_id: TenantKekId,
		slab_id: SlabId,
		cipher_id: CipherId,
		wrapped_slab_dek: &[u8],
	) -> std::io::Result<Arc<PreparedSlabDek>> {
		{
			let mut slab_cache = self.lock_slab_dek_cache()?;
			let cache_key = (tenant_id, slab_id, cipher_id);
			if let Some(prepared) = slab_cache.get(&cache_key) {
				return Ok(prepared);
			}
		}
		let tenant_kek = self.get_tenant_kek(tenant_id, tenant_kek_id)?;
		let unwrapped = unwrap_key(&tenant_kek, wrapped_slab_dek)?;
		let prepared = Arc::new(PreparedSlabDek::new(
			cipher_id,
			Arc::new(LockedKeyMaterial::new(unwrapped)?),
		)?);
		let mut slab_cache = self.lock_slab_dek_cache()?;
		slab_cache.insert((tenant_id, slab_id, cipher_id), prepared.clone());
		Ok(prepared)
	}

	fn get_tenant_kek(
		&self,
		tenant_id: TenantId,
		tenant_kek_id: TenantKekId,
	) -> std::io::Result<Arc<LockedKeyMaterial>> {
		{
			let mut cache = self.lock_tenant_kek_cache()?;
			if let Some(key) = cache.get(&(tenant_id, tenant_kek_id)) {
				metrics().tenant_kek_cache_hits_total.inc();
				return Ok(key);
			}
		}

		let wrapped = {
			let guard = self.lock_keysets()?;
			let keyset = guard.get(&tenant_id).ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!("tenant {} has no wrapped KEK state", tenant_id.get()),
				)
			})?;
			keyset.keys.get(&tenant_kek_id).cloned().ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"tenant {} does not have tenant_kek_id {}",
						tenant_id.get(),
						tenant_kek_id
					),
				)
			})?
		};

		self.unwrap_and_cache_tenant_kek(tenant_id, tenant_kek_id, &wrapped)
	}

	fn get_active_tenant_kek(
		&self,
		tenant_id: TenantId,
	) -> std::io::Result<(TenantKekId, Arc<LockedKeyMaterial>)> {
		let (tenant_kek_id, wrapped) = {
			let guard = self.lock_keysets()?;
			let keyset = guard.get(&tenant_id).ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"no tenant KEK has been provisioned for tenant {}",
						tenant_id.get()
					),
				)
			})?;
			let tenant_kek_id = keyset.active_tenant_kek_id.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!("tenant {} does not have an active KEK", tenant_id.get()),
				)
			})?;
			let wrapped = keyset.keys.get(&tenant_kek_id).cloned().ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"tenant {} active tenant_kek_id {} is missing wrapped key state",
						tenant_id.get(),
						tenant_kek_id
					),
				)
			})?;
			(tenant_kek_id, wrapped)
		};
		let tenant_kek = self.unwrap_and_cache_tenant_kek(tenant_id, tenant_kek_id, &wrapped)?;
		Ok((tenant_kek_id, tenant_kek))
	}

	fn unwrap_and_cache_tenant_kek(
		&self,
		tenant_id: TenantId,
		tenant_kek_id: TenantKekId,
		wrapped: &WrappedTenantKek,
	) -> std::io::Result<Arc<LockedKeyMaterial>> {
		{
			let mut cache = self.lock_tenant_kek_cache()?;
			if let Some(key) = cache.get(&(tenant_id, tenant_kek_id)) {
				metrics().tenant_kek_cache_hits_total.inc();
				return Ok(key);
			}
		}
		metrics().tenant_kek_cache_misses_total.inc();

		let unwrapped = self.root_kek.unwrap_tenant_kek(&wrapped.wrapped_key)?;
		let locked = Arc::new(LockedKeyMaterial::new(unwrapped)?);
		let mut cache = self.lock_tenant_kek_cache()?;
		if let Some(key) = cache.get(&(tenant_id, tenant_kek_id)) {
			return Ok(key);
		}
		cache.insert((tenant_id, tenant_kek_id), locked.clone());
		Ok(locked)
	}

	fn invalidate_all_caches(&self) -> std::io::Result<()> {
		self.lock_tenant_kek_cache()?.clear();
		self.lock_slab_dek_cache()?.clear();
		Ok(())
	}

	fn invalidate_tenant(&self, tenant_id: TenantId) -> std::io::Result<()> {
		self.lock_tenant_kek_cache()?
			.invalidate_where(|(entry_tenant, _)| *entry_tenant == tenant_id);
		self.lock_slab_dek_cache()?
			.invalidate_where(|(entry_tenant, _, _)| *entry_tenant == tenant_id);
		Ok(())
	}

	fn lock_keysets(
		&self,
	) -> std::io::Result<std::sync::MutexGuard<'_, HashMap<TenantId, TenantKeyset>>> {
		self.keysets
			.lock()
			.map_err(|_| Error::other("crypto tenant keyset mutex poisoned"))
	}

	fn lock_tenant_kek_cache(
		&self,
	) -> std::io::Result<
		std::sync::MutexGuard<'_, KeyCache<(TenantId, TenantKekId), LockedKeyMaterial>>,
	> {
		self.tenant_kek_cache
			.lock()
			.map_err(|_| Error::other("tenant KEK cache mutex poisoned"))
	}

	fn lock_slab_dek_cache(
		&self,
	) -> std::io::Result<
		std::sync::MutexGuard<'_, KeyCache<(TenantId, SlabId, CipherId), PreparedSlabDek>>,
	> {
		self.slab_dek_cache
			.lock()
			.map_err(|_| Error::other("slab DEK cache mutex poisoned"))
	}

	fn lock_audit_context(
		&self,
	) -> std::io::Result<std::sync::MutexGuard<'_, Option<CryptoAuditContext>>> {
		self.audit_context
			.lock()
			.map_err(|_| Error::other("crypto audit context mutex poisoned"))
	}

	fn emit_decrypt_rejected(
		&self,
		expected_slab_id: SlabId,
		footer: Option<FooterSummary>,
		stage: &'static str,
		error: &std::io::Error,
	) {
		let context = match self.lock_audit_context() {
			Ok(guard) => guard.clone(),
			Err(lock_error) => {
				crate::metrics::global()
					.crypto_decrypt_audit_emit_failures_total
					.inc();
				tracing::error!(
					error = %lock_error,
					"crypto decrypt rejection could not acquire audit context"
				);
				return;
			}
		};
		let Some(context) = context else {
			return;
		};
		let message = match footer {
			Some(footer) => format!(
				"failure_stage={} expected_slab_id={} footer_slab_id={} tenant_id={} tenant_kek_id={} cipher_id={} chunk_count={} error={}",
				stage,
				expected_slab_id,
				footer.slab_id,
				footer.tenant_id.get(),
				footer.tenant_kek_id.get(),
				footer.cipher_id.to_wire(),
				footer.chunk_count,
				error
			),
			None => format!(
				"failure_stage={} expected_slab_id={} error={}",
				stage, expected_slab_id, error
			),
		};
		let mut audit = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CryptoDecryptRejected,
			context.node_id,
			message,
		)
		.with_outcome(AuditOutcome::Error)
		.with_parameter("failure_stage", stage)
		.with_parameter("expected_slab_id", expected_slab_id.get())
		.with_parameter("error", error.to_string());
		if let Some(footer) = footer {
			audit = audit
				.with_parameter("footer_slab_id", footer.slab_id.get())
				.with_parameter("tenant_id", footer.tenant_id.get())
				.with_parameter("tenant_kek_id", footer.tenant_kek_id.get())
				.with_parameter("cipher_id", footer.cipher_id.to_wire() as u64)
				.with_parameter("chunk_count", footer.chunk_count as u64);
		}
		if let Err(emit_error) = context.sink.emit(audit) {
			// Decrypt rejections are the forensic signal for a tampered slab.
			// A sink-write failure (full disk, fs remounted ro, fsync error)
			// drops a security event that ADR 025 expects to be durable, so
			// surface it as an error and bump a metric an alert can fire on.
			crate::metrics::global()
				.crypto_decrypt_audit_emit_failures_total
				.inc();
			tracing::error!(
				error = %emit_error,
				"crypto decrypt rejection audit emission failed"
			);
		}
	}
}

#[async_trait]
impl crate::consensus::TenantKekObserver for CryptoManager {
	async fn on_tenant_kek_rotated(
		&self,
		rotation: &InstallTenantKekVersion,
		_applied_log_index: u64,
	) -> std::io::Result<()> {
		self.apply_rotation(rotation)
	}

	async fn sync_tenant_keysets(
		&self,
		snapshot: &HashMap<TenantId, TenantKeyset>,
	) -> std::io::Result<()> {
		self.replace_keysets_from_authoritative_snapshot(snapshot)
	}
}

pub fn selected_cipher_id() -> CipherId {
	static SELECTED: OnceLock<CipherId> = OnceLock::new();
	*SELECTED.get_or_init(detect_cipher_once)
}

pub fn selected_cipher_detection_method() -> &'static str {
	#[cfg(feature = "fips")]
	{
		"compile_time"
	}
	#[cfg(not(feature = "fips"))]
	{
		"feature_probe"
	}
}

pub fn mlock_status_label() -> &'static str {
	static STATUS: OnceLock<&'static str> = OnceLock::new();
	STATUS.get_or_init(|| {
		let mut probe = [0_u8; 32];
		match best_effort_mlock(probe.as_mut_ptr().cast(), probe.len()) {
			Ok(()) => {
				unsafe {
					libc::munlock(probe.as_mut_ptr().cast(), probe.len());
				}
				"ok"
			}
			Err(_) => "unavailable",
		}
	})
}

#[doc(hidden)]
pub mod test_support {
	use super::*;

	pub fn seeded_dev_crypto_manager(tenant_id: TenantId) -> std::io::Result<CryptoManager> {
		let crypto = CryptoManager::new(Arc::new(DevRootKekProvider::new_ephemeral_for_tests()?));
		crypto.install_local_only_for_tests(tenant_id)?;
		Ok(crypto)
	}

	#[cfg(any(test, not(feature = "fips")))]
	pub fn persistent_dev_crypto_manager(data_dir: &Path) -> std::io::Result<CryptoManager> {
		Ok(CryptoManager::new(Arc::new(
			DevRootKekProvider::from_dev_insecure_dir(data_dir)?,
		)))
	}
}

pub fn is_encrypted_tiered_blob(blob: &[u8]) -> bool {
	blob.starts_with(&ADR_TIERED_SLAB_MAGIC_HEAD)
}

pub fn encode_chunk_nonce(chunk_index: u64) -> [u8; SLAB_NONCE_BYTES] {
	let mut nonce = [0_u8; SLAB_NONCE_BYTES];
	nonce[..4].copy_from_slice(&SLAB_NONCE_VERSION.to_be_bytes());
	nonce[4..].copy_from_slice(&chunk_index.to_be_bytes());
	nonce
}

pub fn encode_chunk_aad(
	tenant_id: TenantId,
	slab_id: SlabId,
	chunk_index: u64,
	slab_format_version: u32,
	cipher_id: CipherId,
) -> [u8; SLAB_AAD_BYTES] {
	let mut aad = [0_u8; SLAB_AAD_BYTES];
	aad[..16].copy_from_slice(&encode_u64_aad_field(tenant_id.get()));
	aad[16..32].copy_from_slice(&encode_u64_aad_field(slab_id.get()));
	aad[32..40].copy_from_slice(&chunk_index.to_be_bytes());
	aad[40..44].copy_from_slice(&slab_format_version.to_be_bytes());
	aad[44] = cipher_id.to_wire();
	aad
}

fn encode_u64_aad_field(value: u64) -> [u8; SLAB_AAD_WIDE_FIELD_BYTES] {
	let mut out = [0_u8; SLAB_AAD_WIDE_FIELD_BYTES];
	// ADR 019 fixes the tenant_id/slab_id AAD fields at 16 bytes each.
	// Wendelon canonically projects the current u64 ids into those 16-byte slots
	// by zero-extending into the high eight bytes and storing the BE u64 in
	// the low eight bytes.
	out[SLAB_AAD_WIDE_FIELD_BYTES - std::mem::size_of::<u64>()..]
		.copy_from_slice(&value.to_be_bytes());
	out
}

fn detect_cipher_once() -> CipherId {
	#[cfg(feature = "fips")]
	{
		CipherId::Aes256GcmInjective
	}
	#[cfg(not(feature = "fips"))]
	{
		let probe = [0_u8; 32];
		if UnboundKey::new(&AES_256_GCM_SIV, &probe).is_ok() {
			CipherId::Aes256GcmSiv
		} else {
			CipherId::Aes256GcmInjective
		}
	}
}

fn algorithm_for_cipher(
	cipher_id: CipherId,
) -> std::io::Result<&'static aws_lc_rs::aead::Algorithm> {
	match cipher_id {
		CipherId::Aes256GcmSiv => {
			#[cfg(feature = "fips")]
			{
				Err(Error::new(
					ErrorKind::Unsupported,
					"AES-256-GCM-SIV is compiled out in the fips build profile",
				))
			}
			#[cfg(not(feature = "fips"))]
			{
				Ok(&AES_256_GCM_SIV)
			}
		}
		CipherId::Aes256GcmInjective => Ok(&AES_256_GCM),
	}
}

struct SlabDek {
	slab_id: SlabId,
	key: KeyMaterialBytes,
}

impl SlabDek {
	fn fresh_for(slab_id: SlabId) -> std::io::Result<Self> {
		Ok(Self {
			slab_id,
			key: random_256_bit_key()?,
		})
	}

	fn wrap(self, tenant_kek: &LockedKeyMaterial) -> std::io::Result<WrappedSlabDek> {
		let wrapped_slab_dek = wrap_key(tenant_kek, &self.key)?;
		Ok(WrappedSlabDek {
			slab_id: self.slab_id,
			key: self.key,
			wrapped_slab_dek,
		})
	}
}

struct WrappedSlabDek {
	slab_id: SlabId,
	key: KeyMaterialBytes,
	wrapped_slab_dek: Vec<u8>,
}

struct EncryptedChunks {
	ciphertext: Vec<u8>,
	wrapped_slab_dek: Vec<u8>,
	cache_key: Arc<PreparedSlabDek>,
}

struct PreparedSlabDek {
	key: Arc<LockedKeyMaterial>,
	aead: LessSafeKey,
}

impl PreparedSlabDek {
	fn new(cipher_id: CipherId, key: Arc<LockedKeyMaterial>) -> std::io::Result<Self> {
		let aead = LessSafeKey::new(
			UnboundKey::new(algorithm_for_cipher(cipher_id)?, key.as_ref().as_ref())
				.map_err(|_| Error::other("failed to construct prepared AEAD key for slab DEK"))?,
		);
		Ok(Self { key, aead })
	}

	fn aead(&self) -> &LessSafeKey {
		debug_assert_eq!(self.key.as_ref().as_ref().len(), 32);
		&self.aead
	}
}

struct ChunkSealBuffer<'a> {
	body: &'a mut [u8],
	suffix: &'a mut [u8],
	suffix_len: usize,
}

impl<'a> ChunkSealBuffer<'a> {
	fn new(body: &'a mut [u8], suffix: &'a mut [u8]) -> Self {
		Self {
			body,
			suffix,
			suffix_len: 0,
		}
	}

	fn suffix_len(&self) -> usize {
		self.suffix_len
	}
}

impl AsMut<[u8]> for ChunkSealBuffer<'_> {
	fn as_mut(&mut self) -> &mut [u8] {
		self.body
	}
}

impl<'a> Extend<&'a u8> for ChunkSealBuffer<'_> {
	fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
		for byte in iter {
			assert!(
				self.suffix_len < self.suffix.len(),
				"AEAD append exceeded reserved tag capacity"
			);
			self.suffix[self.suffix_len] = *byte;
			self.suffix_len += 1;
		}
	}
}

fn encrypt_chunk_range(
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	plaintext: &[u8],
	chunk_range: std::ops::Range<usize>,
	out: &mut [u8],
) -> std::io::Result<()> {
	let base_ciphertext_offset = ciphertext_offset_for_chunk(plaintext.len(), chunk_range.start);
	for chunk_index in chunk_range {
		let plaintext_bounds = plaintext_bounds_for_chunk(plaintext.len(), chunk_index);
		let local_cipher_start =
			ciphertext_offset_for_chunk(plaintext.len(), chunk_index) - base_ciphertext_offset;
		let local_cipher_end =
			ciphertext_offset_for_chunk(plaintext.len(), chunk_index + 1) - base_ciphertext_offset;
		let (chunk_buf, tag_buf) =
			out[local_cipher_start..local_cipher_end].split_at_mut(plaintext_bounds.len());
		chunk_buf.copy_from_slice(&plaintext[plaintext_bounds.clone()]);
		let mut in_out = ChunkSealBuffer::new(chunk_buf, tag_buf);
		// Safety: `LessSafeKey` is sound here because slab encryption mints a
		// fresh DEK per slab, and chunk nonces are injectively derived from the
		// chunk index within that slab. The `(key, nonce)` pair cannot repeat.
		prepared
			.aead()
			.seal_in_place_append_tag(
				Nonce::assume_unique_for_key(encode_chunk_nonce(chunk_index as u64)),
				Aad::from(encode_chunk_aad(
					tenant_id,
					slab_id,
					chunk_index as u64,
					SLAB_ENVELOPE_FORMAT_VERSION,
					cipher_id,
				)),
				&mut in_out,
			)
			.map_err(|_| Error::other("slab chunk encrypt failed"))?;
		debug_assert_eq!(in_out.suffix_len(), SLAB_AEAD_TAG_BYTES);
	}
	Ok(())
}

fn encrypt_chunks(
	cipher_id: CipherId,
	tenant_id: TenantId,
	dek: WrappedSlabDek,
	chunk_count: u32,
	plaintext: &[u8],
) -> std::io::Result<EncryptedChunks> {
	let WrappedSlabDek {
		slab_id,
		key: dek_key,
		wrapped_slab_dek,
	} = dek;
	let prepared = Arc::new(PreparedSlabDek::new(
		cipher_id,
		Arc::new(LockedKeyMaterial::new(dek_key)?),
	)?);
	let ciphertext_len = plaintext
		.len()
		.checked_add(chunk_count as usize * SLAB_AEAD_TAG_BYTES)
		.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "ciphertext length overflow"))?;
	let mut out = vec![0_u8; ciphertext_len];
	let chunk_count = chunk_count as usize;
	let workers = parallel_chunk_worker_count(chunk_count);
	if workers <= 1 || chunk_count == 0 {
		encrypt_chunk_range(
			prepared.as_ref(),
			cipher_id,
			tenant_id,
			slab_id,
			plaintext,
			0..chunk_count,
			out.as_mut_slice(),
		)?;
	} else {
		let ranges = parallel_chunk_ranges(chunk_count, workers);
		thread::scope(|scope| -> std::io::Result<()> {
			let mut handles = Vec::with_capacity(ranges.len());
			let mut tail = out.as_mut_slice();
			let mut consumed = 0_usize;
			for range in ranges {
				let region_start = ciphertext_offset_for_chunk(plaintext.len(), range.start);
				let region_end = ciphertext_offset_for_chunk(plaintext.len(), range.end);
				debug_assert_eq!(region_start, consumed);
				let (worker_out, rest) = tail.split_at_mut(region_end - consumed);
				tail = rest;
				consumed = region_end;
				let prepared = prepared.clone();
				handles.push(scope.spawn(move || {
					encrypt_chunk_range(
						prepared.as_ref(),
						cipher_id,
						tenant_id,
						slab_id,
						plaintext,
						range,
						worker_out,
					)
				}));
			}
			for handle in handles {
				let result = handle
					.join()
					.map_err(|_| Error::other("parallel slab encrypt worker panicked"))?;
				result?;
			}
			Ok(())
		})?;
	}
	Ok(EncryptedChunks {
		ciphertext: out,
		wrapped_slab_dek,
		cache_key: prepared,
	})
}

fn decrypt_chunk_range(
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	plaintext_len: usize,
	ciphertext: &[u8],
	chunk_range: std::ops::Range<usize>,
	out: &mut [u8],
) -> std::io::Result<()> {
	let base_plaintext_offset = plaintext_bounds_for_chunk(plaintext_len, chunk_range.start).start;
	let mut scratch = vec![0_u8; SLAB_ENCRYPTION_CHUNK_BYTES + SLAB_AEAD_TAG_BYTES];
	for chunk_index in chunk_range {
		let plaintext_bounds = plaintext_bounds_for_chunk(plaintext_len, chunk_index);
		let ciphertext_start = ciphertext_offset_for_chunk(plaintext_len, chunk_index);
		let ciphertext_end = ciphertext_offset_for_chunk(plaintext_len, chunk_index + 1);
		let encrypted_len = ciphertext_end - ciphertext_start;
		scratch[..encrypted_len].copy_from_slice(&ciphertext[ciphertext_start..ciphertext_end]);
		// Safety: decrypt mirrors the same nonce discipline as encrypt: one DEK
		// per slab and an injective chunk-index nonce mapping for that slab.
		let opened = prepared
			.aead()
			.open_in_place(
				Nonce::assume_unique_for_key(encode_chunk_nonce(chunk_index as u64)),
				Aad::from(encode_chunk_aad(
					tenant_id,
					slab_id,
					chunk_index as u64,
					SLAB_ENVELOPE_FORMAT_VERSION,
					cipher_id,
				)),
				&mut scratch[..encrypted_len],
			)
			.map_err(|_| Error::new(ErrorKind::InvalidData, "slab chunk decrypt failed"))?;
		let local_plaintext_start = plaintext_bounds.start - base_plaintext_offset;
		let local_plaintext_end = local_plaintext_start + plaintext_bounds.len();
		out[local_plaintext_start..local_plaintext_end].copy_from_slice(opened);
	}
	Ok(())
}

fn decrypt_chunks(
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	prepared: Arc<PreparedSlabDek>,
	chunk_count: u32,
	plaintext_len: usize,
	ciphertext: &[u8],
) -> std::io::Result<Vec<u8>> {
	if ciphertext_offset_for_chunk(plaintext_len, chunk_count as usize) != ciphertext.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ciphertext length disagrees with slab footer chunk layout",
		));
	}
	let mut plaintext = vec![0_u8; plaintext_len];
	let chunk_count = chunk_count as usize;
	let workers = parallel_chunk_worker_count(chunk_count);
	if workers <= 1 || chunk_count == 0 {
		decrypt_chunk_range(
			prepared.as_ref(),
			cipher_id,
			tenant_id,
			slab_id,
			plaintext_len,
			ciphertext,
			0..chunk_count,
			plaintext.as_mut_slice(),
		)?;
	} else {
		let ranges = parallel_chunk_ranges(chunk_count, workers);
		thread::scope(|scope| -> std::io::Result<()> {
			let mut handles = Vec::with_capacity(ranges.len());
			let mut tail = plaintext.as_mut_slice();
			let mut consumed = 0_usize;
			for range in ranges {
				let region_start = plaintext_bounds_for_chunk(plaintext_len, range.start).start;
				let region_end = plaintext_bounds_for_chunk(plaintext_len, range.end).start;
				debug_assert_eq!(region_start, consumed);
				let (worker_out, rest) = tail.split_at_mut(region_end - consumed);
				tail = rest;
				consumed = region_end;
				let prepared = prepared.clone();
				handles.push(scope.spawn(move || {
					decrypt_chunk_range(
						prepared.as_ref(),
						cipher_id,
						tenant_id,
						slab_id,
						plaintext_len,
						ciphertext,
						range,
						worker_out,
					)
				}));
			}
			for handle in handles {
				let result = handle
					.join()
					.map_err(|_| Error::other("parallel slab decrypt worker panicked"))?;
				result?;
			}
			Ok(())
		})?;
	}
	Ok(plaintext)
}

fn decrypt_chunk_range_from_slice(
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	plaintext_len: usize,
	ciphertext: &[u8],
	chunk_range: Range<usize>,
	out: &mut [u8],
) -> std::io::Result<()> {
	let base_plaintext_offset = plaintext_bounds_for_chunk(plaintext_len, chunk_range.start).start;
	let base_ciphertext_offset = ciphertext_offset_for_chunk(plaintext_len, chunk_range.start);
	let mut scratch = vec![0_u8; SLAB_ENCRYPTION_CHUNK_BYTES + SLAB_AEAD_TAG_BYTES];
	for chunk_index in chunk_range {
		let plaintext_bounds = plaintext_bounds_for_chunk(plaintext_len, chunk_index);
		let ciphertext_start =
			ciphertext_offset_for_chunk(plaintext_len, chunk_index) - base_ciphertext_offset;
		let ciphertext_end =
			ciphertext_offset_for_chunk(plaintext_len, chunk_index + 1) - base_ciphertext_offset;
		let encrypted_len = ciphertext_end - ciphertext_start;
		scratch[..encrypted_len].copy_from_slice(&ciphertext[ciphertext_start..ciphertext_end]);
		let opened = prepared
			.aead()
			.open_in_place(
				Nonce::assume_unique_for_key(encode_chunk_nonce(chunk_index as u64)),
				Aad::from(encode_chunk_aad(
					tenant_id,
					slab_id,
					chunk_index as u64,
					SLAB_ENVELOPE_FORMAT_VERSION,
					cipher_id,
				)),
				&mut scratch[..encrypted_len],
			)
			.map_err(|_| Error::new(ErrorKind::InvalidData, "slab chunk decrypt failed"))?;
		let local_plaintext_start = plaintext_bounds.start - base_plaintext_offset;
		let local_plaintext_end = local_plaintext_start + plaintext_bounds.len();
		out[local_plaintext_start..local_plaintext_end].copy_from_slice(opened);
	}
	Ok(())
}

fn decrypt_partial_chunks(
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	prepared: Arc<PreparedSlabDek>,
	plaintext_len: usize,
	ciphertext: &[u8],
	chunk_range: Range<usize>,
) -> std::io::Result<Vec<u8>> {
	if chunk_range.is_empty() {
		return Ok(Vec::new());
	}
	let expected_ciphertext_len = ciphertext_offset_for_chunk(plaintext_len, chunk_range.end)
		.checked_sub(ciphertext_offset_for_chunk(
			plaintext_len,
			chunk_range.start,
		))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ciphertext range underflow"))?;
	if expected_ciphertext_len != ciphertext.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ciphertext length disagrees with requested chunk range",
		));
	}
	let base_plaintext_offset = plaintext_bounds_for_chunk(plaintext_len, chunk_range.start).start;
	let end_plaintext_offset = plaintext_bounds_for_chunk(plaintext_len, chunk_range.end).start;
	let mut plaintext = vec![0_u8; end_plaintext_offset.saturating_sub(base_plaintext_offset)];
	decrypt_chunk_range_from_slice(
		prepared.as_ref(),
		cipher_id,
		tenant_id,
		slab_id,
		plaintext_len,
		ciphertext,
		chunk_range,
		plaintext.as_mut_slice(),
	)?;
	Ok(plaintext)
}

fn adr022_surface_hmac_payload(
	header_bytes: &[u8],
	index_bytes: &[u8],
	footer_prefix_without_hmac: &[u8],
	footer_suffix_after_hmac: &[u8],
	footer_len: u32,
	index_len: u32,
) -> Vec<u8> {
	let mut payload = Vec::with_capacity(
		ADR_TIERED_SLAB_MAGIC_HEAD.len()
			+ header_bytes.len()
			+ index_bytes.len()
			+ footer_prefix_without_hmac.len()
			+ footer_suffix_after_hmac.len()
			+ ADR_TIERED_SLAB_TAIL_BYTES,
	);
	payload.extend_from_slice(&ADR_TIERED_SLAB_MAGIC_HEAD);
	payload.extend_from_slice(header_bytes);
	payload.extend_from_slice(index_bytes);
	payload.extend_from_slice(footer_prefix_without_hmac);
	payload.extend_from_slice(footer_suffix_after_hmac);
	payload.extend_from_slice(&footer_len.to_be_bytes());
	payload.extend_from_slice(&index_len.to_be_bytes());
	payload.extend_from_slice(&ADR_TIERED_SLAB_MAGIC_TAIL);
	payload
}

fn compute_adr022_surface_hmac(
	tenant_kek: &LockedKeyMaterial,
	header_bytes: &[u8],
	index_bytes: &[u8],
	footer_prefix_without_hmac: &[u8],
	footer_suffix_after_hmac: &[u8],
	footer_len: u32,
	index_len: u32,
) -> hmac::Tag {
	let key = hmac::Key::new(hmac::HMAC_SHA256, tenant_kek.as_ref());
	let payload = adr022_surface_hmac_payload(
		header_bytes,
		index_bytes,
		footer_prefix_without_hmac,
		footer_suffix_after_hmac,
		footer_len,
		index_len,
	);
	hmac::sign(&key, &payload)
}

fn verify_adr022_surface_hmac(
	tenant_kek: &LockedKeyMaterial,
	header_bytes: &[u8],
	index_bytes: &[u8],
	footer_prefix_without_hmac: &[u8],
	footer_suffix_after_hmac: &[u8],
	footer_len: u32,
	index_len: u32,
	expected: &[u8],
) -> std::io::Result<()> {
	let key = hmac::Key::new(hmac::HMAC_SHA256, tenant_kek.as_ref());
	let payload = adr022_surface_hmac_payload(
		header_bytes,
		index_bytes,
		footer_prefix_without_hmac,
		footer_suffix_after_hmac,
		footer_len,
		index_len,
	);
	hmac::verify(&key, &payload, expected).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"tiered slab footer HMAC failed before DEK unwrap",
		)
	})
}

fn wrap_key(kek: &LockedKeyMaterial, plaintext_key: &KeyMaterialBytes) -> std::io::Result<Vec<u8>> {
	let mut out = vec![0_u8; plaintext_key.as_ref().len() + 15];
	let wrapped = AesKek::new(&AES_256, kek.as_ref())
		.map_err(|_| Error::other("failed to construct AES-KWP KEK"))?
		.wrap_with_padding(plaintext_key.as_ref(), &mut out)
		.map_err(|_| Error::other("failed to wrap key with AES-KWP"))?;
	Ok(wrapped.to_vec())
}

fn unwrap_key(kek: &LockedKeyMaterial, wrapped_key: &[u8]) -> std::io::Result<KeyMaterialBytes> {
	let mut out = Zeroizing::new(vec![0_u8; wrapped_key.len()]);
	let plaintext = AesKek::new(&AES_256, kek.as_ref())
		.map_err(|_| Error::other("failed to construct AES-KWP KEK"))?
		.unwrap_with_padding(wrapped_key, &mut out)
		.map_err(|_| Error::new(ErrorKind::InvalidData, "failed to unwrap key with AES-KWP"))?;
	KeyMaterialBytes::copy_from_slice(plaintext, "unwrapped key")
}

fn random_256_bit_key() -> std::io::Result<KeyMaterialBytes> {
	KeyMaterialBytes::random()
}

fn random_tenant_kek_id(context: &str) -> std::io::Result<TenantKekId> {
	loop {
		let mut out = [0_u8; 8];
		aws_lc_rs::rand::fill(&mut out)
			.map_err(|_| Error::other(format!("failed to generate random {context}")))?;
		if let Some(value) = TenantKekId::try_new(u64::from_be_bytes(out)) {
			return Ok(value);
		}
	}
}

fn chunk_count_for_len(len: usize) -> std::io::Result<u32> {
	let chunks = len.div_ceil(SLAB_ENCRYPTION_CHUNK_BYTES);
	u32::try_from(chunks).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"slab chunk count exceeds u32::MAX for tiered encryption",
		)
	})
}

fn parallel_chunk_worker_count(chunk_count: usize) -> usize {
	let available_parallelism = thread::available_parallelism()
		.map(|parallelism| parallelism.get())
		.unwrap_or(1);
	parallel_chunk_worker_count_for(chunk_count, available_parallelism)
}

fn parallel_chunk_worker_count_for(chunk_count: usize, available_parallelism: usize) -> usize {
	if chunk_count < PARALLEL_CHUNK_CRYPTO_MIN_CHUNKS {
		return 1;
	}
	chunk_count
		.min(available_parallelism.max(1))
		.min(PARALLEL_CHUNK_CRYPTO_MAX_WORKERS)
		.max(1)
}

fn parallel_chunk_ranges(chunk_count: usize, workers: usize) -> Vec<std::ops::Range<usize>> {
	if chunk_count == 0 {
		return Vec::new();
	}
	let workers = workers.clamp(1, chunk_count);
	let base = chunk_count / workers;
	let remainder = chunk_count % workers;
	let mut next = 0_usize;
	let mut ranges = Vec::with_capacity(workers);
	for worker in 0..workers {
		let width = base + usize::from(worker < remainder);
		let end = next + width;
		ranges.push(next..end);
		next = end;
	}
	ranges
}

fn plaintext_bounds_for_chunk(plaintext_len: usize, chunk_index: usize) -> std::ops::Range<usize> {
	let start = chunk_index
		.checked_mul(SLAB_ENCRYPTION_CHUNK_BYTES)
		.expect("chunk_index bounds checked by caller")
		.min(plaintext_len);
	let end = start
		.saturating_add(SLAB_ENCRYPTION_CHUNK_BYTES)
		.min(plaintext_len);
	start..end
}

fn ciphertext_offset_for_chunk(plaintext_len: usize, chunk_index: usize) -> usize {
	let plaintext_offset = chunk_index
		.checked_mul(SLAB_ENCRYPTION_CHUNK_BYTES)
		.expect("chunk_index bounds checked by caller")
		.min(plaintext_len);
	let tag_offset = chunk_index
		.checked_mul(SLAB_AEAD_TAG_BYTES)
		.expect("chunk_index bounds checked by caller");
	plaintext_offset + tag_offset
}

fn push_u32_be(buf: &mut Vec<u8>, value: u32) {
	buf.extend_from_slice(&value.to_be_bytes());
}

fn push_u64_be(buf: &mut Vec<u8>, value: u64) {
	buf.extend_from_slice(&value.to_be_bytes());
}

struct ParsedEncryptedFooterFields {
	slab_id: SlabId,
	tenant_id: TenantId,
	tenant_kek_id: TenantKekId,
	cipher_id: CipherId,
	chunk_count: u32,
	plaintext_len: usize,
	nonce_version: u32,
	wrapped_slab_dek: Vec<u8>,
	surface_hmac: Vec<u8>,
	footer_prefix_without_hmac: Vec<u8>,
	footer_suffix_after_hmac: Vec<u8>,
	footer_len_u32: u32,
	index_len_u32: u32,
	ciphertext_offset_in_object: u64,
	header_bytes_for_hmac: Vec<u8>,
	index_from_tail: Option<Vec<u8>>,
	missing_index_range_in_object: Option<Range<u64>>,
	#[cfg(test)]
	wrapped_slab_dek_range_in_footer: std::ops::Range<usize>,
}

struct ParsedEncryptedFooter<'a> {
	slab_id: SlabId,
	tenant_id: TenantId,
	tenant_kek_id: TenantKekId,
	cipher_id: CipherId,
	chunk_count: u32,
	plaintext_len: usize,
	nonce_version: u32,
	wrapped_slab_dek: Vec<u8>,
	surface_hmac: Vec<u8>,
	footer_prefix_without_hmac: Vec<u8>,
	footer_suffix_after_hmac: Vec<u8>,
	footer_len_u32: u32,
	index_len_u32: u32,
	header_bytes_for_hmac: &'a [u8],
	index: &'a [u8],
	ciphertext: &'a [u8],
	#[cfg(test)]
	wrapped_slab_dek_range_in_blob: std::ops::Range<usize>,
}

impl ParsedEncryptedFooterFields {
	fn finalize_from_ciphertext_len(mut self, ciphertext_len: usize) -> std::io::Result<Self> {
		self.plaintext_len = plaintext_len_from_ciphertext_len(ciphertext_len, self.chunk_count)?;
		Ok(self)
	}

	fn footer_summary(&self) -> FooterSummary {
		FooterSummary {
			tenant_id: self.tenant_id,
			slab_id: self.slab_id,
			tenant_kek_id: self.tenant_kek_id,
			cipher_id: self.cipher_id,
			chunk_count: self.chunk_count,
		}
	}

	fn missing_index_range(&self) -> Option<Range<u64>> {
		self.missing_index_range_in_object.clone()
	}

	fn index_bytes<'a>(
		&'a self,
		additional_metadata: Option<&'a [u8]>,
	) -> std::io::Result<&'a [u8]> {
		if let Some(bytes) = self.index_from_tail.as_deref() {
			return Ok(bytes);
		}
		let Some(range) = self.missing_index_range_in_object.as_ref() else {
			return Ok(&[]);
		};
		let bytes = additional_metadata.ok_or_else(|| {
			Error::new(
				ErrorKind::UnexpectedEof,
				"tiered slab range read requires additional footer index bytes",
			)
		})?;
		let expected_len = usize::try_from(range.end - range.start).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab index span does not fit into usize",
			)
		})?;
		if bytes.len() != expected_len {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				format!(
					"tiered slab index fetch expected {expected_len} bytes, got {}",
					bytes.len()
				),
			));
		}
		Ok(bytes)
	}
}

impl ParsedEncryptedFooter<'_> {
	fn footer_summary(&self) -> FooterSummary {
		FooterSummary {
			tenant_id: self.tenant_id,
			slab_id: self.slab_id,
			tenant_kek_id: self.tenant_kek_id,
			cipher_id: self.cipher_id,
			chunk_count: self.chunk_count,
		}
	}
}

fn verify_parsed_surface_hmac(
	tenant_kek: &LockedKeyMaterial,
	parsed: &ParsedEncryptedFooter<'_>,
) -> std::io::Result<()> {
	verify_adr022_surface_hmac(
		tenant_kek,
		parsed.header_bytes_for_hmac,
		parsed.index,
		&parsed.footer_prefix_without_hmac,
		&parsed.footer_suffix_after_hmac,
		parsed.footer_len_u32,
		parsed.index_len_u32,
		parsed.surface_hmac.as_ref(),
	)
}

fn verify_tail_surface_hmac(
	tenant_kek: &LockedKeyMaterial,
	parsed: &ParsedEncryptedFooterFields,
	additional_metadata: Option<&[u8]>,
) -> std::io::Result<()> {
	verify_adr022_surface_hmac(
		tenant_kek,
		&parsed.header_bytes_for_hmac,
		parsed.index_bytes(additional_metadata)?,
		&parsed.footer_prefix_without_hmac,
		&parsed.footer_suffix_after_hmac,
		parsed.footer_len_u32,
		parsed.index_len_u32,
		parsed.surface_hmac.as_ref(),
	)
}

fn encode_adr022_u64_slot(dst: &mut [u8], value: u64) -> std::io::Result<()> {
	if dst.len() < std::mem::size_of::<u64>() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"ADR 022 slot shorter than u64 payload",
		));
	}
	dst[..8].copy_from_slice(&value.to_be_bytes());
	for byte in &mut dst[8..] {
		*byte = 0;
	}
	Ok(())
}

fn decode_adr022_u64_slot(src: &[u8], field: &str) -> std::io::Result<u64> {
	if src.len() < std::mem::size_of::<u64>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("ADR 022 field {field} shorter than u64 payload"),
		));
	}
	if src[8..].iter().any(|byte| *byte != 0) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("ADR 022 field {field} has non-zero padding"),
		));
	}
	Ok(u64::from_be_bytes(src[..8].try_into().expect("slice len")))
}

fn encode_adr022_header(
	slab_id: SlabId,
	tenant_id: TenantId,
	created_hlc: u64,
	cipher_id: CipherId,
	chunk_count: u32,
) -> std::io::Result<[u8; ADR_TIERED_SLAB_HEADER_BYTES]> {
	let mut header = [0_u8; ADR_TIERED_SLAB_HEADER_BYTES];
	header[..4].copy_from_slice(&SLAB_ENVELOPE_FORMAT_VERSION.to_be_bytes());
	header[4] = cipher_id.to_wire();
	encode_adr022_u64_slot(
		&mut header[8..8 + ADR_TIERED_SLAB_WIDE_ID_BYTES],
		slab_id.get(),
	)?;
	encode_adr022_u64_slot(
		&mut header[24..24 + ADR_TIERED_SLAB_WIDE_ID_BYTES],
		tenant_id.get(),
	)?;
	header[40..48].copy_from_slice(&created_hlc.to_be_bytes());
	header[48..52].copy_from_slice(&ADR_TIERED_SLAB_CHUNK_SIZE_BYTES.to_be_bytes());
	header[52..56].copy_from_slice(&chunk_count.to_be_bytes());
	let header_crc = u64::from(crc32c(&header[..56]));
	header[56..64].copy_from_slice(&header_crc.to_be_bytes());
	Ok(header)
}

fn encode_adr022_footer(
	slab_id: SlabId,
	tenant_id: TenantId,
	created_hlc: u64,
	cipher_id: CipherId,
	chunk_count: u32,
	record_count: u64,
	tenant_kek_id: TenantKekId,
	wrapped_slab_dek: &[u8],
) -> std::io::Result<[u8; ADR_TIERED_SLAB_FOOTER_BYTES]> {
	if wrapped_slab_dek.len() != ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"wrapped slab DEK must be {} bytes for ADR 022, got {}",
				ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES,
				wrapped_slab_dek.len()
			),
		));
	}
	let mut footer = [0_u8; ADR_TIERED_SLAB_FOOTER_BYTES];
	footer[..4].copy_from_slice(&SLAB_ENVELOPE_FORMAT_VERSION.to_be_bytes());
	footer[4] = cipher_id.to_wire();
	encode_adr022_u64_slot(&mut footer[8..24], slab_id.get())?;
	encode_adr022_u64_slot(&mut footer[24..40], tenant_id.get())?;
	footer[40..48].copy_from_slice(&created_hlc.to_be_bytes());
	footer[48..52].copy_from_slice(&ADR_TIERED_SLAB_CHUNK_SIZE_BYTES.to_be_bytes());
	footer[52..56].copy_from_slice(&chunk_count.to_be_bytes());
	footer[56..64].copy_from_slice(&record_count.to_be_bytes());
	encode_adr022_u64_slot(&mut footer[64..96], tenant_kek_id.get())?;
	footer[96..96 + wrapped_slab_dek.len()].copy_from_slice(wrapped_slab_dek);
	Ok(footer)
}

fn verify_index_contains_range(
	index_bytes: &[u8],
	plaintext_range: Range<usize>,
) -> std::io::Result<()> {
	// Cold tiered reads currently request exactly one record span from
	// `RecordLocation`, so the ADR 022 footer index must contain the
	// same record-aligned `(offset, length)` pair. A future multi-record
	// pushdown path will need a separate coverage-based validator.
	verify_adr022_index_contains_range(index_bytes, plaintext_range)
}

fn verify_adr022_index_contains_range(
	index_bytes: &[u8],
	plaintext_range: Range<usize>,
) -> std::io::Result<()> {
	if index_bytes.len() % ADR_TIERED_SLAB_INDEX_ENTRY_BYTES != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 offset index length is not an integral number of entries",
		));
	}
	let expected_offset = u64::try_from(plaintext_range.start).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"requested plaintext range start does not fit into u64",
		)
	})?;
	let expected_len = u32::try_from(plaintext_range.end.saturating_sub(plaintext_range.start))
		.map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"requested plaintext range length does not fit into u32",
			)
		})?;
	for entry in index_bytes.chunks_exact(ADR_TIERED_SLAB_INDEX_ENTRY_BYTES) {
		let byte_offset = u64::from_be_bytes(entry[16..24].try_into().expect("slice len"));
		let byte_length = u32::from_be_bytes(entry[24..28].try_into().expect("slice len"));
		if byte_offset == expected_offset && byte_length == expected_len {
			return Ok(());
		}
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		format!(
			"ADR 022 footer index does not contain requested plaintext range {}..{}",
			plaintext_range.start, plaintext_range.end
		),
	))
}

fn encode_adr022_offset_index(
	entries: &[TieredOffsetIndexEntry],
	plaintext_len: usize,
) -> std::io::Result<Vec<u8>> {
	let capacity = entries
		.len()
		.checked_mul(ADR_TIERED_SLAB_INDEX_ENTRY_BYTES)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered offset index length overflow",
			)
		})?;
	let mut out = Vec::with_capacity(capacity);
	for entry in entries {
		let end = entry
			.byte_offset
			.checked_add(entry.byte_length as u64)
			.ok_or_else(|| {
				Error::new(ErrorKind::InvalidInput, "tiered index entry range overflow")
			})?;
		if end > plaintext_len as u64 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"tiered index entry {}..{} falls outside plaintext slab length {}",
					entry.byte_offset, end, plaintext_len
				),
			));
		}
		out.extend_from_slice(entry.key_locator.as_bytes());
		push_u64_be(&mut out, entry.byte_offset);
		push_u32_be(&mut out, entry.byte_length);
		push_u32_be(&mut out, entry.flags);
	}
	Ok(out)
}

fn crc32c(bytes: &[u8]) -> u32 {
	const POLY: u32 = 0x82f63b78;
	let mut crc = u32::MAX;
	for byte in bytes {
		crc ^= u32::from(*byte);
		for _ in 0..8 {
			let mask = 0_u32.wrapping_sub(crc & 1);
			crc = (crc >> 1) ^ (POLY & mask);
		}
	}
	!crc
}

fn plaintext_len_from_ciphertext_len(
	ciphertext_len: usize,
	chunk_count: u32,
) -> std::io::Result<usize> {
	let tag_bytes = (chunk_count as usize)
		.checked_mul(SLAB_AEAD_TAG_BYTES)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tiered slab tag length overflow"))?;
	let plaintext_len = ciphertext_len.checked_sub(tag_bytes).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"tiered slab ciphertext shorter than chunk authentication tags",
		)
	})?;
	if chunk_count_for_len(plaintext_len)? != chunk_count {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"tiered slab chunk_count disagrees with ciphertext length",
		));
	}
	Ok(plaintext_len)
}

fn parse_adr022_tail_anchor(src: &[u8]) -> std::io::Result<(u32, u32, usize)> {
	let tail_offset = src
		.len()
		.checked_sub(ADR_TIERED_SLAB_TAIL_BYTES)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::UnexpectedEof,
				"tiered slab shorter than ADR 022 tail",
			)
		})?;
	let footer_len_u32 = u32::from_be_bytes(
		src[tail_offset..tail_offset + 4]
			.try_into()
			.expect("slice len"),
	);
	let index_len_u32 = u32::from_be_bytes(
		src[tail_offset + 4..tail_offset + 8]
			.try_into()
			.expect("slice len"),
	);
	if src[tail_offset + 8..] != ADR_TIERED_SLAB_MAGIC_TAIL {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"tiered slab tail magic mismatch",
		));
	}
	if footer_len_u32 as usize != ADR_TIERED_SLAB_FOOTER_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"unsupported ADR 022 footer length {footer_len_u32}; expected {}",
				ADR_TIERED_SLAB_FOOTER_BYTES
			),
		));
	}
	Ok((footer_len_u32, index_len_u32, tail_offset))
}

fn parse_adr022_footer_fields(
	footer: &[u8],
	footer_len_u32: u32,
	index_len_u32: u32,
) -> std::io::Result<ParsedEncryptedFooterFields> {
	if footer.len() != ADR_TIERED_SLAB_FOOTER_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"ADR 022 footer must be {} bytes, got {}",
				ADR_TIERED_SLAB_FOOTER_BYTES,
				footer.len()
			),
		));
	}
	let slab_format_version = u32::from_be_bytes(footer[..4].try_into().expect("slice len"));
	if slab_format_version != SLAB_ENVELOPE_FORMAT_VERSION {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("unsupported ADR 022 slab format version {slab_format_version}"),
		));
	}
	let cipher_id = CipherId::from_wire(footer[4])?;
	if footer[5..8] != [0, 0, 0] {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"non-zero reserved bytes in ADR 022 footer",
		));
	}
	let slab_id = SlabId::new(decode_adr022_u64_slot(&footer[8..24], "slab_id")?);
	let tenant_id = TenantId::new(decode_adr022_u64_slot(&footer[24..40], "tenant_id")?);
	let created_hlc = u64::from_be_bytes(footer[40..48].try_into().expect("slice len"));
	let chunk_size = u32::from_be_bytes(footer[48..52].try_into().expect("slice len"));
	if chunk_size != ADR_TIERED_SLAB_CHUNK_SIZE_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"unsupported ADR 022 chunk size {chunk_size}; expected {ADR_TIERED_SLAB_CHUNK_SIZE_BYTES}",
			),
		));
	}
	let chunk_count = u32::from_be_bytes(footer[52..56].try_into().expect("slice len"));
	let record_count = u64::from_be_bytes(footer[56..64].try_into().expect("slice len"));
	if index_len_u32 as usize % ADR_TIERED_SLAB_INDEX_ENTRY_BYTES != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 offset index length is not an integral number of entries",
		));
	}
	let expected_record_count =
		u64::try_from(index_len_u32 as usize / ADR_TIERED_SLAB_INDEX_ENTRY_BYTES)
			.map_err(|_| Error::new(ErrorKind::InvalidData, "ADR 022 record count overflow"))?;
	if record_count != expected_record_count {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"ADR 022 footer record_count {record_count} does not match index entry count {expected_record_count}",
			),
		));
	}
	let tenant_kek_id =
		TenantKekId::try_from(decode_adr022_u64_slot(&footer[64..96], "tenant_kek_id")?).map_err(
			|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("invalid tenant_kek_id in ADR 022 footer: {err}"),
				)
			},
		)?;
	let wrapped_slot = &footer[96..96 + ADR_TIERED_SLAB_WRAPPED_DEK_BYTES];
	if wrapped_slot[ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES..]
		.iter()
		.any(|byte| *byte != 0)
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 wrapped_slab_dek padding must be zero",
		));
	}
	let wrapped_slab_dek = wrapped_slot[..ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES].to_vec();
	let surface_hmac = footer
		[ADR_TIERED_SLAB_HMAC_OFFSET..ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES]
		.to_vec();
	let footer_suffix_after_hmac =
		footer[ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES..].to_vec();
	if footer_suffix_after_hmac.len() != ADR_TIERED_SLAB_RESERVED_SUFFIX_BYTES
		|| footer_suffix_after_hmac.iter().any(|byte| *byte != 0)
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 footer reserved suffix must be all zero",
		));
	}
	let header_bytes_for_hmac =
		encode_adr022_header(slab_id, tenant_id, created_hlc, cipher_id, chunk_count)?.to_vec();
	Ok(ParsedEncryptedFooterFields {
		slab_id,
		tenant_id,
		tenant_kek_id,
		cipher_id,
		chunk_count,
		plaintext_len: 0,
		nonce_version: SLAB_NONCE_VERSION,
		wrapped_slab_dek,
		surface_hmac,
		footer_prefix_without_hmac: footer[..ADR_TIERED_SLAB_HMAC_OFFSET].to_vec(),
		footer_suffix_after_hmac,
		footer_len_u32,
		index_len_u32,
		ciphertext_offset_in_object: (ADR_TIERED_SLAB_MAGIC_HEAD.len()
			+ ADR_TIERED_SLAB_HEADER_BYTES) as u64,
		header_bytes_for_hmac,
		index_from_tail: None,
		missing_index_range_in_object: None,
		#[cfg(test)]
		wrapped_slab_dek_range_in_footer: 96..(96 + ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES),
	})
}

fn parse_adr022_encrypted_footer(blob: &[u8]) -> std::io::Result<ParsedEncryptedFooter<'_>> {
	if !blob.starts_with(&ADR_TIERED_SLAB_MAGIC_HEAD) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"blob is not in ADR 022 tiered slab format",
		));
	}
	let (footer_len_u32, index_len_u32, tail_offset) = parse_adr022_tail_anchor(blob)?;
	let footer_start = tail_offset
		.checked_sub(footer_len_u32 as usize)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tiered slab footer underflow"))?;
	let index_start = footer_start
		.checked_sub(index_len_u32 as usize)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tiered slab index underflow"))?;
	let ciphertext_start = ADR_TIERED_SLAB_MAGIC_HEAD.len() + ADR_TIERED_SLAB_HEADER_BYTES;
	if index_start < ciphertext_start {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 tiered slab index overlaps ciphertext prefix",
		));
	}
	let header = &blob[ADR_TIERED_SLAB_MAGIC_HEAD.len()..ciphertext_start];
	let footer = &blob[footer_start..tail_offset];
	let fields = parse_adr022_footer_fields(footer, footer_len_u32, index_len_u32)?;
	if header != fields.header_bytes_for_hmac.as_slice() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 slab header does not match footer metadata",
		));
	}
	let ciphertext = &blob[ciphertext_start..index_start];
	let fields = fields.finalize_from_ciphertext_len(ciphertext.len())?;
	Ok(ParsedEncryptedFooter {
		slab_id: fields.slab_id,
		tenant_id: fields.tenant_id,
		tenant_kek_id: fields.tenant_kek_id,
		cipher_id: fields.cipher_id,
		chunk_count: fields.chunk_count,
		plaintext_len: fields.plaintext_len,
		nonce_version: fields.nonce_version,
		wrapped_slab_dek: fields.wrapped_slab_dek,
		surface_hmac: fields.surface_hmac,
		footer_prefix_without_hmac: fields.footer_prefix_without_hmac,
		footer_suffix_after_hmac: fields.footer_suffix_after_hmac,
		footer_len_u32: fields.footer_len_u32,
		index_len_u32: fields.index_len_u32,
		header_bytes_for_hmac: header,
		index: &blob[index_start..footer_start],
		ciphertext,
		#[cfg(test)]
		wrapped_slab_dek_range_in_blob: (footer_start
			+ fields.wrapped_slab_dek_range_in_footer.start)
			..(footer_start + fields.wrapped_slab_dek_range_in_footer.end),
	})
}

fn parse_encrypted_footer(blob: &[u8]) -> std::io::Result<ParsedEncryptedFooter<'_>> {
	parse_adr022_encrypted_footer(blob)
}

fn parse_adr022_encrypted_footer_from_tail_window(
	object_size_bytes: u64,
	tail_window: &[u8],
) -> std::io::Result<ParsedEncryptedFooterFields> {
	let (footer_len_u32, index_len_u32, tail_offset) = parse_adr022_tail_anchor(tail_window)?;
	let footer_start = tail_offset
		.checked_sub(footer_len_u32 as usize)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::UnexpectedEof,
				"tail window does not include full ADR 022 footer",
			)
		})?;
	let footer = &tail_window[footer_start..tail_offset];
	let fields = parse_adr022_footer_fields(footer, footer_len_u32, index_len_u32)?;
	let footer_start_in_object = object_size_bytes
		.checked_sub(ADR_TIERED_SLAB_TAIL_BYTES as u64 + u64::from(footer_len_u32))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 022 footer underflow"))?;
	let index_start_in_object = footer_start_in_object
		.checked_sub(u64::from(index_len_u32))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 022 index underflow"))?;
	let ciphertext_offset_in_object =
		(ADR_TIERED_SLAB_MAGIC_HEAD.len() + ADR_TIERED_SLAB_HEADER_BYTES) as u64;
	if index_start_in_object < ciphertext_offset_in_object {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 tiered slab index overlaps ciphertext prefix",
		));
	}
	let ciphertext_len_u64 = index_start_in_object - ciphertext_offset_in_object;
	let ciphertext_len = usize::try_from(ciphertext_len_u64).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 022 ciphertext length does not fit into usize",
		)
	})?;
	let mut fields = fields.finalize_from_ciphertext_len(ciphertext_len)?;
	let tail_window_start_in_object = object_size_bytes.saturating_sub(tail_window.len() as u64);
	if index_len_u32 == 0 {
		fields.index_from_tail = Some(Vec::new());
	} else if index_start_in_object >= tail_window_start_in_object {
		let local_start = usize::try_from(index_start_in_object - tail_window_start_in_object)
			.map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"ADR 022 index start does not fit into usize",
				)
			})?;
		let local_end = local_start + index_len_u32 as usize;
		if local_end > tail_window.len() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"tail window does not include full ADR 022 offset index",
			));
		}
		fields.index_from_tail = Some(tail_window[local_start..local_end].to_vec());
	} else {
		fields.missing_index_range_in_object = Some(index_start_in_object..footer_start_in_object);
	}
	Ok(fields)
}

fn parse_encrypted_footer_from_tail_window(
	object_size_bytes: u64,
	tail_window: &[u8],
) -> std::io::Result<ParsedEncryptedFooterFields> {
	parse_adr022_encrypted_footer_from_tail_window(object_size_bytes, tail_window)
}

pub(crate) trait RootKekProvider: Send + Sync {
	fn label(&self) -> &'static str;
	fn health(&self) -> std::io::Result<RootKekProviderHealth>;
	fn wrap_tenant_kek(&self, plaintext_key: &KeyMaterialBytes) -> std::io::Result<Vec<u8>>;
	fn unwrap_tenant_kek(&self, wrapped_key: &[u8]) -> std::io::Result<KeyMaterialBytes>;

	#[allow(dead_code)]
	fn rotate_root_kek(&self) -> std::io::Result<()> {
		Err(Error::new(
			ErrorKind::Unsupported,
			format!(
				"root KEK provider '{}' does not support in-place root KEK rotation",
				self.label()
			),
		))
	}
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub(crate) struct KeyMaterialBytes([u8; 32]);

impl KeyMaterialBytes {
	fn random() -> std::io::Result<Self> {
		let mut out = Self([0_u8; 32]);
		aws_lc_rs::rand::fill(&mut out.0)
			.map_err(|_| Error::other("failed to generate random 256-bit key"))?;
		Ok(out)
	}

	fn copy_from_slice(src: &[u8], context: &str) -> std::io::Result<Self> {
		if src.len() != 32 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("expected 32-byte {context}, got {}", src.len()),
			));
		}
		let mut out = Self([0_u8; 32]);
		out.0.copy_from_slice(src);
		Ok(out)
	}
}

impl AsRef<[u8]> for KeyMaterialBytes {
	fn as_ref(&self) -> &[u8] {
		&self.0
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
struct AwsKmsCredentials {
	access_key_id: String,
	secret_access_key: String,
	session_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AwsKmsRootKekEnvelope {
	region: String,
	key_id: String,
	ciphertext_blob: Vec<u8>,
}

struct AwsKmsClient {
	http: reqwest::blocking::Client,
	endpoint: Url,
	region: String,
	key_id: String,
	credentials: AwsKmsCredentials,
}

impl AwsKmsClient {
	fn new(cfg: &AwsKmsRootKekConfig, credentials: AwsKmsCredentials) -> std::io::Result<Self> {
		let endpoint = match cfg.endpoint.as_deref() {
			Some(endpoint) => Url::parse(endpoint),
			None => Url::parse(&format!("https://kms.{}.amazonaws.com/", cfg.region)),
		}
		.map_err(|error| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("failed to construct aws-kms endpoint URL: {error}"),
			)
		})?;
		let http = reqwest::blocking::Client::builder()
			.timeout(Duration::from_secs(AWS_KMS_TIMEOUT_SECS))
			.build()
			.map_err(|error| {
				Error::other(format!("failed to construct aws-kms client: {error}"))
			})?;
		Ok(Self {
			http,
			endpoint,
			region: cfg.region.clone(),
			key_id: cfg.key_id.clone(),
			credentials,
		})
	}

	fn encrypt_root_kek(&self, plaintext: &KeyMaterialBytes) -> std::io::Result<Vec<u8>> {
		let request = serde_json::json!({
			"KeyId": self.key_id,
			"Plaintext": base64::engine::general_purpose::STANDARD.encode(plaintext.as_ref()),
			"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
		});
		let response: AwsKmsEncryptResponse = self.call("TrentService.Encrypt", &request)?;
		base64::engine::general_purpose::STANDARD
			.decode(response.ciphertext_blob)
			.map_err(|error| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"aws-kms encrypt response contained invalid base64 ciphertext: {error}"
					),
				)
			})
	}

	fn decrypt_root_kek(&self, ciphertext_blob: &[u8]) -> std::io::Result<KeyMaterialBytes> {
		let request = serde_json::json!({
			"CiphertextBlob": base64::engine::general_purpose::STANDARD.encode(ciphertext_blob),
			"KeyId": self.key_id,
			"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
		});
		let response: AwsKmsDecryptResponse = self.call("TrentService.Decrypt", &request)?;
		let plaintext = Zeroizing::new(
			base64::engine::general_purpose::STANDARD
				.decode(response.plaintext.as_bytes())
				.map_err(|error| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"aws-kms decrypt response contained invalid base64 plaintext: {error}"
						),
					)
				})?,
		);
		KeyMaterialBytes::copy_from_slice(plaintext.as_ref(), "aws-kms root KEK plaintext")
	}

	fn describe_key(&self) -> std::io::Result<RootKekProviderHealth> {
		let request = serde_json::json!({
			"KeyId": self.key_id,
		});
		let response: AwsKmsDescribeKeyResponse =
			self.call("TrentService.DescribeKey", &request)?;
		let metadata = response.key_metadata;
		if metadata.enabled {
			Ok(RootKekProviderHealth::ready())
		} else {
			Ok(RootKekProviderHealth::degraded(format!(
				"aws-kms key '{}' is not enabled (state={})",
				metadata.key_id.unwrap_or_else(|| self.key_id.clone()),
				metadata.key_state.unwrap_or_else(|| "Unknown".into())
			)))
		}
	}

	fn call<T>(&self, target: &str, payload: &serde_json::Value) -> std::io::Result<T>
	where
		T: for<'de> Deserialize<'de>,
	{
		let body = serde_json::to_vec(payload).map_err(|error| {
			Error::other(format!("failed to encode aws-kms request body: {error}"))
		})?;
		let now = Utc::now();
		let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
		let date_scope = now.format("%Y%m%d").to_string();
		let payload_hash = bytes_to_hex(&crate::hash::sha256::digest(&body));
		let host = host_header_value(&self.endpoint)?;
		let mut headers = vec![
			("content-type".to_string(), AWS_KMS_CONTENT_TYPE.to_string()),
			("host".to_string(), host.clone()),
			("x-amz-date".to_string(), amz_date.clone()),
			("x-amz-target".to_string(), target.to_string()),
		];
		if let Some(token) = self.credentials.session_token.as_deref() {
			headers.push(("x-amz-security-token".to_string(), token.to_string()));
		}
		headers.sort_by(|a, b| a.0.cmp(&b.0));
		let canonical_headers = headers
			.iter()
			.map(|(name, value)| format!("{name}:{}\n", canonicalize_header_value(value)))
			.collect::<String>();
		let signed_headers = headers
			.iter()
			.map(|(name, _)| name.as_str())
			.collect::<Vec<_>>()
			.join(";");
		let canonical_request =
			format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
		let canonical_request_hash =
			bytes_to_hex(&crate::hash::sha256::digest(canonical_request.as_bytes()));
		let credential_scope = format!(
			"{date_scope}/{}/{}/aws4_request",
			self.region, AWS_KMS_SERVICE
		);
		let string_to_sign =
			format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_request_hash}");
		let signing_key = derive_aws_sigv4_signing_key(
			self.credentials.secret_access_key.as_bytes(),
			&date_scope,
			&self.region,
			AWS_KMS_SERVICE,
		)?;
		let signature =
			bytes_to_hex(aws_hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes()).as_ref());
		let authorization = format!(
			"AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
			self.credentials.access_key_id, credential_scope, signed_headers, signature
		);

		let mut request = self
			.http
			.post(self.endpoint.clone())
			.header("content-type", AWS_KMS_CONTENT_TYPE)
			.header("x-amz-date", amz_date)
			.header("x-amz-target", target)
			.header("authorization", authorization)
			.header("host", host);
		if let Some(token) = self.credentials.session_token.as_deref() {
			request = request.header("x-amz-security-token", token);
		}
		let response = request
			.body(body)
			.send()
			.map_err(|error| Error::other(format!("aws-kms request failed: {error}")))?;
		let status = response.status();
		let body = response.bytes().map_err(|error| {
			Error::other(format!("failed to read aws-kms response body: {error}"))
		})?;
		if !status.is_success() {
			return Err(map_aws_kms_error(status.as_u16(), &body));
		}
		serde_json::from_slice(&body).map_err(|error| {
			Error::new(
				ErrorKind::InvalidData,
				format!("failed to decode aws-kms response: {error}"),
			)
		})
	}
}

struct AwsKmsRootKekProvider {
	key: LockedKeyMaterial,
	client: AwsKmsClient,
}

impl AwsKmsRootKekProvider {
	fn initialize(
		init: RootKekProviderInit<'_>,
		cfg: &AwsKmsRootKekConfig,
	) -> std::io::Result<Self> {
		let credentials = cfg.load_credentials()?;
		let client = AwsKmsClient::new(cfg, credentials)?;
		let path = aws_kms_root_kek_path(init.data_dir);
		let key = match init.bootstrap_mode {
			RootKekBootstrapMode::GenerateOrCreate => {
				match read_aws_kms_root_kek_envelope(&path, cfg) {
					Ok(envelope) => client.decrypt_root_kek(&envelope.ciphertext_blob)?,
					Err(error) if error.kind() == ErrorKind::NotFound => {
						let key = KeyMaterialBytes::random()?;
						let ciphertext_blob = client.encrypt_root_kek(&key)?;
						let envelope = AwsKmsRootKekEnvelope {
							region: cfg.region.clone(),
							key_id: cfg.key_id.clone(),
							ciphertext_blob,
						};
						match persist_new_aws_kms_root_kek_envelope(&path, &envelope) {
							Ok(()) => key,
							Err(persist_error)
								if persist_error.kind() == ErrorKind::AlreadyExists =>
							{
								let envelope = read_aws_kms_root_kek_envelope(&path, cfg)?;
								client.decrypt_root_kek(&envelope.ciphertext_blob)?
							}
							Err(persist_error) => {
								return Err(Error::new(
									persist_error.kind(),
									format!(
										"failed to persist aws-kms root KEK envelope at {}: {persist_error}",
										path.display()
									),
								));
							}
						}
					}
					Err(error) => {
						return Err(Error::new(
							error.kind(),
							format!(
								"failed to read aws-kms root KEK envelope at {}: {error}",
								path.display()
							),
						));
					}
				}
			}
			RootKekBootstrapMode::EnrollExisting => {
				let envelope = read_aws_kms_root_kek_envelope(&path, cfg).map_err(|error| {
					Error::new(
						error.kind(),
						format!(
							"failed to enroll existing aws-kms root KEK envelope from {}: {error}",
							path.display()
						),
					)
				})?;
				client.decrypt_root_kek(&envelope.ciphertext_blob)?
			}
		};
		Ok(Self {
			key: LockedKeyMaterial::new(key)?,
			client,
		})
	}
}

fn aws_kms_root_kek_path(data_dir: &Path) -> PathBuf {
	data_dir
		.join(AWS_KMS_ROOT_KEK_DIR)
		.join(AWS_KMS_ROOT_KEK_FILE)
}

fn read_aws_kms_root_kek_envelope(
	path: &Path,
	cfg: &AwsKmsRootKekConfig,
) -> std::io::Result<AwsKmsRootKekEnvelope> {
	let mut file = OpenOptions::new().read(true).open(path)?;
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes)?;
	if bytes.len() < AWS_KMS_ROOT_KEK_MAGIC.len() + 12 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("aws-kms root KEK file {} is truncated", path.display()),
		));
	}
	if bytes[..AWS_KMS_ROOT_KEK_MAGIC.len()] != AWS_KMS_ROOT_KEK_MAGIC {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"aws-kms root KEK file {} has invalid magic header",
				path.display()
			),
		));
	}
	let mut cursor = AWS_KMS_ROOT_KEK_MAGIC.len();
	let version = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().expect("slice len"));
	cursor += 4;
	if version != AWS_KMS_ROOT_KEK_VERSION {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"aws-kms root KEK file {} has unsupported version {version}",
				path.display()
			),
		));
	}
	let region_len =
		u16::from_be_bytes(bytes[cursor..cursor + 2].try_into().expect("slice len")) as usize;
	cursor += 2;
	let key_id_len =
		u16::from_be_bytes(bytes[cursor..cursor + 2].try_into().expect("slice len")) as usize;
	cursor += 2;
	let ciphertext_len =
		u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().expect("slice len")) as usize;
	cursor += 4;
	let expected_len = cursor + region_len + key_id_len + ciphertext_len;
	if bytes.len() != expected_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"aws-kms root KEK file {} has invalid length {}; expected {} bytes",
				path.display(),
				bytes.len(),
				expected_len
			),
		));
	}
	let region =
		String::from_utf8(bytes[cursor..cursor + region_len].to_vec()).map_err(|error| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"aws-kms root KEK file {} has invalid region bytes: {error}",
					path.display()
				),
			)
		})?;
	cursor += region_len;
	let key_id =
		String::from_utf8(bytes[cursor..cursor + key_id_len].to_vec()).map_err(|error| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"aws-kms root KEK file {} has invalid key-id bytes: {error}",
					path.display()
				),
			)
		})?;
	cursor += key_id_len;
	if region != cfg.region || key_id != cfg.key_id {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"aws-kms root KEK file {} was sealed for region='{}' key_id='{}', but config selected region='{}' key_id='{}'",
				path.display(),
				region,
				key_id,
				cfg.region,
				cfg.key_id
			),
		));
	}
	Ok(AwsKmsRootKekEnvelope {
		region,
		key_id,
		ciphertext_blob: bytes[cursor..].to_vec(),
	})
}

fn persist_new_aws_kms_root_kek_envelope(
	path: &Path,
	envelope: &AwsKmsRootKekEnvelope,
) -> std::io::Result<()> {
	let region_bytes = envelope.region.as_bytes();
	let key_id_bytes = envelope.key_id.as_bytes();
	let region_len = u16::try_from(region_bytes.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"aws-kms region exceeds u16::MAX bytes",
		)
	})?;
	let key_id_len = u16::try_from(key_id_bytes.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"aws-kms key_id exceeds u16::MAX bytes",
		)
	})?;
	let ciphertext_len = u32::try_from(envelope.ciphertext_blob.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"aws-kms ciphertext blob exceeds u32::MAX bytes",
		)
	})?;
	let mut encoded = Vec::with_capacity(
		AWS_KMS_ROOT_KEK_MAGIC.len()
			+ 4 + 2 + 2
			+ 4 + region_bytes.len()
			+ key_id_bytes.len()
			+ envelope.ciphertext_blob.len(),
	);
	encoded.extend_from_slice(&AWS_KMS_ROOT_KEK_MAGIC);
	encoded.extend_from_slice(&AWS_KMS_ROOT_KEK_VERSION.to_be_bytes());
	encoded.extend_from_slice(&region_len.to_be_bytes());
	encoded.extend_from_slice(&key_id_len.to_be_bytes());
	encoded.extend_from_slice(&ciphertext_len.to_be_bytes());
	encoded.extend_from_slice(region_bytes);
	encoded.extend_from_slice(key_id_bytes);
	encoded.extend_from_slice(&envelope.ciphertext_blob);
	persist_new_file_atomically(path, &encoded)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AwsKmsEncryptResponse {
	ciphertext_blob: String,
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "PascalCase")]
struct AwsKmsDecryptResponse {
	plaintext: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AwsKmsDescribeKeyResponse {
	key_metadata: AwsKmsKeyMetadata,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AwsKmsKeyMetadata {
	#[serde(default)]
	key_id: Option<String>,
	enabled: bool,
	#[serde(default)]
	key_state: Option<String>,
}

#[derive(Deserialize)]
struct AwsKmsErrorBody {
	#[serde(rename = "__type")]
	#[serde(default)]
	type_name: Option<String>,
	#[serde(rename = "message")]
	#[serde(default)]
	message_lower: Option<String>,
	#[serde(rename = "Message")]
	#[serde(default)]
	message_upper: Option<String>,
}

fn map_aws_kms_error(status: u16, body: &[u8]) -> std::io::Error {
	let parsed = serde_json::from_slice::<AwsKmsErrorBody>(body).ok();
	let type_name = parsed
		.as_ref()
		.and_then(|error| error.type_name.as_deref())
		.unwrap_or("AwsKmsError");
	let message = parsed
		.as_ref()
		.and_then(|error| {
			error
				.message_lower
				.as_deref()
				.or(error.message_upper.as_deref())
		})
		.unwrap_or("request failed");
	let kind = if type_name.contains("AccessDenied") {
		ErrorKind::PermissionDenied
	} else if type_name.contains("NotFound") {
		ErrorKind::NotFound
	} else if type_name.contains("InvalidCiphertext") || type_name.contains("Disabled") {
		ErrorKind::InvalidData
	} else {
		ErrorKind::Other
	};
	Error::new(
		kind,
		format!("aws-kms returned HTTP {status} ({type_name}): {message}"),
	)
}

fn host_header_value(url: &Url) -> std::io::Result<String> {
	let host = url.host_str().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			"aws-kms endpoint URL is missing a host",
		)
	})?;
	Ok(match url.port() {
		Some(port) => format!("{host}:{port}"),
		None => host.to_string(),
	})
}

fn canonicalize_header_value(value: &str) -> String {
	value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn derive_aws_sigv4_signing_key(
	secret_access_key: &[u8],
	date_scope: &str,
	region: &str,
	service: &str,
) -> std::io::Result<KeyMaterialBytes> {
	let mut prefixed = Zeroizing::new(Vec::with_capacity(4 + secret_access_key.len()));
	prefixed.extend_from_slice(b"AWS4");
	prefixed.extend_from_slice(secret_access_key);
	let date_key = hmac_key_material(
		prefixed.as_ref(),
		date_scope.as_bytes(),
		"aws sigv4 date key",
	)?;
	let region_key =
		hmac_key_material(date_key.as_ref(), region.as_bytes(), "aws sigv4 region key")?;
	let service_key = hmac_key_material(
		region_key.as_ref(),
		service.as_bytes(),
		"aws sigv4 service key",
	)?;
	hmac_key_material(
		service_key.as_ref(),
		b"aws4_request",
		"aws sigv4 signing key",
	)
}

fn hmac_key_material(key: &[u8], data: &[u8], context: &str) -> std::io::Result<KeyMaterialBytes> {
	let tag = aws_hmac_sha256(key, data);
	KeyMaterialBytes::copy_from_slice(tag.as_ref(), context)
}

fn aws_hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
	let key = hmac::Key::new(hmac::HMAC_SHA256, key);
	hmac::sign(&key, data)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		use std::fmt::Write as _;
		let _ = write!(&mut out, "{byte:02x}");
	}
	out
}

struct DevRootKekProvider {
	key: LockedKeyMaterial,
}

impl DevRootKekProvider {
	#[cfg(any(test, not(feature = "fips")))]
	fn from_dev_insecure_dir(data_dir: &Path) -> std::io::Result<Self> {
		Self::initialize(RootKekProviderInit::generate_or_create(data_dir))
	}

	#[cfg(any(test, not(feature = "fips")))]
	fn initialize(init: RootKekProviderInit<'_>) -> std::io::Result<Self> {
		let path = dev_insecure_root_kek_path(init.data_dir);
		let key = match init.bootstrap_mode {
			RootKekBootstrapMode::GenerateOrCreate => match read_dev_insecure_root_kek(&path) {
				Ok(key) => key,
				Err(error) if error.kind() == ErrorKind::NotFound => {
					let key = KeyMaterialBytes::random()?;
					match persist_new_dev_insecure_root_kek(&path, &key) {
						Ok(()) => key,
						Err(persist_error) if persist_error.kind() == ErrorKind::AlreadyExists => {
							read_dev_insecure_root_kek(&path)?
						}
						Err(persist_error) => {
							return Err(Error::new(
								persist_error.kind(),
								format!(
									"failed to persist dev root KEK at {}: {persist_error}",
									path.display()
								),
							));
						}
					}
				}
				Err(error) => {
					return Err(Error::new(
						error.kind(),
						format!("failed to read dev root KEK at {}: {error}", path.display()),
					));
				}
			},
			RootKekBootstrapMode::EnrollExisting => {
				read_dev_insecure_root_kek(&path).map_err(|error| {
					Error::new(
						error.kind(),
						format!(
							"failed to enroll existing dev root KEK from {}: {error}",
							path.display()
						),
					)
				})?
			}
		};
		Ok(Self {
			key: LockedKeyMaterial::new(key)?,
		})
	}

	fn new_ephemeral_for_tests() -> std::io::Result<Self> {
		let key = KeyMaterialBytes::random()?;
		Ok(Self {
			key: LockedKeyMaterial::new(key)?,
		})
	}
}

#[cfg(any(test, not(feature = "fips")))]
fn dev_insecure_root_kek_path(data_dir: &Path) -> PathBuf {
	data_dir
		.join(DEV_INSECURE_ROOT_KEK_DIR)
		.join(DEV_INSECURE_ROOT_KEK_FILE)
}

#[cfg(any(test, not(feature = "fips")))]
fn read_dev_insecure_root_kek(path: &Path) -> std::io::Result<KeyMaterialBytes> {
	let mut file = OpenOptions::new().read(true).open(path)?;
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes)?;
	KeyMaterialBytes::copy_from_slice(&bytes, "dev root KEK file")
}

#[cfg(any(test, not(feature = "fips")))]
fn persist_new_dev_insecure_root_kek(path: &Path, key: &KeyMaterialBytes) -> std::io::Result<()> {
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)?;
	}
	let mut options = OpenOptions::new();
	options.write(true).create_new(true);
	#[cfg(unix)]
	options.mode(0o600);
	let mut file = options.open(path)?;
	file.write_all(key.as_ref())?; // crypto-no-disk-cache-ok: dev-only root KEK persistence, cfg-gated out of FIPS builds; production uses an external root KEK provider
	file.sync_all()
}

struct LocalSealedRootKekProvider {
	key: LockedKeyMaterial,
}

impl LocalSealedRootKekProvider {
	fn initialize(
		init: RootKekProviderInit<'_>,
		cfg: &LocalSealedRootKekConfig,
	) -> std::io::Result<Self> {
		let path = local_sealed_root_kek_path(init.data_dir);
		let unlock_secret = cfg.load_unlock_secret()?;
		let key = match init.bootstrap_mode {
			RootKekBootstrapMode::GenerateOrCreate => {
				match read_local_sealed_root_kek(&path, unlock_secret.as_ref()) {
					Ok(key) => key,
					Err(error) if error.kind() == ErrorKind::NotFound => {
						let key = KeyMaterialBytes::random()?;
						match persist_new_local_sealed_root_kek(&path, unlock_secret.as_ref(), &key)
						{
							Ok(()) => key,
							Err(persist_error)
								if persist_error.kind() == ErrorKind::AlreadyExists =>
							{
								read_local_sealed_root_kek(&path, unlock_secret.as_ref())?
							}
							Err(persist_error) => {
								return Err(Error::new(
									persist_error.kind(),
									format!(
										"failed to persist local-sealed root KEK at {}: {persist_error}",
										path.display()
									),
								));
							}
						}
					}
					Err(error) => {
						return Err(Error::new(
							error.kind(),
							format!(
								"failed to read local-sealed root KEK at {}: {error}",
								path.display()
							),
						));
					}
				}
			}
			RootKekBootstrapMode::EnrollExisting => {
				read_local_sealed_root_kek(&path, unlock_secret.as_ref()).map_err(|error| {
					Error::new(
						error.kind(),
						format!(
							"failed to enroll existing local-sealed root KEK from {}: {error}",
							path.display()
						),
					)
				})?
			}
		};
		Ok(Self {
			key: LockedKeyMaterial::new(key)?,
		})
	}
}

fn local_sealed_root_kek_path(data_dir: &Path) -> PathBuf {
	data_dir
		.join(LOCAL_SEALED_ROOT_KEK_DIR)
		.join(LOCAL_SEALED_ROOT_KEK_FILE)
}

fn derive_local_sealed_wrapping_key(
	unlock_secret: &[u8],
	salt: &[u8; LOCAL_SEALED_ROOT_KEK_SALT_BYTES],
) -> std::io::Result<KeyMaterialBytes> {
	let params = ScryptParams::new(
		LOCAL_SEALED_ROOT_KEK_SCRYPT_LOG_N,
		LOCAL_SEALED_ROOT_KEK_SCRYPT_R,
		LOCAL_SEALED_ROOT_KEK_SCRYPT_P,
		32,
	)
	.map_err(|error| {
		Error::other(format!(
			"failed to construct local-sealed scrypt params: {error}"
		))
	})?;
	let mut derived = KeyMaterialBytes([0_u8; 32]);
	scrypt(unlock_secret, salt, &params, &mut derived.0)
		.map_err(|error| Error::other(format!("local-sealed scrypt derivation failed: {error}")))?;
	Ok(derived)
}

fn read_local_sealed_root_kek(
	path: &Path,
	unlock_secret: &[u8],
) -> std::io::Result<KeyMaterialBytes> {
	let mut file = OpenOptions::new().read(true).open(path)?;
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes)?;
	if bytes.len() != LOCAL_SEALED_ROOT_KEK_TOTAL_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"local-sealed root KEK file {} has invalid length {}; expected {} bytes",
				path.display(),
				bytes.len(),
				LOCAL_SEALED_ROOT_KEK_TOTAL_BYTES
			),
		));
	}
	if bytes[..LOCAL_SEALED_ROOT_KEK_MAGIC.len()] != LOCAL_SEALED_ROOT_KEK_MAGIC {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"local-sealed root KEK file {} has invalid magic header",
				path.display()
			),
		));
	}
	let version_offset = LOCAL_SEALED_ROOT_KEK_MAGIC.len();
	let version = u32::from_be_bytes(
		bytes[version_offset..version_offset + 4]
			.try_into()
			.expect("slice len"),
	);
	if version != LOCAL_SEALED_ROOT_KEK_VERSION {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"local-sealed root KEK file {} has unsupported version {version}",
				path.display()
			),
		));
	}
	let salt_offset = version_offset + 4;
	let nonce_offset = salt_offset + LOCAL_SEALED_ROOT_KEK_SALT_BYTES;
	let ciphertext_offset = nonce_offset + LOCAL_SEALED_ROOT_KEK_NONCE_BYTES;
	let salt: [u8; LOCAL_SEALED_ROOT_KEK_SALT_BYTES] = bytes[salt_offset..nonce_offset]
		.try_into()
		.expect("slice len");
	let seal_key = derive_local_sealed_wrapping_key(unlock_secret, &salt)?;
	let key = LessSafeKey::new(
		UnboundKey::new(&AES_256_GCM, seal_key.as_ref())
			.map_err(|_| Error::other("failed to construct local-sealed AES-GCM key"))?,
	);
	let nonce: [u8; LOCAL_SEALED_ROOT_KEK_NONCE_BYTES] = bytes[nonce_offset..ciphertext_offset]
		.try_into()
		.expect("slice len");
	let mut in_out = Zeroizing::new(bytes[ciphertext_offset..].to_vec());
	let plaintext = key
		.open_in_place(
			Nonce::assume_unique_for_key(nonce),
			Aad::from(LOCAL_SEALED_ROOT_KEK_AAD),
			in_out.as_mut_slice(),
		)
		.map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"failed to decrypt local-sealed root KEK at {}; unlock secret is invalid or the file is corrupted",
					path.display()
				),
			)
		})?;
	KeyMaterialBytes::copy_from_slice(plaintext, "local-sealed root KEK")
}

fn persist_new_local_sealed_root_kek(
	path: &Path,
	unlock_secret: &[u8],
	key: &KeyMaterialBytes,
) -> std::io::Result<()> {
	let mut salt = [0_u8; LOCAL_SEALED_ROOT_KEK_SALT_BYTES];
	aws_lc_rs::rand::fill(&mut salt)
		.map_err(|_| Error::other("failed to generate local-sealed root KEK salt"))?;
	let seal_key = derive_local_sealed_wrapping_key(unlock_secret, &salt)?;
	let mut nonce = [0_u8; LOCAL_SEALED_ROOT_KEK_NONCE_BYTES];
	aws_lc_rs::rand::fill(&mut nonce)
		.map_err(|_| Error::other("failed to generate local-sealed root KEK nonce"))?;
	let aead = LessSafeKey::new(
		UnboundKey::new(&AES_256_GCM, seal_key.as_ref())
			.map_err(|_| Error::other("failed to construct local-sealed AES-GCM key"))?,
	);
	let mut ciphertext = key.as_ref().to_vec();
	aead.seal_in_place_append_tag(
		Nonce::assume_unique_for_key(nonce),
		Aad::from(LOCAL_SEALED_ROOT_KEK_AAD),
		&mut ciphertext,
	)
	.map_err(|_| Error::other("failed to encrypt local-sealed root KEK"))?;

	let mut encoded = Vec::with_capacity(LOCAL_SEALED_ROOT_KEK_TOTAL_BYTES);
	encoded.extend_from_slice(&LOCAL_SEALED_ROOT_KEK_MAGIC);
	encoded.extend_from_slice(&LOCAL_SEALED_ROOT_KEK_VERSION.to_be_bytes());
	encoded.extend_from_slice(&salt);
	encoded.extend_from_slice(&nonce);
	encoded.extend_from_slice(&ciphertext);
	ciphertext.zeroize();
	persist_new_file_atomically(path, &encoded)
}

fn persist_new_file_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)?;
	}
	if path.exists() {
		return Err(Error::new(
			ErrorKind::AlreadyExists,
			format!("refusing to overwrite existing file {}", path.display()),
		));
	}
	let tmp_path = atomic_tmp_path(path);
	let persist_result = (|| {
		let mut options = OpenOptions::new();
		options.write(true).create_new(true).truncate(true);
		#[cfg(unix)]
		options.mode(0o600);
		let mut file = options.open(&tmp_path)?;
		file.write_all(bytes)?;
		file.sync_all()?;
		drop(file);
		if path.exists() {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!("refusing to overwrite existing file {}", path.display()),
			));
		}
		std::fs::rename(&tmp_path, path)?;
		fsync_parent_dir_sync(path)
	})();
	if persist_result.is_err() {
		let _ = std::fs::remove_file(&tmp_path);
	}
	persist_result
}

fn atomic_tmp_path(path: &Path) -> PathBuf {
	let mut tmp = path.to_path_buf();
	tmp.as_mut_os_string().push(format!(
		".tmp-{}-{}",
		std::process::id(),
		Utc::now().timestamp_nanos_opt().unwrap_or_default()
	));
	tmp
}

fn fsync_parent_dir_sync(path: &Path) -> std::io::Result<()> {
	let Some(parent) = path.parent() else {
		return Ok(());
	};
	OpenOptions::new().read(true).open(parent)?.sync_all()
}

impl RootKekProvider for DevRootKekProvider {
	fn label(&self) -> &'static str {
		RootKekProviderConfig::DevInsecure.label()
	}

	fn health(&self) -> std::io::Result<RootKekProviderHealth> {
		Ok(RootKekProviderHealth::ready())
	}

	fn wrap_tenant_kek(&self, plaintext_key: &KeyMaterialBytes) -> std::io::Result<Vec<u8>> {
		let mut out = vec![0_u8; plaintext_key.as_ref().len() + 15];
		let wrapped = AesKek::new(&AES_256, self.key.as_ref())
			.map_err(|_| Error::other("failed to construct root AES-KWP key"))?
			.wrap_with_padding(plaintext_key.as_ref(), &mut out)
			.map_err(|_| Error::other("failed to wrap tenant KEK under root KEK"))?;
		Ok(wrapped.to_vec())
	}

	fn unwrap_tenant_kek(&self, wrapped_key: &[u8]) -> std::io::Result<KeyMaterialBytes> {
		let mut out = Zeroizing::new(vec![0_u8; wrapped_key.len()]);
		let plaintext = AesKek::new(&AES_256, self.key.as_ref())
			.map_err(|_| Error::other("failed to construct root AES-KWP key"))?
			.unwrap_with_padding(wrapped_key, &mut out)
			.map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"failed to unwrap tenant KEK under root KEK",
				)
			})?;
		KeyMaterialBytes::copy_from_slice(plaintext, "tenant KEK")
	}
}

impl RootKekProvider for LocalSealedRootKekProvider {
	fn label(&self) -> &'static str {
		"local-sealed"
	}

	fn health(&self) -> std::io::Result<RootKekProviderHealth> {
		Ok(RootKekProviderHealth::ready())
	}

	fn wrap_tenant_kek(&self, plaintext_key: &KeyMaterialBytes) -> std::io::Result<Vec<u8>> {
		let mut out = vec![0_u8; plaintext_key.as_ref().len() + 15];
		let wrapped = AesKek::new(&AES_256, self.key.as_ref())
			.map_err(|_| Error::other("failed to construct root AES-KWP key"))?
			.wrap_with_padding(plaintext_key.as_ref(), &mut out)
			.map_err(|_| Error::other("failed to wrap tenant KEK under root KEK"))?;
		Ok(wrapped.to_vec())
	}

	fn unwrap_tenant_kek(&self, wrapped_key: &[u8]) -> std::io::Result<KeyMaterialBytes> {
		let mut out = Zeroizing::new(vec![0_u8; wrapped_key.len()]);
		let plaintext = AesKek::new(&AES_256, self.key.as_ref())
			.map_err(|_| Error::other("failed to construct root AES-KWP key"))?
			.unwrap_with_padding(wrapped_key, &mut out)
			.map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"failed to unwrap tenant KEK under root KEK",
				)
			})?;
		KeyMaterialBytes::copy_from_slice(plaintext, "tenant KEK")
	}
}

impl RootKekProvider for AwsKmsRootKekProvider {
	fn label(&self) -> &'static str {
		"aws-kms"
	}

	fn health(&self) -> std::io::Result<RootKekProviderHealth> {
		self.client.describe_key()
	}

	fn wrap_tenant_kek(&self, plaintext_key: &KeyMaterialBytes) -> std::io::Result<Vec<u8>> {
		let mut out = vec![0_u8; plaintext_key.as_ref().len() + 15];
		let wrapped = AesKek::new(&AES_256, self.key.as_ref())
			.map_err(|_| Error::other("failed to construct root AES-KWP key"))?
			.wrap_with_padding(plaintext_key.as_ref(), &mut out)
			.map_err(|_| Error::other("failed to wrap tenant KEK under root KEK"))?;
		Ok(wrapped.to_vec())
	}

	fn unwrap_tenant_kek(&self, wrapped_key: &[u8]) -> std::io::Result<KeyMaterialBytes> {
		let mut out = Zeroizing::new(vec![0_u8; wrapped_key.len()]);
		let plaintext = AesKek::new(&AES_256, self.key.as_ref())
			.map_err(|_| Error::other("failed to construct root AES-KWP key"))?
			.unwrap_with_padding(wrapped_key, &mut out)
			.map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"failed to unwrap tenant KEK under root KEK",
				)
			})?;
		KeyMaterialBytes::copy_from_slice(plaintext, "tenant KEK")
	}
}

struct LockedKeyMaterial {
	key: Box<KeyMaterialBytes>,
	locked: bool,
}

impl LockedKeyMaterial {
	fn new(key: KeyMaterialBytes) -> std::io::Result<Self> {
		let mut boxed = Box::new(KeyMaterialBytes([0_u8; 32]));
		boxed.0.copy_from_slice(key.as_ref());
		let locked =
			match best_effort_mlock(boxed.0.as_mut_ptr().cast(), std::mem::size_of::<[u8; 32]>()) {
				Ok(()) => true,
				Err(error) => {
					tracing::warn!(
						error = %error,
						"mlock unavailable for crypto key material; pages may be swapped to disk"
					);
					false
				}
			};
		Ok(Self { key: boxed, locked })
	}

	fn scrub_for_drop(&mut self) {
		self.key.zeroize();
		if self.locked {
			unsafe {
				libc::munlock(
					self.key.0.as_mut_ptr().cast(),
					std::mem::size_of::<[u8; 32]>(),
				);
			}
			self.locked = false;
		}
	}
}

impl AsRef<[u8]> for LockedKeyMaterial {
	fn as_ref(&self) -> &[u8] {
		self.key.as_ref().as_ref()
	}
}

impl Drop for LockedKeyMaterial {
	fn drop(&mut self) {
		self.scrub_for_drop();
	}
}

fn best_effort_mlock(ptr: *mut libc::c_void, len: usize) -> std::io::Result<()> {
	if unsafe { libc::mlock(ptr.cast_const(), len) } == 0 {
		Ok(())
	} else {
		Err(Error::last_os_error())
	}
}

struct CachedKey<V> {
	key: Arc<V>,
	inserted_at: Instant,
	last_accessed_at: Instant,
}

struct KeyCache<K, V>
where
	K: Clone + Eq + Hash,
{
	entries: HashMap<K, CachedKey<V>>,
	max_entries: usize,
	idle_ttl: Duration,
	absolute_ttl: Option<Duration>,
}

impl<K, V> KeyCache<K, V>
where
	K: Clone + Eq + Hash,
{
	fn new(max_entries: usize, idle_ttl: Duration, absolute_ttl: Option<Duration>) -> Self {
		Self {
			entries: HashMap::new(),
			max_entries,
			idle_ttl,
			absolute_ttl,
		}
	}

	fn clear(&mut self) {
		self.entries.clear();
	}

	fn invalidate_where<F>(&mut self, mut predicate: F)
	where
		F: FnMut(&K) -> bool,
	{
		self.entries.retain(|key, _| !predicate(key));
	}

	fn get(&mut self, key: &K) -> Option<Arc<V>> {
		self.prune_expired();
		let now = Instant::now();
		let key_material = {
			let cached = self.entries.get_mut(key)?;
			cached.last_accessed_at = now;
			cached.key.clone()
		};
		self.touch_lru(key);
		Some(key_material)
	}

	fn insert(&mut self, key: K, value: Arc<V>) {
		self.prune_expired();
		let now = Instant::now();
		let inserted_at = self
			.entries
			.get(&key)
			.map(|existing| existing.inserted_at)
			.unwrap_or(now);
		self.entries.insert(
			key.clone(),
			CachedKey {
				key: value,
				inserted_at,
				last_accessed_at: now,
			},
		);
		while self.entries.len() > self.max_entries {
			let Some(evict) = self.oldest_key_by_last_access() else {
				break;
			};
			self.entries.remove(&evict);
		}
	}

	fn prune_expired(&mut self) {
		let now = Instant::now();
		self.entries.retain(|_, value| {
			if now.duration_since(value.last_accessed_at) > self.idle_ttl {
				return false;
			}
			if let Some(absolute_ttl) = self.absolute_ttl {
				if now.duration_since(value.inserted_at) > absolute_ttl {
					return false;
				}
			}
			true
		});
	}

	fn touch_lru(&mut self, key: &K) {
		if let Some(cached) = self.entries.get_mut(key) {
			cached.last_accessed_at = Instant::now();
		}
	}

	fn oldest_key_by_last_access(&self) -> Option<K> {
		self.entries
			.iter()
			.min_by_key(|(_, cached)| (cached.last_accessed_at, cached.inserted_at))
			.map(|(key, _)| key.clone())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashMap as StdHashMap;
	use std::io::{BufRead, BufReader};
	use std::net::{TcpListener, TcpStream};
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::time::Duration as StdDuration;

	const WEEKLY_NONCE_SAMPLE_COUNT: u64 = 100_000_000;

	#[derive(Clone, Debug, PartialEq, Eq)]
	struct FakeAwsKmsRequest {
		target: String,
		authorization: String,
		security_token: Option<String>,
	}

	#[derive(Default)]
	struct FakeAwsKmsState {
		requests: Vec<FakeAwsKmsRequest>,
		ciphertexts: StdHashMap<Vec<u8>, Vec<u8>>,
	}

	struct FakeAwsKmsServer {
		endpoint: String,
		state: Arc<Mutex<FakeAwsKmsState>>,
		stop: Arc<AtomicBool>,
		thread: Option<std::thread::JoinHandle<()>>,
	}

	impl FakeAwsKmsServer {
		fn start(key_id: &str) -> Self {
			let listener = TcpListener::bind("127.0.0.1:0").unwrap();
			listener.set_nonblocking(true).unwrap();
			let endpoint = format!("http://{}", listener.local_addr().unwrap());
			let state = Arc::new(Mutex::new(FakeAwsKmsState::default()));
			let stop = Arc::new(AtomicBool::new(false));
			let key_id = key_id.to_string();
			let thread_state = state.clone();
			let thread_stop = stop.clone();
			let thread = std::thread::spawn(move || {
				while !thread_stop.load(Ordering::SeqCst) {
					match listener.accept() {
						Ok((stream, _)) => {
							handle_fake_aws_kms_connection(stream, &key_id, &thread_state);
						}
						Err(error) if error.kind() == ErrorKind::WouldBlock => {
							std::thread::sleep(StdDuration::from_millis(10));
						}
						Err(_) => break,
					}
				}
			});
			Self {
				endpoint,
				state,
				stop,
				thread: Some(thread),
			}
		}

		fn endpoint(&self) -> &str {
			&self.endpoint
		}

		fn requests(&self) -> Vec<FakeAwsKmsRequest> {
			self.state.lock().unwrap().requests.clone()
		}
	}

	impl Drop for FakeAwsKmsServer {
		fn drop(&mut self) {
			self.stop.store(true, Ordering::SeqCst);
			let _ = TcpStream::connect(
				self.endpoint
					.strip_prefix("http://")
					.expect("endpoint prefix"),
			);
			if let Some(thread) = self.thread.take() {
				let _ = thread.join();
			}
		}
	}

	fn handle_fake_aws_kms_connection(
		stream: TcpStream,
		key_id: &str,
		state: &Arc<Mutex<FakeAwsKmsState>>,
	) {
		stream
			.set_read_timeout(Some(StdDuration::from_secs(1)))
			.unwrap();
		let mut reader = BufReader::new(stream.try_clone().unwrap());
		let mut request_line = String::new();
		if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
			return;
		}
		let mut headers = StdHashMap::new();
		loop {
			let mut line = String::new();
			if reader.read_line(&mut line).is_err() || line == "\r\n" {
				break;
			}
			if let Some((name, value)) = line.split_once(':') {
				headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
			}
		}
		let content_length = headers
			.get("content-length")
			.and_then(|value| value.parse::<usize>().ok())
			.unwrap_or(0);
		let mut body = vec![0_u8; content_length];
		if reader.read_exact(&mut body).is_err() {
			return;
		}
		let target = headers
			.get("x-amz-target")
			.cloned()
			.unwrap_or_else(|| "unknown".into());
		let authorization = headers.get("authorization").cloned().unwrap_or_default();
		let security_token = headers.get("x-amz-security-token").cloned();
		{
			let mut guard = state.lock().unwrap();
			guard.requests.push(FakeAwsKmsRequest {
				target: target.clone(),
				authorization,
				security_token,
			});
		}
		let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
		let response_body = match target.as_str() {
			"TrentService.Encrypt" => {
				let plaintext = base64::engine::general_purpose::STANDARD
					.decode(payload["Plaintext"].as_str().unwrap())
					.unwrap();
				let mut ciphertext = b"cipher:".to_vec();
				ciphertext.extend_from_slice(&plaintext);
				state
					.lock()
					.unwrap()
					.ciphertexts
					.insert(ciphertext.clone(), plaintext);
				serde_json::json!({
					"CiphertextBlob": base64::engine::general_purpose::STANDARD.encode(ciphertext),
					"KeyId": key_id,
				})
			}
			"TrentService.Decrypt" => {
				let ciphertext = base64::engine::general_purpose::STANDARD
					.decode(payload["CiphertextBlob"].as_str().unwrap())
					.unwrap();
				let plaintext = state
					.lock()
					.unwrap()
					.ciphertexts
					.get(&ciphertext)
					.cloned()
					.unwrap();
				serde_json::json!({
					"Plaintext": base64::engine::general_purpose::STANDARD.encode(plaintext),
					"KeyId": key_id,
				})
			}
			"TrentService.DescribeKey" => serde_json::json!({
				"KeyMetadata": {
					"KeyId": key_id,
					"Arn": key_id,
					"Enabled": true,
					"KeyState": "Enabled"
				}
			}),
			other => serde_json::json!({
				"__type": "UnsupportedOperationException",
				"message": format!("unsupported fake aws-kms target {other}")
			}),
		};
		let response_body = serde_json::to_vec(&response_body).unwrap();
		let response = format!(
			"HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
			response_body.len()
		);
		let mut stream = stream;
		stream.write_all(response.as_bytes()).unwrap();
		stream.write_all(&response_body).unwrap();
		stream.flush().unwrap();
	}

	/// Real injectivity check for the chunk-nonce encoder.
	///
	/// ADR 019's acceptance bar is "distinct chunk_index inputs under
	/// one DEK produce distinct nonces across a fuzzed input space."
	/// Two layers cover that:
	///
	/// 1. A per-sample identity assertion: for every drawn input,
	///    the encoded nonce equals `version_be || input.to_be_bytes()`.
	///    Since u64 → BE-bytes is a bijection and the version prefix
	///    is constant, this is a structural proof of injectivity for
	///    the sampled input — there is no way two distinct inputs can
	///    produce equal nonces if both pass this check.
	/// 2. A bounded HashSet check (capped at 1M entries, ~30 MiB) that
	///    catches a regression where the encoder drops bits or folds
	///    state in a way that bypasses layer 1 for some samples.
	///
	/// The input draws come from a fixed-seed LCG so that a failure
	/// is reproducible and the input space spans high, low, and mixed
	/// bit positions of the u64 — not just `0..N` sequential.
	const NONCE_LADDER_HASHSET_CAP: usize = 1_000_000;

	fn assert_nonce_encoding_matches_contract(sample_count: u64) {
		use std::collections::HashSet;

		let cap = (sample_count as usize).min(NONCE_LADDER_HASHSET_CAP);
		let mut seen: HashSet<[u8; SLAB_NONCE_BYTES]> = HashSet::with_capacity(cap);

		let multiplier: u64 = 6364136223846793005;
		let increment: u64 = 1442695040888963407;
		let mut state: u64 = 0xDEAD_BEEF_FACE_FEED;

		let boundary = [
			0u64,
			1,
			0xFF,
			0xFFFF,
			0xFFFF_FFFF,
			0xFFFF_FFFF_FFFF_FFFF,
			1u64 << 32,
			1u64 << 63,
			(1u64 << 63) | 1,
			0xAAAA_AAAA_AAAA_AAAA,
			0x5555_5555_5555_5555,
		];

		let mut drawn: u64 = 0;
		let check =
			|chunk_index: u64, seen: &mut HashSet<[u8; SLAB_NONCE_BYTES]>, drawn: &mut u64| {
				let nonce = encode_chunk_nonce(chunk_index);
				assert_eq!(
					u32::from_be_bytes(nonce[..4].try_into().expect("version slice")),
					SLAB_NONCE_VERSION,
					"nonce version drifted at chunk_index={chunk_index}"
				);
				assert_eq!(
					&nonce[4..],
					&chunk_index.to_be_bytes(),
					"nonce chunk-index encoding drifted at chunk_index={chunk_index}"
				);
				if (*drawn as usize) < NONCE_LADDER_HASHSET_CAP && !seen.insert(nonce) {
					panic!("duplicate nonce observed at chunk_index={chunk_index}");
				}
				*drawn += 1;
			};

		for chunk_index in boundary {
			check(chunk_index, &mut seen, &mut drawn);
		}
		while drawn < sample_count {
			state = state.wrapping_mul(multiplier).wrapping_add(increment);
			check(state, &mut seen, &mut drawn);
		}
	}

	fn encrypt_test_tiered_slab(
		crypto: &CryptoManager,
		slab_id: SlabId,
		tenant_id: TenantId,
		plaintext: &[u8],
	) -> EncryptedTieredSlab {
		crypto
			.encrypt_tiered_slab_object(
				slab_id,
				&TieredSlabObjectMetadata {
					tenant_id,
					created_hlc: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: u32::try_from(plaintext.len())
							.expect("test slab plaintext length must fit in u32"),
						flags: 0,
					}],
				},
				plaintext,
			)
			.unwrap()
	}

	#[test]
	fn chunk_aad_matches_golden_layout() {
		let aad = encode_chunk_aad(
			TenantId::new(0x0102_0304_0506_0708),
			SlabId::new(0x1112_1314_1516_1718),
			9,
			SLAB_ENVELOPE_FORMAT_VERSION,
			CipherId::Aes256GcmInjective,
		);
		assert_eq!(aad.len(), SLAB_AAD_BYTES);
		assert_eq!(
			&aad,
			&[
				0, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0, 0, 0, 0,
				0, 0, 0, 0, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0, 0, 0, 0, 0, 0, 0, 9,
				0, 0, 0, 1, 1, 0, 0, 0
			]
		);
	}

	#[test]
	fn nonce_encoding_is_injective_for_sampled_space() {
		assert_nonce_encoding_matches_contract(100_000);
	}

	#[test]
	fn parallel_chunk_planning_respects_threshold_and_covers_all_chunks() {
		assert_eq!(
			parallel_chunk_worker_count_for(PARALLEL_CHUNK_CRYPTO_MIN_CHUNKS - 1, 8),
			1
		);
		assert_eq!(
			parallel_chunk_worker_count_for(PARALLEL_CHUNK_CRYPTO_MIN_CHUNKS, 8),
			8
		);
		assert_eq!(
			parallel_chunk_worker_count_for(PARALLEL_CHUNK_CRYPTO_MIN_CHUNKS, 64),
			PARALLEL_CHUNK_CRYPTO_MAX_WORKERS
		);

		let ranges = parallel_chunk_ranges(257, 8);
		assert_eq!(ranges.first().unwrap().start, 0);
		assert_eq!(ranges.last().unwrap().end, 257);
		for pair in ranges.windows(2) {
			assert_eq!(pair[0].end, pair[1].start);
		}
	}

	#[test]
	#[ignore = "weekly ladder: run via ci/check_nonce_ladder.sh"]
	fn nonce_encoding_is_injective_for_weekly_ladder() {
		let sample_count = std::env::var("NONCE_INJECTIVE_SAMPLES")
			.ok()
			.and_then(|raw| raw.parse::<u64>().ok())
			.unwrap_or(WEEKLY_NONCE_SAMPLE_COUNT);
		eprintln!("running nonce injectivity ladder with {sample_count} samples");
		assert_nonce_encoding_matches_contract(sample_count);
	}

	#[test]
	fn adr022_range_plan_requires_requested_range_in_footer_index() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = b"abcdefghijklmno".to_vec();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(41),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 11,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 4,
						byte_length: 5,
						flags: 0,
					}],
				},
				&plaintext,
			)
			.unwrap();
		let object_size = encrypted.bytes.len() as u64;
		let tail_start = object_size.saturating_sub(64 * 1024) as usize;
		let tail_window = &encrypted.bytes[tail_start..];
		let err = crypto
			.plan_tiered_range_read(SlabId::new(41), object_size, tail_window, None, 0..4)
			.err()
			.expect("range plan should reject ranges absent from footer index");
		assert!(
			err.to_string().contains("footer index"),
			"unexpected error: {err}"
		);
	}

	#[test]
	fn adr022_round_trips_with_cached_keys() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = b"hello wendelon tiered slab".repeat(4000);
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(7), TenantId::ZERO, &plaintext);
		assert!(is_encrypted_tiered_blob(&encrypted.bytes));
		assert!(
			!encrypted
				.bytes
				.windows(b"hello".len())
				.any(|w| w == b"hello")
		);

		let decrypted = crypto
			.decrypt_tiered_slab(SlabId::new(7), &encrypted.bytes)
			.unwrap();
		assert_eq!(decrypted.plaintext, plaintext);
		assert_eq!(decrypted.tenant_id, TenantId::ZERO);
		assert_eq!(decrypted.slab_id, SlabId::new(7));
		assert_eq!(decrypted.tenant_kek_id, encrypted.tenant_kek_id);
	}

	#[test]
	fn large_adr022_round_trip_matches_parallel_chunk_contract() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let chunk_count = PARALLEL_CHUNK_CRYPTO_MIN_CHUNKS + 3;
		let plaintext = vec![0x5a; chunk_count * SLAB_ENCRYPTION_CHUNK_BYTES + 17];
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(17), TenantId::ZERO, &plaintext);
		let decrypted = crypto
			.decrypt_tiered_slab(SlabId::new(17), &encrypted.bytes)
			.unwrap();
		assert_eq!(decrypted.plaintext, plaintext);
		assert_eq!(decrypted.chunk_count as usize, chunk_count + 1);
	}

	#[test]
	fn key_material_bytes_drop_zeroizes_memory() {
		let raw = Box::into_raw(Box::new(
			KeyMaterialBytes::copy_from_slice(&[0x5a; 32], "test key material").unwrap(),
		));
		unsafe {
			std::ptr::drop_in_place(raw);
			let scrubbed = std::ptr::read(raw.cast::<[u8; 32]>());
			assert_eq!(scrubbed, [0_u8; 32]);
			std::alloc::dealloc(
				raw.cast::<u8>(),
				std::alloc::Layout::new::<KeyMaterialBytes>(),
			);
		}
	}

	#[test]
	fn locked_key_material_scrub_zeroizes_inner_bytes() {
		let mut locked = LockedKeyMaterial::new(
			KeyMaterialBytes::copy_from_slice(&[0xa5; 32], "locked key material").unwrap(),
		)
		.unwrap();
		assert_eq!(locked.key.0, [0xa5; 32]);
		locked.scrub_for_drop();
		assert_eq!(locked.key.0, [0_u8; 32]);
		assert!(!locked.locked);
	}

	#[cfg(not(feature = "fips"))]
	#[test]
	fn dev_insecure_root_kek_persists_per_data_dir() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-dev-root-kek-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let provider = DevRootKekProvider::from_dev_insecure_dir(&dir).unwrap();
		let root_kek_path = dev_insecure_root_kek_path(&dir);
		assert!(root_kek_path.exists());
		let tenant_kek = random_256_bit_key().unwrap();
		let wrapped = provider.wrap_tenant_kek(&tenant_kek).unwrap();

		let reopened = DevRootKekProvider::from_dev_insecure_dir(&dir).unwrap();
		let unwrapped = reopened.unwrap_tenant_kek(&wrapped).unwrap();
		assert_eq!(unwrapped.as_ref(), tenant_kek.as_ref());

		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[cfg(not(feature = "fips"))]
	#[test]
	fn dev_insecure_root_kek_enroll_existing_requires_materialized_key() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-dev-root-kek-enroll-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();

		let err = match RootKekProviderConfig::DevInsecure.initialize(RootKekProviderInit {
			data_dir: &dir,
			bootstrap_mode: RootKekBootstrapMode::EnrollExisting,
		}) {
			Ok(_) => panic!("enrolling an unmateralized dev-insecure root KEK should fail"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::NotFound);

		let generated = RootKekProviderConfig::DevInsecure.build(&dir).unwrap();
		let tenant_kek = random_256_bit_key().unwrap();
		let wrapped = generated.wrap_tenant_kek(&tenant_kek).unwrap();

		let enrolled = RootKekProviderConfig::DevInsecure
			.initialize(RootKekProviderInit {
				data_dir: &dir,
				bootstrap_mode: RootKekBootstrapMode::EnrollExisting,
			})
			.unwrap();
		let unwrapped = enrolled.unwrap_tenant_kek(&wrapped).unwrap();
		assert_eq!(unwrapped.as_ref(), tenant_kek.as_ref());

		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[cfg(not(feature = "fips"))]
	#[test]
	fn dev_insecure_root_kek_reports_ready_and_rotation_unsupported() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-dev-root-kek-health-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let provider = RootKekProviderConfig::DevInsecure.build(&dir).unwrap();
		let health = provider.health().unwrap();
		assert!(health.ready);
		assert!(health.detail.is_none());

		let err = provider.rotate_root_kek().unwrap_err();
		assert_eq!(err.kind(), ErrorKind::Unsupported);

		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn local_sealed_root_kek_persists_per_data_dir_with_env_secret() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-local-sealed-root-kek-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let env_name = format!(
			"WENDELON_ROOT_KEK_SECRET_{}_{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		);
		unsafe {
			std::env::set_var(&env_name, "correct horse battery staple");
		}

		let provider = RootKekProviderConfig::LocalSealed(LocalSealedRootKekConfig {
			unlock_secret_env: Some(env_name.clone()),
			unlock_secret_file: None,
		})
		.build(&dir)
		.unwrap();
		let root_kek_path = local_sealed_root_kek_path(&dir);
		assert!(root_kek_path.exists());
		let tenant_kek = random_256_bit_key().unwrap();
		let wrapped = provider.wrap_tenant_kek(&tenant_kek).unwrap();

		let reopened = RootKekProviderConfig::LocalSealed(LocalSealedRootKekConfig {
			unlock_secret_env: Some(env_name.clone()),
			unlock_secret_file: None,
		})
		.build(&dir)
		.unwrap();
		let unwrapped = reopened.unwrap_tenant_kek(&wrapped).unwrap();
		assert_eq!(unwrapped.as_ref(), tenant_kek.as_ref());

		unsafe {
			std::env::remove_var(&env_name);
		}
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn local_sealed_root_kek_supports_unlock_secret_file() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-local-sealed-root-kek-file-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let secret_file = dir.join("root-kek.secret");
		std::fs::write(&secret_file, b"mounted-secret-material").unwrap();

		let provider = RootKekProviderConfig::LocalSealed(LocalSealedRootKekConfig {
			unlock_secret_env: None,
			unlock_secret_file: Some(secret_file.clone()),
		})
		.build(&dir)
		.unwrap();
		let tenant_kek = random_256_bit_key().unwrap();
		let wrapped = provider.wrap_tenant_kek(&tenant_kek).unwrap();

		let reopened = RootKekProviderConfig::LocalSealed(LocalSealedRootKekConfig {
			unlock_secret_env: None,
			unlock_secret_file: Some(secret_file),
		})
		.build(&dir)
		.unwrap();
		let unwrapped = reopened.unwrap_tenant_kek(&wrapped).unwrap();
		assert_eq!(unwrapped.as_ref(), tenant_kek.as_ref());

		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn local_sealed_root_kek_rejects_wrong_unlock_secret() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-local-sealed-root-kek-wrong-secret-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let secret_file = dir.join("root-kek.secret");
		std::fs::write(&secret_file, b"correct-secret").unwrap();
		let provider_cfg = RootKekProviderConfig::LocalSealed(LocalSealedRootKekConfig {
			unlock_secret_env: None,
			unlock_secret_file: Some(secret_file.clone()),
		});
		let provider = provider_cfg.build(&dir).unwrap();
		let tenant_kek = random_256_bit_key().unwrap();
		let wrapped = provider.wrap_tenant_kek(&tenant_kek).unwrap();

		std::fs::write(&secret_file, b"wrong-secret").unwrap();
		let err = match provider_cfg.build(&dir) {
			Ok(_) => panic!("local-sealed provider should reject an invalid unlock secret"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("unlock secret is invalid"));

		std::fs::write(&secret_file, b"correct-secret").unwrap();
		let reopened = provider_cfg.build(&dir).unwrap();
		let unwrapped = reopened.unwrap_tenant_kek(&wrapped).unwrap();
		assert_eq!(unwrapped.as_ref(), tenant_kek.as_ref());

		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn aws_kms_root_kek_persists_per_data_dir_with_env_credentials() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-aws-kms-root-kek-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let key_id = "arn:aws:kms:us-east-1:111122223333:key/test-root-kek";
		let server = FakeAwsKmsServer::start(key_id);
		let access_key_env = format!("WENDELON_AWS_KMS_ACCESS_KEY_{}", uuid::Uuid::new_v4());
		let secret_key_env = format!("WENDELON_AWS_KMS_SECRET_KEY_{}", uuid::Uuid::new_v4());
		let session_token_env = format!("WENDELON_AWS_KMS_SESSION_TOKEN_{}", uuid::Uuid::new_v4());
		unsafe {
			std::env::set_var(&access_key_env, "AKIATESTACCESSKEY");
			std::env::set_var(&secret_key_env, "super-secret-test-key");
			std::env::set_var(&session_token_env, "session-token-test");
		}
		let provider_cfg = RootKekProviderConfig::AwsKms(AwsKmsRootKekConfig {
			region: "us-east-1".into(),
			key_id: key_id.into(),
			endpoint: Some(server.endpoint().into()),
			access_key_id_env: access_key_env.clone(),
			secret_access_key_env: secret_key_env.clone(),
			session_token_env: session_token_env.clone(),
		});

		let provider = provider_cfg.build(&dir).unwrap();
		let root_kek_path = aws_kms_root_kek_path(&dir);
		assert!(root_kek_path.exists());
		let tenant_kek = random_256_bit_key().unwrap();
		let wrapped = provider.wrap_tenant_kek(&tenant_kek).unwrap();

		let reopened = provider_cfg.build(&dir).unwrap();
		let unwrapped = reopened.unwrap_tenant_kek(&wrapped).unwrap();
		assert_eq!(unwrapped.as_ref(), tenant_kek.as_ref());

		let health = reopened.health().unwrap();
		assert!(health.ready);
		assert!(health.detail.is_none());

		let requests = server.requests();
		assert_eq!(
			requests
				.iter()
				.map(|request| request.target.as_str())
				.collect::<Vec<_>>(),
			vec![
				"TrentService.Encrypt",
				"TrentService.Decrypt",
				"TrentService.DescribeKey"
			]
		);
		for request in requests {
			assert!(
				request
					.authorization
					.contains("Credential=AKIATESTACCESSKEY/"),
				"missing access key in aws-kms authorization header: {:?}",
				request
			);
			assert_eq!(
				request.security_token.as_deref(),
				Some("session-token-test")
			);
		}

		unsafe {
			std::env::remove_var(&access_key_env);
			std::env::remove_var(&secret_key_env);
			std::env::remove_var(&session_token_env);
		}
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn pkcs11_provider_seam_is_configurable_but_build_is_unsupported() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-pkcs11-root-kek-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let module_path = dir.join("libpkcs11-test.so");
		std::fs::write(&module_path, b"pkcs11 seam only").unwrap();
		let pin_env = format!("WENDELON_PKCS11_PIN_{}", uuid::Uuid::new_v4());
		unsafe {
			std::env::set_var(&pin_env, "123456");
		}

		let cfg = RootKekProviderConfig::Pkcs11(Pkcs11RootKekConfig {
			library_path: module_path.clone(),
			token_label: "WendelonToken".into(),
			key_label: "root-kek".into(),
			pin_env: pin_env.clone(),
			slot: Some(5),
		});
		cfg.validate().unwrap();
		let err = match cfg.build(&dir) {
			Ok(_) => panic!("pkcs11 provider seam should remain unsupported at build time"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::Unsupported);
		assert!(err.to_string().contains("provider seam only"));
		assert!(
			err.to_string()
				.contains(module_path.to_string_lossy().as_ref())
		);

		unsafe {
			std::env::remove_var(&pin_env);
		}
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn footer_hmac_rejects_tamper_before_unwrap() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(9), TenantId::ZERO, b"tamper me");
		let parsed = parse_encrypted_footer(&encrypted.bytes).unwrap();
		let mut tampered = encrypted.bytes.clone();
		assert!(!parsed.wrapped_slab_dek_range_in_blob.is_empty());
		tampered[parsed.wrapped_slab_dek_range_in_blob.start] ^= 0x80;

		let err = crypto
			.decrypt_tiered_slab(SlabId::new(9), &tampered)
			.unwrap_err();
		assert!(
			err.to_string().contains("footer HMAC failed"),
			"unexpected decrypt error: {err}"
		);
	}

	#[test]
	fn decrypt_rejected_emits_audit_event_with_stage() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let audit = Arc::new(crate::audit::InMemoryAuditSink::new());
		crypto
			.attach_audit_sink(
				NodeId {
					id: 42,
					_reserved: 0,
				},
				audit.clone(),
			)
			.unwrap();
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(27), TenantId::ZERO, b"audit me");
		let parsed = parse_encrypted_footer(&encrypted.bytes).unwrap();
		let mut tampered = encrypted.bytes.clone();
		tampered[parsed.wrapped_slab_dek_range_in_blob.start] ^= 0x40;

		let err = crypto
			.decrypt_tiered_slab(SlabId::new(27), &tampered)
			.unwrap_err();
		assert!(err.to_string().contains("footer HMAC failed"));
		let events = audit.events();
		let event = events
			.iter()
			.find(|event| event.event_type() == "crypto.decrypt_rejected")
			.expect("decrypt rejection should emit audit event");
		assert_eq!(event.node_id.id, 42);
		assert!(event.message.contains("failure_stage=hmac"));
		assert!(event.message.contains("expected_slab_id=27"));
		assert!(event.message.contains("tenant_id=0"));
		assert!(event.message.contains("tenant_kek_id="));
	}

	#[test]
	fn rotation_keeps_old_tenant_key_versions_readable() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let before =
			encrypt_test_tiered_slab(&crypto, SlabId::new(10), TenantId::ZERO, b"before rotation");
		let rotate = crypto.prepare_rotation(TenantId::ZERO).unwrap();
		crypto.apply_rotation(&rotate).unwrap();
		let after =
			encrypt_test_tiered_slab(&crypto, SlabId::new(11), TenantId::ZERO, b"after rotation");

		assert_ne!(before.tenant_kek_id, after.tenant_kek_id);
		assert_eq!(
			crypto
				.decrypt_tiered_slab(SlabId::new(10), &before.bytes)
				.unwrap()
				.plaintext,
			b"before rotation"
		);
		assert_eq!(
			crypto
				.decrypt_tiered_slab(SlabId::new(11), &after.bytes)
				.unwrap()
				.plaintext,
			b"after rotation"
		);
	}
}
