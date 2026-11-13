use crate::audit::{AuditAuthority, AuditEvent, AuditOperation, AuditOutcome, AuditSink};
use crate::metrics::global as metrics;
use crate::schema::{
	ChunkSizeBytes, EncryptedChunkCount, EncryptedChunkIndex, IdentityPrefix, NodeId,
	RecordBodyByteLen, RecordBodyByteOffset, SlabId, TenantId, TenantKekId,
};
use crate::storage::ownership::DataDirectoryOwner;
use crate::storage::tier_object_layout::{LogicalExtent, TierObjectLayout};
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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::{Error, ErrorKind};
use std::io::{Read, Write};
use std::ops::Range;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

pub const SLAB_MAX_BYTES_HARD_CEILING: u64 = 1 << 35;
pub const SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
pub const SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE: ChunkSizeBytes =
	ChunkSizeBytes::new(SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES as u32);
pub const SLAB_ENCRYPTION_CHUNK_SIZES: [usize; 5] =
	[4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024];
pub const SLAB_AEAD_TAG_BYTES: usize = 16;
pub const SLAB_AAD_BYTES: usize = 48;
pub const SLAB_STATS_AAD_BYTES: usize = 64;
pub const SLAB_NONCE_BYTES: usize = 12;
pub const SLAB_NONCE_VERSION: u32 = 0;
/// The envelope versions this build writes and reads. Both carry the object's
/// logical base in the footer.
pub const SLAB_ENVELOPE_FORMAT_VERSION: u32 = 3;
pub const SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS: u32 = 4;
/// Envelope versions written before objects carried a logical base. Their
/// layout cannot be validated, so they refuse typed `MalformedObjectLayout`
/// (pre-release: no migration).
const SLAB_ENVELOPE_FORMAT_VERSIONS_WITHOUT_LOGICAL_BASE: [u32; 2] = [1, 2];
pub const SLAB_STATS_FORMAT_VERSION_ADR039: u16 = 1;
pub const DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES: usize = 2048;
pub const DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES: usize = 10_000;
pub const HAS_ROW_TOMBSTONE_RECORDS_BIT: u8 = 1 << 0;

const _: [(); 8] = [(); std::mem::size_of::<u64>()];
const ADR_TIERED_SLAB_MAGIC_HEAD: [u8; 8] = *b"WENDSLAB";
const ADR_TIERED_SLAB_MAGIC_TAIL: [u8; 8] = *b"BALSDNEW";
const ADR_TIERED_SLAB_HEADER_BYTES: usize = 64;
const ADR_TIERED_SLAB_FOOTER_BYTES: usize = 256;
const ADR_TIERED_SLAB_TAIL_BYTES: usize = 16;
const ADR_TIERED_SLAB_INDEX_ENTRY_BYTES: usize = 32;
const ADR_TIERED_SLAB_WIDE_ID_BYTES: usize = 16;
const ADR_TIERED_SLAB_WRAPPED_DEK_OFFSET: usize = 96;
const ADR_TIERED_SLAB_WRAPPED_DEK_BYTES: usize = 88;
const ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES: usize = 40;
/// The object's logical base (`TierObjectLayout`): the last eight bytes
/// before the HMAC, so the footer HMAC binds it.
const ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET: usize = 184;
const ADR_TIERED_SLAB_LOGICAL_BASE_BYTES: usize = 8;
const ADR_TIERED_SLAB_HMAC_OFFSET: usize = 192;
const ADR_TIERED_SLAB_HMAC_BYTES: usize = 32;
const ADR_TIERED_SLAB_RESERVED_SUFFIX_BYTES: usize = 32;
const ADR039_STATS_LOCATOR_OFFSET: usize = 224;
const ADR039_STATS_LOCATOR_BYTES: usize = 32;
const ADR039_STATS_FLAG_PRESENT_AND_ENCRYPTED: u16 = 1 << 0;
const ADR039_STATS_FLAG_HAS_SMALL_SET: u16 = 1 << 1;
const ADR039_STATS_FLAG_HAS_BLOOM: u16 = 1 << 2;
const ADR039_STATS_FLAG_HAS_HLL: u16 = 1 << 3;
const ADR039_STATS_FLAG_ALWAYS_ON_COMPLETE_FOR_SUPPORTED: u16 = 1 << 4;
const ADR159_STATS_FLAG_HAS_RANGE_DISTRIBUTION: u16 = 1 << 5;
const ADR039_STATS_FLAGS_RESERVED_MASK: u16 = !0x003F;
const _: [(); ADR039_STATS_LOCATOR_OFFSET] =
	[(); ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES];
const _: [(); ADR_TIERED_SLAB_FOOTER_BYTES] =
	[(); ADR039_STATS_LOCATOR_OFFSET + ADR039_STATS_LOCATOR_BYTES];
const _: [(); ADR_TIERED_SLAB_RESERVED_SUFFIX_BYTES] = [(); ADR039_STATS_LOCATOR_BYTES];
const _: [(); ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET] =
	[(); ADR_TIERED_SLAB_WRAPPED_DEK_OFFSET + ADR_TIERED_SLAB_WRAPPED_DEK_BYTES];
const _: [(); ADR_TIERED_SLAB_HMAC_OFFSET] =
	[(); ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET + ADR_TIERED_SLAB_LOGICAL_BASE_BYTES];
/// The largest wrapped slab DEK any footer can carry. A slab DEK is
/// `KeyMaterialBytes` (32 bytes) for every cipher (`PreparedSlabDek` keys both
/// AES-256-GCM and AES-256-GCM-SIV with it), and its only wrapping is
/// `SlabDek::wrap` -> `wrap_key`: AES-256 key wrap with padding (RFC 5649,
/// aws-lc-rs `KeyWrapPadded`) under the tenant KEK. RFC 5649 §4.1 pads the key
/// to whole 8-byte semiblocks and prepends one 8-byte integrity semiblock, so
/// a 32-byte key wraps to 40 bytes. Root-KEK providers wrap tenant KEKs, which
/// the footer never stores.
const AES_KWP_SEMIBLOCK_BYTES: usize = 8;
const MAX_WRAPPED_SLAB_DEK_BYTES: usize = std::mem::size_of::<KeyMaterialBytes>()
	.div_ceil(AES_KWP_SEMIBLOCK_BYTES)
	* AES_KWP_SEMIBLOCK_BYTES
	+ AES_KWP_SEMIBLOCK_BYTES;
const _: () = assert!(MAX_WRAPPED_SLAB_DEK_BYTES <= ADR_TIERED_SLAB_WRAPPED_DEK_BYTES);
const _: () = assert!(MAX_WRAPPED_SLAB_DEK_BYTES == ADR_TIERED_SLAB_WRAPPED_DEK_PREFIX_BYTES);
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
	owner: Option<&'a DataDirectoryOwner>,
}

impl<'a> RootKekProviderInit<'a> {
	fn generate_or_create(data_dir: &'a Path) -> Self {
		Self {
			data_dir,
			bootstrap_mode: RootKekBootstrapMode::GenerateOrCreate,
			owner: None,
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

	#[cfg(test)]
	pub(crate) fn build(&self, data_dir: &Path) -> std::io::Result<Arc<dyn RootKekProvider>> {
		self.initialize(RootKekProviderInit::generate_or_create(data_dir))
	}

	/// Opens the provider for `data_dir`. Only a genuinely fresh forming store
	/// passes `GenerateOrCreate`; every other open is `EnrollExisting`, which
	/// reads the artifact and never generates one (ADR 0726).
	pub(crate) fn build_with_owner(
		&self,
		data_dir: &Path,
		owner: &DataDirectoryOwner,
		bootstrap_mode: RootKekBootstrapMode,
	) -> std::io::Result<Arc<dyn RootKekProvider>> {
		owner.ensure_contains(data_dir)?;
		match self {
			Self::DevInsecure => {
				#[cfg(any(test, not(feature = "fips")))]
				owner.ensure_contains(dev_insecure_root_kek_path(data_dir))?;
			}
			Self::LocalSealed(_) => owner.ensure_contains(local_sealed_root_kek_path(data_dir))?,
			Self::AwsKms(_) => owner.ensure_contains(aws_kms_root_kek_path(data_dir))?,
			Self::Pkcs11(_) => {}
		}
		self.initialize(RootKekProviderInit {
			data_dir,
			bootstrap_mode,
			owner: Some(owner),
		})
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

/// Version of the replicated root check (ADR 0726).
pub const ROOT_CHECK_VERSION: u16 = 1;
/// AES-KWP output length for the 32-byte check plaintext.
pub const ROOT_CHECK_WRAPPED_BYTES: usize = 40;
/// The fixed public plaintext every root wraps to produce its check value.
const ROOT_CHECK_PLAINTEXT_V1: [u8; 32] = *b"wendelon root check plaintext v1";

/// First 16 bytes of SHA-256 over the big-endian version and the wrapped bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RootCheckId([u8; 16]);

impl RootCheckId {
	pub fn as_bytes(&self) -> &[u8; 16] {
		&self.0
	}

	#[cfg(test)]
	pub(crate) fn for_test(fill: u8) -> Self {
		Self([fill; 16])
	}
}

impl std::fmt::Display for RootCheckId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for byte in self.0 {
			write!(f, "{byte:02x}")?;
		}
		Ok(())
	}
}

/// The cluster's write-once root compatibility check: the AES-KWP wrap of one
/// fixed public constant under the root KEK. A compatibility check value, not
/// cluster identity, proof of custody, or proof about any tenant KEK.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootCheck {
	pub version: u16,
	#[serde(with = "serde_bytes")]
	pub wrapped: Vec<u8>,
}

impl RootCheck {
	/// Version first, then the fixed size. Nothing else is read before this.
	pub fn validate(&self) -> Result<(), &'static str> {
		if self.version != ROOT_CHECK_VERSION {
			return Err("unsupported root check version");
		}
		if self.wrapped.len() != ROOT_CHECK_WRAPPED_BYTES {
			return Err("root check has the wrong length");
		}
		Ok(())
	}

	pub fn id(&self) -> RootCheckId {
		let mut input = Vec::with_capacity(2 + self.wrapped.len());
		input.extend_from_slice(&self.version.to_be_bytes());
		input.extend_from_slice(&self.wrapped);
		let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &input);
		let mut id = [0_u8; 16];
		id.copy_from_slice(&digest.as_ref()[..16]);
		RootCheckId(id)
	}
}

/// A well-formed root check for reducer tests that use no real provider.
#[cfg(test)]
pub(crate) fn test_root_check() -> RootCheck {
	RootCheck {
		version: ROOT_CHECK_VERSION,
		wrapped: vec![0xC3; ROOT_CHECK_WRAPPED_BYTES],
	}
}

/// The final artifact file name of every implemented root KEK provider that
/// this build can open.
#[cfg(any(test, not(feature = "fips")))]
const ROOT_KEK_ARTIFACT_FILES: &[&str] = &[
	DEV_INSECURE_ROOT_KEK_FILE,
	LOCAL_SEALED_ROOT_KEK_FILE,
	AWS_KMS_ROOT_KEK_FILE,
];
#[cfg(not(any(test, not(feature = "fips"))))]
const ROOT_KEK_ARTIFACT_FILES: &[&str] = &[LOCAL_SEALED_ROOT_KEK_FILE, AWS_KMS_ROOT_KEK_FILE];

/// ADR 0726: whether `path` is the root KEK directory of a data directory,
/// empty or holding nothing but the implemented providers' own root artifact
/// files. Enrollment places exactly that before a member's first start, and a
/// forming node writes it before anything else, so the setup censuses treat
/// it as neutral, as they treat `LOCK`. The one classifier for all of them.
///
/// Neutral only: it is not recovery evidence, authorizes no store identity,
/// allocator position, incarnation, keyset or membership, and says nothing
/// about the artifact's content, which the provider still opens and the
/// cluster's root check still verifies. A symlinked directory or file, a
/// nested directory or any other entry is not this, and errors propagate.
pub(crate) fn is_root_kek_artifact_dir(path: &Path) -> std::io::Result<bool> {
	// Every provider keeps its artifact in the same directory.
	const _: () = assert!(
		LOCAL_SEALED_ROOT_KEK_DIR.len() == AWS_KMS_ROOT_KEK_DIR.len()
			&& LOCAL_SEALED_ROOT_KEK_DIR.len() == ".wendelon".len()
	);
	if path.file_name().and_then(|name| name.to_str()) != Some(LOCAL_SEALED_ROOT_KEK_DIR) {
		return Ok(false);
	}
	if !std::fs::symlink_metadata(path)?.is_dir() {
		return Ok(false);
	}
	for entry in std::fs::read_dir(path)? {
		let entry = entry?;
		let name = entry.file_name();
		let known = ROOT_KEK_ARTIFACT_FILES
			.iter()
			.any(|file| name.to_str() == Some(file));
		if !known || !std::fs::symlink_metadata(entry.path())?.is_file() {
			return Ok(false);
		}
	}
	Ok(true)
}

fn compute_root_check(provider: &dyn RootKekProvider) -> std::io::Result<RootCheck> {
	let plaintext =
		KeyMaterialBytes::copy_from_slice(&ROOT_CHECK_PLAINTEXT_V1, "root check plaintext")?;
	let check = RootCheck {
		version: ROOT_CHECK_VERSION,
		wrapped: provider.wrap_tenant_kek(&plaintext)?,
	};
	check.validate().map_err(Error::other)?;
	Ok(check)
}

/// Whether `provider` reproduces `check`: the same wrapped bytes, and the
/// check unwraps to the prescribed plaintext. `Ok(false)` is a definitive
/// mismatch; an unsupported or malformed check is an error.
fn verify_root_check(provider: &dyn RootKekProvider, check: &RootCheck) -> std::io::Result<bool> {
	check
		.validate()
		.map_err(|reason| Error::new(ErrorKind::InvalidData, reason))?;
	let candidate = compute_root_check(provider)?;
	if candidate.wrapped != check.wrapped {
		return Ok(false);
	}
	match provider.unwrap_tenant_kek(&check.wrapped) {
		Ok(plaintext) => Ok(plaintext.as_ref() == ROOT_CHECK_PLAINTEXT_V1),
		Err(_) => Ok(false),
	}
}

#[cfg(test)]
pub(crate) mod binding_test_hooks {
	use super::CryptoManager;
	use std::sync::Mutex;

	type Hook = Box<dyn Fn(&CryptoManager) + Send>;
	static BEFORE_VERIFIED_INSTALL: Mutex<Vec<(usize, Hook)>> = Mutex::new(Vec::new());

	/// Runs `hook` once, between the unverified fence and the verified
	/// install of the next delivery on `manager`.
	pub(crate) fn on_next_verified_install(manager: &CryptoManager, hook: Hook) {
		BEFORE_VERIFIED_INSTALL
			.lock()
			.unwrap()
			.push((manager.cell_key(), hook));
	}

	type Once = Box<dyn FnOnce() + Send>;
	static BETWEEN_UNWRAP_AND_INSERT: Mutex<Vec<(u64, Once)>> = Mutex::new(Vec::new());

	/// Runs `hook` once, in the next tenant-KEK unwrap under the binding
	/// `serial`, after the unwrap and before the cache insert.
	pub(crate) fn on_next_unwrap_before_insert(serial: u64, hook: Once) {
		BETWEEN_UNWRAP_AND_INSERT
			.lock()
			.unwrap()
			.push((serial, hook));
	}

	pub(super) fn between_unwrap_and_insert(serial: u64) {
		let hook = {
			let mut hooks = BETWEEN_UNWRAP_AND_INSERT.lock().unwrap();
			hooks
				.iter()
				.position(|(s, _)| *s == serial)
				.map(|i| hooks.remove(i).1)
		};
		if let Some(hook) = hook {
			hook();
		}
	}

	pub(super) fn before_verified_install(manager: &CryptoManager) {
		let key = manager.cell_key();
		let hook = {
			let mut hooks = BEFORE_VERIFIED_INSTALL.lock().unwrap();
			hooks
				.iter()
				.position(|(k, _)| *k == key)
				.map(|i| hooks.remove(i).1)
		};
		if let Some(hook) = hook {
			hook(manager);
		}
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
	/// The root check the proposer verified its root against before wrapping.
	pub under_root: RootCheckId,
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
	pub chunk_size_bytes: ChunkSizeBytes,
	pub chunk_count: u32,
	pub slab_format_version: u32,
	pub stats_locator: Option<TieredStatsBlockLocator>,
	pub has_row_tombstone_records: bool,
	/// The binding this object was encrypted under; see `still_current`.
	pub binding_serial: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecryptedTieredSlab {
	pub plaintext: Vec<u8>,
	pub tenant_id: TenantId,
	pub slab_id: SlabId,
	pub tenant_kek_id: TenantKekId,
	pub cipher_id: CipherId,
	pub chunk_size_bytes: ChunkSizeBytes,
	pub chunk_count: u32,
	/// The object's validated layout: where its plaintext sits in the slab's
	/// logical offsets.
	pub layout: TierObjectLayout,
	/// The distinct record start offsets the object's offset index lists,
	/// decoded after the surface HMAC that binds the index and the chunk AEAD
	/// verified. They describe this object's own layout: authenticated, but
	/// written by whichever build produced the object, so they do not prove
	/// that every acknowledged record was kept. They are logical, each inside
	/// `layout`.
	pub record_offsets: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FooterSummary {
	pub slab_format_version: u32,
	pub tenant_id: TenantId,
	pub slab_id: SlabId,
	pub tenant_kek_id: TenantKekId,
	pub cipher_id: CipherId,
	pub chunk_size_bytes: ChunkSizeBytes,
	pub chunk_count: u32,
	pub stats_locator: Option<TieredStatsBlockLocator>,
	pub has_row_tombstone_records: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredOffsetIndexEntry {
	pub key_locator: IdentityPrefix,
	pub byte_offset: u64,
	pub byte_length: u32,
	pub flags: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TieredStatsBlockLocator {
	pub stats_format_version: u16,
	pub stats_flags: u16,
	pub stats_block_len: u32,
	pub stats_plaintext_len: u32,
	pub stats_column_count: u32,
	pub stats_block_hash128: [u8; 16],
}

impl TieredStatsBlockLocator {
	pub fn has_small_set(self) -> bool {
		self.stats_flags & ADR039_STATS_FLAG_HAS_SMALL_SET != 0
	}

	pub fn has_bloom(self) -> bool {
		self.stats_flags & ADR039_STATS_FLAG_HAS_BLOOM != 0
	}

	pub fn has_hll(self) -> bool {
		self.stats_flags & ADR039_STATS_FLAG_HAS_HLL != 0
	}

	pub fn has_range_distribution(self) -> bool {
		self.stats_flags & ADR159_STATS_FLAG_HAS_RANGE_DISTRIBUTION != 0
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecryptedTieredStatsBlock {
	pub plaintext: Vec<u8>,
	pub locator: TieredStatsBlockLocator,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TieredColumnStatsBlock {
	Encrypted {
		ciphertext: Vec<u8>,
		plaintext_len: u32,
		column_count: u32,
		flags: u16,
	},
	Plaintext {
		plaintext: Vec<u8>,
		schema_generation: u64,
		column_count: u32,
		flags: u16,
	},
}

impl TieredColumnStatsBlock {
	pub fn encrypted(
		ciphertext: Vec<u8>,
		plaintext_len: u32,
		column_count: u32,
		flags: u16,
	) -> Self {
		Self::Encrypted {
			ciphertext,
			plaintext_len,
			column_count,
			flags,
		}
	}

	pub fn plaintext(
		plaintext: Vec<u8>,
		schema_generation: u64,
		column_count: u32,
		flags: u16,
	) -> Self {
		Self::Plaintext {
			plaintext,
			schema_generation,
			column_count,
			flags,
		}
	}

	fn plaintext_len(&self) -> std::io::Result<u32> {
		match self {
			Self::Encrypted { plaintext_len, .. } => Ok(*plaintext_len),
			Self::Plaintext { plaintext, .. } => u32::try_from(plaintext.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"ADR 039 stats block plaintext exceeds u32::MAX bytes",
				)
			}),
		}
	}

	fn column_count(&self) -> u32 {
		match self {
			Self::Encrypted { column_count, .. } | Self::Plaintext { column_count, .. } => {
				*column_count
			}
		}
	}

	fn flags(&self) -> u16 {
		match self {
			Self::Encrypted { flags, .. } | Self::Plaintext { flags, .. } => *flags,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredSlabObjectMetadata {
	pub tenant_id: TenantId,
	/// Transitional until the slab footer carries the full in-memory HLC.
	pub created_hlc: u64,
	pub chunk_size_bytes: ChunkSizeBytes,
	/// Where the plaintext starts in the slab's logical offsets; every
	/// `offset_index` entry is logical and lies in `[logical_base,
	/// logical_base + plaintext length)`.
	pub logical_base: u64,
	pub offset_index: Vec<TieredOffsetIndexEntry>,
	pub column_stats_block: Option<TieredColumnStatsBlock>,
	pub has_row_tombstone_records: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TieredPlaintextRange {
	pub start: RecordBodyByteOffset,
	pub len: RecordBodyByteLen,
}

impl TieredPlaintextRange {
	pub fn new(start: RecordBodyByteOffset, len: RecordBodyByteLen) -> Self {
		Self { start, len }
	}

	fn from_usize_unchecked(start: usize, len: usize) -> std::io::Result<Self> {
		let start = u64::try_from(start).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered plaintext range start does not fit into u64",
			)
		})?;
		let len = u64::try_from(len).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered plaintext range length does not fit into u64",
			)
		})?;
		Ok(Self {
			start: RecordBodyByteOffset::new(start),
			len: RecordBodyByteLen::new(len),
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredChunkRunReadPlan {
	first_chunk: EncryptedChunkIndex,
	chunk_count: EncryptedChunkCount,
	chunk_range: Range<usize>,
	base_plaintext_offset: usize,
	ciphertext_range_in_object: Range<u64>,
}

impl TieredChunkRunReadPlan {
	pub fn first_chunk(&self) -> EncryptedChunkIndex {
		self.first_chunk
	}

	pub fn chunk_count(&self) -> EncryptedChunkCount {
		self.chunk_count
	}

	pub fn ciphertext_range_in_object(&self) -> Range<u64> {
		self.ciphertext_range_in_object.clone()
	}
}

pub struct TieredMultiRangeReadPlan {
	footer: FooterSummary,
	tenant_id: TenantId,
	slab_id: SlabId,
	slab_format_version: u32,
	cipher_id: CipherId,
	chunk_size_bytes: ChunkSizeBytes,
	plaintext_len: usize,
	chunk_runs: Vec<TieredChunkRunReadPlan>,
	requested_plaintext_ranges: Vec<Range<usize>>,
	prepared_slab_dek: Arc<PreparedSlabDek>,
}

impl TieredMultiRangeReadPlan {
	pub fn footer(&self) -> FooterSummary {
		self.footer
	}

	pub fn chunk_runs(&self) -> &[TieredChunkRunReadPlan] {
		self.chunk_runs.as_slice()
	}

	pub fn ciphertext_ranges_in_object(&self) -> Vec<Range<u64>> {
		self.chunk_runs
			.iter()
			.map(TieredChunkRunReadPlan::ciphertext_range_in_object)
			.collect()
	}

	pub fn decrypt<B: AsRef<[u8]>>(&self, ciphertext_runs: &[B]) -> std::io::Result<Vec<Vec<u8>>> {
		if ciphertext_runs.len() != self.chunk_runs.len() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"tiered multi-range read expected {} ciphertext runs, got {}",
					self.chunk_runs.len(),
					ciphertext_runs.len()
				),
			));
		}

		let mut decrypted_runs = Vec::with_capacity(self.chunk_runs.len());
		for (run, ciphertext) in self.chunk_runs.iter().zip(ciphertext_runs) {
			let decrypted = decrypt_partial_chunks(
				self.cipher_id,
				self.tenant_id,
				self.slab_id,
				self.slab_format_version,
				self.prepared_slab_dek.clone(),
				self.chunk_size_bytes,
				self.plaintext_len,
				ciphertext.as_ref(),
				run.chunk_range.clone(),
			)?;
			decrypted_runs.push(decrypted);
		}

		let mut out = Vec::with_capacity(self.requested_plaintext_ranges.len());
		for requested in &self.requested_plaintext_ranges {
			if requested.is_empty() {
				out.push(Vec::new());
				continue;
			}
			let Some((run, decrypted)) =
				self.chunk_runs
					.iter()
					.zip(decrypted_runs.iter())
					.find(|(run, decrypted)| {
						let run_start = run.base_plaintext_offset;
						run_start
							.checked_add(decrypted.len())
							.is_some_and(|run_end| {
								requested.start >= run_start && requested.end <= run_end
							})
					})
			else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"requested plaintext range is not covered by decrypted chunk runs",
				));
			};
			// Covered above: `run.base_plaintext_offset <= requested.start`.
			let relative_start = requested.start - run.base_plaintext_offset;
			let relative_end = requested.end - run.base_plaintext_offset;
			out.push(decrypted[relative_start..relative_end].to_vec());
		}
		Ok(out)
	}
}

struct TieredRangePlanningContext {
	parsed: ParsedEncryptedFooterFields,
	footer: FooterSummary,
	/// The requested record's physical range in the object's plaintext,
	/// converted once by the object's layout.
	record_physical_range: Range<usize>,
	prepared_slab_dek: Arc<PreparedSlabDek>,
}

pub struct TieredRangeReadPlan {
	footer: FooterSummary,
	tenant_id: TenantId,
	slab_id: SlabId,
	slab_format_version: u32,
	cipher_id: CipherId,
	chunk_size_bytes: ChunkSizeBytes,
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
			self.slab_format_version,
			self.prepared_slab_dek.clone(),
			self.chunk_size_bytes,
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

pub struct TieredStatsBlockReadPlan {
	footer: FooterSummary,
	tenant_id: TenantId,
	slab_id: SlabId,
	cipher_id: CipherId,
	locator: TieredStatsBlockLocator,
	stats_range_in_object: Range<u64>,
	prepared_slab_dek: Arc<PreparedSlabDek>,
}

impl TieredStatsBlockReadPlan {
	pub fn footer(&self) -> FooterSummary {
		self.footer
	}

	pub fn locator(&self) -> TieredStatsBlockLocator {
		self.locator
	}

	pub fn stats_range_in_object(&self) -> Range<u64> {
		self.stats_range_in_object.clone()
	}

	pub fn decrypt(
		&self,
		ciphertext: &[u8],
		schema_generation: u64,
	) -> std::io::Result<DecryptedTieredStatsBlock> {
		let start = Instant::now();
		let result = (|| {
			let expected_ciphertext_len =
				(self.stats_range_in_object.end - self.stats_range_in_object.start) as usize;
			if ciphertext.len() != expected_ciphertext_len {
				return Err(Error::new(
					ErrorKind::UnexpectedEof,
					format!(
						"tiered stats read expected {expected_ciphertext_len} ciphertext bytes, got {}",
						ciphertext.len()
					),
				));
			}
			let plaintext = decrypt_stats_block(
				self.prepared_slab_dek.as_ref(),
				self.cipher_id,
				self.tenant_id,
				self.slab_id,
				schema_generation,
				self.locator,
				ciphertext,
			)?;
			Ok(DecryptedTieredStatsBlock {
				plaintext,
				locator: self.locator,
			})
		})();
		if result.is_err() {
			metrics().pbc_slab_stats_decrypt_failed_total.inc();
		}
		metrics().observe_pbc_slab_stats_decrypt(start.elapsed());
		result
	}
}

#[derive(Clone)]
pub struct CryptoManager {
	_directory_owner: Option<DataDirectoryOwner>,
	/// The loaded root provider. Reached only to produce the candidate root
	/// check, to verify against an authoritative one, and to build a binding.
	provider: Arc<dyn RootKekProvider>,
	cache_config: CryptoCacheConfig,
	cell: Arc<RwLock<BindingCell>>,
	/// Bindings displaced by a fence, destroyed outside the publication
	/// critical section.
	displaced: Arc<Mutex<Vec<Arc<Bound>>>>,
	/// Whether the last key authority delivery returned an error. Reported
	/// by readiness; the fence, not this flag, is what protects.
	last_delivery_failed: Arc<std::sync::atomic::AtomicBool>,
	audit_context: Arc<Mutex<Option<CryptoAuditContext>>>,
}

/// The one place a root binding lives. Every key, cached or not, is reached
/// through the `Arc<Bound>` this cell hands out (ADR 0726).
struct BindingCell {
	/// Applied index of the last accepted authority delivery.
	accepted_index: Option<u64>,
	/// No delivery below this index may install a binding.
	fenced_index: u64,
	state: BindingState,
}

enum BindingState {
	/// A loaded provider that no authoritative root check has been compared
	/// with. Holds no keysets and no caches.
	Unverified,
	Verified(Arc<Bound>),
	/// The loaded provider does not reproduce the authoritative root check.
	Mismatch(RootCheckId),
}

/// A root verified against one authoritative root check, with everything that
/// may be used under it: the tenant keysets and both key caches.
struct Bound {
	id: RootCheckId,
	serial: u64,
	provider: Arc<dyn RootKekProvider>,
	keysets: Mutex<HashMap<TenantId, TenantKeyset>>,
	tenant_kek_cache: Mutex<KeyCache<(TenantId, TenantKekId), LockedKeyMaterial>>,
	slab_dek_cache: Mutex<KeyCache<SlabDekCacheKey, PreparedSlabDek>>,
}

static BOUND_SERIAL: AtomicU64 = AtomicU64::new(1);

/// Why a crypto operation was not admitted. Carried as the inner error of
/// the `std::io::Error` the operation returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RootBindingRefusal {
	/// No authoritative root check has been verified yet (pending).
	#[error(
		"RootNotVerified: this node has not verified its root KEK against the cluster root check"
	)]
	NotVerified,
	/// The loaded root KEK does not reproduce the cluster root check.
	#[error("RootKekMismatch: this node's root KEK is not the cluster's root KEK")]
	Mismatch,
}

impl RootBindingRefusal {
	/// Pending is "not yet": a caller that retries on availability errors
	/// retries, as it did when a tenant KEK had not arrived. A mismatch is
	/// definitive and is never retried into.
	fn into_io(self) -> Error {
		let kind = match self {
			Self::NotVerified => ErrorKind::WouldBlock,
			Self::Mismatch => ErrorKind::PermissionDenied,
		};
		Error::new(kind, self)
	}

	pub(crate) fn into_startup_error(self, id: RootCheckId) -> Error {
		Error::new(
			ErrorKind::PermissionDenied,
			format!("{self} (cluster root check {id}); startup refused"),
		)
	}

	pub fn of(error: &Error) -> Option<Self> {
		error
			.get_ref()
			.and_then(|inner| inner.downcast_ref::<Self>())
			.copied()
	}
}

/// What a node knows about its root KEK against the cluster root check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootBindingStatus {
	Pending,
	Verified(RootCheckId),
	Mismatch(RootCheckId),
}

/// The root check and the tenant keysets of one applied state, delivered
/// together.
#[derive(Clone, Debug)]
pub struct KeyAuthority {
	pub applied_index: u64,
	pub root_check: Option<RootCheck>,
	pub keysets: HashMap<TenantId, TenantKeyset>,
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
			_directory_owner: None,
			provider: root_kek,
			cache_config,
			cell: Arc::new(RwLock::new(BindingCell {
				accepted_index: None,
				fenced_index: 0,
				state: BindingState::Unverified,
			})),
			displaced: Arc::new(Mutex::new(Vec::new())),
			last_delivery_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
			audit_context: Arc::new(Mutex::new(None)),
		}
	}

	pub(crate) fn new_with_cache_config_and_owner(
		root_kek: Arc<dyn RootKekProvider>,
		cache_config: CryptoCacheConfig,
		owner: DataDirectoryOwner,
	) -> Self {
		let mut manager = Self::new_with_cache_config(root_kek, cache_config);
		manager._directory_owner = Some(owner);
		manager
	}

	/// Dev/test-only helper: an ephemeral root bound to its own candidate
	/// check. Production bootstrap selects a provider through
	/// `NodeBootstrapConfig` and verifies against the replicated check.
	#[cfg(test)]
	pub(crate) fn new_dev() -> Self {
		let manager = Self::new(Arc::new(
			DevRootKekProvider::new_ephemeral_for_tests()
				.expect("dev root KEK provider must build"),
		));
		manager
			.bind_to_own_candidate_without_cluster()
			.expect("a dev root binds to its own candidate");
		manager
	}

	pub fn selected_cipher_label(&self) -> &'static str {
		selected_cipher_id().label()
	}

	pub fn selected_cipher_detection_method(&self) -> &'static str {
		selected_cipher_detection_method()
	}

	pub fn root_kek_provider_label(&self) -> &'static str {
		self.provider.label()
	}

	pub fn root_kek_provider_health(&self) -> std::io::Result<RootKekProviderHealth> {
		self.provider.health()
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

	/// The candidate root check of the loaded provider: the wrap of the fixed
	/// public constant. The only key operation an unverified manager offers.
	/// It takes no key argument and yields no binding.
	pub(crate) fn candidate_root_check(&self) -> std::io::Result<RootCheck> {
		compute_root_check(self.provider.as_ref())
	}

	#[cfg(test)]
	fn cell_key(&self) -> usize {
		Arc::as_ptr(&self.cell) as usize
	}

	/// Takes the current binding, or refuses. Every key use starts here.
	fn admit(&self) -> std::io::Result<Arc<Bound>> {
		let cell = self
			.cell
			.read()
			.map_err(|_| RootBindingRefusal::NotVerified.into_io())?;
		match &cell.state {
			BindingState::Verified(bound) => Ok(bound.clone()),
			BindingState::Unverified => Err(RootBindingRefusal::NotVerified.into_io()),
			BindingState::Mismatch(_) => Err(RootBindingRefusal::Mismatch.into_io()),
		}
	}

	pub fn key_authority_delivery_failed(&self) -> bool {
		self.last_delivery_failed.load(Ordering::Relaxed)
	}

	pub fn root_binding_status(&self) -> RootBindingStatus {
		let Ok(cell) = self.cell.read() else {
			return RootBindingStatus::Pending;
		};
		match &cell.state {
			BindingState::Unverified => RootBindingStatus::Pending,
			BindingState::Verified(bound) => RootBindingStatus::Verified(bound.id),
			BindingState::Mismatch(id) => RootBindingStatus::Mismatch(*id),
		}
	}

	/// The id of the root check this node's root is verified against.
	pub fn verified_root_check_id(&self) -> std::io::Result<RootCheckId> {
		Ok(self.admit()?.id)
	}

	/// Whether `serial` names the binding a new operation would get now. A
	/// local narrowing before an upload submits its pointer; it proves
	/// nothing about what commits afterwards.
	pub fn still_current(&self, serial: u64) -> bool {
		self.admit().is_ok_and(|bound| bound.serial == serial)
	}

	/// Called by the state machine, under its state guard, before a state
	/// whose root check is `next` becomes visible. Removes a verified binding
	/// of any other id so no new operation can take it. No I/O, no await; the
	/// displaced binding is destroyed later, outside this critical section.
	pub(crate) fn fence_before_publication(&self, next: Option<RootCheckId>, index: u64) {
		let mut cell = match self.cell.write() {
			Ok(cell) => cell,
			// A poisoned cell already refuses every admission; still fence.
			Err(poisoned) => poisoned.into_inner(),
		};
		cell.fenced_index = cell.fenced_index.max(index);
		let keep = match (&cell.state, next) {
			(BindingState::Verified(bound), Some(next)) => bound.id == next,
			(BindingState::Verified(_), None) => false,
			_ => true,
		};
		if keep {
			return;
		}
		let old = std::mem::replace(&mut cell.state, BindingState::Unverified);
		drop(cell);
		if let BindingState::Verified(bound) = old {
			match self.displaced.lock() {
				Ok(mut displaced) => displaced.push(bound),
				// Leak rather than destroy key material under the state guard.
				Err(_) => std::mem::forget(bound),
			}
		}
	}

	/// Installs the root check and the keysets of one applied state. The cell
	/// is unverified before a new or different check is compared, and every
	/// error leaves it so.
	pub(crate) fn sync_key_authority(&self, authority: &KeyAuthority) -> std::io::Result<()> {
		let graveyard = self
			.displaced
			.lock()
			.map(|mut displaced| std::mem::take(&mut *displaced))
			.unwrap_or_default();
		drop(graveyard);
		let index = authority.applied_index;
		let (check, old) = {
			let mut cell = self
				.cell
				.write()
				.map_err(|_| Error::other("crypto root binding cell poisoned"))?;
			if index < cell.fenced_index || cell.accepted_index.is_some_and(|a| index <= a) {
				return Ok(());
			}
			cell.accepted_index = Some(index);
			let Some(check) = authority.root_check.as_ref() else {
				let old = std::mem::replace(&mut cell.state, BindingState::Unverified);
				drop(cell);
				drop(old);
				return Ok(());
			};
			let id = check.id();
			if let BindingState::Verified(bound) = &cell.state
				&& bound.id == id
			{
				let next = Arc::new(Bound::new(
					id,
					self.provider.clone(),
					authority.keysets.clone(),
					self.cache_config,
				));
				let old = std::mem::replace(&mut cell.state, BindingState::Verified(next));
				drop(cell);
				drop(old);
				return Ok(());
			}
			let old = std::mem::replace(&mut cell.state, BindingState::Unverified);
			(check.clone(), old)
		};
		drop(old);
		#[cfg(test)]
		binding_test_hooks::before_verified_install(self);
		let id = check.id();
		let verdict = verify_root_check(self.provider.as_ref(), &check);
		let mut cell = self
			.cell
			.write()
			.map_err(|_| Error::other("crypto root binding cell poisoned"))?;
		if cell.accepted_index != Some(index) || cell.fenced_index > index {
			return Ok(());
		}
		match verdict {
			Ok(true) => {
				cell.state = BindingState::Verified(Arc::new(Bound::new(
					id,
					self.provider.clone(),
					authority.keysets.clone(),
					self.cache_config,
				)));
				Ok(())
			}
			Ok(false) => {
				cell.state = BindingState::Mismatch(id);
				Ok(())
			}
			Err(error) => Err(error),
		}
	}

	/// Binds the loaded root to its own candidate check with no cluster and
	/// no replicated state. For fixtures that use a crypto manager with no
	/// node around it; a node verifies against the replicated check instead.
	///
	/// Private: only this module's fixture factories reach it. A node's
	/// manager is admitted by committed key authority and by nothing else.
	fn bind_to_own_candidate_without_cluster(&self) -> std::io::Result<()> {
		let keysets = match self.admit() {
			Ok(bound) => bound.lock_keysets()?.clone(),
			Err(_) => HashMap::new(),
		};
		self.deliver_without_cluster(Some(self.candidate_root_check()?), keysets)
	}

	fn deliver_without_cluster(
		&self,
		root_check: Option<RootCheck>,
		keysets: HashMap<TenantId, TenantKeyset>,
	) -> std::io::Result<()> {
		let applied_index = {
			let cell = self
				.cell
				.read()
				.map_err(|_| Error::other("crypto root binding cell poisoned"))?;
			cell.accepted_index
				.map_or(0, |a| a + 1)
				.max(cell.fenced_index)
		};
		self.sync_key_authority(&KeyAuthority {
			applied_index,
			root_check,
			keysets,
		})
	}

	/// Test-only: replaces the keysets under this manager's own candidate
	/// check, as a keyset-only authority delivery would.
	#[cfg(test)]
	pub(crate) fn replace_keysets_from_authoritative_snapshot(
		&self,
		snapshot: &HashMap<TenantId, TenantKeyset>,
	) -> std::io::Result<()> {
		self.deliver_without_cluster(Some(self.candidate_root_check()?), snapshot.clone())
	}

	pub fn tenant_keysets_snapshot(&self) -> std::io::Result<HashMap<TenantId, TenantKeyset>> {
		Ok(self.admit()?.lock_keysets()?.clone())
	}

	/// Fixture helper that seeds local in-memory keysets without a Raft
	/// commit. Production code must never call this path.
	pub(crate) fn install_local_only_for_tests(&self, tenant_id: TenantId) -> std::io::Result<()> {
		let bound = self.admit()?;
		let exists = bound
			.lock_keysets()?
			.get(&tenant_id)
			.map(|keyset| !keyset.keys.is_empty())
			.unwrap_or(false);
		if exists {
			return Ok(());
		}
		let rotation = bound.prepare_rotation(tenant_id)?;
		bound.apply_rotation(&rotation)
	}

	/// A fresh tenant KEK wrapped under the verified root, with the id of
	/// the root check it was wrapped under.
	pub fn prepare_rotation(
		&self,
		tenant_id: TenantId,
	) -> std::io::Result<InstallTenantKekVersion> {
		self.admit()?.prepare_rotation(tenant_id)
	}

	/// Test-only: applies a rotation to the current binding's keysets.
	#[cfg(test)]
	pub(crate) fn apply_rotation(&self, rotation: &InstallTenantKekVersion) -> std::io::Result<()> {
		self.admit()?.apply_rotation(rotation)
	}

	pub fn active_tenant_kek_id(&self, tenant_id: TenantId) -> std::io::Result<TenantKekId> {
		let bound = self.admit()?;
		let guard = bound.lock_keysets()?;
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
		let bound = self.admit()?;
		let (tenant_kek_id, tenant_kek) = bound.get_active_tenant_kek(metadata.tenant_id)?;
		let wrapped_slab_dek = SlabDek::fresh_for(slab_id)?.wrap(&tenant_kek)?;
		let cipher_id = selected_cipher_id();
		// ADR 039 forbids writer-side lazy CSB attachment: no stats means a
		// v1 slab, while any v2 locator is sealed before the footer HMAC.
		let slab_format_version = if metadata.column_stats_block.is_some() {
			SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS
		} else {
			SLAB_ENVELOPE_FORMAT_VERSION
		};
		validate_slab_chunk_size(metadata.chunk_size_bytes)?;
		let plaintext_len = u64::try_from(plaintext.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered slab plaintext length does not fit into u64",
			)
		})?;
		let layout = TierObjectLayout::new(slab_id, metadata.logical_base, plaintext_len)?;
		let chunk_count = chunk_count_for_len(plaintext.len(), metadata.chunk_size_bytes)?;
		let encrypted_chunks = encrypt_chunks(
			cipher_id,
			metadata.tenant_id,
			slab_format_version,
			wrapped_slab_dek,
			metadata.chunk_size_bytes,
			chunk_count,
			plaintext,
		)?;
		let header_bytes = encode_adr022_header(
			slab_format_version,
			slab_id,
			metadata.tenant_id,
			metadata.created_hlc,
			cipher_id,
			metadata.chunk_size_bytes,
			chunk_count,
		)?;
		let index_bytes = encode_adr022_offset_index(&metadata.offset_index, &layout)?;
		let stats_ciphertext = metadata
			.column_stats_block
			.as_ref()
			.map(|block| {
				stats_ciphertext_for_block(
					block,
					encrypted_chunks.cache_key.as_ref(),
					cipher_id,
					metadata.tenant_id,
					slab_id,
				)
			})
			.transpose()?;
		let record_count = u64::try_from(metadata.offset_index.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered offset index record count exceeds u64::MAX",
			)
		})?;
		let mut footer = encode_adr022_footer(
			slab_format_version,
			slab_id,
			metadata.tenant_id,
			metadata.created_hlc,
			cipher_id,
			metadata.chunk_size_bytes,
			chunk_count,
			record_count,
			tenant_kek_id,
			&encrypted_chunks.wrapped_slab_dek,
			metadata.has_row_tombstone_records,
			&layout,
		)?;
		let stats_locator = metadata
			.column_stats_block
			.as_ref()
			.zip(stats_ciphertext.as_deref())
			.map(|(block, ciphertext)| stats_locator_for_block(block, ciphertext))
			.transpose()?;
		encode_adr039_stats_locator(&mut footer, stats_locator);
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
			.and_then(|n| n.checked_add(stats_ciphertext.as_ref().map(Vec::len).unwrap_or(0)))
			.and_then(|n| n.checked_add(index_bytes.len()))
			.and_then(|n| n.checked_add(footer.len()))
			.and_then(|n| n.checked_add(ADR_TIERED_SLAB_TAIL_BYTES))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tiered slab length overflow"))?;
		let mut out = Vec::with_capacity(total_len);
		out.extend_from_slice(&ADR_TIERED_SLAB_MAGIC_HEAD);
		out.extend_from_slice(&header_bytes);
		out.extend_from_slice(&encrypted_chunks.ciphertext);
		if let Some(stats_ciphertext) = stats_ciphertext.as_deref() {
			out.extend_from_slice(stats_ciphertext);
		}
		out.extend_from_slice(&index_bytes);
		out.extend_from_slice(&footer);
		push_u32_be(&mut out, footer_len_u32);
		push_u32_be(&mut out, index_len_u32);
		out.extend_from_slice(&ADR_TIERED_SLAB_MAGIC_TAIL);

		{
			let mut slab_cache = bound.lock_slab_dek_cache()?;
			slab_cache.insert(
				slab_dek_cache_key(
					metadata.tenant_id,
					tenant_kek_id,
					slab_id,
					cipher_id,
					&encrypted_chunks.wrapped_slab_dek,
				),
				encrypted_chunks.cache_key,
			);
		}

		Ok(EncryptedTieredSlab {
			bytes: out,
			tenant_id: metadata.tenant_id,
			tenant_kek_id,
			cipher_id,
			chunk_size_bytes: metadata.chunk_size_bytes,
			chunk_count,
			slab_format_version,
			stats_locator,
			has_row_tombstone_records: metadata.has_row_tombstone_records,
			binding_serial: bound.serial,
		})
	}

	pub fn decrypt_tiered_slab(
		&self,
		expected_slab_id: SlabId,
		blob: &[u8],
	) -> std::io::Result<DecryptedTieredSlab> {
		let bound = self.admit()?;
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
		let tenant_kek = match bound.get_tenant_kek(parsed.tenant_id, parsed.tenant_kek_id) {
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
		// The verified index must lie inside the object before any chunk is
		// decrypted.
		let record_offsets = match decode_adr022_record_offsets(parsed.index, &parsed.layout) {
			Ok(offsets) => offsets,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, Some(footer), "offset_index", &error);
				return Err(error);
			}
		};
		let slab_dek = match bound.resolve_prepared_slab_dek(
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
			parsed.slab_format_version,
			slab_dek,
			parsed.chunk_size_bytes,
			parsed.chunk_count,
			parsed.layout.plaintext_len_usize(),
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
			chunk_size_bytes: parsed.chunk_size_bytes,
			chunk_count: parsed.chunk_count,
			layout: parsed.layout,
			record_offsets,
		})
	}

	pub fn decrypt_tiered_stats_block(
		&self,
		expected_slab_id: SlabId,
		blob: &[u8],
		schema_generation: u64,
	) -> std::io::Result<Option<DecryptedTieredStatsBlock>> {
		let bound = self.admit()?;
		let start = Instant::now();
		let result = (|| {
			let parsed = parse_encrypted_footer(blob)?;
			if parsed.slab_id != expected_slab_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"tiered slab footer slab_id {} does not match expected {}",
						parsed.slab_id, expected_slab_id
					),
				));
			}
			if parsed.nonce_version != SLAB_NONCE_VERSION {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"unsupported nonce version {} in tiered slab {}",
						parsed.nonce_version, expected_slab_id
					),
				));
			}
			let Some(locator) = parsed.stats_locator else {
				return Ok(None);
			};
			let tenant_kek = bound.get_tenant_kek(parsed.tenant_id, parsed.tenant_kek_id)?;
			verify_parsed_surface_hmac(&tenant_kek, &parsed)?;
			let slab_dek = bound.resolve_prepared_slab_dek(
				parsed.tenant_id,
				parsed.tenant_kek_id,
				parsed.slab_id,
				parsed.cipher_id,
				&parsed.wrapped_slab_dek,
			)?;
			let plaintext = decrypt_stats_block(
				slab_dek.as_ref(),
				parsed.cipher_id,
				parsed.tenant_id,
				parsed.slab_id,
				schema_generation,
				locator,
				parsed.stats_ciphertext,
			)?;
			Ok(Some(DecryptedTieredStatsBlock { plaintext, locator }))
		})();
		if result.is_err() {
			metrics().pbc_slab_stats_decrypt_failed_total.inc();
		}
		if !matches!(result, Ok(None)) {
			metrics().observe_pbc_slab_stats_decrypt(start.elapsed());
		}
		result
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

	pub fn plan_tiered_stats_block_read(
		&self,
		expected_slab_id: SlabId,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
	) -> std::io::Result<Option<TieredStatsBlockReadPlan>> {
		let bound = self.admit()?;
		let parsed = match parse_encrypted_footer_from_tail_window(object_size_bytes, tail_window) {
			Ok(parsed) => parsed,
			Err(error) => {
				metrics().pbc_slab_stats_decrypt_failed_total.inc();
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
			metrics().pbc_slab_stats_decrypt_failed_total.inc();
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
			metrics().pbc_slab_stats_decrypt_failed_total.inc();
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "footer_parse", &error);
			return Err(error);
		}
		let Some(locator) = parsed.stats_locator else {
			return Ok(None);
		};
		let Some(stats_range_in_object) = parsed.stats_ciphertext_range_in_object.clone() else {
			metrics().pbc_slab_stats_decrypt_failed_total.inc();
			return Err(Error::new(
				ErrorKind::InvalidData,
				"ADR 039 stats locator present without object range",
			));
		};
		let tenant_kek = match bound.get_tenant_kek(parsed.tenant_id, parsed.tenant_kek_id) {
			Ok(tenant_kek) => tenant_kek,
			Err(error) => {
				self.emit_decrypt_rejected(
					expected_slab_id,
					Some(footer),
					"tenant_kek_lookup",
					&error,
				);
				metrics().pbc_slab_stats_decrypt_failed_total.inc();
				return Err(error);
			}
		};
		if let Err(error) = verify_tail_surface_hmac(&tenant_kek, &parsed, additional_metadata) {
			metrics().pbc_slab_stats_decrypt_failed_total.inc();
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
			return Err(error);
		}
		let prepared_slab_dek = match bound.resolve_prepared_slab_dek(
			parsed.tenant_id,
			parsed.tenant_kek_id,
			parsed.slab_id,
			parsed.cipher_id,
			&parsed.wrapped_slab_dek,
		) {
			Ok(prepared) => prepared,
			Err(error) => {
				metrics().pbc_slab_stats_decrypt_failed_total.inc();
				self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
				return Err(error);
			}
		};
		Ok(Some(TieredStatsBlockReadPlan {
			footer,
			tenant_id: parsed.tenant_id,
			slab_id: parsed.slab_id,
			cipher_id: parsed.cipher_id,
			locator,
			stats_range_in_object,
			prepared_slab_dek,
		}))
	}

	/// The footer's layout is used only after the footer HMAC verified it.
	/// Then, in order: every index entry lies in the object (else typed
	/// `MalformedObjectLayout`); the logical `record` lies in the object (else
	/// typed `OutsideGeneration`, before any subtraction or index lookup); the
	/// index lists exactly that logical record.
	fn prepare_tiered_range_read(
		&self,
		expected_slab_id: SlabId,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		record: LogicalExtent,
	) -> std::io::Result<TieredRangePlanningContext> {
		let bound = self.admit()?;
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
		let tenant_kek = match bound.get_tenant_kek(parsed.tenant_id, parsed.tenant_kek_id) {
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
		let index_bytes = parsed.index_bytes(additional_metadata)?;
		if let Err(error) = validate_adr022_index_extent(index_bytes, &parsed.layout) {
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "offset_index", &error);
			return Err(error);
		}
		let record_physical_range = match parsed.layout.physical_extent(record) {
			Ok(range) => range,
			Err(error) => {
				self.emit_decrypt_rejected(expected_slab_id, Some(footer), "range_plan", &error);
				return Err(error);
			}
		};
		if let Err(error) = verify_index_contains_range(index_bytes, record) {
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "range_plan", &error);
			return Err(error);
		}
		let prepared_slab_dek = match bound.resolve_prepared_slab_dek(
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

		Ok(TieredRangePlanningContext {
			parsed,
			footer,
			record_physical_range,
			prepared_slab_dek,
		})
	}

	/// A read of `plaintext_ranges`, relative to the logical `record`'s start,
	/// from the object whose tail is `tail_window`.
	pub fn plan_tiered_multi_range_read(
		&self,
		expected_slab_id: SlabId,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		record: LogicalExtent,
		plaintext_ranges: &[TieredPlaintextRange],
	) -> std::io::Result<TieredMultiRangeReadPlan> {
		let context = self.prepare_tiered_range_read(
			expected_slab_id,
			object_size_bytes,
			tail_window,
			additional_metadata,
			record,
		)?;
		let requested_plaintext_ranges = record_relative_ranges_to_absolute(
			context.record_physical_range.clone(),
			plaintext_ranges,
			context.parsed.plaintext_len(),
		)
		.map_err(|error| {
			self.emit_decrypt_rejected(
				expected_slab_id,
				Some(context.footer),
				"range_plan",
				&error,
			);
			error
		})?;
		let chunk_runs = chunk_read_runs_for_plaintext_ranges(
			context.parsed.plaintext_len(),
			context.parsed.chunk_size_bytes,
			context.parsed.ciphertext_offset_in_object,
			&requested_plaintext_ranges,
		)
		.map_err(|error| {
			self.emit_decrypt_rejected(
				expected_slab_id,
				Some(context.footer),
				"range_plan",
				&error,
			);
			error
		})?;

		Ok(TieredMultiRangeReadPlan {
			footer: context.footer,
			tenant_id: context.parsed.tenant_id,
			slab_id: context.parsed.slab_id,
			slab_format_version: context.parsed.slab_format_version,
			cipher_id: context.parsed.cipher_id,
			chunk_size_bytes: context.parsed.chunk_size_bytes,
			plaintext_len: context.parsed.plaintext_len(),
			chunk_runs,
			requested_plaintext_ranges,
			prepared_slab_dek: context.prepared_slab_dek,
		})
	}

	/// A read of the whole logical `record` from the object whose tail is
	/// `tail_window`.
	pub fn plan_tiered_range_read(
		&self,
		expected_slab_id: SlabId,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		record: LogicalExtent,
	) -> std::io::Result<TieredRangeReadPlan> {
		let record_len = usize::try_from(record.length).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"requested record length does not fit into usize",
			)
		})?;
		let relative_range = TieredPlaintextRange::from_usize_unchecked(0, record_len)?;
		let multi = self.plan_tiered_multi_range_read(
			expected_slab_id,
			object_size_bytes,
			tail_window,
			additional_metadata,
			record,
			&[relative_range],
		)?;
		// The record's physical range, as the object's layout converted it.
		let plaintext_range = multi
			.requested_plaintext_ranges
			.first()
			.cloned()
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"tiered range read planned no record range",
				)
			})?;
		let Some(run) = multi.chunk_runs.first() else {
			return Ok(TieredRangeReadPlan {
				footer: multi.footer,
				tenant_id: multi.tenant_id,
				slab_id: multi.slab_id,
				slab_format_version: multi.slab_format_version,
				cipher_id: multi.cipher_id,
				chunk_size_bytes: multi.chunk_size_bytes,
				plaintext_len: multi.plaintext_len,
				chunk_range: 0..0,
				base_plaintext_offset: plaintext_range.start,
				requested_plaintext_range: plaintext_range,
				ciphertext_range_in_object: 0..0,
				prepared_slab_dek: multi.prepared_slab_dek,
			});
		};
		Ok(TieredRangeReadPlan {
			footer: multi.footer,
			tenant_id: multi.tenant_id,
			slab_id: multi.slab_id,
			slab_format_version: multi.slab_format_version,
			cipher_id: multi.cipher_id,
			chunk_size_bytes: multi.chunk_size_bytes,
			plaintext_len: multi.plaintext_len,
			chunk_range: run.chunk_range.clone(),
			base_plaintext_offset: run.base_plaintext_offset,
			requested_plaintext_range: plaintext_range,
			ciphertext_range_in_object: run.ciphertext_range_in_object.clone(),
			prepared_slab_dek: multi.prepared_slab_dek,
		})
	}

	pub fn encrypted_footer_summary(&self, blob: &[u8]) -> std::io::Result<Option<FooterSummary>> {
		if !is_encrypted_tiered_blob(blob) {
			return Ok(None);
		}
		let parsed = parse_encrypted_footer(blob)?;
		Ok(Some(parsed.footer_summary()))
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

impl Bound {
	fn new(
		id: RootCheckId,
		provider: Arc<dyn RootKekProvider>,
		keysets: HashMap<TenantId, TenantKeyset>,
		cache_config: CryptoCacheConfig,
	) -> Self {
		Self {
			id,
			serial: BOUND_SERIAL.fetch_add(1, Ordering::Relaxed),
			provider,
			keysets: Mutex::new(keysets),
			tenant_kek_cache: Mutex::new(KeyCache::new(
				cache_config.tenant_kek_cache_max_entries,
				DEFAULT_TENANT_KEK_IDLE_TTL,
				Some(DEFAULT_TENANT_KEK_ABSOLUTE_TTL),
			)),
			slab_dek_cache: Mutex::new(KeyCache::new(
				cache_config.slab_dek_cache_max_entries,
				DEFAULT_SLAB_DEK_IDLE_TTL,
				None,
			)),
		}
	}

	fn prepare_rotation(&self, tenant_id: TenantId) -> std::io::Result<InstallTenantKekVersion> {
		let tenant_kek = random_256_bit_key()?;
		let tenant_kek_id = self.mint_unused_tenant_kek_id(tenant_id)?;
		let wrapped_tenant_kek = self.provider.wrap_tenant_kek(&tenant_kek)?;
		let unwrapped_tenant_kek = self.provider.unwrap_tenant_kek(&wrapped_tenant_kek)?;
		if unwrapped_tenant_kek.as_ref() != tenant_kek.as_ref() {
			return Err(Error::other(
				"root KEK provider returned mismatched tenant KEK bytes after wrap/unwrap self-check",
			));
		}
		Ok(InstallTenantKekVersion {
			tenant_id,
			tenant_kek_id,
			wrapped_tenant_kek,
			under_root: self.id,
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

	fn apply_rotation(&self, rotation: &InstallTenantKekVersion) -> std::io::Result<()> {
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
			let cache_key = slab_dek_cache_key(
				tenant_id,
				tenant_kek_id,
				slab_id,
				cipher_id,
				wrapped_slab_dek,
			);
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
		slab_cache.insert(
			slab_dek_cache_key(
				tenant_id,
				tenant_kek_id,
				slab_id,
				cipher_id,
				wrapped_slab_dek,
			),
			prepared.clone(),
		);
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

		let unwrapped = self.provider.unwrap_tenant_kek(&wrapped.wrapped_key)?;
		let locked = Arc::new(LockedKeyMaterial::new(unwrapped)?);
		#[cfg(test)]
		binding_test_hooks::between_unwrap_and_insert(self.serial);
		let mut cache = self.lock_tenant_kek_cache()?;
		if let Some(key) = cache.get(&(tenant_id, tenant_kek_id)) {
			return Ok(key);
		}
		cache.insert((tenant_id, tenant_kek_id), locked.clone());
		Ok(locked)
	}

	fn invalidate_tenant(&self, tenant_id: TenantId) -> std::io::Result<()> {
		self.lock_tenant_kek_cache()?
			.invalidate_where(|(entry_tenant, _)| *entry_tenant == tenant_id);
		self.lock_slab_dek_cache()?
			.invalidate_where(|(entry_tenant, ..)| *entry_tenant == tenant_id);
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
	) -> std::io::Result<std::sync::MutexGuard<'_, KeyCache<SlabDekCacheKey, PreparedSlabDek>>> {
		self.slab_dek_cache
			.lock()
			.map_err(|_| Error::other("slab DEK cache mutex poisoned"))
	}
}

#[async_trait]
impl crate::consensus::TenantKekObserver for CryptoManager {
	fn fence_before_publication(&self, next: Option<RootCheckId>, index: u64) {
		CryptoManager::fence_before_publication(self, next, index);
	}

	async fn sync_key_authority(&self, authority: &KeyAuthority) -> std::io::Result<()> {
		let result = CryptoManager::sync_key_authority(self, authority);
		self.last_delivery_failed
			.store(result.is_err(), Ordering::Relaxed);
		result
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
		let probe = [0_u8; 32];
		match key_guard::custody::lock_memory(&probe) {
			Ok(()) => {
				key_guard::custody::unlock_memory(&probe);
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
		crypto.bind_to_own_candidate_without_cluster()?;
		crypto.install_local_only_for_tests(tenant_id)?;
		Ok(crypto)
	}

	/// Places a freshly generated development root KEK in `data_dir`, as an
	/// operator supplying a root of their own would, or as a member that was
	/// never given its cluster's root would have. Synthetic, per call.
	#[cfg(any(test, not(feature = "fips")))]
	pub fn place_fresh_dev_root_artifact(data_dir: &Path) -> std::io::Result<()> {
		persist_new_dev_insecure_root_kek(
			&dev_insecure_root_kek_path(data_dir),
			&KeyMaterialBytes::random()?,
		)
	}

	/// The enrollment procedure of ADR 0726 as a fixture performs it: copy the
	/// forming member's sealed root artifact, owner-only, to the same path in
	/// another member's data directory before that member first starts. Copies
	/// whichever provider artifact exists; the unlock capability is the
	/// caller's to supply.
	pub fn enroll_root_artifact(forming: &Path, member: &Path) -> std::io::Result<()> {
		let mut copied = false;
		for file in ROOT_KEK_ARTIFACT_FILES {
			let dir = LOCAL_SEALED_ROOT_KEK_DIR;
			let source = forming.join(dir).join(file);
			if !source.exists() {
				continue;
			}
			let target_dir = member.join(dir);
			std::fs::create_dir_all(&target_dir)?;
			let target = target_dir.join(file);
			std::fs::copy(&source, &target)?;
			#[cfg(unix)]
			{
				use std::os::unix::fs::PermissionsExt;
				std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))?;
			}
			copied = true;
		}
		if copied {
			Ok(())
		} else {
			Err(Error::new(
				ErrorKind::NotFound,
				format!("no root KEK artifact under {}", forming.display()),
			))
		}
	}

	#[cfg(any(test, not(feature = "fips")))]
	pub fn persistent_dev_crypto_manager(data_dir: &Path) -> std::io::Result<CryptoManager> {
		let crypto = CryptoManager::new(Arc::new(DevRootKekProvider::from_dev_insecure_dir(
			data_dir,
		)?));
		crypto.bind_to_own_candidate_without_cluster()?;
		Ok(crypto)
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

fn encode_stats_nonce() -> [u8; SLAB_NONCE_BYTES] {
	let mut nonce = [0_u8; SLAB_NONCE_BYTES];
	nonce[..4].copy_from_slice(&SLAB_NONCE_VERSION.to_be_bytes());
	nonce[4..].copy_from_slice(&u64::MAX.to_be_bytes());
	nonce
}

fn encode_stats_aad(
	tenant_id: TenantId,
	slab_id: SlabId,
	cipher_id: CipherId,
	schema_generation: u64,
) -> [u8; SLAB_STATS_AAD_BYTES] {
	let mut aad = [0_u8; SLAB_STATS_AAD_BYTES];
	aad[..16].copy_from_slice(&encode_u64_aad_field(tenant_id.get()));
	aad[16..32].copy_from_slice(&encode_u64_aad_field(slab_id.get()));
	aad[32..36].copy_from_slice(&SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS.to_be_bytes());
	aad[36..38].copy_from_slice(&SLAB_STATS_FORMAT_VERSION_ADR039.to_be_bytes());
	aad[38] = cipher_id.to_wire();
	aad[40..48].copy_from_slice(&schema_generation.to_be_bytes());
	// Bytes 48..64 stay reserved-zero in V1. The locator hash is checked
	// before decrypt and covered by the footer HMAC; feeding it into AAD would
	// be circular for AEAD modes whose ciphertext depends on AAD.
	aad
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

fn encrypt_stats_block(
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	schema_generation: u64,
	plaintext: &[u8],
) -> std::io::Result<Vec<u8>> {
	seal_stats_block(
		prepared,
		cipher_id,
		tenant_id,
		slab_id,
		schema_generation,
		plaintext,
	)
}

fn seal_stats_block(
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	schema_generation: u64,
	plaintext: &[u8],
) -> std::io::Result<Vec<u8>> {
	let mut out = vec![0_u8; plaintext.len() + SLAB_AEAD_TAG_BYTES];
	let (body, tag) = out.split_at_mut(plaintext.len());
	body.copy_from_slice(plaintext);
	// The stats block uses the slab DEK and a reserved nonce-domain value.
	// Slab chunk indices cannot reach u64::MAX under the hard slab size cap.
	let sealed_tag = prepared
		.aead()
		.seal_in_place_separate_tag(
			Nonce::assume_unique_for_key(encode_stats_nonce()),
			Aad::from(encode_stats_aad(
				tenant_id,
				slab_id,
				cipher_id,
				schema_generation,
			)),
			body,
		)
		.map_err(|_| Error::other("ADR 039 stats block encrypt failed"))?;
	assert_eq!(sealed_tag.as_ref().len(), SLAB_AEAD_TAG_BYTES);
	tag.copy_from_slice(sealed_tag.as_ref());
	Ok(out)
}

fn decrypt_stats_block(
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	schema_generation: u64,
	locator: TieredStatsBlockLocator,
	ciphertext: &[u8],
) -> std::io::Result<Vec<u8>> {
	if ciphertext.len() != locator.stats_block_len as usize {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats block length disagrees with locator",
		));
	}
	if stats_ciphertext_hash128(ciphertext) != locator.stats_block_hash128 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats block hash disagrees with locator",
		));
	}
	if ciphertext.len() < SLAB_AEAD_TAG_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats block shorter than AEAD tag",
		));
	}
	let mut scratch = ciphertext.to_vec();
	let opened = prepared
		.aead()
		.open_in_place(
			Nonce::assume_unique_for_key(encode_stats_nonce()),
			Aad::from(encode_stats_aad(
				tenant_id,
				slab_id,
				cipher_id,
				schema_generation,
			)),
			scratch.as_mut_slice(),
		)
		.map_err(|_| Error::new(ErrorKind::InvalidData, "ADR 039 stats block decrypt failed"))?;
	if opened.len() != locator.stats_plaintext_len as usize {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats block plaintext length disagrees with locator",
		));
	}
	Ok(opened.to_vec())
}

fn encrypt_chunk_range(
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
	slab_format_version: u32,
	chunk_size_bytes: ChunkSizeBytes,
	plaintext: &[u8],
	chunk_range: std::ops::Range<usize>,
	out: &mut [u8],
) -> std::io::Result<()> {
	let base_ciphertext_offset =
		ciphertext_offset_for_chunk(plaintext.len(), chunk_size_bytes, chunk_range.start);
	for chunk_index in chunk_range {
		let plaintext_bounds =
			plaintext_bounds_for_chunk(plaintext.len(), chunk_size_bytes, chunk_index);
		let local_cipher_start =
			ciphertext_offset_for_chunk(plaintext.len(), chunk_size_bytes, chunk_index)
				- base_ciphertext_offset;
		let local_cipher_end =
			ciphertext_offset_for_chunk(plaintext.len(), chunk_size_bytes, chunk_index + 1)
				- base_ciphertext_offset;
		let (chunk_buf, tag_buf) =
			out[local_cipher_start..local_cipher_end].split_at_mut(plaintext_bounds.len());
		chunk_buf.copy_from_slice(&plaintext[plaintext_bounds.clone()]);
		// Safety: `LessSafeKey` is sound here because slab encryption mints a
		// fresh DEK per slab, and chunk nonces are injectively derived from the
		// chunk index within that slab. The `(key, nonce)` pair cannot repeat.
		let sealed_tag = prepared
			.aead()
			.seal_in_place_separate_tag(
				Nonce::assume_unique_for_key(encode_chunk_nonce(chunk_index as u64)),
				Aad::from(encode_chunk_aad(
					tenant_id,
					slab_id,
					chunk_index as u64,
					slab_format_version,
					cipher_id,
				)),
				chunk_buf,
			)
			.map_err(|_| Error::other("slab chunk encrypt failed"))?;
		assert_eq!(sealed_tag.as_ref().len(), SLAB_AEAD_TAG_BYTES);
		tag_buf.copy_from_slice(sealed_tag.as_ref());
	}
	Ok(())
}

fn encrypt_chunks(
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_format_version: u32,
	dek: WrappedSlabDek,
	chunk_size_bytes: ChunkSizeBytes,
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
			slab_format_version,
			chunk_size_bytes,
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
				let region_start =
					ciphertext_offset_for_chunk(plaintext.len(), chunk_size_bytes, range.start);
				let region_end =
					ciphertext_offset_for_chunk(plaintext.len(), chunk_size_bytes, range.end);
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
						slab_format_version,
						chunk_size_bytes,
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
	slab_format_version: u32,
	chunk_size_bytes: ChunkSizeBytes,
	plaintext_len: usize,
	ciphertext: &[u8],
	chunk_range: std::ops::Range<usize>,
	out: &mut [u8],
) -> std::io::Result<()> {
	let chunk_size = chunk_size_usize(chunk_size_bytes);
	let base_plaintext_offset =
		plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, chunk_range.start).start;
	let mut scratch = vec![0_u8; chunk_size + SLAB_AEAD_TAG_BYTES];
	for chunk_index in chunk_range {
		let plaintext_bounds =
			plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, chunk_index);
		let ciphertext_start =
			ciphertext_offset_for_chunk(plaintext_len, chunk_size_bytes, chunk_index);
		let ciphertext_end =
			ciphertext_offset_for_chunk(plaintext_len, chunk_size_bytes, chunk_index + 1);
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
					slab_format_version,
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
	slab_format_version: u32,
	prepared: Arc<PreparedSlabDek>,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_count: u32,
	plaintext_len: usize,
	ciphertext: &[u8],
) -> std::io::Result<Vec<u8>> {
	if ciphertext_offset_for_chunk(plaintext_len, chunk_size_bytes, chunk_count as usize)
		!= ciphertext.len()
	{
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
			slab_format_version,
			chunk_size_bytes,
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
				let region_start =
					plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, range.start).start;
				let region_end =
					plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, range.end).start;
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
						slab_format_version,
						chunk_size_bytes,
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
	slab_format_version: u32,
	chunk_size_bytes: ChunkSizeBytes,
	plaintext_len: usize,
	ciphertext: &[u8],
	chunk_range: Range<usize>,
	out: &mut [u8],
) -> std::io::Result<()> {
	let chunk_size = chunk_size_usize(chunk_size_bytes);
	let base_plaintext_offset =
		plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, chunk_range.start).start;
	let base_ciphertext_offset =
		ciphertext_offset_for_chunk(plaintext_len, chunk_size_bytes, chunk_range.start);
	let mut scratch = vec![0_u8; chunk_size + SLAB_AEAD_TAG_BYTES];
	for chunk_index in chunk_range {
		let plaintext_bounds =
			plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, chunk_index);
		let ciphertext_start =
			ciphertext_offset_for_chunk(plaintext_len, chunk_size_bytes, chunk_index)
				- base_ciphertext_offset;
		let ciphertext_end =
			ciphertext_offset_for_chunk(plaintext_len, chunk_size_bytes, chunk_index + 1)
				- base_ciphertext_offset;
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
					slab_format_version,
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
	slab_format_version: u32,
	prepared: Arc<PreparedSlabDek>,
	chunk_size_bytes: ChunkSizeBytes,
	plaintext_len: usize,
	ciphertext: &[u8],
	chunk_range: Range<usize>,
) -> std::io::Result<Vec<u8>> {
	if chunk_range.is_empty() {
		return Ok(Vec::new());
	}
	let expected_ciphertext_len =
		ciphertext_offset_for_chunk(plaintext_len, chunk_size_bytes, chunk_range.end)
			.checked_sub(ciphertext_offset_for_chunk(
				plaintext_len,
				chunk_size_bytes,
				chunk_range.start,
			))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ciphertext range underflow"))?;
	if expected_ciphertext_len != ciphertext.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ciphertext length disagrees with requested chunk range",
		));
	}
	let base_plaintext_offset =
		plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, chunk_range.start).start;
	let end_plaintext_offset =
		plaintext_bounds_for_chunk(plaintext_len, chunk_size_bytes, chunk_range.end).start;
	let mut plaintext = vec![0_u8; end_plaintext_offset.saturating_sub(base_plaintext_offset)];
	decrypt_chunk_range_from_slice(
		prepared.as_ref(),
		cipher_id,
		tenant_id,
		slab_id,
		slab_format_version,
		chunk_size_bytes,
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

/// A prepared slab DEK is cached per generation: the wrapped DEK named by the
/// authenticated footer of the object being read, under its KEK. Two
/// generations of one slab id never share an entry.
type SlabDekCacheKey = (TenantId, TenantKekId, SlabId, CipherId, [u8; 32]);

fn slab_dek_cache_key(
	tenant_id: TenantId,
	tenant_kek_id: TenantKekId,
	slab_id: SlabId,
	cipher_id: CipherId,
	wrapped_slab_dek: &[u8],
) -> SlabDekCacheKey {
	let mut wrapped_digest = [0_u8; 32];
	wrapped_digest.copy_from_slice(
		aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, wrapped_slab_dek).as_ref(),
	);
	(tenant_id, tenant_kek_id, slab_id, cipher_id, wrapped_digest)
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

pub fn validate_slab_chunk_size(chunk_size_bytes: ChunkSizeBytes) -> std::io::Result<()> {
	if SLAB_ENCRYPTION_CHUNK_SIZES.contains(&(chunk_size_bytes.get() as usize)) {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		format!("unsupported ADR 022 chunk size {}", chunk_size_bytes.get()),
	))
}

fn chunk_size_usize(chunk_size_bytes: ChunkSizeBytes) -> usize {
	chunk_size_bytes.get() as usize
}

fn chunk_count_for_len(len: usize, chunk_size_bytes: ChunkSizeBytes) -> std::io::Result<u32> {
	validate_slab_chunk_size(chunk_size_bytes)?;
	let chunks = len.div_ceil(chunk_size_usize(chunk_size_bytes));
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

fn plaintext_bounds_for_chunk(
	plaintext_len: usize,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_index: usize,
) -> std::ops::Range<usize> {
	let chunk_size = chunk_size_usize(chunk_size_bytes);
	let start = chunk_index
		.checked_mul(chunk_size)
		.expect("chunk_index bounds checked by caller")
		.min(plaintext_len);
	let end = start.saturating_add(chunk_size).min(plaintext_len);
	start..end
}

fn ciphertext_offset_for_chunk(
	plaintext_len: usize,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_index: usize,
) -> usize {
	let chunk_size = chunk_size_usize(chunk_size_bytes);
	let plaintext_offset = chunk_index
		.checked_mul(chunk_size)
		.expect("chunk_index bounds checked by caller")
		.min(plaintext_len);
	let tag_offset = chunk_index
		.checked_mul(SLAB_AEAD_TAG_BYTES)
		.expect("chunk_index bounds checked by caller");
	plaintext_offset + tag_offset
}

/// `ciphertext_offset_for_chunk` for a range plan, with checked arithmetic.
fn checked_ciphertext_offset_for_chunk(
	plaintext_len: usize,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_index: usize,
) -> std::io::Result<u64> {
	let overflow = || {
		Error::new(
			ErrorKind::InvalidInput,
			"tiered chunk ciphertext offset overflow",
		)
	};
	let plaintext_offset = chunk_index
		.checked_mul(chunk_size_usize(chunk_size_bytes))
		.ok_or_else(overflow)?
		.min(plaintext_len);
	let tag_offset = chunk_index
		.checked_mul(SLAB_AEAD_TAG_BYTES)
		.ok_or_else(overflow)?;
	let offset = plaintext_offset
		.checked_add(tag_offset)
		.ok_or_else(overflow)?;
	u64::try_from(offset).map_err(|_| overflow())
}

fn record_relative_ranges_to_absolute(
	record_plaintext_range: Range<usize>,
	plaintext_ranges: &[TieredPlaintextRange],
	slab_plaintext_len: usize,
) -> std::io::Result<Vec<Range<usize>>> {
	if record_plaintext_range.start > record_plaintext_range.end
		|| record_plaintext_range.end > slab_plaintext_len
	{
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"record plaintext range {}..{} is outside slab plaintext length {}",
				record_plaintext_range.start, record_plaintext_range.end, slab_plaintext_len
			),
		));
	}

	let record_len = record_plaintext_range.end - record_plaintext_range.start;
	let mut out = Vec::with_capacity(plaintext_ranges.len());
	let mut previous_start = None;
	for range in plaintext_ranges {
		let relative_start = usize::try_from(range.start.get()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered plaintext range start does not fit into usize",
			)
		})?;
		let relative_len = usize::try_from(range.len.get()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered plaintext range length does not fit into usize",
			)
		})?;
		if previous_start.is_some_and(|previous| relative_start < previous) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tiered plaintext ranges must be sorted by start offset",
			));
		}
		previous_start = Some(relative_start);
		let relative_end = relative_start.checked_add(relative_len).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered plaintext range end offset overflow",
			)
		})?;
		if relative_end > record_len {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"tiered plaintext range {}..{} exceeds indexed record length {}",
					relative_start, relative_end, record_len
				),
			));
		}
		let absolute_start = record_plaintext_range
			.start
			.checked_add(relative_start)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"absolute tiered plaintext range start overflow",
				)
			})?;
		let absolute_end = record_plaintext_range
			.start
			.checked_add(relative_end)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"absolute tiered plaintext range end overflow",
				)
			})?;
		out.push(absolute_start..absolute_end);
	}
	Ok(out)
}

fn chunk_read_runs_for_plaintext_ranges(
	plaintext_len: usize,
	chunk_size_bytes: ChunkSizeBytes,
	ciphertext_offset_in_object: u64,
	plaintext_ranges: &[Range<usize>],
) -> std::io::Result<Vec<TieredChunkRunReadPlan>> {
	validate_slab_chunk_size(chunk_size_bytes)?;
	let chunk_size = chunk_size_usize(chunk_size_bytes);
	let mut chunk_ranges: Vec<Range<usize>> = Vec::new();
	for plaintext_range in plaintext_ranges {
		if plaintext_range.start > plaintext_range.end || plaintext_range.end > plaintext_len {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"requested plaintext range {}..{} is outside slab plaintext length {}",
					plaintext_range.start, plaintext_range.end, plaintext_len
				),
			));
		}
		if plaintext_range.is_empty() {
			continue;
		}
		let start_chunk = plaintext_range.start / chunk_size;
		// Non-empty above, so `end >= 1`.
		let end_chunk = ((plaintext_range.end - 1) / chunk_size) + 1;
		if let Some(last) = chunk_ranges.last_mut() {
			if start_chunk <= last.end {
				last.end = last.end.max(end_chunk);
				continue;
			}
		}
		chunk_ranges.push(start_chunk..end_chunk);
	}

	let mut out = Vec::with_capacity(chunk_ranges.len());
	for chunk_range in chunk_ranges {
		let chunk_count = chunk_range
			.end
			.checked_sub(chunk_range.start)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "chunk range end precedes start"))?;
		let chunk_count = u32::try_from(chunk_count).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered chunk run count exceeds u32::MAX",
			)
		})?;
		let first_chunk = u64::try_from(chunk_range.start).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"tiered chunk run start does not fit into u64",
			)
		})?;
		let ciphertext_start = ciphertext_offset_in_object
			.checked_add(checked_ciphertext_offset_for_chunk(
				plaintext_len,
				chunk_size_bytes,
				chunk_range.start,
			)?)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"tiered chunk ciphertext start overflow",
				)
			})?;
		let ciphertext_end = ciphertext_offset_in_object
			.checked_add(checked_ciphertext_offset_for_chunk(
				plaintext_len,
				chunk_size_bytes,
				chunk_range.end,
			)?)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"tiered chunk ciphertext end overflow",
				)
			})?;
		out.push(TieredChunkRunReadPlan {
			first_chunk: EncryptedChunkIndex::new(first_chunk),
			chunk_count: EncryptedChunkCount::new(chunk_count),
			base_plaintext_offset: plaintext_bounds_for_chunk(
				plaintext_len,
				chunk_size_bytes,
				chunk_range.start,
			)
			.start,
			chunk_range,
			ciphertext_range_in_object: ciphertext_start..ciphertext_end,
		});
	}
	Ok(out)
}

fn push_u32_be(buf: &mut Vec<u8>, value: u32) {
	buf.extend_from_slice(&value.to_be_bytes());
}

fn push_u64_be(buf: &mut Vec<u8>, value: u64) {
	buf.extend_from_slice(&value.to_be_bytes());
}

struct ParsedEncryptedFooterFields {
	slab_format_version: u32,
	slab_id: SlabId,
	tenant_id: TenantId,
	tenant_kek_id: TenantKekId,
	cipher_id: CipherId,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_count: u32,
	stats_locator: Option<TieredStatsBlockLocator>,
	has_row_tombstone_records: bool,
	/// The validated layout; its extent is the plaintext once the ciphertext
	/// length is known (`finalize_from_ciphertext_len`), empty before.
	layout: TierObjectLayout,
	nonce_version: u32,
	wrapped_slab_dek: Vec<u8>,
	surface_hmac: Vec<u8>,
	footer_prefix_without_hmac: Vec<u8>,
	footer_suffix_after_hmac: Vec<u8>,
	footer_len_u32: u32,
	index_len_u32: u32,
	ciphertext_offset_in_object: u64,
	stats_ciphertext_range_in_object: Option<Range<u64>>,
	header_bytes_for_hmac: Vec<u8>,
	index_from_tail: Option<Vec<u8>>,
	missing_index_range_in_object: Option<Range<u64>>,
	#[cfg(test)]
	wrapped_slab_dek_range_in_footer: std::ops::Range<usize>,
}

struct ParsedEncryptedFooter<'a> {
	slab_format_version: u32,
	slab_id: SlabId,
	tenant_id: TenantId,
	tenant_kek_id: TenantKekId,
	cipher_id: CipherId,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_count: u32,
	stats_locator: Option<TieredStatsBlockLocator>,
	has_row_tombstone_records: bool,
	/// The validated layout; its extent is the plaintext once the ciphertext
	/// length is known (`finalize_from_ciphertext_len`), empty before.
	layout: TierObjectLayout,
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
	stats_ciphertext: &'a [u8],
	#[cfg(test)]
	wrapped_slab_dek_range_in_blob: std::ops::Range<usize>,
}

impl ParsedEncryptedFooterFields {
	fn finalize_from_ciphertext_len(mut self, ciphertext_len: usize) -> std::io::Result<Self> {
		let plaintext_len = plaintext_len_from_ciphertext_len(
			ciphertext_len,
			self.chunk_count,
			self.chunk_size_bytes,
		)?;
		let plaintext_len = u64::try_from(plaintext_len).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"ADR 022 plaintext length does not fit into u64",
			)
		})?;
		self.layout = TierObjectLayout::new(self.slab_id, self.layout.base(), plaintext_len)?;
		Ok(self)
	}

	fn plaintext_len(&self) -> usize {
		self.layout.plaintext_len_usize()
	}

	fn footer_summary(&self) -> FooterSummary {
		FooterSummary {
			slab_format_version: self.slab_format_version,
			tenant_id: self.tenant_id,
			slab_id: self.slab_id,
			tenant_kek_id: self.tenant_kek_id,
			cipher_id: self.cipher_id,
			chunk_size_bytes: self.chunk_size_bytes,
			chunk_count: self.chunk_count,
			stats_locator: self.stats_locator,
			has_row_tombstone_records: self.has_row_tombstone_records,
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
			slab_format_version: self.slab_format_version,
			tenant_id: self.tenant_id,
			slab_id: self.slab_id,
			tenant_kek_id: self.tenant_kek_id,
			cipher_id: self.cipher_id,
			chunk_size_bytes: self.chunk_size_bytes,
			chunk_count: self.chunk_count,
			stats_locator: self.stats_locator,
			has_row_tombstone_records: self.has_row_tombstone_records,
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

fn stats_ciphertext_for_block(
	block: &TieredColumnStatsBlock,
	prepared: &PreparedSlabDek,
	cipher_id: CipherId,
	tenant_id: TenantId,
	slab_id: SlabId,
) -> std::io::Result<Vec<u8>> {
	match block {
		TieredColumnStatsBlock::Encrypted { ciphertext, .. } => Ok(ciphertext.clone()),
		TieredColumnStatsBlock::Plaintext {
			plaintext,
			schema_generation,
			..
		} => encrypt_stats_block(
			prepared,
			cipher_id,
			tenant_id,
			slab_id,
			*schema_generation,
			plaintext,
		),
	}
}

fn stats_locator_for_block(
	block: &TieredColumnStatsBlock,
	ciphertext: &[u8],
) -> std::io::Result<TieredStatsBlockLocator> {
	let stats_block_len = u32::try_from(ciphertext.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"ADR 039 stats block ciphertext exceeds u32::MAX bytes",
		)
	})?;
	if stats_block_len == 0 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"ADR 039 stats block ciphertext must not be empty",
		));
	}
	let stats_flags = block.flags()
		| ADR039_STATS_FLAG_PRESENT_AND_ENCRYPTED
		| ADR039_STATS_FLAG_ALWAYS_ON_COMPLETE_FOR_SUPPORTED;
	validate_stats_locator_flags(stats_flags)?;
	let mut stats_block_hash128 = [0_u8; 16];
	stats_block_hash128.copy_from_slice(&stats_ciphertext_hash128(ciphertext));
	Ok(TieredStatsBlockLocator {
		stats_format_version: SLAB_STATS_FORMAT_VERSION_ADR039,
		stats_flags,
		stats_block_len,
		stats_plaintext_len: block.plaintext_len()?,
		stats_column_count: block.column_count(),
		stats_block_hash128,
	})
}

fn stats_ciphertext_hash128(ciphertext: &[u8]) -> [u8; 16] {
	let mut hash = [0_u8; 16];
	hash.copy_from_slice(&crate::hash::blake3::digest(ciphertext)[..16]);
	hash
}

fn validate_stats_locator_flags(stats_flags: u16) -> std::io::Result<()> {
	if stats_flags & ADR039_STATS_FLAGS_RESERVED_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats_flags reserved bits must be zero",
		));
	}
	if stats_flags & ADR039_STATS_FLAG_PRESENT_AND_ENCRYPTED == 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats_flags must set present_and_encrypted",
		));
	}
	if stats_flags & ADR039_STATS_FLAG_ALWAYS_ON_COMPLETE_FOR_SUPPORTED == 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats_flags must set always_on_complete_for_supported",
		));
	}
	Ok(())
}

fn encode_adr039_stats_locator(
	footer: &mut [u8; ADR_TIERED_SLAB_FOOTER_BYTES],
	locator: Option<TieredStatsBlockLocator>,
) {
	let suffix = &mut footer
		[ADR039_STATS_LOCATOR_OFFSET..ADR039_STATS_LOCATOR_OFFSET + ADR039_STATS_LOCATOR_BYTES];
	let Some(locator) = locator else {
		suffix.fill(0);
		return;
	};
	suffix[0..2].copy_from_slice(&locator.stats_format_version.to_be_bytes());
	suffix[2..4].copy_from_slice(&locator.stats_flags.to_be_bytes());
	suffix[4..8].copy_from_slice(&locator.stats_block_len.to_be_bytes());
	suffix[8..12].copy_from_slice(&locator.stats_plaintext_len.to_be_bytes());
	suffix[12..16].copy_from_slice(&locator.stats_column_count.to_be_bytes());
	suffix[16..32].copy_from_slice(&locator.stats_block_hash128);
}

fn parse_adr039_stats_locator(
	slab_format_version: u32,
	suffix: &[u8],
) -> std::io::Result<Option<TieredStatsBlockLocator>> {
	if suffix.len() != ADR039_STATS_LOCATOR_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 stats locator must be 32 bytes",
		));
	}
	if slab_format_version == SLAB_ENVELOPE_FORMAT_VERSION {
		if suffix.iter().any(|byte| *byte != 0) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"ADR 022 footer reserved suffix must be all zero",
			));
		}
		return Ok(None);
	}
	if slab_format_version != SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("unsupported ADR slab format version {slab_format_version}"),
		));
	}
	let stats_format_version = u16::from_be_bytes(suffix[0..2].try_into().expect("slice len"));
	if stats_format_version == 0 {
		if suffix.iter().any(|byte| *byte != 0) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"ADR 039 absent stats locator must be all zero",
			));
		}
		return Ok(None);
	}
	if stats_format_version != SLAB_STATS_FORMAT_VERSION_ADR039 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("unsupported ADR 039 stats format version {stats_format_version}"),
		));
	}
	let stats_flags = u16::from_be_bytes(suffix[2..4].try_into().expect("slice len"));
	validate_stats_locator_flags(stats_flags)?;
	let stats_block_len = u32::from_be_bytes(suffix[4..8].try_into().expect("slice len"));
	let stats_plaintext_len = u32::from_be_bytes(suffix[8..12].try_into().expect("slice len"));
	let stats_column_count = u32::from_be_bytes(suffix[12..16].try_into().expect("slice len"));
	if stats_block_len == 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 039 present stats locator has zero stats_block_len",
		));
	}
	let mut stats_block_hash128 = [0_u8; 16];
	stats_block_hash128.copy_from_slice(&suffix[16..32]);
	Ok(Some(TieredStatsBlockLocator {
		stats_format_version,
		stats_flags,
		stats_block_len,
		stats_plaintext_len,
		stats_column_count,
		stats_block_hash128,
	}))
}

fn encode_adr022_header(
	slab_format_version: u32,
	slab_id: SlabId,
	tenant_id: TenantId,
	created_hlc: u64,
	cipher_id: CipherId,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_count: u32,
) -> std::io::Result<[u8; ADR_TIERED_SLAB_HEADER_BYTES]> {
	validate_slab_chunk_size(chunk_size_bytes)?;
	let mut header = [0_u8; ADR_TIERED_SLAB_HEADER_BYTES];
	header[..4].copy_from_slice(&slab_format_version.to_be_bytes());
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
	header[48..52].copy_from_slice(&chunk_size_bytes.get().to_be_bytes());
	header[52..56].copy_from_slice(&chunk_count.to_be_bytes());
	let header_crc = u64::from(crc32c(&header[..56]));
	header[56..64].copy_from_slice(&header_crc.to_be_bytes());
	Ok(header)
}

fn encode_adr022_footer(
	slab_format_version: u32,
	slab_id: SlabId,
	tenant_id: TenantId,
	created_hlc: u64,
	cipher_id: CipherId,
	chunk_size_bytes: ChunkSizeBytes,
	chunk_count: u32,
	record_count: u64,
	tenant_kek_id: TenantKekId,
	wrapped_slab_dek: &[u8],
	has_row_tombstone_records: bool,
	layout: &TierObjectLayout,
) -> std::io::Result<[u8; ADR_TIERED_SLAB_FOOTER_BYTES]> {
	validate_slab_chunk_size(chunk_size_bytes)?;
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
	footer[..4].copy_from_slice(&slab_format_version.to_be_bytes());
	footer[4] = cipher_id.to_wire();
	if has_row_tombstone_records {
		footer[5] |= HAS_ROW_TOMBSTONE_RECORDS_BIT;
	}
	encode_adr022_u64_slot(&mut footer[8..24], slab_id.get())?;
	encode_adr022_u64_slot(&mut footer[24..40], tenant_id.get())?;
	footer[40..48].copy_from_slice(&created_hlc.to_be_bytes());
	footer[48..52].copy_from_slice(&chunk_size_bytes.get().to_be_bytes());
	footer[52..56].copy_from_slice(&chunk_count.to_be_bytes());
	footer[56..64].copy_from_slice(&record_count.to_be_bytes());
	encode_adr022_u64_slot(&mut footer[64..96], tenant_kek_id.get())?;
	footer[ADR_TIERED_SLAB_WRAPPED_DEK_OFFSET
		..ADR_TIERED_SLAB_WRAPPED_DEK_OFFSET + wrapped_slab_dek.len()]
		.copy_from_slice(wrapped_slab_dek);
	footer[ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET
		..ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET + ADR_TIERED_SLAB_LOGICAL_BASE_BYTES]
		.copy_from_slice(&layout.base().to_be_bytes());
	Ok(footer)
}

fn verify_index_contains_range(index_bytes: &[u8], record: LogicalExtent) -> std::io::Result<()> {
	// Cold tiered reads currently request exactly one record span from
	// `RecordLocation`, so the ADR 022 footer index must contain the
	// same record-aligned logical `(offset, length)` pair. A future
	// multi-record pushdown path will need a separate coverage-based
	// validator.
	verify_adr022_index_contains_range(index_bytes, record)
}

/// Every entry of a verified offset index lies inside the object (`layout`),
/// or the object refuses typed `MalformedObjectLayout`.
fn validate_adr022_index_extent(
	index_bytes: &[u8],
	layout: &TierObjectLayout,
) -> std::io::Result<()> {
	decode_adr022_record_offsets(index_bytes, layout).map(|_| ())
}

fn verify_adr022_index_contains_range(
	index_bytes: &[u8],
	record: LogicalExtent,
) -> std::io::Result<()> {
	if index_bytes.len() % ADR_TIERED_SLAB_INDEX_ENTRY_BYTES != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 offset index length is not an integral number of entries",
		));
	}
	let expected_len = u32::try_from(record.length).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"requested record length does not fit into u32",
		)
	})?;
	for entry in index_bytes.chunks_exact(ADR_TIERED_SLAB_INDEX_ENTRY_BYTES) {
		let byte_offset = u64::from_be_bytes(entry[16..24].try_into().expect("slice len"));
		let byte_length = u32::from_be_bytes(entry[24..28].try_into().expect("slice len"));
		if byte_offset == record.offset && byte_length == expected_len {
			return Ok(());
		}
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		format!(
			"ADR 022 footer index does not contain requested logical record {}+{}",
			record.offset, record.length
		),
	))
}

/// The offset index of an object with `layout`. Entries are logical; one
/// outside the object refuses typed `MalformedObjectLayout`.
fn encode_adr022_offset_index(
	entries: &[TieredOffsetIndexEntry],
	layout: &TierObjectLayout,
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
		layout.check_index_entry(LogicalExtent::new(
			entry.byte_offset,
			u64::from(entry.byte_length),
		))?;
		out.extend_from_slice(entry.key_locator.as_bytes());
		push_u64_be(&mut out, entry.byte_offset);
		push_u32_be(&mut out, entry.byte_length);
		push_u32_be(&mut out, entry.flags);
	}
	Ok(out)
}

/// The distinct logical record start offsets in an ADR 022 offset index.
/// Every entry must lie inside the object's extent (`layout`); one that does
/// not refuses the object typed `MalformedObjectLayout`, and a length that is
/// not a whole number of entries refuses the index, rather than either being
/// dropped. Callers decode only bytes the surface HMAC already verified.
fn decode_adr022_record_offsets(
	index_bytes: &[u8],
	layout: &TierObjectLayout,
) -> std::io::Result<BTreeSet<u64>> {
	if index_bytes.len() % ADR_TIERED_SLAB_INDEX_ENTRY_BYTES != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 offset index length is not an integral number of entries",
		));
	}
	let mut offsets = BTreeSet::new();
	for entry in index_bytes.chunks_exact(ADR_TIERED_SLAB_INDEX_ENTRY_BYTES) {
		let byte_offset = u64::from_be_bytes(entry[16..24].try_into().expect("slice len"));
		let byte_length = u32::from_be_bytes(entry[24..28].try_into().expect("slice len"));
		layout.check_index_entry(LogicalExtent::new(byte_offset, u64::from(byte_length)))?;
		offsets.insert(byte_offset);
	}
	Ok(offsets)
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
	chunk_size_bytes: ChunkSizeBytes,
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
	if chunk_count_for_len(plaintext_len, chunk_size_bytes)? != chunk_count {
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
	if SLAB_ENVELOPE_FORMAT_VERSIONS_WITHOUT_LOGICAL_BASE.contains(&slab_format_version) {
		let slab_id = SlabId::new(decode_adr022_u64_slot(&footer[8..24], "slab_id")?);
		return Err(crate::storage::manager::stale_location_error(
			slab_id,
			crate::storage::manager::StaleLocationReason::MalformedObjectLayout,
		));
	}
	if !matches!(
		slab_format_version,
		SLAB_ENVELOPE_FORMAT_VERSION | SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS
	) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("unsupported ADR slab format version {slab_format_version}"),
		));
	}
	let cipher_id = CipherId::from_wire(footer[4])?;
	let footer_flags = footer[5];
	if footer_flags & !HAS_ROW_TOMBSTONE_RECORDS_BIT != 0 || footer[6..8] != [0, 0] {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"non-zero reserved bytes in ADR 022 footer",
		));
	}
	let has_row_tombstone_records = footer_flags & HAS_ROW_TOMBSTONE_RECORDS_BIT != 0;
	let slab_id = SlabId::new(decode_adr022_u64_slot(&footer[8..24], "slab_id")?);
	let tenant_id = TenantId::new(decode_adr022_u64_slot(&footer[24..40], "tenant_id")?);
	let created_hlc = u64::from_be_bytes(footer[40..48].try_into().expect("slice len"));
	let chunk_size_bytes = ChunkSizeBytes::new(u32::from_be_bytes(
		footer[48..52].try_into().expect("slice len"),
	));
	validate_slab_chunk_size(chunk_size_bytes)?;
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
	let wrapped_slot = &footer[ADR_TIERED_SLAB_WRAPPED_DEK_OFFSET
		..ADR_TIERED_SLAB_WRAPPED_DEK_OFFSET + ADR_TIERED_SLAB_WRAPPED_DEK_BYTES];
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
	let logical_base = u64::from_be_bytes(
		footer[ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET
			..ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET + ADR_TIERED_SLAB_LOGICAL_BASE_BYTES]
			.try_into()
			.expect("slice len"),
	);
	let surface_hmac = footer
		[ADR_TIERED_SLAB_HMAC_OFFSET..ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES]
		.to_vec();
	let footer_suffix_after_hmac =
		footer[ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES..].to_vec();
	if footer_suffix_after_hmac.len() != ADR_TIERED_SLAB_RESERVED_SUFFIX_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR footer suffix must be 32 bytes",
		));
	}
	let stats_locator = parse_adr039_stats_locator(slab_format_version, &footer_suffix_after_hmac)?;
	let header_bytes_for_hmac = encode_adr022_header(
		slab_format_version,
		slab_id,
		tenant_id,
		created_hlc,
		cipher_id,
		chunk_size_bytes,
		chunk_count,
	)?
	.to_vec();
	Ok(ParsedEncryptedFooterFields {
		slab_format_version,
		slab_id,
		tenant_id,
		tenant_kek_id,
		cipher_id,
		chunk_size_bytes,
		chunk_count,
		stats_locator,
		has_row_tombstone_records,
		layout: TierObjectLayout::new(slab_id, logical_base, 0)?,
		nonce_version: SLAB_NONCE_VERSION,
		wrapped_slab_dek,
		surface_hmac,
		footer_prefix_without_hmac: footer[..ADR_TIERED_SLAB_HMAC_OFFSET].to_vec(),
		footer_suffix_after_hmac,
		footer_len_u32,
		index_len_u32,
		ciphertext_offset_in_object: (ADR_TIERED_SLAB_MAGIC_HEAD.len()
			+ ADR_TIERED_SLAB_HEADER_BYTES) as u64,
		stats_ciphertext_range_in_object: None,
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
	let footer = &blob[footer_start..tail_offset];
	let fields = parse_adr022_footer_fields(footer, footer_len_u32, index_len_u32)?;
	let stats_len = fields
		.stats_locator
		.map(|locator| locator.stats_block_len as usize)
		.unwrap_or(0);
	let stats_start = index_start
		.checked_sub(stats_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 039 stats block underflow"))?;
	if stats_start < ciphertext_start {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR tiered slab stats/index overlaps ciphertext prefix",
		));
	}
	let header = &blob[ADR_TIERED_SLAB_MAGIC_HEAD.len()..ciphertext_start];
	if header != fields.header_bytes_for_hmac.as_slice() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 022 slab header does not match footer metadata",
		));
	}
	let ciphertext = &blob[ciphertext_start..stats_start];
	let fields = fields.finalize_from_ciphertext_len(ciphertext.len())?;
	Ok(ParsedEncryptedFooter {
		slab_format_version: fields.slab_format_version,
		slab_id: fields.slab_id,
		tenant_id: fields.tenant_id,
		tenant_kek_id: fields.tenant_kek_id,
		cipher_id: fields.cipher_id,
		chunk_size_bytes: fields.chunk_size_bytes,
		chunk_count: fields.chunk_count,
		stats_locator: fields.stats_locator,
		has_row_tombstone_records: fields.has_row_tombstone_records,
		layout: fields.layout,
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
		stats_ciphertext: &blob[stats_start..index_start],
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
	let stats_block_len = fields
		.stats_locator
		.map(|locator| u64::from(locator.stats_block_len))
		.unwrap_or(0);
	let stats_start_in_object = index_start_in_object
		.checked_sub(stats_block_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 039 stats block underflow"))?;
	let ciphertext_offset_in_object =
		(ADR_TIERED_SLAB_MAGIC_HEAD.len() + ADR_TIERED_SLAB_HEADER_BYTES) as u64;
	if stats_start_in_object < ciphertext_offset_in_object {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR tiered slab stats/index overlaps ciphertext prefix",
		));
	}
	let ciphertext_len_u64 = stats_start_in_object - ciphertext_offset_in_object;
	let ciphertext_len = usize::try_from(ciphertext_len_u64).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 022 ciphertext length does not fit into usize",
		)
	})?;
	let mut fields = fields.finalize_from_ciphertext_len(ciphertext_len)?;
	if fields.stats_locator.is_some() {
		fields.stats_ciphertext_range_in_object =
			Some(stats_start_in_object..index_start_in_object);
	}
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
						let persist = {
							let _permit = init
								.owner
								.map(DataDirectoryOwner::write_permit)
								.transpose()?;
							persist_new_aws_kms_root_kek_envelope(&path, &envelope)
						};
						match persist {
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
					let persist = {
						let _permit = init
							.owner
							.map(DataDirectoryOwner::write_permit)
							.transpose()?;
						persist_new_dev_insecure_root_kek(&path, &key)
					};
					match persist {
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
						let persist = {
							let _permit = init
								.owner
								.map(DataDirectoryOwner::write_permit)
								.transpose()?;
							persist_new_local_sealed_root_kek(&path, unlock_secret.as_ref(), &key)
						};
						match persist {
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
		let locked = match key_guard::custody::lock_memory(&boxed.0) {
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
			key_guard::custody::unlock_memory(&self.key.0);
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

	#[cfg(test)]
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
mod root_binding_tests;

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::tier_object_layout::TIER_LAYOUT_PAGE_BYTES;
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
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: u32::try_from(plaintext.len())
							.expect("test slab plaintext length must fit in u32"),
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				plaintext,
			)
			.unwrap()
	}

	#[test]
	fn adr047_footer_roundtrips_row_tombstone_presence_bit() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(47),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 47,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: 4,
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: true,
				},
				b"rows",
			)
			.unwrap();
		assert!(encrypted.has_row_tombstone_records);
		let parsed = parse_encrypted_footer(&encrypted.bytes).unwrap();
		assert!(parsed.has_row_tombstone_records);
		assert!(parsed.footer_summary().has_row_tombstone_records);
	}

	#[test]
	fn adr047_footer_row_tombstone_presence_bit_is_hmac_covered() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(48),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 48,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: 4,
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				b"rows",
			)
			.unwrap();
		let (footer_len, _, tail_offset) = parse_adr022_tail_anchor(&encrypted.bytes).unwrap();
		let footer_start = tail_offset - footer_len as usize;
		let mut tampered = encrypted.bytes.clone();
		tampered[footer_start + 5] ^= HAS_ROW_TOMBSTONE_RECORDS_BIT;

		let err = crypto
			.decrypt_tiered_slab(SlabId::new(48), &tampered)
			.unwrap_err();
		assert!(
			err.to_string().contains("footer HMAC failed"),
			"row tombstone footer bit must be inside footer HMAC scope: {err}"
		);
	}

	fn patterned_plaintext(len: usize) -> Vec<u8> {
		(0..len).map(|index| (index % 251) as u8).collect()
	}

	fn rel_range(start: usize, len: usize) -> TieredPlaintextRange {
		TieredPlaintextRange::new(
			RecordBodyByteOffset::try_new(start as u64).unwrap(),
			RecordBodyByteLen::try_new(len as u64).unwrap(),
		)
	}

	fn tail_window_for(encrypted: &EncryptedTieredSlab) -> (u64, &[u8]) {
		let object_size = encrypted.bytes.len() as u64;
		let tail_start = object_size.saturating_sub(64 * 1024) as usize;
		(object_size, &encrypted.bytes[tail_start..])
	}

	fn ciphertext_slices<'a>(bytes: &'a [u8], ranges: &[Range<u64>]) -> Vec<&'a [u8]> {
		ranges
			.iter()
			.map(|range| {
				let start = usize::try_from(range.start).unwrap();
				let end = usize::try_from(range.end).unwrap();
				&bytes[start..end]
			})
			.collect()
	}

	fn footer_stats_locator_suffix(bytes: &[u8]) -> &[u8] {
		let (footer_len, _, tail_offset) = parse_adr022_tail_anchor(bytes).unwrap();
		let footer_start = tail_offset - footer_len as usize;
		&bytes[footer_start + ADR039_STATS_LOCATOR_OFFSET
			..footer_start + ADR039_STATS_LOCATOR_OFFSET + ADR039_STATS_LOCATOR_BYTES]
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
				0, 0, 0, 3, 1, 0, 0, 0
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
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 4,
						byte_length: 5,
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				&plaintext,
			)
			.unwrap();
		let object_size = encrypted.bytes.len() as u64;
		let tail_start = object_size.saturating_sub(64 * 1024) as usize;
		let tail_window = &encrypted.bytes[tail_start..];
		let err = crypto
			.plan_tiered_range_read(
				SlabId::new(41),
				object_size,
				tail_window,
				None,
				LogicalExtent::new(0, 4),
			)
			.err()
			.expect("range plan should reject ranges absent from footer index");
		assert!(
			err.to_string().contains("footer index"),
			"unexpected error: {err}"
		);
	}

	#[test]
	fn adr061_chunk_size_roundtrips_and_drives_range_planning() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let chunk_size = ChunkSizeBytes::new(16 * 1024);
		let plaintext = patterned_plaintext(chunk_size.get() as usize * 3 + 19);
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(610),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 61,
					chunk_size_bytes: chunk_size,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: plaintext.len().try_into().unwrap(),
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				&plaintext,
			)
			.unwrap();
		assert_eq!(encrypted.chunk_size_bytes, chunk_size);
		let parsed = parse_encrypted_footer(&encrypted.bytes).unwrap();
		assert_eq!(parsed.chunk_size_bytes, chunk_size);
		assert_eq!(parsed.footer_summary().chunk_size_bytes, chunk_size);

		let (object_size, tail_window) = tail_window_for(&encrypted);
		let range = rel_range(chunk_size.get() as usize + 7, 11);
		let plan = crypto
			.plan_tiered_multi_range_read(
				SlabId::new(610),
				object_size,
				tail_window,
				None,
				LogicalExtent::new(0, plaintext.len() as u64),
				&[range],
			)
			.unwrap();
		assert_eq!(plan.chunk_runs().len(), 1);
		assert_eq!(plan.chunk_runs()[0].first_chunk().get(), 1);
		assert_eq!(plan.chunk_runs()[0].chunk_count().get(), 1);
		let ciphertext_ranges = plan.ciphertext_ranges_in_object();
		let selected_ciphertext = ciphertext_slices(&encrypted.bytes, &ciphertext_ranges);
		let selected_plaintext = plan.decrypt(&selected_ciphertext).unwrap();
		assert_eq!(
			selected_plaintext,
			vec![
				plaintext[range.start.get() as usize
					..range.start.get() as usize + range.len.get() as usize]
					.to_vec()
			]
		);
	}

	#[test]
	fn adr061_writer_rejects_unsupported_chunk_size() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let err = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(611),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 61,
					chunk_size_bytes: ChunkSizeBytes::new(12 * 1024),
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: 4,
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				b"rows",
			)
			.unwrap_err();
		assert!(
			err.to_string().contains("unsupported ADR 022 chunk size"),
			"unexpected error: {err}"
		);
	}

	#[test]
	fn adr044_multi_range_matches_single_range_for_one_record_span() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = patterned_plaintext(SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES * 2 + 33);
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(440), TenantId::ZERO, &plaintext);
		let (object_size, tail_window) = tail_window_for(&encrypted);
		let record_range = LogicalExtent::new(0, plaintext.len() as u64);

		let single = crypto
			.plan_tiered_range_read(
				SlabId::new(440),
				object_size,
				tail_window,
				None,
				record_range,
			)
			.unwrap();
		let multi = crypto
			.plan_tiered_multi_range_read(
				SlabId::new(440),
				object_size,
				tail_window,
				None,
				record_range,
				&[rel_range(0, plaintext.len())],
			)
			.unwrap();

		assert_eq!(multi.chunk_runs().len(), 1);
		assert_eq!(
			multi.chunk_runs()[0].ciphertext_range_in_object(),
			single.ciphertext_range_in_object()
		);
		let single_ciphertext =
			ciphertext_slices(&encrypted.bytes, &[single.ciphertext_range_in_object()]);
		let multi_ciphertext =
			ciphertext_slices(&encrypted.bytes, &multi.ciphertext_ranges_in_object());
		let single_plaintext = single.decrypt(single_ciphertext[0]).unwrap();
		let multi_plaintext = multi.decrypt(&multi_ciphertext).unwrap();
		assert_eq!(multi_plaintext, vec![single_plaintext]);
	}

	#[test]
	fn adr044_multi_range_coalesces_overlapping_and_adjacent_chunk_runs() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = patterned_plaintext(SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES * 3 + 17);
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(441), TenantId::ZERO, &plaintext);
		let (object_size, tail_window) = tail_window_for(&encrypted);

		let plan = crypto
			.plan_tiered_multi_range_read(
				SlabId::new(441),
				object_size,
				tail_window,
				None,
				LogicalExtent::new(0, plaintext.len() as u64),
				&[
					rel_range(10, 40),
					rel_range(20, 10),
					rel_range(SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES + 5, 10),
				],
			)
			.unwrap();

		assert_eq!(plan.chunk_runs().len(), 1);
		assert_eq!(
			plan.chunk_runs()[0].first_chunk(),
			EncryptedChunkIndex::new(0)
		);
		assert_eq!(
			plan.chunk_runs()[0].chunk_count(),
			EncryptedChunkCount::new(2)
		);
	}

	#[test]
	fn adr044_multi_range_keeps_disjoint_runs_separate_and_extracts_windows() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = patterned_plaintext(SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES * 4 + 99);
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(442), TenantId::ZERO, &plaintext);
		let (object_size, tail_window) = tail_window_for(&encrypted);
		let first = 3..23;
		let second = (SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES * 2 + 7)
			..(SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES * 2 + 31);

		let plan = crypto
			.plan_tiered_multi_range_read(
				SlabId::new(442),
				object_size,
				tail_window,
				None,
				LogicalExtent::new(0, plaintext.len() as u64),
				&[
					rel_range(first.start, first.len()),
					rel_range(second.start, second.len()),
				],
			)
			.unwrap();

		assert_eq!(plan.chunk_runs().len(), 2);
		assert_eq!(
			plan.chunk_runs()[0].first_chunk(),
			EncryptedChunkIndex::new(0)
		);
		assert_eq!(
			plan.chunk_runs()[1].first_chunk(),
			EncryptedChunkIndex::new(2)
		);
		let ciphertext = ciphertext_slices(&encrypted.bytes, &plan.ciphertext_ranges_in_object());
		let decrypted = plan.decrypt(&ciphertext).unwrap();
		assert_eq!(decrypted[0], plaintext[first]);
		assert_eq!(decrypted[1], plaintext[second]);
	}

	#[test]
	fn adr044_multi_range_rejects_out_of_record_spans() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = patterned_plaintext(128);
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(443), TenantId::ZERO, &plaintext);
		let (object_size, tail_window) = tail_window_for(&encrypted);

		let err = match crypto.plan_tiered_multi_range_read(
			SlabId::new(443),
			object_size,
			tail_window,
			None,
			LogicalExtent::new(0, plaintext.len() as u64),
			&[rel_range(120, 16)],
		) {
			Ok(_) => panic!("out-of-record range should fail"),
			Err(error) => error,
		};
		assert!(
			err.to_string().contains("exceeds indexed record length"),
			"unexpected error: {err}"
		);
	}

	#[test]
	fn adr044_multi_range_tampered_chunk_fails_aead() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = patterned_plaintext(SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES + 1);
		let encrypted =
			encrypt_test_tiered_slab(&crypto, SlabId::new(444), TenantId::ZERO, &plaintext);
		let (object_size, tail_window) = tail_window_for(&encrypted);
		let plan = crypto
			.plan_tiered_multi_range_read(
				SlabId::new(444),
				object_size,
				tail_window,
				None,
				LogicalExtent::new(0, plaintext.len() as u64),
				&[rel_range(0, 16)],
			)
			.unwrap();
		let ranges = plan.ciphertext_ranges_in_object();
		let mut tampered = ciphertext_slices(&encrypted.bytes, &ranges)[0].to_vec();
		tampered[0] ^= 0x80;

		let err = plan.decrypt(&[tampered]).unwrap_err();
		assert!(
			err.to_string().contains("slab chunk decrypt failed"),
			"unexpected error: {err}"
		);
	}

	#[test]
	fn adr039_stats_locator_roundtrips_without_entering_point_read_hmac_payload() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = b"stats-aware slab payload".to_vec();
		let stats_ciphertext = b"opaque encrypted stats block".to_vec();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(42),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 12,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: plaintext.len().try_into().unwrap(),
						flags: 0,
					}],
					column_stats_block: Some(TieredColumnStatsBlock::encrypted(
						stats_ciphertext.clone(),
						84,
						2,
						0,
					)),
					has_row_tombstone_records: false,
				},
				&plaintext,
			)
			.unwrap();

		assert_eq!(
			encrypted.slab_format_version,
			SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS
		);
		let locator = encrypted.stats_locator.expect("stats locator");
		assert_eq!(
			locator.stats_format_version,
			SLAB_STATS_FORMAT_VERSION_ADR039
		);
		assert_eq!(
			locator.stats_flags,
			ADR039_STATS_FLAG_PRESENT_AND_ENCRYPTED
				| ADR039_STATS_FLAG_ALWAYS_ON_COMPLETE_FOR_SUPPORTED
		);
		assert_eq!(locator.stats_block_len, stats_ciphertext.len() as u32);
		assert_eq!(locator.stats_plaintext_len, 84);
		assert_eq!(locator.stats_column_count, 2);
		let footer = crypto
			.encrypted_footer_summary(&encrypted.bytes)
			.unwrap()
			.expect("encrypted footer");
		assert_eq!(
			footer.slab_format_version,
			SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS
		);
		assert_eq!(footer.stats_locator, Some(locator));
		assert_eq!(
			crypto
				.decrypt_tiered_slab(SlabId::new(42), &encrypted.bytes)
				.unwrap()
				.plaintext,
			plaintext
		);

		let stats_start = ADR_TIERED_SLAB_MAGIC_HEAD.len()
			+ ADR_TIERED_SLAB_HEADER_BYTES
			+ ciphertext_offset_for_chunk(
				plaintext.len(),
				encrypted.chunk_size_bytes,
				encrypted.chunk_count as usize,
			);
		let mut stats_tampered = encrypted.bytes.clone();
		stats_tampered[stats_start] ^= 0x80;
		assert_eq!(
			crypto
				.decrypt_tiered_slab(SlabId::new(42), &stats_tampered)
				.unwrap()
				.plaintext,
			plaintext,
			"point reads must not authenticate or fetch stats bytes"
		);

		let tail_offset = encrypted.bytes.len() - ADR_TIERED_SLAB_TAIL_BYTES;
		let footer_start = tail_offset - ADR_TIERED_SLAB_FOOTER_BYTES;
		let mut locator_tampered = encrypted.bytes.clone();
		locator_tampered[footer_start + 240] ^= 0x01;
		let err = crypto
			.decrypt_tiered_slab(SlabId::new(42), &locator_tampered)
			.unwrap_err();
		assert!(
			err.to_string().contains("footer HMAC failed"),
			"locator bytes must be covered by footer HMAC: {err}"
		);
	}

	#[test]
	fn adr039_stats_locator_uses_footer_reserved_suffix_layout() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let no_stats =
			encrypt_test_tiered_slab(&crypto, SlabId::new(44), TenantId::ZERO, b"no stats");
		assert_eq!(no_stats.slab_format_version, SLAB_ENVELOPE_FORMAT_VERSION);
		assert!(
			footer_stats_locator_suffix(&no_stats.bytes)
				.iter()
				.all(|byte| *byte == 0),
			"ADR 022 v1 footer suffix must remain reserved-zero"
		);
		assert_eq!(
			crypto
				.encrypted_footer_summary(&no_stats.bytes)
				.unwrap()
				.expect("footer summary")
				.stats_locator,
			None
		);

		let stats_ciphertext = b"known stats ciphertext bytes".to_vec();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(45),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 14,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: 10,
						flags: 0,
					}],
					column_stats_block: Some(TieredColumnStatsBlock::encrypted(
						stats_ciphertext,
						123,
						3,
						ADR039_STATS_FLAG_HAS_SMALL_SET | ADR039_STATS_FLAG_HAS_BLOOM,
					)),
					has_row_tombstone_records: false,
				},
				b"stats here",
			)
			.unwrap();
		assert_eq!(
			encrypted.slab_format_version,
			SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS
		);
		let locator = encrypted.stats_locator.expect("stats locator");
		let suffix = footer_stats_locator_suffix(&encrypted.bytes);
		assert_eq!(
			u16::from_be_bytes(suffix[0..2].try_into().unwrap()),
			SLAB_STATS_FORMAT_VERSION_ADR039
		);
		assert_eq!(
			u16::from_be_bytes(suffix[2..4].try_into().unwrap()),
			locator.stats_flags
		);
		assert_eq!(
			u32::from_be_bytes(suffix[4..8].try_into().unwrap()),
			locator.stats_block_len
		);
		assert_eq!(
			u32::from_be_bytes(suffix[8..12].try_into().unwrap()),
			locator.stats_plaintext_len
		);
		assert_eq!(
			u32::from_be_bytes(suffix[12..16].try_into().unwrap()),
			locator.stats_column_count
		);
		assert_eq!(&suffix[16..32], locator.stats_block_hash128.as_slice());
		assert_eq!(
			parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, suffix).unwrap(),
			Some(locator)
		);
	}

	#[test]
	fn adr039_stats_locator_rejects_reserved_suffix_and_flag_violations() {
		let valid_flags = ADR039_STATS_FLAG_PRESENT_AND_ENCRYPTED
			| ADR039_STATS_FLAG_ALWAYS_ON_COMPLETE_FOR_SUPPORTED;
		let mut suffix = [0_u8; ADR039_STATS_LOCATOR_BYTES];
		suffix[0..2].copy_from_slice(&SLAB_STATS_FORMAT_VERSION_ADR039.to_be_bytes());
		suffix[2..4].copy_from_slice(&valid_flags.to_be_bytes());
		suffix[4..8].copy_from_slice(&1_u32.to_be_bytes());
		assert!(
			parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, &suffix)
				.unwrap()
				.is_some()
		);

		let err = parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION, &suffix).unwrap_err();
		assert!(
			err.to_string().contains("reserved suffix must be all zero"),
			"v1 footer suffix must reject non-zero locator bytes: {err}"
		);

		let mut absent_dirty = [0_u8; ADR039_STATS_LOCATOR_BYTES];
		absent_dirty[2] = 1;
		let err =
			parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, &absent_dirty)
				.unwrap_err();
		assert!(
			err.to_string()
				.contains("absent stats locator must be all zero"),
			"format_version=0 locator must be wholly zero: {err}"
		);

		let mut unknown_format = suffix;
		unknown_format[0..2].copy_from_slice(&2_u16.to_be_bytes());
		let err =
			parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, &unknown_format)
				.unwrap_err();
		assert!(
			err.to_string().contains("unsupported ADR 039 stats format"),
			"unknown stats locator format must reject: {err}"
		);

		let mut reserved_flag = suffix;
		reserved_flag[2..4].copy_from_slice(&(valid_flags | (1 << 6)).to_be_bytes());
		let err =
			parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, &reserved_flag)
				.unwrap_err();
		assert!(
			err.to_string().contains("reserved bits must be zero"),
			"reserved stats_flags bits must reject: {err}"
		);

		let mut missing_present = suffix;
		missing_present[2..4]
			.copy_from_slice(&ADR039_STATS_FLAG_ALWAYS_ON_COMPLETE_FOR_SUPPORTED.to_be_bytes());
		let err =
			parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, &missing_present)
				.unwrap_err();
		assert!(
			err.to_string().contains("present_and_encrypted"),
			"present_and_encrypted is mandatory in V1: {err}"
		);

		let mut missing_always_on = suffix;
		missing_always_on[2..4]
			.copy_from_slice(&ADR039_STATS_FLAG_PRESENT_AND_ENCRYPTED.to_be_bytes());
		let err =
			parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, &missing_always_on)
				.unwrap_err();
		assert!(
			err.to_string().contains("always_on_complete_for_supported"),
			"always_on_complete_for_supported is mandatory in V1: {err}"
		);

		let mut zero_len = suffix;
		zero_len[4..8].copy_from_slice(&0_u32.to_be_bytes());
		let err = parse_adr039_stats_locator(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, &zero_len)
			.unwrap_err();
		assert!(
			err.to_string().contains("zero stats_block_len"),
			"present locator must have a non-zero ciphertext length: {err}"
		);
	}

	#[test]
	fn adr039_no_stats_writer_emits_v1_and_stats_plan_returns_none() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let slab_id = SlabId::new(46);
		let encrypted = encrypt_test_tiered_slab(&crypto, slab_id, TenantId::ZERO, b"no lazy csb");

		assert_eq!(encrypted.slab_format_version, SLAB_ENVELOPE_FORMAT_VERSION);
		assert_eq!(encrypted.stats_locator, None);
		assert_eq!(
			crypto
				.encrypted_footer_summary(&encrypted.bytes)
				.unwrap()
				.expect("footer summary")
				.stats_locator,
			None
		);
		assert!(
			crypto
				.plan_tiered_stats_block_read(
					slab_id,
					encrypted.bytes.len() as u64,
					&encrypted.bytes,
					None,
				)
				.unwrap()
				.is_none(),
			"stats-required paths treat no-locator slabs as stats unavailable"
		);
		assert!(
			parse_adr039_stats_locator(
				SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS,
				&[0_u8; ADR039_STATS_LOCATOR_BYTES],
			)
			.unwrap()
			.is_none(),
			"transitional v2 zero-locator slabs are also stats unavailable"
		);
	}

	#[test]
	fn adr039_lazy_stats_attachment_is_rejected_by_footer_hmac() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let slab_id = SlabId::new(47);
		let encrypted = encrypt_test_tiered_slab(&crypto, slab_id, TenantId::ZERO, b"late stats");
		let fake_stats = b"not actually sealed at slab finalization".to_vec();
		let (footer_len, index_len, tail_offset) =
			parse_adr022_tail_anchor(&encrypted.bytes).unwrap();
		let footer_start = tail_offset - footer_len as usize;
		let index_start = footer_start - index_len as usize;

		let mut lazily_attached = Vec::with_capacity(encrypted.bytes.len() + fake_stats.len());
		lazily_attached.extend_from_slice(&encrypted.bytes[..index_start]);
		lazily_attached.extend_from_slice(&fake_stats);
		lazily_attached.extend_from_slice(&encrypted.bytes[index_start..]);

		let new_tail_offset = lazily_attached.len() - ADR_TIERED_SLAB_TAIL_BYTES;
		let new_footer_start = new_tail_offset - footer_len as usize;
		lazily_attached[new_footer_start..new_footer_start + 4]
			.copy_from_slice(&SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS.to_be_bytes());
		let locator_start = new_footer_start + ADR039_STATS_LOCATOR_OFFSET;
		let valid_flags = ADR039_STATS_FLAG_PRESENT_AND_ENCRYPTED
			| ADR039_STATS_FLAG_ALWAYS_ON_COMPLETE_FOR_SUPPORTED;
		lazily_attached[locator_start..locator_start + 2]
			.copy_from_slice(&SLAB_STATS_FORMAT_VERSION_ADR039.to_be_bytes());
		lazily_attached[locator_start + 2..locator_start + 4]
			.copy_from_slice(&valid_flags.to_be_bytes());
		lazily_attached[locator_start + 4..locator_start + 8]
			.copy_from_slice(&(fake_stats.len() as u32).to_be_bytes());
		lazily_attached[locator_start + 8..locator_start + 12]
			.copy_from_slice(&17_u32.to_be_bytes());
		lazily_attached[locator_start + 12..locator_start + 16]
			.copy_from_slice(&1_u32.to_be_bytes());
		lazily_attached[locator_start + 16..locator_start + 32]
			.copy_from_slice(&stats_ciphertext_hash128(&fake_stats));

		let err = match crypto.plan_tiered_stats_block_read(
			slab_id,
			lazily_attached.len() as u64,
			&lazily_attached,
			None,
		) {
			Ok(_) => panic!("late CSB attachment must not produce a stats read plan"),
			Err(error) => error,
		};
		assert!(
			err.to_string().contains("footer HMAC failed"),
			"late CSB attachment must rebuild the slab footer and HMAC: {err}"
		);
	}

	/// The typed `StaleLocation` reason an error carries, if any.
	fn stale_reason(error: &Error) -> Option<crate::storage::manager::StaleLocationReason> {
		crate::storage::manager::stale_location(error).map(|stale| stale.reason)
	}

	const MALFORMED: Option<crate::storage::manager::StaleLocationReason> =
		Some(crate::storage::manager::StaleLocationReason::MalformedObjectLayout);
	const OUTSIDE: Option<crate::storage::manager::StaleLocationReason> =
		Some(crate::storage::manager::StaleLocationReason::OutsideGeneration);

	/// An object of `plaintext` stored from logical `base`, with one index
	/// entry per `records` (physical offset, length), shifted by `base`.
	fn encrypt_based_test_slab(
		crypto: &CryptoManager,
		slab_id: SlabId,
		base: u64,
		plaintext: &[u8],
		records: &[(u64, u32)],
	) -> std::io::Result<EncryptedTieredSlab> {
		crypto.encrypt_tiered_slab_object(
			slab_id,
			&TieredSlabObjectMetadata {
				tenant_id: TenantId::ZERO,
				created_hlc: 0,
				chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
				logical_base: base,
				offset_index: records
					.iter()
					.map(|(offset, length)| TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: base + offset,
						byte_length: *length,
						flags: 0,
					})
					.collect(),
				column_stats_block: None,
				has_row_tombstone_records: false,
			},
			plaintext,
		)
	}

	/// `bytes` with its footer and index rewritten by `edit` and the surface
	/// HMAC signed again under the object's tenant KEK: an object whose
	/// layout is wrong yet authentic, so only layout validation refuses it.
	fn resigned(
		crypto: &CryptoManager,
		bytes: &[u8],
		edit: impl FnOnce(&mut [u8], &mut [u8]),
	) -> Vec<u8> {
		let mut out = bytes.to_vec();
		let (footer_len, index_len, tail_offset) = parse_adr022_tail_anchor(&out).unwrap();
		let footer_start = tail_offset - footer_len as usize;
		let index_start = footer_start - index_len as usize;
		let (head, footer_and_tail) = out.split_at_mut(footer_start);
		let (footer, _) = footer_and_tail.split_at_mut(footer_len as usize);
		edit(footer, &mut head[index_start..]);
		let tenant_id =
			TenantId::new(decode_adr022_u64_slot(&footer[24..40], "tenant_id").unwrap());
		let kek_id =
			TenantKekId::try_from(decode_adr022_u64_slot(&footer[64..96], "kek").unwrap()).unwrap();
		let tenant_kek = crypto
			.admit()
			.unwrap()
			.get_tenant_kek(tenant_id, kek_id)
			.unwrap();
		let header = &out[ADR_TIERED_SLAB_MAGIC_HEAD.len()
			..ADR_TIERED_SLAB_MAGIC_HEAD.len() + ADR_TIERED_SLAB_HEADER_BYTES];
		let footer = &out[footer_start..tail_offset];
		let tag = compute_adr022_surface_hmac(
			&tenant_kek,
			header,
			&out[index_start..footer_start],
			&footer[..ADR_TIERED_SLAB_HMAC_OFFSET],
			&footer[ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES..],
			footer_len,
			index_len,
		);
		out[footer_start + ADR_TIERED_SLAB_HMAC_OFFSET
			..footer_start + ADR_TIERED_SLAB_HMAC_OFFSET + ADR_TIERED_SLAB_HMAC_BYTES]
			.copy_from_slice(tag.as_ref());
		out
	}

	fn set_footer_base(footer: &mut [u8], base: u64) {
		footer[ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET
			..ADR_TIERED_SLAB_LOGICAL_BASE_OFFSET + ADR_TIERED_SLAB_LOGICAL_BASE_BYTES]
			.copy_from_slice(&base.to_be_bytes());
	}

	/// Both the full decrypt and the ranged planner refuse `bytes` with
	/// `reason`, before anything is decrypted or a ciphertext range planned.
	fn assert_object_refused(
		crypto: &CryptoManager,
		slab_id: SlabId,
		bytes: &[u8],
		record: LogicalExtent,
		reason: Option<crate::storage::manager::StaleLocationReason>,
		what: &str,
	) {
		let full = crypto.decrypt_tiered_slab(slab_id, bytes).unwrap_err();
		assert_eq!(stale_reason(&full), reason, "{what}: full decrypt: {full}");
		let ranged =
			match crypto.plan_tiered_range_read(slab_id, bytes.len() as u64, bytes, None, record) {
				Ok(_) => panic!("{what}: a ranged read was planned"),
				Err(error) => error,
			};
		assert_eq!(
			stale_reason(&ranged),
			reason,
			"{what}: ranged read: {ranged}"
		);
	}

	#[test]
	fn envelope_versions_without_a_logical_base_refuse_typed() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let slab_id = SlabId::new(705);
		let plaintext = patterned_plaintext(64);
		let encrypted = encrypt_test_tiered_slab(&crypto, slab_id, TenantId::ZERO, &plaintext);
		assert_eq!(encrypted.slab_format_version, SLAB_ENVELOPE_FORMAT_VERSION);
		assert_eq!(SLAB_ENVELOPE_FORMAT_VERSION, 3);
		assert_eq!(SLAB_ENVELOPE_FORMAT_VERSION_WITH_STATS, 4);
		let record = LogicalExtent::new(0, plaintext.len() as u64);
		for old in SLAB_ENVELOPE_FORMAT_VERSIONS_WITHOUT_LOGICAL_BASE {
			let mut bytes = encrypted.bytes.clone();
			let (footer_len, _, tail_offset) = parse_adr022_tail_anchor(&bytes).unwrap();
			let footer_start = tail_offset - footer_len as usize;
			bytes[footer_start..footer_start + 4].copy_from_slice(&old.to_be_bytes());
			assert_object_refused(
				&crypto,
				slab_id,
				&bytes,
				record,
				MALFORMED,
				&format!("version {old}"),
			);
			assert!(crypto.encrypted_footer_summary(&bytes).is_err());
		}
	}

	#[test]
	fn a_based_object_reads_as_a_base_zero_object_at_logical_offsets() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let slab_id = SlabId::new(706);
		let chunk = SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES;
		let plaintext = patterned_plaintext(chunk * 2 + 4096 + 100);
		let records = [(0_u64, 4096_u32), (chunk as u64 + 4096, 4196_u32)];
		let base = 7 * TIER_LAYOUT_PAGE_BYTES;
		let dense = encrypt_based_test_slab(&crypto, slab_id, 0, &plaintext, &records).unwrap();
		let based = encrypt_based_test_slab(&crypto, slab_id, base, &plaintext, &records).unwrap();

		let dense_full = crypto.decrypt_tiered_slab(slab_id, &dense.bytes).unwrap();
		let based_full = crypto.decrypt_tiered_slab(slab_id, &based.bytes).unwrap();
		assert_eq!(dense_full.plaintext, based_full.plaintext);
		assert_eq!(dense_full.layout.base(), 0);
		assert_eq!(based_full.layout.base(), base);
		assert_eq!(based_full.layout.plaintext_len(), plaintext.len() as u64);
		assert_eq!(
			based_full.layout.logical_end(),
			(base + plaintext.len() as u64).next_multiple_of(TIER_LAYOUT_PAGE_BYTES)
		);
		assert_eq!(
			based_full.record_offsets,
			records.iter().map(|(offset, _)| base + offset).collect()
		);

		for (offset, length) in records {
			let read = |bytes: &[u8], logical: u64| {
				let plan = crypto
					.plan_tiered_range_read(
						slab_id,
						bytes.len() as u64,
						bytes,
						None,
						LogicalExtent::new(logical, u64::from(length)),
					)
					.unwrap();
				let range = plan.ciphertext_range_in_object();
				plan.decrypt(&bytes[range.start as usize..range.end as usize])
					.unwrap()
			};
			let expected = &plaintext[offset as usize..(offset + u64::from(length)) as usize];
			assert_eq!(read(&dense.bytes, offset), expected);
			assert_eq!(read(&based.bytes, base + offset), expected);
		}
	}

	#[test]
	fn a_location_outside_a_based_object_refuses_typed_before_any_read_is_planned() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let slab_id = SlabId::new(707);
		let plaintext = patterned_plaintext(8192);
		let base = 2 * TIER_LAYOUT_PAGE_BYTES;
		let based = encrypt_based_test_slab(
			&crypto,
			slab_id,
			base,
			&plaintext,
			&[(0, 4096), (4096, 4096)],
		)
		.unwrap();
		let plan = |record: LogicalExtent| {
			crypto.plan_tiered_range_read(
				slab_id,
				based.bytes.len() as u64,
				&based.bytes,
				None,
				record,
			)
		};
		// Below the base (a source location the replacement does not hold),
		// straddling the end, beyond the end, and an overflowing extent.
		for (what, record) in [
			("below base", LogicalExtent::new(0, 4096)),
			("ends past base", LogicalExtent::new(base - 4096, 8192)),
			("straddles end", LogicalExtent::new(base + 4096, 8192)),
			("beyond end", LogicalExtent::new(base + 8192, 4096)),
			("overflow", LogicalExtent::new(u64::MAX - 10, 4096)),
		] {
			let error = match plan(record) {
				Ok(_) => panic!("{what}: a read was planned"),
				Err(error) => error,
			};
			assert_eq!(stale_reason(&error), OUTSIDE, "{what}: {error}");
		}
		assert!(plan(LogicalExtent::new(base + 4096, 4096)).is_ok());
	}

	#[test]
	fn an_object_layout_is_refused_typed_when_its_base_or_index_is_out_of_bounds() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let slab_id = SlabId::new(708);
		// Not a whole number of pages, so an aligned end can overflow while
		// `base + len` fits.
		let plaintext = patterned_plaintext(8192 + 100);
		let base = TIER_LAYOUT_PAGE_BYTES;
		let records = [(0, 4096), (4096, 4196)];
		let based = encrypt_based_test_slab(&crypto, slab_id, base, &plaintext, &records).unwrap();
		let record = LogicalExtent::new(base, 4096);
		// Control: re-signing alone changes nothing.
		let control = resigned(&crypto, &based.bytes, |_, _| {});
		assert_eq!(
			crypto
				.decrypt_tiered_slab(slab_id, &control)
				.unwrap()
				.plaintext,
			plaintext
		);

		// The producer refuses a misaligned base and an index entry outside
		// the object; nothing is encrypted.
		for (what, result) in [
			(
				"misaligned base",
				encrypt_based_test_slab(&crypto, slab_id, base + 1, &plaintext, &records),
			),
			(
				"entry past the end",
				encrypt_based_test_slab(&crypto, slab_id, base, &plaintext, &[(4096, 4300)]),
			),
		] {
			let error = result.unwrap_err();
			assert_eq!(stale_reason(&error), MALFORMED, "producer, {what}: {error}");
		}

		// An authentic object whose footer or index is wrong.
		let aligned_max = u64::MAX - (u64::MAX % TIER_LAYOUT_PAGE_BYTES);
		let cases: [(&str, Box<dyn Fn(&mut [u8], &mut [u8])>); 5] = [
			(
				"misaligned base",
				Box::new(move |footer, _| set_footer_base(footer, base + 8)),
			),
			(
				"base + len overflows",
				Box::new(move |footer, _| set_footer_base(footer, aligned_max)),
			),
			(
				"aligned end overflows",
				// base + 8292 fits; its page-aligned end does not.
				Box::new(move |footer, _| set_footer_base(footer, aligned_max - 8192)),
			),
			(
				"index entry below the base",
				Box::new(|_, index| index[16..24].copy_from_slice(&0_u64.to_be_bytes())),
			),
			(
				"index entry beyond the end",
				Box::new(move |_, index| {
					index[16..24].copy_from_slice(&(base + 8192 + 100).to_be_bytes())
				}),
			),
		];
		for (what, edit) in cases {
			let bytes = resigned(&crypto, &based.bytes, |footer, index| edit(footer, index));
			assert_object_refused(&crypto, slab_id, &bytes, record, MALFORMED, what);
		}
	}

	#[test]
	fn record_offsets_decode_the_offset_index_and_refuse_bad_entries() {
		let entry = |byte_offset: u64, byte_length: u32| TieredOffsetIndexEntry {
			key_locator: IdentityPrefix::new([0x33; 16]),
			byte_offset,
			byte_length,
			flags: 0,
		};
		let layout = |len: u64| TierObjectLayout::new(SlabId::new(9), 0, len).unwrap();
		// Two rows of one record share an offset; the set holds it once.
		let entries = [entry(0, 100), entry(0, 100), entry(4096, 200)];
		let bytes = encode_adr022_offset_index(&entries, &layout(8192)).unwrap();
		assert_eq!(
			decode_adr022_record_offsets(&bytes, &layout(8192)).unwrap(),
			BTreeSet::from([0, 4096])
		);
		assert_eq!(
			decode_adr022_record_offsets(&[], &layout(8192)).unwrap(),
			BTreeSet::new()
		);
		// A partial entry, an entry past the plaintext, and an overflowing
		// range each refuse the whole index; none is dropped.
		assert!(decode_adr022_record_offsets(&bytes[..bytes.len() - 1], &layout(8192)).is_err());
		let malformed = |error: Error| {
			crate::storage::manager::stale_location(&error).map(|stale| stale.reason)
		};
		assert_eq!(
			malformed(decode_adr022_record_offsets(&bytes, &layout(4296 - 1)).unwrap_err()),
			Some(crate::storage::manager::StaleLocationReason::MalformedObjectLayout)
		);
		let mut overflow = encode_adr022_offset_index(&[entry(0, 1)], &layout(8192)).unwrap();
		overflow[16..24].copy_from_slice(&u64::MAX.to_be_bytes());
		assert_eq!(
			malformed(decode_adr022_record_offsets(&overflow, &layout(8192)).unwrap_err()),
			Some(crate::storage::manager::StaleLocationReason::MalformedObjectLayout)
		);
	}

	#[test]
	fn adr039_stats_section_sits_between_chunks_and_offset_index() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = b"stats block physical layout payload".to_vec();
		let stats_plaintext = b"variable length encrypted stats payload".to_vec();
		let offset_index = vec![
			TieredOffsetIndexEntry {
				key_locator: IdentityPrefix::new([0x11; 16]),
				byte_offset: 0,
				byte_length: 12,
				flags: 0,
			},
			TieredOffsetIndexEntry {
				key_locator: IdentityPrefix::new([0x22; 16]),
				byte_offset: 12,
				byte_length: (plaintext.len() - 12).try_into().unwrap(),
				flags: 0,
			},
		];
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(43),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 13,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: offset_index.clone(),
					column_stats_block: Some(TieredColumnStatsBlock::plaintext(
						stats_plaintext.clone(),
						99,
						1,
						0,
					)),
					has_row_tombstone_records: false,
				},
				&plaintext,
			)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let expected_index = encode_adr022_offset_index(
			&offset_index,
			&TierObjectLayout::new(SlabId::new(43), 0, plaintext.len() as u64).unwrap(),
		)
		.unwrap();
		let stats_start = ADR_TIERED_SLAB_MAGIC_HEAD.len()
			+ ADR_TIERED_SLAB_HEADER_BYTES
			+ ciphertext_offset_for_chunk(
				plaintext.len(),
				encrypted.chunk_size_bytes,
				encrypted.chunk_count as usize,
			);
		let stats_end = stats_start + locator.stats_block_len as usize;
		let (footer_len, index_len, tail_offset) =
			parse_adr022_tail_anchor(&encrypted.bytes).unwrap();
		let footer_start = tail_offset - footer_len as usize;
		let index_start = footer_start - index_len as usize;

		assert_eq!(footer_len as usize, ADR_TIERED_SLAB_FOOTER_BYTES);
		assert_eq!(index_len as usize, expected_index.len());
		assert_eq!(stats_end, index_start);
		assert_eq!(
			&encrypted.bytes[index_start..footer_start],
			expected_index.as_slice()
		);
		assert_eq!(
			&encrypted.bytes[tail_offset + 8..],
			ADR_TIERED_SLAB_MAGIC_TAIL.as_slice()
		);
		assert_eq!(
			locator.stats_block_len,
			stats_plaintext.len() as u32 + SLAB_AEAD_TAG_BYTES as u32
		);
		assert_eq!(
			locator.stats_block_hash128,
			stats_ciphertext_hash128(&encrypted.bytes[stats_start..stats_end])
		);

		let mut stats_tampered = encrypted.bytes.clone();
		stats_tampered[stats_start] ^= 0x01;
		let stats_err = crypto
			.decrypt_tiered_stats_block(SlabId::new(43), &stats_tampered, 99)
			.unwrap_err();
		assert!(
			stats_err
				.to_string()
				.contains("stats block hash disagrees with locator"),
			"stats reads must reject mutated CSB bytes before AEAD open: {stats_err}"
		);
		assert_eq!(
			crypto
				.decrypt_tiered_slab(SlabId::new(43), &stats_tampered)
				.unwrap()
				.plaintext,
			plaintext,
			"point reads must not authenticate or fetch stats bytes"
		);

		let mut index_tampered = encrypted.bytes.clone();
		index_tampered[index_start] ^= 0x01;
		let index_err = crypto
			.decrypt_tiered_slab(SlabId::new(43), &index_tampered)
			.unwrap_err();
		assert!(
			index_err.to_string().contains("footer HMAC failed"),
			"offset index bytes must stay in the point-read HMAC payload: {index_err}"
		);
	}

	#[test]
	fn adr039_plaintext_stats_block_is_encrypted_and_decryptable() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = b"stats-backed slab payload".to_vec();
		let stats_plaintext = b"real column stats block bytes".to_vec();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(43),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 13,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: plaintext.len().try_into().unwrap(),
						flags: 0,
					}],
					column_stats_block: Some(TieredColumnStatsBlock::plaintext(
						stats_plaintext.clone(),
						99,
						1,
						0,
					)),
					has_row_tombstone_records: false,
				},
				&plaintext,
			)
			.unwrap();

		let locator = encrypted.stats_locator.expect("stats locator");
		assert_eq!(locator.stats_plaintext_len, stats_plaintext.len() as u32);
		assert_eq!(
			locator.stats_block_len,
			stats_plaintext.len() as u32 + SLAB_AEAD_TAG_BYTES as u32
		);
		let stats_start = ADR_TIERED_SLAB_MAGIC_HEAD.len()
			+ ADR_TIERED_SLAB_HEADER_BYTES
			+ ciphertext_offset_for_chunk(
				plaintext.len(),
				encrypted.chunk_size_bytes,
				encrypted.chunk_count as usize,
			);
		let stats_end = stats_start + locator.stats_block_len as usize;
		assert_ne!(
			&encrypted.bytes[stats_start..stats_start + stats_plaintext.len()],
			stats_plaintext.as_slice()
		);
		assert_eq!(
			locator.stats_block_hash128,
			stats_ciphertext_hash128(&encrypted.bytes[stats_start..stats_end])
		);

		let decrypted = crypto
			.decrypt_tiered_stats_block(SlabId::new(43), &encrypted.bytes, 99)
			.unwrap()
			.expect("stats block");
		assert_eq!(decrypted.locator, locator);
		assert_eq!(decrypted.plaintext, stats_plaintext);

		let err = crypto
			.decrypt_tiered_stats_block(SlabId::new(43), &encrypted.bytes, 100)
			.unwrap_err();
		assert!(
			err.to_string().contains("decrypt failed"),
			"schema generation must bind stats AAD: {err}"
		);
	}

	#[test]
	fn adr039_arrow_csb_plaintext_roundtrips_through_stats_envelope() {
		use crate::schema::table::{
			LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
			TableObjectId, TableSchemaDescriptor,
		};
		use crate::storage::stats::{
			build_always_on_column_stats_block, decode_column_stats_payload,
			encode_signed_stat_value, parse_column_stats_block,
		};
		use arrow::array::{Array, Int64Array, LargeStringArray};
		use arrow::datatypes::{DataType, Field, Schema};
		use arrow::record_batch::RecordBatch;

		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([3; 16]),
			17,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(5), Some(-2), Some(8)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some("us"), None, Some("eu")]))
					as Arc<dyn Array>,
			],
		)
		.unwrap();
		let csb = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed_before = parse_column_stats_block(&csb).unwrap();
		assert_eq!(parsed_before.header.column_block_count, 2);
		let payload = decode_column_stats_payload(&parsed_before.columns[0]).unwrap();
		assert_eq!(payload.min, Some(encode_signed_stat_value(-2, 8).unwrap()));

		let plaintext = b"payload covered by normal slab chunks".to_vec();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(44),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 14,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					logical_base: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: plaintext.len().try_into().unwrap(),
						flags: 0,
					}],
					column_stats_block: Some(TieredColumnStatsBlock::plaintext(
						csb,
						descriptor.schema_generation,
						parsed_before.header.column_block_count,
						0,
					)),
					has_row_tombstone_records: false,
				},
				&plaintext,
			)
			.unwrap();
		let decrypted = crypto
			.decrypt_tiered_stats_block(SlabId::new(44), &encrypted.bytes, 17)
			.unwrap()
			.expect("stats block");
		let parsed_after = parse_column_stats_block(&decrypted.plaintext).unwrap();
		assert_eq!(parsed_after, parsed_before);
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
		let plaintext = vec![0x5a; chunk_count * SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES + 17];
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
			owner: None,
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
				owner: None,
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

	// Frozen from 71e096320 + aws-lc-rs 1.16.3, before the separate-tag repair.
	// Fixed DEK=[0x35;32], tenant=7, slab=42, generation=9, chunk size=16.
	// These are format compatibility controls; this key/nonce reuse is test-only.
	// The envelope version is an AEAD input of both: the chunk AAD binds the
	// object's version (layout pinned by `chunk_aad_matches_golden_layout`) and
	// the stats AAD the with-stats version (`stats_aad_matches_golden_layout`).
	// Re-frozen when production began writing versions 3 and 4 (ADR 0705): the
	// chunk vectors pin what a version-3 object writes, the stats vectors what a
	// version-4 object writes; only the version bytes of each AAD changed.
	const FROZEN_SLAB_PLAINTEXT: &[u8] = b"fixed slab format control: three chunks!";

	#[test]
	fn stats_aad_matches_golden_layout() {
		let aad = encode_stats_aad(
			TenantId::new(0x0102_0304_0506_0708),
			SlabId::new(0x1112_1314_1516_1718),
			CipherId::Aes256GcmInjective,
			9,
		);
		let mut expected = [0_u8; SLAB_STATS_AAD_BYTES];
		expected[8..16].copy_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
		expected[24..32].copy_from_slice(&[0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18]);
		expected[32..36].copy_from_slice(&[0, 0, 0, 4]);
		expected[36..38].copy_from_slice(&[0, 1]);
		expected[38] = CipherId::Aes256GcmInjective.to_wire();
		expected[47] = 9;
		assert_eq!(aad, expected);
	}

	fn prepared_slab_format_key(cipher: CipherId) -> PreparedSlabDek {
		let key = KeyMaterialBytes::copy_from_slice(&[0x35; 32], "format control").unwrap();
		PreparedSlabDek::new(cipher, Arc::new(LockedKeyMaterial::new(key).unwrap())).unwrap()
	}

	fn assert_slab_stats_frozen_format(cipher: CipherId, expected: &[u8]) {
		let prepared = prepared_slab_format_key(cipher);
		let tenant = TenantId::new(7);
		let slab = SlabId::new(42);
		assert_eq!(
			seal_stats_block(&prepared, cipher, tenant, slab, 9, FROZEN_SLAB_PLAINTEXT).unwrap(),
			expected
		);
		let locator = TieredStatsBlockLocator {
			stats_format_version: SLAB_STATS_FORMAT_VERSION_ADR039,
			stats_flags: 0,
			stats_block_len: expected.len() as u32,
			stats_plaintext_len: FROZEN_SLAB_PLAINTEXT.len() as u32,
			stats_column_count: 0,
			stats_block_hash128: stats_ciphertext_hash128(expected),
		};
		assert_eq!(
			decrypt_stats_block(&prepared, cipher, tenant, slab, 9, locator, expected).unwrap(),
			FROZEN_SLAB_PLAINTEXT
		);
	}

	fn assert_slab_chunks_frozen_format(cipher: CipherId, expected: &[u8]) {
		let prepared = prepared_slab_format_key(cipher);
		let tenant = TenantId::new(7);
		let slab = SlabId::new(42);
		let size = ChunkSizeBytes::new(16);
		let mut ciphertext = vec![0; FROZEN_SLAB_PLAINTEXT.len() + 3 * SLAB_AEAD_TAG_BYTES];
		encrypt_chunk_range(
			&prepared,
			cipher,
			tenant,
			slab,
			SLAB_ENVELOPE_FORMAT_VERSION,
			size,
			FROZEN_SLAB_PLAINTEXT,
			0..3,
			&mut ciphertext,
		)
		.unwrap();
		assert_eq!(ciphertext, expected);
		let mut plaintext = vec![0; FROZEN_SLAB_PLAINTEXT.len()];
		decrypt_chunk_range(
			&prepared,
			cipher,
			tenant,
			slab,
			SLAB_ENVELOPE_FORMAT_VERSION,
			size,
			FROZEN_SLAB_PLAINTEXT.len(),
			expected,
			0..3,
			&mut plaintext,
		)
		.unwrap();
		assert_eq!(plaintext, FROZEN_SLAB_PLAINTEXT);
	}

	#[test]
	fn slab_stats_gcm_matches_frozen_format() {
		assert_slab_stats_frozen_format(
			CipherId::Aes256GcmInjective,
			&[
				0x17, 0xd0, 0xc7, 0xa6, 0xec, 0xe7, 0x94, 0x40, 0xb1, 0xfd, 0x52, 0x69, 0x88, 0xbb,
				0x4d, 0xdb, 0x80, 0x41, 0xf2, 0xdd, 0xb9, 0xb6, 0xc8, 0xce, 0x67, 0x59, 0xb4, 0x4d,
				0xa1, 0x92, 0x42, 0x4a, 0x5b, 0xa8, 0x80, 0xbd, 0xe3, 0x7b, 0x9b, 0x44, 0x19, 0x76,
				0xa9, 0xda, 0x3a, 0x51, 0xb4, 0x67, 0x22, 0x43, 0x76, 0x97, 0xe0, 0xf1, 0x65, 0xd7,
			],
		);
	}

	#[test]
	fn slab_chunks_gcm_matches_frozen_format() {
		assert_slab_chunks_frozen_format(
			CipherId::Aes256GcmInjective,
			&[
				0x41, 0x0b, 0x00, 0x84, 0x83, 0xf5, 0x15, 0x2d, 0x3a, 0x02, 0x9b, 0xd2, 0x45, 0x18,
				0x14, 0x15, 0x81, 0xe5, 0x0f, 0xee, 0xd4, 0xc6, 0xe2, 0xdb, 0x72, 0xb3, 0xda, 0xeb,
				0x36, 0xd0, 0xdf, 0x9f, 0x58, 0xd4, 0x34, 0xe8, 0x1d, 0x37, 0x99, 0x4f, 0xa6, 0xe9,
				0x61, 0xbb, 0x97, 0x42, 0x9d, 0xea, 0x76, 0xc6, 0xb3, 0x8e, 0x31, 0x80, 0x17, 0xd3,
				0xc2, 0x5d, 0xf4, 0x9d, 0xdf, 0xa4, 0x7c, 0xf2, 0x2b, 0x6e, 0xcb, 0x00, 0x71, 0xf5,
				0x1f, 0xcb, 0x3f, 0x2a, 0xf6, 0x3f, 0x63, 0xc8, 0x5c, 0x9f, 0x33, 0xbd, 0x74, 0x56,
				0xe6, 0x45, 0x89, 0x43,
			],
		);
	}

	#[cfg(not(feature = "fips"))]
	#[test]
	fn slab_stats_gcm_siv_matches_frozen_format() {
		assert_slab_stats_frozen_format(
			CipherId::Aes256GcmSiv,
			&[
				0x8c, 0x62, 0x8a, 0x18, 0xd7, 0xa8, 0xb2, 0x62, 0x6d, 0x40, 0x85, 0x38, 0xf5, 0x51,
				0x3d, 0x1c, 0x4d, 0x04, 0x39, 0xb3, 0xab, 0x49, 0x3b, 0x43, 0x73, 0xcb, 0xdc, 0x4a,
				0x89, 0xf2, 0x3e, 0x69, 0x5d, 0x2e, 0xf8, 0x35, 0x53, 0x51, 0x62, 0x2f, 0xa5, 0xc8,
				0x57, 0x6f, 0x1c, 0x31, 0x06, 0x41, 0x30, 0x68, 0x62, 0xe5, 0x6d, 0xe5, 0xfa, 0x9f,
			],
		);
	}

	#[cfg(not(feature = "fips"))]
	#[test]
	fn slab_chunks_gcm_siv_matches_frozen_format() {
		assert_slab_chunks_frozen_format(
			CipherId::Aes256GcmSiv,
			&[
				0xf6, 0x82, 0x4e, 0x33, 0x1a, 0x22, 0x3f, 0x5b, 0x66, 0x39, 0xd8, 0xe2, 0x23, 0x77,
				0x55, 0xd9, 0xf9, 0x70, 0x81, 0x1a, 0xc4, 0x24, 0xd3, 0x5b, 0xa3, 0x1d, 0xf0, 0x46,
				0xf6, 0xc3, 0x2b, 0x18, 0xe4, 0x02, 0x7a, 0x19, 0xc5, 0x04, 0x43, 0x39, 0x64, 0x74,
				0xaf, 0x23, 0x06, 0x75, 0xd1, 0x3a, 0x5d, 0xcf, 0x97, 0x50, 0xcb, 0x06, 0xc6, 0xfc,
				0x7e, 0x7f, 0x62, 0xe8, 0x86, 0x5d, 0x27, 0x4e, 0x9c, 0xcd, 0x8a, 0x2b, 0x90, 0xb0,
				0x6b, 0x48, 0x35, 0x43, 0x43, 0xba, 0xad, 0xa1, 0xe5, 0x28, 0x9d, 0x49, 0x93, 0x47,
				0x42, 0xfb, 0xde, 0x93,
			],
		);
	}
}
