use crate::audit::{AuditAuthority, AuditEvent, AuditSink};
use crate::schema::{NodeId, SlabId, TenantId, TenantKekId};
use async_trait::async_trait;
#[cfg(all(feature = "fips", feature = "non-fips"))]
compile_error!("features `fips` and `non-fips` are mutually exclusive");
#[cfg(not(any(feature = "fips", feature = "non-fips")))]
compile_error!("enable one of the `fips` or `non-fips` crypto feature profiles");
#[cfg(not(feature = "fips"))]
use aws_lc_rs::aead::AES_256_GCM_SIV;
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::hmac;
use aws_lc_rs::key_wrap::{AES_256, AesKek, KeyWrapPadded};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
#[cfg(any(test, not(feature = "fips")))]
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::{Error, ErrorKind};
#[cfg(any(test, not(feature = "fips")))]
use std::io::{Read, Write};
#[cfg(all(unix, any(test, not(feature = "fips"))))]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
#[cfg(any(test, not(feature = "fips")))]
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
const SLAB_ENVELOPE_MAGIC_HEAD: [u8; 8] = *b"W4SLAB01";
const SLAB_ENVELOPE_MAGIC_TAIL: [u8; 8] = *b"W4TAIL01";
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RootKekProviderConfig {
	#[default]
	DevInsecure,
}

impl RootKekProviderConfig {
	pub fn label(self) -> &'static str {
		match self {
			Self::DevInsecure => "dev-insecure",
		}
	}

	pub(crate) fn build(self, _data_dir: &Path) -> std::io::Result<Arc<dyn RootKekProvider>> {
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
					Ok(Arc::new(DevRootKekProvider::from_dev_insecure_dir(
						_data_dir,
					)?))
				}
			}
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedTieredSlab {
	pub bytes: Vec<u8>,
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

	pub fn encrypt_tiered_slab(
		&self,
		slab_id: SlabId,
		tenant_id: TenantId,
		plaintext: &[u8],
	) -> std::io::Result<EncryptedTieredSlab> {
		let (tenant_kek_id, tenant_kek) = self.get_active_tenant_kek(tenant_id)?;
		let wrapped_slab_dek = SlabDek::fresh_for(slab_id)?.wrap(&tenant_kek)?;
		let cipher_id = selected_cipher_id();
		let chunk_count = chunk_count_for_len(plaintext.len())?;
		let encrypted_chunks = encrypt_chunks(
			cipher_id,
			tenant_id,
			wrapped_slab_dek,
			chunk_count,
			plaintext,
		)?;

		let mut footer_prefix = Vec::with_capacity(64 + encrypted_chunks.wrapped_slab_dek.len());
		push_u32_be(&mut footer_prefix, SLAB_ENVELOPE_FORMAT_VERSION);
		push_u32_be(&mut footer_prefix, SLAB_NONCE_VERSION);
		footer_prefix.push(cipher_id.to_wire());
		footer_prefix.extend_from_slice(&[0, 0, 0]);
		push_u64_be(&mut footer_prefix, slab_id.get());
		push_u64_be(&mut footer_prefix, tenant_id.get());
		push_u64_be(
			&mut footer_prefix,
			u64::try_from(plaintext.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"plaintext slab length exceeds u64::MAX during envelope encode",
				)
			})?,
		);
		push_u32_be(&mut footer_prefix, chunk_count);
		push_u64_be(&mut footer_prefix, tenant_kek_id.get());
		push_u32_be(
			&mut footer_prefix,
			u32::try_from(encrypted_chunks.wrapped_slab_dek.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"wrapped slab DEK exceeds u32::MAX during envelope encode",
				)
			})?,
		);
		footer_prefix.extend_from_slice(&encrypted_chunks.wrapped_slab_dek);

		let footer_len = footer_prefix
			.len()
			.checked_add(hmac::HMAC_SHA256.tag_len())
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "slab footer length overflow"))?;
		let footer_len_u32 = u32::try_from(footer_len).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"slab footer exceeds u32::MAX during envelope encode",
			)
		})?;
		let surface_hmac = compute_surface_hmac(&tenant_kek, &footer_prefix, footer_len_u32);

		let total_len = SLAB_ENVELOPE_MAGIC_HEAD
			.len()
			.checked_add(encrypted_chunks.ciphertext.len())
			.and_then(|n| n.checked_add(footer_len))
			.and_then(|n| n.checked_add(4 + SLAB_ENVELOPE_MAGIC_TAIL.len()))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tiered slab length overflow"))?;
		let mut out = Vec::with_capacity(total_len);
		out.extend_from_slice(&SLAB_ENVELOPE_MAGIC_HEAD);
		out.extend_from_slice(&encrypted_chunks.ciphertext);
		out.extend_from_slice(&footer_prefix);
		out.extend_from_slice(surface_hmac.as_ref());
		push_u32_be(&mut out, footer_len_u32);
		out.extend_from_slice(&SLAB_ENVELOPE_MAGIC_TAIL);

		{
			let mut slab_cache = self.lock_slab_dek_cache()?;
			slab_cache.insert((tenant_id, slab_id, cipher_id), encrypted_chunks.cache_key);
		}

		Ok(EncryptedTieredSlab {
			bytes: out,
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
		let footer = FooterSummary {
			tenant_id: parsed.tenant_id,
			slab_id: parsed.slab_id,
			tenant_kek_id: parsed.tenant_kek_id,
			cipher_id: parsed.cipher_id,
			chunk_count: parsed.chunk_count,
		};
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
		if let Err(error) = verify_surface_hmac(
			&tenant_kek,
			&parsed.footer_prefix_without_hmac,
			parsed.footer_len_u32,
			parsed.surface_hmac,
		) {
			self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
			return Err(error);
		}
		let slab_dek = {
			let mut slab_cache = self.lock_slab_dek_cache()?;
			let cache_key = (parsed.tenant_id, parsed.slab_id, parsed.cipher_id);
			if let Some(prepared) = slab_cache.get(&cache_key) {
				prepared
			} else {
				let unwrapped = match unwrap_key(&tenant_kek, &parsed.wrapped_slab_dek) {
					Ok(unwrapped) => unwrapped,
					Err(error) => {
						self.emit_decrypt_rejected(expected_slab_id, Some(footer), "hmac", &error);
						return Err(error);
					}
				};
				let prepared = Arc::new(PreparedSlabDek::new(
					parsed.cipher_id,
					Arc::new(LockedKeyMaterial::new(unwrapped)?),
				)?);
				slab_cache.insert(cache_key, prepared.clone());
				prepared
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

	pub fn encrypted_footer_summary(&self, blob: &[u8]) -> std::io::Result<Option<FooterSummary>> {
		if !is_encrypted_tiered_blob(blob) {
			return Ok(None);
		}
		let parsed = parse_encrypted_footer(blob)?;
		Ok(Some(FooterSummary {
			tenant_id: parsed.tenant_id,
			slab_id: parsed.slab_id,
			tenant_kek_id: parsed.tenant_kek_id,
			cipher_id: parsed.cipher_id,
			chunk_count: parsed.chunk_count,
		}))
	}

	fn get_tenant_kek(
		&self,
		tenant_id: TenantId,
		tenant_kek_id: TenantKekId,
	) -> std::io::Result<Arc<LockedKeyMaterial>> {
		{
			let mut cache = self.lock_tenant_kek_cache()?;
			if let Some(key) = cache.get(&(tenant_id, tenant_kek_id)) {
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
				return Ok(key);
			}
		}

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
		if let Err(emit_error) = context.sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			"crypto.decrypt_rejected",
			context.node_id,
			message,
		)) {
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
	blob.starts_with(&SLAB_ENVELOPE_MAGIC_HEAD)
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
	// W4 canonically projects the current u64 ids into those 16-byte slots
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

fn compute_surface_hmac(
	tenant_kek: &LockedKeyMaterial,
	footer_prefix: &[u8],
	footer_len: u32,
) -> hmac::Tag {
	let key = hmac::Key::new(hmac::HMAC_SHA256, tenant_kek.as_ref());
	let payload = surface_hmac_payload(footer_prefix, footer_len);
	hmac::sign(&key, &payload)
}

fn verify_surface_hmac(
	tenant_kek: &LockedKeyMaterial,
	footer_prefix: &[u8],
	footer_len: u32,
	expected: &[u8],
) -> std::io::Result<()> {
	let key = hmac::Key::new(hmac::HMAC_SHA256, tenant_kek.as_ref());
	let payload = surface_hmac_payload(footer_prefix, footer_len);
	hmac::verify(&key, &payload, expected).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"tiered slab footer HMAC failed before DEK unwrap",
		)
	})
}

fn surface_hmac_payload(footer_prefix: &[u8], footer_len: u32) -> Vec<u8> {
	// Ciphertext authentication is handled by the per-chunk AEAD tags.
	// This HMAC binds only the unwrap-path inputs so a footer tamper is
	// rejected before the slab DEK is exposed, while the fixed tail magic
	// authenticates the exact envelope delimiter paired with `footer_len`.
	let mut payload = Vec::with_capacity(
		SLAB_ENVELOPE_MAGIC_HEAD.len() + footer_prefix.len() + 4 + SLAB_ENVELOPE_MAGIC_TAIL.len(),
	);
	payload.extend_from_slice(&SLAB_ENVELOPE_MAGIC_HEAD);
	payload.extend_from_slice(footer_prefix);
	payload.extend_from_slice(&footer_len.to_be_bytes());
	payload.extend_from_slice(&SLAB_ENVELOPE_MAGIC_TAIL);
	payload
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

fn read_u32_be(src: &[u8], cursor: &mut usize) -> std::io::Result<u32> {
	let end = cursor
		.checked_add(4)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "u32 cursor overflow"))?;
	if end > src.len() {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"unexpected EOF while decoding u32 from slab envelope",
		));
	}
	let value = u32::from_be_bytes(src[*cursor..end].try_into().expect("slice len"));
	*cursor = end;
	Ok(value)
}

fn read_u64_be(src: &[u8], cursor: &mut usize) -> std::io::Result<u64> {
	let end = cursor
		.checked_add(8)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "u64 cursor overflow"))?;
	if end > src.len() {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"unexpected EOF while decoding u64 from slab envelope",
		));
	}
	let value = u64::from_be_bytes(src[*cursor..end].try_into().expect("slice len"));
	*cursor = end;
	Ok(value)
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
	surface_hmac: &'a [u8],
	footer_prefix_without_hmac: Vec<u8>,
	footer_len_u32: u32,
	ciphertext: &'a [u8],
	#[cfg(test)]
	wrapped_slab_dek_range_in_blob: std::ops::Range<usize>,
}

fn parse_encrypted_footer(blob: &[u8]) -> std::io::Result<ParsedEncryptedFooter<'_>> {
	if !blob.starts_with(&SLAB_ENVELOPE_MAGIC_HEAD) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"blob is not in W4 envelope-encrypted slab format",
		));
	}
	let tail_offset = blob
		.len()
		.checked_sub(4 + SLAB_ENVELOPE_MAGIC_TAIL.len())
		.ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "tiered slab shorter than tail"))?;
	let footer_len_u32 = u32::from_be_bytes(
		blob[tail_offset..tail_offset + 4]
			.try_into()
			.expect("slice len"),
	);
	if blob[tail_offset + 4..] != SLAB_ENVELOPE_MAGIC_TAIL {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"tiered slab tail magic mismatch",
		));
	}
	let footer_len = footer_len_u32 as usize;
	let footer_start = tail_offset
		.checked_sub(footer_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tiered slab footer underflow"))?;
	let footer = &blob[footer_start..tail_offset];
	let mut cursor = 0_usize;
	let slab_format_version = read_u32_be(footer, &mut cursor)?;
	if slab_format_version != SLAB_ENVELOPE_FORMAT_VERSION {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("unsupported slab envelope version {slab_format_version}"),
		));
	}
	let nonce_version = read_u32_be(footer, &mut cursor)?;
	let cipher_id_wire = *footer.get(cursor).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"unexpected EOF while decoding cipher_id from slab envelope",
		)
	})?;
	cursor += 1;
	let reserved = footer.get(cursor..cursor + 3).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"unexpected EOF while decoding reserved bytes from slab envelope",
		)
	})?;
	if reserved != [0, 0, 0] {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"non-zero reserved bytes in slab envelope footer",
		));
	}
	cursor += 3;
	let slab_id = SlabId::new(read_u64_be(footer, &mut cursor)?);
	let tenant_id = TenantId::new(read_u64_be(footer, &mut cursor)?);
	let plaintext_len = usize::try_from(read_u64_be(footer, &mut cursor)?).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"plaintext_len does not fit into usize on this platform",
		)
	})?;
	let chunk_count = read_u32_be(footer, &mut cursor)?;
	let tenant_kek_id =
		TenantKekId::try_from(read_u64_be(footer, &mut cursor)?).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("invalid tenant_kek_id in slab envelope footer: {err}"),
			)
		})?;
	let wrapped_slab_dek_len = read_u32_be(footer, &mut cursor)? as usize;
	#[cfg(test)]
	let wrapped_start = cursor;
	let wrapped_end = cursor
		.checked_add(wrapped_slab_dek_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "wrapped_slab_dek length overflow"))?;
	let wrapped_slab_dek = footer
		.get(cursor..wrapped_end)
		.ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "wrapped_slab_dek truncated"))?
		.to_vec();
	cursor = wrapped_end;
	let hmac_len = hmac::HMAC_SHA256.tag_len();
	let hmac_end = cursor
		.checked_add(hmac_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "footer HMAC length overflow"))?;
	let surface_hmac = footer
		.get(cursor..hmac_end)
		.ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "footer HMAC truncated"))?;
	if hmac_end != footer.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"extra bytes remain after slab footer HMAC",
		));
	}
	let cipher_id = CipherId::from_wire(cipher_id_wire)?;
	let ciphertext = &blob[SLAB_ENVELOPE_MAGIC_HEAD.len()..footer_start];
	let footer_prefix_without_hmac = footer[..cursor].to_vec();
	Ok(ParsedEncryptedFooter {
		slab_id,
		tenant_id,
		tenant_kek_id,
		cipher_id,
		chunk_count,
		plaintext_len,
		nonce_version,
		wrapped_slab_dek,
		surface_hmac,
		footer_prefix_without_hmac,
		footer_len_u32,
		ciphertext,
		#[cfg(test)]
		wrapped_slab_dek_range_in_blob: (footer_start + wrapped_start)
			..(footer_start + wrapped_end),
	})
}

pub(crate) trait RootKekProvider: Send + Sync {
	fn label(&self) -> &'static str;
	fn wrap_tenant_kek(&self, plaintext_key: &KeyMaterialBytes) -> std::io::Result<Vec<u8>>;
	fn unwrap_tenant_kek(&self, wrapped_key: &[u8]) -> std::io::Result<KeyMaterialBytes>;
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

struct DevRootKekProvider {
	key: LockedKeyMaterial,
}

impl DevRootKekProvider {
	#[cfg(any(test, not(feature = "fips")))]
	fn from_dev_insecure_dir(data_dir: &Path) -> std::io::Result<Self> {
		let path = dev_insecure_root_kek_path(data_dir);
		let key = match read_dev_insecure_root_kek(&path) {
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
	file.write_all(key.as_ref())?; // w4-no-disk-cache-ok: dev-only root KEK persistence, cfg-gated out of FIPS builds; production uses an external root KEK provider
	file.sync_all()
}

impl RootKekProvider for DevRootKekProvider {
	fn label(&self) -> &'static str {
		RootKekProviderConfig::DevInsecure.label()
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
}

impl AsRef<[u8]> for LockedKeyMaterial {
	fn as_ref(&self) -> &[u8] {
		self.key.as_ref().as_ref()
	}
}

impl Drop for LockedKeyMaterial {
	fn drop(&mut self) {
		self.key.zeroize();
		if self.locked {
			unsafe {
				libc::munlock(
					self.key.0.as_mut_ptr().cast(),
					std::mem::size_of::<[u8; 32]>(),
				);
			}
		}
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

	const WEEKLY_NONCE_SAMPLE_COUNT: u64 = 100_000_000;

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
	#[ignore = "weekly ladder: run via ci/check_w4_nonce_ladder.sh"]
	fn nonce_encoding_is_injective_for_weekly_ladder() {
		let sample_count = std::env::var("W4_NONCE_INJECTIVE_SAMPLES")
			.ok()
			.and_then(|raw| raw.parse::<u64>().ok())
			.unwrap_or(WEEKLY_NONCE_SAMPLE_COUNT);
		eprintln!("running W4 nonce ladder with {sample_count} samples");
		assert_nonce_encoding_matches_contract(sample_count);
	}

	#[test]
	fn envelope_round_trips_with_cached_keys() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let plaintext = b"hello wendelon tiered slab".repeat(4000);
		let encrypted = crypto
			.encrypt_tiered_slab(SlabId::new(7), TenantId::ZERO, &plaintext)
			.unwrap();
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
	fn large_envelope_round_trip_matches_parallel_chunk_contract() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let chunk_count = PARALLEL_CHUNK_CRYPTO_MIN_CHUNKS + 3;
		let plaintext = vec![0x5a; chunk_count * SLAB_ENCRYPTION_CHUNK_BYTES + 17];
		let encrypted = crypto
			.encrypt_tiered_slab(SlabId::new(17), TenantId::ZERO, &plaintext)
			.unwrap();
		let decrypted = crypto
			.decrypt_tiered_slab(SlabId::new(17), &encrypted.bytes)
			.unwrap();
		assert_eq!(decrypted.plaintext, plaintext);
		assert_eq!(decrypted.chunk_count as usize, chunk_count + 1);
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

	#[test]
	fn footer_hmac_rejects_tamper_before_unwrap() {
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let encrypted = crypto
			.encrypt_tiered_slab(SlabId::new(9), TenantId::ZERO, b"tamper me")
			.unwrap();
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
		let encrypted = crypto
			.encrypt_tiered_slab(SlabId::new(27), TenantId::ZERO, b"audit me")
			.unwrap();
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
			.find(|event| event.event_type == "crypto.decrypt_rejected")
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
		let before = crypto
			.encrypt_tiered_slab(SlabId::new(10), TenantId::ZERO, b"before rotation")
			.unwrap();
		let rotate = crypto.prepare_rotation(TenantId::ZERO).unwrap();
		crypto.apply_rotation(&rotate).unwrap();
		let after = crypto
			.encrypt_tiered_slab(SlabId::new(11), TenantId::ZERO, b"after rotation")
			.unwrap();

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
