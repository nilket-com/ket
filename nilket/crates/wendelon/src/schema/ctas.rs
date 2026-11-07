use crate::integration::compute::SchemaGeneration;
use crate::schema::{ClientWriteId, TableObjectId, TableSchemaDescriptor, TenantId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Caller-retained ADR 548 composite identity. The bytes are the SQL/Rune UUIDv7
/// query id; phase-specific identities must not replace these bytes with a hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CtasOperationId([u8; 16]);

impl CtasOperationId {
	pub const fn new(bytes: [u8; 16]) -> Self {
		Self(bytes)
	}

	pub const fn into_bytes(self) -> [u8; 16] {
		self.0
	}

	/// UUIDv7 issuance time carried by the caller-retained query identity.
	/// CTAS requires this shape so expiry remains provable after bounded saga
	/// detail has been reclaimed.
	pub fn issued_at_unix_millis(self) -> Option<u64> {
		crate::schema::uuid_v7_unix_millis(self.0)
	}
}

/// Active sagas and retained terminal detail share one bounded Control Ring
/// admission pool. Active work is never evicted; admission refuses when the
/// pool is full and no terminal detail has aged past the retention horizon.
pub const CTAS_SAGA_CAPACITY: usize = 4_096;
pub const CTAS_SAGA_DETAIL_RETENTION_MS: u64 = 15 * 60 * 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CtasIntentFingerprint([u8; 32]);

impl CtasIntentFingerprint {
	pub const fn new(bytes: [u8; 32]) -> Self {
		Self(bytes)
	}

	pub const fn into_bytes(self) -> [u8; 32] {
		self.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasExpectedBinding {
	Absent,
	Bound {
		table_object_id: TableObjectId,
		schema_generation: SchemaGeneration,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasSagaPhase {
	Provisioned,
	RowDispatched,
	RowsCommitted { row_count: u64 },
	Published,
	RetiringOld,
	Complete,
	Conflict,
	Reclaimed,
}

impl CtasSagaPhase {
	pub const fn is_terminal(self) -> bool {
		matches!(self, Self::Complete | Self::Conflict | Self::Reclaimed)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasTerminalCause {
	PublicationConflict,
	RowKnownNotCommitted,
	DeadlineBeforeRowDispatch,
	OwnerAbandonedBeforeRowDispatch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasSagaEntry {
	pub tenant_id: TenantId,
	pub operation_id: CtasOperationId,
	pub intent_fingerprint: CtasIntentFingerprint,
	pub descriptor: TableSchemaDescriptor,
	pub expected_binding: CtasExpectedBinding,
	/// Descriptor needed to resume post-publication retirement after a
	/// coordinator restart, when the old object is no longer name-resolvable.
	#[serde(default)]
	pub prior_descriptor: Option<TableSchemaDescriptor>,
	pub row_write_id: ClientWriteId,
	/// Expected result cardinality, durably recorded before row dispatch. This
	/// lets a recovery activation attest an already-committed write without
	/// re-running the CTAS source.
	#[serde(default)]
	pub row_count: Option<u64>,
	/// ADR 0094: the complete materialization write chain, declared before
	/// any child outcome pin is installed or any row is dispatched.
	#[serde(default)]
	pub chunk_count: Option<u16>,
	/// Durable cleanup acknowledgement for the chain's outcome pins. Terminal
	/// detail may not be pruned while this is false.
	#[serde(default)]
	pub chain_pins_released: bool,
	pub phase: CtasSagaPhase,
	pub terminal_cause: Option<CtasTerminalCause>,
	pub owner_epoch: u64,
	pub owner_lease_expires_at_ms: u64,
	pub retry_debt: u32,
	/// Wall-clock completion time used only for bounded terminal-detail
	/// retention. Active phases never carry this field and are never aged out.
	#[serde(default)]
	pub terminal_at_ms: Option<u64>,
	/// ADR 0087: the materialization certificate, carried from `Provision` so
	/// a recovery re-publish binds table and certificate together. The saga is
	/// the certificate's carrier; its home is the attachment catalog, written
	/// inside the `Publish` apply. CTAS's own verb never sets this.
	#[serde(default)]
	pub certificate: Option<Vec<u8>>,
}

impl CtasSagaEntry {
	/// Whether `write_id` is one of this saga's durably declared row-write
	/// children. Before ADR 0094 declaration, the historical single write is
	/// the only admitted identity. Once declared, the operation-local sequence
	/// interval admits every child without re-deriving chain shape at the read
	/// site.
	pub fn contains_row_write_id(&self, write_id: ClientWriteId) -> bool {
		if self.row_write_id.client_uuid != write_id.client_uuid {
			return false;
		}
		let Some(offset) = write_id
			.sequence_no
			.checked_sub(self.row_write_id.sequence_no)
		else {
			return false;
		};
		let chunk_count = self.chunk_count.unwrap_or(1);
		offset < u64::from(chunk_count)
	}
}

/// ADR 0087: the materialization certificate's admission bound. Boson v1
/// certificates measure tens to a few hundred bytes; 64 KiB is generous
/// headroom while staying far inside the write discipline. An oversized
/// certificate refuses at `Provision`, before any row is dispatched.
pub const MATERIALIZATION_CERTIFICATE_MAX_BYTES: usize = 64 * 1024;

/// ADR 0094: whole-chain pin residency is capped below the global outcome
/// store capacity. This is a chain-count bound, independent of all per-frame
/// row and byte limits.
pub const CTAS_MAX_WRITE_CHUNKS: u16 = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasSagaMutation {
	Provision {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		intent_fingerprint: CtasIntentFingerprint,
		descriptor: TableSchemaDescriptor,
		expected_binding: CtasExpectedBinding,
		prior_descriptor: Option<TableSchemaDescriptor>,
		row_write_id: ClientWriteId,
		owner_epoch: u64,
		observed_now_ms: u64,
		owner_lease_expires_at_ms: u64,
		/// ADR 0087: set only by the materialize verb; bounded by
		/// [`MATERIALIZATION_CERTIFICATE_MAX_BYTES`], refused whole here.
		#[serde(default)]
		certificate: Option<Vec<u8>>,
	},
	AttestRowsCommitted {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
		row_count: u64,
	},
	DeclareChain {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
		row_count: u64,
		chunk_count: u16,
	},
	MarkRowDispatched {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
		row_count: u64,
	},
	MarkChainPinsReleased {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
	},
	Publish {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
	},
	MarkRetiringOld {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
	},
	Complete {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
		completed_at_ms: u64,
	},
	Reclaim {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
		cause: CtasTerminalCause,
		completed_at_ms: u64,
	},
	RecordPublicationConflict {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
		completed_at_ms: u64,
	},
	RecordRetryDebt {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		owner_epoch: u64,
	},
	Claim {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		expected_owner_epoch: u64,
		new_owner_epoch: u64,
		observed_now_ms: u64,
		lease_expires_at_ms: u64,
	},
}

impl CtasSagaMutation {
	/// ADR 0092: every saga-mutation variant, by name, in declaration order.
	/// This enum rides inside the WAL body (`WendelonRequest::CtasSaga`), so
	/// the same exact-set discipline applies: an appended variant moves no
	/// stored golden bytes, and only this list plus the total match below can
	/// catch it. Adding a variant here means the WAL format version must move
	/// with it.
	pub const ALL_VARIANT_NAMES: [&'static str; 12] = [
		"provision",
		"attest_rows_committed",
		"declare_chain",
		"mark_row_dispatched",
		"mark_chain_pins_released",
		"publish",
		"mark_retiring_old",
		"complete",
		"reclaim",
		"record_publication_conflict",
		"record_retry_debt",
		"claim",
	];

	/// Total by construction: a new variant fails this match at compile time.
	pub fn variant_name(&self) -> &'static str {
		match self {
			Self::Provision { .. } => "provision",
			Self::AttestRowsCommitted { .. } => "attest_rows_committed",
			Self::DeclareChain { .. } => "declare_chain",
			Self::MarkRowDispatched { .. } => "mark_row_dispatched",
			Self::MarkChainPinsReleased { .. } => "mark_chain_pins_released",
			Self::Publish { .. } => "publish",
			Self::MarkRetiringOld { .. } => "mark_retiring_old",
			Self::Complete { .. } => "complete",
			Self::Reclaim { .. } => "reclaim",
			Self::RecordPublicationConflict { .. } => "record_publication_conflict",
			Self::RecordRetryDebt { .. } => "record_retry_debt",
			Self::Claim { .. } => "claim",
		}
	}

	pub const fn tenant_id(&self) -> TenantId {
		match self {
			Self::Provision { tenant_id, .. }
			| Self::AttestRowsCommitted { tenant_id, .. }
			| Self::DeclareChain { tenant_id, .. }
			| Self::MarkRowDispatched { tenant_id, .. }
			| Self::MarkChainPinsReleased { tenant_id, .. }
			| Self::Publish { tenant_id, .. }
			| Self::MarkRetiringOld { tenant_id, .. }
			| Self::Complete { tenant_id, .. }
			| Self::Reclaim { tenant_id, .. }
			| Self::RecordPublicationConflict { tenant_id, .. }
			| Self::RecordRetryDebt { tenant_id, .. }
			| Self::Claim { tenant_id, .. } => *tenant_id,
		}
	}

	pub const fn operation_id(&self) -> CtasOperationId {
		match self {
			Self::Provision { operation_id, .. }
			| Self::AttestRowsCommitted { operation_id, .. }
			| Self::DeclareChain { operation_id, .. }
			| Self::MarkRowDispatched { operation_id, .. }
			| Self::MarkChainPinsReleased { operation_id, .. }
			| Self::Publish { operation_id, .. }
			| Self::MarkRetiringOld { operation_id, .. }
			| Self::Complete { operation_id, .. }
			| Self::Reclaim { operation_id, .. }
			| Self::RecordPublicationConflict { operation_id, .. }
			| Self::RecordRetryDebt { operation_id, .. }
			| Self::Claim { operation_id, .. } => *operation_id,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasSagaReject {
	NotFound,
	Expired,
	InvalidOperationId,
	CapacityExhausted,
	IntentMismatch,
	BindingChanged,
	ForeignProvisionalObject,
	RowsNotCommitted,
	StaleOwner,
	OwnerLeaseStillLive,
	InvalidTransition,
	/// ADR 0087: the certificate exceeds
	/// [`MATERIALIZATION_CERTIFICATE_MAX_BYTES`]; the materialization refuses
	/// whole, before any row is dispatched.
	CertificateTooLarge,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasSagaResponse {
	Applied(CtasSagaEntry),
	Rejected(CtasSagaReject),
}

/// ADR 0094 replay-only bridge for a WAL-v2 `MarkRowDispatched`, whose one
/// mutation historically both declared a one-child chain and dispatched it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayLegacyCtasRowDispatched {
	pub tenant_id: TenantId,
	pub operation_id: CtasOperationId,
	pub owner_epoch: u64,
	pub row_count: u64,
}

pub const CTAS_SAGA_REJECT_PREFIX: &str = "CTAS_SAGA_REJECT:";

impl CtasSagaReject {
	pub const fn code(self) -> &'static str {
		match self {
			Self::NotFound => "not_found",
			Self::Expired => "expired",
			Self::InvalidOperationId => "invalid_operation_id",
			Self::CapacityExhausted => "capacity_exhausted",
			Self::IntentMismatch => "intent_mismatch",
			Self::BindingChanged => "binding_changed",
			Self::ForeignProvisionalObject => "foreign_provisional_object",
			Self::RowsNotCommitted => "rows_not_committed",
			Self::StaleOwner => "stale_owner",
			Self::OwnerLeaseStillLive => "owner_lease_still_live",
			Self::InvalidTransition => "invalid_transition",
			Self::CertificateTooLarge => "certificate_too_large",
		}
	}

	pub fn from_code(code: &str) -> Option<Self> {
		Some(match code {
			"not_found" => Self::NotFound,
			"expired" => Self::Expired,
			"invalid_operation_id" => Self::InvalidOperationId,
			"capacity_exhausted" => Self::CapacityExhausted,
			"intent_mismatch" => Self::IntentMismatch,
			"binding_changed" => Self::BindingChanged,
			"foreign_provisional_object" => Self::ForeignProvisionalObject,
			"rows_not_committed" => Self::RowsNotCommitted,
			"stale_owner" => Self::StaleOwner,
			"owner_lease_still_live" => Self::OwnerLeaseStillLive,
			"invalid_transition" => Self::InvalidTransition,
			"certificate_too_large" => Self::CertificateTooLarge,
			_ => return None,
		})
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasSagaCatalog {
	#[serde(default)]
	entries: BTreeMap<(TenantId, CtasOperationId), CtasSagaEntry>,
	/// Durable high-water evidence for terminal detail already reclaimed from
	/// `entries`. Combined with UUIDv7 issuance time, this makes an old replay
	/// rejectable without retaining one tombstone per operation forever.
	#[serde(default)]
	expired_through_ms: BTreeMap<TenantId, u64>,
}

impl CtasSagaCatalog {
	pub fn get(
		&self,
		tenant_id: TenantId,
		operation_id: CtasOperationId,
	) -> Option<&CtasSagaEntry> {
		self.entries.get(&(tenant_id, operation_id))
	}

	pub fn get_mut(
		&mut self,
		tenant_id: TenantId,
		operation_id: CtasOperationId,
	) -> Option<&mut CtasSagaEntry> {
		self.entries.get_mut(&(tenant_id, operation_id))
	}

	pub fn insert(&mut self, entry: CtasSagaEntry) {
		self.entries
			.insert((entry.tenant_id, entry.operation_id), entry);
	}

	pub fn is_expired(
		&self,
		tenant_id: TenantId,
		operation_id: CtasOperationId,
		observed_now_ms: u64,
	) -> bool {
		let Some(issued_at_ms) = operation_id.issued_at_unix_millis() else {
			return false;
		};
		issued_at_ms
			<= self
				.expired_through_ms
				.get(&tenant_id)
				.copied()
				.unwrap_or_default()
			|| issued_at_ms.saturating_add(CTAS_SAGA_DETAIL_RETENTION_MS) <= observed_now_ms
	}

	/// Reclaim only terminal detail past the declared horizon. Each removal
	/// advances a durable per-tenant high-water mark, so bounded storage does not
	/// turn an old operation back into a fresh one.
	pub fn prune_terminal_detail(&mut self, observed_now_ms: u64) {
		let horizon = observed_now_ms.saturating_sub(CTAS_SAGA_DETAIL_RETENTION_MS);
		let expired = self
			.entries
			.iter()
			.filter_map(|(key, entry)| {
				entry
					.terminal_at_ms
					.filter(|terminal_at_ms| {
						*terminal_at_ms <= horizon && entry.chain_pins_released
					})
					.map(|_| *key)
			})
			.collect::<Vec<_>>();
		for (tenant_id, operation_id) in expired {
			self.entries.remove(&(tenant_id, operation_id));
			if let Some(issued_at_ms) = operation_id.issued_at_unix_millis() {
				self.expired_through_ms
					.entry(tenant_id)
					.and_modify(|current| *current = (*current).max(issued_at_ms))
					.or_insert(issued_at_ms);
			}
		}
	}

	pub fn operation_for_object(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&CtasSagaEntry> {
		self.entries.values().find(|entry| {
			entry.tenant_id == tenant_id
				&& entry.descriptor.table_object_id == table_object_id
				&& !entry.phase.is_terminal()
		})
	}

	pub fn retiring_old_binding(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		schema_generation: SchemaGeneration,
	) -> bool {
		self.entries.values().any(|entry| {
			entry.tenant_id == tenant_id
				&& entry.phase == CtasSagaPhase::RetiringOld
				&& matches!(
					entry.expected_binding,
					CtasExpectedBinding::Bound {
						table_object_id: expected_object,
						schema_generation: expected_generation,
					} if expected_object == table_object_id
						&& expected_generation == schema_generation
				)
		})
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}
}
