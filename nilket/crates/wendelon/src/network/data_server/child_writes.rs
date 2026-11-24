//! ADR 0699 S3b: child writes under the ParentDecisions authority.
//!
//! A child write carries the parent's own user rows for one child. Every
//! receiver re-derives the row identities, ring positions and manifest from
//! those rows and the replicated claim, and builds its own storage frames:
//! nothing a sender computed is trusted. This module holds the parts that do
//! not touch the network: the manifest, ring positions and the parent locks.

// The frames and dispatch that call this pipeline arrive with the coordinator
// path (ADR 0699 S3c); until then only its controls reach it.
#![cfg_attr(not(test), allow(dead_code))]

use super::*;
use crate::schema::ClientWriteId;
use crate::storage::index::parent_intents::OwnedVersion;
use crate::storage::manager::tier_authority::RetirementWait;

/// Stripes of the per-parent lock. A fixed set: no entry is created or freed
/// per request, so nothing leaks and one `rsv` always maps to one mutex.
pub(super) const PARENT_LOCK_STRIPES: usize = 256;

/// The lock every step of one parent's child write and its refused cleanup
/// takes. Guards are owned, so work spawned under a guard keeps it until that
/// work finishes, even if the request that started it is cancelled.
pub(crate) struct ParentLocks {
	stripes: Vec<Arc<tokio::sync::Mutex<()>>>,
	/// Test-only (DC-272): fences that found a stripe busy and waited for it.
	#[cfg(test)]
	pub(crate) fence_waits: std::sync::atomic::AtomicUsize,
}

impl ParentLocks {
	pub(crate) fn new() -> Self {
		Self {
			stripes: (0..PARENT_LOCK_STRIPES)
				.map(|_| Arc::new(tokio::sync::Mutex::new(())))
				.collect(),
			#[cfg(test)]
			fence_waits: Default::default(),
		}
	}

	fn stripe(&self, rsv: u64) -> &Arc<tokio::sync::Mutex<()>> {
		&self.stripes[(rsv % self.stripes.len() as u64) as usize]
	}

	/// How many stripes this registry has.
	pub(crate) fn stripe_count(&self) -> usize {
		self.stripes.len()
	}

	pub(super) async fn lock(&self, rsv: u64) -> tokio::sync::OwnedMutexGuard<()> {
		self.stripe(rsv).clone().lock_owned().await
	}

	/// DC-271: `rsv`'s stripe now, or `None` while a producer (or anything
	/// else on that stripe) holds it.
	pub(super) fn try_lock(&self, rsv: u64) -> Option<tokio::sync::OwnedMutexGuard<()>> {
		self.stripe(rsv).clone().try_lock_owned().ok()
	}

	/// DC-272: the stripes marked in `wanted` (one flag per stripe), acquired in
	/// ascending stripe order, waiting for any producer that holds one. Taken
	/// before, never under, the index write lock.
	pub(crate) async fn fence(&self, wanted: &[bool]) -> ProducerFence {
		let mut covered = vec![false; self.stripes.len()];
		let mut guards = Vec::new();
		for (stripe, lock) in self.stripes.iter().enumerate() {
			if !wanted.get(stripe).copied().unwrap_or(false) {
				continue;
			}
			let guard = match lock.clone().try_lock_owned() {
				Ok(guard) => guard,
				Err(_) => {
					#[cfg(test)]
					self.fence_waits
						.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
					lock.clone().lock_owned().await
				}
			};
			covered[stripe] = true;
			guards.push(guard);
		}
		ProducerFence {
			covered,
			_guards: guards,
		}
	}

	/// Test-only: holds `rsv`'s stripe as a producer would.
	#[cfg(test)]
	pub(crate) async fn lock_for_test(&self, rsv: u64) -> tokio::sync::OwnedMutexGuard<()> {
		self.lock(rsv).await
	}
}

/// DC-272: the producer stripes an activation holds while it retires intents.
/// Built only by [`ParentLocks::fence`] (its guards are real); it is moved into
/// the index operation that commits or aborts, so the stripes stay held until
/// that operation ends, even if its async caller is cancelled.
pub(crate) struct ProducerFence {
	covered: Vec<bool>,
	_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

impl ProducerFence {
	/// Whether this fence holds `rsv`'s stripe.
	pub(crate) fn covers(&self, rsv: u64) -> bool {
		let stripes = self.covered.len() as u64;
		stripes > 0 && self.covered[(rsv % stripes) as usize]
	}

	/// Test-only: every stripe of a fresh registry, uncontended.
	#[cfg(test)]
	pub(crate) async fn uncontended_for_test() -> Self {
		let locks = ParentLocks::new();
		let all = vec![true; locks.stripe_count()];
		locks.fence(&all).await
	}
}

/// Why a child's rows cannot form a manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ManifestRefusal {
	/// Only primary-key tables are admitted in this mode: append-only identities
	/// depend on the row's place in the original batch, and content-hash
	/// identities are shared across parents.
	UnsupportedTable,
	/// Wendelon ADR 0119: the ordinals or identities given for the rows are
	/// malformed: a count mismatch or a repeat.
	RowKeys(&'static str),
	/// The rows fail today's admission for this table.
	Admission(String),
	/// A row's canonical bytes cannot be formed.
	Canonical(String),
	/// Two rows name the same version.
	DuplicateVersion,
}

/// A row's content commitment: its table, schema generation and canonical bytes.
pub(super) fn row_digest(
	table: TableObjectId,
	schema_generation: u64,
	canonical: &[u8],
) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new_derive_key("wendelon parent child row v1");
	hasher.update(table.as_bytes());
	hasher.update(&schema_generation.to_le_bytes());
	hasher.update(&(canonical.len() as u64).to_le_bytes());
	hasher.update(canonical);
	*hasher.finalize().as_bytes()
}

/// A child's content commitment: its manifest, as the claim binds it.
pub(crate) fn manifest_content(manifest: &[OwnedVersion]) -> [u8; 32] {
	let bytes = postcard::to_stdvec(manifest).expect("a manifest encodes");
	let mut hasher = blake3::Hasher::new_derive_key("wendelon parent child manifest v1");
	hasher.update(&bytes);
	*hasher.finalize().as_bytes()
}

/// The request meta a receiver rebuilds from the claim, for today's admission.
pub(crate) fn child_request_meta(
	key: &crate::consensus::decisions::ParentKey,
	table_object_id: TableObjectId,
	schema_generation: u64,
	rows: usize,
	commit_hlc: HybridTimestamp,
) -> std::io::Result<TableWriteRequestMeta> {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id = ClientWriteId {
		client_uuid: key.client_uuid,
		sequence_no: key.sequence_no,
		idempotency_ttl_ms: 0,
		_pad: 0,
	};
	meta.client_hlc = commit_hlc;
	meta.tenant_id = key.tenant;
	meta.table_object_id = table_object_id;
	meta.schema_generation = schema_generation;
	meta.row_count = crate::schema::table::encode_table_write_row_count(rows)?;
	Ok(meta)
}

/// The writer identity of the coordinator that stamped `commit_hlc`.
pub(super) fn writer_of(commit_hlc: HybridTimestamp) -> WriterNodeId {
	WriterNodeId::from_node_id(NodeId {
		id: commit_hlc.node_id,
		_reserved: 0,
	})
}

/// Wendelon ADR 0119: where a child's row identities come from. A PrimaryKey
/// table's are derived from each row's key value. A named AppendOnly table's are
/// given: by each row's original ordinal in the request (a write, a replica), or
/// as identities checked against the signed content (a repair answer, a
/// recovered page).
#[derive(Clone, Copy, Debug)]
pub(crate) enum RowKeys<'a> {
	Values,
	Ordinals(&'a [u32]),
	Identities(&'a [crate::schema::Identity]),
}

impl<'a> RowKeys<'a> {
	/// The keys of a request's rows at `ordinals`: those ordinals for an
	/// append-only table, the rows' own values otherwise.
	pub(crate) fn of_request(descriptor: &TableSchemaDescriptor, ordinals: &'a [u32]) -> Self {
		if matches!(
			descriptor.row_identity_policy,
			crate::schema::RowIdentityPolicy::AppendOnly
		) {
			Self::Ordinals(ordinals)
		} else {
			Self::Values
		}
	}
}

/// Wendelon ADR 0119 D1/D4: the identities of a write's rows at `ordinals`:
/// one per row, distinct, each derived from the write's key and its ordinal.
fn append_only_identities(
	meta: &TableWriteRequestMeta,
	ordinals: &[u32],
	rows: usize,
) -> Result<Vec<crate::schema::Identity>, ManifestRefusal> {
	if ordinals.len() != rows {
		return Err(ManifestRefusal::RowKeys("one ordinal per row"));
	}
	let mut sorted = ordinals.to_vec();
	sorted.sort_unstable();
	if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
		return Err(ManifestRefusal::RowKeys("ordinals repeat"));
	}
	Ok(ordinals
		.iter()
		.map(|ordinal| {
			crate::schema::table::parent_decisions_append_only_row_id(
				meta.tenant_id,
				meta.table_object_id,
				meta.parent_write_id.client_uuid,
				meta.parent_write_id.sequence_no,
				*ordinal,
			)
		})
		.collect())
}

/// The admission of a child's rows with identities from `keys` (wendelon ADR
/// 0119): each row's identity and locality, index-aligned with the batch.
pub(crate) fn keyed_admission(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	commit_hlc: HybridTimestamp,
	keys: RowKeys<'_>,
) -> Result<crate::schema::TableWriteAdmission, ManifestRefusal> {
	use crate::schema::RowIdentityPolicy;
	// Wendelon ADR 0119 D5 (Codex 01a1f9ca, 01a1fa16): a named AppendOnly table is
	// written under ParentDecisions only when every column is of the ADR's
	// declared types (`parent_decisions_append_only_schema`), checked here before
	// any identity is derived; others stay refused, as before.
	if matches!(
		descriptor.row_identity_policy,
		RowIdentityPolicy::AppendOnly
	) && !parent_decisions_append_only_schema(descriptor)
	{
		return Err(ManifestRefusal::UnsupportedTable);
	}
	let admitted = match (&descriptor.row_identity_policy, keys) {
		(RowIdentityPolicy::PrimaryKey(_), RowKeys::Values) => validate_table_write_request_batch(
			meta,
			batch,
			descriptor,
			writer_of(commit_hlc),
			commit_hlc,
		),
		(RowIdentityPolicy::AppendOnly, RowKeys::Ordinals(ordinals)) => {
			let identities = append_only_identities(meta, ordinals, batch.num_rows())?;
			crate::schema::table::admit_parent_decisions_append_only_rows(
				meta,
				batch,
				descriptor,
				&identities,
			)
		}
		(RowIdentityPolicy::AppendOnly, RowKeys::Identities(identities)) => {
			crate::schema::table::admit_parent_decisions_append_only_rows(
				meta, batch, descriptor, identities,
			)
		}
		_ => return Err(ManifestRefusal::UnsupportedTable),
	};
	admitted.map_err(|error| ManifestRefusal::Admission(error.to_string()))
}

/// Each row's version, in the batch's row order, with identities from `keys`
/// (wendelon ADR 0119). PrimaryKey derives them from values; a named AppendOnly
/// table takes them given. Every other combination is refused.
pub(crate) fn row_versions_keyed(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	commit_hlc: HybridTimestamp,
	keys: RowKeys<'_>,
) -> Result<Vec<OwnedVersion>, ManifestRefusal> {
	let admission = keyed_admission(meta, batch, descriptor, commit_hlc, keys)?;
	let mut manifest = Vec::with_capacity(admission.key_identities.len());
	for (row, key_identity) in admission.key_identities.iter().enumerate() {
		let canonical = crate::storage::content_hash::encode_row_canonical(descriptor, batch, row)
			.map_err(|error| ManifestRefusal::Canonical(format!("{error:?}")))?;
		manifest.push(OwnedVersion {
			table: descriptor.table_object_id,
			key_identity: *key_identity.as_bytes(),
			hlc: commit_hlc,
			row_digest: row_digest(
				descriptor.table_object_id,
				descriptor.schema_generation,
				canonical.as_slice(),
			),
		});
	}
	Ok(manifest)
}

/// The manifest of a child's rows: one version per row, sorted, no repeats. A
/// PrimaryKey table's rows only.
pub(crate) fn child_manifest(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	commit_hlc: HybridTimestamp,
) -> Result<Vec<OwnedVersion>, ManifestRefusal> {
	Ok(child_versions_keyed(meta, batch, descriptor, commit_hlc, RowKeys::Values)?.1)
}

/// The sorted manifest of `in_order`, refused if a version repeats.
fn sorted_manifest(in_order: &[OwnedVersion]) -> Result<Vec<OwnedVersion>, ManifestRefusal> {
	let mut manifest = in_order.to_vec();
	manifest.sort();
	if manifest.windows(2).any(|pair| {
		(pair[0].table, pair[0].key_identity, pair[0].hlc)
			== (pair[1].table, pair[1].key_identity, pair[1].hlc)
	}) {
		return Err(ManifestRefusal::DuplicateVersion);
	}
	Ok(manifest)
}

/// The ring position a stored record is placed by: its partition for a
/// partitioned record, its raw locality otherwise (ADR 0090).
pub(super) fn record_position(child: &BifurcatedTableChildWrite) -> u64 {
	child
		.partition
		.map(|partition| partition.get())
		.unwrap_or_else(|| child.locality.get())
}

/// What a child write names: the reservation, the child and the descriptor
/// generation of its rows. Everything else comes from the replicated claim.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChildWriteMeta {
	pub rsv: u64,
	pub ordinal: u16,
	pub claim_digest: [u8; 32],
	pub schema_generation: u64,
}

/// Which hop a child write is: coordinator to the child's holder, or holder to
/// one of the child's plan members.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChildRole {
	Holder,
	Replica,
	/// ADR 0699 S4: a plan member's verified copy of an Accepted child, fetched
	/// from another plan member. Persists without an acknowledgement.
	Repair,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChildRefusal {
	ModeToday,
	NotCaughtUp,
	UnknownReservation,
	ClaimMismatch,
	/// The authenticated sender is not the role the claim records.
	WrongSender,
	/// This store is not in the child's plan at its current incarnation.
	NotInPlan,
	LineageClosed,
	/// No unexpired local lease covers the child range.
	LeaseMissing,
	SchemaUnknown,
	Rows(ManifestRefusal),
	/// A record's ring position is outside the child range.
	OutOfRange,
	/// The rows' manifest is not the one the claim commits to.
	ContentMismatch,
	ParentRefused,
	/// Accepted, and this store holds no complete intent to acknowledge.
	ParentAccepted,
	/// A repair of a parent not recorded Accepted.
	NotAccepted,
	Intent(crate::storage::index::parent_intents::IntentRefusal),
	/// The stored rows do not prove the manifest: a version absent, a copy with
	/// other content, or ownership inconsistent.
	Unverified(String),
	Storage(String),
}

impl ChildRefusal {
	/// DC-287: this refusal's variant, as a stable code (1 + its position above).
	pub(crate) fn code(&self) -> u8 {
		match self {
			Self::ModeToday => 1,
			Self::NotCaughtUp => 2,
			Self::UnknownReservation => 3,
			Self::ClaimMismatch => 4,
			Self::WrongSender => 5,
			Self::NotInPlan => 6,
			Self::LineageClosed => 7,
			Self::LeaseMissing => 8,
			Self::SchemaUnknown => 9,
			Self::Rows(_) => 10,
			Self::OutOfRange => 11,
			Self::ContentMismatch => 12,
			Self::ParentRefused => 13,
			Self::ParentAccepted => 14,
			Self::NotAccepted => 15,
			Self::Intent(_) => 16,
			Self::Unverified(_) => 17,
			Self::Storage(_) => 18,
		}
	}
}

/// A child write checked against replicated state, ready for the storage step.
#[derive(Clone, Debug)]
pub(crate) struct ChildPlan {
	pub(crate) intent: crate::storage::index::parent_intents::ParentIntent,
	pub(crate) descriptor: TableSchemaDescriptor,
	pub(crate) secondary_indexes: Vec<SecondaryIndexDescriptor>,
	pub(crate) batch: RecordBatch,
	/// Each row's version, in batch order.
	pub(crate) row_versions: Vec<OwnedVersion>,
	pub(crate) ack: crate::consensus::decisions::ChildAckBody,
	/// ADR 0699 S4b, wendelon ADR 0112: the rows this delete child publishes a
	/// tombstone for, in identity order.
	pub(crate) tombstone: Option<Vec<crate::consensus::decisions::TombstoneTarget>>,
}

/// Wendelon ADR 0108 D5 (DC-250): a child plan whose secondary-index
/// descriptors were read under the store's index admission guard. The guard
/// travels with the plan into its persistence and is released only when the
/// durable append and its index buffer entries are done, so a declaration cut
/// never falls between reading the descriptors and writing their entries. It
/// is taken before any parent lock, never while one is held.
pub(crate) struct AdmittedPlan {
	plan: ChildPlan,
	admission: tokio::sync::OwnedRwLockReadGuard<()>,
}

impl AdmittedPlan {
	/// Takes the guard, then reads the plan (with its descriptors) from
	/// applied state.
	pub(crate) async fn admit(
		slab: &SlabManager,
		state_machine: &WendelonStateMachine,
		plan: impl FnOnce(&SmState) -> Result<ChildPlan, ChildRefusal>,
	) -> Result<Self, ChildRefusal> {
		let admission = slab.acquire_secondary_index_admission_guard().await;
		let plan = state_machine.read_state(plan).await?;
		Ok(Self { plan, admission })
	}

	pub(crate) fn plan(&self) -> &ChildPlan {
		&self.plan
	}

	/// Wendelon ADR 0108 D5: the same plan with no index descriptor, for a
	/// store whose buffers no declaration cut proves (a ControlOnly
	/// replacement).
	pub(crate) fn without_index_buffers(mut self) -> Self {
		self.plan.secondary_indexes.clear();
		self
	}

	/// A plan built by a test, admitted under the guard as `admit` would.
	#[cfg(test)]
	pub(crate) async fn for_test(slab: &SlabManager, plan: ChildPlan) -> Self {
		let admission = slab.acquire_secondary_index_admission_guard().await;
		Self { plan, admission }
	}
}

/// The manifest of a child's rows: one version per row in batch order, and
/// the sorted manifest the claim commits to. A PrimaryKey table's rows only.
pub(super) fn child_versions(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	commit_hlc: HybridTimestamp,
) -> Result<(Vec<OwnedVersion>, Vec<OwnedVersion>), ManifestRefusal> {
	child_versions_keyed(meta, batch, descriptor, commit_hlc, RowKeys::Values)
}

/// `child_versions`, with identities from `keys` (wendelon ADR 0119): the
/// batch-order versions come from one admission, index-aligned, and the sorted
/// manifest is their sorted copy. No per-row search.
pub(crate) fn child_versions_keyed(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	commit_hlc: HybridTimestamp,
	keys: RowKeys<'_>,
) -> Result<(Vec<OwnedVersion>, Vec<OwnedVersion>), ManifestRefusal> {
	let in_order = row_versions_keyed(meta, batch, descriptor, commit_hlc, keys)?;
	let manifest = sorted_manifest(&in_order)?;
	Ok((in_order, manifest))
}

/// Checks a child write against the applied state, independently of whoever
/// sent it: the reservation and claim, the sender's recorded role, this store's
/// place in the plan, the holder's lineage and lease, and the rows themselves.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_child(
	state: &SmState,
	local: crate::consensus::decisions::Signer,
	sender: u32,
	role: ChildRole,
	meta: &ChildWriteMeta,
	batch: RecordBatch,
	lease_covers: impl Fn(u64, u64) -> bool,
) -> Result<ChildPlan, ChildRefusal> {
	plan_child_keyed(
		state,
		local,
		sender,
		role,
		meta,
		batch,
		GivenKeys::None,
		lease_covers,
	)
}

/// `plan_child`, with the row keys the frame carried (wendelon ADR 0119): none
/// for a PrimaryKey or delete child, a named AppendOnly child's original
/// ordinals on a write or replicate, its identities on a repair answer. Any
/// other combination refuses.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_child_keyed(
	state: &SmState,
	local: crate::consensus::decisions::Signer,
	sender: u32,
	role: ChildRole,
	meta: &ChildWriteMeta,
	batch: RecordBatch,
	given: GivenKeys,
	lease_covers: impl Fn(u64, u64) -> bool,
) -> Result<ChildPlan, ChildRefusal> {
	use crate::consensus::decisions::{ChildAckBody, lineage_is_open};
	let decisions = state.decisions.as_ref().ok_or(ChildRefusal::ModeToday)?;
	let body = decisions
		.body(meta.rsv)
		.ok_or(ChildRefusal::UnknownReservation)?;
	if body.claim_digest != meta.claim_digest {
		return Err(ChildRefusal::ClaimMismatch);
	}
	let claim = &body.claim;
	let child = claim
		.children
		.get(usize::from(meta.ordinal))
		.ok_or(ChildRefusal::ClaimMismatch)?;
	let table = parent_table(claim.key.namespace);
	let expected_sender = match role {
		ChildRole::Holder => claim
			.children
			.get(usize::from(claim.coordinator))
			.map(|coordinator| coordinator.holder.node),
		ChildRole::Replica => Some(child.holder.node),
		ChildRole::Repair => child
			.plan
			.iter()
			.map(|member| member.node)
			.find(|member| *member == sender && *member != local.node),
	};
	if expected_sender != Some(sender) {
		return Err(ChildRefusal::WrongSender);
	}
	let current = state
		.incarnations
		.get(&local.node)
		.and_then(|registry| registry.current);
	// ADR 0106 (wendelon): a repair may copy a child naming an ancestor of this
	// store (its recorded promotion chain); it never acknowledges for it.
	let named = child.plan.contains(&local)
		|| (role == ChildRole::Repair
			&& child.plan.iter().any(|member| {
				member.node == local.node
					&& crate::consensus::decisions::in_recorded_ancestry(
						state,
						local.node,
						local.incarnation,
						member.incarnation,
					)
			}));
	if current != Some(local.incarnation) || !named {
		return Err(ChildRefusal::NotInPlan);
	}
	if role == ChildRole::Holder {
		if child.holder != local {
			return Err(ChildRefusal::WrongSender);
		}
		if !lineage_is_open(state, child.lineage, child.holder) {
			return Err(ChildRefusal::LineageClosed);
		}
		if !lease_covers(child.range_start, child.range_end) {
			return Err(ChildRefusal::LeaseMissing);
		}
	}
	let descriptor =
		recorded_table_descriptor(state, claim.key.tenant, table, meta.schema_generation)
			.ok_or(ChildRefusal::SchemaUnknown)?;
	// ADR 0699 S4b, wendelon ADR 0112: a delete child's versions are derived from
	// the recorded claim; its payload only names its rows, which must be exactly
	// the claim's matched rows in this child's range, in identity order.
	let (row_versions, manifest, tombstone) =
		if let crate::consensus::decisions::Operation::Delete { request, selection } =
			&claim.operation
		{
			let rows = super::deletes::child_rows(
				claim.key.tenant,
				&descriptor,
				request,
				selection,
				child,
			)
			.ok_or(ChildRefusal::ContentMismatch)?;
			if super::deletes::row_ids_of(&batch).as_ref() != Some(&rows)
				|| request.schema_generation != meta.schema_generation
			{
				return Err(ChildRefusal::ContentMismatch);
			}
			if given != GivenKeys::None {
				return Err(ChildRefusal::Rows(ManifestRefusal::RowKeys(
					"a delete child carries no row keys",
				)));
			}
			let manifest = super::deletes::child_manifest(request, &rows, claim.commit_hlc);
			let targets = rows.iter().map(|row| request.target(*row)).collect();
			(manifest.clone(), manifest, Some(targets))
		} else {
			let request = child_request_meta(
				&claim.key,
				table,
				meta.schema_generation,
				batch.num_rows(),
				claim.commit_hlc,
			)
			.map_err(|_| ChildRefusal::Rows(ManifestRefusal::Admission("row count".into())))?;
			let append_only = matches!(
				descriptor.row_identity_policy,
				crate::schema::RowIdentityPolicy::AppendOnly
			);
			// Wendelon ADR 0119 D4/D5: the section is decoded only here, once the
			// operation (a write), the policy and the role fix what it holds, and
			// only after the rows are bounded: the batch has exactly this child's
			// count, and the write's N (the checked sum of the claim's child
			// counts) is within the AppendOnly write bound.
			let section = match &given {
				GivenKeys::None => None,
				GivenKeys::Section(bytes) => Some(bytes.as_slice()),
			};
			let refused = |why: &'static str| ChildRefusal::Rows(ManifestRefusal::RowKeys(why));
			let ordinals: Vec<u32>;
			let identities: Vec<crate::schema::Identity>;
			let keys = match (append_only, section) {
				(false, None) => RowKeys::Values,
				(true, Some(bytes)) => {
					let rows = claim_rows(claim).ok_or(ChildRefusal::ClaimMismatch)?;
					if rows > crate::schema::table::TABLE_WRITE_MAX_ROWS as u64 {
						return Err(refused("the write exceeds its row bound"));
					}
					if batch.num_rows() != child.rows as usize {
						return Err(ChildRefusal::ContentMismatch);
					}
					match role {
						ChildRole::Holder | ChildRole::Replica => {
							ordinals = decode_ordinals(bytes, batch.num_rows())
								.map_err(|_| refused("the ordinals do not match the rows"))?;
							// D4: each ordinal lies inside the write of N rows.
							if ordinals.iter().any(|ordinal| u64::from(*ordinal) >= rows) {
								return Err(refused("an ordinal lies outside the write"));
							}
							RowKeys::Ordinals(&ordinals)
						}
						ChildRole::Repair => {
							identities = decode_identities(bytes, batch.num_rows())
								.map_err(|_| refused("the identities do not match the rows"))?;
							RowKeys::Identities(&identities)
						}
					}
				}
				_ => return Err(refused("the row keys do not match the table and role")),
			};
			let (row_versions, manifest) =
				child_versions_keyed(&request, &batch, &descriptor, claim.commit_hlc, keys)
					.map_err(ChildRefusal::Rows)?;
			(row_versions, manifest, None)
		};
	if manifest.len() != child.rows as usize || manifest_content(&manifest) != child.content {
		return Err(ChildRefusal::ContentMismatch);
	}
	let secondary_indexes = WendelonDataServer::secondary_index_admission_descriptors(
		&state.secondary_indexes,
		claim.key.tenant,
		&descriptor,
	);
	Ok(ChildPlan {
		intent: crate::storage::index::parent_intents::ParentIntent {
			tenant: claim.key.tenant,
			rsv: meta.rsv,
			ordinal: meta.ordinal,
			claim_digest: meta.claim_digest,
			range_start: child.range_start,
			range_end: child.range_end,
			content: child.content,
			manifest,
			state: crate::storage::index::parent_intents::ParentIntentState::Intent,
		},
		descriptor,
		secondary_indexes,
		batch,
		row_versions,
		tombstone,
		ack: ChildAckBody {
			key: claim.key,
			reservation: meta.rsv,
			claim_digest: meta.claim_digest,
			ordinal: meta.ordinal,
			range_start: child.range_start,
			range_end: child.range_end,
			lineage: child.lineage,
			holder_incarnation: child.holder.incarnation,
			rows: child.rows,
			content: child.content,
			acker: local,
		},
	})
}

/// ADR 0699 S4b: the table a parent's rows belong to. A KV parent writes one
/// row of the default KV table.
pub(crate) fn parent_table(
	namespace: crate::consensus::decisions::ParentNamespace,
) -> TableObjectId {
	use crate::consensus::decisions::ParentNamespace;
	match namespace {
		ParentNamespace::Table(table) => table,
		ParentNamespace::Kv => TableObjectId::DEFAULT_KV,
	}
}

/// The descriptor of `table` at `generation` from applied state; the default KV
/// table's is implicit, at generation 0, and never registered.
pub(crate) fn table_descriptor(
	state: &SmState,
	tenant: TenantId,
	table: TableObjectId,
	generation: u64,
) -> Option<TableSchemaDescriptor> {
	if table == TableObjectId::DEFAULT_KV {
		return (generation == 0)
			.then(crate::schema::table::default_kv_table_descriptor_v1)
			.and_then(Result::ok);
	}
	state.table_schemas.get(tenant, table, generation).cloned()
}

/// Wendelon ADR 0120 D4: the descriptor a recorded claim, a stored or certified
/// version, or a replacement's page names. The public lookup or, only for an
/// object a CTAS chain routes or retired, its provisional (retained) descriptor,
/// whatever the chain's phase. New traffic's admission, reads and export use the
/// public lookup, which hides provisional ids.
pub(crate) fn recorded_table_descriptor(
	state: &SmState,
	tenant: TenantId,
	table: TableObjectId,
	generation: u64,
) -> Option<TableSchemaDescriptor> {
	table_descriptor(state, tenant, table, generation).or_else(|| {
		ctas_private(state, tenant, table)
			.then(|| {
				state
					.table_schemas
					.get_including_provisional(tenant, table, generation)
					.cloned()
			})
			.flatten()
	})
}

/// [`recorded_table_descriptor`] at the table's latest generation.
pub(crate) fn recorded_latest_table_descriptor(
	state: &SmState,
	tenant: TenantId,
	table: TableObjectId,
) -> Option<TableSchemaDescriptor> {
	table_descriptor(state, tenant, table, 0)
		.or_else(|| state.table_schemas.latest_by_id(tenant, table).cloned())
		.or_else(|| {
			ctas_private(state, tenant, table)
				.then(|| {
					state
						.table_schemas
						.latest_by_id_including_provisional(tenant, table)
						.cloned()
				})
				.flatten()
		})
}

/// Wendelon ADR 0120 D6: an authenticated CTAS chunk dispatch's descriptor and its
/// chain's immutable declaration: only when the operation's chain is over
/// `table`, the key's client id is the operation, and the credential's
/// commitment is the winning owner's. In any phase: a recorded chunk's lost
/// acknowledgement replays after Publish or Abandon; a new chunk is refused
/// outside `Dispatching` by the coordinator and the reducer.
pub(crate) fn ctas_dispatch_descriptor(
	state: &SmState,
	tenant: TenantId,
	table: TableObjectId,
	generation: u64,
	op: [u8; 16],
	key_uuid: [u8; 16],
	owner: [u8; 32],
) -> Option<(
	TableSchemaDescriptor,
	Vec<crate::consensus::decisions::ctas::ChunkDecl>,
)> {
	let chain = state.decisions.as_ref()?.ctas().chain(tenant, op)?;
	let authorized = chain.object == table && key_uuid == op && chain.dispatch_hash == owner;
	if !authorized {
		return None;
	}
	let declared = chain.chunks.clone();
	recorded_table_descriptor(state, tenant, table, generation)
		.map(|descriptor| (descriptor, declared))
}

/// An object a CTAS chain routes or retired: its descriptor is private.
fn ctas_private(state: &SmState, tenant: TenantId, table: TableObjectId) -> bool {
	state
		.decisions
		.as_ref()
		.is_some_and(|decisions| decisions.ctas().claims_object(tenant, table))
}

/// The recorded status of reservation `rsv`, read from the applied state.
async fn parent_status(
	state_machine: &WendelonStateMachine,
	rsv: u64,
) -> Option<crate::consensus::decisions::ParentStatus> {
	state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.body(rsv))
				.map(|body| body.status)
		})
		.await
}

pub(super) fn replica_children(bifurcated: &BifurcatedTableWrite) -> Vec<TableReplicaChildPayload> {
	to_replica_children(bifurcated)
}

fn to_replica_children(bifurcated: &BifurcatedTableWrite) -> Vec<TableReplicaChildPayload> {
	bifurcated
		.children
		.iter()
		.map(|child| TableReplicaChildPayload {
			child_ordinal: child.child_ordinal,
			key_identity: child.key_identity,
			locality: child.locality,
			record_row_index: child.record_row_index,
			frame_bytes: child.frame_bytes.clone(),
			payload_hash: child.payload_hash,
			charged_bytes: child.charged_bytes,
		})
		.collect()
}

/// `rows` are the indices in `plan.batch` of `batch`'s rows, in order (all of
/// them, or the missing subset). Wendelon ADR 0119 D3: a named AppendOnly
/// table's rows keep the identities the plan checked, taken at the same indices,
/// so a sparse subset is never re-indexed.
fn bifurcate_child(
	slab: &SlabManager,
	plan: &ChildPlan,
	batch: &RecordBatch,
	rows: &[u32],
) -> Result<BifurcatedTableWrite, ChildRefusal> {
	let hlc = plan.ack_hlc();
	let request = child_request_meta(
		&plan.ack.key,
		plan.descriptor.table_object_id,
		plan.descriptor.schema_generation,
		batch.num_rows(),
		hlc,
	)
	.map_err(|error| ChildRefusal::Storage(error.to_string()))?;
	let admission = if matches!(
		plan.descriptor.row_identity_policy,
		crate::schema::RowIdentityPolicy::AppendOnly
	) {
		let identities = rows
			.iter()
			.map(|row| {
				plan.row_versions
					.get(*row as usize)
					.map(|version| crate::schema::Identity::new(version.key_identity))
					.ok_or(ChildRefusal::Rows(ManifestRefusal::RowKeys(
						"a row outside the plan",
					)))
			})
			.collect::<Result<Vec<_>, _>>()?;
		crate::schema::table::admit_parent_decisions_append_only_rows(
			&request,
			batch,
			&plan.descriptor,
			&identities,
		)
	} else {
		validate_table_write_request_batch(&request, batch, &plan.descriptor, writer_of(hlc), hlc)
	}
	.map_err(|error| ChildRefusal::Rows(ManifestRefusal::Admission(error.to_string())))?;
	bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
		&request,
		batch,
		&plan.descriptor,
		&admission,
		hlc,
		slab.row_group_writer_config(),
		slab.encoding_writer_config(),
		slab.l2_chunk_layout_config(),
	)
	.map_err(|error| ChildRefusal::Rows(ManifestRefusal::Admission(error.to_string())))
}

/// ADR 0106 (wendelon): stores one batch of a recovered base's rows, all at
/// `hlc`, durably, as a child's rows are stored, but with no intent and no
/// owner: a certified base row has none once its intent retired. Secondary
/// index buffers are not written; the replacement rebuilds its indexes.
pub(crate) async fn ingest_recovered_rows(
	slab: &SlabManager,
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	hlc: HybridTimestamp,
	identities: &[crate::schema::Identity],
) -> std::io::Result<()> {
	let mut request: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	request.client_hlc = hlc;
	request.tenant_id = tenant;
	request.table_object_id = descriptor.table_object_id;
	request.schema_generation = descriptor.schema_generation;
	request.row_count = crate::schema::table::encode_table_write_row_count(batch.num_rows())?;
	let invalid = |error: String| std::io::Error::new(std::io::ErrorKind::InvalidData, error);
	// Wendelon ADR 0119 D3: an append-only table's rows keep the identities of
	// their certified entries, index-aligned with `batch`.
	let admission = if matches!(
		descriptor.row_identity_policy,
		crate::schema::RowIdentityPolicy::AppendOnly
	) {
		crate::schema::table::admit_parent_decisions_append_only_rows(
			&request, batch, descriptor, identities,
		)
	} else {
		validate_table_write_request_batch(&request, batch, descriptor, writer_of(hlc), hlc)
	}
	.map_err(|error| invalid(error.to_string()))?;
	let full = bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
		&request,
		batch,
		descriptor,
		&admission,
		hlc,
		slab.row_group_writer_config(),
		slab.encoding_writer_config(),
		slab.l2_chunk_layout_config(),
	)
	.map_err(|error| invalid(error.to_string()))?;
	super::append_table_children_durable(slab, tenant, hlc, &to_replica_children(&full), &[]).await
}

impl ChildPlan {
	fn ack_hlc(&self) -> HybridTimestamp {
		self.intent.manifest[0].hlc
	}
}

/// The live leases this node holds, as `(start, end, epoch)`, read when asked.
/// ADR 0700: the applied grants to `local` at the current epoch that have not
/// expired at `now_ms`, whatever leader issued them.
pub(crate) fn applied_leases(
	state: &SmState,
	local: u32,
	now_ms: u64,
) -> Vec<crate::schema::LeaseDescriptor> {
	scan_applied_leases(state, local, now_ms, |_, _| {})
}

/// The one scan behind [`applied_leases`]: each of `local`'s rows is passed to `visit` with the
/// view's own filter result (the current epoch, and `HLC(now, 0, 0) < lease_expiry`, the full
/// HLC comparison), and the rows it admits are returned. Wendelon ADR 0127 D2: a traced holder
/// read captures inside this same pass, reusing the same result.
pub(crate) fn scan_applied_leases(
	state: &SmState,
	local: u32,
	now_ms: u64,
	mut visit: impl FnMut(&crate::schema::LeaseDescriptor, bool),
) -> Vec<crate::schema::LeaseDescriptor> {
	let epoch = state
		.epoch_history
		.last_key_value()
		.map(|(_, config)| config.epoch);
	let now = HybridTimestamp {
		physical_ms: now_ms,
		logical: 0,
		node_id: 0,
	};
	let mut live = Vec::new();
	for lease in state
		.leases
		.values()
		.filter(|lease| lease.holder.id == local)
	{
		let admitted = Some(lease.epoch) == epoch && now < lease.lease_expiry;
		visit(lease, admitted);
		if admitted {
			live.push(*lease);
		}
	}
	live
}

pub(crate) type LeaseSource = Arc<
	dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<(u64, u64, u64)>> + Send>>
		+ Send
		+ Sync,
>;

/// What a store needs to acknowledge a child: its signer, and for the holder
/// its live leases, re-read once the parent's lock is held.
#[derive(Clone)]
pub(crate) struct ChildAuthority {
	pub(crate) signer: Arc<DecisionSigner>,
	pub(crate) role: ChildRole,
	pub(crate) leases: Option<LeaseSource>,
	/// Wendelon ADR 0127 D2: the coordinator's local holder child, traced: where its first
	/// `LeaseMissing` evidence goes, with the capturing lease view.
	pub(crate) lease_evidence: Option<Arc<super::lease_evidence::LeaseEvidenceSink>>,
	/// Test-only pause after persistence and before the final check and
	/// signature: `(reached, release)`.
	#[cfg(test)]
	pub(crate) before_sign: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

/// A stored row's content commitment, from its stored bytes: the row projected
/// to the descriptor's columns, encoded canonically.
pub(crate) fn stored_row_digest(
	descriptor: &TableSchemaDescriptor,
	stored: &RecordBatch,
) -> Result<[u8; 32], String> {
	let row = stored_row_projected(descriptor, stored)?;
	let canonical = crate::storage::content_hash::encode_row_canonical(descriptor, &row, 0)
		.map_err(|error| format!("{error:?}"))?;
	Ok(row_digest(
		descriptor.table_object_id,
		descriptor.schema_generation,
		canonical.as_slice(),
	))
}

/// A stored row projected to the descriptor's columns: the user row it was
/// written as.
pub(crate) fn stored_row_projected(
	descriptor: &TableSchemaDescriptor,
	stored: &RecordBatch,
) -> Result<RecordBatch, String> {
	let schema = Arc::new(
		descriptor
			.arrow_schema()
			.map_err(|error| error.to_string())?,
	);
	let columns = schema
		.fields()
		.iter()
		.map(|field| {
			stored
				.column_by_name(field.name())
				.cloned()
				.ok_or_else(|| format!("stored row lacks column {}", field.name()))
		})
		.collect::<Result<Vec<_>, _>>()?;
	RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}

/// The proof an acknowledgement attests: every manifest version is stored, every
/// stored copy of it carries the manifest's content, and every version is owned
/// by this `(rsv, ordinal)`.
pub(crate) async fn verify_owned_rows(
	slab: &SlabManager,
	index: &crate::storage::index::PersistentLocalIndex,
	intent: &crate::storage::index::parent_intents::ParentIntent,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), ChildRefusal> {
	verify_owned_rows_waiting(slab, index, intent, descriptor, RetirementWait::NONE).await
}

/// One owned version's stored rows. With a wait, a holder's own read, whose location-list
/// admission waits out a logged retirement fence until the wait's end (wendelon ADR 0126 D3).
async fn stored_rows(
	slab: &SlabManager,
	tenant: TenantId,
	version: &crate::storage::index::parent_intents::OwnedVersion,
	wait: RetirementWait,
) -> std::io::Result<Vec<RecordBatch>> {
	let key = Identity::new(version.key_identity);
	if wait == RetirementWait::NONE {
		slab.stored_rows_at_version(tenant, key, version.hlc, None)
			.await
	} else {
		slab.stored_rows_at_version_for_holder(tenant, key, version.hlc, wait)
			.await
	}
}

/// [`verify_owned_rows`], its stored-row reads waiting out a logged retirement fence until
/// `wait`'s end (wendelon ADR 0126; a holder's own child only).
pub(crate) async fn verify_owned_rows_waiting(
	slab: &SlabManager,
	index: &crate::storage::index::PersistentLocalIndex,
	intent: &crate::storage::index::parent_intents::ParentIntent,
	descriptor: &TableSchemaDescriptor,
	wait: RetirementWait,
) -> Result<(), ChildRefusal> {
	let unverified = |detail: String| ChildRefusal::Unverified(detail);
	for version in &intent.manifest {
		// ADR 0699 S4b: a typed tombstone is proved by its stored tombstone
		// locations and owner, never by the absence of a row.
		if super::deletes::is_tombstone_version(descriptor, version) {
			let owner = index
				.version_owner(intent.tenant, *version)
				.await
				.map_err(|error| unverified(error.to_string()))?;
			let locations = slab
				.version_locations(
					intent.tenant,
					Identity::new(version.key_identity),
					version.hlc,
				)
				.map_err(|error| unverified(error.to_string()))?;
			if owner != Some((intent.rsv, intent.ordinal))
				|| locations.is_empty()
				|| !locations.iter().all(|location| location.is_tombstone)
			{
				return Err(unverified("a tombstone is not stored as owned".into()));
			}
			continue;
		}
		let owner = index
			.version_owner(intent.tenant, *version)
			.await
			.map_err(|error| unverified(error.to_string()))?;
		if owner != Some((intent.rsv, intent.ordinal)) {
			return Err(unverified(
				"a manifest version is not owned by this child".into(),
			));
		}
		let rows = stored_rows(slab, intent.tenant, version, wait)
			.await
			.map_err(|error| unverified(error.to_string()))?;
		if rows.is_empty() {
			return Err(unverified("a manifest version is not stored".into()));
		}
		for row in &rows {
			if stored_row_digest(descriptor, row).map_err(unverified)? != version.row_digest {
				return Err(unverified("a stored copy differs from the manifest".into()));
			}
		}
	}
	Ok(())
}

/// The holder's authority, re-read under the lock: its lineage is open and a
/// live lease of the current epoch covers the child range.
async fn holder_authority(
	state_machine: &WendelonStateMachine,
	plan: &ChildPlan,
	authority: &ChildAuthority,
) -> Result<(), ChildRefusal> {
	if authority.role != ChildRole::Holder {
		return Ok(());
	}
	let holder = crate::consensus::decisions::Signer {
		node: authority.signer.signer.node,
		incarnation: plan.ack.holder_incarnation,
	};
	let lineage = plan.ack.lineage;
	let (open, epoch) = state_machine
		.read_state(move |state| {
			(
				crate::consensus::decisions::lineage_is_open(state, lineage, holder),
				state
					.epoch_history
					.last_key_value()
					.map(|(_, config)| config.epoch),
			)
		})
		.await;
	if !open {
		return Err(ChildRefusal::LineageClosed);
	}
	let (start, end) = (plan.intent.range_start, plan.intent.range_end);
	// Wendelon ADR 0127 D2: a traced local holder child reads the same lease view, captured,
	// after the same epoch and lineage read.
	let (leases, capture) = match (&authority.lease_evidence, &authority.leases) {
		(Some(sink), _) => {
			let (leases, capture) = (sink.source)((start, end)).await;
			(leases, Some(capture))
		}
		(None, Some(source)) => (source().await, None),
		(None, None) => (Vec::new(), None),
	};
	if !leases.iter().any(|(low, high, lease_epoch)| {
		Some(*lease_epoch) == epoch && *low <= start && end <= *high
	}) {
		if let (Some(sink), Some(capture)) = (&authority.lease_evidence, &capture) {
			sink.offer(super::lease_evidence::LeaseEvidence::build(
				super::lease_evidence::LeaseSite::Stripe,
				capture,
				epoch,
				(start, end),
				sink.lease_witness(),
			));
		}
		return Err(ChildRefusal::LeaseMissing);
	}
	Ok(())
}

/// The recorded decision, re-read under the lock. Only a recorded Refused
/// refuses (and marks the intent); an absent decision refuses as unknown.
async fn decision_allows(
	state_machine: &WendelonStateMachine,
	index: &crate::storage::index::PersistentLocalIndex,
	rsv: u64,
	ordinal: u16,
) -> Result<crate::consensus::decisions::ParentStatus, ChildRefusal> {
	use crate::consensus::decisions::ParentStatus;
	use crate::storage::index::parent_intents::ParentIntentState::{Complete, Intent, Refused};
	let storage = |error: std::io::Error| ChildRefusal::Storage(error.to_string());
	match parent_status(state_machine, rsv).await {
		None => Err(ChildRefusal::UnknownReservation),
		Some(ParentStatus::Refused { .. }) => {
			if index
				.parent_intent(rsv, ordinal)
				.await
				.map_err(storage)?
				.is_some()
			{
				index
					.set_parent_intent_state(rsv, ordinal, &[Intent, Complete], Refused)
					.await
					.map_err(storage)?
					.map_err(ChildRefusal::Intent)?;
			}
			Err(ChildRefusal::ParentRefused)
		}
		Some(status) => Ok(status),
	}
}

/// The storage step and the acknowledgement, as one operation under the
/// parent's lock, inside a task that keeps the lock until it finishes, so a
/// cancelled request cannot release it early. Under the lock: the decision and
/// the holder's authority are re-read; the intent is recorded; only the missing
/// versions are appended; the stored rows are verified against the manifest and
/// owners before Complete and before any retry acknowledgement; the decision is
/// read once more; and only then is the acknowledgement signed.
pub(crate) async fn persist_child(
	slab: Arc<SlabManager>,
	state_machine: WendelonStateMachine,
	locks: Arc<ParentLocks>,
	plan: AdmittedPlan,
	authority: ChildAuthority,
) -> Result<
	crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
	ChildRefusal,
> {
	persist_child_waiting(
		slab,
		state_machine,
		locks,
		plan,
		authority,
		RetirementWait::NONE,
	)
	.await
}

/// [`persist_child`], a holder's own reads waiting out a logged retirement fence until `wait`'s
/// end (wendelon ADR 0126 D3). Every other role ignores the wait.
pub(crate) async fn persist_child_waiting(
	slab: Arc<SlabManager>,
	state_machine: WendelonStateMachine,
	locks: Arc<ParentLocks>,
	plan: AdmittedPlan,
	authority: ChildAuthority,
	wait: RetirementWait,
) -> Result<
	crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
	ChildRefusal,
> {
	if authority.role == ChildRole::Repair {
		return Err(ChildRefusal::WrongSender);
	}
	persist_locked(slab, state_machine, locks, plan, authority, wait)
		.await?
		.ok_or(ChildRefusal::WrongSender)
}

/// The persistence of [`persist_child`], shared with the typed repair, which
/// signs nothing.
pub(super) async fn persist_locked(
	slab: Arc<SlabManager>,
	state_machine: WendelonStateMachine,
	locks: Arc<ParentLocks>,
	admitted: AdmittedPlan,
	authority: ChildAuthority,
	wait: RetirementWait,
) -> Result<
	Option<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>>,
	ChildRefusal,
> {
	use crate::consensus::decisions::ParentStatus;
	use crate::storage::index::parent_intents::ParentIntentState::{Complete, Intent, Refused};
	let AdmittedPlan { plan, admission } = admitted;
	// Wendelon ADR 0126 D3: only a holder's own child waits; every other role reads as before.
	let wait = if authority.role == ChildRole::Holder {
		wait
	} else {
		RetirementWait::NONE
	};
	// Lock order: the index admission guard (taken with the plan), then the
	// parent lock. Both move into the spawned work, which a cancelled caller
	// does not stop, so neither is released before that work ends.
	#[cfg(test)]
	registry_probe::note(slab.data_dir(), registry_probe::Role::Persist, &locks);
	let guard = locks.lock(plan.intent.rsv).await;
	let work = tokio::spawn(async move {
		let _admission = admission;
		let _guard = guard;
		let index = slab
			.persistent_index()
			.ok_or_else(|| ChildRefusal::Storage("no persistent index".into()))?;
		let (rsv, ordinal) = (plan.intent.rsv, plan.intent.ordinal);
		let storage = |error: std::io::Error| ChildRefusal::Storage(error.to_string());
		let status = decision_allows(&state_machine, &index, rsv, ordinal).await?;
		let existing = index.parent_intent(rsv, ordinal).await.map_err(storage)?;
		let recorded_complete = existing.as_ref().is_some_and(|existing| {
			existing.state == Complete && existing.manifest == plan.intent.manifest
		});
		// A repair re-checks a complete intent's stored versions and appends any
		// that are missing; every other role takes a complete intent as written.
		let complete = recorded_complete && authority.role != ChildRole::Repair;
		let accepted = matches!(status, ParentStatus::Accepted { .. });
		// A fresh write of an Accepted child is refused; only the typed repair
		// of an Accepted child persists one, and repair needs Accepted.
		if authority.role == ChildRole::Repair && !accepted {
			return Err(ChildRefusal::NotAccepted);
		}
		if accepted && !complete && authority.role != ChildRole::Repair {
			return Err(ChildRefusal::ParentAccepted);
		}
		if !complete {
			holder_authority(&state_machine, &plan, &authority).await?;
			// Every record lands inside the child range before anything is recorded.
			if let Some(targets) = &plan.tombstone {
				if targets.iter().any(|target| {
					crate::consensus::decisions::delete_row_position(
						plan.intent.tenant,
						&plan.descriptor,
						target.row,
					)
					.is_none_or(|position| {
						position < plan.intent.range_start || position > plan.intent.range_end
					})
				}) {
					return Err(ChildRefusal::OutOfRange);
				}
			} else {
				let all: Vec<u32> = (0..plan.batch.num_rows() as u32).collect();
				let full = bifurcate_child(&slab, &plan, &plan.batch, &all)?;
				if full.children.iter().any(|record| {
					let position = record_position(record);
					position < plan.intent.range_start || position > plan.intent.range_end
				}) {
					return Err(ChildRefusal::OutOfRange);
				}
			}
			// DC-269 (test only): a pause after the last eligibility and authority
			// checks, immediately before the intent is begun.
			#[cfg(test)]
			let probe = persist_pause::before_parent_intent(&slab, plan.descriptor.table_object_id).await;
			let begun = index.begin_parent_intent(plan.intent.clone()).await;
			#[cfg(test)]
			if let Some(probe) = probe {
				probe.record(&plan.intent.manifest, &begun);
			}
			match begun.map_err(storage)?.map_err(ChildRefusal::Intent)? {
				Refused => return Err(ChildRefusal::ParentRefused),
				Intent | Complete => {}
			}
			// DROP design (test only): a pause after a successful begin, before any
			// append; the guard signals when this spawned work ends.
			#[cfg(test)]
			let _ended = persist_pause::after_parent_intent(
				&slab,
				plan.descriptor.table_object_id,
				&plan.intent.manifest,
			)
			.await;
			// Each tombstone is appended once: only when no copy of it is stored.
			for target in plan.tombstone.iter().flatten() {
				let stored = slab
					.version_locations(plan.intent.tenant, target.row, plan.ack_hlc())
					.map_err(storage)?;
				if stored.is_empty() {
					super::deletes::append_tombstone(
						&slab,
						plan.intent.tenant,
						&plan.descriptor,
						target,
						plan.ack_hlc(),
						&plan.secondary_indexes,
					)
					.await
					.map_err(storage)?;
				}
			}
			// Append only the rows whose version has no stored copy yet.
			let mut missing = Vec::new();
			for (row, version) in plan
				.row_versions
				.iter()
				.enumerate()
				.filter(|_| plan.tombstone.is_none())
			{
				let stored = stored_rows(&slab, plan.intent.tenant, version, wait)
					.await
					.map_err(storage)?;
				if stored.is_empty() {
					missing.push(row as u32);
				}
			}
			if !missing.is_empty() {
				let rows = arrow::compute::take_record_batch(
					&plan.batch,
					&arrow::array::UInt32Array::from(missing.clone()),
				)
				.map_err(|error| ChildRefusal::Storage(error.to_string()))?;
				let bifurcated = bifurcate_child(&slab, &plan, &rows, &missing)?;
				append_table_children_durable(
					&slab,
					plan.intent.tenant,
					plan.ack_hlc(),
					&to_replica_children(&bifurcated),
					&plan.secondary_indexes,
				)
				.await
				.map_err(storage)?;
			}
		}
		// The proof the acknowledgement attests, for a new and a retried Complete.
		verify_owned_rows_waiting(&slab, &index, &plan.intent, &plan.descriptor, wait).await?;
		if !recorded_complete {
			index
				.set_parent_intent_state(rsv, ordinal, &[Intent], Complete)
				.await
				.map_err(storage)?
				.map_err(ChildRefusal::Intent)?;
		}
		#[cfg(test)]
		if let Some((reached, release)) = &authority.before_sign {
			reached.notify_one();
			release.notified().await;
		}
		// No acknowledgement after an observed refusal.
		decision_allows(&state_machine, &index, rsv, ordinal).await?;
		Ok((authority.role != ChildRole::Repair).then(|| {
			crate::consensus::decisions::evidence::sign(
				&authority.signer.key,
				plan.ack.clone(),
				authority.signer.signer,
			)
		}))
	});
	work.await
		.map_err(|error| ChildRefusal::Storage(error.to_string()))?
}

/// Marks every intent whose parent is recorded Refused, under each parent's
/// lock. An intent whose decision is absent or Reserved is left alone.
pub(crate) async fn refuse_recorded_intents(
	slab: &SlabManager,
	state_machine: &WendelonStateMachine,
	locks: &ParentLocks,
) -> std::io::Result<usize> {
	use crate::consensus::decisions::ParentStatus;
	use crate::storage::index::parent_intents::ParentIntentState::{Complete, Intent, Refused};
	let Some(index) = slab.persistent_index() else {
		return Ok(0);
	};
	let mut refused = 0;
	for (rsv, ordinal) in index.parent_intents_in(&[Intent, Complete]).await? {
		let _guard = locks.lock(rsv).await;
		if matches!(
			parent_status(state_machine, rsv).await,
			Some(ParentStatus::Refused { .. })
		) && index
			.set_parent_intent_state(rsv, ordinal, &[Intent, Complete], Refused)
			.await?
			.is_ok()
		{
			refused += 1;
		}
	}
	Ok(refused)
}

/// The data server's decision state: parent locks, and in this mode the
/// store's signer. One field in the bounded root.
#[derive(Clone)]
pub(crate) struct DecisionPeer {
	pub(crate) locks: Arc<ParentLocks>,
	pub(crate) signer: Option<Arc<DecisionSigner>>,
	/// ADR 0699 S3c: this node's path to the leader for decisions.
	pub(crate) proposals: Option<Arc<crate::network::decision_proposals::DecisionProposals>>,
	/// ADR 0699 S5a: set at close; the maintenance loop stops between steps,
	/// so no lifecycle permit is dropped mid-step by an orderly close.
	pub(crate) maintenance_stop: Arc<tokio::sync::watch::Sender<bool>>,
	/// ADR 0699 S5a: the generation whose staged candidate this run built, or
	/// rebuilt and matched, with its proofs (0: none). Activating any other
	/// candidate does not inherit this start's validation.
	pub(crate) built_gid: Arc<std::sync::atomic::AtomicU64>,
	/// Wendelon ADR 0123 (L2/L3): this store's certification work in this process.
	pub(crate) certification_work: Arc<super::certification::CertificationWork>,
	/// ADR 0699 S5b: the last sealed slab a cleanup step visited, and the
	/// debt sweep's position.
	pub(crate) cleanup_cursor: Arc<std::sync::atomic::AtomicU64>,
	/// Sealed slabs found debt-free, with the active generation then: new
	/// debt appears only at an activation, so they are skipped until one.
	pub(crate) debt_free: Arc<std::sync::Mutex<std::collections::BTreeMap<u64, u64>>>,
	pub(crate) debt_cursor:
		Arc<std::sync::Mutex<Option<crate::storage::index::certified_base::VersionKey>>>,
	/// ADR 0699 S5a: a committed generation has no matching staged candidate
	/// here; the store refuses to serve until an operator decides (no reset).
	pub(crate) behind_boundary: Arc<std::sync::atomic::AtomicBool>,
	/// ADR 0699 S5a: the certified base validated since this start. Reads
	/// refuse until it is.
	pub(crate) base_validated: Arc<std::sync::atomic::AtomicBool>,
	/// DC-281: bumped after every local activation attempt. A read whose authority
	/// meets this store's own lag in activating the committed generation waits on
	/// it, then rechecks its whole authority.
	pub(crate) activations: tokio::sync::watch::Sender<u64>,
	/// Wendelon ADR 0121 D1: this store's configured retention window (ms; 0 is
	/// none). It serves and signs only while it equals the replicated binding.
	pub(crate) retention_window_ms: u64,
	/// DC-287: this node's opt-in record of stalled coordinated writes (none by default; see
	/// [`super::WendelonDataServer::with_write_stall_trace`]).
	pub(crate) write_stall_trace: Option<Arc<super::write_stall_trace::WriteStallTrace>>,
	/// Test-only: how many more read-authority checks pass before every
	/// later one fails.
	#[cfg(test)]
	pub(crate) read_checks_left: Arc<std::sync::atomic::AtomicUsize>,
	/// Test-only faults: refuse child replicates, refuse fetches, or alter the
	/// first row of a served fetch.
	#[cfg(test)]
	pub(crate) refuse_replicas: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (DC-287): refuse every holder child write here, so a coordinator sees its own
	/// holder refuse.
	#[cfg(test)]
	pub(crate) refuse_holders: Arc<std::sync::atomic::AtomicBool>,
	#[cfg(test)]
	pub(crate) refuse_fetches: Arc<std::sync::atomic::AtomicBool>,
	#[cfg(test)]
	pub(crate) tamper_fetches: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: leave a committed generation staged, the crash row between
	/// Commit applied and local activation.
	#[cfg(test)]
	pub(crate) hold_activation: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (DC-281): every read-authority check waits while set.
	#[cfg(test)]
	pub(crate) hold_read_checks: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (DC-281): how many checks reached the hold above.
	#[cfg(test)]
	pub(crate) held_read_checks_reached: Arc<std::sync::atomic::AtomicUsize>,
	/// Test-only (DC-281): admissions here are recorded only while set (the
	/// fixture that observes them sets it).
	#[cfg(test)]
	pub(crate) observe_activation_waits: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (DC-281): what table-query admissions observed while waiting for
	/// this store's activation, and a wakeup per event.
	#[cfg(test)]
	pub(crate) activation_waits: Arc<std::sync::Mutex<Vec<super::reader::ActivationWaitEvent>>>,
	#[cfg(test)]
	pub(crate) activation_wait_seen: Arc<tokio::sync::Notify>,
	/// Test-only (DC-271): debts the sweep left because their producer's
	/// reservation stripe was busy.
	#[cfg(test)]
	pub(crate) debt_busy_skips: Arc<std::sync::atomic::AtomicUsize>,
	/// Test-only: base validation fails, as a physical mismatch would.
	#[cfg(test)]
	pub(crate) fail_base_validation: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: the next cleanup copy read fails once, as a transient read
	/// error would.
	#[cfg(test)]
	pub(crate) fail_copy_read_once: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (ADR 0703): this store's local certified cleanup and its own
	/// LSM step are paused, so a test drives certified LSM itself.
	#[cfg(test)]
	pub(crate) pause_cleanup: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (wendelon ADR 0118 interval controls): this store does not build,
	/// stage or report a pending generation, so no target can commit while it is
	/// held; activation of committed generations, writes and reads continue.
	#[cfg(test)]
	pub(crate) hold_certification: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (DC-296): the next signed report is not proposed (once), so its candidate is
	/// reused, unbuilt and unstaged, by the next pass.
	#[cfg(test)]
	pub(crate) skip_next_report: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (ADR 0706): only the local certified cleanup step is paused,
	/// so a held certified LSM job is the only work on its slabs.
	#[cfg(test)]
	pub(crate) pause_local_cleanup: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (ADR 0729): this store asks for no leases, as a holder that is
	/// running and cut off from the leader would not get any.
	#[cfg(test)]
	pub(crate) pause_lease_refresh: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (ADR 0729): this store proposes no takeover, as a candidate
	/// that cannot act.
	#[cfg(test)]
	pub(crate) pause_takeover: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (ADR 0734): when set, this store's own lease batch, once it HAS been
	/// committed and applied, signals the first notifier and then waits on the second before
	/// it returns its answer: a grant that applied with its answer withheld.
	#[cfg(test)]
	pub(crate) hold_lease_answer:
		Arc<std::sync::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
	/// Test-only (wendelon ADR 0127): the next own lease batch names a generation its lineage
	/// does not have, so the leader's apply denies it per target (a valid results vector).
	#[cfg(test)]
	pub(crate) skew_lease_generation: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (wendelon ADR 0127): the next own lease batch's answer, after its real apply,
	/// is treated as a results vector that does not match its leases.
	#[cfg(test)]
	pub(crate) malformed_lease_results: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: a KV key read as if at another position, the seam for a
	/// different key at the same position.
	#[cfg(test)]
	pub(crate) kv_position_override: Arc<std::sync::Mutex<Option<(Vec<u8>, u64)>>>,
	/// Test-only pause points in the coordinator, taken once each, in order.
	#[cfg(test)]
	pub(crate) pauses: Arc<
		std::sync::Mutex<
			std::collections::VecDeque<(
				PauseStage,
				Arc<tokio::sync::Notify>,
				Arc<tokio::sync::Notify>,
			)>,
		>,
	>,
	/// Test-only (ADR 0706): what each held certified step returned.
	#[cfg(test)]
	pub(crate) held_outcomes: Arc<std::sync::Mutex<Vec<HeldStepOutcome>>>,
	/// Test-only (DC-207): this store's handling of one child frame, held.
	#[cfg(test)]
	pub(crate) child_hold: Arc<std::sync::Mutex<Option<ChildHold>>>,
}

/// Test-only (DC-207): holds this store's handling of the next child frame of
/// `role` from `sender`, once, after it decoded and before anything is
/// written: `reached` is notified with its metadata recorded in `held`, and the
/// handler waits for `release`.
#[cfg(test)]
pub(crate) struct ChildHold {
	pub(crate) sender: u32,
	pub(crate) role: ChildRole,
	pub(crate) reached: Arc<tokio::sync::Notify>,
	pub(crate) release: Arc<tokio::sync::Notify>,
	pub(crate) held: Arc<std::sync::Mutex<Option<ChildWriteMeta>>>,
}

/// Where a test may pause a coordinated write.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PauseStage {
	/// After the lookup found the key absent and eligible.
	AfterAbsent,
	/// Wendelon ADR 0120: a CTAS chunk after its coordinator saw the chain
	/// dispatching and built its claim, before its Reserve.
	AfterCtasPhase,
	/// After a Reserve answer, before the record is read again.
	AfterReserve,
	/// After the children are written, before Finalize.
	BeforeFinalize,
	/// In a reader, after its cut is applied and before its inventory.
	ReaderAfterCut,
	/// In a reader, before it repairs a child it lacks.
	ReaderBeforeRepair,
	/// In a delete, after its cut and before its evaluation.
	DeleteEvaluation,
	/// In a certification step, holding its lifecycle permit.
	CertificationStep,
	/// ADR 0707: in a certification step, after its candidate is durably
	/// staged and its report signed, before the report is proposed.
	CertificationBeforeReport,
	/// DC-296: in a certification step whose same-gid candidate this process already built
	/// (neither built nor staged again), before its staged readback.
	CertificationReusedReadback,
	/// In a cleanup step, after its plan reclaims something and before the
	/// rewrite.
	CleanupBeforeRewrite,
	/// ADR 0706: in the leader's certified tier step, holding its lifecycle
	/// permit, before its work.
	CertifiedTierStep,
	/// ADR 0706: in the leader's certified LSM step, holding its lifecycle
	/// permit, before its work.
	CertifiedLsmStep,
}

/// Test-only (ADR 0706): the terminal return of a certified step that a test
/// held, recorded after its permit completed.
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) enum HeldStepOutcome {
	Tier(Result<crate::storage::manager::certified_tier::CertifiedTierStep, String>),
	Lsm(Result<Option<crate::storage::CompactionOutcome>, String>),
}

/// Test-only (DC-281).
#[cfg(test)]
impl DecisionPeer {
	/// Records what a table-query admission observed while waiting for this store's
	/// activation, and wakes a test waiting on it.
	pub(crate) fn record_activation_wait(&self, event: super::reader::ActivationWaitEvent) {
		if !self
			.observe_activation_waits
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return;
		}
		self.activation_waits.lock().unwrap().push(event);
		self.activation_wait_seen.notify_one();
	}
}

#[cfg(test)]
impl DecisionPeer {
	/// Pauses here if the next queued pause is for this stage; whether it
	/// did.
	pub(crate) async fn pause_at(&self, stage: PauseStage) -> bool {
		let next = {
			let mut queue = self.pauses.lock().unwrap();
			match queue.front() {
				Some((at, _, _)) if *at == stage => queue.pop_front(),
				_ => None,
			}
		};
		match next {
			Some((_, reached, release)) => {
				reached.notify_one();
				release.notified().await;
				true
			}
			None => false,
		}
	}

	/// Test-only (DC-207): queues a hold of one child frame (see [`ChildHold`]).
	pub(crate) fn hold_child_for_test(&self, hold: ChildHold) {
		*self.child_hold.lock().unwrap() = Some(hold);
	}

	/// Test-only (DC-207): a child frame's handler pauses here if a queued hold
	/// matches it.
	async fn pause_child(&self, sender: u32, role: ChildRole, meta: &ChildWriteMeta) {
		let hold = {
			let mut slot = self.child_hold.lock().unwrap();
			match slot.as_ref() {
				Some(hold) if hold.sender == sender && hold.role == role => slot.take(),
				_ => None,
			}
		};
		if let Some(hold) = hold {
			*hold.held.lock().unwrap() = Some(meta.clone());
			hold.reached.notify_one();
			hold.release.notified().await;
		}
	}
}

impl Default for DecisionPeer {
	fn default() -> Self {
		Self {
			locks: Arc::new(ParentLocks::new()),
			signer: None,
			proposals: None,
			maintenance_stop: Arc::new(tokio::sync::watch::channel(false).0),
			built_gid: Default::default(),
			certification_work: Default::default(),
			cleanup_cursor: Default::default(),
			debt_free: Default::default(),
			debt_cursor: Default::default(),
			#[cfg(test)]
			pauses: Default::default(),
			#[cfg(test)]
			held_outcomes: Default::default(),
			#[cfg(test)]
			child_hold: Default::default(),
			behind_boundary: Default::default(),
			base_validated: Default::default(),
			activations: tokio::sync::watch::channel(0).0,
			retention_window_ms: 0,
			write_stall_trace: None,
			#[cfg(test)]
			read_checks_left: Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX)),
			#[cfg(test)]
			refuse_replicas: Default::default(),
			#[cfg(test)]
			refuse_holders: Default::default(),
			#[cfg(test)]
			refuse_fetches: Default::default(),
			#[cfg(test)]
			tamper_fetches: Default::default(),
			#[cfg(test)]
			hold_activation: Default::default(),
			#[cfg(test)]
			hold_read_checks: Default::default(),
			#[cfg(test)]
			held_read_checks_reached: Default::default(),
			#[cfg(test)]
			observe_activation_waits: Default::default(),
			#[cfg(test)]
			activation_waits: Default::default(),
			#[cfg(test)]
			activation_wait_seen: Default::default(),
			#[cfg(test)]
			debt_busy_skips: Default::default(),
			#[cfg(test)]
			fail_base_validation: Default::default(),
			#[cfg(test)]
			fail_copy_read_once: Default::default(),
			#[cfg(test)]
			pause_cleanup: Default::default(),
			#[cfg(test)]
			hold_certification: Default::default(),
			#[cfg(test)]
			skip_next_report: Default::default(),
			#[cfg(test)]
			pause_local_cleanup: Default::default(),
			#[cfg(test)]
			pause_lease_refresh: Default::default(),
			#[cfg(test)]
			pause_takeover: Default::default(),
			#[cfg(test)]
			hold_lease_answer: Default::default(),
			#[cfg(test)]
			skew_lease_generation: Default::default(),
			#[cfg(test)]
			malformed_lease_results: Default::default(),
			#[cfg(test)]
			kv_position_override: Default::default(),
		}
	}
}

impl DecisionPeer {
	/// ADR 0700: whether `msg_type` passes this mode's service restriction.
	/// Exactly the checkpoint's surfaces are served: a table write and its
	/// child frames, a streamed table query, a single-key KV write and read, and
	/// a single-row delete. Each is then served only in its supported shape (the
	/// reader, the coordinator and the delete path refuse the rest). Every
	/// other data message (migration, bulk, counters, frames, other mutation
	/// paths) stays refused. Wendelon ADR 0113: a public mutation-outcome resolve
	/// is served from the replicated decisions, and an outcome pin or acknowledge
	/// answers a typed unsupported. The cancellation of an in-flight table query is
	/// served by its existing authorized handler and registry.
	pub(crate) fn opens(&self, msg_type: u8) -> bool {
		const SERVED: [MsgType; 13] = [
			MsgType::TABLE_QUERY_CANCEL_REQ,
			MsgType::MUTATION_OUTCOME_REQ,
			MsgType::MUTATION_OUTCOME_PIN_REQ,
			MsgType::TABLE_BASE_FETCH_REQ,
			MsgType::TABLE_WRITE_REQ,
			MsgType::TABLE_CHILD_WRITE_REQ,
			MsgType::TABLE_CHILD_REPLICATE_REQ,
			MsgType::TABLE_CHILD_FETCH_REQ,
			MsgType::TABLE_QUERY_REQ,
			MsgType::WRITE_REQ,
			MsgType::READ_REQ,
			MsgType::TABLE_DELETE_REQ,
			// Wendelon ADR 0120 D6.
			MsgType::TABLE_CTAS_CHUNK_WRITE_REQ,
		];
		SERVED.iter().any(|served| served.0 == msg_type)
	}
}

/// This store's decision signer: its key and the incarnation it signs as.
pub(crate) struct DecisionSigner {
	pub(crate) key: ed25519_dalek::SigningKey,
	pub(crate) signer: crate::consensus::decisions::Signer,
}

/// Bounds checked before any sender-sized allocation is decoded.
pub(crate) const MAX_CHILD_META_BYTES: usize = 4096;
pub(crate) const MAX_CHILD_ROWS_BYTES: usize = 64 << 20;
/// How long a receiver waits for its applied state to reach the reservation.
const CATCH_UP: std::time::Duration = std::time::Duration::from_secs(2);

/// What a child write answers: the signed acknowledgements gathered (the
/// holder's own first, then its plan members'), or why it was refused.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ChildWriteResponse {
	Acked(Vec<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>>),
	Refused(String),
}

/// Decodes a child write frame, bounding both parts before decoding them.
pub(crate) fn decode_child_frame(
	meta: &[u8],
	rows: Option<&[u8]>,
) -> Result<(ChildWriteMeta, RecordBatch), String> {
	if meta.len() > MAX_CHILD_META_BYTES {
		return Err("child write meta exceeds its bound".into());
	}
	let rows = rows.ok_or("child write has no rows")?;
	if rows.len() > MAX_CHILD_ROWS_BYTES {
		return Err("child write rows exceed their bound".into());
	}
	let meta = postcard::from_bytes(meta).map_err(|error| error.to_string())?;
	let batch = crate::storage::multirow::decode_batch(rows).map_err(|error| error.to_string())?;
	if batch.num_rows() == 0
		|| batch.num_rows() > crate::consensus::decisions::MAX_ROWS_PER_CHILD as usize
	{
		return Err("child write row count out of bounds".into());
	}
	Ok((meta, batch))
}

impl WendelonDataServer {
	pub(crate) fn with_decision_mode(
		mut self,
		decision_mode: crate::consensus::decision_mode::DecisionMode,
	) -> Self {
		self.decision_mode = decision_mode;
		// ADR 0699 S4: no rewrite drops a stored version in this mode until S5.
		if decision_mode == crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			self.slab.contain_version_reclamation();
		}
		self
	}

	pub(crate) fn with_decision_signer(mut self, signer: Option<Arc<DecisionSigner>>) -> Self {
		self.decisions.signer = signer;
		self
	}

	/// Wendelon ADR 0121 D1: the configured ParentDecisions retention window.
	pub(crate) fn with_retention_window_ms(mut self, window_ms: u64) -> Self {
		self.decisions.retention_window_ms = window_ms;
		self
	}

	/// ADR 0725: what this store signs and sends its own slab operations with,
	/// in ParentDecisions mode. `None` under today's authority.
	pub(crate) fn owner_authority(&self) -> Option<crate::storage::OwnerAuthority> {
		let signer = self.decisions.signer.as_ref()?;
		let proposals = self.decisions.proposals.clone()?;
		Some(crate::storage::OwnerAuthority::new(
			signer.key.clone(),
			proposals,
		))
	}

	/// Test-only: the node's decision proposals.
	#[cfg(test)]
	pub(crate) fn decision_proposals_for_test(
		&self,
	) -> Arc<crate::network::decision_proposals::DecisionProposals> {
		self.decisions
			.proposals
			.clone()
			.expect("a decision mode node")
	}

	/// Test-only: the store's decision signer identity.
	#[cfg(test)]
	pub(crate) fn decision_signer_for_test(&self) -> crate::consensus::decisions::Signer {
		self.decisions
			.signer
			.as_ref()
			.expect("a decision mode node")
			.signer
	}

	/// Test-only: the decision peer's fault switches and counters.
	#[cfg(test)]
	pub(crate) fn decision_peer_for_test(&self) -> &DecisionPeer {
		&self.decisions
	}

	/// Test-only (wendelon ADR 0120): pauses or resumes this node's CTAS
	/// maintenance.
	#[cfg(test)]
	pub(crate) fn pause_ctas_maintenance_for_test(&self, paused: bool) {
		if let Some(proposals) = &self.decisions.proposals {
			proposals
				.ctas_maintenance_paused
				.store(paused, std::sync::atomic::Ordering::Release);
		}
	}

	/// Test-only (wendelon ADR 0123 L2/L3): this store's certification work: (builds,
	/// base entries loaded, staged rows, activations, activation rows).
	#[cfg(test)]
	pub(crate) fn certification_work_for_test(&self) -> [u64; 5] {
		self.decisions.certification_work.snapshot()
	}

	/// Test-only (wendelon ADR 0123): this node's recent proposals (gid, target digest,
	/// reason).
	#[cfg(test)]
	pub(crate) fn proposed_trace_for_test(
		&self,
	) -> Vec<(
		u64,
		[u8; 32],
		crate::consensus::decisions::generations::TargetReason,
	)> {
		self.decisions
			.proposals
			.as_ref()
			.map(|proposals| proposals.proposed_trace())
			.unwrap_or_default()
	}

	/// Test-only (wendelon ADR 0123 L2): this node's proposals as leader, by reason.
	#[cfg(test)]
	pub(crate) fn proposed_counts_for_test(&self) -> std::collections::BTreeMap<&'static str, u64> {
		self.decisions
			.proposals
			.as_ref()
			.map(|proposals| proposals.proposed_counts())
			.unwrap_or_default()
	}

	/// Test-only (wendelon ADR 0123 D7): the reason this node proposed target `gid` with.
	#[cfg(test)]
	pub(crate) fn proposed_reason_for_test(
		&self,
		gid: u64,
	) -> Option<crate::consensus::decisions::generations::TargetReason> {
		self.decisions.proposals.as_ref()?.proposed_reason(gid)
	}

	/// Test-only (wendelon ADR 0122): pauses or resumes this node's generation steps
	/// (targets and commits).
	#[cfg(test)]
	pub(crate) fn pause_generations_for_test(&self, paused: bool) {
		if let Some(proposals) = &self.decisions.proposals {
			proposals
				.generations_paused
				.store(paused, std::sync::atomic::Ordering::Release);
		}
	}

	/// Test-only: the lease ranges this node holds now, as a holder sees them
	/// (`(low, high, epoch)`).
	#[cfg(test)]
	pub(crate) async fn held_leases_for_test(&self) -> Vec<(u64, u64, u64)> {
		self.lease_source()().await
	}

	/// Test-only: drops every pause queued at this node, reached or not.
	#[cfg(test)]
	pub(crate) fn clear_pauses_for_test(&self) {
		self.decisions.pauses.lock().unwrap().clear();
	}

	/// Test-only: queue a pause in this node's coordinator.
	#[cfg(test)]
	pub(crate) fn pause_coordinator_for_test(
		&self,
		stage: PauseStage,
	) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
		let (reached, release) = (
			Arc::new(tokio::sync::Notify::new()),
			Arc::new(tokio::sync::Notify::new()),
		);
		self.decisions
			.pauses
			.lock()
			.unwrap()
			.push_back((stage, reached.clone(), release.clone()));
		(reached, release)
	}

	pub(crate) fn with_decision_proposals(
		mut self,
		proposals: Option<Arc<crate::network::decision_proposals::DecisionProposals>>,
	) -> Self {
		self.decisions.proposals = proposals;
		self
	}

	/// ADR 0699 S3c: marks intents whose parent is recorded Refused.
	pub(crate) async fn sweep_refused_intents(&self) -> std::io::Result<usize> {
		refuse_recorded_intents(&self.slab, &self.state_machine, &self.decisions.locks).await
	}

	/// The holder's write of its child, then its plan's acknowledgements.
	pub(super) async fn holder_write(
		&self,
		sender: u32,
		meta: ChildWriteMeta,
		batch: RecordBatch,
		rows: Arc<Vec<u8>>,
		trailing: Arc<Vec<u8>>,
		wait: RetirementWait,
		lease_evidence: Option<Arc<super::lease_evidence::LeaseEvidenceSink>>,
	) -> Result<
		Vec<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>>,
		ChildRefusal,
	> {
		let given = GivenKeys::of_section(&trailing);
		match self
			.child_write_keyed(
				sender,
				ChildRole::Holder,
				meta.clone(),
				batch,
				given,
				wait,
				lease_evidence,
			)
			.await
		{
			Ok(own) => {
				let mut acks = vec![own];
				acks.extend(self.fan_out_child(&meta, rows, trailing).await);
				Ok(acks)
			}
			Err(refusal) => {
				#[cfg(test)]
				eprintln!("COORD holder write: {refusal:?}");
				Err(refusal)
			}
		}
	}

	/// ADR 0699 S5a: the decision maintenance loop's stop signal.
	pub(crate) fn decision_maintenance_stop(&self) -> tokio::sync::watch::Receiver<bool> {
		self.decisions.maintenance_stop.subscribe()
	}

	/// ADR 0699 S5a: asks the decision maintenance loop to stop after its
	/// current step.
	pub(crate) fn stop_decision_maintenance(&self) {
		self.decisions.maintenance_stop.send_replace(true);
	}

	/// ADR 0700: why this store cannot serve in ParentDecisions mode now, or
	/// `None`. A revocable observation, not a capability: admission and read
	/// authority are still checked at use time. It requires this store's
	/// decision signer bound at its current incarnation, and every range the
	/// current ring makes it primary for (derived from the ring, so an empty
	/// lineage set never passes) held as a lineage at that incarnation and
	/// epoch and covered by its own lease at the current epoch. It never gates
	/// the lease, recovery or control traffic that makes it true.
	pub(crate) async fn decision_readiness_reason(&self) -> Option<String> {
		if !self.decision_mode.refuses_service() {
			return None;
		}
		let Some((signer, local_key)) = self
			.decisions
			.signer
			.as_ref()
			.map(|signer| (signer.signer, signer.key.verifying_key().to_bytes()))
		else {
			return Some("DecisionSignerMissing".into());
		};
		let leases = self.lease_source()().await;
		let configured_window = self.decisions.retention_window_ms;
		self.state_machine
			.read_state(move |state| {
				use crate::consensus::decisions::LocalSignerBinding;
				let Some(decisions) = state.decisions.as_ref() else {
					return Some("DecisionStateMissing".to_string());
				};
				// Wendelon ADR 0121 D1: unbound, or bound to another window than
				// this store's configuration.
				if let Err(reason) =
					crate::consensus::decisions::retention_gate(state, configured_window)
				{
					return Some(reason.into());
				}
				// ADR 0721: a bound key that is not the one this store signs
				// with is its own reason. This is a diagnosis, not the fence:
				// evidence signed with another key is refused where it is
				// verified.
				match crate::consensus::decisions::local_signer_binding(state, signer, local_key) {
					LocalSignerBinding::NotCurrent => {
						return Some("IncarnationNotCurrent".into());
					}
					LocalSignerBinding::Unbound => {
						return Some("DecisionSignerUnbound".into());
					}
					LocalSignerBinding::Mismatch => {
						return Some("DecisionSignerMismatch".into());
					}
					LocalSignerBinding::Bound => {}
				}
				let Some((_, config)) = state.epoch_history.last_key_value() else {
					return Some("NoEpoch".into());
				};
				let ring = crate::network::ring::RingState::build(
					&crate::network::ring::membership_from_epoch_config(config),
					1,
				);
				let node = crate::schema::NodeId {
					id: signer.node,
					_reserved: 0,
				};
				let leased = |start: u64, end: u64| {
					leases.iter().any(|(low, high, epoch)| {
						*epoch == config.epoch && *low <= start && end <= *high
					})
				};
				// ADR 0729: every range this store holds must be leased. A
				// primary range with no lineage yet is not ready; one another
				// node holds is that node's to lease.
				for (start, end) in ring.primary_ranges_for(node) {
					match decisions.lineages().get(&start) {
						None => return Some("PrimaryRangeNotLeased".into()),
						Some(lineage) if lineage.holder.id != signer.node => {}
						Some(lineage) => {
							let held = lineage.range_end == end
								&& lineage.incarnation == signer.incarnation
								&& lineage.epoch == config.epoch;
							if !held || !leased(start, end) {
								return Some("PrimaryRangeNotLeased".into());
							}
						}
					}
				}
				for (start, lineage) in decisions.lineages() {
					if lineage.holder.id == signer.node
						&& (lineage.incarnation != signer.incarnation
							|| lineage.epoch != config.epoch
							|| !leased(*start, lineage.range_end))
					{
						return Some("HeldRangeNotLeased".into());
					}
				}
				None
			})
			.await
	}

	pub(crate) fn decision_mode_refuses_service(&self) -> bool {
		self.decision_mode.refuses_service()
	}

	/// ADR 0699 S5a: a store under ParentDecisions is not Ready until this
	/// start has validated its certified base.
	pub(crate) fn certified_base_unvalidated(&self) -> bool {
		self.decisions.signer.is_some()
			&& !self
				.decisions
				.base_validated
				.load(std::sync::atomic::Ordering::Acquire)
	}

	/// A child frame from an authenticated node, by its kind.
	pub(super) async fn dispatch_child_frame(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		// Wendelon ADR 0119 D6: the exact envelope version, before any decoding.
		crate::consensus::decision_mode::check_peer_exchange_version(frame.header.version)?;
		if frame.header.msg_type == MsgType::TABLE_CHILD_FETCH_REQ.0 {
			let sender = context.require_authenticated_node("TABLE_CHILD_FETCH_REQ")?;
			return self.handle_child_fetch(sender.id, frame).await;
		}
		let (role, name) = if frame.header.msg_type == MsgType::TABLE_CHILD_WRITE_REQ.0 {
			(ChildRole::Holder, "TABLE_CHILD_WRITE_REQ")
		} else {
			(ChildRole::Replica, "TABLE_CHILD_REPLICATE_REQ")
		};
		let sender = context.require_authenticated_node(name)?;
		self.handle_child_frame(sender.id, role, frame).await
	}

	/// Waits, bounded, until the applied state includes reservation `rsv`.
	pub(super) async fn await_applied(&self, rsv: u64) -> Result<(), ChildRefusal> {
		let deadline = tokio::time::Instant::now() + CATCH_UP;
		loop {
			let applied = self
				.state_machine
				.read_state(|state| state.last_applied_log_id.map(|id| id.index))
				.await;
			if applied.is_some_and(|applied| applied >= rsv) {
				return Ok(());
			}
			if tokio::time::Instant::now() >= deadline {
				return Err(ChildRefusal::NotCaughtUp);
			}
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		}
	}

	/// ADR 0700: the ranges this store holds now, `(low, high, epoch)`. In this
	/// mode the committed lease record is the authority, not the current
	/// leader's signature on a cached grant: a lineage continues across a change
	/// of issuer (`admit_lease_grant`), so an applied, unexpired grant of the
	/// current epoch to this store still covers after a failover. It is read
	/// with the current epoch in one state read; no uncommitted, cached or
	/// prospective grant counts, and the issuer's signature must still verify.
	/// It grants nothing by itself: holder admission, repair and readiness keep
	/// their lineage (generation, holder incarnation), sender and plan checks.
	fn lease_source(&self) -> LeaseSource {
		let (state_machine, clock, authority, local) = (
			self.state_machine.clone(),
			self.clock.clone(),
			self.lease_authority.clone(),
			self.local_node.id,
		);
		Arc::new(move || {
			let (state_machine, clock, authority) =
				(state_machine.clone(), clock.clone(), authority.clone());
			Box::pin(async move {
				let now_ms = clock.now_ms();
				state_machine
					.read_state(move |state| applied_leases(state, local, now_ms))
					.await
					.into_iter()
					.filter(|lease| authority.verify_lease(lease).is_ok())
					.map(|lease| (lease.range_start, lease.range_end, lease.epoch))
					.collect()
			})
		})
	}

	/// Wendelon ADR 0127 D2: [`Self::lease_source`] with its capture, for a traced node. The same
	/// sampled time, the same read and the same verification loop, in the same order; the
	/// observation's cutoff ticket is taken before the read, and the capture inside it.
	fn captured_lease_source(
		&self,
		trace: &Arc<super::write_stall_trace::WriteStallTrace>,
	) -> super::lease_evidence::CapturedLeaseSource {
		let (state_machine, clock, authority, local, trace) = (
			self.state_machine.clone(),
			self.clock.clone(),
			self.lease_authority.clone(),
			self.local_node.id,
			trace.clone(),
		);
		Arc::new(move |target| {
			let (state_machine, clock, authority, trace) = (
				state_machine.clone(),
				clock.clone(),
				authority.clone(),
				trace.clone(),
			);
			Box::pin(async move {
				let now_ms = clock.now_ms();
				let cutoff = trace.leases.observe();
				let (rows, mut capture) = state_machine
					.read_state(move |state| {
						super::lease_evidence::applied_leases_captured(
							state, local, now_ms, target, cutoff,
						)
					})
					.await;
				let mut leases = Vec::with_capacity(rows.len());
				for lease in rows {
					let verified = authority.verify_lease(&lease).is_ok();
					capture.note_verify(&lease, verified);
					if verified {
						leases.push((lease.range_start, lease.range_end, lease.epoch));
					}
				}
				(leases, capture)
			})
		})
	}

	/// Wendelon ADR 0127: a local holder child's evidence sink, when this node traces; `hint`
	/// is the child's range as the coordinator knows it (a diagnostic hint, checked at use).
	pub(super) fn lease_evidence_sink(
		&self,
		hint: (u64, u64),
	) -> Option<Arc<super::lease_evidence::LeaseEvidenceSink>> {
		let trace = self.decisions.write_stall_trace.as_ref()?;
		Some(super::lease_evidence::LeaseEvidenceSink::new(
			hint,
			self.captured_lease_source(trace),
			trace.clone(),
		))
	}

	/// One child write at this store: checked against applied state, persisted
	/// under the parent's lock, and acknowledged with this store's signature.
	/// A child write with the row keys its frame carried (wendelon ADR 0119).
	pub(crate) async fn child_write_keyed(
		&self,
		sender: u32,
		role: ChildRole,
		meta: ChildWriteMeta,
		batch: RecordBatch,
		given: GivenKeys,
		wait: RetirementWait,
		lease_evidence: Option<Arc<super::lease_evidence::LeaseEvidenceSink>>,
	) -> Result<
		crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
		ChildRefusal,
	> {
		if self.decision_mode != crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			return Err(ChildRefusal::ModeToday);
		}
		#[cfg(test)]
		if role == ChildRole::Replica
			&& self
				.decisions
				.refuse_replicas
				.load(std::sync::atomic::Ordering::Acquire)
		{
			return Err(ChildRefusal::Storage("test: replicates refused".into()));
		}
		#[cfg(test)]
		if role == ChildRole::Holder
			&& self
				.decisions
				.refuse_holders
				.load(std::sync::atomic::Ordering::Acquire)
		{
			return Err(ChildRefusal::Storage("test: holder writes refused".into()));
		}
		let signer = self
			.decisions
			.signer
			.clone()
			.ok_or(ChildRefusal::ModeToday)?;
		self.await_applied(meta.rsv).await?;
		// Wendelon ADR 0127 D2: a traced local holder child reads the same lease view, captured.
		let lease_evidence = lease_evidence.filter(|_| role == ChildRole::Holder);
		let (leases, capture) = match &lease_evidence {
			Some(sink) => {
				let (leases, capture) = (sink.source)(sink.hint).await;
				(leases, Some(capture))
			}
			None => ((self.lease_source())().await, None),
		};
		// What the coverage predicate actually checked when it refused: its range and epoch.
		let refused_check: std::sync::Mutex<Option<((u64, u64), Option<u64>)>> =
			std::sync::Mutex::new(None);
		let refused_at = &refused_check;
		let capturing = lease_evidence.is_some();
		let local = signer.signer;
		let configured_window = self.decisions.retention_window_ms;
		let plan = AdmittedPlan::admit(&self.slab, &self.state_machine, move |state| {
			// Wendelon ADR 0121 D1: a store whose configured window is not the bound
			// one acknowledges nothing.
			if let Err(reason) =
				crate::consensus::decisions::retention_gate(state, configured_window)
			{
				return Err(ChildRefusal::Storage(format!("retention: {reason}")));
			}
			let epoch = state
				.epoch_history
				.last_key_value()
				.map(|(_, config)| config.epoch);
			plan_child_keyed(
				state,
				local,
				sender,
				role,
				&meta,
				batch,
				given,
				|start, end| {
					let covered = leases.iter().any(|(low, high, lease_epoch)| {
						Some(*lease_epoch) == epoch && *low <= start && end <= *high
					});
					if !covered && capturing {
						*refused_at.lock().unwrap_or_else(|e| e.into_inner()) =
							Some(((start, end), epoch));
					}
					covered
				},
			)
		})
		.await;
		let plan = match plan {
			Err(ChildRefusal::LeaseMissing) => {
				let checked = refused_check
					.into_inner()
					.unwrap_or_else(|e| e.into_inner());
				if let (Some(sink), Some(capture), Some((actual, e_dec))) =
					(&lease_evidence, &capture, checked)
				{
					sink.offer(super::lease_evidence::LeaseEvidence::build(
						super::lease_evidence::LeaseSite::PlanAdmission,
						capture,
						e_dec,
						actual,
						sink.lease_witness(),
					));
				}
				return Err(ChildRefusal::LeaseMissing);
			}
			other => other?,
		};
		persist_child_waiting(
			self.slab.clone(),
			self.state_machine.clone(),
			self.decisions.locks.clone(),
			plan,
			ChildAuthority {
				signer,
				role,
				leases: (role == ChildRole::Holder).then(|| self.lease_source()),
				lease_evidence,
				#[cfg(test)]
				before_sign: None,
			},
			wait,
		)
		.await
	}

	/// The holder forwards the same rows to each other plan member and gathers
	/// their signed acknowledgements; failures are left for the coordinator to
	/// count, since quorum is the reducer's to judge.
	async fn fan_out_child(
		&self,
		meta: &ChildWriteMeta,
		rows: Arc<Vec<u8>>,
		trailing: Arc<Vec<u8>>,
	) -> Vec<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>> {
		let local = self.local_node.id;
		let rsv = meta.rsv;
		let ordinal = usize::from(meta.ordinal);
		let members: Vec<u32> = self
			.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.body(rsv))
					.and_then(|body| body.claim.children.get(ordinal))
					.map(|child| child.plan.iter().map(|member| member.node).collect())
					.unwrap_or_default()
			})
			.await;
		let Ok(meta_bytes) = postcard::to_stdvec(meta) else {
			return Vec::new();
		};
		// The set owns every send: each ends by its replicate deadline, and
		// dropping the set (this future cancelled) aborts the rest.
		let mut sends = tokio::task::JoinSet::new();
		for member in members.into_iter().filter(|member| *member != local) {
			sends.spawn(send_child_frame_with_trailing(
				self.factory.clone(),
				self.pool.clone(),
				NodeId {
					id: member,
					_reserved: 0,
				},
				ChildRpc::Replicate,
				meta_bytes.clone(),
				rows.clone(),
				trailing.clone(),
			));
		}
		let mut acks = Vec::new();
		while let Some(response) = sends.join_next().await {
			if let Ok(Ok(ChildWriteResponse::Acked(member_acks))) = response {
				acks.extend(member_acks);
			}
		}
		acks
	}

	/// A child write or child replicate frame from an authenticated sender.
	pub(super) async fn handle_child_frame(
		&self,
		sender: u32,
		role: ChildRole,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let rows: Option<&[u8]> = frame.arrow_payload.as_ref().map(|bytes| bytes.as_ref());
		// Wendelon ADR 0119 D5: the trailing section is kept undecoded here.
		let trailing = Arc::new(
			frame
				.trailing
				.as_ref()
				.map(|bytes| bytes.as_ref().to_vec())
				.unwrap_or_default(),
		);
		let decoded = decode_child_frame(frame.meta.as_ref(), rows);
		#[cfg(test)]
		if let Ok((meta, _)) = &decoded {
			self.decisions.pause_child(sender, role, meta).await;
		}
		let response = match decoded {
			Err(reason) => ChildWriteResponse::Refused(reason),
			Ok((meta, batch)) if role == ChildRole::Holder => {
				let rows = Arc::new(rows.unwrap_or_default().to_vec());
				match self
					.holder_write(
						sender,
						meta,
						batch,
						rows,
						trailing,
						RetirementWait::NONE,
						None,
					)
					.await
				{
					Ok(acks) => ChildWriteResponse::Acked(acks),
					Err(_) => {
						ChildWriteResponse::Refused("the holder could not write its child".into())
					}
				}
			}
			Ok((meta, batch)) => match self
				.child_write_keyed(
					sender,
					role,
					meta,
					batch,
					GivenKeys::of_section(&trailing),
					RetirementWait::NONE,
					None,
				)
				.await
			{
				Err(refusal) => ChildWriteResponse::Refused(format!("{refusal:?}")),
				Ok(own) => ChildWriteResponse::Acked(vec![own]),
			},
		};
		let msg_type = match role {
			ChildRole::Holder => MsgType::TABLE_CHILD_WRITE_RESP,
			ChildRole::Replica => MsgType::TABLE_CHILD_REPLICATE_RESP,
			ChildRole::Repair => MsgType::TABLE_CHILD_FETCH_RESP,
		};
		let bytes = postcard::to_stdvec(&response).map_err(Error::other)?;
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(&bytes);
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
				msg_type: msg_type.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}
}

/// The bound on one coordinator-to-holder child write attempt, from before
/// connecting until the reply is decoded. A design value, not a guarantee:
/// the holder's work in it includes receiving and decoding the rows, waiting
/// for the reservation to apply (nominally up to 2 s), local persistence and
/// locking, its own fan-out (each send bounded by
/// [`CHILD_REPLICATE_DEADLINE`]) and the reply. Persistence and locks have no
/// demonstrated hard bound, so the margin over the replicate budget is
/// headroom, not a promise that a healthy holder's answer arrives in time.
pub(crate) const CHILD_WRITE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// The bound on one holder-to-plan-member replicate attempt, from before
/// connecting until the reply is decoded. A design value, not a guarantee:
/// a receiver accepts up to [`MAX_CHILD_ROWS_BYTES`] of rows (the sender is
/// bounded by rows per child, not bytes), and such a child needs at least
/// 12.8 MiB/s of payload throughput to fit before any other latency. No
/// maximum-size performance is claimed.
pub(crate) const CHILD_REPLICATE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// Which remote child RPC a send is: its request and reply types and its
/// budget follow from it, so no message type falls back to a default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChildRpc {
	/// A coordinator's write of a child to its holder.
	Write,
	/// A holder's replicate of its child to one plan member.
	Replicate,
}

impl ChildRpc {
	fn request(self) -> MsgType {
		match self {
			Self::Write => MsgType::TABLE_CHILD_WRITE_REQ,
			Self::Replicate => MsgType::TABLE_CHILD_REPLICATE_REQ,
		}
	}

	fn response(self) -> MsgType {
		match self {
			Self::Write => MsgType::TABLE_CHILD_WRITE_RESP,
			Self::Replicate => MsgType::TABLE_CHILD_REPLICATE_RESP,
		}
	}

	/// The attempt's budget: [`CHILD_WRITE_DEADLINE`] or
	/// [`CHILD_REPLICATE_DEADLINE`].
	pub(crate) fn budget(self) -> std::time::Duration {
		match self {
			Self::Write => CHILD_WRITE_DEADLINE,
			Self::Replicate => CHILD_REPLICATE_DEADLINE,
		}
	}
}

/// Sends one child frame of kind `rpc` and decodes its bounded response, the
/// whole attempt bounded by `rpc`'s budget: resolution and connection,
/// opening the stream, writing the metadata and rows, finishing and reading
/// and decoding the reply. The deadline is set once, before the first of them,
/// and never renewed. On expiry the attempt ends with [`ErrorKind::TimedOut`]
/// and drops only its own stream; the shared connection stays usable. A
/// timed-out attempt is no acknowledgement from `target`, never evidence that
/// it did not store the child.
pub(crate) async fn send_child_frame(
	factory: Arc<TransportProviderFactory>,
	pool: Arc<BufPool>,
	target: NodeId,
	rpc: ChildRpc,
	meta: Vec<u8>,
	rows: Arc<Vec<u8>>,
) -> std::io::Result<ChildWriteResponse> {
	send_child_frame_with_trailing(factory, pool, target, rpc, meta, rows, Arc::new(Vec::new()))
		.await
}

/// `send_child_frame`, with `trailing` (a named AppendOnly child's ordinals,
/// wendelon ADR 0119 D5) as the frame's trailing section when non-empty.
pub(crate) async fn send_child_frame_with_trailing(
	factory: Arc<TransportProviderFactory>,
	pool: Arc<BufPool>,
	target: NodeId,
	rpc: ChildRpc,
	meta: Vec<u8>,
	rows: Arc<Vec<u8>>,
	trailing: Arc<Vec<u8>>,
) -> std::io::Result<ChildWriteResponse> {
	let deadline = tokio::time::Instant::now() + rpc.budget();
	let attempt = async {
		let conn = factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
			msg_type: rpc.request().0,
			flags: 0,
			stream_id: 0,
			meta_len: meta.len() as u32,
			arrow_len: rows.len() as u32,
			trailing_len: trailing.len() as u32,
		};
		let trailing_section = (!trailing.is_empty()).then_some(trailing.as_slice());
		write_frame(
			&mut *stream,
			&header,
			&meta,
			Some(rows.as_slice()),
			trailing_section,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|error| Error::new(ErrorKind::BrokenPipe, error))?;
		let response = read_frame(&mut *stream, &pool).await?;
		crate::consensus::decision_mode::check_peer_exchange_version(response.header.version)?;
		if response.header.msg_type != rpc.response().0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"unexpected child write response 0x{:02x}",
					response.header.msg_type
				),
			));
		}
		if response.meta.len() > crate::network::decision_proposals::MAX_DECISION_FRAME_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"child write response exceeds its bound",
			));
		}
		postcard::from_bytes(response.meta.as_ref())
			.map_err(|error| Error::new(ErrorKind::InvalidData, error))
	};
	tokio::time::timeout_at(deadline, attempt)
		.await
		.unwrap_or_else(|_| {
			Err(Error::new(
				ErrorKind::TimedOut,
				format!(
					"child {rpc:?} to node {} had no answer within {:?}",
					target.id,
					rpc.budget()
				),
			))
		})
}

#[cfg(test)]
#[path = "child_writes/tests.rs"]
mod tests;

/// DC-269 (test only): a pause inside `persist_locked`, which runs in a spawned
/// task with no handle on a server's pause queue. A test arms it for one store
/// (its slab manager) and one table; the first persist there consumes it, waits
/// for its release, and records what its own `begin_parent_intent` returned.
#[cfg(test)]
pub(crate) mod persist_pause {
	use crate::storage::index::parent_intents::{IntentRefusal, OwnedVersion, ParentIntentState};
	use std::sync::{Arc, Mutex};
	use tokio::sync::Notify;

	/// What the paused worker's begin returned, with the versions it named.
	#[derive(Clone, Debug)]
	pub(crate) struct Begun {
		pub manifest: Vec<OwnedVersion>,
		pub result: Result<Result<ParentIntentState, IntentRefusal>, String>,
	}

	#[derive(Clone)]
	pub(crate) struct Probe {
		pub reached: Arc<Notify>,
		pub release: Arc<Notify>,
		pub begun: Arc<Notify>,
		outcome: Arc<Mutex<Option<Begun>>>,
	}

	impl Probe {
		pub(crate) fn record(
			&self,
			manifest: &[OwnedVersion],
			result: &std::io::Result<Result<ParentIntentState, IntentRefusal>>,
		) {
			*self.outcome.lock().unwrap() = Some(Begun {
				manifest: manifest.to_vec(),
				result: result
					.as_ref()
					.map(|r| *r)
					.map_err(|error| error.to_string()),
			});
			self.begun.notify_one();
		}

		pub(crate) fn outcome(&self) -> Option<Begun> {
			self.outcome.lock().unwrap().clone()
		}
	}

	static ARMED: Mutex<Vec<(usize, crate::schema::TableObjectId, Probe)>> = Mutex::new(Vec::new());

	/// Arms the pause before `begin_parent_intent` for `store` and `table`.
	pub(crate) fn arm(
		store: &crate::storage::manager::SlabManager,
		table: crate::schema::TableObjectId,
	) -> Probe {
		let probe = Probe {
			reached: Arc::new(Notify::new()),
			release: Arc::new(Notify::new()),
			begun: Arc::new(Notify::new()),
			outcome: Arc::new(Mutex::new(None)),
		};
		ARMED
			.lock()
			.unwrap()
			.push((store as *const _ as usize, table, probe.clone()));
		probe
	}

	pub(crate) async fn before_parent_intent(
		store: &crate::storage::manager::SlabManager,
		table: crate::schema::TableObjectId,
	) -> Option<Probe> {
		let key = store as *const _ as usize;
		let probe = {
			let mut all = ARMED.lock().unwrap();
			all.iter()
				.position(|(at, armed, _)| *at == key && *armed == table)
				.map(|index| all.remove(index).2)
		}?;
		probe.reached.notify_one();
		probe.release.notified().await;
		Some(probe)
	}

	/// DROP design (test only): a pause immediately after a successful
	/// `begin_parent_intent`, before any append. Its guard lives in the spawned
	/// work and signals `ended` when that work ends, on any path.
	#[derive(Clone)]
	pub(crate) struct AfterBegin {
		pub reached: Arc<Notify>,
		pub release: Arc<Notify>,
		pub ended: Arc<Notify>,
		manifest: Arc<Mutex<Vec<OwnedVersion>>>,
	}

	impl AfterBegin {
		/// The versions the paused worker's intent names.
		pub(crate) fn manifest(&self) -> Vec<OwnedVersion> {
			self.manifest.lock().unwrap().clone()
		}
	}

	pub(crate) struct EndGuard(Arc<Notify>);

	impl Drop for EndGuard {
		fn drop(&mut self) {
			self.0.notify_one();
		}
	}

	static ARMED_AFTER: Mutex<Vec<(usize, crate::schema::TableObjectId, AfterBegin)>> =
		Mutex::new(Vec::new());

	/// Arms the pause after a successful begin for `store` and `table`.
	pub(crate) fn arm_after_begin(
		store: &crate::storage::manager::SlabManager,
		table: crate::schema::TableObjectId,
	) -> AfterBegin {
		let probe = AfterBegin {
			reached: Arc::new(Notify::new()),
			release: Arc::new(Notify::new()),
			ended: Arc::new(Notify::new()),
			manifest: Arc::new(Mutex::new(Vec::new())),
		};
		ARMED_AFTER
			.lock()
			.unwrap()
			.push((store as *const _ as usize, table, probe.clone()));
		probe
	}

	pub(crate) async fn after_parent_intent(
		store: &crate::storage::manager::SlabManager,
		table: crate::schema::TableObjectId,
		manifest: &[OwnedVersion],
	) -> Option<EndGuard> {
		let key = store as *const _ as usize;
		let probe = {
			let mut all = ARMED_AFTER.lock().unwrap();
			all.iter()
				.position(|(at, armed, _)| *at == key && *armed == table)
				.map(|index| all.remove(index).2)
		}?;
		*probe.manifest.lock().unwrap() = manifest.to_vec();
		probe.reached.notify_one();
		probe.release.notified().await;
		Some(EndGuard(probe.ended))
	}
}

/// DC-272 (test only): which producer registry the roles used on one armed
/// store. A test arms a data directory at its replacement boundary and keeps the
/// returned guard; dropping it deregisters. Calls for unarmed directories
/// allocate nothing; an armed store's evidence is fixed-size counters.
#[cfg(test)]
pub(crate) mod registry_probe {
	use std::path::{Path, PathBuf};
	use std::sync::{Arc, Mutex};

	/// What one armed store saw. `driver` is the first recovery driver's
	/// registry; later roles count as matching it or not.
	#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
	pub(crate) struct Evidence {
		pub driver: Option<usize>,
		pub drivers: usize,
		pub persists_matching: usize,
		pub persists_other: usize,
		pub fences_matching: usize,
		pub fences_other: usize,
		/// Persists or fences before any driver was seen.
		pub before_driver: usize,
	}

	type Armed = (PathBuf, Arc<Mutex<Evidence>>);
	static ARMED: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

	/// Deregisters its store when dropped.
	pub(crate) struct Probe {
		dir: PathBuf,
		evidence: Arc<Mutex<Evidence>>,
	}

	impl Probe {
		pub(crate) fn evidence(&self) -> Evidence {
			*self.evidence.lock().unwrap()
		}
	}

	impl Drop for Probe {
		fn drop(&mut self) {
			ARMED.lock().unwrap().retain(|(dir, evidence)| {
				!(dir == &self.dir && Arc::ptr_eq(evidence, &self.evidence))
			});
		}
	}

	/// Arms `dir` (and the stores under it) from now on.
	pub(crate) fn arm(dir: &Path) -> Probe {
		let evidence = Arc::new(Mutex::new(Evidence::default()));
		ARMED
			.lock()
			.unwrap()
			.push((dir.to_path_buf(), evidence.clone()));
		Probe {
			dir: dir.to_path_buf(),
			evidence,
		}
	}

	pub(crate) enum Role {
		RecoveryDriver,
		Persist,
		ActivationFence,
	}

	pub(crate) fn note(dir: &Path, role: Role, locks: &super::ParentLocks) {
		let armed = ARMED.lock().unwrap();
		let Some((_, evidence)) = armed.iter().find(|(at, _)| dir.starts_with(at)) else {
			return;
		};
		let registry = locks as *const _ as usize;
		let mut seen = evidence.lock().unwrap();
		match role {
			Role::RecoveryDriver => {
				seen.drivers += 1;
				seen.driver.get_or_insert(registry);
			}
			Role::Persist | Role::ActivationFence => {
				let Some(driver) = seen.driver else {
					seen.before_driver += 1;
					return;
				};
				let matching = registry == driver;
				match (role, matching) {
					(Role::Persist, true) => seen.persists_matching += 1,
					(Role::Persist, false) => seen.persists_other += 1,
					(_, true) => seen.fences_matching += 1,
					(_, false) => seen.fences_other += 1,
				}
			}
		}
	}
}

/// Wendelon ADR 0119 D5: the row keys a child frame carries in its trailing
/// section, still undecoded: none, or the section's raw bytes. What they are (a
/// named AppendOnly write or replicate's original ordinals, a repair answer's
/// identities) and how long they must be are known only once the claim, its
/// operation, the table's policy and the receiver's role are; `plan_child_keyed`
/// decodes them only then, after the row bounds (Codex 01a1f9ca).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum GivenKeys {
	None,
	Section(Arc<Vec<u8>>),
}

impl GivenKeys {
	/// A frame's trailing section: absent or empty, none.
	pub(crate) fn of_section(section: &Arc<Vec<u8>>) -> Self {
		if section.is_empty() {
			Self::None
		} else {
			Self::Section(section.clone())
		}
	}
}

/// Wendelon ADR 0119 D4: N, the rows of a claim's write: the checked sum of its
/// children's counts.
pub(crate) fn claim_rows(claim: &crate::consensus::decisions::Claim) -> Option<u64> {
	claim
		.children
		.iter()
		.try_fold(0u64, |sum, child| sum.checked_add(u64::from(child.rows)))
}

#[cfg(test)]
thread_local! {
	/// Test-only: how many key sections this thread decoded into a vector (Codex
	/// 01a1f9ca, 01a1f9f6: a section of the wrong kind is refused before any is
	/// built; scoped to the thread, so only the calling test's own plans count).
	pub(crate) static KEY_SECTION_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Wendelon ADR 0119 D5: ordinals as the trailing section carries them: u32
/// big-endian, in batch order.
pub(crate) fn encode_ordinals(ordinals: &[u32]) -> Vec<u8> {
	ordinals
		.iter()
		.flat_map(|ordinal| ordinal.to_be_bytes())
		.collect()
}

/// Decodes `rows` ordinals from a trailing section, its exact length checked
/// (with checked arithmetic) before any vector is built.
pub(crate) fn decode_ordinals(bytes: &[u8], rows: usize) -> Result<Vec<u32>, String> {
	let expected = rows.checked_mul(4).ok_or("the ordinal section overflows")?;
	if bytes.len() != expected {
		return Err("the ordinals do not match the rows".into());
	}
	#[cfg(test)]
	KEY_SECTION_DECODES.with(|count| count.set(count.get() + 1));
	Ok(bytes
		.chunks_exact(4)
		.map(|chunk| u32::from_be_bytes(chunk.try_into().expect("four bytes")))
		.collect())
}

/// Wendelon ADR 0119 D5: identities as a repair answer's trailing section carries
/// them: 32 bytes each, in row order.
pub(crate) fn encode_identities(identities: &[crate::schema::Identity]) -> Vec<u8> {
	identities
		.iter()
		.flat_map(|identity| *identity.as_bytes())
		.collect()
}

/// Decodes `rows` identities from a trailing section, its exact length checked
/// before any vector is built.
pub(crate) fn decode_identities(
	bytes: &[u8],
	rows: usize,
) -> Result<Vec<crate::schema::Identity>, String> {
	let expected = rows
		.checked_mul(32)
		.ok_or("the identity section overflows")?;
	if bytes.len() != expected {
		return Err("the identities do not match the rows".into());
	}
	#[cfg(test)]
	KEY_SECTION_DECODES.with(|count| count.set(count.get() + 1));
	Ok(bytes
		.chunks_exact(32)
		.map(|chunk| crate::schema::Identity::new(chunk.try_into().expect("32 bytes")))
		.collect())
}

/// Wendelon ADR 0119 D5 / ADR 0120 D3: the declared-type predicates live with
/// the schema (Begin checks them at apply); re-exported for the write paths.
pub(crate) use crate::schema::table::parent_decisions_append_only_schema;
#[cfg(test)]
pub(crate) use crate::schema::table::repair_bounded_column;

/// The IPC body alignment of `encode_batch` (`IpcWriteOptions::default()`).
pub(crate) const IPC_BUFFER_ALIGNMENT: usize = 64;

/// Wendelon ADR 0119 D5 (Codex 01a1f9ca): the largest encoded repair answer for
/// a child whose write sub-batch (`take` of its rows) encodes to `write_bytes`,
/// over `columns` flat columns and `rows` rows. The answer is the server's
/// concatenation of the stored rows, encoded uncompressed under the same schema:
/// - every value and offset buffer has exactly the write's length (flat columns);
/// - its stream holds the same messages, with the same field nodes and buffer
///   entries (fixed-size records), so its metadata is the write's size;
/// - a column's validity buffer is at most one bitmap of `rows` bits, which the
///   write may lack: at most `ceil(rows / 8)` bytes, padded to the alignment.
///
/// So `answer <= write_bytes + columns * pad(ceil(rows / 8))`. `None` on overflow.
pub(crate) fn repair_rows_bound(write_bytes: usize, columns: usize, rows: usize) -> Option<usize> {
	let bitmap = rows
		.div_ceil(8)
		.checked_next_multiple_of(IPC_BUFFER_ALIGNMENT)?;
	columns.checked_mul(bitmap)?.checked_add(write_bytes)
}
