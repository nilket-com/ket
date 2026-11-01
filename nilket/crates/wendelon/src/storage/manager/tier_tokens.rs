//! Proposer tokens for tier history (ADR 0692 §7, ADR 0696 fast follow).
//!
//! Every proposer of a tier pointer change reserves the history its
//! publication may need before it proposes, and the reservation lives in a
//! fixed-size, charged registry slot until the proposal is definitively
//! resolved:
//! - the local observer consumes it into the published layout;
//! - a refusal before submission, or a successful response for exactly this
//!   attempt after the observer funded or skipped it, releases it;
//! - an ambiguous outcome keeps it until the manager closes, because no log
//!   position is ever inferred for it.
//!
//! A [`TierProposal`] binds the proposed pointer to its slot: it is the only
//! argument a committer receives, so no token can be paired with another
//! pointer or manager.
use std::sync::{Arc, Mutex, Weak};

use super::tier_budget::{
	BudgetReservation, LINEAGE_HISTORY_CAP, TierBudgetKind, TierHistoryShortfall, TierMemoryBudget,
	checked_sum, hash_table_bytes_bound, tier_history_refusal, vec_bytes,
};
use super::*;
use crate::consensus::TieredSlab;

/// How many proposals may hold history at once on one manager.
pub(super) const TOKEN_SLOTS: usize = 1024;

/// The generation a proposal's plan replaces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TierBase {
	/// The slab has no published tier generation (a first tier).
	Untiered,
	/// The published pointer and its layout generation.
	Published {
		pointer: TieredSlabPointer,
		generation: u64,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotState {
	/// Reserved; `client_write` not yet entered.
	Held,
	/// Submitted; resolved only by its own response, the observer or close.
	Submitted,
}

#[derive(Debug)]
struct Slot {
	attempt: u64,
	pointer: TieredSlabPointer,
	base: TierBase,
	state: SlotState,
	/// Retired before `charge` refunds (fields drop after `Drop::drop`).
	backing: Arc<super::tier_budget::TokenBacking>,
	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "held for its Drop: the token's reservation")
	)]
	charge: BudgetReservation,
}

impl Drop for Slot {
	fn drop(&mut self) {
		self.backing.retire();
	}
}

/// One manager's token slots. The table's own memory is charged once, at
/// open, and never grows.
#[derive(Debug)]
pub(super) struct TierTokenRegistry {
	slots: Mutex<Vec<Option<Slot>>>,
	next_attempt: std::sync::atomic::AtomicU64,
	budget: Arc<TierMemoryBudget>,
	_table: BudgetReservation,
}

impl TierTokenRegistry {
	pub(super) fn new(budget: &Arc<TierMemoryBudget>) -> std::io::Result<Arc<Self>> {
		let bytes = vec_bytes::<Option<Slot>>(TOKEN_SLOTS)
			.ok_or_else(|| Error::new(ErrorKind::OutOfMemory, "tier token registry size"))?;
		let table = budget
			.try_reserve(TierBudgetKind::Registry, bytes)
			.ok_or_else(|| Error::new(ErrorKind::OutOfMemory, "tier token registry"))?;
		let mut slots = Vec::new();
		slots
			.try_reserve_exact(TOKEN_SLOTS)
			.map_err(|_| Error::new(ErrorKind::OutOfMemory, "tier token registry"))?;
		slots.resize_with(TOKEN_SLOTS, || None);
		Ok(Arc::new(Self {
			slots: Mutex::new(slots),
			next_attempt: std::sync::atomic::AtomicU64::new(1),
			budget: budget.clone(),
			_table: table,
		}))
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Option<Slot>>> {
		self.slots
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
	}

	/// Occupied slots and the bytes they hold.
	#[cfg(test)]
	pub(super) fn held(&self) -> (usize, u64) {
		self.lock()
			.iter()
			.flatten()
			.fold((0, 0), |(count, bytes), slot| {
				(count + 1, bytes + slot.charge.bytes())
			})
	}

	fn admit(
		self: &Arc<Self>,
		pointer: TieredSlabPointer,
		base: TierBase,
		charge: BudgetReservation,
	) -> Option<TokenRef> {
		// Attempt ids never wrap: an exhausted counter admits nothing.
		let attempt = self
			.next_attempt
			.fetch_update(
				std::sync::atomic::Ordering::AcqRel,
				std::sync::atomic::Ordering::Acquire,
				|attempt| attempt.checked_add(1),
			)
			.ok()?;
		let mut slots = self.lock();
		let index = slots.iter().position(Option::is_none)?;
		slots[index] = Some(Slot {
			attempt,
			pointer,
			base,
			state: SlotState::Held,
			backing: super::tier_budget::TokenBacking::new(&self.budget, charge.bytes()),
			charge,
		});
		Some(TokenRef {
			registry: Arc::downgrade(self),
			index,
			attempt,
		})
	}

	/// Back an applied entry's build with the lowest *submitted* attempt
	/// holding exactly `pointer` over `base`. The token stays in its slot and
	/// charged; only [`Self::consume`], after that entry is installed, ends it.
	/// A held (never submitted) attempt is never used, and other attempts for
	/// the same pointer are left as they are.
	pub(super) fn back_matching(
		&self,
		pointer: &TieredSlabPointer,
		base: &TierBase,
	) -> Option<TokenUse> {
		let slots = self.lock();
		let (attempt, index, backing) = slots
			.iter()
			.enumerate()
			.filter_map(|(index, slot)| {
				slot.as_ref()
					.filter(|slot| {
						slot.state == SlotState::Submitted
							&& slot.pointer == *pointer
							&& slot.base == *base
					})
					.map(|slot| (slot.attempt, index, slot.backing.clone()))
			})
			.min_by_key(|(attempt, index, _)| (*attempt, *index))?;
		Some(TokenUse {
			index,
			attempt,
			backing,
		})
	}

	/// The applied entry this token backed is installed: its proposal is
	/// resolved and the reservation goes.
	pub(super) fn consume(&self, token: TokenUse) {
		let mut slots = self.lock();
		if slots[token.index]
			.as_ref()
			.is_some_and(|slot| slot.attempt == token.attempt)
		{
			slots[token.index] = None;
		}
	}

	fn with_slot(&self, token: &TokenRef, change: impl FnOnce(&mut Option<Slot>)) {
		let mut slots = self.lock();
		let slot = &mut slots[token.index];
		// A slot reused by a newer attempt is not this token's.
		if slot
			.as_ref()
			.is_some_and(|slot| slot.attempt == token.attempt)
		{
			change(slot);
		}
	}

	pub(super) fn budget(&self) -> &Arc<TierMemoryBudget> {
		&self.budget
	}
}

/// A submitted token backing one applied entry's build.
#[derive(Debug)]
pub(super) struct TokenUse {
	index: usize,
	attempt: u64,
	pub(super) backing: Arc<super::tier_budget::TokenBacking>,
}

#[cfg(test)]
impl TokenUse {
	fn backing_bytes(&self) -> u64 {
		self.backing.bytes()
	}
}

/// A token's address in its registry, valid only while the slot still holds
/// the same attempt.
#[derive(Debug)]
struct TokenRef {
	registry: Weak<TierTokenRegistry>,
	index: usize,
	attempt: u64,
}

impl TokenRef {
	fn with_slot(&self, change: impl FnOnce(&mut Option<Slot>)) {
		if let Some(registry) = self.registry.upgrade() {
			registry.with_slot(self, change);
		}
	}
}

/// A tier pointer proposal together with the history reservation its
/// publication may need. Dropped before submission, it releases the
/// reservation.
#[derive(Debug)]
pub struct TierProposal {
	tier: TieredSlab,
	token: Option<TokenRef>,
}

impl TierProposal {
	/// The entry to propose.
	pub fn tier(&self) -> &TieredSlab {
		&self.tier
	}

	/// Mark the proposal submitted, before the submission future starts.
	/// From here the reservation is kept until [`TierSubmission::applied`]
	/// or [`TierSubmission::not_submitted`] resolves it, the observer takes
	/// it, or the manager closes.
	pub fn begin_submission(mut self) -> std::io::Result<TierSubmission> {
		let Some(token) = self.token.take() else {
			return Ok(TierSubmission { token: None });
		};
		let mut live = false;
		token.with_slot(|slot| {
			if let Some(slot) = slot
				&& slot.state == SlotState::Held
			{
				slot.state = SlotState::Submitted;
				live = true;
			}
		});
		if live {
			Ok(TierSubmission { token: Some(token) })
		} else {
			// The reservation is gone (its manager closed): proposing now would
			// propose without one.
			Err(tier_history_refusal(
				self.tier.slab_id,
				TierHistoryShortfall::History,
			))
		}
	}

	/// A proposal without a manager behind it, for committers in tests.
	#[cfg(any(test, feature = "test-support"))]
	pub fn unreserved_for_test(tier: TieredSlab) -> Self {
		Self { tier, token: None }
	}
}

impl Drop for TierProposal {
	fn drop(&mut self) {
		// Never submitted: nothing can apply it, so its reservation goes.
		if let Some(token) = self.token.take() {
			token.with_slot(|slot| {
				if slot
					.as_ref()
					.is_some_and(|slot| slot.state == SlotState::Held)
				{
					*slot = None;
				}
			});
		}
	}
}

/// A submitted proposal. Dropped unresolved (an ambiguous outcome or a
/// cancelled submission), it keeps its reservation.
#[derive(Debug)]
pub struct TierSubmission {
	token: Option<TokenRef>,
}

impl TierSubmission {
	/// The command was applied: whatever the observer did not take is this
	/// attempt's unused reservation.
	pub fn applied(self) {
		self.release();
	}

	/// The command was refused before `client_write`: it can never apply.
	pub fn not_submitted(self) {
		self.release();
	}

	fn release(mut self) {
		if let Some(token) = self.token.take() {
			token.with_slot(|slot| *slot = None);
		}
	}
}

impl SlabManager {
	/// Reserve the history a publication of `tier` may need, before it is
	/// proposed. The reservation is an upper bound computed without
	/// allocating: any plan over the slab's indexed rows yields at most the
	/// published redirects plus one per row, and at most the published
	/// superseded entries and redirects plus one per row, each within the
	/// per-lineage cap. Refuses typed when the budget or the registry is full.
	pub async fn reserve_tier_history(&self, tier: TieredSlab) -> std::io::Result<TierProposal> {
		let slab_id = tier.slab_id;
		let refuse = || tier_history_refusal(slab_id, TierHistoryShortfall::History);
		let (base, redirects, superseded) = self.tier_history_base(slab_id).await;
		let rows = self.count_record_rows_for_slab(slab_id);
		let redirect_bound = redirects
			.checked_add(rows)
			.ok_or_else(refuse)?
			.min(LINEAGE_HISTORY_CAP);
		let superseded_bound = superseded
			.checked_add(redirects)
			.and_then(|bound| bound.checked_add(rows))
			.ok_or_else(refuse)?
			.min(LINEAGE_HISTORY_CAP);
		// The slot also keeps the proposed pointer and its base, whose URIs
		// are the only variable allocations it holds.
		let base_uri = match &base {
			TierBase::Published { pointer, .. } => pointer.l2_uri.len(),
			TierBase::Untiered => 0,
		};
		let bytes = checked_sum(&[
			hash_table_bytes_bound::<(RecordLocation, RecordLocation)>(redirect_bound),
			hash_table_bytes_bound::<RecordLocation>(superseded_bound),
			u64::try_from(tier.l2_uri.len()).ok(),
			u64::try_from(base_uri).ok(),
		])
		.ok_or_else(refuse)?;
		let charge = self
			.tier_tokens
			.budget()
			.try_reserve(TierBudgetKind::Token, bytes)
			.ok_or_else(refuse)?;
		let pointer = tiered_slab_pointer(&tier);
		let token = self
			.tier_tokens
			.admit(pointer, base, charge)
			.ok_or_else(refuse)?;
		Ok(TierProposal {
			tier,
			token: Some(token),
		})
	}

	/// The slab's published base and its history sizes, read in one `state`
	/// snapshot.
	async fn tier_history_base(&self, slab_id: SlabId) -> (TierBase, usize, usize) {
		let state = self.state.lock().await;
		let base = self.tier_publication_base(&state, slab_id);
		let (redirects, superseded) = state
			.tier_layouts
			.get(&slab_id)
			.map(|layout| (layout.redirects.len(), layout.superseded.len()))
			.unwrap_or((0, 0));
		(base, redirects, superseded)
	}

	/// The observer's build of `pointer`'s next layout over `current`. For an
	/// applied log entry (`applied`), a matching submitted proposer token, if
	/// this manager holds one, backs the reservation (ADR 0692 §7: the
	/// observer consumes the token); the caller consumes it only after the
	/// entry is installed. Every other publication (verification, snapshot,
	/// local reconciliation) reserves afresh and never touches a token.
	pub(super) fn build_funded_tier_layout(
		&self,
		current: &super::tier_generation::TierLayout,
		pointer: &TieredSlabPointer,
		base: &TierBase,
		plan: &super::tiered_io::TieredRowRefreshPlan,
		applied: bool,
	) -> std::io::Result<(super::tier_generation::TierLayout, Option<TokenUse>)> {
		let token = applied
			.then(|| self.tier_tokens.back_matching(pointer, base))
			.flatten();
		let layout = current.next_with(
			pointer.slab_id,
			plan,
			&self.tier_budget,
			token.as_ref().map(|token| &token.backing),
		)?;
		Ok((layout, token))
	}

	/// The observer's build, with §7's outcome reporting: a budget shortfall
	/// fences the slab (`Fenced`, retried; history kept, nothing unreserved
	/// allocated) and is counted as a fence; any other refusal is counted as a
	/// refused rewrite and returned as it is.
	pub(super) fn build_observed_tier_layout(
		&self,
		current: &super::tier_generation::TierLayout,
		pointer: &TieredSlabPointer,
		base: &TierBase,
		plan: &super::tiered_io::TieredRowRefreshPlan,
		applied: bool,
	) -> std::io::Result<(super::tier_generation::TierLayout, Option<TokenUse>)> {
		self.build_funded_tier_layout(current, pointer, base, plan, applied)
			.map_err(|error| self.observed_publication_error(pointer.slab_id, error))
	}

	/// §7's reporting of a refused observer publication build, applied once
	/// at the publication boundary whatever step refused (plan scratch, build
	/// scratch or history): a budget shortfall fences the slab (typed
	/// `Fenced`, retried; history kept, nothing unreserved allocated) and is
	/// counted as a fence; any other refusal is counted as a refused rewrite
	/// and returned as it is.
	pub(super) fn observed_publication_error(&self, slab_id: SlabId, error: Error) -> Error {
		use crate::metrics::TierPublicationFenceReason as Fence;
		let fence = match super::tier_budget::tier_history_refusal_of(&error)
			.map(|refusal| refusal.shortfall)
		{
			Some(TierHistoryShortfall::History) => Some(Fence::NoBudget),
			Some(TierHistoryShortfall::Scratch) => Some(Fence::NoScratch),
			_ => None,
		};
		match fence {
			Some(reason) => {
				crate::metrics::global().observe_tier_publication_fenced(reason);
				Error::new(
					ErrorKind::WouldBlock,
					StaleLocation {
						slab_id,
						reason: StaleLocationReason::Fenced,
					},
				)
			}
			None => {
				observe_tier_rewrite_refusal(&error);
				error
			}
		}
	}

	/// The local base a publication over the current generation replaces.
	pub(super) fn tier_publication_base(&self, state: &ManagerState, slab_id: SlabId) -> TierBase {
		match state.tiered.get(&slab_id) {
			None => TierBase::Untiered,
			Some(pointer) => TierBase::Published {
				pointer: pointer.value().clone(),
				generation: state
					.tier_layouts
					.get(&slab_id)
					.map_or(0, |layout| layout.generation),
			},
		}
	}
}

/// The pointer a `TieredSlab` entry names.
pub(super) fn tiered_slab_pointer(tier: &TieredSlab) -> TieredSlabPointer {
	TieredSlabPointer {
		slab_id: tier.slab_id,
		l2_uri: tier.l2_uri.clone(),
		payload_hash: tier.payload_hash,
		is_encrypted: tier.is_encrypted,
		tenant_id: tier.tenant_id,
		tenant_kek_id: tier.tenant_kek_id,
		stats_format_version: tier.stats_format_version,
		stats_block_len: tier.stats_block_len,
		stats_block_hash128: tier.stats_block_hash128,
		tombstone_metadata: tier.tombstone_metadata,
		supersession_metadata: tier.supersession_metadata,
	}
}

/// Count a refused tier rewrite under its §7 reason, if it is one.
pub(super) fn observe_tier_rewrite_refusal(error: &Error) {
	use crate::metrics::TierRewriteRefusalReason as Reason;
	let reason =
		match super::tier_budget::tier_history_refusal_of(error).map(|refusal| refusal.shortfall) {
			Some(TierHistoryShortfall::History | TierHistoryShortfall::Scratch) => {
				Some(Reason::NodeHistoryBudget)
			}
			Some(TierHistoryShortfall::LineageCap) => Some(Reason::LineageHistoryCap),
			Some(TierHistoryShortfall::StalePlan) => Some(Reason::StalePlan),
			None => match stale_location(error).map(|stale| stale.reason) {
				Some(StaleLocationReason::LayoutCollision) => Some(Reason::LayoutCollision),
				_ => None,
			},
		};
	if let Some(reason) = reason {
		crate::metrics::global().observe_tier_rewrite_refusal(reason);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pointer(uri: &str) -> TieredSlabPointer {
		TieredSlabPointer {
			slab_id: SlabId::new(7),
			l2_uri: uri.to_string(),
			payload_hash: [0; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: crate::schema::TombstoneMetadata::None,
			supersession_metadata: crate::schema::SupersessionMetadata::None,
		}
	}

	fn registry(limit: u64) -> (Arc<TierMemoryBudget>, Arc<TierTokenRegistry>) {
		let budget = TierMemoryBudget::new(limit);
		let registry = TierTokenRegistry::new(&budget).unwrap();
		(budget, registry)
	}

	fn proposal(registry: &Arc<TierTokenRegistry>, uri: &str, bytes: u64) -> TierProposal {
		let charge = registry
			.budget()
			.try_reserve(TierBudgetKind::Token, bytes)
			.unwrap();
		let token = registry
			.admit(pointer(uri), TierBase::Untiered, charge)
			.unwrap();
		TierProposal {
			tier: TieredSlab {
				slab_id: SlabId::new(7),
				l2_uri: uri.to_string(),
				payload_hash: [0; 32],
				is_encrypted: false,
				tenant_id: None,
				tenant_kek_id: None,
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: crate::schema::TombstoneMetadata::None,
				supersession_metadata: crate::schema::SupersessionMetadata::None,
			},
			token: Some(token),
		}
	}

	fn tokens(budget: &TierMemoryBudget) -> u64 {
		budget.used_by(TierBudgetKind::Token)
	}

	#[test]
	fn the_registry_table_is_charged_once_and_never_grows() {
		let (budget, registry) = registry(1 << 20);
		assert!(budget.used_by(TierBudgetKind::Registry) > 0);
		assert_eq!(registry.held(), (0, 0));
		let weak = Arc::downgrade(&budget);
		drop(registry);
		assert_eq!(budget.used(), 0, "closing the registry releases its table");
		drop(budget);
		assert!(weak.upgrade().is_none());
	}

	#[test]
	fn a_proposal_dropped_before_submission_releases_its_reservation() {
		let (budget, registry) = registry(1 << 20);
		drop(proposal(&registry, "a", 100));
		assert_eq!(tokens(&budget), 0);
		assert_eq!(registry.held(), (0, 0));
	}

	#[test]
	fn a_submission_cancelled_or_ambiguous_keeps_its_reservation_until_close() {
		let (budget, registry) = registry(1 << 20);
		let submission = proposal(&registry, "a", 100).begin_submission().unwrap();
		drop(submission);
		assert_eq!(tokens(&budget), 100, "an unknown outcome holds the charge");
		assert_eq!(registry.held(), (1, 100));
		drop(registry);
		assert_eq!(budget.used(), 0);
	}

	#[test]
	fn not_submitted_and_applied_release_exactly_their_attempt() {
		let (budget, registry) = registry(1 << 20);
		let first = proposal(&registry, "a", 100).begin_submission().unwrap();
		let second = proposal(&registry, "a", 50).begin_submission().unwrap();
		first.not_submitted();
		assert_eq!(registry.held(), (1, 50));
		second.applied();
		assert_eq!(registry.held(), (0, 0));
		assert_eq!(tokens(&budget), 0);
	}

	#[test]
	fn only_a_submitted_attempt_backs_an_applied_build_and_a_held_one_keeps_its_slot() {
		let (budget, registry) = registry(1 << 20);
		// An older held attempt and a newer submitted one for the same pointer.
		let held = proposal(&registry, "a", 100);
		let submitted = proposal(&registry, "a", 60).begin_submission().unwrap();
		let token = registry
			.back_matching(&pointer("a"), &TierBase::Untiered)
			.unwrap();
		assert_eq!(token.backing_bytes(), 60, "the held attempt is never used");
		registry.consume(token);
		assert_eq!(registry.held(), (1, 100));
		submitted.applied();
		let held = held
			.begin_submission()
			.expect("the held attempt's own reservation is still live");
		assert_eq!(registry.held(), (1, 100));
		held.applied();
		assert_eq!(tokens(&budget), 0);
	}

	#[test]
	fn backing_never_removes_the_token_and_a_late_response_is_a_no_op_after_consume() {
		let (budget, registry) = registry(1 << 20);
		let first = proposal(&registry, "a", 100).begin_submission().unwrap();
		let second = proposal(&registry, "a", 60).begin_submission().unwrap();
		let token = registry
			.back_matching(&pointer("a"), &TierBase::Untiered)
			.unwrap();
		assert_eq!(
			registry.held(),
			(2, 160),
			"backing keeps the token charged in its slot"
		);
		drop(token);
		assert_eq!(
			registry.held(),
			(2, 160),
			"a build that did not install changes nothing"
		);
		let token = registry
			.back_matching(&pointer("a"), &TierBase::Untiered)
			.unwrap();
		registry.consume(token);
		first.applied();
		assert_eq!(registry.held(), (1, 60), "the identical attempt stays");
		second.applied();
		assert_eq!(tokens(&budget), 0);
	}

	#[test]
	fn a_backed_reservation_may_overlap_its_token_but_never_exceeds_the_budget_otherwise() {
		let (budget, registry) = registry(1 << 20);
		let table = budget.used_by(TierBudgetKind::Registry);
		let room = budget.limit() - table;
		let _submitted = proposal(&registry, "a", room).begin_submission().unwrap();
		assert!(
			budget.try_reserve(TierBudgetKind::Layout, 1).is_none(),
			"the token holds all headroom"
		);
		let token = registry
			.back_matching(&pointer("a"), &TierBase::Untiered)
			.unwrap();
		let layout = budget
			.try_reserve_backed(TierBudgetKind::Layout, room, &token.backing)
			.expect("the token backs the layout it reserved for");
		assert!(
			budget
				.try_reserve_backed(TierBudgetKind::Layout, 1, &token.backing)
				.is_none(),
			"backing covers at most the token"
		);
		assert_eq!(
			budget.peak(),
			budget.limit(),
			"the overlap is not counted twice"
		);
		registry.consume(token);
		drop(layout);
		assert_eq!(budget.used(), table);
	}

	#[test]
	fn concurrent_builds_share_one_tokens_backing_and_claims_return_or_end_with_it() {
		let (budget, registry) = registry(1 << 20);
		let table = budget.used_by(TierBudgetKind::Registry);
		let room = budget.limit() - table;
		let _submitted = proposal(&registry, "a", room).begin_submission().unwrap();
		let first = registry
			.back_matching(&pointer("a"), &TierBase::Untiered)
			.unwrap();
		let second = registry
			.back_matching(&pointer("a"), &TierBase::Untiered)
			.unwrap();
		let half = budget
			.try_reserve_backed(TierBudgetKind::Layout, room / 2, &first.backing)
			.expect("half the token backs the first build");
		let rest = room - room / 2;
		assert!(
			budget
				.try_reserve_backed(TierBudgetKind::Layout, rest + 1, &second.backing)
				.is_none(),
			"the second build may overlap only what the first left unclaimed"
		);
		let other = budget
			.try_reserve_backed(TierBudgetKind::Layout, rest, &second.backing)
			.expect("the rest of the token backs the second build");
		assert_eq!(first.backing.claimed(), room);
		assert!(
			Arc::ptr_eq(&first.backing, &second.backing),
			"one backing per token"
		);
		assert_eq!(budget.peak(), budget.limit(), "the overlap is counted once");

		drop(other);
		assert_eq!(
			first.backing.claimed(),
			room / 2,
			"a dropped build returns its claim"
		);
		let mut retry = budget
			.try_reserve_backed(TierBudgetKind::Layout, rest, &second.backing)
			.expect("a retry is backed again");
		retry.shrink_to(1).unwrap();
		assert_eq!(
			first.backing.claimed(),
			room / 2 + 1,
			"a shrink returns the surplus overlap"
		);

		registry.consume(first);
		assert_eq!(
			second.backing.claimed(),
			0,
			"a released token backs nothing"
		);
		budget.reset_peak_for_test();
		assert_eq!(
			budget.peak(),
			budget.used(),
			"layouts that outlive their token count in full"
		);
		drop((half, retry, second));
		assert_eq!(budget.used(), table);
		assert_eq!(budget.peak(), budget.used() + room / 2 + 1);
	}

	#[test]
	fn submission_refuses_when_its_reservation_is_gone() {
		let (_budget, registry) = registry(1 << 20);
		let proposal = proposal(&registry, "a", 100);
		drop(registry);
		assert!(
			proposal.begin_submission().is_err(),
			"no proposal is submitted without its live reservation"
		);
	}

	#[test]
	fn a_token_never_matches_another_pointer_or_base() {
		let (_budget, registry) = registry(1 << 20);
		let _submission = proposal(&registry, "a", 100).begin_submission().unwrap();
		assert!(
			registry
				.back_matching(&pointer("b"), &TierBase::Untiered)
				.is_none()
		);
		let published = TierBase::Published {
			pointer: pointer("a"),
			generation: 0,
		};
		assert!(registry.back_matching(&pointer("a"), &published).is_none());
	}

	#[test]
	fn a_late_response_never_touches_a_newer_occupant_of_its_slot() {
		let (budget, registry) = registry(1 << 20);
		let old = proposal(&registry, "a", 100).begin_submission().unwrap();
		let token = registry
			.back_matching(&pointer("a"), &TierBase::Untiered)
			.unwrap();
		registry.consume(token);
		let newer = proposal(&registry, "b", 70).begin_submission().unwrap();
		old.applied();
		assert_eq!(
			registry.held(),
			(1, 70),
			"the reused slot keeps the newer attempt"
		);
		newer.not_submitted();
		assert_eq!(tokens(&budget), 0);
	}

	#[test]
	fn a_full_registry_admits_nothing_more() {
		let (_budget, registry) = registry(u64::MAX / 2);
		let held = (0..TOKEN_SLOTS)
			.map(|_| proposal(&registry, "a", 0).begin_submission().unwrap())
			.collect::<Vec<_>>();
		let charge = registry
			.budget()
			.try_reserve(TierBudgetKind::Token, 0)
			.unwrap();
		assert!(
			registry
				.admit(pointer("a"), TierBase::Untiered, charge)
				.is_none(),
			"zero-byte tokens still need a slot"
		);
		drop(held);
	}
}
