//! Tails of remote objects one table query has already fetched.
//!
//! A ParentDecisions table query reads each selected record once to prove it
//! and once to execute, and may load the slab's statistics block between the
//! two. Each of those asks the object store for the object's size and its
//! tail window. A [`QueryTails`] lives for one query and remembers, per object
//! URI, the size and the tail bytes of the first fetch, so the later reads of
//! that query make no size and no tail request.
//!
//! A held tail is bytes and nothing more. A read that takes its tail from the
//! holder still resolves its own pointer, builds its own plan, verifies the
//! footer and the index, decrypts and checks the record exactly as a read that
//! fetched the tail itself.

use super::*;
use std::collections::HashMap;
use std::sync::Mutex;

/// The most tails one query holds.
pub(crate) const QUERY_TAILS_MAX_ENTRIES: usize = 4096;
/// The most bytes one query's held tails are charged for.
pub(crate) const QUERY_TAILS_MAX_CHARGED_BYTES: usize = 8 * 1024 * 1024;
/// Charged per entry beside its tail and its URI, for the map's bookkeeping.
const ENTRY_ALLOWANCE_BYTES: usize = 160;

/// One query's fetched tails, valid only against the object store of the
/// manager that created it.
pub(crate) struct QueryTails {
	/// The creating manager's store. Held, so its allocation cannot be freed
	/// and its address reused by another store while this holder lives: equal
	/// addresses mean the same store.
	owner: Option<Arc<dyn ObjectStore>>,
	tail_window_bytes: u64,
	max_entries: usize,
	max_charged: usize,
	state: Mutex<State>,
}

#[derive(Default)]
struct State {
	entries: HashMap<Box<str>, HeldTail>,
	charged: usize,
}

/// One fetched tail: `bytes` are the object's bytes `start..size`.
#[derive(Clone)]
pub(super) struct HeldTail {
	pub(super) size: u64,
	pub(super) start: u64,
	pub(super) bytes: Bytes,
}

impl SlabManager {
	/// A holder for one query's fetched tails, bound to this manager's object
	/// store and tail window.
	pub(crate) fn query_tails(&self) -> Arc<QueryTails> {
		let tails = Arc::new(QueryTails::new(
			self.config.l2.clone(),
			self.config.rehydration.tail_read_window_bytes,
			QUERY_TAILS_MAX_ENTRIES,
			QUERY_TAILS_MAX_CHARGED_BYTES,
		));
		#[cfg(test)]
		{
			*self
				.last_query_tails
				.lock()
				.expect("the test observation lock is never poisoned") = Arc::downgrade(&tails);
		}
		tails
	}

	/// Test seam: the holder the latest [`Self::query_tails`] call made, if it
	/// is still alive.
	#[cfg(test)]
	pub(crate) fn last_query_tails_for_test(&self) -> std::sync::Weak<QueryTails> {
		self.last_query_tails
			.lock()
			.expect("the test observation lock is never poisoned")
			.clone()
	}

	/// Test seam: this manager's key holder.
	#[cfg(test)]
	pub(crate) fn crypto_for_test(&self) -> &Arc<CryptoManager> {
		&self.config.crypto
	}

	/// Test seam: a holder of this manager with limits a test chooses.
	#[cfg(test)]
	pub(crate) fn query_tails_with_limits_for_test(
		&self,
		max_entries: usize,
		max_charged: usize,
	) -> QueryTails {
		QueryTails::new(
			self.config.l2.clone(),
			self.config.rehydration.tail_read_window_bytes,
			max_entries,
			max_charged,
		)
	}
}

impl QueryTails {
	fn new(
		owner: Option<Arc<dyn ObjectStore>>,
		tail_window_bytes: u64,
		max_entries: usize,
		max_charged: usize,
	) -> Self {
		Self {
			owner,
			tail_window_bytes,
			max_entries,
			max_charged,
			state: Mutex::new(State::default()),
		}
	}

	/// Whether `store` is the store this holder was created for.
	fn owned_by(&self, store: &dyn ObjectStore) -> bool {
		self.owner
			.as_ref()
			.is_some_and(|owner| std::ptr::addr_eq(Arc::as_ptr(owner), std::ptr::from_ref(store)))
	}

	fn state(&self) -> std::sync::MutexGuard<'_, State> {
		// The lock guards plain data and no code under it panics; a poisoned
		// lock still holds a consistent state.
		self.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// The tail held for `uri`, for a caller about to read from `store`.
	pub(super) fn get(&self, store: &dyn ObjectStore, uri: &str) -> Option<HeldTail> {
		if !self.owned_by(store) {
			return None;
		}
		self.state().entries.get(uri).cloned()
	}

	/// Offer a tail just fetched from `store` as `start..size` of `uri`.
	/// Returns whether it is now held. Refused, with nothing changed, for
	/// another store, an empty tail, a tail whose extent is not exactly
	/// `start..size`, one longer than the tail window, a URI already held, a
	/// full holder, or a charge past the budget. Nothing is ever evicted.
	pub(super) fn offer(
		&self,
		store: &dyn ObjectStore,
		uri: &str,
		size: u64,
		start: u64,
		fetched: &Bytes,
	) -> bool {
		if !self.owned_by(store) || fetched.is_empty() {
			return false;
		}
		let Ok(len) = u64::try_from(fetched.len()) else {
			return false;
		};
		if start.checked_add(len) != Some(size) || len > self.tail_window_bytes {
			return false;
		}
		let Some(charge) = fetched
			.len()
			.checked_add(uri.len())
			.and_then(|bytes| bytes.checked_add(ENTRY_ALLOWANCE_BYTES))
		else {
			return false;
		};
		let mut state = self.state();
		if state.entries.len() >= self.max_entries || state.entries.contains_key(uri) {
			return false;
		}
		let Some(charged) = state
			.charged
			.checked_add(charge)
			.filter(|charged| *charged <= self.max_charged)
		else {
			return false;
		};
		state.charged = charged;
		state.entries.insert(
			uri.into(),
			HeldTail {
				size,
				start,
				// Detached: a held tail never keeps a transport buffer alive.
				bytes: Bytes::copy_from_slice(fetched),
			},
		);
		true
	}

	/// Test seam: `(entries, charged bytes)`.
	#[cfg(test)]
	pub(crate) fn held_for_test(&self) -> (usize, usize) {
		let state = self.state();
		(state.entries.len(), state.charged)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::l2::LocalFsObjectStore;

	/// A store no test here asks anything of: only its identity matters.
	fn store() -> Arc<dyn ObjectStore> {
		Arc::new(LocalFsObjectStore::new(std::env::temp_dir()).unwrap())
	}

	fn holder(owner: &Arc<dyn ObjectStore>, entries: usize, charged: usize) -> QueryTails {
		QueryTails::new(Some(owner.clone()), 64, entries, charged)
	}

	#[test]
	fn an_admitted_tail_is_returned_as_a_detached_copy_of_the_fetched_bytes() {
		let owner = store();
		let tails = holder(&owner, 4, 4096);
		let backing = Bytes::from(vec![7u8; 1024]);
		let fetched = backing.slice(1000..1024);
		assert!(tails.offer(owner.as_ref(), "a", 124, 100, &fetched));
		let held = tails.get(owner.as_ref(), "a").expect("held");
		assert_eq!((held.size, held.start), (124, 100));
		assert_eq!(held.bytes, fetched);
		// Not a view of the larger buffer the fetch came from.
		assert_ne!(held.bytes.as_ptr(), fetched.as_ptr());
		assert_eq!(tails.held_for_test(), (1, 24 + 1 + ENTRY_ALLOWANCE_BYTES));
		assert!(tails.get(owner.as_ref(), "b").is_none());
	}

	#[test]
	fn an_empty_inconsistent_oversized_or_duplicate_offer_changes_nothing() {
		let owner = store();
		let tails = holder(&owner, 4, 4096);
		let bytes = Bytes::from(vec![1u8; 10]);
		// Empty.
		assert!(!tails.offer(owner.as_ref(), "a", 10, 10, &Bytes::new()));
		// The extent is not start..size: short, long, and overflowing.
		assert!(!tails.offer(owner.as_ref(), "a", 21, 10, &bytes));
		assert!(!tails.offer(owner.as_ref(), "a", 19, 10, &bytes));
		assert!(!tails.offer(owner.as_ref(), "a", 5, u64::MAX - 4, &bytes));
		// Longer than the tail window (64).
		assert!(!tails.offer(owner.as_ref(), "a", 65, 0, &Bytes::from(vec![1u8; 65])));
		assert_eq!(tails.held_for_test(), (0, 0));
		assert!(tails.get(owner.as_ref(), "a").is_none());
		// Admitted once; the duplicate leaves the first entry as it is.
		assert!(tails.offer(owner.as_ref(), "a", 20, 10, &bytes));
		let before = tails.held_for_test();
		assert!(!tails.offer(owner.as_ref(), "a", 30, 20, &Bytes::from(vec![2u8; 10])));
		assert_eq!(tails.held_for_test(), before);
		let held = tails.get(owner.as_ref(), "a").unwrap();
		assert_eq!((held.size, held.start, held.bytes), (20, 10, bytes));
	}

	#[test]
	fn the_entry_limit_and_the_charge_limit_refuse_and_never_evict() {
		let owner = store();
		let bytes = Bytes::from(vec![1u8; 10]);
		let one = 10 + 1 + ENTRY_ALLOWANCE_BYTES;

		let tails = holder(&owner, 2, usize::MAX);
		assert!(tails.offer(owner.as_ref(), "a", 10, 0, &bytes));
		assert!(tails.offer(owner.as_ref(), "b", 10, 0, &bytes));
		assert!(!tails.offer(owner.as_ref(), "c", 10, 0, &bytes));
		assert_eq!(tails.held_for_test(), (2, 2 * one));
		assert!(tails.get(owner.as_ref(), "a").is_some());
		assert!(tails.get(owner.as_ref(), "c").is_none());

		// The charge may reach the limit and never pass it.
		let tails = holder(&owner, 16, 2 * one);
		assert!(tails.offer(owner.as_ref(), "a", 10, 0, &bytes));
		assert!(tails.offer(owner.as_ref(), "b", 10, 0, &bytes));
		assert!(!tails.offer(owner.as_ref(), "c", 10, 0, &bytes));
		assert_eq!(tails.held_for_test(), (2, 2 * one));
		let tails = holder(&owner, 16, one - 1);
		assert!(!tails.offer(owner.as_ref(), "a", 10, 0, &bytes));
		assert_eq!(tails.held_for_test(), (0, 0));
		// A smaller tail still fits after a larger one was refused.
		assert!(tails.offer(owner.as_ref(), "a", 9, 0, &Bytes::from(vec![1u8; 9])));
	}

	#[test]
	fn a_holder_answers_only_for_the_store_it_was_created_for() {
		let owner = store();
		let other = store();
		let tails = holder(&owner, 4, 4096);
		let bytes = Bytes::from(vec![1u8; 10]);
		assert!(!tails.offer(other.as_ref(), "a", 10, 0, &bytes));
		assert_eq!(tails.held_for_test(), (0, 0));
		assert!(tails.offer(owner.as_ref(), "a", 10, 0, &bytes));
		assert!(tails.get(other.as_ref(), "a").is_none());
		assert!(tails.get(owner.as_ref(), "a").is_some());
		// A clone of the owning handle is the same store.
		assert!(tails.get(owner.clone().as_ref(), "a").is_some());
		// A holder with no store holds nothing for anyone.
		let none = QueryTails::new(None, 64, 4, 4096);
		assert!(!none.offer(owner.as_ref(), "a", 10, 0, &bytes));
		assert!(none.get(owner.as_ref(), "a").is_none());
	}
}
