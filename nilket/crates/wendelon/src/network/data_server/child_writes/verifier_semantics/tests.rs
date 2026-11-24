//! Wendelon ADR 0128: the verifier's observable semantics under controlled changes between
//! copies and versions of ONE verification call. These controls assert outcomes only and use
//! nothing ADR 0128 adds, so they run unchanged on the base (the oracle) and on the candidate.
//! Each prints the physical frame reads its verification made (the L1 read histogram's count).

use super::*;

/// Physical local frame reads so far, as the scrape renders the L1 read histogram's count.
fn l1_reads() -> u64 {
	crate::metrics::global()
		.render_prometheus()
		.lines()
		.find_map(|line| line.strip_prefix("wendelon_l1_read_latency_seconds_count "))
		.and_then(|count| count.trim().parse().ok())
		.unwrap_or(0)
}

const ROWS: i64 = 48;

/// A holder's child of 48 rows, persisted and acknowledged: the store, the reservation, the
/// plan and its Complete intent.
async fn stored_child() -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	Arc<Reserved>,
	ChildPlan,
	crate::storage::index::parent_intents::ParentIntent,
) {
	let rows: Vec<(i64, &str)> = (1..=ROWS).map(|id| (id, "value")).collect();
	let r = reserved(&rows, 1);
	let (dir, slab) = manager().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	persist_child(
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		admitted(&slab, planned.clone()).await,
		authority(&r, ChildRole::Holder),
	)
	.await
	.expect("the child persists");
	let intent = slab
		.persistent_index()
		.unwrap()
		.parent_intent(r.rsv, 0)
		.await
		.unwrap()
		.expect("the child's intent");
	assert_eq!(intent.manifest.len(), ROWS as usize);
	(dir, slab, Arc::new(r), planned, intent)
}

/// The child's copies and distinct physical frames, and the longest run of one frame in
/// manifest order.
fn shape(
	slab: &SlabManager,
	intent: &crate::storage::index::parent_intents::ParentIntent,
) -> (usize, usize) {
	let mut frames = std::collections::BTreeSet::new();
	let mut copies = 0;
	for version in &intent.manifest {
		for location in slab
			.version_locations(TENANT, Identity::new(version.key_identity), version.hlc)
			.unwrap()
		{
			copies += 1;
			frames.insert((location.slab_id, location.offset, location.length));
		}
	}
	(copies, frames.len())
}

async fn verify(
	slab: &Arc<SlabManager>,
	r: &Reserved,
	intent: &crate::storage::index::parent_intents::ParentIntent,
	wait: RetirementWait,
) -> (Result<(), ChildRefusal>, u64, std::time::Duration) {
	let index = slab.persistent_index().unwrap();
	let before = l1_reads();
	let started = std::time::Instant::now();
	let result = verify_owned_rows_waiting(slab, &index, intent, &r.descriptor, wait).await;
	(result, l1_reads() - before, started.elapsed())
}

fn refusal(result: &Result<(), ChildRefusal>) -> (&'static str, String) {
	match result {
		Ok(()) => ("verified", String::new()),
		Err(ChildRefusal::Unverified(text)) => ("Unverified", text.clone()),
		Err(other) => ("other", format!("{other:?}")),
	}
}

/// The stable child verifies; its copies share frames (so a frame is visited more than once).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stable_child_verifies_and_its_copies_share_frames() {
	let (_dir, slab, r, _planned, intent) = stored_child().await;
	let (copies, frames) = shape(&slab, &intent);
	let (result, reads, _) = verify(&slab, &r, &intent, RetirementWait::NONE).await;
	eprintln!(
		"ADR0128-SEM stable: copies {copies} frames {frames} reads {reads} -> {:?}",
		refusal(&result)
	);
	assert!(result.is_ok(), "{result:?}");
	assert_eq!(copies, ROWS as usize);
	assert!(frames < copies, "copies share frames: {frames} frames");
}

/// Copies of one version, as the store allows them.
/// - A second copy with the SAME content is stored; the call still checks every copy and verifies.
/// - A second copy with OTHER content cannot be stored at all: the store's own conflict check
///   refuses the append (`ConflictingVersion`), so no verifier ever meets one through this path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_copy_is_checked_and_a_differing_one_is_refused_by_the_store() {
	for row in [0usize, (ROWS / 2) as usize, (ROWS - 1) as usize] {
		let (_dir, slab, r, planned, intent) = stored_child().await;
		let id = row as i64 + 1;
		let append = |content: &'static str| {
			let (slab, planned, r) = (slab.clone(), planned.clone(), r.clone());
			async move {
				let rows = batch(&r.descriptor, &[(id, content)]);
				let bifurcated = bifurcate_child(&slab, &planned, &rows, &[row as u32]).unwrap();
				append_table_children_durable(
					&slab,
					TENANT,
					planned.ack_hlc(),
					&to_replica_children(&bifurcated),
					&planned.secondary_indexes,
				)
				.await
			}
		};
		let differing = append("OTHER").await;
		let text = format!("{differing:?}");
		assert!(text.contains("ConflictingVersion"), "{text}");
		append("value")
			.await
			.expect("an identical second copy is stored");
		let (copies, frames) = shape(&slab, &intent);
		let (result, reads, _) = verify(&slab, &r, &intent, RetirementWait::NONE).await;
		eprintln!(
			"ADR0128-SEM identical second copy of row {row}: copies {copies} frames {frames} reads {reads} -> {:?}; a differing one: {text}",
			refusal(&result)
		);
		assert_eq!(copies, ROWS as usize + 1, "one version has two copies");
		assert!(result.is_ok(), "{result:?}");
	}
}

/// One version stored with OTHER content before its child is persisted: the scan finds it
/// stored, the other rows are appended, and the verification refuses the child as a differing
/// copy, after verifying the versions before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_stored_with_other_bytes_refuses_the_child_as_a_differing_copy() {
	let rows: Vec<(i64, &str)> = (1..=ROWS).map(|id| (id, "value")).collect();
	let r = reserved(&rows, 1);
	let (_dir, slab) = manager().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	slab.persistent_index()
		.unwrap()
		.begin_parent_intent(planned.intent.clone())
		.await
		.unwrap()
		.unwrap();
	let row = (ROWS - 1) as usize;
	let other = batch(&r.descriptor, &[(ROWS, "OTHER")]);
	let bifurcated = bifurcate_child(&slab, &planned, &other, &[row as u32]).unwrap();
	append_table_children_durable(
		&slab,
		TENANT,
		planned.ack_hlc(),
		&to_replica_children(&bifurcated),
		&planned.secondary_indexes,
	)
	.await
	.unwrap();
	let before = l1_reads();
	let result = persist_child(
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		admitted(&slab, planned).await,
		authority(&r, ChildRole::Holder),
	)
	.await;
	let outcome = match &result {
		Ok(_) => ("acknowledged", String::new()),
		Err(ChildRefusal::Unverified(text)) => ("Unverified", text.clone()),
		Err(other) => ("other", format!("{other:?}")),
	};
	eprintln!(
		"ADR0128-SEM other bytes stored first: reads in the whole persist {} -> {outcome:?}",
		l1_reads() - before
	);
	assert_eq!(
		outcome,
		(
			"Unverified",
			"a stored copy differs from the manifest".to_owned()
		)
	);
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Intent)
	);
}

/// A retirement fence raised between two versions of one call (after half the manifest was
/// verified): the next version's own admission refuses, with no wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_raised_between_versions_refuses_at_the_next_versions_admission() {
	let (_dir, slab, r, _planned, intent) = stored_child().await;
	let fence = SlabId::new(0x7700);
	assert_ne!(slab.active_slab_id().await, fence);
	let (reached, release) = verify_pause::arm(&slab, (ROWS / 2) as usize);
	let task = {
		let (slab, r, intent) = (slab.clone(), r.clone(), intent.clone());
		tokio::spawn(async move { verify(&slab, &r, &intent, RetirementWait::NONE).await })
	};
	tokio::time::timeout(std::time::Duration::from_secs(30), reached.notified())
		.await
		.expect("the verifier pauses between versions");
	slab.raise_retirement_fence_for_test(fence);
	release.notify_one();
	let (result, reads, elapsed) = task.await.unwrap();
	slab.clear_retirement_fence_for_test(fence);
	eprintln!(
		"ADR0128-SEM fence between versions, no wait: reads {reads} elapsed_ms {} -> {:?}",
		elapsed.as_millis(),
		refusal(&result)
	);
	let (kind, text) = refusal(&result);
	assert_eq!(kind, "Unverified", "{text}");
	assert!(text.contains("fenced"), "{text}");
	// Cleared, the same intent verifies again.
	let (again, _, _) = verify(&slab, &r, &intent, RetirementWait::NONE).await;
	assert!(again.is_ok(), "{again:?}");
}

/// The same fence with a holder's wait: it is waited out only until the inherited absolute
/// deadline, then refused; cleared within the deadline, the call verifies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_between_versions_is_waited_only_to_the_inherited_deadline() {
	let budget = std::time::Duration::from_millis(600);
	// Never cleared: refused, and not after the deadline by more than scheduling.
	{
		let (_dir, slab, r, _planned, intent) = stored_child().await;
		let fence = SlabId::new(0x7700);
		let (reached, release) = verify_pause::arm(&slab, (ROWS / 2) as usize);
		let end = tokio::time::Instant::now() + budget;
		let task = {
			let (slab, r, intent) = (slab.clone(), r.clone(), intent.clone());
			tokio::spawn(
				async move { verify(&slab, &r, &intent, RetirementWait::until(end)).await },
			)
		};
		tokio::time::timeout(std::time::Duration::from_secs(30), reached.notified())
			.await
			.expect("the verifier pauses between versions");
		slab.raise_retirement_fence_for_test(fence);
		release.notify_one();
		let (result, reads, _) = task.await.unwrap();
		let late = tokio::time::Instant::now().saturating_duration_since(end);
		eprintln!(
			"ADR0128-SEM fence held past the deadline: reads {reads} ended_ms_after_deadline {} waits {} -> {:?}",
			late.as_millis(),
			slab.holder_retirement_waits_for_test(),
			refusal(&result)
		);
		assert_eq!(refusal(&result).0, "Unverified");
		assert!(late < std::time::Duration::from_secs(5), "{late:?}");
	}
	// Cleared inside the deadline: verified.
	{
		let (_dir, slab, r, _planned, intent) = stored_child().await;
		let fence = SlabId::new(0x7700);
		let (reached, release) = verify_pause::arm(&slab, (ROWS / 2) as usize);
		let end = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
		let task = {
			let (slab, r, intent) = (slab.clone(), r.clone(), intent.clone());
			tokio::spawn(
				async move { verify(&slab, &r, &intent, RetirementWait::until(end)).await },
			)
		};
		tokio::time::timeout(std::time::Duration::from_secs(30), reached.notified())
			.await
			.expect("the verifier pauses between versions");
		slab.raise_retirement_fence_for_test(fence);
		release.notify_one();
		tokio::time::sleep(std::time::Duration::from_millis(200)).await;
		slab.clear_retirement_fence_for_test(fence);
		let (result, reads, elapsed) = task.await.unwrap();
		eprintln!(
			"ADR0128-SEM fence cleared inside the deadline: reads {reads} elapsed_ms {} waits {} -> {:?}",
			elapsed.as_millis(),
			slab.holder_retirement_waits_for_test(),
			refusal(&result)
		);
		assert!(result.is_ok(), "{result:?}");
	}
}

/// The rows' slab sealed between two versions of one call: the same local backing, so the
/// call still verifies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_between_versions_leaves_the_call_verifying() {
	let (_dir, slab, r, _planned, intent) = stored_child().await;
	let (reached, release) = verify_pause::arm(&slab, (ROWS / 2) as usize);
	let task = {
		let (slab, r, intent) = (slab.clone(), r.clone(), intent.clone());
		tokio::spawn(async move { verify(&slab, &r, &intent, RetirementWait::NONE).await })
	};
	tokio::time::timeout(std::time::Duration::from_secs(30), reached.notified())
		.await
		.expect("the verifier pauses between versions");
	let sealed = slab.force_seal_active_for_test().await;
	release.notify_one();
	let (result, reads, _) = task.await.unwrap();
	eprintln!(
		"ADR0128-SEM sealed {sealed:?} between versions: reads {reads} -> {:?}",
		refusal(&result)
	);
	assert!(result.is_ok(), "{result:?}");
}

/// The single location of the manifest version at `position`.
fn location_of(
	slab: &SlabManager,
	intent: &crate::storage::index::parent_intents::ParentIntent,
	position: usize,
) -> crate::storage::RecordLocation {
	let version = &intent.manifest[position];
	let locations = slab
		.version_locations(TENANT, Identity::new(version.key_identity), version.hlc)
		.unwrap();
	assert_eq!(locations.len(), 1, "premise: one copy");
	locations[0]
}

/// Manifest positions whose rows share one physical frame with `position`'s, and one that
/// does not.
fn frame_mates(
	slab: &SlabManager,
	intent: &crate::storage::index::parent_intents::ParentIntent,
	position: usize,
) -> (usize, usize) {
	let at = location_of(slab, intent, position);
	let mut same = None;
	let mut other = None;
	for candidate in 0..intent.manifest.len() {
		if candidate == position {
			continue;
		}
		let location = location_of(slab, intent, candidate);
		if (location.slab_id, location.offset) == (at.slab_id, at.offset) {
			same.get_or_insert(candidate);
		} else {
			other.get_or_insert(candidate);
		}
	}
	(
		same.expect("a row in the same frame"),
		other.expect("a row in another frame"),
	)
}

/// A second copy that holds OTHER content, listed by the index (the write path refuses to
/// store one, so the listing is planted): the location of another row, in the same frame and
/// in another frame, listed first and last. The call refuses the child as a differing copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listed_second_copy_with_other_content_refuses_as_a_differing_copy() {
	for position in [0usize, (ROWS / 2) as usize, (ROWS - 1) as usize] {
		for (same_frame, first) in [(true, false), (true, true), (false, false), (false, true)] {
			let (_dir, slab, r, _planned, intent) = stored_child().await;
			let (same, other) = frame_mates(&slab, &intent, position);
			let source = if same_frame { same } else { other };
			let mut bad = location_of(&slab, &intent, source);
			let version = &intent.manifest[position];
			bad.hlc = version.hlc;
			slab.list_location_for_test(TENANT, Identity::new(version.key_identity), bad, first);
			let (result, reads, _) = verify(&slab, &r, &intent, RetirementWait::NONE).await;
			eprintln!(
				"ADR0128-SEM bad second copy at {position} (same frame {same_frame}, listed first {first}): reads {reads} -> {:?}",
				refusal(&result)
			);
			assert_eq!(
				refusal(&result),
				(
					"Unverified",
					"a stored copy differs from the manifest".to_owned()
				)
			);
		}
	}
}

/// One version whose manifest digest no stored copy matches (an earlier digest mismatch) and
/// whose other listed copy cannot be read (a location that is not a frame). Every copy of a
/// version is read before any is compared, so the read's refusal is the one reported, whichever
/// copy is listed first; without the unreadable copy the same version refuses as a differing
/// copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_error_on_one_copy_is_reported_before_another_copys_mismatch() {
	let position = (ROWS / 2) as usize;
	// The mismatch alone.
	{
		let (_dir, slab, r, _planned, mut intent) = stored_child().await;
		intent.manifest[position].row_digest[0] ^= 0xFF;
		let (result, reads, _) = verify(&slab, &r, &intent, RetirementWait::NONE).await;
		eprintln!(
			"ADR0128-SEM digest mismatch alone: reads {reads} -> {:?}",
			refusal(&result)
		);
		assert_eq!(
			refusal(&result),
			(
				"Unverified",
				"a stored copy differs from the manifest".to_owned()
			)
		);
	}
	for (mismatch, first) in [(true, false), (true, true), (false, false), (false, true)] {
		let (_dir, slab, r, _planned, mut intent) = stored_child().await;
		if mismatch {
			intent.manifest[position].row_digest[0] ^= 0xFF;
		}
		let version = intent.manifest[position];
		let mut unreadable = location_of(&slab, &intent, position);
		unreadable.offset += 8;
		slab.list_location_for_test(
			TENANT,
			Identity::new(version.key_identity),
			unreadable,
			first,
		);
		let (result, reads, _) = verify(&slab, &r, &intent, RetirementWait::NONE).await;
		let (kind, text) = refusal(&result);
		eprintln!(
			"ADR0128-SEM unreadable copy (digest mismatch {mismatch}, listed first {first}): reads {reads} -> {kind}: {text}"
		);
		assert_eq!(kind, "Unverified", "{text}");
		assert_eq!(
			text, "slab record magic mismatch",
			"the read's refusal, not the comparison's"
		);
	}
}
