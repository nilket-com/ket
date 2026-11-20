//! ADR 0106 (wendelon): one page of the certified base for a recovering
//! replacement (`TABLE_BASE_FETCH`).
//!
//! The request carries the replacement's identity (token, instance, birth,
//! store), the certificate (gid, digest), a cursor and its bounds. A member
//! other than the replacement answers only when that identity is the slot's
//! CURRENT promoted replacement, the certificate is the current one, and its
//! own base is active at that gid, validated and not behind. A page is the
//! next entries in key order after the cursor, of one (tenant, table), with
//! the rows at exactly those versions (read through to this member's own
//! cold objects when needed) and the next cursor, or none at the end. The
//! receiver checks every page (`check_page`) and completeness at publication
//! (`publish_recovered`: the streamed sums equal the certificate's, counts
//! included). Nothing here writes.

use super::*;
use crate::consensus::StoreIncarnation;
use crate::consensus::decisions::instances::Birth;
use crate::storage::index::certified_base::BaseEntry;

/// Test-only: how a misbehaving source answers.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Tamper {
	/// Every page carries one fabricated extra entry.
	ExtraEntry,
	/// A page with rows keeps only its first entry and claims to be the last.
	ShortRows,
}

/// Test-only: stores (by data directory, unique per test cluster) that answer
/// base pages tampered.
#[cfg(test)]
pub(crate) static TAMPERED_SOURCES: std::sync::Mutex<
	std::collections::BTreeMap<std::path::PathBuf, Tamper>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Test-only: the tamper of the store at `dir`, if any.
#[cfg(test)]
fn tamper_of(dir: &std::path::Path) -> Option<Tamper> {
	TAMPERED_SOURCES
		.lock()
		.unwrap()
		.iter()
		.find(|(tampered, _)| dir.starts_with(tampered))
		.map(|(_, tamper)| *tamper)
}

/// Test-only: how many pages a tampered store has answered with its extra.
#[cfg(test)]
pub(crate) static TAMPERED_PAGES: std::sync::atomic::AtomicUsize =
	std::sync::atomic::AtomicUsize::new(0);

/// The most entries one page carries.
pub(crate) const MAX_PAGE_ROWS: u32 = 1024;
/// The bytes one page normally carries: its answer meta plus its encoded Arrow
/// payload, as sent.
pub(crate) const MAX_PAGE_BYTES: u32 = 4 << 20;
/// The largest page a request may ask for: one child's row bound
/// (`MAX_CHILD_ROWS_BYTES`), so any single stored row fits a page by itself. A
/// row larger than the page asked for is refused `RowTooLarge`, never sent over
/// the bound, and asked again at this bound.
pub(crate) const MAX_ROW_PAGE_BYTES: u32 = super::child_writes::MAX_CHILD_ROWS_BYTES as u32;
/// What the answer's meta may add beyond its rows' bytes, in the receiver's
/// check of the frame before it is read.
const META_ALLOWANCE: u32 = 1 << 20;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct BaseFetchRequest {
	pub token: u128,
	pub r_new: u32,
	pub birth: Birth,
	pub store: StoreIncarnation,
	pub gid: u64,
	pub target_digest: [u8; 32],
	/// The last base key received (56 bytes), or none for the first page.
	pub after: Option<Vec<u8>>,
	pub max_rows: u32,
	pub max_bytes: u32,
	/// Phase 1 asks for the entries only, to be checked whole against the
	/// certificate before any row is stored; phase 2 asks again with rows.
	pub with_rows: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum BaseFetchRefusal {
	/// Not the slot's current promoted replacement.
	NotAuthorized,
	/// Not the current certificate.
	WrongGeneration,
	/// This member's base is not active at that gid, validated and ahead.
	NotReady,
	/// The request does not decode or exceeds its bounds.
	Malformed,
	/// The next row alone exceeds the page's byte bound; ask again with a
	/// larger one (at most `MAX_ROW_PAGE_BYTES`).
	RowTooLarge,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum BaseFetchAnswer {
	/// Entries in key order after the cursor, all of one (tenant, table); the
	/// rows of its non-tombstone entries, in order, are the frame's Arrow
	/// payload. `next` is the cursor to ask from, none at the end.
	Page {
		entries: Vec<BaseEntry>,
		next: Option<Vec<u8>>,
	},
	Refused(BaseFetchRefusal),
}

impl WendelonDataServer {
	pub(super) async fn handle_base_fetch(
		&self,
		sender: u32,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (answer, rows) = match postcard::from_bytes::<BaseFetchRequest>(frame.meta.as_ref()) {
			Err(_) => (BaseFetchAnswer::Refused(BaseFetchRefusal::Malformed), None),
			Ok(request) => match self.serve_base_page(sender, request).await {
				Ok((answer, rows)) => (answer, rows),
				Err(refusal) => (BaseFetchAnswer::Refused(refusal), None),
			},
		};
		let bytes = postcard::to_stdvec(&answer).map_err(Error::other)?;
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(&bytes);
		let arrow_payload = rows.map(|rows| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&rows);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
				msg_type: MsgType::TABLE_BASE_FETCH_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: arrow_payload.as_ref().map_or(0, |buf| buf.len() as u32),
				trailing_len: 0,
			},
			meta,
			arrow_payload,
			trailing: None,
		})
	}

	async fn serve_base_page(
		&self,
		sender: u32,
		request: BaseFetchRequest,
	) -> Result<(BaseFetchAnswer, Option<Vec<u8>>), BaseFetchRefusal> {
		use BaseFetchRefusal::*;
		if request.max_rows == 0
			|| request.max_rows > MAX_PAGE_ROWS
			|| request.max_bytes == 0
			|| request.max_bytes > MAX_ROW_PAGE_BYTES
		{
			return Err(Malformed);
		}
		let after: Option<[u8; 56]> = match &request.after {
			None => None,
			Some(after) => Some(after.as_slice().try_into().map_err(|_| Malformed)?),
		};
		let (token, r_new, birth, store) =
			(request.token, request.r_new, request.birth, request.store);
		let (authorized, current) = self
			.state_machine
			.read_state(move |state| {
				let authorized = crate::consensus::decisions::current_replacement(
					state, sender, token, r_new, birth, store,
				);
				let current = state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.generations().current.clone())
					.map(|certificate| (certificate.gid, certificate.target_digest));
				(authorized, current)
			})
			.await;
		if !authorized || sender == self.local_node.id {
			return Err(NotAuthorized);
		}
		if current != Some((request.gid, request.target_digest)) {
			return Err(WrongGeneration);
		}
		let ready = self
			.decisions
			.base_validated
			.load(std::sync::atomic::Ordering::Acquire)
			&& !self
				.decisions
				.behind_boundary
				.load(std::sync::atomic::Ordering::Acquire);
		let index = self.slab.persistent_index().ok_or(NotReady)?;
		let meta = index.base_meta().await.map_err(|_| NotReady)?;
		if !ready || meta.active_gid != request.gid {
			return Err(NotReady);
		}
		let mut candidates = index
			.base_page(after, request.max_rows as usize + 1)
			.await
			.map_err(|_| NotReady)?;
		let more = candidates.len() > request.max_rows as usize;
		candidates.truncate(request.max_rows as usize);
		let Some(first) = candidates.first().copied() else {
			return Ok((
				BaseFetchAnswer::Page {
					entries: Vec::new(),
					next: None,
				},
				None,
			));
		};
		let (tenant, table) = (first.tenant, first.table);
		let descriptor = self
			.state_machine
			.read_state(move |state| {
				super::child_writes::table_descriptor(state, tenant, table, 0)
					.or_else(|| state.table_schemas.latest_by_id(tenant, table).cloned())
			})
			.await
			.ok_or(NotReady)?;
		// The charge is what is sent: the answer's meta plus the encoded Arrow
		// payload. Rows are measured by their own encoding (a concatenated batch
		// is no larger than the sum of its rows' encodings).
		let mut entries = Vec::new();
		let mut rows = Vec::new();
		let mut row_bytes = 0_u64;
		for entry in &candidates {
			if (entry.tenant, entry.table) != (tenant, table) {
				break;
			}
			if request.with_rows && !entry.tombstone {
				let stored = self
					.slab
					.stored_rows_at_version(
						entry.tenant,
						Identity::new(entry.key_identity),
						entry.hlc,
						None,
					)
					.await
					.map_err(|_| NotReady)?;
				let row = stored.first().ok_or(NotReady)?;
				let projected = super::child_writes::stored_row_projected(&descriptor, row)
					.map_err(|_| NotReady)?;
				let encoded = crate::storage::multirow::encode_batch(&projected)
					.map_err(|_| NotReady)?
					.len() as u64;
				if row_bytes + encoded > u64::from(request.max_bytes) {
					if entries.is_empty() {
						return Err(RowTooLarge);
					}
					break;
				}
				row_bytes += encoded;
				rows.push(projected);
			}
			entries.push(*entry);
		}
		#[allow(unused_mut)]
		let mut next = if more || entries.len() < candidates.len() {
			entries.last().map(|entry| entry.key().to_vec())
		} else {
			None
		};
		#[cfg(test)]
		if request.with_rows
			&& entries.len() > 1
			&& tamper_of(self.slab.data_dir()) == Some(Tamper::ShortRows)
		{
			// A misbehaving source: the first entry only, said to be the last.
			let kept = usize::from(!entries[0].tombstone);
			entries.truncate(1);
			rows.truncate(kept);
			next = None;
			TAMPERED_PAGES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		}
		let payload = if rows.is_empty() {
			None
		} else {
			let schema = Arc::new(descriptor.arrow_schema().map_err(|_| NotReady)?);
			let batch = arrow::compute::concat_batches(&schema, &rows).map_err(|_| NotReady)?;
			Some(crate::storage::multirow::encode_batch(&batch).map_err(|_| NotReady)?)
		};
		#[cfg(test)]
		if tamper_of(self.slab.data_dir()) == Some(Tamper::ExtraEntry)
			&& let Some(last) = entries.last().copied()
		{
			// A misbehaving source: one fabricated extra entry, placed and
			// ordered so it passes the page checks.
			let mut extra = last;
			extra.key_identity = [0xFF; 32];
			extra.tombstone = true;
			extra.row_digest = [0; 32];
			extra.position = crate::consensus::decisions::delete_row_position(
				extra.tenant,
				&descriptor,
				Identity::new(extra.key_identity),
			)
			.unwrap_or(extra.position);
			if extra.key() > last.key() {
				TAMPERED_PAGES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
				entries.push(extra);
				if next.is_some() {
					next = Some(extra.key().to_vec());
				}
			}
		}
		let answer = BaseFetchAnswer::Page { entries, next };
		let meta = postcard::to_stdvec(&answer).map_err(|_| NotReady)?.len() as u64;
		let sent = meta + payload.as_ref().map_or(0, |payload| payload.len() as u64);
		if sent > u64::from(request.max_bytes) + u64::from(META_ALLOWANCE) {
			return Err(RowTooLarge);
		}
		Ok((answer, payload))
	}
}

/// ADR 0106 (wendelon): the most bytes the receiver reads for an answer to
/// `request`, checked from the frame header before its body is read.
pub(crate) fn answer_bound(request: &BaseFetchRequest) -> u64 {
	u64::from(request.max_bytes) + u64::from(META_ALLOWANCE)
}

/// Why a received page is not kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PageRefusal {
	OutOfOrder,
	OverBound,
	MixedTables,
	RowsMismatch(String),
	PositionMismatch,
	/// An empty page that is not the end.
	EmptyNotEnd,
}

/// The receiver's check of one page against its request, before any of it is
/// stored: keys ascend strictly after the cursor; the page is within its row
/// bound and of one (tenant, table); the rows are exactly the non-tombstone
/// entries' (identity and content digest); every position is the one the
/// row's identity places it at. Completeness and the chunk sums are checked at
/// publication.
pub(crate) fn check_page(
	request: &BaseFetchRequest,
	entries: &[BaseEntry],
	next: &Option<Vec<u8>>,
	rows: Option<&RecordBatch>,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), PageRefusal> {
	if entries.len() > request.max_rows as usize {
		return Err(PageRefusal::OverBound);
	}
	if entries.is_empty() {
		return if next.is_none() && rows.is_none() {
			Ok(())
		} else {
			Err(PageRefusal::EmptyNotEnd)
		};
	}
	let mut previous: Option<[u8; 56]> = match &request.after {
		None => None,
		Some(after) => Some(
			after
				.as_slice()
				.try_into()
				.map_err(|_| PageRefusal::OutOfOrder)?,
		),
	};
	let (tenant, table) = (entries[0].tenant, entries[0].table);
	for entry in entries {
		if (entry.tenant, entry.table) != (tenant, table) || table != descriptor.table_object_id {
			return Err(PageRefusal::MixedTables);
		}
		let key = entry.key();
		if previous.is_some_and(|previous| key <= previous) {
			return Err(PageRefusal::OutOfOrder);
		}
		previous = Some(key);
		let placed = crate::consensus::decisions::delete_row_position(
			tenant,
			descriptor,
			Identity::new(entry.key_identity),
		);
		if placed != Some(entry.position) {
			return Err(PageRefusal::PositionMismatch);
		}
	}
	if let Some(next) = next
		&& Some(next.as_slice()) != previous.as_ref().map(|key| key.as_slice())
	{
		return Err(PageRefusal::OutOfOrder);
	}
	if !request.with_rows {
		return if rows.is_none() {
			Ok(())
		} else {
			Err(PageRefusal::RowsMismatch(
				"rows in an entries-only page".into(),
			))
		};
	}
	let wanted: Vec<&BaseEntry> = entries.iter().filter(|entry| !entry.tombstone).collect();
	let received = rows.map_or(0, RecordBatch::num_rows);
	if received != wanted.len() {
		return Err(PageRefusal::RowsMismatch(format!(
			"{received} rows for {} entries",
			wanted.len()
		)));
	}
	let Some(rows) = rows else {
		return Ok(());
	};
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	let hlc = wanted[0].hlc;
	meta.client_hlc = hlc;
	meta.row_count = crate::schema::table::encode_table_write_row_count(rows.num_rows())
		.map_err(|error| PageRefusal::RowsMismatch(error.to_string()))?;
	// Row i must be entry i: the rows are paired with their entries by position,
	// and each entry's HLC is assigned to its own row at ingestion. A permutation
	// is refused here, before anything is stored.
	// Wendelon ADR 0119 D3: an append-only table's rows are paired with their
	// verified entries' identities; their payload digests are checked below.
	let identities: Vec<Identity> = wanted
		.iter()
		.map(|entry| Identity::new(entry.key_identity))
		.collect();
	let keys = if matches!(
		descriptor.row_identity_policy,
		crate::schema::RowIdentityPolicy::AppendOnly
	) {
		super::child_writes::RowKeys::Identities(&identities)
	} else {
		super::child_writes::RowKeys::Values
	};
	let versions = super::child_writes::row_versions_keyed(&meta, rows, descriptor, hlc, keys)
		.map_err(|error| PageRefusal::RowsMismatch(format!("{error:?}")))?;
	for (version, entry) in versions.iter().zip(wanted) {
		if (version.key_identity, version.row_digest) != (entry.key_identity, entry.row_digest) {
			return Err(PageRefusal::RowsMismatch(
				"a row is not its entry's identity and content, in order".into(),
			));
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	//! ADR 0106 (wendelon) control 3: what a received base page must be before
	//! any of it is stored. A page that is not is refused whole.

	use super::*;
	use arrow::array::{Int64Array, StringArray};

	fn descriptor() -> TableSchemaDescriptor {
		crate::consensus::decisions::tests::fixture_table()
	}

	fn rows(ids: &[i64]) -> RecordBatch {
		let descriptor = descriptor();
		RecordBatch::try_new(
			Arc::new(descriptor.arrow_schema().unwrap()),
			vec![
				Arc::new(Int64Array::from(ids.to_vec())),
				Arc::new(StringArray::from(
					ids.iter().map(|id| format!("v{id}")).collect::<Vec<_>>(),
				)),
			],
		)
		.unwrap()
	}

	const HLC: crate::schema::HybridTimestamp = crate::schema::HybridTimestamp {
		physical_ms: 1_800_000_000_000,
		logical: 1,
		node_id: 1,
	};

	/// The entries a correct source sends for `batch`, in key order, with the
	/// rows reordered to match.
	fn page(ids: &[i64]) -> (Vec<BaseEntry>, RecordBatch) {
		let descriptor = descriptor();
		let tenant = TenantId::ZERO;
		let mut entries: Vec<(BaseEntry, i64)> = ids
			.iter()
			.map(|id| {
				let one = rows(&[*id]);
				let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
				meta.tenant_id = tenant;
				meta.table_object_id = descriptor.table_object_id;
				meta.schema_generation = descriptor.schema_generation;
				meta.client_hlc = HLC;
				meta.row_count = crate::schema::table::encode_table_write_row_count(1).unwrap();
				let version =
					super::super::child_writes::child_manifest(&meta, &one, &descriptor, HLC)
						.unwrap()[0];
				let position = crate::consensus::decisions::delete_row_position(
					tenant,
					&descriptor,
					Identity::new(version.key_identity),
				)
				.unwrap();
				(
					BaseEntry {
						tenant,
						table: descriptor.table_object_id,
						key_identity: version.key_identity,
						hlc: HLC,
						row_digest: version.row_digest,
						tombstone: false,
						position,
					},
					*id,
				)
			})
			.collect();
		entries.sort_by_key(|(entry, _)| entry.key());
		let ordered: Vec<i64> = entries.iter().map(|(_, id)| *id).collect();
		(
			entries.into_iter().map(|(entry, _)| entry).collect(),
			rows(&ordered),
		)
	}

	fn request(after: Option<Vec<u8>>, max_rows: u32) -> BaseFetchRequest {
		BaseFetchRequest {
			token: 1,
			r_new: 2,
			birth: [3; 16],
			store: StoreIncarnation::from_raw(4),
			gid: 5,
			target_digest: [6; 32],
			after,
			max_rows,
			max_bytes: MAX_PAGE_BYTES,
			with_rows: true,
		}
	}

	#[test]
	fn a_checked_page_is_kept_and_every_defect_refuses_it_whole() {
		let descriptor = descriptor();
		let (entries, batch) = page(&[1, 2, 3]);
		let last = Some(entries[2].key().to_vec());
		assert_eq!(
			check_page(
				&request(None, 1024),
				&entries,
				&None,
				Some(&batch),
				&descriptor
			),
			Ok(())
		);
		assert_eq!(
			check_page(
				&request(None, 1024),
				&entries,
				&last,
				Some(&batch),
				&descriptor
			),
			Ok(()),
			"a page that continues names its last key"
		);
		// Over its row bound.
		assert_eq!(
			check_page(
				&request(None, 2),
				&entries,
				&None,
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::OverBound)
		);
		// Not after the cursor, or not ascending.
		assert_eq!(
			check_page(
				&request(Some(entries[1].key().to_vec()), 1024),
				&entries,
				&None,
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::OutOfOrder)
		);
		let mut swapped = entries.clone();
		swapped.swap(0, 1);
		assert_eq!(
			check_page(
				&request(None, 1024),
				&swapped,
				&None,
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::OutOfOrder)
		);
		// A continuation cursor that is not the page's last key.
		assert_eq!(
			check_page(
				&request(None, 1024),
				&entries,
				&Some(entries[0].key().to_vec()),
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::OutOfOrder)
		);
		// Another table in the page.
		let mut mixed = entries.clone();
		mixed[2].table = crate::schema::TableObjectId::DEFAULT_KV;
		assert_eq!(
			check_page(
				&request(None, 1024),
				&mixed,
				&None,
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::MixedTables)
		);
		// A position the identity does not place.
		let mut placed = entries.clone();
		placed[1].position ^= 1;
		assert_eq!(
			check_page(
				&request(None, 1024),
				&placed,
				&None,
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::PositionMismatch)
		);
		// A missing row, an extra row, or a row whose content differs.
		let (_, short) = page(&[1, 2]);
		assert!(matches!(
			check_page(
				&request(None, 1024),
				&entries,
				&None,
				Some(&short),
				&descriptor
			),
			Err(PageRefusal::RowsMismatch(_))
		));
		let (_, long) = page(&[1, 2, 3, 4]);
		assert!(matches!(
			check_page(
				&request(None, 1024),
				&entries,
				&None,
				Some(&long),
				&descriptor
			),
			Err(PageRefusal::RowsMismatch(_))
		));
		let mut digest = entries.clone();
		digest[0].row_digest[0] ^= 0xFF;
		assert!(matches!(
			check_page(
				&request(None, 1024),
				&digest,
				&None,
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::RowsMismatch(_))
		));
		// An empty page is only the end, never a continuation or with rows.
		assert_eq!(
			check_page(&request(None, 1024), &[], &None, None, &descriptor),
			Ok(())
		);
		assert_eq!(
			check_page(&request(None, 1024), &[], &last, None, &descriptor),
			Err(PageRefusal::EmptyNotEnd)
		);
		// A tombstone carries no row.
		let mut tombstoned = entries.clone();
		tombstoned[1].tombstone = true;
		assert!(matches!(
			check_page(
				&request(None, 1024),
				&tombstoned,
				&None,
				Some(&batch),
				&descriptor
			),
			Err(PageRefusal::RowsMismatch(_))
		));
	}

	/// R2: rows are paired with entries by position. Two entries at different
	/// HLCs with their rows swapped are refused, so no row is stored under the
	/// other's version.
	#[test]
	fn a_page_whose_rows_are_not_in_entry_order_is_refused() {
		let descriptor = descriptor();
		let (mut entries, _) = page(&[1, 2]);
		entries[1].hlc = crate::schema::HybridTimestamp { logical: 2, ..HLC };
		// The rows in entry order are kept; reversed, refused.
		let ids: Vec<i64> = [1_i64, 2]
			.into_iter()
			.filter(|id| page(&[*id]).0[0].key_identity == entries[0].key_identity)
			.chain(
				[1_i64, 2]
					.into_iter()
					.filter(|id| page(&[*id]).0[0].key_identity == entries[1].key_identity),
			)
			.collect();
		let ordered = rows(&ids);
		let swapped = rows(&[ids[1], ids[0]]);
		assert_eq!(
			check_page(
				&request(None, 1024),
				&entries,
				&None,
				Some(&ordered),
				&descriptor
			),
			Ok(())
		);
		assert!(matches!(
			check_page(
				&request(None, 1024),
				&entries,
				&None,
				Some(&swapped),
				&descriptor
			),
			Err(PageRefusal::RowsMismatch(_))
		));
		// An entries-only page carries no rows.
		let entries_only = BaseFetchRequest {
			with_rows: false,
			..request(None, 1024)
		};
		assert_eq!(
			check_page(&entries_only, &entries, &None, None, &descriptor),
			Ok(())
		);
		assert!(matches!(
			check_page(&entries_only, &entries, &None, Some(&ordered), &descriptor),
			Err(PageRefusal::RowsMismatch(_))
		));
	}
}
