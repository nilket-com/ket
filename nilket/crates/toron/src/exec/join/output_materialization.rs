//! Gathering a join's selected rows into the output batch.
//!
//! Carved out of the join root by ADR 554's output-materialization line: run
//! tracking, the lazy index build, the zero-copy slice fast path and the
//! equivalence check that proves it are one body of work with one set of tests,
//! and they had grown to a third of the file that hosts them.
//!
//! Nothing here changed in the move. The carve exists so the next change to this
//! machinery is reviewed against its own module rather than against a join root
//! whose size bound had been raised twice to hold it.

use super::*;

/// Tracks whether one side's indices form an EXACT contiguous ascending run over
/// the whole selection — the only shape a zero-copy slice can serve in place of
/// `take` while still returning a single `RecordBatch`.
///
/// A long run is not the same thing. A selection can carry a multi-million-row
/// prefix run followed by a scatter tail; exploiting that prefix would need
/// multiple output batches, and concatenating them back restores the copy the
/// slice avoided. Only a selection contiguous end to end is a drop-in
/// substitution — an upper bound of roughly a fifth of right-side gather time
/// on the measured board, which the whole-source restriction below narrows
/// further by an amount only a run can report.
///
/// Contiguity is necessary but not sufficient: [`Self::slice_offset`] also
/// requires the run to cover the whole source batch, because a slice retains
/// the source's whole buffers. See its comment for why.
#[derive(Clone, Copy)]
pub(super) struct ContiguousRun {
	start: Option<u32>,
	next: u32,
	intact: bool,
}

impl ContiguousRun {
	fn new() -> Self {
		Self {
			start: None,
			next: 0,
			intact: true,
		}
	}

	fn observe(&mut self, index: Option<u32>) {
		if !self.intact {
			return;
		}
		// A null cannot be sliced: the output row needs a null, and a slice of the
		// source produces a value.
		let Some(index) = index else {
			self.intact = false;
			return;
		};
		match self.start {
			None => {
				self.start = Some(index);
				self.next = index.wrapping_add(1);
			}
			Some(_) if index == self.next => {
				// `checked_add` for the same reason the locality probe uses it: at
				// `u32::MAX` a saturating successor would let a repeat look contiguous.
				match self.next.checked_add(1) {
					Some(next) => self.next = next,
					None => self.intact = false,
				}
			}
			Some(_) => self.intact = false,
		}
	}

	/// The slice offset, when this side is contiguous over exactly `rows` rows and
	/// stays inside `source_rows`. `None` means `take` must run.
	fn resolve(self, rows: usize, source_rows: usize) -> Option<usize> {
		if !self.intact {
			return None;
		}
		let start = self.start?;
		// The run must span exactly the rows being gathered. Callers happen to
		// observe once per selection row, but this function slices on the answer
		// and a shorter or longer run would slice the wrong span with the right
		// row count — a silent wrong answer, so it is checked here rather than
		// assumed of the caller.
		let observed = usize::try_from(self.next.checked_sub(start)?).ok()?;
		if observed != rows {
			return None;
		}
		let start = usize::try_from(start).ok()?;
		// Bounds are checked, never assumed: a slice past the source panics, and
		// these indices arrive from a probe loop this function does not own.
		(start.checked_add(rows)? <= source_rows).then_some(start)
	}

	/// The offset this side may be sliced at, or `None` when `take` must run.
	///
	/// Narrower than [`Self::resolve`] on purpose: the run must also cover the
	/// WHOLE source batch. `Array::slice` retains the source's entire backing
	/// buffers, so a four-row slice of a million-row batch keeps the million
	/// rows alive while the materialization budget charges it four. Whole-source
	/// coverage makes the retention non-amplifying — the output holds exactly
	/// what the input already held — which is what lets this cut land without
	/// touching the budget.
	///
	/// A mid-batch contiguous run is therefore still gathered by `take`. Serving
	/// it is a later cut, and it needs retained-memory accounting first, not
	/// just this arithmetic.
	fn slice_offset(self, rows: usize, source_rows: usize) -> Option<usize> {
		let offset = self.resolve(rows, source_rows)?;
		(offset == 0 && rows == source_rows).then_some(offset)
	}
}

/// What a side's gather consumes.
///
/// `slice_offset` carries the fast path's decision, made once by
/// [`ContiguousRun`] rather than re-derived by the gather. `array` is absent
/// exactly when the deferral held to the end — the indices would have been
/// `0..rows` and nothing needed them.
pub(super) struct SideIndices {
	array: Option<UInt32Array>,
	slice_offset: Option<usize>,
}

/// Builds a side's index vector, deferring the allocation while the selection is
/// still walking the whole source in order.
///
/// The slice fast path reads a side's rows straight out of the source when its
/// selection is an exact whole-source run. Until now the index vector was built
/// anyway and then dropped unread. This builder defers that allocation: it holds
/// nothing while the run could still turn out whole-source, and materializes —
/// backfilling the rows it has already accepted — the moment that becomes
/// impossible.
///
/// **One pass, never two.** The fallback backfills from the run it has already
/// observed rather than rescanning the selection: a second pass would trade an
/// allocation for a re-walk and could cost more than it saves on exactly the
/// scattered selections that are already the expensive case.
///
/// The deferral is only available when the selection could cover the source
/// exactly — `rows == source_rows` — because that is what
/// [`ContiguousRun::slice_offset`] requires. Any other shape builds eagerly, so
/// the lazy path adds no branch to selections it can never serve.
pub(super) struct LazyIndexBuilder {
	run: ContiguousRun,
	storage: IndexStorage,
	/// The representation this side intends to build, decided by join kind before
	/// any row arrives — never by the rows observed, which is the same rule the
	/// eligible/control timing split follows.
	representation: IndexRepresentation,
	/// Rows accepted so far. While deferred these are exactly `0..accepted`.
	accepted: usize,
	rows: usize,
	source_rows: usize,
}

/// Which index vector a join kind's gather builds.
///
/// `Plain` is the substitution: a `Vec<u32>` and a null-free `UInt32Array`, half
/// the per-row storage and no null buffer. It is available only to kinds whose
/// arms cannot emit a null index — `Inner`, `Cross` and the four semi/anti kinds.
/// The outer kinds keep `Nullable` and are byte-identical to before this cut.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum IndexRepresentation {
	Plain,
	Nullable,
}

enum IndexStorage {
	/// No allocation yet: a whole-source run is still reachable.
	Deferred,
	Plain(Vec<u32>),
	Nullable(Vec<Option<u32>>),
}

/// What one side's build produced, with the row counts that make it checkable.
pub(super) struct SideBuild {
	pub(super) indices: SideIndices,
	/// Rows the deferral never materialized.
	pub(super) avoided: u64,
	/// Rows materialized into a vector, in either representation.
	pub(super) materialized: u64,
	/// Of those, the rows materialized as PLAIN indices.
	///
	/// Its own counter because `materialized` cannot identify a representation:
	/// the substitution changes how a row is stored, not whether it is stored, so
	/// a run without this number could not tell a working substitution from an
	/// inert one.
	pub(super) plain: u64,
}

/// The index-row counts a gather reports, over one side or both.
#[derive(Clone, Copy, Default)]
pub(super) struct IndexRows {
	pub(super) avoided: u64,
	pub(super) materialized: u64,
	pub(super) plain: u64,
}

impl SideBuild {
	/// This side's counts alone, for the semi and anti arms that gather one side.
	pub(super) fn rows(&self) -> IndexRows {
		IndexRows {
			avoided: self.avoided,
			materialized: self.materialized,
			plain: self.plain,
		}
	}

	/// Both sides' counts, for the arms that gather a pair.
	pub(super) fn rows_built(&self, other: &Self) -> IndexRows {
		IndexRows {
			avoided: self.avoided + other.avoided,
			materialized: self.materialized + other.materialized,
			plain: self.plain + other.plain,
		}
	}
}

impl LazyIndexBuilder {
	pub(super) fn new(
		rows: usize,
		source_rows: usize,
		deferrable: bool,
		representation: IndexRepresentation,
	) -> Self {
		let deferred = deferrable && rows == source_rows && rows > 0;
		Self {
			run: ContiguousRun::new(),
			storage: if deferred {
				IndexStorage::Deferred
			} else {
				Self::allocate(representation, rows)
			},
			representation,
			accepted: 0,
			rows,
			source_rows,
		}
	}

	fn allocate(representation: IndexRepresentation, rows: usize) -> IndexStorage {
		match representation {
			IndexRepresentation::Plain => IndexStorage::Plain(Vec::with_capacity(rows)),
			IndexRepresentation::Nullable => IndexStorage::Nullable(Vec::with_capacity(rows)),
		}
	}

	pub(super) fn push(&mut self, index: Option<u32>) {
		self.run.observe(index);
		if matches!(self.storage, IndexStorage::Deferred) {
			// Still deferred only while a whole-source run remains reachable: the
			// run has to be intact AND anchored at zero, since `slice_offset`
			// accepts no other offset. The check is on the run's own state rather
			// than on this row, so a null — which breaks the run without being a
			// wrong index — materializes here too.
			if self.run.intact && self.run.start == Some(0) {
				self.accepted += 1;
				return;
			}
			self.materialize();
		}
		self.push_materialized(index);
		self.accepted += 1;
	}

	fn push_materialized(&mut self, index: Option<u32>) {
		match (&mut self.storage, index) {
			(IndexStorage::Plain(indices), Some(index)) => indices.push(index),
			(IndexStorage::Nullable(indices), index) => indices.push(index),
			(IndexStorage::Plain(_), None) => {
				// A null reaching a plain side means the selection carried a fill
				// row for a kind whose arm is not supposed to produce one. Widen
				// rather than panic or error: the output must be exactly what the
				// nullable path would have produced, and a representation choice is
				// never worth a behaviour change. `an_eligible_side_that_meets_a_
				// null_widens_to_nullable` pins the values.
				self.widen_to_nullable();
				self.push_materialized(None);
			}
			(IndexStorage::Deferred, _) => {
				unreachable!("push_materialized runs only after materialize")
			}
		}
	}

	fn widen_to_nullable(&mut self) {
		let IndexStorage::Plain(plain) = &mut self.storage else {
			return;
		};
		let mut widened = Vec::with_capacity(self.rows);
		widened.extend(plain.iter().copied().map(Some));
		self.storage = IndexStorage::Nullable(widened);
		self.representation = IndexRepresentation::Nullable;
	}

	/// Allocate and backfill the rows accepted while deferred.
	///
	/// Those rows were accepted only while the run was intact from zero, so their
	/// indices were exactly `0..accepted` — the backfill reconstructs them
	/// without consulting the selection again. A wrong bound here would gather
	/// the wrong rows with the right count, which is why
	/// `a_broken_run_backfills_exactly_what_the_eager_builder_would` compares the
	/// result against eager construction rather than against a row count.
	fn materialize(&mut self) {
		self.storage = match self.representation {
			IndexRepresentation::Plain => {
				let mut indices = Vec::with_capacity(self.rows);
				indices.extend(0..self.accepted as u32);
				IndexStorage::Plain(indices)
			}
			IndexRepresentation::Nullable => {
				let mut indices = Vec::with_capacity(self.rows);
				indices.extend((0..self.accepted).map(|row| Some(row as u32)));
				IndexStorage::Nullable(indices)
			}
		};
	}

	/// Finish the side, reporting the indices and the row counts that let a run
	/// check what actually happened.
	///
	/// `avoided` and `materialized` are exclusive — a side is deferred or it is
	/// not — and `plain` is a subset of `materialized`, naming the representation.
	pub(super) fn finish(mut self) -> SideBuild {
		// One decision, made by the run's own arithmetic. The loop's bookkeeping
		// says only whether an allocation is still avoidable; whether a slice is
		// legal stays with `slice_offset`, including the whole-source restriction
		// that keeps the retention non-amplifying.
		let slice_offset = self.run.slice_offset(self.rows, self.source_rows);
		if matches!(self.storage, IndexStorage::Deferred) {
			if slice_offset.is_some() {
				return SideBuild {
					indices: SideIndices {
						array: None,
						slice_offset,
					},
					avoided: self.rows as u64,
					materialized: 0,
					plain: 0,
				};
			}
			self.materialize();
		}
		let (array, plain) = match self.storage {
			IndexStorage::Plain(indices) => (UInt32Array::from(indices), self.rows as u64),
			IndexStorage::Nullable(indices) => (UInt32Array::from(indices), 0),
			IndexStorage::Deferred => unreachable!("materialized above"),
		};
		SideBuild {
			indices: SideIndices {
				array: Some(array),
				slice_offset,
			},
			avoided: 0,
			materialized: self.rows as u64,
			plain,
		}
	}
}

/// Gather one side's columns, preferring a zero-copy slice over `take`.
///
/// Returns the rows the fast path served, so a caller can record whether it
/// actually fired rather than inferring it from a timing that moved.
///
/// When [`join_slice_equivalence_enabled`] is set, every column the fast path
/// serves is also gathered by `take` and the two are compared. That check is
/// opt-in for the same reason the locality probe is: it does the very work the
/// slice exists to avoid, so leaving it on would erase the effect being
/// measured. It is a correctness rail for a diagnostic run, not a guard.
pub(super) fn gather_side(
	batch: &RecordBatch,
	indices: &SideIndices,
	rows: usize,
	columns: &mut Vec<arrow::array::ArrayRef>,
) -> Result<u64, ExecutionError> {
	if let Some(offset) = indices.slice_offset {
		// The equivalence check needs the array the slice replaced, so enabling it
		// also disables the deferral (see `LazyIndexBuilder::new`'s caller). Both
		// halves of that arrangement are the same doctrine: a diagnostic that
		// performs the work it measures cannot also time the board, and it
		// withholds timing authority for exactly that reason.
		let verify = join_slice_equivalence_enabled();
		for (position, column) in batch.columns().iter().enumerate() {
			let sliced = column.slice(offset, rows);
			if verify && let Some(array) = indices.array.as_ref() {
				verify_slice_matches_take(position, &sliced, column, array)?;
			}
			columns.push(sliced);
		}
		return Ok(rows as u64);
	}
	let array = indices
		.array
		.as_ref()
		.expect("a side that cannot slice always materializes its indices");
	for column in batch.columns() {
		columns.push(arrow::compute::take(column, array, None)?);
	}
	Ok(0)
}

/// Whether a gather may defer its index-vector allocation.
///
/// False under the equivalence diagnostic, which compares each sliced column
/// against the `take` it replaced and therefore needs the very array the
/// deferral declines to build. That run measures correctness, not cost, and
/// already withholds timing authority — so paying for the array there costs
/// nothing this arc reads.
pub(super) fn index_deferral_available() -> bool {
	!join_slice_equivalence_enabled()
}

/// Environment variable enabling the slice/`take` equivalence check.
///
/// Opt-in, like the locality probe and for the same reason: the check gathers
/// every sliced column a second time with the `take` the slice replaced, so a
/// run with it on measures nothing about the optimization. Enable it to prove
/// the fast path is correct on real data; disable it to time the board.
pub const JOIN_SLICE_EQUIVALENCE_ENV: &str = "TORON_JOIN_SLICE_EQUIVALENCE";

/// Whether the slice/`take` equivalence check is enabled for this process.
pub fn join_slice_equivalence_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| std::env::var(JOIN_SLICE_EQUIVALENCE_ENV).is_ok())
}

/// Columns the equivalence check has compared and found equal in this process.
static SLICE_EQUIVALENCE_COLUMNS: std::sync::atomic::AtomicU64 =
	std::sync::atomic::AtomicU64::new(0);

/// How many columns the equivalence check has verified.
///
/// The positive control for the check itself. Without it a run that reported no
/// mismatch could not be told from a run where the fast path never fired, and
/// those two carry opposite conclusions.
pub fn join_slice_equivalence_columns_verified() -> u64 {
	SLICE_EQUIVALENCE_COLUMNS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Compare one sliced column against the `take` it replaced.
///
/// Fails the query rather than logging: this runs only under an explicit
/// diagnostic opt-in, where a mismatch means the fast path is producing wrong
/// rows and every later number in the run is worthless.
fn verify_slice_matches_take(
	position: usize,
	sliced: &arrow::array::ArrayRef,
	source: &arrow::array::ArrayRef,
	indices: &UInt32Array,
) -> Result<(), ExecutionError> {
	let taken = arrow::compute::take(source, indices, None)?;
	// `ArrayData`'s `PartialEq` is LOGICAL: it walks each array through its own
	// offset, so a slice starting mid-buffer and a freshly built `take` result
	// starting at zero compare equal when they carry the same rows. That is what
	// makes this check possible at all — a buffer-identity comparison would
	// reject every correct slice. It covers the data type, the length and the
	// null bitmap as well as the values, which is the part a value-only check
	// would miss.
	if sliced.to_data() != taken.to_data() {
		return Err(ExecutionError::Kernel {
			message: format!(
				"join output slice does not match take at column {position}: \
				 sliced {sliced_rows} rows of {sliced_type:?} with {sliced_nulls} nulls, \
				 take produced {taken_rows} rows of {taken_type:?} with {taken_nulls} nulls",
				sliced_rows = sliced.len(),
				sliced_type = sliced.data_type(),
				sliced_nulls = sliced.null_count(),
				taken_rows = taken.len(),
				taken_type = taken.data_type(),
				taken_nulls = taken.null_count(),
			),
		});
	}
	SLICE_EQUIVALENCE_COLUMNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	Ok(())
}

#[cfg(test)]
mod slice_fast_path_tests {
	use super::*;
	use arrow::array::{Int64Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc as StdArc;

	fn batch(prefix: &str, rows: i64) -> RecordBatch {
		let schema = StdArc::new(Schema::new(vec![
			Field::new(format!("{prefix}_id"), DataType::Int64, false),
			Field::new(format!("{prefix}_name"), DataType::Utf8, false),
		]));
		RecordBatch::try_new(
			schema,
			vec![
				StdArc::new(Int64Array::from_iter_values(0..rows)),
				StdArc::new(StringArray::from_iter_values(
					(0..rows).map(|row| format!("{prefix}-{row}")),
				)),
			],
		)
		.expect("fixture batch")
	}

	fn output_schema(left: &RecordBatch, right: &RecordBatch) -> arrow::datatypes::SchemaRef {
		let mut fields = left.schema().fields().to_vec();
		fields.extend(right.schema().fields().iter().cloned());
		StdArc::new(Schema::new(fields))
	}

	fn materialize(selection: &[JoinOutputRow]) -> RecordBatch {
		let left = batch("l", 8);
		let right = batch("r", 8);
		let schema = output_schema(&left, &right);
		materialize_join_selection(
			JoinKind::Inner,
			&left,
			&right,
			selection,
			&schema,
			u64::MAX,
			SideTakeTiming::AccountedElsewhere,
		)
		.expect("materialize")
	}

	/// Run one side through `gather_side`, reporting the rows the fast path served.
	///
	/// Built eagerly on purpose: this helper answers what the GATHER does with a
	/// given shape, so it hands over a materialized array in every case. Whether
	/// the array should have existed at all is the deferral's question, and
	/// `index_deferral_tests` asks it separately.
	fn gather(source: &RecordBatch, indices: &[u32]) -> u64 {
		let mut builder = LazyIndexBuilder::new(
			indices.len(),
			source.num_rows(),
			false,
			IndexRepresentation::Nullable,
		);
		for index in indices {
			builder.push(Some(*index));
		}
		let built = builder.finish();
		let side = built.indices;
		assert_eq!(
			(built.avoided, built.materialized),
			(0, indices.len() as u64),
			"an eager builder materializes every row"
		);
		let mut columns = Vec::new();
		gather_side(source, &side, indices.len(), &mut columns).expect("gather")
	}

	/// The fast path must produce the SAME batch `take` would.
	///
	/// This is the test that matters: a slice with a wrong offset returns the
	/// wrong rows silently, with the right row count and the right types. No
	/// downstream check would catch it, and a query would simply return wrong
	/// answers.
	#[test]
	fn a_sliced_side_matches_what_take_would_have_produced() {
		// Right side ascends 0..8 over the whole eight-row source, so it slices.
		// Left side is scattered, so it takes. One batch, mixed strategies.
		let selection: Vec<JoinOutputRow> = [7u32, 1, 4, 0, 6, 2, 5, 3]
			.iter()
			.enumerate()
			.map(|(right, left)| JoinOutputRow::Paired {
				left: *left,
				right: right as u32,
			})
			.collect();
		let produced = materialize(&selection);

		let left_ids = produced
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left id column");
		let right_ids = produced
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right id column");
		let right_names = produced
			.column(3)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("right name column");

		assert_eq!(
			left_ids.values(),
			&[7, 1, 4, 0, 6, 2, 5, 3],
			"the taken side is unchanged"
		);
		assert_eq!(
			right_ids.values(),
			&[0, 1, 2, 3, 4, 5, 6, 7],
			"the sliced side must carry the source's own values"
		);
		assert_eq!(right_names.value(0), "r-0");
		assert_eq!(right_names.value(7), "r-7");
		assert_eq!(produced.num_rows(), 8);
	}

	/// The positive control for the whole cut: whole-source coverage slices, and
	/// nothing else does. Without this a passing value assertion cannot say
	/// whether the fast path ran at all.
	#[test]
	fn only_whole_source_coverage_takes_the_fast_path() {
		let source = batch("r", 8);
		assert_eq!(
			gather(&source, &[0, 1, 2, 3, 4, 5, 6, 7]),
			8,
			"an ascending run over the whole source slices"
		);
		assert_eq!(
			gather(&source, &[2, 3, 4, 5]),
			0,
			"a contiguous run inside the source must not slice: the slice would \
			 retain all eight rows while the budget charges four"
		);
		assert_eq!(
			gather(&source, &[0, 1, 2, 3]),
			0,
			"a prefix run is a sub-slice too, however tidy its offset"
		);
		assert_eq!(gather(&source, &[7, 1, 4, 0]), 0, "a scatter takes");
	}

	/// A mid-batch run still has to produce the right rows — it just gets them
	/// from `take`.
	#[test]
	fn a_mid_batch_run_falls_back_to_take_with_the_same_values() {
		let selection: Vec<JoinOutputRow> = [(7u32, 2u32), (1, 3), (4, 4), (0, 5)]
			.iter()
			.map(|(left, right)| JoinOutputRow::Paired {
				left: *left,
				right: *right,
			})
			.collect();
		let produced = materialize(&selection);

		let right_ids = produced
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right id column");
		assert_eq!(right_ids.values(), &[2, 3, 4, 5]);
		assert_eq!(produced.num_rows(), 4);
	}

	/// A run that does not cover the whole selection must NOT slice.
	///
	/// This is the distinction the measurement turned on: a selection can carry a
	/// multi-million-row prefix run plus a scatter tail, and slicing that prefix
	/// would need multiple output batches. Only an end-to-end contiguous
	/// selection is a drop-in substitution.
	#[test]
	fn a_prefix_run_with_a_tail_does_not_slice() {
		let mut run = ContiguousRun::new();
		for index in 0..6u32 {
			run.observe(Some(index));
		}
		run.observe(Some(99));
		assert_eq!(
			run.resolve(7, 128),
			None,
			"a 6-row prefix inside a 7-row selection is not a whole-selection run"
		);
	}

	/// A null cannot be sliced: the output needs a null and a slice yields a value.
	#[test]
	fn a_null_breaks_the_fast_path() {
		let mut run = ContiguousRun::new();
		run.observe(Some(0));
		run.observe(None);
		run.observe(Some(1));
		assert_eq!(run.resolve(3, 128), None);
	}

	/// The slice must stay inside the source. Indices arrive from a probe loop
	/// this function does not own, and `Array::slice` panics out of bounds.
	#[test]
	fn a_run_past_the_source_end_does_not_slice() {
		let mut run = ContiguousRun::new();
		for index in 126..130u32 {
			run.observe(Some(index));
		}
		assert_eq!(
			run.resolve(4, 128),
			None,
			"126..130 does not fit in a 128-row source"
		);
		assert_eq!(
			run.resolve(2, 128),
			None,
			"the run length must match the rows"
		);
	}

	/// A whole-selection ascending run resolves to its start offset — and is
	/// still refused by `slice_offset` unless it covers the whole source.
	#[test]
	fn a_whole_selection_run_resolves_to_its_offset() {
		let mut run = ContiguousRun::new();
		for index in 5..9u32 {
			run.observe(Some(index));
		}
		assert_eq!(run.resolve(4, 128), Some(5));
		assert_eq!(
			run.slice_offset(4, 128),
			None,
			"the run arithmetic is sound but the retention restriction bites"
		);
		assert_eq!(run.slice_offset(4, 9), None, "offset 5 is not offset 0");

		let mut whole = ContiguousRun::new();
		for index in 0..4u32 {
			whole.observe(Some(index));
		}
		assert_eq!(whole.slice_offset(4, 4), Some(0));
	}

	/// Repeated `u32::MAX` must not look contiguous, for the same reason the
	/// locality probe uses `checked_add`.
	#[test]
	fn a_repeated_maximum_index_does_not_slice() {
		let mut run = ContiguousRun::new();
		run.observe(Some(u32::MAX));
		run.observe(Some(u32::MAX));
		assert_eq!(run.resolve(2, usize::MAX), None);
	}

	/// The assumption the whole equivalence check rests on: `ArrayData` equality
	/// is LOGICAL, so a mid-source slice equals a `take` result built from zero
	/// when the two carry the same rows.
	///
	/// If arrow ever compared buffer identity instead, the check would reject
	/// every correct slice and a diagnostic run would fail with a false alarm.
	/// This pins that, so the failure would land here and not in an atlas.
	#[test]
	fn equivalence_is_logical_not_buffer_identity() {
		let source = batch("r", 8);
		let indices = UInt32Array::from(vec![2u32, 3, 4, 5]);
		for position in 0..source.num_columns() {
			let column = source.column(position);
			verify_slice_matches_take(position, &column.slice(2, 4), column, &indices)
				.expect("a mid-source slice carrying the same rows must verify");
		}
	}

	/// Why the fast path is restricted to whole-source coverage: a slice keeps the
	/// source's ALLOCATION alive, while `take` allocates for the rows it produced.
	///
	/// This is the fact no value-equality check can see. A four-row slice of a
	/// large batch would be charged four rows by the materialization estimate and
	/// retain the whole thing, so `slice_offset` refuses it. If arrow ever made
	/// `slice` copy, this test would fail and the restriction could be revisited.
	#[test]
	fn a_slice_retains_the_whole_source_allocation() {
		let source = batch("r", 1_024);
		let column = source.column(0);
		let indices = UInt32Array::from(vec![2u32, 3, 4, 5]);

		let sliced = column.slice(2, 4);
		let taken = arrow::compute::take(column, &indices, None).expect("take");

		let source_capacity = column.to_data().buffers()[0].capacity();
		let sliced_capacity = sliced.to_data().buffers()[0].capacity();
		let taken_capacity = taken.to_data().buffers()[0].capacity();

		assert_eq!(
			sliced_capacity, source_capacity,
			"a slice retains the source's whole allocation"
		);
		assert!(
			taken_capacity < sliced_capacity,
			"take allocates for the rows it produced ({taken_capacity} bytes), \
			 the slice retains {sliced_capacity}"
		);
	}

	/// The discriminating half: the check must REJECT a wrong slice. Without
	/// this, a clean diagnostic run would only prove the check never fires.
	#[test]
	fn a_slice_at_the_wrong_offset_is_rejected() {
		let source = batch("r", 8);
		let indices = UInt32Array::from(vec![2u32, 3, 4, 5]);
		let column = source.column(1);
		// Off by one: the right row count, the right type, the wrong rows —
		// exactly the failure no downstream check would catch.
		let wrong = column.slice(1, 4);
		let error = verify_slice_matches_take(1, &wrong, column, &indices)
			.expect_err("a slice one row off must not verify");
		assert!(
			format!("{error:?}").contains("does not match take"),
			"the error must name what failed: {error:?}"
		);
	}

	/// Nulls are part of the comparison, not just values.
	#[test]
	fn a_null_bitmap_difference_is_rejected() {
		let schema = StdArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
		let column: arrow::array::ArrayRef =
			StdArc::new(Int64Array::from(vec![Some(0), None, Some(2), Some(3)]));
		let source = RecordBatch::try_new(schema, vec![StdArc::clone(&column)]).expect("fixture");
		let column = source.column(0);

		let indices = UInt32Array::from(vec![0u32, 1, 2, 3]);
		verify_slice_matches_take(0, &column.slice(0, 4), column, &indices)
			.expect("a whole-source slice keeps the null in place");

		// Same values in the overlapping rows, different null placement.
		let shifted = UInt32Array::from(vec![2u32, 3, 0, 1]);
		verify_slice_matches_take(0, &column.slice(0, 4), column, &shifted)
			.expect_err("a moved null must not verify");
	}

	/// The tally is the positive control, so it has to count.
	#[test]
	fn verified_columns_are_counted() {
		let source = batch("r", 4);
		let indices = UInt32Array::from(vec![0u32, 1, 2, 3]);
		let before = join_slice_equivalence_columns_verified();
		verify_slice_matches_take(0, &source.column(0).slice(0, 4), source.column(0), &indices)
			.expect("verify");
		assert!(
			join_slice_equivalence_columns_verified() > before,
			"a verified column must advance the tally"
		);
	}
}

#[cfg(test)]
mod index_deferral_tests {
	use super::*;
	use crate::distributed::task::TaskTelemetry;
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc as StdArc;

	fn source(prefix: &str, rows: i64) -> RecordBatch {
		let schema = StdArc::new(Schema::new(vec![Field::new(
			format!("{prefix}_id"),
			DataType::Int64,
			true,
		)]));
		RecordBatch::try_new(
			schema,
			vec![StdArc::new(Int64Array::from_iter_values(0..rows))],
		)
		.expect("fixture batch")
	}

	/// Build one side both ways and return `(lazy, eager)` index arrays, with the
	/// lazy build's avoided and materialized row counts.
	fn both_ways(
		indices: &[Option<u32>],
		source_rows: usize,
	) -> (SideIndices, SideIndices, u64, u64) {
		let mut lazy = LazyIndexBuilder::new(
			indices.len(),
			source_rows,
			true,
			IndexRepresentation::Nullable,
		);
		let mut eager = LazyIndexBuilder::new(
			indices.len(),
			source_rows,
			false,
			IndexRepresentation::Nullable,
		);
		for index in indices {
			lazy.push(*index);
			eager.push(*index);
		}
		let lazy = lazy.finish();
		let eager = eager.finish();
		assert_eq!(eager.avoided, 0, "the eager builder never avoids a row");
		(lazy.indices, eager.indices, lazy.avoided, lazy.materialized)
	}

	fn ascending(rows: usize) -> Vec<Option<u32>> {
		(0..rows).map(|row| Some(row as u32)).collect()
	}

	/// A whole-source run allocates nothing: the gather reads the source directly,
	/// and its indices would have been `0..rows`.
	#[test]
	fn a_whole_source_run_materializes_no_indices() {
		let (lazy, _, avoided, materialized) = both_ways(&ascending(64), 64);

		assert!(
			lazy.array.is_none(),
			"a deferred whole-source run must reach the gather with no index array"
		);
		assert_eq!(lazy.slice_offset, Some(0));
		assert_eq!(
			(avoided, materialized),
			(64, 0),
			"every row avoided, none materialized"
		);
	}

	/// The correctness rail: a broken run must backfill EXACTLY what the eager
	/// builder would have produced.
	///
	/// The backfill reconstructs the deferred prefix from the run rather than from
	/// the selection, so an off-by-one would gather the wrong rows with the right
	/// count and the right types — silently wrong answers, invisible downstream.
	/// Compared as `ArrayData`, which covers the null bitmap as well as values.
	#[test]
	fn a_broken_run_backfills_exactly_what_the_eager_builder_would() {
		let shapes: Vec<(&str, Vec<Option<u32>>)> = vec![
			(
				"break at the second row",
				vec![Some(0), Some(7), Some(2), Some(3)],
			),
			(
				"break at the last row",
				vec![Some(0), Some(1), Some(2), Some(0)],
			),
			(
				"never anchored at zero",
				vec![Some(1), Some(2), Some(3), Some(4)],
			),
			("a null mid-run", vec![Some(0), Some(1), None, Some(3)]),
			("a null at the head", vec![None, Some(1), Some(2), Some(3)]),
			("a repeat", vec![Some(0), Some(1), Some(1), Some(2)]),
			("descending tail", vec![Some(0), Some(1), Some(2), Some(1)]),
		];

		for (label, indices) in shapes {
			let (lazy, eager, avoided, materialized) = both_ways(&indices, 4);
			let lazy_array = lazy
				.array
				.as_ref()
				.unwrap_or_else(|| panic!("{label}: a broken run must materialize"));
			let eager_array = eager.array.as_ref().expect("eager always materializes");

			assert_eq!(
				lazy_array.to_data(),
				eager_array.to_data(),
				"{label}: the backfilled vector must equal eager construction"
			);
			assert_eq!(
				(avoided, materialized),
				(0, indices.len() as u64),
				"{label}: a materialized side avoids nothing"
			);
			assert_eq!(
				lazy.slice_offset, None,
				"{label}: a broken run is not sliceable"
			);
		}
	}

	/// A selection that cannot cover the source never enters the lazy path.
	///
	/// `slice_offset` requires whole-source coverage, so a contiguous prefix of a
	/// larger batch is ineligible however tidy it looks — and deferring for it
	/// would buy a backfill and no slice.
	#[test]
	fn a_selection_shorter_than_its_source_never_defers() {
		let (lazy, eager, avoided, materialized) = both_ways(&ascending(4), 64);

		assert_eq!(
			lazy.array.as_ref().expect("materialized").to_data(),
			eager.array.as_ref().expect("materialized").to_data()
		);
		assert_eq!((avoided, materialized), (0, 4));
		assert_eq!(lazy.slice_offset, None);
	}

	/// The equivalence diagnostic and the deferral are mutually exclusive by
	/// construction: the check compares against the array the deferral declines
	/// to build.
	#[test]
	fn the_equivalence_check_and_the_deferral_are_exclusive() {
		assert_eq!(
			index_deferral_available(),
			!join_slice_equivalence_enabled(),
			"a diagnostic run must keep its index arrays; a measuring run must not \
			 be forced to build them"
		);
	}

	/// End to end through the gather: the deferral fires, the slice still serves
	/// the side, and the rows are the ones `take` would have produced.
	#[test]
	fn a_deferred_side_still_gathers_the_same_rows() {
		let left = source("l", 8);
		let right = source("r", 8);
		let mut fields = left.schema().fields().to_vec();
		fields.extend(right.schema().fields().iter().cloned());
		let schema = StdArc::new(Schema::new(fields));
		let selection: Vec<JoinOutputRow> = (0..8)
			.map(|row| JoinOutputRow::Paired {
				left: 7 - row,
				right: row,
			})
			.collect();
		let slot: Option<Arc<Mutex<TaskTelemetry>>> =
			Some(Arc::new(Mutex::new(TaskTelemetry::default())));

		let batch = materialize_join_selection(
			JoinKind::Inner,
			&left,
			&right,
			&selection,
			&schema,
			u64::MAX,
			SideTakeTiming::Charged(&slot),
		)
		.expect("materialize");

		let right_values = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right column");
		assert_eq!(
			right_values.values(),
			&[0, 1, 2, 3, 4, 5, 6, 7],
			"the deferred side must still deliver its rows in selection order"
		);
		let left_values = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left column");
		assert_eq!(
			left_values.values(),
			&[7, 6, 5, 4, 3, 2, 1, 0],
			"the scattered side is unaffected by the other side's deferral"
		);

		let guard = slot.as_ref().unwrap().lock().unwrap();
		assert_eq!(
			(
				guard.counters.join_output_index_rows_avoided,
				guard.counters.join_output_index_rows_materialized
			),
			(8, 8),
			"one side deferred and one side materialized, both counted"
		);
		assert_eq!(
			(
				guard.counters.join_output_left_slice_rows,
				guard.counters.join_output_right_slice_rows
			),
			(0, 8),
			"slice coverage is unchanged by the deferral: the same side slices, \
			 the same rows"
		);
	}
}

#[cfg(test)]
mod index_representation_tests {
	use super::*;
	use crate::distributed::task::TaskTelemetry;
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc as StdArc;

	const ROWS: usize = 64;

	fn build(representation: IndexRepresentation, indices: &[Option<u32>]) -> SideBuild {
		// Eager on purpose: the deferral's behaviour is `index_deferral_tests`'
		// subject, and a deferred side would build no vector to inspect.
		let mut builder =
			LazyIndexBuilder::new(indices.len(), indices.len() + 1, false, representation);
		for index in indices {
			builder.push(*index);
		}
		builder.finish()
	}

	fn scattered() -> Vec<Option<u32>> {
		(0..ROWS)
			.map(|row| Some(((row * 7) % ROWS) as u32))
			.collect()
	}

	/// An eligible kind builds a null-free array and says so.
	///
	/// The array comparison is the substitution's correctness rail: a plain vector
	/// and a nullable one must produce the same indices, or every gather that
	/// consumes them returns different rows.
	#[test]
	fn an_eligible_kind_builds_plain_indices_with_the_same_values() {
		let indices = scattered();
		let plain = build(IndexRepresentation::Plain, &indices);
		let nullable = build(IndexRepresentation::Nullable, &indices);

		assert_eq!(
			plain.plain, ROWS as u64,
			"every materialized row must be counted as plain"
		);
		assert_eq!(nullable.plain, 0, "the control builds no plain rows");

		let plain_array = plain.indices.array.as_ref().expect("materialized");
		let nullable_array = nullable.indices.array.as_ref().expect("materialized");
		assert_eq!(
			plain_array.values(),
			nullable_array.values(),
			"the substitution must not change a single index"
		);
		assert_eq!(
			plain_array.null_count(),
			0,
			"a plain build carries no null buffer"
		);
		assert_eq!(plain_array.len(), nullable_array.len());
	}

	/// An outer kind is untouched by this cut.
	#[test]
	fn an_outer_kind_still_builds_a_nullable_array() {
		let mut indices = scattered();
		indices[3] = None;
		let built = build(IndexRepresentation::Nullable, &indices);
		let array = built.indices.array.as_ref().expect("materialized");

		assert_eq!(built.plain, 0);
		assert_eq!(array.null_count(), 1, "the null must survive");
		assert!(array.is_null(3));
	}

	/// A null reaching an eligible side widens rather than panicking or erroring.
	///
	/// The arms that select `Plain` are not supposed to produce a fill row, but
	/// nothing in the type system stops a selection from carrying one, and a
	/// representation choice must never change what a query returns. The widened
	/// build must equal the nullable one exactly — and it must stop counting
	/// itself as plain, or the positive control would claim coverage the
	/// substitution did not deliver.
	#[test]
	fn an_eligible_side_that_meets_a_null_widens_to_nullable() {
		let mut indices = scattered();
		indices[9] = None;
		let widened = build(IndexRepresentation::Plain, &indices);
		let nullable = build(IndexRepresentation::Nullable, &indices);

		assert_eq!(
			widened
				.indices
				.array
				.as_ref()
				.expect("materialized")
				.to_data(),
			nullable
				.indices
				.array
				.as_ref()
				.expect("materialized")
				.to_data(),
			"a widened build must equal the nullable one, nulls and values alike"
		);
		assert_eq!(
			widened.plain, 0,
			"a side that widened is no longer a plain build and must not be counted as one"
		);
	}

	/// End to end: an eligible join records plain rows, an outer join records none,
	/// and both gather the rows they always did.
	#[test]
	fn the_plain_row_counter_follows_the_join_kind() {
		fn source(prefix: &str) -> RecordBatch {
			let schema = StdArc::new(Schema::new(vec![Field::new(
				format!("{prefix}_id"),
				DataType::Int64,
				true,
			)]));
			RecordBatch::try_new(
				schema,
				vec![StdArc::new(Int64Array::from_iter_values(0..8))],
			)
			.expect("fixture batch")
		}

		fn counters(kind: JoinKind, selection: &[JoinOutputRow]) -> (u64, u64, RecordBatch) {
			let left = source("l");
			let right = source("r");
			let mut fields = left.schema().fields().to_vec();
			fields.extend(right.schema().fields().iter().cloned());
			let schema = StdArc::new(Schema::new(fields));
			let slot: Option<Arc<Mutex<TaskTelemetry>>> =
				Some(Arc::new(Mutex::new(TaskTelemetry::default())));
			let batch = materialize_join_selection(
				kind,
				&left,
				&right,
				selection,
				&schema,
				u64::MAX,
				SideTakeTiming::Charged(&slot),
			)
			.expect("materialize");
			let guard = slot.as_ref().unwrap().lock().unwrap();
			(
				guard.counters.join_output_plain_index_rows,
				guard.counters.join_output_index_rows_materialized,
				batch.clone(),
			)
		}

		// Scattered on both sides, so neither side defers and both build a vector.
		let selection: Vec<JoinOutputRow> = (0..8u32)
			.map(|row| JoinOutputRow::Paired {
				left: (row * 3) % 8,
				right: (row * 5) % 8,
			})
			.collect();

		let (plain, materialized, inner_batch) = counters(JoinKind::Inner, &selection);
		assert_eq!(
			(plain, materialized),
			(16, 16),
			"an Inner gather builds both sides plain"
		);

		let (outer_plain, outer_materialized, outer_batch) =
			counters(JoinKind::LeftOuter, &selection);
		assert_eq!(
			(outer_plain, outer_materialized),
			(0, 16),
			"a LeftOuter gather builds the same rows and none of them plain"
		);

		assert_eq!(
			inner_batch, outer_batch,
			"the two representations must gather identical rows from an identical \
			 selection — the only difference is how the indices were stored"
		);
	}
}
