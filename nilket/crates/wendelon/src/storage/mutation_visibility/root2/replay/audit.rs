//! The offline compromise audit's fold: the dormant replay's own acquisition, dispatch, production
//! verification and reducer over a read-only scan of the retained Control log, from a provisioned
//! baseline. It has no recovery-mark store, writes nothing and returns no authority state: only
//! the audit's per-entry trace, after the same closure checks and administrator binding as `run`.
//!
//! The reader serves the scanned entries through the stop; the sources report that stop and the
//! history evidence the scan established (every entry from genesis through the stop was read,
//! checksum-validated and contiguous, with nothing purged), which is what the shared prefix
//! validation requires. That evidence is local to this node's files, not quorum currentness.
use super::{ClosureError2, Fault2, ReplayDriver2, ReplayError2};
use crate::consensus::retention::{ControlRetention, HistoryCheck, RetentionRefusals};
use crate::consensus::{RetentionStatus, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::acquisition::{
	AcquisitionCaps2, AcquisitionSources2, SourceClosed2,
};
use crate::storage::mutation_visibility::root2::admin_install::{
	confirm_terminal2, install_admin2,
};
use crate::storage::mutation_visibility::root2::compromise::Trace2;
use crate::storage::mutation_visibility::root2::origin::root_and_origin2;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::PinnedBoundTuple2;
use crate::storage::mutation_visibility::root2::recovery::{Baseline2, Deployment2};
use crate::storage::mutation_visibility::root2::retirement::is_retired;
use openraft::storage::RaftLogReader;
use openraft::{Entry, LogId, OptionalSend, StorageError};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;

/// The scanned retained entries through the stop, served by exact range.
struct Retained2 {
	entries: BTreeMap<u64, Entry<WendelonTypeConfig>>,
}
impl RaftLogReader<WendelonTypeConfig> for Retained2 {
	async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
		&mut self,
		range: RB,
	) -> Result<Vec<Entry<WendelonTypeConfig>>, StorageError<u32>> {
		Ok(self.entries.range(range).map(|(_, e)| e.clone()).collect())
	}
}
/// The offline sources: the local stop and the scan's history evidence over `0..=stop`.
struct Scanned2 {
	stop: LogId<u32>,
}
impl AcquisitionSources2 for Scanned2 {
	fn retention(&self) -> RetentionStatus {
		RetentionStatus {
			// A label for the scan's evidence (0..=stop read, validated and contiguous, nothing
			// purged), which is what the shared prefix check requires; it is not this node's
			// actual `control_retention` setting, which the audit never reads.
			configured: ControlRetention::Full,
			history: HistoryCheck::Complete {
				stored_last: self.stop.index,
			},
			violation: None,
			refusals: RetentionRefusals::default(),
		}
	}
	fn applied(&self) -> Result<Option<LogId<u32>>, SourceClosed2> {
		Ok(Some(self.stop))
	}
}

/// Folds `entries` (exactly the retained `0..=stop`) for the root `pin` binds, recording each
/// carrier into `trace`. Any acquisition, verification, closure or binding refusal is returned
/// as such; the caller reports it as invalid history.
pub(in crate::storage::mutation_visibility::root2) async fn fold2(
	entries: BTreeMap<u64, Entry<WendelonTypeConfig>>,
	stop: LogId<u32>,
	deployment: Deployment2,
	pin: &PinnedBoundTuple2,
	trace: Trace2,
) -> Result<Trace2, ReplayError2> {
	let mut reader = Retained2 { entries };
	let sources = Scanned2 { stop };
	let caps = AcquisitionCaps2 {
		max_entries: 1024,
		max_batch_bytes: 1 << 30,
		max_total_bytes: u64::MAX,
	};
	let (acc, report) = ReplayDriver2 {
		deployment,
		baseline: Baseline2::Provisioned,
		pin,
	}
	.compose_traced(&mut reader, &sources, caps, Some(trace))
	.await?;
	let a = sources
		.applied()
		.map_err(|_| ReplayError2::Closure(ClosureError2::SourceClosed))?
		.ok_or(ReplayError2::Closure(ClosureError2::Regressed))?;
	if a != report.terminal_applied() {
		return Err(ReplayError2::Closure(ClosureError2::Conflict));
	}
	let retired = root_and_origin2(&acc.state).is_some_and(|(r, _)| is_retired(&r.state));
	if retired {
		confirm_terminal2(&acc.state, pin).map_err(ReplayError2::Binding)?;
	} else {
		install_admin2(&acc.state, pin).map_err(ReplayError2::Binding)?;
	}
	acc.trace.ok_or(ReplayError2::Fault(Fault2::Internal))
}
