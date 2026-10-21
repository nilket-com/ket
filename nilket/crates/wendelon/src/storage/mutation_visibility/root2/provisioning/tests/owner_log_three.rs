//! Profile-3 ports of the `owner_log.rs` rows on a marked (format 2) owner directory: the same
//! typed I/O, latch, repair, export and clear outcomes over VISOEV02 logs, VISREP04 evidence and
//! profile-3 roots (the independent `p3_` vectors). A fresh marked directory already holds an empty
//! log. The forked-child lock test is not repeated: the lock path is the same for both formats.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin::record::BootstrapObservation2;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::v2::encode_evidence2;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::*;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::seam;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::decode_snapshot2;
use crate::storage::mutation_visibility::root2::provisioning::terminal::TerminalObservation3;
use crate::storage::mutation_visibility::root2::types::Log2;
use IoStep2::*;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);
const E: usize =
	crate::storage::mutation_visibility::root2::provisioning::inventory::v2::EVENT_BYTES2;
const B3: usize = 3 * E;

// A fresh 0700 marked (format 2) owner directory unique to this process and call; removed on
// drop. `bare` is the same directory before initialization.
struct Scratch(PathBuf);
impl Scratch {
	fn bare() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("owner-log-three-{}-{n}", std::process::id()));
		let _ = fs::remove_dir_all(&p);
		fs::create_dir_all(&p).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		Self(p)
	}
	fn new() -> Self {
		let s = Self::bare();
		drop(OwnerDir2::create_v2(&s.0).unwrap());
		s
	}
	fn at(&self, rel: &str) -> PathBuf {
		self.0.join(rel)
	}
	fn open(&self) -> OwnerDir2 {
		OwnerDir2::open(&self.0).unwrap()
	}
	fn repairs(&self) -> PathBuf {
		let p = self.at("repairs");
		if !p.exists() {
			fs::create_dir(&p).unwrap();
			fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		}
		p
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}
fn sha(b: &[u8]) -> [u8; 32] {
	hash(b"", b)
}
fn op(n: u8) -> OpId2 {
	op_id2([n; 16]).unwrap()
}
fn manifest_of(name: &str) -> AnyAdminManifest2 {
	let EventBody2::Reserve { manifest } = p3_event(name).body else {
		unreachable!()
	};
	manifest
}
fn id_of(name: &str) -> RootIdentity2 {
	p3_event(name).identity.unwrap()
}
fn approve(id: RootIdentity2, name: &str) -> OwnerRequest2 {
	let observation = match p3_event(name).body {
		EventBody2::Approve { record } => BootstrapObservation2 {
			identity: record.identity,
		},
		EventBody2::Conflict { observation } => observation,
		_ => unreachable!(),
	};
	OwnerRequest2::Approve {
		identity: id,
		observation,
		confirmed: id,
	}
}
fn reserve(name: &str) -> OwnerRequest2 {
	OwnerRequest2::Reserve {
		manifest: manifest_of(name),
	}
}
fn fifo(p: &Path) {
	assert!(
		std::process::Command::new("mkfifo")
			.arg(p)
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.status()
			.unwrap()
			.success()
	);
}
fn vector(case: &str, field: &str) -> Vec<u8> {
	let v: serde_json::Value = serde_json::from_str(include_str!("owner_vectors.json")).unwrap();
	unhex(v["p3_cases"][case][field].as_str().unwrap())
}
fn vector_name(case: &str) -> String {
	let v: serde_json::Value = serde_json::from_str(include_str!("owner_vectors.json")).unwrap();
	v["p3_cases"][case]["name"].as_str().unwrap().to_owned()
}
/// The committed retirement of the first root, as the golden terminal Retire records it.
fn retire_first() -> OwnerRequest2 {
	let EventBody2::Approve { record } = p3_event("approve").body else {
		unreachable!()
	};
	OwnerRequest2::Retire {
		identity: id_of("reserve"),
		terminal: TerminalObservation3 {
			identity: record.identity,
			original_admin_key: id_of("reserve").admin_key,
			at: Log2 {
				term: 1,
				node: 1,
				index: 9,
			},
			fingerprint: [0x19; 32],
			command_digest: [0x1a; 32],
			state_digest: [0x1b; 32],
		},
	}
}
fn torn_tail() -> Vec<u8> {
	let v: serde_json::Value = serde_json::from_str(include_str!("owner_vectors.json")).unwrap();
	unhex(v["tail"].as_str().unwrap())
}
// An owner directory whose log holds three golden events and a torn 100-byte tail.
fn torn_owner() -> Scratch {
	let s = Scratch::new();
	let torn = [&p3_log_bytes()[..B3], torn_tail().as_slice()].concat();
	fs::write(s.at("inventory.log"), torn).unwrap();
	s
}
fn base_context() -> [u8; 32] {
	vector("base", "context").try_into().unwrap()
}
// Runs `f` on a fresh handle and requires the test crash stop to end it; the handle is dropped
// during unwinding, which releases the lock before the caller reopens.
fn crash(s: &Scratch, stop: (IoStep2, u32), f: impl FnOnce(&mut OwnerDir2)) {
	let _plan = seam::plan(&[], Some(stop));
	let r = catch_unwind(AssertUnwindSafe(|| {
		let mut o = s.open();
		f(&mut o);
	}));
	assert!(r.is_err(), "crash stop at {stop:?} was not reached");
}
fn in_order(seen: &[IoStep2], want: &[IoStep2]) -> bool {
	let mut it = seen.iter();
	want.iter().all(|w| it.any(|s| s == w))
}

#[test]
fn lock_is_flock_and_released_on_drop() {
	let s = Scratch::new();
	let owner = s.open();
	// L-1: an independent open of LOCK in the same process cannot take the lock (flock, not fcntl).
	let other = fs::OpenOptions::new()
		.read(true)
		.write(true)
		.open(s.at("LOCK"))
		.unwrap();
	assert!(matches!(
		other.try_lock(),
		Err(std::fs::TryLockError::WouldBlock)
	));
	assert_eq!(OwnerDir2::open(&s.0).err(), Some(OwnerIoError2::Busy));
	// L-2: dropping the owner releases it.
	drop(owner);
	other.try_lock().unwrap();
	other.unlock().unwrap();
	drop(other);
	drop(s.open());
}
#[test]
fn a_fresh_owner_directory_is_empty_and_never_written_by_reads() {
	let s = Scratch::new();
	let out = Scratch::new();
	let mut o = s.open();
	let status = o.inspect().unwrap();
	assert_eq!(
		status.classification,
		Classification2::Clean { last_seq: 0 }
	);
	assert!(status.log_present && !status.marker_present && status.poisoned.is_none());
	assert_eq!(o.repair_check(), Ok(RepairOutcome2::NothingToRepair));
	let pin = o.export(&out.0, "inv-0").unwrap();
	let bytes = fs::read(out.at("inv-0")).unwrap();
	assert_eq!(pin, sha(&bytes));
	let snapshot = decode_snapshot2(&bytes).unwrap();
	assert!(snapshot.export_seq == 0 && snapshot.entries.is_empty());
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), b"");
	assert!(!s.at("repairs").exists());
	assert!(!out.at("inv-0.tmp").exists());
}
#[test]
fn appends_write_the_golden_log_and_report_outcomes() {
	let s = Scratch::new();
	let mut o = s.open();
	let (id1, id2) = (id_of("reserve"), id_of("reserve_second"));
	let at = |op: EventOp2, seq: u64, current: RootState2| OpOutcome2 {
		original: Some(EventRef2 { op, seq }),
		current,
	};
	{
		let _plan = seam::plan(&[], None);
		assert_eq!(
			o.apply(&op(1), &reserve("reserve")),
			Ok(at(EventOp2::Reserve, 1, RootState2::Prepared))
		);
		// The marked directory's log already exists: it is opened for the write, never created.
		assert!(in_order(
			&seam::seen(),
			&[LogWrite, LogSync, OwnerDirSync, LogOpen]
		));
		assert!(!seam::seen().contains(&LogCreate));
	}
	assert_eq!(
		o.apply(&op(2), &approve(id1, "approve")),
		Ok(at(EventOp2::Approve, 2, RootState2::Bound))
	);
	assert_eq!(
		o.apply(&op(3), &reserve("reserve_second")),
		Ok(at(EventOp2::Reserve, 3, RootState2::Prepared))
	);
	assert_eq!(
		o.apply(&op(4), &approve(id2, "conflict")),
		Ok(at(EventOp2::Conflict, 4, RootState2::Conflicted))
	);
	assert_eq!(
		o.apply(&op(5), &retire_first()),
		Ok(at(EventOp2::Retire, 5, RootState2::Retired))
	);
	assert_eq!(
		fs::read(s.at("inventory.log")).unwrap(),
		p3_log_bytes()[..5 * E]
	);
	// Retry and no-op re-sync and report; refusals pass through.
	{
		let _plan = seam::plan(&[], None);
		assert_eq!(
			o.apply(&op(2), &approve(id1, "approve")),
			Ok(at(EventOp2::Approve, 2, RootState2::Retired))
		);
		assert!(in_order(&seam::seen(), &[LogSync, OwnerDirSync]));
	}
	assert_eq!(
		o.apply(&op(0x40), &retire_first()),
		Ok(OpOutcome2 {
			original: None,
			current: RootState2::Retired
		})
	);
	assert_eq!(
		o.apply(&op(0x41), &reserve("reserve")),
		Err(OwnerIoError2::Decision(OwnerError2::AlreadyReserved))
	);
}
#[test]
fn non_regular_and_symlink_targets_are_refused_without_blocking() {
	let s = Scratch::bare();
	fifo(&s.at("LOCK"));
	assert_eq!(
		OwnerDir2::create_v2(&s.0).err(),
		Some(OwnerIoError2::NotRegular { step: LockOpen })
	);
	let s = Scratch::bare();
	symlink("/dev/null", s.at("LOCK")).unwrap();
	assert_eq!(
		OwnerDir2::create_v2(&s.0).err(),
		Some(OwnerIoError2::Io {
			step: LockOpen,
			errno: Some(libc::ELOOP)
		})
	);
	let s = Scratch::new();
	let mut o = s.open();
	let log = s.at("inventory.log");
	fs::remove_file(&log).unwrap();
	fifo(&log);
	assert_eq!(
		o.inspect().err(),
		Some(OwnerIoError2::NotRegular { step: LogOpen })
	);
	fs::remove_file(&log).unwrap();
	fs::create_dir(&log).unwrap();
	assert_eq!(
		o.apply(&op(1), &reserve("reserve")).err(),
		Some(OwnerIoError2::NotRegular { step: LogOpen })
	);
	fs::remove_dir(&log).unwrap();
	symlink(s.at("LOCK"), &log).unwrap();
	assert_eq!(
		o.inspect().err(),
		Some(OwnerIoError2::Io {
			step: LogOpen,
			errno: Some(libc::ELOOP)
		})
	);
	fs::remove_file(&log).unwrap();
	// Evidence entries must be regular files.
	fifo(&s.repairs().join(vector_name("base")));
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::RepairDirForeign { index: 0 }
	);
	fs::remove_dir_all(s.at("repairs")).unwrap();
	// A marker of any type blocks; clear refuses a non-regular one.
	fs::create_dir(s.at("SYNC_FAILED")).unwrap();
	assert_eq!(
		o.apply(&op(1), &reserve("reserve")).err(),
		Some(OwnerIoError2::SyncFailurePending)
	);
	assert_eq!(
		o.clear_sync_failure(&sha(b"")),
		Err(OwnerIoError2::ClearRefused(ClearRefusal2::NonRegularMarker))
	);
}
#[test]
fn a_permission_denied_log_is_an_error_not_an_absent_log() {
	let s = Scratch::new();
	let o = s.open();
	let log = s.at("inventory.log");
	fs::write(&log, &p3_log_bytes()[..E]).unwrap();
	fs::set_permissions(&log, fs::Permissions::from_mode(0o000)).unwrap();
	if fs::read(&log).is_ok() {
		eprintln!("OWNER_LOG_SKIP permission: running with privilege that ignores mode 000");
		return;
	}
	assert_eq!(
		o.inspect().err(),
		Some(OwnerIoError2::Io {
			step: LogOpen,
			errno: Some(libc::EACCES)
		})
	);
}
#[test]
fn a_failed_sync_latches_the_same_object_and_sets_a_marker() {
	let s = Scratch::new();
	let out = Scratch::new();
	let mut o = s.open();
	let failed = |step, marker| Err(OwnerIoError2::SyncFailed { step, marker });
	{
		let _plan = seam::plan(&[(LogSync, 1)], None);
		assert_eq!(
			o.apply(&op(1), &reserve("reserve")),
			failed(LogSync, MarkerOutcome2::Durable)
		);
	}
	assert!(s.at("SYNC_FAILED").is_file());
	let poisoned = Some(OwnerIoError2::Poisoned { step: LogSync });
	assert_eq!(o.apply(&op(1), &reserve("reserve")).err(), poisoned);
	assert_eq!(o.repair_check().err(), poisoned);
	assert_eq!(o.export(&out.0, "x").err(), poisoned);
	let status = o.inspect().unwrap();
	assert!(status.marker_present && status.poisoned == Some(LogSync));
	// A reopened handle sees only the marker.
	drop(o);
	let mut o = s.open();
	assert_eq!(
		o.apply(&op(1), &reserve("reserve")).err(),
		Some(OwnerIoError2::SyncFailurePending)
	);
	let log = fs::read(s.at("inventory.log")).unwrap();
	assert_eq!(
		o.clear_sync_failure(&[0; 32]),
		Err(OwnerIoError2::ExpectLogMismatch { actual: sha(&log) })
	);
	assert_eq!(o.clear_sync_failure(&sha(&log)), Ok(()));
	assert!(!s.at("SYNC_FAILED").exists());
	// The event was written before the failed sync: the same op is now a retry.
	assert_eq!(
		o.apply(&op(1), &reserve("reserve")).map(|r| r.original),
		Ok(Some(EventRef2 {
			op: EventOp2::Reserve,
			seq: 1
		}))
	);
	assert_eq!(
		o.clear_sync_failure(&sha(&log)),
		Err(OwnerIoError2::NothingToClear)
	);
}
#[test]
fn marker_failures_still_latch_the_same_object() {
	for (second, marker_file) in [
		(MarkerCreate, false),
		(MarkerWrite, true),
		(MarkerSync, true),
		(MarkerDirSync, true),
	] {
		let s = Scratch::new();
		let mut o = s.open();
		{
			let _plan = seam::plan(&[(LogSync, 1), (second, 1)], None);
			assert_eq!(
				o.apply(&op(1), &reserve("reserve")),
				Err(OwnerIoError2::SyncFailed {
					step: LogSync,
					marker: MarkerOutcome2::NotDurable
				})
			);
		}
		assert_eq!(s.at("SYNC_FAILED").exists(), marker_file, "{second:?}");
		// One non-recursive marker attempt, whatever sub-step failed.
		assert_eq!(
			seam::seen().iter().filter(|x| **x == MarkerCreate).count(),
			1,
			"{second:?}"
		);
		for _ in 0..2 {
			assert_eq!(
				o.apply(&op(1), &reserve("reserve")).err(),
				Some(OwnerIoError2::Poisoned { step: LogSync })
			);
		}
		assert_eq!(o.inspect().unwrap().poisoned, Some(LogSync));
		// Attested clear also works when the latch is set but no marker was made durable.
		let log = fs::read(s.at("inventory.log")).unwrap();
		assert_eq!(o.clear_sync_failure(&sha(&log)), Ok(()));
		assert!(o.apply(&op(1), &reserve("reserve")).is_ok());
	}
}
#[test]
fn clear_latches_its_own_sync_failures() {
	// A marker left by an earlier run, with no latch on the reopened handle.
	let s = Scratch::new();
	{
		let mut o = s.open();
		let _plan = seam::plan(&[(LogSync, 1)], None);
		assert!(o.apply(&op(1), &reserve("reserve")).is_err());
	}
	let log = fs::read(s.at("inventory.log")).unwrap();
	for step in [LogSync, OwnerDirSync] {
		let mut o = s.open();
		assert!(o.inspect().unwrap().poisoned.is_none());
		{
			let _plan = seam::plan(&[(step, 1)], None);
			assert_eq!(
				o.clear_sync_failure(&sha(&log)),
				Err(OwnerIoError2::SyncFailed {
					step,
					marker: MarkerOutcome2::AlreadyPresent
				})
			);
		}
		assert!(s.at("SYNC_FAILED").exists());
		assert_eq!(
			o.repair_check().err(),
			Some(OwnerIoError2::Poisoned { step })
		);
	}
	// A failed final directory sync keeps the latch and makes one marker attempt.
	let mut o = s.open();
	{
		let _plan = seam::plan(&[(ClearDirSync, 1), (MarkerCreate, 1)], None);
		assert_eq!(
			o.clear_sync_failure(&sha(&log)),
			Err(OwnerIoError2::SyncFailed {
				step: ClearDirSync,
				marker: MarkerOutcome2::NotDurable
			})
		);
		assert_eq!(
			seam::seen().iter().filter(|s| **s == MarkerCreate).count(),
			1
		);
	}
	assert!(!s.at("SYNC_FAILED").exists());
	assert_eq!(
		o.apply(&op(1), &reserve("reserve")).err(),
		Some(OwnerIoError2::Poisoned { step: ClearDirSync })
	);
	assert_eq!(o.clear_sync_failure(&sha(&log)), Ok(()));
	assert!(o.repair_check().is_ok());
}
#[test]
fn marker_stat_errors_and_empty_owner_clears_are_defined() {
	let s = Scratch::new();
	let mut o = s.open();
	{
		let _plan = seam::plan(&[(MarkerStat, 1)], None);
		assert_eq!(
			o.repair_check(),
			Err(OwnerIoError2::Io {
				step: MarkerStat,
				errno: Some(libc::EIO)
			})
		);
	}
	// A failure before the first append: the log is empty, and the clear expects the empty hash.
	{
		let _plan = seam::plan(&[(OwnerDirSync, 1)], None);
		assert_eq!(
			o.repair_check(),
			Err(OwnerIoError2::SyncFailed {
				step: OwnerDirSync,
				marker: MarkerOutcome2::Durable
			})
		);
	}
	drop(o);
	let mut o = s.open();
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), b"");
	assert_eq!(o.clear_sync_failure(&sha(b"")), Ok(()));
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), b"");
	assert!(!s.at("repairs").exists());
}
#[test]
fn an_open_time_lock_sync_failure_returns_no_owner_and_no_lock() {
	let s = Scratch::bare();
	{
		let _plan = seam::plan(&[(LockCreateDirSync, 1)], None);
		assert_eq!(
			OwnerDir2::create_v2(&s.0).err(),
			Some(OwnerIoError2::OpenSyncFailed {
				step: LockCreateDirSync
			})
		);
	}
	assert!(!s.at("SYNC_FAILED").exists());
	// No lock is held and nothing but the lock file was left: initialization now succeeds.
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	drop(s.open());
}
#[test]
fn repair_publishes_evidence_once_and_restores_a_clean_log() {
	let s = torn_owner();
	let mut o = s.open();
	let Classification2::Unclaimed { tail_sha256, .. } = o.inspect().unwrap().classification else {
		panic!("torn log is unclaimed")
	};
	assert!(matches!(
		o.apply(&op(9), &reserve("reserve_second")),
		Err(OwnerIoError2::Classified(Classification2::Unclaimed { .. }))
	));
	assert_eq!(
		o.repair_fresh(&[0; 32]),
		Err(OwnerIoError2::ExpectTailMismatch {
			actual: tail_sha256
		})
	);
	// A stale tmp from an earlier crash is replaced, never trusted.
	fs::write(
		s.repairs().join(format!("{}.tmp", vector_name("base"))),
		b"stale",
	)
	.unwrap();
	assert_eq!(
		o.repair_fresh(&tail_sha256),
		Ok(RepairOutcome2::Repaired {
			context: base_context(),
			seq: 4
		})
	);
	let repaired = [
		&p3_log_bytes()[..B3],
		vector("base", "expected_repair").as_slice(),
	]
	.concat();
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), repaired);
	let evidence = s.at("repairs").join(vector_name("base"));
	assert_eq!(fs::read(&evidence).unwrap(), vector("base", "evidence"));
	assert_eq!(fs::read_dir(s.at("repairs")).unwrap().count(), 1);
	assert_eq!(o.repair_check(), Ok(RepairOutcome2::NothingToRepair));
	// A later unrelated torn append is a new context; the old evidence is untouched.
	let mut later = repaired.clone();
	later.extend([0xcd; 50]);
	fs::write(s.at("inventory.log"), &later).unwrap();
	assert!(matches!(
		o.inspect().unwrap().classification,
		Classification2::Unclaimed { boundary, .. } if boundary == (B3 + E) as u64
	));
	assert_eq!(fs::read(&evidence).unwrap(), vector("base", "evidence"));
}
#[test]
fn crash_points_resume_to_the_same_repair() {
	let repaired = [
		&p3_log_bytes()[..B3],
		vector("base", "expected_repair").as_slice(),
	]
	.concat();
	let tail = sha(&torn_tail());
	let done = Ok(RepairOutcome2::Repaired {
		context: base_context(),
		seq: 4,
	});
	// R1/R2: before the link only a tmp exists, so the tail is still unclaimed.
	let s = torn_owner();
	crash(&s, (EvidenceSync, 1), |o| {
		let _ = o.repair_fresh(&tail);
	});
	let mut o = s.open();
	assert!(matches!(
		o.inspect().unwrap().classification,
		Classification2::Unclaimed { .. }
	));
	assert_eq!(o.repair_fresh(&tail), done);
	// R3/R5: evidence published, not truncated; R6/R7: truncated, Repair not written.
	for (stop, stage) in [
		((EvidenceLink, 1), RepairStage2::NotTruncated),
		((Truncate, 1), RepairStage2::Truncated),
		((TruncateSync, 1), RepairStage2::Truncated),
	] {
		let s = torn_owner();
		crash(&s, stop, |o| {
			let _ = o.repair_fresh(&tail);
		});
		let mut o = s.open();
		let pending = Classification2::Pending {
			context: base_context(),
			stage,
		};
		assert_eq!(o.inspect().unwrap().classification, pending, "{stop:?}");
		assert_eq!(
			o.apply(&op(9), &reserve("reserve_second")).err(),
			Some(OwnerIoError2::Classified(pending))
		);
		assert_eq!(
			o.repair_fresh(&tail).err(),
			Some(OwnerIoError2::Classified(pending))
		);
		assert_eq!(
			o.repair_resume(&[0x11; 32]).err(),
			Some(OwnerIoError2::RepairContextMismatch)
		);
		assert_eq!(o.repair_resume(&base_context()), done, "{stop:?}");
		assert_eq!(fs::read(s.at("inventory.log")).unwrap(), repaired);
	}
	// R10: the Repair is written but not yet synced; the next check re-syncs it.
	let s = torn_owner();
	crash(&s, (LogWrite, 1), |o| {
		let _ = o.repair_fresh(&tail);
	});
	let mut o = s.open();
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::Clean { last_seq: 4 }
	);
	assert_eq!(o.repair_check(), Ok(RepairOutcome2::NothingToRepair));
	// R8/R9: a partial Repair, or one followed by exposed zero bytes, resumes in place.
	for partial in [
		vector("base", "expected_repair")[..100].to_vec(),
		[&vector("base", "expected_repair")[..60], &[0u8; 40][..]].concat(),
	] {
		let s = Scratch::new();
		fs::write(
			s.at("inventory.log"),
			[&p3_log_bytes()[..B3], partial.as_slice()].concat(),
		)
		.unwrap();
		fs::write(
			s.repairs().join(vector_name("base")),
			vector("base", "evidence"),
		)
		.unwrap();
		let mut o = s.open();
		assert_eq!(
			o.inspect().unwrap().classification,
			Classification2::Pending {
				context: base_context(),
				stage: RepairStage2::PartialRepair
			}
		);
		assert_eq!(o.repair_resume(&base_context()), done);
		assert_eq!(fs::read(s.at("inventory.log")).unwrap(), repaired);
	}
	// R12: a failed sync during repair latches the handle.
	let s = torn_owner();
	let mut o = s.open();
	{
		let _plan = seam::plan(&[(EvidenceSync, 1)], None);
		assert_eq!(
			o.repair_fresh(&tail),
			Err(OwnerIoError2::SyncFailed {
				step: EvidenceSync,
				marker: MarkerOutcome2::Durable
			})
		);
	}
	assert_eq!(
		o.repair_check().err(),
		Some(OwnerIoError2::Poisoned { step: EvidenceSync })
	);
}
#[test]
fn conflicting_or_invalid_evidence_is_never_chosen_or_overwritten() {
	// R13: two contexts match the same bytes; the tool refuses and changes nothing.
	let prefix = p3_log_bytes()[..B3].to_vec();
	let tail = torn_tail();
	let a = [prefix.as_slice(), &tail].concat();
	let b = [prefix.as_slice(), &tail[..30], &[0x01; 40]].concat();
	let s = Scratch::new();
	for log in [&a, &b] {
		let (name, bytes) = encode_evidence2(log).unwrap();
		fs::write(s.repairs().join(name), bytes).unwrap();
	}
	let shared = [prefix.as_slice(), &tail[..30]].concat();
	fs::write(s.at("inventory.log"), &shared).unwrap();
	let mut o = s.open();
	let conflicting = Classification2::Conflicting { count: 2 };
	assert_eq!(o.inspect().unwrap().classification, conflicting);
	assert_eq!(
		o.repair_resume(&base_context()).err(),
		Some(OwnerIoError2::Classified(conflicting))
	);
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), shared);
	// Invalid evidence under a valid name is refused and left as it is.
	let s = torn_owner();
	let bad = s.repairs().join(vector_name("base"));
	fs::write(&bad, [0x5a; 200]).unwrap();
	let mut o = s.open();
	assert!(matches!(
		o.repair_fresh(&sha(&tail)),
		Err(OwnerIoError2::Classified(
			Classification2::EvidenceInvalid { index: 0, .. }
		))
	));
	assert_eq!(fs::read(&bad).unwrap(), [0x5a; 200]);
}
#[test]
fn repair_directory_enumeration_is_bounded() {
	let s = Scratch::new();
	let o = s.open();
	let repairs = s.repairs();
	for i in 0..=MAX_REPAIR_ENTRIES2 {
		fs::write(repairs.join(format!("x{i:04}")), b"").unwrap();
	}
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::RepairDirFull
	);
	fs::remove_dir_all(&repairs).unwrap();
	let repairs = s.repairs();
	fs::create_dir(repairs.join("sub")).unwrap();
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::RepairDirForeign { index: 0 }
	);
	fs::remove_dir(repairs.join("sub")).unwrap();
	symlink(s.at("LOCK"), repairs.join("link")).unwrap();
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::RepairDirForeign { index: 0 }
	);
	fs::remove_file(repairs.join("link")).unwrap();
	fs::write(repairs.join(std::ffi::OsStr::from_bytes(b"\xff.rep")), b"").unwrap();
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::RepairDirForeign { index: 0 }
	);
	fs::remove_dir_all(&repairs).unwrap();
	let repairs = s.repairs();
	fs::write(repairs.join(vector_name("base")), vec![0; 4000]).unwrap();
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::EvidenceInvalid {
			index: 0,
			error: EvidenceError2::Length
		}
	);
}
#[test]
fn export_containment_names_and_exclusive_publication() {
	let s = Scratch::new();
	let out = Scratch::new();
	let mut o = s.open();
	o.apply(&op(1), &reserve("reserve")).unwrap();
	let sub = s.at("sub");
	fs::create_dir(&sub).unwrap();
	let nested = s.repairs().join("sub");
	fs::create_dir(&nested).unwrap();
	let link = out.at("to-owner");
	symlink(&s.0, &link).unwrap();
	for inside in [&s.0, &sub, &nested, &link] {
		assert_eq!(
			o.export(inside, "inv"),
			Err(OwnerIoError2::BadExportDir),
			"{inside:?}"
		);
	}
	fs::remove_dir(&nested).unwrap();
	fs::remove_dir(&sub).unwrap();
	let long = "a".repeat(65);
	for name in ["", ".hidden", "a/b", "x.tmp", "..", long.as_str()] {
		assert_eq!(
			o.export(&out.0, name),
			Err(OwnerIoError2::BadExportName),
			"{name:?}"
		);
	}
	// A sibling whose name merely extends the owner path is outside.
	let sibling = PathBuf::from(format!("{}-x", s.0.display()));
	fs::create_dir_all(&sibling).unwrap();
	let sibling_pin = o.export(&sibling, "inv");
	let _ = fs::remove_dir_all(&sibling);
	assert!(sibling_pin.is_ok());
	let pin = o.export(&out.0, "inv-1").unwrap();
	let bytes = fs::read(out.at("inv-1")).unwrap();
	assert_eq!(pin, sha(&bytes));
	let snapshot = decode_snapshot2(&bytes).unwrap();
	assert!(snapshot.export_seq == 1 && snapshot.entries.len() == 1);
	// Never replaced: the name is taken, and this export's tmp does not linger.
	assert_eq!(o.export(&out.0, "inv-1"), Err(OwnerIoError2::ExportExists));
	assert!(!out.at("inv-1.tmp").exists());
	assert_eq!(fs::read(out.at("inv-1")).unwrap(), bytes);
	fs::write(out.at("inv-2.tmp"), b"").unwrap();
	assert_eq!(
		o.export(&out.0, "inv-2"),
		Err(OwnerIoError2::ExportTmpExists)
	);
}
#[test]
fn export_refusals_do_no_work_and_clean_only_their_own_tmp() {
	let s = Scratch::new();
	let out = Scratch::new();
	let mut o = s.open();
	o.apply(&op(1), &reserve("reserve")).unwrap();
	o.export(&out.0, "inv").unwrap();
	// The refusal's cleanup can itself fail; the published file is untouched either way.
	{
		let _plan = seam::plan(&[(ExportTmpUnlink, 1)], None);
		assert_eq!(
			o.export(&out.0, "inv"),
			Err(OwnerIoError2::Io {
				step: ExportTmpUnlink,
				errno: Some(libc::EIO)
			})
		);
	}
	assert!(out.at("inv.tmp").exists());
	fs::remove_file(out.at("inv.tmp")).unwrap();
	assert_eq!(o.export(&out.0, "inv"), Err(OwnerIoError2::ExportExists));
	assert!(!out.at("inv.tmp").exists());
	// A latched object refuses before any path, name or publication work.
	{
		let _plan = seam::plan(&[(LogSync, 1)], None);
		assert!(
			o.apply(&op(2), &approve(id_of("reserve"), "approve"))
				.is_err()
		);
	}
	let _plan = seam::plan(&[], None);
	assert_eq!(
		o.export(&out.at("missing"), "bad/name"),
		Err(OwnerIoError2::Poisoned { step: LogSync })
	);
	assert!(!seam::seen().iter().any(|step| matches!(
		step,
		ExportCanonical | ExportDirOpen | ExportTmpCreate | ExportWrite | ExportLink
	)));
}
#[test]
fn repair_uses_the_reserved_slots_and_refuses_when_they_are_exhausted() {
	let tail = torn_tail();
	let mut log = p3_log_bytes()[..5 * E].to_vec();
	for seq in 6..=ORDINARY_EVENTS2 {
		log.extend(encoded3(&repair_at3(seq, seq)));
	}
	// At the ordinary cap with a torn tail: refused until repaired, then repaired at 65,001.
	let s = Scratch::new();
	fs::write(s.at("inventory.log"), [log.as_slice(), &tail].concat()).unwrap();
	let mut o = s.open();
	assert!(matches!(
		o.apply(
			&op(9),
			&OwnerRequest2::Reserve {
				manifest: AnyAdminManifest2::decode(&p3_variant(
					[0x5b; 16], [0x5c; 16], [0x5d; 32]
				))
				.unwrap()
			}
		),
		Err(OwnerIoError2::Classified(Classification2::Unclaimed { .. }))
	));
	assert!(matches!(
		o.repair_fresh(&sha(&tail)),
		Ok(RepairOutcome2::Repaired { seq, .. }) if seq == ORDINARY_EVENTS2 + 1
	));
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::Clean {
			last_seq: ORDINARY_EVENTS2 + 1
		}
	);
	assert_eq!(
		o.apply(
			&op(9),
			&OwnerRequest2::Reserve {
				manifest: AnyAdminManifest2::decode(&p3_variant(
					[0x5b; 16], [0x5c; 16], [0x5d; 32]
				))
				.unwrap()
			}
		),
		Err(OwnerIoError2::Decision(OwnerError2::LogFull))
	);
	drop(o);
	drop(s);
	// Every slot through 65,536 used: a tail needing 65,537 is refused, nothing truncated or published.
	for seq in ORDINARY_EVENTS2 + 1..=TOTAL_EVENTS2 {
		log.extend(encoded3(&repair_at3(seq, seq)));
	}
	let full = [log.as_slice(), &tail].concat();
	let s = Scratch::new();
	fs::write(s.at("inventory.log"), &full).unwrap();
	let mut o = s.open();
	assert!(matches!(
		o.inspect().unwrap().classification,
		Classification2::Unclaimed { boundary, .. } if boundary == TOTAL_EVENTS2 * E as u64
	));
	assert_eq!(
		o.repair_fresh(&sha(&tail)),
		Err(OwnerIoError2::EvidenceInvalid)
	);
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), full);
	assert!(!s.at("repairs").exists());
}
// A torn log at the third event's boundary whose evidence is already published: `log_tail` is what
// follows the prefix on disk (all or part of the original tail).
fn published(log_tail: &[u8]) -> Scratch {
	let s = Scratch::new();
	fs::write(
		s.at("inventory.log"),
		[&p3_log_bytes()[..B3], log_tail].concat(),
	)
	.unwrap();
	fs::write(
		s.repairs().join(vector_name("base")),
		vector("base", "evidence"),
	)
	.unwrap();
	s
}
// A repaired owner (valid evidence present) with only a durable marker left by an earlier run.
fn marked_with_evidence() -> Scratch {
	let s = torn_owner();
	let mut o = s.open();
	o.repair_fresh(&sha(&torn_tail())).unwrap();
	let _plan = seam::plan(&[(OwnerDirSync, 1)], None);
	assert_eq!(
		o.repair_check(),
		Err(OwnerIoError2::SyncFailed {
			step: OwnerDirSync,
			marker: MarkerOutcome2::Durable
		})
	);
	s
}
#[test]
fn clear_latches_evidence_sync_failures_and_refuses_unsafe_evidence() {
	for step in [EvidenceSync, RepairsDirSync] {
		let s = marked_with_evidence();
		let log = fs::read(s.at("inventory.log")).unwrap();
		let mut o = s.open();
		assert!(o.inspect().unwrap().poisoned.is_none());
		{
			let _plan = seam::plan(&[(step, 1)], None);
			assert_eq!(
				o.clear_sync_failure(&sha(&log)),
				Err(OwnerIoError2::SyncFailed {
					step,
					marker: MarkerOutcome2::AlreadyPresent
				})
			);
		}
		assert!(s.at("SYNC_FAILED").exists(), "{step:?}");
		assert_eq!(
			o.repair_check().err(),
			Some(OwnerIoError2::Poisoned { step })
		);
		assert_eq!(o.clear_sync_failure(&sha(&log)), Ok(()));
		assert!(!s.at("SYNC_FAILED").exists());
		assert_eq!(o.repair_check(), Ok(RepairOutcome2::NothingToRepair));
	}
	// A failed unlink is not a sync failure: no latch, and the marker still blocks.
	let s = marked_with_evidence();
	let log = fs::read(s.at("inventory.log")).unwrap();
	let mut o = s.open();
	{
		let _plan = seam::plan(&[(MarkerUnlink, 1)], None);
		assert_eq!(
			o.clear_sync_failure(&sha(&log)),
			Err(OwnerIoError2::Io {
				step: MarkerUnlink,
				errno: Some(libc::EIO)
			})
		);
	}
	assert!(s.at("SYNC_FAILED").exists());
	assert!(o.inspect().unwrap().poisoned.is_none());
	assert_eq!(
		o.repair_check().err(),
		Some(OwnerIoError2::SyncFailurePending)
	);
	// Invalid or conflicting evidence refuses the clear and keeps the marker.
	fs::write(
		s.at("repairs").join(format!("{}.rep", "a".repeat(64))),
		[0x5a; 200],
	)
	.unwrap();
	assert!(matches!(
		o.clear_sync_failure(&sha(&log)),
		Err(OwnerIoError2::ClearRefused(ClearRefusal2::Evidence(
			Classification2::EvidenceInvalid { .. }
		)))
	));
	assert!(s.at("SYNC_FAILED").exists());
	let prefix = p3_log_bytes()[..B3].to_vec();
	let tail = torn_tail();
	let s = Scratch::new();
	for log in [
		[prefix.as_slice(), &tail].concat(),
		[prefix.as_slice(), &tail[..30], &[0x01; 40]].concat(),
	] {
		let (name, bytes) = encode_evidence2(&log).unwrap();
		fs::write(s.repairs().join(name), bytes).unwrap();
	}
	let shared = [prefix.as_slice(), &tail[..30]].concat();
	fs::write(s.at("inventory.log"), &shared).unwrap();
	fs::write(s.at("SYNC_FAILED"), b"").unwrap();
	let mut o = s.open();
	assert_eq!(
		o.clear_sync_failure(&sha(&shared)),
		Err(OwnerIoError2::ClearRefused(ClearRefusal2::Evidence(
			Classification2::Conflicting { count: 2 }
		)))
	);
	assert!(s.at("SYNC_FAILED").exists());
}
#[test]
fn every_operation_sync_failure_latches_without_reporting_success() {
	let failed = |step| {
		Some(OwnerIoError2::SyncFailed {
			step,
			marker: MarkerOutcome2::Durable,
		})
	};
	// Ordinary append: the owner-directory sync after the log sync.
	let s = Scratch::new();
	let mut o = s.open();
	{
		let _plan = seam::plan(&[(OwnerDirSync, 1)], None);
		assert_eq!(
			o.apply(&op(1), &reserve("reserve")).err(),
			failed(OwnerDirSync)
		);
	}
	assert!(s.at("SYNC_FAILED").exists());
	assert_eq!(
		o.apply(&op(2), &reserve("reserve_second")).err(),
		Some(OwnerIoError2::Poisoned { step: OwnerDirSync })
	);
	// Repair resume from published evidence: the repairs/ sync and the truncate sync.
	for step in [RepairsDirSync, TruncateSync] {
		let s = published(&torn_tail());
		let mut o = s.open();
		{
			let _plan = seam::plan(&[(step, 1)], None);
			assert_eq!(
				o.repair_resume(&base_context()).err(),
				failed(step),
				"{step:?}"
			);
		}
		assert!(s.at("SYNC_FAILED").exists());
		assert_eq!(
			o.repair_check().err(),
			Some(OwnerIoError2::Poisoned { step })
		);
	}
	// Export: the snapshot sync before publication, and the directory sync after it.
	for step in [ExportSync, ExportDirSync] {
		let s = Scratch::new();
		let out = Scratch::new();
		let mut o = s.open();
		o.apply(&op(1), &reserve("reserve")).unwrap();
		{
			let _plan = seam::plan(&[(step, 1)], None);
			assert_eq!(o.export(&out.0, "inv").err(), failed(step), "{step:?}");
		}
		assert_eq!(out.at("inv").exists(), step == ExportDirSync);
		assert!(s.at("SYNC_FAILED").exists());
		assert_eq!(
			o.export(&out.0, "other").err(),
			Some(OwnerIoError2::Poisoned { step })
		);
	}
}
#[test]
fn resume_resyncs_published_evidence_before_truncating() {
	let tail = torn_tail();
	let repaired = [
		&p3_log_bytes()[..B3],
		vector("base", "expected_repair").as_slice(),
	]
	.concat();
	let done = Ok(RepairOutcome2::Repaired {
		context: base_context(),
		seq: 4,
	});
	let s = published(&tail);
	let mut o = s.open();
	{
		let _plan = seam::plan(&[], None);
		assert_eq!(o.repair_resume(&base_context()), done);
		assert!(in_order(
			&seam::seen(),
			&[
				EvidenceSync,
				RepairsDirSync,
				OwnerDirSync,
				Truncate,
				TruncateSync,
				LogWrite
			]
		));
	}
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), repaired);
	// A failed evidence or repairs/ sync stops before any truncation or write.
	for step in [EvidenceSync, RepairsDirSync] {
		let s = published(&tail);
		let before = fs::read(s.at("inventory.log")).unwrap();
		let mut o = s.open();
		let _plan = seam::plan(&[(step, 1)], None);
		assert!(matches!(
			o.repair_resume(&base_context()),
			Err(OwnerIoError2::SyncFailed { step: s, .. }) if s == step
		));
		assert!(
			!seam::seen()
				.iter()
				.any(|x| matches!(x, Truncate | TruncateSync | LogWrite))
		);
		assert_eq!(fs::read(s.at("inventory.log")).unwrap(), before);
	}
	// r7 R6: only part of the original tail persisted (0 < N < tail length).
	let s = published(&tail[..40]);
	let mut o = s.open();
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::Pending {
			context: base_context(),
			stage: RepairStage2::NotTruncated
		}
	);
	assert_eq!(o.repair_resume(&base_context()), done);
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), repaired);
	assert_eq!(
		fs::read(s.at("repairs").join(vector_name("base"))).unwrap(),
		vector("base", "evidence")
	);
}
/// The live owner over a log whose third event carries a version it cannot read (3): apply,
/// export and repair check refuse at the classification, before any log append, truncation,
/// evidence or export file, and no byte or directory entry changes.
#[test]
fn owner_refuses_an_unreadable_event_version_before_any_write() {
	let mut third = p3_event_bytes("reserve_second");
	third[8..10].copy_from_slice(&3u16.to_le_bytes());
	let log = [
		p3_event_bytes("reserve"),
		p3_event_bytes("approve"),
		reseal_event3(third),
	]
	.concat();
	let s = Scratch::new();
	let out = Scratch::bare();
	fs::write(s.at("inventory.log"), &log).unwrap();
	let mut o = s.open();
	let entries = |p: &Path| {
		let mut v: Vec<_> = fs::read_dir(p)
			.unwrap()
			.map(|e| e.unwrap().file_name().into_string().unwrap())
			.collect();
		v.sort();
		v
	};
	let before = entries(&s.0);
	assert_eq!(before, ["LOCK", "OWNER_FORMAT", "inventory.log"]);
	let refused = Err(OwnerIoError2::Classified(Classification2::Log(
		LogError2::Corrupt {
			seq: 3,
			reason: CorruptReason2::Version,
		},
	)));
	let _plan = seam::plan(&[], None);
	let fresh = AnyAdminManifest2::decode(&p3_variant([0x5b; 16], [0x5c; 16], [0x5d; 32])).unwrap();
	assert_eq!(
		o.apply(&op(0x31), &OwnerRequest2::Reserve { manifest: fresh })
			.map(|_| ()),
		refused
	);
	assert_eq!(o.export(&out.0, "inv").map(|_| ()), refused);
	assert_eq!(o.repair_check().map(|_| ()), refused);
	let seen = seam::seen();
	for step in [
		LogCreate,
		LogWrite,
		LogSync,
		Truncate,
		RepairsCreate,
		EvidenceTmpCreate,
		ExportTmpCreate,
		ExportWrite,
		MarkerCreate,
	] {
		assert!(!seen.contains(&step), "{step:?} in {seen:?}");
	}
	assert_eq!(entries(&s.0), before);
	assert!(entries(&out.0).is_empty());
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), log);
}
