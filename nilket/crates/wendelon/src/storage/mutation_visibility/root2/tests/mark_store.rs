//! Stage M: the dormant recovery-mark store. Fault plans fail a step before its system call
//! (`Before`) or after it succeeded (`After`); a stop panics after a successful call and the
//! directory is reopened by a fresh store. That models process interruption with the same kernel
//! page cache, not power loss or reordered unsynced metadata, which only the module's conditional
//! argument covers.
use crate::storage::mutation_visibility::root2::durable::mark_store::seam::{self, Fault};
use crate::storage::mutation_visibility::root2::durable::mark_store::{
	MarkIoError2, MarkOpen2, MarkStep2, MarkStore2, MarkWrite2,
};
use crate::storage::mutation_visibility::root2::recovery::{
	Baseline2, Deployment2, FieldError2, RecoveryError2, RecoveryMark2, encode_recovery_mark2,
};
use crate::storage::mutation_visibility::root2::types::Log2;
use MarkStep2 as S;
use std::os::unix::fs::PermissionsExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DEPLOYMENT: Deployment2 = Deployment2 {
	generation: [7; 16],
	cluster: [9; 16],
};

fn dir(tag: &str) -> PathBuf {
	static NEXT: AtomicU64 = AtomicU64::new(0);
	let dir = std::env::temp_dir().join(format!(
		"wendelon-mark-store-{tag}-{}-{}",
		std::process::id(),
		NEXT.fetch_add(1, Ordering::Relaxed)
	));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(&dir).unwrap();
	std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
	dir
}
fn mark(index: u64) -> RecoveryMark2 {
	RecoveryMark2 {
		generation: DEPLOYMENT.generation,
		cluster: DEPLOYMENT.cluster,
		cut: Log2 {
			term: 2,
			node: 1,
			index,
		},
		fingerprint: [index as u8; 32],
		command: [0xc0; 32],
		state: [0x5a; 32],
	}
}
fn bytes(b: &Baseline2) -> Vec<u8> {
	encode_recovery_mark2(b).unwrap().to_vec()
}
fn mark_file(d: &Path) -> PathBuf {
	d.join("RECOVERY_MARK")
}
fn tmp_file(d: &Path) -> PathBuf {
	d.join("RECOVERY_MARK.tmp")
}
fn open(d: &Path) -> (MarkStore2, MarkOpen2) {
	MarkStore2::open(d).unwrap()
}
fn reopen(d: &Path) -> MarkOpen2 {
	open(d).1
}
/// A directory whose confirmed mark is `mark(index)`.
fn at(tag: &str, index: u64) -> PathBuf {
	let d = dir(tag);
	let (mut s, _) = open(&d);
	s.provision().unwrap();
	s.advance(&DEPLOYMENT, &mark(index)).unwrap();
	d
}
/// Runs `f`; `None` if it was stopped by the seam's crash stop.
fn interrupted<T>(f: impl FnOnce() -> T) -> Option<T> {
	catch_unwind(AssertUnwindSafe(f)).ok()
}

#[test]
fn open_classifies_and_confirms() {
	let d = dir("absent");
	let _plan = seam::plan(&[], None);
	assert_eq!(reopen(&d), MarkOpen2::Absent);
	std::fs::write(tmp_file(&d), b"partial").unwrap();
	assert_eq!(reopen(&d), MarkOpen2::TmpOnly);

	let d = dir("confirmed");
	std::fs::write(mark_file(&d), bytes(&Baseline2::Provisioned)).unwrap();
	let _plan = seam::plan(&[], None);
	assert_eq!(reopen(&d), MarkOpen2::Confirmed(Baseline2::Provisioned));
	let seen = seam::seen();
	assert_eq!(
		&seen[seen.len() - 2..],
		[S::ConfirmFileSync, S::ConfirmDirSync]
	);

	let valid = bytes(&Baseline2::Mark(mark(5)));
	let with = |at: std::ops::Range<usize>, value: u8| {
		let mut b = valid.clone();
		b[at].iter_mut().for_each(|x| *x = value);
		b
	};
	let mut padded = bytes(&Baseline2::Provisioned);
	padded[100] = 1;
	let cases = [
		(valid[..158].to_vec(), RecoveryError2::Length),
		([valid.clone(), vec![0]].concat(), RecoveryError2::Length),
		(with(0..1, b'X'), RecoveryError2::Magic),
		(with(8..9, 9), RecoveryError2::Version),
		(with(10..11, 3), RecoveryError2::Tag),
		(padded, RecoveryError2::Padding),
		(
			with(11..27, 0),
			RecoveryError2::Field(FieldError2::Generation),
		),
		(with(27..43, 0), RecoveryError2::Field(FieldError2::Cluster)),
		(with(43..51, 0), RecoveryError2::Field(FieldError2::Cut)),
		(with(51..55, 0), RecoveryError2::Field(FieldError2::Cut)),
		(with(55..63, 0), RecoveryError2::Field(FieldError2::Cut)),
	];
	for (raw, want) in cases {
		let d = dir("corrupt");
		std::fs::write(mark_file(&d), &raw).unwrap();
		let _plan = seam::plan(&[], None);
		assert_eq!(reopen(&d), MarkOpen2::Corrupt(want));
		assert!(
			!seam::seen().contains(&S::ConfirmFileSync),
			"{want:?}: no sync"
		);
		assert_eq!(
			std::fs::read(mark_file(&d)).unwrap(),
			raw,
			"{want:?}: untouched"
		);
	}

	for step in [S::ConfirmFileSync, S::ConfirmDirSync] {
		for kind in [Fault::Before, Fault::After] {
			let d = dir("confirm-fault");
			std::fs::write(mark_file(&d), &valid).unwrap();
			let _plan = seam::plan(&[(step, 1, kind)], None);
			assert_eq!(
				MarkStore2::open(&d).err(),
				Some(MarkIoError2::Uncertain { step }),
				"{step:?} {kind:?}"
			);
		}
	}
	let d = dir("lock-sync");
	let _plan = seam::plan(&[(S::LockCreateDirSync, 1, Fault::Before)], None);
	assert_eq!(
		MarkStore2::open(&d).err(),
		Some(MarkIoError2::Uncertain {
			step: S::LockCreateDirSync
		})
	);
}

#[test]
fn provision_matrix() {
	use MarkOpen2::{Absent, Confirmed, TmpOnly};
	let provisioned = Confirmed(Baseline2::Provisioned);
	// (step, needs a stale tmp, error, reopened state after Before, after After or a stop)
	let rows = [
		(
			S::TmpUnlink,
			true,
			MarkIoError2::Io {
				step: S::TmpUnlink,
				errno: Some(libc::EIO),
			},
			TmpOnly,
			Absent,
		),
		(
			S::TmpUnlinkDirSync,
			true,
			MarkIoError2::Uncertain {
				step: S::TmpUnlinkDirSync,
			},
			Absent,
			Absent,
		),
		(
			S::TmpCreate,
			false,
			MarkIoError2::Io {
				step: S::TmpCreate,
				errno: Some(libc::EIO),
			},
			Absent,
			TmpOnly,
		),
		(
			S::TmpWrite,
			false,
			MarkIoError2::Io {
				step: S::TmpWrite,
				errno: Some(libc::EIO),
			},
			TmpOnly,
			TmpOnly,
		),
		(
			S::TmpSync,
			false,
			MarkIoError2::Poisoned { step: S::TmpSync },
			TmpOnly,
			TmpOnly,
		),
		(
			S::Link,
			false,
			MarkIoError2::Uncertain { step: S::Link },
			TmpOnly,
			provisioned,
		),
		(
			S::LinkTmpUnlink,
			false,
			MarkIoError2::Uncertain {
				step: S::LinkTmpUnlink,
			},
			provisioned,
			provisioned,
		),
		(
			S::DirSync,
			false,
			MarkIoError2::Uncertain { step: S::DirSync },
			provisioned,
			provisioned,
		),
	];
	for (step, stale, error, before, after) in rows {
		for (kind, want) in [
			(Some(Fault::Before), before),
			(Some(Fault::After), after),
			(None, after),
		] {
			let d = dir("provision");
			if stale {
				std::fs::write(tmp_file(&d), b"stale").unwrap();
			}
			let (mut s, _) = open(&d);
			let faults: Vec<_> = kind.map(|k| (step, 1, k)).into_iter().collect();
			let stop = kind.is_none().then_some((step, 1));
			let _plan = seam::plan(&faults, stop);
			match interrupted(|| s.provision()) {
				Some(result) => {
					if step == S::TmpSync {
						assert!(
							!seam::seen().contains(&S::Link),
							"no link after a failed tmp sync"
						);
					}
					assert_eq!(result, Err(error), "{step:?} {kind:?}");
					assert_eq!(s.durable(), None, "{step:?}: poison clears durable");
					let _clear = seam::plan(&[], None);
					assert!(matches!(s.provision(), Err(MarkIoError2::Poisoned { .. })));
					assert!(seam::seen().is_empty(), "{step:?}: no I/O when poisoned");
				}
				None => assert!(kind.is_none(), "{step:?}: only a stop panics"),
			}
			drop(s);
			let _plan = seam::plan(&[], None);
			let (mut s, state) = open(&d);
			assert_eq!(state, want, "{step:?} {kind:?}");
			let retry = s.provision().unwrap();
			if state == provisioned {
				assert_eq!(retry, MarkWrite2::Unchanged(Baseline2::Provisioned));
			} else {
				assert_eq!(retry, MarkWrite2::Durable(Baseline2::Provisioned));
			}
		}
	}

	// A contender planted after ReRead: the link refuses with EEXIST, which is never success.
	let d = dir("eexist");
	let (mut s, _) = open(&d);
	let _plan = seam::plan(&[], None);
	let contender = mark_file(&d);
	seam::hook(S::Link, move || {
		std::fs::write(&contender, bytes(&Baseline2::Provisioned)).unwrap()
	});
	assert_eq!(
		s.provision(),
		Err(MarkIoError2::Uncertain { step: S::Link })
	);
	drop(s);
	let (mut s, state) = open(&d);
	assert_eq!(state, provisioned);
	assert_eq!(
		s.provision(),
		Ok(MarkWrite2::Unchanged(Baseline2::Provisioned))
	);
	// A contender present before ReRead is interference.
	let d = dir("interference");
	let (mut s, _) = open(&d);
	std::fs::write(mark_file(&d), bytes(&Baseline2::Provisioned)).unwrap();
	assert_eq!(s.provision(), Err(MarkIoError2::Interference));
	assert_eq!(s.durable(), None);
}

#[test]
fn advance_matrix() {
	let (old, new) = (mark(5), mark(9));
	let (was, now) = (
		MarkOpen2::Confirmed(Baseline2::Mark(old)),
		MarkOpen2::Confirmed(Baseline2::Mark(new)),
	);
	let rows = [
		(
			S::TmpUnlink,
			true,
			MarkIoError2::Io {
				step: S::TmpUnlink,
				errno: Some(libc::EIO),
			},
			was,
			was,
		),
		(
			S::TmpUnlinkDirSync,
			true,
			MarkIoError2::Uncertain {
				step: S::TmpUnlinkDirSync,
			},
			was,
			was,
		),
		(
			S::TmpCreate,
			false,
			MarkIoError2::Io {
				step: S::TmpCreate,
				errno: Some(libc::EIO),
			},
			was,
			was,
		),
		(
			S::TmpWrite,
			false,
			MarkIoError2::Io {
				step: S::TmpWrite,
				errno: Some(libc::EIO),
			},
			was,
			was,
		),
		(
			S::TmpSync,
			false,
			MarkIoError2::Poisoned { step: S::TmpSync },
			was,
			was,
		),
		(
			S::Rename,
			false,
			MarkIoError2::Uncertain { step: S::Rename },
			was,
			now,
		),
		(
			S::DirSync,
			false,
			MarkIoError2::Uncertain { step: S::DirSync },
			now,
			now,
		),
	];
	for (step, stale, error, before, after) in rows {
		for (kind, want) in [
			(Some(Fault::Before), before),
			(Some(Fault::After), after),
			(None, after),
		] {
			let d = at("advance", 5);
			if stale {
				std::fs::write(tmp_file(&d), b"stale").unwrap();
			}
			let (mut s, state) = open(&d);
			assert_eq!(state, was);
			let faults: Vec<_> = kind.map(|k| (step, 1, k)).into_iter().collect();
			let stop = kind.is_none().then_some((step, 1));
			let _plan = seam::plan(&faults, stop);
			match interrupted(|| s.advance(&DEPLOYMENT, &new)) {
				Some(result) => {
					assert_eq!(result, Err(error), "{step:?} {kind:?}");
					assert_eq!(s.durable(), None);
				}
				None => assert!(kind.is_none()),
			}
			if step == S::TmpSync && kind.is_some() {
				assert!(
					!seam::seen().contains(&S::Rename),
					"no rename after a failed tmp sync"
				);
			}
			drop(s);
			let _plan = seam::plan(&[], None);
			let (mut s, state) = open(&d);
			assert_eq!(
				state, want,
				"{step:?} {kind:?}: old or new, never absent or corrupt"
			);
			// Completing the interrupted operation means retrying the requested new mark.
			let retry = s.advance(&DEPLOYMENT, &new).unwrap();
			if state == now {
				assert_eq!(retry, MarkWrite2::Unchanged(Baseline2::Mark(new)));
			} else {
				assert_eq!(retry, MarkWrite2::Durable(Baseline2::Mark(new)));
			}
			assert_eq!(s.durable(), Some(Baseline2::Mark(new)));
		}
	}
}

#[test]
fn advance_rules() {
	let d = at("rules", 5);
	let (mut s, _) = open(&d);
	let _plan = seam::plan(&[], None);
	let with = |f: &dyn Fn(&mut RecoveryMark2)| {
		let mut m = mark(5);
		f(&mut m);
		m
	};
	let pure = [
		(
			with(&|m| m.generation = [0; 16]),
			MarkIoError2::Invalid(FieldError2::Generation),
		),
		(
			with(&|m| m.cluster = [0; 16]),
			MarkIoError2::Invalid(FieldError2::Cluster),
		),
		(
			with(&|m| m.cut.term = 0),
			MarkIoError2::Invalid(FieldError2::Cut),
		),
		(
			with(&|m| m.cut.node = 0),
			MarkIoError2::Invalid(FieldError2::Cut),
		),
		(
			with(&|m| m.cut.index = 0),
			MarkIoError2::Invalid(FieldError2::Cut),
		),
		(with(&|m| m.generation = [8; 16]), MarkIoError2::Identity),
		(mark(3), MarkIoError2::Regressed),
		(with(&|m| m.cut.term = 3), MarkIoError2::Conflict),
		(with(&|m| m.cut.node = 2), MarkIoError2::Conflict),
		(with(&|m| m.fingerprint = [1; 32]), MarkIoError2::Conflict),
		(with(&|m| m.command = [1; 32]), MarkIoError2::Conflict),
		(with(&|m| m.state = [1; 32]), MarkIoError2::Conflict),
	];
	for (m, want) in pure {
		assert_eq!(s.advance(&DEPLOYMENT, &m), Err(want));
	}
	let other = Deployment2 {
		generation: [1; 16],
		cluster: DEPLOYMENT.cluster,
	};
	assert_eq!(s.advance(&other, &mark(9)), Err(MarkIoError2::Identity));
	assert!(
		seam::seen().is_empty(),
		"rejected arguments do no filesystem I/O"
	);
	assert_eq!(s.durable(), Some(Baseline2::Mark(mark(5))), "not poisoned");
	assert_eq!(
		s.advance(&DEPLOYMENT, &mark(5)),
		Ok(MarkWrite2::Unchanged(Baseline2::Mark(mark(5))))
	);
	assert_eq!(
		seam::seen(),
		[S::DirCheck, S::ReRead],
		"exact retry rechecks, writes nothing"
	);

	let (mut s, _) = open(&dir("unprovisioned"));
	assert_eq!(
		s.advance(&DEPLOYMENT, &mark(5)),
		Err(MarkIoError2::NotProvisioned)
	);
	let d = dir("corrupt");
	std::fs::write(mark_file(&d), b"corrupt").unwrap();
	let (mut s, _) = open(&d);
	assert_eq!(s.advance(&DEPLOYMENT, &mark(5)), Err(MarkIoError2::Corrupt));
}

#[test]
fn poisoned_handles_never_authorize() {
	let causes: [(&str, Option<(MarkStep2, Fault)>); 3] = [
		("tmp sync", Some((S::TmpSync, Fault::Before))),
		("rename after success", Some((S::Rename, Fault::After))),
		("interference", None),
	];
	for (name, fault) in causes {
		let d = at("poison", 5);
		let (mut s, _) = open(&d);
		let faults: Vec<_> = fault.map(|(st, k)| (st, 1, k)).into_iter().collect();
		let _plan = seam::plan(&faults, None);
		if fault.is_none() {
			std::fs::write(mark_file(&d), bytes(&Baseline2::Mark(mark(6)))).unwrap();
		}
		assert!(s.advance(&DEPLOYMENT, &mark(9)).is_err(), "{name}");
		assert_eq!(
			s.durable(),
			None,
			"{name}: poison clears durable immediately"
		);
		let _clear = seam::plan(&[], None);
		assert!(matches!(s.provision(), Err(MarkIoError2::Poisoned { .. })));
		assert!(matches!(
			s.advance(&DEPLOYMENT, &mark(9)),
			Err(MarkIoError2::Poisoned { .. })
		));
		assert!(
			seam::seen().is_empty(),
			"{name}: a poisoned handle does no I/O"
		);
	}
}

#[test]
fn paths_and_identity() {
	let d = dir("busy");
	let held = open(&d);
	assert_eq!(MarkStore2::open(&d).err(), Some(MarkIoError2::Busy));
	drop(held);

	let d = dir("symlink-mark");
	std::fs::write(d.join("target"), bytes(&Baseline2::Provisioned)).unwrap();
	std::os::unix::fs::symlink(d.join("target"), mark_file(&d)).unwrap();
	assert_eq!(
		MarkStore2::open(&d).err(),
		Some(MarkIoError2::Io {
			step: S::MarkRead,
			errno: Some(libc::ELOOP)
		})
	);
	let d = dir("symlink-lock");
	std::fs::write(d.join("target"), b"").unwrap();
	std::os::unix::fs::symlink(d.join("target"), d.join("LOCK")).unwrap();
	assert_eq!(
		MarkStore2::open(&d).err(),
		Some(MarkIoError2::Io {
			step: S::LockOpen,
			errno: Some(libc::ELOOP)
		})
	);
	let d = dir("dir-mark");
	std::fs::create_dir(mark_file(&d)).unwrap();
	assert_eq!(
		MarkStore2::open(&d).err(),
		Some(MarkIoError2::NotRegular { step: S::MarkRead })
	);
	let d = dir("fifo-mark");
	let status = std::process::Command::new("mkfifo")
		.arg(mark_file(&d))
		.status()
		.unwrap();
	assert!(status.success());
	assert_eq!(
		MarkStore2::open(&d).err(),
		Some(MarkIoError2::NotRegular { step: S::MarkRead })
	);
	let d = dir("insecure");
	std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o777)).unwrap();
	assert_eq!(MarkStore2::open(&d).err(), Some(MarkIoError2::InsecureDir));
	let d = dir("file");
	std::fs::write(d.join("plain"), b"").unwrap();
	assert_eq!(
		MarkStore2::open(&d.join("plain")).err(),
		Some(MarkIoError2::NotDirectory)
	);
	let d = dir("oversized");
	std::fs::write(mark_file(&d), vec![0u8; 1 << 20]).unwrap();
	assert_eq!(reopen(&d), MarkOpen2::Corrupt(RecoveryError2::Length));

	let d = at("replaced", 5);
	let (mut s, _) = open(&d);
	let moved = d.with_extension("moved");
	std::fs::rename(&d, &moved).unwrap();
	std::fs::create_dir(&d).unwrap();
	std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o700)).unwrap();
	assert_eq!(
		s.advance(&DEPLOYMENT, &mark(9)),
		Err(MarkIoError2::DirectoryChanged)
	);
	assert_eq!(s.durable(), None);

	let d = at("rewritten", 5);
	let (mut s, _) = open(&d);
	std::fs::write(mark_file(&d), bytes(&Baseline2::Mark(mark(6)))).unwrap();
	assert_eq!(
		s.advance(&DEPLOYMENT, &mark(9)),
		Err(MarkIoError2::Interference)
	);
}

#[test]
fn no_accidental_reset() {
	let d = at("mark-kept", 5);
	let before = std::fs::read(mark_file(&d)).unwrap();
	let (mut s, _) = open(&d);
	assert_eq!(s.provision(), Err(MarkIoError2::AlreadyProvisioned));
	assert_eq!(s.durable(), Some(Baseline2::Mark(mark(5))));
	drop(s);
	assert_eq!(std::fs::read(mark_file(&d)).unwrap(), before);

	let d = dir("corrupt-kept");
	std::fs::write(mark_file(&d), b"not a mark").unwrap();
	let (mut s, _) = open(&d);
	assert_eq!(s.provision(), Err(MarkIoError2::AlreadyProvisioned));
	assert_eq!(s.advance(&DEPLOYMENT, &mark(5)), Err(MarkIoError2::Corrupt));
	drop(s);
	assert_eq!(std::fs::read(mark_file(&d)).unwrap(), b"not a mark");
	assert_eq!(reopen(&d), MarkOpen2::Corrupt(RecoveryError2::Length));

	// An uncertain advance never leaves Provisioned or nothing behind.
	let d = at("uncertain-kept", 5);
	let (mut s, _) = open(&d);
	let _plan = seam::plan(&[(S::Rename, 1, Fault::Before)], None);
	assert!(s.advance(&DEPLOYMENT, &mark(9)).is_err());
	assert!(matches!(s.provision(), Err(MarkIoError2::Poisoned { .. })));
	drop(s);
	let _plan = seam::plan(&[], None);
	assert_eq!(reopen(&d), MarkOpen2::Confirmed(Baseline2::Mark(mark(5))));
}
