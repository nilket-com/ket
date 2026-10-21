//! Dormant owner-side I/O over one trusted owner directory: the lock, the inventory log, repair
//! evidence, the sync-failure latch and marker, snapshot export, and the exported provisioning tuple
//! (one Bound root's exact manifest plus the snapshot, from one folded state, published as two
//! independent exclusive files; never atomic as a pair). Every decision, encoding and
//! classification comes unchanged from the pure core; this layer only reads, writes and syncs.
//! Supported path model: Linux, a local filesystem, one owner host, and trusted owner and export
//! parent paths that are not replaced while an operation runs. Nothing here yields a root,
//! authority state or trust token, and a marker's absence never proves no sync failed.
use super::inventory::RootState2;
use super::inventory::v2 as events;
use super::owner_core::v2 as decisions;
use super::owner_core::{
	Classification2, Decision2, EventRef2, OpId2, OwnerError2, OwnerRequest2, event_request_key2,
};
use super::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin::manifest::ManifestProfile2;
use crate::storage::mutation_visibility::root2::types::Digest2;
use std::fs::{self, File, TryLockError};
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const LOCK: &str = "LOCK";
const LOG: &str = "inventory.log";
const REPAIRS: &str = "repairs";
const MARKER: &str = "SYNC_FAILED";
const MARKER_MAGIC: &[u8; 8] = b"VISSYNC1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum IoStep2 {
	LockOpen,
	LockCreate,
	LockCreateDirSync,
	Lock,
	MarkerStat,
	MarkerCreate,
	MarkerWrite,
	MarkerSync,
	MarkerDirSync,
	MarkerUnlink,
	LogOpen,
	LogRead,
	LogCreate,
	LogWrite,
	LogSync,
	OwnerDirSync,
	RepairsOpen,
	RepairsEnum,
	RepairsCreate,
	RepairsDirSync,
	EvidenceRead,
	EvidenceTmpCreate,
	EvidenceTmpUnlink,
	EvidenceWrite,
	EvidenceSync,
	EvidenceLink,
	Truncate,
	TruncateSync,
	VerifyRead,
	ExportDirOpen,
	ExportCanonical,
	ExportTmpCreate,
	ExportWrite,
	ExportSync,
	ExportLink,
	ExportTmpUnlink,
	ExportDirSync,
	ClearDirSync,
	// The provisioning tuple's manifest artifact (appended: existing marker step codes unchanged).
	ManifestTmpCreate,
	ManifestWrite,
	ManifestSync,
	ManifestLink,
	ManifestTmpUnlink,
	ManifestDirSync,
	FormatTempCreate,
	FormatWrite,
	FormatSync,
	FormatPublish,
	FormatDirSync,
	FormatLogCreate,
	FormatLogSync,
	FormatFinalSync,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum MarkerOutcome2 {
	Durable,
	/// Presence only: never proven durable.
	AlreadyPresent,
	NotDurable,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ClearRefusal2 {
	NonRegularMarker,
	Evidence(Classification2),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum OwnerIoError2 {
	Incomplete,
	MarkerCorrupt,
	ForeignFormat,
	/// No `OWNER_FORMAT` marker: an unmarked (pre-v2) directory is never opened or upgraded.
	Unformatted,
	CreationNotEmpty,
	Busy,
	InsecureDir,
	NotRegular {
		step: IoStep2,
	},
	Io {
		step: IoStep2,
		errno: Option<i32>,
	},
	OpenSyncFailed {
		step: IoStep2,
	},
	SyncFailed {
		step: IoStep2,
		marker: MarkerOutcome2,
	},
	Poisoned {
		step: IoStep2,
	},
	SyncFailurePending,
	NothingToClear,
	ClearRefused(ClearRefusal2),
	ExpectLogMismatch {
		actual: Digest2,
	},
	ExpectTailMismatch {
		actual: Digest2,
	},
	Classified(Classification2),
	Decision(OwnerError2),
	EvidenceInvalid,
	RepairContextMismatch,
	RepairVerifyFailed,
	BadExportName,
	BadExportDir,
	ExportTmpExists,
	ExportExists,
	ExportNoSuchRoot,
	ExportNotBound(RootState2),
	ExportNotTerminal(RootState2),
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct OpOutcome2 {
	pub original: Option<EventRef2>,
	pub current: RootState2,
}
redacted_debug!(OpOutcome2, "OpOutcome2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum RepairOutcome2 {
	Repaired { context: Digest2, seq: u64 },
	NothingToRepair,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct OwnerStatus2 {
	pub classification: Classification2,
	pub marker_present: bool,
	pub poisoned: Option<IoStep2>,
	pub log_present: bool,
}
redacted_debug!(OwnerStatus2, "OwnerStatus2");
/// The result of a complete tuple export: both artifacts published and their directory synced.
/// Descriptive only. The manifest pin is the folded root's whole-manifest pin (the domain-separated
/// manifest digest the event header and install bind); the inventory pin is the snapshot hash.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct TupleExport2 {
	pub manifest_pin: Digest2,
	pub inventory_pin: Digest2,
	pub export_seq: u64,
	pub profile: ManifestProfile2,
}
redacted_debug!(TupleExport2, "TupleExport2");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct TerminalExport3 {
	pub manifest_pin: Digest2,
	pub inventory_pin: Digest2,
	pub terminal_pin: Digest2,
	pub export_seq: u64,
}
redacted_debug!(TerminalExport3, "TerminalExport3");
pub(in crate::storage::mutation_visibility::root2) struct OwnerDir2 {
	path: PathBuf,
	dir: File,
	id: (u64, u64),
	lock: File,
	poisoned: Option<IoStep2>,
}

impl Drop for OwnerDir2 {
	/// Unlock before the descriptor closes, so a concurrently forked child that
	/// shares the open file description cannot extend the lock past this drop.
	fn drop(&mut self) {
		let _ = self.lock.unlock();
	}
}
redacted_debug!(OwnerDir2, "OwnerDir2");

mod directory;
mod ops;
use ops::{
	Mode, Owned, checked_dir, export_name_ok, io_err, manifest_steps, open_dir, open_file, regular,
	run, sha, snapshot_steps,
};

impl OwnerDir2 {
	fn locked(path: &Path) -> Owned<Self> {
		let path = fs::canonicalize(path).map_err(io_err(IoStep2::LockOpen))?;
		let dir = open_dir(&path).map_err(io_err(IoStep2::LockOpen))?;
		let id = checked_dir(&dir, IoStep2::LockOpen)?;
		let lock_path = path.join(LOCK);
		let lock = match run(IoStep2::LockOpen, || open_file(&lock_path, Mode::ReadWrite)) {
			Ok(f) => regular(f, IoStep2::LockOpen)?,
			Err(e) if e.kind() == ErrorKind::NotFound => {
				match run(IoStep2::LockCreate, || {
					open_file(&lock_path, Mode::Create(0o600))
				}) {
					Ok(f) => {
						let f = regular(f, IoStep2::LockCreate)?;
						// The lock is not yet held, so no marker may be written here.
						if run(IoStep2::LockCreateDirSync, || dir.sync_all()).is_err() {
							return Err(OwnerIoError2::OpenSyncFailed {
								step: IoStep2::LockCreateDirSync,
							});
						}
						f
					}
					Err(e) if e.kind() == ErrorKind::AlreadyExists => {
						let f = run(IoStep2::LockOpen, || open_file(&lock_path, Mode::ReadWrite))
							.map_err(io_err(IoStep2::LockOpen))?;
						regular(f, IoStep2::LockOpen)?
					}
					Err(e) => return Err(io_err(IoStep2::LockCreate)(e)),
				}
			}
			Err(e) => return Err(io_err(IoStep2::LockOpen)(e)),
		};
		run(IoStep2::Lock, || Ok(())).map_err(io_err(IoStep2::Lock))?;
		match lock.try_lock() {
			Ok(()) => {}
			Err(TryLockError::WouldBlock) => return Err(OwnerIoError2::Busy),
			Err(TryLockError::Error(e)) => return Err(io_err(IoStep2::Lock)(e)),
		}
		Ok(Self {
			path,
			dir,
			id,
			lock,
			poisoned: None,
		})
	}

	pub(in crate::storage::mutation_visibility::root2) fn inspect(&self) -> Owned<OwnerStatus2> {
		let marker_present = self.marker_stat()?.is_some();
		let view = self.view()?;
		Ok(OwnerStatus2 {
			classification: view.classification,
			marker_present,
			poisoned: self.poisoned,
			log_present: view.log.is_some(),
		})
	}

	pub(in crate::storage::mutation_visibility::root2) fn apply(
		&mut self,
		op: &OpId2,
		req: &OwnerRequest2,
	) -> Owned<OpOutcome2> {
		self.check_framing()?;
		self.blocked()?;
		let view = self.clean_view()?;
		match decisions::decide2(view.bytes(), op, req) {
			Decision2::Refuse(e) => Err(OwnerIoError2::Decision(e)),
			Decision2::Retry { original, current } => {
				self.resync(view.log.is_some(), &[])?;
				Ok(OpOutcome2 {
					original: Some(original),
					current,
				})
			}
			Decision2::NoOp(current) => {
				self.resync(view.log.is_some(), &[])?;
				Ok(OpOutcome2 {
					original: None,
					current,
				})
			}
			Decision2::Append(event) => {
				let encoded = events::encode_event2(&event)
					.map_err(|_| OwnerIoError2::Decision(OwnerError2::Codec))?;
				let at = view.bytes().len() as u64;
				let file = self.log_for_write(view.log.is_none())?;
				run(IoStep2::LogWrite, || file.write_all_at(&encoded, at))
					.map_err(io_err(IoStep2::LogWrite))?;
				let r = run(IoStep2::LogSync, || file.sync_data());
				self.synced(IoStep2::LogSync, r)?;
				let r = run(IoStep2::OwnerDirSync, || self.dir.sync_all());
				self.synced(IoStep2::OwnerDirSync, r)?;
				let after = self.view()?;
				let verified = matches!(
					after.classification,
					Classification2::Clean { last_seq } if last_seq == event.seq
				);
				let state = events::fold_events2(after.bytes())
					.ok()
					.filter(|_| verified);
				let current = state.and_then(|s| {
					let id = event.identity?;
					s.roots.iter().find(|r| r.identity == id).map(|r| r.state)
				});
				let op = event_request_key2(&event).map(|(op, _)| op);
				match (current, op) {
					(Some(current), Some(op)) => Ok(OpOutcome2 {
						original: Some(EventRef2 { op, seq: event.seq }),
						current,
					}),
					_ => Err(OwnerIoError2::Io {
						step: IoStep2::VerifyRead,
						errno: None,
					}),
				}
			}
		}
	}

	pub(in crate::storage::mutation_visibility::root2) fn repair_fresh(
		&mut self,
		expect_tail: &Digest2,
	) -> Owned<RepairOutcome2> {
		self.check_framing()?;
		self.blocked()?;
		let view = self.view()?;
		let Classification2::Unclaimed { tail_sha256, .. } = view.classification else {
			return Err(OwnerIoError2::Classified(view.classification));
		};
		if tail_sha256 != *expect_tail {
			return Err(OwnerIoError2::ExpectTailMismatch {
				actual: tail_sha256,
			});
		}
		let (name, bytes) = decisions::encode_evidence2(view.bytes())
			.map_err(|_| OwnerIoError2::EvidenceInvalid)?;
		let evidence = decisions::decode_evidence2(&name, &bytes)
			.map_err(|_| OwnerIoError2::EvidenceInvalid)?;
		self.ensure_repairs()?;
		self.publish_evidence(&name, &bytes)?;
		let context = decisions::repair_context2(
			&evidence.prefix_sha256,
			evidence.boundary,
			evidence.original_len,
			&evidence.tail_sha256,
		);
		self.resume(&context)
	}

	pub(in crate::storage::mutation_visibility::root2) fn repair_resume(
		&mut self,
		context: &Digest2,
	) -> Owned<RepairOutcome2> {
		self.check_framing()?;
		self.blocked()?;
		self.resume(context)
	}

	pub(in crate::storage::mutation_visibility::root2) fn repair_check(
		&mut self,
	) -> Owned<RepairOutcome2> {
		self.check_framing()?;
		self.blocked()?;
		let view = self.clean_view()?;
		self.resync(view.log.is_some(), &view.rep_names())?;
		Ok(RepairOutcome2::NothingToRepair)
	}

	pub(in crate::storage::mutation_visibility::root2) fn export(
		&mut self,
		out_dir: &Path,
		name: &str,
	) -> Owned<Digest2> {
		self.check_framing()?;
		self.blocked()?;
		if !export_name_ok(name) {
			return Err(OwnerIoError2::BadExportName);
		}
		let (out, out_handle) = self.export_dir(out_dir)?;
		let view = self.clean_view()?;
		self.resync(view.log.is_some(), &[])?;
		let state =
			events::fold_events2(view.bytes()).map_err(|_| OwnerIoError2::RepairVerifyFailed)?;
		let bytes = encode_snapshot2(&state).map_err(|_| OwnerIoError2::Io {
			step: IoStep2::ExportWrite,
			errno: None,
		})?;
		self.publish(&out_handle, &out, name, &bytes, &snapshot_steps())?;
		Ok(sha(&bytes))
	}

	/// One Bound root's exact canonical manifest and the whole snapshot, both from ONE folded
	/// state; every refusal before any artifact is written. The manifest is published first, then
	/// the snapshot, each exclusively with its own directory sync. The pair is not atomic: a
	/// failure after the manifest leaves it published and returns no pins from this call; finals
	/// are never replaced or removed. Path-based owner-side publication under the supported path
	/// model (stable, trusted export parent); not the replica reader's openat2 guarantees.
	pub(in crate::storage::mutation_visibility::root2) fn export_tuple(
		&mut self,
		out_dir: &Path,
		manifest_name: &str,
		snapshot_name: &str,
		generation: [u8; 16],
		cluster: [u8; 16],
	) -> Owned<TupleExport2> {
		self.check_framing()?;
		self.blocked()?;
		if !export_name_ok(manifest_name)
			|| !export_name_ok(snapshot_name)
			|| manifest_name == snapshot_name
		{
			return Err(OwnerIoError2::BadExportName);
		}
		let (out, out_handle) = self.export_dir(out_dir)?;
		let view = self.clean_view()?;
		self.resync(view.log.is_some(), &[])?;
		let state =
			events::fold_events2(view.bytes()).map_err(|_| OwnerIoError2::RepairVerifyFailed)?;
		let mut matching = state
			.roots
			.iter()
			.filter(|r| (r.identity.generation, r.identity.cluster) == (generation, cluster));
		let root = matching.next().ok_or(OwnerIoError2::ExportNoSuchRoot)?;
		// The fold refuses shared identity components, so a second match is a broken invariant.
		if matching.next().is_some() {
			return Err(OwnerIoError2::Decision(OwnerError2::Inconsistent));
		}
		if root.state != RootState2::Bound {
			return Err(OwnerIoError2::ExportNotBound(root.state));
		}
		fn encode_failed<E>(_: E) -> OwnerIoError2 {
			OwnerIoError2::Io {
				step: IoStep2::ExportWrite,
				errno: None,
			}
		}
		let manifest = root.manifest.encode().map_err(encode_failed)?;
		let snapshot = encode_snapshot2(&state).map_err(encode_failed)?;
		// The folded identity pin is the whole-manifest digest (the fold's Reserve binding).
		let result = TupleExport2 {
			manifest_pin: root.identity.pin,
			inventory_pin: sha(&snapshot),
			export_seq: state.last_seq,
			profile: root.manifest.profile(),
		};
		self.publish(
			&out_handle,
			&out,
			manifest_name,
			manifest.as_slice(),
			&manifest_steps(),
		)?;
		self.publish(
			&out_handle,
			&out,
			snapshot_name,
			&snapshot,
			&snapshot_steps(),
		)?;
		Ok(result)
	}

	/// Publish a retired profile-3 manifest followed by its terminal descriptor.
	/// The snapshot pin describes the same folded state; publication is not atomic.
	pub(in crate::storage::mutation_visibility::root2) fn export_terminal(
		&mut self,
		out_dir: &Path,
		manifest_name: &str,
		terminal_name: &str,
		identity: &super::inventory::RootIdentity2,
	) -> Owned<TerminalExport3> {
		self.check_framing()?;
		self.blocked()?;
		if !export_name_ok(manifest_name)
			|| !export_name_ok(terminal_name)
			|| manifest_name == terminal_name
		{
			return Err(OwnerIoError2::BadExportName);
		}
		let (out, out_handle) = self.export_dir(out_dir)?;
		let view = self.clean_view()?;
		self.resync(view.log.is_some(), &[])?;
		let state =
			events::fold_events2(view.bytes()).map_err(|_| OwnerIoError2::RepairVerifyFailed)?;
		let root = state
			.roots
			.iter()
			.find(|r| r.identity == *identity)
			.ok_or(OwnerIoError2::ExportNoSuchRoot)?;
		if root.state != RootState2::Retired || root.manifest.profile() != ManifestProfile2::Three {
			return Err(OwnerIoError2::ExportNotTerminal(root.state));
		}
		// Every Retired root carries its terminal record: plain Retire no longer exists.
		let terminal = root
			.terminal
			.ok_or(OwnerIoError2::Decision(OwnerError2::Inconsistent))?;
		let failed = |_| OwnerIoError2::Decision(OwnerError2::Codec);
		let manifest = root
			.manifest
			.encode()
			.map_err(|_| OwnerIoError2::Decision(OwnerError2::Codec))?;
		let snapshot =
			encode_snapshot2(&state).map_err(|_| OwnerIoError2::Decision(OwnerError2::Codec))?;
		let inventory_pin = sha(&snapshot);
		let bytes =
			super::terminal::encode_terminal_artifact3(&super::terminal::TerminalArtifact3 {
				identity: root.identity,
				terminal,
				inventory_pin,
				export_seq: state.last_seq,
			})
			.map_err(failed)?;
		let result = TerminalExport3 {
			manifest_pin: root.identity.pin,
			inventory_pin,
			terminal_pin: sha(&bytes),
			export_seq: state.last_seq,
		};
		self.publish(
			&out_handle,
			&out,
			manifest_name,
			manifest.as_slice(),
			&manifest_steps(),
		)?;
		self.publish(&out_handle, &out, terminal_name, &bytes, &snapshot_steps())?;
		Ok(result)
	}

	pub(in crate::storage::mutation_visibility::root2) fn clear_sync_failure(
		&mut self,
		expect_log: &Digest2,
	) -> Owned<()> {
		self.check_framing()?;
		let marker = self.marker_stat()?;
		if self.poisoned.is_none() && marker.is_none() {
			return Err(OwnerIoError2::NothingToClear);
		}
		if marker.is_some_and(|is_file| !is_file) {
			return Err(OwnerIoError2::ClearRefused(ClearRefusal2::NonRegularMarker));
		}
		let marker_present = marker.is_some();
		let log = self.read_log()?;
		let actual = sha(log.as_deref().unwrap_or(&[]));
		if actual != *expect_log {
			return Err(OwnerIoError2::ExpectLogMismatch { actual });
		}
		if log.is_some() {
			let file = self.log_file(Mode::Read)?;
			let r = run(IoStep2::LogSync, || file.sync_data());
			self.clear_synced(IoStep2::LogSync, r, marker_present)?;
		}
		let r = run(IoStep2::OwnerDirSync, || self.dir.sync_all());
		self.clear_synced(IoStep2::OwnerDirSync, r, marker_present)?;
		let entries = match self.entries() {
			Ok(e) => e,
			Err(OwnerIoError2::Classified(c)) => {
				return Err(OwnerIoError2::ClearRefused(ClearRefusal2::Evidence(c)));
			}
			Err(e) => return Err(e),
		};
		let refs: Vec<(&str, &[u8])> = entries
			.iter()
			.map(|(n, b)| (n.as_str(), b.as_slice()))
			.collect();
		let classification = decisions::classify2(log.as_deref().unwrap_or(&[]), &refs);
		if !matches!(
			classification,
			Classification2::Clean { .. }
				| Classification2::Pending { .. }
				| Classification2::Unclaimed { .. }
		) {
			return Err(OwnerIoError2::ClearRefused(ClearRefusal2::Evidence(
				classification,
			)));
		}
		if !entries.is_empty() {
			for (name, _) in entries.iter().filter(|(n, _)| n.ends_with(".rep")) {
				let file = self.evidence_file(name)?;
				let r = run(IoStep2::EvidenceSync, || file.sync_data());
				self.clear_synced(IoStep2::EvidenceSync, r, marker_present)?;
			}
			let repairs = self.repairs_dir()?;
			let r = run(IoStep2::RepairsDirSync, || repairs.sync_all());
			self.clear_synced(IoStep2::RepairsDirSync, r, marker_present)?;
		}
		if marker_present {
			run(IoStep2::MarkerUnlink, || {
				fs::remove_file(self.path.join(MARKER))
			})
			.map_err(io_err(IoStep2::MarkerUnlink))?;
		}
		if run(IoStep2::ClearDirSync, || self.dir.sync_all()).is_err() {
			// The unlink's persistence is unknown: keep the latch, one marker attempt.
			self.poisoned = Some(IoStep2::ClearDirSync);
			let marker = self.set_marker(IoStep2::ClearDirSync);
			return Err(OwnerIoError2::SyncFailed {
				step: IoStep2::ClearDirSync,
				marker,
			});
		}
		self.poisoned = None;
		Ok(())
	}
}

/// Test-only fault plan and crash stop: thread-local, bounded, errors only. Dropping the plan
/// clears it, so tests never interfere with each other.
#[cfg(test)]
pub(super) mod seam {
	use super::IoStep2;
	use std::cell::RefCell;

	thread_local! {
		static FAULTS: RefCell<Vec<(IoStep2, u32)>> = const { RefCell::new(Vec::new()) };
		static STOP: RefCell<Option<(IoStep2, u32)>> = const { RefCell::new(None) };
		static SEEN: RefCell<Vec<IoStep2>> = const { RefCell::new(Vec::new()) };
	}
	pub(in crate::storage::mutation_visibility::root2::provisioning) struct Plan;
	impl Drop for Plan {
		fn drop(&mut self) {
			FAULTS.with(|f| f.borrow_mut().clear());
			STOP.with(|s| *s.borrow_mut() = None);
		}
	}
	/// Fail the nth (1-based) occurrence of each listed step; stop after the nth of `stop`.
	pub(in crate::storage::mutation_visibility::root2::provisioning) fn plan(
		faults: &[(IoStep2, u32)],
		stop: Option<(IoStep2, u32)>,
	) -> Plan {
		assert!(faults.len() <= 8, "bounded fault plan");
		FAULTS.with(|f| *f.borrow_mut() = faults.to_vec());
		STOP.with(|s| *s.borrow_mut() = stop);
		SEEN.with(|s| s.borrow_mut().clear());
		Plan
	}
	pub(in crate::storage::mutation_visibility::root2::provisioning) fn seen() -> Vec<IoStep2> {
		SEEN.with(|s| s.borrow().clone())
	}
	fn count(step: IoStep2) -> u32 {
		SEEN.with(|s| s.borrow().iter().filter(|x| **x == step).count() as u32)
	}
	pub(super) fn hit(step: IoStep2) -> std::io::Result<()> {
		SEEN.with(|s| s.borrow_mut().push(step));
		let n = count(step);
		if FAULTS.with(|f| f.borrow().contains(&(step, n))) {
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		Ok(())
	}
	pub(super) fn after(step: IoStep2) {
		if STOP.with(|s| *s.borrow()) == Some((step, count(step))) {
			panic!("crash stop after {step:?}");
		}
	}
}
