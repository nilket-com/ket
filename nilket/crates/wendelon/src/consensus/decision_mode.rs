//! Which write-decision authority a store and its cluster run under.
//!
//! `Today` is the default path: bytes, frames and behaviour are unchanged.
//! `ParentDecisions` is selected for a fresh store, by a daemon's configuration
//! (`cluster.decision_authority`, ADR 0721) or programmatically, and a store
//! keeps the authority it was created under. It has its own control-log and
//! snapshot format versions and its own envelope version for Raft frames and
//! forwarded decision proposals, so a binary or peer on the other authority
//! refuses at a boundary before state is applied.
//!
//! It is a fresh, disposable checkpoint, not a durable deployment: it has no
//! backup, no member replacement and no resizing (its membership is fixed at
//! the first admitted write, ADR 0720), and it serves a limited surface. Real
//! signing keys do not change that.

use std::fs::{self, File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Write};
use std::path::Path;

use crate::schema::{
	SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FORMAT_VERSION_PARENT_DECISIONS, WAL_FORMAT_VERSION,
	WAL_FORMAT_VERSION_PARENT_DECISIONS,
};

/// Written by the log store after its first format-10 record is durable and
/// verified, and before any other artifact. It lets a current binary refuse early;
/// older binaries do not read it, and for them that record is the boundary.
const MODE_MARKER: &str = "PARENT_DECISIONS";
const MODE_MARKER_TMP: &str = ".PARENT_DECISIONS.tmp";
const MODE_MARKER_BYTES: &[u8] = b"parent-decisions-v12\n";
/// S1's development marker. Its stores hold format-9 records, whose bodies were
/// today's; no conversion exists, so both authorities refuse it as malformed.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V1: &[u8] = b"parent-decisions-v1\n";
/// S2's marker, for stores holding format-10 records (no signer registry,
/// stand-in signatures). Refused like S1's.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V2: &[u8] = b"parent-decisions-v2\n";
/// S3's marker, for stores holding format-11 records (claims without an
/// operation, no noops). Refused like S1's and S2's.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V3: &[u8] = b"parent-decisions-v3\n";
/// S4b's marker, for stores holding format-12 records (no generations).
#[cfg(test)]
const RETIRED_MARKER_BYTES_V4: &[u8] = b"parent-decisions-v4\n";
/// ADR 0699 S5a to ADR 0700's marker, for stores holding format-13 records (no
/// batched lease renewals, ADR 0701). Refused like the earlier ones; an
/// untouched v5 store stays readable by its own binary.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V5: &[u8] = b"parent-decisions-v5\n";
/// ADR 0701 to ADR 0702's marker, for stores whose LSM installs carry no
/// certificates (ADR 0703). A pre-0703 binary refuses the v7 marker, so it never
/// recovers a certified install; this binary refuses v6 like the earlier ones.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V6: &[u8] = b"parent-decisions-v6\n";
/// ADR 0703's marker, for stores holding format-14 records (no certified tier
/// provenance, ADR 0704). Refused like the earlier ones; an untouched v7 store
/// stays readable by its own binary.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V7: &[u8] = b"parent-decisions-v7\n";
/// ADR 0704 to ADR 0719's marker, for stores holding format-16 records, whose
/// decision state records no sealed membership (ADR 0720). Such a store may have
/// admitted writes that it no longer holds a record of, so it is refused whole,
/// never read as unsealed; it stays readable by its own binary.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V8: &[u8] = b"parent-decisions-v8\n";
/// ADR 0720 to ADR 0724's marker, for stores holding format-17 records, whose
/// slab operations are bare (ADR 0725 takes only owner-signed ones). Such a log
/// would replay to another state under the new rule, so it is refused whole; it
/// stays readable by its own binary.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V9: &[u8] = b"parent-decisions-v9\n";
/// ADR 0725's marker, for stores holding format-18 records, whose tenant KEKs
/// no root check preceded (ADR 0726). Refused whole; readable by its own binary.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V10: &[u8] = b"parent-decisions-v10\n";
/// ADR 0726 to ADR 0727's marker, for stores holding format-20 records, whose
/// log may hold an applied tier publication naming an absent tenant KEK (ADR
/// 0728). Refused whole; readable by its own binary.
#[cfg(test)]
const RETIRED_MARKER_BYTES_V11: &[u8] = b"parent-decisions-v11\n";

/// The envelope version of Raft frames between Today nodes. ADR 0726: 13, its
/// own, because tenant creation and tenant KEK install carry a root check id
/// and a root check request exists: an earlier Today peer's Raft bodies decode
/// differently, and the WAL format is no boundary on the wire. Until then
/// Today's Raft frames carried the one HRFT envelope version, 1, which every
/// other frame still uses; a version-1 Raft frame is now refused before
/// decode, and an earlier peer refuses 13. (2 to 12 are `ParentDecisions`
/// versions, so a number is never reused.) ADR 0728: 15, because a peer of
/// either rule must not replicate a tier publication the other decides
/// differently; 13 is refused before decode (14 is `ParentDecisions`').
pub(crate) const RAFT_FRAME_VERSION_TODAY: u8 = 15;
/// The envelope version of Raft frames between `ParentDecisions` nodes. Older
/// binaries refuse it when reading the frame. Version 2 was S1's development
/// envelope, 3 was S2's, 4 was S3's, 5 was S4b's, 6 was S5a to ADR 0700's
/// (no batched lease renewals), 7 was ADR 0701 to ADR 0703's (no certified
/// tier provenance, ADR 0704), 8 was ADR 0704 to ADR 0719's (no sealed
/// membership, ADR 0720) and 9 was ADR 0720's (a forwarded signer binding
/// without the sender's key, and forwarded proposals outside this envelope,
/// ADR 0721) and 10 was ADR 0721 to ADR 0724's (bare slab operations, and no
/// forwarded owner operation, ADR 0725) and 11 was ADR 0725's (no root check
/// request, and tenant KEK requests without a root check id, ADR 0726); their
/// peers decode other encodings, so every build refuses them. ADR 0728: 14,
/// since a version-12 peer decides a tier publication naming an absent tenant
/// KEK differently and its forwarded receipts lack the new rejections (13 was
/// Today's).
pub(crate) const RAFT_FRAME_VERSION_PARENT_DECISIONS: u8 = 14;

/// ADR 0721: the frames that may carry the `ParentDecisions` envelope: the Raft
/// frames and a forwarded decision proposal with its answer. This classifies an
/// envelope family for the frame reader; it dispatches nothing, and a decision
/// proposal is never handled as a Raft frame. The frame reader is the same in
/// both modes, so which version a node accepts on these types is decided by
/// the mode-aware handler and client, before any body is decoded.
pub(crate) fn carries_decision_envelope(msg_type: u8) -> bool {
	use crate::schema::MsgType;
	is_raft_frame(msg_type)
		|| msg_type == MsgType::DECISION_PROPOSE_REQ.0
		|| msg_type == MsgType::DECISION_PROPOSE_RESP.0
}

/// The Raft request and response types.
pub(crate) fn is_raft_frame(msg_type: u8) -> bool {
	use crate::schema::MsgType;
	[
		MsgType::APPEND_ENTRIES_REQ,
		MsgType::APPEND_ENTRIES_RESP,
		MsgType::INSTALL_SNAPSHOT_REQ,
		MsgType::INSTALL_SNAPSHOT_RESP,
		MsgType::VOTE_REQ,
		MsgType::VOTE_RESP,
	]
	.iter()
	.any(|raft| raft.0 == msg_type)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DecisionMode {
	#[default]
	Today,
	ParentDecisions,
}

impl DecisionMode {
	pub(crate) const fn wal_format(self) -> u8 {
		match self {
			Self::Today => WAL_FORMAT_VERSION,
			Self::ParentDecisions => WAL_FORMAT_VERSION_PARENT_DECISIONS,
		}
	}

	pub(crate) const fn snapshot_format(self) -> u8 {
		match self {
			Self::Today => SNAPSHOT_FORMAT_VERSION,
			Self::ParentDecisions => SNAPSHOT_FORMAT_VERSION_PARENT_DECISIONS,
		}
	}

	pub(crate) const fn raft_frame_version(self) -> u8 {
		match self {
			Self::Today => RAFT_FRAME_VERSION_TODAY,
			Self::ParentDecisions => RAFT_FRAME_VERSION_PARENT_DECISIONS,
		}
	}

	/// Refuses a Raft frame from a peer on the other authority, before decode.
	pub(crate) fn check_raft_frame(self, version: u8) -> std::io::Result<()> {
		if version == self.raft_frame_version() {
			return Ok(());
		}
		Err(Error::new(
			ErrorKind::PermissionDenied,
			format!(
				"raft frame version {version} is from a peer on another decision authority; this \
				 node runs {self:?}"
			),
		))
	}

	/// A Raft response must carry this node's envelope and the expected type.
	pub(crate) fn check_raft_response(
		self,
		version: u8,
		msg_type: u8,
		expected: u8,
	) -> std::io::Result<()> {
		self.check_raft_frame(version)?;
		if msg_type != expected {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("raft response msg_type=0x{msg_type:02x}, expected 0x{expected:02x}"),
			));
		}
		Ok(())
	}

	/// Refuses a joiner on the other authority, or of another `ParentDecisions`
	/// version, before it becomes a learner. ADR 0701: this version advertises
	/// both `ParentDecisions` and `ParentDecisionsBatchedLeases`; a joiner with
	/// only the first (an earlier version) is refused. An earlier cluster checks
	/// only the first bit, so it may admit this version's joiner; their Raft
	/// frames are refused both ways before decode.
	pub(crate) fn check_joiner(
		self,
		joiner: crate::network::transport::NodeCapabilitySet,
	) -> std::io::Result<()> {
		use crate::network::transport::NodeCapability::{
			ParentDecisions, ParentDecisionsBatchedLeases,
		};
		let this_version =
			joiner.contains(ParentDecisions) && joiner.contains(ParentDecisionsBatchedLeases);
		let any_mode_bit =
			joiner.contains(ParentDecisions) || joiner.contains(ParentDecisionsBatchedLeases);
		let admitted = match self {
			Self::ParentDecisions => this_version,
			Self::Today => !any_mode_bit,
		};
		if admitted {
			return Ok(());
		}
		Err(Error::new(
			ErrorKind::PermissionDenied,
			format!("joiner is on another decision authority; this cluster runs {self:?}"),
		))
	}

	/// True for the mode whose data server serves only its own surfaces: every
	/// message type outside them is refused `ParentDecisionsIncomplete`.
	pub(crate) const fn refuses_service(self) -> bool {
		matches!(self, Self::ParentDecisions)
	}

	pub(crate) fn service_refusal(self) -> Error {
		Error::new(ErrorKind::Unsupported, "ParentDecisionsIncomplete")
	}

	/// Run before the control log opens, under the directory owner. An existing
	/// store never changes authority. A new-mode store starts with a format-10
	/// control-log record, written by the log store, before the marker exists, so
	/// an older binary has a record to refuse at every point after the first write.
	pub(crate) fn admit_store(self, data_dir: &Path) -> std::io::Result<()> {
		let marked = read_marker(data_dir)?;
		match (self, marked) {
			(Self::Today, false) | (Self::ParentDecisions, true) => Ok(()),
			(Self::Today, true) => Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"store {} belongs to the ParentDecisions authority",
					data_dir.display()
				),
			)),
			// Fresh, or a creation interrupted before its marker. Anything but setup
			// files and control-log segments refuses. A segment of the other format is
			// refused by the log store before it writes anything.
			(Self::ParentDecisions, false) => {
				for entry in fs::read_dir(data_dir)? {
					let name = entry?.file_name();
					let name = name.to_string_lossy();
					let setup =
						name == "LOCK" || name == ".CLEAN_SHUTDOWN.tmp" || name == MODE_MARKER_TMP;
					let control_log = name.starts_with("control.wal.")
						&& (name.ends_with(".seg") || name.ends_with(".staging"));
					// ADR 0726: a member is enrolled by placing the cluster's sealed
					// root KEK before its first start, and a forming node persists
					// its root before its first record. That directory, holding
					// nothing but root KEK artifacts, is setup, not data.
					let enrolled_root =
						crate::crypto::is_root_kek_artifact_dir(&data_dir.join(&*name))?;
					if !setup && !control_log && !enrolled_root {
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"store {} has data and no ParentDecisions marker; only a fresh \
								 store can enter that mode",
								data_dir.display()
							),
						));
					}
				}
				Ok(())
			}
		}
	}

	/// Called by the log store once replay has verified that every control-log
	/// record is this authority's, and before it creates the vote file.
	pub(crate) fn bind_store(self, data_dir: &Path) -> std::io::Result<()> {
		if self == Self::Today || read_marker(data_dir)? {
			return Ok(());
		}
		write_marker(data_dir)
	}
}

/// ADR 0703: whether `data_dir` is durably a ParentDecisions store, read by the
/// slab store's open before it reconciles any install. A marker it cannot
/// read refuses; it is never taken for Today.
pub(crate) fn store_is_marked(data_dir: &Path) -> std::io::Result<bool> {
	read_marker(data_dir)
}

/// Test-only: marks `data_dir` as a ParentDecisions store.
#[cfg(test)]
pub(crate) fn write_mode_marker_for_test(data_dir: &Path) {
	write_marker(data_dir).unwrap();
}

/// `Ok(true)` only for a regular file with exactly the expected bytes. Absence is
/// `Ok(false)`. Anything else, including an unreadable path, refuses.
fn read_marker(dir: &Path) -> std::io::Result<bool> {
	let path = dir.join(MODE_MARKER);
	let metadata = match fs::symlink_metadata(&path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
		Err(error) => return Err(error),
	};
	if !metadata.file_type().is_file() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"decision mode marker {} is not a regular file",
				path.display()
			),
		));
	}
	let mut bytes = Vec::new();
	open_no_follow(&path, false)?.read_to_end(&mut bytes)?;
	if bytes != MODE_MARKER_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"decision mode marker {} is malformed or of another version",
				path.display()
			),
		));
	}
	Ok(true)
}

fn open_no_follow(path: &Path, create: bool) -> std::io::Result<File> {
	use std::os::unix::fs::OpenOptionsExt;
	let mut options = OpenOptions::new();
	if create {
		options.write(true).create_new(true).mode(0o600);
	} else {
		options.read(true);
	}
	options
		.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
		.open(path)
}

fn write_marker(dir: &Path) -> std::io::Result<()> {
	let tmp = dir.join(MODE_MARKER_TMP);
	match fs::symlink_metadata(&tmp) {
		Ok(metadata) if metadata.file_type().is_file() => fs::remove_file(&tmp)?,
		Ok(_) => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("{} is not a regular file", tmp.display()),
			));
		}
		Err(error) if error.kind() == ErrorKind::NotFound => {}
		Err(error) => return Err(error),
	}
	{
		let mut file = open_no_follow(&tmp, true)?;
		file.write_all(MODE_MARKER_BYTES)?;
		file.sync_all()?;
	}
	fs::rename(&tmp, dir.join(MODE_MARKER))?;
	File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn fresh_dir(tag: &str) -> crate::test_support::TestDir {
		crate::test_support::TestDir::new(tag)
	}

	#[test]
	fn today_keeps_its_formats_and_frame_version() {
		assert_eq!(DecisionMode::default(), DecisionMode::Today);
		assert_eq!(DecisionMode::Today.wal_format(), WAL_FORMAT_VERSION);
		assert_eq!(
			DecisionMode::Today.snapshot_format(),
			SNAPSHOT_FORMAT_VERSION
		);
		// ADR 0726: Today's Raft frames have a version of their own; every
		// other Today frame keeps the one HRFT envelope version.
		assert_eq!(DecisionMode::Today.raft_frame_version(), 15);
		assert_ne!(
			DecisionMode::Today.raft_frame_version(),
			crate::schema::HRFT_ENVELOPE_VERSION
		);
		assert!(!DecisionMode::Today.refuses_service());
	}

	#[test]
	fn the_new_mode_has_formats_an_older_binary_refuses() {
		let mode = DecisionMode::ParentDecisions;
		assert!(mode.wal_format() > WAL_FORMAT_VERSION);
		assert_eq!(
			(mode.wal_format(), mode.snapshot_format()),
			(22, 22),
			"S1's 9, S2's 10, S3's 11, S4b's 12, S5a to ADR 0700's 13, ADR 0701 to ADR \
			 0703's 14, ADR 0704 to ADR 0719's 16, ADR 0720 to ADR 0724's 17 and ADR \
			 0725's 18 and ADR 0726 to ADR 0727's 20 are retired; 15 and 19 were \
			 Today's and 21 is"
		);
		assert_eq!(mode.raft_frame_version(), 14);
		assert!(mode.snapshot_format() > SNAPSHOT_FORMAT_VERSION);
		assert_ne!(mode.raft_frame_version(), 1);
		assert!(mode.refuses_service());
	}

	#[test]
	fn a_frame_or_joiner_from_the_other_authority_is_refused_both_ways() {
		let (today, new) = (DecisionMode::Today, DecisionMode::ParentDecisions);
		today.check_raft_frame(15).unwrap();
		new.check_raft_frame(14).unwrap();
		// ADR 0728: an ADR 0726 to ADR 0727 peer of either mode decides a tier
		// publication naming an absent tenant KEK differently; its frames are
		// refused before decode.
		assert!(today.check_raft_frame(13).is_err());
		assert!(new.check_raft_frame(12).is_err());
		assert!(new.check_raft_frame(15).is_err());
		// ADR 0726: an earlier Today peer's version-1 Raft frames, whose tenant
		// bodies carry no root check id, are refused before decode.
		assert_eq!(
			today.check_raft_frame(1).unwrap_err().kind(),
			ErrorKind::PermissionDenied
		);
		assert!(new.check_raft_frame(13).is_err());
		// ADR 0701: an ADR 0700 peer's version-6 frames are refused before decode;
		// ADR 0704: so are an ADR 0701 to ADR 0703 peer's version-7 frames; ADR
		// 0720: so are an ADR 0704 to ADR 0719 peer's version-8 frames.
		assert!(new.check_raft_frame(6).is_err());
		assert!(new.check_raft_frame(7).is_err());
		assert!(new.check_raft_frame(8).is_err());
		// ADR 0721: and an ADR 0720 peer's version-9 frames.
		assert!(new.check_raft_frame(9).is_err());
		// ADR 0725: and an ADR 0721 to ADR 0724 peer's version-10 frames.
		assert!(new.check_raft_frame(10).is_err());
		// ADR 0726: and an ADR 0725 peer's version-11 frames.
		assert!(new.check_raft_frame(11).is_err());
		for retired in [2, 3, 4, 5] {
			assert!(
				new.check_raft_frame(retired).is_err(),
				"S1's and S2's envelopes"
			);
		}
		for other in 1..=14 {
			assert!(today.check_raft_frame(other).is_err(), "{other}");
		}
		assert_eq!(
			today.check_raft_frame(14).unwrap_err().kind(),
			ErrorKind::PermissionDenied
		);
		assert_eq!(
			new.check_raft_frame(1).unwrap_err().kind(),
			ErrorKind::PermissionDenied
		);
		use crate::network::transport::{NodeCapability, NodeCapabilitySet};
		let none = NodeCapabilitySet::empty();
		let earlier = none.with(NodeCapability::ParentDecisions);
		let batched_only = none.with(NodeCapability::ParentDecisionsBatchedLeases);
		let this_version = earlier.with(NodeCapability::ParentDecisionsBatchedLeases);
		today.check_joiner(none).unwrap();
		new.check_joiner(this_version).unwrap();
		// ADR 0701: an earlier ParentDecisions version, a Today node and a
		// half-advertised node are refused before they become learners.
		for (mode, joiner) in [
			(today, earlier),
			(today, batched_only),
			(today, this_version),
			(new, none),
			(new, earlier),
			(new, batched_only),
		] {
			assert!(mode.check_joiner(joiner).is_err(), "{mode:?} {joiner:?}");
		}
	}

	#[test]
	fn the_other_envelope_is_valid_only_on_raft_frames() {
		use crate::network::wire::validate_frame_version;
		use crate::schema::MsgType;
		validate_frame_version(1, MsgType::READ_RESP.0).unwrap();
		// ADR 0726: Today's Raft version rides Raft frames only, not a forwarded
		// proposal and not any data or admin frame.
		for raft in [
			MsgType::APPEND_ENTRIES_REQ,
			MsgType::APPEND_ENTRIES_RESP,
			MsgType::INSTALL_SNAPSHOT_REQ,
			MsgType::INSTALL_SNAPSHOT_RESP,
			MsgType::VOTE_REQ,
			MsgType::VOTE_RESP,
		] {
			validate_frame_version(15, raft.0).unwrap();
		}
		for other in [
			MsgType::READ_RESP,
			MsgType::TABLE_WRITE_REQ,
			MsgType::LEASES_REQ,
			MsgType::DECISION_PROPOSE_REQ,
			MsgType::DECISION_PROPOSE_RESP,
			MsgType::DECISION_LOOKUP_REQ,
		] {
			assert!(validate_frame_version(15, other.0).is_err());
		}
		validate_frame_version(14, MsgType::VOTE_REQ.0).unwrap();
		validate_frame_version(14, MsgType::APPEND_ENTRIES_RESP.0).unwrap();
		// ADR 0721: a forwarded decision proposal and its answer carry it too.
		validate_frame_version(14, MsgType::DECISION_PROPOSE_REQ.0).unwrap();
		validate_frame_version(14, MsgType::DECISION_PROPOSE_RESP.0).unwrap();
		assert!(validate_frame_version(14, MsgType::READ_RESP.0).is_err());
		assert!(validate_frame_version(14, MsgType::TABLE_WRITE_REQ.0).is_err());
		assert!(validate_frame_version(14, MsgType::LEASES_REQ.0).is_err());
		assert!(validate_frame_version(14, MsgType::DECISION_LOOKUP_REQ.0).is_err());
		assert!(validate_frame_version(14, MsgType::DECISION_CUT_REQ.0).is_err());
		for retired in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] {
			assert!(
				validate_frame_version(retired, MsgType::DECISION_PROPOSE_REQ.0).is_err(),
				"retired envelope {retired} on a forwarded proposal"
			);
			assert!(
				validate_frame_version(retired, MsgType::VOTE_REQ.0).is_err(),
				"retired envelope {retired}"
			);
		}
		assert!(validate_frame_version(16, MsgType::VOTE_REQ.0).is_err());
		// The classifier is an envelope family, not Raft dispatch.
		assert!(!is_raft_frame(MsgType::DECISION_PROPOSE_REQ.0));
		assert!(carries_decision_envelope(MsgType::DECISION_PROPOSE_REQ.0));
		assert!(!carries_decision_envelope(MsgType::DECISION_LOOKUP_REQ.0));
	}

	#[test]
	fn a_raft_response_must_match_the_mode_and_the_request() {
		use crate::schema::MsgType;
		let (today, new) = (DecisionMode::Today, DecisionMode::ParentDecisions);
		let vote = MsgType::VOTE_RESP.0;
		today.check_raft_response(15, vote, vote).unwrap();
		assert!(today.check_raft_response(1, vote, vote).is_err());
		assert!(today.check_raft_response(13, vote, vote).is_err());
		assert!(new.check_raft_response(15, vote, vote).is_err());
		new.check_raft_response(14, vote, vote).unwrap();
		assert!(new.check_raft_response(12, vote, vote).is_err());
		assert!(new.check_raft_response(6, vote, vote).is_err());
		assert!(new.check_raft_response(7, vote, vote).is_err());
		assert!(new.check_raft_response(8, vote, vote).is_err());
		assert!(new.check_raft_response(9, vote, vote).is_err());
		assert!(new.check_raft_response(10, vote, vote).is_err());
		assert!(new.check_raft_response(11, vote, vote).is_err());
		assert!(today.check_raft_response(14, vote, vote).is_err());
		for retired in [1, 2, 3, 4, 5] {
			assert!(new.check_raft_response(retired, vote, vote).is_err());
		}
		assert!(
			new.check_raft_response(14, MsgType::APPEND_ENTRIES_RESP.0, vote)
				.is_err()
		);
	}

	#[test]
	fn a_fresh_store_is_admitted_and_marked_only_after_its_log_opens() {
		let dir = fresh_dir("mode-fresh");
		fs::write(dir.join("LOCK"), b"").unwrap();
		DecisionMode::ParentDecisions.admit_store(&dir).unwrap();
		assert!(!dir.join(MODE_MARKER).exists(), "admission writes nothing");
		DecisionMode::ParentDecisions.bind_store(&dir).unwrap();
		assert_eq!(fs::read(dir.join(MODE_MARKER)).unwrap(), MODE_MARKER_BYTES);
		DecisionMode::ParentDecisions.admit_store(&dir).unwrap();
		DecisionMode::ParentDecisions.bind_store(&dir).unwrap();
	}

	#[test]
	fn a_creation_interrupted_before_its_marker_resumes() {
		let dir = fresh_dir("mode-interrupted");
		fs::write(dir.join("LOCK"), b"").unwrap();
		fs::write(dir.join("control.wal.00000000000000000000.seg"), b"holder").unwrap();
		fs::write(
			dir.join("control.wal.00000000000000000000.staging"),
			b"torn",
		)
		.unwrap();
		fs::write(dir.join(MODE_MARKER_TMP), b"torn").unwrap();
		DecisionMode::ParentDecisions.admit_store(&dir).unwrap();
		DecisionMode::ParentDecisions.bind_store(&dir).unwrap();
		assert_eq!(fs::read(dir.join(MODE_MARKER)).unwrap(), MODE_MARKER_BYTES);
		assert!(!dir.join(MODE_MARKER_TMP).exists());
	}

	#[test]
	fn an_unmarked_store_with_a_saved_vote_is_not_an_interrupted_creation() {
		let dir = fresh_dir("mode-saved-vote");
		fs::write(dir.join("control.wal.00000000000000000000.seg"), b"holder").unwrap();
		fs::write(dir.join("vote.wnd"), b"vote").unwrap();
		assert!(DecisionMode::ParentDecisions.admit_store(&dir).is_err());
		assert!(!dir.join(MODE_MARKER).exists());
	}

	#[test]
	fn an_existing_store_never_changes_authority() {
		let dir = fresh_dir("mode-existing");
		fs::write(dir.join("storage_index.redb"), b"x").unwrap();
		DecisionMode::Today.admit_store(&dir).unwrap();
		let error = DecisionMode::ParentDecisions.admit_store(&dir).unwrap_err();
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		assert!(!dir.join(MODE_MARKER).exists(), "a refusal writes nothing");

		let dir = fresh_dir("mode-marked");
		DecisionMode::ParentDecisions.bind_store(&dir).unwrap();
		assert!(DecisionMode::Today.admit_store(&dir).is_err());
	}

	#[test]
	fn a_malformed_marker_refuses_both_authorities_and_is_left_alone() {
		for (tag, make) in [
			("empty", 0u8),
			("other-version", 1),
			("s1-v1", 4),
			("s2-v2", 5),
			("s3-v3", 6),
			("s4b-v4", 7),
			("adr0700-v5", 8),
			("adr0702-v6", 9),
			("adr0703-v7", 10),
			("adr0719-v8", 11),
			("adr0724-v9", 12),
			("adr0725-v10", 13),
			("adr0727-v11", 14),
			("directory", 2),
			("symlink", 3),
		] {
			let dir = fresh_dir(&format!("mode-malformed-{tag}"));
			let marker = dir.join(MODE_MARKER);
			match make {
				0 => fs::write(&marker, b"").unwrap(),
				1 => fs::write(&marker, b"parent-decisions-v13\n").unwrap(),
				4 => fs::write(&marker, RETIRED_MARKER_BYTES_V1).unwrap(),
				5 => fs::write(&marker, RETIRED_MARKER_BYTES_V2).unwrap(),
				6 => fs::write(&marker, RETIRED_MARKER_BYTES_V3).unwrap(),
				7 => fs::write(&marker, RETIRED_MARKER_BYTES_V4).unwrap(),
				8 => fs::write(&marker, RETIRED_MARKER_BYTES_V5).unwrap(),
				9 => fs::write(&marker, RETIRED_MARKER_BYTES_V6).unwrap(),
				10 => fs::write(&marker, RETIRED_MARKER_BYTES_V7).unwrap(),
				11 => fs::write(&marker, RETIRED_MARKER_BYTES_V8).unwrap(),
				12 => fs::write(&marker, RETIRED_MARKER_BYTES_V9).unwrap(),
				13 => fs::write(&marker, RETIRED_MARKER_BYTES_V10).unwrap(),
				14 => fs::write(&marker, RETIRED_MARKER_BYTES_V11).unwrap(),
				2 => fs::create_dir(&marker).unwrap(),
				_ => {
					fs::write(dir.join("target"), MODE_MARKER_BYTES).unwrap();
					std::os::unix::fs::symlink(dir.join("target"), &marker).unwrap();
				}
			}
			let before = fs::symlink_metadata(&marker).unwrap().modified().unwrap();
			for mode in [DecisionMode::Today, DecisionMode::ParentDecisions] {
				assert!(mode.admit_store(&dir).is_err(), "{tag}: {mode:?}");
				assert!(
					mode.bind_store(&dir).is_err() || mode == DecisionMode::Today,
					"{tag}"
				);
			}
			assert_eq!(
				fs::symlink_metadata(&marker).unwrap().modified().unwrap(),
				before
			);
		}
	}
}
