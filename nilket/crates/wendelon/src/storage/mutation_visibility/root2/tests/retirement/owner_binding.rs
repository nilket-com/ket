//! Owner binding is a functional fixture, not a terminal consumer or runtime gate.
use super::*;
use crate::storage::mutation_visibility::root2::provisioning::{
	inventory::*, owner_core::*, owner_log::*, snapshot::*, terminal::*,
};
fn op(n: u8) -> OpId2 {
	op_id2([n; 16]).unwrap()
}
fn observation() -> (F, TerminalObservation3) {
	let mut f = F::new();
	f.retire([3, 0]);
	let o = observe_retirement3(&f.f.state).unwrap();
	(f, o)
}
struct Owned {
	owner: Option<OwnerDir2>,
	path: std::path::PathBuf,
	out: std::path::PathBuf,
}
impl Owned {
	fn new() -> Self {
		use std::os::unix::fs::PermissionsExt;
		static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
		let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		let path = std::env::temp_dir().join(format!("terminal-owner-{}-{n}", std::process::id()));
		std::fs::create_dir(&path).unwrap();
		std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
		let owner = OwnerDir2::create_v2(&path).unwrap();
		let out = path.with_extension("export");
		std::fs::create_dir(&out).unwrap();
		Self {
			owner: Some(owner),
			path,
			out,
		}
	}
	fn apply(
		&mut self,
		n: u8,
		o: TerminalObservation3,
		id: RootIdentity2,
	) -> Result<OpOutcome2, OwnerIoError2> {
		self.owner.as_mut().unwrap().apply(
			&op(n),
			&OwnerRequest2::Retire {
				identity: id,
				terminal: o,
			},
		)
	}
	fn reopen(&mut self) {
		self.owner.take();
		self.owner = Some(OwnerDir2::open(&self.path).unwrap());
	}
	fn snapshot(&mut self) -> InventorySnapshot2 {
		self.owner
			.as_mut()
			.unwrap()
			.export(&self.out, "snapshot")
			.unwrap();
		decode_snapshot2(&std::fs::read(self.out.join("snapshot")).unwrap()).unwrap()
	}
}
impl Drop for Owned {
	fn drop(&mut self) {
		self.owner.take();
		let _ = std::fs::remove_dir_all(&self.path);
		let _ = std::fs::remove_dir_all(&self.out);
	}
}
fn bound(f: &F) -> (Owned, RootIdentity2) {
	let m = f.f.any.clone();
	let a = m.admission();
	let id = RootIdentity2 {
		generation: a.generation,
		cluster: a.cluster,
		pin: any_manifest_digest2(&m.encode().unwrap()),
		admin_key: a.admin_key,
	};
	let mut b = Owned::new();
	let owner = b.owner.as_mut().unwrap();
	owner
		.apply(&op(1), &OwnerRequest2::Reserve { manifest: m })
		.unwrap();
	owner
		.apply(
			&op(2),
			&OwnerRequest2::Approve {
				identity: id,
				confirmed: id,
				observation: observe_bootstrap2(&f.f.state).unwrap(),
			},
		)
		.unwrap();
	(b, id)
}
#[test]
fn terminal_observation_requires_retired_profile_three() {
	let f = F::new();
	assert_eq!(
		observe_retirement3(&f.f.state),
		Err(ObserveError2::NotRetired)
	);
	// admin_install.rs:365 (§4.1 invariant): a state not committed to 241 bytes is refused before
	// the retirement check, whether or not it is retired; the 241 twins are NotRetired above and
	// the accepted observation below.
	let (retired, o) = observation();
	assert_eq!(observe_retirement3(&retired.f.state).unwrap(), o);
	for wrong in [240, 242] {
		for state in [&f.f.state, &retired.f.state] {
			assert_eq!(
				observe_retirement3(&with_control_len_for_test(state, wrong)),
				Err(ObserveError2::ProfileUnsupported),
				"{wrong}"
			);
		}
	}
}
#[test]
fn terminal_tip_normalization_covers_zero_one_five_denials() {
	for n in [0, 1, 5] {
		let mut f = F::new();
		let proof = f.proof(&Command2::RetireRoot, [3, 0]);
		f.retire([3, 0]);
		let o = observe_retirement3(&f.f.state).unwrap();
		for _ in 0..n {
			denied(f.terminal(Command2::RetireRoot, Some(proof.clone())), 25);
		}
		assert_eq!(observe_retirement3(&f.f.state).unwrap(), o);
	}
}
#[test]
fn terminal_record_exact_length_nonzero_and_sequence() {
	let (_, o) = observation();
	let t = terminal_record3(&o, 3).unwrap();
	let b = encode_terminal3(&t).unwrap();
	assert_eq!(b.len(), 156);
	assert_eq!(decode_terminal3(&b).unwrap(), t);
	for n in 0..156 {
		assert!(decode_terminal3(&b[..n]).is_err());
	}
	let mut bad = t;
	bad.inventory_seq = 0;
	assert!(encode_terminal3(&bad).is_err());
	for field in 0..4 {
		let mut b = b;
		b[20 + field * 32..52 + field * 32].fill(0);
		assert!(decode_terminal3(&b).is_err());
	}
}
#[test]
fn terminal_owner_first_transition_and_snapshot_v4() {
	let (f, o) = observation();
	let (mut b, id) = bound(&f);
	b.apply(3, o, id).unwrap();
	let snapshot = b.snapshot();
	let root = &snapshot.entries[0];
	assert_eq!(root.state, RootState2::Retired);
	assert!(root.record.is_none());
	assert!(root.terminal.is_some());
	let bytes = std::fs::read(b.out.join("snapshot")).unwrap();
	assert_eq!(&bytes[8..10], &4u16.to_le_bytes());
	assert!(bytes[22 + 112 + 156..22 + 320].iter().all(|x| *x == 0));
	let log = std::fs::read(b.path.join("inventory.log")).unwrap();
	assert_eq!(&log[2 * 512 + 132..2 * 512 + 134], &156u16.to_le_bytes());
}
#[test]
fn terminal_retry_every_observation_field_before_after_reopen() {
	let (f, o) = observation();
	let (mut b, id) = bound(&f);
	b.apply(3, o, id).unwrap();
	let mut changed = Vec::new();
	for k in 0..13 {
		let mut x = o;
		match k {
			0 => x.identity.generation[0] ^= 1,
			1 => x.identity.cluster[0] ^= 1,
			2 => x.identity.pin[0] ^= 1,
			3 => x.identity.initial_epoch += 1,
			4 => x.identity.bootstrap.index += 1,
			5 => x.identity.bootstrap_fingerprint[0] ^= 1,
			6 => x.identity.bootstrap_command[0] ^= 1,
			7 => x.identity.genesis_fingerprint[0] ^= 1,
			8 => x.original_admin_key[0] ^= 1,
			9 => x.at.index += 1,
			10 => x.fingerprint[0] ^= 1,
			11 => x.command_digest[0] ^= 1,
			_ => x.state_digest[0] ^= 1,
		};
		changed.push(x);
	}
	let before = std::fs::read(b.path.join("inventory.log")).unwrap();
	for reopen in [false, true] {
		if reopen {
			b.reopen();
		}
		for n in [3, 4] {
			assert_eq!(b.apply(n, o, id).unwrap().current, RootState2::Retired);
			for x in &changed {
				assert_eq!(
					b.apply(n, *x, id).unwrap_err(),
					OwnerIoError2::Decision(OwnerError2::RetirementMismatch)
				);
			}
		}
	}
	assert_eq!(std::fs::read(b.path.join("inventory.log")).unwrap(), before);
}
#[test]
fn terminal_artifact_export_and_binding() {
	let (f, o) = observation();
	let (mut b, id) = bound(&f);
	b.apply(3, o, id).unwrap();
	let state = b.snapshot();
	let pin = hash(b"", &std::fs::read(b.out.join("snapshot")).unwrap());
	let a = TerminalArtifact3 {
		identity: id,
		terminal: state.entries[0].terminal.unwrap(),
		inventory_pin: pin,
		export_seq: 3,
	};
	let bytes = encode_terminal_artifact3(&a).unwrap();
	assert_eq!(read_terminal3(&bytes, &f.f.any, pin).unwrap(), a);
	assert!(read_terminal3(&bytes, &f.f.any, [9; 32]).is_err());
	for n in 0..bytes.len() {
		assert!(read_terminal3(&bytes[..n], &f.f.any, pin).is_err());
	}
	let mut bad = bytes;
	bad[200] ^= 1;
	assert!(read_terminal3(&bad, &f.f.any, pin).is_err());
	b.owner
		.as_mut()
		.unwrap()
		.export_terminal(&b.out, "manifest", "terminal", &id)
		.unwrap();
	assert_eq!(std::fs::read(b.out.join("terminal")).unwrap(), bytes);
}

#[test]
fn terminal_independent_wire_and_state_goldens() {
	use crate::storage::mutation_visibility::root2::admin::record::BootstrapIdentity2;
	let v: serde_json::Value =
		serde_json::from_str(include_str!("owner_binding/vectors.json")).unwrap();
	let log = |index| Log2 {
		term: 1,
		node: 1,
		index,
	};
	let o = TerminalObservation3 {
		identity: BootstrapIdentity2 {
			generation: [1; 16],
			cluster: [2; 16],
			pin: [3; 32],
			initial_epoch: 4,
			bootstrap: log(5),
			bootstrap_fingerprint: [6; 32],
			bootstrap_command: [7; 32],
			genesis_fingerprint: [8; 32],
		},
		original_admin_key: [9; 32],
		at: log(10),
		fingerprint: [11; 32],
		command_digest: [12; 32],
		state_digest: [13; 32],
	};
	assert_eq!(
		encode_terminal_observation3(&o).unwrap(),
		unhex(v["observation"].as_str().unwrap())
	);
	assert_eq!(
		observation_digest3(&o).unwrap().as_slice(),
		unhex(v["observation_digest"].as_str().unwrap())
	);
	let terminal = terminal_record3(&o, 3).unwrap();
	assert_eq!(
		encode_terminal3(&terminal).unwrap().as_slice(),
		unhex(v["terminal"].as_str().unwrap())
	);
	let a = TerminalArtifact3 {
		identity: RootIdentity2 {
			generation: [1; 16],
			cluster: [2; 16],
			pin: [3; 32],
			admin_key: [9; 32],
		},
		terminal,
		inventory_pin: [14; 32],
		export_seq: 4,
	};
	assert_eq!(
		encode_terminal_artifact3(&a).unwrap().as_slice(),
		unhex(v["artifact"].as_str().unwrap())
	);
	for row in v["states"].as_array().unwrap() {
		let bytes = unhex(row["input"].as_str().unwrap());
		let u = super::super::super::UnverifiedRoot2::decode(&bytes).unwrap();
		let (g, c, l, fp) = u.fixture_fields();
		let root = u.fixture_install(g, c, l, fp, hash(b"", &bytes)).unwrap();
		assert_eq!(
			super::super::super::retirement::terminal_state_digest3(&root)
				.unwrap()
				.as_slice(),
			unhex(row["digest"].as_str().unwrap())
		);
		let mut normalized = root.clone();
		let retired = normalized
			.state
			.role_epochs
			.as_ref()
			.unwrap()
			.retired
			.clone()
			.unwrap();
		normalized.state.covered = retired.at;
		normalized.state.fingerprint = retired.fingerprint;
		normalized.state.command_digest = retired.digest;
		assert_eq!(
			normalized.encode().unwrap(),
			unhex(row["normalized"].as_str().unwrap())
		);
	}
}
/// S4: a Retired entry is terminal-retired; one without its terminal record (the removed plain
/// Retire) is refused on encode as on decode (corpus N7). The accepted twin is the owner's v4 export.
#[test]
fn a_retired_entry_without_its_terminal_is_refused_on_encode() {
	let (f, o) = observation();
	let (mut b, id) = bound(&f);
	b.apply(3, o, id).unwrap();
	assert_eq!(b.snapshot().entries[0].state, RootState2::Retired);
	let plain = LogState2 {
		roots: vec![RootEntry2 {
			identity: id,
			manifest: f.f.any.clone(),
			state: RootState2::Retired,
			last_seq: 3,
			record: None,
			terminal: None,
		}],
		last_seq: 3,
	};
	assert_eq!(
		encode_snapshot2(&plain).err(),
		Some(SnapshotError2::Binding)
	);
}
#[test]
fn terminal_snapshot_v4_rejects_padding_profile_state_sequence() {
	let (f, o) = observation();
	let (mut b, id) = bound(&f);
	b.apply(3, o, id).unwrap();
	b.snapshot();
	let raw = std::fs::read(b.out.join("snapshot")).unwrap();
	for (at, value) in [
		(22 + 112 + 156, 1),
		(22 + 97, 2),
		(22 + 96, 2),
		(22 + 112 + 148, 4),
	] {
		let mut bytes = raw.clone();
		bytes[at] = value;
		let n = bytes.len() - 32;
		let sum = hash(b"", &bytes[..n]);
		bytes[n..].copy_from_slice(&sum);
		assert!(decode_snapshot2(&bytes).is_err(), "offset {at}");
	}
	let mut bytes = raw;
	bytes[22 + 112..22 + 320].fill(0);
	let n = bytes.len() - 32;
	let sum = hash(b"", &bytes[..n]);
	bytes[n..].copy_from_slice(&sum);
	assert_eq!(
		decode_snapshot2(&bytes).unwrap_err(),
		SnapshotError2::NotCanonical
	);
}

#[test]
fn terminal_mixed_cover_denied_suffix_keeps_state_digest() {
	let (f, _) = observation();
	let digest = super::super::super::retirement::terminal_state_digest3(f.root()).unwrap();
	for k in [0, 1, 5] {
		let mut root = f.root().clone();
		for n in 0..k {
			let at = Log2 {
				index: root.state.covered.index + 1,
				..root.state.covered
			};
			root = if n % 2 == 0 {
				Root2::cover(&root, at, [0x62; 32], 1).unwrap().0
			} else {
				let command = Command2::RetireRoot;
				let e = super::super::super::trust::TrustedEntry2::fixture(
					root.state.generation,
					root.state.cluster,
					at,
					&command,
				)
				.unwrap();
				super::super::super::retirement::consume(&root, &e, &command)
					.unwrap()
					.0
			};
		}
		assert_eq!(
			super::super::super::retirement::terminal_state_digest3(&root).unwrap(),
			digest
		);
	}
}
#[test]
fn terminal_first_transition_each_bootstrap_field_refused_without_append() {
	use crate::storage::mutation_visibility::root2::admin::record::BootstrapField2::*;
	let (f, o) = observation();
	let fields = [
		Generation,
		Cluster,
		Pin,
		InitialEpoch,
		Bootstrap,
		BootstrapFingerprint,
		BootstrapCommand,
		GenesisFingerprint,
	];
	for (k, field) in fields.into_iter().enumerate() {
		let (mut b, id) = bound(&f);
		let before = std::fs::read(b.path.join("inventory.log")).unwrap();
		let mut x = o;
		match k {
			0 => x.identity.generation[0] ^= 1,
			1 => x.identity.cluster[0] ^= 1,
			2 => x.identity.pin[0] ^= 1,
			3 => x.identity.initial_epoch += 1,
			4 => x.identity.bootstrap.index += 1,
			5 => x.identity.bootstrap_fingerprint[0] ^= 1,
			6 => x.identity.bootstrap_command[0] ^= 1,
			_ => x.identity.genesis_fingerprint[0] ^= 1,
		}
		assert_eq!(
			b.apply(3, x, id).unwrap_err(),
			OwnerIoError2::Decision(OwnerError2::RetirementField(field))
		);
		assert_eq!(std::fs::read(b.path.join("inventory.log")).unwrap(), before);
	}
	let (mut b, id) = bound(&f);
	let mut x = o;
	x.original_admin_key[0] ^= 1;
	assert_eq!(
		b.apply(3, x, id).unwrap_err(),
		OwnerIoError2::Decision(OwnerError2::RetirementMismatch)
	);
}
#[test]
fn terminal_observation_state_digest_keeps_non_tip_bytes() {
	let (f, o) = observation();
	let root = f.root();
	let mut changed = Vec::new();
	let mut x = root.clone();
	x.state.generation[0] ^= 1;
	changed.push(x);
	let mut x = root.clone();
	x.state.cluster[0] ^= 1;
	changed.push(x);
	let mut x = root.clone();
	x.state.initial_epoch += 1;
	changed.push(x);
	for x in changed {
		assert_ne!(
			super::super::super::retirement::terminal_state_digest3(&x).unwrap(),
			o.state_digest
		);
	}
	// Every byte outside the normalized tip is fed into the hash: byte-level negative controls
	// intentionally do not claim their mutated bytes are valid states.
	let mut normalized = root.clone();
	let retired = normalized
		.state
		.role_epochs
		.as_ref()
		.unwrap()
		.retired
		.clone()
		.unwrap();
	normalized.state.covered = retired.at;
	normalized.state.fingerprint = retired.fingerprint;
	normalized.state.command_digest = retired.digest;
	let bytes = normalized.encode().unwrap();
	for i in 0..bytes.len() {
		if (68..152).contains(&i) {
			continue;
		}
		let mut b = bytes.clone();
		b[i] ^= 1;
		assert_ne!(hash(b"VISROOT2-TERMINAL-STATE3\0", &b), o.state_digest);
	}
}

#[test]
fn terminal_observation_direct_unrooted_and_invalid_refusals() {
	assert_eq!(
		observe_retirement3(&empty_authority2()),
		Err(ObserveError2::PrerequisiteUnavailable)
	);
	let (f, _) = observation();
	let root = f.root();
	let invalid = with_tip_for_test(
		&f.f.state,
		Log2 {
			term: 0,
			node: 0,
			index: 0,
		},
		root.state.fingerprint,
		root.state.command_digest,
	);
	assert_eq!(
		observe_retirement3(&invalid),
		Err(ObserveError2::InvalidState)
	);
}
#[test]
fn terminal_export_refusal_order_and_reader_profile_manifest_negatives() {
	let f = F::new();
	let (mut b, id) = bound(&f);
	let before = std::fs::read(b.path.join("inventory.log")).unwrap();
	let owner = b.owner.as_mut().unwrap();
	assert_eq!(
		owner.export_terminal(&b.path, "../bad", "terminal", &id),
		Err(OwnerIoError2::BadExportName)
	);
	assert_eq!(
		owner.export_terminal(&b.path, "manifest", "terminal", &id),
		Err(OwnerIoError2::BadExportDir)
	);
	assert_eq!(
		owner.export_terminal(&b.out, "manifest", "terminal", &id),
		Err(OwnerIoError2::ExportNotTerminal(RootState2::Bound))
	);
	let mut absent = id;
	absent.generation[0] ^= 1;
	assert_eq!(
		owner.export_terminal(&b.out, "manifest", "terminal", &absent),
		Err(OwnerIoError2::ExportNoSuchRoot)
	);
	assert_eq!(std::fs::read_dir(&b.out).unwrap().count(), 0);
	assert_eq!(std::fs::read(b.path.join("inventory.log")).unwrap(), before);
	let (retired, o) = observation();
	let pin = [17; 32];
	let a = TerminalArtifact3 {
		identity: id,
		terminal: terminal_record3(&o, 3).unwrap(),
		inventory_pin: pin,
		export_seq: 3,
	};
	let bytes = encode_terminal_artifact3(&a).unwrap();
	assert_eq!(read_terminal3(&bytes, &retired.f.any, pin).unwrap(), a);
	// The manifest half of the profile check (terminal.rs:208, removed in d): a profile-1/2
	// manifest still decodes, and the reader refuses it.
	for name in ["a", "b"] {
		let old =
			AnyAdminManifest2::decode(&super::super::profile_two_binding::manifest_bytes(name))
				.unwrap();
		assert_eq!(
			read_terminal3(&bytes, &old, pin),
			Err(TerminalError3::Profile),
			"{name}"
		);
	}
	let mut base = retired.f.any.admission().clone();
	base.cluster[0] ^= 1;
	let wrong = AnyAdminManifest2::Three(
		AdminManifestThree3::new(
			base,
			GovernanceRole2::new([0xd2; 16], key(12)).unwrap(),
			RecoveryRole3::new([0xd3; 16], key(13)).unwrap(),
		)
		.unwrap(),
	);
	assert_eq!(
		read_terminal3(&bytes, &wrong, pin),
		Err(TerminalError3::Binding)
	);
}
/// N16: the VISTRM03 profile word (bytes 10..12) is checked after the version and before the
/// trailer: on a profile-3 manifest, word 1 or 2 is `Profile` whether or not the trailer was
/// resealed; word 3 is the accepted twin.
#[test]
fn terminal_artifact_profile_word_is_checked_before_the_trailer() {
	let f = F::new();
	let (_b, id) = bound(&f);
	let (retired, o) = observation();
	let pin = [17; 32];
	let a = TerminalArtifact3 {
		identity: id,
		terminal: terminal_record3(&o, 3).unwrap(),
		inventory_pin: pin,
		export_seq: 3,
	};
	let bytes = encode_terminal_artifact3(&a).unwrap().to_vec();
	assert_eq!(&bytes[10..12], &3u16.to_le_bytes());
	assert_eq!(read_terminal3(&bytes, &retired.f.any, pin).unwrap(), a);
	let with_word = |word: u16, reseal: bool| {
		let mut b = bytes.clone();
		b[10..12].copy_from_slice(&word.to_le_bytes());
		if reseal {
			let sum = hash(b"", &b[..304]);
			b[304..].copy_from_slice(&sum);
		}
		b
	};
	for word in [1u16, 2] {
		for reseal in [false, true] {
			assert_eq!(
				read_terminal3(&with_word(word, reseal), &retired.f.any, pin),
				Err(TerminalError3::Profile),
				"word {word}, resealed {reseal}"
			);
		}
	}
	// The version precedes the profile word; the trailer follows it.
	let mut versioned = with_word(1, true);
	versioned[8..10].copy_from_slice(&2u16.to_le_bytes());
	assert_eq!(
		read_terminal3(&versioned, &retired.f.any, pin),
		Err(TerminalError3::Version)
	);
	let mut stale = bytes.clone();
	stale[200] ^= 1;
	assert_eq!(
		read_terminal3(&stale, &retired.f.any, pin),
		Err(TerminalError3::Trailer)
	);
}
