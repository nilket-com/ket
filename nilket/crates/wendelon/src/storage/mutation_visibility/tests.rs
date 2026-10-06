use super::commitment::TestProofKey;
use super::*;

mod authority_tests;
mod b1_assay;
mod codec_tests;
mod commitment_tests;
mod manifest_tests;
mod provenance_tests;

fn identity(sequence: u64) -> MutationIdentity {
	MutationIdentity {
		generation: [1; 16],
		tenant: 7,
		scope: MutationScope::Table,
		table: [2; 16],
		schema_generation: 3,
		caller: [4; 16],
		sequence,
	}
}
fn log(index: u64) -> DecisionLogId {
	DecisionLogId {
		term: 2,
		node: 1,
		index,
	}
}
fn manifest(sequence: u64) -> ParentManifest {
	ParentManifest {
		identity: identity(sequence),
		admission: Admission {
			epoch: 1,
			holder: 1,
		},
		key_id: [5; 16],
		key_version: 1,
		operation: MutationOperation::Insert,
		content_codec: 1,
		children: vec![ManifestChild {
			ordinal: 0,
			range_start: 10,
			range_end: 100,
			partition: Some(1),
			versions: vec![LogicalVersion {
				ordinal: 0,
				key_token: [6; 32],
				content_commitment: [7; 32],
				tombstone: false,
			}],
		}],
	}
}
fn event(index: u64, sequence: u64, terminal: TerminalDecision) -> AuthorityEvent {
	AuthorityEvent::Decision {
		log: log(index),
		manifest: manifest(sequence),
		commitment: [8; 32],
		terminal,
	}
}
fn authority() -> DecisionAuthority {
	DecisionAuthority::new([1; 16], 1, log(1)).unwrap()
}
fn record(outcome: ApplyOutcome) -> DecisionRecord {
	match outcome {
		ApplyOutcome::Recorded(r) | ApplyOutcome::Replayed(r) => r,
		_ => panic!("expected decision"),
	}
}
fn overwrite_envelope(bytes: &mut [u8], offset: usize, value: &[u8]) {
	bytes[offset..offset + value.len()].copy_from_slice(value);
	let end = bytes.len() - 32;
	let checksum = blake3::hash(&bytes[..end]);
	bytes[end..].copy_from_slice(checksum.as_bytes());
}
fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}
