//! The durable local certificate of a certified tiered rewrite or retirement.
//!
//! A certified job's one operation is allocated with its certificate in one
//! transaction, which first gives the command's provenance the operation's own
//! key, then fixes the command digest, and requires the active generation to be
//! the one the selection was decided under. Every transaction that removes the
//! operation's intent removes its certificate, by exact binding. A store whose
//! intents and certificates disagree fences certified tiered cleanup at open.

// The certified tier step is its production caller; until then, tests only.
#![cfg_attr(not(test), allow(dead_code))]

use super::op_state::{CertificateRequirement, OpAllocation, OpIntent};
use super::*;
use crate::consensus::core::{SlabRef, TieredSlab};
use crate::consensus::slab_ops::{
	CertifiedProvenance, CertifiedRetirement, OpKey, RetireOp, SlabCommand, TierGeneration, TierOp,
};
use serde::{Deserialize, Serialize};

pub(super) const TIER_CERTIFICATES_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("tier_certified_selections_v1");

/// What a certified job publishes: survivors in a fresh object, or nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TierCertifiedPostimage {
	/// The delivery's own rows derivation over the uploaded, read-back object.
	Rewrite { postimage_rows_digest: [u8; 32] },
	/// No record survives: the slab retires and has no postimage.
	Retire,
}

/// What a certified tiered operation is bound to, keyed by its `op_seq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TierCertifiedSelection {
	/// The job: owner, incarnation, slab, source generation and its rows digest.
	pub job_fingerprint: [u8; 32],
	/// The generation the selection was decided under.
	pub active_gid: u64,
	/// The published generation the job read, which the operation replaces.
	pub source_generation: TierGeneration,
	/// The slab's durable rows under `source_generation`.
	pub source_rows_digest: [u8; 32],
	pub postimage: TierCertifiedPostimage,
	/// `canonical_drop`'s logical digest, physical rows digest and count.
	pub drop_digest: [u8; 32],
	pub drop_rows_digest: [u8; 32],
	pub drop_count: u64,
	/// The operation this certifies: its command digest.
	pub op_binding: [u8; 32],
}

/// A certified selection's evidence before allocation binds it to an operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TierSelectionEvidence {
	pub job_fingerprint: [u8; 32],
	pub active_gid: u64,
	pub source_generation: TierGeneration,
	pub source_rows_digest: [u8; 32],
	pub drop_digest: [u8; 32],
	pub drop_rows_digest: [u8; 32],
	pub drop_count: u64,
}

/// The one operation a certified job proposes. Its provenance is not supplied:
/// allocation builds it from the allocated key and the evidence's source, so a
/// provenance naming another operation, source or slab cannot be allocated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CertifiedTierWork {
	/// Survivors: `tier` (with no provenance of its own) replaces the source.
	Rewrite {
		tier: TieredSlab,
		postimage_rows_digest: [u8; 32],
	},
	/// None survive: `slab` retires its source generation.
	Retire { slab: SlabRef },
}

impl CertifiedTierWork {
	pub(crate) fn slab(&self) -> SlabRef {
		match self {
			Self::Rewrite { tier, .. } => tier.slab,
			Self::Retire { slab } => *slab,
		}
	}

	fn postimage(&self) -> TierCertifiedPostimage {
		match self {
			Self::Rewrite {
				postimage_rows_digest,
				..
			} => TierCertifiedPostimage::Rewrite {
				postimage_rows_digest: *postimage_rows_digest,
			},
			Self::Retire { .. } => TierCertifiedPostimage::Retire,
		}
	}

	/// Operation `op`'s command: its provenance names `op` and `source`, so the
	/// command digest covers both.
	pub(super) fn command_of(
		&self,
		op: OpKey,
		source: &TierGeneration,
	) -> std::io::Result<SlabCommand> {
		if op.owner != self.slab().owner() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"a certified tier operation belongs to its slab's owner",
			));
		}
		Ok(match self {
			Self::Rewrite {
				tier,
				postimage_rows_digest,
			} => {
				if tier.certified.is_some() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"allocation, not its caller, gives a certified rewrite its provenance",
					));
				}
				let mut tier = tier.clone();
				tier.certified = Some(CertifiedProvenance {
					op,
					source: source.clone(),
					postimage_rows_digest: *postimage_rows_digest,
				});
				SlabCommand::Tier(TierOp {
					tier,
					replaces: Some(source.clone()),
				})
			}
			Self::Retire { slab } => SlabCommand::Retire(RetireOp {
				slab: *slab,
				expected: source.clone(),
				certified: Some(CertifiedRetirement {
					op,
					slab: *slab,
					source: source.clone(),
				}),
			}),
		})
	}

	/// The certificate of the command allocated with digest `op_binding`.
	pub(super) fn certificate(
		&self,
		evidence: &TierSelectionEvidence,
		op_binding: [u8; 32],
	) -> TierCertifiedSelection {
		TierCertifiedSelection {
			job_fingerprint: evidence.job_fingerprint,
			active_gid: evidence.active_gid,
			source_generation: evidence.source_generation.clone(),
			source_rows_digest: evidence.source_rows_digest,
			postimage: self.postimage(),
			drop_digest: evidence.drop_digest,
			drop_rows_digest: evidence.drop_rows_digest,
			drop_count: evidence.drop_count,
			op_binding,
		}
	}
}

/// What a certified tiered allocation did.
#[derive(Debug)]
pub(crate) enum CertifiedTierAllocation {
	/// The intent (`Required`) and its certificate, durable together.
	Allocated(OpAllocation),
	/// The active generation is not the selection's: nothing was written and no
	/// sequence was consumed.
	GenerationMoved,
}

/// The certificate at an operation's sequence binds another operation; the
/// transaction that would have removed it is refused whole.
#[derive(Debug)]
pub(crate) struct TierCertificateBindsAnotherOp {
	pub op_seq: u64,
}

impl std::fmt::Display for TierCertificateBindsAnotherOp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"the tier certificate at op {} binds another operation",
			self.op_seq
		)
	}
}

impl std::error::Error for TierCertificateBindsAnotherOp {}

/// One disagreement between the op intents and the tier certificates, found at
/// open. Any one fences certified tiered cleanup on the store; reads are unaffected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CertifiedTierOrphan {
	/// A certificate whose intent is gone.
	CertificateWithoutIntent { op_seq: u64 },
	/// A certificate beside an intent that does not require one.
	CertificateOfUncertifiedIntent { op_seq: u64 },
	/// A `Required` intent whose certificate is gone.
	IntentWithoutCertificate { op_seq: u64 },
	/// A certificate that does not bind its intent's operation (its digest,
	/// source generation or postimage).
	CertificateBindsAnotherOp { op_seq: u64 },
	/// An intent whose requirement disagrees with its command's provenance.
	RequirementMismatch { op_seq: u64 },
	/// A certificate row that does not decode.
	UndecodableCertificate { op_seq: u64 },
}

fn decode(bytes: &[u8]) -> std::io::Result<TierCertifiedSelection> {
	serde_json::from_slice(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

fn encode(certificate: &TierCertifiedSelection) -> std::io::Result<Vec<u8>> {
	serde_json::to_vec(certificate).map_err(Error::other)
}

/// Whether the command carries certified provenance, so its intent needs a certificate.
pub(super) fn requirement_of(command: &SlabCommand) -> CertificateRequirement {
	match command {
		SlabCommand::Tier(op) if op.tier.certified.is_some() => CertificateRequirement::Required,
		SlabCommand::Retire(op) if op.certified.is_some() => CertificateRequirement::Required,
		_ => CertificateRequirement::None,
	}
}

/// Whether `certificate` binds exactly `intent`'s operation: its digest, and the
/// source and postimage its command's provenance names.
fn binds(intent: &OpIntent, certificate: &TierCertifiedSelection) -> bool {
	if certificate.op_binding != intent.digest {
		return false;
	}
	match (&intent.command, certificate.postimage) {
		(
			SlabCommand::Tier(TierOp {
				tier: TieredSlab {
					certified: Some(provenance),
					..
				},
				..
			}),
			TierCertifiedPostimage::Rewrite {
				postimage_rows_digest,
			},
		) => {
			provenance.source == certificate.source_generation
				&& provenance.postimage_rows_digest == postimage_rows_digest
		}
		(
			SlabCommand::Retire(RetireOp {
				certified: Some(retirement),
				..
			}),
			TierCertifiedPostimage::Retire,
		) => retirement.source == certificate.source_generation,
		_ => false,
	}
}

/// Writes `certificate` for operation `op_seq` in the caller's transaction,
/// after requiring the active generation it was decided under. `Ok(false)`: the
/// generation moved and nothing is written. A second certificate for one
/// sequence is `AlreadyExists`.
pub(super) fn insert_certificate(
	tx: &redb::WriteTransaction,
	op_seq: u64,
	certificate: &TierCertifiedSelection,
) -> std::io::Result<bool> {
	if certified_base::active_gid_in(tx)? != certificate.active_gid {
		return Ok(false);
	}
	let bytes = encode(certificate)?;
	let mut table = tx.open_table(TIER_CERTIFICATES_TABLE).map_err(redb_to_io)?;
	if table.get(&op_seq).map_err(redb_to_io)?.is_some() {
		return Err(Error::new(
			ErrorKind::AlreadyExists,
			"a tier certificate already exists for this operation",
		));
	}
	table
		.insert(&op_seq, bytes.as_slice())
		.map_err(redb_to_io)?;
	Ok(true)
}

/// Removes the certificate of the intent being removed, in the caller's
/// transaction. An intent with no requirement has nothing to remove; a
/// `Required` one whose certificate is already gone removes nothing. A
/// certificate binding another operation refuses the whole transaction.
pub(super) fn remove_certificate_of(
	tx: &redb::WriteTransaction,
	op_seq: u64,
	intent: &OpIntent,
) -> std::io::Result<()> {
	if intent.certificate == CertificateRequirement::None {
		return Ok(());
	}
	let mut table = tx.open_table(TIER_CERTIFICATES_TABLE).map_err(redb_to_io)?;
	let Some(bytes) = table
		.get(&op_seq)
		.map_err(redb_to_io)?
		.map(|bytes| bytes.value().to_vec())
	else {
		return Ok(());
	};
	if !decode(&bytes).is_ok_and(|existing| binds(intent, &existing)) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			TierCertificateBindsAnotherOp { op_seq },
		));
	}
	table.remove(&op_seq).map_err(redb_to_io)?;
	Ok(())
}

/// Every disagreement between `intents` and the certificate table.
pub(super) fn orphans(
	intents: &[(u64, OpIntent)],
	table: &impl ReadableTable<u64, &'static [u8]>,
) -> std::io::Result<Vec<CertifiedTierOrphan>> {
	let mut orphans = Vec::new();
	let mut certificates = std::collections::BTreeMap::new();
	for row in table.iter().map_err(redb_to_io)? {
		let (op_seq, bytes) = row.map_err(redb_to_io)?;
		let op_seq = op_seq.value();
		match decode(bytes.value()) {
			Ok(certificate) => {
				certificates.insert(op_seq, certificate);
			}
			Err(_) => orphans.push(CertifiedTierOrphan::UndecodableCertificate { op_seq }),
		}
	}
	for (op_seq, intent) in intents {
		let op_seq = *op_seq;
		if requirement_of(&intent.command) != intent.certificate {
			orphans.push(CertifiedTierOrphan::RequirementMismatch { op_seq });
		}
		let certificate = certificates.remove(&op_seq);
		match (intent.certificate, certificate) {
			(CertificateRequirement::None, None) => {}
			(CertificateRequirement::None, Some(_)) => {
				orphans.push(CertifiedTierOrphan::CertificateOfUncertifiedIntent { op_seq })
			}
			(CertificateRequirement::Required, None) => {
				if !orphans.contains(&CertifiedTierOrphan::UndecodableCertificate { op_seq }) {
					orphans.push(CertifiedTierOrphan::IntentWithoutCertificate { op_seq });
				}
			}
			(CertificateRequirement::Required, Some(certificate)) => {
				if !binds(intent, &certificate) {
					orphans.push(CertifiedTierOrphan::CertificateBindsAnotherOp { op_seq });
				}
			}
		}
	}
	orphans.extend(
		certificates
			.into_keys()
			.map(|op_seq| CertifiedTierOrphan::CertificateWithoutIntent { op_seq }),
	);
	Ok(orphans)
}

impl PersistentLocalIndex {
	/// The certificate of operation `op_seq`, as it stands now.
	pub(crate) async fn tier_certificate(
		&self,
		op_seq: u64,
	) -> std::io::Result<Option<TierCertifiedSelection>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				tier_certificate_in(&tx, op_seq)
			})
			.await
	}
}

/// The certificate of operation `op_seq`, under the caller's read transaction.
pub(crate) fn tier_certificate_in(
	tx: &redb::ReadTransaction,
	op_seq: u64,
) -> std::io::Result<Option<TierCertifiedSelection>> {
	tx.open_table(TIER_CERTIFICATES_TABLE)
		.map_err(redb_to_io)?
		.get(&op_seq)
		.map_err(redb_to_io)?
		.map(|bytes| decode(bytes.value()))
		.transpose()
}

#[cfg(test)]
impl PersistentLocalIndex {
	/// Test-only: writes or removes an operation's certificate as raw durable
	/// bytes, with no binding or generation check.
	pub(crate) async fn put_tier_certificate_raw_for_test(
		&self,
		op_seq: u64,
		bytes: Option<Vec<u8>>,
	) {
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx.open_table(TIER_CERTIFICATES_TABLE).map_err(redb_to_io)?;
					match &bytes {
						Some(bytes) => {
							table
								.insert(&op_seq, bytes.as_slice())
								.map_err(redb_to_io)?;
						}
						None => {
							table.remove(&op_seq).map_err(redb_to_io)?;
						}
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
			.unwrap();
	}

	/// Test-only: a certificate's raw durable bytes.
	pub(crate) fn encode_tier_certificate_for_test(
		certificate: &TierCertifiedSelection,
	) -> Vec<u8> {
		encode(certificate).unwrap()
	}
}
