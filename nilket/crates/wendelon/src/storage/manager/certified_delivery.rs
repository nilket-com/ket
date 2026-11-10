//! ADR 0704 §4: how every publication and retirement path classifies a certified
//! generation, under the canonical lease it already holds.
//!
//! The ring's provenance (a committed `Tier`'s `CertifiedProvenance`, or an
//! Applied entry's `CertifiedRetirement`) decides the class; the local
//! certificate is evidence for a first publication only, never the class.
//! - No provenance: today's ordinary rows, unchanged.
//! - Already published (local generation and rows are the ring's postimage, or
//!   for a retirement nothing is left): a no-op, reading no certificate and no
//!   debt.
//! - Otherwise its certificate must bind this operation, the local source and
//!   its rows must be the certificate's, and the publication's own deletes and
//!   inserts must be the certificate's drop and the ring's postimage.
//!
//! Anything else is a typed, non-replayable `CertifiedEvidence` fence: nothing
//! is installed or retired, and reads of the slab refuse.

use super::*;
use crate::consensus::TierSnapshotLease;
use crate::consensus::slab_ops::{OpKey, TierGeneration};
use crate::storage::index::{TierCertifiedPostimage, TierCertifiedSelection};
use crate::storage::manager::certified_lsm::{
	residual_after_outputs, residual_drop, residual_tables, row_member_key,
};
use crate::storage::manager::tier_deferral::{FenceCause, TierDeferral, fenced_error};

/// Why a certified generation or retirement is not proved by its local evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CertifiedEvidenceFault {
	/// The local generation is the committed one, but its rows are not the
	/// ring's postimage.
	PostimageNotLocal,
	/// No certificate for the operation (and nothing already published).
	CertificateMissing,
	/// The certificate binds another operation, source or postimage, or does
	/// not decode.
	CertificateMismatch,
	/// The local generation is not the certificate's source.
	SourceNotLocal,
	/// The local rows are not the certificate's source rows.
	SourceRowsMismatch,
	/// What the publication deletes is not the certificate's drop.
	DropMismatch,
	/// What the publication inserts does not hash to the ring's postimage.
	PostimageMismatch,
	/// A marked store holds a non-empty preimage of a slab the ring removed,
	/// and no Applied certified retirement names it: never deletion authority.
	NoRetirementAuthority,
}

/// Certified work met the no-source fallback (an isolated store, with no canonical
/// tier state): it has no provenance to classify by, so it is refused and nothing
/// is installed, retired or handed off.
#[derive(Debug)]
pub(crate) struct CertifiedWorkNeedsCanonicalState {
	pub slab_id: SlabId,
}

impl std::fmt::Display for CertifiedWorkNeedsCanonicalState {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"certified work on slab {} needs the canonical tier state",
			self.slab_id.get()
		)
	}
}

impl std::error::Error for CertifiedWorkNeedsCanonicalState {}

/// Whether `error` is that refusal.
#[cfg(test)]
pub(crate) fn is_certified_work_without_source(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|inner| inner.is::<CertifiedWorkNeedsCanonicalState>())
}

/// Whether a command is certified work: a Tier or Retire carrying provenance.
pub(super) fn command_is_certified(command: &crate::consensus::slab_ops::SlabCommand) -> bool {
	use crate::consensus::slab_ops::SlabCommand;
	match command {
		SlabCommand::Tier(op) => op.tier.certified.is_some(),
		SlabCommand::Retire(op) => op.certified.is_some(),
		_ => false,
	}
}

/// The class of a committed generation's publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum CertifiedPublication {
	/// No provenance: ordinary.
	Ordinary,
	/// The local generation and rows are already the ring's postimage.
	AlreadyPublished,
	/// A first publication: its rows must be exactly these.
	Publish(CertifiedRows),
}

/// What a certified publication must delete and insert.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CertifiedRows {
	source_rows_digest: [u8; 32],
	postimage_rows_digest: [u8; 32],
	drop_digest: [u8; 32],
	drop_rows_digest: [u8; 32],
	drop_count: u64,
}

impl CertifiedRows {
	fn of(certificate: &TierCertifiedSelection, postimage_rows_digest: [u8; 32]) -> Self {
		Self {
			source_rows_digest: certificate.source_rows_digest,
			postimage_rows_digest,
			drop_digest: certificate.drop_digest,
			drop_rows_digest: certificate.drop_rows_digest,
			drop_count: certificate.drop_count,
		}
	}
}

impl SlabManager {
	/// The no-source fallback's refusal of certified work on `slab_id`: an op intent
	/// naming the slab that requires a certificate, or whose command carries
	/// provenance. Ordinary work is untouched.
	pub(super) async fn refuse_certified_work_without_source(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<()> {
		let Some(index) = self.persistent_index.as_deref() else {
			return Ok(());
		};
		let certified = index.load_op_intents().await?.iter().any(|op| {
			op.command()
				.slabs()
				.iter()
				.any(|slab| slab.local_id() == slab_id)
				&& (op.certificate_requirement()
					== crate::storage::index::CertificateRequirement::Required
					|| command_is_certified(op.command()))
		});
		if certified {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				CertifiedWorkNeedsCanonicalState { slab_id },
			));
		}
		Ok(())
	}

	/// The slab's local rows and their digest.
	fn local_slab_rows(&self, slab_id: SlabId) -> (Vec<PersistentRecordRow>, [u8; 32]) {
		let rows = self.record_rows_for_slab_for_recovery(slab_id);
		let digest = crate::storage::index::lsm_rows_digest(rows.iter().copied());
		(rows, digest)
	}

	/// The certificate of `op`, checked to bind it: the digest the ring
	/// recorded it Applied under, the source generation and the postimage.
	async fn binding_certificate(
		&self,
		lease: &TierSnapshotLease,
		op: OpKey,
		source: &TierGeneration,
		postimage: TierCertifiedPostimage,
	) -> std::io::Result<Result<TierCertifiedSelection, CertifiedEvidenceFault>> {
		let index = self.require_op_index()?;
		let certificate = match index.tier_certificate(op.op_seq).await {
			Ok(Some(certificate)) => certificate,
			Ok(None) => return Ok(Err(CertifiedEvidenceFault::CertificateMissing)),
			Err(error) if error.kind() == ErrorKind::InvalidData => {
				return Ok(Err(CertifiedEvidenceFault::CertificateMismatch));
			}
			Err(error) => return Err(error),
		};
		let binds = lease.applied_op_digest(op) == Some(certificate.op_binding)
			&& certificate.source_generation == *source
			&& certificate.postimage == postimage;
		Ok(if binds {
			Ok(certificate)
		} else {
			Err(CertifiedEvidenceFault::CertificateMismatch)
		})
	}

	/// ADR 0704 §4, under the caller's lease and slab lock, for `pointer`, the
	/// committed generation (its standing is current).
	pub(super) async fn classify_certified_publication(
		&self,
		lease: &TierSnapshotLease,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Result<CertifiedPublication, CertifiedEvidenceFault>> {
		let slab = self.owner_ref()?.slab(pointer.slab_id);
		let Some(provenance) = lease.committed_provenance(slab) else {
			return Ok(Ok(CertifiedPublication::Ordinary));
		};
		let local = self.tier_pointer(pointer.slab_id).await;
		let (_, local_digest) = self.local_slab_rows(pointer.slab_id);
		if local.as_ref().map(TierGeneration::from) == Some(TierGeneration::from(pointer)) {
			// Already published, also after a hand-off: no certificate, no debt.
			return Ok(if local_digest == provenance.postimage_rows_digest {
				Ok(CertifiedPublication::AlreadyPublished)
			} else {
				Err(CertifiedEvidenceFault::PostimageNotLocal)
			});
		}
		let certificate = match self
			.binding_certificate(
				lease,
				provenance.op,
				&provenance.source,
				TierCertifiedPostimage::Rewrite {
					postimage_rows_digest: provenance.postimage_rows_digest,
				},
			)
			.await?
		{
			Ok(certificate) => certificate,
			Err(fault) => return Ok(Err(fault)),
		};
		if local.as_ref().map(TierGeneration::from) != Some(provenance.source.clone()) {
			return Ok(Err(CertifiedEvidenceFault::SourceNotLocal));
		}
		if local_digest != certificate.source_rows_digest {
			return Ok(Err(CertifiedEvidenceFault::SourceRowsMismatch));
		}
		Ok(Ok(CertifiedPublication::Publish(CertifiedRows::of(
			&certificate,
			provenance.postimage_rows_digest,
		))))
	}

	/// A first certified publication's own rows: the old rows are the
	/// certificate's source, the rows it deletes (a consuming match against what
	/// it inserts) are exactly the certificate's drop, and the rows it inserts
	/// hash to the ring's postimage.
	pub(super) async fn verify_certified_rows(
		&self,
		expected: &CertifiedRows,
		rows: Option<(&[PersistentRecordRow], &[PersistentRecordRow])>,
	) -> std::io::Result<Result<(), CertifiedEvidenceFault>> {
		let Some((old_rows, new_rows)) = rows else {
			return Ok(Err(CertifiedEvidenceFault::PostimageMismatch));
		};
		let digest = |rows: &[PersistentRecordRow]| {
			crate::storage::index::lsm_rows_digest(rows.iter().copied())
		};
		if digest(old_rows) != expected.source_rows_digest {
			return Ok(Err(CertifiedEvidenceFault::SourceRowsMismatch));
		}
		if digest(new_rows) != expected.postimage_rows_digest {
			return Ok(Err(CertifiedEvidenceFault::PostimageMismatch));
		}
		let produced: Vec<_> = new_rows.iter().map(row_member_key).collect();
		let Ok(deleted) = residual_after_outputs(old_rows, &produced) else {
			return Ok(Err(CertifiedEvidenceFault::DropMismatch));
		};
		self.drop_is_certified(&deleted, expected).await
	}

	/// Whether `deleted` is exactly the certificate's drop: its physical rows,
	/// its count, and its logical digest with each version's durable debt table.
	async fn drop_is_certified(
		&self,
		deleted: &[PersistentRecordRow],
		expected: &CertifiedRows,
	) -> std::io::Result<Result<(), CertifiedEvidenceFault>> {
		if deleted.len() as u64 != expected.drop_count
			|| crate::storage::index::lsm_rows_digest(deleted.iter().copied())
				!= expected.drop_rows_digest
		{
			return Ok(Err(CertifiedEvidenceFault::DropMismatch));
		}
		let index = self.require_op_index()?;
		let tables = residual_tables(index, deleted).await?;
		Ok(match residual_drop(deleted, &tables) {
			Some(drop) if drop.drop_digest == expected.drop_digest => Ok(()),
			_ => Err(CertifiedEvidenceFault::DropMismatch),
		})
	}

	/// ADR 0704 §4, a retirement in force, under the caller's lease and slab
	/// lock. Required authority only from an Applied certified retirement of
	/// exactly this owner and slab (several inconsistent refuse). Without one,
	/// an empty local postimage is a duplicate no-op, a non-empty preimage in a
	/// marked store is fenced, and Today is unchanged.
	pub(super) async fn classify_certified_retirement(
		&self,
		lease: &TierSnapshotLease,
		slab_id: SlabId,
	) -> std::io::Result<Result<(), CertifiedEvidenceFault>> {
		let owner = self.owner_ref()?;
		let local = self.tier_pointer(slab_id).await;
		let (rows, local_digest) = self.local_slab_rows(slab_id);
		let empty = local.is_none() && rows.is_empty();
		let Some(retirement) = lease.certified_retirement(owner, owner.slab(slab_id))? else {
			return Ok(if empty || !self.versions_contained() {
				Ok(())
			} else {
				Err(CertifiedEvidenceFault::NoRetirementAuthority)
			});
		};
		if empty {
			// Already published: no certificate, no debt.
			return Ok(Ok(()));
		}
		let certificate = match self
			.binding_certificate(
				lease,
				retirement.op,
				&retirement.source,
				TierCertifiedPostimage::Retire,
			)
			.await?
		{
			Ok(certificate) => certificate,
			Err(fault) => return Ok(Err(fault)),
		};
		if local.as_ref().map(TierGeneration::from) != Some(retirement.source.clone()) {
			return Ok(Err(CertifiedEvidenceFault::SourceNotLocal));
		}
		if local_digest != certificate.source_rows_digest {
			return Ok(Err(CertifiedEvidenceFault::SourceRowsMismatch));
		}
		// A retirement deletes every local row: they are exactly its drop.
		self.drop_is_certified(&rows, &CertifiedRows::of(&certificate, [0; 32]))
			.await
	}

	/// A snapshot retirement mark's slab, classified against `lease` (the installed or
	/// canonical state) when there is one. With none, a marked store has no evidence of
	/// a certified retirement: an empty local postimage is a no-op, a non-empty one is
	/// fenced. Today is unchanged.
	pub(super) async fn classify_snapshot_retirement(
		&self,
		lease: Option<&TierSnapshotLease>,
		slab_id: SlabId,
	) -> std::io::Result<Result<(), CertifiedEvidenceFault>> {
		if let Some(lease) = lease {
			return self.classify_certified_retirement(lease, slab_id).await;
		}
		let empty = self.tier_pointer(slab_id).await.is_none()
			&& self.local_slab_rows(slab_id).0.is_empty();
		Ok(if empty || !self.versions_contained() {
			Ok(())
		} else {
			Err(CertifiedEvidenceFault::NoRetirementAuthority)
		})
	}

	/// Fences `pointer`'s publication: a typed, non-replayable deferral of exactly
	/// it, under its callback's index. Nothing is installed.
	pub(super) fn fence_certified_publication(
		&self,
		pointer: &TieredSlabPointer,
		fault: CertifiedEvidenceFault,
		last_control_log_index: Option<u64>,
	) -> Error {
		tracing::error!(
			slab_id = pointer.slab_id.get(),
			l2_uri = %pointer.l2_uri,
			?fault,
			"a certified tier publication is not proved by its local evidence; it stays fenced"
		);
		// Test-only trace of the fault behind a fence.
		#[cfg(test)]
		eprintln!("CERTIFIED FENCE {fault:?}");
		self.record_tier_deferral(
			pointer.slab_id,
			TierDeferral::fenced(
				pointer.clone(),
				FenceCause::CertifiedEvidence,
				last_control_log_index,
			),
		);
		fenced_error(
			ErrorKind::PermissionDenied,
			pointer.slab_id,
			FenceCause::CertifiedEvidence,
		)
	}

	/// Fences a retirement: the local generation keeps a typed deferral (reads of
	/// it refuse); with no local pointer, the retirement fence. Nothing is retired.
	pub(super) async fn fence_certified_retirement(
		&self,
		slab_id: SlabId,
		fault: CertifiedEvidenceFault,
		last_control_log_index: Option<u64>,
	) -> Error {
		tracing::error!(
			slab_id = slab_id.get(),
			?fault,
			"a certified retirement is not proved by its local evidence; nothing is retired"
		);
		// Test-only trace of the fault behind a fence.
		#[cfg(test)]
		eprintln!("CERTIFIED FENCE {fault:?}");
		match self.tier_pointer(slab_id).await {
			Some(local) => self.record_tier_deferral(
				slab_id,
				TierDeferral::fenced(local, FenceCause::CertifiedEvidence, last_control_log_index),
			),
			None => self.fence_unapplied_tier_retirement(slab_id, last_control_log_index),
		}
		fenced_error(
			ErrorKind::PermissionDenied,
			slab_id,
			FenceCause::CertifiedEvidence,
		)
	}

	/// For the hand-offs: whether this store holds `tier`'s exact certified
	/// postimage. An ordinary generation needs only its pointer.
	pub(super) fn local_postimage_is_exact(&self, tier: &TieredSlab) -> bool {
		match &tier.certified {
			None => true,
			Some(provenance) => {
				self.local_slab_rows(tier.slab.local_id()).1 == provenance.postimage_rows_digest
			}
		}
	}

	/// For the hand-offs: whether a retirement left nothing of the slab here.
	pub(super) fn local_retirement_is_exact(&self, slab_id: SlabId) -> bool {
		self.local_slab_rows(slab_id).0.is_empty()
	}
}

/// The verification path's check of a pending certified generation, from the
/// plan it built and under the lease it holds: the plan's new rows must be the
/// ring's postimage; unchanged rows are already published; otherwise the
/// certificate must bind the operation and the plan must be its exact drop.
pub(super) async fn verify_certified_generation(
	manager: &SlabManager,
	lease: &TierSnapshotLease,
	pointer: &TieredSlabPointer,
	old_rows: &[PersistentRecordRow],
	new_rows: &[PersistentRecordRow],
) -> std::io::Result<()> {
	let slab = manager.owner_ref()?.slab(pointer.slab_id);
	// Only this store's current committed generation verifies; an absent or other
	// generation is obsolete, and only a matching ordinary one is ordinary.
	let Some(committed) = lease
		.committed_tier(slab)
		.filter(|tier| TierGeneration::from(tier) == TierGeneration::from(pointer))
	else {
		return Err(super::tier_authority::obsolete_verification());
	};
	let Some(provenance) = committed.certified else {
		return Ok(());
	};
	let new_digest = crate::storage::index::lsm_rows_digest(new_rows.iter().copied());
	let fault = if new_digest != provenance.postimage_rows_digest {
		Some(CertifiedEvidenceFault::PostimageMismatch)
	} else if old_rows == new_rows {
		None
	} else {
		match manager
			.binding_certificate(
				lease,
				provenance.op,
				&provenance.source,
				TierCertifiedPostimage::Rewrite {
					postimage_rows_digest: provenance.postimage_rows_digest,
				},
			)
			.await?
		{
			Err(fault) => Some(fault),
			Ok(certificate) => manager
				.verify_certified_rows(
					&CertifiedRows::of(&certificate, provenance.postimage_rows_digest),
					Some((old_rows, new_rows)),
				)
				.await?
				.err(),
		}
	};
	match fault {
		None => Ok(()),
		Some(fault) => {
			tracing::error!(
				slab_id = pointer.slab_id.get(),
				?fault,
				"a pending certified generation is not proved; verification refuses"
			);
			Err(fenced_error(
				ErrorKind::PermissionDenied,
				pointer.slab_id,
				FenceCause::CertifiedEvidence,
			))
		}
	}
}
