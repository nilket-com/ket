//! ADR 0703: the durable certificate of a certified LSM install.
//!
//! A certified job's install intent is written with its certificate in one
//! transaction, which also requires the active generation to be the one the
//! selection was decided under. Every transaction that removes the intent
//! removes its certificate, both by exact match. Recovery accepts a certified
//! install only with its matching certificate and never decides again.

use super::*;
use serde::{Deserialize, Serialize};

pub(super) const LSM_CERTIFICATES_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("lsm_certified_selections_v1");

/// What a certified job's install is bound to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LsmCertificate {
	/// The job and its exact claimed inputs.
	pub job_fingerprint: [u8; 32],
	/// The generation the selection was decided under.
	pub active_gid: u64,
	/// `canonical_drop`'s logical digest, physical rows digest and count.
	pub drop_digest: [u8; 32],
	pub drop_rows_digest: [u8; 32],
	pub drop_count: u64,
	/// The install this certifies: see [`intent_binding`].
	pub intent_binding: [u8; 32],
}

/// An install's identity, stable across its intent's phase transitions: its
/// commit digest and its recorded input and output row digests.
pub(crate) fn intent_binding(intent: &LsmInstallIntent) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(b"lsm-certificate-binding-v1");
	hasher.update(&intent.commit.job_id.get().to_le_bytes());
	hasher.update(&intent.commit_digest);
	hasher.update(&intent.local_input_rows_digest);
	hasher.update(&intent.local_output_rows_digest);
	*hasher.finalize().as_bytes()
}

fn decode(bytes: &[u8]) -> std::io::Result<LsmCertificate> {
	serde_json::from_slice(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

/// Writes `certificate` for `intent`'s job in the caller's transaction, after
/// requiring the active generation it was decided under. `Ok(false)`: the
/// generation moved, nothing is written and the caller abandons.
pub(super) fn insert_certificate(
	tx: &redb::WriteTransaction,
	intent: &LsmInstallIntent,
	certificate: &LsmCertificate,
) -> std::io::Result<bool> {
	if certificate.intent_binding != intent_binding(intent) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"an LSM certificate binds another install",
		));
	}
	if certified_base::active_gid_in(tx)? != certificate.active_gid {
		return Ok(false);
	}
	let job = intent.commit.job_id.get();
	let bytes = serde_json::to_vec(certificate).map_err(Error::other)?;
	let mut table = tx.open_table(LSM_CERTIFICATES_TABLE).map_err(redb_to_io)?;
	if table.get(&job).map_err(redb_to_io)?.is_some() {
		return Err(Error::new(
			ErrorKind::AlreadyExists,
			"an LSM certificate already exists for this job",
		));
	}
	table.insert(&job, bytes.as_slice()).map_err(redb_to_io)?;
	Ok(true)
}

/// Removes the certificate of the intent being removed in the caller's
/// transaction. A certificate binding another install refuses the whole
/// transaction; an intent without one has nothing to remove.
pub(super) fn remove_certificate_of(
	tx: &redb::WriteTransaction,
	intent: &LsmInstallIntent,
) -> std::io::Result<()> {
	let job = intent.commit.job_id.get();
	let mut table = tx.open_table(LSM_CERTIFICATES_TABLE).map_err(redb_to_io)?;
	let Some(existing) = table
		.get(&job)
		.map_err(redb_to_io)?
		.map(|bytes| decode(bytes.value()))
		.transpose()?
	else {
		return Ok(());
	};
	if existing.intent_binding != intent_binding(intent) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"the LSM certificate binds another install",
		));
	}
	table.remove(&job).map_err(redb_to_io)?;
	Ok(())
}

/// Every certificate, by job.
pub(super) fn read_certificates(
	table: &impl ReadableTable<u64, &'static [u8]>,
) -> std::io::Result<Vec<(u64, LsmCertificate)>> {
	let mut certificates = Vec::new();
	for row in table.iter().map_err(redb_to_io)? {
		let (job, bytes) = row.map_err(redb_to_io)?;
		certificates.push((job.value(), decode(bytes.value())?));
	}
	Ok(certificates)
}

#[cfg(test)]
impl PersistentLocalIndex {
	/// Test-only: writes or removes a job's certificate as raw durable state,
	/// with no binding or generation check.
	pub(crate) async fn put_lsm_certificate_raw_for_test(
		&self,
		job: u64,
		certificate: Option<LsmCertificate>,
	) {
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx.open_table(LSM_CERTIFICATES_TABLE).map_err(redb_to_io)?;
					match &certificate {
						Some(certificate) => {
							let bytes = serde_json::to_vec(certificate).map_err(Error::other)?;
							table.insert(&job, bytes.as_slice()).map_err(redb_to_io)?;
						}
						None => {
							table.remove(&job).map_err(redb_to_io)?;
						}
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
			.unwrap();
	}
}
