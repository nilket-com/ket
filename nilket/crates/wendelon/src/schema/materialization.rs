//! The materialization certificate attachment catalog (ADR 0087).
//!
//! One immutable record per materialized `TableObjectId`, created only
//! inside the CTAS saga's `Publish` apply and removed only when the object
//! is destroyed. The catalog exposes no update path: "immutable" is the
//! absence of an API, not a convention. This is deliberately one attachment
//! kind, not a general per-object metadata slot — a second kind is its own
//! admission decision.

use std::collections::HashMap;

use crate::schema::{TableObjectId, TenantId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationCertificateCatalog {
	by_object: HashMap<(TenantId, TableObjectId), Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CertificateAlreadyAttached;

impl MaterializationCertificateCatalog {
	/// Admits the one record an object will ever carry. A second admission
	/// for the same object is a broken invariant, surfaced rather than
	/// overwritten — the saga admits each object's publish exactly once.
	pub fn admit(
		&mut self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		certificate: Vec<u8>,
	) -> Result<(), CertificateAlreadyAttached> {
		match self.by_object.entry((tenant_id, table_object_id)) {
			std::collections::hash_map::Entry::Occupied(_) => Err(CertificateAlreadyAttached),
			std::collections::hash_map::Entry::Vacant(slot) => {
				slot.insert(certificate);
				Ok(())
			}
		}
	}

	pub fn get(&self, tenant_id: TenantId, table_object_id: TableObjectId) -> Option<&[u8]> {
		self.by_object
			.get(&(tenant_id, table_object_id))
			.map(Vec::as_slice)
	}

	/// Destruction coupling: called from the apply that finalizes the
	/// object's destruction, and from nowhere else.
	pub fn remove(&mut self, tenant_id: TenantId, table_object_id: TableObjectId) {
		self.by_object.remove(&(tenant_id, table_object_id));
	}

	pub fn len(&self) -> usize {
		self.by_object.len()
	}

	pub fn is_empty(&self) -> bool {
		self.by_object.is_empty()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn object(byte: u8) -> TableObjectId {
		TableObjectId::new([byte; 16])
	}

	#[test]
	fn admits_once_and_removal_couples_to_destruction() {
		let mut catalog = MaterializationCertificateCatalog::default();
		let tenant = TenantId(7);
		catalog
			.admit(tenant, object(1), vec![1, 2, 3])
			.expect("first admission");
		assert_eq!(catalog.get(tenant, object(1)), Some(&[1u8, 2, 3][..]));
		assert_eq!(
			catalog.admit(tenant, object(1), vec![9]),
			Err(CertificateAlreadyAttached),
			"immutability is structural: no second admission, no overwrite"
		);
		assert_eq!(
			catalog.get(tenant, object(1)),
			Some(&[1u8, 2, 3][..]),
			"the failed admission changed nothing"
		);
		catalog.remove(tenant, object(1));
		assert_eq!(catalog.get(tenant, object(1)), None);
		assert!(catalog.is_empty());
	}
}
