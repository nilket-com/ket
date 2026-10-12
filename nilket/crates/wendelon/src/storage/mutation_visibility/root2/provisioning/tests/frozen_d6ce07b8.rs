//! The published d6ce07b8 inventory and snapshot codecs and fold, frozen VERBATIM for the
//! old-reader proof: each `frozen_d6ce07b8/*.in` file is the published file minus its two
//! leading `//!` lines (inner doc comments cannot be `include!`d). Nothing else in them is
//! adapted.
//!
//! Not verbatim, and only these: this file, the two module declarations, `#[allow(dead_code)]`
//! and the `include!` calls. The frozen snapshot's own `use super::inventory::{..}` resolves to
//! the frozen inventory, so it never reaches the live `validate`, `encode` or types.
//!
//! Shared with the live tree, not frozen, because M2 leaves their source, API and semantics
//! exactly unchanged (M2 only appends to admin/manifest.rs): `AdminManifest2` with its decode,
//! encode and `admin_manifest_digest2`; `BootstrapRecord2` and `BootstrapObservation2`;
//! `check_enrollable_admin_key2`; `codec::hash`; `Digest2`; the `redacted_debug!` macro.
#[allow(dead_code)]
pub(super) mod inventory {
	include!("frozen_d6ce07b8/inventory.in");
}
#[allow(dead_code)]
pub(super) mod snapshot {
	include!("frozen_d6ce07b8/snapshot.in");
}
