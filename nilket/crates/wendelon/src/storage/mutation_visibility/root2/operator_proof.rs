//! The operator proof facade (key custody K7): what an offline custody tool needs to produce a
//! Root2 role proof or administrator statement without reimplementing any of it. It re-verifies a
//! context export into a `VerifiedContext`, builds unsigned layouts and their preimages per form,
//! splices a signature, merges a two-host joint proof, and self-checks the result with the
//! production checkers under the view the reader built.
//!
//! Library only, dormant: no node path calls it. Its public signatures take and return byte arrays
//! (and the facade's own result types) only; no other root2 item becomes public through it.
//! Nothing here signs or holds a key: a signature arrives from outside as 64 bytes.
mod context;
mod proof;

pub use context::{AuditDerived, ContextRefusal, VerifiedContext, verify_context_export};
pub use proof::{
	ProofRefusal, Unsigned, check_new_role_key, merge, prepare, prepare_statement, self_check,
	splice,
};

pub(in crate::storage::mutation_visibility::root2) use context::Chain3;
#[cfg(test)]
pub(in crate::storage::mutation_visibility::root2) use proof::check_under;

use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;

/// The export binds only a profile-3 manifest.
fn is_profile_three(manifest: &[u8]) -> bool {
	matches!(
		AnyAdminManifest2::decode(manifest),
		Ok(AnyAdminManifest2::Three(_))
	)
}
