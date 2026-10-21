//! The role-checker view: exactly the six fields the four role checkers read (the role-proof slot
//! loop, the two administrator statement checks and the VISADG01 governance check). Production
//! obtains one only from `InstalledAdmin2::view()`, a one-for-one copy; the offline operator facade
//! obtains one only from a re-verified context export (`RoleView3::offline`, over the reader's own
//! replayed chain). A view is input to the checkers and nothing more: it carries no cut, recovery
//! role or provenance, and no path turns it into a command token.
use super::admin::manifest::{
	AdminNamespace2, AnyAdminManifest2, GovernanceRole2, ManifestProfile2, any_manifest_digest2,
};
use super::admin_install::{
	AdminContext2, InstalledAdmin2, admin_active3, admin_context2, admin_governance2, admin_key2,
	admin_namespace2, admin_profile2,
};
use super::governance_command::GovernanceAction2;
use super::operator_proof::Chain3;
use super::role_command::{CONTEXT_BYTES3, context_from_bytes};
use super::types::Digest2;
use crate::storage::mutation_visibility::redacted_debug;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct RoleView3 {
	context: AdminContext2,
	profile: ManifestProfile2,
	admin_key: [u8; 32],
	namespace: AdminNamespace2,
	governance: Option<GovernanceRole2>,
	active: Option<[(u64, Digest2); 3]>,
	// Test builds only: a governance action mask other than the manifest's fixed one.
	#[cfg(test)]
	test_mask: Option<u32>,
}
redacted_debug!(RoleView3, "RoleView3");

impl InstalledAdmin2 {
	/// The six checker fields, copied one-for-one from the installed administrator.
	pub(super) fn view(&self) -> RoleView3 {
		RoleView3 {
			context: admin_context2(self),
			profile: admin_profile2(self),
			admin_key: admin_key2(self),
			namespace: admin_namespace2(self),
			governance: admin_governance2(self),
			active: admin_active3(self),
			#[cfg(test)]
			test_mask: None,
		}
	}
}

impl RoleView3 {
	/// The view of a profile-3 root an offline host holds: its 188-byte context, its trusted
	/// 241-byte manifest (whose pin the context must carry) and the successors a context-export
	/// reader replayed. Field for field what installation copies from the same manifest, with
	/// each role active at its last replayed successor, or at its original key.
	pub(super) fn offline(
		context: &[u8; CONTEXT_BYTES3],
		manifest: &[u8],
		chain: &Chain3,
	) -> Option<Self> {
		let Ok(AnyAdminManifest2::Three(m)) = AnyAdminManifest2::decode(manifest) else {
			return None;
		};
		let governance = m.governance();
		let any = AnyAdminManifest2::Three(m.clone());
		let context = context_from_bytes(context);
		if any_manifest_digest2(&any.encode().ok()?) != context.manifest_pin {
			return None;
		}
		let base = any.admission();
		let original = [base.admin_key, governance.key(), m.recovery().key()];
		let successors = chain.successors();
		Some(Self {
			context,
			profile: ManifestProfile2::Three,
			admin_key: base.admin_key,
			namespace: base.namespace,
			governance: Some(governance),
			active: Some(std::array::from_fn(|r| {
				successors[r]
					.last()
					.map_or((0, original[r]), |k| (successors[r].len() as u64, *k))
			})),
			#[cfg(test)]
			test_mask: None,
		})
	}
	pub(super) fn context(&self) -> AdminContext2 {
		self.context
	}
	pub(super) fn profile(&self) -> ManifestProfile2 {
		self.profile
	}
	pub(super) fn admin_key(&self) -> [u8; 32] {
		self.admin_key
	}
	pub(super) fn namespace(&self) -> AdminNamespace2 {
		self.namespace
	}
	/// The copied descriptive governance role; never a verifier on its own.
	pub(super) fn governance_role(&self) -> Option<GovernanceRole2> {
		self.governance
	}
	pub(super) fn active(&self, role: usize) -> Option<(u64, Digest2)> {
		self.active.and_then(|roles| roles.get(role).copied())
	}
	/// The governance action-bit predicate a `VISROL03` governance proof must also pass: the one
	/// copy, applied by `verify_governance2` and by the offline facade's self-check.
	pub(super) fn governs(&self, action: GovernanceAction2) -> bool {
		self.governance
			.is_some_and(|r| self.mask(r) & action.bit() != 0)
	}
	#[cfg(not(test))]
	fn mask(&self, role: GovernanceRole2) -> u32 {
		role.actions()
	}
	#[cfg(test)]
	fn mask(&self, role: GovernanceRole2) -> u32 {
		self.test_mask.unwrap_or(role.actions())
	}
}

// Test-only edits of one view field at a time, for the T-VIEW negative controls and the
// action-mask refusal a profile-3 manifest cannot express; never reachable through the facade.
#[cfg(test)]
impl RoleView3 {
	pub(super) fn with_admin_key(mut self, key: [u8; 32]) -> Self {
		self.admin_key = key;
		self
	}
	pub(super) fn with_active0(mut self, active: (u64, Digest2)) -> Self {
		if let Some(roles) = &mut self.active {
			roles[0] = active;
		}
		self
	}
	pub(super) fn with_namespace(mut self, namespace: AdminNamespace2) -> Self {
		self.namespace = namespace;
		self
	}
	pub(super) fn with_mask(mut self, mask: u32) -> Self {
		self.test_mask = Some(mask);
		self
	}
	pub(super) fn with_governance(mut self, governance: Option<GovernanceRole2>) -> Self {
		self.governance = governance;
		self
	}
}
