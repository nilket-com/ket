use super::committed::CommittedSource2;
use super::entry_identity::EntryPosition2;
use super::origin::EntryClass2;
use super::types::*;
use crate::storage::mutation_visibility::root2::command::Command2;
/// No public constructor. Dormant replay binds entries through one guarded internal caller.
pub struct TrustedEntry2 {
	generation: [u8; 16],
	cluster: [u8; 16],
	log: Log2,
	fingerprint: Digest2,
	command: Digest2,
}
/// No decoder/public constructor: fake verification exists only in cfg(test).
pub struct VerifiedCommand2 {
	generation: [u8; 16],
	cluster: [u8; 16],
	command: Digest2,
	authorized: bool,
	role_seed: Option<super::role_epochs::Seed3>,
	historical: Option<Digest2>,
	evidence: u8,
}
/// A view is not a naked caller index. Future lineage/barrier adapter is held.
pub struct TrustedView2 {
	generation: [u8; 16],
	cluster: [u8; 16],
	bootstrap: Log2,
	index: u64,
}
/// Any committed entry, genesis (0,0,0) included, with its version-1 identity and producer
/// classification. No decoder. Its one production factory requires a `CommittedSource2`, which
/// dormant replay alone can mint; the committed-entry adapter is its only caller.
pub struct CommittedEntry2 {
	position: EntryPosition2,
	identity: Digest2,
	class: EntryClass2,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BindError2 {
	NotRoot2Command,
	DigestMismatch,
	Position,
}
impl TrustedEntry2 {
	pub(super) fn bind(
		committed: &CommittedEntry2,
		command: &Command2,
		generation: [u8; 16],
		cluster: [u8; 16],
	) -> Result<Self, BindError2> {
		let (p, fingerprint, class) = committed.entry_fields();
		let digest = match class {
			EntryClass2::Bootstrap(d) | EntryClass2::OtherRoot2(d) => d,
			_ => return Err(BindError2::NotRoot2Command),
		};
		if command
			.digest(generation, cluster)
			.map_err(|_| BindError2::DigestMismatch)?
			!= digest
		{
			return Err(BindError2::DigestMismatch);
		}
		if p.term == 0 || p.node == 0 || p.index == 0 {
			return Err(BindError2::Position);
		}
		Ok(Self {
			generation,
			cluster,
			log: Log2 {
				term: p.term,
				node: p.node,
				index: p.index,
			},
			fingerprint,
			command: digest,
		})
	}
	pub(super) fn fields(&self) -> ([u8; 16], [u8; 16], Log2, Digest2, Digest2) {
		(
			self.generation,
			self.cluster,
			self.log,
			self.fingerprint,
			self.command,
		)
	}
	#[cfg(test)]
	pub(super) fn fixture(
		g: [u8; 16],
		c: [u8; 16],
		log: Log2,
		command: &Command2,
	) -> Result2<Self> {
		let hash = command.digest(g, c)?;
		Ok(Self {
			generation: g,
			cluster: c,
			log,
			fingerprint: super::codec::hash(b"TEST-S1-ENTRY\0", &command.payload(g, c)?),
			command: hash,
		})
	}
	#[cfg(test)]
	pub(super) fn change_fingerprint(&mut self) {
		self.fingerprint[0] ^= 1;
	}
}
impl VerifiedCommand2 {
	pub(super) fn role_seed3(&self) -> Option<super::role_epochs::Seed3> {
		self.role_seed
	}
	pub(super) fn matches(&self, g: [u8; 16], c: [u8; 16], command: Digest2) -> bool {
		(self.generation, self.cluster, self.command) == (g, c, command)
	}
	pub(super) fn authorized(&self) -> bool {
		self.authorized
	}
	pub(super) fn historical(&self) -> Option<Digest2> {
		self.historical
	}
	pub(super) fn evidence(&self) -> u8 {
		self.evidence
	}
	/// The one production constructor, called only by `verification::verify_command2` (guarded).
	/// Its arguments are that function's verdict, not an independently supported API.
	pub(super) fn verified2(
		generation: [u8; 16],
		cluster: [u8; 16],
		command: Digest2,
		authorized: bool,
		historical: Option<Digest2>,
		evidence: u8,
		role_seed: Option<super::role_epochs::Seed3>,
	) -> Self {
		Self {
			generation,
			cluster,
			command,
			authorized,
			role_seed,
			historical,
			evidence,
		}
	}
	/// Applied trace only, not a signature-only historical grant oracle.
	#[cfg(test)]
	pub(super) fn fixture(
		g: [u8; 16],
		c: [u8; 16],
		command: &Command2,
		applied_grants: &[OriginalGrant2],
		evidence: u8,
	) -> Result2<Self> {
		let historical = match command {
			Command2::Reserve(claim) if applied_grants.contains(&claim.original) => {
				Some(claim.original.reference()?.digest)
			}
			_ => None,
		};
		Ok(Self {
			generation: g,
			cluster: c,
			command: command.digest(g, c)?,
			authorized: true,
			role_seed: None,
			historical,
			evidence,
		})
	}
	#[cfg(test)]
	pub(super) fn unauthorized(mut self) -> Self {
		self.authorized = false;
		self
	}
}
impl TrustedView2 {
	pub(super) fn fields(&self) -> ([u8; 16], [u8; 16], Log2, u64) {
		(self.generation, self.cluster, self.bootstrap, self.index)
	}
	#[cfg(test)]
	pub(super) fn fixture(g: [u8; 16], c: [u8; 16], bootstrap: Log2, index: u64) -> Self {
		Self {
			generation: g,
			cluster: c,
			bootstrap,
			index,
		}
	}
}
impl CommittedEntry2 {
	pub(super) fn entry_fields(&self) -> (EntryPosition2, Digest2, EntryClass2) {
		(self.position, self.identity, self.class)
	}
	/// Called only by `committed_entry2`, which derives every field from a real entry.
	pub(super) fn from_source(
		_: &CommittedSource2,
		position: EntryPosition2,
		identity: Digest2,
		class: EntryClass2,
	) -> Self {
		Self {
			position,
			identity,
			class,
		}
	}
	#[cfg(test)]
	pub(super) fn fixture(position: EntryPosition2, identity: Digest2, class: EntryClass2) -> Self {
		Self {
			position,
			identity,
			class,
		}
	}
}
