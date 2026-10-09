#[cfg(test)]
use super::command::Command2;
use super::entry_identity::EntryPosition2;
use super::origin::EntryClass2;
use super::types::*;
/// No decoder/public constructor: future committed-entry adapter is deliberately absent.
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
/// classification. No decoder/public constructor: the committed-entry producer is held.
pub struct CommittedEntry2 {
	position: EntryPosition2,
	identity: Digest2,
	class: EntryClass2,
}
impl TrustedEntry2 {
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
	#[cfg(test)]
	pub(super) fn fixture(position: EntryPosition2, identity: Digest2, class: EntryClass2) -> Self {
		Self {
			position,
			identity,
			class,
		}
	}
}
