use super::{
	admission_registry::{PERMISSIONS2, PermissionMap2},
	persistent::Map,
	stored::{Cuts2, StoredParent2},
};
use crate::storage::mutation_visibility::{DecisionLogId, ParentManifest, ProofKeyBinding};

pub type Log2 = DecisionLogId;
pub type Digest2 = [u8; 32];
pub const PARENT_SLOTS2: usize = 32768;
pub const PARENT_BYTES2: usize = 134217728;
pub const HISTORY_BYTES2: usize = 16777216;
pub const REGISTRY_BYTES2: usize = 8388608;
pub const ROOT_BYTES2: usize = 167772160;
pub const COMMAND_BYTES2: usize = 262144;
pub const CLAIM_BYTES2: usize = 131072;
pub const ATTESTATION_BYTES2: usize = 32768;
pub const LINEAGES2: usize = 16384;
pub const CUTS2: usize = 4096;
pub const REGISTRY_COUNT2: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootError2 {
	Codec,
	Limit,
	Overflow,
	TrustMismatch,
	LineageGap,
	RootInvariant,
}
pub type Result2<T> = Result<T, RootError2>;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hlc2 {
	pub physical_ms: u64,
	pub logical: u32,
	pub node: u32,
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeRef2 {
	pub node: u32,
	pub incarnation: [u8; 16],
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthorityRef2 {
	pub id: [u8; 16],
	pub revision: u64,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct GrantRef2 {
	pub lineage: [u8; 16],
	pub log: Log2,
	pub digest: Digest2,
}
#[derive(Clone, PartialEq, Eq)]
pub struct SignedLease2 {
	pub epoch: u64,
	pub holder: u32,
	pub range_start: u64,
	pub range_end: u64,
	pub start: Hlc2,
	pub expiry: Hlc2,
	pub min_commit_index: u64,
	pub issuer: u32,
	pub signature: [u8; 64],
}
#[derive(Clone, PartialEq, Eq)]
pub struct OriginalGrant2 {
	pub lineage: [u8; 16],
	pub incarnation: [u8; 16],
	pub lease: SignedLease2,
	pub log: Log2,
	pub predecessor: Option<GrantRef2>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Closure2 {
	pub cause: u8,
	pub log: Log2,
	pub successor: Option<[u8; 16]>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct LineageRecord2 {
	pub latest: OriginalGrant2,
	pub closure: Option<Closure2>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct ReplicaPlan2 {
	pub child: u32,
	pub nodes: Vec<NodeRef2>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct VersionStamp2 {
	pub child: u32,
	pub row: u32,
	pub hlc: Hlc2,
}
#[derive(Clone, PartialEq, Eq)]
pub struct ClaimProjection2 {
	pub manifest: ParentManifest,
	pub manifest_commitment: Digest2,
	pub original: OriginalGrant2,
	pub placement: Digest2,
	pub authority: AuthorityRef2,
	pub stamps: Vec<VersionStamp2>,
	pub plans: Vec<ReplicaPlan2>,
	pub accounting_delta: i64,
	pub claim_commitment: Digest2,
	pub signature: [u8; 64],
}
#[derive(Clone, PartialEq, Eq)]
pub struct ChildAck2 {
	pub child: u32,
	pub durable_binding: Digest2,
	pub nodes: Vec<NodeRef2>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct HolderAttestation2 {
	pub grant: GrantRef2,
	pub reservation: Log2,
	pub claim_digest: Digest2,
	pub authority: AuthorityRef2,
	pub children: Vec<ChildAck2>,
	pub signature: [u8; 64],
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Terminal2 {
	Accepted {
		first: Log2,
		evidence: Digest2,
	},
	Refused {
		first: Log2,
		cause: u8,
		anchor: Log2,
	},
}
impl Terminal2 {
	pub fn first(&self) -> Log2 {
		match self {
			Self::Accepted { first, .. } | Self::Refused { first, .. } => *first,
		}
	}
}
#[derive(Clone, PartialEq, Eq)]
pub struct ParentRecord2 {
	pub claim: ClaimProjection2,
	pub reservation: Log2,
	pub held_bytes: u64,
	pub terminal: Option<Terminal2>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct Selector2 {
	pub binding: ProofKeyBinding,
	pub enrolled: Log2,
}
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorityRegistration2 {
	pub authority: AuthorityRef2,
	pub node: NodeRef2,
	pub public_key: Digest2,
	pub enrolled: Log2,
	pub revoked: Option<Log2>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct EpochCut2 {
	pub prior: u64,
	pub new: u64,
	pub log: Log2,
	pub configuration: Digest2,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Usage2 {
	pub parents: usize,
	pub held: usize,
	pub history: usize,
	pub registry: usize,
}
#[derive(Clone, Copy)]
pub(super) struct Limits2 {
	pub slots: usize,
	pub parent: usize,
	pub history: usize,
	pub registry: usize,
	pub lineages: usize,
	pub cuts: usize,
	pub registrations: usize,
	pub permissions: usize,
}
impl Default for Limits2 {
	fn default() -> Self {
		Self {
			slots: PARENT_SLOTS2,
			parent: PARENT_BYTES2,
			history: HISTORY_BYTES2,
			registry: REGISTRY_BYTES2,
			lineages: LINEAGES2,
			cuts: CUTS2,
			registrations: REGISTRY_COUNT2,
			permissions: PERMISSIONS2,
		}
	}
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct State2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub bootstrap: Log2,
	pub covered: Log2,
	pub fingerprint: Digest2,
	pub command_digest: Digest2,
	pub initial_epoch: u64,
	pub control_digest: Digest2,
	pub control_len: u64,
	pub selectors: Map<[u8; 44], Selector2>,
	pub authorities: Map<[u8; 24], AuthorityRegistration2>,
	pub permissions: PermissionMap2,
	pub lineages: Map<[u8; 16], LineageRecord2>,
	pub cuts: Cuts2,
	pub parents: Map<[u8; 73], StoredParent2>,
	pub derived: super::incremental::Derived2,
}
macro_rules! opaque_debug { ($($t:ty),* $(,)?)=>{$(impl std::fmt::Debug for $t {fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {f.write_str(concat!(stringify!($t)," { redacted }"))}})*}; }
opaque_debug!(
	Hlc2,
	NodeRef2,
	AuthorityRef2,
	GrantRef2,
	SignedLease2,
	OriginalGrant2,
	Closure2,
	LineageRecord2,
	ReplicaPlan2,
	VersionStamp2,
	ClaimProjection2,
	ChildAck2,
	HolderAttestation2,
	Terminal2,
	ParentRecord2,
	Selector2,
	AuthorityRegistration2,
	EpochCut2,
	State2,
	Usage2
);
