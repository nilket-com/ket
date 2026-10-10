use super::*;
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Credit {
	pub(super) data: u32,
	pub(super) index: u32,
	pub(super) journal: u32,
	pub(super) claim: u32,
	pub(super) children: u16,
	pub(super) versions: u16,
	pub(super) slots: u16,
	pub(super) response: u32,
	pub(super) charge: u64,
	pub(super) plan: Digest2,
	pub(super) role: u8,
	pub(super) local: NodeRef2,
}
impl Credit {
	pub(super) fn freeze(
		data: &UnverifiedData2,
		response: u32,
		role: u8,
		local: NodeRef2,
	) -> StoreResult<Self> {
		require(data.bundle.claims.len() == 1 && response > 0 && response <= 65536)?;
		let (claim, _) = &data.bundle.claims[0];
		let raw = claim.encode()?;
		let c = raw.len() as u64;
		let k = claim.manifest.children.len() as u64;
		let v = claim
			.manifest
			.children
			.iter()
			.map(|c| c.versions.len())
			.sum::<usize>() as u64;
		require(k > 0 && k <= 256 && v > 0 && v <= 512)?;
		require(
			crate::storage::mutation_visibility::root2::durable::loader::ipc_preflight(&data.frame)?
				as u64 == v,
		)?;
		require(local.node != 0 && local.incarnation != [0; 16])?;
		let n = match role {
			1 => {
				require(
					local.node == claim.original.lease.holder
						&& local.incarnation == claim.original.incarnation,
				)?;
				claim.plans.iter().map(|p| p.nodes.len()).sum::<usize>() as u64
			}
			2 => claim
				.plans
				.iter()
				.filter(|p| p.nodes.contains(&local))
				.count() as u64,
			_ => return Err(StoreError::Unavailable),
		};
		require(n > 0 && n <= 4090)?;
		let d = 117 + data.encode()?.len() as u64;
		let i = 117 + 303 * v;
		let j = Self::journal_bound(c, k, n, response as u64, role)?;
		let charge = 512 + d + i + j;
		Ok(Self {
			data: u32::try_from(d).map_err(|_| StoreError::Limit)?,
			index: i as u32,
			journal: j as u32,
			claim: c as u32,
			children: k as u16,
			versions: v as u16,
			slots: n as u16,
			response,
			charge,
			plan: sha(&raw),
			role,
			local,
		})
	}
	pub(super) fn journal_bound(c: u64, k: u64, n: u64, s: u64, role: u8) -> StoreResult<u64> {
		require(
			c > 0
				&& c <= CLAIM_BYTES2 as u64
				&& k > 0 && k <= 256
				&& n > 0 && n <= 4090
				&& s <= 65536,
		)?;
		let base = 269 + 254 + c + 640 * n;
		let j = match role {
			1 => {
				let h = 212 + 37 * k + 20 * n;
				let w = 78 + c + 390 * n + h;
				let r = c + 241;
				let o = 214 + r + s;
				require(h <= 32768 && w <= WITNESS_MAX as u64 && o <= OUTCOME_MAX as u64)?;
				require(
					254 + w <= JOURNAL_MAX as u64
						&& 274 + r <= JOURNAL_MAX as u64
						&& 254 + o <= JOURNAL_MAX as u64,
				)?;
				base + 254 + w + 274 + r + 254 + o + 302 + 250
			}
			2 => base,
			_ => return Err(StoreError::Unavailable),
		};
		require(j <= JOURNAL_CAP as u64)?;
		Ok(j)
	}
	pub(super) fn validate(&self) -> StoreResult<()> {
		require(
			self.response > 0
				&& self.slots > 0
				&& match self.role {
					1 => {
						self.slots >= self.children && self.slots as u32 <= 3 * self.children as u32
					}
					2 => self.slots <= self.children,
					_ => false,
				},
		)?;
		require(
			self.data > 117
				&& self.data as usize <= DATA_MAX + 117
				&& self.versions > 0
				&& self.versions <= 512,
		)?;
		require(self.index == 117 + 303 * self.versions as u32)?;
		let j = Self::journal_bound(
			self.claim as u64,
			self.children as u64,
			self.slots as u64,
			self.response as u64,
			self.role,
		)?;
		require(
			self.journal as u64 == j
				&& self.charge == 512 + self.data as u64 + self.index as u64 + j,
		)?;
		require(self.local.node != 0 && self.local.incarnation != [0; 16])
	}
	pub(super) fn matches(&self, data: &UnverifiedData2) -> StoreResult<()> {
		require(*self == Self::freeze(data, self.response, self.role, self.local)?)
	}
}
