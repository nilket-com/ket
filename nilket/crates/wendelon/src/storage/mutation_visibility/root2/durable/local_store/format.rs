use super::*;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct ContainerId {
	pub(super) generation: [u8; 16],
	pub(super) cluster: [u8; 16],
	pub(super) store: [u8; 16],
}
impl ContainerId {
	pub(super) fn write(self, b: &mut Vec<u8>) -> StoreResult<()> {
		require(self.generation != [0; 16] && self.cluster != [0; 16] && self.store != [0; 16])?;
		b.extend(self.generation);
		b.extend(self.cluster);
		b.extend(self.store);
		Ok(())
	}
	pub(super) fn read(r: &mut Read2<'_>) -> StoreResult<Self> {
		let id = Self {
			generation: r.array()?,
			cluster: r.array()?,
			store: r.array()?,
		};
		id.write(&mut Vec::new())?;
		Ok(id)
	}
	pub(super) fn owner(self, o: &JournalIdentity2) -> StoreResult<()> {
		require(o.generation == self.generation && o.cluster == self.cluster)?;
		o.bytes()?;
		Ok(())
	}
}
pub(super) fn envelope(m: &[u8; 8], payload: &[u8], cap: usize) -> StoreResult<Vec<u8>> {
	if payload.len().checked_add(48).is_none_or(|n| n > cap) {
		return Err(StoreError::Limit);
	}
	let mut b = Vec::with_capacity(payload.len() + 48);
	b.extend(m);
	b.extend(1u16.to_le_bytes());
	b.extend([1, 0]);
	b.extend(
		u32::try_from(payload.len())
			.map_err(|_| StoreError::Limit)?
			.to_le_bytes(),
	);
	b.extend(payload);
	b.extend(sha(&b));
	Ok(b)
}
pub(super) fn payload<'a>(b: &'a [u8], m: &[u8; 8], cap: usize) -> StoreResult<Read2<'a>> {
	if b.len() > cap {
		return Err(StoreError::Limit);
	}
	let mut r = Read2::new(b);
	require(r.take(8)? == m && r.u16()? == 1 && r.u8()? == 1 && r.u8()? == 0)?;
	let n = r.u32()? as usize;
	require(n.checked_add(48) == Some(b.len()))?;
	let body = r.take(n)?;
	let checksum = r.take(32)?;
	r.end()?;
	require(sha(&b[..b.len() - 32]) == checksum)?;
	Ok(Read2::new(body))
}
pub(super) fn owner(r: &mut Read2<'_>) -> StoreResult<JournalIdentity2> {
	let o = JournalIdentity2 {
		generation: r.array()?,
		cluster: r.array()?,
		identity: identity(r)?,
		claim: r.array()?,
		reservation: r.log()?,
	};
	o.bytes()?;
	Ok(o)
}
pub(super) fn hex(b: &[u8]) -> String {
	b.iter().map(|x| format!("{x:02x}")).collect()
}
pub(super) fn hex_octets<const N: usize>(name: &str) -> StoreResult<[u8; N]> {
	require(
		name.len() == 2 * N
			&& name
				.bytes()
				.all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
	)?;
	let mut b = [0; N];
	for (i, v) in b.iter_mut().enumerate() {
		*v =
			u8::from_str_radix(&name[2 * i..2 * i + 2], 16).map_err(|_| StoreError::Unavailable)?;
	}
	Ok(b)
}
pub(super) fn id_name(name: &str) -> StoreResult<[u8; 16]> {
	let id = hex_octets(name)?;
	require(id != [0; 16])?;
	Ok(id)
}
#[derive(Clone)]
pub(super) struct ParentEntry {
	pub(super) owner: JournalIdentity2,
	pub(super) data: [u8; 16],
	pub(super) index: [u8; 16],
	pub(super) credit: Credit,
	pub(super) observations: u8,
}
impl PartialEq for ParentEntry {
	fn eq(&self, other: &Self) -> bool {
		self.owner == other.owner
			&& self.data == other.data
			&& self.index == other.index
			&& self.credit == other.credit
	}
}
impl Eq for ParentEntry {}
impl ParentEntry {
	pub(super) fn bytes(&self) -> StoreResult<Vec<u8>> {
		self.credit.validate()?;
		require(self.data != [0; 16] && self.index != [0; 16] && self.data != self.index)?;
		let mut b = self.owner.bytes()?;
		b.extend(self.data);
		b.extend(self.index);
		let c = &self.credit;
		for n in [c.data, c.index, c.journal, c.claim] {
			b.extend(n.to_le_bytes())
		}
		for n in [c.children, c.versions, c.slots] {
			b.extend(n.to_le_bytes())
		}
		b.extend(c.response.to_le_bytes());
		b.extend(c.charge.to_le_bytes());
		b.extend(c.plan);
		b.push(c.role);
		b.extend(c.local.node.to_le_bytes());
		b.extend(c.local.incarnation);
		require(b.len() == 276)?;
		Ok(b)
	}
	pub(super) fn read(r: &mut Read2<'_>) -> StoreResult<Self> {
		let p = Self {
			observations: 0,
			owner: owner(r)?,
			data: r.array()?,
			index: r.array()?,
			credit: Credit {
				data: r.u32()?,
				index: r.u32()?,
				journal: r.u32()?,
				claim: r.u32()?,
				children: r.u16()?,
				versions: r.u16()?,
				slots: r.u16()?,
				response: r.u32()?,
				charge: r.u64()?,
				plan: r.array()?,
				role: r.u8()?,
				local: NodeRef2 {
					node: r.u32()?,
					incarnation: r.array()?,
				},
			},
		};
		p.bytes()?;
		Ok(p)
	}
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct GrantEntry {
	pub(super) digest: Digest2,
	pub(super) object: [u8; 16],
	pub(super) bytes: u32,
	pub(super) charge: u64,
}
impl GrantEntry {
	pub(super) fn bytes(&self) -> StoreResult<Vec<u8>> {
		require(self.object != [0; 16] && self.bytes == 655 && self.charge == 1167)?;
		let mut b = self.digest.to_vec();
		b.extend(self.object);
		b.extend(self.bytes.to_le_bytes());
		b.extend(self.charge.to_le_bytes());
		Ok(b)
	}
	pub(super) fn read(r: &mut Read2<'_>) -> StoreResult<Self> {
		let g = Self {
			digest: r.array()?,
			object: r.array()?,
			bytes: r.u32()?,
			charge: r.u64()?,
		};
		g.bytes()?;
		Ok(g)
	}
}
#[derive(Clone, PartialEq, Eq)]
pub(super) enum Allocation {
	Parent(ParentEntry),
	Grant(GrantEntry),
}
impl Allocation {
	pub(super) fn charge(&self) -> u64 {
		match self {
			Self::Parent(p) => p.credit.charge,
			Self::Grant(g) => g.charge,
		}
	}
	pub(super) fn encode(
		&self,
		id: ContainerId,
		seq: u64,
		previous: Digest2,
	) -> StoreResult<Vec<u8>> {
		require(seq > 0 && (seq == 1) == (previous == [0; 32]))?;
		let mut b = vec![match self {
			Self::Parent(_) => 1,
			Self::Grant(_) => 2,
		}];
		id.write(&mut b)?;
		b.extend(seq.to_le_bytes());
		b.extend(previous);
		match self {
			Self::Parent(p) => {
				id.owner(&p.owner)?;
				b.extend(p.bytes()?)
			}
			Self::Grant(g) => {
				b.extend(g.bytes()?);
				b.extend([0; 216])
			}
		}
		b.extend([0; 99]);
		envelope(b"VISALC01", &b, 512)
	}
	pub(super) fn decode(
		raw: &[u8],
		id: ContainerId,
		seq: u64,
		previous: Digest2,
	) -> StoreResult<Self> {
		require(raw.len() == 512)?;
		let mut r = payload(raw, b"VISALC01", 512)?;
		let tag = r.u8()?;
		require(
			ContainerId::read(&mut r)? == id && r.u64()? == seq && r.array::<32>()? == previous,
		)?;
		let a = match tag {
			1 => {
				let p = ParentEntry::read(&mut r)?;
				id.owner(&p.owner)?;
				Self::Parent(p)
			}
			2 => {
				let g = GrantEntry::read(&mut r)?;
				require(r.take(216)?.iter().all(|b| *b == 0))?;
				Self::Grant(g)
			}
			_ => return Err(StoreError::Unavailable),
		};
		require(r.take(99)?.iter().all(|b| *b == 0))?;
		r.end()?;
		a.encode(id, seq, previous)?;
		Ok(a)
	}
}
pub(super) fn segment_header(
	id: ContainerId,
	number: u64,
	first: u64,
	previous: Digest2,
) -> StoreResult<Vec<u8>> {
	require(
		number > 0
			&& number <= SEGMENTS as u64
			&& first == (number - 1) * SEGMENT_RECORDS + 1
			&& (first == 1) == (previous == [0; 32]),
	)?;
	let mut b = Vec::new();
	id.write(&mut b)?;
	b.extend(number.to_le_bytes());
	b.extend(first.to_le_bytes());
	b.extend(previous);
	envelope(b"VISALH01", &b, 144)
}
pub(super) fn check_segment(
	raw: &[u8],
	id: ContainerId,
	n: u64,
	seq: u64,
	prev: Digest2,
) -> StoreResult<()> {
	// Exact canonical comparison includes every field and checksum.
	require(raw == segment_header(id, n, seq, prev)?)
}
pub(super) fn init(id: ContainerId, empty: Digest2) -> StoreResult<Vec<u8>> {
	let mut b = Vec::new();
	id.write(&mut b)?;
	b.extend(0u64.to_le_bytes());
	b.extend(empty);
	envelope(b"VISINI01", &b, 136)
}
pub(super) fn check_init(raw: &[u8], id: ContainerId, empty: Digest2) -> StoreResult<()> {
	require(raw == init(id, empty)?)
}
pub(super) fn object(
	id: ContainerId,
	kind: u8,
	obj: [u8; 16],
	inner: &[u8],
) -> StoreResult<Vec<u8>> {
	require(obj != [0; 16])?;
	let cap = match kind {
		1 => DATA_MAX,
		2 => 303 * 512,
		3 => 538,
		_ => return Err(StoreError::Unavailable),
	};
	require(
		!inner.is_empty()
			&& inner.len() <= cap
			&& (kind != 2 || inner.len() % 303 == 0)
			&& (kind != 3 || inner.len() == 538),
	)?;
	let mut b = vec![kind];
	id.write(&mut b)?;
	b.extend(obj);
	b.extend((inner.len() as u32).to_le_bytes());
	b.extend(inner);
	envelope(b"VISOBJ01", &b, cap + 117)
}
pub(super) fn object_inner<'a>(
	raw: &'a [u8],
	id: ContainerId,
	kind: u8,
	obj: [u8; 16],
	cap: usize,
) -> StoreResult<&'a [u8]> {
	let mut r = payload(raw, b"VISOBJ01", cap)?;
	require(r.u8()? == kind && ContainerId::read(&mut r)? == id && r.array::<16>()? == obj)?;
	let n = r.u32()? as usize;
	let inner = r.take(n)?;
	r.end()?;
	require(raw == object(id, kind, obj, inner)?)?;
	Ok(inner)
}
pub(super) fn journal_header(id: ContainerId, o: &JournalIdentity2) -> StoreResult<Vec<u8>> {
	id.owner(o)?;
	let loc = o.locator()?;
	let obj = id_name(&loc[..32])?;
	let mut b = Vec::new();
	id.write(&mut b)?;
	b.extend(obj);
	b.extend(o.bytes()?);
	envelope(b"VISJHD01", &b, 269)
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Inventory {
	pub(super) parents: BTreeMap<Digest2, ParentEntry>,
	pub(super) grants: BTreeMap<Digest2, GrantEntry>,
	pub(super) objects: Vec<[u8; 16]>,
	pub(super) sequence: u64,
	pub(super) digest: Digest2,
	pub(super) charged: u64,
}
impl Default for Inventory {
	fn default() -> Self {
		Self {
			parents: BTreeMap::new(),
			grants: BTreeMap::new(),
			objects: Vec::with_capacity(2 * PARENTS + GRANTS),
			sequence: 0,
			digest: [0; 32],
			charged: 0,
		}
	}
}
impl Inventory {
	pub(super) fn insert_object(&mut self, id: [u8; 16]) {
		let pos = self.objects.binary_search(&id).unwrap_err();
		self.objects.insert(pos, id)
	}
	pub(super) fn summary_bytes(&self) -> usize {
		self.parents.len() * (std::mem::size_of::<(Digest2, ParentEntry)>() + 80)
			+ self.grants.len() * (std::mem::size_of::<(Digest2, GrantEntry)>() + 80)
			+ self.objects.capacity() * 16
	}

	pub(super) fn add(&mut self, a: Allocation, seq: u64, digest: Digest2) -> StoreResult<()> {
		require(seq == self.sequence + 1)?;
		let charge = a.charge();
		require(
			self.charged
				.checked_add(charge)
				.is_some_and(|q| q + HEADROOM <= QUOTA),
		)?;
		match a {
			Allocation::Parent(mut p) => {
				// Allocation metadata never imports volatile content/durability observations.
				p.observations = 0;
				if self.parents.len() >= PARENTS {
					return Err(StoreError::Limit);
				}
				let key = sha(&p.owner.bytes()?);
				require(
					!self.parents.contains_key(&key)
						&& self.objects.binary_search(&p.data).is_err()
						&& self.objects.binary_search(&p.index).is_err(),
				)?;
				self.insert_object(p.data);
				self.insert_object(p.index);
				self.parents.insert(key, p);
			}
			Allocation::Grant(g) => {
				if self.grants.len() >= GRANTS {
					return Err(StoreError::Limit);
				}
				require(
					!self.grants.contains_key(&g.digest)
						&& self.objects.binary_search(&g.object).is_err(),
				)?;
				self.insert_object(g.object);
				self.grants.insert(g.digest, g);
			}
		}
		if self.summary_bytes() > 16 * 1024 * 1024 {
			return Err(StoreError::Limit);
		}
		self.charged += charge;
		self.sequence = seq;
		self.digest = digest;
		Ok(())
	}
	pub(super) fn checkpoint(&self, id: ContainerId) -> StoreResult<Vec<u8>> {
		require(
			(self.sequence == 0) == (self.digest == [0; 32])
				&& self.sequence as usize == self.parents.len() + self.grants.len(),
		)?;
		let mut b = Vec::new();
		id.write(&mut b)?;
		b.extend(self.sequence.to_le_bytes());
		b.extend(self.digest);
		b.extend((self.parents.len() as u32).to_le_bytes());
		let mut parents = self.parents.values().collect::<Vec<_>>();
		parents.sort_by_cached_key(|p| p.owner.bytes().expect("validated owner"));
		for p in parents {
			id.owner(&p.owner)?;
			b.extend(p.bytes()?)
		}
		b.extend((self.grants.len() as u32).to_le_bytes());
		for g in self.grants.values() {
			b.extend(g.bytes()?)
		}
		envelope(b"VISCAT01", &b, CHECKPOINT_CAP)
	}
	pub(super) fn check_checkpoint(raw: &[u8], id: ContainerId) -> StoreResult<(u64, Digest2)> {
		let mut r = payload(raw, b"VISCAT01", CHECKPOINT_CAP)?;
		require(ContainerId::read(&mut r)? == id)?;
		let seq = r.u64()?;
		let digest = r.array()?;
		let n = r.count(PARENTS, 276)?;
		let mut inv = Self::default();
		let mut last = None;
		for _ in 0..n {
			let p = ParentEntry::read(&mut r)?;
			id.owner(&p.owner)?;
			let k = p.owner.bytes()?;
			require(last.as_ref().is_none_or(|old| old < &k))?;
			last = Some(k);
			let s = inv.sequence + 1;
			inv.add(Allocation::Parent(p), s, [1; 32])?;
		}
		let n = r.count(GRANTS, 60)?;
		let mut last = None;
		for _ in 0..n {
			let g = GrantEntry::read(&mut r)?;
			require(last.is_none_or(|old| old < g.digest))?;
			last = Some(g.digest);
			let s = inv.sequence + 1;
			inv.add(Allocation::Grant(g), s, [1; 32])?;
		}
		r.end()?;
		require(seq == inv.sequence && (seq == 0) == (digest == [0; 32]))?;
		Ok((seq, digest))
	}
}
