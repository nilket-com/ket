//! Private shared parent payload and ordered cuts; public records/bytes unchanged.
use super::{codec::*, persistent::Map, types::*};
use std::sync::Arc;
pub(super) trait ParentView2 {
	fn parts(&self) -> (&ClaimProjection2, Log2, u64, Option<Terminal2>);
}
impl ParentView2 for ParentRecord2 {
	fn parts(&self) -> (&ClaimProjection2, Log2, u64, Option<Terminal2>) {
		(
			&self.claim,
			self.reservation,
			self.held_bytes,
			self.terminal,
		)
	}
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct StoredParent2 {
	pub claim: Arc<ClaimProjection2>,
	pub reservation: Log2,
	pub held_bytes: u64,
	pub terminal: Option<Terminal2>,
}
impl From<ParentRecord2> for StoredParent2 {
	fn from(p: ParentRecord2) -> Self {
		Self {
			claim: Arc::new(p.claim),
			reservation: p.reservation,
			held_bytes: p.held_bytes,
			terminal: p.terminal,
		}
	}
}
impl StoredParent2 {
	pub fn record(&self) -> ParentRecord2 {
		ParentRecord2 {
			claim: (*self.claim).clone(),
			reservation: self.reservation,
			held_bytes: self.held_bytes,
			terminal: self.terminal,
		}
	}
}
impl ParentView2 for StoredParent2 {
	fn parts(&self) -> (&ClaimProjection2, Log2, u64, Option<Terminal2>) {
		(
			&self.claim,
			self.reservation,
			self.held_bytes,
			self.terminal,
		)
	}
}
impl Wire2 for StoredParent2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		super::records::write_parent(self, b)
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		Ok(ParentRecord2::read(r)?.into())
	}
}
impl std::fmt::Debug for StoredParent2 {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("StoredParent2 { redacted }")
	}
}
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub(super) struct Cuts2(Map<u64, EpochCut2>);
impl Cuts2 {
	pub fn len(&self) -> usize {
		self.0.len()
	}
	pub fn iter(&self) -> impl Iterator<Item = &EpochCut2> {
		self.0.values()
	}
	pub fn last(&self) -> Option<&EpochCut2> {
		self.0.last_key_value().map(|(_, v)| v)
	}
	pub fn at(&self, k: u64) -> Option<&EpochCut2> {
		self.0.upper_bound(&k).map(|(_, v)| v)
	}
	pub fn push(&mut self, c: EpochCut2) {
		self.0.insert(c.log.index, c);
	}
	pub fn from_vec(v: Vec<EpochCut2>) -> Result2<Self> {
		let mut out = Self::default();
		let mut prev = None;
		for c in v {
			if prev.is_some_and(|k| k >= c.log.index) {
				return Err(RootError2::RootInvariant);
			}
			prev = Some(c.log.index);
			out.push(c)
		}
		Ok(out)
	}
}
impl<'a> IntoIterator for &'a Cuts2 {
	type Item = &'a EpochCut2;
	type IntoIter = std::iter::Map<
		super::persistent::Iter<'a, u64, EpochCut2>,
		fn((&'a u64, &'a EpochCut2)) -> &'a EpochCut2,
	>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.iter().map(|(_, v)| v)
	}
}
#[cfg(test)]
impl std::ops::Index<usize> for Cuts2 {
	type Output = EpochCut2;
	fn index(&self, i: usize) -> &EpochCut2 {
		self.iter().nth(i).unwrap()
	}
}
