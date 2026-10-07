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
// Parsed cuts retain original order/duplicates until full validation establishes precedence.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) enum Cuts2 {
	Ordered(Map<u64, EpochCut2>),
	Parsed(Vec<EpochCut2>),
}
impl Default for Cuts2 {
	fn default() -> Self {
		Self::Ordered(Map::new())
	}
}
pub(super) enum CutIter<'a> {
	Ordered(super::persistent::Iter<'a, u64, EpochCut2>),
	Parsed(std::slice::Iter<'a, EpochCut2>),
}
impl<'a> Iterator for CutIter<'a> {
	type Item = &'a EpochCut2;
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::Ordered(i) => i.next().map(|(_, v)| v),
			Self::Parsed(i) => i.next(),
		}
	}
}
impl Cuts2 {
	pub fn len(&self) -> usize {
		match self {
			Self::Ordered(m) => m.len(),
			Self::Parsed(v) => v.len(),
		}
	}
	pub fn iter(&self) -> CutIter<'_> {
		match self {
			Self::Ordered(m) => CutIter::Ordered(m.iter()),
			Self::Parsed(v) => CutIter::Parsed(v.iter()),
		}
	}
	pub fn last(&self) -> Option<&EpochCut2> {
		match self {
			Self::Ordered(m) => m.last_key_value().map(|(_, v)| v),
			Self::Parsed(v) => v.last(),
		}
	}
	pub fn at(&self, k: u64) -> Option<&EpochCut2> {
		match self {
			Self::Ordered(m) => m.upper_bound(&k).map(|(_, v)| v),
			Self::Parsed(v) => v.iter().filter(|c| c.log.index <= k).last(),
		}
	}
	pub fn push(&mut self, c: EpochCut2) {
		match self {
			Self::Ordered(m) => {
				m.insert(c.log.index, c);
			}
			Self::Parsed(v) => v.push(c),
		}
	}
	pub fn from_parsed(v: Vec<EpochCut2>) -> Self {
		Self::Parsed(v)
	}
	pub fn seal_validated(&mut self) {
		if let Self::Parsed(v) = self {
			let mut m = Map::new();
			for c in std::mem::take(v) {
				m.insert(c.log.index, c);
			}
			*self = Self::Ordered(m);
		}
	}
}
impl<'a> IntoIterator for &'a Cuts2 {
	type Item = &'a EpochCut2;
	type IntoIter = CutIter<'a>;
	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}
#[cfg(test)]
impl std::ops::Index<usize> for Cuts2 {
	type Output = EpochCut2;
	fn index(&self, i: usize) -> &EpochCut2 {
		self.iter().nth(i).unwrap()
	}
}
