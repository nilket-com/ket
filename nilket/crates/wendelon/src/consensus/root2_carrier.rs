//! Durable bytes only. V-S1 holds every carrier; no Root2 command is verified or applied here.
use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{Error, SeqAccess, Visitor},
	ser::SerializeSeq,
};
use std::fmt;

pub const ROOT2_COMMAND_ENVELOPE_MAX1: usize = 262192;
pub const ROOT2_GOVERNANCE_MAX1: usize = 296;
// Maximal log id varints (10+5+10), EntryPayload tag1, request tag1, version1,
// command length3+bytes262192, option1, governance length2+bytes296.
pub const ROOT2_ENTRY_MAX1: usize = 262522;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Root2CarrierError1 {
	Bound,
	Allocation,
}

#[derive(Clone, PartialEq, Eq)]
struct Root2Bytes<const N: usize>(Vec<u8>);
impl<const N: usize> TryFrom<&[u8]> for Root2Bytes<N> {
	type Error = Root2CarrierError1;
	fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
		if bytes.len() > N {
			return Err(Root2CarrierError1::Bound);
		}
		let mut b = Vec::new();
		b.try_reserve_exact(bytes.len())
			.map_err(|_| Root2CarrierError1::Allocation)?;
		b.extend_from_slice(bytes);
		Ok(Self(b))
	}
}
impl<const N: usize> Serialize for Root2Bytes<N> {
	fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		let mut seq = s.serialize_seq(Some(self.0.len()))?;
		for byte in &self.0 {
			seq.serialize_element(byte)?;
		}
		seq.end()
	}
}
impl<'de, const N: usize> Deserialize<'de> for Root2Bytes<N> {
	fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
		struct BytesVisitor<const N: usize>;
		impl<'de, const N: usize> Visitor<'de> for BytesVisitor<N> {
			type Value = Root2Bytes<N>;
			fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
				write!(f, "at most {N} bytes")
			}
			fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
				let hint = seq.size_hint();
				#[cfg(test)]
				probe_leaf(N, hint);
				if hint.is_some_and(|n| n > N) {
					return Err(A::Error::custom("Root2 byte bound"));
				}
				let mut bytes = Vec::new();
				let initial = hint.unwrap_or(0).min(N);
				#[cfg(test)]
				probe_capacity(initial);
				bytes.try_reserve_exact(initial).map_err(A::Error::custom)?;
				while let Some(byte) = seq.next_element::<u8>()? {
					#[cfg(test)]
					probe_read();
					if bytes.len() == N {
						return Err(A::Error::custom("Root2 byte bound"));
					}
					if bytes.len() == bytes.capacity() {
						// Vec::push's default geometric growth can exceed N. Clamp each
						// requested capacity and refuse N+1 before any growth attempt.
						let capacity = bytes.capacity().saturating_mul(2).max(1).min(N);
						let additional = capacity - bytes.len();
						#[cfg(test)]
						probe_capacity(capacity);
						bytes
							.try_reserve_exact(additional)
							.map_err(A::Error::custom)?;
					}
					bytes.push(byte);
				}
				Ok(Root2Bytes(bytes))
			}
		}
		d.deserialize_seq(BytesVisitor::<N>)
	}
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
struct Version1(u8);
impl<'de> Deserialize<'de> for Version1 {
	fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
		let version = u8::deserialize(d)?;
		if version != 1 {
			return Err(D::Error::custom("Root2 carrier version"));
		}
		Ok(Self(version))
	}
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Root2Carrier1 {
	version: Version1,
	command: Root2Bytes<ROOT2_COMMAND_ENVELOPE_MAX1>,
	governance: Option<Root2Bytes<ROOT2_GOVERNANCE_MAX1>>,
}
impl Root2Carrier1 {
	pub fn new(command: &[u8], governance: Option<&[u8]>) -> Result<Self, Root2CarrierError1> {
		Ok(Self {
			version: Version1(1),
			command: Root2Bytes::try_from(command)?,
			governance: governance.map(Root2Bytes::try_from).transpose()?,
		})
	}
	pub fn version(&self) -> u8 {
		self.version.0
	}
	pub fn command(&self) -> &[u8] {
		&self.command.0
	}
	pub fn governance_bytes(&self) -> Option<&[u8]> {
		self.governance.as_ref().map(|b| b.0.as_slice())
	}
}
impl fmt::Debug for Root2Carrier1 {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Root2Carrier1 { redacted }")
	}
}
// Observes only the requested carrier byte-buffer capacity and successful body reads.
// Test-only/thread-scoped; no allocator/RSS observation or production mutable state.
#[cfg(test)]
#[derive(Debug)]
struct ByteProbe1 {
	cap: usize,
	hint: Option<usize>,
	reads: usize,
	requests: Vec<(usize, usize)>, // successful body reads, requested total capacity
}
#[cfg(test)]
thread_local! {
	static VISITOR_PROBE1: std::cell::RefCell<Option<Vec<ByteProbe1>>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn probe_leaf(cap: usize, hint: Option<usize>) {
	VISITOR_PROBE1.with(|p| {
		if let Some(leaves) = p.borrow_mut().as_mut() {
			leaves.push(ByteProbe1 {
				cap,
				hint,
				reads: 0,
				requests: Vec::new(),
			});
		}
	});
}
#[cfg(test)]
fn probe_capacity(target: usize) {
	VISITOR_PROBE1.with(|p| {
		if let Some(leaf) = p.borrow_mut().as_mut().and_then(|leaves| leaves.last_mut()) {
			leaf.requests.push((leaf.reads, target));
		}
	});
}
#[cfg(test)]
fn probe_read() {
	VISITOR_PROBE1.with(|p| {
		if let Some(leaf) = p.borrow_mut().as_mut().and_then(|leaves| leaves.last_mut()) {
			leaf.reads += 1;
		}
	});
}
#[cfg(test)]
mod tests;
