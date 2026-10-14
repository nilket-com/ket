use super::*;
use crate::consensus::{WendelonRequest, WendelonTypeConfig};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use serde::de::{DeserializeSeed, IntoDeserializer};
use std::cell::Cell;

struct Hint<'a> {
	bytes: &'a [u8],
	hint: Option<usize>,
	reads: &'a Cell<usize>,
}
impl<'de> SeqAccess<'de> for Hint<'_> {
	type Error = serde::de::value::Error;
	fn next_element_seed<T: DeserializeSeed<'de>>(
		&mut self,
		seed: T,
	) -> Result<Option<T::Value>, Self::Error> {
		self.reads.set(self.reads.get() + 1);
		let Some((&byte, rest)) = self.bytes.split_first() else {
			return Ok(None);
		};
		self.bytes = rest;
		seed.deserialize(byte.into_deserializer()).map(Some)
	}
	fn size_hint(&self) -> Option<usize> {
		self.hint
	}
}
impl<'de> Deserializer<'de> for Hint<'_> {
	type Error = serde::de::value::Error;
	fn deserialize_any<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Self::Error> {
		v.visit_seq(self)
	}
	fn deserialize_seq<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Self::Error> {
		v.visit_seq(self)
	}
	serde::forward_to_deserialize_any! { bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf option unit unit_struct newtype_struct tuple tuple_struct map struct enum identifier ignored_any }
}
fn varint(mut n: u64) -> Vec<u8> {
	let mut b = Vec::new();
	loop {
		let low = (n & 127) as u8;
		n >>= 7;
		b.push(low | if n == 0 { 0 } else { 128 });
		if n == 0 {
			return b;
		}
	}
}
#[test]
fn carrier_minimum_maximum_and_canonical_vec_wire() {
	for (command, governance) in [
		(vec![], None),
		(
			vec![0x81; ROOT2_COMMAND_ENVELOPE_MAX1],
			Some(vec![0x62; ROOT2_GOVERNANCE_MAX1]),
		),
	] {
		let carrier = Root2Carrier1::new(&command, governance.as_deref()).unwrap();
		let bytes = postcard::to_stdvec(&carrier).unwrap();
		assert_eq!(
			bytes,
			postcard::to_stdvec(&(1u8, command.clone(), governance.clone())).unwrap()
		);
		assert_eq!(
			postcard::from_bytes::<Root2Carrier1>(&bytes).unwrap(),
			carrier
		);
		assert_eq!(carrier.version(), 1);
		assert_eq!(carrier.command(), command);
		assert_eq!(carrier.governance_bytes(), governance.as_deref());
	}
	// Arbitrary malformed command and short sidecar remain structural refusal inputs for V-S2.
	let carrier = Root2Carrier1::new(&[7], Some(&[8])).unwrap();
	assert_eq!(
		postcard::from_bytes::<Root2Carrier1>(&postcard::to_stdvec(&carrier).unwrap()).unwrap(),
		carrier
	);
}
fn probed<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> (postcard::Result<T>, Vec<ByteProbe1>) {
	VISITOR_PROBE1.with(|p| assert!(p.borrow_mut().replace(Vec::new()).is_none()));
	let result = postcard::from_bytes(bytes);
	let probes = VISITOR_PROBE1.with(|p| p.borrow_mut().take().unwrap());
	(result, probes)
}
fn truncated_probe(probe: &ByteProbe1, consumed: usize) {
	assert_eq!(probe.hint, None);
	assert_eq!(probe.reads, consumed);
	for &(reads, requested) in &probe.requests {
		assert!(requested <= probe.cap.min(2 * reads), "{probe:?}");
	}
	assert_eq!(probe.requests.first(), Some(&(0, 0)));
}
#[test]
fn carrier_checked_construction_and_declared_overbounds_fail_early() {
	const N: usize = ROOT2_COMMAND_ENVELOPE_MAX1;
	assert_eq!(
		Root2Carrier1::new(&vec![0; N + 1], None).err(),
		Some(Root2CarrierError1::Bound)
	);
	assert_eq!(
		Root2Carrier1::new(&[], Some(&vec![0; ROOT2_GOVERNANCE_MAX1 + 1])).err(),
		Some(Root2CarrierError1::Bound)
	);
	// Narrowed G-early: default postcard exposes the full declared hint only when
	// physical remaining bytes cover it. Full N+1 refuses before reserve/body read.
	let mut full = vec![1];
	full.extend(varint(N as u64 + 1));
	full.extend(vec![9; N + 1]);
	let (result, probe) = probed::<Root2Carrier1>(&full);
	assert_ne!(
		result.unwrap_err(),
		postcard::Error::DeserializeUnexpectedEnd
	);
	assert_eq!(probe.len(), 1);
	assert_eq!(probe[0].hint, Some(N + 1));
	assert_eq!(probe[0].reads, 0);
	assert!(probe[0].requests.is_empty());
	let mut full_sidecar = vec![1, 0, 1];
	full_sidecar.extend(varint(ROOT2_GOVERNANCE_MAX1 as u64 + 1));
	full_sidecar.extend(vec![9; ROOT2_GOVERNANCE_MAX1 + 1]);
	let (result, probe) = probed::<Root2Carrier1>(&full_sidecar);
	assert_ne!(
		result.unwrap_err(),
		postcard::Error::DeserializeUnexpectedEnd
	);
	assert_eq!(probe.len(), 2);
	assert_eq!(probe[1].hint, Some(ROOT2_GOVERNANCE_MAX1 + 1));
	assert_eq!(probe[1].reads, 0);
	assert!(probe[1].requests.is_empty());
	// G-truncated: hint is hidden; zero payload requests zero, while partial
	// payload follows capped geometric growth, <= min(N, 2*consumed), until EOF.
	for consumed in [0, 3, 257, 131073, N] {
		let mut bytes = vec![1];
		bytes.extend(varint(N as u64 + 1));
		bytes.extend(vec![9; consumed]);
		let (result, probe) = probed::<Root2Carrier1>(&bytes);
		assert_eq!(
			result.unwrap_err(),
			postcard::Error::DeserializeUnexpectedEnd
		);
		assert_eq!(probe.len(), 1);
		truncated_probe(&probe[0], consumed);
		if consumed == 0 {
			assert_eq!(probe[0].requests, vec![(0, 0)]);
		}
		if consumed >= 257 {
			assert!(probe[0].requests.len() < consumed);
		} // no per-byte reserve
	}
	// Hostile declared length does not imply EOF when N+1 physical bytes exist.
	for consumed in [0, N + 1] {
		let mut bytes = vec![1];
		bytes.extend(varint(u64::MAX));
		bytes.extend(vec![9; consumed]);
		let (result, probe) = probed::<Root2Carrier1>(&bytes);
		if usize::BITS == 64 {
			if consumed == 0 {
				assert_eq!(
					result.unwrap_err(),
					postcard::Error::DeserializeUnexpectedEnd
				);
			} else {
				assert_ne!(
					result.unwrap_err(),
					postcard::Error::DeserializeUnexpectedEnd
				);
			}
			assert_eq!(probe.len(), 1);
			truncated_probe(&probe[0], consumed);
			if consumed > N {
				assert!(probe[0].requests.iter().all(|&(reads, _)| reads <= N));
			} // no new requested capacity for the N+1 byte
		} else {
			assert_eq!(result.unwrap_err(), postcard::Error::DeserializeBadVarint);
			assert!(probe.is_empty());
		}
	}
	// Default serde exact maximum remains accepted with canonical Vec wire.
	let accepted = Root2Carrier1::new(&vec![9; N], None).unwrap();
	let (result, probe) = probed::<Root2Carrier1>(&postcard::to_stdvec(&accepted).unwrap());
	assert_eq!(result.unwrap(), accepted);
	assert_eq!(probe[0].hint, Some(N));
	assert_eq!(probe[0].reads, N);
	assert_eq!(probe[0].requests, vec![(0, N)]);
	let mut sidecar = vec![1, 0, 1];
	sidecar.extend(varint(ROOT2_GOVERNANCE_MAX1 as u64 + 1));
	let (result, probe) = probed::<Root2Carrier1>(&sidecar);
	assert_eq!(
		result.unwrap_err(),
		postcard::Error::DeserializeUnexpectedEnd
	);
	assert_eq!(probe[1].reads, 0);
	assert_eq!(probe[1].requests, vec![(0, 0)]);
	for (bytes, error) in [
		(vec![1, 128], postcard::Error::DeserializeUnexpectedEnd),
		(
			vec![1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 2],
			postcard::Error::DeserializeBadVarint,
		),
	] {
		let (result, probe) = probed::<Root2Carrier1>(&bytes);
		assert_eq!(result.unwrap_err(), error);
		assert!(probe.is_empty());
	}
	let (result, probe) = probed::<Root2Carrier1>(&[2]);
	assert_ne!(
		result.unwrap_err(),
		postcard::Error::DeserializeUnexpectedEnd
	);
	assert!(probe.is_empty());
}
#[test]
fn bounded_sequence_absent_and_misleading_hints_never_grow_past_cap() {
	fn check<const N: usize>() {
		let data = vec![9; N];
		for hint in [None, Some(0), Some(1), Some(N / 2), Some(N)] {
			let reads = Cell::new(0);
			let value = Root2Bytes::<N>::deserialize(Hint {
				bytes: &data,
				hint,
				reads: &reads,
			})
			.unwrap();
			assert_eq!(value.0.len(), N);
			assert!(value.0.capacity() <= N);
			let over = vec![9; N + 1];
			let reads = Cell::new(0);
			assert!(
				Root2Bytes::<N>::deserialize(Hint {
					bytes: &over,
					hint,
					reads: &reads
				})
				.is_err()
			);
			assert_eq!(reads.get(), N + 1);
		}
		// An overreported hint refuses without reading even if the actual payload is empty.
		let reads = Cell::new(0);
		assert!(
			Root2Bytes::<N>::deserialize(Hint {
				bytes: &[],
				hint: Some(N + 1),
				reads: &reads
			})
			.is_err()
		);
		assert_eq!(reads.get(), 0);
	}
	check::<296>();
	check::<262192>();
	let reads = Cell::new(0);
	assert!(
		Root2Bytes::<0>::deserialize(Hint {
			bytes: &[],
			hint: None,
			reads: &reads
		})
		.unwrap()
		.0
		.is_empty()
	);
}
#[test]
fn carrier_whole_normal_entry_maximum_is_exact() {
	let carrier = Root2Carrier1::new(
		&vec![1; ROOT2_COMMAND_ENVELOPE_MAX1],
		Some(&vec![2; ROOT2_GOVERNANCE_MAX1]),
	)
	.unwrap();
	let entry = Entry::<WendelonTypeConfig> {
		log_id: LogId::new(CommittedLeaderId::new(u64::MAX, u32::MAX), u64::MAX),
		payload: EntryPayload::Normal(WendelonRequest::Root2Command(carrier)),
	};
	let bytes = postcard::to_stdvec(&entry).unwrap();
	assert_eq!(bytes.len(), ROOT2_ENTRY_MAX1);
	assert_eq!(
		postcard::experimental::serialized_size(&entry).unwrap(),
		ROOT2_ENTRY_MAX1
	);
	let decoded = postcard::from_bytes::<Entry<WendelonTypeConfig>>(&bytes).unwrap();
	assert_eq!(postcard::to_stdvec(&decoded).unwrap(), bytes);
}

#[test]
fn carrier_independent_small_wire_literal() {
	let carrier = Root2Carrier1::new(&[1, 2, 3], Some(&[4, 5])).unwrap();
	let entry = Entry::<WendelonTypeConfig> {
		log_id: LogId::new(CommittedLeaderId::new(3, 1), 11),
		payload: EntryPayload::Normal(WendelonRequest::Root2Command(carrier)),
	};
	assert_eq!(
		postcard::to_stdvec(&entry).unwrap(),
		vec![3, 1, 11, 1, 26, 1, 3, 1, 2, 3, 1, 2, 4, 5]
	);
	let reads = Cell::new(0);
	let value = Root2Bytes::<296>::deserialize(Hint {
		bytes: &[8],
		hint: Some(296),
		reads: &reads,
	})
	.unwrap();
	assert_eq!(value.0, vec![8]);
	assert!(value.0.capacity() <= 296);
}
