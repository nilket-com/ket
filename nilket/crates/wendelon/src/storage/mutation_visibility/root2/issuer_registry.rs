//! Dormant lease-issuer lifecycle. Verification deliberately refuses these commands until S3.
use crate::storage::mutation_visibility::root2::{
	admin::key::check_enrollable_admin_key2,
	codec::{Read2, Wire2, log, optional_log},
	command::ApplyResult2,
	types::{Digest2, IssuerRegistration2, Limits2, Log2, Result2, RootError2, State2},
};
use std::collections::BTreeSet;

pub(super) fn valid_node(node: u32) -> Result2<()> {
	if node == 0 {
		Err(RootError2::Codec)
	} else {
		Ok(())
	}
}
impl Wire2 for IssuerRegistration2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_node(self.node)?;
		if self.revoked.is_some_and(|l| l.index <= self.enrolled.index) {
			return Err(RootError2::Codec);
		}
		b.extend(self.node.to_le_bytes());
		b.extend(self.key);
		log(b, self.enrolled)?;
		b.push(u8::from(self.revoked.is_some()));
		optional_log(b, self.revoked)
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let node = r.u32()?;
		valid_node(node)?;
		let key = r.array()?;
		let enrolled = r.log()?;
		let flag = r.u8()?;
		let revoked = r.optional_log()?;
		if flag > 1
			|| (flag == 1) != revoked.is_some()
			|| revoked.is_some_and(|l| l.index <= enrolled.index)
		{
			return Err(RootError2::Codec);
		}
		Ok(Self {
			node,
			key,
			enrolled,
			revoked,
		})
	}
}
impl IssuerRegistration2 {
	pub fn encode(&self) -> Result2<Vec<u8>> {
		self.bytes()
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		Self::from_bytes(bytes, 77)
	}
}
fn denied(reason: u16, reference: Option<Log2>) -> ApplyResult2 {
	ApplyResult2::Denied { reason, reference }
}
pub(super) fn enroll(
	s: &mut State2,
	limits: Limits2,
	node: u32,
	key: Digest2,
	l: Log2,
) -> Result2<ApplyResult2> {
	valid_node(node)?;
	// Invalid command keys are consumed denials, including on occupied/revoked nodes.
	if check_enrollable_admin_key2(&key).is_err() {
		return Ok(denied(9, None));
	}
	let k = node.to_be_bytes();
	if let Some(a) = s.issuers.get(&k) {
		return Ok(if a.key == key && a.revoked.is_none() {
			ApplyResult2::Registered(a.enrolled)
		} else {
			denied(2, Some(a.enrolled))
		});
	}
	if s.issuers.len() >= limits.registrations
		|| s.derived
			.usage
			.registry
			.checked_add(81)
			.ok_or(RootError2::Overflow)?
			> limits.registry
	{
		return Ok(denied(11, None));
	}
	s.issuers.insert(
		k,
		IssuerRegistration2 {
			node,
			key,
			enrolled: l,
			revoked: None,
		},
	);
	Ok(ApplyResult2::Registered(l))
}
pub(super) fn revoke(s: &mut State2, node: u32, l: Log2) -> Result2<ApplyResult2> {
	valid_node(node)?;
	let k = node.to_be_bytes();
	let Some(a) = s.issuers.get(&k) else {
		return Ok(denied(9, None));
	};
	if let Some(first) = a.revoked {
		return Ok(ApplyResult2::Registered(first));
	}
	let a = s.issuers.get_mut(&k).ok_or(RootError2::RootInvariant)?;
	let first = *a.revoked.get_or_insert(l);
	Ok(ApplyResult2::Registered(first))
}
pub(super) fn validate(s: &State2, coords: &mut BTreeSet<u64>) -> Result2<()> {
	for (k, a) in &s.issuers {
		// Stored semantic faults are invariants; structural faults retain Codec mapping.
		if *k != a.node.to_be_bytes() {
			return Err(RootError2::RootInvariant);
		}
		a.bytes()?;
		if check_enrollable_admin_key2(&a.key).is_err() {
			return Err(RootError2::RootInvariant);
		}
		for l in [Some(a.enrolled), a.revoked].into_iter().flatten() {
			if l.index < s.bootstrap.index || l.index > s.covered.index || !coords.insert(l.index) {
				return Err(RootError2::RootInvariant);
			}
		}
	}
	Ok(())
}
