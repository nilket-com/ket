pub(super) const BUNDLE_MAX: usize = 2 * 1024 * 1024;
pub(super) const DATA_MAX: usize = 20 * 1024 * 1024;
pub(super) const FRAME_MAX: usize = 16 * 1024 * 1024;
pub(super) const WITNESS_MAX: usize = 1024 * 1024;
pub(super) const JOURNAL_MAX: usize = 2 * 1024 * 1024;
pub(super) const OUTCOME_MAX: usize = 512 * 1024;
macro_rules! opaque {($($t:ty),* $(,)?)=>{$(impl std::fmt::Debug for $t{fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.write_str(concat!(stringify!($t)," { unverified; redacted }"))}})*};}
pub(super) use opaque;

use super::*;
use aws_lc_rs::digest::{SHA256, digest};
pub(super) fn sha(b: &[u8]) -> Digest2 {
	digest(&SHA256, b)
		.as_ref()
		.try_into()
		.expect("SHA256 width")
}
pub(super) fn wrap(m: &[u8; 8], b: &[u8], cap: usize) -> Result2<Vec<u8>> {
	if b.len().checked_add(48).ok_or(RootError2::Overflow)? > cap {
		return Err(RootError2::Limit);
	}
	super::super::codec::envelope(m, b, cap - 48)
}
pub(super) fn open<'a>(b: &'a [u8], m: &[u8; 8], cap: usize) -> Result2<Read2<'a>> {
	if b.len() > cap {
		return Err(RootError2::Limit);
	}
	Ok(Read2::new(super::super::codec::open(b, m, cap - 48)?))
}
pub(super) fn identity(r: &mut Read2<'_>) -> Result2<MutationIdentity> {
	MutationIdentity::decode(r.take(73)?).map_err(|_| RootError2::Codec)
}
pub(super) fn put_id(b: &mut Vec<u8>, id: MutationIdentity) -> Result2<()> {
	b.extend(id.encode().map_err(|_| RootError2::Codec)?);
	Ok(())
}
pub(super) fn check(ok: bool) -> Result2<()> {
	if ok { Ok(()) } else { Err(RootError2::Codec) }
}
pub(super) fn nonzero16(b: [u8; 16]) -> Result2<()> {
	check(b != [0; 16])
}
pub(super) fn count16(r: &mut Read2<'_>, cap: usize, minimum: usize) -> Result2<usize> {
	let n = r.u16()? as usize;
	if n > cap {
		return Err(RootError2::Limit);
	}
	if n.checked_mul(minimum).ok_or(RootError2::Overflow)? > r.remaining().len() {
		return Err(RootError2::Codec);
	}
	Ok(n)
}
pub(super) fn check_claim(c: &ClaimProjection2) -> Result2<()> {
	c.encode()?;
	Ok(())
}
pub(super) fn claim_identity(c: &ClaimProjection2) -> MutationIdentity {
	c.manifest.identity
}
