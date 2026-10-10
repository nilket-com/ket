use super::types::*;
use aws_lc_rs::digest::{SHA256, digest};

pub(super) fn hash(domain: &[u8], body: &[u8]) -> Digest2 {
	let mut ctx = aws_lc_rs::digest::Context::new(&SHA256);
	ctx.update(domain);
	ctx.update(body);
	ctx.finish().as_ref().try_into().expect("sha256 width")
}
pub(super) fn envelope(magic: &[u8; 8], body: &[u8], max: usize) -> Result2<Vec<u8>> {
	envelope_versioned(magic, 2, body, max)
}
pub(super) fn envelope_versioned(
	magic: &[u8; 8],
	version: u16,
	body: &[u8],
	max: usize,
) -> Result2<Vec<u8>> {
	if body.len() > max {
		return Err(RootError2::Limit);
	}
	let mut b = Vec::new();
	b.extend(magic);
	b.extend(version.to_le_bytes());
	b.extend([1, 0]);
	b.extend(
		u32::try_from(body.len())
			.map_err(|_| RootError2::Overflow)?
			.to_le_bytes(),
	);
	b.extend(body);
	let sum = digest(&SHA256, &b);
	b.extend(sum.as_ref());
	Ok(b)
}
pub(super) fn open<'a>(bytes: &'a [u8], magic: &[u8; 8], max: usize) -> Result2<&'a [u8]> {
	open_versioned(bytes, magic, 2, max)
}
pub(super) fn open_versioned<'a>(
	bytes: &'a [u8],
	magic: &[u8; 8],
	version: u16,
	max: usize,
) -> Result2<&'a [u8]> {
	if bytes.len() > max.checked_add(48).ok_or(RootError2::Overflow)? {
		return Err(RootError2::Limit);
	}
	let mut r = Read2::new(bytes);
	if r.take(8)? != magic || r.u16()? != version || r.u8()? != 1 || r.u8()? != 0 {
		return Err(RootError2::Codec);
	}
	let n = r.u32()? as usize;
	if n > max || n.checked_add(48) != Some(bytes.len()) {
		return Err(RootError2::Codec);
	}
	let b = r.take(n)?;
	let checksum = r.take(32)?;
	r.end()?;
	if digest(&SHA256, &bytes[..bytes.len() - 32]).as_ref() != checksum {
		return Err(RootError2::Codec);
	}
	Ok(b)
}
pub(super) struct Read2<'a> {
	bytes: &'a [u8],
	pos: usize,
}
impl<'a> Read2<'a> {
	pub(super) fn remaining(&self) -> &'a [u8] {
		&self.bytes[self.pos..]
	}
	pub fn new(bytes: &'a [u8]) -> Self {
		Self { bytes, pos: 0 }
	}
	pub fn take(&mut self, n: usize) -> Result2<&'a [u8]> {
		let end = self.pos.checked_add(n).ok_or(RootError2::Overflow)?;
		let s = self.bytes.get(self.pos..end).ok_or(RootError2::Codec)?;
		self.pos = end;
		Ok(s)
	}
	pub fn array<const N: usize>(&mut self) -> Result2<[u8; N]> {
		self.take(N)?.try_into().map_err(|_| RootError2::Codec)
	}
	pub fn u8(&mut self) -> Result2<u8> {
		Ok(self.array::<1>()?[0])
	}
	pub fn u16(&mut self) -> Result2<u16> {
		Ok(u16::from_le_bytes(self.array()?))
	}
	pub fn u32(&mut self) -> Result2<u32> {
		Ok(u32::from_le_bytes(self.array()?))
	}
	pub fn u64(&mut self) -> Result2<u64> {
		Ok(u64::from_le_bytes(self.array()?))
	}
	pub fn i64(&mut self) -> Result2<i64> {
		Ok(i64::from_le_bytes(self.array()?))
	}
	pub fn blob(&mut self, max: usize) -> Result2<&'a [u8]> {
		let n = self.u32()? as usize;
		if n > max {
			return Err(RootError2::Limit);
		}
		self.take(n)
	}
	pub fn count(&mut self, max: usize, minimum: usize) -> Result2<usize> {
		let n = self.u32()? as usize;
		if n > max {
			return Err(RootError2::Limit);
		}
		if n.checked_mul(minimum).ok_or(RootError2::Overflow)? > self.bytes.len() - self.pos {
			return Err(RootError2::Codec);
		}
		Ok(n)
	}
	pub fn end(self) -> Result2<()> {
		if self.pos == self.bytes.len() {
			Ok(())
		} else {
			Err(RootError2::Codec)
		}
	}
	pub fn log(&mut self) -> Result2<Log2> {
		let b = self.take(20)?;
		decode_log(b)?.ok_or(RootError2::Codec)
	}
	pub fn optional_log(&mut self) -> Result2<Option<Log2>> {
		decode_log(self.take(20)?)
	}
}
fn decode_log(b: &[u8]) -> Result2<Option<Log2>> {
	if b == [0u8; 20] {
		return Ok(None);
	}
	let mut r = Read2::new(b);
	let l = Log2 {
		term: r.u64()?,
		node: r.u32()?,
		index: r.u64()?,
	};
	valid_log(l)?;
	Ok(Some(l))
}
pub(super) fn valid_log(l: Log2) -> Result2<()> {
	if l.term == 0 || l.node == 0 || l.index == 0 {
		Err(RootError2::Codec)
	} else {
		Ok(())
	}
}
pub(super) fn log(b: &mut Vec<u8>, l: Log2) -> Result2<()> {
	valid_log(l)?;
	b.extend(l.term.to_le_bytes());
	b.extend(l.node.to_le_bytes());
	b.extend(l.index.to_le_bytes());
	Ok(())
}
pub(super) fn optional_log(b: &mut Vec<u8>, l: Option<Log2>) -> Result2<()> {
	match l {
		Some(x) => log(b, x),
		None => {
			b.extend([0; 20]);
			Ok(())
		}
	}
}
pub(super) fn blob(b: &mut Vec<u8>, x: &[u8]) -> Result2<()> {
	b.extend(
		u32::try_from(x.len())
			.map_err(|_| RootError2::Overflow)?
			.to_le_bytes(),
	);
	b.extend(x);
	Ok(())
}
pub(super) trait Wire2: Sized {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()>;
	fn read(r: &mut Read2<'_>) -> Result2<Self>;
	fn bytes(&self) -> Result2<Vec<u8>> {
		let mut b = Vec::new();
		self.write(&mut b)?;
		Ok(b)
	}
	fn from_bytes(b: &[u8], max: usize) -> Result2<Self> {
		if b.len() > max {
			return Err(RootError2::Limit);
		}
		let mut r = Read2::new(b);
		let v = Self::read(&mut r)?;
		r.end()?;
		Ok(v)
	}
}
pub(super) fn write_vec<T: Wire2>(
	b: &mut Vec<u8>,
	xs: impl Iterator<Item = T>,
	count: usize,
) -> Result2<()> {
	b.extend(
		u32::try_from(count)
			.map_err(|_| RootError2::Overflow)?
			.to_le_bytes(),
	);
	for x in xs {
		blob(b, &x.bytes()?)?
	}
	Ok(())
}
pub(super) fn read_vec<T: Wire2>(r: &mut Read2<'_>, count: usize, max: usize) -> Result2<Vec<T>> {
	let n = r.count(count, 4)?;
	let mut v = Vec::new();
	for _ in 0..n {
		v.push(T::from_bytes(r.blob(max)?, max)?)
	}
	Ok(v)
}

pub(super) fn read_binding2(r: &mut Read2<'_>) -> Result2<super::super::ProofKeyBinding> {
	let b = super::super::ProofKeyBinding {
		generation: r.array()?,
		tenant: r.u64()?,
		key_id: r.array()?,
		key_version: r.u32()?,
	};
	b.encode().map_err(|_| RootError2::Codec)?;
	Ok(b)
}
