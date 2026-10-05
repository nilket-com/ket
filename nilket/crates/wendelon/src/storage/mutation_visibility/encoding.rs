use super::VisibilityError;

pub(super) struct Reader<'a> {
	bytes: &'a [u8],
	position: usize,
}
impl<'a> Reader<'a> {
	pub fn new(bytes: &'a [u8]) -> Self {
		Self { bytes, position: 0 }
	}
	pub fn take(&mut self, size: usize) -> Result<&'a [u8], VisibilityError> {
		let end = self
			.position
			.checked_add(size)
			.ok_or(VisibilityError::LimitExceeded)?;
		let value = self
			.bytes
			.get(self.position..end)
			.ok_or(VisibilityError::Truncated)?;
		self.position = end;
		Ok(value)
	}
	pub fn array<const N: usize>(&mut self) -> Result<[u8; N], VisibilityError> {
		self.take(N)?
			.try_into()
			.map_err(|_| VisibilityError::Truncated)
	}
	pub fn u8(&mut self) -> Result<u8, VisibilityError> {
		Ok(self.array::<1>()?[0])
	}
	pub fn u16(&mut self) -> Result<u16, VisibilityError> {
		Ok(u16::from_le_bytes(self.array()?))
	}
	pub fn u32(&mut self) -> Result<u32, VisibilityError> {
		Ok(u32::from_le_bytes(self.array()?))
	}
	pub fn u64(&mut self) -> Result<u64, VisibilityError> {
		Ok(u64::from_le_bytes(self.array()?))
	}
	pub fn finish(self) -> Result<(), VisibilityError> {
		if self.position == self.bytes.len() {
			Ok(())
		} else {
			Err(VisibilityError::TrailingBytes)
		}
	}
}
pub(super) fn envelope(
	magic: &[u8; 8],
	payload: &[u8],
	max: usize,
) -> Result<Vec<u8>, VisibilityError> {
	let size = payload
		.len()
		.checked_add(46)
		.ok_or(VisibilityError::LimitExceeded)?;
	if size > max {
		return Err(VisibilityError::LimitExceeded);
	}
	let len = u32::try_from(payload.len()).map_err(|_| VisibilityError::LimitExceeded)?;
	let mut bytes = Vec::with_capacity(size);
	bytes.extend_from_slice(magic);
	bytes.extend_from_slice(&1u16.to_le_bytes());
	bytes.extend_from_slice(&len.to_le_bytes());
	bytes.extend_from_slice(payload);
	let checksum = blake3::hash(&bytes);
	bytes.extend_from_slice(checksum.as_bytes());
	Ok(bytes)
}
pub(super) fn open_envelope<'a>(
	bytes: &'a [u8],
	magic: &[u8; 8],
	max: usize,
) -> Result<&'a [u8], VisibilityError> {
	if bytes.len() > max {
		return Err(VisibilityError::LimitExceeded);
	}
	let mut r = Reader::new(bytes);
	if &r.array::<8>()? != magic || r.u16()? != 1 {
		return Err(VisibilityError::UnknownFormat);
	}
	let len = r.u32()? as usize;
	if len.checked_add(46).ok_or(VisibilityError::LimitExceeded)? != bytes.len() {
		return Err(VisibilityError::Corrupt);
	}
	let payload = r.take(len)?;
	let checksum = r.array::<32>()?;
	r.finish()?;
	if blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &checksum {
		return Err(VisibilityError::Corrupt);
	}
	Ok(payload)
}
