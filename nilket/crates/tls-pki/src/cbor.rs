use crate::error::PkiError;

pub struct CborReader<'a> {
	input: &'a [u8],
	pos: usize,
}

impl<'a> CborReader<'a> {
	pub fn new(input: &'a [u8]) -> Self {
		Self { input, pos: 0 }
	}

	pub fn is_empty(&self) -> bool {
		self.pos >= self.input.len()
	}

	pub fn peek_simple_null(&self) -> bool {
		matches!(self.input.get(self.pos), Some(0xF6))
	}

	pub fn read_text(&mut self, what: &str) -> Result<String, PkiError> {
		let len = self.read_len_for_major(3, what)?;
		let end = self.checked_end(len, what)?;
		let text = std::str::from_utf8(&self.input[self.pos..end]).map_err(|error| {
			PkiError::CborParse {
				reason: format!("invalid UTF-8 while reading {what}: {error}"),
			}
		})?;
		self.pos = end;
		Ok(text.to_string())
	}

	pub fn read_bytes(&mut self, what: &str) -> Result<Vec<u8>, PkiError> {
		let len = self.read_len_for_major(2, what)?;
		let end = self.checked_end(len, what)?;
		let bytes = self.input[self.pos..end].to_vec();
		self.pos = end;
		Ok(bytes)
	}

	pub fn read_uint(&mut self, what: &str) -> Result<u64, PkiError> {
		let byte = self.read_u8(what)?;
		let major = byte >> 5;
		let additional = byte & 0x1F;
		if major != 0 {
			return Err(PkiError::CborParse {
				reason: format!("unexpected CBOR major type for {what}: expected 0, got {major}"),
			});
		}
		self.read_additional_u64(additional, what)
	}

	pub fn read_null(&mut self, what: &str) -> Result<(), PkiError> {
		let byte = self.read_u8(what)?;
		if byte != 0xF6 {
			return Err(PkiError::CborParse {
				reason: format!("expected CBOR null for {what}, got 0x{byte:02x}"),
			});
		}
		Ok(())
	}

	pub fn read_len_for_major(
		&mut self,
		expected_major: u8,
		what: &str,
	) -> Result<usize, PkiError> {
		let byte = self.read_u8(what)?;
		let major = byte >> 5;
		let additional = byte & 0x1F;
		if major != expected_major {
			return Err(PkiError::CborParse {
				reason: format!(
					"unexpected CBOR major type for {what}: expected {expected_major}, got {major}"
				),
			});
		}
		self.read_additional(additional, what)
	}

	fn read_additional(&mut self, additional: u8, what: &str) -> Result<usize, PkiError> {
		let value = self.read_additional_u64(additional, what)?;
		usize::try_from(value).map_err(|_| PkiError::CborParse {
			reason: format!("CBOR length overflow while reading {what}"),
		})
	}

	fn read_additional_u64(&mut self, additional: u8, what: &str) -> Result<u64, PkiError> {
		match additional {
			n @ 0..=23 => Ok(n as u64),
			24 => Ok(self.read_u8(what)? as u64),
			25 => {
				let end = self.checked_end(2, what)?;
				let value = u16::from_be_bytes(self.input[self.pos..end].try_into().unwrap());
				self.pos = end;
				Ok(value as u64)
			}
			26 => {
				let end = self.checked_end(4, what)?;
				let value = u32::from_be_bytes(self.input[self.pos..end].try_into().unwrap());
				self.pos = end;
				Ok(value as u64)
			}
			27 => {
				let end = self.checked_end(8, what)?;
				let value = u64::from_be_bytes(self.input[self.pos..end].try_into().unwrap());
				self.pos = end;
				Ok(value)
			}
			31 => Err(PkiError::CborParse {
				reason: format!("indefinite-length CBOR is not allowed for {what}"),
			}),
			_ => Err(PkiError::CborParse {
				reason: format!("unsupported CBOR additional info {additional} for {what}"),
			}),
		}
	}

	fn read_u8(&mut self, what: &str) -> Result<u8, PkiError> {
		if self.is_empty() {
			return Err(PkiError::CborParse {
				reason: format!("truncated CBOR while reading {what}"),
			});
		}
		let byte = self.input[self.pos];
		self.pos += 1;
		Ok(byte)
	}

	fn checked_end(&self, len: usize, what: &str) -> Result<usize, PkiError> {
		let end = self
			.pos
			.checked_add(len)
			.ok_or_else(|| PkiError::CborParse {
				reason: format!("CBOR length overflow while reading {what}"),
			})?;
		if end > self.input.len() {
			return Err(PkiError::CborParse {
				reason: format!("truncated CBOR payload while reading {what}"),
			});
		}
		Ok(end)
	}
}
