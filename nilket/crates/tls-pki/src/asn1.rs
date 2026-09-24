use rustls::pki_types::CertificateDer;

use crate::error::PkiError;

const SUBJECT_ALT_NAME_OID: &[u8] = &[0x55, 0x1D, 0x11];
const DNS_NAME_TAG: u8 = 0x82;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject {
	pub raw: Vec<u8>,
	pub display: String,
}

pub fn extract_dns_sans(cert: &CertificateDer<'_>) -> Result<Vec<String>, PkiError> {
	let mut tbs = tbs_certificate_reader(cert.as_ref())?;
	skip_tbs_header_through_subject_public_key(&mut tbs, true)?;
	let Some(mut extensions) = extensions_reader(&mut tbs)? else {
		return Ok(Vec::new());
	};
	let mut dns_sans = Vec::new();

	while !extensions.is_empty() {
		let extension = extensions.read_tlv(0x30, "extension")?;
		let mut extension = DerReader::new(extension);
		let oid = extension.read_tlv(0x06, "extension oid")?;
		if extension.peek_tag() == Some(0x01) {
			extension.read_any_tlv("extension critical")?;
		}
		let value = extension.read_tlv(0x04, "extension value")?;
		if oid == SUBJECT_ALT_NAME_OID {
			let mut san_value = DerReader::new(value);
			let general_names = san_value.read_tlv(0x30, "subjectAltName")?;
			let mut general_names = DerReader::new(general_names);
			while !general_names.is_empty() {
				let (tag, value) = general_names.read_any_tlv("generalName")?;
				if tag == DNS_NAME_TAG {
					let dns_name =
						std::str::from_utf8(value).map_err(|error| PkiError::DerParse {
							reason: format!("DNS SAN is not valid UTF-8/IA5: {error}"),
						})?;
					dns_sans.push(dns_name.to_string());
				}
			}
		}
	}

	Ok(dns_sans)
}

pub fn extract_subject(cert: &CertificateDer<'_>) -> Result<Subject, PkiError> {
	let mut tbs = tbs_certificate_reader(cert.as_ref())?;
	skip_tbs_header_through_subject_public_key(&mut tbs, false)?;
	let subject = tbs.read_tlv(0x30, "subject")?;
	Ok(Subject {
		raw: subject.to_vec(),
		display: format_subject(subject)?,
	})
}

pub fn extract_extension(
	cert: &CertificateDer<'_>,
	oid: &[u32],
) -> Result<Option<Vec<u8>>, PkiError> {
	let expected_oid = encode_oid(oid)?;
	let mut tbs = tbs_certificate_reader(cert.as_ref())?;
	skip_tbs_header_through_subject_public_key(&mut tbs, true)?;
	let Some(mut extensions) = extensions_reader(&mut tbs)? else {
		return Ok(None);
	};

	while !extensions.is_empty() {
		let extension = extensions.read_tlv(0x30, "extension")?;
		let mut extension = DerReader::new(extension);
		let actual_oid = extension.read_tlv(0x06, "extension oid")?;
		if extension.peek_tag() == Some(0x01) {
			extension.read_any_tlv("extension critical")?;
		}
		let value = extension.read_tlv(0x04, "extension value")?;
		if actual_oid == expected_oid.as_slice() {
			return Ok(Some(value.to_vec()));
		}
	}
	Ok(None)
}

fn tbs_certificate_reader(cert_der: &[u8]) -> Result<DerReader<'_>, PkiError> {
	let mut cert = DerReader::new(cert_der);
	let certificate = cert.read_tlv(0x30, "certificate")?;
	let mut certificate = DerReader::new(certificate);
	let tbs = certificate.read_tlv(0x30, "tbsCertificate")?;
	Ok(DerReader::new(tbs))
}

fn skip_tbs_header_through_subject_public_key(
	tbs: &mut DerReader<'_>,
	include_subject: bool,
) -> Result<(), PkiError> {
	if tbs.peek_tag() == Some(0xA0) {
		tbs.read_any_tlv("version")?;
	}
	for field in ["serialNumber", "signature", "issuer", "validity"] {
		tbs.read_any_tlv(field)?;
	}
	if include_subject {
		tbs.read_any_tlv("subject")?;
		tbs.read_any_tlv("subjectPublicKeyInfo")?;
	}
	while matches!(tbs.peek_tag(), Some(0x81 | 0x82 | 0xA1 | 0xA2)) {
		tbs.read_any_tlv("uniqueId")?;
	}
	Ok(())
}

fn extensions_reader<'a>(tbs: &mut DerReader<'a>) -> Result<Option<DerReader<'a>>, PkiError> {
	if tbs.peek_tag() != Some(0xA3) {
		return Ok(None);
	}
	let extensions = tbs.read_tlv(0xA3, "extensions")?;
	let mut extensions = DerReader::new(extensions);
	let extensions = extensions.read_tlv(0x30, "extensions sequence")?;
	Ok(Some(DerReader::new(extensions)))
}

fn format_subject(subject_der: &[u8]) -> Result<String, PkiError> {
	let mut subject = DerReader::new(subject_der);
	let mut rdns = Vec::new();
	while !subject.is_empty() {
		let rdn = subject.read_tlv(0x31, "relative distinguished name")?;
		let mut rdn = DerReader::new(rdn);
		let mut attributes = Vec::new();
		while !rdn.is_empty() {
			let attribute = rdn.read_tlv(0x30, "attribute type and value")?;
			let mut attribute = DerReader::new(attribute);
			let oid = attribute.read_tlv(0x06, "subject attribute oid")?;
			let (tag, value) = attribute.read_any_tlv("subject attribute value")?;
			let key = subject_attribute_name(oid);
			let value = decode_subject_attribute_value(tag, value)?;
			attributes.push(format!("{key}={value}"));
		}
		if !attributes.is_empty() {
			rdns.push(attributes.join("+"));
		}
	}
	if rdns.is_empty() {
		return Err(PkiError::DerParse {
			reason: "peer certificate subject is empty".to_string(),
		});
	}
	Ok(rdns.join(","))
}

fn subject_attribute_name(oid: &[u8]) -> &'static str {
	match oid {
		[0x55, 0x04, 0x03] => "CN",
		[0x55, 0x04, 0x06] => "C",
		[0x55, 0x04, 0x07] => "L",
		[0x55, 0x04, 0x08] => "ST",
		[0x55, 0x04, 0x0A] => "O",
		[0x55, 0x04, 0x0B] => "OU",
		[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x09, 0x01] => "emailAddress",
		_ => "OID",
	}
}

fn decode_subject_attribute_value(tag: u8, value: &[u8]) -> Result<String, PkiError> {
	match tag {
		0x0C | 0x13 | 0x14 | 0x16 => {
			std::str::from_utf8(value)
				.map(str::to_string)
				.map_err(|error| PkiError::DerParse {
					reason: format!("certificate subject is not valid UTF-8: {error}"),
				})
		}
		0x1E => {
			if !value.len().is_multiple_of(2) {
				return Err(PkiError::DerParse {
					reason: "certificate BMPString subject has odd byte length".to_string(),
				});
			}
			let code_units = value
				.as_chunks::<2>()
				.0
				.iter()
				.map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
				.collect::<Vec<_>>();
			String::from_utf16(&code_units).map_err(|error| PkiError::DerParse {
				reason: format!("certificate subject BMPString is invalid UTF-16: {error}"),
			})
		}
		_ => Ok(format!("#{}", hex_string(value))),
	}
}

fn encode_oid(oid: &[u32]) -> Result<Vec<u8>, PkiError> {
	if oid.len() < 2 || oid[0] > 2 || (oid[0] < 2 && oid[1] >= 40) {
		return Err(PkiError::DerParse {
			reason: format!("invalid OID arcs: {oid:?}"),
		});
	}
	let mut out = vec![(oid[0] * 40 + oid[1]) as u8];
	for &arc in &oid[2..] {
		let mut stack = vec![(arc & 0x7F) as u8];
		let mut value = arc >> 7;
		while value > 0 {
			stack.push(((value & 0x7F) as u8) | 0x80);
			value >>= 7;
		}
		out.extend(stack.into_iter().rev());
	}
	Ok(out)
}

fn hex_string(bytes: &[u8]) -> String {
	const HEX: &[u8; 16] = b"0123456789abcdef";
	let mut out = String::with_capacity(bytes.len() * 2);
	for &byte in bytes {
		out.push(HEX[(byte >> 4) as usize] as char);
		out.push(HEX[(byte & 0x0F) as usize] as char);
	}
	out
}

pub struct DerReader<'a> {
	input: &'a [u8],
	pos: usize,
}

impl<'a> DerReader<'a> {
	pub fn new(input: &'a [u8]) -> Self {
		Self { input, pos: 0 }
	}

	pub fn is_empty(&self) -> bool {
		self.pos >= self.input.len()
	}

	pub fn peek_tag(&self) -> Option<u8> {
		self.input.get(self.pos).copied()
	}

	pub fn read_tlv(&mut self, expected_tag: u8, what: &str) -> Result<&'a [u8], PkiError> {
		let (tag, value) = self.read_any_tlv(what)?;
		if tag != expected_tag {
			return Err(PkiError::DerParse {
				reason: format!(
					"invalid DER for {what}: expected tag 0x{expected_tag:02x}, got 0x{tag:02x}"
				),
			});
		}
		Ok(value)
	}

	pub fn read_any_tlv(&mut self, what: &str) -> Result<(u8, &'a [u8]), PkiError> {
		if self.is_empty() {
			return Err(PkiError::DerParse {
				reason: format!("truncated DER while reading {what}"),
			});
		}
		let tag = self.input[self.pos];
		self.pos += 1;
		let len = self.read_len(what)?;
		let end = self
			.pos
			.checked_add(len)
			.ok_or_else(|| PkiError::DerParse {
				reason: format!("DER length overflow while reading {what}"),
			})?;
		if end > self.input.len() {
			return Err(PkiError::DerParse {
				reason: format!("truncated DER payload while reading {what}"),
			});
		}
		let value = &self.input[self.pos..end];
		self.pos = end;
		Ok((tag, value))
	}

	pub fn read_len(&mut self, what: &str) -> Result<usize, PkiError> {
		if self.is_empty() {
			return Err(PkiError::DerParse {
				reason: format!("truncated DER length while reading {what}"),
			});
		}
		let first = self.input[self.pos];
		self.pos += 1;
		if first & 0x80 == 0 {
			return Ok(first as usize);
		}
		let width = (first & 0x7F) as usize;
		if width == 0 {
			return Err(PkiError::DerParse {
				reason: format!("indefinite DER length is not allowed for {what}"),
			});
		}
		if width > std::mem::size_of::<usize>() || self.pos + width > self.input.len() {
			return Err(PkiError::DerParse {
				reason: format!("invalid DER length width for {what}"),
			});
		}
		let mut len = 0usize;
		for _ in 0..width {
			len = (len << 8) | self.input[self.pos] as usize;
			self.pos += 1;
		}
		Ok(len)
	}
}
