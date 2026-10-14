//! Offline trust files are explicit input, never inferred from audit records.
use crate::audit::{AuditRootTrust, AuditTrustError, audit_root_trust};
use crate::schema::NodeId;
use serde::{
	Deserialize, Deserializer,
	de::{self, MapAccess, Visitor},
};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

const MAX_TRUST_FILE_BYTES: u64 = 1024 * 1024;
#[derive(Debug)]
pub(super) enum TrustFileError {
	Io,
	Oversize,
	Json,
	Version,
	Node,
	KeyEncoding,
	Enrollment(AuditTrustError),
}
impl std::fmt::Display for TrustFileError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Enrollment(reason) => write!(f, "audit trust file refused: {reason}"),
			_ => write!(f, "audit trust file refused: {self:?}"),
		}
	}
}
impl std::error::Error for TrustFileError {}

struct TrustFile {
	version: u32,
	keys: Keys,
}
struct Keys(BTreeMap<String, String>);
impl<'de> Deserialize<'de> for Keys {
	fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
		struct KeysVisitor;
		impl<'de> Visitor<'de> for KeysVisitor {
			type Value = Keys;
			fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
				f.write_str("unique node keys")
			}
			fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Keys, M::Error> {
				let mut keys = BTreeMap::new();
				while let Some(node) = map.next_key::<String>()? {
					if keys.contains_key(&node) {
						return Err(de::Error::custom("duplicate node"));
					}
					keys.insert(node, map.next_value::<String>()?);
				}
				Ok(Keys(keys))
			}
		}
		d.deserialize_map(KeysVisitor)
	}
}
impl<'de> Deserialize<'de> for TrustFile {
	fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
		struct FileVisitor;
		impl<'de> Visitor<'de> for FileVisitor {
			type Value = TrustFile;
			fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
				f.write_str("version and unique keys")
			}
			fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<TrustFile, M::Error> {
				let mut version = None;
				let mut keys = None;
				while let Some(field) = map.next_key::<String>()? {
					match field.as_str() {
						"version" if version.is_none() => version = Some(map.next_value()?),
						"keys" if keys.is_none() => keys = Some(map.next_value()?),
						_ => return Err(de::Error::custom("unknown or duplicate field")),
					}
				}
				Ok(TrustFile {
					version: version.ok_or_else(|| de::Error::custom("missing version"))?,
					keys: keys.ok_or_else(|| de::Error::custom("missing keys"))?,
				})
			}
		}
		d.deserialize_map(FileVisitor)
	}
}
pub(super) fn load(path: &Path) -> Result<AuditRootTrust, TrustFileError> {
	let file = std::fs::File::open(path).map_err(|_| TrustFileError::Io)?;
	let mut bytes = Vec::new();
	file.take(MAX_TRUST_FILE_BYTES + 1)
		.read_to_end(&mut bytes)
		.map_err(|_| TrustFileError::Io)?;
	// This bound is checked before serde sees any input.
	if bytes.len() as u64 > MAX_TRUST_FILE_BYTES {
		return Err(TrustFileError::Oversize);
	}
	parse(&bytes)
}
fn parse(bytes: &[u8]) -> Result<AuditRootTrust, TrustFileError> {
	let file: TrustFile = serde_json::from_slice(bytes).map_err(|_| TrustFileError::Json)?;
	if file.version != 1 {
		return Err(TrustFileError::Version);
	}
	let mut entries = Vec::new();
	for (wire, raw) in file.keys.0 {
		let node = NodeId::parse_wire(&wire).map_err(|_| TrustFileError::Node)?;
		if node.format_wire() != wire {
			return Err(TrustFileError::Node);
		}
		if raw.len() != 64 {
			return Err(TrustFileError::KeyEncoding);
		}
		let mut key = [0; 32];
		for (slot, pair) in key.iter_mut().zip(raw.as_bytes().chunks_exact(2)) {
			let nibble = |b| match b {
				b'0'..=b'9' => Ok(b - b'0'),
				b'a'..=b'f' => Ok(b - b'a' + 10),
				_ => Err(TrustFileError::KeyEncoding),
			};
			*slot = (nibble(pair[0])? << 4) | nibble(pair[1])?;
		}
		entries.push((node.id, key));
	}
	audit_root_trust(entries).map_err(TrustFileError::Enrollment)
}

#[cfg(test)]
mod tests {
	use super::*;
	fn valid() -> String {
		format!(
			r#"{{"version":1,"keys":{{"n-01":"{}"}}}}"#,
			crate::audit::AuditRootSigner::from_seed([51; 32]).verifying_key_hex()
		)
	}
	#[test]
	fn audit_trust_file_refuses_duplicate_unknown_malformed_and_version() {
		parse(valid().as_bytes()).unwrap();
		for text in [
			r#"{"version":1,"version":1,"keys":{}}"#,
			r#"{"version":1,"keys":{"n-01":"a","n-01":"b"}}"#,
			r#"{"version":1,"keys":{},"secret":"SENSITIVE"}"#,
			r#"{"version":2,"keys":{}}"#,
			"SENSITIVE",
		] {
			let err = parse(text.as_bytes()).unwrap_err();
			assert!(!err.to_string().contains("SENSITIVE"));
		}
	}
	#[test]
	fn audit_trust_file_requires_canonical_node_lowercase_key_and_strong_enrollment() {
		let good = valid();
		assert!(matches!(
			parse(good.replace("n-01", "n-1").as_bytes()),
			Err(TrustFileError::Node)
		));
		let key = crate::audit::AuditRootSigner::from_seed([51; 32]).verifying_key_hex();
		assert!(matches!(
			parse(good.replace(&key, &key.to_uppercase()).as_bytes()),
			Err(TrustFileError::KeyEncoding)
		));
		assert!(matches!(
			parse(good.replace(&key, "00").as_bytes()),
			Err(TrustFileError::KeyEncoding)
		));
		assert!(matches!(
			parse(good.replace(&key, &"00".repeat(32)).as_bytes()),
			Err(TrustFileError::Enrollment(
				AuditTrustError::InvalidTrustedKey { node: 1 }
			))
		));
	}
	#[test]
	fn audit_trust_file_bounds_read_before_json_parse() {
		let path = std::env::temp_dir().join(format!("audit-trust-bound-{}", uuid::Uuid::new_v4()));
		std::fs::write(&path, vec![b'!'; MAX_TRUST_FILE_BYTES as usize + 1]).unwrap();
		assert!(matches!(load(&path), Err(TrustFileError::Oversize)));
		std::fs::remove_file(path).unwrap();
	}
}
