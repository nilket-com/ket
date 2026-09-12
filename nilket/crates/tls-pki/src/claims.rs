//! The actor-claims certificate extension Wendelon reads from a storage
//! client: role, scopes, tenant, and key id, as a fixed CBOR map. Portable,
//! so both the development writer and deployment issuance can build it.

use rcgen::{
	Certificate, CertificateParams, CustomExtension, DnType, ExtendedKeyUsagePurpose,
	KeyUsagePurpose,
};

pub const ACTOR_CLAIMS_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 55555, 1, 1];

/// Encode the claims exactly as Wendelon's admission decodes them.
pub fn actor_claims(role: &str, tenant_id: Option<&str>, key_id: &[u8]) -> Vec<u8> {
	let mut out = Vec::new();
	cbor_len(&mut out, 5, 4);
	cbor_text(&mut out, "role");
	cbor_text(&mut out, role);
	cbor_text(&mut out, "scopes");
	cbor_len(&mut out, 4, 1);
	cbor_text(&mut out, "cluster");
	cbor_text(&mut out, "tenant_id");
	match tenant_id {
		Some(tenant_id) => cbor_text(&mut out, tenant_id),
		None => out.push(0xf6),
	}
	cbor_text(&mut out, "key_id");
	cbor_len(&mut out, 2, key_id.len());
	out.extend_from_slice(key_id);
	out
}

/// A leaf certificate carrying actor claims: the shape every Nilket
/// node-to-node identity uses.
pub fn claimed_leaf(
	common_name: &str,
	dns_name: Option<String>,
	role: &str,
	tenant_id: Option<&str>,
	key_id: Vec<u8>,
) -> Result<Certificate, rcgen::Error> {
	let mut params = CertificateParams::new(dns_name.into_iter().collect::<Vec<_>>());
	params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	params.extended_key_usages = vec![
		ExtendedKeyUsagePurpose::ServerAuth,
		ExtendedKeyUsagePurpose::ClientAuth,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, common_name);
	params
		.custom_extensions
		.push(CustomExtension::from_oid_content(
			ACTOR_CLAIMS_OID,
			actor_claims(role, tenant_id, &key_id),
		));
	Certificate::from_params(params)
}

fn cbor_text(out: &mut Vec<u8>, value: &str) {
	cbor_len(out, 3, value.len());
	out.extend_from_slice(value.as_bytes());
}

fn cbor_len(out: &mut Vec<u8>, major: u8, length: usize) {
	match length {
		0..=23 => out.push((major << 5) | length as u8),
		24..=0xff => out.extend_from_slice(&[(major << 5) | 24, length as u8]),
		0x100..=0xffff => {
			out.push((major << 5) | 25);
			out.extend_from_slice(&(length as u16).to_be_bytes());
		}
		_ => unreachable!("local actor claim fields are bounded"),
	}
}
