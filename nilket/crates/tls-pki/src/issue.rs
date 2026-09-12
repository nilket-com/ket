//! Certificate issuance for a deployment's own CA (ADR 0165).
//!
//! Private keys never move between machines: a client generates its key
//! where it will live and sends a certificate signing request; the CA signs
//! it here and returns only the certificate. The listener certificate is the
//! one exception, generated beside the CA for the node that will serve it.
//! Portable: no file modes, no Unix-only calls.

use rcgen::{
	BasicConstraints, Certificate, CertificateParams, CertificateSigningRequest, DnType,
	ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType,
};

use crate::error::PkiError;
use crate::san::canonical_dns_name;

/// PEM text for a certificate and, where one was generated here, its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedPem {
	pub cert: String,
	pub key: Option<String>,
}

fn rcgen_error(error: rcgen::Error) -> PkiError {
	PkiError::Issue {
		reason: error.to_string(),
	}
}

/// A new self-signed CA. The key is returned to be stored by the caller.
pub fn generate_ca(common_name: &str) -> Result<IssuedPem, PkiError> {
	let mut params = CertificateParams::new(Vec::new());
	params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
	params.key_usages = vec![
		KeyUsagePurpose::DigitalSignature,
		KeyUsagePurpose::KeyCertSign,
		KeyUsagePurpose::CrlSign,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, common_name);
	let ca = Certificate::from_params(params).map_err(rcgen_error)?;
	Ok(IssuedPem {
		cert: ca.serialize_pem().map_err(rcgen_error)?,
		key: Some(ca.serialize_private_key_pem()),
	})
}

/// Reload an existing CA from its PEM certificate and key so it can sign.
pub fn load_ca(cert_pem: &str, key_pem: &str) -> Result<Certificate, PkiError> {
	let key_pair = KeyPair::from_pem(key_pem).map_err(rcgen_error)?;
	let params = CertificateParams::from_ca_cert_pem(cert_pem, key_pair).map_err(rcgen_error)?;
	Certificate::from_params(params).map_err(rcgen_error)
}

/// The listener identity for a Toron node: SAN `toron-<worker_id>.<domain>`,
/// which is what the daemon validates its own listener certificate against.
pub fn issue_listener(
	ca: &Certificate,
	worker_id: &str,
	cluster_domain: &str,
) -> Result<IssuedPem, PkiError> {
	let dns_name = canonical_dns_name("toron", worker_id, cluster_domain);
	let mut params = CertificateParams::new(vec![dns_name.clone()]);
	params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	params.extended_key_usages = vec![
		ExtendedKeyUsagePurpose::ServerAuth,
		ExtendedKeyUsagePurpose::ClientAuth,
	];
	params.distinguished_name.push(DnType::CommonName, dns_name);
	let leaf = Certificate::from_params(params).map_err(rcgen_error)?;
	Ok(IssuedPem {
		cert: leaf.serialize_pem_with_signer(ca).map_err(rcgen_error)?,
		key: Some(leaf.serialize_private_key_pem()),
	})
}

/// An operator's identity toward Wendelon (ADR 0167): role `operator`, no
/// tenant claim, no DNS name, signed by the operator CA. It is what tenant
/// administration is authorized against; a deployment beyond `nilket local`
/// has one because an operator issued it, never the development leaf.
pub fn issue_operator(ca: &Certificate, name: &str, key_id: &[u8]) -> Result<IssuedPem, PkiError> {
	if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
		return Err(PkiError::Issue {
			reason: format!("operator name {name:?} must be ASCII letters, digits, and hyphens"),
		});
	}
	let leaf = crate::claims::claimed_leaf(name, None, "operator", None, key_id.to_vec())
		.map_err(rcgen_error)?;
	Ok(IssuedPem {
		cert: leaf.serialize_pem_with_signer(ca).map_err(rcgen_error)?,
		key: Some(leaf.serialize_private_key_pem()),
	})
}

/// A node's storage identity toward Wendelon: SAN `node-<index>.<domain>`,
/// role `data_node`, and the tenant claim Wendelon authorizes schema and data
/// operations against. A Toron node serves the tenant its storage identity
/// claims, so this is what makes a deployment "tenant N".
pub fn issue_storage(
	ca: &Certificate,
	tenant: &str,
	cluster_domain: &str,
	node_index: u32,
) -> Result<IssuedPem, PkiError> {
	validate_tenant_token(tenant)?;
	let leaf = crate::claims::claimed_leaf(
		"toron-storage",
		Some(format!("node-{node_index}.{cluster_domain}")),
		"data_node",
		Some(tenant),
		node_index.to_le_bytes().to_vec(),
	)
	.map_err(rcgen_error)?;
	Ok(IssuedPem {
		cert: leaf.serialize_pem_with_signer(ca).map_err(rcgen_error)?,
		key: Some(leaf.serialize_private_key_pem()),
	})
}

/// The SQL client SAN token is a tenant id: a positive integer without a
/// leading zero, exactly what the listener's admission parses.
pub fn validate_tenant_token(tenant: &str) -> Result<(), PkiError> {
	let valid = !tenant.is_empty()
		&& !tenant.starts_with('0')
		&& tenant.chars().all(|ch| ch.is_ascii_digit())
		&& tenant.parse::<u64>().is_ok();
	if valid {
		Ok(())
	} else {
		Err(PkiError::Issue {
			reason: format!("tenant {tenant:?} must be a positive integer without a leading zero"),
		})
	}
}

/// Sign a client's request for tenant `tenant`. The request supplies only
/// the public key and subject; the SAN is set here, so a request cannot ask
/// for a tenant other than the one the operator names. No key is returned
/// because none was generated here.
pub fn sign_client_csr(
	ca: &Certificate,
	csr_pem: &str,
	tenant: &str,
	cluster_domain: &str,
) -> Result<IssuedPem, PkiError> {
	validate_tenant_token(tenant)?;
	let mut csr = CertificateSigningRequest::from_pem(csr_pem).map_err(rcgen_error)?;
	let dns_name = canonical_dns_name("client", tenant, cluster_domain);
	csr.params.subject_alt_names = vec![SanType::DnsName(dns_name)];
	csr.params.is_ca = IsCa::ExplicitNoCa;
	csr.params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	csr.params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
	Ok(IssuedPem {
		cert: csr.serialize_pem_with_signer(ca).map_err(rcgen_error)?,
		key: None,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::san::admit_san;
	use crate::{extract_dns_sans, load_ca_roots_from_pem, load_cert_chain_from_pem};
	use rustls::pki_types::UnixTime;
	use rustls::server::WebPkiClientVerifier;
	use std::sync::Arc;

	const DOMAIN: &str = "cluster.local";

	fn client_csr() -> (String, String) {
		let mut params = CertificateParams::new(Vec::new());
		params
			.distinguished_name
			.push(DnType::CommonName, "windows-desktop");
		let requester = Certificate::from_params(params).unwrap();
		(
			requester.serialize_request_pem().unwrap(),
			requester.serialize_private_key_pem(),
		)
	}

	#[test]
	fn a_signed_csr_carries_the_tenant_san_and_chains_to_the_ca() {
		let ca_pem = generate_ca("test-ca").unwrap();
		let ca = load_ca(&ca_pem.cert, ca_pem.key.as_deref().unwrap()).unwrap();
		let (csr, _key) = client_csr();
		let issued = sign_client_csr(&ca, &csr, "7", DOMAIN).unwrap();
		assert!(issued.key.is_none(), "signing returns no private key");

		let chain = load_cert_chain_from_pem(&issued.cert).unwrap();
		let sans = extract_dns_sans(&chain[0]).unwrap();
		assert_eq!(sans, vec![format!("client-7.{DOMAIN}")]);
		let identity = admit_san(&sans[0], DOMAIN, &["client"]).unwrap();
		assert_eq!(identity.id_token, "7");

		let roots = load_ca_roots_from_pem(&ca_pem.cert).unwrap();
		let verifier = WebPkiClientVerifier::builder_with_provider(
			Arc::new(roots),
			Arc::new(rustls::crypto::ring::default_provider()),
		)
		.build()
		.unwrap();
		verifier
			.verify_client_cert(&chain[0], &[], UnixTime::now())
			.expect("chains to the CA");
	}

	#[test]
	fn a_request_cannot_choose_its_own_san() {
		let ca_pem = generate_ca("test-ca").unwrap();
		let ca = load_ca(&ca_pem.cert, ca_pem.key.as_deref().unwrap()).unwrap();
		let mut params = CertificateParams::new(vec![format!("client-1.{DOMAIN}")]);
		params.distinguished_name.push(DnType::CommonName, "greedy");
		let requester = Certificate::from_params(params).unwrap();
		let csr = requester.serialize_request_pem().unwrap();
		let issued = sign_client_csr(&ca, &csr, "42", DOMAIN).unwrap();
		let chain = load_cert_chain_from_pem(&issued.cert).unwrap();
		assert_eq!(
			extract_dns_sans(&chain[0]).unwrap(),
			vec![format!("client-42.{DOMAIN}")]
		);
	}

	#[test]
	fn tenant_tokens_follow_the_listener_parser() {
		for good in ["1", "42", "18446744073709551615"] {
			validate_tenant_token(good).unwrap();
		}
		for bad in ["", "0", "07", "-1", "1a", "18446744073709551616", "windows"] {
			assert!(validate_tenant_token(bad).is_err(), "{bad:?}");
		}
	}

	#[test]
	fn a_storage_identity_claims_its_tenant() {
		let ca_pem = generate_ca("node-ca").unwrap();
		let ca = load_ca(&ca_pem.cert, ca_pem.key.as_deref().unwrap()).unwrap();
		let issued = issue_storage(&ca, "1", DOMAIN, 2).unwrap();
		let chain = load_cert_chain_from_pem(&issued.cert).unwrap();
		assert_eq!(
			extract_dns_sans(&chain[0]).unwrap(),
			vec![format!("node-2.{DOMAIN}")]
		);
		let claims = crate::extract_extension(&chain[0], &[1, 3, 6, 1, 4, 1, 55555, 1, 1])
			.expect("claims extension")
			.expect("claims present");
		assert_eq!(
			claims,
			crate::actor_claims("data_node", Some("1"), &2_u32.to_le_bytes())
		);
		assert!(
			issue_storage(&ca, "0", DOMAIN, 2).is_err(),
			"tenant 0 is not a tenant token"
		);
	}

	/// ADR 0167 §2: an issued operator identity claims role operator and no
	/// tenant, names no host, and chains to the CA that signed it.
	#[test]
	fn an_operator_identity_claims_operator_and_no_tenant() {
		let ca_pem = generate_ca("operator-ca").unwrap();
		let ca = load_ca(&ca_pem.cert, ca_pem.key.as_deref().unwrap()).unwrap();
		let issued = issue_operator(&ca, "bigfoot-ops", b"key-7").unwrap();
		let chain = load_cert_chain_from_pem(&issued.cert).unwrap();
		assert!(extract_dns_sans(&chain[0]).unwrap().is_empty());
		let claims = crate::extract_extension(&chain[0], &[1, 3, 6, 1, 4, 1, 55555, 1, 1])
			.expect("claims extension")
			.expect("claims present");
		assert_eq!(claims, crate::actor_claims("operator", None, b"key-7"));
		assert!(issued.key.is_some(), "the operator holds the key");
		assert!(issue_operator(&ca, "", b"k").is_err());
		assert!(
			issue_operator(&ca, "ops team", b"k").is_err(),
			"no spaces in a name"
		);
	}

	#[test]
	fn a_listener_certificate_names_the_worker() {
		let ca_pem = generate_ca("test-ca").unwrap();
		let ca = load_ca(&ca_pem.cert, ca_pem.key.as_deref().unwrap()).unwrap();
		let worker = "00000000-0000-0000-0000-000000000042";
		let issued = issue_listener(&ca, worker, DOMAIN).unwrap();
		assert!(issued.key.is_some());
		let chain = load_cert_chain_from_pem(&issued.cert).unwrap();
		assert_eq!(
			extract_dns_sans(&chain[0]).unwrap(),
			vec![format!("toron-{worker}.{DOMAIN}")]
		);
	}

	#[test]
	fn a_reloaded_ca_signs_certificates_the_original_would_verify() {
		let ca_pem = generate_ca("test-ca").unwrap();
		let reloaded = load_ca(&ca_pem.cert, ca_pem.key.as_deref().unwrap()).unwrap();
		let (csr, _) = client_csr();
		let issued = sign_client_csr(&reloaded, &csr, "3", DOMAIN).unwrap();
		let roots = load_ca_roots_from_pem(&ca_pem.cert).unwrap();
		let chain = load_cert_chain_from_pem(&issued.cert).unwrap();
		WebPkiClientVerifier::builder_with_provider(
			Arc::new(roots),
			Arc::new(rustls::crypto::ring::default_provider()),
		)
		.build()
		.unwrap()
		.verify_client_cert(&chain[0], &[], UnixTime::now())
		.expect("original CA verifies what the reloaded CA signed");
	}
}
