// RUSTSEC-2026-0381/0383: the bundled PostgreSQL TLS trust store must not
// reintroduce the emSign root whose intermediate signing keys were compromised.
#[test]
fn bundled_tls_roots_exclude_distrusted_emsign_g3() {
	let distrusted = b"emSign ECC Root CA - G3";
	assert!(!webpki_roots::TLS_SERVER_ROOTS.is_empty());
	for root in webpki_roots::TLS_SERVER_ROOTS {
		assert!(
			!root
				.subject
				.as_ref()
				.windows(distrusted.len())
				.any(|part| part == distrusted)
		);
	}
	assert!(!webpki_root_certs::TLS_SERVER_ROOT_CERTS.is_empty());
	for root in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
		assert!(
			!root
				.as_ref()
				.windows(distrusted.len())
				.any(|part| part == distrusted)
		);
	}
}
