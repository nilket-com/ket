//! Shared TLS / PKI substrate for Nilket rings.
//!
//! This crate owns policy-neutral certificate material loading, small
//! DER/CBOR parsing primitives, and rustls config assembly. Ring-local
//! admission policy remains in the consuming crates.

#![deny(unsafe_code)]

pub mod asn1;
pub mod cbor;
pub mod config_builder;
pub mod development;
pub mod error;
pub mod material;
pub mod san;
pub mod verifier;

pub use asn1::{DerReader, Subject, extract_dns_sans, extract_extension, extract_subject};
pub use cbor::CborReader;
pub use config_builder::{
	build_rustls_client_config, build_rustls_client_config_with_alpns, build_rustls_server_config,
	build_rustls_server_config_with_alpns, install_rustls_provider,
};
pub use development::{DevelopmentPki, write_local_development_pki};
pub use error::PkiError;
pub use material::{TlsMaterial, load_ca_roots, load_cert_chain, load_private_key};
pub use san::{PeerIdentity, admit_san, canonical_dns_name};
pub use verifier::{build_client_cert_chain_verifier, build_server_cert_chain_verifier};

pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";
