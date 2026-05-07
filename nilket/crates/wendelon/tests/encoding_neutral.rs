use std::fs;
use std::path::Path;
use std::str::FromStr;

use proc_macro2::{TokenStream, TokenTree};

fn rust_identifiers(root: &Path, relative: &str) -> Vec<String> {
	let source = fs::read_to_string(root.join(relative)).unwrap();
	let tokens = TokenStream::from_str(&source).unwrap_or_else(|err| {
		panic!("{relative}: Rust tokenization failed: {err}");
	});
	let mut identifiers = Vec::new();
	collect_identifiers(tokens, &mut identifiers);
	identifiers
}

fn collect_identifiers(tokens: TokenStream, identifiers: &mut Vec<String>) {
	for token in tokens {
		match token {
			TokenTree::Ident(ident) => identifiers.push(ident.to_string()),
			TokenTree::Group(group) => collect_identifiers(group.stream(), identifiers),
			TokenTree::Punct(_) | TokenTree::Literal(_) => {}
		}
	}
}

fn has_identifier(identifiers: &[String], ident: &str) -> bool {
	identifiers.iter().any(|candidate| candidate == ident)
}

#[test]
fn cdc_recovery_and_rehydration_stay_byte_neutral_to_encoding_internals() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let cdc = rust_identifiers(root, "src/storage/cdc.rs");
	let multirow = rust_identifiers(root, "src/storage/multirow.rs");
	let manager = rust_identifiers(root, "src/storage/manager.rs");
	let data_server = rust_identifiers(root, "src/network/data_server.rs");

	for forbidden in ["EncodingDirectory", "EncodingCascade", "BufferEncoding"] {
		assert!(
			!has_identifier(&cdc, forbidden),
			"CDC must not import encoding internals directly: {forbidden}"
		);
	}
	assert!(has_identifier(&multirow, "materialize_encoding_ipc_stream"));
	assert!(has_identifier(
		&manager,
		"read_full_projected_record_from_l2"
	));
	assert!(has_identifier(&manager, "decode_full_projected_l2_record"));
	assert!(has_identifier(
		&data_server,
		"emit_encoding_invariant_audit_for_error"
	));
}
