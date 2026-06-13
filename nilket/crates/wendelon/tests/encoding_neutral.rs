use std::path::Path;
use std::str::FromStr;

use proc_macro2::{TokenStream, TokenTree};

fn read_module_concat(path: impl AsRef<std::path::Path>) -> String {
	let path = path.as_ref();
	let mut source = std::fs::read_to_string(path).unwrap();
	// ADR 078: storage/manager.rs is now a module dir; scan root + non-test submodules.
	if path.file_name().and_then(|s| s.to_str()) == Some("manager.rs")
		&& path
			.parent()
			.and_then(|p| p.file_name())
			.and_then(|s| s.to_str())
			== Some("storage")
	{
		let dir = path.with_extension("");
		if dir.is_dir() {
			let mut subs: Vec<_> = std::fs::read_dir(&dir)
				.unwrap()
				.flatten()
				.map(|e| e.path())
				.filter(|p| {
					p.extension().and_then(|s| s.to_str()) == Some("rs")
						&& p.file_name().and_then(|s| s.to_str()) != Some("tests.rs")
				})
				.collect();
			subs.sort();
			for p in subs {
				source.push('\n');
				source.push_str(&std::fs::read_to_string(p).unwrap());
			}
		}
	}
	source
}

fn rust_identifiers(root: &Path, relative: &str) -> Vec<String> {
	let source = read_module_concat(root.join(relative));
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
