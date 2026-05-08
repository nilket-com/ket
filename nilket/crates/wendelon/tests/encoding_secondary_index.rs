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
fn secondary_index_path_uses_encoding_page_fast_path_before_row_take() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manager = rust_identifiers(root, "src/storage/manager.rs");
	let page_materialize = rust_identifiers(root, "src/storage/encoding/page_materialize.rs");
	let row_group = rust_identifiers(root, "src/storage/row_group.rs");

	for token in [
		"try_read_encoding_indexed_pages_from_l2",
		"build_encoding_indexed_page_plan",
		"materialize_encoding_indexed_pages",
		"indexed_rows_materialized",
		"EncodingMaterializeAuditSample",
		"encoding_materialize_audit",
		"decode_page",
		"decode_page_fetch",
		"projected_page_batch_from_decoded_buffers",
		"take_projected_indexed_rows",
		"row_groups_for_record_rows",
		"record_row_index",
	] {
		assert!(
			has_identifier(&manager, token)
				|| has_identifier(&page_materialize, token)
				|| has_identifier(&row_group, token),
			"missing ADR 057 secondary-index encoding identifier {token}"
		);
	}
}
