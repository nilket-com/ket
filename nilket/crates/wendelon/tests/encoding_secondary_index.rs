use std::fs;
use std::path::Path;
use std::str::FromStr;

use proc_macro2::{TokenStream, TokenTree};

fn read_module_concat(path: impl AsRef<std::path::Path>, include_tests: bool) -> String {
	let path = path.as_ref();
	let mut source = std::fs::read_to_string(path).unwrap();
	// ADR 078: storage/manager.rs is now a module dir. Production guards exclude
	// tests.rs; behavior-coverage guards opt into it explicitly.
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
						&& (include_tests
							|| p.file_name().and_then(|s| s.to_str()) != Some("tests.rs"))
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

fn rust_identifiers(root: &Path, relative: &str, include_tests: bool) -> Vec<String> {
	let source = read_module_concat(root.join(relative), include_tests);
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
	let manager = rust_identifiers(root, "src/storage/manager.rs", false);
	let page_materialize =
		rust_identifiers(root, "src/storage/encoding/page_materialize.rs", false);
	let row_group = rust_identifiers(root, "src/storage/row_group.rs", false);

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
		"rebase_validity_bitmap",
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

#[test]
fn secondary_index_adr066_fsst_confirm_step_status_is_explicit() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manager = rust_identifiers(root, "src/storage/manager.rs", false);
	let manager_with_tests = rust_identifiers(root, "src/storage/manager.rs", true);
	let impl_plan = fs::read_to_string(root.join("plans/impl/066_fsst_evaluator.md")).unwrap();

	assert!(
		has_identifier(
			&manager_with_tests,
			"fsst_phase_b_l2_read_filters_a1_and_declines_a2_overlap"
		),
		"missing ADR 066 FSST confirm-step behavior test"
	);
	for token in [
		"read_projected_record_from_l2",
		"EncodingComputeEvaluatorKind",
		"FsstWholeBuffer",
	] {
		assert!(
			has_identifier(&manager, token),
			"missing ADR 066 FSST confirm-step related identifier {token}"
		);
	}
	for required in [
		"ADR 052 confirm-step row",
		"conditional per",
		"if not feasible, document as Phase 4",
	] {
		assert!(
			impl_plan.contains(required),
			"ADR 066 secondary-index confirm-step status is missing text {required}"
		);
	}
}
