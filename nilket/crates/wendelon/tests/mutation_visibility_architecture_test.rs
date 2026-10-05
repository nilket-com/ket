use std::{fs, path::PathBuf};

#[test]
fn dormant_authority_has_no_production_provider_or_activation() {
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	let module = root.join("src/storage/mutation_visibility");
	let provider = fs::read_to_string(module.join("commitment.rs")).unwrap();
	let ast = syn::parse_file(&provider).unwrap();
	let test_only = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| matches!(&a.meta,syn::Meta::List(m) if m.path.is_ident("cfg") && m.tokens.to_string()=="test"))
	};
	let mut providers = 0;
	for item in ast.items {
		match item {
			syn::Item::Struct(s) if s.ident == "TestProofKey" => assert!(test_only(&s.attrs)),
			syn::Item::Impl(i)
				if i.trait_.as_ref().is_some_and(|(_, p, _)| {
					p.segments
						.last()
						.is_some_and(|s| s.ident == "ProofKeyProvider")
				}) =>
			{
				providers += 1;
				assert!(test_only(&i.attrs));
			}
			_ => {}
		}
	}
	assert_eq!(providers, 1);
	for name in [
		"authority.rs",
		"codec.rs",
		"commitment.rs",
		"encoding.rs",
		"error.rs",
		"identity.rs",
		"manifest.rs",
		"mod.rs",
	] {
		let source = fs::read_to_string(module.join(name)).unwrap();
		syn::parse_file(&source).unwrap();
		assert!(
			source.lines().count() <= 750,
			"{name}: keep substrate modules bounded"
		);
	}
	// Existing live formats are deliberately untouched; no boot/WAL/reader wiring.
	let core = fs::read_to_string(root.join("src/schema/core.rs")).unwrap();
	assert!(core.contains("SNAPSHOT_FORMAT_VERSION: u8 = 5"));
	assert!(core.contains("WAL_FORMAT_VERSION: u8 = 4"));
	fn visit(dir: PathBuf, module: &PathBuf) {
		for entry in fs::read_dir(&dir).unwrap() {
			let p = entry.unwrap().path();
			if p == *module {
				continue;
			}
			if p.is_dir() {
				visit(p, module);
			} else if p.extension().is_some_and(|e| e == "rs")
				&& p.file_name().is_some_and(|n| n != "mod.rs")
			{
				let text = fs::read_to_string(p).unwrap();
				assert!(
					!text.contains("storage::mutation_visibility"),
					"no production activation in slice A"
				);
			}
		}
	}
	visit(root.join("src"), &module);
}
