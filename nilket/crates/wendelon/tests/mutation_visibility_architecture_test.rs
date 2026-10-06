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
	// B.1 is still dormant; validate every new non-test module and constructor gate.
	fn bounded_modules(dir: &std::path::Path) {
		for entry in fs::read_dir(dir).unwrap() {
			let p = entry.unwrap().path();
			if p.file_name()
				.is_some_and(|n| n == "tests" || n == "fixtures" || n == "tests.rs")
			{
				continue;
			}
			if p.is_dir() {
				bounded_modules(&p)
			} else if p.extension().is_some_and(|e| e == "rs") {
				let s = fs::read_to_string(&p).unwrap();
				syn::parse_file(&s).unwrap();
				assert!(
					s.lines().count() <= 750,
					"{}: split bounded modules",
					p.display()
				);
			}
		}
	}
	bounded_modules(&module);
	let source = fs::read_to_string(module.join("provenance/verification.rs")).unwrap();
	let ast = syn::parse_file(&source).unwrap();
	let mut constructors = 0;
	for item in ast.items {
		match item {
			syn::Item::Struct(s) if s.ident == "TrustedFrameRows" => {
				for f in &s.fields {
					assert!(matches!(f.vis, syn::Visibility::Inherited))
				}
			}
			syn::Item::Impl(i) => {
				for member in i.items {
					if let syn::ImplItem::Fn(f) = member {
						let name = f.sig.ident.to_string();
						if name.starts_with("from_") {
							constructors += 1;
							assert!(test_only(&f.attrs));
							assert!(!matches!(f.vis, syn::Visibility::Public(_)));
						}
					}
				}
			}
			_ => {}
		}
	}
	assert_eq!(
		constructors, 2,
		"all linkage constructors must remain synthetic-only"
	);
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

#[test]
fn s1_private_trust_tokens_and_fake_install_are_test_only() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let test_only = |attrs: &[syn::Attribute]| {
		attrs.iter().any(
			|a| matches!(&a.meta,syn::Meta::List(m) if m.path.is_ident("cfg")&&m.tokens.to_string()=="test"),
		)
	};
	let trust = syn::parse_file(&fs::read_to_string(root.join("trust.rs")).unwrap()).unwrap();
	let mut tokens = 0;
	let mut factories = 0;
	for item in trust.items {
		match item {
			syn::Item::Struct(s) => {
				tokens += 1;
				assert!(
					s.attrs.iter().all(|a| !a.path().is_ident("derive")),
					"no default/serde trust token"
				);
				for f in s.fields {
					assert!(matches!(f.vis, syn::Visibility::Inherited));
				}
			}
			syn::Item::Impl(i) => {
				for x in i.items {
					if let syn::ImplItem::Fn(f) = x {
						if f.sig.ident == "fixture"
							|| f.sig.ident == "unauthorized"
							|| f.sig.ident == "change_fingerprint"
						{
							factories += 1;
							assert!(test_only(&f.attrs));
							assert!(!matches!(f.vis, syn::Visibility::Public(_)));
						} else {
							assert!(
								f.sig.ident == "fields"
									|| f.sig.ident == "matches" || f.sig.ident == "authorized"
									|| f.sig.ident == "historical"
									|| f.sig.ident == "evidence",
								"new factory requires review"
							);
						}
					}
				}
			}
			_ => {}
		}
	}
	assert_eq!(tokens, 3);
	assert_eq!(factories, 5);
	let state = syn::parse_file(&fs::read_to_string(root.join("state.rs")).unwrap()).unwrap();
	let mut installs = 0;
	for item in state.items {
		if let syn::Item::Impl(i) = item {
			for x in i.items {
				if let syn::ImplItem::Fn(f) = x {
					if f.sig.ident == "fixture_install"
						|| f.sig.ident == "fixture_fields"
						|| f.sig.ident == "lower_limits"
					{
						assert!(test_only(&f.attrs));
						installs += 1;
					}
					if f.sig.ident == "decode" {
						assert!(
							matches!(&*i.self_ty,syn::Type::Path(p) if p.path.is_ident("UnverifiedRoot2"))
						);
					}
				}
			}
		}
	}
	assert_eq!(installs, 3);
	let reducer = fs::read_to_string(root.join("reducer.rs")).unwrap();
	assert!(reducer.contains("TrustedEntry2"));
	assert!(reducer.contains("VerifiedCommand2"));
	assert!(reducer.contains("command.digest"));
	assert!(!reducer.contains("Instant::now"));
	assert!(!reducer.contains("SystemTime::now"));
	assert!(!reducer.contains(".await"));
}
