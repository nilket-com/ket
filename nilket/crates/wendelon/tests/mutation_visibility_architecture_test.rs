fn exact_visibility_scope(vis: &syn::Visibility) -> bool {
	matches!(vis, syn::Visibility::Restricted(r) if r.in_token.is_some() && exact_path(&r.path, &["crate", "storage", "mutation_visibility"]))
}
fn exact_path(path: &syn::Path, expected: &[&str]) -> bool {
	path.leading_colon.is_none()
		&& path.segments.len() == expected.len()
		&& path
			.segments
			.iter()
			.zip(expected)
			.all(|(s, n)| s.ident == *n && matches!(s.arguments, syn::PathArguments::None))
}
// Conservative token guard: explicit capability literals are forbidden even in tests;
// tests use the reviewed cfg(test) synthetic method. Struct declarations are not literals.
// This also checks macro token bodies; it is not general Rust name resolution.
fn no_explicit_capability_literal(tokens: proc_macro2::TokenStream) -> bool {
	let ts: Vec<_> = tokens.into_iter().collect();
	for (i, t) in ts.iter().enumerate() {
		if matches!(t,proc_macro2::TokenTree::Ident(id) if id == "LoaderCapability2")
			&& matches!(ts.get(i+1),Some(proc_macro2::TokenTree::Ident(id)) if id=="as")
		{
			return false;
		}
		if matches!(t,proc_macro2::TokenTree::Ident(id) if id == "LoaderCapability2")
			&& matches!(ts.get(i+1),Some(proc_macro2::TokenTree::Group(g)) if g.delimiter()==proc_macro2::Delimiter::Brace)
			&& !matches!(i.checked_sub(1).and_then(|n|ts.get(n)),Some(proc_macro2::TokenTree::Ident(id)) if id == "struct" || id == "impl")
		{
			return false;
		}
		if let proc_macro2::TokenTree::Group(g) = t {
			if !no_explicit_capability_literal(g.stream()) {
				return false;
			}
		}
	}
	true
}

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
	let mut bounded_loaders = 0;
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
						if name == "from_bounded_rows" {
							bounded_loaders += 1;
							assert!(exact_visibility_scope(&f.vis));
							assert_eq!(
								f.sig.inputs.len(),
								4,
								"owned bytes only, no independent batch argument"
							);
							assert!(f.sig.inputs.iter().any(|arg| matches!(arg, syn::FnArg::Typed(t) if matches!(&*t.ty, syn::Type::Reference(r) if matches!(&*r.elem, syn::Type::Path(p) if exact_path(&p.path,&["crate","storage","mutation_visibility","root2","durable","loader","LoaderCapability2"]))))));
						} else if name.starts_with("from_") {
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
		bounded_loaders, 1,
		"bounded linkage requires an unconstructable scoped capability"
	);
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

#[test]
fn r0_assay_and_shared_allocator_are_below_test_only_modules() {
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility");
	for path in [root.join("mod.rs"), root.join("root2/mod.rs")] {
		let parsed = syn::parse_file(&fs::read_to_string(path).unwrap()).unwrap();
		let module = parsed
			.items
			.iter()
			.find_map(|i| match i {
				syn::Item::Mod(m) if m.ident == "tests" => Some(m),
				_ => None,
			})
			.unwrap();
		assert!(module.attrs.iter().any(|a| matches!(&a.meta, syn::Meta::List(m) if m.path.is_ident("cfg") && m.tokens.to_string() == "test")));
	}
	let assay = fs::read_to_string(root.join("root2/tests/r0_assay.rs")).unwrap();
	let parsed = syn::parse_file(&assay).unwrap();
	let test = parsed
		.items
		.iter()
		.find_map(|i| match i {
			syn::Item::Fn(f) if f.sig.ident == "r0_isolated_cost_assay" => Some(f),
			_ => None,
		})
		.unwrap();
	assert!(test.attrs.iter().any(|a| a.path().is_ident("test")));
	assert!(test.attrs.iter().any(|a| a.path().is_ident("ignore")));
	assert!(matches!(test.vis, syn::Visibility::Inherited));
	assert!(!assay.contains("tokio::spawn") && !assay.contains("thread::spawn"));
	let allocator = fs::read_to_string(root.join("tests/b1_assay.rs")).unwrap();
	assert_eq!(allocator.matches("#[global_allocator]").count(), 1);
	assert!(allocator.contains("r0_counters") && allocator.contains("r0_reset_peak"));
}

#[test]
fn s1c_incremental_updates_do_not_scan_or_create_authority_and_oracle_is_test_only() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	for name in ["persistent.rs", "stored.rs", "incremental.rs"] {
		let text = fs::read_to_string(root.join(name)).unwrap();
		syn::parse_file(&text).unwrap();
		assert!(text.lines().count() <= 750);
		assert!(!text.contains("unsafe "));
	}
	// Scan every local helper body reachable from fresh reduce, not only update.
	// Token groups include nested blocks/macros and exclude comments and literals.
	// This conservative module inventory is not Rust symbol-resolution/devirtualization.
	fn no_full_scan(tokens: proc_macro2::TokenStream, label: &str) {
		let ts = tokens.into_iter().collect::<Vec<_>>();
		for (i, t) in ts.iter().enumerate() {
			if let proc_macro2::TokenTree::Ident(id) = t {
				if [
					"validate_state",
					"usage",
					"capacity",
					"build",
					"encode_state",
				]
				.contains(&id.to_string().as_str())
					&& matches!(ts.get(i+1),Some(proc_macro2::TokenTree::Group(g)) if g.delimiter()==proc_macro2::Delimiter::Parenthesis)
				{
					panic!("full-state call in {label}: {id}");
				}
			}
			if let proc_macro2::TokenTree::Group(g) = t {
				no_full_scan(g.stream(), label);
			}
		}
	}
	fn functions(tokens: proc_macro2::TokenStream, module: &str) {
		let ts = tokens.into_iter().collect::<Vec<_>>();
		let mut i = 0;
		while i < ts.len() {
			if matches!(&ts[i],proc_macro2::TokenTree::Ident(id) if id=="fn") {
				let Some(proc_macro2::TokenTree::Ident(name)) = ts.get(i + 1) else {
					panic!("function name")
				};
				let mut j = i + 2;
				while j < ts.len() {
					if let proc_macro2::TokenTree::Group(body) = &ts[j] {
						if body.delimiter() == proc_macro2::Delimiter::Brace {
							let exempt = (module == "reducer.rs" && name == "bootstrap_root")
								|| (module == "incremental.rs" && name == "build")
								|| (module == "validation.rs"
									&& ["validate_state", "usage", "capacity"]
										.contains(&name.to_string().as_str()));
							if !exempt {
								no_full_scan(body.stream(), &format!("{module}::{name}"));
							}
							break;
						}
					}
					if matches!(&ts[j],proc_macro2::TokenTree::Punct(p) if p.as_char()==';') {
						break;
					}
					j += 1;
				}
				i = j + 1;
			} else {
				if let proc_macro2::TokenTree::Group(g) = &ts[i] {
					functions(g.stream(), module);
				}
				i += 1;
			}
		}
	}
	for module in [
		"reducer.rs",
		"incremental.rs",
		"validation.rs",
		"persistent.rs",
		"stored.rs",
		"command.rs",
		"records.rs",
		"codec.rs",
	] {
		let source = fs::read_to_string(root.join(module)).unwrap();
		syn::parse_file(&source).unwrap();
		functions(source.parse().unwrap(), module);
	}
	let reducer = fs::read_to_string(root.join("reducer.rs")).unwrap();
	assert_eq!(reducer.matches("Derived2::build(").count(), 1);
	assert_eq!(reducer.matches("return bootstrap_root(").count(), 1);
	let tests = fs::read_to_string(root.join("tests/mod.rs")).unwrap();
	assert!(tests.contains("mod s1c_oracle;"));
	let modules = fs::read_to_string(root.join("mod.rs")).unwrap();
	assert!(!modules.contains("mod s1c_oracle"));
	assert!(
		fs::read_to_string(root.join("reducer.rs"))
			.unwrap()
			.contains("command.digest")
	);
}

#[test]
fn r2_loader_capability_has_no_runtime_factory_and_content_is_not_readiness() {
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("src/storage/mutation_visibility/root2/durable");
	let source = fs::read_to_string(root.join("loader.rs")).unwrap();
	assert!(
		no_explicit_capability_literal(source.parse().unwrap()),
		"no free-function or macro capability literal"
	);
	let ast = syn::parse_file(&source).unwrap();
	let mut token = 0;
	let mut factories = 0;
	let mut result = 0;
	for item in ast.items {
		match item {
			syn::Item::Struct(s)
				if s.ident == "LoaderCapability2" || s.ident == "LoadedContent2" =>
			{
				assert!(exact_visibility_scope(&s.vis));
				assert!(s.attrs.iter().all(|a| !a.path().is_ident("derive")));
				for field in s.fields {
					assert!(matches!(field.vis, syn::Visibility::Inherited));
				}
				if s.ident == "LoaderCapability2" {
					token += 1
				} else {
					result += 1
				}
			}
			syn::Item::Impl(i) if matches!(&*i.self_ty,syn::Type::Path(p) if p.path.segments.last().is_some_and(|s|s.ident=="LoaderCapability2")) =>
			{
				assert!(i.trait_.is_none(), "no default/serde capability factory");
				assert!(
					matches!(&*i.self_ty,syn::Type::Path(p) if p.path.is_ident("LoaderCapability2")),
					"no qualified/aliased capability impl"
				);
				for member in i.items {
					let syn::ImplItem::Fn(f) = member else {
						panic!("review new capability member")
					};
					assert_eq!(f.sig.ident, "synthetic");
					assert!(f.attrs.iter().any(|a|matches!(&a.meta,syn::Meta::List(m) if m.path.is_ident("cfg")&&m.tokens.to_string()=="test")));
					factories += 1;
				}
			}
			_ => {}
		}
	}
	assert_eq!((token, result, factories), (1, 1, 1));
	let ast = syn::parse_file(&source).unwrap();
	for item in ast.items {
		if let syn::Item::Type(t) = item {
			assert!(
				!matches!(&*t.ty,syn::Type::Path(p) if p.path.segments.iter().any(|s|s.ident=="LoaderCapability2")),
				"no capability type alias"
			);
		}
	}
	for name in ["bundle.rs", "fixed.rs", "journal.rs", "witness.rs"] {
		let text = fs::read_to_string(root.join(name)).unwrap();
		for forbidden in [
			"TrustedEntry2::",
			"VerifiedCommand2::",
			"TrustedView2::",
			"AcceptedWrite",
			"std::fs::",
			"tokio::fs::",
		] {
			assert!(
				!text.contains(forbidden),
				"no authority constructor or live IO in structural codec"
			);
		}
	}
}

#[test]
fn r2_guard_refuses_broader_scope_foreign_type_and_free_factory_literal() {
	let vis: syn::Visibility = syn::parse_str("pub(crate)").unwrap();
	assert!(!exact_visibility_scope(&vis));
	assert!(exact_visibility_scope(
		&syn::parse_str("pub(in crate::storage::mutation_visibility)").unwrap()
	));
	assert!(!exact_path(
		&syn::parse_str("other::LoaderCapability2").unwrap(),
		&[
			"crate",
			"storage",
			"mutation_visibility",
			"root2",
			"durable",
			"loader",
			"LoaderCapability2"
		]
	));
	assert!(!no_explicit_capability_literal(
		"fn forged() -> Option<LoaderCapability2> { Some(LoaderCapability2 { scope: panic!() }) }"
			.parse()
			.unwrap()
	));
	assert!(no_explicit_capability_literal(
		"struct LoaderCapability2 { scope: () }".parse().unwrap()
	));
}

#[test]
fn r2_local_store_and_fault_model_remain_private_and_dormant() {
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("src/storage/mutation_visibility/root2/durable");
	let ast = syn::parse_file(&fs::read_to_string(root.join("mod.rs")).unwrap()).unwrap();
	let module = ast
		.items
		.iter()
		.find_map(|i| match i {
			syn::Item::Mod(m) if m.ident == "local_store" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(matches!(module.vis, syn::Visibility::Inherited));
	let ast =
		syn::parse_file(&fs::read_to_string(root.join("local_store/mod.rs")).unwrap()).unwrap();
	let tests = ast
		.items
		.iter()
		.find_map(|i| match i {
			syn::Item::Mod(m) if m.ident == "tests" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(tests.attrs.iter().any(
		|a| matches!(&a.meta,syn::Meta::List(m) if m.path.is_ident("cfg")&&m.tokens.to_string()=="test")
	));
	for name in [
		"mod.rs",
		"adapter.rs",
		"capacity.rs",
		"format.rs",
		"prospective.rs",
		"store.rs",
		"scan.rs",
		"recovery.rs",
		"repair.rs",
		"inspection.rs",
		"types.rs",
	] {
		let s = fs::read_to_string(root.join("local_store").join(name)).unwrap();
		syn::parse_file(&s).unwrap();
		assert!(no_explicit_capability_literal(s.parse().unwrap()));
		for forbidden in [
			"TestProofKey",
			"impl ProofKeyProvider",
			"TrustedEntry2::",
			"VerifiedCommand2::",
			"TrustedView2::",
			"AcceptedWrite",
			"tokio::time::sleep",
			"std::thread::sleep",
		] {
			assert!(
				!s.contains(forbidden),
				"{name}: dormant physical IO must not activate authority"
			);
		}
	}
	let store = fs::read_to_string(root.join("local_store/store.rs")).unwrap();
	assert!(store.contains("tokio::spawn") && store.contains("parent_operation"));
	let model = fs::read_to_string(root.join("local_store/tests/model.rs")).unwrap();
	assert!(model.contains("snapshot_file") && model.contains("snapshot_names"));
}

// Fixed-math subtree cannot mention known trust/capability or signing producers, even
// through an alias or macro token body. This is conservative syntax, not Rust name resolution.
fn no_auth_authority(tokens: proc_macro2::TokenStream) -> bool {
	for token in tokens {
		match token {
			proc_macro2::TokenTree::Ident(id) => {
				let name = id.to_string();
				if name.starts_with("Trusted")
					|| name.starts_with("Verified")
					|| name.starts_with("Authenticated")
					|| [
						"LoaderCapability2",
						"Root2",
						"UnverifiedRoot2",
						"ProofKeyProvider",
						"SigningKey",
						"Signer",
						"sign",
						"sign_strict",
						"serde",
						"Serialize",
						"Deserialize",
						"Default",
					]
					.contains(&name.as_str())
				{
					return false;
				}
			}
			proc_macro2::TokenTree::Group(group) if !no_auth_authority(group.stream()) => {
				return false;
			}
			_ => {}
		}
	}
	true
}
#[test]
fn auth_math_stays_private_and_has_no_capability_or_signing_producer() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let parsed = syn::parse_file(&fs::read_to_string(root.join("mod.rs")).unwrap()).unwrap();
	let auth = parsed
		.items
		.iter()
		.find_map(|item| match item {
			syn::Item::Mod(m) if m.ident == "auth" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(matches!(auth.vis, syn::Visibility::Inherited));
	let module = syn::parse_file(&fs::read_to_string(root.join("auth/mod.rs")).unwrap()).unwrap();
	for item in module.items {
		match item {
			syn::Item::Mod(m) if m.ident == "tests" => assert!(m.attrs.iter().any(|a| matches!(&a.meta,syn::Meta::List(m) if m.path.is_ident("cfg") && m.tokens.to_string()=="test"))),
			syn::Item::Mod(m) => {
 assert!(["errors", "preimage", "signature"].iter().any(|name| m.ident == *name));
 assert!(matches!(m.vis, syn::Visibility::Public(_))); // Private auth parent and root2-scoped items prevent escape.
},
			syn::Item::Use(u) => assert!(matches!(u.vis,syn::Visibility::Inherited)),
			_ => panic!("auth mod.rs must be declarative"),
		}
	}
	let mut entrypoints = Vec::new();
	for name in ["errors.rs", "preimage.rs", "signature.rs"] {
		let text = fs::read_to_string(root.join("auth").join(name)).unwrap();
		assert!(
			no_auth_authority(text.parse().unwrap()),
			"{name}: no known authority/capability/signing escape"
		);
		let ast = syn::parse_file(&text).unwrap();
		for item in ast.items {
			match item {
				syn::Item::Fn(f) => {
					assert!(!matches!(f.vis, syn::Visibility::Public(_)));
					if matches!(&f.vis,syn::Visibility::Restricted(r) if r.in_token.is_some()) {
						assert!(
							matches!(&f.vis,syn::Visibility::Restricted(r) if exact_path(&r.path,&["crate","storage","mutation_visibility","root2"]))
						);
						entrypoints.push(f.sig.ident.to_string());
					}
				}
				syn::Item::Enum(e) => {
					assert_eq!(e.ident, "AuthError2");
					let variants = e
						.variants
						.iter()
						.map(|v| v.ident.to_string())
						.collect::<Vec<_>>();
					assert_eq!(
						variants,
						[
							"Structural",
							"Limit",
							"Overflow",
							"ScopeMismatch",
							"CryptoInvalid"
						]
					);
				}
				syn::Item::Type(t) => assert_eq!(t.ident, "AuthResult2"),
				syn::Item::Struct(_) | syn::Item::Trait(_) => {
					panic!("no producer/token/provider types")
				}
				_ => {}
			}
		}
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"admission_preimage2",
			"claim_mac_preimage2",
			"holder_preimage2",
			"verify_admission_signature2",
			"verify_holder_signature2"
		]
	);
}
#[test]
fn auth_boundary_negative_examples_reject_known_constructor_escapes() {
	for text in [
		"fn promote() -> TrustedEntry2 { todo!() }",
		"use crate::VerifiedCommand2 as BoolResult;",
		"type Alias = TrustedView2;",
		"impl From<bool> for LoaderCapability2 {}",
		"factory! { AuthenticatedReceipt2 { authorized: true } }",
		"impl Default for TrustedPublicContext2 {}",
		"#[derive(serde::Deserialize)] struct VerifiedContext2;",
		"fn verify() { SigningKey::from_bytes(&key).sign(bytes); }",
	] {
		assert!(!no_auth_authority(text.parse().unwrap()), "{text}");
	}
	assert!(no_auth_authority(
		"fn verify(bytes: &[u8]) -> Result<(), AuthError2> { Ok(()) }"
			.parse()
			.unwrap()
	));
}
// Administrator-manifest math: the auth prohibitions plus no I/O, command/state or root access.
// Conservative syntax over these files only; it is neither name resolution nor provenance evidence.
fn no_admin_authority(tokens: proc_macro2::TokenStream) -> bool {
	fn walk(tokens: proc_macro2::TokenStream) -> bool {
		tokens.into_iter().all(|t| match t {
			proc_macro2::TokenTree::Ident(id) => ![
				"fs", "io", "net", "tokio", "File", "config", "Config", "Command2", "State2",
				"Limits2", "Usage2", "envelope", "open",
			]
			.contains(&id.to_string().as_str()),
			proc_macro2::TokenTree::Group(g) => walk(g.stream()),
			_ => true,
		})
	}
	no_auth_authority(tokens.clone()) && walk(tokens)
}
fn root2_scoped(vis: &syn::Visibility) -> bool {
	matches!(vis, syn::Visibility::Restricted(r) if r.in_token.is_some() && exact_path(&r.path, &["crate", "storage", "mutation_visibility", "root2"]))
}
// Returns the root2-scoped entrypoint names, or None if any item widens visibility or adds a producer shape.
fn admin_items(file: &syn::File) -> Option<Vec<String>> {
	let mut entrypoints = Vec::new();
	for item in &file.items {
		match item {
			syn::Item::Fn(f) => {
				if root2_scoped(&f.vis) {
					entrypoints.push(f.sig.ident.to_string());
				} else if !matches!(f.vis, syn::Visibility::Inherited) {
					return None;
				}
			}
			syn::Item::Const(c)
				if !(root2_scoped(&c.vis) || matches!(c.vis, syn::Visibility::Inherited)) =>
			{
				return None;
			}
			syn::Item::Type(t)
				if !(root2_scoped(&t.vis)
					&& ["AdminResult2", "BootstrapResult2"]
						.iter()
						.any(|n| t.ident == *n)) =>
			{
				return None;
			}
			syn::Item::Enum(e)
				if !(root2_scoped(&e.vis)
					&& [
						"AdminManifestError2",
						"BootstrapRecordError2",
						"BootstrapField2",
					]
					.iter()
					.any(|n| e.ident == *n)) =>
			{
				return None;
			}
			// Fields are declared pub inside root2-private types; the type itself bounds their reach.
			syn::Item::Struct(s)
				if !(root2_scoped(&s.vis)
					&& [
						"AdminManifest2",
						"AdminNamespace2",
						"BootstrapIdentity2",
						"BootstrapRecord2",
						"BootstrapObservation2",
					]
					.iter()
					.any(|n| s.ident == *n)) =>
			{
				return None;
			}
			syn::Item::Impl(i) => {
				if i.trait_.is_some() {
					return None;
				}
				for it in &i.items {
					match it {
						syn::ImplItem::Fn(f) if root2_scoped(&f.vis) => {
							entrypoints.push(f.sig.ident.to_string())
						}
						syn::ImplItem::Fn(f) if matches!(f.vis, syn::Visibility::Inherited) => {}
						_ => return None,
					}
				}
			}
			syn::Item::Use(u) if !matches!(u.vis, syn::Visibility::Inherited) => return None,
			syn::Item::Trait(_) | syn::Item::Mod(_) | syn::Item::Static(_) => return None,
			_ => {}
		}
	}
	Some(entrypoints)
}
#[test]
fn admin_manifest_math_stays_private_and_has_no_provenance_producer() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let parsed = syn::parse_file(&fs::read_to_string(root.join("mod.rs")).unwrap()).unwrap();
	let admin = parsed
		.items
		.iter()
		.find_map(|item| match item {
			syn::Item::Mod(m) if m.ident == "admin" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(matches!(admin.vis, syn::Visibility::Inherited));
	fn mentions(tree: &syn::UseTree, name: &str) -> bool {
		match tree {
			syn::UseTree::Path(p) => p.ident == name || mentions(&p.tree, name),
			syn::UseTree::Name(n) => n.ident == name,
			syn::UseTree::Rename(r) => r.ident == name,
			syn::UseTree::Group(g) => g.items.iter().any(|t| mentions(t, name)),
			syn::UseTree::Glob(_) => false,
		}
	}
	assert!(
		!parsed
			.items
			.iter()
			.any(|i| matches!(i, syn::Item::Use(u) if mentions(&u.tree, "admin")))
	);
	let module = syn::parse_file(&fs::read_to_string(root.join("admin/mod.rs")).unwrap()).unwrap();
	let mut children = Vec::new();
	for item in module.items {
		match item {
			syn::Item::Mod(m) if m.ident == "tests" => assert!(m.attrs.iter().any(|a| matches!(&a.meta,syn::Meta::List(m) if m.path.is_ident("cfg") && m.tokens.to_string()=="test"))),
			// Declarative children: pub only within the private admin parent.
			syn::Item::Mod(m) if m.content.is_none() && matches!(m.vis, syn::Visibility::Public(_)) => children.push(m.ident.to_string()),
			_ => panic!("admin mod.rs must be declarative"),
		}
	}
	children.sort();
	assert_eq!(children, ["errors", "key", "manifest", "record"]);
	let mut entrypoints = Vec::new();
	for name in ["errors.rs", "key.rs", "manifest.rs", "record.rs"] {
		let text = fs::read_to_string(root.join("admin").join(name)).unwrap();
		assert!(
			no_admin_authority(text.parse().unwrap()),
			"{name}: prohibited token"
		);
		entrypoints.extend(
			admin_items(&syn::parse_file(&text).unwrap())
				.unwrap_or_else(|| panic!("{name}: widened or producer item")),
		);
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"admin_manifest_digest2",
			"check_admin_manifest_digest2",
			"check_enrollable_admin_key2",
			"decode",
			"decode",
			"decode",
			"encode",
			"encode",
			"encode",
			"first_mismatch"
		]
	);
}
#[test]
fn admin_guard_negative_examples_reject_widening_constructors_signing_and_io() {
	for text in [
		"fn install() -> TrustedView2 { todo!() }",
		"fn read(p: &str) { std::fs::read(p).unwrap(); }",
		"async fn fetch() { tokio::spawn(async {}); }",
		"fn sign() { SigningKey::from_bytes(&k).sign(b); }",
		"fn pin(c: &Command2) {}",
		"#[derive(serde::Serialize)] struct AdminManifest2;",
	] {
		assert!(!no_admin_authority(text.parse().unwrap()), "{text}");
	}
	for text in [
		"pub fn decode() {}",
		"pub(crate) fn decode() {}",
		"pub(in crate::storage::mutation_visibility) fn decode() {}",
		"pub(in crate::storage::mutation_visibility::root2) struct AdminPin2;",
		"impl Default for AdminManifest2 { fn default() -> Self { todo!() } }",
		"impl AdminManifest2 { pub fn widen() {} }",
		"pub use super::manifest::AdminManifest2;",
		"pub(in crate::storage::mutation_visibility::root2) trait Provider {}",
	] {
		assert!(
			admin_items(&syn::parse_file(text).unwrap()).is_none(),
			"{text}"
		);
	}
	let ok = "pub(in crate::storage::mutation_visibility::root2) fn decode(b: &[u8]) -> AdminResult2<()> { Ok(()) }";
	assert!(no_admin_authority(ok.parse().unwrap()));
	assert_eq!(
		admin_items(&syn::parse_file(ok).unwrap()),
		Some(vec!["decode".to_string()])
	);
}
