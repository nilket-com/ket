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
	let mut production_factories = 0;
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
						} else if f.sig.ident == "from_source" {
							// Stage A0: the one production factory, on CommittedEntry2 only, and
							// only with the committed-source capability as its first parameter.
							production_factories += 1;
							assert!(!test_only(&f.attrs));
							assert!(
								matches!(&*i.self_ty, syn::Type::Path(p) if p.path.is_ident("CommittedEntry2"))
							);
							assert!(
								matches!(&f.vis, syn::Visibility::Restricted(r) if r.path.is_ident("super"))
							);
							let first = f.sig.inputs.first().expect("capability parameter");
							assert!(
								matches!(first, syn::FnArg::Typed(t) if matches!(&*t.ty, syn::Type::Reference(r) if r.mutability.is_none() && matches!(&*r.elem, syn::Type::Path(p) if p.path.is_ident("CommittedSource2")))),
								"from_source requires &CommittedSource2"
							);
						} else {
							assert!(
								f.sig.ident == "fields"
									|| f.sig.ident == "matches" || f.sig.ident == "authorized"
									|| f.sig.ident == "historical"
									|| f.sig.ident == "evidence"
									|| f.sig.ident == "entry_fields",
								"new factory requires review"
							);
						}
					}
				}
			}
			_ => {}
		}
	}
	// Fourth token: CommittedEntry2 (origin contract r4 E1), whose only factory is a test fixture.
	assert_eq!(tokens, 4);
	assert_eq!(factories, 6);
	assert_eq!(production_factories, 1);
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
		"admission_gate.rs",
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
// Entry identity is byte math over caller-supplied decoded values: no openraft entry adapter,
// no I/O, no trust/state/reducer access and no buffer proportional to the payload.
// Conservative syntax over this file only; it is not name resolution.
#[test]
fn entry_identity_math_has_no_entry_adapter_trust_or_payload_buffer() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let parsed = syn::parse_file(&fs::read_to_string(root.join("mod.rs")).unwrap()).unwrap();
	let module = parsed
		.items
		.iter()
		.find_map(|item| match item {
			syn::Item::Mod(m) if m.ident == "entry_identity" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(matches!(module.vis, syn::Visibility::Inherited));
	let text = fs::read_to_string(root.join("entry_identity.rs")).unwrap();
	fn walk(tokens: proc_macro2::TokenStream) -> bool {
		tokens.into_iter().all(|t| match t {
			proc_macro2::TokenTree::Ident(id) => {
				let name = id.to_string();
				!(name.starts_with("Trusted")
					|| name.starts_with("Verified")
					|| [
						"Entry",
						"EntryPayload",
						"LogId",
						"raft_storage",
						"fs",
						"io",
						"net",
						"tokio",
						"Command2",
						"Root2",
						"State2",
						"Vec",
						"to_allocvec",
						"to_stdvec",
						"to_io",
						"SigningKey",
					]
					.contains(&name.as_str()))
			}
			proc_macro2::TokenTree::Group(g) => walk(g.stream()),
			_ => true,
		})
	}
	assert!(
		walk(text.parse().unwrap()),
		"entry_identity.rs: prohibited token"
	);
	let test_only = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| matches!(&a.meta, syn::Meta::List(m) if m.path.is_ident("cfg") && m.tokens.to_string() == "test"))
	};
	let mut entrypoints = Vec::new();
	for item in syn::parse_file(&text).unwrap().items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => entrypoints.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) if test_only(&f.attrs) => {
				assert!(!matches!(f.vis, syn::Visibility::Public(_)))
			}
			syn::Item::Fn(f) => assert!(
				matches!(f.vis, syn::Visibility::Inherited),
				"{}",
				f.sig.ident
			),
			syn::Item::Struct(s) => assert!(
				matches!(s.vis, syn::Visibility::Inherited)
					|| (root2_scoped(&s.vis) && s.ident == "EntryPosition2")
			),
			syn::Item::Enum(e) => assert!(root2_scoped(&e.vis) && e.ident == "EntryIdentityError2"),
			syn::Item::Impl(i) => assert!(i.items.iter().all(
				|it| matches!(it, syn::ImplItem::Fn(f) if matches!(f.vis, syn::Visibility::Inherited))
					|| matches!(it, syn::ImplItem::Type(_))
			)),
			syn::Item::Use(u) => assert!(matches!(u.vis, syn::Visibility::Inherited)),
			syn::Item::Trait(_) | syn::Item::Mod(_) | syn::Item::Static(_) => {
				panic!("no traits/modules/statics")
			}
			_ => {}
		}
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"blank_entry_identity2",
			"membership_entry_identity2",
			"normal_entry_identity2"
		]
	);
}
// The origin layer consumes opaque trusted inputs and never builds them, never turns decoded bytes
// into a root, and does no I/O or clock reads. Syntax over origin.rs and the one state.rs accessor.
#[test]
fn origin_layer_builds_no_trusted_input_and_promotes_no_bytes() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let parsed = syn::parse_file(&fs::read_to_string(root.join("mod.rs")).unwrap()).unwrap();
	let module = parsed
		.items
		.iter()
		.find_map(|item| match item {
			syn::Item::Mod(m) if m.ident == "origin" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(matches!(module.vis, syn::Visibility::Inherited));
	let text = fs::read_to_string(root.join("origin.rs")).unwrap();
	let file = syn::parse_file(&text).unwrap();
	let test_only = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| matches!(&a.meta, syn::Meta::List(m) if m.path.is_ident("cfg") && m.tokens.to_string() == "test"))
	};
	fn forbidden(tokens: proc_macro2::TokenStream) -> bool {
		let ts: Vec<_> = tokens.into_iter().collect();
		for (i, t) in ts.iter().enumerate() {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if [
						"fs",
						"io",
						"net",
						"tokio",
						"Instant",
						"SystemTime",
						"await",
						"fixture_install",
						"fixture",
						"unauthorized",
						"change_fingerprint",
					]
					.contains(&n.as_str())
					{
						return true;
					}
					// `Root2 { .. }`, `TrustedEntry2::..`, `VerifiedCommand2::..`, `CommittedEntry2::..` constructions.
					let next = ts.get(i + 1);
					if n == "Root2"
						&& matches!(next, Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Brace)
					{
						return true;
					}
					if [
						"TrustedEntry2",
						"VerifiedCommand2",
						"CommittedEntry2",
						"TrustedView2",
					]
					.contains(&n.as_str())
						&& matches!(next, Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == ':')
					{
						return true;
					}
				}
				proc_macro2::TokenTree::Group(g) if forbidden(g.stream()) => return true,
				_ => {}
			}
		}
		false
	}
	assert!(
		!forbidden(without_test_items(text.parse().unwrap())),
		"origin.rs: prohibited construction or effect outside cfg(test)"
	);
	let mut entrypoints = Vec::new();
	for item in &file.items {
		let attrs = match item {
			syn::Item::Fn(f) => &f.attrs,
			syn::Item::Impl(i) => &i.attrs,
			_ => continue,
		};
		if test_only(attrs) {
			continue;
		}
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => entrypoints.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => assert!(matches!(f.vis, syn::Visibility::Inherited)),
			syn::Item::Impl(i) => {
				assert!(i.trait_.is_none(), "no trait impls in origin.rs");
				for it in &i.items {
					if let syn::ImplItem::Fn(f) = it {
						if root2_scoped(&f.vis) {
							entrypoints.push(f.sig.ident.to_string());
						} else {
							assert!(matches!(f.vis, syn::Visibility::Inherited));
						}
					}
				}
			}
			_ => {}
		}
	}
	for item in &file.items {
		if let syn::Item::Fn(f) = item {
			if test_only(&f.attrs) {
				assert!(!matches!(f.vis, syn::Visibility::Public(_)));
			}
		}
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"advance_admission2",
			"advance_cover2",
			"advance_pre_root2",
			"advance_root2",
			"bootstrap2",
			"coverage2",
			"decode",
			"empty_authority2",
			"encode",
			"encode_authority2",
			"root_and_origin2"
		]
	);
	// The one state.rs accessor returns exactly four scalars.
	let state = fs::read_to_string(root.join("state.rs")).unwrap();
	let accessor = state
		.split("fn origin_anchor_fields2(")
		.nth(1)
		.expect("accessor present");
	let signature: String = accessor
		.split('{')
		.next()
		.unwrap()
		.split_whitespace()
		.collect();
	assert_eq!(signature, "&self,)->(Log2,Log2,Digest2,Digest2)");
}
// Top-level tokens of a file with every `#[cfg(test)]` item (through its body group) removed.
fn without_test_items(tokens: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
	let ts: Vec<_> = tokens.into_iter().collect();
	let mut out = Vec::new();
	let mut i = 0;
	while i < ts.len() {
		let cfg_test = matches!(&ts[i], proc_macro2::TokenTree::Punct(p) if p.as_char() == '#')
			&& matches!(ts.get(i + 1), Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Bracket && g.stream().to_string().replace(' ', "") == "cfg(test)");
		if cfg_test {
			i += 2;
			while i < ts.len()
				&& !matches!(&ts[i], proc_macro2::TokenTree::Group(g) if g.delimiter() == proc_macro2::Delimiter::Brace)
			{
				i += 1;
			}
			i += 1;
			continue;
		}
		out.push(ts[i].clone());
		i += 1;
	}
	out.into_iter().collect()
}
// Provisioning reader: private dormant module, exactly two SAFETY-commented unsafe blocks in
// anchored.rs outside cfg(test), a test-only error seam, and no network, trust or root access.
// Syntax over these files only; not name resolution.
#[test]
fn provisioning_reader_has_two_documented_unsafe_blocks_and_no_trust_access() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let parsed = syn::parse_file(&fs::read_to_string(root.join("mod.rs")).unwrap()).unwrap();
	let module = parsed
		.items
		.iter()
		.find_map(|item| match item {
			syn::Item::Mod(m) if m.ident == "provisioning" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(matches!(module.vis, syn::Visibility::Inherited));
	let decl =
		syn::parse_file(&fs::read_to_string(root.join("provisioning/mod.rs")).unwrap()).unwrap();
	let mut children = Vec::new();
	for item in decl.items {
		match item {
			syn::Item::Mod(m) if m.ident == "tests" => assert!(m.attrs.iter().any(|a| matches!(&a.meta, syn::Meta::List(l) if l.path.is_ident("cfg") && l.tokens.to_string() == "test"))),
			syn::Item::Mod(m) if m.content.is_none() => children.push(m.ident.to_string()),
			_ => panic!("provisioning/mod.rs must be declarative"),
		}
	}
	assert_eq!(
		children,
		[
			"anchored",
			"inventory",
			"owner_core",
			"owner_log",
			"replica_anchor",
			"replica_read",
			"snapshot"
		]
	);
	let text = fs::read_to_string(root.join("provisioning/anchored.rs")).unwrap();
	let production = without_test_items(text.parse().unwrap());
	fn unsafe_blocks(tokens: proc_macro2::TokenStream) -> usize {
		tokens
			.into_iter()
			.map(|t| match t {
				proc_macro2::TokenTree::Ident(id) if id == "unsafe" => 1,
				proc_macro2::TokenTree::Group(g) => unsafe_blocks(g.stream()),
				_ => 0,
			})
			.sum()
	}
	assert_eq!(
		unsafe_blocks(production.clone()),
		2,
		"exactly two production unsafe blocks"
	);
	let lines: Vec<&str> = text.lines().collect();
	let mut documented = 0;
	for (i, line) in lines.iter().enumerate() {
		if line.contains("unsafe {") {
			let before = lines[..i]
				.iter()
				.rev()
				.take(6)
				.any(|l| l.trim_start().starts_with("// SAFETY:"));
			assert!(before, "unsafe at line {} lacks a SAFETY comment", i + 1);
			documented += 1;
		}
	}
	assert_eq!(documented, 2);
	fn banned(tokens: proc_macro2::TokenStream) -> bool {
		tokens.into_iter().any(|t| match t {
			proc_macro2::TokenTree::Ident(id) => {
				let n = id.to_string();
				n.starts_with("Trusted")
					|| n.starts_with("Verified")
					|| [
						"net",
						"tokio",
						"Root2",
						"CommittedEntry2",
						"State2",
						"Command2",
						"TcpStream",
						"UdpSocket",
					]
					.contains(&n.as_str())
			}
			proc_macro2::TokenTree::Group(g) => banned(g.stream()),
			_ => false,
		})
	}
	assert!(
		!banned(text.parse().unwrap()),
		"anchored.rs: network, trust or root access"
	);
	// The error seam exists only under cfg(test): its state and function are cfg(test) items, and
	// the single call site is a cfg(test) statement.
	assert!(
		!production.to_string().contains("INJECTED_OPEN_ERRNO"),
		"test seam state outside cfg(test)"
	);
	let calls: Vec<usize> = lines
		.iter()
		.enumerate()
		.filter(|(_, l)| l.contains("injected_open_error()?"))
		.map(|(i, _)| i)
		.collect();
	assert_eq!(calls.len(), 1);
	assert_eq!(
		lines[calls[0] - 1].trim(),
		"#[cfg(test)]",
		"seam call without cfg(test)"
	);
	let file = syn::parse_file(&text).unwrap();
	let mut entrypoints = Vec::new();
	for item in &file.items {
		if let syn::Item::Impl(i) = item {
			for it in &i.items {
				if let syn::ImplItem::Fn(f) = it {
					if root2_scoped(&f.vis) {
						entrypoints.push(format!("{}::{}", quote_type(&i.self_ty), f.sig.ident));
					}
				}
			}
		}
		if let syn::Item::Fn(f) = item {
			assert!(
				!root2_scoped(&f.vis) && !matches!(f.vis, syn::Visibility::Public(_)),
				"free fn exported"
			);
		}
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"AnchorDir2::open",
			"AnchoredFile2::open",
			"AnchoredFile2::read_bounded"
		]
	);
}
fn quote_type(ty: &syn::Type) -> String {
	match ty {
		syn::Type::Path(p) => p.path.segments.last().unwrap().ident.to_string(),
		_ => String::from("?"),
	}
}
// Pure inventory and snapshot codecs, the pure owner core and replica tuple validation: no I/O,
// clock, networking, unsafe, trust or root construction; fixed entrypoints. Syntax over these
// four files only.
#[test]
fn provisioning_inventory_and_snapshot_are_pure_codecs() {
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("src/storage/mutation_visibility/root2/provisioning");
	fn banned(tokens: proc_macro2::TokenStream) -> Option<String> {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if n.starts_with("Trusted")
						|| n.starts_with("Verified")
						|| [
							"fs",
							"io",
							"net",
							"tokio",
							"Instant",
							"SystemTime",
							"unsafe",
							"Root2",
							"State2",
							"Command2",
							"CommittedEntry2",
							"File",
							"OpenOptions",
							"syscall",
							"libc",
						]
						.contains(&n.as_str())
					{
						return Some(n);
					}
				}
				proc_macro2::TokenTree::Group(g) => {
					if let Some(n) = banned(g.stream()) {
						return Some(n);
					}
				}
				_ => {}
			}
		}
		None
	}
	let mut entrypoints = Vec::new();
	for name in [
		"inventory.rs",
		"snapshot.rs",
		"owner_core.rs",
		"replica_anchor.rs",
	] {
		let text = fs::read_to_string(root.join(name)).unwrap();
		assert_eq!(
			banned(text.parse().unwrap()),
			None,
			"{name}: prohibited token"
		);
		for item in syn::parse_file(&text).unwrap().items {
			match item {
				syn::Item::Fn(f) if root2_scoped(&f.vis) => {
					entrypoints.push(f.sig.ident.to_string())
				}
				syn::Item::Fn(f) => assert!(
					matches!(f.vis, syn::Visibility::Inherited),
					"{name}: {}",
					f.sig.ident
				),
				syn::Item::Impl(i) => {
					for it in &i.items {
						if let syn::ImplItem::Fn(f) = it {
							assert!(
								matches!(f.vis, syn::Visibility::Inherited) || i.trait_.is_some(),
								"{name}: impl method exported"
							);
						}
					}
				}
				syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
					panic!("{name}: no traits, statics or modules")
				}
				_ => {}
			}
		}
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"classify2",
			"decide2",
			"decode_event2",
			"decode_evidence2",
			"decode_snapshot2",
			"encode_event2",
			"encode_evidence2",
			"encode_snapshot2",
			"event_request_key2",
			"evidence_name2",
			"expected_repair_event2",
			"fold_events2",
			"op_id2",
			"repair_context2",
			"repair_op_id2",
			"request_key2",
			"tuple_entry_last_seq2",
			"tuple_export_seq2",
			"tuple_inventory_pin2",
			"tuple_manifest2",
			"tuple_manifest_pin2",
			"tuple_record2",
			"validate_replica_tuple2"
		]
	);
}
// Owner I/O: std only, zero unsafe, libc constants only, no network, clock, env, trust or root
// construction; exactly eight exported methods on OwnerDir2 and no exported free functions. Its
// one private child, owner_log/ops.rs, obeys the same bans and exposes nothing beyond owner_log.
// Syntax only; #[cfg(test)] items are excluded from the production surface.
#[test]
fn provisioning_owner_log_is_std_only_with_eight_methods() {
	let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("src/storage/mutation_visibility/root2/provisioning");
	let text = fs::read_to_string(dir.join("owner_log.rs")).unwrap();
	let ops = fs::read_to_string(dir.join("owner_log/ops.rs")).unwrap();
	fn walk(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
		let ts: Vec<_> = tokens.into_iter().collect();
		for (i, t) in ts.iter().enumerate() {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if n.starts_with("Trusted")
						|| n.starts_with("Verified")
						|| [
							"unsafe",
							"net",
							"tokio",
							"Instant",
							"SystemTime",
							"env",
							"Root2",
							"State2",
							"Command2",
							"CommittedEntry2",
						]
						.contains(&n.as_str())
					{
						out.push(n.clone());
					}
					if n == "libc" {
						let name = match (ts.get(i + 1), ts.get(i + 2), ts.get(i + 3)) {
							(
								Some(proc_macro2::TokenTree::Punct(a)),
								Some(proc_macro2::TokenTree::Punct(b)),
								Some(proc_macro2::TokenTree::Ident(c)),
							) if a.as_char() == ':' && b.as_char() == ':' => c.to_string(),
							_ => {
								out.push("libc path".into());
								continue;
							}
						};
						if !name
							.chars()
							.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
						{
							out.push(format!("libc::{name}"));
						}
						if matches!(ts.get(i + 4), Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Parenthesis)
						{
							out.push(format!("libc::{name}()"));
						}
					}
				}
				proc_macro2::TokenTree::Group(g) => walk(g.stream(), out),
				_ => {}
			}
		}
	}
	// Both files, tests included, have no unsafe; the other bans apply to production items.
	for (name, source) in [("owner_log.rs", &text), ("owner_log/ops.rs", &ops)] {
		let mut found = Vec::new();
		walk(source.parse().unwrap(), &mut found);
		assert!(!found.iter().any(|n| n == "unsafe"), "{name}: unsafe");
		let mut production = Vec::new();
		walk(without_test_items(source.parse().unwrap()), &mut production);
		assert!(production.is_empty(), "{name}: {production:?}");
	}
	let cfg_test = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| {
			a.path().is_ident("cfg")
				&& matches!(&a.meta, syn::Meta::List(l) if l.tokens.to_string() == "test")
		})
	};
	let mut methods = Vec::new();
	for item in syn::parse_file(&text).unwrap().items {
		match item {
			syn::Item::Fn(f) if !cfg_test(&f.attrs) => assert!(
				matches!(f.vis, syn::Visibility::Inherited),
				"owner_log.rs exports fn {}",
				f.sig.ident
			),
			syn::Item::Impl(i) if !cfg_test(&i.attrs) => {
				for it in &i.items {
					if let syn::ImplItem::Fn(f) = it
						&& !matches!(f.vis, syn::Visibility::Inherited)
					{
						assert!(i.trait_.is_none(), "trait methods carry no visibility");
						assert_eq!(quote_type(&i.self_ty), "OwnerDir2");
						assert!(root2_scoped(&f.vis), "{}", f.sig.ident);
						methods.push(f.sig.ident.to_string());
					}
				}
			}
			syn::Item::Trait(t) if !cfg_test(&t.attrs) => panic!("owner_log.rs: trait"),
			syn::Item::Static(s) if !cfg_test(&s.attrs) => panic!("owner_log.rs: static"),
			syn::Item::Mod(m) if !cfg_test(&m.attrs) => assert!(
				m.ident == "ops"
					&& m.content.is_none()
					&& matches!(m.vis, syn::Visibility::Inherited),
				"owner_log.rs: only the private `mod ops;`"
			),
			syn::Item::Macro(m) if !cfg_test(&m.attrs) => assert!(
				m.mac.path.is_ident("redacted_debug"),
				"owner_log.rs: item macro"
			),
			_ => {}
		}
	}
	// ops.rs: private or pub(super) only, no trait impls, traits, statics, modules or macros.
	let parent_only = |v: &syn::Visibility| {
		matches!(v, syn::Visibility::Inherited)
			|| matches!(v, syn::Visibility::Restricted(r) if r.in_token.is_none() && r.path.is_ident("super"))
	};
	for item in syn::parse_file(&ops).unwrap().items {
		match item {
			syn::Item::Fn(f) => assert!(parent_only(&f.vis), "ops.rs: {}", f.sig.ident),
			syn::Item::Struct(s) => {
				assert!(parent_only(&s.vis), "ops.rs: {}", s.ident);
				for f in &s.fields {
					assert!(parent_only(&f.vis), "ops.rs: field of {}", s.ident);
				}
			}
			syn::Item::Enum(e) => assert!(parent_only(&e.vis), "ops.rs: {}", e.ident),
			syn::Item::Type(t) => assert!(parent_only(&t.vis), "ops.rs: {}", t.ident),
			syn::Item::Impl(i) => {
				assert!(i.trait_.is_none(), "ops.rs: trait impl");
				for it in &i.items {
					if let syn::ImplItem::Fn(f) = it {
						assert!(parent_only(&f.vis), "ops.rs: {}", f.sig.ident);
					}
				}
			}
			syn::Item::Use(_) => {}
			_ => panic!("ops.rs: unexpected item kind"),
		}
	}
	methods.sort();
	assert_eq!(
		methods,
		[
			"apply",
			"clear_sync_failure",
			"export",
			"inspect",
			"open",
			"repair_check",
			"repair_fresh",
			"repair_resume"
		]
	);
}
// Derived administrator view: pure, no I/O, clock or unsafe; it constructs no trusted input, root or
// authority state and mutates nothing; exactly six root2-private entrypoints. Syntax only.
#[test]
fn admin_install_is_a_pure_derivation() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("admin_install.rs")).unwrap();
	fn forbidden(tokens: proc_macro2::TokenStream) -> Option<String> {
		let ts: Vec<_> = tokens.into_iter().collect();
		for (i, t) in ts.iter().enumerate() {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if [
						"unsafe",
						"mut",
						"fs",
						"io",
						"net",
						"tokio",
						"env",
						"Instant",
						"SystemTime",
						"fixture",
						"unauthorized",
						"change_fingerprint",
					]
					.contains(&n.as_str())
					{
						return Some(n);
					}
					let next = ts.get(i + 1);
					let constructs = matches!(next, Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Brace)
						|| matches!(next, Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == ':');
					if [
						"Root2",
						"UnverifiedRoot2",
						"TrustedEntry2",
						"VerifiedCommand2",
						"CommittedEntry2",
						"TrustedView2",
						"MutationAuthorityState2",
					]
					.contains(&n.as_str())
						&& constructs
					{
						return Some(n);
					}
				}
				proc_macro2::TokenTree::Group(g) => {
					if let Some(n) = forbidden(g.stream()) {
						return Some(n);
					}
				}
				_ => {}
			}
		}
		None
	}
	assert_eq!(
		forbidden(without_test_items(text.parse().unwrap())),
		None,
		"admin_install.rs: prohibited token"
	);
	let mut entrypoints = Vec::new();
	for item in syn::parse_file(&text).unwrap().items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => entrypoints.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => assert!(matches!(f.vis, syn::Visibility::Inherited)),
			syn::Item::Impl(_) | syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
				panic!("admin_install.rs: no impls, traits, statics or modules")
			}
			_ => {}
		}
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"admin_context2",
			"admin_id2",
			"admin_installed_at2",
			"admin_key2",
			"admin_namespace2",
			"install_admin2",
			"observe_bootstrap2"
		]
	);
}
// Replica tuple read: composes only the accepted anchored reader and the pure tuple validator; no
// unsafe, direct filesystem or libc use, network, clock, env, trust or root construction; one
// root2-private entrypoint. Syntax only.
#[test]
fn provisioning_replica_read_composes_only_the_accepted_reader() {
	let text = fs::read_to_string(
		PathBuf::from(env!("CARGO_MANIFEST_DIR"))
			.join("src/storage/mutation_visibility/root2/provisioning/replica_read.rs"),
	)
	.unwrap();
	fn banned(tokens: proc_macro2::TokenStream) -> Option<String> {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if n.starts_with("Trusted")
						|| n.starts_with("Verified")
						|| [
							"unsafe",
							"fs",
							"File",
							"OpenOptions",
							"libc",
							"syscall",
							"net",
							"tokio",
							"env",
							"Instant",
							"SystemTime",
							"CommittedEntry2",
							"Root2",
							"State2",
						]
						.contains(&n.as_str())
					{
						return Some(n);
					}
				}
				proc_macro2::TokenTree::Group(g) => {
					if let Some(n) = banned(g.stream()) {
						return Some(n);
					}
				}
				_ => {}
			}
		}
		None
	}
	assert_eq!(
		banned(text.parse().unwrap()),
		None,
		"replica_read.rs: prohibited token"
	);
	let mut entrypoints = Vec::new();
	for item in syn::parse_file(&text).unwrap().items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => entrypoints.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => assert!(matches!(f.vis, syn::Visibility::Inherited)),
			syn::Item::Impl(_) | syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
				panic!("replica_read.rs: no impls, traits, statics or modules")
			}
			_ => {}
		}
	}
	assert_eq!(entrypoints, ["read_replica_tuple2"]);
}
// Administrator statement authentication: pure std and strict Ed25519 only; zero unsafe, I/O,
// network, clock or env; no mutation of authority or root state and no trust, root or authority
// state construction; exactly six root2-private entrypoints. Syntax only, plus source review.
#[test]
fn admin_statement_is_pure_authentication() {
	let text = fs::read_to_string(
		PathBuf::from(env!("CARGO_MANIFEST_DIR"))
			.join("src/storage/mutation_visibility/root2/admin_statement.rs"),
	)
	.unwrap();
	fn forbidden(tokens: proc_macro2::TokenStream) -> Option<String> {
		let ts: Vec<_> = tokens.into_iter().collect();
		for (i, t) in ts.iter().enumerate() {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if [
						"unsafe",
						"fs",
						"io",
						"net",
						"tokio",
						"env",
						"Instant",
						"SystemTime",
						"fixture",
						"SigningKey",
					]
					.contains(&n.as_str())
					{
						return Some(n);
					}
					let next = ts.get(i + 1);
					if [
						"Root2",
						"UnverifiedRoot2",
						"TrustedEntry2",
						"VerifiedCommand2",
						"CommittedEntry2",
						"TrustedView2",
						"MutationAuthorityState2",
						"State2",
					]
					.contains(&n.as_str())
						&& (matches!(next, Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Brace)
							|| matches!(next, Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == ':'))
					{
						return Some(n);
					}
					// `&mut` of authority or root state.
					if n == "mut"
						&& i > 0 && matches!(&ts[i - 1], proc_macro2::TokenTree::Punct(p) if p.as_char() == '&')
						&& matches!(next, Some(proc_macro2::TokenTree::Ident(t)) if ["Root2", "State2", "MutationAuthorityState2"].contains(&t.to_string().as_str()))
					{
						return Some("&mut state".into());
					}
				}
				proc_macro2::TokenTree::Group(g) => {
					if let Some(n) = forbidden(g.stream()) {
						return Some(n);
					}
				}
				_ => {}
			}
		}
		None
	}
	assert_eq!(
		forbidden(without_test_items(text.parse().unwrap())),
		None,
		"admin_statement.rs: prohibited token"
	);
	let mut entrypoints = Vec::new();
	for item in syn::parse_file(&text).unwrap().items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => entrypoints.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => assert!(matches!(f.vis, syn::Visibility::Inherited)),
			syn::Item::Impl(_) | syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
				panic!("admin_statement.rs: no impls, traits, statics or modules")
			}
			_ => {}
		}
	}
	entrypoints.sort();
	assert_eq!(
		entrypoints,
		[
			"admin_statement_preimage2",
			"check_admin_statement2",
			"checked_action2",
			"checked_context2",
			"checked_key2",
			"encode_admin_statement2",
			"inspect_admin_statement2"
		]
	);
	// Inspection facts never become a checked statement: the only literal is the declaration plus
	// the one inside check_admin_statement2.
	assert_eq!(text.matches("CheckedStatement2 {").count(), 2);
	let check = text.split("fn check_admin_statement2(").nth(1).unwrap();
	let check = check.split("\npub(in").next().unwrap();
	assert_eq!(check.matches("CheckedStatement2 {").count(), 1);
}
// Admission permission registry: pure codecs and semantic transitions only. No impls, traits,
// statics or modules; no I/O, crypto, trust token, issuer decision or root construction; exactly
// the eleven root2-scoped entrypoints. Syntax over this file only; not name resolution.
fn registry_violation(text: &str) -> Option<String> {
	fn forbidden(tokens: proc_macro2::TokenStream) -> Option<String> {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if [
						"unsafe",
						"async",
						"fs",
						"io",
						"net",
						"tokio",
						"env",
						"Instant",
						"SystemTime",
						"fixture",
						"ed25519_dalek",
						"aws_lc_rs",
						"Signature",
						"SigningKey",
						"VerifyingKey",
						"CheckedStatement2",
						"check_admin_statement2",
						"InstalledAdmin2",
						"VerifiedCommand2",
						"TrustedEntry2",
						"TrustedView2",
						"CommittedEntry2",
						"MutationAuthorityState2",
						"Root2",
						"UnverifiedRoot2",
						"State2",
						"Command2",
					]
					.contains(&n.as_str())
					{
						return Some(n);
					}
				}
				proc_macro2::TokenTree::Group(g) => {
					if let Some(n) = forbidden(g.stream()) {
						return Some(n);
					}
				}
				_ => {}
			}
		}
		None
	}
	let file = syn::parse_file(text).ok()?;
	if let Some(n) = forbidden(without_test_items(text.parse().unwrap())) {
		return Some(format!("prohibited token {n}"));
	}
	let mut entrypoints = Vec::new();
	for item in file.items {
		let vis = match &item {
			syn::Item::Fn(f) => {
				if root2_scoped(&f.vis) {
					entrypoints.push(f.sig.ident.to_string());
				}
				&f.vis
			}
			syn::Item::Struct(s) => &s.vis,
			syn::Item::Enum(e) => &e.vis,
			syn::Item::Type(t) => &t.vis,
			syn::Item::Const(c) => &c.vis,
			syn::Item::Use(u) => &u.vis,
			syn::Item::Macro(m) if m.mac.path.is_ident("redacted_debug") => continue,
			_ => return Some("impl, trait, static, module or other item".into()),
		};
		if !matches!(vis, syn::Visibility::Inherited) && !root2_scoped(vis) {
			return Some("export wider than root2".into());
		}
	}
	entrypoints.sort();
	let expected = [
		"apply_permission_delta2",
		"decode_permission_key2",
		"decode_permission_record2",
		"decode_permission_section2",
		"encode_permission_key2",
		"encode_permission_record2",
		"encode_permission_section2",
		"grant_permission2",
		"permission_limits2",
		"revoke_permission2",
		"validate_permissions2",
	];
	if entrypoints != expected {
		return Some(format!("entrypoints {entrypoints:?}"));
	}
	None
}
#[test]
fn admission_registry_is_a_pure_registry() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("admission_registry.rs")).unwrap();
	assert!(text.lines().count() <= 750);
	assert_eq!(registry_violation(&text), None, "admission_registry.rs");
	let modules = fs::read_to_string(root.join("mod.rs")).unwrap();
	assert!(modules.contains("#[allow(dead_code)]\n// Dormant Admission permission registry; no production caller until reviewed integration.\nmod admission_registry;\n"));
	assert!(!modules.contains("pub mod admission_registry"));
	// Root integration (profile 3) may name the registry in state, validation, incremental and
	// types; the reducer and command codec may not, and no file outside the module calls a
	// transition (below).
	for name in ["reducer.rs"] {
		let t = fs::read_to_string(root.join(name)).unwrap();
		assert!(
			!t.contains("admission_registry"),
			"{name} reaches the registry"
		);
	}
	// The command codec may use the key and record codecs only.
	let command = fs::read_to_string(root.join("command.rs")).unwrap();
	for f in [
		"grant_permission2",
		"revoke_permission2",
		"validate_permissions2",
		"apply_permission_delta2",
	] {
		assert!(!command.contains(f), "command.rs names {f}");
	}
	let state = fs::read_to_string(root.join("state.rs")).unwrap();
	assert_eq!(state.matches("VISAUT03").count(), 2);
	assert!(!state.contains("VISAUT02"));
	let codec = fs::read_to_string(root.join("codec.rs")).unwrap();
	assert!(codec.contains("\tenvelope_versioned(magic, 2, body, max)\n}"));
	assert!(codec.contains("\topen_versioned(bytes, magic, 2, max)\n}"));
	// No production caller of a transition anywhere outside the module itself.
	fn walk(dir: PathBuf, tests: &PathBuf, out: &mut Vec<PathBuf>) {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != *tests {
					walk(p, tests, out)
				}
			} else if p.extension().is_some_and(|x| x == "rs") {
				out.push(p)
			}
		}
	}
	let mut files = Vec::new();
	walk(src, &root.join("tests"), &mut files);
	assert!(files.len() > 100);
	// The issuer gate is the one production caller of each transition (call graph pinned in
	// admission_gate_call_graph_is_single_and_dormant).
	for f in files {
		if f.ends_with("root2/admission_registry.rs") {
			continue;
		}
		let t = fs::read_to_string(&f).unwrap();
		if f.ends_with("root2/admission_gate.rs") {
			assert_eq!(t.matches("grant_permission2(").count(), 1);
			assert_eq!(t.matches("revoke_permission2(").count(), 1);
			continue;
		}
		assert!(
			!t.contains("grant_permission2") && !t.contains("revoke_permission2"),
			"production caller in {}",
			f.display()
		);
	}
	for (bad, why) in [
		(
			"fn promote() -> CheckedStatement2 { todo!() }",
			"CheckedStatement2",
		),
		("fn admit(v: &VerifiedCommand2) {}", "VerifiedCommand2"),
		("fn read(p: &str) { std::fs::read(p).unwrap(); }", "fs"),
		("impl PermissionRecord2 {}", "impl"),
		("trait Issue {}", "impl"),
		("static X: u8 = 0;", "impl"),
		("mod hidden {}", "impl"),
		("pub fn decode() {}", "export"),
		("pub(crate) struct Wide;", "export"),
		("pub(super) const C: u8 = 0;", "export"),
	] {
		let v = registry_violation(bad).unwrap_or_else(|| panic!("accepted {bad}"));
		assert!(v.contains(why), "{bad}: {v}");
	}
}
// Dormant dispatch: the reducer refuses an Admission permission command before covered-entry
// replay, the gap check and authorization, and its apply arm is the same refusal.
#[test]
fn admission_dispatch_fails_closed() {
	let reducer = fs::read_to_string(
		PathBuf::from(env!("CARGO_MANIFEST_DIR"))
			.join("src/storage/mutation_visibility/root2/reducer.rs"),
	)
	.unwrap();
	assert_eq!(reducer.matches("Command2::AdmissionPermission").count(), 2);
	let gate = reducer
		.find("if let Command2::AdmissionPermission(_) = command {")
		.unwrap();
	let replay = reducer.find("if l.index == s.covered.index {").unwrap();
	let gap = reducer.find("LineageGap").unwrap();
	let authorized = reducer
		.find("let result = if verified.authorized()")
		.unwrap();
	assert!(gate < replay && gate < gap && gate < authorized);
	for (at, _) in reducer.match_indices("Command2::AdmissionPermission") {
		let tail = &reducer[at..(at + 120).min(reducer.len())];
		assert!(tail.contains("Err(RootError2::TrustMismatch)"), "{tail}");
	}
	let incremental = fs::read_to_string(
		PathBuf::from(env!("CARGO_MANIFEST_DIR"))
			.join("src/storage/mutation_visibility/root2/incremental.rs"),
	)
	.unwrap();
	assert!(
		!incremental.contains("grant_permission2") && !incremental.contains("revoke_permission2")
	);
}
// The issuer gate's whole production call graph: one caller of each transition, inside the checked
// helper; the helper called only from the gated reducer; that reducer called only from the origin
// wrapper; the wrapper itself uncalled (dormant); authentication before application; no
// inspection as authority; the legacy reducer untouched. Source text, not name resolution.
fn fn_body<'a>(text: &'a str, name: &str) -> &'a str {
	let start = text
		.find(&format!("fn {name}("))
		.unwrap_or_else(|| panic!("fn {name}"));
	let open = start + text[start..].find('{').unwrap();
	let mut depth = 0;
	for (i, ch) in text[open..].char_indices() {
		match ch {
			'{' => depth += 1,
			'}' => {
				depth -= 1;
				if depth == 0 {
					return &text[open..open + i + 1];
				}
			}
			_ => {}
		}
	}
	panic!("unbalanced fn {name}")
}
fn calls(text: &str, name: &str) -> usize {
	text.matches(&format!("{name}(")).count() - text.matches(&format!("fn {name}(")).count()
}
#[test]
fn admission_gate_call_graph_is_single_and_dormant() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let gate = fs::read_to_string(root.join("admission_gate.rs")).unwrap();
	let origin = fs::read_to_string(root.join("origin.rs")).unwrap();
	let reducer = fs::read_to_string(root.join("reducer.rs")).unwrap();
	assert!(gate.lines().count() <= 750);
	// 1. Each transition once, inside apply_admission.
	let apply = fn_body(&gate, "apply_admission");
	for t in ["grant_permission2", "revoke_permission2"] {
		assert_eq!(calls(&gate, t), 1, "{t}");
		assert_eq!(calls(apply, t), 1, "{t} in apply_admission");
	}
	// 2. apply_admission called once, from reduce_admission; 5. after the one authentication.
	let reduce = fn_body(&gate, "reduce_admission");
	assert_eq!(calls(&gate, "apply_admission"), 1);
	assert_eq!(calls(reduce, "apply_admission"), 1);
	assert_eq!(calls(&gate, "check_admin_statement2"), 1);
	assert!(
		reduce.find("check_admin_statement2(").unwrap() < reduce.find("apply_admission(").unwrap()
	);
	assert!(!gate.contains("inspect_admin_statement2"));
	// 3, 4, 6. reduce_admission called once, from advance_admission2, which installs once and is
	// called nowhere in production.
	let wrapper = fn_body(&origin, "advance_admission2");
	assert_eq!(calls(wrapper, "reduce_admission"), 1);
	assert_eq!(calls(&origin, "install_admin2"), 1);
	assert_eq!(calls(wrapper, "install_admin2"), 1);
	fn walk(dir: PathBuf, tests: &PathBuf, out: &mut Vec<PathBuf>) {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != *tests {
					walk(p, tests, out)
				}
			} else if p.extension().is_some_and(|x| x == "rs") {
				out.push(p)
			}
		}
	}
	let mut files = Vec::new();
	walk(src, &root.join("tests"), &mut files);
	let (mut reduce_calls, mut advance_defs, mut advance_calls) = (0, 0, 0);
	for f in &files {
		let t = without_test_items(fs::read_to_string(f).unwrap().parse().unwrap()).to_string();
		let t = t.replace(' ', "");
		reduce_calls +=
			t.matches("reduce_admission(").count() - t.matches("fnreduce_admission(").count();
		advance_defs += t.matches("fnadvance_admission2(").count();
		advance_calls +=
			t.matches("advance_admission2(").count() - t.matches("fnadvance_admission2(").count();
	}
	assert_eq!((reduce_calls, advance_defs, advance_calls), (1, 1, 0));
	// 7. The legacy reducer is untouched by the gate.
	assert!(!reducer.contains("admission_registry") && !reducer.contains("reduce_admission"));
	assert!(!reducer.contains("check_admin_statement2"));
}
// G1d-4: the Reserve permission check has exactly two production callers (apply_reserve and the
// full validator), the reducer still never names the registry, and trigger's body is unchanged
// from 57df2290 (a later permission revocation creates no terminal refusal).
const TRIGGER_BODY_57DF2290: &str = r##"{
	let (claim, _, _, _) = p.parts();
	let mut candidates = Vec::new();
	let g = s
		.lineages
		.get(&claim.original.lineage)
		.ok_or(RootError2::RootInvariant)?;
	if let Some(c) = g.closure {
		candidates.push((
			match c.cause {
				1 => 2,
				2 => 3,
				3 => 1,
				_ => return Err(RootError2::RootInvariant),
			},
			c.log,
		))
	}
	if let Some(a) = s
		.authorities
		.get(&authority_key(claim.authority)?)
		.and_then(|a| a.revoked)
	{
		candidates.push((4, a))
	}
	candidates.sort_by_key(|(cause, l)| (l.index, *cause));
	Ok(candidates.first().copied())
}"##;
#[test]
fn admission_reserve_check_is_exactly_twice_and_trigger_is_unchanged() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let validation = fs::read_to_string(root.join("validation.rs")).unwrap();
	let reducer = fs::read_to_string(root.join("reducer.rs")).unwrap();
	assert_eq!(fn_body(&validation, "trigger"), TRIGGER_BODY_57DF2290);
	assert_eq!(calls(&reducer, "admission_permission"), 1);
	assert_eq!(
		calls(fn_body(&reducer, "apply_reserve"), "admission_permission"),
		1
	);
	assert_eq!(calls(&validation, "admission_permission"), 1);
	assert_eq!(
		calls(
			fn_body(&validation, "validate_state"),
			"admission_permission"
		),
		1
	);
	assert!(!reducer.contains("admission_registry"));
	fn walk(dir: PathBuf, tests: &PathBuf, out: &mut Vec<PathBuf>) {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != *tests {
					walk(p, tests, out)
				}
			} else if p.extension().is_some_and(|x| x == "rs") {
				out.push(p)
			}
		}
	}
	let mut files = Vec::new();
	walk(
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src"),
		&root.join("tests"),
		&mut files,
	);
	let total: usize = files
		.iter()
		.map(|f| calls(&fs::read_to_string(f).unwrap(), "admission_permission"))
		.sum();
	assert_eq!(total, 2, "exactly two production callers");
}
// G1e-1: the replay monitor and recovery mark are pure and dormant. No state construction,
// decoding or promotion; no trusted token, install, transition or I/O; the one bounded full
// encoding (state_digest) is called exactly twice; exactly the listed exports; zero production
// callers outside the module.
#[test]
fn recovery_is_pure_and_dormant() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("recovery.rs")).unwrap();
	assert!(text.lines().count() <= 750);
	fn forbidden(tokens: proc_macro2::TokenStream) -> Option<String> {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if [
						"unsafe",
						"async",
						"fs",
						"io",
						"net",
						"tokio",
						"StateInner2",
						"UnverifiedAuthority2",
						"UnverifiedRoot2",
						"TrustedEntry2",
						"VerifiedCommand2",
						"CommittedEntry2",
						"TrustedView2",
						"install_admin2",
						"bootstrap2",
						"advance_root2",
						"advance_pre_root2",
						"advance_admission2",
						"reduce",
						"reduce_admission",
						"empty_authority2",
					]
					.contains(&n.as_str())
					{
						return Some(n);
					}
				}
				proc_macro2::TokenTree::Group(g) => {
					if let Some(n) = forbidden(g.stream()) {
						return Some(n);
					}
				}
				_ => {}
			}
		}
		None
	}
	assert_eq!(
		forbidden(without_test_items(text.parse().unwrap())),
		None,
		"recovery.rs: prohibited token"
	);
	let file = syn::parse_file(&text).unwrap();
	let mut exports = Vec::new();
	for item in &file.items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => exports.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => assert!(
				matches!(f.vis, syn::Visibility::Inherited)
					|| matches!(&f.vis, syn::Visibility::Restricted(r) if r.path.is_ident("super"))
			),
			syn::Item::Impl(_) | syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
				panic!("recovery.rs: no impls, traits, statics or modules")
			}
			_ => {}
		}
	}
	exports.sort();
	assert_eq!(
		exports,
		[
			"decode_recovery_mark2",
			"encode_recovery_mark2",
			"mark_of2",
			"observe2",
			"replay_monitor2"
		]
	);
	// The single full encoding, called at exactly the two bounded sites.
	assert_eq!(text.matches("encode_authority2(").count(), 1);
	assert_eq!(
		calls(fn_body(&text, "state_digest"), "encode_authority2"),
		1
	);
	assert_eq!(calls(&text, "state_digest"), 2);
	assert_eq!(calls(fn_body(&text, "mark_of2"), "state_digest"), 1);
	assert_eq!(calls(fn_body(&text, "observe"), "state_digest"), 1);
	// Dormant: no production caller of any export outside the module.
	fn walk(dir: PathBuf, tests: &PathBuf, out: &mut Vec<PathBuf>) {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != *tests {
					walk(p, tests, out)
				}
			} else if p.extension().is_some_and(|x| x == "rs") {
				out.push(p)
			}
		}
	}
	let mut files = Vec::new();
	walk(src, &root.join("tests"), &mut files);
	for f in files {
		if f.ends_with("root2/recovery.rs") {
			continue;
		}
		let t = fs::read_to_string(&f).unwrap();
		// The dormant mark store (stage M) is the one consumer of the codec, and only of the
		// codec; its own guard pins it to zero production callers.
		let mark_store = f.ends_with("root2/durable/mark_store.rs");
		for name in [
			"replay_monitor2",
			"observe2",
			"mark_of2",
			"encode_recovery_mark2",
			"decode_recovery_mark2",
		] {
			if mark_store && name.ends_with("_recovery_mark2") {
				assert!(t.contains(name), "mark store uses the codec: {name}");
				continue;
			}
			assert!(
				!t.contains(name),
				"production caller of {name} in {}",
				f.display()
			);
		}
	}
}

// Recovery-mark store: std only, zero unsafe (tests included), libc constants only, no network,
// clock, env, trust, root or authority-state construction; exactly four root2-scoped methods on
// MarkStore2, no exported free functions, traits or statics; no caller outside its own module and
// the root2 tests; bounded. Syntax only; #[cfg(test)] items are excluded from the production surface.
#[test]
fn durable_mark_store_is_std_only_with_four_methods() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let path = src.join("storage/mutation_visibility/root2/durable/mark_store.rs");
	let text = fs::read_to_string(&path).unwrap();
	assert!(text.lines().count() <= 750);
	fn walk(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
		let ts: Vec<_> = tokens.into_iter().collect();
		for (i, t) in ts.iter().enumerate() {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if n.starts_with("Trusted")
						|| n.starts_with("Verified")
						|| [
							"unsafe",
							"net",
							"tokio",
							"Instant",
							"SystemTime",
							"env",
							"Root2",
							"State2",
							"Command2",
							"CommittedEntry2",
							"MutationAuthorityState2",
						]
						.contains(&n.as_str())
					{
						out.push(n.clone());
					}
					if n == "libc" {
						match (ts.get(i + 1), ts.get(i + 2), ts.get(i + 3)) {
							(
								Some(proc_macro2::TokenTree::Punct(a)),
								Some(proc_macro2::TokenTree::Punct(b)),
								Some(proc_macro2::TokenTree::Ident(c)),
							) if a.as_char() == ':' && b.as_char() == ':' => {
								let name = c.to_string();
								if !name.chars().all(|c| {
									c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'
								}) || matches!(ts.get(i + 4), Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Parenthesis)
								{
									out.push(format!("libc::{name}"));
								}
							}
							_ => out.push("libc path".into()),
						}
					}
				}
				proc_macro2::TokenTree::Group(g) => walk(g.stream(), out),
				_ => {}
			}
		}
	}
	let mut found = Vec::new();
	walk(text.parse().unwrap(), &mut found);
	assert!(
		!found.iter().any(|n| n == "unsafe"),
		"mark_store.rs: unsafe"
	);
	let mut production = Vec::new();
	walk(without_test_items(text.parse().unwrap()), &mut production);
	assert!(production.is_empty(), "mark_store.rs: {production:?}");
	let cfg_test = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| {
			a.path().is_ident("cfg")
				&& matches!(&a.meta, syn::Meta::List(l) if l.tokens.to_string() == "test")
		})
	};
	let mut methods = Vec::new();
	for item in syn::parse_file(&text).unwrap().items {
		match item {
			syn::Item::Fn(f) if !cfg_test(&f.attrs) => assert!(
				matches!(f.vis, syn::Visibility::Inherited),
				"mark_store.rs exports fn {}",
				f.sig.ident
			),
			syn::Item::Impl(i) if !cfg_test(&i.attrs) => {
				assert!(i.trait_.is_none(), "mark_store.rs: trait impl");
				for it in &i.items {
					if let syn::ImplItem::Fn(f) = it
						&& !matches!(f.vis, syn::Visibility::Inherited)
					{
						assert_eq!(quote_type(&i.self_ty), "MarkStore2");
						assert!(root2_scoped(&f.vis), "{}", f.sig.ident);
						methods.push(f.sig.ident.to_string());
					}
				}
			}
			syn::Item::Trait(t) if !cfg_test(&t.attrs) => panic!("mark_store.rs: trait"),
			syn::Item::Static(s) if !cfg_test(&s.attrs) => panic!("mark_store.rs: static"),
			syn::Item::Mod(m) if !cfg_test(&m.attrs) => panic!("mark_store.rs: module"),
			syn::Item::Macro(m) if !cfg_test(&m.attrs) => assert!(
				m.mac.path.is_ident("redacted_debug"),
				"mark_store.rs: item macro"
			),
			_ => {}
		}
	}
	methods.sort();
	assert_eq!(methods, ["advance", "durable", "open", "provision"]);
	// No production caller: outside its own file, only its module declaration and root2's
	// tests name it.
	fn files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
		for entry in fs::read_dir(dir).unwrap() {
			let p = entry.unwrap().path();
			if p.is_dir() {
				files(&p, out);
			} else if p.extension().is_some_and(|e| e == "rs") {
				out.push(p);
			}
		}
	}
	let mut all = Vec::new();
	files(&src, &mut all);
	let tests = src.join("storage/mutation_visibility/root2/tests");
	let declaration = src.join("storage/mutation_visibility/root2/durable/mod.rs");
	for file in all {
		if file == path || file.starts_with(&tests) {
			continue;
		}
		let text = fs::read_to_string(&file).unwrap();
		let mentions = text.matches("mark_store").count();
		if file == declaration {
			assert_eq!(mentions, 1, "durable/mod.rs: only the module declaration");
		} else {
			assert_eq!(mentions, 0, "{}: names mark_store", file.display());
		}
	}
}

// Committed-entry adapter (stage A0): a private dormant module; its capability has private fields,
// no derives and only a test-only mint, and is never written as a literal elsewhere; one
// root2-scoped entrypoint; no I/O, clock, trust input or authority-state construction; the
// request classification lists every `WendelonRequest` variant by name, once, with no wildcard;
// `CommittedEntry2::from_source` is called only here; nothing outside root2 names the adapter.
#[test]
fn committed_adapter_is_pure_capability_gated_and_exhaustive() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("committed.rs")).unwrap();
	assert!(text.lines().count() <= 750);
	let module = syn::parse_file(&fs::read_to_string(root.join("mod.rs")).unwrap())
		.unwrap()
		.items
		.into_iter()
		.find_map(|i| match i {
			syn::Item::Mod(m) if m.ident == "committed" => Some(m),
			_ => None,
		})
		.unwrap();
	assert!(matches!(module.vis, syn::Visibility::Inherited));
	let cfg_test = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| {
			a.path().is_ident("cfg")
				&& matches!(&a.meta, syn::Meta::List(l) if l.tokens.to_string() == "test")
		})
	};
	let file = syn::parse_file(&text).unwrap();
	let mut entrypoints = Vec::new();
	let mut mints = 0;
	let mut arms: Vec<String> = Vec::new();
	fn collect(p: &syn::Pat, out: &mut Vec<String>) {
		match p {
			syn::Pat::Or(o) => o.cases.iter().for_each(|c| collect(c, out)),
			syn::Pat::TupleStruct(t) => {
				// `WendelonRequest::Variant(_)` only: no binding, no field access.
				assert!(
					t.elems.len() == 1 && matches!(t.elems.first(), Some(syn::Pat::Wild(_))),
					"classification patterns bind nothing"
				);
				let segs: Vec<String> = t
					.path
					.segments
					.iter()
					.map(|s| s.ident.to_string())
					.collect();
				assert_eq!(segs.len(), 2, "qualified WendelonRequest::Variant");
				assert_eq!(segs[0], "WendelonRequest");
				out.push(segs[1].clone());
			}
			other => panic!("unexpected classification pattern: {}", quote_pat(other)),
		}
	}
	fn quote_pat(p: &syn::Pat) -> String {
		match p {
			syn::Pat::Wild(_) => "_ wildcard".into(),
			syn::Pat::Ident(i) => format!("binding {}", i.ident),
			_ => "other".into(),
		}
	}
	for item in &file.items {
		match item {
			syn::Item::Struct(s) if s.ident == "CommittedSource2" => {
				assert!(s.attrs.iter().all(|a| !a.path().is_ident("derive")));
				for f in &s.fields {
					assert!(matches!(f.vis, syn::Visibility::Inherited));
				}
			}
			syn::Item::Impl(i) => {
				assert!(i.trait_.is_none(), "committed.rs: trait impl");
				assert_eq!(quote_type(&i.self_ty), "CommittedSource2");
				for it in &i.items {
					if let syn::ImplItem::Fn(f) = it {
						assert_eq!(f.sig.ident, "synthetic");
						assert!(cfg_test(&f.attrs), "the only mint is test-only");
						mints += 1;
					}
				}
			}
			syn::Item::Fn(f) if !cfg_test(&f.attrs) => {
				if !matches!(f.vis, syn::Visibility::Inherited) {
					assert!(root2_scoped(&f.vis), "{}", f.sig.ident);
					entrypoints.push(f.sig.ident.to_string());
				}
				if f.sig.ident == "request_class" {
					let syn::Stmt::Expr(syn::Expr::Match(m), _) = f.block.stmts.last().unwrap()
					else {
						panic!("request_class is one match");
					};
					assert!(
						matches!(&*m.expr, syn::Expr::Path(p) if p.path.is_ident("request")),
						"request_class matches its argument"
					);
					for arm in &m.arms {
						assert!(arm.guard.is_none(), "no match guard");
						assert!(
							matches!(&*arm.body, syn::Expr::Path(p) if p.path.segments.iter().map(|s| s.ident.to_string()).collect::<Vec<_>>() == ["EntryClass2", "NonRoot2"]),
							"every request classifies NonRoot2"
						);
						collect(&arm.pat, &mut arms);
					}
					assert_eq!(f.block.stmts.len(), 1, "request_class is only the match");
				}
			}
			syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
				panic!("committed.rs: unexpected item")
			}
			_ => {}
		}
	}
	assert_eq!(entrypoints, ["committed_entry2"]);
	assert_eq!(mints, 1);
	// The declared variants of WendelonRequest, by name.
	let core =
		syn::parse_file(&fs::read_to_string(src.join("consensus/core.rs")).unwrap()).unwrap();
	let mut declared: Vec<String> = core
		.items
		.iter()
		.find_map(|i| match i {
			syn::Item::Enum(e) if e.ident == "WendelonRequest" => {
				Some(e.variants.iter().map(|v| v.ident.to_string()).collect())
			}
			_ => None,
		})
		.unwrap();
	let mut listed = arms.clone();
	listed.sort();
	let before = listed.len();
	listed.dedup();
	assert_eq!(listed.len(), before, "a variant is listed twice");
	declared.sort();
	assert_eq!(
		listed, declared,
		"classification lists exactly the declared variants"
	);
	fn idents(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => out.push(id.to_string()),
				proc_macro2::TokenTree::Group(g) => idents(g.stream(), out),
				_ => {}
			}
		}
	}
	let mut production = Vec::new();
	idents(without_test_items(text.parse().unwrap()), &mut production);
	for banned in [
		"fs",
		"io",
		"net",
		"tokio",
		"Instant",
		"SystemTime",
		"Root2",
		"State2",
		"Command2",
		"TrustedEntry2",
		"VerifiedCommand2",
		"TrustedView2",
		"fixture",
	] {
		assert!(
			!production.iter().any(|w| w == banned),
			"committed.rs: {banned}"
		);
	}
	// No capability literal or factory call outside this module; nothing outside root2 names it.
	fn files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
		for entry in fs::read_dir(dir).unwrap() {
			let p = entry.unwrap().path();
			if p.is_dir() {
				files(&p, out);
			} else if p.extension().is_some_and(|e| e == "rs") {
				out.push(p);
			}
		}
	}
	let mut all = Vec::new();
	files(&src, &mut all);
	let tests = root.join("tests");
	for file in all {
		let t = fs::read_to_string(&file).unwrap();
		let here = file == root.join("committed.rs");
		let trust = file == root.join("trust.rs");
		if !here && !file.starts_with(&tests) {
			assert!(
				!t.contains("from_source(") || trust,
				"{}: calls from_source",
				file.display()
			);
			assert!(
				!t.contains("CommittedSource2 {"),
				"{}: capability literal",
				file.display()
			);
		}
		if !file.starts_with(&root) {
			assert!(
				!t.contains("committed_entry2") && !t.contains("CommittedSource2"),
				"{}",
				file.display()
			);
		}
	}
	assert_eq!(text.matches("CommittedEntry2::from_source(").count(), 1);
}

// Post-root coverage (A1): derived from one genuine committed entry, and unable to authorize.
// Syntax over origin.rs and reducer.rs outside cfg(test); not name resolution.
#[test]
fn post_root_coverage_is_derived_and_cannot_authorize() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let origin = without_test_items(
		fs::read_to_string(root.join("origin.rs"))
			.unwrap()
			.parse()
			.unwrap(),
	)
	.to_string();
	let reducer = without_test_items(
		fs::read_to_string(root.join("reducer.rs"))
			.unwrap()
			.parse()
			.unwrap(),
	)
	.to_string();
	// The entrypoint takes exactly the state and a committed entry, nothing caller-supplied.
	let signature = origin
		.split("fn advance_cover2 (")
		.nth(1)
		.expect("advance_cover2 present")
		.split(") ->")
		.next()
		.unwrap()
		.replace(' ', "");
	assert_eq!(
		signature.trim_end_matches(','),
		"state:&MutationAuthorityState2,entry:&CommittedEntry2"
	);
	let advance = fn_body(&origin, "advance_cover2 ");
	// Every class named once, no wildcard, and the malformed class fails closed.
	for class in [
		"EntryClass2 :: Blank",
		"EntryClass2 :: Membership",
		"EntryClass2 :: NonRoot2",
		"EntryClass2 :: Bootstrap",
		"EntryClass2 :: OtherRoot2",
		"EntryClass2 :: MalformedRoot2",
	] {
		assert_eq!(advance.matches(class).count(), 1, "{class}");
	}
	assert!(!advance.contains("_ =>"), "no wildcard over the class");
	assert!(
		advance.contains(
			"EntryClass2 :: MalformedRoot2 => return Err (CoverError2 :: MalformedAfterRoot)"
		),
		"MalformedRoot2 fails closed"
	);
	let cover = fn_body(&reducer, "cover ");
	for body in [advance, cover] {
		for token in [
			"VerifiedCommand2",
			"TrustedEntry2",
			"TrustedView2",
			"apply (",
			"authorized",
			"Root2 {",
		] {
			assert!(!body.contains(token), "coverage must not use `{token}`");
		}
	}
	assert!(cover.contains("Command2 :: Cover { kind , external : fp }"));
	assert!(cover.contains("finish_step ("));
	// One caller of cover; finish_step shared by exactly reduce and cover.
	assert_eq!(calls(&origin, "Root2 :: cover "), 1);
	assert_eq!(calls(&reducer, "finish_step "), 2);
	assert!(fn_body(&reducer, "reduce ").contains("finish_step ("));
	// Outside the codec, the one Cover construction (not a `{ .. }` pattern) is in cover.
	let mut constructions = Vec::new();
	visit_root2_sources(&root, &mut |path, text| {
		if path.ends_with("command.rs") {
			return;
		}
		let tokens = without_test_items(text.parse().unwrap()).to_string();
		for (i, _) in tokens.match_indices("Command2 :: Cover {") {
			if !tokens[i..].starts_with("Command2 :: Cover { .. }") {
				constructions.push(path.file_name().unwrap().to_string_lossy().to_string());
			}
		}
	});
	assert_eq!(constructions, ["reducer.rs"]);
}

// The capacity invariant behind coverage (A1 r2 §3): every non-test Root2 comes from the
// bootstrap literal or from a reviewed step that re-checks capacity. This pins that census.
#[test]
fn root2_constructors_preserve_capacity_invariant() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let mut literals = Vec::new();
	let mut limit_writers = Vec::new();
	let mut producers = Vec::new();
	visit_root2_sources(&root, &mut |path, text| {
		let name = path.file_name().unwrap().to_string_lossy().to_string();
		let tokens = without_test_items_deep(text.parse().unwrap());
		let flat: Vec<_> = flatten(tokens.clone());
		for (i, t) in flat.iter().enumerate() {
			if let proc_macro2::TokenTree::Ident(id) = t {
				let before = i.checked_sub(1).and_then(|j| flat.get(j));
				let after = flat.get(i + 1);
				let definition = matches!(before, Some(proc_macro2::TokenTree::Ident(b)) if ["struct", "impl", "for"].contains(&b.to_string().as_str()));
				if id == "Root2"
					&& !definition && matches!(after, Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Brace)
				{
					literals.push(name.clone());
				}
			}
		}
		let text = tokens.to_string();
		limit_writers.extend(
			text.matches("limits : Limits2 :: default ()")
				.map(|_| format!("{name}: initializer")),
		);
		limit_writers.extend(
			text.matches(". limits =")
				.map(|_| format!("{name}: assignment")),
		);
		producers.extend(owned_root2_producers(tokens));
	});
	assert_eq!(literals, ["reducer.rs"], "the bootstrap literal only");
	assert_eq!(
		limit_writers,
		["reducer.rs: initializer"],
		"default limits only"
	);
	producers.sort();
	assert_eq!(
		producers,
		[
			"bootstrap_root",
			"cover",
			"finish_step",
			"reduce",
			"reduce_admission"
		],
		"every non-test producer of an owned Root2 re-checks capacity or is the bootstrap"
	);
}
fn visit_root2_sources(root: &std::path::Path, f: &mut dyn FnMut(&std::path::Path, &str)) {
	for entry in fs::read_dir(root).unwrap() {
		let path = entry.unwrap().path();
		if path.is_dir() {
			if path.file_name().unwrap() != "tests" {
				visit_root2_sources(&path, f);
			}
		} else if path.extension().is_some_and(|e| e == "rs")
			&& path.file_name().unwrap() != "tests.rs"
		{
			f(&path, &fs::read_to_string(&path).unwrap());
		}
	}
}
fn flatten(tokens: proc_macro2::TokenStream) -> Vec<proc_macro2::TokenTree> {
	let mut out = Vec::new();
	for t in tokens {
		if let proc_macro2::TokenTree::Group(g) = &t {
			out.push(t.clone());
			out.extend(flatten(g.stream()));
		} else {
			out.push(t);
		}
	}
	out
}
// Names of functions whose return type holds an owned `Root2` (not `&Root2`): `fn name (..) ->`
// up to the body or `where`, at any nesting depth.
fn owned_root2_producers(tokens: proc_macro2::TokenStream) -> Vec<String> {
	let ts: Vec<_> = tokens.into_iter().collect();
	let mut out = Vec::new();
	for (i, t) in ts.iter().enumerate() {
		if let proc_macro2::TokenTree::Group(g) = t {
			out.extend(owned_root2_producers(g.stream()));
		}
		let is_fn = matches!(t, proc_macro2::TokenTree::Ident(id) if id == "fn");
		let (Some(proc_macro2::TokenTree::Ident(name)), Some(proc_macro2::TokenTree::Group(args))) =
			(ts.get(i + 1), ts.get(i + 2))
		else {
			continue;
		};
		if !is_fn || args.delimiter() != proc_macro2::Delimiter::Parenthesis {
			continue;
		}
		let arrow = matches!(ts.get(i + 3), Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == '-')
			&& matches!(ts.get(i + 4), Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == '>');
		if !arrow {
			continue;
		}
		let ret: Vec<_> = ts[i + 5..]
			.iter()
			.take_while(|t| {
				!matches!(t, proc_macro2::TokenTree::Group(g) if g.delimiter() == proc_macro2::Delimiter::Brace)
					&& !matches!(t, proc_macro2::TokenTree::Ident(id) if id == "where")
			})
			.cloned()
			.collect();
		let flat = flatten(ret.into_iter().collect());
		let owned = flat.iter().enumerate().any(|(j, t)| {
			matches!(t, proc_macro2::TokenTree::Ident(id) if id == "Root2")
				&& !matches!(j.checked_sub(1).and_then(|k| flat.get(k)), Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == '&')
		});
		if owned {
			out.push(name.to_string());
		}
	}
	out
}
// Every `#[cfg(test)]` item removed at every nesting depth (a test method inside an impl block
// too). An item ends at its body group or at `;`, so a bodiless test item never hides the next.
fn without_test_items_deep(tokens: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
	let ts: Vec<_> = tokens.into_iter().collect();
	let mut out = Vec::new();
	let mut i = 0;
	while i < ts.len() {
		let cfg_test = matches!(&ts[i], proc_macro2::TokenTree::Punct(p) if p.as_char() == '#')
			&& matches!(ts.get(i + 1), Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Bracket && g.stream().to_string().replace(' ', "") == "cfg(test)");
		if cfg_test {
			i += 2;
			while i < ts.len() {
				let end = matches!(&ts[i], proc_macro2::TokenTree::Group(g) if g.delimiter() == proc_macro2::Delimiter::Brace)
					|| matches!(&ts[i], proc_macro2::TokenTree::Punct(p) if p.as_char() == ';');
				i += 1;
				if end {
					break;
				}
			}
			continue;
		}
		out.push(match &ts[i] {
			proc_macro2::TokenTree::Group(g) => {
				let mut inner =
					proc_macro2::Group::new(g.delimiter(), without_test_items_deep(g.stream()));
				inner.set_span(g.span());
				proc_macro2::TokenTree::Group(inner)
			}
			other => other.clone(),
		});
		i += 1;
	}
	out.into_iter().collect()
}
