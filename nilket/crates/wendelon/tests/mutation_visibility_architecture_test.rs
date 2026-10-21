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
	assert!(core.contains("WAL_FORMAT_VERSION: u8 = 5"));
	// The one admitted outside caller is the offline, read-only compromise audit CLI (ADR 0101
	// C-S6): exactly one mention, its canonical import of the single audit entry point.
	let audit_cli = root.join("src/cli/root2_audit.rs");
	fn visit(dir: PathBuf, module: &PathBuf, audit_cli: &PathBuf) {
		for entry in fs::read_dir(&dir).unwrap() {
			let p = entry.unwrap().path();
			if p == *module {
				continue;
			}
			if p.is_dir() {
				visit(p, module, audit_cli);
			} else if p.extension().is_some_and(|e| e == "rs")
				&& p.file_name().is_some_and(|n| n != "mod.rs")
			{
				let text = fs::read_to_string(&p).unwrap();
				if p == *audit_cli {
					assert_eq!(text.matches("storage::mutation_visibility").count(), 1);
					assert!(
						text.contains(
							"\nuse crate::storage::mutation_visibility::root2::compromise::compromise_audit2;\n"
						),
						"the audit CLI names only the audit entry point"
					);
					continue;
				}
				assert!(
					!text.contains("storage::mutation_visibility"),
					"no production activation in slice A"
				);
			}
		}
	}
	visit(root.join("src"), &module, &audit_cli);
	// The audit module is crate-visible for that one import; its only crate-visible item is the
	// entry point, the trace is root2-scoped, and its children stay private.
	let root2_mod = fs::read_to_string(module.join("root2/mod.rs")).unwrap();
	assert_eq!(root2_mod.matches("mod compromise;").count(), 1);
	assert!(root2_mod.contains("\npub(crate) mod compromise;\n"));
	assert!(!root2_mod.contains("use compromise::"));
	let compromise =
		syn::parse_file(&fs::read_to_string(module.join("root2/compromise.rs")).unwrap()).unwrap();
	let mut visible = Vec::new();
	for item in &compromise.items {
		let (vis, name) = match item {
			syn::Item::Fn(f) => (&f.vis, f.sig.ident.to_string()),
			syn::Item::Mod(m) => (&m.vis, m.ident.to_string()),
			syn::Item::Use(u) => (&u.vis, "use".to_string()),
			syn::Item::Const(c) => (&c.vis, c.ident.to_string()),
			syn::Item::Type(t) => (&t.vis, t.ident.to_string()),
			syn::Item::Struct(_)
			| syn::Item::Enum(_)
			| syn::Item::Trait(_)
			| syn::Item::Static(_) => {
				panic!("compromise.rs: no types, traits or statics")
			}
			_ => continue,
		};
		if !matches!(vis, syn::Visibility::Inherited) {
			assert!(
				!matches!(vis, syn::Visibility::Public(_)),
				"compromise.rs: no pub item"
			);
			visible.push(name);
		}
	}
	assert_eq!(visible, ["use", "compromise_audit2"]);
	assert!(compromise.items.iter().any(|i| matches!(i, syn::Item::Fn(f) if f.sig.ident == "compromise_audit2" && matches!(&f.vis, syn::Visibility::Restricted(r) if r.path.is_ident("crate")))));
	assert!(
		compromise
			.items
			.iter()
			.any(|i| matches!(i, syn::Item::Use(u) if root2_scoped(&u.vis)))
	);
}

// C-S6b (ADR 0101 context export). The export is pure bytes in one private child of the audit,
// produced only after the trace has finished (a complete fold), from the one context encoder the
// role-proof digest hashes; the CLI checks the output path before anything is read and publishes
// the file (temp, fsync, RENAME_NOREPLACE, directory fsync) before any row is printed.
fn context_export_violation(
	compromise: &str,
	export: &str,
	role_command: &str,
	cli: &str,
	publish: &str,
) -> Option<&'static str> {
	let flat = |t: &str| t.replace(char::is_whitespace, "");
	// A missing anchor is a violation, never a pass.
	let ordered = |t: &str, anchors: &[&str]| {
		let at: Option<Vec<_>> = anchors.iter().map(|a| t.find(a)).collect();
		at.is_some_and(|at| at.windows(2).all(|w| w[0] < w[1]))
	};
	let (Some(digest), Some(digest3)) = (
		role_command
			.contains("fn context_digest(")
			.then(|| flat(fn_body(role_command, "context_digest"))),
		role_command
			.contains("fn context_digest3(")
			.then(|| flat(fn_body(role_command, "context_digest3"))),
	) else {
		return Some("role_command.rs: no context_digest or context_digest3");
	};
	// The withdrawal removes only the file this run published: fstatat (no symlink follow) of
	// the name, compared with the recorded identity, strictly before the unlink.
	let body = |name: &str| {
		publish
			.contains(&format!("fn {name}("))
			.then(|| flat(fn_body(publish, name)))
	};
	let (Some(withdraw), Some(identity_at)) = (body("withdraw"), body("identity_at")) else {
		return Some("root2_audit/publish.rs: withdraw and identity_at");
	};
	if !ordered(
		&withdraw,
		&[
			"identity_at(&target.parent,&target.name)",
			"Ok(found)iffound==published.0=>{unlinkat(&target.parent,&target.name);",
		],
	) || withdraw.matches("unlinkat(").count() != 1
		|| identity_at != "{identity(&openat(dir,name,libc::O_PATH|libc::O_NOFOLLOW,0)?)}"
	{
		return Some("root2_audit/publish.rs: withdraw unlinks only its own file");
	}
	let (compromise, role_command, cli, publish) = (
		flat(compromise),
		flat(role_command),
		flat(cli),
		flat(publish),
	);
	let Ok(tokens) = export.parse() else {
		return Some("compromise/export.rs: unparseable");
	};
	let production = flat(&without_test_items_deep(tokens).to_string());
	// The audit: one private child, each entry point called once, the export after `finish`.
	if compromise.matches("modexport;").count() != 1
		|| compromise.contains("pubmodexport")
		|| calls(&compromise, "export::export2") != 1
		|| calls(&compromise, "export::rotations2") != 1
		|| !ordered(&compromise, &["trace.finish()", "export::export2("])
		|| !ordered(&compromise, &["export::rotations2(", "audit::fold2("])
	{
		return Some("compromise.rs: the export is one private child, used after a complete fold");
	}
	// The export: pure, the context from the shared encoder, one integrity digest.
	for banned in [
		"std::fs",
		"std::io",
		"std::net",
		"File",
		"unsafe",
		"spawn",
		"libc",
		"SystemTime",
		"Instant",
		"VISROOT2-ROLE3-CONTEXT",
		"fncontext_bytes",
		"c.generation",
	] {
		if production.contains(banned) {
			return Some("compromise/export.rs: pure bytes from the shared context encoder");
		}
	}
	if calls(&production, "context_bytes") != 1 || production.matches("hash(b\"\",&b)").count() != 1
	{
		return Some("compromise/export.rs: one context encoding and one integrity digest");
	}
	// The single context encoder: one domain literal, in `context_digest3`, which hashes exactly
	// `context_bytes`; the installed administrator's `context_digest` is that over its context.
	if role_command.matches("fncontext_bytes(").count() != 1
		|| role_command.matches("VISROOT2-ROLE3-CONTEXT").count() != 1
		|| digest3 != "{hash(b\"VISROOT2-ROLE3-CONTEXT\\0\",&context_bytes(c))}"
		|| digest != "{context_digest3(&admin_context2(a))}"
	{
		return Some("role_command.rs: context_digest hashes context_bytes and nothing else");
	}
	// The CLI order: path check, trust inputs, fold, the exit-0 gate, publish, stdout, and the
	// withdrawal of a published export when stdout then fails.
	let order = [
		"publish::target(",
		"read_bounded(&options.manifest)",
		"runtime.block_on(compromise_audit2(",
		"ifletSome(target)=&target&&code==0{",
		"publish::publish(target,&bytes)",
		"out.write_all(",
		"publish::withdraw(target,&file)",
	];
	if !ordered(&cli, &order)
		|| cli.matches("publish::publish(").count() != 1
		|| cli.matches("publish::withdraw(").count() != 1
	{
		return Some("root2_audit.rs: path check first, publish before any row");
	}
	// The publish: exclusive 0600 temp beside the target, no-replace rename, no plain rename.
	if publish.contains("storage::mutation_visibility")
		|| publish.contains("std::fs::rename")
		|| publish.contains("libc::rename(")
		|| publish.contains("libc::renameat(")
		|| calls(&publish, "libc::renameat2") != 1
		|| publish.matches("libc::RENAME_NOREPLACE").count() != 1
		|| !publish.contains("libc::O_WRONLY|libc::O_CREAT|libc::O_EXCL|libc::O_NOFOLLOW,0o600,")
		|| !publish.contains("constTEMP_PREFIX:&str=\".wrctxexp.tmp.\";")
		|| !publish.contains("\"{TEMP_PREFIX}{}.{}\"")
		|| !publish.contains("name.starts_with(TEMP_PREFIX.as_bytes())")
		|| publish.matches("unsafe{").count() != 4
		|| publish.matches("//SAFETY:").count() != 4
		// The recorded identity is the temp file's own, from its descriptor, before the rename.
		|| !ordered(
			&publish,
			&[
				"file.metadata()",
				"Published((meta.dev(),meta.ino()))",
				"rename_noreplace(&target.parent,&temp,&target.name)",
			],
		) {
		return Some("root2_audit/publish.rs: atomic no-replace publish, documented unsafe");
	}
	None
}
#[test]
fn context_export_is_pure_after_a_complete_fold_and_published_before_rows() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root2 = src.join("storage/mutation_visibility/root2");
	let read = |p: PathBuf| fs::read_to_string(p).unwrap();
	let compromise = read(root2.join("compromise.rs"));
	let export = read(root2.join("compromise/export.rs"));
	let role_command = read(root2.join("role_command.rs"));
	let cli = read(src.join("cli/root2_audit.rs"));
	let publish = read(src.join("cli/root2_audit/publish.rs"));
	let check =
		|c: &str, e: &str, r: &str, l: &str, p: &str| context_export_violation(c, e, r, l, p);
	assert_eq!(
		check(&compromise, &export, &role_command, &cli, &publish),
		None
	);
	// Probes: each guarded property, broken once, is caught.
	let probes: [(&str, String, String, String, String, String); 17] = [
		(
			"a withdrawal that unlinks without the identity check",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.replace("Ok(found) if found == published.0 =>", "_ if true =>"),
		),
		(
			"a withdrawal that follows a symlink",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.replace(
				"libc::O_PATH | libc::O_NOFOLLOW, 0)?)",
				"libc::O_PATH, 0)?)",
			),
		),
		(
			"no withdrawal when stdout fails after the publish",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.replace("publish::withdraw(target, &file)", "Ok::<(), String>(())"),
			publish.clone(),
		),
		(
			"publishing outside the exit-0 gate",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.replace("&& code == 0", "&& code < 5"),
			publish.clone(),
		),
		(
			"a renameat fallback",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.clone() + "\nfn f() { libc::renameat(0, p, 0, q); }",
		),
		(
			"a temp-prefixed output name accepted",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.replace("name.starts_with(TEMP_PREFIX.as_bytes())", "false"),
		),
		(
			"the export before the fold finished",
			compromise.replace("trace.finish()", "trace.done()"),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.clone(),
		),
		(
			"a public export module",
			compromise.replace("\nmod export;", "\npub mod export;"),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.clone(),
		),
		(
			"file I/O in the export",
			compromise.clone(),
			format!("{export}\nfn w() {{ std::fs::write(\"x\", []).unwrap(); }}"),
			role_command.clone(),
			cli.clone(),
			publish.clone(),
		),
		(
			"a second context encoder in the export",
			compromise.clone(),
			export.replace(
				"b.extend(context_bytes(&context));",
				"b.extend(context.generation);",
			),
			role_command.clone(),
			cli.clone(),
			publish.clone(),
		),
		(
			"context_digest with its own encoding",
			compromise.clone(),
			export.clone(),
			role_command.replace(
				"&context_bytes(c))",
				"&{ let mut b = Vec::new(); b.extend(c.generation); b })",
			),
			cli.clone(),
			publish.clone(),
		),
		(
			"context_digest bypassing context_digest3",
			compromise.clone(),
			export.clone(),
			role_command.replace(
				"context_digest3(&admin_context2(a))",
				"hash(b\"VISROOT2-ROLE3-CONTEXT\\0\", &context_bytes(&admin_context2(a)))",
			),
			cli.clone(),
			publish.clone(),
		),
		(
			"a second context domain",
			compromise.clone(),
			export.clone(),
			role_command.clone() + "\nconst D: &[u8] = b\"VISROOT2-ROLE3-CONTEXT\\0\";",
			cli.clone(),
			publish.clone(),
		),
		(
			"rows printed before the export is published",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.replace(
				"publish::publish(target, &bytes)",
				"publish::later(target, &bytes)",
			) + "\nfn z() { publish::publish(target, &bytes); }",
			publish.clone(),
		),
		(
			"the path check after the trust inputs",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.replace("publish::target(", "publish::checked(")
				+ "\nfn z() { publish::target(); }",
			publish.clone(),
		),
		(
			"a plain rename fallback",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.clone() + "\nfn f() { let _ = std::fs::rename(\"a\", \"b\"); }",
		),
		(
			"an undocumented unsafe block",
			compromise.clone(),
			export.clone(),
			role_command.clone(),
			cli.clone(),
			publish.clone() + "\nfn f() { unsafe { libc::sync() } }",
		),
	];
	for (what, c, e, r, l, p) in &probes {
		assert!(check(c, e, r, l, p).is_some(), "probe not caught: {what}");
	}
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
						} else if f.sig.ident == "verified2" {
							// Stage C: the one production VerifiedCommand2 constructor; its single
							// call site is pinned by command_verification_is_single_site_and_unwired.
							production_factories += 1;
							assert!(!test_only(&f.attrs));
							assert!(
								matches!(&*i.self_ty, syn::Type::Path(p) if p.path.is_ident("VerifiedCommand2"))
							);
							assert!(
								matches!(&f.vis, syn::Visibility::Restricted(r) if r.path.is_ident("super"))
							);
						} else if f.sig.ident == "bind" {
							production_factories += 1;
							assert!(!test_only(&f.attrs));
							assert!(
								matches!(&*i.self_ty, syn::Type::Path(p) if p.path.is_ident("TrustedEntry2"))
							);
							assert!(
								matches!(&f.vis, syn::Visibility::Restricted(r) if r.path.is_ident("super"))
							);
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
									|| f.sig.ident == "entry_fields"
									|| f.sig.ident == "role_seed3",
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
	assert_eq!(production_factories, 3);
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
		"issuer_registry.rs",
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
						"ManifestRole3",
						"RolePair3",
						"ReuseField3",
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
						"GovernanceRole2",
						"RecoveryRole3",
						"AdminManifestThree3",
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
	assert_eq!(entrypoints, ADMIN_ENTRYPOINTS);
	// The concrete profile-3 census: decode x3, encode x3, new x3, base x1, governance x1.
	let count = |n: &str| entrypoints.iter().filter(|e| *e == n).count();
	assert_eq!(
		["decode", "encode", "new", "base", "governance"].map(count),
		[3, 3, 3, 1, 1]
	);
	// Probe: a re-added digest entrypoint is a different census.
	let mut probe = entrypoints.clone();
	probe.extend(
		admin_items(
			&syn::parse_file(
				"pub(in crate::storage::mutation_visibility::root2) fn legacy_digest2() {}",
			)
			.unwrap(),
		)
		.unwrap(),
	);
	probe.sort();
	assert_ne!(probe, ADMIN_ENTRYPOINTS);
}
// The admin entrypoints after the concrete profile-3 types (d): no profile-1/2 codec,
// digest, digest check or structural peek remains.
const ADMIN_ENTRYPOINTS: [&str; 21] = [
	"actions",
	"actions",
	"base",
	"check_enrollable_admin_key2",
	"decode",
	"decode",
	"decode",
	"encode",
	"encode",
	"encode",
	"first_mismatch",
	"governance",
	"id",
	"id",
	"key",
	"key",
	"manifest_digest3",
	"new",
	"new",
	"new",
	"recovery",
];
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
			"advance_retired2",
			"advance_root2",
			"bootstrap2",
			"coverage2",
			"decode",
			"empty_authority2",
			"encode",
			"encode_authority2",
			"origin_view2",
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
			syn::Item::Mod(m) if m.content.is_none() => { if m.ident=="format" {assert!(matches!(m.vis,syn::Visibility::Inherited));} children.push(m.ident.to_string()); },
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
			"snapshot",
			"terminal"
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
// files only. Directory format 2 is the only owner format: there is no v1 codec, owner core,
// evidence module or format dispatch.
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
	// Scoped negative controls: terminal codecs retain the original token bans.
	for token in ["Root2", "File", "TrustedTerminal3"] {
		assert!(
			banned(
				format!("fn forbidden() -> {token} {{ todo!() }}")
					.parse()
					.unwrap()
			)
			.is_some()
		);
	}
	let mut entrypoints = Vec::new();
	for name in [
		"inventory.rs",
		"snapshot.rs",
		"owner_core.rs",
		"replica_anchor.rs",
		"terminal.rs",
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
				syn::Item::Mod(m)
					if matches!(name, "inventory.rs" | "owner_core.rs") && m.ident == "v2" =>
				{
					assert!(m.content.is_none());
					assert!(
						matches!(&m.vis, syn::Visibility::Restricted(v) if v.in_token.is_none() && v.path.is_ident("super"))
					);
				}
				syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
					panic!("{name}: no traits, statics or modules")
				}
				_ => {}
			}
		}
	}

	// Exact new private format surface; retain the same no-I/O/no-trust token bans.
	for name in ["inventory/v2.rs", "owner_core/v2.rs"] {
		let text = fs::read_to_string(root.join(name)).unwrap();
		assert_eq!(
			banned(text.parse().unwrap()),
			None,
			"{name}: prohibited token"
		);
		let mut functions = Vec::new();
		let mut constants = Vec::new();
		for item in syn::parse_file(&text).unwrap().items {
			match item {
				syn::Item::Use(_) => {}
				syn::Item::Const(c) => {
					assert!(!matches!(c.vis, syn::Visibility::Public(_)));
					if !matches!(c.vis, syn::Visibility::Inherited) {
						constants.push(c.ident.to_string());
					}
				}
				syn::Item::Fn(f) => {
					assert!(!matches!(f.vis, syn::Visibility::Public(_)));
					if !matches!(f.vis, syn::Visibility::Inherited) {
						functions.push(f.sig.ident.to_string());
					}
				}
				_ => panic!("{name}: unexpected producer/type/module item"),
			}
		}
		functions.sort();
		constants.sort();
		let (f, c): (&[&str], &[&str]) = match name {
			"inventory/v2.rs" => (
				&["decode_event2", "encode_event2", "fold_events2"],
				&["EVENT_BYTES2", "MAX_LOG_BYTES2", "MAX_TAIL2"],
			),
			"owner_core/v2.rs" => (
				&[
					"classify2",
					"decide2",
					"decode_evidence2",
					"encode_evidence2",
					"evidence_name2",
					"expected_repair_event2",
					"repair_context2",
					"repair_op_id2",
				],
				&["MAX_EVIDENCE_BYTES2"],
			),
			_ => unreachable!("{name}"),
		};
		assert_eq!(functions, f, "{name}: free functions");
		assert_eq!(constants, c, "{name}: constants");
	}

	entrypoints.sort();
	assert_eq!(entrypoints, PROVISIONING_ENTRYPOINTS);
	// Probe: one more root2-scoped free function in a scanned file is a different census.
	let mut probe = entrypoints.clone();
	let extra = "pub(in crate::storage::mutation_visibility::root2) fn decode_event2() {}";
	for item in syn::parse_file(extra).unwrap().items {
		if let syn::Item::Fn(f) = item
			&& root2_scoped(&f.vis)
		{
			probe.push(f.sig.ident.to_string());
		}
	}
	probe.sort();
	assert_ne!(probe, PROVISIONING_ENTRYPOINTS);
}
// The exact provisioning entrypoints once format 2 is the only owner format: no v1 inventory codec
// or fold, owner core, classifier or evidence codec remains outside the private `v2` children.
const PROVISIONING_ENTRYPOINTS: [&str; 20] = [
	"decode_snapshot2",
	"decode_terminal3",
	"encode_snapshot2",
	"encode_terminal3",
	"encode_terminal_artifact3",
	"encode_terminal_observation3",
	"event_request_key2",
	"observation_digest3",
	"op_id2",
	"read_terminal3",
	"request_key2",
	"terminal_record3",
	"tuple_entry_last_seq2",
	"tuple_export_seq2",
	"tuple_inventory_pin2",
	"tuple_manifest2",
	"tuple_manifest_len2",
	"tuple_manifest_pin2",
	"tuple_record2",
	"validate_replica_tuple2",
];
// Owner I/O: std only, zero unsafe, libc constants only, no network, clock, env, trust or root
// construction; exactly nine exported methods on OwnerDir2 and no exported free functions. Its
// one private child, owner_log/ops.rs, obeys the same bans and exposes nothing beyond owner_log.
// Syntax only; #[cfg(test)] items are excluded from the production surface.
#[test]
fn provisioning_owner_log_is_std_only_with_nine_methods() {
	let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("src/storage/mutation_visibility/root2/provisioning");
	let text = fs::read_to_string(dir.join("owner_log.rs")).unwrap();
	let ops = fs::read_to_string(dir.join("owner_log/ops.rs")).unwrap();
	let directory = fs::read_to_string(dir.join("owner_log/directory.rs")).unwrap();
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
	for (name, source) in [
		("owner_log.rs", &text),
		("owner_log/ops.rs", &ops),
		("owner_log/directory.rs", &directory),
	] {
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
	for item in syn::parse_file(&text)
		.unwrap()
		.items
		.into_iter()
		.chain(syn::parse_file(&directory).unwrap().items)
	{
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
						if f.sig.ident == "check_framing" {
							assert!(
								matches!(&f.vis,syn::Visibility::Restricted(v) if v.in_token.is_none() && v.path.is_ident("super"))
							);
						} else {
							assert!(root2_scoped(&f.vis), "{}", f.sig.ident);
							methods.push(f.sig.ident.to_string());
						}
					}
				}
			}
			syn::Item::Trait(t) if !cfg_test(&t.attrs) => panic!("owner_log.rs: trait"),
			syn::Item::Static(s) if !cfg_test(&s.attrs) => panic!("owner_log.rs: static"),
			syn::Item::Mod(m) if !cfg_test(&m.attrs) => assert!(
				(m.ident == "ops" || m.ident == "directory")
					&& m.content.is_none()
					&& matches!(m.vis, syn::Visibility::Inherited),
				"owner_log.rs: only private ops/directory children"
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
			"create_v2",
			"export",
			"export_terminal",
			"export_tuple",
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
			"active_role3",
			"admin_active3",
			"admin_context2",
			"admin_cut3",
			"admin_governance2",
			"admin_id2",
			"admin_installed_at2",
			"admin_key2",
			"admin_manifest_len2",
			"admin_namespace2",
			"admin_recovery3",
			"confirm_terminal2",
			"install_admin2",
			"observe_bootstrap2",
			"observe_retirement3",
			"original_roles3"
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
			"check_epoch_statement3",
			"checked_action2",
			"checked_context2",
			"checked_key2",
			"encode_admin_statement2",
			"encode_epoch_statement3",
			"epoch_statement_preimage3",
			"inspect_admin_statement2",
			"inspect_epoch_statement3"
		]
	);
	// Inspection facts never become checked statements: declaration plus the two strict checkers.
	assert_eq!(text.matches("CheckedStatement2 {").count(), 3);
	assert_eq!(
		fn_body(&text, "check_epoch_statement3")
			.matches("CheckedStatement2 {")
			.count(),
		1
	);
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
	assert_eq!(state.matches("VISAUT04").count(), 2);
	assert!(!state.contains("VISAUT03"));
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
	assert_eq!(
		reducer
			.matches("Command2::EpochAdmissionPermission")
			.count(),
		2
	);
	let compact = reducer.replace(char::is_whitespace, "");
	let predicate = "ifmatches!(command,Command2::AdmissionPermission(_)|Command2::EpochAdmissionPermission(_))";
	assert_eq!(compact.matches(predicate).count(), 1);
	assert!(compact.contains(&format!(
		"{predicate}{{returnErr(RootError2::TrustMismatch);}}"
	)));
	let gate = reducer.find("if matches!(").unwrap();
	let replay = reducer.find("if l.index == s.covered.index {").unwrap();
	let gap = reducer.find("LineageGap").unwrap();
	let authorized = reducer
		.find("let result = if super::role_freeze::blocked")
		.unwrap();
	let result_block = &reducer[authorized..];
	let result_block = result_block.split_once("};").unwrap().0;
	fn frozen_before_auth(block: &str) -> bool {
		let flat = block.replace(char::is_whitespace, "");
		flat.starts_with("letresult=ifsuper::role_freeze::blocked(&old.state,command){denied(23,None)}elseifverified.authorized(){")
			&& flat.contains("}else{denied(8,None)")
	}
	assert!(frozen_before_auth(result_block));
	assert!(!frozen_before_auth(
		&result_block.replace("super::role_freeze::blocked", "removed")
	));
	assert!(!frozen_before_auth(&result_block.replace(
		"if super::role_freeze::blocked",
		"if verified.authorized"
	)));
	assert!(!frozen_before_auth(
		&result_block.replace("denied(23, None)", "denied(8, None)")
	));
	assert!(gate < replay && gate < gap && gate < authorized);
	for variant in ["AdmissionPermission", "EpochAdmissionPermission"] {
		assert!(compact.contains(&format!(
			"Command2::{variant}(_)=>Err(RootError2::TrustMismatch)"
		)));
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
	assert_eq!(calls(&gate, "check_epoch_statement3"), 1);
	assert!(
		reduce.find("check_epoch_statement3(").unwrap() < reduce.find("apply_admission(").unwrap()
	);
	assert!(
		reduce.find("admin_cut3(admin)").unwrap()
			< reduce.find("if l.index == s.covered.index").unwrap()
	);
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
	// Stage C: advance_admission2's one production caller is advance_verified2, which is itself
	// uncalled in production (pinned by command_verification_is_single_site_and_unwired).
	assert_eq!((reduce_calls, advance_defs, advance_calls), (1, 1, 1));
	let verification = without_test_items(
		fs::read_to_string(root.join("verification.rs"))
			.unwrap()
			.parse()
			.unwrap(),
	)
	.to_string()
	.replace(' ', "");
	assert_eq!(verification.matches("advance_admission2(").count(), 1);
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
		if f.ends_with("root2/recovery.rs")
			|| f.ends_with("root2/replay.rs")
			|| f.starts_with(root.join("replay"))
		{
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
			// The one permitted trait impl: Drop for MarkStore2 whose body is exactly
			// the explicit unlock, so a forked child sharing the lock's descriptor
			// cannot keep it held past the drop. Anything else is still refused.
			syn::Item::Impl(i)
				if !cfg_test(&i.attrs)
					&& i.trait_
						.as_ref()
						.is_some_and(|(_, path, _)| path.is_ident("Drop")) =>
			{
				assert_eq!(
					quote_type(&i.self_ty),
					"MarkStore2",
					"mark_store.rs: Drop impl"
				);
				let only_unlock = |f: &syn::ImplItemFn| {
					let [syn::Stmt::Local(local)] = f.block.stmts.as_slice() else {
						return false;
					};
					let Some(init) = &local.init else {
						return false;
					};
					let syn::Expr::MethodCall(call) = &*init.expr else {
						return false;
					};
					let syn::Expr::Field(field) = &*call.receiver else {
						return false;
					};
					matches!(&local.pat, syn::Pat::Wild(_))
						&& call.method == "unlock"
						&& call.args.is_empty()
						&& matches!(&field.member, syn::Member::Named(name) if name == "_lock")
						&& matches!(&*field.base, syn::Expr::Path(base) if base.path.is_ident("self"))
				};
				assert!(
					i.items.len() == 1
						&& matches!(&i.items[0], syn::ImplItem::Fn(f) if f.sig.ident == "drop" && only_unlock(f)),
					"mark_store.rs: Drop must only unlock"
				);
			}
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
		if file == path
			|| file.starts_with(&tests)
			|| file.ends_with("root2/replay.rs")
			|| file.starts_with(src.join("storage/mutation_visibility/root2/replay"))
		{
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
	let mut sidecar_constants = 0;
	let mut arms: Vec<String> = Vec::new();
	fn collect(p: &syn::Pat, out: &mut Vec<String>) {
		match p {
			syn::Pat::Or(o) => o.cases.iter().for_each(|c| collect(c, out)),
			syn::Pat::TupleStruct(t) => {
				// `WendelonRequest::Variant(_)` only: no binding, no field access.
				assert!(
					t.elems.len() == 1
						&& (matches!(t.elems.first(), Some(syn::Pat::Wild(_)))
							|| (t
								.path
								.segments
								.last()
								.is_some_and(|s| s.ident == "Root2Command")
								&& matches!(t.elems.first(), Some(syn::Pat::Ident(i)) if i.ident == "carrier" && i.by_ref.is_none() && i.mutability.is_none() && i.subpat.is_none()))),
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
	fn shape(t: &syn::Type) -> String {
		match t {
			syn::Type::Path(p) => {
				assert!(
					p.qself.is_none()
						&& p.path.leading_colon.is_none()
						&& p.path.segments.len() == 1
				);
				let s = &p.path.segments[0];
				let mut out = s.ident.to_string();
				match &s.arguments {
					syn::PathArguments::None => {}
					syn::PathArguments::AngleBracketed(a) => {
						out.push('<');
						let args: Vec<_> = a
							.args
							.iter()
							.map(|g| match g {
								syn::GenericArgument::Lifetime(l) => format!("'{}", l.ident),
								syn::GenericArgument::Type(t) => shape(t),
								_ => panic!("unreviewed adapter generic"),
							})
							.collect();
						out.push_str(&args.join(","));
						out.push('>');
					}
					_ => panic!("unreviewed adapter path"),
				}
				out
			}
			syn::Type::Reference(r) => {
				assert!(r.mutability.is_none());
				format!(
					"&{}{}",
					r.lifetime
						.as_ref()
						.map(|l| format!("'{}", l.ident))
						.unwrap_or_default(),
					shape(&r.elem)
				)
			}
			syn::Type::Slice(t) => format!("[{}]", shape(&t.elem)),
			syn::Type::Array(a) => {
				let syn::Expr::Lit(l) = &a.len else {
					panic!("array length")
				};
				let syn::Lit::Int(i) = &l.lit else {
					panic!("array integer")
				};
				format!(
					"[{};{}]",
					shape(&a.elem),
					i.base10_parse::<usize>().unwrap()
				)
			}
			syn::Type::Tuple(t) => format!(
				"({})",
				t.elems.iter().map(shape).collect::<Vec<_>>().join(",")
			),
			_ => panic!("unreviewed adapter type"),
		}
	}
	fn self_field(e: &syn::Expr, name: &str) -> bool {
		matches!(e,syn::Expr::Field(f) if matches!(&*f.base,syn::Expr::Path(p) if p.path.is_ident("self")) && matches!(&f.member,syn::Member::Named(n) if n==name))
	}
	for item in &file.items {
		match item {
			syn::Item::Struct(s) if s.ident == "CommittedSource2" => {
				assert!(s.attrs.iter().all(|a| !a.path().is_ident("derive")));
				for f in &s.fields {
					assert!(matches!(f.vis, syn::Visibility::Inherited));
				}
			}
			syn::Item::Struct(v) if v.ident == "AdaptedEntry2" || v.ident == "CarrierCommand2" => {
				assert!(root2_scoped(&v.vis));
				assert!(v.attrs.iter().all(|a| a.path().is_ident("doc")));
				assert!(
					v.fields
						.iter()
						.all(|f| matches!(f.vis, syn::Visibility::Inherited))
				);
				assert!(
					v.generics.params.len() == 1
						&& v.generics.where_clause.is_none()
						&& matches!(v.generics.params.first(),Some(syn::GenericParam::Lifetime(l)) if l.lifetime.ident=="e" && l.bounds.is_empty())
				);
				let actual: Vec<_> = v
					.fields
					.iter()
					.map(|f| (f.ident.as_ref().unwrap().to_string(), shape(&f.ty)))
					.collect();
				let want: &[(&str, &str)] = if v.ident == "CarrierCommand2" {
					&[
						("generation", "[u8;16]"),
						("cluster", "[u8;16]"),
						("command", "Command2"),
						("governance", "Option<&'e[u8]>"),
					]
				} else {
					&[
						("committed", "CommittedEntry2"),
						("raw_carrier", "bool"),
						("carrier", "Option<CarrierCommand2<'e>>"),
					]
				};
				assert_eq!(
					actual,
					want.iter()
						.map(|(n, t)| (n.to_string(), t.to_string()))
						.collect::<Vec<_>>()
				);
			}
			syn::Item::Const(c) => {
				assert_eq!(c.ident, "GOVERNANCE_SIDECAR_BYTES2");
				assert!(root2_scoped(&c.vis));
				assert!(c.attrs.iter().all(|a| a.path().is_ident("doc")));
				assert!(c.generics.params.is_empty() && c.generics.where_clause.is_none());
				assert_eq!(shape(&c.ty), "usize");
				assert!(
					matches!(&*c.expr, syn::Expr::Lit(l) if matches!(&l.lit, syn::Lit::Int(i) if i.base10_parse::<usize>().unwrap() == 296))
				);
				sidecar_constants += 1;
			}
			syn::Item::Impl(i) => {
				assert!(i.trait_.is_none(), "committed.rs: trait impl");
				let ty = shape(&i.self_ty);
				if ty == "CarrierCommand2<'e>" || ty == "AdaptedEntry2<'e>" {
					assert!(
						i.generics.params.len() == 1
							&& matches!(i.generics.params.first(),Some(syn::GenericParam::Lifetime(l)) if l.lifetime.ident=="e" && l.bounds.is_empty())
					);
					let carrier = ty == "CarrierCommand2<'e>";
					let allowed: &[&str] = if carrier {
						&["into_parts"]
					} else {
						&["committed", "is_carrier", "into_parts", "into_committed"]
					};
					let names: Vec<_> = i.items.iter().map(|item| match item { syn::ImplItem::Fn(f) => {
                        assert!(root2_scoped(&f.vis));
                        let name=f.sig.ident.to_string();
                        assert_eq!(cfg_test(&f.attrs),name=="into_committed");
                        assert!(f.sig.inputs.len()==1 && f.sig.generics.params.is_empty() && f.sig.asyncness.is_none() && f.sig.constness.is_none() && f.sig.unsafety.is_none() && f.sig.abi.is_none() && f.sig.variadic.is_none());
                        let Some(syn::FnArg::Receiver(recv))=f.sig.inputs.first() else {panic!("accessor receiver")};
                        assert!(recv.mutability.is_none() && recv.colon_token.is_none());
                        assert_eq!(recv.reference.is_some(),name=="committed" || name=="is_carrier");
                        let syn::ReturnType::Type(_,ret)=&f.sig.output else {panic!("accessor return")};
                        let want=match name.as_str(){"committed"=>"&CommittedEntry2","is_carrier"=>"bool","into_committed"=>"CommittedEntry2","into_parts" if carrier=>"([u8;16],[u8;16],Command2,Option<&'e[u8]>)","into_parts"=>"(CommittedEntry2,bool,Option<CarrierCommand2<'e>>)",_=>panic!("unreviewed accessor")};
                        assert_eq!(shape(ret),want);
                        assert_eq!(f.block.stmts.len(),1);
                        let syn::Stmt::Expr(body,None)=&f.block.stmts[0] else {panic!("accessor expression only")};
                        match name.as_str(){
                            "committed"=>assert!(matches!(body,syn::Expr::Reference(r) if r.mutability.is_none() && self_field(&r.expr,"committed"))),
                            "is_carrier"=>assert!(self_field(body,"raw_carrier")),
                            "into_committed"=>assert!(self_field(body,"committed")),
                            "into_parts"=>{let syn::Expr::Tuple(t)=body else {panic!("accessor fields only")};let fields:&[&str]=if carrier{&["generation","cluster","command","governance"]}else{&["committed","raw_carrier","carrier"]};assert_eq!(t.elems.len(),fields.len());assert!(t.elems.iter().zip(fields).all(|(e,n)|self_field(e,n)));},
                            _=>unreachable!()
                        }
                        name
                    }, _ => panic!("adapter accessors only") }).collect();
					assert_eq!(names, allowed);
					continue;
				}
				assert_eq!(ty, "CommittedSource2");
				for it in &i.items {
					if let syn::ImplItem::Fn(f) = it {
						if f.sig.ident == "synthetic" {
							assert!(cfg_test(&f.attrs));
							mints += 1;
						} else {
							assert_eq!(f.sig.ident, "acquired");
							assert!(root2_scoped(&f.vis) && !cfg_test(&f.attrs));
						}
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
						let mut variants = Vec::new();
						collect(&arm.pat, &mut variants);
						let carrier = variants == ["Root2Command"];
						assert!(!variants.iter().any(|v| v == "Root2Command") || carrier);
						if carrier {
							assert!(
								matches!(&*arm.body, syn::Expr::Block(_)),
								"one narrow carrier decode arm"
							);
							let body = fn_body(&text, "request_class");
							assert_eq!(calls(body, "Command2::decode"), 1);
							assert!(body.contains("command.digest(generation, cluster)"));
							assert!(body.contains("EntryClass2::Bootstrap(digest)"));
							assert!(body.contains("EntryClass2::OtherRoot2(digest)"));
							assert!(body.contains("EntryClass2::MalformedRoot2"));
						} else {
							assert!(
								matches!(&*arm.body, syn::Expr::Tuple(t) if t.elems.len()==2 && matches!(&t.elems[0],syn::Expr::Path(p) if exact_path(&p.path,&["EntryClass2","NonRoot2"])) && matches!(&t.elems[1],syn::Expr::Path(p) if p.path.is_ident("None")))
							);
						}
						arms.extend(variants);
					}
					assert_eq!(f.block.stmts.len(), 1, "request_class is only the match");
				}
			}
			syn::Item::Mod(m) if m.ident == "fixture" && cfg_test(&m.attrs) => {}
			syn::Item::Trait(_) | syn::Item::Static(_) | syn::Item::Mod(_) => {
				panic!("committed.rs: unexpected item")
			}
			_ => {}
		}
	}
	assert_eq!(entrypoints, ["committed_entry2"]);
	assert_eq!(mints, 1);
	assert_eq!(sidecar_constants, 1);
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
	let production_tokens = without_test_items_deep(text.parse().unwrap());
	let mut production = Vec::new();
	idents(production_tokens.clone(), &mut production);
	let compact = production_tokens.to_string().replace(' ', "");
	assert_eq!(
		production
			.iter()
			.filter(|w| *w == "GOVERNANCE_SIDECAR_BYTES2")
			.count(),
		2
	);
	assert_eq!(
		compact
			.matches("bytes.len()!=GOVERNANCE_SIDECAR_BYTES2")
			.count(),
		1
	);
	assert!(!production.iter().any(|w| w == "GOVERNANCE_COMMAND_BYTES2"));
	// Five reviewed Command2 identifiers only: the exact import, carrier field, accessor
	// return type, decode call and Bootstrap classification pattern. The field and accessor
	// schemas above pin two occurrences; the remaining three are pinned here. No blanket
	// Command2 exemption permits another associated method, type use or constructor.
	assert_eq!(production.iter().filter(|w| *w == "Command2").count(), 5);
	assert_eq!(
		compact
			.matches("usecrate::storage::mutation_visibility::root2::command::Command2;")
			.count(),
		1
	);
	assert_eq!(compact.matches("Command2::").count(), 2);
	assert_eq!(
		compact
			.matches("Command2::decode(carrier.command())")
			.count(),
		1
	);
	assert_eq!(
		compact
			.matches("matches!(command,Command2::Bootstrap{..})")
			.count(),
		1
	);
	// Reject any additional method on the retained command, even if it does not spell
	// Command2. The one digest call must use this decoded envelope's context arguments.
	fn command_methods(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
		let ts: Vec<_> = tokens.into_iter().collect();
		for (i, t) in ts.iter().enumerate() {
			if matches!(t, proc_macro2::TokenTree::Ident(id) if id == "command")
				&& matches!(ts.get(i + 1), Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == '.')
				&& let (
					Some(proc_macro2::TokenTree::Ident(method)),
					Some(proc_macro2::TokenTree::Group(args)),
				) = (ts.get(i + 2), ts.get(i + 3))
				&& args.delimiter() == proc_macro2::Delimiter::Parenthesis
			{
				out.push(format!(
					"{}({})",
					method,
					args.stream().to_string().replace(' ', "")
				));
			}
			if let proc_macro2::TokenTree::Group(g) = t {
				command_methods(g.stream(), out);
			}
		}
	}
	let mut methods = Vec::new();
	command_methods(production_tokens, &mut methods);
	assert_eq!(methods, ["digest(generation,cluster)"]);
	for banned in [
		"fs",
		"io",
		"net",
		"tokio",
		"Instant",
		"SystemTime",
		"Root2",
		"State2",
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
	let retirement = fs::read_to_string(root.join("retirement.rs")).unwrap();
	let retirement = without_test_items_deep(retirement.parse().unwrap()).to_string();
	assert_eq!(
		retirement
			.matches("cached_capacity (& next . state , next . limits)")
			.count(),
		1
	);
	assert_eq!(
		producers,
		[
			"bootstrap_root",
			"consume",
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

// Committed-prefix shape validation (T-S1): pure over captured values. No store read, no
// committed-source mint or adaptation, no reducer, origin or authorization; one root2-scoped
// function and its error type; no impls, traits, statics or modules; no caller outside its own
// module and the root2 tests. Syntax only; #[cfg(test)] items are excluded from the surface.
#[test]
fn committed_prefix_check_is_pure_and_unwired() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("prefix.rs")).unwrap();
	assert!(text.lines().count() <= 200);
	fn forbidden(tokens: proc_macro2::TokenStream) -> Option<String> {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if [
						"unsafe",
						"async",
						"await",
						"fs",
						"io",
						"net",
						"tokio",
						"Raft",
						"RaftLogReader",
						"try_get_log_entries",
						"get_log_entries",
						"CommittedSource2",
						"committed_entry2",
						"from_source",
						"CommittedEntry2",
						"TrustedEntry2",
						"VerifiedCommand2",
						"TrustedView2",
						"MutationAuthorityState2",
						"StateInner2",
						"Root2",
						"bootstrap2",
						"install_admin2",
						"advance_root2",
						"advance_pre_root2",
						"advance_cover2",
						"advance_admission2",
						"reduce",
						"reduce_admission",
						"authorized",
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
		forbidden(without_test_items_deep(text.parse().unwrap())),
		None,
		"prefix.rs: prohibited token"
	);
	let file = syn::parse_file(&text).unwrap();
	let cfg_test = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| {
			a.path().is_ident("cfg")
				&& matches!(&a.meta, syn::Meta::List(l) if l.tokens.to_string().contains("test"))
		})
	};
	let mut exports = Vec::new();
	for item in &file.items {
		match item {
			syn::Item::Fn(f) if cfg_test(&f.attrs) => {}
			syn::Item::Fn(f) if root2_scoped(&f.vis) => exports.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => assert!(matches!(f.vis, syn::Visibility::Inherited)),
			syn::Item::Enum(e) => assert_eq!(e.ident, "PrefixError2"),
			syn::Item::Impl(_)
			| syn::Item::Trait(_)
			| syn::Item::Static(_)
			| syn::Item::Mod(_)
			| syn::Item::Struct(_) => panic!("prefix.rs: no impls, traits, statics, modules or structs"),
			_ => {}
		}
	}
	assert_eq!(exports, ["check_committed_prefix2"]);
	// Dormant: nothing outside the module and the root2 tests names it.
	let tests = root.join("tests");
	let mut stack = vec![src];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != tests {
					stack.push(p);
				}
			} else if p.extension().is_some_and(|x| x == "rs") && !p.ends_with("root2/prefix.rs") {
				let t = fs::read_to_string(&p).unwrap();
				// The dormant acquisition driver (S2a) is the one consumer; its own guard pins it
				// to zero production callers.
				if p.ends_with("root2/acquisition.rs") {
					assert!(
						t.contains("check_committed_prefix2"),
						"acquisition uses the check"
					);
					continue;
				}
				for name in ["check_committed_prefix2", "PrefixError2", "prefix::"] {
					assert!(
						!t.contains(name),
						"production caller of {name} in {}",
						p.display()
					);
				}
			}
		}
	}
}

// Committed-prefix acquisition (S2a): the exact item census of the reviewed contract; no mint,
// adapter, trust token, authority state, reducer or origin entrypoint, spawn, filesystem or
// network; the report is never a parameter; no caller outside its own module and the root2 tests.
// Syntax only; #[cfg(test)] items are excluded from the surface.
#[test]
fn committed_prefix_acquisition_is_unminted_and_unwired() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("acquisition.rs")).unwrap();
	assert!(text.lines().count() <= 250);
	fn forbidden(tokens: proc_macro2::TokenStream) -> Option<String> {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if [
						"CommittedSource2",
						"committed_entry2",
						"from_source",
						"CommittedEntry2",
						"TrustedEntry2",
						"VerifiedCommand2",
						"TrustedView2",
						"MutationAuthorityState2",
						"StateInner2",
						"Root2",
						"bootstrap2",
						"install_admin2",
						"advance_root2",
						"advance_pre_root2",
						"advance_cover2",
						"advance_admission2",
						"reduce",
						"reduce_admission",
						"authorized",
						"spawn",
						"fs",
						"net",
						"unsafe",
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
	let surface = without_test_items_deep(text.parse().unwrap());
	assert_eq!(
		forbidden(surface.clone()),
		None,
		"acquisition.rs: prohibited token"
	);
	// No function's parameter list names the report.
	fn report_parameter(tokens: proc_macro2::TokenStream) -> bool {
		let flat: Vec<_> = tokens.into_iter().collect();
		for (i, t) in flat.iter().enumerate() {
			if let proc_macro2::TokenTree::Group(g) = t {
				if g.delimiter() == proc_macro2::Delimiter::Parenthesis
					&& i >= 2 && matches!(&flat[i - 2], proc_macro2::TokenTree::Ident(f) if f == "fn")
					&& g.stream().to_string().contains("PrefixValidated2")
				{
					return true;
				}
				if report_parameter(g.stream()) {
					return true;
				}
			}
		}
		false
	}
	assert!(!report_parameter(surface));
	let file = syn::parse_file(&text).unwrap();
	let cfg_test = |attrs: &[syn::Attribute]| {
		attrs.iter().any(|a| {
			a.path().is_ident("cfg")
				&& matches!(&a.meta, syn::Meta::List(l) if l.tokens.to_string().contains("test"))
		})
	};
	let (mut types, mut exported, mut private, mut impls) = (vec![], vec![], vec![], vec![]);
	for item in &file.items {
		match item {
			syn::Item::Fn(f) if cfg_test(&f.attrs) => {}
			syn::Item::Fn(f) if root2_scoped(&f.vis) => exported.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => {
				assert!(matches!(f.vis, syn::Visibility::Inherited));
				private.push(f.sig.ident.to_string())
			}
			syn::Item::Struct(s) => {
				assert!(root2_scoped(&s.vis));
				types.push(s.ident.to_string())
			}
			syn::Item::Enum(e) => {
				assert!(root2_scoped(&e.vis));
				types.push(e.ident.to_string())
			}
			syn::Item::Trait(t) => {
				assert!(root2_scoped(&t.vis) && t.supertraits.is_empty());
				let mut methods: Vec<_> = t
					.items
					.iter()
					.map(|i| match i {
						syn::TraitItem::Fn(m) => {
							assert!(m.default.is_none(), "no default methods");
							m.sig.ident.to_string()
						}
						_ => panic!("acquisition.rs: trait items are methods only"),
					})
					.collect();
				methods.sort();
				assert_eq!(methods, ["applied", "retention"]);
				types.push(t.ident.to_string())
			}
			syn::Item::Impl(i) => {
				assert!(i.trait_.is_none(), "acquisition.rs: no trait impls");
				let syn::Type::Path(p) = &*i.self_ty else {
					panic!("impl target")
				};
				let mut fns: Vec<_> = i
					.items
					.iter()
					.map(|x| match x {
						syn::ImplItem::Fn(m) => m.sig.ident.to_string(),
						_ => panic!("acquisition.rs: impl items are methods only"),
					})
					.collect();
				fns.sort();
				impls.push((p.path.segments.last().unwrap().ident.to_string(), fns));
			}
			syn::Item::Static(_) | syn::Item::Const(_) | syn::Item::Mod(_) => {
				panic!("acquisition.rs: no statics, consts or modules")
			}
			_ => {}
		}
	}
	types.sort();
	private.sort();
	assert_eq!(
		types,
		[
			"AcquisitionCaps2",
			"AcquisitionError2",
			"AcquisitionSources2",
			"FoldError2",
			"PrefixValidated2",
			"SourceClosed2"
		]
	);
	assert_eq!(exported, ["acquire_committed_prefix2"]);
	assert_eq!(private, ["add_bytes", "encoded_size", "next_range"]);
	assert_eq!(
		impls,
		[(
			"PrefixValidated2".to_string(),
			vec![
				"encoded_bytes".to_string(),
				"entries".to_string(),
				"terminal_applied".to_string(),
				"terminal_retention".to_string(),
				"through".to_string()
			]
		)]
	);
	// Dormant: nothing outside the module and the root2 tests names its surface.
	let tests = root.join("tests");
	let mut stack = vec![src];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != tests {
					stack.push(p);
				}
			} else if p.extension().is_some_and(|x| x == "rs")
				&& !p.ends_with("root2/acquisition.rs")
				&& !p.ends_with("root2/replay.rs")
				&& !p.starts_with(root.join("replay"))
			{
				let t = fs::read_to_string(&p).unwrap();
				for name in [
					"acquire_committed_prefix2",
					"AcquisitionSources2",
					"AcquisitionCaps2",
					"AcquisitionError2",
					"PrefixValidated2",
					"FoldError2",
					"SourceClosed2",
					"acquisition::",
				] {
					assert!(
						!t.contains(name),
						"production caller of {name} in {}",
						p.display()
					);
				}
			}
		}
	}
}

// Stage C (narrow slice): one production VerifiedCommand2 constructor (`verified2`), called only
// in `verify_command2`, which is private and called once, in `advance_verified2`; no other
// production function returns a VerifiedCommand2 or builds one by literal; `advance_verified2` is
// uncalled outside its module and the root2 tests; the module census is exact; no signing, I/O,
// clock, network or spawn. Syntax checks, not compiler resolution or a general purity proof.
#[test]
fn command_verification_is_single_site_and_unwired() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("verification.rs")).unwrap();
	assert!(text.lines().count() <= 350);
	let flat = without_test_items_deep(text.parse().unwrap())
		.to_string()
		.replace(' ', "");
	for banned in [
		"SigningKey",
		"Signer",
		"std::fs",
		"std::io",
		"std::net",
		"tokio",
		"spawn",
		"Instant::now",
		"SystemTime::now",
		"unsafe",
		"serde",
		"fixture",
		"CommittedSource2",
	] {
		assert!(!flat.contains(banned), "verification.rs: {banned}");
	}
	assert_eq!(
		flat.matches("verify_command2(").count(),
		2,
		"definition + one call"
	);
	assert_eq!(flat.matches("fnverify_command2(").count(), 1);
	let advance = fn_body(&text, "advance_verified2");
	assert!(text.contains("governance: Option<&[u8]>"));
	assert!(advance.find("E::RootMismatch").unwrap() < advance.find("E::ExtraSidecar").unwrap());
	assert!(advance.find("E::ExtraSidecar").unwrap() < advance.find("verify_command2").unwrap());
	assert_eq!(calls(advance, "verify_command2"), 1);
	let verify = fn_body(&text, "verify_command2");
	assert_eq!(calls(verify, "VerifiedCommand2::verified2"), 1);
	// Key custody K7: the role pair and form come from the one shared requirement, and every
	// checker view comes from an administrator `install_admin2` just installed: no view type, no
	// offline construction and no facade here.
	let verify_flat = verify.replace(char::is_whitespace, "");
	assert_eq!(calls(verify, "super::role_command::role_requirement"), 1);
	assert!(verify_flat.contains(
		"super::role_command::role_requirement(command,bytes[76]).ok_or(VerifyError2::StateKind)?"
	));
	assert!(!verify_flat.contains("[3,0]") && !verify_flat.contains("RoleForm3"));
	for banned in ["RoleView3", "offline", "Chain3", "operator_proof"] {
		assert!(!flat.contains(banned), "verification.rs: {banned}");
	}
	assert_eq!(flat.matches(".view()").count(), 2);
	assert_eq!(
		flat.matches("letadmin=").count(),
		flat.matches("letadmin=install_admin2(state,pin)").count()
	);
	assert!(flat.contains("check_epoch_statement3(&admin.view(),bytes)"));
	assert!(flat.contains(
		"install_admin2(state,pin).is_ok_and(|admin|check_admin_statement2(&admin.view(),bytes)"
	));
	assert_eq!(calls(verify, "verify_governance2"), 1);
	assert!(
		verify.find("verify_governance2(").unwrap()
			< verify.find("VerifiedCommand2::verified2(").unwrap()
	);
	assert!(
		verify
			.replace(char::is_whitespace, "")
			.contains("verify_governance2(state,pin,command,governance,d)?")
	);
	let kind = fn_body(&text, "kind");
	assert!(!kind.contains("Unsupported"));
	let governance_kinds = [
		"EnrollSelector",
		"EnrollAuthority",
		"RevokeAuthority",
		"Grant",
		"Release",
		"Cut",
		"Resolve",
		"EnrollLeaseIssuer",
		"RevokeLeaseIssuer",
	];
	fn command_variants(p: &syn::Pat, names: &mut Vec<String>) {
		match p {
			syn::Pat::Or(p) => p.cases.iter().for_each(|p| command_variants(p, names)),
			syn::Pat::Struct(p) => {
				assert_eq!(p.path.segments.len(), 2);
				assert_eq!(p.path.segments[0].ident, "Command2");
				names.push(p.path.segments[1].ident.to_string());
			}
			syn::Pat::TupleStruct(p) => {
				assert_eq!(p.path.segments.len(), 2);
				assert_eq!(p.path.segments[0].ident, "Command2");
				names.push(p.path.segments[1].ident.to_string());
			}
			syn::Pat::Path(p) => {
				assert!(p.qself.is_none());
				assert_eq!(p.path.segments.len(), 2);
				assert_eq!(p.path.segments[0].ident, "Command2");
				names.push(p.path.segments[1].ident.to_string());
			}
			_ => panic!("exact Command2 pattern required; no wildcard"),
		}
	}
	let kind_file = syn::parse_file(&text).unwrap();
	let kind_fn = kind_file
		.items
		.iter()
		.find_map(|i| match i {
			syn::Item::Fn(f) if f.sig.ident == "kind" => Some(f),
			_ => None,
		})
		.unwrap();
	let syn::Stmt::Expr(syn::Expr::Match(kind_match), _) = kind_fn.block.stmts.last().unwrap()
	else {
		panic!("kind match")
	};
	let (mut accepted, mut cover) = (Vec::new(), Vec::new());
	for arm in &kind_match.arms {
		assert!(arm.guard.is_none());
		let mut names = Vec::new();
		command_variants(&arm.pat, &mut names);
		let syn::Expr::Call(call) = arm.body.as_ref() else {
			panic!("kind result")
		};
		let syn::Expr::Path(path) = call.func.as_ref() else {
			panic!("kind result path")
		};
		if path.path.is_ident("Ok") {
			accepted.extend(names);
		} else {
			assert!(path.path.is_ident("Err"));
			assert!(
				matches!(call.args.first(), Some(syn::Expr::Path(p)) if exact_path(&p.path, &["E", "NotApplicable"]))
			);
			cover.extend(names);
		}
	}
	let mut expected = governance_kinds
		.iter()
		.map(|s| s.to_string())
		.collect::<Vec<_>>();
	expected.extend(
		[
			"Bootstrap",
			"AdmissionPermission",
			"Reserve",
			"Finalize",
			"RotateAdmin",
			"RotateGovernance",
			"RotateRecovery",
			"EpochAdmissionPermission",
			"Freeze",
			"Unfreeze",
			"RetireRoot",
		]
		.map(str::to_string),
	);
	expected.sort();
	accepted.sort();
	assert_eq!(accepted, expected);
	assert_eq!(cover, ["Cover"]);
	let helper_text = fs::read_to_string(root.join("governance_verification.rs")).unwrap();
	assert!(helper_text.lines().count() <= 250);
	let helper = without_test_items_deep(helper_text.parse().unwrap());
	let helper_flat = helper.to_string().replace(' ', "");
	for banned in [
		"SigningKey",
		"Signer",
		"std::fs",
		"std::io",
		"std::net",
		"tokio",
		"spawn",
		"unsafe",
		"serde",
		"VerifiedCommand2",
		"TrustedEntry2",
		"CommittedEntry2",
		"CommittedSource2",
		"SystemTime",
		"Instant",
		"thread_local",
		"static",
	] {
		assert!(!helper_flat.contains(banned), "governance helper: {banned}");
	}
	assert!(helper_flat.contains("fnverify_governance2(state:&MutationAuthorityState2,pin:&PinnedBoundTuple2,command:&Command2,governance:Option<&[u8]>,digest:Digest2,)->Result<bool,VerifyError2>"));
	// The VISROL03 governance path's action bit is the view's one predicate, from the install.
	assert_eq!(
		helper_flat.matches("admin.view().governs(action)").count(),
		1
	);
	assert!(!helper_flat.contains("action.bit()") && !helper_flat.contains(".actions()"));
	assert!(helper_flat.contains("fngovernance_failure2(state:&MutationAuthorityState2,pin:&PinnedBoundTuple2,command:&Command2,governance:Option<&[u8]>,digest:Digest2,)->Result<Option<Denial2>,VerifyError2>"));
	let helper_file: syn::File = syn::parse2(helper).unwrap();
	let (mut helper_public, mut helper_private, mut helper_enums) =
		(Vec::new(), Vec::new(), Vec::new());
	for item in &helper_file.items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => helper_public.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => {
				assert!(matches!(f.vis, syn::Visibility::Inherited));
				helper_private.push(f.sig.ident.to_string());
			}
			syn::Item::Enum(e) => {
				assert!(matches!(e.vis, syn::Visibility::Inherited));
				assert_eq!(e.ident, "Denial2");
				assert_eq!(
					e.variants
						.iter()
						.map(|v| v.ident.to_string())
						.collect::<Vec<_>>(),
					[
						"Envelope",
						"Scope",
						"IssuerMissing",
						"IssuerRevoked",
						"IssuerKey",
						"IssuerSignature"
					]
				);
				helper_enums.push(e.ident.to_string());
			}
			syn::Item::Use(u) => assert!(!matches!(
				u.tree,
				syn::UseTree::Rename(_) | syn::UseTree::Glob(_)
			)),
			_ => panic!("helper only imports, private Denial2 and exact two functions"),
		}
	}
	assert_eq!(helper_public, ["verify_governance2"]);
	assert_eq!(helper_private, ["governance_failure2"]);
	assert_eq!(helper_enums, ["Denial2"]);
	fn imports(tree: &syn::UseTree, prefix: &[String], out: &mut Vec<Vec<String>>) {
		match tree {
			syn::UseTree::Path(p) => {
				let mut next = prefix.to_vec();
				next.push(p.ident.to_string());
				imports(&p.tree, &next, out);
			}
			syn::UseTree::Name(n) => {
				let mut path = prefix.to_vec();
				path.push(n.ident.to_string());
				out.push(path);
			}
			syn::UseTree::Group(g) => g.items.iter().for_each(|t| imports(t, prefix, out)),
			_ => panic!("canonical helper imports only; no alias or glob"),
		}
	}
	let mut imported = Vec::new();
	for item in &helper_file.items {
		if let syn::Item::Use(u) = item {
			imports(&u.tree, &[], &mut imported);
		}
	}
	let mut actual_imports = imported.iter().map(|p| p.join("::")).collect::<Vec<_>>();
	let mut expected_imports = [
		"admin_install::admin_namespace2",
		"admin_install::install_admin2",
		"command::Command2",
		"governance_command::GOVERNANCE_COMMAND_BYTES2",
		"governance_command::GovernanceAction2",
		"governance_command::GovernanceCommandError2",
		"governance_command::check_governance_command2",
		"governance_command::parse_governance_command2",
		"lease_payload::LeasePayloadError2",
		"lease_payload::verify_issuer_signature2",
		"origin::MutationAuthorityState2",
		"origin::root_and_origin2",
		"provisioning::replica_anchor::PinnedBoundTuple2",
		"types::Digest2",
		"verification::VerifyError2",
	]
	.map(|p| format!("crate::storage::mutation_visibility::root2::{p}"));
	actual_imports.sort();
	expected_imports.sort();
	assert_eq!(actual_imports, expected_imports);
	let failure_fn = helper_file
		.items
		.iter()
		.find_map(|i| match i {
			syn::Item::Fn(f) if f.sig.ident == "governance_failure2" => Some(f),
			_ => None,
		})
		.unwrap();
	let action_match = failure_fn
		.block
		.stmts
		.iter()
		.find_map(|s| match s {
			syn::Stmt::Local(l) if matches!(&l.pat, syn::Pat::Ident(p) if p.ident == "action") => {
				match &*l.init.as_ref().unwrap().expr {
					syn::Expr::Match(m) => Some(m),
					_ => None,
				}
			}
			_ => None,
		})
		.unwrap();
	assert!(matches!(action_match.expr.as_ref(), syn::Expr::Path(p) if p.path.is_ident("command")));
	let mut action_kinds = Vec::new();
	for arm in &action_match.arms {
		assert!(arm.guard.is_none());
		let mut names = Vec::new();
		command_variants(&arm.pat, &mut names);
		if names.len() == 1 && governance_kinds.contains(&names[0].as_str()) {
			assert!(
				matches!(arm.body.as_ref(), syn::Expr::Path(p) if exact_path(&p.path, &["GovernanceAction2", &names[0]]))
			);
			action_kinds.extend(names);
		} else {
			names.sort();
			assert_eq!(
				names,
				[
					"AdmissionPermission",
					"Bootstrap",
					"Cover",
					"EpochAdmissionPermission",
					"Finalize",
					"Freeze",
					"Reserve",
					"RetireRoot",
					"RotateAdmin",
					"RotateGovernance",
					"RotateRecovery",
					"Unfreeze"
				]
			);
		}
	}
	action_kinds.sort();
	let mut expected_actions = governance_kinds.map(str::to_string);
	expected_actions.sort();
	assert_eq!(action_kinds, expected_actions);
	assert_eq!(
		calls(
			fn_body(&helper_text, "verify_governance2"),
			"governance_failure2"
		),
		1
	);
	let failure = fn_body(&helper_text, "governance_failure2");
	let ordered = [
		"root_and_origin2(state)",
		"let action = match command",
		"governance.ok_or(VerifyError2::MissingSidecar)",
		"bytes.len() != GOVERNANCE_COMMAND_BYTES2",
		"parse_governance_command2(bytes)",
		"install_admin2(state, pin)",
		"check_governance_command2(&admin.view(), action, bytes, digest)",
		"admin_namespace2(&admin)",
		"if !in_scope",
		"root.state.issuers.get(&lease.issuer.to_be_bytes())",
		"issuer.revoked.is_some()",
		"verify_issuer_signature2(&issuer.key, lease)",
	];
	let positions = ordered.map(|s| failure.find(s).expect(s));
	assert!(positions.windows(2).all(|w| w[0] < w[1]));
	for name in [
		"parse_governance_command2",
		"install_admin2",
		"check_governance_command2",
		"verify_issuer_signature2",
	] {
		assert_eq!(calls(failure, name), 1, "{name}");
	}
	assert!(!failure.contains("VerifyError2::Unsupported"));
	assert!(!failure.contains("UnsupportedKind2"));
	let failure_flat = failure.replace(char::is_whitespace, "");
	for scope in [
		"binding.tenant==namespace.tenant",
		"binding.generation==root.state.generation",
		"identity.generation==root.state.generation",
		"identity.tenant==namespace.tenant",
		"identity.scope==namespace.scope",
		"identity.table==namespace.table",
		"identity.schema_generation==namespace.schema",
	] {
		assert!(failure_flat.contains(scope), "scope: {scope}");
	}
	assert!(helper_text.contains(
		"#[cfg(test)]\n#[path = \"tests/governance_verification_private.rs\"]\nmod tests;"
	));
	assert!(
		fs::read_to_string(root.join("mod.rs"))
			.unwrap()
			.contains("\nmod governance_verification;\n")
	);
	// The token reaches exactly one consumer per command, by reference.
	for consumer in ["bootstrap2", "advance_admission2", "advance_root2"] {
		assert_eq!(calls(advance, consumer), 1, "{consumer}");
	}
	assert_eq!(advance.matches("&token").count(), 3);
	assert!(!advance.contains("token.clone") && !advance.contains("Ok(token"));
	// Census.
	let file = syn::parse_file(&text).unwrap();
	let (mut types, mut exported, mut private) = (vec![], vec![], vec![]);
	for item in &file.items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => exported.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => {
				assert!(matches!(f.vis, syn::Visibility::Inherited));
				private.push(f.sig.ident.to_string())
			}
			syn::Item::Enum(e) => {
				assert!(root2_scoped(&e.vis));
				if e.ident == "VerifyError2" {
					assert_eq!(
						e.variants
							.iter()
							.map(|v| v.ident.to_string())
							.collect::<Vec<_>>(),
						[
							"NotApplicable",
							"EnvelopeContext",
							"ExtraSidecar",
							"MissingSidecar",
							"SidecarLength",
							"Governance",
							"Role",
							"Install",
							"IssuerPayload",
							"Unsupported",
							"DigestMismatch",
							"CommittedMismatch",
							"StateKind",
							"RootMismatch",
							"Consumer"
						]
					);
					for (variant, ty) in [
						("Governance", "GovernanceCommandError2"),
						("Role", "RoleProofError3"),
						("Install", "InstallError2"),
					] {
						let v = e.variants.iter().find(|v| v.ident == variant).unwrap();
						if variant == "Role" {
							assert!(
								matches!(&v.fields, syn::Fields::Unnamed(f) if matches!(&f.unnamed[0].ty, syn::Type::Path(t) if exact_path(&t.path, &["super", "role_command", "RoleProofError3"])))
							);
						}
						assert!(
							matches!(&v.fields, syn::Fields::Unnamed(f) if f.unnamed.len()==1 && quote_type(&f.unnamed.first().unwrap().ty)==ty)
						);
					}
					assert!(matches!(
						e.variants
							.iter()
							.find(|v| v.ident == "IssuerPayload")
							.unwrap()
							.fields,
						syn::Fields::Unit
					));
				}
				types.push(e.ident.to_string())
			}
			syn::Item::Use(_) => {}
			_ => panic!("verification.rs: only enums, functions and imports"),
		}
	}
	types.sort();
	private.sort();
	assert_eq!(types, ["UnsupportedKind2", "VerifyError2"]);
	assert_eq!(exported, ["advance_verified2"]);
	assert_eq!(
		private,
		[
			"bootstrap_authorized",
			"finalize_authorized",
			"kind",
			"reserve_authorized",
			"statement_authorized",
			"verify_command2"
		]
	);
	// Production-wide: one verified2 call (in verification.rs), VerifiedCommand2 literals only in
	// trust.rs, no other function returning VerifiedCommand2, no caller of advance_verified2.
	let tests = root.join("tests");
	let mut stack = vec![src];
	let (mut verified2_calls, mut returns) = (0, 0);
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != tests && !p.ends_with("tests") {
					stack.push(p);
				}
				continue;
			}
			if p.extension().is_none_or(|x| x != "rs") || p.ends_with("tests.rs") {
				continue;
			}
			let t = without_test_items_deep(fs::read_to_string(&p).unwrap().parse().unwrap())
				.to_string()
				.replace(' ', "");
			verified2_calls += t.matches("::verified2(").count();
			returns += t.matches("->VerifiedCommand2").count()
				+ t.matches("->Result<VerifiedCommand2").count()
				+ t.matches("->Result2<VerifiedCommand2").count();
			if !p.ends_with("root2/trust.rs") {
				assert!(
					!t.contains("VerifiedCommand2{"),
					"VerifiedCommand2 literal in {}",
					p.display()
				);
			}
			if !p.ends_with("root2/verification.rs") && !p.ends_with("root2/replay.rs") {
				assert!(
					!t.contains("advance_verified2("),
					"production C call in {}",
					p.display()
				);
				let remaining = if p == root.join("governance_verification.rs") {
					let import = "usecrate::storage::mutation_visibility::root2::verification::VerifyError2;";
					assert_eq!(
						t.matches(import).count(),
						1,
						"canonical type-only helper import"
					);
					t.replace(import, "")
				} else {
					t.clone()
				};
				assert!(
					!remaining.contains("root2::verification"),
					"production caller of C in {}",
					p.display()
				);
			}
			if p != root.join("verification.rs") && p != root.join("governance_verification.rs") {
				assert!(
					!t.contains("verify_governance2(")
						&& !t.contains("root2::governance_verification"),
					"governance helper caller in {}",
					p.display()
				);
			}
		}
	}
	assert_eq!(verified2_calls, 1);
	assert_eq!(
		returns, 1,
		"only verify_command2 returns a VerifiedCommand2"
	);
}

// VISADC01 is deleted (c2, D-C1) and the operator-proof facade refuses the form outright. Successor
// of the deleted VISADC01 census; its module name is a manifest-codec tombstone. Probed by
// removing the facade arm.
fn visadc01_facade_refuses(proof_rs: &str) -> bool {
	let squash = |t: &str| t.chars().filter(|c| !c.is_whitespace()).collect::<String>();
	squash(proof_rs).contains("Some(b\"VISADC01\")=>returnErr(P::NoProductionForm),")
}
#[test]
fn visadc01_is_deleted_and_the_facade_refuses_it() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let proof = fs::read_to_string(root.join("operator_proof/proof.rs")).unwrap();
	assert!(visadc01_facade_refuses(&proof));
	let arm = "Some(b\"VISADC01\") => return Err(P::NoProductionForm),";
	assert!(proof.contains(arm));
	assert!(!visadc01_facade_refuses(&proof.replace(arm, "")));
}

// The manifest names each production file may use only under its exact allowance.
const TRACKED: [&str; 5] = [
	"AdminManifestThree3",
	"GovernanceRole2",
	"manifest_digest3",
	"RecoveryRole3",
	"ADMIN_MANIFEST_P3_BYTES3",
];
// The tombstone list of manifest_codec_census_is_exact_and_dormant: the removed profile-1/2,
// class-0, VISADC01 and v1-directory names, which no root2 production file may name again.
const TOMBSTONES: [&str; 21] = [
	"AdminManifestTwo2",
	"admin_manifest_two_digest2",
	"manifest_profile2",
	"ADMIN_MANIFEST_P2_BYTES2",
	"ADMIN_MANIFEST_BYTES2",
	"admin_manifest_digest2",
	"check_admin_manifest_digest2",
	"PROFILE_ONE",
	"PROFILE_TWO",
	"ProfileLength",
	"RoleReuse",
	"AdminClass2",
	"LegacyNonManifestZero",
	"admin_command",
	"DirectoryFormat2",
	"VISREP03",
	"VERSION_TWO",
	"ManifestProfile2",
	"AnyAdminManifest2",
	"ManifestBytes2",
	"any_manifest_digest2",
];
// The manifest codec census (renamed at c2 from the profile-2 census): profile 3 is the only form.
// Exact census of admin/manifest.rs (struct fields private, no profile or manifest enums, the
// constants) and no production name of a manifest item outside src/.../admin and the test
// modules, except the exact per-file allowances. The tombstone names below must never return.
// Syntax only.
#[test]
fn manifest_codec_census_is_exact_and_dormant() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("admin/manifest.rs")).unwrap();
	let file = syn::parse_file(&text).unwrap();
	let mut consts = Vec::new();
	let mut structs = Vec::new();
	for item in &file.items {
		match item {
			syn::Item::Struct(s) => {
				let fields: Vec<_> = s
					.fields
					.iter()
					.map(|f| {
						if s.ident != "AdminManifest2" && s.ident != "AdminNamespace2" {
							assert!(
								matches!(f.vis, syn::Visibility::Inherited),
								"private fields"
							);
						}
						f.ident.as_ref().unwrap().to_string()
					})
					.collect();
				let want: &[&str] = match s.ident.to_string().as_str() {
					"GovernanceRole2" | "RecoveryRole3" => &["id", "key"],
					"AdminManifestThree3" => &["base", "governance", "recovery"],
					"AdminManifest2" => &[
						"generation",
						"cluster",
						"initial_epoch",
						"admin_id",
						"admin_key",
						"namespace",
					],
					"AdminNamespace2" => &["tenant", "scope", "table", "schema"],
					other => panic!("unexpected struct {other}"),
				};
				assert_eq!(fields, want);
				structs.push(s.ident.to_string());
			}
			syn::Item::Enum(e) => panic!("unexpected enum {}", e.ident),
			syn::Item::Const(c) => consts.push((c.ident.to_string(), root2_scoped(&c.vis))),
			_ => {}
		}
	}
	structs.sort();
	assert_eq!(
		structs,
		[
			"AdminManifest2",
			"AdminManifestThree3",
			"AdminNamespace2",
			"GovernanceRole2",
			"RecoveryRole3"
		]
	);
	consts.sort();
	assert_eq!(
		consts,
		[
			("ACTIONS".to_string(), false),
			("ADMIN_MANIFEST_P3_BYTES3".to_string(), true),
			("ADMIN_MANIFEST_READ_BOUND2".to_string(), true),
			("DOMAIN".to_string(), false),
			("GOVERNANCE_ACTIONS".to_string(), false),
			("MAGIC".to_string(), false),
			("RECOVERY_ACTIONS3".to_string(), false)
		]
	);
	// The encoder returns the one fixed canonical array and digest accepts that exact array.
	assert!(text.contains("-> AdminResult2<[u8; ADMIN_MANIFEST_P3_BYTES3]>"));
	assert!(text.contains("bytes: &[u8; ADMIN_MANIFEST_P3_BYTES3]"));
	assert!(text.contains("hash(DOMAIN, bytes)"));
	// Dormant: no production file outside admin/ names a manifest item, except these exact
	// allowances: the owner request -> event -> fold -> snapshot path, the replica tuple, install,
	// the governed marker and C's Bootstrap length conjunct (§4.2).
	let allowed: &[(&str, &[&str])] = &[
		(
			"provisioning/inventory.rs",
			&["AdminManifestThree3", "manifest_digest3"],
		),
		("provisioning/inventory/v2.rs", &["AdminManifestThree3"]),
		(
			"provisioning/owner_core.rs",
			&[
				"AdminManifestThree3",
				"manifest_digest3",
				"ADMIN_MANIFEST_P3_BYTES3",
			],
		),
		("provisioning/owner_core/v2.rs", &["manifest_digest3"]),
		("provisioning/snapshot.rs", &[]),
		(
			"provisioning/terminal.rs",
			&["AdminManifestThree3", "manifest_digest3"],
		),
		("provisioning/owner_log.rs", &[]),
		("provisioning/owner_log/ops.rs", &[]),
		("provisioning/owner_log/directory.rs", &[]),
		(
			"provisioning/replica_anchor.rs",
			&[
				"AdminManifestThree3",
				"manifest_digest3",
				"ADMIN_MANIFEST_P3_BYTES3",
			],
		),
		(
			"provisioning/replica_read.rs",
			&["ADMIN_MANIFEST_P3_BYTES3"],
		),
		(
			"admin_install.rs",
			&[
				"GovernanceRole2",
				"RecoveryRole3",
				"ADMIN_MANIFEST_P3_BYTES3",
			],
		),
		("validation.rs", &["ADMIN_MANIFEST_P3_BYTES3"]),
		("verification.rs", &["ADMIN_MANIFEST_P3_BYTES3"]),
		("governance_command.rs", &[]),
		("role_epochs.rs", &["ADMIN_MANIFEST_P3_BYTES3"]),
		("role_freeze.rs", &["ADMIN_MANIFEST_P3_BYTES3"]),
		("retirement.rs", &["ADMIN_MANIFEST_P3_BYTES3"]),
		("role_command.rs", &[]),
		(
			"role_view.rs",
			&["AdminManifestThree3", "GovernanceRole2", "manifest_digest3"],
		),
		("operator_proof.rs", &["AdminManifestThree3"]),
		(
			"compromise/binding.rs",
			&["AdminManifestThree3", "manifest_digest3"],
		),
	];
	// The one new out-of-line test file is excluded only while its declaration is cfg(test).
	let replay = syn::parse_file(&fs::read_to_string(root.join("replay.rs")).unwrap()).unwrap();
	assert!(replay.items.iter().any(|i| matches!(i, syn::Item::Mod(m) if m.ident == "tests"
		&& m.attrs.iter().any(|a| matches!(&a.meta, syn::Meta::List(l) if l.path.is_ident("cfg") && l.tokens.to_string() == "test")))));
	let tests = root.join("tests");
	let admin = root.join("admin");
	let mut stack = vec![src];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != tests && p != admin && !p.ends_with("tests") {
					stack.push(p);
				}
			} else if p.extension().is_some_and(|x| x == "rs") {
				if p == root.join("replay/tests.rs") {
					continue;
				}
				let t = fs::read_to_string(&p).unwrap();
				let allowance = allowed
					.iter()
					.find(|(f, _)| p == root.join(f))
					.map_or(&[][..], |(_, names)| *names);
				for name in TRACKED {
					assert_eq!(
						t.contains(name),
						allowance.contains(&name),
						"production use of {name} in {} against its exact allowance",
						p.display()
					);
				}
				// The tombstones are root2 names; other subsystems may reuse a word.
				if p.starts_with(&root) {
					for name in TOMBSTONES {
						assert!(!t.contains(name), "tombstone {name} in {}", p.display());
					}
				}
			}
		}
	}
	// Probes: a tombstone name, or a tracked name outside its allowance, in a synthetic production
	// file is caught by the same per-name checks.
	let probe = format!(
		"mod {};\nfn f() -> AdminManifestThree3 {{ todo!() }}",
		TOMBSTONES[13]
	);
	assert!(TOMBSTONES.iter().any(|n| probe.contains(n)));
	assert!(TRACKED.iter().any(|n| probe.contains(n)));
}

// The frozen old-reader copies are deleted (c1, §14.1): no `.in` file exists under src and no
// source file includes one. The checker is probed with a synthetic vendored file and include.
fn vendored_reader_findings(files: &[(PathBuf, Option<String>)]) -> Vec<String> {
	let mut found = Vec::new();
	for (path, text) in files {
		if path.extension().is_some_and(|x| x == "in") {
			found.push(format!("{}: .in file", path.display()));
		}
		if let Some(text) = text
			&& text.contains(".in\")")
		{
			found.push(format!("{}: includes a .in file", path.display()));
		}
	}
	found
}
#[test]
fn no_vendored_reader_copy_exists_under_src() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut files = Vec::new();
	let mut stack = vec![src];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				stack.push(p);
			} else {
				let text = if p.extension().is_some_and(|x| x == "rs") {
					Some(fs::read_to_string(&p).unwrap())
				} else {
					None
				};
				files.push((p, text));
			}
		}
	}
	assert!(files.len() > 100);
	assert_eq!(vendored_reader_findings(&files), Vec::<String>::new());
	// Probes: a vendored excerpt and an include of one are each found.
	let probe = [
		(PathBuf::from("frozen/inventory.in"), None),
		(
			PathBuf::from("frozen.rs"),
			Some(String::from("include!(\"frozen/inventory.in\");")),
		),
	];
	assert_eq!(vendored_reader_findings(&probe).len(), 2);
}

// Directory format 2 is the only owner format (c1, D-O1): `open` refuses an unmarked directory with
// `Unformatted` and a marked one holding a VISIEV02 log with `ForeignFormat`; the framing check and
// the log read refuse VISIEV02; the format-dispatch type name is a manifest-codec tombstone. Text
// pins, each probed by a mutation.
fn owner_directory_is_format_two_only(directory: &str, ops: &str) -> Result<(), &'static str> {
	let squash = |t: &str| t.chars().filter(|c| !c.is_whitespace()).collect::<String>();
	let open = directory
		.find("fn open(")
		.and_then(|a| {
			directory[a..]
				.find("fn check_framing(")
				.map(|b| &directory[a..a + b])
		})
		.ok_or("open")?;
	let open = squash(open);
	if !open.contains("Err(OwnerIoError2::Unformatted)}") {
		return Err("unmarked directory is not Unformatted");
	}
	if !open.contains(
		"ifprefix==b\"VISIEV02\"{returnErr(OwnerIoError2::ForeignFormat);}returnOk(owner);",
	) {
		return Err("a marked directory with a VISIEV02 log opens");
	}
	let framing = directory
		.find("fn check_framing(")
		.map(|a| squash(&directory[a..]))
		.ok_or("check_framing")?;
	if !framing.contains("&&p==b\"VISIEV02\"{returnErr(OwnerIoError2::ForeignFormat);}") {
		return Err("framing check accepts VISIEV02");
	}
	if !squash(ops)
		.contains("ifbytes.starts_with(b\"VISIEV02\"){returnErr(OwnerIoError2::ForeignFormat);}")
	{
		return Err("log read accepts VISIEV02");
	}
	Ok(())
}
#[test]
fn owner_directory_refuses_unmarked_and_v1_directories() {
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("src/storage/mutation_visibility/root2/provisioning");
	let directory = fs::read_to_string(root.join("owner_log/directory.rs")).unwrap();
	let ops = fs::read_to_string(root.join("owner_log/ops.rs")).unwrap();
	assert_eq!(owner_directory_is_format_two_only(&directory, &ops), Ok(()));
	assert!(!root.join("format.rs").exists());
	assert!(!root.join("owner_core/evidence.rs").exists());
	for (from, to, want) in [
		(
			"Err(OwnerIoError2::Unformatted)",
			"Ok(owner)",
			"unmarked directory is not Unformatted",
		),
		(
			"if prefix == b\"VISIEV02\"",
			"if prefix == b\"VISXXX02\"",
			"a marked directory with a VISIEV02 log opens",
		),
		(
			"&& p == b\"VISIEV02\"",
			"&& p == b\"VISXXX02\"",
			"framing check accepts VISIEV02",
		),
	] {
		assert!(directory.contains(from), "{from}");
		assert_eq!(
			owner_directory_is_format_two_only(&directory.replace(from, to), &ops),
			Err(want)
		);
	}
	assert_eq!(
		owner_directory_is_format_two_only(
			&directory,
			&ops.replace("bytes.starts_with(b\"VISIEV02\")", "false")
		),
		Err("log read accepts VISIEV02")
	);
}

// M3: the administrator binding surface, exactly (renamed at c2). Syntactic regression pins only (no name resolution,
// no proof against aliases or forwarding): the installed governance role is a descriptive copy
// that no production code outside the admin codec reads, the installed view's fields and the tuple
// field variants are exact, and the descriptive accessor has no production caller.
#[test]
fn admin_binding_surface_is_exact() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let install = fs::read_to_string(root.join("admin_install.rs")).unwrap();
	let fields_of = |text: &str, name: &str| -> Vec<String> {
		let file = syn::parse_file(text).unwrap();
		file.items
			.iter()
			.find_map(|item| match item {
				syn::Item::Struct(s) if s.ident == name => Some(
					s.fields
						.iter()
						.map(|f| {
							assert!(
								matches!(f.vis, syn::Visibility::Inherited),
								"private fields"
							);
							f.ident.as_ref().unwrap().to_string()
						})
						.collect(),
				),
				_ => None,
			})
			.unwrap()
	};
	assert_eq!(
		fields_of(&install, "InstalledAdmin2"),
		[
			"context",
			"admin_id",
			"admin_key",
			"namespace",
			"governance",
			"recovery",
			"active",
			"cut"
		]
	);
	let anchor = fs::read_to_string(root.join("provisioning/replica_anchor.rs")).unwrap();
	let variants = syn::parse_file(&anchor)
		.unwrap()
		.items
		.iter()
		.find_map(|item| match item {
			syn::Item::Enum(e) if e.ident == "TupleField2" => Some(
				e.variants
					.iter()
					.map(|v| v.ident.to_string())
					.collect::<Vec<_>>(),
			),
			_ => None,
		})
		.unwrap();
	assert_eq!(
		variants,
		["Generation", "Cluster", "Pin", "AdminKey", "InitialEpoch",]
	);
	let governance = fs::read_to_string(root.join("governance_command.rs")).unwrap();
	assert_eq!(governance.matches("admin_governance2(").count(), 0);
	assert_eq!(
		governance
			.matches("RoleView3::governance_role(view)")
			.count(),
		1
	);
	assert_eq!(governance.matches("role.key()").count(), 1);
	assert_eq!(governance.matches("role.actions()").count(), 1);
	// Only original profile-3 seed derivation and terminal confirmation read the two copied role keys.
	assert_eq!(install.matches(".key()").count(), 6);
	assert_eq!(
		fn_body(&install, "install_admin2")
			.matches(".key()")
			.count(),
		2
	);
	assert_eq!(
		fn_body(&install, "original_roles3")
			.matches(".key()")
			.count(),
		2
	);
	assert_eq!(
		fn_body(&install, "confirm_terminal2")
			.matches(".key()")
			.count(),
		2
	);
	for call in [".id()", ".actions()"] {
		assert!(!install.contains(call), "admin_install.rs calls {call}");
	}
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let admin = root.join("admin");
	let mut stack = vec![src];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != admin && !p.ends_with("tests") {
					stack.push(p);
				}
			} else if p.extension().is_some_and(|x| x == "rs") {
				let t = fs::read_to_string(&p).unwrap();
				let is_install = p == root.join("admin_install.rs");
				let is_view = p == root.join("role_view.rs");
				// Only install and the offline view (once) read a manifest's governance role, and
				// only to copy it.
				let reads = t.matches(".governance()").count();
				assert!(
					if is_install {
						reads > 0
					} else {
						reads == usize::from(is_view)
					},
					"{}",
					p.display()
				);
				if is_view {
					assert!(fn_body(&t, "offline").contains("m.governance()"));
				}
				// The installed copy reaches the checkers only through the view's one-for-one copy.
				assert_eq!(
					t.matches("admin_governance2(").count(),
					usize::from(is_install || is_view),
					"{}",
					p.display()
				);
			}
		}
	}
}

// L-S1 is private owned composition, not a public unconfirmed authority path. Each factory has
// exactly one production call site. These are syntax guards, not general name resolution.
#[test]
fn dormant_replay_owns_state_and_has_single_site_factories() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("replay.rs")).unwrap();
	assert!(text.lines().count() <= 750);
	let file = syn::parse_file(&text).unwrap();
	let mut exported = Vec::new();
	for item in file.items {
		match item {
			syn::Item::Struct(s) => {
				assert!(s.attrs.iter().all(|a| !a.path().is_ident("derive")));
				assert!(
					s.fields
						.iter()
						.all(|f| matches!(f.vis, syn::Visibility::Inherited))
				);
				if !matches!(s.vis, syn::Visibility::Inherited) {
					assert!(root2_scoped(&s.vis));
					exported.push(s.ident.to_string());
				}
				if s.ident == "Provisional2" || s.ident == "Replayed2" {
					for f in s.fields {
						let t = quote_type(&f.ty);
						assert!(!t.contains("TrustedEntry2") && !t.contains("CommittedEntry2"));
					}
				}
			}
			syn::Item::Fn(f) => {
				if f.sig.ident == "run" {
					assert!(root2_scoped(&f.vis));
					assert!(f.sig.asyncness.is_some());
				} else {
					assert!(matches!(f.vis, syn::Visibility::Inherited));
				}
			}
			syn::Item::Impl(i) => {
				assert!(i.trait_.is_none());
				for m in i.items {
					if let syn::ImplItem::Fn(f) = m {
						if !matches!(f.vis, syn::Visibility::Inherited) {
							assert!(root2_scoped(&f.vis));
							assert_eq!(quote_type(&i.self_ty), "Replayed2");
							assert!(
								["state", "mark", "report"]
									.contains(&f.sig.ident.to_string().as_str())
							);
						}
					}
				}
			}
			syn::Item::Enum(e) => {
				assert!(root2_scoped(&e.vis));
				assert!(
					[
						"Fault2",
						"ReplayError2",
						"BaselineError2",
						"ClosureError2",
						"MarkRefusal2"
					]
					.contains(&e.ident.to_string().as_str())
				);
			}
			// C-S6: the one production child is the offline audit fold, root2-scoped and out of line.
			syn::Item::Mod(m) if m.ident == "audit" => {
				assert!(root2_scoped(&m.vis) && m.content.is_none());
				assert!(m.attrs.iter().all(|a| !a.path().is_ident("cfg")));
			}
			syn::Item::Mod(m) => {
				assert_eq!(m.ident, "tests");
				assert!(m.attrs.iter().any(|a| matches!(&a.meta, syn::Meta::List(l) if l.path.is_ident("cfg") && l.tokens.to_string()=="test")));
			}
			syn::Item::Use(_) => {}
			_ => panic!("unreviewed replay item"),
		}
	}
	assert_eq!(exported, ["PrefixRun2", "Replayed2"]);
	let production = without_test_items_deep(text.parse().unwrap())
		.to_string()
		.replace(' ', "");
	for banned in [
		"unsafe",
		"spawn",
		"std::fs",
		"std::net",
		"TrustedView2",
		"VerifiedCommand2::",
		"Clone",
	] {
		assert!(!production.contains(banned), "replay: {banned}");
	}

	// S2's sole completion path: baseline -> acquisition -> closure -> binding -> mark -> output.
	let stages = [
		"letbaseline=mark_store.durable()",
		"let(acc,report)=ReplayDriver2",
		"letterminal=report.terminal_applied()",
		"letretention=sources.retention()",
		"install_admin2(&acc.state,pin)",
		"letmark=mark_of2(&acc.state)",
		"mark_store.advance(&deployment,&mark)",
		"confirm_mark(outcome,mark_store.durable(),mark)",
		"Ok(Replayed2{",
	];
	let positions = stages.map(|stage| production.find(stage).expect(stage));
	assert!(positions.windows(2).all(|w| w[0] < w[1]));
	assert_eq!(production.matches("mark_store.advance(").count(), 1);
	assert_eq!(production.matches("finish(").count(), 2); // declaration and sole run call
	assert_eq!(production.matches("observe2(").count(), 1);
	assert_eq!(production.matches("CommittedSource2::acquired(").count(), 1);
	assert_eq!(production.matches("TrustedEntry2::bind(").count(), 1);
	// C-S6: `compose` is `compose_traced` with no trace; the audit's record is set once at
	// initialization and observed once, in `command`, after verification and the replay check.
	assert_eq!(production.matches("compose_traced(").count(), 1);
	assert!(production.contains("self.compose_traced(reader,sources,caps,None)"));
	assert_eq!(production.matches("acc.trace=trace;").count(), 1);
	assert_eq!(production.matches("trace.entry(").count(), 1);
	// The audit also records which entry the dispatcher takes up, first thing in `dispatch`.
	assert_eq!(production.matches("trace.attempt(").count(), 1);
	let dispatch = fn_body(&text, "dispatch").replace(char::is_whitespace, "");
	assert!(
		dispatch.find("trace.attempt(entry.log_id.index)").unwrap()
			< dispatch.find("committed_entry2(").unwrap()
	);
	let command = fn_body(&text, "command").replace(char::is_whitespace, "");
	assert!(
		command.find("advance_verified2(").unwrap()
			< command.find("ApplyResult2::AlreadyProcessed").unwrap()
			&& command.find("ApplyResult2::AlreadyProcessed").unwrap()
				< command.find("trace.entry(").unwrap()
	);
	// The audit fold, the one production file below replay/: the shared driver from a provisioned
	// baseline, then `run`'s closure and binding; no mark, factory, direct verifier call or write.
	let mut below: Vec<_> = fs::read_dir(root.join("replay"))
		.unwrap()
		.map(|e| e.unwrap().file_name().into_string().unwrap())
		.collect();
	below.sort();
	assert_eq!(below, ["audit.rs", "tests", "tests.rs"]);
	let audit_text = fs::read_to_string(root.join("replay/audit.rs")).unwrap();
	assert!(audit_text.lines().count() <= 150);
	let audit = without_test_items_deep(audit_text.parse().unwrap())
		.to_string()
		.replace(' ', "");
	for banned in [
		"mark_store",
		"MarkStore2",
		"advance(",
		"finish(",
		"CommittedSource2",
		"TrustedEntry2",
		"advance_verified2",
		"Replayed2",
		"PrefixRun2",
		"observe2",
		"unsafe",
		"spawn",
		"std::fs",
		"std::net",
	] {
		assert!(!audit.contains(banned), "replay/audit.rs: {banned}");
	}
	assert_eq!(audit.matches("Baseline2::Provisioned").count(), 1);
	assert_eq!(audit.matches(".compose_traced(").count(), 1);
	assert_eq!(audit.matches("Some(trace)").count(), 1);
	let order = [
		".compose_traced(",
		"report.terminal_applied()",
		"confirm_terminal2(&acc.state,pin)",
		"install_admin2(&acc.state,pin)",
		"acc.trace.ok_or(",
	]
	.map(|s| audit.find(s).expect(s));
	assert!(order.windows(2).all(|w| w[0] < w[1]));
	let (mut structs, mut impls, mut fns) = (Vec::new(), Vec::new(), Vec::new());
	for item in syn::parse_file(&audit_text).unwrap().items {
		match item {
			syn::Item::Use(_) => {}
			syn::Item::Struct(s) => {
				assert!(matches!(s.vis, syn::Visibility::Inherited));
				assert!(s.attrs.iter().all(|a| !a.path().is_ident("derive")));
				structs.push(s.ident.to_string());
			}
			syn::Item::Impl(i) => impls.push(quote_type(&i.self_ty)),
			syn::Item::Fn(f) => {
				assert!(root2_scoped(&f.vis) && f.sig.asyncness.is_some());
				fns.push(f.sig.ident.to_string());
			}
			_ => panic!("replay/audit.rs: unreviewed item"),
		}
	}
	assert_eq!(structs, ["Retained2", "Scanned2"]);
	assert_eq!(impls, ["Retained2", "Scanned2"]);
	assert_eq!(fns, ["fold2"]);
	let mut stack = vec![src];
	let mut acquired = 0;
	let mut bind = 0;
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != root.join("tests") && p != root.join("replay") {
					stack.push(p);
				}
				continue;
			}
			if p.extension().is_none_or(|x| x != "rs") {
				continue;
			}
			let t = without_test_items_deep(fs::read_to_string(&p).unwrap().parse().unwrap())
				.to_string()
				.replace(' ', "");
			let a = t.matches("CommittedSource2::acquired(").count();
			let b = t.matches("TrustedEntry2::bind(").count();
			if !p.ends_with("root2/replay.rs") {
				assert_eq!(a + b, 0, "factory outside replay {}", p.display());
			}
			acquired += a;
			bind += b;
			if !p.starts_with(&root) {
				for name in [
					"ReplayDriver2",
					"PrefixRun2",
					"Provisional2",
					"Replayed2",
					"ReplayError2",
					"root2::replay",
				] {
					assert!(!t.contains(name), "{}", p.display());
				}
			}
			if p.ends_with("root2/mod.rs") {
				continue;
			}
			if !p.ends_with("root2/replay.rs") && !p.ends_with("root2/committed.rs") {
				assert!(!t.contains("PrefixRun2"), "{}", p.display());
			}
		}
	}
	assert_eq!((acquired, bind), (1, 1));
	let committed = fs::read_to_string(root.join("committed.rs")).unwrap();
	let ast = syn::parse_file(&committed).unwrap();
	assert!(ast.items.iter().any(|i| matches!(i,syn::Item::Mod(m) if m.ident=="fixture" && m.attrs.iter().any(|a|matches!(&a.meta,syn::Meta::List(l) if l.path.is_ident("cfg") && l.tokens.to_string()=="test")))));
	assert!(
		!without_test_items_deep(committed.parse().unwrap())
			.to_string()
			.contains("fixture")
	);
}

#[test]
fn governance_command_envelope_is_pure_and_unwired() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let text = fs::read_to_string(root.join("governance_command.rs")).unwrap();
	assert!(text.lines().count() <= 250);
	let flat = without_test_items_deep(text.parse().unwrap())
		.to_string()
		.replace(' ', "");
	// Identifier tokens only: doc comments are literals and may name the types they exclude.
	fn idents(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => out.push(id.to_string()),
				proc_macro2::TokenTree::Group(g) => idents(g.stream(), out),
				_ => {}
			}
		}
	}
	let mut names = Vec::new();
	idents(without_test_items_deep(text.parse().unwrap()), &mut names);
	for banned in [
		"SigningKey",
		"Signer",
		"sign",
		"fs",
		"io",
		"net",
		"tokio",
		"spawn",
		"Instant",
		"SystemTime",
		"unsafe",
		"serde",
		"VerifiedCommand2",
		"TrustedEntry2",
		"CommittedEntry2",
		"Root2",
		"install_admin2",
	] {
		assert!(
			!names.iter().any(|n| n == banned),
			"governance_command.rs: {banned}"
		);
	}
	assert_eq!(flat.matches("!permits(role.actions(),action)").count(), 1);
	// The checked token: declared, given accessors, and constructed exactly once, inside the check.
	assert_eq!(flat.matches("(CheckedGovernanceCommand2{").count(), 1);
	assert!(fn_body(&text, "check_governance_command2").contains("Ok(CheckedGovernanceCommand2 {"));
	assert_eq!(flat.matches("verify_strict(").count(), 1);
	let file: syn::File = syn::parse2(without_test_items_deep(text.parse().unwrap())).unwrap();
	let (mut types, mut exported, mut private, mut consts) = (vec![], vec![], vec![], vec![]);
	for item in &file.items {
		match item {
			syn::Item::Fn(f) if root2_scoped(&f.vis) => exported.push(f.sig.ident.to_string()),
			syn::Item::Fn(f) => {
				assert!(matches!(f.vis, syn::Visibility::Inherited));
				private.push(f.sig.ident.to_string())
			}
			syn::Item::Enum(e) => {
				assert!(root2_scoped(&e.vis));
				types.push(e.ident.to_string())
			}
			syn::Item::Struct(s) => {
				assert!(root2_scoped(&s.vis));
				assert!(
					s.attrs.iter().all(|a| !a.path().is_ident("derive")),
					"no derive"
				);
				for f in &s.fields {
					assert!(
						matches!(f.vis, syn::Visibility::Inherited),
						"private fields"
					);
				}
				types.push(s.ident.to_string())
			}
			syn::Item::Const(c) => consts.push((c.ident.to_string(), root2_scoped(&c.vis))),
			syn::Item::Impl(i) => {
				assert!(i.trait_.is_none());
				let syn::Type::Path(ty) = i.self_ty.as_ref() else {
					panic!("named impl type required")
				};
				let name = ty.path.get_ident().expect("single type name").to_string();
				let mut methods = Vec::new();
				let mut associated = Vec::new();
				for item in &i.items {
					match item {
						syn::ImplItem::Fn(f) => {
							assert!(root2_scoped(&f.vis));
							methods.push(f.sig.ident.to_string());
						}
						syn::ImplItem::Const(c) => {
							assert!(root2_scoped(&c.vis));
							associated.push(c.ident.to_string());
						}
						_ => panic!("unexpected associated item"),
					}
				}
				methods.sort();
				associated.sort();
				match name.as_str() {
					"GovernanceAction2" => {
						assert_eq!(methods, ["bit", "command_tag"]);
						assert_eq!(associated, ["ALL"]);
					}
					"CheckedGovernanceCommand2" => {
						assert_eq!(methods, ["action", "command_digest", "context"]);
						assert!(associated.is_empty());
					}
					_ => panic!("unexpected impl"),
				}
			}
			syn::Item::Use(_) | syn::Item::Macro(_) => {}
			_ => panic!("governance_command.rs: unexpected item"),
		}
	}
	types.sort();
	exported.sort();
	private.sort();
	consts.sort();
	assert_eq!(
		types,
		[
			"CheckedGovernanceCommand2",
			"GovernanceAction2",
			"GovernanceCommandError2",
			"GovernanceField2"
		]
	);
	assert_eq!(
		exported,
		[
			"check_governance_command2",
			"encode_governance_command2",
			"governance_command_preimage2",
			"parse_governance_command2"
		]
	);
	assert_eq!(
		private,
		["array", "check_context", "permits", "u64_at", "write"]
	);
	assert_eq!(
		consts,
		[
			("DOMAIN".to_string(), false),
			("GOVERNANCE_COMMAND_BYTES2".to_string(), true),
			("MAGIC".to_string(), false),
			("SIGNED".to_string(), false),
			("VERSION".to_string(), false)
		]
	);
	// Dormant: nothing outside the module and the root2 tests names its surface.
	let modules = fs::read_to_string(root.join("mod.rs")).unwrap();
	assert!(modules.contains("#[allow(dead_code)] // Dormant pure governance check; no production caller.\nmod governance_command;"));
	let tests = root.join("tests");
	let mut stack = vec![src];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != tests {
					stack.push(p);
				}
			} else if p.extension().is_some_and(|x| x == "rs")
				&& !p.ends_with("root2/governance_command.rs")
			{
				let t = fs::read_to_string(&p).unwrap();
				let t = without_test_items_deep(t.parse().unwrap())
					.to_string()
					.replace(' ', "");
				let helper = p == root.join("governance_verification.rs");
				let shape = p == root.join("retirement_shape.rs");
				let verifier = p == root.join("verification.rs");
				// C-S6: the offline audit re-verifies a VISADG01 signature under the history's
				// epoch-0 governance key: the structural parse and the preimage, once each.
				let audit = p == root.join("compromise/attribution.rs");
				// Key custody K7: the role view names the action type for its one action-bit
				// predicate; the offline facade builds and checks VISADG01 with the production
				// encoder, preimage and checker, once each.
				let view = p == root.join("role_view.rs");
				let facade = p == root.join("operator_proof/proof.rs");
				let remaining = if verifier {
					let import = "usecrate::storage::mutation_visibility::root2::governance_command::GovernanceCommandError2;";
					assert_eq!(t.matches(import).count(), 1);
					assert_eq!(
						t.matches("GovernanceCommandError2").count(),
						2,
						"import and one error wrapper only"
					);
					assert_eq!(t.matches("Governance(GovernanceCommandError2)").count(), 1);
					t.replace(import, "")
				} else {
					t.clone()
				};
				for name in [
					"check_governance_command2",
					"governance_command_preimage2",
					"encode_governance_command2",
					"parse_governance_command2",
					"GOVERNANCE_COMMAND_BYTES2",
					"GovernanceAction2",
					"CheckedGovernanceCommand2",
					"GovernanceCommandError2",
					"root2::governance_command",
				] {
					let permitted = (helper
						&& [
							"parse_governance_command2",
							"check_governance_command2",
							"GovernanceAction2",
							"GovernanceCommandError2",
							"GOVERNANCE_COMMAND_BYTES2",
							"root2::governance_command",
						]
						.contains(&name)) || (shape
						&& ["parse_governance_command2", "GOVERNANCE_COMMAND_BYTES2"]
							.contains(&name)) || (audit
						&& [
							"parse_governance_command2",
							"governance_command_preimage2",
							"root2::governance_command",
						]
						.contains(&name)) || (view && name == "GovernanceAction2")
						|| (facade
							&& [
								"check_governance_command2",
								"governance_command_preimage2",
								"encode_governance_command2",
								"GovernanceAction2",
								"root2::governance_command",
							]
							.contains(&name));
					let typed_wrapper = verifier && name == "GovernanceCommandError2";
					assert!(
						permitted || typed_wrapper || !remaining.contains(name),
						"production caller of {name} in {}",
						p.display()
					);
				}
				if shape {
					assert_eq!(t.matches("parse_governance_command2(").count(), 1);
				}
				if audit {
					assert_eq!(calls(&t, "parse_governance_command2"), 1);
					assert_eq!(calls(&t, "governance_command_preimage2"), 1);
					assert_eq!(t.matches("root2::governance_command").count(), 1);
					assert!(t.contains("usecrate::storage::mutation_visibility::root2::governance_command::{governance_command_preimage2,parse_governance_command2,};"));
				}
				if view {
					assert_eq!(t.matches("GovernanceAction2").count(), 2);
					assert_eq!(t.matches("root2::governance_command").count(), 0);
				}
				if facade {
					for name in [
						"check_governance_command2",
						"governance_command_preimage2",
						"encode_governance_command2",
					] {
						assert_eq!(calls(&t, name), 1, "{name}");
					}
					assert_eq!(t.matches("root2::governance_command").count(), 1);
				}
				if helper {
					assert_eq!(calls(&t, "parse_governance_command2"), 1);
					assert_eq!(calls(&t, "check_governance_command2"), 1);
					assert!(t.contains(
						"usecrate::storage::mutation_visibility::root2::governance_command::{"
					));
				}
			}
		}
	}
}

#[test]
fn lease_payload_is_pure_and_unwired() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let root = src.join("storage/mutation_visibility/root2");
	let path = root.join("lease_payload.rs");
	let text = fs::read_to_string(&path).unwrap();
	let tokens = without_test_items_deep(text.parse().unwrap());
	fn idents(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => out.push(id.to_string()),
				proc_macro2::TokenTree::Group(g) => idents(g.stream(), out),
				_ => {}
			}
		}
	}
	let mut names = Vec::new();
	idents(tokens.clone(), &mut names);
	for banned in [
		"fs",
		"io",
		"net",
		"tokio",
		"spawn",
		"Instant",
		"SystemTime",
		"unsafe",
		"serde",
		"Signer",
		"SigningKey",
		"sign",
		"Command2",
		"State2",
		"Root2",
		"VerifiedCommand2",
		"TrustedEntry2",
		"CommittedEntry2",
		"reducer",
		"bytemuck",
	] {
		assert!(!names.iter().any(|n| n == banned), "{banned}");
	}
	let flat = tokens.to_string().replace(' ', "");
	assert_eq!(flat.matches("verify_strict(").count(), 1);
	let file: syn::File = syn::parse2(tokens).unwrap();
	let (mut types, mut exported, mut constants) = (vec![], vec![], vec![]);
	for item in file.items {
		match item {
			syn::Item::Enum(e) => {
				assert!(root2_scoped(&e.vis));
				types.push(e.ident.to_string());
			}
			syn::Item::Fn(f) => {
				assert!(root2_scoped(&f.vis));
				exported.push(f.sig.ident.to_string());
			}
			syn::Item::Const(c) => {
				assert!(root2_scoped(&c.vis));
				constants.push(c.ident.to_string());
			}
			syn::Item::Use(_) => {}
			_ => panic!("unexpected lease module item"),
		}
	}
	types.sort();
	exported.sort();
	constants.sort();
	assert_eq!(types, ["LeasePayloadError2"]);
	assert_eq!(exported, ["lease_payload2", "verify_issuer_signature2"]);
	assert_eq!(constants, ["LEASE_PAYLOAD_BYTES2"]);
	let modules = fs::read_to_string(root.join("mod.rs")).unwrap();
	assert!(modules.contains("#[allow(dead_code)] // Dormant pure lease payload check; no production caller.\nmod lease_payload;"));
	let mut stack = vec![src];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				if p != root.join("tests") {
					stack.push(p);
				}
			} else if p.extension().is_some_and(|x| x == "rs") && p != path {
				let t = fs::read_to_string(&p).unwrap();
				let t = without_test_items_deep(t.parse().unwrap())
					.to_string()
					.replace(' ', "");
				let helper = p == root.join("governance_verification.rs");
				for name in [
					"lease_payload2",
					"verify_issuer_signature2",
					"LeasePayloadError2",
					"LEASE_PAYLOAD_BYTES2",
					"root2::lease_payload",
				] {
					let permitted = helper
						&& [
							"verify_issuer_signature2",
							"LeasePayloadError2",
							"root2::lease_payload",
						]
						.contains(&name);
					assert!(
						permitted || !t.contains(name),
						"{} names {name}",
						p.display()
					);
				}
				if helper {
					assert_eq!(calls(&t, "verify_issuer_signature2"), 1);
				}
			}
		}
	}
}

#[test]
fn root2_carrier_is_bounded_private_held_and_response_mappings_are_explicit() {
	let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
	let text = fs::read_to_string(src.join("consensus/root2_carrier.rs")).unwrap();
	assert!(text.lines().count() <= 750);
	let file = syn::parse_file(&text).unwrap();
	for item in &file.items {
		if let syn::Item::Struct(s) = item {
			if s.ident == "Root2Bytes" {
				assert!(matches!(s.vis, syn::Visibility::Inherited));
			}
			for field in &s.fields {
				assert!(
					matches!(field.vis, syn::Visibility::Inherited),
					"carrier fields stay private"
				);
			}
		}
	}
	assert!(text.contains("d.deserialize_seq(BytesVisitor::<N>)"));
	assert!(!text.contains("Vec::deserialize"));
	assert!(!text.contains("deserialize_byte_buf"));
	assert!(text.contains("try_reserve_exact"));
	assert!(text.contains("if version != 1"));
	let core =
		syn::parse_file(&fs::read_to_string(src.join("consensus/core.rs")).unwrap()).unwrap();
	for (name, last) in [
		("WendelonRequest", "Root2Command"),
		("WendelonResponse", "Root2Dormant"),
	] {
		let e = core
			.items
			.iter()
			.find_map(|i| {
				if let syn::Item::Enum(e) = i {
					(e.ident == name).then_some(e)
				} else {
					None
				}
			})
			.unwrap();
		assert_eq!(e.variants.last().unwrap().ident.to_string(), last);
	}
	let frozen = fs::read_to_string(src.join("consensus/wal_body.rs")).unwrap();
	let file = syn::parse_file(&frozen).unwrap();
	let module = file
		.items
		.iter()
		.find_map(|i| {
			if let syn::Item::Mod(m) = i {
				(m.ident == "v4").then_some(m)
			} else {
				None
			}
		})
		.unwrap();
	let (_, items) = module.content.as_ref().unwrap();
	let request = items
		.iter()
		.find_map(|i| {
			if let syn::Item::Enum(e) = i {
				(e.ident == "WendelonRequest").then_some(e)
			} else {
				None
			}
		})
		.unwrap();
	assert_eq!(request.variants.len(), 26);
	assert!(request.variants.iter().all(|v| v.ident != "Root2Command"));
	let storage = fs::read_to_string(src.join("consensus/raft_storage.rs")).unwrap();
	let dispatch = storage
		.split("let entry: Entry<WendelonTypeConfig> = match header.format_version {")
		.nth(1)
		.unwrap()
		.split("if entry.log_id.index != fixed.index")
		.next()
		.unwrap();
	for version in 0..=5 {
		assert!(dispatch.contains(&format!("{version} =>")));
	}
	assert!(dispatch.contains("version =>"));
	assert!(!dispatch.contains("_ => match postcard"));
	for (path, count) in [
		("node.rs", 2),
		("network/control_server.rs", 11),
		("network/data_server.rs", 2),
	] {
		let source = fs::read_to_string(src.join(path)).unwrap();
		assert_eq!(
			source.matches("WendelonResponse::Root2Dormant =>").count(),
			count,
			"{path}: explicit dormant rejection census"
		);
	}
	for helper in [
		"audit_outcome",
		"audit_outcome_message",
		"audit_outcome_parameters",
	] {
		let function = storage
			.split(&format!("fn {helper}("))
			.nth(1)
			.unwrap()
			.split("\n}\n")
			.next()
			.unwrap();
		assert!(
			function.contains("WendelonResponse::Root2Dormant"),
			"{helper}: no generic success for held response"
		);
	}
	let root = src.join("storage/mutation_visibility/root2");
	let committed = fs::read_to_string(root.join("committed.rs")).unwrap();
	assert!(committed.contains("if raw_carrier"));
	assert!(committed.contains("EntryPayload::Normal(WendelonRequest::Root2Command(_))"));
	let replay_source = fs::read_to_string(root.join("replay.rs")).unwrap();
	let header = "fn command_at<'e>(";
	assert_eq!(replay_source.matches(header).count(), 1);
	let start = replay_source.find(header).unwrap();
	let signature = replay_source[start..].split('{').next().unwrap();
	assert_eq!(
		signature
			.chars()
			.filter(|c| !c.is_whitespace())
			.collect::<String>(),
		"fncommand_at<'e>(adapted:AdaptedEntry2<'e>,)->Result<(CommittedEntry2,Command2,Option<&'e[u8]>,Option<([u8;16],[u8;16])>,),Fault2,>"
	);
	// Exact local signature normalization only; the shared nongeneric extractor stays unchanged.
	let extraction_source = replay_source.replacen(header, "fn command_at(", 1);
	let extraction = fn_body(&extraction_source, "command_at").to_owned();
	assert!(
		extraction.find("if raw_carrier").unwrap() < extraction.find("fixture::command").unwrap()
	);
	assert!(extraction.contains("carrier.ok_or(Fault2::Carrier)"));
	let replay = fs::read_to_string(root.join("replay.rs")).unwrap();
	let dispatch = replay
		.split("fn dispatch(")
		.nth(1)
		.unwrap()
		.split("fn command(")
		.next()
		.unwrap();
	assert!(dispatch.find("Fault2::Carrier").unwrap() < dispatch.find("origin_view2").unwrap());
	let initialize = fn_body(&replay, "initialize");
	assert!(
		initialize.find("Fault2::Carrier").unwrap() < initialize.find("replay_monitor2").unwrap()
	);
	let command = fn_body(&replay, "command");
	assert!(
		command.find("VerifyError2::EnvelopeContext").unwrap()
			< command.find("TrustedEntry2::bind").unwrap()
	);
	assert!(extraction.contains("#[cfg(test)]"));
	assert!(extraction.contains("#[cfg(not(test))]"));
	assert!(
		extraction
			.split("#[cfg(not(test))]")
			.nth(1)
			.unwrap()
			.contains("Err(Fault2::NoCarrier)")
	);
	assert!(
		!extraction
			.split("#[cfg(not(test))]")
			.nth(1)
			.unwrap()
			.contains("fixture::")
	);
}

// C-S4b1 syntax pins; behavioral cuts, signatures and wire bytes have separate tests.
#[test]
fn role_epoch_control_is_pre_entry_bounded_and_dormant() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let read = |name: &str| fs::read_to_string(root.join(name)).unwrap();
	fn forbidden(tokens: proc_macro2::TokenStream) -> Option<String> {
		let ts: Vec<_> = tokens.into_iter().collect();
		for (i, t) in ts.iter().enumerate() {
			match t {
				proc_macro2::TokenTree::Ident(id) => {
					let n = id.to_string();
					if n == "static"
						&& !matches!(i.checked_sub(1).and_then(|j| ts.get(j)), Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == '\'')
					{
						return Some(n);
					}
					if [
						"SigningKey",
						"Signer",
						"unsafe",
						"fs",
						"io",
						"net",
						"tokio",
						"SystemTime",
						"Instant",
						"spawn",
						"serde",
					]
					.contains(&n.as_str())
					{
						return Some(n);
					}
					if [
						"Root2",
						"UnverifiedRoot2",
						"TrustedEntry2",
						"VerifiedCommand2",
						"CommittedEntry2",
						"MutationAuthorityState2",
						"TrustedView2",
					]
					.contains(&n.as_str())
						&& (matches!(ts.get(i + 1), Some(proc_macro2::TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Brace)
							|| matches!(ts.get(i + 1), Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == ':'))
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
	for good in [
		"fn f(x: &Root2) {}",
		"fn f(x: &mut Root2) {}",
		r#"fn f() -> &'static [u8] { b"x" }"#,
	] {
		assert_eq!(forbidden(good.parse().unwrap()), None);
	}
	for bad in [
		"fn f() { let _ = SigningKey::from_bytes(x); }",
		"fn f() { let _ = Root2 { state: x }; }",
		"fn f() { tokio::spawn(x); }",
		"fn f() { static X: u8 = 0; }",
	] {
		assert!(forbidden(bad.parse().unwrap()).is_some());
	}
	for name in [
		"role_epochs.rs",
		"role_command.rs",
		"role_freeze.rs",
		"role_view.rs",
	] {
		let text = read(name);
		assert_eq!(forbidden(text.parse().unwrap()), None, "{name}");
		for item in syn::parse_file(&text).unwrap().items {
			let vis = match &item {
				syn::Item::Fn(x) => Some(&x.vis),
				syn::Item::Struct(x) => Some(&x.vis),
				syn::Item::Enum(x) => Some(&x.vis),
				syn::Item::Type(x) => Some(&x.vis),
				syn::Item::Const(x) => Some(&x.vis),
				_ => None,
			};
			if let Some(vis) = vis {
				assert!(
					matches!(vis, syn::Visibility::Inherited)
						|| matches!(vis, syn::Visibility::Restricted(v) if v.path.is_ident("super")),
					"{name}: private root-only items"
				);
			}
		}
	}
	let epochs = read("role_epochs.rs");
	assert!(epochs.contains("const SUCCESSORS3: usize = 64;"));
	assert!(epochs.contains("frozen || (counts == [0; 3] && !terminal)"));
	assert!(
		epochs.find("usize::from(n) > SUCCESSORS3").unwrap()
			< epochs.find("try_reserve_exact").unwrap()
	);
	assert!(epochs.contains("let frozen = match r.u8()?"));
	assert!(epochs.contains("_ => return Err(RootError2::Codec)"));
	assert!(epochs.contains("let transition = r.optional_log()?"));
	assert!(epochs.contains("e.transition.index <= s.bootstrap.index"));
	assert!(epochs.contains("!coords.insert(e.transition.index)"));
	let rotate = fn_body(&epochs, "rotate");
	assert!(
		rotate.find("history > root.limits.history").unwrap()
			< rotate.find("get_or_insert_with").unwrap()
	);
	assert!(
		rotate.find("e.original != original").unwrap()
			< rotate.find("successors[role].push").unwrap()
	);
	assert!(rotate.contains("(s.covered, s.fingerprint, s.command_digest)"));
	let install = read("admin_install.rs");
	assert!(
		install.find("epochs.original != original").unwrap()
			< install.find("Ok(InstalledAdmin2").unwrap()
	);
	assert!(install.contains("cut: (s.covered, s.fingerprint, s.command_digest)"));
	let proof = read("role_command.rs");
	assert!(proof.contains("const ROLE_PROOF_BYTES3: usize = 224;"));
	assert_eq!(calls(&proof, "verify_strict"), 1);
	// Key custody K7: `verify_for` is a thin wrapper over the one slot loop, which reaches the
	// signature only through `slot_signed`; the role requirement is defined once.
	assert_eq!(
		fn_body(&proof, "verify_for").replace(char::is_whitespace, ""),
		"{verify_core(&a.view(),bytes,command,expected,form)}"
	);
	assert_eq!(calls(fn_body(&proof, "verify_core"), "slot_signed"), 1);
	assert_eq!(calls(fn_body(&proof, "slot_signed"), "verify_strict"), 1);
	assert_eq!(proof.matches("fn role_requirement(").count(), 1);
	assert!(proof.contains("&bytes[..86]") && proof.contains("&bytes[150..160]"));
	let command = syn::parse_file(&read("command.rs")).unwrap();
	let variants = command
		.items
		.iter()
		.find_map(|i| match i {
			syn::Item::Enum(e) if e.ident == "Command2" => Some(
				e.variants
					.iter()
					.map(|v| v.ident.to_string())
					.collect::<Vec<_>>(),
			),
			_ => None,
		})
		.unwrap();
	assert_eq!(variants.len(), 21);
	assert_eq!(
		&variants[14..18],
		[
			"RotateAdmin",
			"RotateGovernance",
			"RotateRecovery",
			"EpochAdmissionPermission"
		]
	);
	assert_eq!(&variants[18..20], ["Freeze", "Unfreeze"]);
	assert_eq!(variants[20], "RetireRoot");
	let freeze = read("role_freeze.rs");
	let transition = fn_body(&freeze, "transition");
	assert!(
		transition.find("history > root.limits.history").unwrap()
			< transition.find("get_or_insert_with").unwrap()
	);
	assert!(transition.contains("reason: 24") && transition.contains("*binding == previous"));
	fn scoped_frozen_order(text: &str, mutable: bool) -> bool {
		let anchor = if mutable {
			"let mut result ="
		} else {
			"let result ="
		};
		let Some((_, tail)) = text.split_once(anchor) else {
			return false;
		};
		let Some((block, _)) = tail.split_once("};") else {
			return false;
		};
		let flat = block.replace(char::is_whitespace, "");
		let prefix = if mutable {
			"ifsuper::role_freeze::blocked(s,command){denied(23,None)}elseif!verified.authorized(){denied(8,None)"
		} else {
			"ifsuper::role_freeze::blocked(&old.state,command){denied(23,None)}elseifverified.authorized(){"
		};
		flat.starts_with(prefix) && (mutable || flat.contains("}else{denied(8,None)"))
	}
	for (file, mutable) in [("reducer.rs", false), ("admission_gate.rs", true)] {
		let text = read(file);
		assert!(scoped_frozen_order(&text, mutable), "{file}");
		assert!(!scoped_frozen_order(
			&text.replace("super::role_freeze::blocked", "removed"),
			mutable
		));
		assert!(!scoped_frozen_order(
			&text
				.replace(
					if mutable {
						"super::role_freeze::blocked(s, command)"
					} else {
						"super::role_freeze::blocked(&old.state, command)"
					},
					"SWAP"
				)
				.replace(
					if mutable {
						"!verified.authorized()"
					} else {
						"verified.authorized()"
					},
					if mutable {
						"super::role_freeze::blocked(s, command)"
					} else {
						"super::role_freeze::blocked(&old.state, command)"
					}
				)
				.replace(
					"SWAP",
					if mutable {
						"!verified.authorized()"
					} else {
						"verified.authorized()"
					}
				),
			mutable
		));
		assert!(!scoped_frozen_order(
			&text.replace("denied(23, None)", "denied(8, None)"),
			mutable
		));
		if !mutable {
			let bootstrap = text.split_once("let result =").unwrap().0;
			assert!(bootstrap.contains("verified.authorized()"));
			assert!(!scoped_frozen_order(bootstrap, false));
		}
	}
	fn proof_has_unsupported_domain(text: &str) -> bool {
		text.contains("UNSUPPORTED")
	}
	assert!(proof_has_unsupported_domain("b\"UNSUPPORTED\""));
	assert!(!proof_has_unsupported_domain("Err(E::Domain)"));
	assert!(!proof_has_unsupported_domain(&read("role_command.rs")));
	assert!(read("trust.rs").contains("role_seed: Option<super::role_epochs::Seed3>"));
	assert_eq!(
		calls(&read("verification.rs"), "VerifiedCommand2::verified2"),
		1
	);
	let modules = read("mod.rs");
	assert!(modules.contains("\nmod role_epochs;") && modules.contains("\nmod role_command;"));
	assert!(modules.contains("#[cfg(test)]\nmod tests;"));
	assert!(read("tests/mod.rs").contains("mod role_epochs;"));
	let tests = read("tests/role_epochs/mod.rs");
	for module in ["codec", "rotation", "verification", "compatibility"] {
		assert!(tests.contains(&format!("mod {module};")));
	}
	let carrier = read("committed.rs");
	assert!(carrier.contains("296"));
	assert!(!carrier.contains("RotateAdmin") && !carrier.contains("SigningKey"));
}

// Key custody K7 (ADR 0101): the operator proof facade and the role view. Root2's public surface
// is exactly the earlier re-exports plus `operator_proof`, whose public signatures carry only byte
// arrays, primitives and the facade's own result types. No non-test Root2 module names a signing
// token. A `RoleView3` has exactly the five checker fields, is built only by `InstalledAdmin2::view`
// and `RoleView3::offline` (called only by the context-export reader), and never meets a command
// token; the four checkers take it; the facade's governance `VISROL03` self-check applies the same
// action-bit predicate as `verify_governance2`. Syntax checks over source text, probed below.
fn facade_violation(files: &std::collections::BTreeMap<String, String>) -> Option<String> {
	fn idents(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
		for t in tokens {
			match t {
				proc_macro2::TokenTree::Ident(id) => out.push(id.to_string()),
				proc_macro2::TokenTree::Group(g) => idents(g.stream(), out),
				_ => {}
			}
		}
	}
	fn type_names(ty: &syn::Type, out: &mut Vec<String>) {
		match ty {
			syn::Type::Path(p) => {
				for s in &p.path.segments {
					out.push(s.ident.to_string());
					if let syn::PathArguments::AngleBracketed(a) = &s.arguments {
						for arg in &a.args {
							if let syn::GenericArgument::Type(t) = arg {
								type_names(t, out);
							}
						}
					}
				}
			}
			syn::Type::Reference(r) => type_names(&r.elem, out),
			syn::Type::Array(a) => type_names(&a.elem, out),
			syn::Type::Slice(s) => type_names(&s.elem, out),
			syn::Type::Tuple(t) => t.elems.iter().for_each(|e| type_names(e, out)),
			other => out.push(format!("unsupported {}", ty_text(other))),
		}
	}
	fn ty_text(ty: &syn::Type) -> String {
		match ty {
			syn::Type::Path(p) => p
				.path
				.segments
				.iter()
				.map(|s| match &s.arguments {
					syn::PathArguments::AngleBracketed(a) => format!(
						"{}<{}>",
						s.ident,
						a.args
							.iter()
							.map(|g| match g {
								syn::GenericArgument::Type(t) => ty_text(t),
								_ => "?".into(),
							})
							.collect::<Vec<_>>()
							.join(",")
					),
					_ => s.ident.to_string(),
				})
				.collect::<Vec<_>>()
				.join("::"),
			syn::Type::Array(a) => match &a.len {
				syn::Expr::Lit(syn::ExprLit {
					lit: syn::Lit::Int(n),
					..
				}) => format!("[{};{}]", ty_text(&a.elem), n.base10_digits()),
				_ => "?".into(),
			},
			syn::Type::Tuple(t) => format!(
				"({})",
				t.elems.iter().map(ty_text).collect::<Vec<_>>().join(",")
			),
			syn::Type::Reference(r) => format!("&{}", ty_text(&r.elem)),
			syn::Type::Slice(s) => format!("[{}]", ty_text(&s.elem)),
			_ => "?".into(),
		}
	}
	fn use_text(tree: &syn::UseTree) -> String {
		match tree {
			syn::UseTree::Path(p) => format!("{}::{}", p.ident, use_text(&p.tree)),
			syn::UseTree::Name(n) => n.ident.to_string(),
			syn::UseTree::Rename(r) => format!("{}as{}", r.ident, r.rename),
			syn::UseTree::Glob(_) => "*".into(),
			syn::UseTree::Group(g) => format!(
				"{{{}}}",
				g.items.iter().map(use_text).collect::<Vec<_>>().join(",")
			),
		}
	}
	fn signature_types(sig: &syn::Signature, out: &mut Vec<String>) {
		for input in &sig.inputs {
			if let syn::FnArg::Typed(t) = input {
				type_names(&t.ty, out);
			}
		}
		if let syn::ReturnType::Type(_, t) = &sig.output {
			type_names(t, out);
		}
	}
	let flat = |t: &str| {
		without_test_items_deep(t.parse().unwrap())
			.to_string()
			.replace(' ', "")
	};
	let get = |name: &str| files.get(name).map(String::as_str).unwrap_or("");
	// 1. No signing token in any non-test Root2 module, the facade included.
	for (name, text) in files {
		let mut names = Vec::new();
		idents(without_test_items_deep(text.parse().unwrap()), &mut names);
		for banned in ["SigningKey", "Signer", "sign"] {
			if names.iter().any(|n| n == banned) {
				return Some(format!("{name}: signing token {banned}"));
			}
		}
	}
	// 2. The public surface of root2: the earlier re-exports plus the facade module.
	let mut surface = Vec::new();
	for item in syn::parse_file(get("mod.rs")).unwrap().items {
		match item {
			syn::Item::Mod(m) if matches!(m.vis, syn::Visibility::Public(_)) => {
				surface.push(format!("mod {}", m.ident))
			}
			syn::Item::Use(u) if matches!(u.vis, syn::Visibility::Public(_)) => {
				surface.push(use_text(&u.tree))
			}
			syn::Item::Mod(m) if matches!(m.vis, syn::Visibility::Restricted(_)) => {
				surface.push(format!("restricted mod {}", m.ident))
			}
			syn::Item::Mod(_) | syn::Item::Use(_) => {}
			_ => return Some("root2/mod.rs: only modules and re-exports".into()),
		}
	}
	if surface
		!= [
			"restricted mod compromise",
			"mod durable",
			"mod operator_proof",
			"command::{ApplyResult2,Command2}",
			"state::{Root2,UnverifiedRoot2}",
			"trust::{TrustedEntry2,TrustedView2,VerifiedCommand2}",
			"types::*",
		] {
		return Some(format!("root2 public surface changed: {surface:?}"));
	}
	// 3. The facade module: private children, the exact public re-exports, nothing else public.
	let mut exported = Vec::new();
	for item in syn::parse_file(get("operator_proof.rs")).unwrap().items {
		match item {
			syn::Item::Mod(m) if matches!(m.vis, syn::Visibility::Inherited) => {}
			syn::Item::Use(u) if matches!(u.vis, syn::Visibility::Public(_)) => {
				exported.push(use_text(&u.tree))
			}
			syn::Item::Use(u) if !matches!(u.vis, syn::Visibility::Public(_)) => {}
			syn::Item::Fn(f) if matches!(f.vis, syn::Visibility::Inherited) => {}
			_ => {
				return Some(
					"operator_proof.rs: private children, re-exports, private helpers".into(),
				);
			}
		}
	}
	if exported
		!= [
			"context::{AuditDerived,ContextRefusal,VerifiedContext,verify_context_export}",
			"proof::{ProofRefusal,Unsigned,check_new_role_key,merge,prepare,prepare_containment,prepare_statement,self_check,self_check_containment,splice}",
		] {
		return Some(format!(
			"operator_proof.rs: public items changed: {exported:?}"
		));
	}
	// 4. Public facade signatures: byte arrays, primitives and the facade's own types only.
	let allowed = [
		"u8",
		"u16",
		"u32",
		"u64",
		"usize",
		"bool",
		"Vec",
		"Option",
		"Result",
		"Self",
		"T",
		"VerifiedContext",
		"Unsigned",
		"ProofRefusal",
		"P",
		"ContextRefusal",
		"AuditDerived",
	];
	for name in ["operator_proof/context.rs", "operator_proof/proof.rs"] {
		let file: syn::File =
			syn::parse2(without_test_items_deep(get(name).parse().unwrap())).unwrap();
		let mut types = Vec::new();
		for item in &file.items {
			match item {
				syn::Item::Fn(f) if matches!(f.vis, syn::Visibility::Public(_)) => {
					signature_types(&f.sig, &mut types)
				}
				syn::Item::Struct(s) if matches!(s.vis, syn::Visibility::Public(_)) => {
					if s.fields
						.iter()
						.any(|f| !matches!(f.vis, syn::Visibility::Inherited))
					{
						return Some(format!("{name}: a public facade struct exposes a field"));
					}
				}
				syn::Item::Impl(i) => {
					for x in &i.items {
						if let syn::ImplItem::Fn(f) = x
							&& matches!(f.vis, syn::Visibility::Public(_))
						{
							signature_types(&f.sig, &mut types)
						}
					}
				}
				_ => {}
			}
		}
		if let Some(t) = types.iter().find(|t| !allowed.contains(&t.as_str())) {
			return Some(format!("{name}: public signature names {t}"));
		}
	}
	// 5. The view: exactly the five checker fields (a test-only mask aside), private, root2-only.
	let view = get("role_view.rs");
	let view_file: syn::File = syn::parse2(without_test_items_deep(view.parse().unwrap())).unwrap();
	let Some(fields) = view_file.items.iter().find_map(|i| match i {
		syn::Item::Struct(s) if s.ident == "RoleView3" => Some(s),
		_ => None,
	}) else {
		return Some("role_view.rs: no RoleView3".into());
	};
	let shape = fields
		.fields
		.iter()
		.map(|f| {
			(
				f.ident.as_ref().unwrap().to_string(),
				ty_text(&f.ty),
				matches!(f.vis, syn::Visibility::Inherited),
			)
		})
		.collect::<Vec<_>>();
	let expected = [
		("context", "AdminContext2"),
		("admin_key", "[u8;32]"),
		("namespace", "AdminNamespace2"),
		("governance", "GovernanceRole2"),
		("active", "[(u64,Digest2);3]"),
	]
	.map(|(n, t)| (n.to_string(), t.to_string(), true));
	if shape != expected
		|| !matches!(&fields.vis, syn::Visibility::Restricted(r) if r.path.is_ident("super"))
	{
		return Some(format!("role_view.rs: RoleView3 fields changed: {shape:?}"));
	}
	// 6. Built only by the one-for-one copy and the offline constructor; `offline` called only
	// by the context-export reader; the replayed chain constructed only there.
	for (name, text) in files {
		let t = flat(text);
		// Its fields are private to role_view.rs, so a literal anywhere else is a compile error;
		// within it, exactly view()'s copy and offline()'s `Self` literal.
		// A literal, not a declaration or a return type before a body (`->RoleView3{`).
		let literal = |ty: &str| {
			t.match_indices(&format!("{ty}{{"))
				.filter(|(i, _)| {
					let before = &t[..*i];
					!before.ends_with("struct")
						&& !before.ends_with("impl")
						&& !before.ends_with('>')
						&& !before.ends_with('&')
				})
				.count()
		};
		let literals = literal("RoleView3");
		let is_view = name == "role_view.rs";
		if literals != usize::from(is_view) || (is_view && t.matches("Self{").count() != 1) {
			return Some(format!(
				"{name}: RoleView3 built outside view() and offline()"
			));
		}
		let offline = t.matches("RoleView3::offline(").count();
		if offline != usize::from(name == "operator_proof/context.rs") * 2 {
			return Some(format!("{name}: RoleView3::offline outside the reader"));
		}
		let chains = literal("Chain3");
		if chains != usize::from(name == "operator_proof/context.rs") {
			return Some(format!("{name}: Chain3 built outside the reader"));
		}
		// A view never meets a command token.
		if t.contains("RoleView3")
			&& [
				"VerifiedCommand2",
				"verified2(",
				"TrustedEntry2",
				"CommittedEntry2",
			]
			.iter()
			.any(|b| t.contains(b))
		{
			return Some(format!("{name}: a role view next to a command token"));
		}
	}
	if fn_body(view, "view").replace(char::is_whitespace, "")
		!= "{RoleView3{context:admin_context2(self),admin_key:admin_key2(self),namespace:admin_namespace2(self),governance:admin_governance2(self),active:admin_active3(self),#[cfg(test)]test_mask:None,}}"
	{
		return Some("role_view.rs: view() is not the one-for-one copy".into());
	}
	for name in [
		"role_view.rs",
		"operator_proof.rs",
		"operator_proof/context.rs",
		"operator_proof/proof.rs",
	] {
		let t = flat(get(name));
		for banned in [
			"VerifiedCommand2",
			"TrustedEntry2",
			"CommittedEntry2",
			"advance_verified2",
			"install_admin2",
			"MutationAuthorityState2",
			"std::fs",
			"std::io",
			"std::net",
			"std::env",
			"tokio",
			"unsafe",
			"SystemTime",
			"Instant",
		] {
			if t.contains(banned) {
				return Some(format!("{name}: {banned}"));
			}
		}
	}
	// 7. The four checkers take the view.
	for (name, check) in [
		("role_command.rs", "fnverify_core(view:&RoleView3,"),
		(
			"admin_statement.rs",
			"fncheck_admin_statement2(view:&RoleView3,",
		),
		(
			"admin_statement.rs",
			"fncheck_epoch_statement3(view:&RoleView3,",
		),
		(
			"governance_command.rs",
			"fncheck_governance_command2(view:&RoleView3,",
		),
	] {
		if !flat(get(name)).contains(check) {
			return Some(format!("{name}: {check}"));
		}
	}
	// 8. One action-bit predicate: defined on the view, applied by `verify_governance2` and by the
	// facade's governance VISROL03 self-check (and its prepare), nowhere else.
	if view.matches("fn governs(").count() != 1
		|| fn_body(view, "governs").replace(char::is_whitespace, "")
			!= "{self.mask(self.governance)&action.bit()!=0}"
	{
		return Some("role_view.rs: governs is not the action-bit predicate".into());
	}
	let proof = get("operator_proof/proof.rs");
	for (name, text) in files {
		let t = flat(text);
		let want = match name.as_str() {
			"governance_verification.rs" => 1,
			"operator_proof/proof.rs" => 2,
			_ => 0,
		};
		if t.matches(".governs(action)").count() != want {
			return Some(format!("{name}: governs calls"));
		}
	}
	let check_under = fn_body(proof, "check_under").replace(char::is_whitespace, "");
	if !check_under.contains(
		"parse(s).is_ok()&&view.governs(action)&&verify_core(view,s,digest,[2,0],RoleForm3::Legacy)==Ok(true)",
	) {
		return Some("operator_proof/proof.rs: the governance VISROL03 self-check skips the action bit".into());
	}
	// 9. One role requirement, used by the verifier, the reader and the facade only.
	for (name, text) in files {
		let t = flat(text);
		let calls =
			t.matches("role_requirement(").count() - t.matches("fnrole_requirement(").count();
		let ok = match name.as_str() {
			"verification.rs" | "operator_proof/context.rs" => calls == 1,
			"operator_proof/proof.rs" => calls >= 1,
			_ => calls == 0,
		};
		if !ok {
			return Some(format!("{name}: role_requirement calls"));
		}
	}
	// 10. The facade's governance actions are the verifier's: the same nine pairs.
	let pairs = |text: &str, function: &str| {
		let body = fn_body(text, function).replace(char::is_whitespace, "");
		let mut out = body
			.split("Command2::")
			.skip(1)
			.filter_map(|arm| {
				let (kind, rest) = arm.split_once(['(', '{', '='])?;
				let action = rest.split_once("GovernanceAction2::")?.1;
				let action: String = action.chars().take_while(|c| c.is_alphanumeric()).collect();
				Some((kind.to_string(), action))
			})
			.filter(|(kind, action)| kind == action)
			.collect::<Vec<_>>();
		out.sort();
		out
	};
	let facade_pairs = pairs(proof, "governance_action");
	if facade_pairs.len() != 9
		|| facade_pairs != pairs(get("governance_verification.rs"), "governance_failure2")
	{
		return Some(
			"operator_proof/proof.rs: governance actions differ from the verifier's".into(),
		);
	}
	None
}
#[test]
fn operator_proof_facade_is_exact_and_never_signs() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let mut files = std::collections::BTreeMap::new();
	let mut stack = vec![root.clone()];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			let name = p.file_name().unwrap().to_string_lossy().to_string();
			if p.is_dir() {
				if name != "tests" {
					stack.push(p);
				}
			} else if name.ends_with(".rs") && !name.ends_with("tests.rs") {
				let rel = p.strip_prefix(&root).unwrap().to_string_lossy().to_string();
				files.insert(rel, fs::read_to_string(&p).unwrap());
			}
		}
	}
	assert!(files.contains_key("operator_proof/proof.rs") && files.len() > 40);
	assert_eq!(facade_violation(&files), None);
	// Probes: each guarded property, broken once, is caught.
	let edit = |name: &str, f: &dyn Fn(&str) -> String| {
		let mut x = files.clone();
		let text = f(&x[name]);
		assert_ne!(text, x[name], "probe did not change {name}");
		x.insert(name.to_string(), text);
		x
	};
	let probes: Vec<(&str, std::collections::BTreeMap<String, String>)> = vec![
		(
			"a signing call in the facade",
			edit("operator_proof/proof.rs", &|t| {
				format!(
					"{t}\nfn s(k: &[u8; 32]) {{ let _ = SigningKey::from_bytes(k).sign(b\"x\"); }}"
				)
			}),
		),
		(
			"a Signer import in a checker",
			edit("admin_statement.rs", &|t| {
				format!("use ed25519_dalek::Signer;\n{t}")
			}),
		),
		(
			"another public root2 module",
			edit("mod.rs", &|t| t.replace("\nmod admin;", "\npub mod admin;")),
		),
		(
			"another public re-export",
			edit("mod.rs", &|t| format!("{t}\npub use role_view::RoleView3;")),
		),
		(
			"a public facade item outside the re-exports",
			edit("operator_proof.rs", &|t| {
				t.replace("\nmod proof;", "\npub mod proof;")
			}),
		),
		(
			"a root2 type in a public facade signature",
			edit("operator_proof/proof.rs", &|t| {
				format!("{t}\npub fn leak(v: &RoleView3) -> Digest2 {{ todo!() }}")
			}),
		),
		(
			"a public field on the verified context",
			edit("operator_proof/context.rs", &|t| {
				t.replace(
					"\thistory: Vec<(u8, u64, [u8; 32])>,",
					"\tpub history: Vec<(u8, u64, [u8; 32])>,",
				)
			}),
		),
		(
			"optional governance on the view",
			edit("role_view.rs", &|t| {
				t.replace(
					"governance: GovernanceRole2,",
					"governance: Option<GovernanceRole2>,",
				)
			}),
		),
		(
			"a sixth view field",
			edit("role_view.rs", &|t| {
				t.replace(
					"\tactive: [(u64, Digest2); 3],\n",
					"\tactive: [(u64, Digest2); 3],\n\tcut: Option<Digest2>,\n",
				)
			}),
		),
		(
			"a view built in the verifier",
			edit("verification.rs", &|t| {
				format!("{t}\nfn v() -> RoleView3 {{ RoleView3 {{ context }} }}")
			}),
		),
		(
			"an offline view outside the reader",
			edit("operator_proof/proof.rs", &|t| {
				format!("{t}\nfn o() {{ RoleView3::offline(&c, &m, &k); }}")
			}),
		),
		(
			"a chain built outside the reader",
			edit("operator_proof/proof.rs", &|t| {
				format!("{t}\nfn o() {{ let _ = Chain3 {{ successors }}; }}")
			}),
		),
		(
			"a view next to a command token",
			edit("role_view.rs", &|t| {
				format!("{t}\nfn t(_: VerifiedCommand2) {{}}")
			}),
		),
		(
			"view() copying a different field",
			edit("role_view.rs", &|t| {
				t.replace("admin_key: admin_key2(self),", "admin_key: [0; 32],")
			}),
		),
		(
			"a checker taking the installed administrator",
			edit("admin_statement.rs", &|t| {
				t.replace(
					"fn check_admin_statement2(\n\tview: &RoleView3,",
					"fn check_admin_statement2(\n\tview: &InstalledAdmin2,",
				)
			}),
		),
		(
			"the facade's governance self-check without the action bit",
			edit("operator_proof/proof.rs", &|t| {
				t.replace("&& view.governs(action)\n", "\n")
			}),
		),
		(
			"a second action-bit predicate",
			edit("governance_verification.rs", &|t| {
				t.replace(
					"!admin.view().governs(action)",
					"admin.view().governance_role().is_none_or(|r| r.actions() & action.bit() == 0)",
				)
			}),
		),
		(
			"a role requirement copied outside the facade",
			edit("compromise/attribution.rs", &|t| {
				format!("{t}\nfn r(c: &Command2) {{ role_requirement(c, 0); }}")
			}),
		),
		(
			"a facade governance action that is not the verifier's",
			edit("operator_proof/proof.rs", &|t| {
				t.replace(
					"Command2::Cut { .. } => GovernanceAction2::Cut,",
					"Command2::Cut { .. } => GovernanceAction2::Release,",
				)
			}),
		),
		(
			"file I/O in the facade",
			edit("operator_proof/context.rs", &|t| {
				format!("{t}\nfn w() {{ std::fs::write(\"x\", []).unwrap(); }}")
			}),
		),
	];
	for (what, x) in &probes {
		assert!(facade_violation(x).is_some(), "probe not caught: {what}");
	}
}

/// The owners (enclosing `fn`, `enum`, `struct`, `trait` or `mod`) of every `target` identifier.
fn ident_owners(
	tokens: proc_macro2::TokenStream,
	owner: &str,
	target: &str,
	out: &mut Vec<String>,
) {
	let ts: Vec<proc_macro2::TokenTree> = tokens.into_iter().collect();
	let mut pending: Option<String> = None;
	for (i, t) in ts.iter().enumerate() {
		match t {
			// A raw identifier (`r#Containment`) is the same name.
			proc_macro2::TokenTree::Ident(id)
				if id.to_string().trim_start_matches("r#") == target =>
			{
				out.push(owner.to_string())
			}
			proc_macro2::TokenTree::Ident(id)
				if matches!(
					id.to_string().as_str(),
					"fn" | "enum" | "struct" | "trait" | "mod"
				) =>
			{
				// Owners are item paths (`outer::inner`), so a nested item never passes for an
				// allowed top-level one.
				if let Some(proc_macro2::TokenTree::Ident(name)) = ts.get(i + 1) {
					let name = name.to_string();
					pending = Some(if owner == "<file>" {
						name
					} else {
						format!("{owner}::{name}")
					});
				}
			}
			proc_macro2::TokenTree::Group(g) if g.delimiter() == proc_macro2::Delimiter::Brace => {
				let inner = pending.take().unwrap_or_else(|| owner.to_string());
				ident_owners(g.stream(), &inner, target, out);
			}
			proc_macro2::TokenTree::Group(g) => {
				let inner = pending.clone().unwrap_or_else(|| owner.to_string());
				ident_owners(g.stream(), &inner, target, out);
			}
			_ => {}
		}
	}
}

/// D-c / activation B1: `RoleForm3::Containment` is named only in `role_command.rs` (the enum and
/// its domain), in the facade's two containment functions, and in `live.rs::verify_containment`.
/// Never in `role_requirement`, the on-log verifiers or the audit attribution.
fn containment_violation(files: &std::collections::BTreeMap<String, String>) -> Option<String> {
	let allowed = |name: &str| -> Option<&'static [&'static str]> {
		Some(match name {
			"role_command.rs" => &["RoleForm3", "domain"],
			"operator_proof/proof.rs" => &["prepare_containment", "self_check_containment"],
			n if n == "live.rs" || n.ends_with("/live.rs") => &["verify_containment"],
			_ => return None,
		})
	};
	for (name, text) in files {
		let tokens = without_test_items_deep(text.parse().unwrap());
		let mut owners = Vec::new();
		ident_owners(tokens, "<file>", "Containment", &mut owners);
		match allowed(name) {
			None if !owners.is_empty() => {
				return Some(format!("{name}: names Containment in {owners:?}"));
			}
			None => {}
			Some(sites) => {
				if let Some(o) = owners.iter().find(|o| !sites.contains(&o.as_str())) {
					return Some(format!("{name}: names Containment in {o}"));
				}
				let needs_all = name != "live.rs" && !name.ends_with("/live.rs");
				if needs_all && sites.iter().any(|s| !owners.iter().any(|o| o == s)) {
					return Some(format!(
						"{name}: an expected containment site is missing: {owners:?}"
					));
				}
			}
		}
	}
	None
}

#[test]
fn containment_form_is_named_only_at_its_three_sites() {
	let root =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let mut files = std::collections::BTreeMap::new();
	visit_root2_sources(&root, &mut |p, text| {
		let rel = p.strip_prefix(&root).unwrap().to_string_lossy().to_string();
		files.insert(rel, text.to_string());
	});
	for required in [
		"role_command.rs",
		"operator_proof/proof.rs",
		"verification.rs",
		"governance_verification.rs",
		"compromise/attribution.rs",
	] {
		assert!(files.contains_key(required), "{required}");
	}
	assert_eq!(containment_violation(&files), None);
	let edit = |name: &str, f: &dyn Fn(&str) -> String| {
		let mut x = files.clone();
		let text = f(&x[name]);
		assert_ne!(text, x[name], "probe did not change {name}");
		x.insert(name.to_string(), text);
		x
	};
	let stray = "\nfn stray() -> RoleForm3 { RoleForm3::Containment }\n";
	for (what, probed) in [
		(
			"role_requirement",
			edit("role_command.rs", &|t| {
				t.replacen(
					"\t\t_ => RoleForm3::Legacy,\n\t};\n\tSome((expected, form))",
					"\t\t_ => RoleForm3::Containment,\n\t};\n\tSome((expected, form))",
					1,
				)
			}),
		),
		(
			"verification.rs",
			edit("verification.rs", &|t| format!("{t}{stray}")),
		),
		(
			"governance_verification.rs",
			edit("governance_verification.rs", &|t| format!("{t}{stray}")),
		),
		(
			"attribution.rs",
			edit("compromise/attribution.rs", &|t| {
				t.replacen(
					"\t\t_ => RoleForm3::Legacy,",
					"\t\t_ => RoleForm3::Containment,",
					1,
				)
			}),
		),
		(
			"another facade function",
			edit("operator_proof/proof.rs", &|t| format!("{t}{stray}")),
		),
		("another live.rs function", {
			let mut x = files.clone();
			x.insert("live.rs".into(), stray.into());
			x
		}),
		(
			"a nested fn named like an allowed site",
			edit("role_command.rs", &|t| {
				t.replacen(
					"\tSome((expected, form))\n}",
					"\tfn domain() -> RoleForm3 {\n\t\tRoleForm3::Containment\n\t}\n\tSome((expected, form))\n}",
					1,
				)
			}),
		),
		(
			"a raw identifier",
			edit("verification.rs", &|t| {
				format!("{t}\nfn stray() -> RoleForm3 {{ RoleForm3::r#Containment }}\n")
			}),
		),
	] {
		assert!(containment_violation(&probed).is_some(), "{what}");
	}
	// Control: the node's future check may name it.
	let mut live = files.clone();
	live.insert(
		"live.rs".into(),
		"\nfn verify_containment() { let _ = RoleForm3::Containment; }\n".into(),
	);
	assert_eq!(containment_violation(&live), None);
}
