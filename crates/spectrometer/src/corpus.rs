use crate::lsp::{RustAnalyzer, position_to_byte};
use crate::model::{Corpus, ResolutionStats, SourceFile, Symbol, SymbolId, split_identifier};
use ra_ap_syntax::ast::{self, HasName as _};
use ra_ap_syntax::{
	AstNode, Edition, NodeOrToken, SourceFile as RaSourceFile, SyntaxKind, SyntaxNode, SyntaxToken,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn load_corpus(root: &Path, resolver: Option<&Path>) -> Result<Corpus, String> {
	let root = root
		.canonicalize()
		.map_err(|error| format!("unable to canonicalize repository root: {error}"))?;
	let relative_paths = tracked_rust_files(&root)?;
	let mut files = Vec::with_capacity(relative_paths.len());
	let mut symbols = Vec::new();
	for relative_path in relative_paths {
		let canonical_path = root.join(&relative_path);
		let text = std::fs::read_to_string(&canonical_path)
			.map_err(|error| format!("unable to read {}: {error}", canonical_path.display()))?;
		let display_path = relative_path.to_string_lossy().replace('\\', "/");
		let file_index = files.len();
		symbols.extend(extract_symbols(&display_path, file_index, &text));
		files.push(SourceFile {
			relative_path: display_path,
			canonical_path,
			text,
		});
	}
	symbols.sort_by(|left, right| left.id.cmp(&right.id));
	let mut corpus = Corpus {
		root,
		files,
		symbols,
		resolution: ResolutionStats::default(),
		resolution_backend: None,
		resolution_error: None,
	};
	if let Some(executable) = resolver {
		match resolve_call_edges(&mut corpus, executable) {
			Ok(backend) => corpus.resolution_backend = Some(backend),
			Err(error) => corpus.resolution_error = Some(error),
		}
	}
	Ok(corpus)
}

fn tracked_rust_files(root: &Path) -> Result<Vec<PathBuf>, String> {
	let output = Command::new("git")
		.arg("-C")
		.arg(root)
		.args(["ls-files", "-z", "--", "*.rs"])
		.output()
		.map_err(|error| format!("unable to enumerate Git corpus: {error}"))?;
	if !output.status.success() {
		return Err(format!(
			"git ls-files failed: {}",
			String::from_utf8_lossy(&output.stderr).trim()
		));
	}
	let mut files = output
		.stdout
		.split(|byte| *byte == 0)
		.filter(|path| !path.is_empty())
		.filter_map(|path| std::str::from_utf8(path).ok())
		.filter(|path| !path.starts_with("crates/spectrometer/"))
		.map(PathBuf::from)
		.collect::<Vec<_>>();
	files.sort();
	Ok(files)
}

fn extract_symbols(relative_path: &str, file_index: usize, text: &str) -> Vec<Symbol> {
	let parse = RaSourceFile::parse(text, Edition::Edition2024);
	parse
		.tree()
		.syntax()
		.descendants()
		.filter_map(ast::Fn::cast)
		.filter_map(|function| extract_function(relative_path, file_index, function))
		.collect()
}

pub fn extract_file_symbols(relative_path: &str, text: &str) -> Vec<Symbol> {
	extract_symbols(relative_path, 0, text)
}

fn extract_function(relative_path: &str, file_index: usize, function: ast::Fn) -> Option<Symbol> {
	let name_node = function.name()?;
	let name = name_node.text().to_string();
	let range = function.syntax().text_range();
	let function_start = u32::from(range.start());
	let function_end = u32::from(range.end());
	let mut identifiers = Vec::new();
	let mut literal_tokens = Vec::new();

	for element in function.syntax().descendants_with_tokens() {
		let NodeOrToken::Token(token) = element else {
			continue;
		};
		if !token_belongs_to_function(&token, function_start, function_end)
			|| token_is_explicit_doc_attribute(&token)
		{
			continue;
		}
		match token.kind() {
			SyntaxKind::IDENT => identifiers.push(
				token
					.text()
					.strip_prefix("r#")
					.unwrap_or(token.text())
					.to_string(),
			),
			SyntaxKind::STRING | SyntaxKind::BYTE_STRING | SyntaxKind::C_STRING => {
				literal_tokens.extend(split_text(token.text()));
			}
			_ => {}
		}
	}

	let call_site_count = function
		.syntax()
		.descendants()
		.filter(|node| node_belongs_to_function(node, function_start, function_end))
		.filter_map(call_site)
		.count();
	let context_tokens = enclosing_impl_tokens(&function);

	Some(Symbol {
		id: SymbolId {
			path: relative_path.to_string(),
			start: function_start,
			end: function_end,
			provider_kind: "FN".to_string(),
		},
		name_tokens: split_identifier(&name),
		name,
		name_offset: u32::from(name_node.syntax().text_range().start()),
		file_index,
		span_identifiers: identifiers,
		literal_tokens,
		context_tokens,
		call_site_count,
		callees: Vec::new(),
		callers: Vec::new(),
	})
}

fn token_belongs_to_function(token: &SyntaxToken, start: u32, end: u32) -> bool {
	token
		.parent_ancestors()
		.find_map(ast::Fn::cast)
		.is_some_and(|function| {
			let range = function.syntax().text_range();
			u32::from(range.start()) == start && u32::from(range.end()) == end
		})
}

fn node_belongs_to_function(node: &SyntaxNode, start: u32, end: u32) -> bool {
	node.ancestors()
		.find_map(ast::Fn::cast)
		.is_some_and(|function| {
			let range = function.syntax().text_range();
			u32::from(range.start()) == start && u32::from(range.end()) == end
		})
}

fn token_is_explicit_doc_attribute(token: &SyntaxToken) -> bool {
	token.parent_ancestors().any(|node| {
		ast::Attr::cast(node)
			.and_then(|attribute| attribute.simple_name())
			.is_some_and(|name| name == "doc")
	})
}

fn split_text(text: &str) -> Vec<String> {
	text.split(|character: char| !character.is_alphanumeric())
		.filter(|word| !word.is_empty())
		.flat_map(split_identifier)
		.collect()
}

fn enclosing_impl_tokens(function: &ast::Fn) -> Vec<String> {
	let Some(implementation) = function.syntax().ancestors().find_map(ast::Impl::cast) else {
		return Vec::new();
	};
	let mut tokens = Vec::new();
	for ty in [implementation.trait_(), implementation.self_ty()]
		.into_iter()
		.flatten()
	{
		for token in ty
			.syntax()
			.descendants_with_tokens()
			.filter_map(NodeOrToken::into_token)
		{
			if token.kind() == SyntaxKind::IDENT {
				tokens.extend(split_identifier(token.text()));
			}
		}
	}
	tokens
}

fn call_site(node: SyntaxNode) -> Option<()> {
	if let Some(method) = ast::MethodCallExpr::cast(node.clone()) {
		return call_site_from_name_ref(method.name_ref()?);
	}
	let call = ast::CallExpr::cast(node)?;
	let ast::Expr::PathExpr(path_expression) = call.expr()? else {
		return None;
	};
	let name_ref = path_expression
		.path()?
		.syntax()
		.descendants()
		.filter_map(ast::NameRef::cast)
		.last()?;
	call_site_from_name_ref(name_ref)
}

fn call_site_from_name_ref(name_ref: ast::NameRef) -> Option<()> {
	name_ref.ident_token().map(|_| ())
}

fn resolve_call_edges(corpus: &mut Corpus, executable: &Path) -> Result<String, String> {
	let mut analyzer = RustAnalyzer::start(executable, &corpus.root)?;
	let backend = format!("{} ({})", executable.display(), analyzer.version());
	let by_path = corpus.symbols.iter().enumerate().fold(
		BTreeMap::<String, Vec<usize>>::new(),
		|mut by_path, (index, symbol)| {
			by_path
				.entry(symbol.id.path.clone())
				.or_default()
				.push(index);
			by_path
		},
	);
	let mut edges = BTreeSet::new();
	let mut stats = ResolutionStats::default();
	let functions_with_calls = corpus
		.symbols
		.iter()
		.filter(|symbol| symbol.call_site_count != 0)
		.count();
	let mut processed = 0usize;

	for (caller_index, symbol) in corpus.symbols.iter().enumerate() {
		if symbol.call_site_count == 0 {
			continue;
		}
		let file = &corpus.files[symbol.file_index];
		processed += 1;
		if processed.is_multiple_of(250) {
			eprintln!(
				"spectrometer: resolved {processed}/{functions_with_calls} function call graphs"
			);
		}
		let targets = analyzer.outgoing_calls(
			&file.canonical_path,
			&file.text,
			symbol.name_offset as usize,
		)?;
		let mut accounted = 0usize;
		let mut resolved = 0usize;
		let mut unresolved = 0usize;
		for target in targets {
			accounted = accounted.saturating_add(target.occurrences);
			let Some(callee) =
				definition_inside_corpus(corpus, std::slice::from_ref(&target.point), &by_path)
			else {
				unresolved = unresolved.saturating_add(target.occurrences);
				continue;
			};
			resolved = resolved.saturating_add(target.occurrences);
			edges.insert((caller_index, callee));
		}
		unresolved = unresolved.saturating_add(symbol.call_site_count.saturating_sub(accounted));
		stats.call_sites = stats
			.call_sites
			.saturating_add(symbol.call_site_count.max(accounted));
		stats.resolved_inside_corpus = stats.resolved_inside_corpus.saturating_add(resolved);
		stats.unresolved_or_external = stats.unresolved_or_external.saturating_add(unresolved);
	}

	for (caller, callee) in edges {
		corpus.symbols[caller].callees.push(callee);
		corpus.symbols[callee].callers.push(caller);
	}
	for symbol in &mut corpus.symbols {
		symbol.callees.sort_unstable();
		symbol.callers.sort_unstable();
	}
	assert_eq!(
		stats.call_sites,
		stats
			.resolved_inside_corpus
			.saturating_add(stats.unresolved_or_external),
		"resolution accounting must partition all observed call references"
	);
	corpus.resolution = stats;
	analyzer.shutdown();
	Ok(backend)
}

fn definition_inside_corpus(
	corpus: &Corpus,
	definitions: &[crate::lsp::DefinitionPoint],
	by_path: &BTreeMap<String, Vec<usize>>,
) -> Option<usize> {
	definitions.iter().find_map(|definition| {
		let canonical = definition.path.canonicalize().ok()?;
		let relative = canonical.strip_prefix(&corpus.root).ok()?;
		let relative_path = relative.to_string_lossy().replace('\\', "/");
		let candidates = by_path.get(&relative_path)?;
		let file_index = corpus
			.files
			.iter()
			.position(|file| file.relative_path == relative_path)?;
		let offset = position_to_byte(
			&corpus.files[file_index].text,
			definition.line,
			definition.utf16_character,
		)?;
		let offset = u32::try_from(offset).ok()?;
		candidates
			.iter()
			.copied()
			.filter(|index| {
				let id = &corpus.symbols[*index].id;
				id.start <= offset && offset < id.end
			})
			.min_by_key(|index| {
				corpus.symbols[*index]
					.id
					.end
					.saturating_sub(corpus.symbols[*index].id.start)
			})
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extraction_excludes_every_doc_spelling_but_keeps_raw_literals_separate() {
		let text = r##"
/// comment_only_supervisor
#[doc = "attribute_only_auditor"]
fn render_frame() {
	// ordinary_only_janitor
	let value = r#"literal_only_watchdog"#;
	let span_identifier = value;
}
"##;
		let symbols = extract_symbols("src/example.rs", 0, text);
		let symbol = &symbols[0];
		assert_eq!(symbol.name, "render_frame");
		assert!(
			!symbol
				.span_identifiers
				.iter()
				.any(|word| word.contains("comment"))
		);
		assert!(
			!symbol
				.span_identifiers
				.iter()
				.any(|word| word.contains("attribute"))
		);
		assert!(
			!symbol
				.span_identifiers
				.iter()
				.any(|word| word.contains("ordinary"))
		);
		assert!(
			symbol
				.span_identifiers
				.iter()
				.any(|word| word == "span_identifier")
		);
		assert!(symbol.literal_tokens.iter().any(|word| word == "literal"));
		assert!(symbol.literal_tokens.iter().any(|word| word == "watchdog"));
		assert!(!symbol.literal_tokens.iter().any(|word| word == "auditor"));
	}

	#[test]
	fn nested_functions_do_not_leak_features_into_their_parent() {
		let text = "fn outer() { fn inner() { hidden_callee(); } visible_callee(); }";
		let symbols = extract_symbols("src/example.rs", 0, text);
		let outer = symbols
			.iter()
			.find(|symbol| symbol.name == "outer")
			.unwrap();
		assert_eq!(outer.call_site_count, 1);
		assert!(
			outer
				.span_identifiers
				.iter()
				.any(|identifier| identifier == "visible_callee")
		);
		assert!(
			!outer
				.span_identifiers
				.iter()
				.any(|identifier| identifier == "hidden_callee")
		);
	}

	#[test]
	fn measured_corpus_excludes_the_instrument_that_contains_pilot_words() {
		let root = Path::new(env!("CARGO_MANIFEST_DIR"))
			.parent()
			.and_then(Path::parent)
			.expect("workspace root");
		let files = tracked_rust_files(root).expect("tracked corpus");
		assert!(
			files
				.iter()
				.all(|path| !path.starts_with("crates/spectrometer"))
		);
	}
}
