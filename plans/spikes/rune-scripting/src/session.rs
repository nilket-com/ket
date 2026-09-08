//! Experimental AST-based input adapter. Its restrictions are intentional.
use super::{Result, call};
use rune::{
	Context, SourceId,
	ast::{self, Spanned},
	runtime::Value,
};
use std::collections::{BTreeMap, BTreeSet};

pub struct Session {
	declarations: BTreeMap<String, (bool, String)>,
	names: BTreeSet<String>,
	state: Value,
}
impl Session {
	pub fn new() -> Self {
		Self {
			declarations: BTreeMap::new(),
			names: BTreeSet::new(),
			state: serde_json::from_str("{}").unwrap(),
		}
	}
	pub fn eval(&mut self, context: &Context, input: &str) -> Result<Value> {
		if input.len() > 32 * 1024 {
			return Err("spike input cap is 32 KiB".into());
		}
		let wrapped = format!("{{{input}\n}}");
		let block: ast::Block = rune::parse::parse_all(&wrapped, SourceId::empty(), false)?;
		let slice = |span: rune::ast::Span| &wrapped[span.range()];
		let mut declarations = self.declarations.clone();
		let mut names = self.names.clone();
		let mut statements = String::new();
		let mut result = "()".to_owned();
		for (index, statement) in block.statements.iter().enumerate() {
			// ItemStruct's derived span omits its closing delimiter in 0.14.1.
			// Use parsed statement boundaries, never a textual brace scanner.
			let start = statement.span().range().start;
			let end = block
				.statements
				.get(index + 1)
				.map(|s| s.span().range().start)
				.unwrap_or(block.close.span().range().start);
			let statement_source = &wrapped[start..end];
			match statement {
				ast::Stmt::Item(item, _) => {
					let (name, is_type) = match item {
						ast::Item::Fn(f) => (slice(f.name.span()), false),
						ast::Item::Struct(s) => (slice(s.ident.span()), true),
						ast::Item::Enum(e) => (slice(e.name.span()), true),
						_ => return Err("spike persists only fn/struct/enum declarations".into()),
					};
					valid_name(name)?;
					let source = statement_source.trim().to_owned();
					if let Some((old_type, old)) = declarations.get(name) {
						if (*old_type || is_type) && old != &source {
							return Err("type redefinition requires :reset in this spike".into());
						}
					}
					declarations.insert(name.to_owned(), (is_type, source));
				}
				ast::Stmt::Local(local) => {
					bindings(&local.pat, &wrapped, &mut names)?;
					statements.push_str(statement_source);
					statements.push('\n');
				}
				ast::Stmt::Expr(expr) if index + 1 == block.statements.len() => {
					result = slice(expr.span()).to_owned()
				}
				ast::Stmt::Semi(_) | ast::Stmt::Expr(_) => {
					statements.push_str(statement_source);
					if matches!(statement, ast::Stmt::Expr(_)) {
						statements.push(';');
					}
					statements.push('\n');
				}
				_ => return Err("unsupported statement in spike".into()),
			}
		}
		let mut source = declarations
			.values()
			.map(|(_, s)| s.as_str())
			.collect::<Vec<_>>()
			.join("\n");
		source.push_str("\npub fn main(__spike_state) {\n");
		for name in &self.names {
			source.push_str(&format!("let {name} = __spike_state[\"{name}\"];\n"));
		}
		source.push_str(&statements);
		source.push_str(&format!("\nlet __spike_result = ({result});\n(#{{"));
		source.push_str(&names.iter().cloned().collect::<Vec<_>>().join(","));
		source.push_str("}, __spike_result)\n}");
		let output = call(context, &source, self.state.clone())?;
		let (state, result): (Value, Value) = rune::from_value(output)?;
		// Publish bindings/declarations only after successful evaluation. Values
		// are shallow shared handles: mutations on a failed input remain visible.
		self.declarations = declarations;
		self.names = names;
		self.state = state;
		Ok(result)
	}
}
fn valid_name(name: &str) -> Result<()> {
	if name == "main" || name.starts_with("__spike_") {
		return Err("reserved spike identifier".into());
	}
	Ok(())
}
fn bindings(pat: &ast::Pat, source: &str, names: &mut BTreeSet<String>) -> Result<()> {
	match pat {
		ast::Pat::Path(p)
			if p.path.global.is_none() && p.path.rest.is_empty() && p.path.trailing.is_none() =>
		{
			if let ast::PathSegment::Ident(ident) = &p.path.first {
				let name = &source[ident.span().range()];
				valid_name(name)?;
				names.insert(name.into());
			} else {
				return Err("unsupported binding path".into());
			}
		}
		ast::Pat::Tuple(p) => {
			for (pat, _) in p.items.iter() {
				bindings(pat, source, names)?;
			}
		}
		ast::Pat::Vec(p) => {
			for (pat, _) in p.items.iter() {
				bindings(pat, source, names)?;
			}
		}
		ast::Pat::Object(p) => {
			for (pat, _) in p.items.iter() {
				bindings(pat, source, names)?;
			}
		}
		ast::Pat::Binding(p) => bindings(&p.pat, source, names)?,
		ast::Pat::Ignore(_) | ast::Pat::Rest(_) | ast::Pat::Lit(_) => (),
		_ => return Err("unsupported binding pattern".into()),
	}
	Ok(())
}

pub fn checks(context: &Context) -> Result<()> {
	let mut session = Session::new();
	for (input, expected) in [
		(
			"let (x, y) = (2, 3); let shared = [1]; let effects = []; fn f() { 10 } struct Boxed { value } let old = f; let c = || f() + shared[0]; let b = Boxed { value: 7 }; x + y",
			"5",
		),
		(
			"effects.push(1); fn f() { 20 } (old(), f(), c(), b.value, b is Boxed)",
			"[10,20,11,7,true]",
		),
		(
			"let x = x + 5; let [left, right] = [8,9]; let #{a: renamed} = #{a: 4}; (x,y,left,right,renamed,effects.len())",
			"[7,3,8,9,4,1]",
		),
	] {
		let value = session.eval(context, input)?;
		let actual = super::display(&value);
		assert_eq!(actual, expected);
		println!("session: {actual}");
	}
	assert!(
		session
			.eval(context, "fn f() { 99 } let broken = ;")
			.is_err()
	);
	assert!(
		session
			.eval(
				context,
				"shared.push(2); let x = 99; let new_name = 1; panic!(\"partial\");"
			)
			.is_err()
	);
	assert!(session.eval(context, "new_name").is_err());
	let value = session.eval(context, "(x, shared, f(), effects.len())")?;
	assert_eq!(super::display(&value), "[7,[1,2],20,1]");
	println!("after compile/runtime failures: {}", super::display(&value));
	assert!(session.eval(context, "struct Boxed { other }").is_err());
	println!("changed type declaration: refused until reset");
	let error = session.eval(context, "while true {} ").unwrap_err();
	println!("infinite loop: {error}");
	assert_eq!(super::display(&session.eval(context, "x")?), "7");
	let effect_path = std::env::temp_dir().join(format!(
		"rune-spike-effect-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)?
			.as_nanos()
	));
	let quoted = serde_json::to_string(&effect_path.to_string_lossy())?;
	assert!(
		session
			.eval(
				context,
				&format!("host::write_new({quoted}, \"once\")?; panic!(\"after external write\");")
			)
			.is_err()
	);
	assert_eq!(std::fs::read_to_string(&effect_path)?, "once");
	assert_eq!(super::display(&session.eval(context, "x")?), "7");
	assert_eq!(std::fs::read_to_string(&effect_path)?, "once");
	std::fs::remove_file(effect_path)?;
	println!("external write before runtime failure survives; later input does not replay it");
	let mut reset = Session::new();
	assert!(reset.eval(context, "x").is_err());
	let start = std::time::Instant::now();
	for _ in 0..100 {
		session.eval(context, "let x = x + 1; x")?;
	}
	assert_eq!(super::display(&session.eval(context, "x")?), "107");
	println!(
		"100 incremental inputs (compile + execute): {:?}",
		start.elapsed()
	);
	Ok(())
}
