//! Origin instrumentation for local Rune sources.
//!
//! A local source (one declaring its own `main`) runs the presentation
//! vocabulary in its single rnx invocation, in ordinary control flow. The
//! shared presentation handlers expect the same injected origins the static
//! view compiler gives them, so this pass rewrites the source's presentation
//! calls before it is compiled, from its parsed AST rather than its tokens:
//!
//! - A free call `name(...)` whose callee is a bare, unqualified identifier
//!   naming a presentation constructor becomes
//!   `polariton_presentation::__name(..., start, end)`, with the per-argument
//!   origins the static rewrite gives marks. Qualified calls are never touched.
//! - A method call `recv.name(...)` whose name is a presentation method becomes
//!   a block that evaluates the receiver once and dispatches on its type:
//!   presentation values take `__name(..., start, end)`, and everything else
//!   (the legacy `viz.plot(...)` chain, battery and user types) takes the
//!   original call unchanged. Only one branch's arguments are evaluated.
//!
//! A bare call is the constructor only when no binding of that name is in
//! lexical scope: the walk resolves names as Rune does (items and imports are
//! visible throughout their block or file; `let` from the next statement on;
//! parameters, `for`, `if let`/`while let`, match and select patterns within
//! their bodies; a nested `fn` does not see enclosing locals; a `mod` sees
//! nothing outside it). A call a user binding shadows is left alone. Where a
//! glob import is visible it could supply the name, so the call is refused.
//! Identifiers with the internal `__pz` prefix are reserved. Syntax the walk
//! does not know is refused rather than skipped. Macro invocations (including
//! template strings) are not descended into; a presentation call inside one
//! stays bare and fails to compile, since bare names are not registered.

use crate::presentation_registry::{CALLABLES, CallPosition};
use rune::ast::{self, Spanned};
use std::collections::HashSet;
use std::ops::Range;

/// The internal identifier prefix this pass reserves.
const RESERVED_PREFIX: &str = "__pz";
const MODULE: &str = "polariton_presentation";

/// The presentation names a local source may call.
pub(crate) struct Vocabulary {
	free: HashSet<&'static str>,
	methods: HashSet<&'static str>,
}

impl Vocabulary {
	/// From the presentation registry, plus `extra_free` constructors that
	/// only local sources have (the runtime `view`).
	pub(crate) fn from_registry(extra_free: &[&'static str]) -> Self {
		let mut free = HashSet::new();
		let mut methods = HashSet::new();
		for spec in CALLABLES {
			match spec.position {
				CallPosition::Free => free.insert(spec.name),
				CallPosition::Method => methods.insert(spec.name),
			};
		}
		free.extend(extra_free.iter().copied());
		Self { free, methods }
	}

	pub(crate) fn is_free(&self, name: &str) -> bool {
		self.free.contains(name)
	}

	fn is_method(&self, name: &str) -> bool {
		self.methods.contains(name)
	}
}

/// Why a source cannot be instrumented, at its original buffer span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InstrumentError {
	pub(crate) span: Range<usize>,
	pub(crate) message: String,
}

#[derive(Clone, Debug)]
struct Segment {
	generated: Range<usize>,
	original: usize,
	copied: bool,
}

/// The rewritten source and how its text maps back to the buffer.
#[derive(Clone, Debug)]
pub(crate) struct Instrumented {
	pub(crate) source: String,
	segments: Vec<Segment>,
	#[cfg(test)]
	rewrote: bool,
}

impl Instrumented {
	/// The original buffer offset of a byte offset in `source`: copied text
	/// maps one to one, synthetic text to the start of the call it belongs to.
	pub(crate) fn original_offset(&self, generated: usize) -> usize {
		let segment = self
			.segments
			.iter()
			.find(|segment| segment.generated.contains(&generated))
			.or_else(|| self.segments.last());
		match segment {
			Some(segment) if segment.copied => {
				segment.original + generated.saturating_sub(segment.generated.start)
			}
			Some(segment) => segment.original,
			None => generated,
		}
	}

	/// Whether any presentation call was rewritten.
	#[cfg(test)]
	pub(crate) fn rewrote(&self) -> bool {
		self.rewrote
	}
}

#[derive(Clone, Debug)]
enum SiteKind {
	Free {
		name: String,
		callee: Range<usize>,
	},
	Method {
		name: String,
		receiver: Range<usize>,
		dot_start: usize,
	},
}

#[derive(Clone, Debug)]
struct Site {
	kind: SiteKind,
	/// The whole call, `callee(...)` or `receiver.name(...)`.
	span: Range<usize>,
	/// Between the parentheses, exclusive of both.
	inner: Range<usize>,
	arguments: Vec<Range<usize>>,
	/// Where injected origins go: after the trailing comma, else after the
	/// last argument, else right after `(`. Comments after it stay after it.
	insert: usize,
	/// What precedes the injected origins at `insert`.
	separator: &'static str,
}

/// Rewrite `source`'s presentation calls. See the module documentation.
pub(crate) fn instrument(
	source: &str,
	vocabulary: &Vocabulary,
) -> Result<Instrumented, InstrumentError> {
	reject_reserved_identifiers(source)?;
	let file = rune::parse::parse_all::<ast::File>(source, rune::SourceId::empty(), false)
		.map_err(|error| InstrumentError {
			span: error.span().range(),
			message: format!("the source does not parse: {error}"),
		})?;
	let mut walk = Walk {
		source,
		vocabulary,
		sites: Vec::new(),
		scopes: vec![Scope::default()],
	};
	walk.hoist(file.items.iter().map(|(item, _)| item))?;
	for (item, _) in &file.items {
		walk.item(item)?;
	}
	walk.sites.sort_by(|left, right| {
		left.span
			.start
			.cmp(&right.span.start)
			.then_with(|| right.span.end.cmp(&left.span.end))
	});
	let mut render = Render {
		source,
		sites: &walk.sites,
		out: String::with_capacity(source.len() + source.len() / 2),
		segments: Vec::new(),
		counter: 0,
	};
	render.range(0..source.len());
	Ok(Instrumented {
		#[cfg(test)]
		rewrote: !walk.sites.is_empty(),
		source: render.out,
		segments: render.segments,
	})
}

/// Refuse any identifier with the reserved prefix, outside string literals
/// and comments. Template strings are scanned whole, conservatively.
fn reject_reserved_identifiers(source: &str) -> Result<(), InstrumentError> {
	let bytes = source.as_bytes();
	let mut index = 0;
	while index < bytes.len() {
		let byte = bytes[index];
		if byte == b'/' && bytes.get(index + 1) == Some(&b'/') {
			while index < bytes.len() && bytes[index] != b'\n' {
				index += 1;
			}
		} else if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
			index += 2;
			while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
				index += 1;
			}
			index += 2;
		} else if byte == b'"' {
			index += 1;
			while index < bytes.len() && bytes[index] != b'"' {
				index += if bytes[index] == b'\\' { 2 } else { 1 };
			}
			index += 1;
		} else if byte == b'_' || byte.is_ascii_alphabetic() {
			let start = index;
			while index < bytes.len()
				&& (bytes[index] == b'_' || bytes[index].is_ascii_alphanumeric())
			{
				index += 1;
			}
			let identifier = &source[start..index];
			if identifier.starts_with(RESERVED_PREFIX) {
				return Err(InstrumentError {
					span: start..index,
					message: format!(
						"`{identifier}` uses the `{RESERVED_PREFIX}` prefix, which Polariton reserves for its presentation instrumentation"
					),
				});
			}
		} else {
			index += 1;
		}
	}
	Ok(())
}

/// One lexical scope. `items` are hoisted item and import names, visible
/// throughout the scope; `locals` are pattern and parameter bindings, visible
/// from their binding on. A `Function` scope hides enclosing locals (a nested
/// `fn` item cannot capture them); a `Module` scope hides everything outside.
#[derive(Default)]
struct Scope {
	items: HashSet<String>,
	locals: HashSet<String>,
	globs: Vec<Range<usize>>,
	boundary: Boundary,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Boundary {
	#[default]
	None,
	Function,
	Module,
}

/// What a bare call to a vocabulary name resolves to.
enum Resolution {
	/// A visible user binding: the call is the user's own.
	Shadowed,
	/// A glob import is visible and could supply the name.
	Glob(Range<usize>),
	/// Nothing shadows it: the presentation constructor.
	Vocabulary,
}

struct Walk<'a> {
	source: &'a str,
	vocabulary: &'a Vocabulary,
	sites: Vec<Site>,
	scopes: Vec<Scope>,
}

fn unsupported(span: Range<usize>, what: &str) -> InstrumentError {
	InstrumentError {
		span,
		message: format!(
			"{what} is not supported in a local source that Polariton instruments; this Rune syntax is not recognised"
		),
	}
}

impl Walk<'_> {
	fn text(&self, span: Range<usize>) -> &str {
		self.source.get(span).unwrap_or("")
	}

	fn ident(&self, ident: &ast::Ident) -> String {
		self.text(ident.span.range()).to_owned()
	}

	fn push(&mut self, boundary: Boundary) {
		self.scopes.push(Scope {
			boundary,
			..Scope::default()
		});
	}

	fn pop(&mut self) {
		self.scopes.pop();
	}

	fn declare_local(&mut self, name: String) {
		if let Some(scope) = self.scopes.last_mut() {
			scope.locals.insert(name);
		}
	}

	fn declare_item(&mut self, name: String) {
		if let Some(scope) = self.scopes.last_mut() {
			scope.items.insert(name);
		}
	}

	fn resolve(&self, name: &str) -> Resolution {
		let mut locals_visible = true;
		for scope in self.scopes.iter().rev() {
			if scope.items.contains(name) || (locals_visible && scope.locals.contains(name)) {
				return Resolution::Shadowed;
			}
			if let Some(glob) = scope.globs.first() {
				return Resolution::Glob(glob.clone());
			}
			match scope.boundary {
				Boundary::None => {}
				Boundary::Function => locals_visible = false,
				Boundary::Module => break,
			}
		}
		Resolution::Vocabulary
	}

	fn single_ident(path: &ast::Path) -> Option<&ast::Ident> {
		match (
			&path.global,
			path.rest.is_empty(),
			&path.trailing,
			&path.first,
		) {
			(None, true, None, ast::PathSegment::Ident(ident)) => Some(ident),
			_ => None,
		}
	}

	/// Declare the names `items` introduce into the current scope before any
	/// of them is walked: items are visible throughout their block or file.
	fn hoist<'i>(
		&mut self,
		items: impl IntoIterator<Item = &'i ast::Item>,
	) -> Result<(), InstrumentError> {
		for item in items {
			match item {
				ast::Item::Use(item) => self.hoist_use(&item.path)?,
				ast::Item::Fn(item) => self.declare_item(self.ident(&item.name)),
				ast::Item::Enum(item) => self.declare_item(self.ident(&item.name)),
				ast::Item::Struct(item) => self.declare_item(self.ident(&item.ident)),
				ast::Item::Mod(item) => self.declare_item(self.ident(&item.name)),
				ast::Item::Const(item) => self.declare_item(self.ident(&item.name)),
				ast::Item::Impl(_) | ast::Item::MacroCall(_) => {}
				other => return Err(unsupported(other.span().range(), "this item")),
			}
		}
		Ok(())
	}

	fn hoist_use(&mut self, path: &ast::ItemUsePath) -> Result<(), InstrumentError> {
		let last = path
			.segments
			.last()
			.map(|(_, segment)| segment)
			.unwrap_or(&path.first);
		for segment in std::iter::once(&path.first).chain(path.segments.iter().map(|(_, s)| s)) {
			if std::ptr::eq(segment, last) {
				continue;
			}
			match segment {
				ast::ItemUseSegment::PathSegment(_) => {}
				ast::ItemUseSegment::Wildcard(_) | ast::ItemUseSegment::Group(_) => {
					return Err(unsupported(
						segment.span().range(),
						"a glob or group before the end of an import",
					));
				}
				other => return Err(unsupported(other.span().range(), "this import segment")),
			}
		}
		if let Some((_, alias)) = &path.alias {
			self.declare_item(self.ident(alias));
			return Ok(());
		}
		match last {
			ast::ItemUseSegment::PathSegment(ast::PathSegment::Ident(ident)) => {
				self.declare_item(self.ident(ident));
				Ok(())
			}
			ast::ItemUseSegment::PathSegment(_) => Ok(()),
			ast::ItemUseSegment::Wildcard(token) => {
				if let Some(scope) = self.scopes.last_mut() {
					scope.globs.push(token.span().range());
				}
				Ok(())
			}
			ast::ItemUseSegment::Group(group) => {
				for (nested, _) in group.iter() {
					self.hoist_use(nested)?;
				}
				Ok(())
			}
			other => Err(unsupported(other.span().range(), "this import segment")),
		}
	}

	/// The names `pattern` binds, after walking any expressions inside it in
	/// the current scope. The caller declares them where they are visible.
	fn pattern(
		&mut self,
		pattern: &ast::Pat,
		names: &mut Vec<String>,
	) -> Result<(), InstrumentError> {
		match pattern {
			ast::Pat::Ignore(_) | ast::Pat::Rest(_) => Ok(()),
			ast::Pat::Path(pattern) => match Self::single_ident(&pattern.path) {
				Some(ident) => {
					names.push(self.ident(ident));
					Ok(())
				}
				None => self.path(&pattern.path),
			},
			ast::Pat::Lit(pattern) => self.expr(&pattern.expr),
			ast::Pat::Vec(pattern) => {
				for (item, _) in pattern.items.iter() {
					self.pattern(item, names)?;
				}
				Ok(())
			}
			ast::Pat::Tuple(pattern) => {
				if let Some(path) = &pattern.path {
					self.path(path)?;
				}
				for (item, _) in pattern.items.iter() {
					self.pattern(item, names)?;
				}
				Ok(())
			}
			ast::Pat::Object(pattern) => {
				if let ast::ObjectIdent::Named(path) = &pattern.ident {
					self.path(path)?;
				}
				for (item, _) in pattern.items.iter() {
					self.pattern(item, names)?;
				}
				Ok(())
			}
			// `key: pattern` binds inside `pattern`; the key is a field name.
			ast::Pat::Binding(pattern) => self.pattern(&pattern.pat, names),
			other => Err(unsupported(other.span().range(), "this pattern")),
		}
	}

	/// Walk `pattern` and declare its bindings as locals in the current scope.
	fn bind(&mut self, pattern: &ast::Pat) -> Result<(), InstrumentError> {
		let mut names = Vec::new();
		self.pattern(pattern, &mut names)?;
		for name in names {
			self.declare_local(name);
		}
		Ok(())
	}

	fn fn_arg(&mut self, argument: &ast::FnArg) -> Result<(), InstrumentError> {
		match argument {
			ast::FnArg::SelfValue(_) => Ok(()),
			ast::FnArg::Pat(pattern) => self.bind(pattern),
			other => Err(unsupported(other.span().range(), "this parameter")),
		}
	}

	fn item(&mut self, item: &ast::Item) -> Result<(), InstrumentError> {
		match item {
			// Declared by `hoist`; nothing to walk.
			ast::Item::Use(_) | ast::Item::Enum(_) | ast::Item::Struct(_) => Ok(()),
			ast::Item::Fn(item) => self.item_fn(item),
			ast::Item::Impl(item) => {
				self.path(&item.path)?;
				// Impl functions are methods, not free bindings.
				for function in &item.functions {
					self.item_fn_body(function)?;
				}
				Ok(())
			}
			ast::Item::Mod(item) => match &item.body {
				ast::ItemModBody::EmptyBody(_) => Ok(()),
				ast::ItemModBody::InlineBody(body) => {
					self.push(Boundary::Module);
					let result = self
						.hoist(body.file.items.iter().map(|(item, _)| item))
						.and_then(|()| {
							for (item, _) in &body.file.items {
								self.item(item)?;
							}
							Ok(())
						});
					self.pop();
					result
				}
				other => Err(unsupported(other.span().range(), "this module body")),
			},
			ast::Item::Const(item) => {
				self.push(Boundary::Function);
				let result = self.expr(&item.expr);
				self.pop();
				result
			}
			// Macro bodies are opaque token streams; see the module docs.
			ast::Item::MacroCall(_) => Ok(()),
			other => Err(unsupported(other.span().range(), "this item")),
		}
	}

	fn item_fn(&mut self, item: &ast::ItemFn) -> Result<(), InstrumentError> {
		self.item_fn_body(item)
	}

	fn item_fn_body(&mut self, item: &ast::ItemFn) -> Result<(), InstrumentError> {
		self.push(Boundary::Function);
		let result = (|| {
			for (argument, _) in item.args.iter() {
				self.fn_arg(argument)?;
			}
			self.block(&item.body)
		})();
		self.pop();
		result
	}

	fn path(&mut self, path: &ast::Path) -> Result<(), InstrumentError> {
		for segment in std::iter::once(&path.first).chain(path.rest.iter().map(|(_, s)| s)) {
			match segment {
				ast::PathSegment::Generics(generics) => {
					for (argument, _) in generics.iter() {
						self.expr(&argument.expr)?;
					}
				}
				ast::PathSegment::SelfType(_)
				| ast::PathSegment::SelfValue(_)
				| ast::PathSegment::Ident(_)
				| ast::PathSegment::Crate(_)
				| ast::PathSegment::Super(_) => {}
				other => return Err(unsupported(other.span().range(), "this path segment")),
			}
		}
		Ok(())
	}

	fn block(&mut self, block: &ast::Block) -> Result<(), InstrumentError> {
		self.push(Boundary::None);
		let result = (|| {
			self.hoist(
				block
					.statements
					.iter()
					.filter_map(|statement| match statement {
						ast::Stmt::Item(item, _) => Some(item),
						_ => None,
					}),
			)?;
			for statement in &block.statements {
				self.stmt(statement)?;
			}
			Ok(())
		})();
		self.pop();
		result
	}

	fn stmt(&mut self, statement: &ast::Stmt) -> Result<(), InstrumentError> {
		match statement {
			// The initializer cannot see the binding it introduces.
			ast::Stmt::Local(local) => {
				self.expr(&local.expr)?;
				self.bind(&local.pat)
			}
			ast::Stmt::Item(item, _) => self.item(item),
			ast::Stmt::Expr(expr) => self.expr(expr),
			ast::Stmt::Semi(semi) => self.expr(&semi.expr),
			other => Err(unsupported(other.span().range(), "this statement")),
		}
	}

	/// Walk `condition`, then `body` with any `let` binding of the condition
	/// in scope.
	fn conditional(
		&mut self,
		condition: &ast::Condition,
		body: &ast::Block,
	) -> Result<(), InstrumentError> {
		self.push(Boundary::None);
		let result = (|| {
			match condition {
				ast::Condition::Expr(expr) => self.expr(expr)?,
				ast::Condition::ExprLet(expr) => {
					self.expr(&expr.expr)?;
					self.bind(&expr.pat)?;
				}
				other => return Err(unsupported(other.span().range(), "this condition")),
			}
			self.block(body)
		})();
		self.pop();
		result
	}

	fn call(&mut self, call: &ast::ExprCall) -> Result<(), InstrumentError> {
		let open = call.args.open.span().range();
		let close = call.args.close.span().range();
		let arguments = call
			.args
			.iter()
			.map(|(argument, _)| argument.span().range())
			.collect::<Vec<_>>();
		let (insert, separator) = match call.args.last() {
			Some((_, Some(comma))) => (comma.span().range().end, " "),
			Some((argument, None)) => (argument.span().range().end, ", "),
			None => (open.end, ""),
		};
		match &*call.expr {
			ast::Expr::Path(path) => {
				if let Some(ident) = Self::single_ident(path) {
					let callee = ident.span.range();
					let name = self.text(callee.clone()).to_owned();
					if self.vocabulary.is_free(&name) {
						match self.resolve(&name) {
							Resolution::Shadowed => {}
							Resolution::Glob(glob) => {
								return Err(InstrumentError {
									span: callee,
									message: format!(
										"`{name}(...)` is a presentation constructor, but the glob import at bytes {}..{} could supply a different `{name}`; import names explicitly",
										glob.start, glob.end
									),
								});
							}
							Resolution::Vocabulary => self.sites.push(Site {
								span: callee.start..close.end,
								inner: open.end..close.start,
								arguments,
								insert,
								separator,
								kind: SiteKind::Free { name, callee },
							}),
						}
					}
				}
				self.path(path)?;
			}
			ast::Expr::FieldAccess(access) => {
				if let ast::ExprField::Path(field) = &access.expr_field
					&& let Some(ident) = Self::single_ident(field)
				{
					let name = self.text(ident.span.range()).to_owned();
					if self.vocabulary.is_method(&name) {
						let receiver = access.expr.span().range();
						self.sites.push(Site {
							span: receiver.start..close.end,
							inner: open.end..close.start,
							arguments,
							insert,
							separator,
							kind: SiteKind::Method {
								name,
								receiver,
								dot_start: access.dot.span().range().start,
							},
						});
					}
				}
				self.expr(&access.expr)?;
			}
			other => self.expr(other)?,
		}
		for (argument, _) in call.args.iter() {
			self.expr(argument)?;
		}
		Ok(())
	}

	fn scoped(
		&mut self,
		f: impl FnOnce(&mut Self) -> Result<(), InstrumentError>,
	) -> Result<(), InstrumentError> {
		self.push(Boundary::None);
		let result = f(self);
		self.pop();
		result
	}

	fn expr(&mut self, expr: &ast::Expr) -> Result<(), InstrumentError> {
		match expr {
			ast::Expr::Path(path) => self.path(path),
			ast::Expr::Assign(expr) => {
				self.expr(&expr.lhs)?;
				self.expr(&expr.rhs)
			}
			ast::Expr::While(expr) => self.conditional(&expr.condition, &expr.body),
			ast::Expr::Loop(expr) => self.block(&expr.body),
			ast::Expr::For(expr) => {
				self.expr(&expr.iter)?;
				self.scoped(|walk| {
					walk.bind(&expr.binding)?;
					walk.block(&expr.body)
				})
			}
			// A `let` outside a condition binds for the rest of its block.
			ast::Expr::Let(expr) => {
				self.expr(&expr.expr)?;
				self.bind(&expr.pat)
			}
			ast::Expr::If(expr) => {
				self.conditional(&expr.condition, &expr.block)?;
				for branch in &expr.expr_else_ifs {
					self.conditional(&branch.condition, &branch.block)?;
				}
				if let Some(branch) = &expr.expr_else {
					self.block(&branch.block)?;
				}
				Ok(())
			}
			ast::Expr::Match(expr) => {
				self.expr(&expr.expr)?;
				for (branch, _) in &expr.branches {
					self.scoped(|walk| {
						walk.bind(&branch.pat)?;
						if let Some((_, guard)) = &branch.condition {
							walk.expr(guard)?;
						}
						walk.expr(&branch.body)
					})?;
				}
				Ok(())
			}
			ast::Expr::Call(call) => self.call(call),
			ast::Expr::FieldAccess(expr) => self.expr(&expr.expr),
			ast::Expr::Binary(expr) => {
				self.expr(&expr.lhs)?;
				self.expr(&expr.rhs)
			}
			ast::Expr::Unary(expr) => self.expr(&expr.expr),
			ast::Expr::Index(expr) => {
				self.expr(&expr.target)?;
				self.expr(&expr.index)
			}
			ast::Expr::Break(expr) => match &expr.expr {
				Some(expr) => self.expr(expr),
				None => Ok(()),
			},
			ast::Expr::Continue(_) | ast::Expr::Lit(_) => Ok(()),
			ast::Expr::Yield(expr) => match &expr.expr {
				Some(expr) => self.expr(expr),
				None => Ok(()),
			},
			ast::Expr::Block(expr) => self.block(&expr.block),
			ast::Expr::Return(expr) => match &expr.expr {
				Some(expr) => self.expr(expr),
				None => Ok(()),
			},
			ast::Expr::Await(expr) => self.expr(&expr.expr),
			ast::Expr::Try(expr) => self.expr(&expr.expr),
			ast::Expr::Select(expr) => {
				for (branch, _) in &expr.branches {
					match branch {
						ast::ExprSelectBranch::Pat(branch) => {
							self.expr(&branch.expr)?;
							self.scoped(|walk| {
								walk.bind(&branch.pat)?;
								walk.expr(&branch.body)
							})?;
						}
						ast::ExprSelectBranch::Default(branch) => self.expr(&branch.body)?,
						other => {
							return Err(unsupported(other.span().range(), "this select branch"));
						}
					}
				}
				Ok(())
			}
			ast::Expr::Closure(expr) => self.scoped(|walk| {
				match &expr.args {
					ast::ExprClosureArgs::Empty { .. } => {}
					ast::ExprClosureArgs::List { args, .. } => {
						for (argument, _) in args {
							walk.fn_arg(argument)?;
						}
					}
					other => {
						return Err(unsupported(
							other.span().range(),
							"these closure parameters",
						));
					}
				}
				walk.expr(&expr.body)
			}),
			ast::Expr::Object(expr) => {
				if let ast::ObjectIdent::Named(path) = &expr.ident {
					self.path(path)?;
				}
				// Keys (including shorthand `#{name}`, a read of `name`) are
				// field names, never bindings.
				for (assignment, _) in expr.assignments.iter() {
					if let Some((_, value)) = &assignment.assign {
						self.expr(value)?;
					}
				}
				Ok(())
			}
			ast::Expr::Tuple(expr) => {
				for (item, _) in expr.items.iter() {
					self.expr(item)?;
				}
				Ok(())
			}
			ast::Expr::Vec(expr) => {
				for (item, _) in expr.items.iter() {
					self.expr(item)?;
				}
				Ok(())
			}
			ast::Expr::Range(expr) => {
				if let Some(start) = &expr.start {
					self.expr(start)?;
				}
				if let Some(end) = &expr.end {
					self.expr(end)?;
				}
				Ok(())
			}
			ast::Expr::Empty(expr) => self.expr(&expr.expr),
			ast::Expr::Group(expr) => self.expr(&expr.expr),
			// Macro bodies are opaque token streams; see the module docs.
			ast::Expr::MacroCall(_) => Ok(()),
			other => Err(unsupported(other.span().range(), "this expression")),
		}
	}
}

struct Render<'a> {
	source: &'a str,
	sites: &'a [Site],
	out: String,
	segments: Vec<Segment>,
	counter: usize,
}

impl Render<'_> {
	fn copied(&mut self, range: Range<usize>) {
		if range.is_empty() {
			return;
		}
		let start = self.out.len();
		self.out.push_str(&self.source[range.clone()]);
		self.segments.push(Segment {
			generated: start..self.out.len(),
			original: range.start,
			copied: true,
		});
	}

	fn synthetic(&mut self, text: &str, owner: usize) {
		if text.is_empty() {
			return;
		}
		let start = self.out.len();
		self.out.push_str(text);
		self.segments.push(Segment {
			generated: start..self.out.len(),
			original: owner,
			copied: false,
		});
	}

	/// Copy `range`, rendering every outermost site inside it.
	fn range(&mut self, range: Range<usize>) {
		let mut cursor = range.start;
		let sites = self.sites;
		for site in sites {
			if site.span.start < cursor || site.span.end > range.end {
				continue;
			}
			self.copied(cursor..site.span.start);
			self.site(site);
			cursor = site.span.end;
		}
		self.copied(cursor..range.end);
	}

	/// The authored arguments with `origins` injected at the site's insertion
	/// point, ahead of any comments that follow it.
	fn arguments_with(&mut self, site: &Site, origins: &[usize], owner: usize) {
		self.range(site.inner.start..site.insert);
		let origins = origins
			.iter()
			.map(usize::to_string)
			.collect::<Vec<_>>()
			.join(", ");
		self.synthetic(&format!("{}{origins}", site.separator), owner);
		self.range(site.insert..site.inner.end);
	}

	fn site(&mut self, site: &Site) {
		let owner = site.span.start;
		match &site.kind {
			SiteKind::Free { name, callee } => {
				self.synthetic(&format!("{MODULE}::__{name}"), callee.start);
				self.copied(callee.end..site.inner.start);
				let mut origins =
					crate::presentation_registry::argument_origins(name).for_call(&site.arguments);
				origins.extend([site.span.start, site.span.end]);
				self.arguments_with(site, &origins, owner);
				self.copied(site.inner.end..site.span.end);
			}
			SiteKind::Method {
				name,
				receiver,
				dot_start,
			} => {
				let binding = format!("{RESERVED_PREFIX}_r{}", self.counter);
				self.counter += 1;
				self.synthetic(&format!("{{ let {binding} = "), owner);
				self.range(receiver.clone());
				self.synthetic(
					&format!("; if {MODULE}::__is_presentation({binding}) {{ {binding}.__{name}("),
					owner,
				);
				self.arguments_with(site, &[*dot_start, site.span.end], owner);
				self.synthetic(&format!(") }} else {{ {binding}.{name}("), owner);
				// The ordinary branch is the authored argument text, unchanged.
				self.range(site.inner.clone());
				self.synthetic(") } }", owner);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn vocabulary() -> Vocabulary {
		Vocabulary::from_registry(&["view"])
	}

	fn rewrite(source: &str) -> Instrumented {
		let instrumented = instrument(source, &vocabulary()).expect("instruments");
		rune::parse::parse_all::<ast::File>(&instrumented.source, rune::SourceId::empty(), false)
			.unwrap_or_else(|error| {
				panic!("generated Rune parses: {error}\n{}", instrumented.source)
			});
		instrumented
	}

	fn refused(source: &str) -> InstrumentError {
		instrument(source, &vocabulary()).expect_err("refused")
	}

	fn main(body: &str) -> String {
		format!("pub fn main(workspace, viz) {{\n{body}\n}}\n")
	}

	#[test]
	fn a_free_call_carries_its_original_origin() {
		let source = main(r#"let t = title("Yields");"#);
		let start = source.find("title").unwrap();
		let end = source.find(");").unwrap() + 1;
		let out = rewrite(&source);
		assert!(
			out.source.contains(&format!(
				r#"polariton_presentation::__title("Yields", {start}, {end})"#
			)),
			"{}",
			out.source
		);
		assert!(out.rewrote());
	}

	#[test]
	fn a_call_without_arguments_has_no_leading_comma() {
		let source = main("let a = x_axis();");
		let start = source.find("x_axis").unwrap();
		let out = rewrite(&source);
		assert!(
			out.source.contains(&format!(
				"polariton_presentation::__x_axis({start}, {})",
				start + "x_axis()".len()
			)),
			"{}",
			out.source
		);
	}

	#[test]
	fn marks_carry_per_argument_origins_where_the_static_rewrite_does() {
		let source = main(r#"let p = pie("k", "v"); let l = line("x", "y");"#);
		let pie = source.find("pie").unwrap();
		let k = source.find(r#""k""#).unwrap();
		let v = source.find(r#""v""#).unwrap();
		let out = rewrite(&source);
		let expected = format!(
			r#"polariton_presentation::__pie("k", "v", {k}, {}, {v}, {}, {pie}, {})"#,
			k + 3,
			v + 3,
			pie + r#"pie("k", "v")"#.len()
		);
		assert!(out.source.contains(&expected), "{}", out.source);
		// Line has no per-argument origin table.
		let line = source.find("line").unwrap();
		assert!(
			out.source.contains(&format!(
				r#"polariton_presentation::__line("x", "y", {line}, {})"#,
				line + r#"line("x", "y")"#.len()
			)),
			"{}",
			out.source
		);
	}

	#[test]
	fn a_method_becomes_a_type_guarded_block() {
		let source = main(r#"let a = x_axis().label("Date");"#);
		let dot = source.find(".label").unwrap();
		let end = source.find(r#""Date")"#).unwrap() + r#""Date")"#.len();
		let out = rewrite(&source);
		assert!(
			out.source.contains(&format!(
				r#"if polariton_presentation::__is_presentation(__pz_r0) {{ __pz_r0.__label("Date", {dot}, {end}) }} else {{ __pz_r0.label("Date") }}"#
			)),
			"{}",
			out.source
		);
		assert!(
			out.source
				.contains("{ let __pz_r0 = polariton_presentation::__x_axis(")
		);
	}

	#[test]
	fn nested_chains_render_inner_rewrites_in_both_branches() {
		let source = main(r#"let a = x_axis().label(title("t")).tick_every("3 months");"#);
		let out = rewrite(&source);
		// Outer method wraps inner method; the inner argument's free call is
		// rewritten in both of the inner method's branches.
		assert_eq!(
			out.source.matches("__pz_r").count() >= 8,
			true,
			"{}",
			out.source
		);
		assert_eq!(
			out.source
				.matches("polariton_presentation::__title(")
				.count(),
			2,
			"{}",
			out.source
		);
		assert!(out.source.contains(".__tick_every(\"3 months\""));
		assert!(out.source.contains(".tick_every(\"3 months\")"));
	}

	#[test]
	fn the_legacy_plot_chain_keeps_its_ordinary_calls() {
		let source = main(r#"viz.plot(c).scatter("x", "y").line().show()?;"#);
		let out = rewrite(&source);
		// scatter/line/show are not presentation methods: untouched.
		assert!(
			out.source
				.contains(r#"viz.plot(c).scatter("x", "y").line().show()?"#),
			"{}",
			out.source
		);
		assert!(!out.rewrote());
		// A presentation method name on any receiver takes the guarded form,
		// whose else branch is the original call text.
		let source = main(r#"viz.plot(c).at(1, 2);"#);
		let out = rewrite(&source);
		assert!(
			out.source.contains("else { __pz_r0.at(1, 2) }"),
			"{}",
			out.source
		);
		assert!(
			out.source.contains("let __pz_r0 = viz.plot(c);"),
			"{}",
			out.source
		);
	}

	#[test]
	fn a_trailing_comma_gets_no_second_comma() {
		let source = main(r#"let t = title("a",);"#);
		let out = rewrite(&source);
		assert!(out.source.contains(r#"__title("a", "#), "{}", out.source);
		assert!(!out.source.contains(",,"), "{}", out.source);
	}

	#[test]
	fn origins_go_before_comments_after_a_trailing_comma() {
		for (body, argument_text) in [
			(
				r#"let t = title("Yields", /* why */);"#,
				r#""Yields", /* why */"#,
			),
			(
				"let t = title(\"Yields\", // why\n);",
				"\"Yields\", // why\n",
			),
			(
				r#"let t = title("Yields" /* why */);"#,
				r#""Yields" /* why */"#,
			),
		] {
			let source = main(body);
			let start = source.find("title").unwrap();
			let end = source.find(");").unwrap() + 1;
			let out = rewrite(&source);
			let trailing = argument_text.contains(',');
			let expected = if trailing {
				format!(
					r#"__title("Yields", {start}, {end}{})"#,
					&argument_text[r#""Yields","#.len()..]
				)
			} else {
				format!(
					r#"__title("Yields", {start}, {end}{})"#,
					&argument_text[r#""Yields""#.len()..]
				)
			};
			assert!(out.source.contains(&expected), "{body}\n{}", out.source);
			assert!(
				!out.source.contains(",,") && !out.source.contains(", ,"),
				"{}",
				out.source
			);
		}
	}

	#[test]
	fn an_empty_call_with_a_comment_gets_origins_after_its_paren() {
		let source = main("let a = x_axis(/* c */);");
		let start = source.find("x_axis").unwrap();
		let end = source.find(");").unwrap() + 1;
		let out = rewrite(&source);
		assert!(
			out.source
				.contains(&format!("__x_axis({start}, {end}/* c */)")),
			"{}",
			out.source
		);
	}

	#[test]
	fn a_method_keeps_its_ordinary_branch_byte_identical() {
		for arguments in [
			r#""Date", /* why */"#,
			"\"Date\", // why\n",
			"/* nothing */",
			r#""Date""#,
		] {
			let source = main(&format!("let a = x_axis().label({arguments});"));
			let out = rewrite(&source);
			assert!(
				out.source
					.contains(&format!("else {{ __pz_r0.label({arguments}) }}")),
				"{arguments:?}\n{}",
				out.source
			);
			let presentation = out.source.split("__pz_r0.__label(").nth(1).unwrap();
			assert!(!presentation.starts_with(','), "{}", out.source);
			assert!(
				!out.source.contains(",,") && !out.source.contains(", ,"),
				"{}",
				out.source
			);
		}
	}

	#[test]
	fn qualified_calls_are_never_rewritten() {
		let source = main(r#"let t = mymod::title("x"); let u = ::title("y");"#);
		let out = rewrite(&source);
		assert_eq!(out.source, source);
	}

	#[test]
	fn the_internal_prefix_is_reserved() {
		let error = refused(&main("let __pz_r0 = 1; let a = x_axis().label(__pz_r0);"));
		assert!(error.message.contains("__pz_r0"), "{}", error.message);
		assert!(
			refused(&main("let a = `${__pz}`;"))
				.message
				.contains("__pz")
		);
		// In a string literal or comment it is text, not an identifier.
		rewrite(&main(r#"let a = "__pz"; // __pz"#));
	}

	/// How many bare `line(...)` calls became the presentation constructor.
	fn rewritten_calls(source: &str) -> usize {
		rewrite(source)
			.source
			.matches("polariton_presentation::__line(")
			.count()
	}

	const CALL: &str = r#"line("u", "v")"#;

	#[test]
	fn every_binding_form_shadows_within_its_scope() {
		let cases = [
			format!("let line = 1; {CALL};"),
			format!("let [a, line] = [1, 2]; {CALL};"),
			format!("let (a, line) = (1, 2); {CALL};"),
			format!("let #{{line}} = #{{line: 1}}; {CALL};"),
			format!("let #{{name: line}} = #{{name: 1}}; {CALL};"),
			format!("for line in [1] {{ {CALL}; }}"),
			format!("match 1 {{ line => {CALL} }}"),
			format!("match 1 {{ line if {CALL} => 1 }}"),
			format!("if let line = 1 {{ {CALL}; }}"),
			format!("while let line = 1 {{ {CALL}; break; }}"),
			format!("let f = |line| {CALL};"),
			format!("let f = |a, [line]| {CALL};"),
			format!("fn line() {{}} {CALL};"),
			format!("const line = 1; {CALL};"),
			format!("use a::line; {CALL};"),
			format!("use a::other as line; {CALL};"),
			format!("use a::{{b, line}}; {CALL};"),
			format!("use a::{{b::{{line}}}}; {CALL};"),
			format!("mod line {{}} {CALL};"),
			format!("struct line {{}} {CALL};"),
			format!("enum line {{}} {CALL};"),
		];
		for case in cases {
			assert_eq!(rewritten_calls(&main(&case)), 0, "{case}");
		}
		// A parameter shadows in its function's body.
		assert_eq!(
			rewritten_calls(&format!("fn helper(line) {{ {CALL} }}\n{}", main(""))),
			0
		);
		// Items and imports are hoisted: visible before their declaration.
		assert_eq!(
			rewritten_calls(&main(&format!("{CALL}; fn line() {{}}"))),
			0
		);
		assert_eq!(
			rewritten_calls(&format!("{}\nuse a::line;", main(&format!("{CALL};")))),
			0
		);
	}

	#[test]
	fn the_same_call_outside_the_scope_is_the_constructor() {
		let cases = [
			// Before a `let`, and in the `let`'s own initializer.
			format!("{CALL}; let line = 1;"),
			format!("let line = {CALL};"),
			// After the block that bound it closes.
			format!("{{ let line = 1; }} {CALL};"),
			format!("for line in [1] {{}} {CALL};"),
			format!("for line in xs {{ let y = line; }} let p = {CALL};"),
			format!("if let line = 1 {{}} {CALL};"),
			format!("while let line = 1 {{ break; }} {CALL};"),
			format!("match 1 {{ line => 1 }}; {CALL};"),
			format!("let f = |line| 1; {CALL};"),
			// An item inside a nested block is visible only there.
			format!("{{ fn line() {{}} }} {CALL};"),
			format!("{{ use a::line; }} {CALL};"),
		];
		for case in cases {
			assert_eq!(rewritten_calls(&main(&case)), 1, "{case}");
		}
		// A sibling function does not see another's parameter or local.
		let source = format!(
			"fn helper(line) {{ let x = 1; }}\nfn other() {{ let line = 1; }}\n{}",
			main(&format!("{CALL};"))
		);
		assert_eq!(rewritten_calls(&source), 1);
		// A nested fn cannot capture an enclosing local.
		assert_eq!(
			rewritten_calls(&main(&format!("let line = 1; fn inner() {{ {CALL} }}"))),
			1
		);
		// A closure can.
		assert_eq!(
			rewritten_calls(&main(&format!("let line = 1; let f = || {CALL};"))),
			0
		);
		// A module sees nothing outside it.
		assert_eq!(
			rewritten_calls(&format!(
				"fn line() {{}}\nmod m {{ fn f() {{ {CALL} }} }}\n{}",
				main("")
			)),
			1
		);
	}

	#[test]
	fn a_fetch_loop_named_line_does_not_block_plotting() {
		let source = main(
			r#"for line in response.body.split("\n") { let parts = line.split(","); }
let v = line("date", "value");"#,
		);
		assert_eq!(rewritten_calls(&source), 1);
		// And the loop variable's methods are untouched.
		assert!(rewrite(&source).source.contains(r#"line.split(",")"#));
	}

	#[test]
	fn keys_fields_methods_and_strings_are_not_bindings() {
		let source = main(
			r#"let row = #{title: 1, line: 2}; let t = row.title; let s = "title"; let m = row.line; let o = #{title}; let v = x.label;"#,
		);
		// `#{title}` is a read of a variable named title, which is not a binding.
		rewrite(&source);
		// A use that imports under a different name binds that name only.
		rewrite(&format!("use a::title as heading;\n{}", main("")));
	}

	#[test]
	fn a_visible_glob_import_refuses_a_bare_constructor_call() {
		rewrite(&format!("use a::*;\n{}", main("let x = 1;")));
		let error = refused(&format!("use a::*;\n{}", main(r#"let t = title("x");"#)));
		assert!(error.message.contains("glob import"), "{}", error.message);
		// A glob in a block that has closed is not visible.
		rewrite(&main(r#"{ use a::*; } let t = title("x");"#));
		// A binding nearer than the glob shadows the name first.
		rewrite(&format!(
			"use a::*;\n{}",
			main(r#"let title = 1; let t = title("x");"#)
		));
	}

	#[test]
	fn macros_are_opaque() {
		// An ordinary macro keeps working.
		let source = main(r#"let s = format!("{}", 1); let t = `x ${1 + 1}`;"#);
		assert_eq!(rewrite(&source).source, source);
		// A presentation call inside a template is left for the compiler to
		// reject: bare names are not registered in the local context.
		let source = main(r#"let t = `x ${title("y")}`;"#);
		assert_eq!(rewrite(&source).source, source);
	}

	#[test]
	fn offsets_map_back_through_length_changing_rewrites() {
		let source = main(
			r#"let s = "é—✓"; let a = title("a"); let b = x_axis().label("b"); let c = subtitle("c"); let z = bogus;"#,
		);
		let out = rewrite(&source);
		let original = source.find("bogus").unwrap();
		let generated = out.source.find("bogus").unwrap();
		assert_ne!(original, generated);
		assert_eq!(out.original_offset(generated), original);
		// Synthetic text maps to its call's start.
		let prefix = out
			.source
			.find("polariton_presentation::__subtitle")
			.unwrap();
		assert_eq!(
			out.original_offset(prefix),
			source.find("subtitle").unwrap()
		);
		// Copied text before any rewrite maps one to one.
		let e = source.find('é').unwrap();
		assert_eq!(out.original_offset(e), e);
	}

	#[test]
	fn receiver_and_arguments_appear_once_per_branch() {
		let source = main(r#"let a = legend().at(f(1), g(2));"#);
		let out = rewrite(&source);
		// The receiver appears once, rewritten.
		assert_eq!(out.source.matches("legend(").count(), 1, "{}", out.source);
		assert_eq!(out.source.matches("__legend(").count(), 1, "{}", out.source);
		// Each branch carries the arguments once; only one runs.
		assert_eq!(
			out.source.matches("f(1), g(2)").count(),
			2,
			"{}",
			out.source
		);
	}
}
