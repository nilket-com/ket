//! Phase 9 — Rune Semantic Reactor (extractor half).
//!
//! Walks a parsed [`rune::ast::File`] and emits high-fidelity
//! [`HighlightSpan`]s that upgrade lexical `Variable` tokens to their true
//! semantic category: function names, type names, methods, macros, `self`.
//!
//! This module deliberately mirrors `python_semantic.rs`. It owns no state,
//! only consumes the AST, so it is safe to call from the background reactor
//! thread.

use crate::core::DocByte;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

use rune::ast::{self, Spanned};

/// Extract semantic highlight spans from a parsed Rune file.
pub fn extract_semantic_highlights(file: &ast::File, global_offset: DocByte) -> Vec<HighlightSpan> {
	let mut spans = Vec::new();
	for (item, _semi) in &file.items {
		visit_item(item, global_offset, &mut spans);
	}
	spans.sort_unstable_by_key(|s| s.start);
	spans
}

#[inline]
fn emit(
	spans: &mut Vec<HighlightSpan>,
	start: u32,
	end: u32,
	category: TokenCategory,
	global_offset: DocByte,
) {
	let s = global_offset.saturating_add(u64::from(start));
	let e = global_offset.saturating_add(u64::from(end));
	spans.push(HighlightSpan::new(s, e, category));
}

#[inline]
fn emit_spanned(
	spans: &mut Vec<HighlightSpan>,
	spanned: &impl Spanned,
	category: TokenCategory,
	global_offset: DocByte,
) {
	let span = spanned.span();
	emit(spans, span.start.0, span.end.0, category, global_offset);
}

// ============================================================
// Items
// ============================================================

fn visit_item(item: &ast::Item, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match item {
		ast::Item::Fn(f) => {
			emit_spanned(spans, &f.name, TokenCategory::Function, offset);
			visit_block(&f.body, offset, spans);
		}
		ast::Item::Struct(s) => {
			emit_spanned(spans, &s.ident, TokenCategory::Type, offset);
		}
		ast::Item::Enum(e) => {
			emit_spanned(spans, &e.name, TokenCategory::Type, offset);
			for (variant, _comma) in e.variants.iter() {
				emit_spanned(spans, &variant.name, TokenCategory::Constant, offset);
			}
		}
		ast::Item::Impl(i) => {
			emit_path_as_type(&i.path, offset, spans);
			for nested in &i.functions {
				let category = if is_instance_fn(nested) {
					TokenCategory::Method
				} else {
					TokenCategory::Function
				};
				emit_spanned(spans, &nested.name, category, offset);
				visit_block(&nested.body, offset, spans);
			}
		}
		ast::Item::Mod(m) => {
			emit_spanned(spans, &m.name, TokenCategory::Module, offset);
			if let ast::ItemModBody::InlineBody(body) = &m.body {
				for (nested, _semi) in &body.file.items {
					visit_item(nested, offset, spans);
				}
			}
		}
		ast::Item::MacroCall(mc) => {
			emit_path_as_macro(&mc.path, offset, spans);
		}
		// Use / Const / future variants — no semantic upgrade beyond lexer.
		_ => {}
	}
}

fn is_instance_fn(f: &ast::ItemFn) -> bool {
	matches!(f.args.first(), Some((ast::FnArg::SelfValue(..), _)))
}

/// Single-segment ident path: `foo` (no `::`, no `Self`, etc.).
fn path_as_ident(path: &ast::Path) -> Option<&ast::Ident> {
	if path.global.is_some() || path.trailing.is_some() || !path.rest.is_empty() {
		return None;
	}
	match &path.first {
		ast::PathSegment::Ident(ident) => Some(ident),
		_ => None,
	}
}

/// True if `path` is a bare `self` reference (single segment, `self`).
fn path_is_bare_self(path: &ast::Path) -> bool {
	if path.global.is_some() || path.trailing.is_some() || !path.rest.is_empty() {
		return false;
	}
	matches!(path.first, ast::PathSegment::SelfValue(..))
}

// ============================================================
// Paths
// ============================================================

/// Emit every identifier segment of a path as `Type`. Used for `impl <Path>`.
fn emit_path_as_type(path: &ast::Path, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	emit_path_segment(&path.first, TokenCategory::Type, offset, spans);
	for (_sep, seg) in &path.rest {
		emit_path_segment(seg, TokenCategory::Type, offset, spans);
	}
}

/// Emit a macro path: leading segments as `Module`, last segment as `Macro`.
fn emit_path_as_macro(path: &ast::Path, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	if path.rest.is_empty() {
		emit_path_segment(&path.first, TokenCategory::Macro, offset, spans);
	} else {
		emit_path_segment(&path.first, TokenCategory::Module, offset, spans);
		let last_idx = path.rest.len() - 1;
		for (i, (_sep, seg)) in path.rest.iter().enumerate() {
			let cat = if i == last_idx {
				TokenCategory::Macro
			} else {
				TokenCategory::Module
			};
			emit_path_segment(seg, cat, offset, spans);
		}
	}
}

fn emit_path_segment(
	seg: &ast::PathSegment,
	category: TokenCategory,
	offset: DocByte,
	spans: &mut Vec<HighlightSpan>,
) {
	match seg {
		ast::PathSegment::Ident(ident) => emit_spanned(spans, ident, category, offset),
		ast::PathSegment::SelfValue(s) => {
			emit_spanned(spans, s, TokenCategory::SelfKeyword, offset)
		}
		ast::PathSegment::SelfType(s) => emit_spanned(spans, s, TokenCategory::Type, offset),
		// Crate / Super / Generics: leave to the lexer.
		_ => {}
	}
}

// ============================================================
// Blocks / statements / expressions
// ============================================================

fn visit_condition(cond: &ast::Condition, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match cond {
		ast::Condition::Expr(e) => visit_expr(e, offset, spans),
		ast::Condition::ExprLet(l) => visit_expr(&l.expr, offset, spans),
		_ => {}
	}
}

fn visit_block(block: &ast::Block, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	for stmt in &block.statements {
		visit_stmt(stmt, offset, spans);
	}
}

fn visit_stmt(stmt: &ast::Stmt, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match stmt {
		ast::Stmt::Local(local) => {
			visit_expr(&local.expr, offset, spans);
		}
		ast::Stmt::Item(item, _semi) => {
			visit_item(item, offset, spans);
		}
		ast::Stmt::Expr(expr) => visit_expr(expr, offset, spans),
		ast::Stmt::Semi(semi) => visit_expr(&semi.expr, offset, spans),
		_ => {}
	}
}

fn visit_expr(expr: &ast::Expr, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match expr {
		ast::Expr::Path(p)
			// Bare `self` reference: simple, single-segment path.
			if path_is_bare_self(p) => {
				emit_spanned(spans, p, TokenCategory::SelfKeyword, offset);
			}
		ast::Expr::Call(call) => {
			// Function or method call. Distinguish by the callee shape:
			//   foo()           -> Path(foo)            => Function
			//   a.bar()         -> FieldAccess          => Method
			//   path::seg()     -> Path(...)            => Function on last seg
			match call.expr.as_ref() {
				ast::Expr::Path(p) => {
					emit_path_as_call(p, offset, spans);
				}
				ast::Expr::FieldAccess(field) => {
					visit_expr(&field.expr, offset, spans);
					if let ast::ExprField::Path(method_path) = &field.expr_field
						&& let Some(ident) = path_as_ident(method_path) {
							emit_spanned(spans, ident, TokenCategory::Method, offset);
						}
				}
				other => visit_expr(other, offset, spans),
			}
			for (arg, _comma) in call.args.iter() {
				visit_expr(arg, offset, spans);
			}
		}
		ast::Expr::MacroCall(mc) => {
			emit_path_as_macro(&mc.path, offset, spans);
		}
		ast::Expr::FieldAccess(field) => {
			visit_expr(&field.expr, offset, spans);
		}
		ast::Expr::Block(block) => {
			visit_block(&block.block, offset, spans);
		}
		ast::Expr::If(expr_if) => {
			visit_condition(&expr_if.condition, offset, spans);
			visit_block(&expr_if.block, offset, spans);
			for branch in &expr_if.expr_else_ifs {
				visit_condition(&branch.condition, offset, spans);
				visit_block(&branch.block, offset, spans);
			}
			if let Some(else_branch) = &expr_if.expr_else {
				visit_block(&else_branch.block, offset, spans);
			}
		}
		ast::Expr::While(w) => {
			visit_condition(&w.condition, offset, spans);
			visit_block(&w.body, offset, spans);
		}
		ast::Expr::Loop(l) => {
			visit_block(&l.body, offset, spans);
		}
		ast::Expr::For(f) => {
			visit_expr(&f.iter, offset, spans);
			visit_block(&f.body, offset, spans);
		}
		ast::Expr::Match(m) => {
			visit_expr(&m.expr, offset, spans);
			for (branch, _comma) in &m.branches {
				if let Some((_if_kw, guard)) = &branch.condition {
					visit_expr(guard, offset, spans);
				}
				visit_expr(&branch.body, offset, spans);
			}
		}
		ast::Expr::Let(l) => {
			visit_expr(&l.expr, offset, spans);
		}
		ast::Expr::Assign(a) => {
			visit_expr(&a.lhs, offset, spans);
			visit_expr(&a.rhs, offset, spans);
		}
		ast::Expr::Binary(b) => {
			visit_expr(&b.lhs, offset, spans);
			visit_expr(&b.rhs, offset, spans);
		}
		ast::Expr::Unary(u) => {
			visit_expr(&u.expr, offset, spans);
		}
		ast::Expr::Index(i) => {
			visit_expr(&i.target, offset, spans);
			visit_expr(&i.index, offset, spans);
		}
		ast::Expr::Return(r) => {
			if let Some(inner) = &r.expr {
				visit_expr(inner, offset, spans);
			}
		}
		ast::Expr::Yield(y) => {
			if let Some(inner) = &y.expr {
				visit_expr(inner, offset, spans);
			}
		}
		ast::Expr::Await(a) => visit_expr(&a.expr, offset, spans),
		ast::Expr::Try(t) => visit_expr(&t.expr, offset, spans),
		ast::Expr::Group(g) => visit_expr(&g.expr, offset, spans),
		ast::Expr::Closure(c) => {
			visit_expr(&c.body, offset, spans);
		}
		ast::Expr::Tuple(t) => {
			for (item, _comma) in t.items.iter() {
				visit_expr(item, offset, spans);
			}
		}
		ast::Expr::Vec(v) => {
			for (item, _comma) in v.items.iter() {
				visit_expr(item, offset, spans);
			}
		}
		ast::Expr::Object(o) => {
			for (field, _comma) in o.assignments.iter() {
				if let Some((_colon, value)) = &field.assign {
					visit_expr(value, offset, spans);
				}
			}
		}
		// Lit / Break / Continue / Range / Empty / Select — nothing to upgrade.
		_ => {}
	}
}

/// A path used as a call target. Treat the last segment as the function name,
/// any leading segments as module path components.
fn emit_path_as_call(path: &ast::Path, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	if path.rest.is_empty() {
		emit_path_segment(&path.first, TokenCategory::Function, offset, spans);
	} else {
		emit_path_segment(&path.first, TokenCategory::Module, offset, spans);
		let last_idx = path.rest.len() - 1;
		for (i, (_sep, seg)) in path.rest.iter().enumerate() {
			let cat = if i == last_idx {
				TokenCategory::Function
			} else {
				TokenCategory::Module
			};
			emit_path_segment(seg, cat, offset, spans);
		}
	}
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::rune_ast::try_parse_rune_file;

	fn highlights_for(source: &str) -> Vec<(TokenCategory, String)> {
		let file = try_parse_rune_file(source).expect("rune source must parse");
		let spans = extract_semantic_highlights(&file, DocByte::ZERO);
		spans
			.into_iter()
			.map(|s| {
				let start = u64::from(s.start) as usize;
				let end = u64::from(s.end) as usize;
				(s.category, source[start..end].to_string())
			})
			.collect()
	}

	#[test]
	fn function_name_categorized_as_function() {
		let hls = highlights_for("fn greet() { let x = 1; }");
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Function && t == "greet"),
			"got: {hls:?}"
		);
	}

	#[test]
	fn struct_name_categorized_as_type() {
		let hls = highlights_for("struct Point { x, y }");
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Type && t == "Point"),
			"got: {hls:?}"
		);
	}

	#[test]
	fn macro_call_path_last_segment_is_macro() {
		let hls = highlights_for("fn main() { println!(\"hi\"); }");
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Macro && t == "println"),
			"got: {hls:?}"
		);
	}

	#[test]
	fn impl_block_marks_type_and_methods() {
		let source = r#"
struct Counter { n }

impl Counter {
	fn new() { 0 }
	fn tick(self) { self }
}
"#;
		let hls = highlights_for(source);
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Type && t == "Counter"),
			"missing Counter as Type: {hls:?}"
		);
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Function && t == "new"),
			"missing new as Function: {hls:?}"
		);
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Method && t == "tick"),
			"missing tick as Method: {hls:?}"
		);
	}

	#[test]
	fn enum_name_and_variants_categorized() {
		let hls = highlights_for("enum Color { Red, Green, Blue }");
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Type && t == "Color"),
			"got: {hls:?}"
		);
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Constant && t == "Red"),
			"got: {hls:?}"
		);
	}

	#[test]
	fn method_call_categorized() {
		let source = "fn main() { let v = make(); v.push(1); }";
		let hls = highlights_for(source);
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Method && t == "push"),
			"missing push as Method: {hls:?}"
		);
		assert!(
			hls.iter()
				.any(|(c, t)| *c == TokenCategory::Function && t == "make"),
			"missing make as Function: {hls:?}"
		);
	}
}
