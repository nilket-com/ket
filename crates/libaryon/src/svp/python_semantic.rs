use crate::core::DocByte;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use ruff_python_ast::{self as ast, Decorator, Expr, Parameter, Stmt};
use ruff_python_parser::Parsed;
use ruff_text_size::Ranged;

/// Extract semantic highlight spans from a parsed Python AST.
///
/// Walks the full AST to identify definition sites and upgrades
/// lexical `Variable` tokens to their true semantic category:
/// function names, class names, parameters, decorators, etc.
pub fn extract_semantic_highlights(
	parsed: &Parsed<ast::Mod>,
	global_offset: DocByte,
) -> Vec<HighlightSpan> {
	let mut spans = Vec::new();

	let module = match parsed.syntax() {
		ast::Mod::Module(m) => m,
		ast::Mod::Expression(_) => return spans,
	};

	for stmt in &module.body {
		visit_stmt(stmt, global_offset, &mut spans);
	}

	spans.sort_unstable_by_key(|s| s.start);
	spans
}

fn emit(
	spans: &mut Vec<HighlightSpan>,
	range: ruff_text_size::TextRange,
	category: TokenCategory,
	global_offset: DocByte,
) {
	let start = global_offset.saturating_add(u64::from(u32::from(range.start())));
	let end = global_offset.saturating_add(u64::from(u32::from(range.end())));
	spans.push(HighlightSpan::new(start, end, category));
}

fn visit_stmt(stmt: &Stmt, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match stmt {
		Stmt::FunctionDef(func) => {
			emit(spans, func.name.range(), TokenCategory::Function, offset);
			for decorator in &func.decorator_list {
				visit_decorator(decorator, offset, spans);
			}
			visit_parameters(&func.parameters, offset, spans);
			if let Some(ref ann) = func.returns {
				visit_type_expr(ann, offset, spans);
			}
			for s in &func.body {
				visit_stmt(s, offset, spans);
			}
		}
		Stmt::ClassDef(class) => {
			emit(spans, class.name.range(), TokenCategory::Type, offset);
			for decorator in &class.decorator_list {
				visit_decorator(decorator, offset, spans);
			}
			if let Some(ref args) = class.arguments {
				for arg in &args.args {
					visit_type_expr(arg, offset, spans);
				}
			}
			for s in &class.body {
				visit_stmt(s, offset, spans);
			}
		}
		Stmt::Import(imp) => {
			for alias in &imp.names {
				emit(spans, alias.name.range(), TokenCategory::Module, offset);
				if let Some(ref asname) = alias.asname {
					emit(spans, asname.range(), TokenCategory::Module, offset);
				}
			}
		}
		Stmt::ImportFrom(imp) => {
			if let Some(ref module_name) = imp.module {
				emit(spans, module_name.range(), TokenCategory::Module, offset);
			}
			for alias in &imp.names {
				emit(spans, alias.name.range(), TokenCategory::Module, offset);
				if let Some(ref asname) = alias.asname {
					emit(spans, asname.range(), TokenCategory::Variable, offset);
				}
			}
		}
		Stmt::For(f) => {
			visit_expr(&f.iter, offset, spans);
			for s in &f.body {
				visit_stmt(s, offset, spans);
			}
			for s in &f.orelse {
				visit_stmt(s, offset, spans);
			}
		}
		Stmt::While(w) => {
			visit_expr(&w.test, offset, spans);
			for s in &w.body {
				visit_stmt(s, offset, spans);
			}
			for s in &w.orelse {
				visit_stmt(s, offset, spans);
			}
		}
		Stmt::If(i) => {
			visit_expr(&i.test, offset, spans);
			for s in &i.body {
				visit_stmt(s, offset, spans);
			}
			for clause in &i.elif_else_clauses {
				if let Some(ref test) = clause.test {
					visit_expr(test, offset, spans);
				}
				for s in &clause.body {
					visit_stmt(s, offset, spans);
				}
			}
		}
		Stmt::With(w) => {
			for s in &w.body {
				visit_stmt(s, offset, spans);
			}
		}
		Stmt::Try(t) => {
			for s in &t.body {
				visit_stmt(s, offset, spans);
			}
			for handler in &t.handlers {
				let ast::ExceptHandler::ExceptHandler(h) = handler;
				if let Some(ref ty) = h.type_ {
					visit_type_expr(ty, offset, spans);
				}
				for s in &h.body {
					visit_stmt(s, offset, spans);
				}
			}
			for s in &t.orelse {
				visit_stmt(s, offset, spans);
			}
			for s in &t.finalbody {
				visit_stmt(s, offset, spans);
			}
		}
		Stmt::Match(m) => {
			visit_expr(&m.subject, offset, spans);
			for case in &m.cases {
				for s in &case.body {
					visit_stmt(s, offset, spans);
				}
			}
		}
		Stmt::AnnAssign(a) => {
			visit_type_expr(&a.annotation, offset, spans);
			if let Some(ref val) = a.value {
				visit_expr(val, offset, spans);
			}
		}
		Stmt::Assign(a) => {
			visit_expr(&a.value, offset, spans);
		}
		Stmt::AugAssign(a) => {
			visit_expr(&a.value, offset, spans);
		}
		Stmt::Return(r) => {
			if let Some(ref val) = r.value {
				visit_expr(val, offset, spans);
			}
		}
		Stmt::Raise(r) => {
			if let Some(ref exc) = r.exc {
				visit_expr(exc, offset, spans);
			}
		}
		Stmt::Assert(a) => {
			visit_expr(&a.test, offset, spans);
			if let Some(ref msg) = a.msg {
				visit_expr(msg, offset, spans);
			}
		}
		Stmt::Expr(e) => {
			visit_expr(&e.value, offset, spans);
		}
		_ => {}
	}
}

fn visit_decorator(decorator: &Decorator, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match &decorator.expression {
		Expr::Name(name) => {
			emit(spans, name.range(), TokenCategory::Attribute, offset);
		}
		Expr::Attribute(attr) => {
			visit_expr(&attr.value, offset, spans);
			emit(spans, attr.attr.range(), TokenCategory::Attribute, offset);
		}
		Expr::Call(call) => {
			visit_decorator_expr(&call.func, offset, spans);
			for arg in &call.arguments.args {
				visit_expr(arg, offset, spans);
			}
		}
		_ => {}
	}
}

fn visit_decorator_expr(expr: &Expr, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match expr {
		Expr::Name(name) => {
			emit(spans, name.range(), TokenCategory::Attribute, offset);
		}
		Expr::Attribute(attr) => {
			visit_expr(&attr.value, offset, spans);
			emit(spans, attr.attr.range(), TokenCategory::Attribute, offset);
		}
		_ => {}
	}
}

fn visit_parameters(params: &ast::Parameters, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	for param in &params.posonlyargs {
		visit_parameter(&param.parameter, offset, spans);
	}
	for param in &params.args {
		visit_parameter(&param.parameter, offset, spans);
	}
	if let Some(ref vararg) = params.vararg {
		emit(spans, vararg.name.range(), TokenCategory::Variable, offset);
		if let Some(ref ann) = vararg.annotation {
			visit_type_expr(ann, offset, spans);
		}
	}
	for param in &params.kwonlyargs {
		visit_parameter(&param.parameter, offset, spans);
	}
	if let Some(ref kwarg) = params.kwarg {
		emit(spans, kwarg.name.range(), TokenCategory::Variable, offset);
		if let Some(ref ann) = kwarg.annotation {
			visit_type_expr(ann, offset, spans);
		}
	}
}

fn visit_parameter(param: &Parameter, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	let name_str = param.name.as_str();
	if name_str == "self" || name_str == "cls" {
		emit(
			spans,
			param.name.range(),
			TokenCategory::SelfKeyword,
			offset,
		);
	} else {
		emit(spans, param.name.range(), TokenCategory::Variable, offset);
	}
	if let Some(ref ann) = param.annotation {
		visit_type_expr(ann, offset, spans);
	}
}

/// Visit an expression in a type annotation context.
fn visit_type_expr(expr: &Expr, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match expr {
		Expr::Name(name) => {
			emit(spans, name.range(), TokenCategory::Type, offset);
		}
		Expr::Attribute(attr) => {
			visit_expr(&attr.value, offset, spans);
			emit(spans, attr.attr.range(), TokenCategory::Type, offset);
		}
		Expr::Subscript(sub) => {
			visit_type_expr(&sub.value, offset, spans);
			visit_type_expr(&sub.slice, offset, spans);
		}
		Expr::Tuple(tup) => {
			for elt in &tup.elts {
				visit_type_expr(elt, offset, spans);
			}
		}
		Expr::BinOp(binop) => {
			visit_type_expr(&binop.left, offset, spans);
			visit_type_expr(&binop.right, offset, spans);
		}
		Expr::NoneLiteral(_) | Expr::StringLiteral(_) => {}
		_ => visit_expr(expr, offset, spans),
	}
}

/// Visit a general expression to identify call sites and attribute access.
fn visit_expr(expr: &Expr, offset: DocByte, spans: &mut Vec<HighlightSpan>) {
	match expr {
		Expr::Call(call) => {
			match &*call.func {
				Expr::Name(name) => {
					let cat = if name.id.starts_with(|c: char| c.is_uppercase()) {
						TokenCategory::Type
					} else {
						TokenCategory::Function
					};
					emit(spans, name.range(), cat, offset);
				}
				Expr::Attribute(attr) => {
					visit_expr(&attr.value, offset, spans);
					emit(spans, attr.attr.range(), TokenCategory::Method, offset);
				}
				other => visit_expr(other, offset, spans),
			}
			for arg in &call.arguments.args {
				visit_expr(arg, offset, spans);
			}
			for kw in &call.arguments.keywords {
				visit_expr(&kw.value, offset, spans);
			}
		}
		Expr::Attribute(attr) => {
			visit_expr(&attr.value, offset, spans);
		}
		Expr::Lambda(lam) => {
			if let Some(ref params) = lam.parameters {
				visit_parameters(params, offset, spans);
			}
			visit_expr(&lam.body, offset, spans);
		}
		Expr::ListComp(comp) => {
			visit_expr(&comp.elt, offset, spans);
			for compr in &comp.generators {
				visit_expr(&compr.iter, offset, spans);
				for f in &compr.ifs {
					visit_expr(f, offset, spans);
				}
			}
		}
		Expr::SetComp(comp) => {
			visit_expr(&comp.elt, offset, spans);
			for compr in &comp.generators {
				visit_expr(&compr.iter, offset, spans);
				for f in &compr.ifs {
					visit_expr(f, offset, spans);
				}
			}
		}
		Expr::DictComp(comp) => {
			visit_expr(&comp.key, offset, spans);
			visit_expr(&comp.value, offset, spans);
			for compr in &comp.generators {
				visit_expr(&compr.iter, offset, spans);
				for f in &compr.ifs {
					visit_expr(f, offset, spans);
				}
			}
		}
		Expr::Generator(comp) => {
			visit_expr(&comp.elt, offset, spans);
			for compr in &comp.generators {
				visit_expr(&compr.iter, offset, spans);
				for f in &compr.ifs {
					visit_expr(f, offset, spans);
				}
			}
		}
		Expr::If(i) => {
			visit_expr(&i.test, offset, spans);
			visit_expr(&i.body, offset, spans);
			visit_expr(&i.orelse, offset, spans);
		}
		Expr::BoolOp(b) => {
			for val in &b.values {
				visit_expr(val, offset, spans);
			}
		}
		Expr::Compare(c) => {
			visit_expr(&c.left, offset, spans);
			for comp in &c.comparators {
				visit_expr(comp, offset, spans);
			}
		}
		Expr::BinOp(b) => {
			visit_expr(&b.left, offset, spans);
			visit_expr(&b.right, offset, spans);
		}
		Expr::UnaryOp(u) => {
			visit_expr(&u.operand, offset, spans);
		}
		Expr::Starred(s) => {
			visit_expr(&s.value, offset, spans);
		}
		Expr::List(l) => {
			for elt in &l.elts {
				visit_expr(elt, offset, spans);
			}
		}
		Expr::Tuple(t) => {
			for elt in &t.elts {
				visit_expr(elt, offset, spans);
			}
		}
		Expr::Set(s) => {
			for elt in &s.elts {
				visit_expr(elt, offset, spans);
			}
		}
		Expr::Dict(d) => {
			for item in &d.items {
				if let Some(ref key) = item.key {
					visit_expr(key, offset, spans);
				}
				visit_expr(&item.value, offset, spans);
			}
		}
		Expr::Subscript(sub) => {
			visit_expr(&sub.value, offset, spans);
			visit_expr(&sub.slice, offset, spans);
		}
		Expr::Yield(y) => {
			if let Some(ref val) = y.value {
				visit_expr(val, offset, spans);
			}
		}
		Expr::YieldFrom(y) => {
			visit_expr(&y.value, offset, spans);
		}
		Expr::Await(a) => {
			visit_expr(&a.value, offset, spans);
		}
		_ => {}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn highlights_for(source: &str) -> Vec<(TokenCategory, String)> {
		let parsed = ruff_python_parser::parse_unchecked(source, ruff_python_parser::Mode::Module);
		let spans = extract_semantic_highlights(&parsed, DocByte::ZERO);
		spans
			.into_iter()
			.map(|s| {
				let start = u64::from(s.start) as usize;
				let end = u64::from(s.end) as usize;
				let text = source[start..end].to_string();
				(s.category, text)
			})
			.collect()
	}

	#[test]
	fn function_def_name_is_function() {
		let h = highlights_for("def foo():\n    pass\n");
		assert!(h.contains(&(TokenCategory::Function, "foo".into())));
	}

	#[test]
	fn class_def_name_is_type() {
		let h = highlights_for("class MyClass:\n    pass\n");
		assert!(h.contains(&(TokenCategory::Type, "MyClass".into())));
	}

	#[test]
	fn self_parameter_is_self_keyword() {
		let h = highlights_for("def method(self, x):\n    pass\n");
		assert!(h.contains(&(TokenCategory::SelfKeyword, "self".into())));
	}

	#[test]
	fn import_name_is_module() {
		let h = highlights_for("import os\n");
		assert!(h.contains(&(TokenCategory::Module, "os".into())));
	}

	#[test]
	fn decorator_is_attribute() {
		let h = highlights_for("@staticmethod\ndef foo():\n    pass\n");
		assert!(h.contains(&(TokenCategory::Attribute, "staticmethod".into())));
	}

	#[test]
	fn method_call_is_method() {
		let h = highlights_for("x.append(1)\n");
		assert!(h.contains(&(TokenCategory::Method, "append".into())));
	}

	#[test]
	fn uppercase_call_is_type() {
		let h = highlights_for("x = MyClass()\n");
		assert!(h.contains(&(TokenCategory::Type, "MyClass".into())));
	}

	#[test]
	fn type_annotation_is_type() {
		let h = highlights_for("def foo(x: int) -> str:\n    pass\n");
		assert!(h.contains(&(TokenCategory::Type, "int".into())));
		assert!(h.contains(&(TokenCategory::Type, "str".into())));
	}
}
