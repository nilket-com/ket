use crate::core::DocByte;
use crate::engine::language::GreenNode;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use crate::svp::sql_parse::{ByteSpan, LineIndex};
use crate::uast::kind::SemanticKind;
use sqlparser::ast::{self, Spanned};

pub fn extract_semantic_highlights(
	statements: &[ast::Statement],
	line_index: &LineIndex,
	global_offset: DocByte,
) -> Vec<HighlightSpan> {
	let mut spans = Vec::new();
	for statement in statements {
		visit_statement(statement, line_index, global_offset, &mut spans);
	}

	spans.sort_unstable_by_key(|span| (span.start.get(), span.end.get(), span.category as usize));
	spans.dedup_by(|left, right| {
		left.start == right.start && left.end == right.end && left.category == right.category
	});
	spans
}

pub fn build_structural_topology(
	statements: &[ast::Statement],
	line_index: &LineIndex,
	source_len: u32,
) -> GreenNode {
	let total_len = source_len as usize;
	let mut children = Vec::new();
	let mut cursor = 0usize;

	for statement in statements {
		let Some(node) = build_statement_node(statement, line_index) else {
			continue;
		};
		if node.start < cursor || node.end > total_len || node.end <= node.start {
			continue;
		}
		if node.start > cursor {
			children.push(RangeNode::leaf(cursor, node.start));
		}
		cursor = node.end;
		children.push(node);
	}

	if cursor < total_len {
		children.push(RangeNode::leaf(cursor, total_len));
	}

	RangeNode {
		kind: SemanticKind::RelationalTable,
		start: 0,
		end: total_len,
		children,
	}
	.to_green()
}

pub fn flat_topology(source_len: u32) -> GreenNode {
	GreenNode {
		kind: SemanticKind::RelationalTable,
		byte_len: source_len,
		children: vec![GreenNode {
			kind: SemanticKind::Token,
			byte_len: source_len,
			children: Vec::new(),
		}],
	}
}

#[derive(Debug, Clone)]
struct RangeNode {
	kind: SemanticKind,
	start: usize,
	end: usize,
	children: Vec<RangeNode>,
}

impl RangeNode {
	fn leaf(start: usize, end: usize) -> Self {
		Self {
			kind: SemanticKind::Token,
			start,
			end,
			children: Vec::new(),
		}
	}

	fn len(&self) -> u32 {
		self.end.saturating_sub(self.start) as u32
	}

	fn to_green(&self) -> GreenNode {
		GreenNode {
			kind: self.kind,
			byte_len: self.len(),
			children: self.children.iter().map(Self::to_green).collect(),
		}
	}
}

fn build_statement_node(statement: &ast::Statement, line_index: &LineIndex) -> Option<RangeNode> {
	match statement {
		ast::Statement::Query(query) => build_query_node(query, line_index),
		ast::Statement::CreateView(view) => build_create_view_node(view, line_index),
		ast::Statement::CreateTable(table) => build_create_table_node(table, line_index),
		ast::Statement::Insert(insert) => build_insert_node(insert, line_index),
		ast::Statement::Update(update) => build_update_node(update, line_index),
		ast::Statement::Delete(delete) => build_delete_node(delete, line_index),
		_ => build_simple_statement_node(statement, line_index),
	}
}

fn build_simple_statement_node(
	statement: &impl Spanned,
	line_index: &LineIndex,
) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(statement.span())?;
	Some(build_branch(SemanticKind::RelationalRow, span, Vec::new()))
}

fn build_query_node(query: &ast::Query, line_index: &LineIndex) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(query.span())?;
	let mut nested = Vec::new();

	if let Some(with_clause) = &query.with {
		for cte in &with_clause.cte_tables {
			if let Some(cte_node) = build_cte_node(cte, line_index) {
				nested.push(cte_node);
			}
		}
	}

	collect_set_expr_nodes(&query.body, line_index, &mut nested);
	Some(build_branch(SemanticKind::RelationalTable, span, nested))
}

fn build_cte_node(cte: &ast::Cte, line_index: &LineIndex) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(cte.span())?;
	let mut nested = Vec::new();
	if let Some(query_node) = build_query_node(&cte.query, line_index) {
		nested.push(query_node);
	}
	Some(build_branch(SemanticKind::RelationalRow, span, nested))
}

fn build_create_view_node(view: &ast::CreateView, line_index: &LineIndex) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(view.span())?;
	let mut nested = Vec::new();
	if let Some(query_node) = build_query_node(&view.query, line_index) {
		nested.push(query_node);
	}
	Some(build_branch(SemanticKind::RelationalTable, span, nested))
}

fn build_create_table_node(table: &ast::CreateTable, line_index: &LineIndex) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(table.span())?;
	let mut nested = Vec::new();
	if let Some(query) = &table.query
		&& let Some(query_node) = build_query_node(query, line_index)
	{
		nested.push(query_node);
	}
	Some(build_branch(SemanticKind::RelationalTable, span, nested))
}

fn build_insert_node(insert: &ast::Insert, line_index: &LineIndex) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(insert.span())?;
	let mut nested = Vec::new();
	if let Some(query) = &insert.source
		&& let Some(query_node) = build_query_node(query, line_index)
	{
		nested.push(query_node);
	}
	Some(build_branch(SemanticKind::RelationalRow, span, nested))
}

fn build_update_node(update: &ast::Update, line_index: &LineIndex) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(update.span())?;
	let mut nested = Vec::new();
	collect_table_factor_subqueries(&update.table.relation, line_index, &mut nested);
	for join in &update.table.joins {
		collect_table_factor_subqueries(&join.relation, line_index, &mut nested);
	}
	match &update.from {
		Some(ast::UpdateTableFromKind::BeforeSet(tables))
		| Some(ast::UpdateTableFromKind::AfterSet(tables)) => {
			for table in tables {
				collect_table_with_joins_subqueries(table, line_index, &mut nested);
			}
		}
		None => {}
	}
	Some(build_branch(SemanticKind::RelationalRow, span, nested))
}

fn build_delete_node(delete: &ast::Delete, line_index: &LineIndex) -> Option<RangeNode> {
	let span = line_index.span_to_byte_span(delete.span())?;
	let mut nested = Vec::new();
	match &delete.from {
		ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => {
			for table in tables {
				collect_table_with_joins_subqueries(table, line_index, &mut nested);
			}
		}
	}
	if let Some(using_tables) = &delete.using {
		for table in using_tables {
			collect_table_with_joins_subqueries(table, line_index, &mut nested);
		}
	}
	Some(build_branch(SemanticKind::RelationalRow, span, nested))
}

fn collect_set_expr_nodes(
	set_expr: &ast::SetExpr,
	line_index: &LineIndex,
	out: &mut Vec<RangeNode>,
) {
	match set_expr {
		ast::SetExpr::Query(query) => {
			if let Some(node) = build_query_node(query, line_index) {
				out.push(node);
			}
		}
		ast::SetExpr::SetOperation { left, right, .. } => {
			collect_set_expr_nodes(left, line_index, out);
			collect_set_expr_nodes(right, line_index, out);
		}
		ast::SetExpr::Select(select) => {
			for table in &select.from {
				collect_table_with_joins_subqueries(table, line_index, out);
			}
			for projection in &select.projection {
				match projection {
					ast::SelectItem::UnnamedExpr(expr)
					| ast::SelectItem::ExprWithAlias { expr, .. } => {
						collect_expr_subqueries(expr, line_index, out);
					}
					ast::SelectItem::QualifiedWildcard(_, _) | ast::SelectItem::Wildcard(_) => {}
				}
			}
			if let Some(expr) = &select.selection {
				collect_expr_subqueries(expr, line_index, out);
			}
			if let Some(expr) = &select.prewhere {
				collect_expr_subqueries(expr, line_index, out);
			}
			if let Some(expr) = &select.having {
				collect_expr_subqueries(expr, line_index, out);
			}
			if let Some(expr) = &select.qualify {
				collect_expr_subqueries(expr, line_index, out);
			}
		}
		ast::SetExpr::Values(values) => {
			for row in &values.rows {
				for expr in row {
					collect_expr_subqueries(expr, line_index, out);
				}
			}
		}
		_ => {}
	}
}

fn collect_table_with_joins_subqueries(
	table_with_joins: &ast::TableWithJoins,
	line_index: &LineIndex,
	out: &mut Vec<RangeNode>,
) {
	collect_table_factor_subqueries(&table_with_joins.relation, line_index, out);
	for join in &table_with_joins.joins {
		collect_table_factor_subqueries(&join.relation, line_index, out);
		match &join.join_operator {
			ast::JoinOperator::Join(constraint)
			| ast::JoinOperator::Inner(constraint)
			| ast::JoinOperator::Left(constraint)
			| ast::JoinOperator::LeftOuter(constraint)
			| ast::JoinOperator::Right(constraint)
			| ast::JoinOperator::RightOuter(constraint)
			| ast::JoinOperator::FullOuter(constraint)
			| ast::JoinOperator::CrossJoin(constraint)
			| ast::JoinOperator::Semi(constraint)
			| ast::JoinOperator::LeftSemi(constraint)
			| ast::JoinOperator::RightSemi(constraint)
			| ast::JoinOperator::Anti(constraint)
			| ast::JoinOperator::LeftAnti(constraint)
			| ast::JoinOperator::RightAnti(constraint)
			| ast::JoinOperator::StraightJoin(constraint) => {
				collect_join_constraint_subqueries(constraint, line_index, out);
			}
			ast::JoinOperator::AsOf {
				match_condition,
				constraint,
			} => {
				collect_expr_subqueries(match_condition, line_index, out);
				collect_join_constraint_subqueries(constraint, line_index, out);
			}
			ast::JoinOperator::CrossApply | ast::JoinOperator::OuterApply => {}
		}
	}
}

fn collect_table_factor_subqueries(
	table_factor: &ast::TableFactor,
	line_index: &LineIndex,
	out: &mut Vec<RangeNode>,
) {
	match table_factor {
		ast::TableFactor::Derived { subquery, .. } => {
			if let Some(node) = build_query_node(subquery, line_index) {
				out.push(node);
			}
		}
		ast::TableFactor::NestedJoin {
			table_with_joins,
			alias: _,
		} => collect_table_with_joins_subqueries(table_with_joins, line_index, out),
		ast::TableFactor::TableFunction { expr, .. } => {
			collect_expr_subqueries(expr, line_index, out)
		}
		ast::TableFactor::Table {
			args: Some(args), ..
		} => {
			for arg in &args.args {
				match arg {
					ast::FunctionArg::Named { arg, .. }
					| ast::FunctionArg::ExprNamed { arg, .. }
					| ast::FunctionArg::Unnamed(arg) => collect_function_arg_expr_subqueries(arg, line_index, out),
				}
			}
		}
		ast::TableFactor::Table { args: None, .. } => {}
		ast::TableFactor::Function { args, .. } => {
			for arg in args {
				match arg {
					ast::FunctionArg::Named { arg, .. }
					| ast::FunctionArg::ExprNamed { arg, .. }
					| ast::FunctionArg::Unnamed(arg) => collect_function_arg_expr_subqueries(arg, line_index, out),
				}
			}
		}
		ast::TableFactor::UNNEST { array_exprs, .. } => {
			for expr in array_exprs {
				collect_expr_subqueries(expr, line_index, out);
			}
		}
		ast::TableFactor::JsonTable { json_expr, .. }
		| ast::TableFactor::OpenJsonTable { json_expr, .. } => {
			collect_expr_subqueries(json_expr, line_index, out);
		}
		ast::TableFactor::Pivot {
			table,
			aggregate_functions,
			value_column,
			default_on_null,
			..
		} => {
			collect_table_factor_subqueries(table, line_index, out);
			for function in aggregate_functions {
				collect_expr_subqueries(&function.expr, line_index, out);
			}
			for expr in value_column {
				collect_expr_subqueries(expr, line_index, out);
			}
			if let Some(expr) = default_on_null {
				collect_expr_subqueries(expr, line_index, out);
			}
		}
		ast::TableFactor::Unpivot { table, value, .. } => {
			collect_table_factor_subqueries(table, line_index, out);
			collect_expr_subqueries(value, line_index, out);
		}
		ast::TableFactor::MatchRecognize {
			table,
			measures,
			rows_per_match: _,
			after_match_skip: _,
			pattern: _,
			symbols,
			partition_by,
			order_by,
			..
		} => {
			collect_table_factor_subqueries(table, line_index, out);
			for measure in measures {
				collect_expr_subqueries(&measure.expr, line_index, out);
			}
			for expr in partition_by {
				collect_expr_subqueries(expr, line_index, out);
			}
			for expr in order_by {
				collect_expr_subqueries(&expr.expr, line_index, out);
			}
			for symbol in symbols {
				collect_expr_subqueries(&symbol.definition, line_index, out);
			}
		}
		_ => {}
	}
}

fn collect_join_constraint_subqueries(
	constraint: &ast::JoinConstraint,
	line_index: &LineIndex,
	out: &mut Vec<RangeNode>,
) {
	match constraint {
		ast::JoinConstraint::On(expr) => collect_expr_subqueries(expr, line_index, out),
		ast::JoinConstraint::Using(_)
		| ast::JoinConstraint::Natural
		| ast::JoinConstraint::None => {}
	}
}

fn collect_function_arg_expr_subqueries(
	arg: &ast::FunctionArgExpr,
	line_index: &LineIndex,
	out: &mut Vec<RangeNode>,
) {
	if let ast::FunctionArgExpr::Expr(expr) = arg {
		collect_expr_subqueries(expr, line_index, out);
	}
}

fn collect_expr_subqueries(expr: &ast::Expr, line_index: &LineIndex, out: &mut Vec<RangeNode>) {
	match expr {
		ast::Expr::Subquery(query) => {
			if let Some(node) = build_query_node(query, line_index) {
				out.push(node);
			}
		}
		ast::Expr::Exists { subquery, .. } => {
			if let Some(node) = build_query_node(subquery, line_index) {
				out.push(node);
			}
		}
		ast::Expr::InSubquery { expr, subquery, .. } => {
			collect_expr_subqueries(expr, line_index, out);
			if let Some(node) = build_query_node(subquery, line_index) {
				out.push(node);
			}
		}
		ast::Expr::BinaryOp { left, right, .. }
		| ast::Expr::IsDistinctFrom(left, right)
		| ast::Expr::IsNotDistinctFrom(left, right)
		| ast::Expr::AtTimeZone {
			timestamp: left,
			time_zone: right,
		}
		| ast::Expr::Position {
			expr: left,
			r#in: right,
		} => {
			collect_expr_subqueries(left, line_index, out);
			collect_expr_subqueries(right, line_index, out);
		}
		ast::Expr::UnaryOp { expr, .. }
		| ast::Expr::Nested(expr)
		| ast::Expr::IsFalse(expr)
		| ast::Expr::IsNotFalse(expr)
		| ast::Expr::IsTrue(expr)
		| ast::Expr::IsNotTrue(expr)
		| ast::Expr::IsNull(expr)
		| ast::Expr::IsNotNull(expr)
		| ast::Expr::IsUnknown(expr)
		| ast::Expr::IsNotUnknown(expr) => collect_expr_subqueries(expr, line_index, out),
		ast::Expr::Between {
			expr,
			low,
			high,
			negated: _,
		} => {
			collect_expr_subqueries(expr, line_index, out);
			collect_expr_subqueries(low, line_index, out);
			collect_expr_subqueries(high, line_index, out);
		}
		ast::Expr::Like { expr, pattern, .. }
		| ast::Expr::ILike { expr, pattern, .. }
		| ast::Expr::SimilarTo { expr, pattern, .. }
		| ast::Expr::RLike { expr, pattern, .. } => {
			collect_expr_subqueries(expr, line_index, out);
			collect_expr_subqueries(pattern, line_index, out);
		}
		ast::Expr::Function(function) => {
			collect_function_subqueries(function, line_index, out);
		}
		ast::Expr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			if let Some(operand) = operand {
				collect_expr_subqueries(operand, line_index, out);
			}
			for condition in conditions {
				collect_expr_subqueries(&condition.condition, line_index, out);
				collect_expr_subqueries(&condition.result, line_index, out);
			}
			if let Some(expr) = else_result {
				collect_expr_subqueries(expr, line_index, out);
			}
		}
		ast::Expr::Tuple(items) => {
			for item in items {
				collect_expr_subqueries(item, line_index, out);
			}
		}
		ast::Expr::Array(array) => {
			for item in &array.elem {
				collect_expr_subqueries(item, line_index, out);
			}
		}
		ast::Expr::GroupingSets(sets) | ast::Expr::Cube(sets) | ast::Expr::Rollup(sets) => {
			for set in sets {
				for item in set {
					collect_expr_subqueries(item, line_index, out);
				}
			}
		}
		ast::Expr::CompoundFieldAccess { root, access_chain } => {
			collect_expr_subqueries(root, line_index, out);
			for access in access_chain {
				match access {
					ast::AccessExpr::Dot(expr) => collect_expr_subqueries(expr, line_index, out),
					ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => {
						collect_expr_subqueries(index, line_index, out)
					}
					ast::AccessExpr::Subscript(ast::Subscript::Slice {
						lower_bound,
						upper_bound,
						stride,
					}) => {
						if let Some(expr) = lower_bound {
							collect_expr_subqueries(expr, line_index, out);
						}
						if let Some(expr) = upper_bound {
							collect_expr_subqueries(expr, line_index, out);
						}
						if let Some(expr) = stride {
							collect_expr_subqueries(expr, line_index, out);
						}
					}
				}
			}
		}
		_ => {}
	}
}

fn collect_function_subqueries(
	function: &ast::Function,
	line_index: &LineIndex,
	out: &mut Vec<RangeNode>,
) {
	collect_function_arguments_subqueries(&function.parameters, line_index, out);
	collect_function_arguments_subqueries(&function.args, line_index, out);
	if let Some(filter) = &function.filter {
		collect_expr_subqueries(filter, line_index, out);
	}
	if let Some(window) = &function.over {
		match window {
			ast::WindowType::WindowSpec(spec) => {
				for expr in &spec.partition_by {
					collect_expr_subqueries(expr, line_index, out);
				}
				for expr in &spec.order_by {
					collect_expr_subqueries(&expr.expr, line_index, out);
				}
			}
			ast::WindowType::NamedWindow(_) => {}
		}
	}
}

fn collect_function_arguments_subqueries(
	args: &ast::FunctionArguments,
	line_index: &LineIndex,
	out: &mut Vec<RangeNode>,
) {
	match args {
		ast::FunctionArguments::None => {}
		ast::FunctionArguments::Subquery(query) => {
			if let Some(node) = build_query_node(query, line_index) {
				out.push(node);
			}
		}
		ast::FunctionArguments::List(list) => {
			for arg in &list.args {
				match arg {
					ast::FunctionArg::Named { arg, .. }
					| ast::FunctionArg::ExprNamed { arg, .. }
					| ast::FunctionArg::Unnamed(arg) => collect_function_arg_expr_subqueries(arg, line_index, out),
				}
			}
			for clause in &list.clauses {
				match clause {
					ast::FunctionArgumentClause::OrderBy(order_by) => {
						for item in order_by {
							collect_expr_subqueries(&item.expr, line_index, out);
						}
					}
					ast::FunctionArgumentClause::Limit(expr) => {
						collect_expr_subqueries(expr, line_index, out)
					}
					ast::FunctionArgumentClause::Having(bound) => {
						collect_expr_subqueries(&bound.1, line_index, out)
					}
					ast::FunctionArgumentClause::IgnoreOrRespectNulls(_)
					| ast::FunctionArgumentClause::OnOverflow(_)
					| ast::FunctionArgumentClause::Separator(_)
					| ast::FunctionArgumentClause::JsonNullClause(_)
					| ast::FunctionArgumentClause::JsonReturningClause(_) => {}
				}
			}
		}
	}
}

fn build_branch(kind: SemanticKind, span: ByteSpan, nested: Vec<RangeNode>) -> RangeNode {
	let mut children = Vec::new();
	let mut cursor = span.start;
	let mut nested = nested;
	nested.sort_unstable_by_key(|node| (node.start, node.end));

	for child in nested {
		if child.start < cursor || child.end > span.end || child.end <= child.start {
			continue;
		}
		if child.start > cursor {
			children.push(RangeNode::leaf(cursor, child.start));
		}
		cursor = child.end;
		children.push(child);
	}

	if cursor < span.end {
		children.push(RangeNode::leaf(cursor, span.end));
	}

	RangeNode {
		kind,
		start: span.start,
		end: span.end,
		children,
	}
}

fn visit_statement(
	statement: &ast::Statement,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match statement {
		ast::Statement::Query(query) => visit_query(query, line_index, global_offset, out),
		ast::Statement::CreateView(view) => {
			emit_object_name(
				&view.name,
				TokenCategory::Type,
				line_index,
				global_offset,
				out,
			);
			for column in &view.columns {
				emit_ident(
					&column.name,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			visit_query(&view.query, line_index, global_offset, out);
		}
		ast::Statement::CreateTable(table) => {
			emit_object_name(
				&table.name,
				TokenCategory::Type,
				line_index,
				global_offset,
				out,
			);
			for column in &table.columns {
				emit_ident(
					&column.name,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			if let Some(query) = &table.query {
				visit_query(query, line_index, global_offset, out);
			}
		}
		ast::Statement::Insert(insert) => {
			visit_table_object(&insert.table, line_index, global_offset, out);
			if let Some(alias) = &insert.table_alias {
				emit_ident(
					alias,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			for column in &insert.columns {
				emit_ident(
					column,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			if let Some(query) = &insert.source {
				visit_query(query, line_index, global_offset, out);
			}
		}
		ast::Statement::Update(update) => {
			visit_table_with_joins(&update.table, line_index, global_offset, out);
			for assignment in &update.assignments {
				match &assignment.target {
					ast::AssignmentTarget::ColumnName(name) => {
						emit_object_name(
							name,
							TokenCategory::Variable,
							line_index,
							global_offset,
							out,
						);
					}
					ast::AssignmentTarget::Tuple(columns) => {
						for column in columns {
							emit_object_name(
								column,
								TokenCategory::Variable,
								line_index,
								global_offset,
								out,
							);
						}
					}
				}
				visit_expr(&assignment.value, line_index, global_offset, out);
			}
			match &update.from {
				Some(ast::UpdateTableFromKind::BeforeSet(tables))
				| Some(ast::UpdateTableFromKind::AfterSet(tables)) => {
					for table in tables {
						visit_table_with_joins(table, line_index, global_offset, out);
					}
				}
				None => {}
			}
			if let Some(selection) = &update.selection {
				visit_expr(selection, line_index, global_offset, out);
			}
		}
		ast::Statement::Delete(delete) => {
			for table in &delete.tables {
				emit_object_name(table, TokenCategory::Type, line_index, global_offset, out);
			}
			match &delete.from {
				ast::FromTable::WithFromKeyword(tables)
				| ast::FromTable::WithoutKeyword(tables) => {
					for table in tables {
						visit_table_with_joins(table, line_index, global_offset, out);
					}
				}
			}
			if let Some(using_tables) = &delete.using {
				for table in using_tables {
					visit_table_with_joins(table, line_index, global_offset, out);
				}
			}
			if let Some(selection) = &delete.selection {
				visit_expr(selection, line_index, global_offset, out);
			}
		}
		_ => {}
	}
}

fn visit_query(
	query: &ast::Query,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	if let Some(with_clause) = &query.with {
		for cte in &with_clause.cte_tables {
			emit_ident(
				&cte.alias.name,
				TokenCategory::Type,
				line_index,
				global_offset,
				out,
			);
			for column in &cte.alias.columns {
				emit_ident(
					&column.name,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			visit_query(&cte.query, line_index, global_offset, out);
		}
	}

	visit_set_expr(&query.body, line_index, global_offset, out);

	if let Some(order_by) = &query.order_by {
		match &order_by.kind {
			ast::OrderByKind::Expressions(exprs) => {
				for expr in exprs {
					visit_expr(&expr.expr, line_index, global_offset, out);
				}
			}
			ast::OrderByKind::All(_) => {}
		}
	}
}

fn visit_set_expr(
	set_expr: &ast::SetExpr,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match set_expr {
		ast::SetExpr::Select(select) => visit_select(select, line_index, global_offset, out),
		ast::SetExpr::Query(query) => visit_query(query, line_index, global_offset, out),
		ast::SetExpr::SetOperation { left, right, .. } => {
			visit_set_expr(left, line_index, global_offset, out);
			visit_set_expr(right, line_index, global_offset, out);
		}
		ast::SetExpr::Values(values) => {
			for row in &values.rows {
				for expr in row {
					visit_expr(expr, line_index, global_offset, out);
				}
			}
		}
		_ => {}
	}
}

fn visit_select(
	select: &ast::Select,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	for projection in &select.projection {
		match projection {
			ast::SelectItem::UnnamedExpr(expr) => visit_expr(expr, line_index, global_offset, out),
			ast::SelectItem::ExprWithAlias { expr, alias } => {
				visit_expr(expr, line_index, global_offset, out);
				emit_ident(
					alias,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			ast::SelectItem::QualifiedWildcard(kind, _) => match kind {
				ast::SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
					emit_object_name(
						object_name,
						TokenCategory::Variable,
						line_index,
						global_offset,
						out,
					);
				}
				ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
					visit_expr(expr, line_index, global_offset, out);
				}
			},
			ast::SelectItem::Wildcard(_) => {}
		}
	}

	if let Some(into) = &select.into {
		emit_object_name(
			&into.name,
			TokenCategory::Type,
			line_index,
			global_offset,
			out,
		);
	}

	for table in &select.from {
		visit_table_with_joins(table, line_index, global_offset, out);
	}

	for lateral_view in &select.lateral_views {
		visit_expr(&lateral_view.lateral_view, line_index, global_offset, out);
		emit_object_name(
			&lateral_view.lateral_view_name,
			TokenCategory::Function,
			line_index,
			global_offset,
			out,
		);
		for alias in &lateral_view.lateral_col_alias {
			emit_ident(
				alias,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
		}
	}

	if let Some(expr) = &select.prewhere {
		visit_expr(expr, line_index, global_offset, out);
	}
	if let Some(expr) = &select.selection {
		visit_expr(expr, line_index, global_offset, out);
	}

	match &select.group_by {
		ast::GroupByExpr::All(_) => {}
		ast::GroupByExpr::Expressions(exprs, _) => {
			for expr in exprs {
				visit_expr(expr, line_index, global_offset, out);
			}
		}
	}

	for expr in &select.cluster_by {
		visit_expr(expr, line_index, global_offset, out);
	}
	for expr in &select.distribute_by {
		visit_expr(expr, line_index, global_offset, out);
	}
	for expr in &select.sort_by {
		visit_expr(&expr.expr, line_index, global_offset, out);
	}
	if let Some(expr) = &select.having {
		visit_expr(expr, line_index, global_offset, out);
	}
	if let Some(expr) = &select.qualify {
		visit_expr(expr, line_index, global_offset, out);
	}
	for named_window in &select.named_window {
		emit_ident(
			&named_window.0,
			TokenCategory::Variable,
			line_index,
			global_offset,
			out,
		);
		match &named_window.1 {
			ast::NamedWindowExpr::NamedWindow(name) => {
				emit_ident(
					name,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			ast::NamedWindowExpr::WindowSpec(spec) => {
				visit_window_spec(spec, line_index, global_offset, out)
			}
		}
	}
}

fn visit_table_with_joins(
	table_with_joins: &ast::TableWithJoins,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	visit_table_factor(&table_with_joins.relation, line_index, global_offset, out);
	for join in &table_with_joins.joins {
		visit_table_factor(&join.relation, line_index, global_offset, out);
		match &join.join_operator {
			ast::JoinOperator::Join(constraint)
			| ast::JoinOperator::Inner(constraint)
			| ast::JoinOperator::Left(constraint)
			| ast::JoinOperator::LeftOuter(constraint)
			| ast::JoinOperator::Right(constraint)
			| ast::JoinOperator::RightOuter(constraint)
			| ast::JoinOperator::FullOuter(constraint)
			| ast::JoinOperator::CrossJoin(constraint)
			| ast::JoinOperator::Semi(constraint)
			| ast::JoinOperator::LeftSemi(constraint)
			| ast::JoinOperator::RightSemi(constraint)
			| ast::JoinOperator::Anti(constraint)
			| ast::JoinOperator::LeftAnti(constraint)
			| ast::JoinOperator::RightAnti(constraint)
			| ast::JoinOperator::StraightJoin(constraint) => {
				visit_join_constraint(constraint, line_index, global_offset, out)
			}
			ast::JoinOperator::AsOf {
				match_condition,
				constraint,
			} => {
				visit_expr(match_condition, line_index, global_offset, out);
				visit_join_constraint(constraint, line_index, global_offset, out);
			}
			ast::JoinOperator::CrossApply | ast::JoinOperator::OuterApply => {}
		}
	}
}

fn visit_table_factor(
	table_factor: &ast::TableFactor,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match table_factor {
		ast::TableFactor::Table {
			name,
			alias,
			args,
			partitions,
			..
		} => {
			emit_object_name(name, TokenCategory::Type, line_index, global_offset, out);
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
			if let Some(args) = args {
				for arg in &args.args {
					visit_function_arg(arg, line_index, global_offset, out);
				}
			}
			for partition in partitions {
				emit_ident(
					partition,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
		}
		ast::TableFactor::Derived {
			subquery,
			alias,
			sample: _,
			lateral: _,
		} => {
			visit_query(subquery, line_index, global_offset, out);
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::TableFunction { expr, alias } => {
			visit_expr(expr, line_index, global_offset, out);
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::Function {
			name, args, alias, ..
		} => {
			emit_object_name(
				name,
				TokenCategory::Function,
				line_index,
				global_offset,
				out,
			);
			for arg in args {
				visit_function_arg(arg, line_index, global_offset, out);
			}
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::UNNEST {
			alias,
			array_exprs,
			with_offset_alias,
			..
		} => {
			for expr in array_exprs {
				visit_expr(expr, line_index, global_offset, out);
			}
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
			if let Some(alias) = with_offset_alias {
				emit_ident(
					alias,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
		}
		ast::TableFactor::JsonTable {
			json_expr,
			columns,
			alias,
			..
		} => {
			visit_expr(json_expr, line_index, global_offset, out);
			for column in columns {
				visit_json_table_column(column, line_index, global_offset, out);
			}
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::OpenJsonTable {
			json_expr,
			columns,
			alias,
			..
		} => {
			visit_expr(json_expr, line_index, global_offset, out);
			for column in columns {
				emit_ident(
					&column.name,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::NestedJoin {
			table_with_joins,
			alias,
		} => {
			visit_table_with_joins(table_with_joins, line_index, global_offset, out);
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::Pivot {
			table,
			aggregate_functions,
			value_column,
			alias,
			..
		} => {
			visit_table_factor(table, line_index, global_offset, out);
			for function in aggregate_functions {
				visit_expr(&function.expr, line_index, global_offset, out);
				if let Some(alias) = &function.alias {
					emit_ident(
						alias,
						TokenCategory::Variable,
						line_index,
						global_offset,
						out,
					);
				}
			}
			for expr in value_column {
				visit_expr(expr, line_index, global_offset, out);
			}
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::Unpivot {
			table,
			value,
			name,
			columns,
			alias,
			..
		} => {
			visit_table_factor(table, line_index, global_offset, out);
			visit_expr(value, line_index, global_offset, out);
			emit_ident(
				name,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
			for column in columns {
				visit_expr(&column.expr, line_index, global_offset, out);
				if let Some(alias) = &column.alias {
					emit_ident(
						alias,
						TokenCategory::Variable,
						line_index,
						global_offset,
						out,
					);
				}
			}
			if let Some(alias) = alias {
				emit_table_alias(alias, line_index, global_offset, out);
			}
		}
		ast::TableFactor::MatchRecognize { table, .. } => {
			visit_table_factor(table, line_index, global_offset, out);
		}
		_ => {}
	}
}

fn visit_join_constraint(
	constraint: &ast::JoinConstraint,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match constraint {
		ast::JoinConstraint::On(expr) => visit_expr(expr, line_index, global_offset, out),
		ast::JoinConstraint::Using(columns) => {
			for column in columns {
				emit_object_name(
					column,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
		}
		ast::JoinConstraint::Natural | ast::JoinConstraint::None => {}
	}
}

fn visit_table_object(
	table: &ast::TableObject,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match table {
		ast::TableObject::TableName(name) => {
			emit_object_name(name, TokenCategory::Type, line_index, global_offset, out)
		}
		ast::TableObject::TableFunction(function) => {
			visit_function(function, line_index, global_offset, out)
		}
	}
}

fn visit_function_arg(
	arg: &ast::FunctionArg,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match arg {
		ast::FunctionArg::Named { name, arg, .. } => {
			emit_ident(
				name,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
			visit_function_arg_expr(arg, line_index, global_offset, out);
		}
		ast::FunctionArg::ExprNamed { name, arg, .. } => {
			visit_expr(name, line_index, global_offset, out);
			visit_function_arg_expr(arg, line_index, global_offset, out);
		}
		ast::FunctionArg::Unnamed(arg) => {
			visit_function_arg_expr(arg, line_index, global_offset, out)
		}
	}
}

fn visit_function_arg_expr(
	arg: &ast::FunctionArgExpr,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match arg {
		ast::FunctionArgExpr::Expr(expr) => visit_expr(expr, line_index, global_offset, out),
		ast::FunctionArgExpr::QualifiedWildcard(name) => {
			emit_object_name(
				name,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
		}
		ast::FunctionArgExpr::Wildcard => {}
	}
}

fn visit_function(
	function: &ast::Function,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	emit_object_name(
		&function.name,
		TokenCategory::Function,
		line_index,
		global_offset,
		out,
	);
	visit_function_arguments(&function.parameters, line_index, global_offset, out);
	visit_function_arguments(&function.args, line_index, global_offset, out);
	if let Some(filter) = &function.filter {
		visit_expr(filter, line_index, global_offset, out);
	}
	if let Some(window) = &function.over {
		match window {
			ast::WindowType::WindowSpec(spec) => {
				visit_window_spec(spec, line_index, global_offset, out)
			}
			ast::WindowType::NamedWindow(name) => emit_ident(
				name,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			),
		}
	}
	for order_by in &function.within_group {
		visit_expr(&order_by.expr, line_index, global_offset, out);
	}
}

fn visit_function_arguments(
	args: &ast::FunctionArguments,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match args {
		ast::FunctionArguments::None => {}
		ast::FunctionArguments::Subquery(query) => {
			visit_query(query, line_index, global_offset, out)
		}
		ast::FunctionArguments::List(list) => {
			for arg in &list.args {
				visit_function_arg(arg, line_index, global_offset, out);
			}
			for clause in &list.clauses {
				match clause {
					ast::FunctionArgumentClause::OrderBy(order_by) => {
						for item in order_by {
							visit_expr(&item.expr, line_index, global_offset, out);
						}
					}
					ast::FunctionArgumentClause::Limit(expr) => {
						visit_expr(expr, line_index, global_offset, out)
					}
					ast::FunctionArgumentClause::Having(bound) => {
						visit_expr(&bound.1, line_index, global_offset, out)
					}
					ast::FunctionArgumentClause::IgnoreOrRespectNulls(_)
					| ast::FunctionArgumentClause::OnOverflow(_)
					| ast::FunctionArgumentClause::Separator(_)
					| ast::FunctionArgumentClause::JsonNullClause(_)
					| ast::FunctionArgumentClause::JsonReturningClause(_) => {}
				}
			}
		}
	}
}

fn visit_window_spec(
	spec: &ast::WindowSpec,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	if let Some(window_name) = &spec.window_name {
		emit_ident(
			window_name,
			TokenCategory::Variable,
			line_index,
			global_offset,
			out,
		);
	}
	for expr in &spec.partition_by {
		visit_expr(expr, line_index, global_offset, out);
	}
	for expr in &spec.order_by {
		visit_expr(&expr.expr, line_index, global_offset, out);
	}
}

fn visit_json_table_column(
	column: &ast::JsonTableColumn,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match column {
		ast::JsonTableColumn::Named(column) => {
			emit_ident(
				&column.name,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
		}
		ast::JsonTableColumn::ForOrdinality(name) => {
			emit_ident(
				name,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
		}
		ast::JsonTableColumn::Nested(nested) => {
			for column in &nested.columns {
				visit_json_table_column(column, line_index, global_offset, out);
			}
		}
	}
}

fn visit_expr(
	expr: &ast::Expr,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	match expr {
		ast::Expr::Identifier(ident) => {
			emit_ident(
				ident,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
		}
		ast::Expr::CompoundIdentifier(idents) => {
			for ident in idents {
				emit_ident(
					ident,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
		}
		ast::Expr::CompoundFieldAccess { root, access_chain } => {
			visit_expr(root, line_index, global_offset, out);
			for access in access_chain {
				match access {
					ast::AccessExpr::Dot(expr) => visit_expr(expr, line_index, global_offset, out),
					ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => {
						visit_expr(index, line_index, global_offset, out)
					}
					ast::AccessExpr::Subscript(ast::Subscript::Slice {
						lower_bound,
						upper_bound,
						stride,
					}) => {
						if let Some(expr) = lower_bound {
							visit_expr(expr, line_index, global_offset, out);
						}
						if let Some(expr) = upper_bound {
							visit_expr(expr, line_index, global_offset, out);
						}
						if let Some(expr) = stride {
							visit_expr(expr, line_index, global_offset, out);
						}
					}
				}
			}
		}
		ast::Expr::JsonAccess { value, .. }
		| ast::Expr::IsFalse(value)
		| ast::Expr::IsNotFalse(value)
		| ast::Expr::IsTrue(value)
		| ast::Expr::IsNotTrue(value)
		| ast::Expr::IsNull(value)
		| ast::Expr::IsNotNull(value)
		| ast::Expr::IsUnknown(value)
		| ast::Expr::IsNotUnknown(value)
		| ast::Expr::UnaryOp { expr: value, .. }
		| ast::Expr::Nested(value)
		| ast::Expr::Prefixed { value, .. } => visit_expr(value, line_index, global_offset, out),
		ast::Expr::IsDistinctFrom(left, right)
		| ast::Expr::IsNotDistinctFrom(left, right)
		| ast::Expr::BinaryOp { left, right, .. }
		| ast::Expr::AtTimeZone {
			timestamp: left,
			time_zone: right,
		}
		| ast::Expr::Position {
			expr: left,
			r#in: right,
		} => {
			visit_expr(left, line_index, global_offset, out);
			visit_expr(right, line_index, global_offset, out);
		}
		ast::Expr::InList { expr, list, .. } => {
			visit_expr(expr, line_index, global_offset, out);
			for item in list {
				visit_expr(item, line_index, global_offset, out);
			}
		}
		ast::Expr::InSubquery { expr, subquery, .. } => {
			visit_expr(expr, line_index, global_offset, out);
			visit_query(subquery, line_index, global_offset, out);
		}
		ast::Expr::InUnnest {
			expr, array_expr, ..
		} => {
			visit_expr(expr, line_index, global_offset, out);
			visit_expr(array_expr, line_index, global_offset, out);
		}
		ast::Expr::Between {
			expr, low, high, ..
		} => {
			visit_expr(expr, line_index, global_offset, out);
			visit_expr(low, line_index, global_offset, out);
			visit_expr(high, line_index, global_offset, out);
		}
		ast::Expr::Like { expr, pattern, .. }
		| ast::Expr::ILike { expr, pattern, .. }
		| ast::Expr::SimilarTo { expr, pattern, .. }
		| ast::Expr::RLike { expr, pattern, .. }
		| ast::Expr::AnyOp {
			left: expr,
			right: pattern,
			..
		}
		| ast::Expr::AllOp {
			left: expr,
			right: pattern,
			..
		} => {
			visit_expr(expr, line_index, global_offset, out);
			visit_expr(pattern, line_index, global_offset, out);
		}
		ast::Expr::Convert { expr, styles, .. } => {
			visit_expr(expr, line_index, global_offset, out);
			for style in styles {
				visit_expr(style, line_index, global_offset, out);
			}
		}
		ast::Expr::Cast { expr, .. }
		| ast::Expr::Extract { expr, .. }
		| ast::Expr::Ceil { expr, .. }
		| ast::Expr::Floor { expr, .. } => visit_expr(expr, line_index, global_offset, out),
		ast::Expr::Function(function) => visit_function(function, line_index, global_offset, out),
		ast::Expr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			if let Some(operand) = operand {
				visit_expr(operand, line_index, global_offset, out);
			}
			for condition in conditions {
				visit_expr(&condition.condition, line_index, global_offset, out);
				visit_expr(&condition.result, line_index, global_offset, out);
			}
			if let Some(expr) = else_result {
				visit_expr(expr, line_index, global_offset, out);
			}
		}
		ast::Expr::Exists { subquery, .. } | ast::Expr::Subquery(subquery) => {
			visit_query(subquery, line_index, global_offset, out);
		}
		ast::Expr::GroupingSets(sets) | ast::Expr::Cube(sets) | ast::Expr::Rollup(sets) => {
			for set in sets {
				for expr in set {
					visit_expr(expr, line_index, global_offset, out);
				}
			}
		}
		ast::Expr::Tuple(exprs) => {
			for expr in exprs {
				visit_expr(expr, line_index, global_offset, out);
			}
		}
		ast::Expr::Struct { values, fields } => {
			for expr in values {
				visit_expr(expr, line_index, global_offset, out);
			}
			for field in fields {
				if let Some(name) = &field.field_name {
					emit_ident(
						name,
						TokenCategory::Variable,
						line_index,
						global_offset,
						out,
					);
				}
			}
		}
		ast::Expr::Named { expr, name } => {
			visit_expr(expr, line_index, global_offset, out);
			emit_ident(
				name,
				TokenCategory::Variable,
				line_index,
				global_offset,
				out,
			);
		}
		ast::Expr::Array(array) => {
			for expr in &array.elem {
				visit_expr(expr, line_index, global_offset, out);
			}
		}
		ast::Expr::Lambda(lambda) => {
			for ident in &lambda.params {
				emit_ident(
					ident,
					TokenCategory::Variable,
					line_index,
					global_offset,
					out,
				);
			}
			visit_expr(&lambda.body, line_index, global_offset, out);
		}
		_ => {}
	}
}

fn emit_object_name(
	name: &ast::ObjectName,
	category: TokenCategory,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	for part in &name.0 {
		match part {
			ast::ObjectNamePart::Identifier(ident) => {
				emit_ident(ident, category, line_index, global_offset, out);
			}
			ast::ObjectNamePart::Function(function) => {
				emit_ident(
					&function.name,
					TokenCategory::Function,
					line_index,
					global_offset,
					out,
				);
				for arg in &function.args {
					visit_function_arg(arg, line_index, global_offset, out);
				}
			}
		}
	}
}

fn emit_table_alias(
	alias: &ast::TableAlias,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	emit_ident(
		&alias.name,
		TokenCategory::Variable,
		line_index,
		global_offset,
		out,
	);
	for column in &alias.columns {
		emit_ident(
			&column.name,
			TokenCategory::Variable,
			line_index,
			global_offset,
			out,
		);
	}
}

fn emit_ident(
	ident: &ast::Ident,
	category: TokenCategory,
	line_index: &LineIndex,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	if let Some(span) = line_index.span_to_byte_span(ident.span) {
		emit_span(span, category, global_offset, out);
	}
}

fn emit_span(
	span: ByteSpan,
	category: TokenCategory,
	global_offset: DocByte,
	out: &mut Vec<HighlightSpan>,
) {
	if span.start >= span.end {
		return;
	}
	out.push(HighlightSpan::new(
		global_offset.saturating_add(span.start as u64),
		global_offset.saturating_add(span.end as u64),
		category,
	));
}

#[cfg(test)]
mod tests {
	use super::*;
	use sqlparser::dialect::GenericDialect;
	use sqlparser::parser::Parser;

	fn parse(sql: &str) -> (Vec<ast::Statement>, LineIndex) {
		let dialect = GenericDialect {};
		let statements = Parser::parse_sql(&dialect, sql).unwrap();
		let index = LineIndex::new(sql);
		(statements, index)
	}

	#[test]
	fn semantic_highlights_mark_functions_tables_and_aliases() {
		let sql =
			"with sales_cte as (select sum(total) as gross from sales) select gross from sales_cte";
		let (statements, index) = parse(sql);
		let spans = extract_semantic_highlights(&statements, &index, DocByte::ZERO);
		let text_for =
			|span: &HighlightSpan| &sql[span.start.get() as usize..span.end.get() as usize];

		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Function && text_for(span) == "sum")
		);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Type && text_for(span) == "sales")
		);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Variable && text_for(span) == "gross")
		);
	}

	#[test]
	fn structural_topology_preserves_total_byte_length() {
		let sql = "with x as (select 1)\nselect * from x\n";
		let (statements, index) = parse(sql);
		let green = build_structural_topology(&statements, &index, sql.len() as u32);

		assert_eq!(green.byte_len, sql.len() as u32);
		assert!(!green.children.is_empty());
		assert!(
			green
				.children
				.iter()
				.any(|child| child.kind == SemanticKind::RelationalTable)
		);
	}
}
