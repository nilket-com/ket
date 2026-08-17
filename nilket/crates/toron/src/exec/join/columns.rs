use std::collections::BTreeSet;

use crate::ir::{BoundAggregateCall, BoundExprId, BoundExprNode, BoundExprProgram, ColumnOrdinal};

pub(in crate::exec) fn collect_program_column_ordinals(
	program: &BoundExprProgram,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	for root in &program.roots {
		collect_program_node_column_ordinals(program, *root, columns);
	}
}

fn collect_program_node_column_ordinals(
	program: &BoundExprProgram,
	id: BoundExprId,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	match program.node(id) {
		Some(BoundExprNode::Column(column)) => {
			columns.insert(column.ordinal);
		}
		Some(BoundExprNode::Literal(_)) | Some(BoundExprNode::NullLiteral) | None => {}
		Some(BoundExprNode::Cast { input, .. })
		| Some(BoundExprNode::TryCast { input, .. })
		| Some(BoundExprNode::Unary { input, .. })
		| Some(BoundExprNode::Like { expr: input, .. })
		| Some(BoundExprNode::Substring { input, .. })
		| Some(BoundExprNode::Floor { input })
		| Some(BoundExprNode::TimeBucket { input, .. })
		| Some(BoundExprNode::AtTimeZone { input, .. })
		| Some(BoundExprNode::Extract { input, .. })
		| Some(BoundExprNode::DateTrunc { input, .. }) => {
			collect_program_node_column_ordinals(program, *input, columns);
		}
		Some(BoundExprNode::Binary { left, right, .. }) => {
			collect_program_node_column_ordinals(program, *left, columns);
			collect_program_node_column_ordinals(program, *right, columns);
		}
		Some(BoundExprNode::InList { expr, list, .. }) => {
			collect_program_node_column_ordinals(program, *expr, columns);
			for item in list {
				collect_program_node_column_ordinals(program, *item, columns);
			}
		}
		Some(BoundExprNode::Case {
			when_then,
			else_input,
		}) => {
			for (condition, result) in when_then {
				collect_program_node_column_ordinals(program, *condition, columns);
				collect_program_node_column_ordinals(program, *result, columns);
			}
			collect_program_node_column_ordinals(program, *else_input, columns);
		}
		Some(BoundExprNode::ScalarFunction { args, .. }) => {
			for arg in args {
				collect_program_node_column_ordinals(program, *arg, columns);
			}
		}
	}
}

pub(in crate::exec) fn grouped_aggregate_input_columns(
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	exchange_key: &BoundExprProgram,
	join_keys: &[ColumnOrdinal],
) -> Vec<ColumnOrdinal> {
	let mut columns = BTreeSet::new();
	collect_program_column_ordinals(group_expr, &mut columns);
	collect_program_column_ordinals(exchange_key, &mut columns);
	for call in aggr_expr {
		collect_program_column_ordinals(&call.args, &mut columns);
	}
	columns.extend(join_keys.iter().copied());
	columns.into_iter().collect()
}
