use crate::schema::pbc::{
	ColumnPredicate, ColumnPredicateComparison, ColumnPredicateLiteral, PredicateTree, Program,
};
use std::sync::Arc;

pub type PbcProgramRef = Arc<Program>;

#[derive(Clone, Debug)]
pub enum EncodingComputePredicatePlan {
	Exact(EncodedPredicateNode),
	Prefilter {
		encoded: EncodedPredicateNode,
		residual_program: PbcProgramRef,
	},
	DenseFallback,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EncodedPredicateNode {
	Comparison(EncodedComparison),
	InList(EncodedInList),
	IsNull(EncodedColumnRef),
	IsNotNull(EncodedColumnRef),
	And(Vec<EncodedPredicateNode>),
	Or(Vec<EncodedPredicateNode>),
	Not(Box<EncodedPredicateNode>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodedResidualMode {
	None,
	DensePbcAfterPrefilter,
	DenseAggregateAfterPrefilter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodedColumnRef {
	pub column_index: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EncodedComparison {
	pub column: EncodedColumnRef,
	pub atom: PredicateAtom,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EncodedInList {
	pub column: EncodedColumnRef,
	pub atom: PredicateAtom,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PredicateAtom {
	pub op: PredicateAtomOp,
	pub literal: Option<ColumnPredicateLiteral>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PredicateAtomOp {
	Eq,
	Neq,
	Gt,
	Gte,
	Lt,
	Lte,
	InList,
	IsNull,
	IsNotNull,
}

impl PredicateAtom {
	pub fn from_column_predicate(predicate: &ColumnPredicate) -> Self {
		Self {
			op: PredicateAtomOp::from(predicate.comparison),
			literal: Some(predicate.literal.clone()),
		}
	}

	pub const fn is_null() -> Self {
		Self {
			op: PredicateAtomOp::IsNull,
			literal: None,
		}
	}

	pub const fn is_not_null() -> Self {
		Self {
			op: PredicateAtomOp::IsNotNull,
			literal: None,
		}
	}
}

pub fn encoded_predicate_node_from_tree(tree: &PredicateTree) -> Option<EncodedPredicateNode> {
	match tree {
		PredicateTree::Comparison(predicate) => {
			let column = EncodedColumnRef {
				column_index: predicate.column_index,
			};
			let atom = PredicateAtom::from_column_predicate(predicate);
			if atom.op == PredicateAtomOp::InList {
				Some(EncodedPredicateNode::InList(EncodedInList { column, atom }))
			} else {
				Some(EncodedPredicateNode::Comparison(EncodedComparison {
					column,
					atom,
				}))
			}
		}
		PredicateTree::IsNull { column_index } => {
			Some(EncodedPredicateNode::IsNull(EncodedColumnRef {
				column_index: *column_index,
			}))
		}
		PredicateTree::IsNotNull { column_index } => {
			Some(EncodedPredicateNode::IsNotNull(EncodedColumnRef {
				column_index: *column_index,
			}))
		}
		PredicateTree::And(branches) => branches
			.iter()
			.map(encoded_predicate_node_from_tree)
			.collect::<Option<Vec<_>>>()
			.map(EncodedPredicateNode::And),
		PredicateTree::Or(branches) => branches
			.iter()
			.map(encoded_predicate_node_from_tree)
			.collect::<Option<Vec<_>>>()
			.map(EncodedPredicateNode::Or),
		PredicateTree::AlwaysTrue | PredicateTree::Unsupported => None,
	}
}

impl From<ColumnPredicateComparison> for PredicateAtomOp {
	fn from(value: ColumnPredicateComparison) -> Self {
		match value {
			ColumnPredicateComparison::Eq => Self::Eq,
			ColumnPredicateComparison::Gt => Self::Gt,
			ColumnPredicateComparison::Gte => Self::Gte,
			ColumnPredicateComparison::Lt => Self::Lt,
			ColumnPredicateComparison::Lte => Self::Lte,
			ColumnPredicateComparison::InList => Self::InList,
		}
	}
}
