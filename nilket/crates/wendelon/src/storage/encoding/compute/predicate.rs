use crate::schema::pbc::{
	ColumnPredicateComparisonV1, ColumnPredicateLiteralV1, ColumnPredicateV1, PredicateTreeV1,
	Program,
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
	pub literal: Option<ColumnPredicateLiteralV1>,
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
	pub fn from_column_predicate(predicate: &ColumnPredicateV1) -> Self {
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

pub fn encoded_predicate_node_from_tree(tree: &PredicateTreeV1) -> Option<EncodedPredicateNode> {
	match tree {
		PredicateTreeV1::Comparison(predicate) => {
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
		PredicateTreeV1::IsNull { column_index } => {
			Some(EncodedPredicateNode::IsNull(EncodedColumnRef {
				column_index: *column_index,
			}))
		}
		PredicateTreeV1::IsNotNull { column_index } => {
			Some(EncodedPredicateNode::IsNotNull(EncodedColumnRef {
				column_index: *column_index,
			}))
		}
		PredicateTreeV1::And(branches) => branches
			.iter()
			.map(encoded_predicate_node_from_tree)
			.collect::<Option<Vec<_>>>()
			.map(EncodedPredicateNode::And),
		PredicateTreeV1::Or(branches) => branches
			.iter()
			.map(encoded_predicate_node_from_tree)
			.collect::<Option<Vec<_>>>()
			.map(EncodedPredicateNode::Or),
		PredicateTreeV1::AlwaysTrue | PredicateTreeV1::Unsupported => None,
	}
}

impl From<ColumnPredicateComparisonV1> for PredicateAtomOp {
	fn from(value: ColumnPredicateComparisonV1) -> Self {
		match value {
			ColumnPredicateComparisonV1::Eq => Self::Eq,
			ColumnPredicateComparisonV1::Gt => Self::Gt,
			ColumnPredicateComparisonV1::Gte => Self::Gte,
			ColumnPredicateComparisonV1::Lt => Self::Lt,
			ColumnPredicateComparisonV1::Lte => Self::Lte,
			ColumnPredicateComparisonV1::InList => Self::InList,
		}
	}
}
